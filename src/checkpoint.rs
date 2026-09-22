// SPDX-License-Identifier: MIT OR Apache-2.0

//! Durable checkpoint creation and recovery.
//!
//! Checkpoints copy each live bucket chain into one of two immutable snapshot
//! generations. Checksummed manifests select the newest valid generation when
//! [`Database::open`] rebuilds mutable runtime files.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::allocator::{Allocation, BlockAllocator, read_high_water};
use crate::config::{Config, KEY_SIZE, Mode};
use crate::error::{Error, Result};
use crate::hash::xxh64;
use crate::layout::{
    BLOB_HEADER_SIZE, FORMAT_PAGE_SIZE, NODE_SIZE_U64, VALUE_BLOB_TAG, VALUE_OFFSET_MASK,
};
use crate::mapped_file::MappedFile;
use crate::node::{
    Node, allocate_node, blob_checksum, decode_blob_header, read_next, read_node, set_private_next,
};
use crate::table::{
    Database, FORMAT_VERSION, HEADER_CHECKSUM_OFFSET, HEADER_MAGIC, create_delete_locks,
    create_runtime_heads, header_checksum, heads_length, snapshot_bank_start, write_header_u64,
};

const CHECKPOINT_IDLE: u64 = 0;
const CHECKPOINT_BUSY: u64 = 1;
const MANIFEST_MAGIC: u64 = 0x4341_5343_484b_5031;
const MANIFEST_BASE: u64 = 128;
const MANIFEST_STRIDE: u64 = 64;

#[derive(Clone, Copy)]
struct Manifest {
    generation: u64,
    bank: u64,
    snapshot_checksum: u64,
}

struct SnapshotFiles {
    body: BlockAllocator,
    blobs: Option<BlockAllocator>,
}

impl Database {
    /// Opens mutable runtime files written by a clean [`Database::close`] without requiring a
    /// checkpoint generation.
    ///
    /// This is intentionally not crash recovery: interrupted writes may leave runtime files
    /// inconsistent. Use [`Database::open`] when checkpoint durability is required.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime layout is invalid or mapped files cannot be reopened.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let path = "floresta-db-open-runtime-example";
    /// let database = Database::create(path, Config::new(Mode::Set, 1_024))?;
    /// database.add(b"key-0001-0000000")?;
    /// database.close()?;
    ///
    /// let reopened = Database::open_runtime(path)?;
    /// assert!(reopened.contains(b"key-0001-0000000")?);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn open_runtime(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let heads_path = path.join("heads");
        let mapped_length = std::fs::metadata(&heads_path)?.len();
        let heads = MappedFile::open(&heads_path, mapped_length, false)?;
        let (config, node_size) = read_config(&heads)?;
        if heads_length(config.bucket_count)? != mapped_length {
            return Err(Error::Corrupt(
                "heads file length does not match its configuration",
            ));
        }
        heads.advise_heads()?;
        let runtime_heads = create_runtime_heads(config.bucket_count, &heads, true)?;
        let delete_locks = create_delete_locks(config.bucket_count)?;
        let body = BlockAllocator::open(
            &path.join("body"),
            &path.join("body.counts"),
            config.body_capacity,
            config.block_size,
        )?;
        let blobs = if config.mode == Mode::Map {
            Some(BlockAllocator::open(
                &path.join("blobs"),
                &path.join("blobs.counts"),
                config.blob_capacity,
                config.block_size,
            )?)
        } else {
            None
        };
        Ok(Self {
            config,
            node_size,
            heads,
            runtime_heads,
            delete_locks,
            body,
            blobs,
            checkpoint_state: AtomicU64::new(CHECKPOINT_IDLE),
            checkpoint_generation: AtomicU64::new(0),
            path: path.to_path_buf(),
        })
    }

    /// Ensures an unopened runtime database can map at least `headroom` bytes
    /// beyond the allocator's body high-water mark.
    ///
    /// Existing offsets and backing-file lengths stay unchanged. The next open
    /// reserves the larger maximum mapping; the allocator grows the file only
    /// when no reusable zero-count page remains.
    ///
    /// Returns `true` when the persisted maximum capacity was enlarged.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid headroom, corrupt allocator metadata, or
    /// failure to persist the enlarged configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let path = "floresta-db-headroom-example";
    /// Database::create(path, Config::new(Mode::Set, 1_024))?.close()?;
    /// let _grew = Database::ensure_runtime_body_headroom(path, 64 << 20)?;
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn ensure_runtime_body_headroom(path: impl AsRef<Path>, headroom: u64) -> Result<bool> {
        let path = path.as_ref();
        let heads_path = path.join("heads");
        let mapped_length = std::fs::metadata(&heads_path)?.len();
        let heads = MappedFile::open(&heads_path, mapped_length, false)?;
        let (mut config, _) = read_config(&heads)?;
        if headroom == 0 {
            return Err(Error::InvalidConfig(
                "runtime body headroom must be nonzero",
            ));
        }
        let required_headroom = headroom
            .div_ceil(config.block_size)
            .checked_mul(config.block_size)
            .ok_or(Error::InvalidConfig("runtime body headroom overflow"))?;

        let next_block = read_high_water(&path.join("body.counts"))?;
        let block_count = config.body_capacity / config.block_size;
        if next_block > block_count {
            return Err(Error::Corrupt(
                "body allocator high-water mark is out of range",
            ));
        }
        let used = next_block
            .checked_mul(config.block_size)
            .ok_or(Error::Corrupt("body allocator used capacity overflow"))?;
        let remaining = config
            .body_capacity
            .checked_sub(used)
            .ok_or(Error::Corrupt("body allocator remaining capacity overflow"))?;
        if remaining >= required_headroom {
            return Ok(false);
        }

        let new_capacity = used
            .checked_add(required_headroom)
            .ok_or(Error::InvalidConfig("runtime body capacity overflow"))?;

        let page_size = usize::try_from(FORMAT_PAGE_SIZE)
            .map_err(|_| Error::Corrupt("page size does not fit memory"))?;
        let mut header = heads.copy_out(0, page_size)?;
        config.body_capacity = new_capacity;
        write_header_u64(&mut header, 40, config.body_capacity)?;
        let checksum = header_checksum(&header)?;
        write_header_u64(&mut header, HEADER_CHECKSUM_OFFSET, checksum)?;
        // SAFETY: callers must invoke this before opening the runtime database, so no header
        // readers or allocator mappings are published.
        unsafe { heads.copy_in(0, &header)? };
        heads.sync_all()?;
        Ok(true)
    }

    /// Opens the newest valid checkpoint and rebuilds fresh mutable runtime files.
    ///
    /// # Errors
    ///
    /// Returns an error when no valid checkpoint exists, the format is invalid,
    /// or rebuilding mapped files fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let path = "floresta-db-open-example";
    /// let database = Database::create(path, Config::new(Mode::Map, 1_024))?;
    /// database.put(b"key-0001-0000000", b"value")?;
    /// database.checkpoint()?;
    /// drop(database);
    ///
    /// let reopened = Database::open(path)?;
    /// assert_eq!(
    ///     reopened.get(b"key-0001-0000000")?.as_deref(),
    ///     Some(b"value".as_slice())
    /// );
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let heads_path = path.join("heads");
        let mapped_length = std::fs::metadata(&heads_path)?.len();
        let heads = MappedFile::open(&heads_path, mapped_length, false)?;
        let (config, node_size) = read_config(&heads)?;
        if heads_length(config.bucket_count)? != mapped_length {
            return Err(Error::Corrupt(
                "heads file length does not match its configuration",
            ));
        }
        let manifests = read_manifests(&heads)?;
        drop(heads);
        let mut first_error = None;
        for manifest in manifests.into_iter().flatten() {
            match Self::open_generation(path, &config, node_size, mapped_length, manifest) {
                Ok(database) => return Ok(database),
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        Err(first_error.unwrap_or(Error::Corrupt("database has no valid checkpoint")))
    }

    fn open_generation(
        path: &Path,
        config: &Config,
        node_size: u64,
        mapped_length: u64,
        manifest: Manifest,
    ) -> Result<Self> {
        let heads = MappedFile::open(&path.join("heads"), mapped_length, false)?;
        let snapshot = open_snapshot(path, config, manifest.bank)?;
        validate_snapshot(&heads, &snapshot, config, node_size, manifest)?;
        heads.advise_heads()?;
        remove_runtime_files(path, config.mode)?;
        let body = BlockAllocator::create(
            &path.join("body"),
            &path.join("body.counts"),
            config.body_capacity,
            config.block_size,
        )?;
        let blobs = if config.mode == Mode::Map {
            Some(BlockAllocator::create(
                &path.join("blobs"),
                &path.join("blobs.counts"),
                config.blob_capacity,
                config.block_size,
            )?)
        } else {
            None
        };
        let runtime_heads = create_runtime_heads(config.bucket_count, &heads, false)?;
        let delete_locks = create_delete_locks(config.bucket_count)?;
        let database = Self {
            config: config.clone(),
            node_size,
            heads,
            runtime_heads,
            delete_locks,
            body,
            blobs,
            checkpoint_state: AtomicU64::new(CHECKPOINT_IDLE),
            checkpoint_generation: AtomicU64::new(manifest.generation),
            path: path.to_path_buf(),
        };
        database.clear_runtime_heads()?;
        database.rebuild_runtime(&snapshot, manifest.bank)?;
        Ok(database)
    }

    /// Copies a concurrent per-bucket view into an immutable durable generation.
    ///
    /// Append-only operations completed before this call are included; appends
    /// that overlap it may appear depending on when their bucket is copied.
    /// Deletions and replacements must be externally quiescent for the complete
    /// checkpoint, as required by [`Database`]'s removal contract.
    ///
    /// # Errors
    ///
    /// Returns an error when another checkpoint is active, snapshot capacity is
    /// exhausted, or ordered flushing fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-checkpoint-example",
    ///     Config::new(Mode::Map, 1_024),
    /// )?;
    /// database.put(b"key-0001-0000000", b"value")?;
    /// assert_eq!(database.checkpoint()?, 1);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn checkpoint(&self) -> Result<u64> {
        self.checkpoint_state
            .compare_exchange(
                CHECKPOINT_IDLE,
                CHECKPOINT_BUSY,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| Error::Busy("another checkpoint is active"))?;
        let Some(failed_generation) = read_shared(&self.checkpoint_generation).checked_add(1)
        else {
            self.checkpoint_state
                .compare_exchange(
                    CHECKPOINT_BUSY,
                    CHECKPOINT_IDLE,
                    Ordering::Release,
                    Ordering::Acquire,
                )
                .map_err(|_| Error::Corrupt("checkpoint state changed unexpectedly"))?;
            return Err(Error::CapacityExhausted("checkpoint generation"));
        };
        let failed_bank = failed_generation & 1;
        let result = self.checkpoint_inner();
        if result.is_err() {
            let _cleanup = self.cleanup_failed_checkpoint(failed_bank);
        }
        let reset = self.checkpoint_state.compare_exchange(
            CHECKPOINT_BUSY,
            CHECKPOINT_IDLE,
            Ordering::Release,
            Ordering::Acquire,
        );
        if reset.is_err() {
            return Err(Error::Corrupt("checkpoint state changed unexpectedly"));
        }
        result
    }

    fn cleanup_failed_checkpoint(&self, bank: u64) -> Result<()> {
        invalidate_manifest(&self.heads, bank)?;
        self.heads.sync_all()?;
        remove_snapshot_files(&self.path, self.config.mode, bank)?;
        self.clear_snapshot_bank(bank)?;
        sync_namespace(&self.path)
    }

    fn checkpoint_inner(&self) -> Result<u64> {
        let previous_generation = read_shared(&self.checkpoint_generation);
        let generation = previous_generation
            .checked_add(1)
            .ok_or(Error::CapacityExhausted("checkpoint generation"))?;
        let bank = generation & 1;
        invalidate_manifest(&self.heads, bank)?;
        self.heads.sync_all()?;
        remove_snapshot_files(&self.path, self.config.mode, bank)?;
        self.clear_snapshot_bank(bank)?;

        let snapshot = create_snapshot(&self.path, &self.config, bank)?;
        for bucket in 0..self.config.bucket_count {
            let root = self.copy_bucket_to_snapshot(bucket, &snapshot)?;
            let destination = self.snapshot_root(bank, bucket)?;
            destination
                .compare_exchange(0, root, Ordering::Release, Ordering::Acquire)
                .map_err(|_| Error::Corrupt("checkpoint root bank was not empty"))?;
        }

        let snapshot_checksum =
            snapshot_checksum(&self.heads, &snapshot, &self.config, self.node_size, bank)?;
        snapshot.body.sync_all()?;
        if let Some(blobs) = &snapshot.blobs {
            blobs.sync_all()?;
        }
        sync_namespace(&self.path)?;
        self.heads.sync_all()?;
        write_manifest(
            &self.heads,
            Manifest {
                generation,
                bank,
                snapshot_checksum,
            },
        )?;
        self.heads.sync_all()?;
        self.checkpoint_generation
            .compare_exchange(
                previous_generation,
                generation,
                Ordering::Release,
                Ordering::Acquire,
            )
            .map_err(|_| Error::Corrupt("checkpoint generation changed unexpectedly"))?;
        Ok(generation)
    }

    fn copy_bucket_to_snapshot(&self, bucket: u64, snapshot: &SnapshotFiles) -> Result<u64> {
        'restart: loop {
            let runtime_head = self.head(bucket)?;
            let source_root = read_shared(runtime_head);
            if read_shared(runtime_head) != source_root {
                continue;
            }
            #[cfg(test)]
            checkpoint_capture_hook(bucket);
            let mut current = source_root;
            let mut snapshot_root = 0_u64;
            let mut snapshot_tail = 0_u64;
            while current != 0 {
                let node = match read_node(&self.body, current) {
                    Ok(node) => node,
                    Err(error) => {
                        release_snapshot_chain(
                            snapshot,
                            snapshot_root,
                            &self.config,
                            self.node_size,
                        )?;
                        return Err(error);
                    }
                };
                if !snapshot_chain_contains(&snapshot.body, snapshot_root, &node.key)? {
                    match copy_node(
                        self.blobs.as_ref(),
                        &snapshot.body,
                        snapshot.blobs.as_ref(),
                        &node,
                        0,
                    ) {
                        Ok(copied) => {
                            if snapshot_root == 0 {
                                snapshot_root = copied;
                            } else if let Err(error) =
                                set_private_next(&snapshot.body, snapshot_tail, 0, copied)
                            {
                                release_snapshot_node(
                                    snapshot,
                                    copied,
                                    &self.config,
                                    self.node_size,
                                )?;
                                release_snapshot_chain(
                                    snapshot,
                                    snapshot_root,
                                    &self.config,
                                    self.node_size,
                                )?;
                                return Err(error);
                            }
                            snapshot_tail = copied;
                        }
                        Err(error) => {
                            release_snapshot_chain(
                                snapshot,
                                snapshot_root,
                                &self.config,
                                self.node_size,
                            )?;
                            return Err(error);
                        }
                    }
                }
                let observed_next = node.next;
                let successor = node.next;
                if read_next(&self.body, current)? != observed_next {
                    release_snapshot_chain(snapshot, snapshot_root, &self.config, self.node_size)?;
                    continue 'restart;
                }
                current = successor;
            }
            if read_shared(runtime_head) != source_root {
                release_snapshot_chain(snapshot, snapshot_root, &self.config, self.node_size)?;
                continue;
            }
            return Ok(snapshot_root);
        }
    }

    fn rebuild_runtime(&self, snapshot: &SnapshotFiles, bank: u64) -> Result<()> {
        for bucket in 0..self.config.bucket_count {
            let mut source = read_shared(self.snapshot_root(bank, bucket)?);
            let mut runtime_root = 0_u64;
            while source != 0 {
                let node = read_node(&snapshot.body, source)?;
                runtime_root = copy_node(
                    snapshot.blobs.as_ref(),
                    &self.body,
                    self.blobs.as_ref(),
                    &node,
                    runtime_root,
                )?;
                source = node.next;
            }
            self.head(bucket)?
                .compare_exchange(0, runtime_root, Ordering::Release, Ordering::Acquire)
                .map_err(|_| Error::Corrupt("runtime head was not cleared during recovery"))?;
        }
        Ok(())
    }

    fn clear_runtime_heads(&self) -> Result<()> {
        for bucket in 0..self.config.bucket_count {
            cas_replace(self.head(bucket)?, 0);
        }
        Ok(())
    }

    fn clear_snapshot_bank(&self, bank: u64) -> Result<()> {
        for bucket in 0..self.config.bucket_count {
            cas_replace(self.snapshot_root(bank, bucket)?, 0);
        }
        Ok(())
    }

    fn snapshot_root(&self, bank: u64, bucket: u64) -> Result<&AtomicU64> {
        snapshot_root_atomic(&self.heads, &self.config, bank, bucket)
    }
}

fn copy_node(
    source_blobs: Option<&BlockAllocator>,
    destination_body: &BlockAllocator,
    destination_blobs: Option<&BlockAllocator>,
    node: &Node,
    next: u64,
) -> Result<u64> {
    let (value, blob) = copy_blob(source_blobs, destination_blobs, node.value)?;
    let allocation = match allocate_node(destination_body, &node.key, value) {
        Ok(allocation) => allocation,
        Err(error) => {
            if let (Some(blobs), Some(blob)) = (destination_blobs, blob) {
                let _released = blobs.release(blob);
            }
            return Err(error);
        }
    };
    if let Err(error) = set_private_next(destination_body, allocation.offset, 0, next) {
        let _released_node = destination_body.release(allocation);
        if let (Some(blobs), Some(blob)) = (destination_blobs, blob) {
            let _released_blob = blobs.release(blob);
        }
        return Err(error);
    }
    Ok(allocation.offset)
}

fn copy_blob(
    source: Option<&BlockAllocator>,
    destination: Option<&BlockAllocator>,
    encoded: u64,
) -> Result<(u64, Option<Allocation>)> {
    if encoded & VALUE_BLOB_TAG == 0 {
        return Ok((encoded, None));
    }
    let offset = encoded & VALUE_OFFSET_MASK;
    let source = source.ok_or(Error::Corrupt("checkpoint source blob file is missing"))?;
    let mut header = [0_u8; BLOB_HEADER_SIZE];
    source.read_into(offset, &mut header)?;
    let (length, expected_checksum) = decode_blob_header(header);
    let value_offset = offset
        .checked_add(BLOB_HEADER_SIZE as u64)
        .ok_or(Error::Corrupt("checkpoint blob offset overflow"))?;
    let bytes = source.read(value_offset, length as usize)?;
    if expected_checksum != blob_checksum(&bytes) {
        return Err(Error::Corrupt("checkpoint blob checksum does not match"));
    }
    let destination = destination.ok_or(Error::Corrupt("checkpoint blob file is missing"))?;
    let total = BLOB_HEADER_SIZE
        .checked_add(bytes.len())
        .ok_or(Error::Corrupt("checkpoint blob length overflow"))?;
    let allocation = destination.allocate(total, 8)?;
    if let Err(error) = destination
        .write_at(allocation, 0, &header)
        .and_then(|()| destination.write_at(allocation, BLOB_HEADER_SIZE, &bytes))
    {
        let _released = destination.release(allocation);
        return Err(error);
    }
    Ok((VALUE_BLOB_TAG | allocation.offset, Some(allocation)))
}

fn release_snapshot_chain(
    snapshot: &SnapshotFiles,
    mut root: u64,
    config: &Config,
    node_size: u64,
) -> Result<()> {
    while root != 0 {
        let node = read_node(&snapshot.body, root)?;
        let successor = node.next;
        release_snapshot_node(snapshot, root, config, node_size)?;
        root = successor;
    }
    Ok(())
}

fn release_snapshot_node(
    snapshot: &SnapshotFiles,
    offset: u64,
    config: &Config,
    node_size: u64,
) -> Result<()> {
    let node = read_node(&snapshot.body, offset)?;
    if node.value & VALUE_BLOB_TAG != 0 {
        if config.mode != Mode::Map {
            return Err(Error::Corrupt("set snapshot node has a blob tag"));
        }
        let blob_offset = node.value & VALUE_OFFSET_MASK;
        let blobs = snapshot
            .blobs
            .as_ref()
            .ok_or(Error::Corrupt("snapshot blob allocator is missing"))?;
        let mut header = [0_u8; BLOB_HEADER_SIZE];
        blobs.read_into(blob_offset, &mut header)?;
        let (length, _checksum) = decode_blob_header(header);
        let total = u32::try_from(BLOB_HEADER_SIZE)
            .ok()
            .and_then(|header| header.checked_add(length))
            .ok_or(Error::Corrupt("snapshot blob length overflow"))?;
        blobs.release(blobs.allocation_for(blob_offset, total)?)?;
    }
    let length =
        u32::try_from(node_size).map_err(|_| Error::Corrupt("snapshot node size overflow"))?;
    snapshot
        .body
        .release(snapshot.body.allocation_for(offset, length)?)
}

fn snapshot_chain_contains(body: &BlockAllocator, mut root: u64, key: &[u8]) -> Result<bool> {
    while root != 0 {
        let node = read_node(body, root)?;
        if node.key == key {
            return Ok(true);
        }
        root = node.next;
    }
    Ok(false)
}

fn validate_snapshot(
    heads: &MappedFile,
    snapshot: &SnapshotFiles,
    config: &Config,
    node_size: u64,
    manifest: Manifest,
) -> Result<()> {
    let checksum = snapshot_checksum(heads, snapshot, config, node_size, manifest.bank)?;
    if checksum != manifest.snapshot_checksum {
        return Err(Error::Corrupt(
            "checkpoint snapshot checksum does not match",
        ));
    }
    Ok(())
}

fn snapshot_checksum(
    heads: &MappedFile,
    snapshot: &SnapshotFiles,
    config: &Config,
    node_size: u64,
    bank: u64,
) -> Result<u64> {
    let node_length =
        u32::try_from(node_size).map_err(|_| Error::Corrupt("checkpoint node size overflow"))?;
    let maximum_nodes = config
        .body_capacity
        .checked_div(node_size)
        .and_then(|count| count.checked_add(1))
        .ok_or(Error::Corrupt("checkpoint node limit overflow"))?;
    let mut checksum = MANIFEST_MAGIC;
    for bucket in 0..config.bucket_count {
        let root = read_shared(snapshot_root_atomic(heads, config, bank, bucket)?);
        checksum = mix_checksum(checksum, bucket);
        checksum = mix_checksum(checksum, root);
        let mut current = root;
        let mut visited = 0_u64;
        while current != 0 {
            visited = visited
                .checked_add(1)
                .ok_or(Error::Corrupt("checkpoint traversal overflow"))?;
            if visited > maximum_nodes {
                return Err(Error::Corrupt("checkpoint body chain contains a cycle"));
            }
            snapshot.body.allocation_for(current, node_length)?;
            let node = read_node(&snapshot.body, current)?;
            let node_hash = xxh64(&node.key, config.hash_seed);
            if node_hash % config.bucket_count != bucket {
                return Err(Error::Corrupt(
                    "checkpoint node is in the wrong hash bucket",
                ));
            }
            if snapshot_prefix_contains(&snapshot.body, root, current, maximum_nodes, &node.key)? {
                return Err(Error::Corrupt("checkpoint contains a duplicate key"));
            }
            validate_snapshot_blob(snapshot, config, &node)?;
            checksum = mix_checksum(checksum, node.offset);
            checksum = mix_checksum(checksum, node.next);
            checksum = mix_checksum(checksum, node.value);
            checksum = xxh64(&node.key, checksum);
            current = node.next;
        }
    }
    Ok(checksum)
}

fn snapshot_prefix_contains(
    body: &BlockAllocator,
    root: u64,
    target: u64,
    maximum_nodes: u64,
    key: &[u8],
) -> Result<bool> {
    let mut current = root;
    let mut visited = 0_u64;
    while current != target {
        if current == 0 {
            return Err(Error::Corrupt(
                "checkpoint target is not reachable from its root",
            ));
        }
        visited = visited
            .checked_add(1)
            .ok_or(Error::Corrupt("checkpoint prefix traversal overflow"))?;
        if visited > maximum_nodes {
            return Err(Error::Corrupt("checkpoint prefix contains a cycle"));
        }
        let node = read_node(body, current)?;
        if node.key == key {
            return Ok(true);
        }
        current = node.next;
    }
    Ok(false)
}

fn validate_snapshot_blob(snapshot: &SnapshotFiles, config: &Config, node: &Node) -> Result<()> {
    if config.mode == Mode::Set {
        return if node.value == 0 {
            Ok(())
        } else {
            Err(Error::Corrupt("set checkpoint node stores a value"))
        };
    }
    if node.value & VALUE_BLOB_TAG == 0 {
        return Ok(());
    }
    let offset = node.value & VALUE_OFFSET_MASK;
    let blobs = snapshot
        .blobs
        .as_ref()
        .ok_or(Error::Corrupt("map checkpoint blob file is missing"))?;
    let mut header = [0_u8; BLOB_HEADER_SIZE];
    blobs.read_into(offset, &mut header)?;
    let (length, expected_checksum) = decode_blob_header(header);
    let total = u32::try_from(BLOB_HEADER_SIZE)
        .ok()
        .and_then(|header| header.checked_add(length))
        .ok_or(Error::Corrupt("checkpoint blob length overflow"))?;
    blobs.allocation_for(offset, total)?;
    let value_offset = offset
        .checked_add(BLOB_HEADER_SIZE as u64)
        .ok_or(Error::Corrupt("checkpoint blob offset overflow"))?;
    let bytes = blobs.read(value_offset, length as usize)?;
    if expected_checksum != blob_checksum(&bytes) {
        return Err(Error::Corrupt("checkpoint blob checksum does not match"));
    }
    Ok(())
}

fn snapshot_root_atomic<'heads>(
    heads: &'heads MappedFile,
    config: &Config,
    bank: u64,
    bucket: u64,
) -> Result<&'heads AtomicU64> {
    if bucket >= config.bucket_count {
        return Err(Error::Corrupt("checkpoint bucket is out of range"));
    }
    let offset = snapshot_bank_start(config.bucket_count, bank)?
        .checked_add(
            bucket
                .checked_mul(size_of::<u64>() as u64)
                .ok_or(Error::Corrupt("checkpoint root offset overflow"))?,
        )
        .ok_or(Error::Corrupt("checkpoint root offset overflow"))?;
    heads.atomic_u64(offset)
}

fn mix_checksum(checksum: u64, value: u64) -> u64 {
    xxh64(&value.to_le_bytes(), checksum)
}

fn create_snapshot(path: &Path, config: &Config, bank: u64) -> Result<SnapshotFiles> {
    let body = BlockAllocator::create(
        &snapshot_path(path, bank, "body"),
        &snapshot_path(path, bank, "body.counts"),
        config.body_capacity,
        config.block_size,
    )?;
    let blobs = if config.mode == Mode::Map {
        Some(BlockAllocator::create(
            &snapshot_path(path, bank, "blobs"),
            &snapshot_path(path, bank, "blobs.counts"),
            config.blob_capacity,
            config.block_size,
        )?)
    } else {
        None
    };
    Ok(SnapshotFiles { body, blobs })
}

fn open_snapshot(path: &Path, config: &Config, bank: u64) -> Result<SnapshotFiles> {
    let body = BlockAllocator::open(
        &snapshot_path(path, bank, "body"),
        &snapshot_path(path, bank, "body.counts"),
        config.body_capacity,
        config.block_size,
    )?;
    let blobs = if config.mode == Mode::Map {
        Some(BlockAllocator::open(
            &snapshot_path(path, bank, "blobs"),
            &snapshot_path(path, bank, "blobs.counts"),
            config.blob_capacity,
            config.block_size,
        )?)
    } else {
        None
    };
    Ok(SnapshotFiles { body, blobs })
}

fn read_config(heads: &MappedFile) -> Result<(Config, u64)> {
    let page_size = usize::try_from(FORMAT_PAGE_SIZE)
        .map_err(|_| Error::Corrupt("page size does not fit memory"))?;
    let header = heads.copy_out(0, page_size)?;
    if header.get(0..8) != Some(HEADER_MAGIC.as_slice()) {
        return Err(Error::Corrupt("heads header magic does not match"));
    }
    if read_u64(&header, 8)? != FORMAT_VERSION {
        return Err(Error::Unsupported(
            "database format version is not supported",
        ));
    }
    if read_u64(&header, HEADER_CHECKSUM_OFFSET)? != header_checksum(&header)? {
        return Err(Error::Corrupt("heads header checksum does not match"));
    }
    let mode = match read_u64(&header, 16)? {
        1 => Mode::Set,
        2 => Mode::Map,
        _ => return Err(Error::Corrupt("database mode is invalid")),
    };
    if usize::try_from(read_u64(&header, 32)?)
        .map_err(|_| Error::Corrupt("key size does not fit memory"))?
        != KEY_SIZE
    {
        return Err(Error::Corrupt("stored key size is invalid"));
    }
    if read_u64(&header, 88)? != 0 {
        return Err(Error::Corrupt("stored inline policy is invalid"));
    }
    let config = Config {
        mode,
        bucket_count: read_u64(&header, 24)?,
        body_capacity: read_u64(&header, 40)?,
        blob_capacity: read_u64(&header, 48)?,
        block_size: read_u64(&header, 56)?,
        hash_seed: read_u64(&header, 72)?,
    };
    config.validate()?;
    let node_size = read_u64(&header, 80)?;
    if NODE_SIZE_U64 != node_size {
        return Err(Error::Corrupt("stored body node size is invalid"));
    }
    Ok((config, node_size))
}

fn read_manifests(heads: &MappedFile) -> Result<[Option<Manifest>; 2]> {
    let first = read_manifest(heads, 0)?;
    let second = read_manifest(heads, 1)?;
    match (first, second) {
        (Some(left), Some(right)) if right.generation > left.generation => {
            Ok([Some(right), Some(left)])
        }
        (left, right) => Ok([left, right]),
    }
}

fn read_manifest(heads: &MappedFile, bank: u64) -> Result<Option<Manifest>> {
    let offset = manifest_offset(bank)?;
    let magic = read_mapped_u64(heads, offset)?;
    if magic != MANIFEST_MAGIC {
        return Ok(None);
    }
    let generation = read_mapped_u64(heads, offset + 8)?;
    let stored_bank = read_mapped_u64(heads, offset + 16)?;
    let checksum = read_mapped_u64(heads, offset + 24)?;
    let snapshot_checksum = read_mapped_u64(heads, offset + 32)?;
    if stored_bank != bank
        || generation & 1 != bank
        || checksum != manifest_checksum(generation, bank, snapshot_checksum)
    {
        return Ok(None);
    }
    Ok(Some(Manifest {
        generation,
        bank,
        snapshot_checksum,
    }))
}

fn write_manifest(heads: &MappedFile, manifest: Manifest) -> Result<()> {
    let offset = manifest_offset(manifest.bank)?;
    cas_replace(heads.atomic_u64(offset)?, 0);
    cas_replace(heads.atomic_u64(offset + 8)?, manifest.generation);
    cas_replace(heads.atomic_u64(offset + 16)?, manifest.bank);
    cas_replace(heads.atomic_u64(offset + 32)?, manifest.snapshot_checksum);
    cas_replace(
        heads.atomic_u64(offset + 24)?,
        manifest_checksum(
            manifest.generation,
            manifest.bank,
            manifest.snapshot_checksum,
        ),
    );
    cas_replace(heads.atomic_u64(offset)?, MANIFEST_MAGIC);
    Ok(())
}

fn invalidate_manifest(heads: &MappedFile, bank: u64) -> Result<()> {
    cas_replace(heads.atomic_u64(manifest_offset(bank)?)?, 0);
    Ok(())
}

fn manifest_offset(bank: u64) -> Result<u64> {
    if bank > 1 {
        return Err(Error::Corrupt("manifest bank is out of range"));
    }
    MANIFEST_BASE
        .checked_add(
            bank.checked_mul(MANIFEST_STRIDE)
                .ok_or(Error::Corrupt("manifest offset overflow"))?,
        )
        .ok_or(Error::Corrupt("manifest offset overflow"))
}

fn manifest_checksum(generation: u64, bank: u64, snapshot_checksum: u64) -> u64 {
    let first = xxh64(&generation.to_le_bytes(), MANIFEST_MAGIC ^ bank);
    xxh64(&snapshot_checksum.to_le_bytes(), first)
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or(Error::Corrupt("header read overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or(Error::Corrupt("header field is out of range"))?;
    let array = <[u8; 8]>::try_from(source).map_err(|_| Error::Corrupt("invalid header field"))?;
    Ok(u64::from_le_bytes(array))
}

fn snapshot_path(path: &Path, bank: u64, suffix: &str) -> PathBuf {
    path.join(format!("snapshot.{bank}.{suffix}"))
}

fn remove_snapshot_files(path: &Path, mode: Mode, bank: u64) -> Result<()> {
    remove_if_exists(&snapshot_path(path, bank, "body"))?;
    remove_if_exists(&snapshot_path(path, bank, "body.counts"))?;
    if mode == Mode::Map {
        remove_if_exists(&snapshot_path(path, bank, "blobs"))?;
        remove_if_exists(&snapshot_path(path, bank, "blobs.counts"))?;
    }
    Ok(())
}

fn remove_runtime_files(path: &Path, mode: Mode) -> Result<()> {
    remove_if_exists(&path.join("body"))?;
    remove_if_exists(&path.join("body.counts"))?;
    if mode == Mode::Map {
        remove_if_exists(&path.join("blobs"))?;
        remove_if_exists(&path.join("blobs.counts"))?;
    }
    Ok(())
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn sync_namespace(path: &Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn cas_replace(atomic: &AtomicU64, replacement: u64) {
    let mut observed = read_shared(atomic);
    loop {
        match atomic.compare_exchange(observed, replacement, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(actual) => observed = actual,
        }
    }
}

fn read_shared(atomic: &AtomicU64) -> u64 {
    // SAFETY: supported targets provide aligned single-copy 64-bit reads. Mutating decisions
    // are guarded by compare-exchange, so a stale observation cannot publish stale state.
    unsafe { std::ptr::read_volatile(atomic.as_ptr()) }
}

fn read_mapped_u64(file: &MappedFile, offset: u64) -> Result<u64> {
    let bytes = file.copy_out(offset, size_of::<u64>())?;
    let array = <[u8; 8]>::try_from(bytes.as_slice())
        .map_err(|_| Error::Corrupt("mapped u64 field is truncated"))?;
    Ok(u64::from_le_bytes(array))
}

#[cfg(test)]
static CHECKPOINT_CAPTURE_HOOK: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
fn checkpoint_capture_hook(bucket: u64) {
    if bucket != 0 {
        return;
    }
    if CHECKPOINT_CAPTURE_HOOK
        .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        while CHECKPOINT_CAPTURE_HOOK.load(Ordering::SeqCst) != 3 {
            std::hint::spin_loop();
        }
        let _reset =
            CHECKPOINT_CAPTURE_HOOK.compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst);
    }
}

#[cfg(all(test, not(miri), any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use crate::table::PutResult;

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("floresta-db-{}-{name}", std::process::id()))
    }

    fn fixed_key(input: &[u8]) -> [u8; KEY_SIZE] {
        let mut key = [0_u8; KEY_SIZE];
        let length = input.len().min(KEY_SIZE);
        key[..length].copy_from_slice(&input[..length]);
        key
    }

    fn test_config() -> Config {
        let mut config = Config::new(Mode::Map, 8);
        config.block_size = 64 * 1_024;
        config.body_capacity = config.block_size * 8;
        config.blob_capacity = config.block_size * 8;
        config
    }

    #[test]
    fn grows_closed_runtime_body_with_stable_offsets() -> Result<()> {
        let path = test_directory("runtime-body-growth");
        let _ignored = std::fs::remove_dir_all(&path);
        let config = test_config();
        let block_size = config.block_size;
        let database = Database::create(&path, config)?;
        database.put(&fixed_key(b"firstkey"), b"one")?;
        database.close()?;

        assert!(Database::ensure_runtime_body_headroom(
            &path,
            block_size * 16
        )?);
        assert!(!Database::ensure_runtime_body_headroom(
            &path,
            block_size * 16
        )?);
        let database = Database::open_runtime(&path)?;
        assert_eq!(
            database.get(&fixed_key(b"firstkey"))?,
            Some(b"one".to_vec())
        );
        database.put(&fixed_key(b"secondky"), b"two")?;
        assert_eq!(
            database.get(&fixed_key(b"secondky"))?,
            Some(b"two".to_vec())
        );
        database.close()?;
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn reopens_the_latest_complete_checkpoint() -> Result<()> {
        let path = test_directory("checkpoint-open");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, test_config())?;
        assert_eq!(
            database.put(&fixed_key(b"firstkey"), b"one")?,
            PutResult::Inserted
        );
        assert_eq!(database.checkpoint()?, 1);
        database.put(&fixed_key(b"firstkey"), b"two")?;
        database.put(&fixed_key(b"secondky"), b"second")?;
        assert_eq!(database.checkpoint()?, 2);
        database.put(&fixed_key(b"firstkey"), b"not-checkpointed")?;
        drop(database);

        let reopened = Database::open(&path)?;
        assert_eq!(
            reopened.get(&fixed_key(b"firstkey"))?,
            Some(b"two".to_vec())
        );
        assert_eq!(
            reopened.get(&fixed_key(b"secondky"))?,
            Some(b"second".to_vec())
        );
        drop(reopened);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn checkpoint_runs_while_writers_continue() -> Result<()> {
        let path = test_directory("checkpoint-concurrent");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, test_config())?;
        for key in 0_u64..64 {
            database.put(&fixed_key(&key.to_le_bytes()), &key.to_le_bytes())?;
        }
        std::thread::scope(|scope| -> Result<()> {
            let writer = scope.spawn(|| -> Result<()> {
                for key in 64_u64..256 {
                    database.put(&fixed_key(&key.to_le_bytes()), &key.to_le_bytes())?;
                }
                Ok(())
            });
            database.checkpoint()?;
            writer
                .join()
                .map_err(|_| Error::Corrupt("checkpoint writer thread panicked"))??;
            Ok(())
        })?;
        drop(database);
        let reopened = Database::open(&path)?;
        for value in 0_u64..64 {
            assert_eq!(
                reopened.get(&fixed_key(&value.to_le_bytes()))?,
                Some(value.to_le_bytes().to_vec())
            );
        }
        drop(reopened);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn checkpoint_restarts_when_append_changes_its_captured_root() -> Result<()> {
        let path = test_directory("checkpoint-append-root");
        let _ignored = std::fs::remove_dir_all(&path);
        let mut config = test_config();
        config.bucket_count = 1;
        let database = Database::create(&path, config)?;
        database.put(&fixed_key(b"only-key"), b"old")?;
        CHECKPOINT_CAPTURE_HOOK
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| Error::Corrupt("checkpoint test hook was already active"))?;
        std::thread::scope(|scope| -> Result<()> {
            let checkpoint = scope.spawn(|| database.checkpoint());
            while CHECKPOINT_CAPTURE_HOOK.load(Ordering::SeqCst) != 2 {
                std::hint::spin_loop();
            }
            assert!(database.put_new(&fixed_key(b"otherkey"), b"new")?);
            CHECKPOINT_CAPTURE_HOOK
                .compare_exchange(2, 3, Ordering::SeqCst, Ordering::SeqCst)
                .map_err(|_| Error::Corrupt("checkpoint test hook changed unexpectedly"))?;
            checkpoint
                .join()
                .map_err(|_| Error::Corrupt("checkpoint test thread panicked"))??;
            Ok(())
        })?;
        drop(database);

        let reopened = Database::open(&path)?;
        assert_eq!(
            reopened.get(&fixed_key(b"only-key"))?,
            Some(b"old".to_vec())
        );
        assert_eq!(
            reopened.get(&fixed_key(b"otherkey"))?,
            Some(b"new".to_vec())
        );
        drop(reopened);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn open_falls_back_when_the_newest_snapshot_is_missing() -> Result<()> {
        let path = test_directory("checkpoint-fallback");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, test_config())?;
        database.put(&fixed_key(b"only-key"), b"generation-one")?;
        assert_eq!(database.checkpoint()?, 1);
        database.put(&fixed_key(b"only-key"), b"generation-two")?;
        assert_eq!(database.checkpoint()?, 2);
        drop(database);

        std::fs::remove_file(snapshot_path(&path, 0, "body"))?;
        let reopened = Database::open(&path)?;
        assert_eq!(
            reopened.get(&fixed_key(b"only-key"))?,
            Some(b"generation-one".to_vec())
        );
        drop(reopened);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn open_rejects_snapshot_root_corruption() -> Result<()> {
        let path = test_directory("checkpoint-root-corruption");
        let _ignored = std::fs::remove_dir_all(&path);
        let config = test_config();
        let database = Database::create(&path, config.clone())?;
        database.put(&fixed_key(b"only-key"), b"value")?;
        assert_eq!(database.checkpoint()?, 1);
        drop(database);

        let heads = MappedFile::open(
            &path.join("heads"),
            heads_length(config.bucket_count)?,
            false,
        )?;
        let key = fixed_key(b"only-key");
        let bucket = xxh64(&key, config.hash_seed) % config.bucket_count;
        let root = snapshot_root_atomic(&heads, &config, 1, bucket)?;
        cas_replace(root, 0);
        heads.sync_all()?;
        drop(heads);
        assert!(matches!(Database::open(&path), Err(Error::Corrupt(_))));
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn open_rejects_snapshot_blob_corruption() -> Result<()> {
        let path = test_directory("checkpoint-blob-corruption");
        let _ignored = std::fs::remove_dir_all(&path);
        let config = test_config();
        let database = Database::create(&path, config.clone())?;
        let key = *b"only-key-0000000";
        database.put(&key, b"value")?;
        assert_eq!(database.checkpoint()?, 1);
        drop(database);

        let heads = MappedFile::open(
            &path.join("heads"),
            heads_length(config.bucket_count)?,
            false,
        )?;
        let snapshot = open_snapshot(&path, &config, 1)?;
        let bucket = xxh64(&key, config.hash_seed) % config.bucket_count;
        let root = read_shared(snapshot_root_atomic(&heads, &config, 1, bucket)?);
        let node = read_node(&snapshot.body, root)?;
        let blob = snapshot
            .blobs
            .as_ref()
            .ok_or(Error::Corrupt("test snapshot blob file is missing"))?;
        let offset = node.value & VALUE_OFFSET_MASK;
        let mut header = [0_u8; BLOB_HEADER_SIZE];
        blob.read_into(offset, &mut header)?;
        let (length, _checksum) = decode_blob_header(header);
        let total = u32::try_from(BLOB_HEADER_SIZE)
            .ok()
            .and_then(|header| header.checked_add(length))
            .ok_or(Error::Corrupt("test blob length overflow"))?;
        blob.write_at(blob.allocation_for(offset, total)?, BLOB_HEADER_SIZE, &[0])?;
        blob.sync_all()?;
        drop(snapshot);
        drop(heads);

        assert!(matches!(Database::open(&path), Err(Error::Corrupt(_))));
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn open_rejects_header_corruption() -> Result<()> {
        let path = test_directory("checkpoint-header-corruption");
        let _ignored = std::fs::remove_dir_all(&path);
        let config = test_config();
        let database = Database::create(&path, config.clone())?;
        database.put(&fixed_key(b"only-key"), b"value")?;
        database.checkpoint()?;
        drop(database);

        let heads = MappedFile::open(
            &path.join("heads"),
            heads_length(config.bucket_count)?,
            false,
        )?;
        cas_replace(heads.atomic_u64(24)?, config.bucket_count + 1);
        heads.sync_all()?;
        drop(heads);
        assert!(matches!(Database::open(&path), Err(Error::Corrupt(_))));
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn checkpoint_supports_single_component_relative_paths() -> Result<()> {
        let path = PathBuf::from(format!(
            "floresta-db-relative-checkpoint-{}",
            std::process::id()
        ));
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, test_config())?;
        database.put(&fixed_key(b"only-key"), b"value")?;
        assert_eq!(database.checkpoint()?, 1);
        drop(database);
        let reopened = Database::open(&path)?;
        assert_eq!(
            reopened.get(&fixed_key(b"only-key"))?,
            Some(b"value".to_vec())
        );
        drop(reopened);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn open_rejects_manifest_with_wrong_generation_parity() -> Result<()> {
        let path = test_directory("checkpoint-manifest-parity");
        let _ignored = std::fs::remove_dir_all(&path);
        let config = test_config();
        let database = Database::create(&path, config.clone())?;
        database.put(&fixed_key(b"only-key"), b"value")?;
        database.checkpoint()?;
        drop(database);

        let heads = MappedFile::open(
            &path.join("heads"),
            heads_length(config.bucket_count)?,
            false,
        )?;
        let offset = manifest_offset(1)?;
        let snapshot_checksum = read_mapped_u64(&heads, offset + 32)?;
        cas_replace(heads.atomic_u64(offset + 8)?, 2);
        cas_replace(
            heads.atomic_u64(offset + 24)?,
            manifest_checksum(2, 1, snapshot_checksum),
        );
        heads.sync_all()?;
        drop(heads);
        assert!(matches!(Database::open(&path), Err(Error::Corrupt(_))));
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
}
