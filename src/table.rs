use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::allocator::{Allocation, BlockAllocator};
use crate::config::{Config, Mode};
use crate::error::{Error, Result};
use crate::hash::xxh64;
use crate::hazard::{HazardGuard, HazardRegistry, RetireQueue};
use crate::layout::{DELETED_BIT, HEADS_START, PAGE_SIZE, align_up};
use crate::mapped_file::MappedFile;
use crate::node::{Node, allocate_node, read_node, set_private_next};

pub(crate) const HEADER_MAGIC: &[u8; 8] = b"CASDB001";
pub(crate) const FORMAT_VERSION: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutResult {
    Inserted,
    Replaced,
}

pub struct Database {
    pub(crate) config: Config,
    pub(crate) node_size: u64,
    pub(crate) heads: MappedFile,
    pub(crate) body: BlockAllocator,
    pub(crate) blobs: Option<BlockAllocator>,
    pub(crate) hazards: HazardRegistry,
    pub(crate) retired: RetireQueue,
    pub(crate) checkpoint_state: AtomicU64,
    pub(crate) checkpoint_generation: AtomicU64,
    pub(crate) path: PathBuf,
}

impl Database {
    /// Creates a new database directory and all sparse backing files.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, existing paths, unsupported
    /// filesystems, or failed mappings and allocations.
    pub fn create(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        config.validate()?;
        let node_size = config.node_size()?;
        let hazards = HazardRegistry::new(config.max_threads)?;
        let retired = RetireQueue::new(config.max_threads)?;
        let path = path.as_ref();
        std::fs::create_dir(path)?;
        let heads_length = heads_length(config.bucket_count)?;
        let heads = MappedFile::create(&path.join("heads"), heads_length, true, true)?;
        initialize_header(&heads, &config, node_size)?;
        heads.advise_heads()?;
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
        Ok(Self {
            config,
            node_size,
            heads,
            body,
            blobs,
            hazards,
            retired,
            checkpoint_state: AtomicU64::new(0),
            checkpoint_generation: AtomicU64::new(0),
            path: path.to_path_buf(),
        })
    }

    /// Adds a key to a set, replacing an existing equivalent entry.
    ///
    /// # Errors
    ///
    /// Returns an error when called on a map or when allocation fails.
    pub fn add(&self, key: &[u8]) -> Result<PutResult> {
        if self.config.mode != Mode::Set {
            return Err(Error::Unsupported("add is available only for sets"));
        }
        let guard = self.hazards.acquire()?;
        self.put_inner(key, None, &guard)
    }

    /// Inserts or replaces a map value.
    ///
    /// # Errors
    ///
    /// Returns an error when called on a set, when the key length differs from
    /// the configured width, or when allocation fails.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<PutResult> {
        if self.config.mode != Mode::Map {
            return Err(Error::Unsupported("put is available only for maps"));
        }
        let guard = self.hazards.acquire()?;
        self.put_inner(key, Some(value), &guard)
    }

    /// Returns a copied map value, allowing mapped storage to be reclaimed.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong key width, set mode, or corrupt storage.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.config.mode != Mode::Map {
            return Err(Error::Unsupported("get is available only for maps"));
        }
        self.validate_key(key)?;
        let guard = self.hazards.acquire()?;
        let hash = xxh64(key, self.config.hash_seed);
        let bucket = hash % self.config.bucket_count;
        let found = self.find(bucket, hash, key, &guard)?;
        let Some(node) = found.node else {
            return Ok(None);
        };
        if node.blob_length == 0 {
            guard.clear()?;
            return Ok(Some(Vec::new()));
        }
        let length = usize::try_from(node.blob_length)
            .map_err(|_| Error::Corrupt("blob length does not fit memory"))?;
        let blobs = self
            .blobs
            .as_ref()
            .ok_or(Error::Corrupt("map has no blob allocator"))?;
        let value = blobs.read(node.blob_offset, length)?;
        guard.clear()?;
        Ok(Some(value))
    }

    /// Tests membership in either a set or map.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong key width or corrupt storage.
    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        self.validate_key(key)?;
        let guard = self.hazards.acquire()?;
        let hash = xxh64(key, self.config.hash_seed);
        let bucket = hash % self.config.bucket_count;
        let contains = self.find(bucket, hash, key, &guard)?.node.is_some();
        guard.clear()?;
        Ok(contains)
    }

    /// Logically marks and physically unlinks one key using CAS operations.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong key width or corrupt storage.
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        self.validate_key(key)?;
        let guard = self.hazards.acquire()?;
        let hash = xxh64(key, self.config.hash_seed);
        let bucket = hash % self.config.bucket_count;
        let found = self.find(bucket, hash, key, &guard)?;
        let Some(target) = found.node else {
            return Ok(false);
        };
        let target_next = self.body.atomic_u64(target.offset)?;
        let next = loop {
            let observed = target_next.load(Ordering::Acquire);
            if observed & DELETED_BIT != 0 {
                break observed & !DELETED_BIT;
            }
            if target_next
                .compare_exchange(
                    observed,
                    observed | DELETED_BIT,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                break observed;
            }
        };
        self.unlink_offset(bucket, target.offset, next, &guard)?;
        guard.clear()?;
        self.reclaim_retired()?;
        Ok(true)
    }

    /// Reclaims currently unprotected retired nodes and their values.
    ///
    /// # Errors
    ///
    /// Returns a storage error if block counts or hole punching fail.
    pub fn reclaim(&self) -> Result<usize> {
        let _guard = self.hazards.acquire()?;
        self.reclaim_retired()
    }

    /// Flushes mapped contents without defining a checkpoint generation.
    ///
    /// # Errors
    ///
    /// Returns the first kernel writeback error.
    pub fn sync(&self) -> Result<()> {
        self.body.sync_all()?;
        if let Some(blobs) = &self.blobs {
            blobs.sync_all()?;
        }
        self.heads.sync_all()
    }

    /// Flushes the database and consumes the handle.
    ///
    /// # Errors
    ///
    /// Returns the first kernel writeback error.
    pub fn close(self) -> Result<()> {
        self.sync()
    }

    fn put_inner(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        guard: &HazardGuard<'_>,
    ) -> Result<PutResult> {
        self.validate_key(key)?;
        let blob = self.allocate_blob(value)?;
        let blob_offset = blob.map_or(0, |allocation| allocation.offset);
        let blob_length = value.map_or(0, <[u8]>::len);
        let blob_length_u64 =
            u64::try_from(blob_length).map_err(|_| Error::CapacityExhausted("blob length"))?;
        let hash = xxh64(key, self.config.hash_seed);
        let node = match allocate_node(
            &self.body,
            self.node_size,
            key,
            hash,
            blob_offset,
            blob_length_u64,
        ) {
            Ok(allocation) => allocation,
            Err(error) => {
                if let Some(blob) = blob {
                    let _released = self.release_blob(blob);
                }
                return Err(error);
            }
        };
        let bucket = hash % self.config.bucket_count;
        let mut private_next = 0;
        loop {
            let found = match self.find(bucket, hash, key, guard) {
                Ok(found) => found,
                Err(error) => {
                    self.release_private(node, blob)?;
                    return Err(error);
                }
            };
            let (existing, result) = if let Some(existing) = found.node {
                (Some(existing), PutResult::Replaced)
            } else {
                (None, PutResult::Inserted)
            };
            set_private_next(&self.body, node.offset, private_next, found.root)?;
            private_next = found.root;
            let incoming = self.head(bucket)?;
            if incoming
                .compare_exchange(
                    found.root,
                    node.offset,
                    Ordering::Release,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                if let Some(existing) = existing {
                    let successor = self.mark_node(existing.offset)?;
                    self.unlink_offset(bucket, existing.offset, successor, guard)?;
                }
                guard.clear()?;
                self.reclaim_retired()?;
                return Ok(result);
            }
        }
    }

    fn allocate_blob(&self, value: Option<&[u8]>) -> Result<Option<Allocation>> {
        let Some(value) = value else {
            return Ok(None);
        };
        if value.is_empty() {
            return Ok(None);
        }
        let blobs = self
            .blobs
            .as_ref()
            .ok_or(Error::Corrupt("map has no blob allocator"))?;
        let allocation = blobs.allocate(value.len(), 8)?;
        if let Err(error) = blobs.write(allocation, value) {
            let _released = blobs.release(allocation);
            return Err(error);
        }
        Ok(Some(allocation))
    }

    fn release_private(&self, node: Allocation, blob: Option<Allocation>) -> Result<()> {
        self.body.release(node)?;
        if let Some(blob) = blob {
            self.release_blob(blob)?;
        }
        Ok(())
    }

    fn release_blob(&self, allocation: Allocation) -> Result<()> {
        self.blobs
            .as_ref()
            .ok_or(Error::Corrupt("map has no blob allocator"))?
            .release(allocation)
    }

    fn find(&self, bucket: u64, hash: u64, key: &[u8], guard: &HazardGuard<'_>) -> Result<Found> {
        'restart: loop {
            guard.clear()?;
            let mut link = Link::Head(bucket);
            let root = self.link_atomic(link)?.load(Ordering::Acquire);
            let mut predecessor_hazard = 0_usize;
            let mut current_hazard = 1_usize;
            guard.protect(current_hazard, root)?;
            if self.link_atomic(link)?.load(Ordering::Acquire) != root {
                continue;
            }
            let mut current = root;
            while current != 0 {
                if current & DELETED_BIT != 0 {
                    continue 'restart;
                }
                let node = read_node(&self.body, current, self.node_size, self.config.key_size)?;
                if node.deleted() {
                    let successor = node.successor();
                    if self
                        .link_atomic(link)?
                        .compare_exchange(current, successor, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        continue 'restart;
                    }
                    self.retire_node(current)?;
                    continue 'restart;
                }
                if node.hash == hash && node.key == key {
                    return Ok(Found {
                        node: Some(node),
                        root,
                    });
                }
                let observed_next = node.next;
                let successor = node.successor();
                guard.protect(predecessor_hazard, successor)?;
                if self.body.atomic_u64(current)?.load(Ordering::Acquire) != observed_next {
                    continue 'restart;
                }
                link = Link::Node(current);
                current = successor;
                std::mem::swap(&mut predecessor_hazard, &mut current_hazard);
            }
            return Ok(Found { node: None, root });
        }
    }

    fn unlink_offset(
        &self,
        bucket: u64,
        target: u64,
        successor: u64,
        guard: &HazardGuard<'_>,
    ) -> Result<()> {
        'restart: loop {
            guard.clear()?;
            let mut link = Link::Head(bucket);
            let root = self.link_atomic(link)?.load(Ordering::Acquire);
            let mut predecessor_hazard = 0_usize;
            let mut current_hazard = 1_usize;
            guard.protect(current_hazard, root)?;
            if self.link_atomic(link)?.load(Ordering::Acquire) != root {
                continue;
            }
            let mut current = root;
            while current != 0 {
                let node = read_node(&self.body, current, self.node_size, self.config.key_size)?;
                if current == target {
                    if self
                        .link_atomic(link)?
                        .compare_exchange(target, successor, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.retire_node(target)?;
                        return Ok(());
                    }
                    continue 'restart;
                }
                if node.deleted() {
                    let next = node.successor();
                    if self
                        .link_atomic(link)?
                        .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        continue 'restart;
                    }
                    self.retire_node(current)?;
                    continue 'restart;
                }
                let observed_next = node.next;
                let next = node.successor();
                guard.protect(predecessor_hazard, next)?;
                if self.body.atomic_u64(current)?.load(Ordering::Acquire) != observed_next {
                    continue 'restart;
                }
                link = Link::Node(current);
                current = next;
                std::mem::swap(&mut predecessor_hazard, &mut current_hazard);
            }
            return Ok(());
        }
    }

    fn mark_node(&self, offset: u64) -> Result<u64> {
        let next = self.body.atomic_u64(offset)?;
        loop {
            let observed = next.load(Ordering::Acquire);
            if observed & DELETED_BIT != 0 {
                return Ok(observed & !DELETED_BIT);
            }
            if next
                .compare_exchange(
                    observed,
                    observed | DELETED_BIT,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(observed);
            }
        }
    }

    fn retire_node(&self, offset: u64) -> Result<()> {
        match self.retired.publish(offset) {
            Ok(()) => Ok(()),
            Err(Error::Busy(_)) => {
                self.reclaim_retired()?;
                self.retired.publish(offset)
            }
            Err(error) => Err(error),
        }
    }

    fn reclaim_retired(&self) -> Result<usize> {
        self.retired
            .reclaim(&self.hazards, |offset| self.release_retired(offset))
    }

    fn release_retired(&self, offset: u64) -> Result<()> {
        let node = read_node(&self.body, offset, self.node_size, self.config.key_size)?;
        let mut first_error = None;
        if node.blob_length != 0 {
            let length = u32::try_from(node.blob_length)
                .map_err(|_| Error::Corrupt("retired blob length exceeds one block"))?;
            let blob = self
                .blobs
                .as_ref()
                .ok_or(Error::Corrupt("retired map node has no blob allocator"))?;
            let allocation = blob.allocation_for(node.blob_offset, length)?;
            if let Err(error) = blob.release(allocation) {
                first_error = Some(error);
            }
        }
        let node_length = u32::try_from(self.node_size)
            .map_err(|_| Error::Corrupt("retired body node length overflow"))?;
        let allocation = self.body.allocation_for(offset, node_length)?;
        if let Err(error) = self.body.release(allocation) {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.len() != self.config.key_size {
            return Err(Error::InvalidKeyLength {
                expected: self.config.key_size,
                actual: key.len(),
            });
        }
        Ok(())
    }

    pub(crate) fn head(&self, bucket: u64) -> Result<&AtomicU64> {
        if bucket >= self.config.bucket_count {
            return Err(Error::Corrupt("bucket index is out of range"));
        }
        let offset = bucket
            .checked_mul(size_of::<u64>() as u64)
            .and_then(|value| value.checked_add(HEADS_START))
            .ok_or(Error::Corrupt("head offset overflow"))?;
        self.heads.atomic_u64(offset)
    }

    fn link_atomic(&self, link: Link) -> Result<&AtomicU64> {
        match link {
            Link::Head(bucket) => self.head(bucket),
            Link::Node(offset) => self.body.atomic_u64(offset),
        }
    }
}

#[derive(Clone, Copy)]
enum Link {
    Head(u64),
    Node(u64),
}

struct Found {
    node: Option<Node>,
    root: u64,
}

pub(crate) fn bank_size(bucket_count: u64) -> Result<u64> {
    bucket_count
        .checked_mul(size_of::<u64>() as u64)
        .and_then(|bytes| align_up(bytes, PAGE_SIZE))
        .ok_or(Error::InvalidConfig("checkpoint bank size overflow"))
}

pub(crate) fn snapshot_bank_start(bucket_count: u64, bank: u64) -> Result<u64> {
    if bank > 1 {
        return Err(Error::Corrupt("checkpoint bank index is out of range"));
    }
    let bank_size = bank_size(bucket_count)?;
    HEADS_START
        .checked_add(bank_size)
        .and_then(|start| start.checked_add(bank.checked_mul(bank_size)?))
        .ok_or(Error::InvalidConfig("checkpoint bank offset overflow"))
}

pub(crate) fn heads_length(bucket_count: u64) -> Result<u64> {
    HEADS_START
        .checked_add(
            bank_size(bucket_count)?
                .checked_mul(3)
                .ok_or(Error::InvalidConfig("heads file size overflow"))?,
        )
        .ok_or(Error::InvalidConfig("heads file size overflow"))
}

pub(crate) fn initialize_header(heads: &MappedFile, config: &Config, node_size: u64) -> Result<()> {
    let page_size = usize::try_from(PAGE_SIZE)
        .map_err(|_| Error::InvalidConfig("page size does not fit memory"))?;
    let mut header = Vec::new();
    header
        .try_reserve_exact(page_size)
        .map_err(|_| Error::OutOfMemory)?;
    header.resize(page_size, 0);
    let magic = header
        .get_mut(0..8)
        .ok_or(Error::Corrupt("heads header is too small"))?;
    magic.copy_from_slice(HEADER_MAGIC);
    write_header_u64(&mut header, 8, FORMAT_VERSION)?;
    write_header_u64(&mut header, 16, config.mode as u64)?;
    write_header_u64(&mut header, 24, config.bucket_count)?;
    write_header_u64(
        &mut header,
        32,
        u64::try_from(config.key_size).map_err(|_| Error::InvalidConfig("key size overflow"))?,
    )?;
    write_header_u64(&mut header, 40, config.body_capacity)?;
    write_header_u64(&mut header, 48, config.blob_capacity)?;
    write_header_u64(&mut header, 56, config.block_size)?;
    write_header_u64(&mut header, 64, u64::from(config.max_threads))?;
    write_header_u64(&mut header, 72, config.hash_seed)?;
    write_header_u64(&mut header, 80, node_size)?;
    // SAFETY: database creation is single-threaded and heads are not published yet.
    unsafe { heads.copy_in(0, &header) }
}

pub(crate) fn write_header_u64(header: &mut [u8], offset: usize, value: u64) -> Result<()> {
    let end = offset
        .checked_add(size_of::<u64>())
        .ok_or(Error::Corrupt("header field overflow"))?;
    header
        .get_mut(offset..end)
        .ok_or(Error::Corrupt("header field is out of bounds"))?
        .copy_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("db-experiment-{}-{name}", std::process::id()))
    }

    fn config(mode: Mode, buckets: u64, key_size: usize) -> Config {
        let mut config = Config::new(mode, buckets, key_size);
        config.block_size = 64 * 1_024;
        config.body_capacity = config.block_size * 8;
        config.blob_capacity = if mode == Mode::Map {
            config.block_size * 8
        } else {
            0
        };
        config
    }

    #[test]
    fn inserts_replaces_and_deletes_colliding_map_keys() -> Result<()> {
        let path = test_directory("map");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Map, 1, 4))?;
        assert_eq!(database.put(b"key1", b"first")?, PutResult::Inserted);
        assert_eq!(database.put(b"key2", b"second")?, PutResult::Inserted);
        assert_eq!(database.put(b"key1", b"new")?, PutResult::Replaced);
        assert_eq!(database.get(b"key1")?, Some(b"new".to_vec()));
        assert_eq!(database.get(b"key2")?, Some(b"second".to_vec()));
        assert!(database.delete(b"key1")?);
        assert_eq!(database.get(b"key1")?, None);
        assert_eq!(database.get(b"key2")?, Some(b"second".to_vec()));
        assert!(!database.delete(b"none")?);
        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn supports_concurrent_set_writers() -> Result<()> {
        let path = test_directory("set-concurrent");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Set, 16, 8))?;
        std::thread::scope(|scope| {
            for thread in 0_u64..8 {
                let database_ref = &database;
                scope.spawn(move || {
                    for item in 0_u64..64 {
                        let key = (thread * 64 + item).to_le_bytes();
                        assert!(database_ref.add(&key).is_ok());
                    }
                });
            }
        });
        for key in 0_u64..512 {
            assert!(database.contains(&key.to_le_bytes())?);
        }
        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn concurrent_replacements_do_not_leave_duplicates() -> Result<()> {
        let path = test_directory("replace-concurrent");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Map, 1, 8))?;
        std::thread::scope(|scope| {
            for value in 0_u64..8 {
                let database_ref = &database;
                scope.spawn(move || {
                    assert!(database_ref.put(b"same-key", &value.to_le_bytes()).is_ok());
                });
            }
        });
        assert!(database.delete(b"same-key")?);
        assert_eq!(database.get(b"same-key")?, None);
        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn hazard_delays_online_hole_punching() -> Result<()> {
        let path = test_directory("hazard-reclaim");
        let _ignored = std::fs::remove_dir_all(&path);
        let mut test_config = config(Mode::Set, 1, 4_000);
        test_config.block_size = PAGE_SIZE;
        test_config.body_capacity = PAGE_SIZE * 8;
        let database = Database::create(&path, test_config)?;
        let key = vec![7; 4_000];
        database.add(&key)?;

        let guard = database.hazards.acquire()?;
        let hash = xxh64(&key, database.config.hash_seed);
        let found = database.find(0, hash, &key, &guard)?;
        let old_offset = found
            .node
            .ok_or(Error::Corrupt("test could not find inserted key"))?
            .offset;
        std::thread::scope(|scope| scope.spawn(|| database.add(&key)).join())
            .map_err(|_| Error::Corrupt("replacement test thread panicked"))??;
        assert!(
            database
                .body
                .read(old_offset, 8)?
                .iter()
                .any(|byte| *byte != 0)
        );

        guard.clear()?;
        assert!(database.reclaim()? > 0);
        assert!(
            database
                .body
                .read(old_offset, 8)?
                .iter()
                .all(|byte| *byte == 0)
        );
        drop(guard);
        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn readers_never_observe_invalid_replacement_state() -> Result<()> {
        let path = test_directory("read-replace-race");
        let _ignored = std::fs::remove_dir_all(&path);
        let mut test_config = config(Mode::Map, 1, 8);
        test_config.body_capacity = test_config.block_size * 16;
        test_config.blob_capacity = test_config.block_size * 16;
        let database = Database::create(&path, test_config)?;
        database.put(b"race-key", &0_u64.to_le_bytes())?;
        std::thread::scope(|scope| -> Result<()> {
            let writer = scope.spawn(|| -> Result<()> {
                for value in 1_u64..1_000 {
                    database.put(b"race-key", &value.to_le_bytes())?;
                }
                Ok(())
            });
            let reader = scope.spawn(|| -> Result<()> {
                for _iteration in 0..1_000 {
                    let value = database
                        .get(b"race-key")?
                        .ok_or(Error::Corrupt("replacement made the key disappear"))?;
                    if value.len() != size_of::<u64>() {
                        return Err(Error::Corrupt("reader observed an invalid value"));
                    }
                }
                Ok(())
            });
            writer
                .join()
                .map_err(|_| Error::Corrupt("writer test thread panicked"))??;
            reader
                .join()
                .map_err(|_| Error::Corrupt("reader test thread panicked"))??;
            Ok(())
        })?;
        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
}
