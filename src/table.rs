// SPDX-License-Identifier: MIT OR Apache-2.0

//! Concurrent hash-table operations and the primary database API.
//!
//! Buckets are separate chains headed by in-memory atomics. Batch APIs SIMD-hash
//! keys and visit heads in ascending bucket order. Append-only collisions are
//! privately linked before one head CAS; batch value reads sort external blob
//! offsets before restoring input order.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::allocator::{Allocation, BlockAllocator};
use crate::config::{Config, KEY_SIZE, Mode};
use crate::error::{Error, Result};
use crate::hash::{xxh64, xxh64_batch4};
use crate::layout::{
    BLOB_HEADER_SIZE, FORMAT_PAGE_SIZE, HEADS_START, NODE_ALIGNMENT, NODE_SIZE, NODE_SIZE_U64,
    VALUE_BLOB_TAG, VALUE_OFFSET_MASK, align_up,
};
use crate::mapped_file::MappedFile;
use crate::node::{
    Node, allocate_node, blob_checksum, blob_header, compare_exchange_next, decode_blob_header,
    read_next, read_node, set_private_next, tombstone_node, write_node,
};

pub(crate) const HEADER_MAGIC: &[u8; 8] = b"CASDB001";
pub(crate) const FORMAT_VERSION: u64 = 7;
pub(crate) const HEADER_CHECKSUM_OFFSET: usize = 96;
pub(crate) const HEADER_CHECKSUM_SEED: u64 = 0x4341_5344_4248_4452;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Reports whether an operation published a new node or replaced one.
///
/// # Examples
///
/// ```
/// use floresta_db::PutResult;
///
/// assert_ne!(PutResult::Inserted, PutResult::Replaced);
/// ```
pub enum PutResult {
    /// A new node was published; append-only set adds may retain an equivalent older node.
    Inserted,

    /// An existing node for the key was replaced.
    Replaced,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutOutcome {
    Inserted,
    Existing,
    Replaced,
}

struct PreparedKey<'key> {
    key: &'key [u8],

    hash: u64,

    bucket: u64,

    order: usize,
}

struct PreparedInsert<'entry> {
    key: &'entry [u8],

    value: Option<&'entry [u8]>,

    hash: u64,

    bucket: u64,

    order: usize,
}

struct PrivateInsert {
    node: Allocation,

    blob: Option<Allocation>,

    bucket: u64,
}

struct DetachedNode {
    body: Allocation,

    blob: Option<Allocation>,
}

struct PrivateGroup<'database> {
    head: &'database AtomicU64,

    tail_node: u64,

    root: u64,
}

struct BucketDeleteGuard<'database> {
    lock: &'database AtomicU64,
}

impl Drop for BucketDeleteGuard<'_> {
    fn drop(&mut self) {
        let mut observed = read_shared(self.lock);
        while observed != 0 {
            match self
                .lock
                .compare_exchange(observed, 0, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(actual) => observed = actual,
            }
        }
    }
}

/// A concurrent handle to one memory-mapped map or set.
///
/// Cloned handles are intentionally not provided; share one `Database` by
/// reference between scoped threads. Every key is exactly 16 bytes.
///
/// # Removal contract
///
/// Append-only writes may run concurrently. Operations that remove an existing
/// node—[`Database::delete`], [`Database::batch_delete`], [`Database::batch_pop`],
/// and replacing [`Database::put`] calls—require unique ownership of each logical
/// key and quiescence from reads or checkpoints that could retain an affected
/// bucket offset. Empty allocation pages can be reused immediately after unlink.
///
/// # Examples
///
/// ```no_run
/// use floresta_db::{Config, Database, Mode};
///
/// let database = Database::create(
///     "floresta-db-example",
///     Config::new(Mode::Set, 1_024),
/// )?;
/// assert!(!database.contains(&[0; 16])?);
/// # Ok::<(), floresta_db::Error>(())
/// ```
pub struct Database {
    pub(crate) config: Config,
    pub(crate) node_size: u64,
    pub(crate) heads: MappedFile,
    pub(crate) runtime_heads: Box<[AtomicU64]>,
    pub(crate) delete_locks: Box<[AtomicU64]>,
    pub(crate) body: BlockAllocator,
    pub(crate) blobs: Option<BlockAllocator>,
    pub(crate) checkpoint_state: AtomicU64,
    pub(crate) checkpoint_generation: AtomicU64,
    pub(crate) path: PathBuf,
}

/// An optimized batch writer for building a map without lookups.
///
/// The writer hashes and validates the complete batch, reserves fixed-width body
/// nodes in block-sized chunks, then publishes buckets in ascending order.
/// Entries targeting one bucket are linked privately and exposed by one
/// successful head CAS. Duplicate keys are retained; the last duplicate in a
/// batch is encountered first by readers.
///
/// # Examples
///
/// ```no_run
/// use floresta_db::{Config, Database, Mode};
///
/// let database = Database::create(
///     "floresta-db-writer-example",
///     Config::new(Mode::Map, 1_024),
/// )?;
/// let entries = [
///     (b"key-0001-0000000".as_slice(), b"value-01".as_slice()),
///     (b"key-0002-0000000".as_slice(), b"value-02".as_slice()),
/// ];
/// database.write_only()?.put_batch(entries)?;
/// # Ok::<(), floresta_db::Error>(())
/// ```
pub struct WriteOnlyWriter<'database> {
    database: &'database Database,
}

impl Database {
    /// Creates a new database directory and all sparse backing files.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, existing paths, unsupported
    /// filesystems, or failed mappings and allocations.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-create-example",
    ///     Config::new(Mode::Set, 1_024),
    /// )?;
    /// assert!(!database.contains(&[0; 16])?);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn create(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        config.validate()?;
        let node_size = NODE_SIZE_U64;
        let path = path.as_ref();
        std::fs::create_dir(path)?;
        let heads_length = heads_length(config.bucket_count)?;
        let heads = MappedFile::create(&path.join("heads"), heads_length, false, false)?;
        initialize_header(&heads, &config, node_size)?;
        heads.advise_heads()?;
        let runtime_heads = create_runtime_heads(config.bucket_count, &heads, false)?;
        let delete_locks = create_delete_locks(config.bucket_count)?;
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
            runtime_heads,
            delete_locks,
            body,
            blobs,
            checkpoint_state: AtomicU64::new(0),
            checkpoint_generation: AtomicU64::new(0),
            path: path.to_path_buf(),
        })
    }

    /// Adds one key to a set without searching the bucket.
    ///
    /// Every call publishes a new node and therefore returns
    /// [`PutResult::Inserted`], even when an equivalent key already exists.
    ///
    /// # Errors
    ///
    /// Returns an error when called on a map, when the key is not 16 bytes,
    /// or when allocation fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode, PutResult};
    ///
    /// let database = Database::create(
    ///     "floresta-db-add-example",
    ///     Config::new(Mode::Set, 1_024),
    /// )?;
    /// assert_eq!(database.add(&[1; 16])?, PutResult::Inserted);
    /// assert_eq!(database.add(&[1; 16])?, PutResult::Inserted);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn add(&self, key: &[u8]) -> Result<PutResult> {
        if self.config.mode != Mode::Set {
            return Err(Error::Unsupported("add is available only for sets"));
        }
        self.insert_one_inner(key, None)?;
        Ok(PutResult::Inserted)
    }

    /// Adds a batch of keys to a set without duplicate lookups.
    ///
    /// The complete batch is validated and hashed first. Entries are sorted by
    /// bucket while retaining input order within each bucket. All nodes for one
    /// bucket are linked privately, and the final input for that bucket becomes
    /// its new head after one successful head CAS. Buckets are published in
    /// ascending order to keep head accesses local.
    ///
    /// Duplicate keys are retained as separate nodes. The returned count is the
    /// number of nodes published.
    ///
    /// # Errors
    ///
    /// Returns an error when called on a map, when any key has the wrong width,
    /// or when validation, metadata allocation, or mapped allocation fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-add-batch-example",
    ///     Config::new(Mode::Set, 1_024),
    /// )?;
    /// let keys = [b"key-0001-0000000".as_slice(), b"key-0002-0000000".as_slice()];
    /// assert_eq!(database.add_batch(keys)?, 2);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn add_batch<'key, I>(&self, keys: I) -> Result<usize>
    where
        I: IntoIterator<Item = &'key [u8]>,
    {
        if self.config.mode != Mode::Set {
            return Err(Error::Unsupported("add_batch is available only for sets"));
        }
        self.insert_batch_inner(keys.into_iter().map(|key| (key, None)))
    }

    /// Inserts or replaces a map value.
    ///
    /// Replacing an existing value follows the [`Database`] removal contract;
    /// inserting an absent key remains append-only.
    ///
    /// # Errors
    ///
    /// Returns an error when called on a set, when the key is not 16 bytes,
    /// or when allocation fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode, PutResult};
    ///
    /// let database = Database::create(
    ///     "floresta-db-put-example",
    ///     Config::new(Mode::Map, 1_024),
    /// )?;
    /// assert_eq!(
    ///     database.put(b"key-0001-0000000", b"value")?,
    ///     PutResult::Inserted
    /// );
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<PutResult> {
        if self.config.mode != Mode::Map {
            return Err(Error::Unsupported("put is available only for maps"));
        }
        match self.put_inner(key, Some(value), true)? {
            PutOutcome::Inserted => Ok(PutResult::Inserted),
            PutOutcome::Replaced => Ok(PutResult::Replaced),
            PutOutcome::Existing => Err(Error::Corrupt("upsert reported an existing key")),
        }
    }

    /// Inserts a map value only when its key does not exist.
    ///
    /// This write-once path never replaces nodes. Concurrent attempts for the
    /// same key produce exactly one insertion.
    ///
    /// # Errors
    ///
    /// Returns an error when called on a set, when the key is not 16 bytes,
    /// or when allocation fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-put-new-example",
    ///     Config::new(Mode::Map, 1_024),
    /// )?;
    /// assert!(database.put_new(b"key-0001-0000000", b"value")?);
    /// assert!(!database.put_new(b"key-0001-0000000", b"other")?);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn put_new(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        if self.config.mode != Mode::Map {
            return Err(Error::Unsupported("put_new is available only for maps"));
        }
        match self.put_inner(key, Some(value), false)? {
            PutOutcome::Inserted => Ok(true),
            PutOutcome::Existing => Ok(false),
            PutOutcome::Replaced => Err(Error::Corrupt("insert-only put replaced a key")),
        }
    }

    /// Creates a batch writer for append-only map construction.
    ///
    /// The writer skips lookup, replacement, deletion, and reclamation.
    /// Duplicate keys are valid and remain as separate nodes.
    ///
    /// # Errors
    ///
    /// Returns an error when called on a set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-write-only-example",
    ///     Config::new(Mode::Map, 1_024),
    /// )?;
    /// let entries = [(b"key-0001-0000000".as_slice(), b"value-01".as_slice())];
    /// database.write_only()?.put_batch(entries)?;
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn write_only(&self) -> Result<WriteOnlyWriter<'_>> {
        if self.config.mode != Mode::Map {
            return Err(Error::Unsupported("write_only is available only for maps"));
        }
        Ok(WriteOnlyWriter { database: self })
    }

    /// Returns a copied map value, allowing mapped storage to be reclaimed.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong key width, set mode, or corrupt storage.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-get-example",
    ///     Config::new(Mode::Map, 1_024),
    /// )?;
    /// database.put(b"key-0001-0000000", b"value")?;
    /// assert_eq!(
    ///     database.get(b"key-0001-0000000")?.as_deref(),
    ///     Some(b"value".as_slice())
    /// );
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.config.mode != Mode::Map {
            return Err(Error::Unsupported("get is available only for maps"));
        }
        Self::validate_key(key)?;
        let hash = xxh64(key, self.config.hash_seed);
        let bucket = hash % self.config.bucket_count;
        let found = self.find(bucket, hash, key)?;
        let Some(node) = found.node else {
            return Ok(None);
        };
        Ok(Some(self.read_node_value(&node)?))
    }

    /// Fetches map values in a bucket-local batch.
    ///
    /// All keys are validated and hashed first, four at a time with AVX2 when
    /// available. Requests are sorted by ascending bucket, each bucket chain is
    /// traversed once, and results are restored to input order. Duplicate input
    /// keys produce duplicate output values.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong key width, set mode, failed result
    /// allocation, or corrupt storage.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-batch-fetch-example",
    ///     Config::new(Mode::Map, 1_024),
    /// )?;
    /// database.put_new(b"key-0001-0000000", b"value-01")?;
    /// let keys = [
    ///     b"key-0001-0000000".as_slice(),
    ///     b"missing!-0000000".as_slice(),
    /// ];
    /// let values = database.batch_fetch(keys)?;
    /// assert_eq!(values[0].as_deref(), Some(b"value-01".as_slice()));
    /// assert_eq!(values[1], None);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn batch_fetch<'key, I>(&self, keys: I) -> Result<Vec<Option<Vec<u8>>>>
    where
        I: IntoIterator<Item = &'key [u8]>,
    {
        if self.config.mode != Mode::Map {
            return Err(Error::Unsupported("batch_fetch is available only for maps"));
        }
        let prepared = self.prepare_keys(keys)?;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(prepared.len())
            .map_err(|_| Error::OutOfMemory)?;
        encoded.resize(prepared.len(), None);

        let mut start = 0;
        while start < prepared.len() {
            let bucket = prepared[start].bucket;
            let mut end = start + 1;
            while end < prepared.len() && prepared[end].bucket == bucket {
                end += 1;
            }
            self.fetch_bucket_batch(bucket, &prepared[start..end], &mut encoded)?;
            start = end;
        }
        self.materialize_values(&encoded)
    }

    /// Tests membership in either a set or map.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong key width or corrupt storage.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-contains-example",
    ///     Config::new(Mode::Set, 1_024),
    /// )?;
    /// database.add(b"key-0001-0000000")?;
    /// assert!(database.contains(b"key-0001-0000000")?);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        Self::validate_key(key)?;
        let hash = xxh64(key, self.config.hash_seed);
        let bucket = hash % self.config.bucket_count;
        Ok(self.find(bucket, hash, key)?.node.is_some())
    }

    /// Unlinks one uniquely owned key and immediately recycles empty pages.
    ///
    /// The caller must guarantee that no other thread can delete the same live
    /// key. Since reclaimed pages may be reused immediately, deletion also
    /// requires external quiescence from reads or checkpoints that could still
    /// retain an offset in the affected bucket.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong key width, corrupt storage, or a detected
    /// violation of the unique-deletion contract.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-delete-example",
    ///     Config::new(Mode::Set, 1_024),
    /// )?;
    /// database.add(b"key-0001-0000000")?;
    /// assert!(database.delete(b"key-0001-0000000")?);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        Self::validate_key(key)?;
        let hash = xxh64(key, self.config.hash_seed);
        let bucket = hash % self.config.bucket_count;
        let _delete_guard = self.lock_delete_bucket(bucket)?;
        loop {
            let found = self.find(bucket, hash, key)?;
            let Some(target) = found.node else {
                return Ok(false);
            };
            match self.unlink_link(found.link, &target) {
                Ok(()) => return Ok(true),
                Err(Error::Busy(_)) => std::hint::spin_loop(),
                Err(error) => return Err(error),
            }
        }
    }

    /// Deletes uniquely owned keys in a bucket-local batch.
    ///
    /// Keys are SIMD-hashed, sorted by ascending bucket, and each bucket chain
    /// is traversed once. Results retain input order and report whether each key
    /// was present. Duplicate input keys are rejected because they violate the
    /// unique-deletion contract.
    ///
    /// This method has the same removal and quiescence requirements as
    /// [`Database::delete`].
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate inputs, wrong key widths, corrupt storage,
    /// allocation failure, or a detected unique-deletion violation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-batch-delete-example",
    ///     Config::new(Mode::Set, 1_024),
    /// )?;
    /// database.add(b"key-0001-0000000")?;
    /// let keys = [
    ///     b"key-0001-0000000".as_slice(),
    ///     b"missing!-0000000".as_slice(),
    /// ];
    /// assert_eq!(database.batch_delete(keys)?, vec![true, false]);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn batch_delete<'key, I>(&self, keys: I) -> Result<Vec<bool>>
    where
        I: IntoIterator<Item = &'key [u8]>,
    {
        let prepared = self.prepare_keys(keys)?;
        Self::validate_unique_batch_keys(&prepared)?;
        let mut deleted = Vec::new();
        deleted
            .try_reserve_exact(prepared.len())
            .map_err(|_| Error::OutOfMemory)?;
        deleted.resize(prepared.len(), false);

        let mut start = 0;
        while start < prepared.len() {
            let bucket = prepared[start].bucket;
            let mut end = start + 1;
            while end < prepared.len() && prepared[end].bucket == bucket {
                end += 1;
            }
            let _delete_guard = self.lock_delete_bucket(bucket)?;
            self.delete_bucket_batch(bucket, &prepared[start..end], &mut deleted)?;
            start = end;
        }
        Ok(deleted)
    }

    /// Deletes map entries and returns their values in input order.
    ///
    /// Keys are SIMD-hashed and visited in bucket order. Removed body nodes stay
    /// reserved until every matching value is known; blob offsets are then sorted
    /// and read in ascending order before either body or blob storage is reused.
    /// Duplicate keys are rejected.
    ///
    /// This method has the same unique-ownership and read-quiescence requirements
    /// as [`Database::delete`].
    ///
    /// # Errors
    ///
    /// Returns an error for set mode, duplicate inputs, wrong key widths, corrupt
    /// storage, allocation failure, or a unique-deletion violation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-batch-pop-example",
    ///     Config::new(Mode::Map, 1_024),
    /// )?;
    /// let key = b"key-0001-0000000";
    /// database.put(key, b"value")?;
    /// assert_eq!(database.batch_pop([key.as_slice()])?, vec![Some(b"value".to_vec())]);
    /// assert_eq!(database.get(key)?, None);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn batch_pop<'key, I>(&self, keys: I) -> Result<Vec<Option<Vec<u8>>>>
    where
        I: IntoIterator<Item = &'key [u8]>,
    {
        if self.config.mode != Mode::Map {
            return Err(Error::Unsupported("batch_pop is available only for maps"));
        }
        let prepared = self.prepare_keys(keys)?;
        Self::validate_unique_batch_keys(&prepared)?;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(prepared.len())
            .map_err(|_| Error::OutOfMemory)?;
        encoded.resize(prepared.len(), None);
        let mut detached = Vec::new();
        detached
            .try_reserve_exact(prepared.len())
            .map_err(|_| Error::OutOfMemory)?;

        let operation = (|| -> Result<Vec<Option<Vec<u8>>>> {
            let mut start = 0;
            while start < prepared.len() {
                let bucket = prepared[start].bucket;
                let mut end = start + 1;
                while end < prepared.len() && prepared[end].bucket == bucket {
                    end += 1;
                }
                let _delete_guard = self.lock_delete_bucket(bucket)?;
                self.pop_bucket_batch(bucket, &prepared[start..end], &mut encoded, &mut detached)?;
                start = end;
            }
            self.materialize_values(&encoded)
        })();

        let release = self.release_detached_batch(&detached);
        match (operation, release) {
            (Ok(values), Ok(())) => Ok(values),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    /// Flushes mapped contents without defining a checkpoint generation.
    ///
    /// # Errors
    ///
    /// Returns the first kernel writeback error.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-sync-example",
    ///     Config::new(Mode::Set, 1_024),
    /// )?;
    /// database.sync()?;
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn sync(&self) -> Result<()> {
        self.body.sync_all()?;
        if let Some(blobs) = &self.blobs {
            blobs.sync_all()?;
        }
        self.persist_runtime_heads()?;
        self.heads.sync_all()
    }

    /// Flushes the database and consumes the handle.
    ///
    /// # Errors
    ///
    /// Returns the first kernel writeback error.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-close-example",
    ///     Config::new(Mode::Set, 1_024),
    /// )?;
    /// database.close()?;
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn close(self) -> Result<()> {
        self.sync()
    }

    fn persist_runtime_heads(&self) -> Result<()> {
        const HEAD_BATCH: usize = 1 << 17;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(HEAD_BATCH * size_of::<u64>())
            .map_err(|_| Error::OutOfMemory)?;
        for (batch, heads) in self.runtime_heads.chunks(HEAD_BATCH).enumerate() {
            bytes.clear();
            for head in heads {
                bytes.extend_from_slice(&read_shared(head).to_le_bytes());
            }
            let first = batch
                .checked_mul(HEAD_BATCH)
                .ok_or(Error::Corrupt("head batch offset overflow"))?;
            let offset = HEADS_START
                .checked_add(
                    u64::try_from(first)
                        .map_err(|_| Error::Corrupt("head batch offset overflow"))?
                        .checked_mul(size_of::<u64>() as u64)
                        .ok_or(Error::Corrupt("head batch offset overflow"))?,
                )
                .ok_or(Error::Corrupt("head batch offset overflow"))?;
            // SAFETY: close/sync owns the serialized destination bytes; atomics are copied above.
            unsafe { self.heads.copy_in(offset, &bytes)? };
        }
        Ok(())
    }

    fn put_inner(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        replace_existing: bool,
    ) -> Result<PutOutcome> {
        Self::validate_key(key)?;
        self.validate_value(value)?;
        let blob = self.allocate_blob(value)?;
        let encoded_value = self.encoded_value(value, blob)?;
        let hash = xxh64(key, self.config.hash_seed);
        let node = match allocate_node(&self.body, key, encoded_value) {
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
            let found = match self.find(bucket, hash, key) {
                Ok(found) => found,
                Err(error) => {
                    self.release_private(node, blob)?;
                    return Err(error);
                }
            };
            let existing_link = found.link;
            let existing = found.node;
            if existing.is_some() && !replace_existing {
                self.release_private(node, blob)?;
                return Ok(PutOutcome::Existing);
            }
            let result = if existing.is_some() {
                PutOutcome::Replaced
            } else {
                PutOutcome::Inserted
            };
            if let Err(error) = set_private_next(&self.body, node.offset, private_next, found.root)
            {
                self.release_private(node, blob)?;
                return Err(error);
            }
            private_next = found.root;
            let incoming = match self.head(bucket) {
                Ok(incoming) => incoming,
                Err(error) => {
                    self.release_private(node, blob)?;
                    return Err(error);
                }
            };
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
                    let link = match existing_link {
                        Link::Head(_) => Link::Node(node.offset),
                        Link::Node(offset) => Link::Node(offset),
                    };
                    self.unlink_link(link, &existing)?;
                }
                return Ok(result);
            }
        }
    }

    fn insert_one_inner(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        Self::validate_key(key)?;
        self.validate_value(value)?;
        let hash = xxh64(key, self.config.hash_seed);
        let bucket = hash % self.config.bucket_count;
        let private = self.allocate_private_insert(key, value, bucket)?;
        let head = match self.head(private.bucket) {
            Ok(head) => head,
            Err(error) => {
                self.release_private(private.node, private.blob)?;
                return Err(error);
            }
        };
        let group = PrivateGroup {
            head,
            tail_node: private.node.offset,
            root: private.node.offset,
        };
        self.commit_private_group(&group)?;
        Ok(())
    }

    fn insert_batch_inner<'entry, I>(&self, entries: I) -> Result<usize>
    where
        I: IntoIterator<Item = (&'entry [u8], Option<&'entry [u8]>)>,
    {
        let prepared = self.prepare_batch(entries)?;
        let count = prepared.len();
        if count == 0 {
            return Ok(0);
        }

        let mut private = Vec::new();
        private
            .try_reserve_exact(count)
            .map_err(|_| Error::OutOfMemory)?;
        let mut groups = Vec::new();
        groups
            .try_reserve_exact(count)
            .map_err(|_| Error::OutOfMemory)?;

        if let Err(error) = self.allocate_private_inserts(&prepared, &mut private) {
            self.release_private_batch(private)?;
            return Err(error);
        }

        if let Err(error) = self.link_private_batch(&private) {
            self.release_private_batch(private)?;
            return Err(error);
        }
        if let Err(error) = self.collect_private_groups(&private, &mut groups) {
            self.release_private_batch(private)?;
            return Err(error);
        }

        for group in &groups {
            self.commit_private_group(group)?;
        }
        Ok(count)
    }

    fn prepare_batch<'entry, I>(&self, entries: I) -> Result<Vec<PreparedInsert<'entry>>>
    where
        I: IntoIterator<Item = (&'entry [u8], Option<&'entry [u8]>)>,
    {
        let entries = entries.into_iter();
        let mut prepared = Vec::new();
        prepared
            .try_reserve(entries.size_hint().0)
            .map_err(|_| Error::OutOfMemory)?;

        for (order, (key, value)) in entries.enumerate() {
            Self::validate_key(key)?;
            self.validate_value(value)?;
            prepared.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
            prepared.push(PreparedInsert {
                key,
                value,
                hash: 0,
                bucket: 0,
                order,
            });
        }

        self.hash_prepared_inserts(&mut prepared);

        prepared.sort_unstable_by_key(|entry| (entry.bucket, entry.order));
        Ok(prepared)
    }

    fn hash_prepared_inserts(&self, prepared: &mut [PreparedInsert<'_>]) {
        for chunk in prepared.chunks_mut(4) {
            if let [first, second, third, fourth] = chunk {
                let hashes = xxh64_batch4(
                    [first.key, second.key, third.key, fourth.key],
                    self.config.hash_seed,
                );
                for (entry, hash) in chunk.iter_mut().zip(hashes) {
                    entry.hash = hash;
                    entry.bucket = hash % self.config.bucket_count;
                }
            } else {
                for entry in chunk {
                    entry.hash = xxh64(entry.key, self.config.hash_seed);
                    entry.bucket = entry.hash % self.config.bucket_count;
                }
            }
        }
    }

    fn prepare_keys<'key, I>(&self, keys: I) -> Result<Vec<PreparedKey<'key>>>
    where
        I: IntoIterator<Item = &'key [u8]>,
    {
        let keys = keys.into_iter();
        let mut prepared = Vec::new();
        prepared
            .try_reserve(keys.size_hint().0)
            .map_err(|_| Error::OutOfMemory)?;
        for (order, key) in keys.enumerate() {
            Self::validate_key(key)?;
            prepared.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
            prepared.push(PreparedKey {
                key,
                hash: 0,
                bucket: 0,
                order,
            });
        }
        self.hash_prepared_keys(&mut prepared);
        prepared.sort_unstable_by_key(|entry| (entry.bucket, entry.hash, entry.order));
        Ok(prepared)
    }

    fn hash_prepared_keys(&self, prepared: &mut [PreparedKey<'_>]) {
        for chunk in prepared.chunks_mut(4) {
            if let [first, second, third, fourth] = chunk {
                let hashes = xxh64_batch4(
                    [first.key, second.key, third.key, fourth.key],
                    self.config.hash_seed,
                );
                for (entry, hash) in chunk.iter_mut().zip(hashes) {
                    entry.hash = hash;
                    entry.bucket = hash % self.config.bucket_count;
                }
            } else {
                for entry in chunk {
                    entry.hash = xxh64(entry.key, self.config.hash_seed);
                    entry.bucket = entry.hash % self.config.bucket_count;
                }
            }
        }
    }

    fn validate_unique_batch_keys(prepared: &[PreparedKey<'_>]) -> Result<()> {
        let mut start = 0;
        while start < prepared.len() {
            let bucket = prepared[start].bucket;
            let hash = prepared[start].hash;
            let mut end = start + 1;
            while end < prepared.len()
                && prepared[end].bucket == bucket
                && prepared[end].hash == hash
            {
                end += 1;
            }
            for (index, entry) in prepared[start..end].iter().enumerate() {
                if prepared[start + index + 1..end]
                    .iter()
                    .any(|other| entry.key == other.key)
                {
                    return Err(Error::InvalidConfig(
                        "batch_delete keys must be globally unique",
                    ));
                }
            }
            start = end;
        }
        Ok(())
    }

    fn fetch_bucket_batch(
        &self,
        bucket: u64,
        prepared: &[PreparedKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<()> {
        'restart: loop {
            for entry in prepared {
                let result = results
                    .get_mut(entry.order)
                    .ok_or(Error::Corrupt("batch-fetch result index is out of range"))?;
                *result = None;
            }
            let mut remaining = prepared.len();
            let mut link = Link::Head(bucket);
            let root = self.link_value(link)?;
            let mut current = root;
            while current != 0 {
                let node = read_node(&self.body, current)?;
                let observed_next = node.next;
                if self.link_value(link)? != current
                    || read_next(&self.body, current)? != observed_next
                {
                    continue 'restart;
                }

                let node_hash = xxh64(&node.key, self.config.hash_seed);
                let first = prepared.partition_point(|entry| entry.hash < node_hash);
                for entry in &prepared[first..] {
                    if entry.hash != node_hash {
                        break;
                    }
                    let result = results
                        .get_mut(entry.order)
                        .ok_or(Error::Corrupt("batch-fetch result index is out of range"))?;
                    if result.is_none() && entry.key == node.key {
                        *result = Some(node.value);
                        remaining = remaining
                            .checked_sub(1)
                            .ok_or(Error::Corrupt("batch-fetch match count underflow"))?;
                    }
                }
                if remaining == 0 {
                    if read_shared(self.head(bucket)?) == root {
                        return Ok(());
                    }
                    continue 'restart;
                }
                link = Link::Node(current);
                current = observed_next;
            }
            if read_shared(self.head(bucket)?) == root {
                return Ok(());
            }
        }
    }

    fn delete_bucket_batch(
        &self,
        bucket: u64,
        prepared: &[PreparedKey<'_>],
        deleted: &mut [bool],
    ) -> Result<()> {
        'restart: loop {
            let mut remaining = prepared
                .iter()
                .filter(|entry| !deleted.get(entry.order).copied().unwrap_or(false))
                .count();
            if remaining == 0 {
                return Ok(());
            }
            let mut link = Link::Head(bucket);
            let head = self.head(bucket)?;
            let mut expected_head = read_shared(head);
            let mut current = expected_head;
            while current != 0 {
                let node = read_node(&self.body, current)?;
                let observed_next = node.next;
                if self.link_value(link)? != current
                    || read_next(&self.body, current)? != observed_next
                {
                    continue 'restart;
                }

                let node_hash = xxh64(&node.key, self.config.hash_seed);
                let first = prepared.partition_point(|entry| entry.hash < node_hash);
                let matching = prepared[first..].iter().find(|entry| {
                    entry.hash == node_hash
                        && !deleted.get(entry.order).copied().unwrap_or(false)
                        && entry.key == node.key
                });
                if let Some(entry) = matching {
                    let removes_head = matches!(link, Link::Head(_));
                    match self.unlink_link(link, &node) {
                        Ok(()) => {
                            let result = deleted.get_mut(entry.order).ok_or(Error::Corrupt(
                                "batch-delete result index is out of range",
                            ))?;
                            *result = true;
                            remaining = remaining
                                .checked_sub(1)
                                .ok_or(Error::Corrupt("batch-delete match count underflow"))?;
                            if remaining == 0 {
                                return Ok(());
                            }
                            if removes_head {
                                expected_head = observed_next;
                            }
                            current = observed_next;
                            continue;
                        }
                        Err(Error::Busy(_)) => continue 'restart,
                        Err(error) => return Err(error),
                    }
                }
                link = Link::Node(current);
                current = observed_next;
            }
            if read_shared(head) == expected_head {
                return Ok(());
            }
        }
    }

    fn pop_bucket_batch(
        &self,
        bucket: u64,
        prepared: &[PreparedKey<'_>],
        encoded: &mut [Option<u64>],
        detached: &mut Vec<DetachedNode>,
    ) -> Result<()> {
        'restart: loop {
            let mut remaining = prepared
                .iter()
                .filter(|entry| encoded.get(entry.order).copied().flatten().is_none())
                .count();
            if remaining == 0 {
                return Ok(());
            }
            let mut link = Link::Head(bucket);
            let head = self.head(bucket)?;
            let mut expected_head = read_shared(head);
            let mut current = expected_head;
            while current != 0 {
                let node = read_node(&self.body, current)?;
                let observed_next = node.next;
                if self.link_value(link)? != current
                    || read_next(&self.body, current)? != observed_next
                {
                    continue 'restart;
                }

                let node_hash = xxh64(&node.key, self.config.hash_seed);
                let first = prepared.partition_point(|entry| entry.hash < node_hash);
                let matching = prepared[first..].iter().find(|entry| {
                    entry.hash == node_hash
                        && encoded.get(entry.order).copied().flatten().is_none()
                        && entry.key == node.key
                });
                if let Some(entry) = matching {
                    let removes_head = matches!(link, Link::Head(_));
                    match self.detach_link(link, &node) {
                        Ok(allocation) => {
                            let result = encoded
                                .get_mut(entry.order)
                                .ok_or(Error::Corrupt("batch-pop result index is out of range"))?;
                            *result = Some(node.value);
                            detached.push(allocation);
                            remaining = remaining
                                .checked_sub(1)
                                .ok_or(Error::Corrupt("batch-pop match count underflow"))?;
                            if remaining == 0 {
                                return Ok(());
                            }
                            if removes_head {
                                expected_head = observed_next;
                            }
                            current = observed_next;
                            continue;
                        }
                        Err(Error::Busy(_)) => continue 'restart,
                        Err(error) => return Err(error),
                    }
                }
                link = Link::Node(current);
                current = observed_next;
            }
            if read_shared(head) == expected_head {
                return Ok(());
            }
        }
    }

    fn read_node_value(&self, node: &Node) -> Result<Vec<u8>> {
        self.read_value(node.value)
    }

    fn read_value(&self, encoded: u64) -> Result<Vec<u8>> {
        if encoded & VALUE_BLOB_TAG == 0 {
            return Ok(inline_value_bytes(encoded));
        }
        self.read_blob(encoded & VALUE_OFFSET_MASK)
    }

    fn read_blob(&self, offset: u64) -> Result<Vec<u8>> {
        let blobs = self
            .blobs
            .as_ref()
            .ok_or(Error::Corrupt("map has no blob allocator"))?;
        let mut header = [0_u8; BLOB_HEADER_SIZE];
        blobs.read_into(offset, &mut header)?;
        let (length, expected_checksum) = decode_blob_header(header);
        let value_offset = offset
            .checked_add(BLOB_HEADER_SIZE as u64)
            .ok_or(Error::Corrupt("blob value offset overflow"))?;
        let value = blobs.read(value_offset, length as usize)?;
        if blob_checksum(&value) != expected_checksum {
            return Err(Error::Corrupt("map value checksum does not match"));
        }
        Ok(value)
    }

    fn materialize_values(&self, encoded: &[Option<u64>]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut results = Vec::new();
        results
            .try_reserve_exact(encoded.len())
            .map_err(|_| Error::OutOfMemory)?;
        results.resize_with(encoded.len(), || None);
        let mut blobs = Vec::new();
        blobs
            .try_reserve_exact(encoded.len())
            .map_err(|_| Error::OutOfMemory)?;
        for (order, value) in encoded.iter().copied().enumerate() {
            let Some(value) = value else {
                continue;
            };
            if value & VALUE_BLOB_TAG == 0 {
                results[order] = Some(self.read_value(value)?);
            } else {
                blobs.push((value & VALUE_OFFSET_MASK, order));
            }
        }
        blobs.sort_unstable_by_key(|(offset, _order)| *offset);
        for (offset, order) in blobs {
            results[order] = Some(self.read_blob(offset)?);
        }
        Ok(results)
    }

    fn allocate_private_inserts(
        &self,
        prepared: &[PreparedInsert<'_>],
        private: &mut Vec<PrivateInsert>,
    ) -> Result<()> {
        let node_size = NODE_SIZE;
        let mut start = 0;
        while start < prepared.len() {
            let reserved =
                self.body
                    .allocate_batch(node_size, NODE_ALIGNMENT, prepared.len() - start)?;
            let reserved_count = reserved.len();
            let mut allocations = reserved.allocations();
            for entry in &prepared[start..start + reserved_count] {
                let allocation = allocations
                    .next()
                    .ok_or(Error::Corrupt("batch allocation ended early"))?;
                match self.initialize_reserved_insert(entry, allocation) {
                    Ok(insert) => private.push(insert),
                    Err(error) => {
                        self.body.release(allocation)?;
                        for unused in allocations {
                            self.body.release(unused)?;
                        }
                        return Err(error);
                    }
                }
            }
            start = start
                .checked_add(reserved_count)
                .ok_or(Error::CapacityExhausted("batch insertion index"))?;
        }
        Ok(())
    }

    fn initialize_reserved_insert(
        &self,
        entry: &PreparedInsert<'_>,
        node: Allocation,
    ) -> Result<PrivateInsert> {
        let blob = self.allocate_blob(entry.value)?;
        let encoded_value = self.encoded_value(entry.value, blob)?;
        if let Err(error) = write_node(&self.body, node, entry.key, encoded_value) {
            if let Some(blob) = blob {
                self.release_blob(blob)?;
            }
            return Err(error);
        }
        Ok(PrivateInsert {
            node,
            blob,
            bucket: entry.bucket,
        })
    }

    fn allocate_private_insert(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        bucket: u64,
    ) -> Result<PrivateInsert> {
        let blob = self.allocate_blob(value)?;
        let encoded_value = self.encoded_value(value, blob)?;
        let node = match allocate_node(&self.body, key, encoded_value) {
            Ok(allocation) => allocation,
            Err(error) => {
                if let Some(blob) = blob {
                    let _released = self.release_blob(blob);
                }
                return Err(error);
            }
        };
        Ok(PrivateInsert { node, blob, bucket })
    }

    fn link_private_batch(&self, private: &[PrivateInsert]) -> Result<()> {
        for pair in private.windows(2) {
            let previous = &pair[0];
            let current = &pair[1];
            if previous.bucket == current.bucket {
                set_private_next(&self.body, current.node.offset, 0, previous.node.offset)?;
            }
        }
        Ok(())
    }

    fn collect_private_groups<'database>(
        &'database self,
        private: &[PrivateInsert],
        groups: &mut Vec<PrivateGroup<'database>>,
    ) -> Result<()> {
        let mut start = 0;
        while start < private.len() {
            let bucket = private[start].bucket;
            let mut end = start + 1;
            while end < private.len() && private[end].bucket == bucket {
                end += 1;
            }
            groups.push(PrivateGroup {
                head: self.head(bucket)?,
                tail_node: private[start].node.offset,
                root: private[end - 1].node.offset,
            });
            start = end;
        }
        Ok(())
    }

    fn lock_delete_bucket(&self, bucket: u64) -> Result<BucketDeleteGuard<'_>> {
        let bucket =
            usize::try_from(bucket).map_err(|_| Error::Corrupt("bucket index overflow"))?;
        let lock = self
            .delete_locks
            .get(bucket)
            .ok_or(Error::Corrupt("delete-lock bucket is out of range"))?;
        loop {
            if lock
                .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(BucketDeleteGuard { lock });
            }
            std::hint::spin_loop();
        }
    }
    fn commit_private_group(&self, group: &PrivateGroup<'_>) -> Result<()> {
        loop {
            let observed = read_shared(group.head);
            let previous_next = read_next(&self.body, group.tail_node)?;
            if !compare_exchange_next(&self.body, group.tail_node, previous_next, observed)? {
                continue;
            }
            if group
                .head
                .compare_exchange(observed, group.root, Ordering::Release, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn release_private_batch(&self, private: Vec<PrivateInsert>) -> Result<()> {
        let mut first_error = None;
        for insert in private.into_iter().rev() {
            if let Err(error) = self.release_private(insert.node, insert.blob) {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn allocate_blob(&self, value: Option<&[u8]>) -> Result<Option<Allocation>> {
        if self.config.mode == Mode::Set {
            return Ok(None);
        }
        let value = value.ok_or(Error::Corrupt("map value is missing"))?;
        if inline_value_word(value).is_some() {
            return Ok(None);
        }
        let length = BLOB_HEADER_SIZE
            .checked_add(value.len())
            .ok_or(Error::CapacityExhausted("blob length"))?;
        let blobs = self
            .blobs
            .as_ref()
            .ok_or(Error::Corrupt("map has no blob allocator"))?;
        let allocation = blobs.allocate(length, 8)?;
        let header = blob_header(value)?;
        if let Err(error) = blobs
            .write_at(allocation, 0, &header)
            .and_then(|()| blobs.write_at(allocation, BLOB_HEADER_SIZE, value))
        {
            let _released = blobs.release(allocation);
            return Err(error);
        }
        Ok(Some(allocation))
    }

    fn encoded_value(&self, value: Option<&[u8]>, blob: Option<Allocation>) -> Result<u64> {
        if self.config.mode == Mode::Set {
            return Ok(0);
        }
        let value = value.ok_or(Error::Corrupt("map value is missing"))?;
        if let Some(inline) = inline_value_word(value) {
            return Ok(inline);
        }
        let offset = blob
            .ok_or(Error::Corrupt("blob map value has no allocation"))?
            .offset;
        if offset & !VALUE_OFFSET_MASK != 0 {
            return Err(Error::CapacityExhausted("tagged blob offset"));
        }
        Ok(VALUE_BLOB_TAG | offset)
    }

    fn release_private(&self, node: Allocation, blob: Option<Allocation>) -> Result<()> {
        self.body.validate_release(node)?;
        if let (Some(blobs), Some(blob)) = (&self.blobs, blob) {
            blobs.validate_release(blob)?;
            blobs.release_count(blob)?;
        }
        tombstone_node(&self.body, node.offset)?;
        self.body.release_count(node)
    }

    fn release_blob(&self, allocation: Allocation) -> Result<()> {
        let blobs = self
            .blobs
            .as_ref()
            .ok_or(Error::Corrupt("map has no blob allocator"))?;
        blobs.release_count(allocation)
    }

    fn find(&self, bucket: u64, hash: u64, key: &[u8]) -> Result<Found> {
        'restart: loop {
            let mut link = Link::Head(bucket);
            let root = self.link_value(link)?;
            let mut current = root;
            while current != 0 {
                let node = read_node(&self.body, current)?;
                let observed_next = node.next;
                if self.link_value(link)? != current
                    || read_next(&self.body, current)? != observed_next
                {
                    continue 'restart;
                }
                if xxh64(&node.key, self.config.hash_seed) == hash && node.key == key {
                    return Ok(Found {
                        node: Some(node),
                        root,
                        link,
                    });
                }
                link = Link::Node(current);
                current = observed_next;
            }
            if read_shared(self.head(bucket)?) != root {
                continue;
            }
            return Ok(Found {
                node: None,
                root,
                link,
            });
        }
    }

    fn unlink_link(&self, link: Link, target: &Node) -> Result<()> {
        let (body_allocation, blob_allocation) =
            self.node_allocations(target.offset, target.value)?;
        if !self.compare_exchange_link(link, target.offset, target.next)? {
            return Err(Error::Busy("unique deletion link changed before unlink"));
        }
        tombstone_node(&self.body, target.offset)?;
        if let (Some(blobs), Some(allocation)) = (&self.blobs, blob_allocation) {
            blobs.release_count(allocation)?;
        }
        self.body.release_count(body_allocation)
    }

    fn detach_link(&self, link: Link, target: &Node) -> Result<DetachedNode> {
        let (body, blob) = self.node_allocations(target.offset, target.value)?;
        if !self.compare_exchange_link(link, target.offset, target.next)? {
            return Err(Error::Busy("unique deletion link changed before unlink"));
        }
        Ok(DetachedNode { body, blob })
    }

    fn release_detached_batch(&self, detached: &[DetachedNode]) -> Result<()> {
        let mut first_error = None;
        for node in detached.iter().rev() {
            if let Err(error) = self.release_detached(node)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn release_detached(&self, node: &DetachedNode) -> Result<()> {
        tombstone_node(&self.body, node.body.offset)?;
        if let (Some(blobs), Some(blob)) = (&self.blobs, node.blob) {
            blobs.release_count(blob)?;
        }
        self.body.release_count(node.body)
    }

    fn node_allocations(
        &self,
        offset: u64,
        encoded_value: u64,
    ) -> Result<(Allocation, Option<Allocation>)> {
        let blob_allocation = self.blob_allocation(encoded_value)?;
        let node_size = u32::try_from(NODE_SIZE)
            .map_err(|_| Error::Corrupt("body node size does not fit u32"))?;
        let body_allocation = self.body.allocation_for(offset, node_size)?;

        self.body.validate_release(body_allocation)?;
        if let (Some(blobs), Some(allocation)) = (&self.blobs, blob_allocation) {
            blobs.validate_release(allocation)?;
        }
        Ok((body_allocation, blob_allocation))
    }

    fn blob_allocation(&self, encoded_value: u64) -> Result<Option<Allocation>> {
        if encoded_value & VALUE_BLOB_TAG == 0 {
            if self.config.mode == Mode::Set && encoded_value != 0 {
                return Err(Error::Corrupt("set node stores a value"));
            }
            return Ok(None);
        }
        if self.config.mode != Mode::Map {
            return Err(Error::Corrupt("set node has a blob tag"));
        }
        let offset = encoded_value & VALUE_OFFSET_MASK;
        let blobs = self
            .blobs
            .as_ref()
            .ok_or(Error::Corrupt("map has no blob allocator"))?;
        let mut header = [0_u8; BLOB_HEADER_SIZE];
        blobs.read_into(offset, &mut header)?;
        let (length, _checksum) = decode_blob_header(header);
        let total = u32::try_from(BLOB_HEADER_SIZE)
            .ok()
            .and_then(|header| header.checked_add(length))
            .ok_or(Error::Corrupt("blob allocation length overflow"))?;
        Ok(Some(blobs.allocation_for(offset, total)?))
    }

    fn validate_key(key: &[u8]) -> Result<()> {
        if key.len() != KEY_SIZE {
            return Err(Error::InvalidKeyLength {
                expected: KEY_SIZE,
                actual: key.len(),
            });
        }
        Ok(())
    }

    fn validate_value(&self, value: Option<&[u8]>) -> Result<()> {
        if self.config.mode == Mode::Set {
            if value.is_some() {
                return Err(Error::Unsupported("sets cannot store values"));
            }
            return Ok(());
        }
        value.ok_or(Error::Corrupt("map value is missing"))?;
        Ok(())
    }

    pub(crate) fn head(&self, bucket: u64) -> Result<&AtomicU64> {
        let bucket =
            usize::try_from(bucket).map_err(|_| Error::Corrupt("bucket index overflow"))?;
        self.runtime_heads
            .get(bucket)
            .ok_or(Error::Corrupt("bucket index is out of range"))
    }

    fn link_value(&self, link: Link) -> Result<u64> {
        match link {
            Link::Head(bucket) => Ok(read_shared(self.head(bucket)?)),
            Link::Node(offset) => read_next(&self.body, offset),
        }
    }

    fn compare_exchange_link(&self, link: Link, expected: u64, replacement: u64) -> Result<bool> {
        match link {
            Link::Head(bucket) => Ok(self
                .head(bucket)?
                .compare_exchange(expected, replacement, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()),
            Link::Node(offset) => compare_exchange_next(&self.body, offset, expected, replacement),
        }
    }
}

impl WriteOnlyWriter<'_> {
    /// Publishes a batch of key-value pairs without duplicate lookups.
    ///
    /// The complete batch is hashed before publication. Entries are sorted by
    /// bucket, input order is retained within a bucket, and each bucket is
    /// exposed with one successful head CAS. Duplicate keys remain in the chain;
    /// the final duplicate in the input is returned by [`Database::get`] first.
    ///
    /// The returned count is the number of nodes published.
    ///
    /// # Errors
    ///
    /// Returns an error when any key has the wrong width, or when validation,
    /// metadata allocation, or mapped allocation fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use floresta_db::{Config, Database, Mode};
    ///
    /// let database = Database::create(
    ///     "floresta-db-put-batch-example",
    ///     Config::new(Mode::Map, 1_024),
    /// )?;
    /// let entries = [
    ///     (b"key-0001-0000000".as_slice(), b"value-01".as_slice()),
    ///     (b"key-0002-0000000".as_slice(), b"value-02".as_slice()),
    /// ];
    /// assert_eq!(database.write_only()?.put_batch(entries)?, 2);
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn put_batch<'entry, I>(&self, entries: I) -> Result<usize>
    where
        I: IntoIterator<Item = (&'entry [u8], &'entry [u8])>,
    {
        self.database
            .insert_batch_inner(entries.into_iter().map(|(key, value)| (key, Some(value))))
    }
}

pub(crate) fn create_runtime_heads(
    bucket_count: u64,
    heads: &MappedFile,
    load_persisted: bool,
) -> Result<Box<[AtomicU64]>> {
    let count = usize::try_from(bucket_count)
        .map_err(|_| Error::InvalidConfig("bucket count does not fit memory"))?;
    let mut runtime_heads = Vec::new();
    runtime_heads
        .try_reserve_exact(count)
        .map_err(|_| Error::OutOfMemory)?;
    for bucket in 0..bucket_count {
        let value = if load_persisted {
            let offset = HEADS_START
                .checked_add(
                    bucket
                        .checked_mul(size_of::<u64>() as u64)
                        .ok_or(Error::Corrupt("head offset overflow"))?,
                )
                .ok_or(Error::Corrupt("head offset overflow"))?;
            let bytes = heads.copy_out(offset, size_of::<u64>())?;
            u64::from_le_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::Corrupt("persisted head is truncated"))?,
            )
        } else {
            0
        };
        runtime_heads.push(AtomicU64::new(value));
    }
    Ok(runtime_heads.into_boxed_slice())
}

pub(crate) fn create_delete_locks(bucket_count: u64) -> Result<Box<[AtomicU64]>> {
    let count = usize::try_from(bucket_count)
        .map_err(|_| Error::InvalidConfig("bucket count does not fit memory"))?;
    let mut locks = Vec::new();
    locks
        .try_reserve_exact(count)
        .map_err(|_| Error::OutOfMemory)?;
    for _bucket in 0..count {
        locks.push(AtomicU64::new(0));
    }
    Ok(locks.into_boxed_slice())
}

fn inline_value_word(value: &[u8]) -> Option<u64> {
    let bytes = <[u8; 8]>::try_from(value).ok()?;
    let word = u64::from_le_bytes(bytes);
    (word & VALUE_BLOB_TAG == 0).then_some(word)
}

fn inline_value_bytes(word: u64) -> Vec<u8> {
    word.to_le_bytes().to_vec()
}

fn read_shared(atomic: &AtomicU64) -> u64 {
    // SAFETY: the target platforms provide aligned single-copy 64-bit reads. Writers use CAS,
    // and every decision based on this possibly stale observation is revalidated by CAS.
    unsafe { std::ptr::read_volatile(atomic.as_ptr()) }
}

#[derive(Clone, Copy)]
enum Link {
    Head(u64),
    Node(u64),
}

struct Found {
    node: Option<Node>,
    root: u64,
    link: Link,
}

pub(crate) fn bank_size(bucket_count: u64) -> Result<u64> {
    bucket_count
        .checked_mul(size_of::<u64>() as u64)
        .and_then(|bytes| align_up(bytes, FORMAT_PAGE_SIZE))
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
    let page_size = usize::try_from(FORMAT_PAGE_SIZE)
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
        u64::try_from(KEY_SIZE).map_err(|_| Error::InvalidConfig("key size overflow"))?,
    )?;
    write_header_u64(&mut header, 40, config.body_capacity)?;
    write_header_u64(&mut header, 48, config.blob_capacity)?;
    write_header_u64(&mut header, 56, config.block_size)?;
    write_header_u64(&mut header, 72, config.hash_seed)?;
    write_header_u64(&mut header, 80, node_size)?;
    write_header_u64(&mut header, 88, 0)?;
    let checksum = header_checksum(&header)?;
    write_header_u64(&mut header, HEADER_CHECKSUM_OFFSET, checksum)?;
    // SAFETY: database creation is single-threaded and heads are not published yet.
    unsafe { heads.copy_in(0, &header) }
}

pub(crate) fn header_checksum(header: &[u8]) -> Result<u64> {
    let bytes = header
        .get(0..HEADER_CHECKSUM_OFFSET)
        .ok_or(Error::Corrupt("heads header is too small for its checksum"))?;
    Ok(xxh64(bytes, HEADER_CHECKSUM_SEED))
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

#[cfg(all(test, not(miri), any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("floresta-db-{}-{name}", std::process::id()))
    }

    fn fixed_key(input: &[u8]) -> [u8; KEY_SIZE] {
        let mut key = [0_u8; KEY_SIZE];
        let length = input.len().min(KEY_SIZE);
        key[..length].copy_from_slice(&input[..length]);
        key
    }

    fn config(mode: Mode, buckets: u64) -> Config {
        let mut config = Config::new(mode, buckets);
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
        let database = Database::create(&path, config(Mode::Map, 1))?;
        let first = fixed_key(b"key1");
        let second = fixed_key(b"key2");
        let missing = fixed_key(b"none");
        assert_eq!(database.put(&first, b"first")?, PutResult::Inserted);
        assert_eq!(database.put(&second, b"second")?, PutResult::Inserted);
        assert_eq!(database.put(&first, b"new")?, PutResult::Replaced);
        assert_eq!(database.get(&first)?, Some(b"new".to_vec()));
        assert_eq!(database.get(&second)?, Some(b"second".to_vec()));
        assert!(database.delete(&first)?);
        assert_eq!(database.get(&first)?, None);
        assert_eq!(database.get(&second)?, Some(b"second".to_vec()));
        assert!(!database.delete(&missing)?);
        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn supports_concurrent_set_writers() -> Result<()> {
        let path = test_directory("set-concurrent");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Set, 16))?;
        std::thread::scope(|scope| {
            for thread in 0_u64..8 {
                let database_ref = &database;
                scope.spawn(move || {
                    for item in 0_u64..64 {
                        let key = fixed_key(&(thread * 64 + item).to_le_bytes());
                        assert!(database_ref.add(&key).is_ok());
                    }
                });
            }
        });
        for value in 0_u64..512 {
            assert!(database.contains(&fixed_key(&value.to_le_bytes()))?);
        }
        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn serializes_concurrent_deleters_per_bucket() -> Result<()> {
        let path = test_directory("delete-concurrent-bucket");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Set, 1))?;
        let keys: Vec<[u8; KEY_SIZE]> = (0_u64..512)
            .map(|value| fixed_key(&value.to_le_bytes()))
            .collect();
        assert_eq!(
            database.add_batch(keys.iter().map(<[u8; KEY_SIZE]>::as_slice))?,
            keys.len()
        );

        std::thread::scope(|scope| {
            for chunk in keys.chunks(64) {
                let database_ref = &database;
                scope.spawn(move || {
                    let result =
                        database_ref.batch_delete(chunk.iter().map(<[u8; KEY_SIZE]>::as_slice));
                    assert!(matches!(result, Ok(deleted) if deleted.iter().all(|value| *value)));
                });
            }
        });
        for key in &keys {
            assert!(!database.contains(key)?);
        }

        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn set_adds_retain_duplicates() -> Result<()> {
        let path = test_directory("set-duplicates");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Set, 1))?;

        let same = fixed_key(b"same-key");
        let batch_key = fixed_key(b"batchkey");
        assert_eq!(database.add(&same)?, PutResult::Inserted);
        assert_eq!(database.add(&same)?, PutResult::Inserted);
        let batch = [batch_key.as_slice(), batch_key.as_slice()];
        assert_eq!(database.add_batch(batch)?, 2);

        assert!(database.delete(&same)?);
        assert!(database.contains(&same)?);
        assert!(database.delete(&same)?);
        assert!(!database.contains(&same)?);
        assert!(database.delete(&batch_key)?);
        assert!(database.contains(&batch_key)?);
        assert!(database.delete(&batch_key)?);
        assert!(!database.contains(&batch_key)?);

        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn prepares_batches_by_ascending_bucket_and_input_order() -> Result<()> {
        let path = test_directory("batch-order");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Set, 8))?;
        let keys: Vec<[u8; KEY_SIZE]> = (0_u64..64)
            .map(|value| fixed_key(&value.to_le_bytes()))
            .rev()
            .collect();
        let prepared =
            database.prepare_batch(keys.iter().map(|key| (key.as_slice(), None::<&[u8]>)))?;

        for pair in prepared.windows(2) {
            assert!(pair[0].bucket <= pair[1].bucket);
            if pair[0].bucket == pair[1].bucket {
                assert!(pair[0].order < pair[1].order);
            }
        }

        let prepared_keys = database.prepare_keys(keys.iter().map(<[u8; KEY_SIZE]>::as_slice))?;
        for pair in prepared_keys.windows(2) {
            assert!(pair[0].bucket <= pair[1].bucket);
            if pair[0].bucket == pair[1].bucket {
                assert!(pair[0].hash <= pair[1].hash);
            }
        }

        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn batch_prelinks_collisions_and_retains_duplicate_values() -> Result<()> {
        let path = test_directory("batch-collisions");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Map, 1))?;
        database.put(&fixed_key(b"key-000A-0000000"), b"value-a")?;

        let entries = [
            (b"key-000B-0000000".as_slice(), b"value-b".as_slice()),
            (b"key-000C-0000000".as_slice(), b"value-c".as_slice()),
        ];
        assert_eq!(database.write_only()?.put_batch(entries)?, 2);

        let root = read_shared(database.head(0)?);
        let node_c = read_node(&database.body, root)?;
        let node_b = read_node(&database.body, node_c.next)?;
        let node_a = read_node(&database.body, node_b.next)?;
        assert_eq!(node_c.key, *b"key-000C-0000000");
        assert_eq!(node_b.key, *b"key-000B-0000000");
        assert_eq!(node_a.key, *b"key-000A-0000000");

        let duplicates = [
            (b"same-key-0000000".as_slice(), b"first".as_slice()),
            (b"same-key-0000000".as_slice(), b"second".as_slice()),
        ];
        assert_eq!(database.write_only()?.put_batch(duplicates)?, 2);
        assert_eq!(database.get(b"same-key-0000000")?, Some(b"second".to_vec()));
        assert!(database.delete(b"same-key-0000000")?);
        assert_eq!(database.get(b"same-key-0000000")?, Some(b"first".to_vec()));

        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn batch_fetch_restores_input_order_across_buckets() -> Result<()> {
        let path = test_directory("batch-fetch");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Map, 8))?;
        let entries: Vec<([u8; KEY_SIZE], [u8; 8])> = (0_u64..8)
            .map(|value| (fixed_key(&value.to_le_bytes()), (value + 100).to_le_bytes()))
            .collect();
        assert_eq!(
            database.write_only()?.put_batch(
                entries
                    .iter()
                    .map(|(key, value)| (key.as_slice(), value.as_slice()))
            )?,
            entries.len()
        );

        let missing = [255_u8; KEY_SIZE];
        let queries = [
            entries[5].0.as_slice(),
            missing.as_slice(),
            entries[1].0.as_slice(),
            entries[5].0.as_slice(),
            entries[7].0.as_slice(),
        ];
        let values = database.batch_fetch(queries)?;
        assert_eq!(values[0], Some(entries[5].1.to_vec()));
        assert_eq!(values[1], None);
        assert_eq!(values[2], Some(entries[1].1.to_vec()));
        assert_eq!(values[3], Some(entries[5].1.to_vec()));
        assert_eq!(values[4], Some(entries[7].1.to_vec()));

        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn concurrent_insert_only_puts_publish_once_without_replacement() -> Result<()> {
        let path = test_directory("insert-only-concurrent");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Map, 1))?;
        let inserted = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for value in 0_u64..8 {
                let database_ref = &database;
                let inserted_ref = &inserted;
                scope.spawn(move || {
                    if database_ref
                        .put_new(&fixed_key(b"same-key"), &value.to_le_bytes())
                        .unwrap_or(false)
                    {
                        inserted_ref.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        assert_eq!(inserted.load(Ordering::Relaxed), 1);
        let expected = database
            .get(&fixed_key(b"same-key"))?
            .ok_or(Error::Corrupt("insert-only key is missing"))?;
        assert!(!database.put_new(&fixed_key(b"same-key"), b"ignored")?);
        assert_eq!(inserted.load(Ordering::Relaxed), 1);
        database.close()?;
        let reopened = Database::open_runtime(&path)?;
        assert_eq!(reopened.get(&fixed_key(b"same-key"))?, Some(expected));
        drop(reopened);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn write_only_automatically_inlines_values_across_reopen() -> Result<()> {
        let path = test_directory("write-only-inline");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Map, 16))?;
        {
            let entries: Vec<([u8; KEY_SIZE], [u8; 8])> = (0_u64..128)
                .map(|value| (fixed_key(&value.to_le_bytes()), (value * 2).to_le_bytes()))
                .collect();
            let writer = database.write_only()?;
            assert_eq!(
                writer.put_batch(
                    entries
                        .iter()
                        .map(|(key, value)| (key.as_slice(), value.as_slice()))
                )?,
                entries.len()
            );
        }
        assert_eq!(
            std::fs::metadata(path.join("blobs"))?.len(),
            FORMAT_PAGE_SIZE
        );
        database.close()?;
        let reopened = Database::open_runtime(&path)?;
        for value in 0_u64..128 {
            assert_eq!(
                reopened.get(&fixed_key(&value.to_le_bytes()))?,
                Some((value * 2).to_le_bytes().to_vec())
            );
        }
        drop(reopened);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn tagged_or_non_eight_byte_values_fall_back_to_blobs() -> Result<()> {
        let path = test_directory("automatic-blob-fallback");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Map, 8))?;
        let tagged_key = fixed_key(b"tagged");
        let short_key = fixed_key(b"short");
        let tagged = [0, 0, 0, 0, 0, 0, 0, 0x80];
        database.put(&tagged_key, &tagged)?;
        database.put(&short_key, b"short")?;

        for key in [tagged_key, short_key] {
            let hash = xxh64(&key, database.config.hash_seed);
            let node = database
                .find(hash % database.config.bucket_count, hash, &key)?
                .node
                .ok_or(Error::Corrupt("automatic blob test node is missing"))?;
            assert_ne!(node.value & VALUE_BLOB_TAG, 0);
        }
        assert_eq!(database.get(&tagged_key)?, Some(tagged.to_vec()));
        assert_eq!(database.get(&short_key)?, Some(b"short".to_vec()));

        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn deletion_reuses_zero_count_page_before_growing_body() -> Result<()> {
        let path = test_directory("delete-zero-page");
        let _ignored = std::fs::remove_dir_all(&path);
        let mut test_config = config(Mode::Set, 1);
        test_config.block_size = FORMAT_PAGE_SIZE;
        test_config.body_capacity = FORMAT_PAGE_SIZE * 2;
        let database = Database::create(&path, test_config)?;
        let keys: Vec<[u8; KEY_SIZE]> = (0_u64..4_096)
            .map(|value| fixed_key(&value.to_le_bytes()))
            .collect();
        assert_eq!(
            database.add_batch(keys.iter().map(<[u8; KEY_SIZE]>::as_slice))?,
            keys.len()
        );
        let grown_length = std::fs::metadata(path.join("body"))?.len();

        let deleted =
            database.batch_delete(keys[..2_048].iter().map(<[u8; KEY_SIZE]>::as_slice))?;
        assert!(deleted.iter().all(|value| *value));
        let replacement = fixed_key(&4_096_u64.to_le_bytes());
        database.add(&replacement)?;
        let hash = xxh64(&replacement, database.config.hash_seed);
        let node = database
            .find(0, hash, &replacement)?
            .node
            .ok_or(Error::Corrupt("reused key is missing"))?;
        let node_size = u32::try_from(NODE_SIZE)
            .map_err(|_| Error::Corrupt("body node size does not fit u32"))?;
        let allocation = database.body.allocation_for(node.offset, node_size)?;
        assert_eq!(allocation.block, 0);
        assert_eq!(std::fs::metadata(path.join("body"))?.len(), grown_length);

        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn batch_delete_stops_after_last_match() -> Result<()> {
        let path = test_directory("batch-delete-early-exit");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Set, 1))?;
        let older = fixed_key(&1_u64.to_le_bytes());
        let newest = fixed_key(&2_u64.to_le_bytes());
        database.add(&older)?;
        database.add(&newest)?;

        let older_hash = xxh64(&older, database.config.hash_seed);
        let older_node = database
            .find(0, older_hash, &older)?
            .node
            .ok_or(Error::Corrupt("older test node is missing"))?;
        let newest_hash = xxh64(&newest, database.config.hash_seed);
        let newest_node = database
            .find(0, newest_hash, &newest)?
            .node
            .ok_or(Error::Corrupt("newest test node is missing"))?;
        database
            .body
            .atomic_u64(older_node.offset + crate::layout::NODE_POINTER_OFFSET as u64)?
            .store(1, Ordering::Release);

        assert_eq!(database.batch_delete([newest.as_slice()])?, vec![true]);
        assert_eq!(
            database
                .body
                .atomic_u64(newest_node.offset + crate::layout::NODE_POINTER_OFFSET as u64)?
                .load(Ordering::Acquire),
            0
        );

        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn batch_delete_preserves_result_order() -> Result<()> {
        let path = test_directory("batch-delete-order");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Set, 1))?;
        let first = fixed_key(b"first");
        let second = fixed_key(b"second");
        let third = fixed_key(b"third");
        let missing = fixed_key(b"missing");
        database.add_batch([first.as_slice(), second.as_slice(), third.as_slice()])?;

        assert!(matches!(
            database.batch_delete([first.as_slice(), first.as_slice()]),
            Err(Error::InvalidConfig(_))
        ));
        assert_eq!(
            database.batch_delete([second.as_slice(), missing.as_slice(), first.as_slice()])?,
            vec![true, false, true]
        );
        assert!(!database.contains(&first)?);
        assert!(!database.contains(&second)?);
        assert!(database.contains(&third)?);

        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn batch_pop_returns_blob_values_in_input_order() -> Result<()> {
        let path = test_directory("batch-pop");
        let _ignored = std::fs::remove_dir_all(&path);
        let database = Database::create(&path, config(Mode::Map, 8))?;
        let first = fixed_key(b"first");
        let second = fixed_key(b"second");
        let third = fixed_key(b"third");
        let missing = fixed_key(b"missing");
        database.put(&first, b"first-value")?;
        database.put(&second, b"second-value")?;
        database.put(&third, b"third-value")?;

        let popped = database.batch_pop([
            third.as_slice(),
            missing.as_slice(),
            first.as_slice(),
            second.as_slice(),
        ])?;
        assert_eq!(
            popped,
            vec![
                Some(b"third-value".to_vec()),
                None,
                Some(b"first-value".to_vec()),
                Some(b"second-value".to_vec()),
            ]
        );
        assert_eq!(
            database.batch_fetch([first.as_slice(), second.as_slice(), third.as_slice()])?,
            vec![None, None, None]
        );

        drop(database);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
}
