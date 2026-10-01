// SPDX-License-Identifier: MIT OR Apache-2.0

//! Database modes and validated storage configuration.
//!
//! [`Config`] describes the persistent layout. Validation rejects combinations
//! that cannot be represented safely by the on-disk format.

use crate::error::{Error, Result};
use crate::layout::{DATA_START, FORMAT_PAGE_SIZE, NODE_POINTER_MASK, NODE_SIZE_U64};

/// The default seed used for key hashing.
///
/// # Examples
///
/// ```
/// assert_eq!(floresta_db::DEFAULT_HASH_SEED, 0);
/// ```
pub const DEFAULT_HASH_SEED: u64 = 0;

/// Required key width for every database operation.
///
/// # Examples
///
/// ```
/// assert_eq!(floresta_db::KEY_SIZE, 16);
/// ```
pub const KEY_SIZE: usize = 16;

/// Width of a value that can be stored directly in a node.
///
/// Eight-byte map values whose high bit is clear are inlined automatically.
/// # Examples
///
/// ```
/// assert_eq!(floresta_db::MAX_INLINE_VALUE_SIZE, 8);
/// ```
pub const MAX_INLINE_VALUE_SIZE: usize = size_of::<u64>();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
/// Selects whether a database stores keys alone or key-value pairs.
///
/// # Examples
///
/// ```
/// use floresta_db::{Config, Mode};
///
/// let set = Config::new(Mode::Set, 1_024);
/// let map = Config::new(Mode::Map, 1_024);
/// assert_ne!(set.mode, map.mode);
/// ```
pub enum Mode {
    /// Stores fixed-width keys without values.
    Set = 1,

    /// Stores a value for every fixed-width key.
    Map = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Defines the persistent layout of a database.
///
/// Capacities are maximum virtual mappings and must be multiples of
/// [`Config::block_size`]. Backing files grow by request-sized page extents.
///
/// # Examples
///
/// ```
/// use floresta_db::{Config, Mode};
///
/// let mut config = Config::new(Mode::Map, 1 << 20);
/// config.body_capacity = 8 << 30;
/// config.blob_capacity = 8 << 30;
/// config.validate()?;
/// # Ok::<(), floresta_db::Error>(())
/// ```
pub struct Config {
    /// Whether the database is a set or a map.
    pub mode: Mode,

    /// Number of hash buckets fixed at database creation.
    pub bucket_count: u64,

    /// Maximum logical bytes reserved for body nodes.
    pub body_capacity: u64,

    /// Maximum logical bytes reserved for map values.
    pub blob_capacity: u64,

    /// Allocation and reclamation granularity in bytes.
    pub block_size: u64,

    /// Seed supplied to XXH64 when selecting buckets.
    pub hash_seed: u64,
}

impl Config {
    #[must_use]
    /// Creates a configuration with one-GiB sparse capacities and one-MiB pages.
    ///
    /// Set mode starts with zero blob capacity. Map mode starts with one GiB of
    /// blob capacity and stores variable-width values there.
    ///
    /// # Examples
    ///
    /// ```
    /// use floresta_db::{Config, KEY_SIZE, Mode};
    ///
    /// let config = Config::new(Mode::Set, 4_096);
    /// assert_eq!(config.bucket_count, 4_096);
    /// assert_eq!(KEY_SIZE, 16);
    /// assert_eq!(config.blob_capacity, 0);
    /// ```
    pub fn new(mode: Mode, bucket_count: u64) -> Self {
        Self {
            mode,
            bucket_count,
            body_capacity: 1 << 30,
            blob_capacity: if mode == Mode::Map { 1 << 30 } else { 0 },
            block_size: 1 << 20,
            hash_seed: DEFAULT_HASH_SEED,
        }
    }

    /// Validates that capacities and layouts can be represented safely.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] when the configuration cannot produce
    /// a valid database layout.
    ///
    /// # Examples
    ///
    /// ```
    /// use floresta_db::{Config, Mode};
    ///
    /// let config = Config::new(Mode::Map, 1_024);
    /// config.validate()?;
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn validate(&self) -> Result<()> {
        if self.bucket_count == 0 {
            return Err(Error::InvalidConfig("bucket count must be nonzero"));
        }
        if self.body_capacity == 0 {
            return Err(Error::InvalidConfig("body capacity must be nonzero"));
        }
        if self.body_capacity > NODE_POINTER_MASK.saturating_sub(DATA_START) {
            return Err(Error::InvalidConfig(
                "body capacity exceeds the packed node-pointer range",
            ));
        }
        if self.mode == Mode::Map && self.blob_capacity == 0 {
            return Err(Error::InvalidConfig(
                "map mode requires nonzero fallback blob capacity",
            ));
        }
        if self.mode == Mode::Set && self.blob_capacity != 0 {
            return Err(Error::InvalidConfig("set mode cannot have blob capacity"));
        }
        if self.block_size < FORMAT_PAGE_SIZE || !self.block_size.is_power_of_two() {
            return Err(Error::InvalidConfig(
                "block size must be a power of two no smaller than 65536",
            ));
        }
        if self.block_size > u64::from(u32::MAX) {
            return Err(Error::InvalidConfig("block size must fit in 32 bits"));
        }
        if self.body_capacity % self.block_size != 0 {
            return Err(Error::InvalidConfig("body capacity must be block aligned"));
        }
        if self.blob_capacity % self.block_size != 0 {
            return Err(Error::InvalidConfig("blob capacity must be block aligned"));
        }
        if NODE_SIZE_U64 > self.block_size {
            return Err(Error::InvalidConfig("body page cannot hold one node"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_default_map_configuration() -> Result<()> {
        Config::new(Mode::Map, 1_024).validate()
    }

    #[test]
    fn rejects_blob_capacity_for_set() {
        let mut config = Config::new(Mode::Set, 1);
        config.blob_capacity = 4_096;
        assert!(matches!(config.validate(), Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn uses_fixed_cache_aligned_node_size() {
        assert_eq!(crate::layout::NODE_SIZE, 32);
        assert_eq!(NODE_SIZE_U64, 32);
    }
}
