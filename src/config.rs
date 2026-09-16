// SPDX-License-Identifier: MIT OR Apache-2.0

//! Database modes and validated storage configuration.
//!
//! [`Config`] describes the persistent layout and concurrency limits. Validation
//! rejects combinations that cannot be represented safely by the on-disk format.

use crate::error::{Error, Result};
use crate::layout::{PAGE_SIZE, align_up};

/// The default seed used for key hashing.
///
/// # Examples
///
/// ```
/// assert_eq!(floresta_db::DEFAULT_HASH_SEED, 0);
/// ```
pub const DEFAULT_HASH_SEED: u64 = 0;

/// The largest fixed key width accepted by [`Config`].
///
/// # Examples
///
/// ```
/// assert_eq!(floresta_db::MAX_KEY_SIZE, 4_096);
/// ```
pub const MAX_KEY_SIZE: usize = 4_096;

/// The largest value width that can be stored directly inside a node.
///
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
/// let set = Config::new(Mode::Set, 1_024, 32);
/// let map = Config::new(Mode::Map, 1_024, 32);
/// assert_ne!(set.mode, map.mode);
/// ```
pub enum Mode {
    /// Stores fixed-width keys without values.
    Set = 1,

    /// Stores a value for every fixed-width key.
    Map = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Defines the persistent layout and concurrency limits of a database.
///
/// Capacities are reserved as sparse files and must be multiples of
/// [`Config::block_size`]. They do not represent immediate physical allocation.
///
/// # Examples
///
/// ```
/// use floresta_db::{Config, Mode};
///
/// let mut config = Config::new(Mode::Map, 1 << 20, 36);
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

    /// Required key width in bytes.
    pub key_size: usize,

    /// Fixed value width stored inside each node, or zero to use the blob file.
    pub inline_value_size: usize,

    /// Maximum logical bytes reserved for body nodes.
    pub body_capacity: u64,

    /// Maximum logical bytes reserved for map values.
    pub blob_capacity: u64,

    /// Allocation and reclamation granularity in bytes.
    pub block_size: u64,

    /// Maximum number of simultaneous hazard-pointer registrations.
    pub max_threads: u16,

    /// Seed supplied to XXH64 when selecting buckets.
    pub hash_seed: u64,
}

impl Config {
    #[must_use]
    /// Creates a configuration with one-GiB sparse capacities and one-MiB blocks.
    ///
    /// Set mode starts with zero blob capacity. Map mode starts with one GiB of
    /// blob capacity and stores variable-width values there.
    ///
    /// # Examples
    ///
    /// ```
    /// use floresta_db::{Config, Mode};
    ///
    /// let config = Config::new(Mode::Set, 4_096, 32);
    /// assert_eq!(config.bucket_count, 4_096);
    /// assert_eq!(config.key_size, 32);
    /// assert_eq!(config.blob_capacity, 0);
    /// ```
    pub fn new(mode: Mode, bucket_count: u64, key_size: usize) -> Self {
        Self {
            mode,
            bucket_count,
            key_size,
            inline_value_size: 0,
            body_capacity: 1 << 30,
            blob_capacity: if mode == Mode::Map { 1 << 30 } else { 0 },
            block_size: 1 << 20,
            max_threads: 64,
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
    /// let config = Config::new(Mode::Map, 1_024, 36);
    /// config.validate()?;
    /// # Ok::<(), floresta_db::Error>(())
    /// ```
    pub fn validate(&self) -> Result<()> {
        if self.bucket_count == 0 {
            return Err(Error::InvalidConfig("bucket count must be nonzero"));
        }
        if self.key_size == 0 || self.key_size > MAX_KEY_SIZE {
            return Err(Error::InvalidConfig(
                "key size must be between 1 and 4096 bytes",
            ));
        }
        if self.body_capacity == 0 {
            return Err(Error::InvalidConfig("body capacity must be nonzero"));
        }
        if self.inline_value_size > MAX_INLINE_VALUE_SIZE {
            return Err(Error::InvalidConfig(
                "inline value size cannot exceed eight bytes",
            ));
        }
        if self.mode == Mode::Map && self.inline_value_size == 0 && self.blob_capacity == 0 {
            return Err(Error::InvalidConfig(
                "map mode without inline values requires nonzero blob capacity",
            ));
        }
        if self.mode == Mode::Map && self.inline_value_size != 0 && self.blob_capacity != 0 {
            return Err(Error::InvalidConfig(
                "inline map values cannot use blob capacity",
            ));
        }
        if self.mode == Mode::Set && (self.blob_capacity != 0 || self.inline_value_size != 0) {
            return Err(Error::InvalidConfig("set mode cannot have values"));
        }
        if self.block_size < PAGE_SIZE || !self.block_size.is_power_of_two() {
            return Err(Error::InvalidConfig(
                "block size must be a power of two no smaller than 4096",
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
        if self.max_threads == 0 {
            return Err(Error::InvalidConfig("maximum thread count must be nonzero"));
        }
        let node_size = self.node_size()?;
        if node_size > self.block_size {
            return Err(Error::InvalidConfig("body block cannot hold one node"));
        }
        Ok(())
    }

    pub(crate) fn node_size(&self) -> Result<u64> {
        let key_size = u64::try_from(self.key_size)
            .map_err(|_| Error::InvalidConfig("key size does not fit the file format"))?;
        let unaligned = crate::layout::NODE_FIXED_SIZE
            .checked_add(key_size)
            .ok_or(Error::InvalidConfig("node size overflow"))?;
        align_up(unaligned, crate::layout::NODE_ALIGNMENT)
            .ok_or(Error::InvalidConfig("node size overflow"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_default_map_configuration() -> Result<()> {
        Config::new(Mode::Map, 1_024, 36).validate()
    }

    #[test]
    fn rejects_blob_capacity_for_set() {
        let mut config = Config::new(Mode::Set, 1, 32);
        config.blob_capacity = 4_096;
        assert!(matches!(config.validate(), Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn aligns_nodes_for_tagged_atomic_links() -> Result<()> {
        let config = Config::new(Mode::Set, 1, 37);
        assert_eq!(config.node_size()? % crate::layout::NODE_ALIGNMENT, 0);
        Ok(())
    }
}
