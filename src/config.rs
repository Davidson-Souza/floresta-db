use crate::error::{Error, Result};
use crate::layout::{PAGE_SIZE, align_up};

pub const DEFAULT_HASH_SEED: u64 = 0;
pub const MAX_KEY_SIZE: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Mode {
    Set = 1,
    Map = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub mode: Mode,
    pub bucket_count: u64,
    pub key_size: usize,
    pub body_capacity: u64,
    pub blob_capacity: u64,
    pub block_size: u64,
    pub max_threads: u16,
    pub hash_seed: u64,
}

impl Config {
    #[must_use]
    pub fn new(mode: Mode, bucket_count: u64, key_size: usize) -> Self {
        Self {
            mode,
            bucket_count,
            key_size,
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
        if self.mode == Mode::Map && self.blob_capacity == 0 {
            return Err(Error::InvalidConfig(
                "map mode requires nonzero blob capacity",
            ));
        }
        if self.mode == Mode::Set && self.blob_capacity != 0 {
            return Err(Error::InvalidConfig("set mode cannot have blob capacity"));
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
