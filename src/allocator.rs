#![allow(dead_code)]

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};
use crate::layout::{DATA_START, PAGE_SIZE, align_up};
use crate::mapped_file::MappedFile;

const COUNTS_START: u64 = PAGE_SIZE;
const USED_MASK: u64 = u32::MAX as u64;
const COUNT_SHIFT: u32 = 32;
const COUNT_MASK: u64 = 0x00ff_ffff << COUNT_SHIFT;
const STATE_SHIFT: u32 = 56;
const MAX_COUNT: u32 = 0x00ff_ffff;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum BlockState {
    Unused = 0,
    Preparing = 1,
    Open = 2,
    Sealed = 3,
    Punching = 4,
    Punched = 5,
    Failed = 6,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Allocation {
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) block: u64,
}

pub(crate) struct BlockAllocator {
    data: MappedFile,
    counts: MappedFile,
    capacity: u64,
    block_size: u64,
    block_count: u64,
}

impl BlockAllocator {
    pub(crate) fn create(
        data_path: &Path,
        counts_path: &Path,
        capacity: u64,
        block_size: u64,
    ) -> Result<Self> {
        validate_layout(capacity, block_size)?;
        let (data_length, count_bytes, block_count) = file_lengths(capacity, block_size)?;
        let data = MappedFile::create(data_path, data_length, false, false)?;
        let counts = MappedFile::create(counts_path, count_bytes, false, true)?;
        data.advise_random()?;
        counts.advise_heads()?;
        Ok(Self {
            data,
            counts,
            capacity,
            block_size,
            block_count,
        })
    }

    pub(crate) fn open(
        data_path: &Path,
        counts_path: &Path,
        capacity: u64,
        block_size: u64,
    ) -> Result<Self> {
        validate_layout(capacity, block_size)?;
        let (data_length, count_bytes, block_count) = file_lengths(capacity, block_size)?;
        let data = MappedFile::open(data_path, data_length, false)?;
        let counts = MappedFile::open(counts_path, count_bytes, false)?;
        data.advise_random()?;
        counts.advise_heads()?;
        Ok(Self {
            data,
            counts,
            capacity,
            block_size,
            block_count,
        })
    }

    pub(crate) fn allocate(&self, length: usize, alignment: u64) -> Result<Allocation> {
        let length = u64::try_from(length).map_err(|_| Error::CapacityExhausted("allocation"))?;
        if length == 0 || length > self.block_size {
            return Err(Error::CapacityExhausted("single-block allocation"));
        }
        if alignment == 0 || !alignment.is_power_of_two() || alignment > self.block_size {
            return Err(Error::InvalidConfig("allocation alignment is invalid"));
        }
        let current = self.current_block()?;
        loop {
            let block = current.load(Ordering::Acquire);
            if block >= self.block_count {
                return Err(Error::CapacityExhausted("mapped file"));
            }
            match self.ensure_open(block)? {
                BlockState::Open => {}
                BlockState::Failed => return Err(Error::CapacityExhausted("physical storage")),
                BlockState::Sealed | BlockState::Punched => {
                    let _advanced = current.compare_exchange(
                        block,
                        block + 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    continue;
                }
                BlockState::Unused | BlockState::Preparing | BlockState::Punching => continue,
            }
            let word = self.block_word(block)?;
            let old = word.load(Ordering::Acquire);
            if state(old)? != BlockState::Open {
                continue;
            }
            let used_bytes = u64::from(used(old));
            let start =
                align_up(used_bytes, alignment).ok_or(Error::CapacityExhausted("block offset"))?;
            let end = start
                .checked_add(length)
                .ok_or(Error::CapacityExhausted("block offset"))?;
            if end > self.block_size {
                let sealed = pack(BlockState::Sealed, used(old), count(old));
                if word
                    .compare_exchange(old, sealed, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.punch_if_empty(block)?;
                    let _advanced = current.compare_exchange(
                        block,
                        block + 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }
                continue;
            }
            let old_count = count(old);
            if old_count == MAX_COUNT {
                let sealed = pack(BlockState::Sealed, used(old), old_count);
                if word
                    .compare_exchange(old, sealed, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    let _advanced = current.compare_exchange(
                        block,
                        block + 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }
                continue;
            }
            let new_count = old_count
                .checked_add(1)
                .ok_or(Error::CapacityExhausted("block object count"))?;
            let end_u32 =
                u32::try_from(end).map_err(|_| Error::CapacityExhausted("block offset"))?;
            let new = pack(BlockState::Open, end_u32, new_count);
            if word
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let offset = DATA_START
                    .checked_add(
                        block
                            .checked_mul(self.block_size)
                            .ok_or(Error::CapacityExhausted("allocation offset"))?,
                    )
                    .and_then(|base| base.checked_add(start))
                    .ok_or(Error::CapacityExhausted("allocation offset"))?;
                return Ok(Allocation {
                    offset,
                    length: u32::try_from(length)
                        .map_err(|_| Error::CapacityExhausted("allocation length"))?,
                    block,
                });
            }
        }
    }

    pub(crate) fn release(&self, allocation: Allocation) -> Result<()> {
        self.release_count(allocation)?;
        self.punch_if_empty(allocation.block)
    }

    pub(crate) fn validate_release(&self, allocation: Allocation) -> Result<()> {
        let old = self.block_word(allocation.block)?.load(Ordering::Acquire);
        if !matches!(state(old)?, BlockState::Open | BlockState::Sealed) {
            return Err(Error::Corrupt(
                "released allocation belongs to an inactive block",
            ));
        }
        if count(old) == 0 {
            return Err(Error::Corrupt("block object count underflow"));
        }
        Ok(())
    }

    pub(crate) fn release_count(&self, allocation: Allocation) -> Result<()> {
        let word = self.block_word(allocation.block)?;
        loop {
            let old = word.load(Ordering::Acquire);
            let block_state = state(old)?;
            if !matches!(block_state, BlockState::Open | BlockState::Sealed) {
                return Err(Error::Corrupt(
                    "released allocation belongs to an inactive block",
                ));
            }
            let new_count = count(old)
                .checked_sub(1)
                .ok_or(Error::Corrupt("block object count underflow"))?;
            let new = pack(block_state, used(old), new_count);
            if word
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    pub(crate) fn reclaim_empty_blocks(&self) -> Result<()> {
        for block in 0..self.block_count {
            self.punch_if_empty(block)?;
        }
        Ok(())
    }

    pub(crate) fn reclaim_block(&self, block: u64) -> Result<()> {
        self.punch_if_empty(block)
    }

    pub(crate) fn seal_current(&self) -> Result<()> {
        let block = self.current_block()?.load(Ordering::Acquire);
        if block >= self.block_count {
            return Ok(());
        }
        let word = self.block_word(block)?;
        loop {
            let old = word.load(Ordering::Acquire);
            match state(old)? {
                BlockState::Open => {
                    let new = pack(BlockState::Sealed, used(old), count(old));
                    if word
                        .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.punch_if_empty(block)?;
                        return Ok(());
                    }
                }
                BlockState::Unused | BlockState::Sealed | BlockState::Punched => return Ok(()),
                BlockState::Preparing | BlockState::Punching => std::hint::spin_loop(),
                BlockState::Failed => return Err(Error::CapacityExhausted("physical storage")),
            }
        }
    }

    pub(crate) fn write(&self, allocation: Allocation, input: &[u8]) -> Result<()> {
        if input.len() > allocation.length as usize {
            return Err(Error::Corrupt("write exceeds allocation"));
        }
        // SAFETY: a fresh Allocation is exclusively owned until its body node is published.
        unsafe { self.data.copy_in(allocation.offset, input) }
    }

    pub(crate) fn read(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.validate_data_range(offset, length)?;
        self.data.copy_out(offset, length)
    }

    pub(crate) fn atomic_u64(&self, offset: u64) -> Result<&AtomicU64> {
        self.validate_data_range(offset, size_of::<u64>())?;
        self.data.atomic_u64(offset)
    }

    pub(crate) fn allocation_for(&self, offset: u64, length: u32) -> Result<Allocation> {
        self.validate_data_range(offset, length as usize)?;
        let relative = offset
            .checked_sub(DATA_START)
            .ok_or(Error::Corrupt("allocation points into the file header"))?;
        let block = relative / self.block_size;
        let end_relative = relative
            .checked_add(u64::from(length))
            .ok_or(Error::Corrupt("allocation range overflow"))?;
        if end_relative.saturating_sub(1) / self.block_size != block {
            return Err(Error::Corrupt("allocation crosses a block boundary"));
        }
        Ok(Allocation {
            offset,
            length,
            block,
        })
    }

    pub(crate) fn sync_all(&self) -> Result<()> {
        self.data.sync_all()?;
        self.counts.sync_all()
    }

    fn ensure_open(&self, block: u64) -> Result<BlockState> {
        let word = self.block_word(block)?;
        loop {
            let old = word.load(Ordering::Acquire);
            match state(old)? {
                BlockState::Unused => {
                    let preparing = pack(BlockState::Preparing, 0, 0);
                    if word
                        .compare_exchange(old, preparing, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        continue;
                    }
                    let offset = DATA_START
                        .checked_add(
                            block
                                .checked_mul(self.block_size)
                                .ok_or(Error::CapacityExhausted("block offset"))?,
                        )
                        .ok_or(Error::CapacityExhausted("block offset"))?;
                    if let Err(error) = self.data.reserve(offset, self.block_size) {
                        let failed = pack(BlockState::Failed, 0, 0);
                        let _changed = word.compare_exchange(
                            preparing,
                            failed,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        );
                        return Err(error);
                    }
                    let open = pack(BlockState::Open, 0, 0);
                    word.compare_exchange(preparing, open, Ordering::Release, Ordering::Acquire)
                        .map_err(|_| Error::Corrupt("block preparation state changed"))?;
                    return Ok(BlockState::Open);
                }
                BlockState::Preparing | BlockState::Punching => std::hint::spin_loop(),
                other => return Ok(other),
            }
        }
    }

    fn punch_if_empty(&self, block: u64) -> Result<()> {
        let word = self.block_word(block)?;
        loop {
            let old = word.load(Ordering::Acquire);
            if state(old)? != BlockState::Sealed || count(old) != 0 {
                return Ok(());
            }
            let punching = pack(BlockState::Punching, used(old), 0);
            if word
                .compare_exchange(old, punching, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let offset = DATA_START
                .checked_add(
                    block
                        .checked_mul(self.block_size)
                        .ok_or(Error::CapacityExhausted("block offset"))?,
                )
                .ok_or(Error::CapacityExhausted("block offset"))?;
            if let Err(error) = self.data.punch(offset, self.block_size) {
                let sealed = pack(BlockState::Sealed, used(old), 0);
                let _changed =
                    word.compare_exchange(punching, sealed, Ordering::AcqRel, Ordering::Acquire);
                return Err(error);
            }
            let punched = pack(BlockState::Punched, used(old), 0);
            word.compare_exchange(punching, punched, Ordering::Release, Ordering::Acquire)
                .map_err(|_| Error::Corrupt("block punch state changed"))?;
            return Ok(());
        }
    }

    fn current_block(&self) -> Result<&AtomicU64> {
        self.counts.atomic_u64(0)
    }

    fn block_word(&self, block: u64) -> Result<&AtomicU64> {
        if block >= self.block_count {
            return Err(Error::Corrupt("block index is out of range"));
        }
        let offset = block
            .checked_mul(size_of::<u64>() as u64)
            .and_then(|value| value.checked_add(COUNTS_START))
            .ok_or(Error::Corrupt("block-count offset overflow"))?;
        self.counts.atomic_u64(offset)
    }

    fn validate_data_range(&self, offset: u64, length: usize) -> Result<()> {
        let length = u64::try_from(length).map_err(|_| Error::Corrupt("data length overflow"))?;
        let relative = offset
            .checked_sub(DATA_START)
            .ok_or(Error::Corrupt("data offset points into the file header"))?;
        let end = relative
            .checked_add(length)
            .ok_or(Error::Corrupt("data range overflow"))?;
        if end > self.capacity {
            return Err(Error::Corrupt("data range is out of bounds"));
        }
        Ok(())
    }
}

fn validate_layout(capacity: u64, block_size: u64) -> Result<()> {
    if capacity == 0 || block_size < PAGE_SIZE || !block_size.is_power_of_two() {
        return Err(Error::InvalidConfig("invalid block allocator layout"));
    }
    if capacity % block_size != 0 || block_size > u64::from(u32::MAX) {
        return Err(Error::InvalidConfig(
            "allocator capacity or block size is not representable",
        ));
    }
    Ok(())
}

fn file_lengths(capacity: u64, block_size: u64) -> Result<(u64, u64, u64)> {
    let block_count = capacity / block_size;
    let count_bytes = block_count
        .checked_mul(size_of::<u64>() as u64)
        .and_then(|bytes| bytes.checked_add(COUNTS_START))
        .and_then(|bytes| align_up(bytes, PAGE_SIZE))
        .ok_or(Error::InvalidConfig("block-count file size overflow"))?;
    let data_length = capacity
        .checked_add(DATA_START)
        .ok_or(Error::InvalidConfig("data file size overflow"))?;
    Ok((data_length, count_bytes, block_count))
}

const fn pack(block_state: BlockState, used: u32, count: u32) -> u64 {
    (block_state as u64) << STATE_SHIFT | (count as u64) << COUNT_SHIFT | used as u64
}

const fn used(word: u64) -> u32 {
    (word & USED_MASK) as u32
}

const fn count(word: u64) -> u32 {
    ((word & COUNT_MASK) >> COUNT_SHIFT) as u32
}

fn state(word: u64) -> Result<BlockState> {
    match (word >> STATE_SHIFT) as u8 {
        0 => Ok(BlockState::Unused),
        1 => Ok(BlockState::Preparing),
        2 => Ok(BlockState::Open),
        3 => Ok(BlockState::Sealed),
        4 => Ok(BlockState::Punching),
        5 => Ok(BlockState::Punched),
        6 => Ok(BlockState::Failed),
        _ => Err(Error::Corrupt("unknown block state")),
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    fn test_paths(name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let prefix = format!("db-experiment-{}-{name}", std::process::id());
        let directory = std::env::temp_dir();
        (
            directory.join(format!("{prefix}.data")),
            directory.join(format!("{prefix}.counts")),
        )
    }

    #[test]
    fn allocates_without_crossing_blocks_and_punches_empty_sealed_blocks() -> Result<()> {
        let (data_path, count_path) = test_paths("allocator");
        let _ignored_data = std::fs::remove_file(&data_path);
        let _ignored_counts = std::fs::remove_file(&count_path);
        let allocator = BlockAllocator::create(&data_path, &count_path, PAGE_SIZE * 2, PAGE_SIZE)?;
        let page_size = usize::try_from(PAGE_SIZE)
            .map_err(|_| Error::InvalidConfig("test page size does not fit usize"))?;
        let first = allocator.allocate(page_size - 8, 8)?;
        allocator.write(first, &[9; 32])?;
        let second = allocator.allocate(16, 8)?;
        assert_eq!(first.block, 0);
        assert_eq!(second.block, 1);
        allocator.release(first)?;
        assert!(
            allocator
                .read(first.offset, 32)?
                .iter()
                .all(|byte| *byte == 0)
        );
        allocator.release(second)?;
        allocator.seal_current()?;
        assert!(
            allocator
                .read(second.offset, 16)?
                .iter()
                .all(|byte| *byte == 0)
        );
        drop(allocator);
        std::fs::remove_file(data_path)?;
        std::fs::remove_file(count_path)?;
        Ok(())
    }

    #[test]
    fn reserves_and_counts_concurrent_allocations_with_cas() -> Result<()> {
        let (data_path, count_path) = test_paths("allocator-concurrent");
        let _ignored_data = std::fs::remove_file(&data_path);
        let _ignored_counts = std::fs::remove_file(&count_path);
        let allocator = BlockAllocator::create(&data_path, &count_path, PAGE_SIZE * 4, PAGE_SIZE)?;
        std::thread::scope(|scope| {
            for _index in 0..8 {
                scope.spawn(|| {
                    for _iteration in 0..16 {
                        let allocation = allocator.allocate(16, 8);
                        assert!(allocation.is_ok());
                    }
                });
            }
        });
        drop(allocator);
        std::fs::remove_file(data_path)?;
        std::fs::remove_file(count_path)?;
        Ok(())
    }

    #[test]
    fn advances_when_a_block_object_count_is_saturated() -> Result<()> {
        let (data_path, count_path) = test_paths("allocator-count-saturation");
        let _ignored_data = std::fs::remove_file(&data_path);
        let _ignored_counts = std::fs::remove_file(&count_path);
        let allocator = BlockAllocator::create(&data_path, &count_path, PAGE_SIZE * 2, PAGE_SIZE)?;
        let first = allocator.allocate(8, 8)?;
        let word = allocator.block_word(first.block)?;
        loop {
            let old = word.load(Ordering::Acquire);
            let saturated = pack(BlockState::Open, used(old), MAX_COUNT);
            if word
                .compare_exchange(old, saturated, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        assert_eq!(allocator.allocate(8, 8)?.block, 1);
        drop(allocator);
        std::fs::remove_file(data_path)?;
        std::fs::remove_file(count_path)?;
        Ok(())
    }
}
