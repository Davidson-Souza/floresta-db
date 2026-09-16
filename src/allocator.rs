// SPDX-License-Identifier: MIT OR Apache-2.0

//! Lock-free allocation within growable memory-mapped files.
//!
//! Each block stores its allocation cursor, live-object count, and lifecycle
//! state in one atomic word. Empty sealed blocks are pushed onto a tagged LIFO
//! free list and reused before the backing files grow to another block.

#![allow(dead_code)]

use std::io::Read;

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};
use crate::layout::{DATA_START, PAGE_SIZE, align_up};
use crate::mapped_file::MappedFile;

const CURRENT_BLOCK_OFFSET: u64 = 0;
const NEXT_BLOCK_OFFSET: u64 = 8;
const FREE_HEAD_OFFSET: u64 = 16;
const COUNTS_START: u64 = PAGE_SIZE;
const CURRENT_INSTALLING: u64 = u64::MAX;
const NEXT_GROWING_BIT: u64 = 1 << 63;
const FREE_INDEX_MASK: u64 = u32::MAX as u64;
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

    Freeing = 4,

    Free = 5,
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
        let data = MappedFile::create_growable(data_path, data_length, DATA_START, false)?;
        let counts = MappedFile::create_growable(counts_path, count_bytes, COUNTS_START, false)?;
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
        let data = MappedFile::open_growable(data_path, data_length, false)?;
        let counts = MappedFile::open_growable(counts_path, count_bytes, false)?;
        data.advise_random()?;
        counts.advise_heads()?;
        let allocator = Self {
            data,
            counts,
            capacity,
            block_size,
            block_count,
        };
        allocator.validate_header()?;
        allocator.recycle_empty_blocks()?;
        Ok(allocator)
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
            let encoded = current.load(Ordering::Acquire);
            if encoded == CURRENT_INSTALLING {
                std::hint::spin_loop();
                continue;
            }
            if encoded == 0 {
                self.install_current(current)?;
                continue;
            }
            let block =
                decode_block(encoded)?.ok_or(Error::Corrupt("current block cannot be null"))?;
            let word = self.block_word(block)?;
            let old = word.load(Ordering::Acquire);
            if state(old)? != BlockState::Open {
                let _cleared =
                    current.compare_exchange(encoded, 0, Ordering::AcqRel, Ordering::Acquire);
                self.recycle_sealed_if_empty(block)?;
                continue;
            }

            let used_bytes = u64::from(used(old));
            let start =
                align_up(used_bytes, alignment).ok_or(Error::CapacityExhausted("block offset"))?;
            let end = start
                .checked_add(length)
                .ok_or(Error::CapacityExhausted("block offset"))?;
            let old_count = count(old);
            if end > self.block_size || old_count == MAX_COUNT {
                let sealed = pack(BlockState::Sealed, used(old), old_count);
                if word
                    .compare_exchange(old, sealed, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    let _cleared =
                        current.compare_exchange(encoded, 0, Ordering::AcqRel, Ordering::Acquire);
                    self.recycle_sealed_if_empty(block)?;
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
                let offset = self
                    .data_offset(block)?
                    .checked_add(start)
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
        self.release_count(allocation)
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
            let new_state = if block_state == BlockState::Sealed && new_count == 0 {
                BlockState::Freeing
            } else {
                block_state
            };
            let new = pack(new_state, used(old), new_count);
            if word
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if new_state == BlockState::Freeing {
                    self.enqueue_free(allocation.block)?;
                }
                return Ok(());
            }
        }
    }

    pub(crate) fn recycle_empty_blocks(&self) -> Result<usize> {
        let next = self.next_block_value()?;
        let mut recycled = 0_usize;
        for block in 0..next {
            if self.recycle_sealed_if_empty(block)? {
                recycled = recycled
                    .checked_add(1)
                    .ok_or(Error::CapacityExhausted("recycled block count"))?;
            }
        }
        Ok(recycled)
    }

    pub(crate) fn seal_current(&self) -> Result<()> {
        let current = self.current_block()?;
        loop {
            let encoded = current.load(Ordering::Acquire);
            if encoded == CURRENT_INSTALLING {
                std::hint::spin_loop();
                continue;
            }
            let Some(block) = decode_block(encoded)? else {
                return Ok(());
            };
            let word = self.block_word(block)?;
            let old = word.load(Ordering::Acquire);
            match state(old)? {
                BlockState::Open => {
                    let sealed = pack(BlockState::Sealed, used(old), count(old));
                    if word
                        .compare_exchange(old, sealed, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        let _cleared = current.compare_exchange(
                            encoded,
                            0,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        );
                        self.recycle_sealed_if_empty(block)?;
                        return Ok(());
                    }
                }
                BlockState::Preparing => std::hint::spin_loop(),
                _ => {
                    let _cleared =
                        current.compare_exchange(encoded, 0, Ordering::AcqRel, Ordering::Acquire);
                    self.recycle_sealed_if_empty(block)?;
                    return Ok(());
                }
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

    fn install_current(&self, current: &AtomicU64) -> Result<()> {
        if current
            .compare_exchange(0, CURRENT_INSTALLING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let block = match self.take_block() {
            Ok(block) => block,
            Err(error) => {
                current
                    .compare_exchange(CURRENT_INSTALLING, 0, Ordering::Release, Ordering::Acquire)
                    .map_err(|_| Error::Corrupt("current-block install state changed"))?;
                return Err(error);
            }
        };
        let encoded = encode_block(block)?;
        current
            .compare_exchange(
                CURRENT_INSTALLING,
                encoded,
                Ordering::Release,
                Ordering::Acquire,
            )
            .map_err(|_| Error::Corrupt("current-block install state changed"))?;
        Ok(())
    }

    fn take_block(&self) -> Result<u64> {
        if let Some(block) = self.pop_free()? {
            return Ok(block);
        }
        self.grow_block()
    }

    fn pop_free(&self) -> Result<Option<u64>> {
        let head = self.free_head()?;
        loop {
            let old = head.load(Ordering::Acquire);
            let Some(block) = decode_free_head(old)? else {
                return Ok(None);
            };
            let next = self.next_block()?.load(Ordering::Acquire) & !NEXT_GROWING_BIT;
            if block >= next {
                return Err(Error::Corrupt(
                    "free-list block is beyond the high-water mark",
                ));
            }
            let link = self.free_link(block)?;
            let next = link.load(Ordering::Acquire);
            if next & !FREE_INDEX_MASK != 0 {
                return Err(Error::Corrupt("free-list link has invalid tag bits"));
            }
            let new = advance_free_head(old, next);
            if head
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            let word = self.block_word(block)?;
            let free = pack(BlockState::Free, 0, 0);
            let preparing = pack(BlockState::Preparing, 0, 0);
            word.compare_exchange(free, preparing, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| Error::Corrupt("free-list block is not free"))?;
            cas_replace(link, 0);
            word.compare_exchange(
                preparing,
                pack(BlockState::Open, 0, 0),
                Ordering::Release,
                Ordering::Acquire,
            )
            .map_err(|_| Error::Corrupt("reused block preparation state changed"))?;
            return Ok(Some(block));
        }
    }

    fn grow_block(&self) -> Result<u64> {
        let next = self.next_block()?;
        loop {
            let old = next.load(Ordering::Acquire);
            if old & NEXT_GROWING_BIT != 0 {
                std::hint::spin_loop();
                continue;
            }
            if old >= self.block_count {
                return Err(Error::CapacityExhausted("mapped file"));
            }
            let growing = old | NEXT_GROWING_BIT;
            if next
                .compare_exchange(old, growing, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            let result = self.prepare_fresh_block(old);
            let published = if result.is_ok() { old + 1 } else { old };
            next.compare_exchange(growing, published, Ordering::Release, Ordering::Acquire)
                .map_err(|_| Error::Corrupt("allocator growth state changed"))?;
            result?;
            return Ok(old);
        }
    }

    fn prepare_fresh_block(&self, block: u64) -> Result<()> {
        let blocks = block
            .checked_add(1)
            .ok_or(Error::CapacityExhausted("block count"))?;
        self.counts.grow(count_file_length(blocks)?)?;
        self.data.grow(data_file_length(blocks, self.block_size)?)?;

        let word = self.block_word(block)?;
        let unused = pack(BlockState::Unused, 0, 0);
        let preparing = pack(BlockState::Preparing, 0, 0);
        word.compare_exchange(unused, preparing, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Corrupt("fresh block metadata is not unused"))?;
        let offset = self.data_offset(block)?;
        if let Err(error) = self.data.reserve(offset, self.block_size) {
            let _reset =
                word.compare_exchange(preparing, unused, Ordering::AcqRel, Ordering::Acquire);
            return Err(error);
        }
        word.compare_exchange(
            preparing,
            pack(BlockState::Open, 0, 0),
            Ordering::Release,
            Ordering::Acquire,
        )
        .map_err(|_| Error::Corrupt("fresh block preparation state changed"))?;
        Ok(())
    }

    fn recycle_sealed_if_empty(&self, block: u64) -> Result<bool> {
        let word = self.block_word(block)?;
        loop {
            let old = word.load(Ordering::Acquire);
            match state(old)? {
                BlockState::Sealed if count(old) == 0 => {
                    let freeing = pack(BlockState::Freeing, used(old), 0);
                    if word
                        .compare_exchange(old, freeing, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.enqueue_free(block)?;
                        return Ok(true);
                    }
                }
                BlockState::Freeing => {
                    std::hint::spin_loop();
                }
                _ => return Ok(false),
            }
        }
    }

    fn enqueue_free(&self, block: u64) -> Result<()> {
        let word = self.block_word(block)?;
        let old = word.load(Ordering::Acquire);
        if state(old)? != BlockState::Freeing || count(old) != 0 {
            return Err(Error::Corrupt("block is not ready for the free list"));
        }
        word.compare_exchange(
            old,
            pack(BlockState::Free, 0, 0),
            Ordering::Release,
            Ordering::Acquire,
        )
        .map_err(|_| Error::Corrupt("freeing block state changed"))?;

        let head = self.free_head()?;
        let link = self.free_link(block)?;
        let encoded = encode_block(block)?;
        loop {
            let old_head = head.load(Ordering::Acquire);
            cas_replace(link, old_head & FREE_INDEX_MASK);
            let new_head = advance_free_head(old_head, encoded);
            if head
                .compare_exchange(old_head, new_head, Ordering::Release, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn validate_header(&self) -> Result<()> {
        let next = self.next_block_value()?;
        if next > self.block_count {
            return Err(Error::Corrupt("allocator high-water mark is out of range"));
        }
        if self.counts.file_length() < count_file_length(next)? {
            return Err(Error::Corrupt(
                "block-count file is shorter than its high-water mark",
            ));
        }
        if self.data.file_length() < data_file_length(next, self.block_size)? {
            return Err(Error::Corrupt(
                "data file is shorter than its high-water mark",
            ));
        }

        let current = self.current_block()?.load(Ordering::Acquire);
        if current == CURRENT_INSTALLING {
            return Err(Error::Corrupt(
                "allocator was closed while installing a current block",
            ));
        }
        if let Some(block) = decode_block(current)? {
            if block >= next {
                return Err(Error::Corrupt(
                    "current block is beyond the high-water mark",
                ));
            }
        }
        let free = self.free_head()?.load(Ordering::Acquire);
        if let Some(block) = decode_free_head(free)? {
            if block >= next {
                return Err(Error::Corrupt(
                    "free-list head is beyond the high-water mark",
                ));
            }
        }
        Ok(())
    }

    fn next_block_value(&self) -> Result<u64> {
        let next = self.next_block()?.load(Ordering::Acquire);
        if next & NEXT_GROWING_BIT != 0 {
            return Err(Error::Corrupt("allocator was closed while growing"));
        }
        Ok(next)
    }

    fn current_block(&self) -> Result<&AtomicU64> {
        self.counts.atomic_u64(CURRENT_BLOCK_OFFSET)
    }

    fn next_block(&self) -> Result<&AtomicU64> {
        self.counts.atomic_u64(NEXT_BLOCK_OFFSET)
    }

    fn free_head(&self) -> Result<&AtomicU64> {
        self.counts.atomic_u64(FREE_HEAD_OFFSET)
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

    fn free_link(&self, block: u64) -> Result<&AtomicU64> {
        self.data.atomic_u64(self.data_offset(block)?)
    }

    fn data_offset(&self, block: u64) -> Result<u64> {
        DATA_START
            .checked_add(
                block
                    .checked_mul(self.block_size)
                    .ok_or(Error::CapacityExhausted("block offset"))?,
            )
            .ok_or(Error::CapacityExhausted("block offset"))
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
    let block_count = capacity / block_size;
    if block_count == 0 || block_count > u64::from(u32::MAX) {
        return Err(Error::InvalidConfig(
            "allocator block count exceeds the tagged free-list format",
        ));
    }
    Ok(())
}

pub(crate) fn file_lengths(capacity: u64, block_size: u64) -> Result<(u64, u64, u64)> {
    let block_count = capacity / block_size;
    Ok((
        data_file_length(block_count, block_size)?,
        count_file_length(block_count)?,
        block_count,
    ))
}

pub(crate) fn read_high_water(counts_path: &Path) -> Result<u64> {
    let mut counts = std::fs::File::open(counts_path)?;
    let mut allocator_header = [0_u8; 16];
    counts.read_exact(&mut allocator_header)?;
    let mut next_bytes = [0_u8; 8];
    next_bytes.copy_from_slice(&allocator_header[8..16]);
    let next = u64::from_le_bytes(next_bytes);
    if next & NEXT_GROWING_BIT != 0 {
        return Err(Error::Corrupt("allocator was closed while growing"));
    }
    Ok(next)
}

pub(crate) fn count_file_length(blocks: u64) -> Result<u64> {
    blocks
        .checked_mul(size_of::<u64>() as u64)
        .and_then(|bytes| bytes.checked_add(COUNTS_START))
        .and_then(|bytes| align_up(bytes, PAGE_SIZE))
        .ok_or(Error::InvalidConfig("block-count file size overflow"))
}

fn data_file_length(blocks: u64, block_size: u64) -> Result<u64> {
    blocks
        .checked_mul(block_size)
        .and_then(|bytes| bytes.checked_add(DATA_START))
        .ok_or(Error::InvalidConfig("data file size overflow"))
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
        4 => Ok(BlockState::Freeing),
        5 => Ok(BlockState::Free),
        _ => Err(Error::Corrupt("unknown block state")),
    }
}

fn encode_block(block: u64) -> Result<u64> {
    let encoded = block
        .checked_add(1)
        .ok_or(Error::CapacityExhausted("block index"))?;
    u32::try_from(encoded)
        .map(u64::from)
        .map_err(|_| Error::CapacityExhausted("block index"))
}

fn decode_block(encoded: u64) -> Result<Option<u64>> {
    if encoded & !FREE_INDEX_MASK != 0 {
        return Err(Error::Corrupt("encoded block index has tag bits"));
    }
    if encoded == 0 {
        Ok(None)
    } else {
        Ok(Some(encoded - 1))
    }
}

fn decode_free_head(head: u64) -> Result<Option<u64>> {
    decode_block(head & FREE_INDEX_MASK)
}

fn advance_free_head(old: u64, encoded_block: u64) -> u64 {
    let tag = (old >> 32).wrapping_add(1) & FREE_INDEX_MASK;
    tag << 32 | encoded_block
}

fn cas_replace(atomic: &AtomicU64, replacement: u64) {
    let mut old = atomic.load(Ordering::Acquire);
    loop {
        match atomic.compare_exchange(old, replacement, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(actual) => old = actual,
        }
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    fn test_paths(name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let prefix = format!("floresta-db-{}-{name}", std::process::id());
        let directory = std::env::temp_dir();
        (
            directory.join(format!("{prefix}.data")),
            directory.join(format!("{prefix}.counts")),
        )
    }

    #[test]
    fn grows_only_without_free_blocks_and_reuses_lifo() -> Result<()> {
        let (data_path, count_path) = test_paths("allocator-free-list");
        let _ignored_data = std::fs::remove_file(&data_path);
        let _ignored_counts = std::fs::remove_file(&count_path);
        let allocator = BlockAllocator::create(&data_path, &count_path, PAGE_SIZE * 3, PAGE_SIZE)?;
        let page_size = usize::try_from(PAGE_SIZE)
            .map_err(|_| Error::InvalidConfig("test page size does not fit usize"))?;

        assert_eq!(std::fs::metadata(&data_path)?.len(), DATA_START);
        let first = allocator.allocate(page_size - 8, 8)?;
        let second = allocator.allocate(page_size - 8, 8)?;
        let third = allocator.allocate(16, 8)?;
        assert_eq!((first.block, second.block, third.block), (0, 1, 2));
        let grown_length = std::fs::metadata(&data_path)?.len();
        assert_eq!(grown_length, DATA_START + PAGE_SIZE * 3);

        allocator.release(first)?;
        allocator.release(second)?;
        allocator.seal_current()?;
        let reused = allocator.allocate(16, 8)?;
        assert_eq!(reused.block, 1);
        assert_eq!(std::fs::metadata(&data_path)?.len(), grown_length);

        drop(allocator);
        std::fs::remove_file(data_path)?;
        std::fs::remove_file(count_path)?;
        Ok(())
    }

    #[test]
    fn persists_free_list_across_clean_reopen() -> Result<()> {
        let (data_path, count_path) = test_paths("allocator-reopen");
        let _ignored_data = std::fs::remove_file(&data_path);
        let _ignored_counts = std::fs::remove_file(&count_path);
        let capacity = PAGE_SIZE * 2;
        {
            let allocator = BlockAllocator::create(&data_path, &count_path, capacity, PAGE_SIZE)?;
            let page_size = usize::try_from(PAGE_SIZE)
                .map_err(|_| Error::InvalidConfig("test page size does not fit usize"))?;
            let first = allocator.allocate(page_size - 8, 8)?;
            let _second = allocator.allocate(16, 8)?;
            allocator.release(first)?;
            allocator.seal_current()?;
            allocator.sync_all()?;
        }

        let length = std::fs::metadata(&data_path)?.len();
        let reopened = BlockAllocator::open(&data_path, &count_path, capacity, PAGE_SIZE)?;
        let reused = reopened.allocate(16, 8)?;
        assert_eq!(reused.block, 0);
        assert_eq!(std::fs::metadata(&data_path)?.len(), length);

        drop(reopened);
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
    fn concurrently_freed_blocks_are_reused_without_growth() -> Result<()> {
        const BLOCKS: usize = 8;

        let (data_path, count_path) = test_paths("allocator-concurrent-free");
        let _ignored_data = std::fs::remove_file(&data_path);
        let _ignored_counts = std::fs::remove_file(&count_path);
        let allocator = BlockAllocator::create(
            &data_path,
            &count_path,
            PAGE_SIZE * BLOCKS as u64,
            PAGE_SIZE,
        )?;
        let page_size = usize::try_from(PAGE_SIZE)
            .map_err(|_| Error::InvalidConfig("test page size does not fit usize"))?;
        let mut allocations = Vec::new();
        allocations
            .try_reserve_exact(BLOCKS)
            .map_err(|_| Error::OutOfMemory)?;
        for _block in 0..BLOCKS {
            allocations.push(allocator.allocate(page_size - 8, 8)?);
        }
        allocator.seal_current()?;
        let grown_length = std::fs::metadata(&data_path)?.len();

        std::thread::scope(|scope| {
            for allocation in &allocations {
                let allocator_ref = &allocator;
                let allocation = *allocation;
                scope.spawn(move || {
                    assert!(allocator_ref.release(allocation).is_ok());
                });
            }
        });

        let mut seen = [false; BLOCKS];
        for _block in 0..BLOCKS {
            let allocation = allocator.allocate(page_size - 8, 8)?;
            let block = usize::try_from(allocation.block)
                .map_err(|_| Error::Corrupt("reused block index does not fit usize"))?;
            let present = seen
                .get_mut(block)
                .ok_or(Error::Corrupt("reused block index is out of range"))?;
            assert!(!*present);
            *present = true;
        }
        assert!(seen.into_iter().all(|present| present));
        assert_eq!(std::fs::metadata(&data_path)?.len(), grown_length);

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
