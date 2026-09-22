// SPDX-License-Identifier: MIT OR Apache-2.0

//! Lock-free page allocation within growable memory-mapped files.
//!
//! The count file stores one 16-bit live-allocation count per data page. Zero
//! pages are reusable, `u16::MAX` marks the first page of an unfinished run, and
//! sealed pages carry their exact live count. A SIMD scan finds the longest zero
//! run whose predecessor is not claimed, then CASes its first page.
//! The owner consumes the following contiguous zero pages without another CAS.

#![allow(dead_code)]

use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

use crate::error::{Error, Result};
use crate::layout::{DATA_START, FORMAT_PAGE_SIZE, align_up};
use crate::mapped_file::MappedFile;

const HIGH_WATER_OFFSET: u64 = 0;
const COUNTS_START: u64 = FORMAT_PAGE_SIZE;
const GROWING_BIT: u64 = 1 << 63;
const CURRENT_INSTALLING: u64 = u64::MAX;
const USED_MASK: u64 = u32::MAX as u64;
const LIVE_SHIFT: u32 = 32;
const LIVE_MASK: u64 = (u16::MAX as u64) << LIVE_SHIFT;
const STATE_SHIFT: u32 = 56;
const MAX_LIVE: u16 = u16::MAX - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum CurrentState {
    Empty = 0,
    Open = 1,
    Sealing = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Allocation {
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) block: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AllocationBatch {
    offset: u64,
    length: u32,
    stride: u32,
    count: u32,
    block: u64,
}

impl AllocationBatch {
    pub(crate) fn len(&self) -> usize {
        usize::try_from(self.count).unwrap_or(usize::MAX)
    }

    pub(crate) fn allocations(&self) -> impl ExactSizeIterator<Item = Allocation> + '_ {
        (0..self.count).map(|index| Allocation {
            offset: self.offset + u64::from(index) * u64::from(self.stride),
            length: self.length,
            block: self.block,
        })
    }
}

pub(crate) struct BlockAllocator {
    data: MappedFile,
    counts: MappedFile,
    capacity: u64,
    block_size: u64,
    block_count: u64,
    current_block: AtomicU64,
    current_word: AtomicU64,
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
        counts.advise_heads()?;
        Ok(Self {
            data,
            counts,
            capacity,
            block_size,
            block_count,
            current_block: AtomicU64::new(0),
            current_word: AtomicU64::new(0),
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
        let allocator = Self {
            data: MappedFile::open_growable(data_path, data_length, false)?,
            counts: MappedFile::open_growable(counts_path, count_bytes, false)?,
            capacity,
            block_size,
            block_count,
            current_block: AtomicU64::new(0),
            current_word: AtomicU64::new(0),
        };
        allocator.counts.advise_heads()?;
        allocator.validate_header()?;
        Ok(allocator)
    }

    pub(crate) fn allocate(&self, length: usize, alignment: u64) -> Result<Allocation> {
        self.allocate_batch(length, alignment, 1)?
            .allocations()
            .next()
            .ok_or(Error::Corrupt("single allocation batch is empty"))
    }

    pub(crate) fn allocate_batch(
        &self,
        length: usize,
        alignment: u64,
        maximum_count: usize,
    ) -> Result<AllocationBatch> {
        let length = u64::try_from(length).map_err(|_| Error::CapacityExhausted("allocation"))?;
        if length == 0 || length > self.block_size || maximum_count == 0 {
            return Err(Error::CapacityExhausted("single-page allocation"));
        }
        if alignment == 0 || !alignment.is_power_of_two() || alignment > self.block_size {
            return Err(Error::InvalidConfig("allocation alignment is invalid"));
        }
        let stride =
            align_up(length, alignment).ok_or(Error::CapacityExhausted("allocation stride"))?;
        let requested = u32::try_from(maximum_count)
            .unwrap_or(u32::MAX)
            .min(u32::from(MAX_LIVE));
        let growth_bytes = stride
            .checked_mul(u64::from(requested))
            .ok_or(Error::CapacityExhausted("batch allocation size"))?;

        loop {
            let encoded = read_shared_u64(&self.current_block);
            if encoded == CURRENT_INSTALLING {
                std::hint::spin_loop();
                continue;
            }
            if encoded == 0 {
                self.install_current(growth_bytes)?;
                continue;
            }
            let block = decode_block(encoded)?;
            let old = read_shared_u64(&self.current_word);
            if current_state(old)? != CurrentState::Open {
                std::hint::spin_loop();
                continue;
            }
            let start = align_up(current_used(old), alignment)
                .ok_or(Error::CapacityExhausted("page offset"))?;
            let old_live = current_live(old);
            let first_end = start
                .checked_add(length)
                .ok_or(Error::CapacityExhausted("page offset"))?;
            if first_end > self.block_size || old_live == MAX_LIVE {
                self.seal_current_block(encoded, true)?;
                continue;
            }
            let fitting = 1_u64
                .checked_add((self.block_size - first_end) / stride)
                .ok_or(Error::CapacityExhausted("batch allocation count"))?;
            let fitting = u32::try_from(fitting).unwrap_or(u32::MAX);
            let reserved = requested.min(fitting).min(u32::from(MAX_LIVE - old_live));
            let end = start
                .checked_add(u64::from(reserved - 1).saturating_mul(stride))
                .and_then(|offset| offset.checked_add(length))
                .ok_or(Error::CapacityExhausted("page offset"))?;
            let new = pack_current(
                CurrentState::Open,
                end,
                old_live
                    .checked_add(
                        u16::try_from(reserved)
                            .map_err(|_| Error::CapacityExhausted("page live-allocation count"))?,
                    )
                    .ok_or(Error::CapacityExhausted("page live-allocation count"))?,
            )?;
            if self
                .current_word
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let offset = self
                .data_offset(block)?
                .checked_add(start)
                .ok_or(Error::CapacityExhausted("allocation offset"))?;
            return Ok(AllocationBatch {
                offset,
                length: u32::try_from(length)
                    .map_err(|_| Error::CapacityExhausted("allocation length"))?,
                stride: u32::try_from(stride)
                    .map_err(|_| Error::CapacityExhausted("allocation stride"))?,
                count: reserved,
                block,
            });
        }
    }

    pub(crate) fn release(&self, allocation: Allocation) -> Result<()> {
        self.release_count(allocation)
    }

    pub(crate) fn validate_release(&self, allocation: Allocation) -> Result<()> {
        self.validate_allocation(allocation)?;
        loop {
            let current = read_shared_u64(&self.current_block);
            if current == CURRENT_INSTALLING {
                std::hint::spin_loop();
                continue;
            }
            if current != 0 && decode_block(current)? == allocation.block {
                let word = read_shared_u64(&self.current_word);
                if current_state(word)? == CurrentState::Open && current_live(word) != 0 {
                    return Ok(());
                }
                std::hint::spin_loop();
                continue;
            }
            let count = self.read_count(allocation.block)?;
            return if count == 0 || count == u16::MAX {
                Err(Error::Corrupt(
                    "released allocation belongs to an inactive page",
                ))
            } else {
                Ok(())
            };
        }
    }

    pub(crate) fn release_count(&self, allocation: Allocation) -> Result<()> {
        self.validate_allocation(allocation)?;
        loop {
            let current = read_shared_u64(&self.current_block);
            if current == CURRENT_INSTALLING {
                std::hint::spin_loop();
                continue;
            }
            if current != 0 && decode_block(current)? == allocation.block {
                let old = read_shared_u64(&self.current_word);
                if current_state(old)? != CurrentState::Open {
                    std::hint::spin_loop();
                    continue;
                }
                let live = current_live(old)
                    .checked_sub(1)
                    .ok_or(Error::Corrupt("page allocation count underflow"))?;
                let new = pack_current(CurrentState::Open, current_used(old), live)?;
                if self
                    .current_word
                    .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Ok(());
                }
                continue;
            }

            let count = self.count_word(allocation.block)?;
            let mut old = self.read_count(allocation.block)?;
            loop {
                if old == 0 || old == u16::MAX {
                    return Err(Error::Corrupt(
                        "released allocation belongs to an inactive page",
                    ));
                }
                match count.compare_exchange_weak(old, old - 1, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => return Ok(()),
                    Err(actual) => old = actual,
                }
            }
        }
    }

    pub(crate) fn recycle_empty_blocks(&self) -> Result<usize> {
        let high_water = self.next_block_value()?;
        let snapshot = self.count_snapshot(high_water)?;
        Ok(snapshot
            .chunks_exact(size_of::<u16>())
            .filter(|bytes| *bytes == [0, 0])
            .count())
    }

    pub(crate) fn seal_current(&self) -> Result<()> {
        loop {
            let encoded = read_shared_u64(&self.current_block);
            if encoded == CURRENT_INSTALLING {
                std::hint::spin_loop();
                continue;
            }
            if encoded == 0 {
                return Ok(());
            }
            return self.seal_current_block(encoded, false);
        }
    }

    pub(crate) fn write(&self, allocation: Allocation, input: &[u8]) -> Result<()> {
        self.write_at(allocation, 0, input)
    }

    pub(crate) fn write_at(
        &self,
        allocation: Allocation,
        relative_offset: usize,
        input: &[u8],
    ) -> Result<()> {
        let end = relative_offset
            .checked_add(input.len())
            .ok_or(Error::Corrupt("write range overflow"))?;
        if end > allocation.length as usize {
            return Err(Error::Corrupt("write exceeds allocation"));
        }
        let offset = allocation
            .offset
            .checked_add(
                u64::try_from(relative_offset)
                    .map_err(|_| Error::Corrupt("write offset overflow"))?,
            )
            .ok_or(Error::Corrupt("write offset overflow"))?;
        // SAFETY: a fresh Allocation is exclusively owned until its body node is published.
        unsafe { self.data.copy_in(offset, input) }
    }

    pub(crate) fn read(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.validate_data_range(offset, length)?;
        self.data.copy_out(offset, length)
    }

    pub(crate) fn read_into(&self, offset: u64, output: &mut [u8]) -> Result<()> {
        self.validate_data_range(offset, output.len())?;
        self.data.copy_out_into(offset, output)
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
            return Err(Error::Corrupt("allocation crosses a page boundary"));
        }
        Ok(Allocation {
            offset,
            length,
            block,
        })
    }

    pub(crate) fn sync_all(&self) -> Result<()> {
        self.seal_current()?;
        self.data.sync_all()?;
        self.counts.sync_all()
    }

    fn install_current(&self, requested_bytes: u64) -> Result<()> {
        if self
            .current_block
            .compare_exchange(0, CURRENT_INSTALLING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let result = self.claim_page(requested_bytes);
        match result {
            Ok(block) => {
                self.current_word
                    .store(pack_current(CurrentState::Open, 0, 0)?, Ordering::Release);
                self.current_block
                    .store(encode_block(block)?, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                self.current_block.store(0, Ordering::Release);
                Err(error)
            }
        }
    }

    fn claim_page(&self, requested_bytes: u64) -> Result<u64> {
        loop {
            let high_water = self.next_block_value()?;
            if let Some(block) = self.longest_zero_run(high_water)? {
                if self
                    .count_word(block)?
                    .compare_exchange(0, u16::MAX, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Ok(block);
                }
                continue;
            }
            self.grow(requested_bytes)?;
        }
    }

    fn seal_current_block(&self, encoded: u64, continue_run: bool) -> Result<()> {
        let block = decode_block(encoded)?;
        let next = if continue_run {
            let next = block
                .checked_add(1)
                .ok_or(Error::CapacityExhausted("page index"))?;
            if next < self.next_block_value()? && self.read_count(next)? == 0 {
                Some(next)
            } else {
                None
            }
        } else {
            None
        };
        if self
            .current_block
            .compare_exchange(
                encoded,
                CURRENT_INSTALLING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Ok(());
        }
        loop {
            let old = read_shared_u64(&self.current_word);
            if current_state(old)? != CurrentState::Open {
                self.current_block.store(encoded, Ordering::Release);
                return Err(Error::Corrupt("unfinished page state is not open"));
            }
            let sealing =
                pack_current(CurrentState::Sealing, current_used(old), current_live(old))?;
            if self
                .current_word
                .compare_exchange(old, sealing, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let count = self.count_word(block)?;
            if count
                .compare_exchange(
                    u16::MAX,
                    current_live(old),
                    Ordering::Release,
                    Ordering::Acquire,
                )
                .is_err()
            {
                self.current_block.store(encoded, Ordering::Release);
                return Err(Error::Corrupt("unfinished page count marker changed"));
            }
            if let Some(next) = next {
                self.count_word(next)?.store(u16::MAX, Ordering::Release);
                self.current_word
                    .store(pack_current(CurrentState::Open, 0, 0)?, Ordering::Release);
                self.current_block
                    .store(encode_block(next)?, Ordering::Release);
            } else {
                self.current_word.store(0, Ordering::Release);
                self.current_block.store(0, Ordering::Release);
            }
            return Ok(());
        }
    }

    fn grow(&self, requested_bytes: u64) -> Result<()> {
        let high_water = self.high_water()?;
        loop {
            let old = read_shared_u64(high_water);
            if old & GROWING_BIT != 0 {
                std::hint::spin_loop();
                continue;
            }
            if old >= self.block_count {
                return Err(Error::CapacityExhausted("mapped file"));
            }
            let requested_pages = requested_bytes
                .checked_add(self.block_size - 1)
                .ok_or(Error::CapacityExhausted("allocation growth"))?
                / self.block_size;
            let pages = requested_pages.max(1).min(self.block_count - old);
            let new = old
                .checked_add(pages)
                .ok_or(Error::CapacityExhausted("page count"))?;
            if high_water
                .compare_exchange(old, old | GROWING_BIT, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            let result = (|| -> Result<()> {
                self.counts.grow(count_file_length(new)?)?;
                self.data.grow(data_file_length(new, self.block_size)?)?;
                self.data
                    .reserve(self.data_offset(old)?, pages * self.block_size)
            })();
            let published = if result.is_ok() { new } else { old };
            high_water
                .compare_exchange(
                    old | GROWING_BIT,
                    published,
                    Ordering::Release,
                    Ordering::Acquire,
                )
                .map_err(|_| Error::Corrupt("allocator growth state changed"))?;
            return result;
        }
    }

    fn longest_zero_run(&self, high_water: u64) -> Result<Option<u64>> {
        if high_water == 0 {
            return Ok(None);
        }
        let snapshot = self.count_snapshot(high_water)?;
        let entries = usize::try_from(high_water)
            .map_err(|_| Error::Corrupt("page count does not fit memory"))?;
        let mut best_start = 0_usize;
        let mut best_length = 0_usize;
        let mut run_start = 0_usize;
        let mut run_length = 0_usize;
        let mut run_claimable = true;
        let mut index = 0_usize;

        while index + 16 <= entries {
            let offset = index
                .checked_mul(size_of::<u16>())
                .ok_or(Error::Corrupt("count snapshot offset overflow"))?;
            let values = snapshot
                .get(offset..offset + 32)
                .ok_or(Error::Corrupt("count snapshot is truncated"))?;
            let zeroes = zero_mask_16(values);
            for lane in 0..16 {
                if zeroes & (1 << lane) != 0 {
                    if run_length == 0 {
                        run_start = index + lane;
                        run_claimable =
                            run_start == 0 || snapshot_count(&snapshot, run_start - 1)? != u16::MAX;
                    }
                    run_length += 1;
                    if run_claimable && run_length > best_length {
                        best_start = run_start;
                        best_length = run_length;
                    }
                } else {
                    run_length = 0;
                }
            }
            index += 16;
        }
        while index < entries {
            let offset = index
                .checked_mul(size_of::<u16>())
                .ok_or(Error::Corrupt("count snapshot offset overflow"))?;
            let bytes = snapshot
                .get(offset..offset + 2)
                .ok_or(Error::Corrupt("count snapshot is truncated"))?;
            if bytes == [0, 0] {
                if run_length == 0 {
                    run_start = index;
                    run_claimable =
                        run_start == 0 || snapshot_count(&snapshot, run_start - 1)? != u16::MAX;
                }
                run_length += 1;
                if run_claimable && run_length > best_length {
                    best_start = run_start;
                    best_length = run_length;
                }
            } else {
                run_length = 0;
            }
            index += 1;
        }
        if best_length == 0 {
            Ok(None)
        } else {
            u64::try_from(best_start)
                .map(Some)
                .map_err(|_| Error::Corrupt("free-page index overflow"))
        }
    }

    fn count_snapshot(&self, high_water: u64) -> Result<Vec<u8>> {
        let bytes = high_water
            .checked_mul(size_of::<u16>() as u64)
            .ok_or(Error::Corrupt("count snapshot length overflow"))?;
        let length = usize::try_from(bytes)
            .map_err(|_| Error::Corrupt("count snapshot length does not fit memory"))?;
        // This intentionally uses ordinary mapped reads so the zero comparison can be
        // vectorized. The winning zero is always revalidated by compare-exchange.
        self.counts.copy_out(COUNTS_START, length)
    }

    fn validate_header(&self) -> Result<()> {
        let high_water = self.closed_block_value()?;
        if high_water > self.block_count {
            return Err(Error::Corrupt("allocator high-water mark is out of range"));
        }
        if self.counts.file_length() < count_file_length(high_water)? {
            return Err(Error::Corrupt(
                "page-count file is shorter than its high-water mark",
            ));
        }
        if self.data.file_length() < data_file_length(high_water, self.block_size)? {
            return Err(Error::Corrupt(
                "data file is shorter than its high-water mark",
            ));
        }
        let snapshot = self.count_snapshot(high_water)?;
        if snapshot
            .chunks_exact(size_of::<u16>())
            .any(|bytes| bytes == u16::MAX.to_le_bytes())
        {
            return Err(Error::Corrupt(
                "allocator was closed with an unfinished page",
            ));
        }
        Ok(())
    }

    /// Reads the live high-water mark, waiting for the thread growing the mapped files to publish.
    fn next_block_value(&self) -> Result<u64> {
        let high_water = self.high_water()?;
        loop {
            let value = read_shared_u64(high_water);
            if value & GROWING_BIT == 0 {
                return Ok(value);
            }
            std::hint::spin_loop();
        }
    }

    /// Reads a persisted high-water mark when no growth operation may still be active.
    fn closed_block_value(&self) -> Result<u64> {
        let value = read_shared_u64(self.high_water()?);
        if value & GROWING_BIT != 0 {
            return Err(Error::Corrupt("allocator was closed while growing"));
        }
        Ok(value)
    }

    fn high_water(&self) -> Result<&AtomicU64> {
        self.counts.atomic_u64(HIGH_WATER_OFFSET)
    }

    fn count_word(&self, block: u64) -> Result<&AtomicU16> {
        if block >= self.block_count {
            return Err(Error::Corrupt("page index is out of range"));
        }
        let offset = block
            .checked_mul(size_of::<u16>() as u64)
            .and_then(|value| value.checked_add(COUNTS_START))
            .ok_or(Error::Corrupt("page-count offset overflow"))?;
        self.counts.atomic_u16(offset)
    }

    fn data_offset(&self, block: u64) -> Result<u64> {
        DATA_START
            .checked_add(
                block
                    .checked_mul(self.block_size)
                    .ok_or(Error::CapacityExhausted("page offset"))?,
            )
            .ok_or(Error::CapacityExhausted("page offset"))
    }

    fn read_count(&self, block: u64) -> Result<u16> {
        if block >= self.block_count {
            return Err(Error::Corrupt("page index is out of range"));
        }
        let offset = block
            .checked_mul(size_of::<u16>() as u64)
            .and_then(|value| value.checked_add(COUNTS_START))
            .ok_or(Error::Corrupt("page-count offset overflow"))?;
        let bytes = self.counts.copy_out(offset, size_of::<u16>())?;
        let array = <[u8; 2]>::try_from(bytes.as_slice())
            .map_err(|_| Error::Corrupt("page-count entry is truncated"))?;
        Ok(u16::from_le_bytes(array))
    }

    fn validate_allocation(&self, allocation: Allocation) -> Result<()> {
        let expected = self.allocation_for(allocation.offset, allocation.length)?;
        if expected.block != allocation.block {
            return Err(Error::Corrupt("allocation page does not match its offset"));
        }
        Ok(())
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
    if capacity == 0 || block_size < FORMAT_PAGE_SIZE || !block_size.is_power_of_two() {
        return Err(Error::InvalidConfig("invalid page allocator layout"));
    }
    if capacity % block_size != 0 || block_size > u64::from(u32::MAX) {
        return Err(Error::InvalidConfig(
            "allocator capacity or page size is not representable",
        ));
    }
    let block_count = capacity / block_size;
    if block_count == 0 || block_count > u64::from(u32::MAX) {
        return Err(Error::InvalidConfig(
            "allocator page count is not representable",
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
    let mut bytes = [0_u8; 8];
    counts.read_exact(&mut bytes)?;
    let high_water = u64::from_le_bytes(bytes);
    if high_water & GROWING_BIT != 0 {
        return Err(Error::Corrupt("allocator was closed while growing"));
    }
    Ok(high_water)
}

pub(crate) fn count_file_length(blocks: u64) -> Result<u64> {
    blocks
        .checked_mul(size_of::<u16>() as u64)
        .and_then(|bytes| bytes.checked_add(COUNTS_START))
        .and_then(|bytes| align_up(bytes, FORMAT_PAGE_SIZE))
        .ok_or(Error::InvalidConfig("page-count file size overflow"))
}

fn data_file_length(blocks: u64, block_size: u64) -> Result<u64> {
    blocks
        .checked_mul(block_size)
        .and_then(|bytes| bytes.checked_add(DATA_START))
        .ok_or(Error::InvalidConfig("data file size overflow"))
}

fn pack_current(state: CurrentState, used: u64, live: u16) -> Result<u64> {
    let used = u32::try_from(used).map_err(|_| Error::CapacityExhausted("page offset"))?;
    Ok((state as u64) << STATE_SHIFT | u64::from(live) << LIVE_SHIFT | u64::from(used))
}

const fn current_used(word: u64) -> u64 {
    word & USED_MASK
}

const fn current_live(word: u64) -> u16 {
    ((word & LIVE_MASK) >> LIVE_SHIFT) as u16
}

fn current_state(word: u64) -> Result<CurrentState> {
    match (word >> STATE_SHIFT) as u8 {
        0 => Ok(CurrentState::Empty),
        1 => Ok(CurrentState::Open),
        2 => Ok(CurrentState::Sealing),
        _ => Err(Error::Corrupt("unknown unfinished-page state")),
    }
}

fn encode_block(block: u64) -> Result<u64> {
    block
        .checked_add(1)
        .ok_or(Error::CapacityExhausted("page index"))
}

fn read_shared_u64(atomic: &AtomicU64) -> u64 {
    // SAFETY: supported targets provide aligned single-copy 64-bit reads. A following acquire
    // fence orders data published before the CAS that installed the observed state.
    let value = unsafe { std::ptr::read_volatile(atomic.as_ptr()) };
    std::sync::atomic::fence(Ordering::Acquire);
    value
}
fn decode_block(encoded: u64) -> Result<u64> {
    encoded
        .checked_sub(1)
        .ok_or(Error::Corrupt("encoded page index is null"))
}

fn snapshot_count(snapshot: &[u8], index: usize) -> Result<u16> {
    let offset = index
        .checked_mul(size_of::<u16>())
        .ok_or(Error::Corrupt("count snapshot offset overflow"))?;
    let bytes = snapshot
        .get(offset..offset + size_of::<u16>())
        .ok_or(Error::Corrupt("count snapshot is truncated"))?;
    let array =
        <[u8; 2]>::try_from(bytes).map_err(|_| Error::Corrupt("page-count entry is truncated"))?;
    Ok(u16::from_le_bytes(array))
}

fn zero_mask_16(bytes: &[u8]) -> u32 {
    debug_assert_eq!(bytes.len(), 32);
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 was detected and the caller supplies 32 readable bytes.
            return unsafe { zero_mask_16_avx2(bytes.as_ptr()) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: AArch64 guarantees NEON and the caller supplies 32 readable bytes.
        return unsafe { zero_mask_16_neon(bytes.as_ptr()) };
    }
    let mut mask = 0_u32;
    for (lane, value) in bytes.chunks_exact(2).enumerate() {
        if value == [0, 0] {
            mask |= 1 << lane;
        }
    }
    mask
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(clippy::cast_ptr_alignment)]
unsafe fn zero_mask_16_avx2(pointer: *const u8) -> u32 {
    use std::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi16, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_setzero_si256,
    };

    // SAFETY: guaranteed by the caller; the unaligned load reads exactly 32 bytes.
    let values = unsafe { _mm256_loadu_si256(pointer.cast::<__m256i>()) };
    let compared = _mm256_cmpeq_epi16(values, _mm256_setzero_si256());
    let byte_mask = u32::from_ne_bytes(_mm256_movemask_epi8(compared).to_ne_bytes());
    let mut lane_mask = 0_u32;
    for lane in 0..16 {
        if byte_mask & (3 << (lane * 2)) == 3 << (lane * 2) {
            lane_mask |= 1 << lane;
        }
    }
    lane_mask
}

#[cfg(target_arch = "aarch64")]
unsafe fn zero_mask_16_neon(pointer: *const u8) -> u32 {
    use std::arch::aarch64::{vceqq_u16, vdupq_n_u16, vld1q_u16, vst1q_u16};

    let mut compared = [0_u16; 8];
    let mut mask = 0_u32;
    for half in 0..2 {
        // SAFETY: guaranteed by the caller; each load reads one 16-byte half.
        let values = unsafe { vld1q_u16(pointer.add(half * 16).cast::<u16>()) };
        let zeroes = vceqq_u16(values, vdupq_n_u16(0));
        // SAFETY: `compared` has space for all eight lanes.
        unsafe { vst1q_u16(compared.as_mut_ptr(), zeroes) };
        for (lane, value) in compared.iter().enumerate() {
            if *value == u16::MAX {
                mask |= 1 << (half * 8 + lane);
            }
        }
    }
    mask
}

#[cfg(all(test, not(miri), any(target_os = "linux", target_os = "macos")))]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn test_paths(name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let prefix = format!("floresta-db-{}-{name}", std::process::id());
        let directory = std::env::temp_dir();
        (
            directory.join(format!("{prefix}.data")),
            directory.join(format!("{prefix}.counts")),
        )
    }

    fn cleanup(paths: &(std::path::PathBuf, std::path::PathBuf)) {
        let _ignored_data = std::fs::remove_file(&paths.0);
        let _ignored_counts = std::fs::remove_file(&paths.1);
    }

    #[test]
    fn reserves_largest_fitting_batch_and_reuses_zero_page() -> Result<()> {
        let paths = test_paths("allocator-batch");
        cleanup(&paths);
        let allocator = BlockAllocator::create(&paths.0, &paths.1, 64 * 1_024 * 2, 64 * 1_024)?;
        let batch = allocator.allocate_batch(32, 32, 3_000)?;
        assert_eq!(batch.len(), 2_048);
        let allocations = batch.allocations().collect::<Vec<_>>();
        for allocation in allocations {
            allocator.release(allocation)?;
        }
        allocator.seal_current()?;
        let reused = allocator.allocate(32, 32)?;
        assert_eq!(reused.block, 0);
        allocator.release(reused)?;
        allocator.sync_all()?;
        drop(allocator);
        cleanup(&paths);
        Ok(())
    }

    #[test]
    fn live_high_water_reader_waits_for_growth_publication() -> Result<()> {
        let paths = test_paths("allocator-live-growth");
        cleanup(&paths);
        let allocator = Arc::new(BlockAllocator::create(
            &paths.0,
            &paths.1,
            64 * 1_024 * 2,
            64 * 1_024,
        )?);
        allocator
            .high_water()?
            .store(GROWING_BIT, Ordering::Release);

        let publisher = Arc::clone(&allocator);
        let thread = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            publisher.high_water().unwrap().store(0, Ordering::Release);
        });
        assert_eq!(allocator.next_block_value()?, 0);
        thread.join().unwrap();

        allocator.sync_all()?;
        drop(allocator);
        cleanup(&paths);
        Ok(())
    }

    #[test]
    fn grows_by_requested_batch_extent() -> Result<()> {
        let paths = test_paths("allocator-growth");
        cleanup(&paths);
        let allocator = BlockAllocator::create(&paths.0, &paths.1, 64 * 1_024 * 4, 64 * 1_024)?;
        let allocation = allocator.allocate_batch(32, 32, 4_096)?;
        assert_eq!(allocation.len(), 2_048);
        assert_eq!(read_high_water(&paths.1)?, 2);
        allocator.sync_all()?;
        drop(allocator);
        cleanup(&paths);
        Ok(())
    }

    #[test]
    fn consumes_contiguous_zero_run_after_first_cas() -> Result<()> {
        let paths = test_paths("allocator-contiguous-run");
        cleanup(&paths);
        let allocator = BlockAllocator::create(&paths.0, &paths.1, 64 * 1_024 * 4, 64 * 1_024)?;

        let first = allocator.allocate_batch(32, 32, 6_144)?;
        assert_eq!(first.block, 0);
        assert_eq!(allocator.read_count(0)?, u16::MAX);
        assert_eq!(allocator.read_count(1)?, 0);
        assert_eq!(allocator.read_count(2)?, 0);

        let second = allocator.allocate_batch(32, 32, 6_144)?;
        assert_eq!(second.block, 1);
        assert_eq!(allocator.read_count(0)?, 2_048);
        assert_eq!(allocator.read_count(1)?, u16::MAX);
        assert_eq!(allocator.read_count(2)?, 0);

        let third = allocator.allocate_batch(32, 32, 6_144)?;
        assert_eq!(third.block, 2);
        assert_eq!(allocator.read_count(1)?, 2_048);
        assert_eq!(allocator.read_count(2)?, u16::MAX);
        assert_eq!(read_high_water(&paths.1)?, 3);

        allocator.sync_all()?;
        drop(allocator);
        cleanup(&paths);
        Ok(())
    }

    #[test]
    fn skips_zero_tail_after_claimed_page() -> Result<()> {
        let paths = test_paths("allocator-claimed-predecessor");
        cleanup(&paths);
        let allocator = BlockAllocator::create(&paths.0, &paths.1, 64 * 1_024 * 6, 64 * 1_024)?;

        let first = allocator.allocate_batch(32, 32, 12_288)?;
        assert_eq!(first.block, 0);
        assert_eq!(read_high_water(&paths.1)?, 6);
        allocator.count_word(3)?.store(1, Ordering::Release);
        assert_eq!(allocator.longest_zero_run(6)?, Some(4));

        drop(allocator);
        cleanup(&paths);
        Ok(())
    }

    #[test]
    fn clean_reopen_preserves_sixteen_bit_counts() -> Result<()> {
        let paths = test_paths("allocator-reopen");
        cleanup(&paths);
        let allocation;
        {
            let allocator = BlockAllocator::create(&paths.0, &paths.1, 64 * 1_024 * 2, 64 * 1_024)?;
            allocation = allocator.allocate(64, 8)?;
            allocator.sync_all()?;
        }
        {
            let allocator = BlockAllocator::open(&paths.0, &paths.1, 64 * 1_024 * 2, 64 * 1_024)?;
            allocator.release(allocation)?;
            let reused = allocator.allocate(64, 8)?;
            assert_eq!(reused.block, allocation.block);
            allocator.release(reused)?;
            allocator.sync_all()?;
        }
        cleanup(&paths);
        Ok(())
    }

    #[test]
    fn concurrent_reservations_do_not_overlap() -> Result<()> {
        let paths = test_paths("allocator-concurrent");
        cleanup(&paths);
        let allocator = Arc::new(BlockAllocator::create(
            &paths.0,
            &paths.1,
            64 * 1_024 * 8,
            64 * 1_024,
        )?);
        let mut offsets = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _thread in 0..8 {
                let allocator = Arc::clone(&allocator);
                handles.push(scope.spawn(move || -> Result<Vec<u64>> {
                    let mut offsets = Vec::new();
                    for _allocation in 0..256 {
                        offsets.push(allocator.allocate(32, 32)?.offset);
                    }
                    Ok(offsets)
                }));
            }
            let mut offsets = Vec::new();
            for handle in handles {
                offsets.extend(
                    handle
                        .join()
                        .map_err(|_| Error::Corrupt("allocator test thread panicked"))??,
                );
            }
            Ok::<Vec<u64>, Error>(offsets)
        })?;
        offsets.sort_unstable();
        offsets.dedup();
        assert_eq!(offsets.len(), 8 * 256);
        allocator.sync_all()?;
        drop(allocator);
        cleanup(&paths);
        Ok(())
    }

    #[test]
    fn simd_zero_mask_finds_zero_lanes() {
        let mut values = [1_u16; 16];
        values[0] = 0;
        values[7] = 0;
        values[15] = 0;
        let bytes = unsafe { std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), 32) };
        assert_eq!(zero_mask_16(bytes), 1 | 1 << 7 | 1 << 15);
    }
}
