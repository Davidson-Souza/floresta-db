#![allow(dead_code)]

use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use db_experiment::xxh64;

const EMPTY: u64 = 0;
const WRITING: u64 = 1;
const READY: u64 = 2;
const READING: u64 = 3;
const DONE: u64 = 4;
const INVALID_HEIGHT: u64 = u64::MAX;
const RECORD_MAGIC: u64 = 0x424c_4f43_4b52_494e;
const RECORD_HEADER_SIZE: u64 = 32;
const RECORD_HEADER_LEN: usize = 32;

struct Slot {
    state: AtomicU64,
    height: AtomicU64,
    length: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FetchLease {
    pub(crate) height: u64,
    index: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConsumeLease {
    pub(crate) height: u64,
    index: usize,
}

pub(crate) struct BlockRing {
    file: File,
    slots: Box<[Slot]>,
    capacity: u64,
    slot_bytes: u64,
    end_height: u64,
    fetch_claim: AtomicU64,
    produced_next: AtomicU64,
    consume_claim: AtomicU64,
    consumed_next: AtomicU64,
    aborted: AtomicU64,
}

impl BlockRing {
    pub(crate) fn create(
        path: &Path,
        start_height: u64,
        end_height: u64,
        capacity: u64,
        slot_bytes: u64,
    ) -> io::Result<Self> {
        if start_height >= end_height {
            return Err(invalid_input("ring height range must be nonempty"));
        }
        if capacity == 0 {
            return Err(invalid_input("ring capacity must be nonzero"));
        }
        if slot_bytes <= RECORD_HEADER_SIZE {
            return Err(invalid_input("ring slot cannot hold its record header"));
        }
        let file_length = capacity
            .checked_mul(slot_bytes)
            .ok_or_else(|| invalid_input("ring file length overflow"))?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        file.set_len(file_length)?;

        let capacity_usize = usize::try_from(capacity)
            .map_err(|_| invalid_input("ring capacity does not fit memory"))?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity_usize)
            .map_err(|_| io::Error::other("ring slot allocation failed"))?;
        for _index in 0..capacity_usize {
            slots.push(Slot {
                state: AtomicU64::new(EMPTY),
                height: AtomicU64::new(INVALID_HEIGHT),
                length: AtomicU64::new(0),
            });
        }
        Ok(Self {
            file,
            slots: slots.into_boxed_slice(),
            capacity,
            slot_bytes,
            end_height,
            fetch_claim: AtomicU64::new(start_height),
            produced_next: AtomicU64::new(start_height),
            consume_claim: AtomicU64::new(start_height),
            consumed_next: AtomicU64::new(start_height),
            aborted: AtomicU64::new(0),
        })
    }

    pub(crate) fn claim_fetch(&self) -> io::Result<Option<FetchLease>> {
        loop {
            self.check_abort()?;
            let height = self.fetch_claim.load(Ordering::Acquire);
            if height >= self.end_height {
                return Ok(None);
            }
            let limit = self
                .consumed_next
                .load(Ordering::Acquire)
                .checked_add(self.capacity)
                .ok_or_else(|| invalid_data("producer window overflow"))?;
            if height >= limit {
                wait_for_progress();
                continue;
            }
            if self
                .fetch_claim
                .compare_exchange(height, height + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let index = self.slot_index(height)?;
            let slot = self.slot(index)?;
            while slot
                .state
                .compare_exchange(EMPTY, WRITING, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                self.check_abort()?;
                wait_for_progress();
            }
            slot.height
                .compare_exchange(INVALID_HEIGHT, height, Ordering::Release, Ordering::Acquire)
                .map_err(|_| invalid_data("empty ring slot retained an old height"))?;
            return Ok(Some(FetchLease { height, index }));
        }
    }

    pub(crate) fn write_fetch(&self, lease: FetchLease, block: &[u8]) -> io::Result<()> {
        let block_length = u64::try_from(block.len())
            .map_err(|_| invalid_input("serialized block length overflow"))?;
        if block_length > self.slot_bytes - RECORD_HEADER_SIZE {
            return Err(invalid_input("serialized block exceeds ring slot size"));
        }
        let slot = self.slot(lease.index)?;
        if slot.state.load(Ordering::Acquire) != WRITING
            || slot.height.load(Ordering::Acquire) != lease.height
        {
            return Err(invalid_data("fetch lease does not own its ring slot"));
        }
        let mut header = [0_u8; RECORD_HEADER_LEN];
        write_header_word(&mut header, 0, RECORD_MAGIC)?;
        write_header_word(&mut header, 8, lease.height)?;
        write_header_word(&mut header, 16, block_length)?;
        write_header_word(&mut header, 24, xxh64(block, lease.height))?;
        let offset = self.slot_offset(lease.index)?;
        write_all_at(&self.file, &header, offset)?;
        write_all_at(&self.file, block, offset + RECORD_HEADER_SIZE)?;
        Ok(())
    }

    pub(crate) fn publish_fetch(&self, lease: FetchLease, block_length: usize) -> io::Result<()> {
        let length = u64::try_from(block_length)
            .map_err(|_| invalid_input("serialized block length overflow"))?;
        let slot = self.slot(lease.index)?;
        slot.length
            .compare_exchange(0, length, Ordering::Release, Ordering::Acquire)
            .map_err(|_| invalid_data("ring slot length was already published"))?;
        slot.state
            .compare_exchange(WRITING, READY, Ordering::Release, Ordering::Acquire)
            .map_err(|_| invalid_data("ring slot left the writing state"))?;
        self.advance_produced()
    }

    pub(crate) fn claim_consume(&self) -> io::Result<Option<ConsumeLease>> {
        loop {
            self.check_abort()?;
            let height = self.consume_claim.load(Ordering::Acquire);
            if height >= self.end_height {
                return Ok(None);
            }
            if height >= self.produced_next.load(Ordering::Acquire) {
                wait_for_progress();
                continue;
            }
            if self
                .consume_claim
                .compare_exchange(height, height + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let index = self.slot_index(height)?;
            let slot = self.slot(index)?;
            if slot.height.load(Ordering::Acquire) != height {
                return Err(invalid_data("published ring slot has the wrong height"));
            }
            slot.state
                .compare_exchange(READY, READING, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| invalid_data("published ring slot was not ready"))?;
            return Ok(Some(ConsumeLease { height, index }));
        }
    }

    pub(crate) fn read_consume(&self, lease: ConsumeLease) -> io::Result<Vec<u8>> {
        let slot = self.slot(lease.index)?;
        if slot.state.load(Ordering::Acquire) != READING
            || slot.height.load(Ordering::Acquire) != lease.height
        {
            return Err(invalid_data("consume lease does not own its ring slot"));
        }
        let offset = self.slot_offset(lease.index)?;
        let mut header = [0_u8; RECORD_HEADER_LEN];
        read_exact_at(&self.file, &mut header, offset)?;
        if read_header_word(&header, 0)? != RECORD_MAGIC
            || read_header_word(&header, 8)? != lease.height
        {
            return Err(invalid_data("ring record header is invalid"));
        }
        let length = read_header_word(&header, 16)?;
        if length != slot.length.load(Ordering::Acquire)
            || length > self.slot_bytes - RECORD_HEADER_SIZE
        {
            return Err(invalid_data("ring record length is invalid"));
        }
        let length_usize = usize::try_from(length)
            .map_err(|_| invalid_data("ring record length does not fit memory"))?;
        let mut block = Vec::new();
        block
            .try_reserve_exact(length_usize)
            .map_err(|_| io::Error::other("block allocation failed"))?;
        block.resize(length_usize, 0);
        read_exact_at(&self.file, &mut block, offset + RECORD_HEADER_SIZE)?;
        if read_header_word(&header, 24)? != xxh64(&block, lease.height) {
            return Err(invalid_data("ring record checksum does not match"));
        }
        Ok(block)
    }

    pub(crate) fn finish_consume(&self, lease: ConsumeLease) -> io::Result<()> {
        let slot = self.slot(lease.index)?;
        slot.state
            .compare_exchange(READING, DONE, Ordering::Release, Ordering::Acquire)
            .map_err(|_| invalid_data("ring slot left the reading state"))?;
        self.advance_consumed()
    }

    #[allow(dead_code)]
    pub(crate) fn abort(&self) {
        let _aborted = self
            .aborted
            .compare_exchange(0, 1, Ordering::Release, Ordering::Acquire);
    }

    pub(crate) fn consumed_height(&self) -> u64 {
        self.consumed_next.load(Ordering::Acquire)
    }

    fn advance_produced(&self) -> io::Result<()> {
        loop {
            let height = self.produced_next.load(Ordering::Acquire);
            if height >= self.end_height {
                return Ok(());
            }
            let slot = self.slot(self.slot_index(height)?)?;
            if slot.height.load(Ordering::Acquire) != height
                || !matches!(slot.state.load(Ordering::Acquire), READY | READING | DONE)
            {
                return Ok(());
            }
            let _advanced = self.produced_next.compare_exchange(
                height,
                height + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    fn advance_consumed(&self) -> io::Result<()> {
        loop {
            let height = self.consumed_next.load(Ordering::Acquire);
            if height >= self.end_height {
                return Ok(());
            }
            let slot = self.slot(self.slot_index(height)?)?;
            if slot.height.load(Ordering::Acquire) != height
                || slot.state.load(Ordering::Acquire) != DONE
            {
                return Ok(());
            }
            if self
                .consumed_next
                .compare_exchange(height, height + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let length = slot.length.load(Ordering::Acquire);
            slot.length
                .compare_exchange(length, 0, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| invalid_data("consumed ring length changed"))?;
            slot.height
                .compare_exchange(height, INVALID_HEIGHT, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| invalid_data("consumed ring height changed"))?;
            slot.state
                .compare_exchange(DONE, EMPTY, Ordering::Release, Ordering::Acquire)
                .map_err(|_| invalid_data("consumed ring state changed"))?;
        }
    }

    fn slot(&self, index: usize) -> io::Result<&Slot> {
        self.slots
            .get(index)
            .ok_or_else(|| invalid_data("ring slot index is out of range"))
    }

    fn slot_index(&self, height: u64) -> io::Result<usize> {
        usize::try_from(height % self.capacity)
            .map_err(|_| invalid_data("ring slot index does not fit memory"))
    }

    fn slot_offset(&self, index: usize) -> io::Result<u64> {
        u64::try_from(index)
            .map_err(|_| invalid_data("ring slot offset does not fit u64"))?
            .checked_mul(self.slot_bytes)
            .ok_or_else(|| invalid_data("ring slot offset overflow"))
    }

    fn check_abort(&self) -> io::Result<()> {
        if self.aborted.load(Ordering::Acquire) == 0 {
            Ok(())
        } else {
            Err(io::Error::new(
                ErrorKind::Interrupted,
                "ring processing aborted",
            ))
        }
    }
}

fn write_header_word(header: &mut [u8], offset: usize, value: u64) -> io::Result<()> {
    let end = offset
        .checked_add(size_of::<u64>())
        .ok_or_else(|| invalid_data("ring header offset overflow"))?;
    header
        .get_mut(offset..end)
        .ok_or_else(|| invalid_data("ring header offset is out of range"))?
        .copy_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_header_word(header: &[u8], offset: usize) -> io::Result<u64> {
    let end = offset
        .checked_add(size_of::<u64>())
        .ok_or_else(|| invalid_data("ring header offset overflow"))?;
    let bytes = header
        .get(offset..end)
        .ok_or_else(|| invalid_data("ring header offset is out of range"))?;
    let array = <[u8; 8]>::try_from(bytes)
        .map_err(|_| invalid_data("ring header word has the wrong size"))?;
    Ok(u64::from_le_bytes(array))
}

fn write_all_at(file: &File, mut bytes: &[u8], mut offset: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        let written = file.write_at(bytes, offset)?;
        if written == 0 {
            return Err(io::Error::new(
                ErrorKind::WriteZero,
                "ring write made no progress",
            ));
        }
        bytes = bytes
            .get(written..)
            .ok_or_else(|| invalid_data("ring write count exceeded input"))?;
        offset = offset
            .checked_add(
                u64::try_from(written)
                    .map_err(|_| invalid_data("ring write count does not fit u64"))?,
            )
            .ok_or_else(|| invalid_data("ring write offset overflow"))?;
    }
    Ok(())
}

fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !bytes.is_empty() {
        let read = file.read_at(bytes, offset)?;
        if read == 0 {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "ring read reached end of file",
            ));
        }
        bytes = bytes
            .get_mut(read..)
            .ok_or_else(|| invalid_data("ring read count exceeded output"))?;
        offset = offset
            .checked_add(
                u64::try_from(read)
                    .map_err(|_| invalid_data("ring read count does not fit u64"))?,
            )
            .ok_or_else(|| invalid_data("ring read offset overflow"))?;
    }
    Ok(())
}

fn wait_for_progress() {
    for _iteration in 0..64 {
        std::hint::spin_loop();
    }
    std::thread::yield_now();
    std::thread::sleep(Duration::from_micros(50));
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("db-experiment-ring-{}-{name}", std::process::id()))
    }

    #[test]
    fn advances_only_contiguous_frontiers_and_reuses_slots() -> io::Result<()> {
        let path = test_path("frontiers");
        let _ignored = std::fs::remove_file(&path);
        let ring = BlockRing::create(&path, 0, 4, 2, 128)?;

        let fetch_zero = ring
            .claim_fetch()?
            .ok_or_else(|| invalid_data("missing fetch lease zero"))?;
        let fetch_one = ring
            .claim_fetch()?
            .ok_or_else(|| invalid_data("missing fetch lease one"))?;
        ring.write_fetch(fetch_one, b"one")?;
        ring.publish_fetch(fetch_one, 3)?;
        assert_eq!(ring.produced_next.load(Ordering::Acquire), 0);
        ring.write_fetch(fetch_zero, b"zero")?;
        ring.publish_fetch(fetch_zero, 4)?;
        assert_eq!(ring.produced_next.load(Ordering::Acquire), 2);

        let consume_zero = ring
            .claim_consume()?
            .ok_or_else(|| invalid_data("missing consume lease zero"))?;
        let consume_one = ring
            .claim_consume()?
            .ok_or_else(|| invalid_data("missing consume lease one"))?;
        assert_eq!(ring.read_consume(consume_zero)?, b"zero");
        assert_eq!(ring.read_consume(consume_one)?, b"one");
        ring.finish_consume(consume_one)?;
        assert_eq!(ring.consumed_height(), 0);
        ring.finish_consume(consume_zero)?;
        assert_eq!(ring.consumed_height(), 2);

        let fetch_two = ring
            .claim_fetch()?
            .ok_or_else(|| invalid_data("missing wrapped fetch lease"))?;
        assert_eq!(fetch_two.height, 2);
        drop(ring);
        std::fs::remove_file(path)?;
        Ok(())
    }
}
