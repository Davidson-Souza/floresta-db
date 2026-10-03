// SPDX-License-Identifier: MIT OR Apache-2.0

//! Destructive extraction, sorting, and hintsfile encoding for live database values.
//!
//! The indexing database stores each eligible output position as an inline `u64` value. After
//! indexing has stopped and the database mapping has closed, [`HintsWriter`] scans live nodes from
//! the body file in physical order, validates their checksums, externally sorts their positions,
//! and replaces the body file with the sorted value stream. The replacement is intentional: the
//! index directory is scratch space after hints generation and is not a reopenable database.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use floresta_db::Config;
use hintsfile::{EliasFano, HintsfileBuilder};
use memmap2::MmapOptions;

use super::{AnyResult, OUTPOINT_KEY_SIZE, invalid_input, log_progress};

const BODY_DATA_START: usize = 65_536;
const BODY_NODE_SIZE: usize = 32;
const BODY_KEY_OFFSET: usize = 0;
const BODY_VALUE_OFFSET: usize = 16;
const BODY_POINTER_OFFSET: usize = 24;
const BODY_CHECKSUM_SHIFT: u32 = 48;
const BODY_VALUE_BLOB_TAG: u64 = 1 << 63;
const BODY_NODE_CHECKSUM_SEED: u64 = 0x4341_534e_4f44_4532;
const HINTS_PROGRESS_INTERVAL: u64 = 10_000;
const SORT_PROGRESS_INTERVAL: u64 = 10_000_000;
#[cfg(not(test))]
const SORT_RUN_VALUE_CAPACITY: usize = 1 << 20;
#[cfg(test)]
const SORT_RUN_VALUE_CAPACITY: usize = 4;

/// A height-local eligible-output index packed into the database's inline value word.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct OutputPosition(u64);

impl OutputPosition {
    /// Packs a block height and its eligible-output index in lexicographic sort order.
    pub(super) fn new(block_height: u32, block_index: u32) -> Self {
        Self(u64::from(block_height) << 32 | u64::from(block_index))
    }

    /// Returns the little-endian database representation.
    pub(super) fn to_le_bytes(self) -> [u8; size_of::<u64>()] {
        self.0.to_le_bytes()
    }

    fn from_word(word: u64) -> Self {
        Self(word)
    }

    fn components(self) -> (u32, u32) {
        let [
            index_0,
            index_1,
            index_2,
            index_3,
            height_0,
            height_1,
            height_2,
            height_3,
        ] = self.0.to_le_bytes();
        (
            u32::from_le_bytes([height_0, height_1, height_2, height_3]),
            u32::from_le_bytes([index_0, index_1, index_2, index_3]),
        )
    }
}

/// Counts and output size produced by one completed hintsfile build.
#[derive(Clone, Copy)]
pub(super) struct HintsStats {
    pub(super) eligible_outputs: u64,
    pub(super) unspent_outputs: u64,
    pub(super) file_bytes: u64,
}

/// Owns the destructive post-index body sort and hintsfile encoding sequence.
pub(super) struct HintsWriter<'path, 'counts> {
    index_path: &'path Path,
    config: &'counts Config,
    sort_workers: usize,
    tip_height: u64,
    counts: &'counts [u32],
    offsets: &'counts [u64],
    output_path: &'path Path,
}

impl<'path, 'counts> HintsWriter<'path, 'counts> {
    /// Captures every input required after the live database has been closed.
    pub(super) fn new(
        index_path: &'path Path,
        config: &'counts Config,
        sort_workers: usize,
        tip_height: u64,
        counts: &'counts [u32],
        offsets: &'counts [u64],
        output_path: &'path Path,
    ) -> Self {
        Self {
            index_path,
            config,
            sort_workers,
            tip_height,
            counts,
            offsets,
            output_path,
        }
    }

    /// Sorts live body values and encodes the final Swift Sync hintsfile.
    pub(super) fn write(self) -> AnyResult<HintsStats> {
        let eligible_outputs = self
            .offsets
            .last()
            .copied()
            .ok_or_else(|| invalid_input("block offsets are empty"))?;
        let body_path = self.index_path.join("body");
        log_progress(format_args!(
            "stage=hints event=sort_start workers={} eligible_outputs={eligible_outputs} body={}",
            self.sort_workers,
            body_path.display()
        ));
        let unspent_outputs =
            BodySorter::new(&body_path, self.config.block_size, self.sort_workers).sort()?;
        log_progress(format_args!(
            "stage=hints event=sort_complete unspent_outputs={unspent_outputs} body_bytes={}",
            std::fs::metadata(&body_path)?.len()
        ));

        log_progress(format_args!(
            "stage=hints event=encode_start path={}",
            self.output_path.display()
        ));
        let encoded_outputs = self.encode_sorted(&body_path)?;
        if encoded_outputs != unspent_outputs {
            return Err(io::Error::other(format!(
                "sorted output count mismatch: sort={unspent_outputs}, encoded={encoded_outputs}"
            ))
            .into());
        }
        log_progress(format_args!(
            "stage=hints event=complete path={} bytes={}",
            self.output_path.display(),
            std::fs::metadata(self.output_path)?.len()
        ));

        Ok(HintsStats {
            eligible_outputs,
            unspent_outputs,
            file_bytes: std::fs::metadata(self.output_path)?.len(),
        })
    }

    fn encode_sorted(&self, sorted_body_path: &Path) -> AnyResult<u64> {
        let stop_height = u32::try_from(self.tip_height)
            .map_err(|_| invalid_input("hintsfile height exceeds u32"))?;
        let mut values = ValueReader::open_flat(sorted_body_path)?;
        let mut next = values.next_value()?.map(OutputPosition::from_word);
        let writer = BufWriter::new(File::create(self.output_path)?);
        let builder = HintsfileBuilder::new(writer);
        let mut builder = builder.initialize(stop_height)?;
        let mut indices = Vec::new();
        let mut encoded_outputs = 0_u64;

        // hintsfile 0.1 encodes heights 1..=stop_height; genesis has no eligible outputs.
        for height in 1..=stop_height {
            if height == 1
                || height == stop_height
                || u64::from(height) % HINTS_PROGRESS_INTERVAL == 0
            {
                log_progress(format_args!(
                    "stage=hints event=encode_progress height={height} tip={stop_height}"
                ));
            }
            indices.clear();
            let height_index = usize::try_from(height)
                .map_err(|_| invalid_input("hintsfile height does not fit usize"))?;
            let count = self
                .counts
                .get(height_index)
                .copied()
                .ok_or_else(|| invalid_input("eligible output count is unavailable"))?;
            let block_offset = self
                .offsets
                .get(height_index)
                .copied()
                .ok_or_else(|| invalid_input("block offset is unavailable"))?;
            let block_end = self
                .offsets
                .get(height_index + 1)
                .copied()
                .ok_or_else(|| invalid_input("next block offset is unavailable"))?;

            while let Some(position) = next {
                let (position_height, block_index) = position.components();
                if position_height < height {
                    return Err(io::Error::other("sorted output positions moved backwards").into());
                }
                if position_height != height {
                    break;
                }
                if block_index >= count {
                    return Err(io::Error::other(format!(
                        "output index {block_index} exceeds block {height} count {count}"
                    ))
                    .into());
                }
                if indices
                    .last()
                    .is_some_and(|previous| *previous >= block_index)
                {
                    return Err(io::Error::other("duplicate sorted output position").into());
                }
                let global = block_offset
                    .checked_add(u64::from(block_index))
                    .ok_or_else(|| invalid_input("global output index overflow"))?;
                if global >= block_end {
                    return Err(io::Error::other("output index exceeds its block range").into());
                }
                indices.push(block_index);
                encoded_outputs = encoded_outputs
                    .checked_add(1)
                    .ok_or_else(|| invalid_input("encoded output count overflow"))?;
                next = values.next_value()?.map(OutputPosition::from_word);
            }
            builder.append(EliasFano::compress(&indices))?;
        }
        if next.is_some() {
            return Err(
                io::Error::other("sorted body contains output above the requested tip").into(),
            );
        }
        builder.finish()?;
        Ok(encoded_outputs)
    }
}

/// Parallel physical-body scanner followed by an external merge sort.
struct BodySorter<'path> {
    body_path: &'path Path,
    block_size: u64,
    workers: usize,
}

impl<'path> BodySorter<'path> {
    fn new(body_path: &'path Path, block_size: u64, workers: usize) -> Self {
        Self {
            body_path,
            block_size,
            workers,
        }
    }

    fn sort(self) -> AnyResult<u64> {
        if self.workers == 0 {
            return Err(invalid_input("offline sort requires at least one worker").into());
        }
        let block_size = usize::try_from(self.block_size)
            .map_err(|_| invalid_input("database block size does not fit memory"))?;
        if block_size == 0 || block_size % BODY_NODE_SIZE != 0 {
            return Err(
                invalid_input("database block size does not contain whole body nodes").into(),
            );
        }
        let body_file = File::open(self.body_path)?;
        // SAFETY: indexing is complete, `Database::close` unmapped the body, this mapping is
        // read-only, and scoped scan workers cannot outlive it.
        let mapping = unsafe { MmapOptions::new().map(&body_file)? };
        let data_length = mapping
            .len()
            .checked_sub(BODY_DATA_START)
            .ok_or_else(|| io::Error::other("body file is shorter than its format page"))?;
        if data_length % block_size != 0 {
            return Err(io::Error::other("body file has a partial allocation block").into());
        }
        let block_count = data_length / block_size;
        let active_workers = self.workers.min(block_count.max(1));
        let blocks_per_worker = block_count.div_ceil(active_workers);
        let run_directory = self
            .body_path
            .parent()
            .ok_or_else(|| invalid_input("body file has no parent directory"))?;

        let runs = std::thread::scope(|scope| -> AnyResult<Vec<SortRun>> {
            let mut handles = Vec::new();
            handles
                .try_reserve_exact(active_workers)
                .map_err(|_| floresta_db::Error::OutOfMemory)?;
            for worker in 0..active_workers {
                let start_block = worker.saturating_mul(blocks_per_worker);
                let end_block = start_block
                    .saturating_add(blocks_per_worker)
                    .min(block_count);
                let mapping_ref = &mapping;
                handles.push(scope.spawn(move || {
                    Self::scan_worker(
                        mapping_ref,
                        run_directory,
                        block_size,
                        worker,
                        start_block,
                        end_block,
                    )
                }));
            }

            let mut runs = Vec::new();
            for handle in handles {
                let worker_runs = handle
                    .join()
                    .map_err(|_| io::Error::other("offline sort worker panicked"))??;
                runs.try_reserve(worker_runs.len())
                    .map_err(|_| floresta_db::Error::OutOfMemory)?;
                runs.extend(worker_runs);
            }
            Ok(runs)
        })?;
        drop(mapping);
        drop(body_file);

        let live_values = runs.iter().try_fold(0_u64, |total, run| {
            total
                .checked_add(run.values)
                .ok_or_else(|| invalid_input("live output count overflow"))
        })?;
        Self::merge_runs(self.body_path, &runs, live_values)?;
        Ok(live_values)
    }

    fn scan_worker(
        mapping: &[u8],
        run_directory: &Path,
        block_size: usize,
        worker: usize,
        start_block: usize,
        end_block: usize,
    ) -> AnyResult<Vec<SortRun>> {
        let mut values = Vec::new();
        values
            .try_reserve_exact(SORT_RUN_VALUE_CAPACITY)
            .map_err(|_| floresta_db::Error::OutOfMemory)?;
        let mut runs = Vec::new();
        let mut run_index = 0_usize;

        for block in start_block..end_block {
            let block_offset = block
                .checked_mul(block_size)
                .and_then(|offset| offset.checked_add(BODY_DATA_START))
                .ok_or_else(|| io::Error::other("body block offset overflow"))?;
            let block_end = block_offset
                .checked_add(block_size)
                .ok_or_else(|| io::Error::other("body block end overflow"))?;
            let block_bytes = mapping
                .get(block_offset..block_end)
                .ok_or_else(|| io::Error::other("body block is outside its mapping"))?;

            for (slot_index, slot) in block_bytes.chunks_exact(BODY_NODE_SIZE).enumerate() {
                if let Some(value) = Self::read_live_value(slot, block, slot_index)? {
                    values.push(value);
                    if values.len() == SORT_RUN_VALUE_CAPACITY {
                        runs.try_reserve(1)
                            .map_err(|_| floresta_db::Error::OutOfMemory)?;
                        runs.push(SortRun::write(
                            run_directory,
                            worker,
                            run_index,
                            &mut values,
                        )?);
                        run_index = run_index
                            .checked_add(1)
                            .ok_or_else(|| invalid_input("sort run index overflow"))?;
                    }
                }
            }
        }
        if !values.is_empty() {
            runs.try_reserve(1)
                .map_err(|_| floresta_db::Error::OutOfMemory)?;
            runs.push(SortRun::write(
                run_directory,
                worker,
                run_index,
                &mut values,
            )?);
        }
        Ok(runs)
    }

    fn read_live_value(slot: &[u8], block: usize, slot_index: usize) -> AnyResult<Option<u64>> {
        let pointer = Self::read_u64(slot, BODY_POINTER_OFFSET)?;
        let checksum = u16::try_from(pointer >> BODY_CHECKSUM_SHIFT)
            .map_err(|_| io::Error::other("body node checksum overflow"))?;
        if checksum == 0 {
            return Ok(None);
        }
        let value = Self::read_u64(slot, BODY_VALUE_OFFSET)?;
        if value & BODY_VALUE_BLOB_TAG != 0 {
            return Err(io::Error::other(format!(
                "body node {slot_index} in block {block} stores a non-inline value"
            ))
            .into());
        }
        let key = <&[u8; OUTPOINT_KEY_SIZE]>::try_from(
            slot.get(BODY_KEY_OFFSET..BODY_VALUE_OFFSET)
                .ok_or_else(|| io::Error::other("body node key is out of bounds"))?,
        )
        .map_err(|_| io::Error::other("body node key has the wrong width"))?;
        if checksum != Self::node_checksum(key, value) {
            return Err(io::Error::other(format!(
                "body node checksum mismatch in block {block}, slot {slot_index}"
            ))
            .into());
        }
        Ok(Some(value))
    }

    fn node_checksum(key: &[u8; OUTPOINT_KEY_SIZE], value: u64) -> u16 {
        let checksum = floresta_db::xxh64(key, BODY_NODE_CHECKSUM_SEED) ^ value.rotate_left(29);
        let folded = checksum ^ (checksum >> 16) ^ (checksum >> 32) ^ (checksum >> 48);
        let bytes = folded.to_le_bytes();
        u16::from_le_bytes([bytes[0], bytes[1]]).max(1)
    }

    fn read_u64(slot: &[u8], offset: usize) -> AnyResult<u64> {
        let end = offset
            .checked_add(size_of::<u64>())
            .ok_or_else(|| io::Error::other("body field range overflow"))?;
        let bytes: [u8; size_of::<u64>()] = slot
            .get(offset..end)
            .ok_or_else(|| io::Error::other("body field is outside its node"))?
            .try_into()
            .map_err(|_| io::Error::other("body field has the wrong width"))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn merge_runs(body_path: &Path, runs: &[SortRun], expected_values: u64) -> AnyResult<()> {
        let output_path = body_path.with_extension("sorted");
        let mut output = TemporaryBody::create(output_path)?;
        let mut readers = Vec::new();
        readers
            .try_reserve_exact(runs.len())
            .map_err(|_| floresta_db::Error::OutOfMemory)?;
        for run in runs {
            readers
                .try_reserve(1)
                .map_err(|_| floresta_db::Error::OutOfMemory)?;
            readers.push(ValueReader::open_run(run)?);
        }

        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            if let Some(value) = reader.next_value()? {
                heap.try_reserve(1)
                    .map_err(|_| floresta_db::Error::OutOfMemory)?;
                heap.push(Reverse((value, index)));
            }
        }

        let file = output
            .file
            .take()
            .ok_or_else(|| io::Error::other("sorted body output file is unavailable"))?;
        let mut writer = BufWriter::new(file);
        let mut written = 0_u64;
        let mut next_progress = SORT_PROGRESS_INTERVAL;
        while let Some(Reverse((value, reader_index))) = heap.pop() {
            writer.write_all(&value.to_le_bytes())?;
            written = written
                .checked_add(1)
                .ok_or_else(|| invalid_input("merged output count overflow"))?;
            if written >= next_progress {
                log_progress(format_args!(
                    "stage=hints event=sort_merge values={written} total={expected_values}"
                ));
                next_progress = next_progress.saturating_add(SORT_PROGRESS_INTERVAL);
            }
            if let Some(next) = readers
                .get_mut(reader_index)
                .ok_or_else(|| io::Error::other("sort run reader index is out of range"))?
                .next_value()?
            {
                heap.push(Reverse((next, reader_index)));
            }
        }
        if written != expected_values {
            return Err(io::Error::other(format!(
                "external sort lost values: expected={expected_values}, written={written}"
            ))
            .into());
        }
        writer.flush()?;
        writer.get_ref().sync_data()?;
        drop(writer);
        std::fs::rename(&output.path, body_path)?;
        output.committed = true;
        Ok(())
    }
}

/// One sorted chunk produced by a body scan worker.
struct SortRun {
    path: PathBuf,
    values: u64,
}

impl SortRun {
    fn write(
        directory: &Path,
        worker: usize,
        run_index: usize,
        values: &mut Vec<u64>,
    ) -> AnyResult<Self> {
        values.sort_unstable();
        let path = directory.join(format!(".hints-sort-{worker:04}-{run_index:06}.run"));
        let result = (|| -> AnyResult<()> {
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            let mut writer = BufWriter::new(file);
            for value in values.iter().copied() {
                writer.write_all(&value.to_le_bytes())?;
            }
            writer.flush()?;
            writer.get_ref().sync_data()?;
            Ok(())
        })();
        if let Err(error) = result {
            let _removed = std::fs::remove_file(&path);
            return Err(error);
        }
        let count = u64::try_from(values.len())
            .map_err(|_| invalid_input("sort run value count does not fit u64"))?;
        values.clear();
        log_progress(format_args!(
            "stage=hints event=sort_run worker={worker} run={run_index} values={count}"
        ));
        Ok(Self {
            path,
            values: count,
        })
    }
}

impl Drop for SortRun {
    fn drop(&mut self) {
        let _removed = std::fs::remove_file(&self.path);
    }
}

/// Checked sequential reader for a fixed count of little-endian `u64` values.
struct ValueReader {
    reader: BufReader<File>,
    remaining: u64,
}

impl ValueReader {
    fn open_run(run: &SortRun) -> AnyResult<Self> {
        let expected_length = run
            .values
            .checked_mul(size_of::<u64>() as u64)
            .ok_or_else(|| io::Error::other("sort run length overflow"))?;
        let file = File::open(&run.path)?;
        if file.metadata()?.len() != expected_length {
            return Err(io::Error::other("sort run has an unexpected length").into());
        }
        Ok(Self::new(file, run.values))
    }

    fn open_flat(path: &Path) -> AnyResult<Self> {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        if length % size_of::<u64>() as u64 != 0 {
            return Err(io::Error::other("sorted values contain a partial record").into());
        }
        Ok(Self::new(file, length / size_of::<u64>() as u64))
    }

    fn new(file: File, values: u64) -> Self {
        Self {
            reader: BufReader::new(file),
            remaining: values,
        }
    }

    fn next_value(&mut self) -> AnyResult<Option<u64>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut bytes = [0_u8; size_of::<u64>()];
        self.reader.read_exact(&mut bytes)?;
        self.remaining -= 1;
        Ok(Some(u64::from_le_bytes(bytes)))
    }
}

/// Rename-on-success output guard for destructive body replacement.
struct TemporaryBody {
    path: PathBuf,
    file: Option<File>,
    committed: bool,
}

impl TemporaryBody {
    fn create(path: PathBuf) -> AnyResult<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Self {
            path,
            file: Some(file),
            committed: false,
        })
    }
}

impl Drop for TemporaryBody {
    fn drop(&mut self) {
        if !self.committed {
            let _removed = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use floresta_db::{Database, Mode};

    #[test]
    fn encodes_sorted_live_body_positions() -> AnyResult<()> {
        let directory =
            std::env::temp_dir().join(format!("floresta-db-hints-encode-{}", std::process::id()));
        let _ignored = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory)?;
        let body_path = directory.join("body");
        let hints_path = directory.join("swiftsync.hints");
        let positions = [
            OutputPosition::new(1, 0),
            OutputPosition::new(1, 2),
            OutputPosition::new(1, 3),
            OutputPosition::new(2, 1),
        ];
        let mut sorted = BufWriter::new(File::create(&body_path)?);
        for position in positions {
            sorted.write_all(&position.to_le_bytes())?;
        }
        sorted.flush()?;
        drop(sorted);

        let counts = [0, 4, 2];
        let offsets = super::super::BlockCounts::offsets(&counts)?;
        let config = Config::new(Mode::Map, 16);
        let writer = HintsWriter::new(&directory, &config, 1, 2, &counts, &offsets, &hints_path);
        assert_eq!(writer.encode_sorted(&body_path)?, 4);
        let encoded = std::fs::read(&hints_path)?;
        let hints = hintsfile::Hintsfile::from_reader(&mut encoded.as_slice())?;
        assert_eq!(hints.stop_height(), 2);
        assert_eq!(hints.indices_at_height(1), Some(vec![0, 2, 3]));
        assert_eq!(hints.indices_at_height(2), Some(vec![1]));

        std::fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn destructively_sorts_live_values_from_database_body() -> AnyResult<()> {
        let directory =
            std::env::temp_dir().join(format!("floresta-db-hints-sort-{}", std::process::id()));
        let _ignored = std::fs::remove_dir_all(&directory);
        let index_path = directory.join("index");
        std::fs::create_dir(&directory)?;
        let mut config = Config::new(Mode::Map, 16);
        config.block_size = 65_536;
        config.body_capacity = 65_536;
        config.blob_capacity = 65_536;
        let database = Database::create(&index_path, config.clone())?;
        let first = OutputPosition::new(2, 0);
        let removed = OutputPosition::new(1, 1);
        let last = OutputPosition::new(1, 0);
        database.put_new(b"key-0001-0000000", &first.to_le_bytes())?;
        database.put_new(b"key-0002-0000000", &removed.to_le_bytes())?;
        database.put_new(b"key-0003-0000000", &last.to_le_bytes())?;
        assert!(database.delete(b"key-0002-0000000")?);
        database.close()?;

        let body_path = index_path.join("body");
        assert_eq!(BodySorter::new(&body_path, config.block_size, 2).sort()?, 2);
        let bytes = std::fs::read(&body_path)?;
        let actual = bytes
            .chunks_exact(size_of::<u64>())
            .map(|value| {
                let value: [u8; size_of::<u64>()] = value
                    .try_into()
                    .map_err(|_| io::Error::other("sorted test value has the wrong width"))?;
                Ok(u64::from_le_bytes(value))
            })
            .collect::<io::Result<Vec<_>>>()?;
        assert_eq!(actual, [last.0, first.0]);

        std::fs::remove_dir_all(directory)?;
        Ok(())
    }
}
