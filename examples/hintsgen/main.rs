// SPDX-License-Identifier: MIT OR Apache-2.0

//! Builds a `floresta-db` UTXO index and a Swift Sync hintsfile from Bitcoin Core block files.
//!
//! Blocks are read through `libbitcoinkernel`. Adders and removers claim small
//! block ranges independently; remover ranges wait on a condition variable until
//! every adder has published progress beyond the block being spent.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::error::Error as StdError;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::ops::Range;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use bitcoin::consensus::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Block, OutPoint, TxOut};
use bitcoinkernel::{ChainType, ChainstateManager, ContextBuilder};
use floresta_db::{Config, Database, Error, Mode};
use hintsfile::{EliasFano, HintsfileBuilder};

const OUTPOINT_KEY_SIZE: usize = 16;
const OUTPUT_INDEX_SIZE: usize = size_of::<u64>();
const MAX_SCRIPT_SIZE: usize = 10_000;
const DEFAULT_BUCKETS: u64 = 1 << 20;
const DEFAULT_CAPACITY_GIB: u64 = 64;
const DEFAULT_BLOCK_MIB: u64 = 1;
const DEFAULT_RANGE_SIZE: u64 = 32;
const HINTS_PROGRESS_INTERVAL: u64 = 10_000;
const MAX_ADDER_LEAD_BLOCKS: u64 = 4096;
const UNPROCESSED_COUNT: u32 = u32::MAX;
const BIP30_UNSPENDABLE_HEIGHTS: [u64; 2] = [91_722, 91_812];
const COMPACTION_MIN_PAGE_LOAD: u16 = 8_192;
#[cfg(not(test))]
const SORT_RUN_VALUE_CAPACITY: usize = 1 << 20;
#[cfg(test)]
const SORT_RUN_VALUE_CAPACITY: usize = 4;
const SORT_PROGRESS_INTERVAL: u64 = 10_000_000;

// Kernel and hintsfile errors are both Send + Sync, so worker failures can cross scoped threads.
type AnyError = Box<dyn StdError + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

fn main() {
    if let Err(error) = run() {
        eprintln!("hints generation failed: {error}");
        std::process::exit(1);
    }
}

#[allow(clippy::too_many_lines)]
fn run() -> AnyResult<()> {
    let arguments = Arguments::parse()?;
    std::fs::create_dir(&arguments.work_dir)?;
    log_progress(format_args!(
        "stage=kernel event=import_start network={:?} data_dir={} blocks_dir={}",
        arguments.network,
        arguments.data_dir.display(),
        arguments.blocks_dir.display()
    ));

    let context = ContextBuilder::new()
        .chain_type(arguments.network)
        .build()?;
    let data_dir = path_text(&arguments.data_dir, "data directory")?;
    let blocks_dir = path_text(&arguments.blocks_dir, "blocks directory")?;
    let chainman = ChainstateManager::builder(&context, data_dir, blocks_dir)?
        .worker_threads(0)
        .build()?;
    log_progress(format_args!("stage=kernel event=manager_ready"));
    chainman.import_blocks()?;

    let chain_height = chainman.active_chain().height();
    let chain_height = u64::try_from(chain_height)
        .map_err(|_| invalid_input("active chain has a negative height"))?;
    log_progress(format_args!(
        "stage=kernel event=import_complete active_tip={chain_height}"
    ));
    let tip_height = arguments.tip_height.unwrap_or(chain_height);
    if tip_height > chain_height {
        return Err(invalid_input("requested tip is above the active chain tip").into());
    }
    let end_height = tip_height
        .checked_add(1)
        .ok_or_else(|| invalid_input("tip height overflow"))?;
    let block_slots = block_count_slots(end_height)?;

    let index_path = arguments.work_dir.join("index");
    let database = Database::create(&index_path, database_config(&arguments, tip_height)?)?;
    let live_outputs = AtomicU64::new(0);
    let add_ranges = RangeAllocator::new(end_height, arguments.range_size)?;
    let remove_ranges = RangeAllocator::new(end_height, arguments.range_size)?;
    let progress =
        WorkerProgress::new(arguments.add_threads, arguments.remove_threads, end_height)?;

    log_progress(format_args!(
        "stage=index event=start tip={} add_threads={} remove_threads={} range_size={} network={:?} work_dir={}",
        tip_height,
        arguments.add_threads,
        arguments.remove_threads,
        arguments.range_size,
        arguments.network,
        arguments.work_dir.display()
    ));

    let started = Instant::now();
    let (adder_stats, remover_stats, spent_runs) = std::thread::scope(|scope| -> AnyResult<_> {
        let mut adders = Vec::new();
        adders
            .try_reserve_exact(arguments.add_threads)
            .map_err(|_| Error::OutOfMemory)?;
        for worker in 0..arguments.add_threads {
            let chainman_ref = &chainman;
            let database_ref = &database;
            let ranges_ref = &add_ranges;
            let progress_ref = &progress;
            let counts_ref = &block_slots;
            let live_ref = &live_outputs;
            let network = arguments.network;
            adders.push(scope.spawn(move || {
                let result = add_worker(
                    worker,
                    network,
                    chainman_ref,
                    database_ref,
                    ranges_ref,
                    progress_ref,
                    counts_ref,
                    live_ref,
                );
                if let Err(error) = &result {
                    eprintln!("adder worker {worker} failed: {error}");
                    progress_ref.abort();
                }
                result
            }));
        }

        let mut removers = Vec::new();
        removers
            .try_reserve_exact(arguments.remove_threads)
            .map_err(|_| Error::OutOfMemory)?;
        for worker in 0..arguments.remove_threads {
            let chainman_ref = &chainman;
            let database_ref = &database;
            let ranges_ref = &remove_ranges;
            let progress_ref = &progress;
            let live_ref = &live_outputs;
            let run_directory = &arguments.work_dir;
            removers.push(scope.spawn(move || {
                let result = remove_worker(
                    worker,
                    chainman_ref,
                    database_ref,
                    ranges_ref,
                    progress_ref,
                    live_ref,
                    run_directory,
                );
                if let Err(error) = &result {
                    eprintln!("remover worker {worker} failed: {error}");
                    progress_ref.abort();
                }
                result
            }));
        }

        let mut adder_stats = WorkerStats::default();
        for handle in adders {
            let stats = handle
                .join()
                .map_err(|_| io::Error::other("adder thread panicked"))??;
            adder_stats.merge(stats);
        }
        let mut remover_stats = WorkerStats::default();
        let mut spent_runs = Vec::new();
        for handle in removers {
            let result = handle
                .join()
                .map_err(|_| io::Error::other("remover thread panicked"))??;
            remover_stats.merge(result.stats);
            spent_runs
                .try_reserve(result.runs.len())
                .map_err(|_| Error::OutOfMemory)?;
            spent_runs.extend(result.runs);
        }
        Ok((adder_stats, remover_stats, spent_runs))
    })?;
    log_progress(format_args!(
        "stage=index event=complete blocks_added={} blocks_removed={} eligible_outputs={} inputs_removed={} live_outputs={}",
        adder_stats.blocks,
        remover_stats.blocks,
        adder_stats.outputs,
        remover_stats.inputs,
        live_outputs.load(Ordering::Acquire)
    ));

    let counts = collect_output_counts(&block_slots)?;
    let offsets = fold_output_counts(&counts)?;
    let live = live_outputs.load(Ordering::Acquire);
    let minimum_page_load = COMPACTION_MIN_PAGE_LOAD;
    let compaction = database.compact(minimum_page_load)?;
    log_progress(format_args!(
        "stage=index event=compact_complete threshold={} candidates={} moved={} reclaimed={} remaining={}",
        minimum_page_load,
        compaction.candidate_pages,
        compaction.moved_nodes,
        compaction.reclaimed_pages,
        compaction.remaining_candidate_pages
    ));
    database.close()?;

    let hints_path = arguments.work_dir.join("swiftsync.hints");
    let hints_started = Instant::now();
    let hints = write_hintsfile(
        &arguments.work_dir,
        &spent_runs,
        tip_height,
        &counts,
        &offsets,
        &hints_path,
    )?;
    let hints_elapsed = hints_started.elapsed();
    if live != hints.unspent_outputs {
        return Err(io::Error::other(format!(
            "live output count mismatch: database={live}, hints={}",
            hints.unspent_outputs
        ))
        .into());
    }
    drop(spent_runs);

    let elapsed = started.elapsed();
    let storage = storage_stats(&arguments.work_dir)?;
    print_report(
        &adder_stats,
        &remover_stats,
        &hints,
        elapsed,
        hints_elapsed,
        storage,
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn add_worker(
    worker: usize,
    network: ChainType,
    chainman: &ChainstateManager,
    database: &Database,
    ranges: &RangeAllocator,
    progress: &WorkerProgress,
    block_counts: &[AtomicU32],
    live_outputs: &AtomicU64,
) -> AnyResult<WorkerStats> {
    let mut stats = WorkerStats::default();
    while let Some(range) = ranges.claim() {
        progress.check_abort()?;
        let range_start = range.start;
        let range_end = range.end;
        progress.publish_adder(worker, range_start)?;
        log_progress(format_args!(
            "stage=add event=range_claim worker={worker} start={range_start} end={}",
            range_end.saturating_sub(1)
        ));
        for height in range {
            if !progress.adder_can_process(height, MAX_ADDER_LEAD_BLOCKS) {
                log_progress(format_args!(
                    "stage=add event=wait worker={worker} next_height={height} remover_frontier={}",
                    progress.minimum_remover()
                ));
            }
            progress.wait_for_remover_window(height, MAX_ADDER_LEAD_BLOCKS)?;
            progress.check_abort()?;
            let read_started = Instant::now();
            let (block, bytes) = read_block(chainman, height)?;
            stats.block_read += read_started.elapsed();
            stats.bytes = stats.bytes.saturating_add(bytes);

            let index_started = Instant::now();
            let outputs = indexed_outputs(&block, height, network)?;
            let writer = database.write_only()?;
            let inserted = writer.put_batch(
                outputs
                    .iter()
                    .map(|output| (output.key.as_slice(), output.value.as_slice())),
            )?;
            if inserted != outputs.len() {
                return Err(io::Error::other("batch insert count mismatch").into());
            }
            let count = u32::try_from(outputs.len())
                .map_err(|_| invalid_input("eligible output count exceeds u32"))?;
            let slot = block_counts
                .get(usize::try_from(height).map_err(|_| invalid_input("height overflow"))?)
                .ok_or_else(|| invalid_input("block count slot is out of range"))?;
            slot.compare_exchange(
                UNPROCESSED_COUNT,
                count,
                Ordering::Release,
                Ordering::Acquire,
            )
            .map_err(|_| io::Error::other("block outputs were indexed twice"))?;
            cas_add(
                live_outputs,
                u64::try_from(inserted)
                    .map_err(|_| invalid_input("insert count does not fit u64"))?,
            )?;
            stats.database += index_started.elapsed();
            stats.blocks = stats.blocks.saturating_add(1);
            stats.outputs = stats.outputs.saturating_add(u64::from(count));

            let completed = height
                .checked_add(1)
                .ok_or_else(|| invalid_input("adder progress overflow"))?;
            progress.publish_adder(worker, completed)?;
        }
        log_progress(format_args!(
            "stage=add event=range_complete worker={worker} start={range_start} end={} completed_blocks={}",
            range_end.saturating_sub(1),
            stats.blocks
        ));
    }
    progress.finish_adder(worker)?;
    log_progress(format_args!(
        "stage=add event=tip worker={worker} completed_blocks={}",
        stats.blocks
    ));
    Ok(stats)
}

fn remove_worker(
    worker: usize,
    chainman: &ChainstateManager,
    database: &Database,
    ranges: &RangeAllocator,
    progress: &WorkerProgress,
    live_outputs: &AtomicU64,
    run_directory: &Path,
) -> AnyResult<RemoveWorkerResult> {
    let mut stats = WorkerStats::default();
    let mut keys = Vec::new();
    let mut input_ranges = Vec::new();
    let mut spent_positions = SortRunCollector::new(run_directory, worker)?;
    loop {
        progress.check_abort()?;
        let safe_height = progress.minimum_adder();
        if let Some(range) = ranges.claim_up_to(safe_height) {
            let range_start = range.start;
            let range_end = range.end;
            progress.publish_remover(worker, range_start)?;
            log_progress(format_args!(
                "stage=remove event=range_claim worker={worker} start={range_start} end={} safe_height={safe_height}",
                range_end.saturating_sub(1)
            ));

            keys.clear();
            input_ranges.clear();
            for height in range {
                let read_started = Instant::now();
                let (block, bytes) = read_block(chainman, height)?;
                stats.block_read += read_started.elapsed();
                stats.bytes = stats.bytes.saturating_add(bytes);

                let prepare_started = Instant::now();
                let input_start = keys.len();
                append_spent_outpoint_keys(&block, &mut keys)?;
                input_ranges.push((height, input_start, keys.len()));
                stats.database += prepare_started.elapsed();
                stats.blocks = stats.blocks.saturating_add(1);
            }

            let pop_started = Instant::now();
            let count = pop_spent_positions(database, &keys, &input_ranges, &mut spent_positions)?;
            cas_sub(live_outputs, count)?;
            stats.database += pop_started.elapsed();
            stats.inputs = stats.inputs.saturating_add(count);
            progress.publish_remover(worker, range_end)?;
            log_progress(format_args!(
                "stage=remove event=range_complete worker={worker} start={range_start} end={} completed_blocks={}",
                range_end.saturating_sub(1),
                stats.blocks
            ));
            continue;
        }
        if ranges.is_finished() {
            break;
        }

        let next = ranges.next_height();
        progress.publish_remover(worker, next)?;
        let required = ranges.next_range_end();
        if progress.minimum_adder() < required {
            log_progress(format_args!(
                "stage=remove event=wait worker={worker} next_height={next} safe_height={}",
                progress.minimum_adder()
            ));
            progress.wait_for_additions(required)?;
        } else {
            std::hint::spin_loop();
        }
    }
    let runs = spent_positions.finish()?;
    progress.finish_remover(worker)?;
    log_progress(format_args!(
        "stage=remove event=tip worker={worker} completed_blocks={}",
        stats.blocks
    ));
    Ok(RemoveWorkerResult { stats, runs })
}

fn pop_spent_positions(
    database: &Database,
    keys: &[[u8; OUTPOINT_KEY_SIZE]],
    input_ranges: &[(u64, usize, usize)],
    spent_positions: &mut SortRunCollector<'_>,
) -> AnyResult<u64> {
    let popped = database.batch_pop(keys.iter().map(<[u8; OUTPOINT_KEY_SIZE]>::as_slice))?;
    for (input, value) in popped.into_iter().enumerate() {
        let Some(value) = value else {
            let Some((height, input_start, _input_end)) = input_ranges
                .iter()
                .find(|(_, input_start, input_end)| (*input_start..*input_end).contains(&input))
            else {
                return Err(Error::Corrupt("missing input index is outside its range").into());
            };
            return Err(io::Error::other(format!(
                "missing spent outpoint at height {height}, input {}",
                input - input_start
            ))
            .into());
        };
        let position = <[u8; OUTPUT_INDEX_SIZE]>::try_from(value.as_slice())
            .map(u64::from_le_bytes)
            .map_err(|_| Error::Corrupt("stored output position has the wrong width"))?;
        spent_positions.push(position)?;
    }
    u64::try_from(keys.len())
        .map_err(|_| invalid_input("popped input count does not fit u64").into())
}

fn read_block(chainman: &ChainstateManager, height: u64) -> AnyResult<(Block, u64)> {
    let height = usize::try_from(height).map_err(|_| invalid_input("height does not fit usize"))?;
    let chain = chainman.active_chain();
    let entry = chain
        .at_height(height)
        .ok_or_else(|| invalid_input("active chain does not contain requested height"))?;
    let kernel_block = chainman.read_block_data(&entry)?;
    let encoded = kernel_block.consensus_encode()?;
    let length = u64::try_from(encoded.len())
        .map_err(|_| invalid_input("serialized block length does not fit u64"))?;
    Ok((deserialize(&encoded)?, length))
}

fn indexed_outputs(
    block: &Block,
    height: u64,
    network: ChainType,
) -> AnyResult<Vec<IndexedOutput>> {
    let output_capacity = block
        .txdata
        .iter()
        .try_fold(0_usize, |total, transaction| {
            total
                .checked_add(transaction.output.len())
                .ok_or_else(|| invalid_input("block output count overflow"))
        })?;
    let mut outputs = Vec::new();
    outputs
        .try_reserve_exact(output_capacity)
        .map_err(|_| Error::OutOfMemory)?;
    let mut eligible_index = 0_u32;
    for (transaction_index, transaction) in block.txdata.iter().enumerate() {
        let txid = transaction.compute_txid();
        append_eligible_outputs(
            height,
            network,
            transaction_index,
            txid,
            &transaction.output,
            &mut eligible_index,
            &mut outputs,
        )?;
    }
    Ok(outputs)
}

fn append_eligible_outputs(
    height: u64,
    network: ChainType,
    transaction_index: usize,
    txid: bitcoin::Txid,
    outputs: &[TxOut],
    eligible_index: &mut u32,
    indexed: &mut Vec<IndexedOutput>,
) -> AnyResult<()> {
    let block_height =
        u32::try_from(height).map_err(|_| invalid_input("block height exceeds u32"))?;
    for (vout, output) in outputs.iter().enumerate() {
        if !should_index_output(network, height, transaction_index, output) {
            continue;
        }
        let vout = u32::try_from(vout)
            .map_err(|_| invalid_input("transaction output index exceeds u32"))?;
        let outpoint = OutPoint { txid, vout };
        indexed.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        indexed.push(IndexedOutput {
            key: outpoint_key(outpoint),
            value: pack_output_position(block_height, *eligible_index).to_le_bytes(),
        });
        *eligible_index = eligible_index
            .checked_add(1)
            .ok_or_else(|| invalid_input("eligible output index overflow"))?;
    }
    Ok(())
}

fn append_spent_outpoint_keys(
    block: &Block,
    keys: &mut Vec<[u8; OUTPOINT_KEY_SIZE]>,
) -> AnyResult<()> {
    let input_count = block
        .txdata
        .iter()
        .try_fold(0_usize, |total, transaction| {
            if transaction.is_coinbase() {
                Ok(total)
            } else {
                total
                    .checked_add(transaction.input.len())
                    .ok_or_else(|| invalid_input("block input count overflow"))
            }
        })?;
    keys.try_reserve(input_count)
        .map_err(|_| Error::OutOfMemory)?;
    for transaction in &block.txdata {
        if transaction.is_coinbase() {
            continue;
        }
        for input in &transaction.input {
            keys.push(outpoint_key(input.previous_output));
        }
    }
    Ok(())
}

fn should_index_output(
    network: ChainType,
    height: u64,
    transaction_index: usize,
    output: &TxOut,
) -> bool {
    height != 0
        && !(network == ChainType::Mainnet
            && transaction_index == 0
            && BIP30_UNSPENDABLE_HEIGHTS.contains(&height))
        && output.script_pubkey.len() <= MAX_SCRIPT_SIZE
        && !output.script_pubkey.is_op_return()
}

fn outpoint_key(outpoint: OutPoint) -> [u8; OUTPOINT_KEY_SIZE] {
    let txid = outpoint.txid.to_byte_array();
    let mut key = [0_u8; OUTPOINT_KEY_SIZE];
    key[..12].copy_from_slice(&txid[..12]);
    key[12..].copy_from_slice(&outpoint.vout.to_le_bytes());
    key
}

fn pack_output_position(block_height: u32, block_index: u32) -> u64 {
    u64::from(block_height) << 32 | u64::from(block_index)
}

fn unpack_output_position(position: u64) -> (u32, u32) {
    let [
        index_0,
        index_1,
        index_2,
        index_3,
        height_0,
        height_1,
        height_2,
        height_3,
    ] = position.to_le_bytes();
    (
        u32::from_le_bytes([height_0, height_1, height_2, height_3]),
        u32::from_le_bytes([index_0, index_1, index_2, index_3]),
    )
}

fn collect_output_counts(slots: &[AtomicU32]) -> AnyResult<Vec<u32>> {
    let mut counts = Vec::new();
    counts
        .try_reserve_exact(slots.len())
        .map_err(|_| Error::OutOfMemory)?;
    for slot in slots {
        let count = slot.load(Ordering::Acquire);
        if count == UNPROCESSED_COUNT {
            return Err(io::Error::other("an output-count slot was never published").into());
        }
        counts.push(count);
    }
    Ok(counts)
}

fn fold_output_counts(counts: &[u32]) -> AnyResult<Vec<u64>> {
    let mut offsets = Vec::new();
    offsets
        .try_reserve_exact(
            counts
                .len()
                .checked_add(1)
                .ok_or_else(|| invalid_input("block offset vector length overflow"))?,
        )
        .map_err(|_| Error::OutOfMemory)?;
    offsets.push(0_u64);
    for count in counts {
        let next = offsets
            .last()
            .copied()
            .ok_or_else(|| invalid_input("block offset vector is empty"))?
            .checked_add(u64::from(*count))
            .ok_or_else(|| invalid_input("global output index overflow"))?;
        offsets.push(next);
    }
    Ok(offsets)
}

fn write_hintsfile(
    directory: &Path,
    spent_runs: &[SortRun],
    tip_height: u64,
    counts: &[u32],
    offsets: &[u64],
    path: &Path,
) -> AnyResult<HintsStats> {
    let eligible_outputs = offsets
        .last()
        .copied()
        .ok_or_else(|| invalid_input("block offsets are empty"))?;
    let mut spent_outputs = 0_u64;
    for run in spent_runs {
        spent_outputs = spent_outputs
            .checked_add(run.values)
            .ok_or_else(|| invalid_input("spent output count overflow"))?;
    }
    let unspent_outputs = eligible_outputs
        .checked_sub(spent_outputs)
        .ok_or_else(|| invalid_input("spent outputs exceed eligible outputs"))?;
    let sorted_spent_path = directory.join("spent-outputs.data");
    log_progress(format_args!(
        "stage=hints event=sort_start runs={} spent_outputs={spent_outputs}",
        spent_runs.len()
    ));
    merge_sort_runs(&sorted_spent_path, spent_runs, spent_outputs)?;
    log_progress(format_args!(
        "stage=hints event=sort_complete spent_outputs={spent_outputs}"
    ));

    log_progress(format_args!(
        "stage=hints event=encode_start path={}",
        path.display()
    ));
    let encoded = encode_unspent_hintsfile(path, &sorted_spent_path, tip_height, counts);
    let cleanup = std::fs::remove_file(&sorted_spent_path);
    let encoded_outputs = match (encoded, cleanup) {
        (Ok(outputs), Ok(())) => outputs,
        (Err(error), _) => return Err(error),
        (Ok(_), Err(error)) => return Err(error.into()),
    };
    if encoded_outputs != unspent_outputs {
        return Err(io::Error::other(format!(
            "encoded output count mismatch: expected={unspent_outputs}, actual={encoded_outputs}"
        ))
        .into());
    }
    log_progress(format_args!(
        "stage=hints event=complete path={} bytes={}",
        path.display(),
        std::fs::metadata(path)?.len()
    ));

    Ok(HintsStats {
        eligible_outputs,
        unspent_outputs,
        file_bytes: std::fs::metadata(path)?.len(),
    })
}

fn write_sort_run(
    directory: &Path,
    worker: usize,
    run_index: usize,
    values: &mut Vec<u64>,
) -> AnyResult<SortRun> {
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
    Ok(SortRun {
        path,
        values: count,
    })
}

fn merge_sort_runs(output_path: &Path, runs: &[SortRun], expected_values: u64) -> AnyResult<()> {
    let temporary_path = output_path.with_extension("sorted");
    let mut output = TemporarySortOutput::create(temporary_path)?;
    let mut readers = Vec::new();
    readers
        .try_reserve_exact(runs.len())
        .map_err(|_| Error::OutOfMemory)?;
    for run in runs {
        readers.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        readers.push(ValueReader::open_run(run)?);
    }

    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = reader.next_value()? {
            heap.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
            heap.push(Reverse((value, index)));
        }
    }

    let file = output
        .file
        .take()
        .ok_or_else(|| io::Error::other("temporary sort output file is unavailable"))?;
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
    std::fs::rename(&output.path, output_path)?;
    output.committed = true;
    Ok(())
}

fn encode_unspent_hintsfile(
    path: &Path,
    sorted_spent_path: &Path,
    tip_height: u64,
    counts: &[u32],
) -> AnyResult<u64> {
    let stop_height =
        u32::try_from(tip_height).map_err(|_| invalid_input("hintsfile height exceeds u32"))?;
    let mut spent_positions = ValueReader::open_flat(sorted_spent_path)?;
    let mut next_spent = spent_positions.next_value()?;
    let writer = BufWriter::new(File::create(path)?);
    let builder = HintsfileBuilder::new(writer);
    let mut builder = builder.initialize(stop_height)?;
    let mut indices = Vec::new();
    let mut encoded_outputs = 0_u64;

    // hintsfile 0.1 encodes heights 1..=stop_height; genesis has no eligible outputs.
    for height in 1..=stop_height {
        if height == 1 || height == stop_height || u64::from(height) % HINTS_PROGRESS_INTERVAL == 0
        {
            log_progress(format_args!(
                "stage=hints event=encode_progress height={height} tip={stop_height}"
            ));
        }
        indices.clear();
        let height_index = usize::try_from(height)
            .map_err(|_| invalid_input("hintsfile height does not fit usize"))?;
        let count = counts
            .get(height_index)
            .copied()
            .ok_or_else(|| invalid_input("eligible output count is unavailable"))?;
        let count_usize = usize::try_from(count)
            .map_err(|_| invalid_input("eligible output count does not fit memory"))?;
        if count_usize > indices.capacity() {
            indices
                .try_reserve_exact(count_usize - indices.capacity())
                .map_err(|_| Error::OutOfMemory)?;
        }

        for output_index in 0..count {
            if let Some(position) = next_spent {
                let (position_height, spent_index) = unpack_output_position(position);
                if position_height < height
                    || (position_height == height && spent_index < output_index)
                {
                    return Err(io::Error::other(
                        "sorted spent output positions contain a duplicate or moved backwards",
                    )
                    .into());
                }
                if position_height == height && spent_index == output_index {
                    next_spent = spent_positions.next_value()?;
                    continue;
                }
            }
            indices.push(output_index);
            encoded_outputs = encoded_outputs
                .checked_add(1)
                .ok_or_else(|| invalid_input("encoded output count overflow"))?;
        }
        builder.append(EliasFano::compress(&indices))?;
    }
    if next_spent.is_some() {
        return Err(io::Error::other(
            "sorted spent outputs contain a position above the requested tip",
        )
        .into());
    }
    builder.finish()?;
    Ok(encoded_outputs)
}

struct SortRunCollector<'directory> {
    directory: &'directory Path,
    worker: usize,
    next_index: usize,
    values: Vec<u64>,
    runs: Vec<SortRun>,
}

impl<'directory> SortRunCollector<'directory> {
    fn new(directory: &'directory Path, worker: usize) -> AnyResult<Self> {
        let mut values = Vec::new();
        values
            .try_reserve_exact(SORT_RUN_VALUE_CAPACITY)
            .map_err(|_| Error::OutOfMemory)?;
        Ok(Self {
            directory,
            worker,
            next_index: 0,
            values,
            runs: Vec::new(),
        })
    }

    fn push(&mut self, position: u64) -> AnyResult<()> {
        self.values.push(position);
        if self.values.len() == SORT_RUN_VALUE_CAPACITY {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> AnyResult<Vec<SortRun>> {
        self.flush()?;
        Ok(self.runs)
    }

    fn flush(&mut self) -> AnyResult<()> {
        if self.values.is_empty() {
            return Ok(());
        }
        let following_index = self
            .next_index
            .checked_add(1)
            .ok_or_else(|| invalid_input("sort run index overflow"))?;
        self.runs.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        self.runs.push(write_sort_run(
            self.directory,
            self.worker,
            self.next_index,
            &mut self.values,
        )?);
        self.next_index = following_index;
        Ok(())
    }
}

struct SortRun {
    path: PathBuf,
    values: u64,
}

impl Drop for SortRun {
    fn drop(&mut self) {
        let _removed = std::fs::remove_file(&self.path);
    }
}

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

struct TemporarySortOutput {
    path: PathBuf,
    file: Option<File>,
    committed: bool,
}

impl TemporarySortOutput {
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

impl Drop for TemporarySortOutput {
    fn drop(&mut self) {
        if !self.committed {
            let _removed = std::fs::remove_file(&self.path);
        }
    }
}

fn block_count_slots(end_height: u64) -> AnyResult<Box<[AtomicU32]>> {
    let count = usize::try_from(end_height)
        .map_err(|_| invalid_input("block count does not fit memory"))?;
    let mut slots = Vec::new();
    slots
        .try_reserve_exact(count)
        .map_err(|_| Error::OutOfMemory)?;
    for _height in 0..count {
        slots.push(AtomicU32::new(UNPROCESSED_COUNT));
    }
    Ok(slots.into_boxed_slice())
}

fn database_config(arguments: &Arguments, tip_height: u64) -> AnyResult<Config> {
    let mut config = Config::new(Mode::Map, arguments.buckets);
    config.block_size = arguments.database_block_bytes;
    config.body_capacity = arguments.body_capacity;
    let minimum_body = tip_height
        .checked_add(1)
        .and_then(|blocks| blocks.checked_mul(96))
        .ok_or_else(|| invalid_input("minimum body capacity overflow"))?;
    if config.body_capacity < minimum_body {
        return Err(invalid_input("body capacity is too small for one output per block").into());
    }
    Ok(config)
}

struct RangeAllocator {
    next: AtomicU64,
    end: u64,
    size: u64,
}

impl RangeAllocator {
    fn new(end: u64, size: u64) -> AnyResult<Self> {
        if size == 0 {
            return Err(invalid_input("range size must be nonzero").into());
        }
        Ok(Self {
            next: AtomicU64::new(0),
            end,
            size,
        })
    }

    fn claim(&self) -> Option<Range<u64>> {
        self.claim_up_to(self.end)
    }

    fn claim_up_to(&self, limit: u64) -> Option<Range<u64>> {
        let limit = limit.min(self.end);
        loop {
            let start = self.next.load(Ordering::Acquire);
            if start >= limit {
                return None;
            }
            let end = start.saturating_add(self.size).min(self.end);
            if end > limit {
                return None;
            }
            if self
                .next
                .compare_exchange(start, end, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(start..end);
            }
        }
    }

    fn next_height(&self) -> u64 {
        self.next.load(Ordering::Acquire)
    }

    fn next_range_end(&self) -> u64 {
        self.next_height().saturating_add(self.size).min(self.end)
    }

    fn is_finished(&self) -> bool {
        self.next_height() >= self.end
    }
}

struct WorkerProgress {
    adders: Box<[AtomicU64]>,
    removers: Box<[AtomicU64]>,
    mutex: Mutex<()>,
    changed: Condvar,
    aborted: AtomicU64,
    end: u64,
}

impl WorkerProgress {
    fn new(adders: usize, removers: usize, end: u64) -> AnyResult<Self> {
        if adders == 0 || removers == 0 {
            return Err(invalid_input("worker progress sets must be nonempty").into());
        }
        Ok(Self {
            adders: progress_slots(adders)?,
            removers: progress_slots(removers)?,
            mutex: Mutex::new(()),
            changed: Condvar::new(),
            aborted: AtomicU64::new(0),
            end,
        })
    }

    fn publish_adder(&self, worker: usize, completed: u64) -> io::Result<()> {
        self.publish(&self.adders, worker, completed, "adder")
    }

    fn publish_remover(&self, worker: usize, completed: u64) -> io::Result<()> {
        self.publish(&self.removers, worker, completed, "remover")
    }

    fn publish(
        &self,
        slots: &[AtomicU64],
        worker: usize,
        completed: u64,
        kind: &str,
    ) -> io::Result<()> {
        let _guard = self
            .mutex
            .lock()
            .map_err(|_| io::Error::other("worker progress mutex is poisoned"))?;
        let height = slots
            .get(worker)
            .ok_or_else(|| io::Error::other(format!("{kind} progress index is out of range")))?;
        let mut old = height.load(Ordering::Acquire);
        loop {
            if completed < old || completed > self.end {
                return Err(io::Error::other(format!(
                    "{kind} progress is not monotonic"
                )));
            }
            match height.compare_exchange(old, completed, Ordering::Release, Ordering::Acquire) {
                Ok(_) => break,
                Err(actual) => old = actual,
            }
        }
        self.changed.notify_all();
        Ok(())
    }

    fn finish_adder(&self, worker: usize) -> io::Result<()> {
        self.publish_adder(worker, self.end)
    }

    fn finish_remover(&self, worker: usize) -> io::Result<()> {
        self.publish_remover(worker, self.end)
    }

    fn minimum_adder(&self) -> u64 {
        minimum_progress(&self.adders, self.end)
    }

    fn minimum_remover(&self) -> u64 {
        minimum_progress(&self.removers, self.end)
    }

    fn adder_can_process(&self, height: u64, maximum_lead: u64) -> bool {
        height < self.minimum_remover().saturating_add(maximum_lead)
    }

    fn wait_for_additions(&self, required: u64) -> io::Result<()> {
        self.wait_until(|| self.minimum_adder() >= required)
    }

    fn wait_for_remover_window(&self, height: u64, maximum_lead: u64) -> io::Result<()> {
        self.wait_until(|| self.adder_can_process(height, maximum_lead))
    }

    fn wait_until<F>(&self, ready: F) -> io::Result<()>
    where
        F: Fn() -> bool,
    {
        let mut guard = self
            .mutex
            .lock()
            .map_err(|_| io::Error::other("worker progress mutex is poisoned"))?;
        loop {
            self.check_abort()?;
            if ready() {
                return Ok(());
            }
            guard = self
                .changed
                .wait(guard)
                .map_err(|_| io::Error::other("worker progress mutex is poisoned"))?;
        }
    }

    fn check_abort(&self) -> io::Result<()> {
        if self.aborted.load(Ordering::Acquire) == 0 {
            Ok(())
        } else {
            Err(io::Error::other("load workers aborted"))
        }
    }

    fn abort(&self) {
        let guard = self.mutex.lock();
        let Ok(_guard) = guard else {
            return;
        };
        let _aborted = self
            .aborted
            .compare_exchange(0, 1, Ordering::Release, Ordering::Acquire);
        self.changed.notify_all();
    }
}

fn progress_slots(workers: usize) -> AnyResult<Box<[AtomicU64]>> {
    let mut heights = Vec::new();
    heights
        .try_reserve_exact(workers)
        .map_err(|_| Error::OutOfMemory)?;
    for _worker in 0..workers {
        heights.push(AtomicU64::new(0));
    }
    Ok(heights.into_boxed_slice())
}

fn minimum_progress(heights: &[AtomicU64], end: u64) -> u64 {
    let mut minimum = end;
    for height in heights {
        minimum = minimum.min(height.load(Ordering::Acquire));
    }
    minimum
}

#[derive(Clone, Copy)]
struct IndexedOutput {
    key: [u8; OUTPOINT_KEY_SIZE],
    value: [u8; OUTPUT_INDEX_SIZE],
}

#[derive(Clone, Copy, Default)]
struct WorkerStats {
    blocks: u64,
    bytes: u64,
    outputs: u64,
    inputs: u64,
    block_read: Duration,
    database: Duration,
}

impl WorkerStats {
    fn merge(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.outputs = self.outputs.saturating_add(other.outputs);
        self.inputs = self.inputs.saturating_add(other.inputs);
        self.block_read += other.block_read;
        self.database += other.database;
    }
}

struct RemoveWorkerResult {
    stats: WorkerStats,
    runs: Vec<SortRun>,
}

#[derive(Clone, Copy)]
struct HintsStats {
    eligible_outputs: u64,
    unspent_outputs: u64,
    file_bytes: u64,
}

#[derive(Clone, Copy, Default)]
struct StorageStats {
    files: u64,
    logical_bytes: u64,
    allocated_bytes: u64,
}

impl StorageStats {
    fn merge(&mut self, other: Self) {
        self.files = self.files.saturating_add(other.files);
        self.logical_bytes = self.logical_bytes.saturating_add(other.logical_bytes);
        self.allocated_bytes = self.allocated_bytes.saturating_add(other.allocated_bytes);
    }
}

#[allow(clippy::too_many_arguments)]
fn print_report(
    adders: &WorkerStats,
    removers: &WorkerStats,
    hints: &HintsStats,
    elapsed: Duration,
    hints_elapsed: Duration,
    storage: StorageStats,
) {
    println!(
        "status=ok elapsed_seconds={:.3} blocks_added={} blocks_removed={} eligible_outputs={} inputs_removed={} live_outputs={} block_mib_per_second={:.1}",
        elapsed.as_secs_f64(),
        adders.blocks,
        removers.blocks,
        hints.eligible_outputs,
        removers.inputs,
        hints.unspent_outputs,
        mebibytes_per_second(adders.bytes.saturating_add(removers.bytes), elapsed),
    );
    println!(
        "timing block_read_seconds={:.3} database_seconds={:.3} hints_seconds={:.3}",
        adders.block_read.as_secs_f64() + removers.block_read.as_secs_f64(),
        adders.database.as_secs_f64() + removers.database.as_secs_f64(),
        hints_elapsed.as_secs_f64(),
    );
    println!(
        "hints bytes={} unspent_outputs={}",
        hints.file_bytes, hints.unspent_outputs
    );
    println!(
        "storage files={} logical_gib={:.3} allocated_mib={:.1}",
        storage.files,
        gibibytes_f64(storage.logical_bytes),
        mebibytes_f64(storage.allocated_bytes),
    );
}

fn storage_stats(path: &Path) -> io::Result<StorageStats> {
    let metadata = path.metadata()?;
    if metadata.is_file() {
        return Ok(StorageStats {
            files: 1,
            logical_bytes: metadata.len(),
            allocated_bytes: metadata.blocks().saturating_mul(512),
        });
    }
    let mut total = StorageStats::default();
    for entry in std::fs::read_dir(path)? {
        total.merge(storage_stats(&entry?.path())?);
    }
    Ok(total)
}

fn cas_add(counter: &AtomicU64, amount: u64) -> io::Result<()> {
    let mut old = counter.load(Ordering::Acquire);
    loop {
        let new = old
            .checked_add(amount)
            .ok_or_else(|| io::Error::other("live output counter overflow"))?;
        match counter.compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(actual) => old = actual,
        }
    }
}

fn cas_sub(counter: &AtomicU64, amount: u64) -> io::Result<()> {
    let mut old = counter.load(Ordering::Acquire);
    loop {
        let new = old
            .checked_sub(amount)
            .ok_or_else(|| io::Error::other("live output counter underflow"))?;
        match counter.compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(actual) => old = actual,
        }
    }
}

struct Arguments {
    data_dir: PathBuf,
    blocks_dir: PathBuf,
    network: ChainType,
    tip_height: Option<u64>,
    add_threads: usize,
    remove_threads: usize,
    range_size: u64,
    work_dir: PathBuf,
    buckets: u64,
    body_capacity: u64,
    database_block_bytes: u64,
}

impl Arguments {
    fn parse() -> AnyResult<Self> {
        let arguments: Vec<String> = std::env::args().skip(1).collect();
        if arguments.len() < 2 {
            print_usage();
            return Err(invalid_input("data and blocks directories are required").into());
        }
        let available = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let default_adders = available.div_ceil(2).max(1);
        let default_removers = available.saturating_sub(default_adders).max(1);
        let add_threads = optional_usize(&arguments, 4, default_adders, "ADD_THREADS")?;
        let remove_threads = optional_usize(&arguments, 5, default_removers, "REMOVE_THREADS")?;
        if add_threads == 0 || remove_threads == 0 {
            return Err(invalid_input("worker counts must be nonzero").into());
        }
        let range_size = optional_u64(&arguments, 6, DEFAULT_RANGE_SIZE, "RANGE_SIZE")?;
        if range_size == 0 {
            return Err(invalid_input("range size must be nonzero").into());
        }
        let body_gib = environment_u64("HINTSGEN_BODY_GIB", DEFAULT_CAPACITY_GIB)?;
        let block_mib = environment_u64("HINTSGEN_BLOCK_MIB", DEFAULT_BLOCK_MIB)?;

        Ok(Self {
            data_dir: PathBuf::from(&arguments[0]),
            blocks_dir: PathBuf::from(&arguments[1]),
            network: arguments
                .get(2)
                .map_or(Ok(ChainType::Mainnet), |network| parse_network(network))?,
            tip_height: arguments.get(3).map_or(Ok(None), |tip| {
                if tip.eq_ignore_ascii_case("tip") {
                    Ok(None)
                } else {
                    parse_u64(tip, "TIP").map(Some)
                }
            })?,
            add_threads,
            remove_threads,
            range_size,
            work_dir: arguments.get(7).map_or_else(
                || PathBuf::from(format!("hintsgen-{}", std::process::id())),
                PathBuf::from,
            ),
            buckets: environment_u64("HINTSGEN_BUCKETS", DEFAULT_BUCKETS)?,
            body_capacity: gibibytes(body_gib)?,
            database_block_bytes: mebibytes(block_mib)?,
        })
    }
}

fn parse_network(value: &str) -> AnyResult<ChainType> {
    match value.to_ascii_lowercase().as_str() {
        "mainnet" | "main" => Ok(ChainType::Mainnet),
        "testnet" | "testnet3" => Ok(ChainType::Testnet),
        "testnet4" => Ok(ChainType::Testnet4),
        "signet" => Ok(ChainType::Signet),
        "regtest" => Ok(ChainType::Regtest),
        _ => Err(invalid_input("unknown Bitcoin network").into()),
    }
}

fn optional_u64(arguments: &[String], index: usize, default: u64, name: &str) -> AnyResult<u64> {
    arguments
        .get(index)
        .map_or(Ok(default), |value| parse_u64(value, name))
}

fn optional_usize(
    arguments: &[String],
    index: usize,
    default: usize,
    name: &str,
) -> AnyResult<usize> {
    let value = optional_u64(
        arguments,
        index,
        u64::try_from(default).map_err(|_| invalid_input("worker default exceeds u64"))?,
        name,
    )?;
    usize::try_from(value).map_err(|_| invalid_input_owned(format!("{name} exceeds usize")).into())
}

fn environment_u64(name: &str, default: u64) -> AnyResult<u64> {
    std::env::var(name).map_or(Ok(default), |value| parse_u64(&value, name))
}

fn parse_u64(value: &str, name: &str) -> AnyResult<u64> {
    value
        .parse::<u64>()
        .map_err(|error| invalid_input_owned(format!("{name} is not an integer: {error}")).into())
}

fn mebibytes(value: u64) -> AnyResult<u64> {
    value
        .checked_mul(1 << 20)
        .ok_or_else(|| invalid_input("MiB value overflow").into())
}

fn gibibytes(value: u64) -> AnyResult<u64> {
    value
        .checked_mul(1 << 30)
        .ok_or_else(|| invalid_input("GiB value overflow").into())
}

#[allow(clippy::cast_precision_loss)]
fn mebibytes_per_second(bytes: u64, elapsed: Duration) -> f64 {
    mebibytes_f64(bytes) / elapsed.as_secs_f64()
}

#[allow(clippy::cast_precision_loss)]
fn mebibytes_f64(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[allow(clippy::cast_precision_loss)]
fn gibibytes_f64(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0 * 1024.0)
}

fn path_text<'path>(path: &'path Path, name: &str) -> AnyResult<&'path str> {
    path.to_str()
        .ok_or_else(|| invalid_input_owned(format!("{name} is not valid UTF-8")).into())
}

fn log_progress(arguments: std::fmt::Arguments<'_>) {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    if writeln!(output, "{arguments}").is_ok() {
        let _flushed = output.flush();
    }
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_input_owned(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn print_usage() {
    println!(
        "Usage: hintsgen DATA_DIR BLOCKS_DIR [mainnet|testnet|testnet4|signet|regtest] \
         [TIP|tip] [ADD_THREADS] [REMOVE_THREADS] [RANGE_SIZE] [WORK_DIR]\n\
         Environment: HINTSGEN_BUCKETS HINTSGEN_BODY_GIB HINTSGEN_BLOCK_MIB"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use bitcoin::{Amount, ScriptBuf, Txid};

    use super::*;

    #[test]
    fn initializes_libbitcoinkernel_context() -> AnyResult<()> {
        let _context = ContextBuilder::new()
            .chain_type(ChainType::Regtest)
            .build()?;
        Ok(())
    }

    #[test]
    fn truncates_txid_and_keeps_little_endian_vout() {
        let mut txid = [0_u8; 32];
        for (index, byte) in txid.iter_mut().enumerate() {
            *byte = u8::try_from(index).unwrap_or(u8::MAX);
        }
        let key = outpoint_key(OutPoint {
            txid: Txid::from_byte_array(txid),
            vout: 0x0102_0304,
        });
        assert_eq!(&key[..12], &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        assert_eq!(&key[12..], &[4, 3, 2, 1]);
    }

    #[test]
    fn indexes_only_eligible_outputs() -> AnyResult<()> {
        let spendable = TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        };
        let op_return = TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![0x6a]),
        };
        let oversized = TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51; MAX_SCRIPT_SIZE + 1]),
        };
        let mut index = 0;
        let mut indexed = Vec::new();
        append_eligible_outputs(
            1,
            ChainType::Mainnet,
            1,
            Txid::from_byte_array([7; 32]),
            &[spendable.clone(), op_return, oversized, spendable],
            &mut index,
            &mut indexed,
        )?;
        assert_eq!(index, 2);
        assert_eq!(indexed[0].value, pack_output_position(1, 0).to_le_bytes());
        assert_eq!(indexed[1].value, pack_output_position(1, 1).to_le_bytes());

        assert!(!should_index_output(
            ChainType::Mainnet,
            0,
            0,
            &indexed_output()
        ));
        assert!(!should_index_output(
            ChainType::Mainnet,
            91_722,
            0,
            &indexed_output()
        ));
        assert!(!should_index_output(
            ChainType::Mainnet,
            91_812,
            0,
            &indexed_output()
        ));
        assert!(should_index_output(
            ChainType::Mainnet,
            91_722,
            1,
            &indexed_output()
        ));
        assert!(should_index_output(
            ChainType::Signet,
            91_722,
            0,
            &indexed_output()
        ));
        Ok(())
    }

    #[test]
    fn folds_block_counts_into_global_offsets() -> AnyResult<()> {
        assert_eq!(fold_output_counts(&[1, 4])?, vec![0, 1, 5]);
        assert_eq!(fold_output_counts(&[])?, vec![0]);
        Ok(())
    }

    #[test]
    fn encodes_complement_of_sorted_spent_positions() -> AnyResult<()> {
        let directory =
            std::env::temp_dir().join(format!("floresta-db-hints-encode-{}", std::process::id()));
        let _ignored = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory)?;
        let spent_path = directory.join("spent");
        let hints_path = directory.join("swiftsync.hints");
        let mut sorted = BufWriter::new(File::create(&spent_path)?);
        for position in [pack_output_position(1, 1), pack_output_position(2, 0)] {
            sorted.write_all(&position.to_le_bytes())?;
        }
        sorted.flush()?;
        drop(sorted);

        let counts = [0, 4, 2];
        assert_eq!(
            encode_unspent_hintsfile(&hints_path, &spent_path, 2, &counts)?,
            4
        );
        let encoded = std::fs::read(&hints_path)?;
        let hints = hintsfile::Hintsfile::from_reader(&mut encoded.as_slice())?;
        assert_eq!(hints.stop_height(), 2);
        assert_eq!(hints.indices_at_height(1), Some(vec![0, 2, 3]));
        assert_eq!(hints.indices_at_height(2), Some(vec![1]));

        std::fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn merges_sorted_spend_runs() -> AnyResult<()> {
        let directory =
            std::env::temp_dir().join(format!("floresta-db-hints-sort-{}", std::process::id()));
        let _ignored = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory)?;
        let mut first = vec![9, 1, 5];
        let mut second = vec![8, 2];
        let runs = [
            write_sort_run(&directory, 0, 0, &mut first)?,
            write_sort_run(&directory, 1, 0, &mut second)?,
        ];
        let sorted_path = directory.join("spent");
        merge_sort_runs(&sorted_path, &runs, 5)?;

        let bytes = std::fs::read(&sorted_path)?;
        let mut actual = Vec::new();
        for value in bytes.chunks_exact(size_of::<u64>()) {
            let value: [u8; size_of::<u64>()] = value
                .try_into()
                .map_err(|_| io::Error::other("sorted test value has the wrong width"))?;
            actual.push(u64::from_le_bytes(value));
        }
        assert_eq!(actual, [1, 2, 5, 8, 9]);

        drop(runs);
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn allocates_small_ranges_once() -> AnyResult<()> {
        let ranges = RangeAllocator::new(10, 3)?;
        assert_eq!(ranges.claim(), Some(0..3));
        assert_eq!(ranges.claim(), Some(3..6));
        assert_eq!(ranges.claim(), Some(6..9));
        assert_eq!(ranges.claim(), Some(9..10));
        assert_eq!(ranges.claim(), None);
        Ok(())
    }

    #[test]
    fn remover_waits_for_complete_ranges() -> AnyResult<()> {
        let ranges = RangeAllocator::new(10, 4)?;
        assert_eq!(ranges.claim_up_to(0), None);
        assert_eq!(ranges.claim_up_to(2), None);
        assert_eq!(ranges.next_range_end(), 4);
        assert_eq!(ranges.claim_up_to(4), Some(0..4));
        assert_eq!(ranges.claim_up_to(7), None);
        assert_eq!(ranges.next_range_end(), 8);
        assert_eq!(ranges.claim_up_to(8), Some(4..8));
        assert_eq!(ranges.claim_up_to(9), None);
        assert_eq!(ranges.claim_up_to(10), Some(8..10));
        assert!(ranges.is_finished());
        Ok(())
    }

    #[test]
    fn remover_waits_for_minimum_adder_height() -> AnyResult<()> {
        let progress = WorkerProgress::new(2, 1, 10)?;
        std::thread::scope(|scope| -> AnyResult<()> {
            let (sender, receiver) = mpsc::channel();
            let progress_ref = &progress;
            let waiter = scope.spawn(move || -> io::Result<()> {
                progress_ref.wait_for_additions(3)?;
                sender
                    .send(())
                    .map_err(|_| io::Error::other("progress test receiver dropped"))
            });

            progress.publish_adder(0, 3)?;
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            progress.publish_adder(1, 3)?;
            receiver
                .recv_timeout(Duration::from_secs(1))
                .map_err(|_| io::Error::other("remover did not observe safe height"))?;
            waiter
                .join()
                .map_err(|_| io::Error::other("progress waiter panicked"))??;
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn adders_wait_when_they_exceed_the_remover_window() -> AnyResult<()> {
        let progress = WorkerProgress::new(1, 1, 20)?;
        assert!(progress.adder_can_process(3, 4));
        assert!(!progress.adder_can_process(4, 4));
        progress.publish_remover(0, 1)?;
        assert!(progress.adder_can_process(4, 4));
        assert!(!progress.adder_can_process(5, 4));
        progress.finish_remover(0)?;
        assert!(progress.adder_can_process(19, 4));
        Ok(())
    }

    #[test]
    fn claiming_a_later_range_releases_the_old_worker_frontier() -> AnyResult<()> {
        let progress = WorkerProgress::new(2, 2, 20)?;
        progress.publish_adder(0, 2)?;
        progress.publish_adder(1, 4)?;
        assert_eq!(progress.minimum_adder(), 2);
        progress.publish_adder(0, 4)?;
        assert_eq!(progress.minimum_adder(), 4);

        progress.publish_remover(0, 3)?;
        progress.publish_remover(1, 5)?;
        assert_eq!(progress.minimum_remover(), 3);
        progress.publish_remover(0, 5)?;
        assert_eq!(progress.minimum_remover(), 5);
        Ok(())
    }

    fn indexed_output() -> TxOut {
        TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }
    }
}
