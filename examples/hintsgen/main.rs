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
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use bitcoin::consensus::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Block, OutPoint, TxOut};
use bitcoinkernel::{ChainType, ChainstateManager, Context, ContextBuilder, Log, Logger};
use floresta_db::{BatchPopStats, Config, Database, DatabaseStats, Error, Mode, PageStats};
use hintsfile::{EliasFano, HintsfileBuilder};
use memmap2::MmapOptions;

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
const BODY_DATA_START: usize = 65_536;
const BODY_NODE_SIZE: usize = 32;
const BODY_KEY_OFFSET: usize = 0;
const BODY_VALUE_OFFSET: usize = 16;
const BODY_POINTER_OFFSET: usize = 24;
const BODY_CHECKSUM_SHIFT: u32 = 48;
const BODY_VALUE_BLOB_TAG: u64 = 1 << 63;
const BODY_NODE_CHECKSUM_SEED: u64 = 0x4341_534e_4f44_4532;
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
    let chainman = open_chainstate(&arguments, &context)?;
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
    let index_config = database_config(&arguments, tip_height)?;
    let database = Database::create(&index_path, index_config.clone())?;
    let instrumentation_dir = arguments.work_dir.join("instrumentation");
    std::fs::create_dir(&instrumentation_dir)?;
    write_mapping_manifest(&instrumentation_dir.join("mappings.csv"), &index_path)?;
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
    let (adder_stats, remover_stats) = std::thread::scope(|scope| -> AnyResult<_> {
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
            removers.push(scope.spawn(move || {
                let result = remove_worker(
                    worker,
                    chainman_ref,
                    database_ref,
                    ranges_ref,
                    progress_ref,
                    live_ref,
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
        for handle in removers {
            let stats = handle
                .join()
                .map_err(|_| io::Error::other("remover thread panicked"))??;
            remover_stats.merge(stats);
        }
        Ok((adder_stats, remover_stats))
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
    let before_compaction = database.stats()?;
    let storage_before_compaction = storage_stats(&index_path)?;
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
    let after_compaction = database.stats()?;
    let storage_after_compaction = storage_stats(&index_path)?;
    write_database_instrumentation(
        &instrumentation_dir,
        &before_compaction,
        &after_compaction,
        storage_before_compaction,
        storage_after_compaction,
        &remover_stats,
    )?;
    database.close()?;
    log_progress(format_args!("stage=index event=close_complete"));

    let sort_workers = arguments
        .add_threads
        .checked_add(arguments.remove_threads)
        .ok_or_else(|| invalid_input("offline sort worker count overflow"))?;
    let hints_path = arguments.work_dir.join("swiftsync.hints");
    let hints_started = Instant::now();
    let hints = write_hintsfile(
        &index_path,
        &index_config,
        sort_workers,
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

#[derive(Clone, Default)]
struct KernelDiagnostics {
    latest: Arc<Mutex<String>>,
}

impl Log for KernelDiagnostics {
    fn log(&self, message: &str) {
        let message = message.trim();
        if !message.is_empty()
            && let Ok(mut latest) = self.latest.lock()
        {
            message.clone_into(&mut latest);
        }
    }
}

impl KernelDiagnostics {
    fn latest(&self) -> Option<String> {
        let message = self.latest.lock().ok()?;
        if message.is_empty() {
            None
        } else {
            Some(message.clone())
        }
    }
}

fn open_chainstate(arguments: &Arguments, context: &Context) -> AnyResult<ChainstateManager> {
    let data_dir = path_text(&arguments.data_dir, "data directory")?;
    let blocks_dir = path_text(&arguments.blocks_dir, "blocks directory")?;
    let diagnostics = KernelDiagnostics::default();
    let logger = Logger::new(diagnostics.clone())?;
    let result = ChainstateManager::builder(context, data_dir, blocks_dir)?
        .worker_threads(0)
        .build();
    drop(logger);

    result.map_err(|error| {
        io::Error::other(chainstate_failure_message(
            &error.to_string(),
            diagnostics.latest().as_deref(),
            arguments.network,
            &arguments.data_dir,
            &arguments.blocks_dir,
        ))
        .into()
    })
}

fn chainstate_failure_message(
    error: &str,
    kernel_detail: Option<&str>,
    network: ChainType,
    data_dir: &Path,
    blocks_dir: &Path,
) -> String {
    let mut message = error.to_owned();
    if let Some(detail) = kernel_detail
        && !message.contains(detail)
    {
        message = format!("{message} Kernel detail: {detail}");
    }
    if network == ChainType::Testnet
        && data_dir.file_name().and_then(|name| name.to_str()) == Some("testnet")
    {
        let corrected_data = data_dir.with_file_name("testnet3");
        let corrected_blocks = if blocks_dir == data_dir.join("blocks") {
            corrected_data.join("blocks")
        } else {
            blocks_dir.to_path_buf()
        };
        message = format!(
            "{message} Bitcoin Core stores testnet3 in `testnet3`, not `testnet`; retry with \
             DATA_DIR={} BLOCKS_DIR={} and network `testnet3`.",
            corrected_data.display(),
            corrected_blocks.display()
        );
    }
    message
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
) -> AnyResult<WorkerStats> {
    let mut stats = WorkerStats::default();
    let mut keys = Vec::new();
    let mut input_ranges = Vec::new();
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
            let pop_stats = pop_spent_outputs(database, &keys, &input_ranges)?;
            cas_sub(live_outputs, pop_stats.pops)?;
            stats.database += pop_started.elapsed();
            stats.inputs = stats.inputs.saturating_add(pop_stats.pops);
            stats.nodes_followed = stats
                .nodes_followed
                .saturating_add(pop_stats.nodes_followed);
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
    progress.finish_remover(worker)?;
    log_progress(format_args!(
        "stage=remove event=tip worker={worker} completed_blocks={}",
        stats.blocks
    ));
    Ok(stats)
}

fn pop_spent_outputs(
    database: &Database,
    keys: &[[u8; OUTPOINT_KEY_SIZE]],
    input_ranges: &[(u64, usize, usize)],
) -> AnyResult<BatchPopStats> {
    let (popped, stats) =
        database.batch_pop_with_stats(keys.iter().map(<[u8; OUTPOINT_KEY_SIZE]>::as_slice))?;
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
        if value.len() != OUTPUT_INDEX_SIZE {
            return Err(Error::Corrupt("stored output position has the wrong width").into());
        }
    }
    Ok(stats)
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
    index_path: &Path,
    config: &Config,
    sort_workers: usize,
    tip_height: u64,
    counts: &[u32],
    offsets: &[u64],
    path: &Path,
) -> AnyResult<HintsStats> {
    let eligible_outputs = offsets
        .last()
        .copied()
        .ok_or_else(|| invalid_input("block offsets are empty"))?;
    let body_path = index_path.join("body");
    log_progress(format_args!(
        "stage=hints event=sort_start workers={sort_workers} eligible_outputs={eligible_outputs} body={}",
        body_path.display()
    ));
    let unspent_outputs =
        destructive_sort_body_values(&body_path, config.block_size, sort_workers)?;
    log_progress(format_args!(
        "stage=hints event=sort_complete unspent_outputs={unspent_outputs} body_bytes={}",
        std::fs::metadata(&body_path)?.len()
    ));

    log_progress(format_args!(
        "stage=hints event=encode_start path={}",
        path.display()
    ));
    let encoded_outputs = encode_sorted_hintsfile(path, &body_path, tip_height, counts, offsets)?;
    if encoded_outputs != unspent_outputs {
        return Err(io::Error::other(format!(
            "sorted output count mismatch: sort={unspent_outputs}, encoded={encoded_outputs}"
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

fn destructive_sort_body_values(
    body_path: &Path,
    block_size: u64,
    workers: usize,
) -> AnyResult<u64> {
    if workers == 0 {
        return Err(invalid_input("offline sort requires at least one worker").into());
    }
    let block_size = usize::try_from(block_size)
        .map_err(|_| invalid_input("database block size does not fit memory"))?;
    if block_size == 0 || block_size % BODY_NODE_SIZE != 0 {
        return Err(invalid_input("database block size does not contain whole body nodes").into());
    }
    let body_file = File::open(body_path)?;
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
    let active_workers = workers.min(block_count.max(1));
    let blocks_per_worker = block_count.div_ceil(active_workers);
    let run_directory = body_path
        .parent()
        .ok_or_else(|| invalid_input("body file has no parent directory"))?;

    let runs = std::thread::scope(|scope| -> AnyResult<Vec<SortRun>> {
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(active_workers)
            .map_err(|_| Error::OutOfMemory)?;
        for worker in 0..active_workers {
            let start_block = worker.saturating_mul(blocks_per_worker);
            let end_block = start_block
                .saturating_add(blocks_per_worker)
                .min(block_count);
            let mapping_ref = &mapping;
            handles.push(scope.spawn(move || {
                scan_body_runs(
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
                .map_err(|_| Error::OutOfMemory)?;
            runs.extend(worker_runs);
        }
        Ok(runs)
    })?;
    drop(mapping);
    drop(body_file);

    let mut live_values = 0_u64;
    for run in &runs {
        live_values = live_values
            .checked_add(run.values)
            .ok_or_else(|| invalid_input("live output count overflow"))?;
    }
    merge_sort_runs(body_path, &runs, live_values)?;
    Ok(live_values)
}

fn scan_body_runs(
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
        .map_err(|_| Error::OutOfMemory)?;
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
            let pointer = read_body_u64(slot, BODY_POINTER_OFFSET)?;
            let checksum = u16::try_from(pointer >> BODY_CHECKSUM_SHIFT)
                .map_err(|_| io::Error::other("body node checksum overflow"))?;
            if checksum == 0 {
                continue;
            }
            let value = read_body_u64(slot, BODY_VALUE_OFFSET)?;
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
            if checksum != body_node_checksum(key, value) {
                return Err(io::Error::other(format!(
                    "body node checksum mismatch in block {block}, slot {slot_index}"
                ))
                .into());
            }
            values.push(value);
            if values.len() == SORT_RUN_VALUE_CAPACITY {
                runs.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
                runs.push(write_sort_run(
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
    if !values.is_empty() {
        runs.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        runs.push(write_sort_run(
            run_directory,
            worker,
            run_index,
            &mut values,
        )?);
    }
    Ok(runs)
}

fn body_node_checksum(key: &[u8; OUTPOINT_KEY_SIZE], value: u64) -> u16 {
    let checksum = floresta_db::xxh64(key, BODY_NODE_CHECKSUM_SEED) ^ value.rotate_left(29);
    let folded = checksum ^ (checksum >> 16) ^ (checksum >> 32) ^ (checksum >> 48);
    let bytes = folded.to_le_bytes();
    u16::from_le_bytes([bytes[0], bytes[1]]).max(1)
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

fn merge_sort_runs(body_path: &Path, runs: &[SortRun], expected_values: u64) -> AnyResult<()> {
    let output_path = body_path.with_extension("sorted");
    let mut output = TemporarySortOutput::create(output_path)?;
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

fn encode_sorted_hintsfile(
    path: &Path,
    sorted_body_path: &Path,
    tip_height: u64,
    counts: &[u32],
    offsets: &[u64],
) -> AnyResult<u64> {
    let stop_height =
        u32::try_from(tip_height).map_err(|_| invalid_input("hintsfile height exceeds u32"))?;
    let mut values = ValueReader::open_flat(sorted_body_path)?;
    let mut next = values.next_value()?;
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
        let block_offset = offsets
            .get(height_index)
            .copied()
            .ok_or_else(|| invalid_input("block offset is unavailable"))?;
        let block_end = offsets
            .get(height_index + 1)
            .copied()
            .ok_or_else(|| invalid_input("next block offset is unavailable"))?;

        while let Some(position) = next {
            let (position_height, block_index) = unpack_output_position(position);
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
            next = values.next_value()?;
        }
        builder.append(EliasFano::compress(&indices))?;
    }
    if next.is_some() {
        return Err(io::Error::other("sorted body contains output above the requested tip").into());
    }
    builder.finish()?;
    Ok(encoded_outputs)
}

fn read_body_u64(slot: &[u8], offset: usize) -> AnyResult<u64> {
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
    nodes_followed: u64,
    block_read: Duration,
    database: Duration,
}

impl WorkerStats {
    fn merge(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.outputs = self.outputs.saturating_add(other.outputs);
        self.inputs = self.inputs.saturating_add(other.inputs);
        self.nodes_followed = self.nodes_followed.saturating_add(other.nodes_followed);
        self.block_read += other.block_read;
        self.database += other.database;
    }
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

#[cfg(target_os = "linux")]
fn write_mapping_manifest(path: &Path, database_path: &Path) -> AnyResult<()> {
    let database_path = std::fs::canonicalize(database_path)?;
    let database_text = path_text(&database_path, "database directory")?;
    let maps = std::fs::read_to_string("/proc/self/maps")?;
    let mut output = BufWriter::new(File::create(path)?);
    writeln!(output, "pid,start,end,file_offset,file")?;
    for line in maps.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        let Some(mapped_path) = fields.get(5) else {
            continue;
        };
        if !mapped_path.starts_with(database_text) {
            continue;
        }
        let range = fields
            .first()
            .ok_or_else(|| io::Error::other("process mapping has no address range"))?;
        let (start, end) = range
            .split_once('-')
            .ok_or_else(|| io::Error::other("process mapping range is invalid"))?;
        let file_offset = fields
            .get(2)
            .ok_or_else(|| io::Error::other("process mapping has no file offset"))?;
        writeln!(
            output,
            "{},0x{start},0x{end},0x{file_offset},{}",
            std::process::id(),
            csv_text(mapped_path)
        )?;
    }
    output.flush()?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn write_mapping_manifest(path: &Path, _database_path: &Path) -> AnyResult<()> {
    let mut output = BufWriter::new(File::create(path)?);
    writeln!(output, "pid,start,end,file_offset,file")?;
    output.flush()?;
    Ok(())
}

fn write_database_instrumentation(
    directory: &Path,
    before: &DatabaseStats,
    after: &DatabaseStats,
    storage_before: StorageStats,
    storage_after: StorageStats,
    removers: &WorkerStats,
) -> AnyResult<()> {
    write_metrics_summary(
        &directory.join("summary.csv"),
        before,
        after,
        storage_before,
        storage_after,
        removers,
    )?;
    write_page_metrics(&directory.join("page-usage.csv"), before, after)?;
    write_page_usage_svg(&directory.join("page-usage.svg"), before, after)?;
    let traversal = BatchPopStats {
        pops: removers.inputs,
        nodes_followed: removers.nodes_followed,
    };
    log_progress(format_args!(
        "stage=instrumentation event=database_metrics body_pages={} body_used_pages={} average_body_page_usage={:.4} database_logical_bytes={} database_allocated_bytes={} average_nodes_followed_per_pop={:.3}",
        after.body.high_water_pages,
        after.body.used_pages,
        after.average_body_page_usage,
        after.total_file_bytes,
        storage_after.allocated_bytes,
        traversal.average_nodes_followed_per_pop()
    ));
    Ok(())
}

fn write_metrics_summary(
    path: &Path,
    before: &DatabaseStats,
    after: &DatabaseStats,
    storage_before: StorageStats,
    storage_after: StorageStats,
    removers: &WorkerStats,
) -> AnyResult<()> {
    let traversal = BatchPopStats {
        pops: removers.inputs,
        nodes_followed: removers.nodes_followed,
    };
    let mut output = BufWriter::new(File::create(path)?);
    writeln!(output, "metric,before_compaction,after_compaction")?;
    writeln!(
        output,
        "database_logical_bytes,{},{}",
        before.total_file_bytes, after.total_file_bytes
    )?;
    writeln!(
        output,
        "database_allocated_bytes,{},{}",
        storage_before.allocated_bytes, storage_after.allocated_bytes
    )?;
    writeln!(
        output,
        "heads_file_bytes,{},{}",
        before.heads_file_bytes, after.heads_file_bytes
    )?;
    write_allocator_summary(&mut output, "body", &before.body, &after.body)?;
    if let (Some(before_blobs), Some(after_blobs)) = (&before.blobs, &after.blobs) {
        write_allocator_summary(&mut output, "blobs", before_blobs, after_blobs)?;
    }
    writeln!(
        output,
        "average_body_page_usage,{:.8},{:.8}",
        before.average_body_page_usage, after.average_body_page_usage
    )?;
    writeln!(output, "pop_requests,{},{}", traversal.pops, traversal.pops)?;
    writeln!(
        output,
        "nodes_followed,{},{}",
        traversal.nodes_followed, traversal.nodes_followed
    )?;
    writeln!(
        output,
        "average_nodes_followed_per_pop,{0:.8},{0:.8}",
        traversal.average_nodes_followed_per_pop()
    )?;
    output.flush()?;
    Ok(())
}
fn write_allocator_summary(
    output: &mut impl Write,
    name: &str,
    before: &PageStats,
    after: &PageStats,
) -> io::Result<()> {
    writeln!(
        output,
        "{name}_page_size_bytes,{},{}",
        before.page_size_bytes, after.page_size_bytes
    )?;
    writeln!(
        output,
        "{name}_data_file_bytes,{},{}",
        before.data_file_bytes, after.data_file_bytes
    )?;
    writeln!(
        output,
        "{name}_count_file_bytes,{},{}",
        before.count_file_bytes, after.count_file_bytes
    )?;
    writeln!(
        output,
        "{name}_high_water_pages,{},{}",
        before.high_water_pages, after.high_water_pages
    )?;
    writeln!(
        output,
        "{name}_used_pages,{},{}",
        before.used_pages, after.used_pages
    )?;
    writeln!(
        output,
        "{name}_empty_pages,{},{}",
        before.empty_pages, after.empty_pages
    )?;
    writeln!(
        output,
        "{name}_claimed_pages,{},{}",
        before.claimed_pages, after.claimed_pages
    )?;
    writeln!(
        output,
        "{name}_claimed_but_unused_pages,{},{}",
        before.claimed_but_unused_pages, after.claimed_but_unused_pages
    )?;
    writeln!(
        output,
        "{name}_live_allocations,{},{}",
        before.live_allocations, after.live_allocations
    )?;
    writeln!(
        output,
        "{name}_average_page_load,{:.8},{:.8}",
        before.average_page_load, after.average_page_load
    )
}

fn write_page_metrics(path: &Path, before: &DatabaseStats, after: &DatabaseStats) -> AnyResult<()> {
    let mut output = BufWriter::new(File::create(path)?);
    writeln!(output, "phase,file,page,live_allocations")?;
    write_page_metric_rows(&mut output, "before_compaction", "body", &before.body)?;
    if let Some(blobs) = &before.blobs {
        write_page_metric_rows(&mut output, "before_compaction", "blobs", blobs)?;
    }
    write_page_metric_rows(&mut output, "after_compaction", "body", &after.body)?;
    if let Some(blobs) = &after.blobs {
        write_page_metric_rows(&mut output, "after_compaction", "blobs", blobs)?;
    }
    output.flush()?;
    Ok(())
}

fn write_page_metric_rows(
    output: &mut impl Write,
    phase: &str,
    file: &str,
    stats: &PageStats,
) -> io::Result<()> {
    for (page, live) in stats.page_loads.iter().copied().enumerate() {
        writeln!(output, "{phase},{file},{page},{live}")?;
    }
    Ok(())
}

fn write_page_usage_svg(
    path: &Path,
    before: &DatabaseStats,
    after: &DatabaseStats,
) -> AnyResult<()> {
    let mut panels = Vec::new();
    panels
        .try_reserve_exact(4)
        .map_err(|_| Error::OutOfMemory)?;
    panels.push((
        "body before compaction",
        &before.body,
        before.body_page_capacity,
    ));
    panels.push((
        "body after compaction",
        &after.body,
        after.body_page_capacity,
    ));
    if let Some(blobs) = &before.blobs {
        panels.push(("blobs before compaction", blobs, maximum_page_load(blobs)));
    }
    if let Some(blobs) = &after.blobs {
        panels.push(("blobs after compaction", blobs, maximum_page_load(blobs)));
    }

    let columns = 256_usize;
    let cell = 3_usize;
    let panel_heights = panels
        .iter()
        .map(|(_, stats, _)| stats.page_loads.len().div_ceil(columns).max(1) * cell + 52)
        .collect::<Vec<_>>();
    let height = panel_heights
        .iter()
        .try_fold(72_usize, |total, panel| total.checked_add(*panel))
        .ok_or_else(|| invalid_input("page heatmap height overflow"))?;
    let mut output = BufWriter::new(File::create(path)?);
    writeln!(
        output,
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="{height}" viewBox="0 0 1080 {height}">"#
    )?;
    writeln!(
        output,
        r##"<rect width="1080" height="{height}" fill="#0f172a"/>"##
    )?;
    writeln!(
        output,
        r##"<text x="28" y="38" fill="#f8fafc" font-family="monospace" font-size="22">floresta-db allocator page usage</text>"##
    )?;
    let mut y = 64_usize;
    for ((label, stats, capacity), panel_height) in panels.iter().zip(panel_heights) {
        write_heatmap_panel(&mut output, label, stats, *capacity, y, columns, cell)?;
        y = y
            .checked_add(panel_height)
            .ok_or_else(|| invalid_input("page heatmap position overflow"))?;
    }
    writeln!(output, "</svg>")?;
    output.flush()?;
    Ok(())
}

fn write_heatmap_panel(
    output: &mut impl Write,
    label: &str,
    stats: &PageStats,
    capacity: u64,
    y: usize,
    columns: usize,
    cell: usize,
) -> AnyResult<()> {
    writeln!(
        output,
        r##"<text x="28" y="{}" fill="#cbd5e1" font-family="monospace" font-size="15">{label}: pages={} used={} average_load={:.2}</text>"##,
        y + 16,
        stats.high_water_pages,
        stats.used_pages,
        stats.average_page_load
    )?;
    let grid_y = y + 28;
    for (page, live) in stats.page_loads.iter().copied().enumerate() {
        let x = 28 + page % columns * cell;
        let cell_y = grid_y + page / columns * cell;
        let color = heat_color(u64::from(live), capacity);
        writeln!(
            output,
            r#"<rect x="{x}" y="{cell_y}" width="{cell}" height="{cell}" fill="{color}"><title>{label} page {page}: {live} live allocations</title></rect>"#
        )?;
    }
    Ok(())
}

fn maximum_page_load(stats: &PageStats) -> u64 {
    stats
        .page_loads
        .iter()
        .copied()
        .map(u64::from)
        .max()
        .unwrap_or(1)
        .max(1)
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn heat_color(load: u64, capacity: u64) -> String {
    if load == 0 || capacity == 0 {
        return "#1e293b".to_owned();
    }
    #[allow(clippy::cast_precision_loss)]
    let intensity = ((load as f64 / capacity as f64).clamp(0.0, 1.0).sqrt() * 255.0) as u8;
    let red = 30_u8.saturating_add(intensity.saturating_mul(3) / 4);
    let green = 64_u8.saturating_add(intensity / 3);
    let blue = 175_u8.saturating_sub(intensity / 2);
    format!("#{red:02x}{green:02x}{blue:02x}")
}

fn csv_text(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
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
    fn explains_the_testnet3_data_directory_name() {
        let message = chainstate_failure_message(
            "Failed to create chainstate manager.",
            Some("Failed to load chain state from your data directory"),
            ChainType::Testnet,
            Path::new("/home/alice/.bitcoin/testnet"),
            Path::new("/home/alice/.bitcoin/testnet/blocks"),
        );
        assert!(message.contains("Failed to load chain state"));
        assert!(message.contains("DATA_DIR=/home/alice/.bitcoin/testnet3"));
        assert!(message.contains("BLOCKS_DIR=/home/alice/.bitcoin/testnet3/blocks"));
        assert!(message.contains("network `testnet3`"));

        let canonical = chainstate_failure_message(
            "Failed to create chainstate manager.",
            None,
            ChainType::Testnet,
            Path::new("/home/alice/.bitcoin/testnet3"),
            Path::new("/home/alice/.bitcoin/testnet3/blocks"),
        );
        assert!(!canonical.contains("retry with"));
    }

    #[test]
    fn writes_database_instrumentation_artifacts() -> AnyResult<()> {
        let directory = std::env::temp_dir().join(format!(
            "floresta-db-hints-instrumentation-{}",
            std::process::id()
        ));
        let _ignored = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory)?;
        let page_stats = |loads: Vec<u16>| PageStats {
            page_size_bytes: 64 * 1_024,
            data_file_bytes: 64 * 1_024 * 4,
            count_file_bytes: 64 * 1_024,
            high_water_pages: 3,
            used_pages: 2,
            empty_pages: 1,
            claimed_pages: 0,
            claimed_but_unused_pages: 0,
            live_allocations: 3,
            average_page_load: 1.5,
            page_loads: loads,
        };
        let before = DatabaseStats {
            heads_file_bytes: 64 * 1_024,
            body: page_stats(vec![2, 0, 1]),
            blobs: None,
            body_page_capacity: 2,
            average_body_page_usage: 0.75,
            total_file_bytes: 320 * 1_024,
        };
        let after = DatabaseStats {
            heads_file_bytes: before.heads_file_bytes,
            body: page_stats(vec![2, 1, 0]),
            blobs: None,
            body_page_capacity: 2,
            average_body_page_usage: 0.75,
            total_file_bytes: before.total_file_bytes,
        };
        let removers = WorkerStats {
            inputs: 2,
            nodes_followed: 5,
            ..WorkerStats::default()
        };
        write_database_instrumentation(
            &directory,
            &before,
            &after,
            StorageStats {
                files: 3,
                logical_bytes: before.total_file_bytes,
                allocated_bytes: 128 * 1_024,
            },
            StorageStats {
                files: 3,
                logical_bytes: after.total_file_bytes,
                allocated_bytes: 128 * 1_024,
            },
            &removers,
        )?;

        let summary = std::fs::read_to_string(directory.join("summary.csv"))?;
        assert!(summary.contains("average_nodes_followed_per_pop,2.50000000,2.50000000"));
        let pages = std::fs::read_to_string(directory.join("page-usage.csv"))?;
        assert!(pages.contains("before_compaction,body,1,0"));
        let heatmap = std::fs::read_to_string(directory.join("page-usage.svg"))?;
        assert!(heatmap.contains("body after compaction page 1: 1 live allocations"));

        std::fs::remove_dir_all(directory)?;
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
    fn encodes_sorted_live_body_positions() -> AnyResult<()> {
        let directory =
            std::env::temp_dir().join(format!("floresta-db-hints-encode-{}", std::process::id()));
        let _ignored = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory)?;
        let body_path = directory.join("body");
        let hints_path = directory.join("swiftsync.hints");
        let mut sorted = BufWriter::new(File::create(&body_path)?);
        for position in [
            pack_output_position(1, 0),
            pack_output_position(1, 2),
            pack_output_position(1, 3),
            pack_output_position(2, 1),
        ] {
            sorted.write_all(&position.to_le_bytes())?;
        }
        sorted.flush()?;
        drop(sorted);

        let counts = [0, 4, 2];
        let offsets = fold_output_counts(&counts)?;
        assert_eq!(
            encode_sorted_hintsfile(&hints_path, &body_path, 2, &counts, &offsets)?,
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
        let first = pack_output_position(2, 0);
        let removed = pack_output_position(1, 1);
        let last = pack_output_position(1, 0);
        database.put_new(b"key-0001-0000000", &first.to_le_bytes())?;
        database.put_new(b"key-0002-0000000", &removed.to_le_bytes())?;
        database.put_new(b"key-0003-0000000", &last.to_le_bytes())?;
        assert!(database.delete(b"key-0002-0000000")?);
        database.close()?;

        let body_path = index_path.join("body");
        assert_eq!(
            destructive_sort_body_values(&body_path, config.block_size, 2)?,
            2
        );
        let bytes = std::fs::read(&body_path)?;
        let mut actual = Vec::new();
        for value in bytes.chunks_exact(size_of::<u64>()) {
            let value: [u8; size_of::<u64>()] = value
                .try_into()
                .map_err(|_| io::Error::other("sorted test value has the wrong width"))?;
            actual.push(u64::from_le_bytes(value));
        }
        assert_eq!(actual, [last, first]);

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
