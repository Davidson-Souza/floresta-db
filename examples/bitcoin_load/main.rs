// SPDX-License-Identifier: MIT OR Apache-2.0

//! Builds a `floresta-db` UTXO index and a Swift Sync hintsfile from Bitcoin Core block files.
//!
//! Blocks are read through `libbitcoinkernel`. Adders and removers claim small
//! block ranges independently; remover ranges wait on a condition variable until
//! every adder has published progress beyond the block being spent.

use std::error::Error as StdError;
use std::fs::File;
use std::io::{self, BufWriter, Write};
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

const OUTPOINT_KEY_SIZE: usize = 12;
const OUTPUT_INDEX_SIZE: usize = 4;
const MAX_SCRIPT_SIZE: usize = 10_000;
const DEFAULT_BUCKETS: u64 = 1 << 20;
const DEFAULT_CAPACITY_GIB: u64 = 64;
const DEFAULT_BLOCK_MIB: u64 = 1;
const DEFAULT_RANGE_SIZE: u64 = 32;
const HINTS_PROGRESS_INTERVAL: u64 = 10_000;
const UNPROCESSED_COUNT: u32 = u32::MAX;
const BIP30_UNSPENDABLE_HEIGHTS: [u64; 2] = [91_722, 91_812];

// Kernel and hintsfile errors are both Send + Sync, so worker failures can cross scoped threads.
type AnyError = Box<dyn StdError + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

fn main() {
    if let Err(error) = run() {
        eprintln!("bitcoin load test failed: {error}");
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
    let progress = AdderProgress::new(arguments.add_threads, end_height)?;

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
    let hints_path = arguments.work_dir.join("swiftsync.hints");
    let hints_started = Instant::now();
    let hints = write_hintsfile(
        &chainman,
        &database,
        tip_height,
        arguments.network,
        &counts,
        &offsets,
        &hints_path,
    )?;
    let hints_elapsed = hints_started.elapsed();

    let live = live_outputs.load(Ordering::Acquire);
    if live != hints.unspent_outputs {
        return Err(io::Error::other(format!(
            "live output count mismatch: database={live}, hints={}",
            hints.unspent_outputs
        ))
        .into());
    }

    database.close()?;
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
    progress: &AdderProgress,
    block_counts: &[AtomicU32],
    live_outputs: &AtomicU64,
) -> AnyResult<WorkerStats> {
    let mut stats = WorkerStats::default();
    while let Some(range) = ranges.claim() {
        progress.check_abort()?;
        let range_start = range.start;
        let range_end = range.end;
        log_progress(format_args!(
            "stage=add event=range_claim worker={worker} start={range_start} end={}",
            range_end.saturating_sub(1)
        ));
        for height in range {
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
            progress.publish(worker, completed)?;
        }
        log_progress(format_args!(
            "stage=add event=range_complete worker={worker} start={range_start} end={} completed_blocks={}",
            range_end.saturating_sub(1),
            stats.blocks
        ));
    }
    progress.finish(worker)?;
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
    progress: &AdderProgress,
    live_outputs: &AtomicU64,
) -> AnyResult<WorkerStats> {
    let mut stats = WorkerStats::default();
    loop {
        progress.check_abort()?;
        let safe_height = progress.minimum();
        if let Some(range) = ranges.claim_up_to(safe_height) {
            let range_start = range.start;
            let range_end = range.end;
            log_progress(format_args!(
                "stage=remove event=range_claim worker={worker} start={range_start} end={} safe_height={safe_height}",
                range_end.saturating_sub(1)
            ));
            for height in range {
                let read_started = Instant::now();
                let (block, bytes) = read_block(chainman, height)?;
                stats.block_read += read_started.elapsed();
                stats.bytes = stats.bytes.saturating_add(bytes);

                let delete_started = Instant::now();
                let keys = spent_outpoint_keys(&block)?;
                let deleted =
                    database.batch_delete(keys.iter().map(<[u8; OUTPOINT_KEY_SIZE]>::as_slice))?;
                if let Some(missing) = deleted.iter().position(|deleted| !deleted) {
                    return Err(io::Error::other(format!(
                        "missing spent outpoint at height {height}, input {missing}"
                    ))
                    .into());
                }
                let count = u64::try_from(deleted.len())
                    .map_err(|_| invalid_input("deleted input count does not fit u64"))?;
                cas_sub(live_outputs, count)?;
                stats.database += delete_started.elapsed();
                stats.blocks = stats.blocks.saturating_add(1);
                stats.inputs = stats.inputs.saturating_add(count);
            }
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
        let required = next
            .checked_add(1)
            .ok_or_else(|| invalid_input("remover progress overflow"))?;
        if progress.minimum() < required {
            log_progress(format_args!(
                "stage=remove event=wait worker={worker} next_height={next} safe_height={}",
                progress.minimum()
            ));
            progress.wait_until(required)?;
        } else {
            std::hint::spin_loop();
        }
    }
    log_progress(format_args!(
        "stage=remove event=tip worker={worker} completed_blocks={}",
        stats.blocks
    ));
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
            value: eligible_index.to_le_bytes(),
        });
        *eligible_index = eligible_index
            .checked_add(1)
            .ok_or_else(|| invalid_input("eligible output index overflow"))?;
    }
    Ok(())
}

fn spent_outpoint_keys(block: &Block) -> AnyResult<Vec<[u8; OUTPOINT_KEY_SIZE]>> {
    let input_capacity = block
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
    let mut keys = Vec::new();
    keys.try_reserve_exact(input_capacity)
        .map_err(|_| Error::OutOfMemory)?;
    for transaction in &block.txdata {
        if transaction.is_coinbase() {
            continue;
        }
        for input in &transaction.input {
            keys.push(outpoint_key(input.previous_output));
        }
    }
    Ok(keys)
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
    key[..8].copy_from_slice(&txid[..8]);
    key[8..].copy_from_slice(&outpoint.vout.to_le_bytes());
    key
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
    chainman: &ChainstateManager,
    database: &Database,
    tip_height: u64,
    network: ChainType,
    counts: &[u32],
    offsets: &[u64],
    path: &Path,
) -> AnyResult<HintsStats> {
    let total = offsets
        .last()
        .copied()
        .ok_or_else(|| invalid_input("block offsets are empty"))?;
    let total_usize = usize::try_from(total)
        .map_err(|_| invalid_input("global output count does not fit memory"))?;
    let mut live = Vec::<bool>::new();
    live.try_reserve_exact(total_usize)
        .map_err(|_| Error::OutOfMemory)?;
    live.resize(total_usize, false);
    let mut unspent_outputs = 0_u64;
    log_progress(format_args!(
        "stage=hints event=scan_start tip={tip_height} eligible_outputs={total}"
    ));

    for height in 0..=tip_height {
        if height == 0 || height == tip_height || height % HINTS_PROGRESS_INTERVAL == 0 {
            log_progress(format_args!(
                "stage=hints event=scan_progress height={height} tip={tip_height}"
            ));
        }
        let (block, _bytes) = read_block(chainman, height)?;
        let outputs = indexed_outputs(&block, height, network)?;
        let height_index =
            usize::try_from(height).map_err(|_| invalid_input("height does not fit memory"))?;
        let expected = counts
            .get(height_index)
            .copied()
            .ok_or_else(|| invalid_input("eligible output count is unavailable"))?;
        if outputs.len()
            != usize::try_from(expected)
                .map_err(|_| invalid_input("eligible output count does not fit usize"))?
        {
            return Err(io::Error::other(format!(
                "eligible output recount mismatch at height {height}"
            ))
            .into());
        }
        let values = database.batch_fetch(outputs.iter().map(|output| output.key.as_slice()))?;
        let block_offset = offsets
            .get(height_index)
            .copied()
            .ok_or_else(|| invalid_input("block offset is unavailable"))?;
        let block_end = offsets
            .get(height_index + 1)
            .copied()
            .ok_or_else(|| invalid_input("next block offset is unavailable"))?;
        for (output, value) in outputs.iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            let index = decode_output_index(&value)?;
            if index.to_le_bytes() != output.value {
                return Err(io::Error::other(format!(
                    "truncated outpoint collision at height {height}"
                ))
                .into());
            }
            let global = block_offset
                .checked_add(u64::from(index))
                .ok_or_else(|| invalid_input("global output index overflow"))?;
            if global >= block_end {
                return Err(io::Error::other("output index exceeds its block range").into());
            }
            let bit = live
                .get_mut(
                    usize::try_from(global)
                        .map_err(|_| invalid_input("global output index does not fit usize"))?,
                )
                .ok_or_else(|| invalid_input("global output index is out of range"))?;
            if *bit {
                return Err(io::Error::other("duplicate global output hint").into());
            }
            *bit = true;
            unspent_outputs = unspent_outputs
                .checked_add(1)
                .ok_or_else(|| invalid_input("unspent output count overflow"))?;
        }
    }
    log_progress(format_args!(
        "stage=hints event=scan_complete unspent_outputs={unspent_outputs}"
    ));

    log_progress(format_args!(
        "stage=hints event=encode_start path={}",
        path.display()
    ));
    encode_hintsfile(path, tip_height, offsets, &live)?;
    log_progress(format_args!(
        "stage=hints event=complete path={} bytes={}",
        path.display(),
        std::fs::metadata(path)?.len()
    ));

    Ok(HintsStats {
        eligible_outputs: total,
        unspent_outputs,
        file_bytes: std::fs::metadata(path)?.len(),
    })
}

fn encode_hintsfile(path: &Path, tip_height: u64, offsets: &[u64], live: &[bool]) -> AnyResult<()> {
    let stop_height =
        u32::try_from(tip_height).map_err(|_| invalid_input("hintsfile height exceeds u32"))?;
    let writer = BufWriter::new(File::create(path)?);
    let builder = HintsfileBuilder::new(writer);
    let mut builder = builder.initialize(stop_height)?;
    // hintsfile 0.1 encodes heights 1..=stop_height; genesis has no eligible outputs.
    for height in 1..=stop_height {
        if height == 1 || height == stop_height || u64::from(height) % HINTS_PROGRESS_INTERVAL == 0
        {
            log_progress(format_args!(
                "stage=hints event=encode_progress height={height} tip={stop_height}"
            ));
        }
        let height_index = usize::try_from(height)
            .map_err(|_| invalid_input("hintsfile height does not fit usize"))?;
        let start = usize::try_from(offsets[height_index])
            .map_err(|_| invalid_input("block offset does not fit usize"))?;
        let end = usize::try_from(offsets[height_index + 1])
            .map_err(|_| invalid_input("next block offset does not fit usize"))?;
        let mut indices = Vec::new();
        indices
            .try_reserve_exact(end.saturating_sub(start))
            .map_err(|_| Error::OutOfMemory)?;
        for (index, present) in live[start..end].iter().enumerate() {
            if *present {
                indices.push(
                    u32::try_from(index)
                        .map_err(|_| invalid_input("block output index exceeds u32"))?,
                );
            }
        }
        builder.append(EliasFano::compress(&indices))?;
    }
    builder.finish()?;
    Ok(())
}

fn decode_output_index(bytes: &[u8]) -> AnyResult<u32> {
    let bytes: [u8; OUTPUT_INDEX_SIZE] = bytes
        .try_into()
        .map_err(|_| io::Error::other("stored output index has the wrong width"))?;
    Ok(u32::from_le_bytes(bytes))
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
    let mut config = Config::new(Mode::Map, arguments.buckets, OUTPOINT_KEY_SIZE);
    config.inline_value_size = OUTPUT_INDEX_SIZE;
    config.block_size = arguments.database_block_bytes;
    config.body_capacity = arguments.body_capacity;
    config.blob_capacity = 0;
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
            let end = start.saturating_add(self.size).min(limit);
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

    fn is_finished(&self) -> bool {
        self.next_height() >= self.end
    }
}

struct AdderProgress {
    heights: Box<[AtomicU64]>,
    mutex: Mutex<()>,
    changed: Condvar,
    aborted: AtomicU64,
    end: u64,
}

impl AdderProgress {
    fn new(workers: usize, end: u64) -> AnyResult<Self> {
        if workers == 0 {
            return Err(invalid_input("adder thread count must be nonzero").into());
        }
        let mut heights = Vec::new();
        heights
            .try_reserve_exact(workers)
            .map_err(|_| Error::OutOfMemory)?;
        for _worker in 0..workers {
            heights.push(AtomicU64::new(0));
        }
        Ok(Self {
            heights: heights.into_boxed_slice(),
            mutex: Mutex::new(()),
            changed: Condvar::new(),
            aborted: AtomicU64::new(0),
            end,
        })
    }

    fn publish(&self, worker: usize, completed: u64) -> io::Result<()> {
        let _guard = self
            .mutex
            .lock()
            .map_err(|_| io::Error::other("adder progress mutex is poisoned"))?;
        let height = self
            .heights
            .get(worker)
            .ok_or_else(|| io::Error::other("adder progress index is out of range"))?;
        let mut old = height.load(Ordering::Acquire);
        loop {
            if completed < old || completed > self.end {
                return Err(io::Error::other("adder progress is not monotonic"));
            }
            match height.compare_exchange(old, completed, Ordering::Release, Ordering::Acquire) {
                Ok(_) => break,
                Err(actual) => old = actual,
            }
        }
        self.changed.notify_all();
        Ok(())
    }

    fn finish(&self, worker: usize) -> io::Result<()> {
        self.publish(worker, self.end)
    }

    fn minimum(&self) -> u64 {
        let mut minimum = self.end;
        for height in &self.heights {
            minimum = minimum.min(height.load(Ordering::Acquire));
        }
        minimum
    }

    fn wait_until(&self, required: u64) -> io::Result<()> {
        let mut guard = self
            .mutex
            .lock()
            .map_err(|_| io::Error::other("adder progress mutex is poisoned"))?;
        loop {
            self.check_abort()?;
            if self.minimum() >= required {
                return Ok(());
            }
            guard = self
                .changed
                .wait(guard)
                .map_err(|_| io::Error::other("adder progress mutex is poisoned"))?;
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
        let body_gib = environment_u64("DB_LOAD_BODY_GIB", DEFAULT_CAPACITY_GIB)?;
        let block_mib = environment_u64("DB_LOAD_BLOCK_MIB", DEFAULT_BLOCK_MIB)?;

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
                || PathBuf::from(format!("bitcoin-load-{}", std::process::id())),
                PathBuf::from,
            ),
            buckets: environment_u64("DB_LOAD_BUCKETS", DEFAULT_BUCKETS)?,
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
        "Usage: bitcoin-load DATA_DIR BLOCKS_DIR [mainnet|testnet|testnet4|signet|regtest] \
         [TIP|tip] [ADD_THREADS] [REMOVE_THREADS] [RANGE_SIZE] [WORK_DIR]\n\
         Environment: DB_LOAD_BUCKETS DB_LOAD_BODY_GIB DB_LOAD_BLOCK_MIB"
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
        assert_eq!(&key[..8], &[0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(&key[8..], &[4, 3, 2, 1]);
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
        assert_eq!(indexed[0].value, 0_u32.to_le_bytes());
        assert_eq!(indexed[1].value, 1_u32.to_le_bytes());

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
    fn encodes_folded_block_hints_with_hintsfile_crate() -> AnyResult<()> {
        let mut encoded = Vec::new();
        {
            let builder = HintsfileBuilder::new(&mut encoded);
            let mut builder = builder.initialize(2)?;
            builder.append(EliasFano::compress(&[0, 3]))?;
            builder.append(EliasFano::compress(&[1]))?;
            builder.finish()?;
        }
        let hints = hintsfile::Hintsfile::from_reader(&mut encoded.as_slice())?;
        assert_eq!(hints.stop_height(), 2);
        assert_eq!(hints.indices_at_height(1), Some(vec![0, 3]));
        assert_eq!(hints.indices_at_height(2), Some(vec![1]));
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
    fn remover_claims_every_currently_safe_block_before_waiting() -> AnyResult<()> {
        let ranges = RangeAllocator::new(10, 4)?;
        assert_eq!(ranges.claim_up_to(0), None);
        assert_eq!(ranges.claim_up_to(2), Some(0..2));
        assert_eq!(ranges.claim_up_to(2), None);
        assert_eq!(ranges.claim_up_to(7), Some(2..6));
        assert_eq!(ranges.claim_up_to(7), Some(6..7));
        assert_eq!(ranges.claim_up_to(7), None);
        assert_eq!(ranges.claim_up_to(10), Some(7..10));
        assert!(ranges.is_finished());
        Ok(())
    }

    #[test]
    fn remover_waits_for_minimum_adder_height() -> AnyResult<()> {
        let progress = AdderProgress::new(2, 10)?;
        std::thread::scope(|scope| -> AnyResult<()> {
            let (sender, receiver) = mpsc::channel();
            let progress_ref = &progress;
            let waiter = scope.spawn(move || -> io::Result<()> {
                progress_ref.wait_until(3)?;
                sender
                    .send(())
                    .map_err(|_| io::Error::other("progress test receiver dropped"))
            });

            progress.publish(0, 3)?;
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            progress.publish(1, 3)?;
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

    fn indexed_output() -> TxOut {
        TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }
    }
}
