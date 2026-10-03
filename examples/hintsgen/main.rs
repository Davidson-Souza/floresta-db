// SPDX-License-Identifier: MIT OR Apache-2.0

//! Builds a `floresta-db` UTXO index and a Swift Sync hintsfile from Bitcoin Core block files.
//!
//! Blocks are read through `libbitcoinkernel`. Adders and removers claim small
//! block ranges independently; remover ranges wait on a condition variable until
//! every adder has published progress beyond the block being spent.
//!
//! The executable is split by lifecycle boundary: this module coordinates chain access and
//! concurrent indexing, `body` owns the destructive offline sort and hints encoding, and
//! `instrumentation` owns persistent metrics and operator reports. During indexing, each adder
//! publishes a monotonically increasing frontier. Removers only claim complete ranges below the
//! minimum frontier, so no input is removed before every possible producing block was indexed.

mod body;
mod instrumentation;

use body::{HintsWriter, OutputPosition};
use instrumentation::{Instrumentation, RunReport, StorageStats};
use std::error::Error as StdError;
use std::io::{self, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use bitcoin::consensus::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Block, OutPoint, TxOut};
use bitcoinkernel::{ChainType, ChainstateManager, Context, ContextBuilder, Log, Logger};
use floresta_db::{BatchPopStats, Config, Database, Error, Mode};

const OUTPOINT_KEY_SIZE: usize = 16;
const OUTPUT_INDEX_SIZE: usize = size_of::<u64>();
const MAX_SCRIPT_SIZE: usize = 10_000;
const DEFAULT_BUCKETS: u64 = 1 << 20;
const DEFAULT_CAPACITY_GIB: u64 = 64;
const DEFAULT_BLOCK_MIB: u64 = 1;
const DEFAULT_RANGE_SIZE: u64 = 32;
const MAX_ADDER_LEAD_BLOCKS: u64 = 4096;
const UNPROCESSED_COUNT: u32 = u32::MAX;
const BIP30_UNSPENDABLE_HEIGHTS: [u64; 2] = [91_722, 91_812];
const COMPACTION_MIN_PAGE_LOAD: u16 = 8_192;

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
    let block_counts = BlockCounts::new(end_height)?;

    let index_path = arguments.work_dir.join("index");
    let index_config = arguments.database_config(tip_height)?;
    let database = Database::create(&index_path, index_config.clone())?;
    let instrumentation = Instrumentation::create(&arguments.work_dir, &index_path)?;
    let live_outputs = LiveOutputs::default();
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
            let counts_ref = block_counts.as_slice();
            let live_ref = &live_outputs;
            let network = arguments.network;
            adders.push(scope.spawn(move || {
                let result = AddWorker {
                    network,
                    chainman: chainman_ref,
                    database: database_ref,
                    ranges: ranges_ref,
                    progress: progress_ref,
                    block_counts: counts_ref,
                    live_outputs: live_ref,
                }
                .run(worker);
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
                let result = RemoveWorker {
                    chainman: chainman_ref,
                    database: database_ref,
                    ranges: ranges_ref,
                    progress: progress_ref,
                    live_outputs: live_ref,
                }
                .run(worker);
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
        live_outputs.current()
    ));

    let counts = block_counts.collect()?;
    let offsets = BlockCounts::offsets(&counts)?;
    let live = live_outputs.current();
    let before_compaction = database.stats()?;
    let storage_before_compaction = StorageStats::read(&index_path)?;
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
    let storage_after_compaction = StorageStats::read(&index_path)?;
    instrumentation.write_database_snapshot(
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
    let hints = HintsWriter::new(
        &index_path,
        &index_config,
        sort_workers,
        tip_height,
        &counts,
        &offsets,
        &hints_path,
    )
    .write()?;
    let hints_elapsed = hints_started.elapsed();
    if live != hints.unspent_outputs {
        return Err(io::Error::other(format!(
            "live output count mismatch: database={live}, hints={}",
            hints.unspent_outputs
        ))
        .into());
    }

    let elapsed = started.elapsed();
    let storage = StorageStats::read(&arguments.work_dir)?;
    RunReport {
        adders: &adder_stats,
        removers: &remover_stats,
        hints: &hints,
        elapsed,
        hints_elapsed,
        storage,
    }
    .print();
    Ok(())
}

/// Captures the latest libbitcoinkernel diagnostic while a manager is being constructed.
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

/// Immutable dependencies used by one adder thread.
struct AddWorker<'shared> {
    network: ChainType,
    chainman: &'shared ChainstateManager,
    database: &'shared Database,
    ranges: &'shared RangeAllocator,
    progress: &'shared WorkerProgress,
    block_counts: &'shared [AtomicU32],
    live_outputs: &'shared LiveOutputs,
}

impl AddWorker<'_> {
    /// Claims block ranges, publishes eligible outputs, and advances the safe-height frontier.
    fn run(self, worker: usize) -> AnyResult<WorkerStats> {
        let Self {
            network,
            chainman,
            database,
            ranges,
            progress,
            block_counts,
            live_outputs,
        } = self;
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
                let outputs = IndexedOutput::from_block(&block, height, network)?;
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
                live_outputs.add(
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
}

/// Immutable dependencies used by one remover thread.
struct RemoveWorker<'shared> {
    chainman: &'shared ChainstateManager,
    database: &'shared Database,
    ranges: &'shared RangeAllocator,
    progress: &'shared WorkerProgress,
    live_outputs: &'shared LiveOutputs,
}

impl RemoveWorker<'_> {
    /// Waits for safe adder progress, removes spent outpoints, and records traversal work.
    fn run(self, worker: usize) -> AnyResult<WorkerStats> {
        let Self {
            chainman,
            database,
            ranges,
            progress,
            live_outputs,
        } = self;
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
                    Self::append_spent_keys(&block, &mut keys)?;
                    input_ranges.push((height, input_start, keys.len()));
                    stats.database += prepare_started.elapsed();
                    stats.blocks = stats.blocks.saturating_add(1);
                }

                let pop_started = Instant::now();
                let pop_stats = Self::pop(database, &keys, &input_ranges)?;
                live_outputs.subtract(pop_stats.pops)?;
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

    fn pop(
        database: &Database,
        keys: &[[u8; OUTPOINT_KEY_SIZE]],
        input_ranges: &[(u64, usize, usize)],
    ) -> AnyResult<BatchPopStats> {
        let (popped, stats) =
            database.batch_pop_with_stats(keys.iter().map(<[u8; OUTPOINT_KEY_SIZE]>::as_slice))?;
        for (input, value) in popped.into_iter().enumerate() {
            let Some(value) = value else {
                let Some((height, input_start, _input_end)) =
                    input_ranges.iter().find(|(_, input_start, input_end)| {
                        (*input_start..*input_end).contains(&input)
                    })
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
    /// Appends the compact keys of every non-coinbase input in one block.
    fn append_spent_keys(block: &Block, keys: &mut Vec<[u8; OUTPOINT_KEY_SIZE]>) -> AnyResult<()> {
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
                keys.push(IndexedOutput::key(input.previous_output));
            }
        }
        Ok(())
    }
}
/// Reads and decodes one active-chain block together with its serialized byte length.
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

impl IndexedOutput {
    /// Collects every hints-eligible output in a block and assigns its local index.
    fn from_block(block: &Block, height: u64, network: ChainType) -> AnyResult<Vec<Self>> {
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
            Self::append_transaction(
                height,
                network,
                transaction_index,
                transaction.compute_txid(),
                &transaction.output,
                &mut eligible_index,
                &mut outputs,
            )?;
        }
        Ok(outputs)
    }

    fn append_transaction(
        height: u64,
        network: ChainType,
        transaction_index: usize,
        txid: bitcoin::Txid,
        outputs: &[TxOut],
        eligible_index: &mut u32,
        indexed: &mut Vec<Self>,
    ) -> AnyResult<()> {
        let block_height =
            u32::try_from(height).map_err(|_| invalid_input("block height exceeds u32"))?;
        for (vout, output) in outputs.iter().enumerate() {
            if !Self::is_eligible(network, height, transaction_index, output) {
                continue;
            }
            let vout = u32::try_from(vout)
                .map_err(|_| invalid_input("transaction output index exceeds u32"))?;
            indexed.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
            indexed.push(Self {
                key: Self::key(OutPoint { txid, vout }),
                value: OutputPosition::new(block_height, *eligible_index).to_le_bytes(),
            });
            *eligible_index = eligible_index
                .checked_add(1)
                .ok_or_else(|| invalid_input("eligible output index overflow"))?;
        }
        Ok(())
    }
}

impl IndexedOutput {
    fn is_eligible(
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

    fn key(outpoint: OutPoint) -> [u8; OUTPOINT_KEY_SIZE] {
        let txid = outpoint.txid.to_byte_array();
        let mut key = [0_u8; OUTPOINT_KEY_SIZE];
        key[..12].copy_from_slice(&txid[..12]);
        key[12..].copy_from_slice(&outpoint.vout.to_le_bytes());
        key
    }
}

/// Per-height eligible-output counts published exactly once by adder workers.
struct BlockCounts {
    slots: Box<[AtomicU32]>,
}

impl BlockCounts {
    fn new(end_height: u64) -> AnyResult<Self> {
        let count = usize::try_from(end_height)
            .map_err(|_| invalid_input("block count does not fit memory"))?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(count)
            .map_err(|_| Error::OutOfMemory)?;
        for _height in 0..count {
            slots.push(AtomicU32::new(UNPROCESSED_COUNT));
        }
        Ok(Self {
            slots: slots.into_boxed_slice(),
        })
    }

    fn as_slice(&self) -> &[AtomicU32] {
        &self.slots
    }

    fn collect(&self) -> AnyResult<Vec<u32>> {
        let mut counts = Vec::new();
        counts
            .try_reserve_exact(self.slots.len())
            .map_err(|_| Error::OutOfMemory)?;
        for slot in &self.slots {
            let count = slot.load(Ordering::Acquire);
            if count == UNPROCESSED_COUNT {
                return Err(io::Error::other("an output-count slot was never published").into());
            }
            counts.push(count);
        }
        Ok(counts)
    }

    fn offsets(counts: &[u32]) -> AnyResult<Vec<u64>> {
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
}

impl Arguments {
    /// Builds the database layout and rejects obviously undersized body reservations.
    fn database_config(&self, tip_height: u64) -> AnyResult<Config> {
        let mut config = Config::new(Mode::Map, self.buckets);
        config.block_size = self.database_block_bytes;
        config.body_capacity = self.body_capacity;
        let minimum_body = tip_height
            .checked_add(1)
            .and_then(|blocks| blocks.checked_mul(96))
            .ok_or_else(|| invalid_input("minimum body capacity overflow"))?;
        if config.body_capacity < minimum_body {
            return Err(
                invalid_input("body capacity is too small for one output per block").into(),
            );
        }
        Ok(config)
    }
}

/// Lock-free monotonically claimed block ranges.
///
/// `claim_up_to` never returns a partial range: removers wait until the complete next range is
/// below the adder safety frontier.
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

/// Monotonic per-worker frontiers and condition-variable wakeups.
///
/// A minimum frontier is the only cross-pipeline ordering signal. Publication while holding
/// `mutex` prevents a waiter from missing the notification between checking and sleeping.
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
            adders: Self::progress_slots(adders)?,
            removers: Self::progress_slots(removers)?,
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
        Self::minimum_progress(&self.adders, self.end)
    }

    fn minimum_remover(&self) -> u64 {
        Self::minimum_progress(&self.removers, self.end)
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
}

/// Compact database entry prepared from one eligible Bitcoin output.
#[derive(Clone, Copy)]
struct IndexedOutput {
    key: [u8; OUTPOINT_KEY_SIZE],
    value: [u8; OUTPUT_INDEX_SIZE],
}

/// Aggregated work and timing counters from one worker class.
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

/// Checked atomic count of currently live database entries.
#[derive(Default)]
struct LiveOutputs {
    count: AtomicU64,
}

impl LiveOutputs {
    fn current(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }

    fn add(&self, amount: u64) -> io::Result<()> {
        let mut old = self.current();
        loop {
            let new = old
                .checked_add(amount)
                .ok_or_else(|| io::Error::other("live output counter overflow"))?;
            match self
                .count
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(actual) => old = actual,
            }
        }
    }

    fn subtract(&self, amount: u64) -> io::Result<()> {
        let mut old = self.current();
        loop {
            let new = old
                .checked_sub(amount)
                .ok_or_else(|| io::Error::other("live output counter underflow"))?;
            match self
                .count
                .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(actual) => old = actual,
            }
        }
    }
}

/// Validated command-line and environment configuration for one run.
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
            Self::print_usage();
            return Err(invalid_input("data and blocks directories are required").into());
        }
        let available = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let default_adders = available.div_ceil(2).max(1);
        let default_removers = available.saturating_sub(default_adders).max(1);
        let add_threads = Self::optional_usize(&arguments, 4, default_adders, "ADD_THREADS")?;
        let remove_threads =
            Self::optional_usize(&arguments, 5, default_removers, "REMOVE_THREADS")?;
        if add_threads == 0 || remove_threads == 0 {
            return Err(invalid_input("worker counts must be nonzero").into());
        }
        let range_size = Self::optional_u64(&arguments, 6, DEFAULT_RANGE_SIZE, "RANGE_SIZE")?;
        if range_size == 0 {
            return Err(invalid_input("range size must be nonzero").into());
        }
        let body_gib = Self::environment_u64("HINTSGEN_BODY_GIB", DEFAULT_CAPACITY_GIB)?;
        let block_mib = Self::environment_u64("HINTSGEN_BLOCK_MIB", DEFAULT_BLOCK_MIB)?;

        Ok(Self {
            data_dir: PathBuf::from(&arguments[0]),
            blocks_dir: PathBuf::from(&arguments[1]),
            network: arguments.get(2).map_or(Ok(ChainType::Mainnet), |network| {
                Self::parse_network(network)
            })?,
            tip_height: arguments.get(3).map_or(Ok(None), |tip| {
                if tip.eq_ignore_ascii_case("tip") {
                    Ok(None)
                } else {
                    Self::parse_u64(tip, "TIP").map(Some)
                }
            })?,
            add_threads,
            remove_threads,
            range_size,
            work_dir: arguments.get(7).map_or_else(
                || PathBuf::from(format!("hintsgen-{}", std::process::id())),
                PathBuf::from,
            ),
            buckets: Self::environment_u64("HINTSGEN_BUCKETS", DEFAULT_BUCKETS)?,
            body_capacity: Self::gibibytes(body_gib)?,
            database_block_bytes: Self::mebibytes(block_mib)?,
        })
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

    fn optional_u64(
        arguments: &[String],
        index: usize,
        default: u64,
        name: &str,
    ) -> AnyResult<u64> {
        arguments
            .get(index)
            .map_or(Ok(default), |value| Self::parse_u64(value, name))
    }

    fn optional_usize(
        arguments: &[String],
        index: usize,
        default: usize,
        name: &str,
    ) -> AnyResult<usize> {
        let value = Self::optional_u64(
            arguments,
            index,
            u64::try_from(default).map_err(|_| invalid_input("worker default exceeds u64"))?,
            name,
        )?;
        usize::try_from(value)
            .map_err(|_| invalid_input_owned(format!("{name} exceeds usize")).into())
    }

    fn environment_u64(name: &str, default: u64) -> AnyResult<u64> {
        std::env::var(name).map_or(Ok(default), |value| Self::parse_u64(&value, name))
    }

    fn parse_u64(value: &str, name: &str) -> AnyResult<u64> {
        value.parse::<u64>().map_err(|error| {
            invalid_input_owned(format!("{name} is not an integer: {error}")).into()
        })
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

    fn print_usage() {
        println!(
            "Usage: hintsgen DATA_DIR BLOCKS_DIR [mainnet|testnet|testnet4|signet|regtest] \
             [TIP|tip] [ADD_THREADS] [REMOVE_THREADS] [RANGE_SIZE] [WORK_DIR]\n\
             Environment: HINTSGEN_BUCKETS HINTSGEN_BODY_GIB HINTSGEN_BLOCK_MIB"
        );
    }
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
    fn truncates_txid_and_keeps_little_endian_vout() {
        let mut txid = [0_u8; 32];
        for (index, byte) in txid.iter_mut().enumerate() {
            *byte = u8::try_from(index).unwrap_or(u8::MAX);
        }
        let key = IndexedOutput::key(OutPoint {
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
        IndexedOutput::append_transaction(
            1,
            ChainType::Mainnet,
            1,
            Txid::from_byte_array([7; 32]),
            &[spendable.clone(), op_return, oversized, spendable],
            &mut index,
            &mut indexed,
        )?;
        assert_eq!(index, 2);
        assert_eq!(indexed[0].value, OutputPosition::new(1, 0).to_le_bytes());
        assert_eq!(indexed[1].value, OutputPosition::new(1, 1).to_le_bytes());

        assert!(!IndexedOutput::is_eligible(
            ChainType::Mainnet,
            0,
            0,
            &indexed_output()
        ));
        assert!(!IndexedOutput::is_eligible(
            ChainType::Mainnet,
            91_722,
            0,
            &indexed_output()
        ));
        assert!(!IndexedOutput::is_eligible(
            ChainType::Mainnet,
            91_812,
            0,
            &indexed_output()
        ));
        assert!(IndexedOutput::is_eligible(
            ChainType::Mainnet,
            91_722,
            1,
            &indexed_output()
        ));
        assert!(IndexedOutput::is_eligible(
            ChainType::Signet,
            91_722,
            0,
            &indexed_output()
        ));
        Ok(())
    }

    #[test]
    fn folds_block_counts_into_global_offsets() -> AnyResult<()> {
        assert_eq!(BlockCounts::offsets(&[1, 4])?, vec![0, 1, 5]);
        assert_eq!(BlockCounts::offsets(&[])?, vec![0]);
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
