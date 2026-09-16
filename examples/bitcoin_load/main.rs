// SPDX-License-Identifier: MIT OR Apache-2.0

//! Loads Bitcoin Core blocks into `floresta-db` and verifies the resulting UTXO set.
//!
//! This feature-gated example uses producer threads, a bounded flat-file ring,
//! and ordered consumers. Run it with `cargo run --release --features
//! bitcoin-load --example bitcoin-load -- <auth> <rpc-url> [tip]`.

mod ring;

use std::error::Error as StdError;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::{Block, OutPoint, TxOut};
use corepc_client::client_sync::{Auth, v31::Client};
use floresta_db::{Config, Database, Mode, PutResult};
use ring::BlockRing;

type AnyError = Box<dyn StdError + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

const OUTPOINT_KEY_SIZE: usize = 36;
const OUTPUT_FIXED_SIZE: usize = 16;
const MAX_SCRIPT_SIZE: usize = 10_000;
const DEFAULT_RPC_URL: &str = "http://127.0.0.1:8332";
const DEFAULT_RING_SLOTS: u64 = 64;
const DEFAULT_SLOT_MIB: u64 = 5;
const DEFAULT_BUCKETS: u64 = 1 << 20;
const DEFAULT_CAPACITY_GIB: u64 = 64;
const DEFAULT_BLOCK_MIB: u64 = 1;
const DEFAULT_RPC_DELAY_MS: u64 = 15;
const RPC_RETRIES: u32 = 60;
const RPC_RETRY_DELAY: Duration = Duration::from_secs(1);

fn main() {
    if let Err(error) = run() {
        eprintln!("bitcoin load test failed: {error}");
        std::process::exit(1);
    }
}

#[allow(clippy::too_many_lines)]
fn run() -> AnyResult<()> {
    let arguments = Arguments::parse()?;
    let rpc = RpcSettings {
        url: arguments.rpc_url.clone(),
        auth: arguments.auth.clone(),
        delay: arguments.rpc_delay,
    };
    let control = rpc.client()?;
    let node_tip = control.get_block_count()?.0;
    let tip_height = arguments.tip_height.unwrap_or(node_tip);
    if tip_height > node_tip {
        return Err(invalid_input("requested tip is above the Bitcoin Core tip").into());
    }
    let tip_hash = control.get_block_hash(tip_height)?.block_hash()?;
    let end_height = tip_height
        .checked_add(1)
        .ok_or_else(|| invalid_input("tip height overflow"))?;
    u32::try_from(tip_height)
        .map_err(|_| invalid_input("tip height does not fit the index value format"))?;

    std::fs::create_dir(&arguments.work_dir)?;
    let index_path = arguments.work_dir.join("index");
    let ring_path = arguments.work_dir.join("blocks.ring");
    let database = Database::create(&index_path, database_config(&arguments, tip_height)?)?;
    let ring = BlockRing::create(
        &ring_path,
        0,
        end_height,
        arguments.ring_slots,
        arguments.slot_bytes,
    )?;
    let output_next = AtomicU64::new(0);
    let live_outputs = AtomicU64::new(0);

    println!(
        "tip={} hash={} fetch_threads={} spend_threads={} ring_slots={}",
        tip_height,
        tip_hash,
        arguments.fetch_threads,
        arguments.spend_threads,
        arguments.ring_slots
    );
    let started = Instant::now();
    let (producer_stats, consumer_stats) = std::thread::scope(|scope| {
        let mut producers = Vec::new();
        producers
            .try_reserve_exact(arguments.fetch_threads)
            .map_err(|_| io::Error::other("producer handle allocation failed"))?;
        let mut consumers = Vec::new();
        consumers
            .try_reserve_exact(arguments.spend_threads)
            .map_err(|_| io::Error::other("consumer handle allocation failed"))?;

        for _worker in 0..arguments.fetch_threads {
            producers.push(scope.spawn(|| {
                let result = produce_blocks(&rpc, &ring, &database, &output_next, &live_outputs);
                if result.is_err() {
                    ring.abort();
                }
                result
            }));
        }
        for _worker in 0..arguments.spend_threads {
            consumers.push(scope.spawn(|| {
                let result = consume_blocks(&ring, &database, &live_outputs);
                if result.is_err() {
                    ring.abort();
                }
                result
            }));
        }

        let mut producer_stats = ProducerStats::default();
        for handle in producers {
            let stats = handle
                .join()
                .map_err(|_| io::Error::other("producer thread panicked"))??;
            producer_stats.merge(stats);
        }
        let mut consumer_stats = ConsumerStats::default();
        for handle in consumers {
            let stats = handle
                .join()
                .map_err(|_| io::Error::other("consumer thread panicked"))??;
            consumer_stats.merge(stats);
        }
        Ok::<_, AnyError>((producer_stats, consumer_stats))
    })?;
    let elapsed = started.elapsed();

    if ring.consumed_height() != end_height {
        return Err(io::Error::other("consumer frontier did not reach the selected tip").into());
    }
    let indexed_outputs = live_outputs.load(Ordering::Acquire);
    if producer_stats
        .outputs
        .checked_sub(consumer_stats.inputs)
        .ok_or_else(|| io::Error::other("spent input count exceeds indexed output count"))?
        != indexed_outputs
    {
        return Err(io::Error::other("tracked live output count is inconsistent").into());
    }

    let verification_started = Instant::now();
    verify_core_utxo_set(&control, tip_hash, tip_height, indexed_outputs)?;
    let verification_elapsed = verification_started.elapsed();
    let checkpoint_elapsed = if arguments.checkpoint {
        let checkpoint_started = Instant::now();
        database.checkpoint()?;
        Some(checkpoint_started.elapsed())
    } else {
        None
    };
    let sync_started = Instant::now();
    database.sync()?;
    let sync_elapsed = sync_started.elapsed();
    let index_storage = storage_stats(&index_path)?;
    let ring_storage = storage_stats(&ring_path)?;
    println!(
        "blocks={} outputs={} inputs={} utxos={} bytes={} elapsed={:.3}s",
        producer_stats.blocks,
        producer_stats.outputs,
        consumer_stats.inputs,
        indexed_outputs,
        producer_stats.bytes,
        elapsed.as_secs_f64()
    );
    print_performance_report(
        &producer_stats,
        &consumer_stats,
        elapsed,
        &CompletionMetrics {
            verification: verification_elapsed,
            checkpoint: checkpoint_elapsed,
            sync: sync_elapsed,
            index_storage,
            ring_storage,
        },
    );
    Ok(())
}

fn produce_blocks(
    rpc: &RpcSettings,
    ring: &BlockRing,
    database: &Database,
    output_next: &AtomicU64,
    live_outputs: &AtomicU64,
) -> AnyResult<ProducerStats> {
    let client = rpc.client()?;
    let mut stats = ProducerStats::default();
    loop {
        let claim_started = Instant::now();
        let lease = ring.claim_fetch();
        stats.ring_claim.record(claim_started.elapsed());
        let Some(lease) = lease? else {
            break;
        };
        let hash = rpc
            .request(&mut stats.rpc, || client.get_block_hash(lease.height))?
            .block_hash()?;
        let block = rpc.request(&mut stats.rpc, || client.get_block(hash))?;
        if block.block_hash() != hash {
            return Err(io::Error::other("RPC returned a block with the wrong hash").into());
        }
        let serialize_started = Instant::now();
        let encoded = serialize(&block);
        stats.serialize.record(serialize_started.elapsed());
        let ring_write_started = Instant::now();
        let write_result = ring.write_fetch(lease, &encoded);
        stats.ring_write.record(ring_write_started.elapsed());
        write_result?;
        let order_wait_started = Instant::now();
        let wait_result = wait_for_height(output_next, lease.height, ring);
        stats.output_order_wait.record(order_wait_started.elapsed());
        wait_result?;
        let output_index_started = Instant::now();
        let outputs = add_block_outputs(
            database,
            ring,
            &block,
            lease.height,
            live_outputs,
            &mut stats,
        );
        stats.output_index.record(output_index_started.elapsed());
        let outputs = outputs?;
        output_next
            .compare_exchange(
                lease.height,
                lease.height + 1,
                Ordering::Release,
                Ordering::Acquire,
            )
            .map_err(|_| io::Error::other("output indexing frontier changed unexpectedly"))?;
        let publish_started = Instant::now();
        let publish_result = ring.publish_fetch(lease, encoded.len());
        stats.ring_publish.record(publish_started.elapsed());
        publish_result?;
        stats.blocks = stats.blocks.saturating_add(1);
        stats.outputs = stats.outputs.saturating_add(outputs);
        stats.bytes = stats
            .bytes
            .saturating_add(u64::try_from(encoded.len()).unwrap_or(u64::MAX));
    }
    Ok(stats)
}

fn consume_blocks(
    ring: &BlockRing,
    database: &Database,
    live_outputs: &AtomicU64,
) -> AnyResult<ConsumerStats> {
    let mut stats = ConsumerStats::default();
    loop {
        let claim_started = Instant::now();
        let lease = ring.claim_consume();
        stats.ring_claim.record(claim_started.elapsed());
        let Some(lease) = lease? else {
            break;
        };
        let ring_read_started = Instant::now();
        let encoded = ring.read_consume(lease);
        stats.ring_read.record(ring_read_started.elapsed());
        let encoded = encoded?;
        let deserialize_started = Instant::now();
        let block = deserialize(&encoded);
        stats.deserialize.record(deserialize_started.elapsed());
        let block: Block = block?;
        let input_remove_started = Instant::now();
        let inputs = remove_block_inputs(database, &block, live_outputs, &mut stats);
        stats.input_remove.record(input_remove_started.elapsed());
        let inputs = inputs?;
        let finish_started = Instant::now();
        let finish_result = ring.finish_consume(lease);
        stats.ring_finish.record(finish_started.elapsed());
        finish_result?;
        stats.blocks = stats.blocks.saturating_add(1);
        stats.inputs = stats.inputs.saturating_add(inputs);
    }
    Ok(stats)
}

fn add_block_outputs(
    database: &Database,
    ring: &BlockRing,
    block: &Block,
    height: u64,
    live_outputs: &AtomicU64,
    stats: &mut ProducerStats,
) -> AnyResult<u64> {
    let height_u32 = u32::try_from(height)
        .map_err(|_| invalid_input("block height does not fit output value"))?;
    let mut added = 0_u64;
    for transaction in &block.txdata {
        let txid = transaction.compute_txid();
        for (vout, output) in transaction.output.iter().enumerate() {
            if !should_index_output(height, output) {
                continue;
            }
            let vout = u32::try_from(vout)
                .map_err(|_| invalid_input("transaction output index exceeds u32"))?;
            let outpoint = OutPoint { txid, vout };
            let key = outpoint_key(outpoint);
            let may_overwrite = if transaction.is_coinbase() {
                let contains_started = Instant::now();
                let contains = database.contains(&key);
                stats.db_contains.record(contains_started.elapsed());
                contains?
            } else {
                false
            };
            if may_overwrite {
                let bip30_wait_started = Instant::now();
                let wait_result = wait_for_consumed_height(ring, height);
                stats.bip30_wait.record(bip30_wait_started.elapsed());
                wait_result?;
            }
            let value = output_value(output, height_u32)?;
            let put_started = Instant::now();
            let put_result = database.put(&key, &value);
            stats.db_put.record(put_started.elapsed());
            match put_result? {
                PutResult::Inserted => {
                    cas_increment(live_outputs)?;
                    added = added.saturating_add(1);
                }
                PutResult::Replaced if may_overwrite => {}
                PutResult::Replaced => {
                    return Err(io::Error::other("duplicate live outpoint encountered").into());
                }
            }
        }
    }
    Ok(added)
}

fn remove_block_inputs(
    database: &Database,
    block: &Block,
    live_outputs: &AtomicU64,
    stats: &mut ConsumerStats,
) -> AnyResult<u64> {
    let mut removed = 0_u64;
    for transaction in &block.txdata {
        for input in &transaction.input {
            if input.previous_output == OutPoint::null() {
                continue;
            }
            let key = outpoint_key(input.previous_output);
            let delete_started = Instant::now();
            let delete_result = database.delete(&key);
            stats.db_delete.record(delete_started.elapsed());
            if !delete_result? {
                return Err(io::Error::other(format!(
                    "input references missing outpoint {}",
                    input.previous_output
                ))
                .into());
            }
            cas_decrement(live_outputs)?;
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

fn verify_core_utxo_set(
    client: &Client,
    expected_tip: bitcoin::BlockHash,
    expected_height: u64,
    indexed_outputs: u64,
) -> AnyResult<()> {
    let info: corepc_client::types::v26::GetTxOutSetInfo = client.call(
        "gettxoutsetinfo",
        &["none".into(), expected_tip.to_string().into(), true.into()],
    )?;
    if info.height != i64::try_from(expected_height)? {
        return Err(io::Error::other("gettxoutsetinfo returned a different height").into());
    }
    if info.best_block != expected_tip.to_string() {
        return Err(io::Error::other("gettxoutsetinfo returned a different tip").into());
    }
    let core_outputs = u64::try_from(info.tx_outs)?;
    if core_outputs != indexed_outputs {
        return Err(io::Error::other(format!(
            "UTXO count mismatch: index={indexed_outputs} core={core_outputs}"
        ))
        .into());
    }
    println!("verified UTXO count against Bitcoin Core");
    Ok(())
}

fn outpoint_key(outpoint: OutPoint) -> [u8; OUTPOINT_KEY_SIZE] {
    let mut key = [0_u8; OUTPOINT_KEY_SIZE];
    key[..32].copy_from_slice(&outpoint.txid.to_byte_array());
    key[32..].copy_from_slice(&outpoint.vout.to_le_bytes());
    key
}

fn should_index_output(height: u64, output: &TxOut) -> bool {
    height != 0
        && output.script_pubkey.len() <= MAX_SCRIPT_SIZE
        && !output.script_pubkey.is_op_return()
}

fn output_value(output: &TxOut, height: u32) -> AnyResult<Vec<u8>> {
    let script = output.script_pubkey.as_bytes();
    let script_length = u32::try_from(script.len())
        .map_err(|_| invalid_input("output script length exceeds u32"))?;
    let capacity = OUTPUT_FIXED_SIZE
        .checked_add(script.len())
        .ok_or_else(|| invalid_input("output value length overflow"))?;
    let mut value = Vec::new();
    value
        .try_reserve_exact(capacity)
        .map_err(|_| io::Error::other("output value allocation failed"))?;
    value.extend_from_slice(&output.value.to_sat().to_le_bytes());
    value.extend_from_slice(&height.to_le_bytes());
    value.extend_from_slice(&script_length.to_le_bytes());
    value.extend_from_slice(script);
    Ok(value)
}

fn wait_for_height(frontier: &AtomicU64, height: u64, ring: &BlockRing) -> io::Result<()> {
    while frontier.load(Ordering::Acquire) != height {
        if ring.is_aborted() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "output indexing aborted",
            ));
        }
        wait_for_progress();
    }
    Ok(())
}

fn wait_for_consumed_height(ring: &BlockRing, height: u64) -> io::Result<()> {
    while ring.consumed_height() < height {
        if ring.is_aborted() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "BIP30 wait aborted",
            ));
        }
        wait_for_progress();
    }
    Ok(())
}

fn wait_for_progress() {
    std::thread::yield_now();
    std::thread::sleep(Duration::from_micros(50));
}

fn print_performance_report(
    producer: &ProducerStats,
    consumer: &ConsumerStats,
    elapsed: Duration,
    completion: &CompletionMetrics,
) {
    let database_operations = producer
        .db_put
        .count
        .saturating_add(consumer.db_delete.count);
    println!(
        "rates blocks_s={:.1} data_mib_s={:.1} db_ops_s={:.1}",
        per_second(producer.blocks, elapsed),
        mebibytes_per_second(producer.bytes, elapsed),
        per_second(database_operations, elapsed)
    );
    println!(
        "rpc calls={} retries={} pacing_s={:.3} retry_wait_s={:.3}",
        producer.rpc.calls,
        producer.rpc.retries,
        producer.rpc.pacing.as_secs_f64(),
        producer.rpc.retry_wait.as_secs_f64()
    );
    print_timing("rpc.transport", &producer.rpc.transport);
    print_timing("producer.ring_claim_wait", &producer.ring_claim);
    print_timing("producer.output_order_wait", &producer.output_order_wait);
    print_timing("producer.bip30_wait", &producer.bip30_wait);
    print_timing("producer.serialize", &producer.serialize);
    print_timing("producer.ring_write", &producer.ring_write);
    print_timing("producer.index_stage", &producer.output_index);
    print_timing("database.contains", &producer.db_contains);
    print_timing("database.put", &producer.db_put);
    print_timing("producer.ring_publish", &producer.ring_publish);
    print_timing("consumer.ring_claim_wait", &consumer.ring_claim);
    print_timing("consumer.ring_read", &consumer.ring_read);
    print_timing("consumer.deserialize", &consumer.deserialize);
    print_timing("consumer.spend_stage", &consumer.input_remove);
    print_timing("database.delete", &consumer.db_delete);
    print_timing("consumer.ring_finish", &consumer.ring_finish);
    match completion.checkpoint {
        Some(checkpoint) => println!(
            "maintenance verify_s={:.3} checkpoint_s={:.3} sync_s={:.3}",
            completion.verification.as_secs_f64(),
            checkpoint.as_secs_f64(),
            completion.sync.as_secs_f64()
        ),
        None => println!(
            "maintenance verify_s={:.3} checkpoint=disabled sync_s={:.3}",
            completion.verification.as_secs_f64(),
            completion.sync.as_secs_f64()
        ),
    }
    println!(
        "storage component=index files={} logical_gib={:.3} allocated_mib={:.1}",
        completion.index_storage.files,
        gibibytes_f64(completion.index_storage.logical_bytes),
        mebibytes_f64(completion.index_storage.allocated_bytes)
    );
    println!(
        "storage component=ring files={} logical_gib={:.3} allocated_mib={:.1}",
        completion.ring_storage.files,
        gibibytes_f64(completion.ring_storage.logical_bytes),
        mebibytes_f64(completion.ring_storage.allocated_bytes)
    );
}

fn print_timing(name: &str, timing: &OperationTiming) {
    println!(
        "timing name={name} count={} total_s={:.3} avg_us={:.3} max_ms={:.3}",
        timing.count,
        timing.total.as_secs_f64(),
        timing.average_micros(),
        timing.max.as_secs_f64() * 1_000.0
    );
}

#[allow(clippy::cast_precision_loss)]
fn per_second(count: u64, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64()
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

fn storage_stats(path: &Path) -> io::Result<StorageStats> {
    let metadata = path.metadata()?;
    if metadata.is_file() {
        return Ok(StorageStats {
            files: 1,
            logical_bytes: metadata.len(),
            allocated_bytes: metadata.blocks().saturating_mul(512),
        });
    }

    let mut stats = StorageStats::default();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let child = storage_stats(&entry.path())?;
        stats.merge(child);
    }
    Ok(stats)
}

fn cas_increment(counter: &AtomicU64) -> io::Result<()> {
    let mut observed = counter.load(Ordering::Acquire);
    loop {
        let next = observed
            .checked_add(1)
            .ok_or_else(|| io::Error::other("live output counter overflow"))?;
        match counter.compare_exchange(observed, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(actual) => observed = actual,
        }
    }
}

fn cas_decrement(counter: &AtomicU64) -> io::Result<()> {
    let mut observed = counter.load(Ordering::Acquire);
    loop {
        let next = observed
            .checked_sub(1)
            .ok_or_else(|| io::Error::other("live output counter underflow"))?;
        match counter.compare_exchange(observed, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(actual) => observed = actual,
        }
    }
}

fn database_config(arguments: &Arguments, tip_height: u64) -> AnyResult<Config> {
    let mut config = Config::new(Mode::Map, arguments.buckets, OUTPOINT_KEY_SIZE);
    config.block_size = arguments.database_block_bytes;
    config.body_capacity = arguments.body_capacity;
    config.blob_capacity = arguments.blob_capacity;
    let workers = arguments
        .fetch_threads
        .checked_add(arguments.spend_threads)
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| invalid_input("worker count overflow"))?;
    config.max_threads =
        u16::try_from(workers).map_err(|_| invalid_input("worker count exceeds database limit"))?;
    let minimum_body = tip_height
        .checked_add(1)
        .and_then(|blocks| blocks.checked_mul(96))
        .ok_or_else(|| invalid_input("minimum body capacity overflow"))?;
    if config.body_capacity < minimum_body {
        return Err(invalid_input("body capacity is too small for one output per block").into());
    }
    Ok(config)
}

#[derive(Clone)]
struct RpcSettings {
    url: String,
    auth: RpcAuth,
    delay: Duration,
}

impl RpcSettings {
    fn client(&self) -> AnyResult<Client> {
        match &self.auth {
            RpcAuth::None => Ok(Client::new(&self.url)),
            RpcAuth::UserPass(user, password) => Ok(Client::new_with_auth(
                &self.url,
                Auth::UserPass(user.clone(), password.clone()),
            )?),
            RpcAuth::Cookie(cookie) => Ok(Client::new_with_auth(
                &self.url,
                Auth::CookieFile(cookie.clone()),
            )?),
        }
    }

    fn request<T>(
        &self,
        metrics: &mut RpcMetrics,
        mut request: impl FnMut() -> corepc_client::client_sync::Result<T>,
    ) -> AnyResult<T> {
        metrics.calls = metrics.calls.saturating_add(1);
        for attempt in 0..=RPC_RETRIES {
            let transport_started = Instant::now();
            let response = request();
            metrics.transport.record(transport_started.elapsed());
            match response {
                Ok(value) => {
                    let pacing_started = Instant::now();
                    std::thread::sleep(self.delay);
                    metrics.pacing = metrics.pacing.saturating_add(pacing_started.elapsed());
                    return Ok(value);
                }
                Err(error) if attempt < RPC_RETRIES => {
                    metrics.retries = metrics.retries.saturating_add(1);
                    eprintln!(
                        "RPC request failed (attempt {}/{}): {error}",
                        attempt + 1,
                        RPC_RETRIES + 1
                    );
                    let retry_wait_started = Instant::now();
                    std::thread::sleep(RPC_RETRY_DELAY);
                    metrics.retry_wait = metrics
                        .retry_wait
                        .saturating_add(retry_wait_started.elapsed());
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(io::Error::other("RPC retry loop ended unexpectedly").into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RpcAuth {
    None,
    UserPass(String, String),
    Cookie(PathBuf),
}

struct Arguments {
    auth: RpcAuth,
    rpc_url: String,
    tip_height: Option<u64>,
    fetch_threads: usize,
    spend_threads: usize,
    ring_slots: u64,
    slot_bytes: u64,
    work_dir: PathBuf,
    buckets: u64,
    body_capacity: u64,
    blob_capacity: u64,
    database_block_bytes: u64,
    checkpoint: bool,
    rpc_delay: Duration,
}

impl Arguments {
    fn parse() -> AnyResult<Self> {
        if matches!(std::env::args().nth(1).as_deref(), Some("-h" | "--help")) {
            print_usage();
            std::process::exit(0);
        }
        let auth_argument = std::env::args()
            .nth(1)
            .ok_or_else(|| invalid_input("missing Bitcoin Core authentication argument"))?;
        let auth = parse_rpc_auth(auth_argument)?;
        let rpc_url = std::env::args()
            .nth(2)
            .unwrap_or_else(|| DEFAULT_RPC_URL.to_owned());
        let tip_height = match std::env::args().nth(3).as_deref() {
            None | Some("tip") => None,
            Some(value) => Some(parse_u64(value, "tip height")?),
        };
        let parallelism = std::thread::available_parallelism().map_or(2, std::num::NonZero::get);
        let fetch_threads = optional_usize(4, parallelism.min(4), "fetch threads")?;
        let spend_threads = optional_usize(5, parallelism.max(2) - 1, "spend threads")?;
        if fetch_threads == 0 || spend_threads == 0 {
            return Err(invalid_input("thread pool sizes must be nonzero").into());
        }
        let ring_slots = optional_u64(6, DEFAULT_RING_SLOTS, "ring slots")?;
        let work_dir = std::env::args()
            .nth(7)
            .map_or_else(|| PathBuf::from("bitcoin-load-run"), PathBuf::from);
        let slot_mib = environment_u64("DB_LOAD_SLOT_MIB", DEFAULT_SLOT_MIB)?;
        let block_mib = environment_u64("DB_LOAD_BLOCK_MIB", DEFAULT_BLOCK_MIB)?;
        let body_gib = environment_u64("DB_LOAD_BODY_GIB", DEFAULT_CAPACITY_GIB)?;
        let blob_gib = environment_u64("DB_LOAD_BLOB_GIB", DEFAULT_CAPACITY_GIB)?;
        Ok(Self {
            auth,
            rpc_url,
            tip_height,
            fetch_threads,
            spend_threads,
            ring_slots,
            slot_bytes: mebibytes(slot_mib)?,
            work_dir,
            buckets: environment_u64("DB_LOAD_BUCKETS", DEFAULT_BUCKETS)?,
            body_capacity: gibibytes(body_gib)?,
            blob_capacity: gibibytes(blob_gib)?,
            database_block_bytes: mebibytes(block_mib)?,
            checkpoint: std::env::var_os("DB_LOAD_CHECKPOINT").is_some(),
            rpc_delay: Duration::from_millis(environment_u64(
                "DB_LOAD_RPC_DELAY_MS",
                DEFAULT_RPC_DELAY_MS,
            )?),
        })
    }
}

#[derive(Clone, Copy, Default)]
struct OperationTiming {
    count: u64,
    total: Duration,
    max: Duration,
}

#[derive(Clone, Copy, Default)]
struct StorageStats {
    files: u64,
    logical_bytes: u64,
    allocated_bytes: u64,
}

struct CompletionMetrics {
    verification: Duration,
    checkpoint: Option<Duration>,
    sync: Duration,
    index_storage: StorageStats,
    ring_storage: StorageStats,
}

impl StorageStats {
    fn merge(&mut self, other: Self) {
        self.files = self.files.saturating_add(other.files);
        self.logical_bytes = self.logical_bytes.saturating_add(other.logical_bytes);
        self.allocated_bytes = self.allocated_bytes.saturating_add(other.allocated_bytes);
    }
}

impl OperationTiming {
    fn record(&mut self, elapsed: Duration) {
        self.count = self.count.saturating_add(1);
        self.total = self.total.saturating_add(elapsed);
        self.max = self.max.max(elapsed);
    }

    fn merge(&mut self, other: Self) {
        self.count = self.count.saturating_add(other.count);
        self.total = self.total.saturating_add(other.total);
        self.max = self.max.max(other.max);
    }

    #[allow(clippy::cast_precision_loss)]
    fn average_micros(self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total.as_secs_f64() * 1_000_000.0 / self.count as f64
        }
    }
}

#[derive(Clone, Copy, Default)]
struct RpcMetrics {
    calls: u64,
    retries: u64,
    transport: OperationTiming,
    pacing: Duration,
    retry_wait: Duration,
}

impl RpcMetrics {
    fn merge(&mut self, other: Self) {
        self.calls = self.calls.saturating_add(other.calls);
        self.retries = self.retries.saturating_add(other.retries);
        self.transport.merge(other.transport);
        self.pacing = self.pacing.saturating_add(other.pacing);
        self.retry_wait = self.retry_wait.saturating_add(other.retry_wait);
    }
}

#[derive(Clone, Copy, Default)]
struct ProducerStats {
    blocks: u64,
    outputs: u64,
    bytes: u64,
    rpc: RpcMetrics,
    ring_claim: OperationTiming,
    output_order_wait: OperationTiming,
    bip30_wait: OperationTiming,
    serialize: OperationTiming,
    ring_write: OperationTiming,
    output_index: OperationTiming,
    db_contains: OperationTiming,
    db_put: OperationTiming,
    ring_publish: OperationTiming,
}

impl ProducerStats {
    fn merge(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.outputs = self.outputs.saturating_add(other.outputs);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.rpc.merge(other.rpc);
        self.ring_claim.merge(other.ring_claim);
        self.output_order_wait.merge(other.output_order_wait);
        self.bip30_wait.merge(other.bip30_wait);
        self.serialize.merge(other.serialize);
        self.ring_write.merge(other.ring_write);
        self.output_index.merge(other.output_index);
        self.db_contains.merge(other.db_contains);
        self.db_put.merge(other.db_put);
        self.ring_publish.merge(other.ring_publish);
    }
}

#[derive(Clone, Copy, Default)]
struct ConsumerStats {
    blocks: u64,
    inputs: u64,
    ring_claim: OperationTiming,
    ring_read: OperationTiming,
    deserialize: OperationTiming,
    input_remove: OperationTiming,
    db_delete: OperationTiming,
    ring_finish: OperationTiming,
}

impl ConsumerStats {
    fn merge(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.inputs = self.inputs.saturating_add(other.inputs);
        self.ring_claim.merge(other.ring_claim);
        self.ring_read.merge(other.ring_read);
        self.deserialize.merge(other.deserialize);
        self.input_remove.merge(other.input_remove);
        self.db_delete.merge(other.db_delete);
        self.ring_finish.merge(other.ring_finish);
    }
}

fn optional_u64(index: usize, default: u64, name: &str) -> AnyResult<u64> {
    std::env::args()
        .nth(index)
        .map_or(Ok(default), |value| parse_u64(&value, name))
}

fn optional_usize(index: usize, default: usize, name: &str) -> AnyResult<usize> {
    let value = optional_u64(
        index,
        u64::try_from(default).map_err(|_| invalid_input("default thread count overflow"))?,
        name,
    )?;
    usize::try_from(value).map_err(|_| invalid_input("thread count does not fit memory").into())
}

fn environment_u64(name: &str, default: u64) -> AnyResult<u64> {
    std::env::var(name).map_or(Ok(default), |value| parse_u64(&value, name))
}

fn parse_u64(value: &str, name: &str) -> AnyResult<u64> {
    value
        .parse::<u64>()
        .map_err(|error| invalid_input_owned(format!("invalid {name} '{value}': {error}")).into())
}

fn parse_rpc_auth(argument: String) -> AnyResult<RpcAuth> {
    if argument == "none" {
        return Ok(RpcAuth::None);
    }
    if let Some((user, password)) = argument.split_once(':') {
        if user.is_empty() || password.is_empty() {
            return Err(invalid_input("RPC user and password must be nonempty").into());
        }
        return Ok(RpcAuth::UserPass(user.to_owned(), password.to_owned()));
    }
    Ok(RpcAuth::Cookie(PathBuf::from(argument)))
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

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_input_owned(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn print_usage() {
    println!(
        "Usage: bitcoin-load COOKIE|USER:PASSWORD|none [RPC_URL] [TIP|tip] [FETCH_THREADS] [SPEND_THREADS] [RING_SLOTS] [WORK_DIR]\n\
         Environment: DB_LOAD_SLOT_MIB DB_LOAD_BUCKETS DB_LOAD_BODY_GIB DB_LOAD_BLOB_GIB \
         DB_LOAD_BLOCK_MIB DB_LOAD_RPC_DELAY_MS DB_LOAD_CHECKPOINT"
    );
}

#[cfg(test)]
mod tests {
    use bitcoin::{Amount, ScriptBuf, Txid};

    use super::*;

    #[test]
    fn serializes_outpoint_as_txid_and_little_endian_vout() {
        let outpoint = OutPoint {
            txid: Txid::from_byte_array([7; 32]),
            vout: 0x0102_0304,
        };
        let key = outpoint_key(outpoint);
        assert_eq!(&key[..32], &[7; 32]);
        assert_eq!(&key[32..], &[4, 3, 2, 1]);
    }

    #[test]
    fn serializes_amount_height_and_script() -> AnyResult<()> {
        let output = TxOut {
            value: Amount::from_sat(42),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51, 0x21]),
        };
        let value = output_value(&output, 9)?;
        assert_eq!(&value[0..8], &42_u64.to_le_bytes());
        assert_eq!(&value[8..12], &9_u32.to_le_bytes());
        assert_eq!(&value[12..16], &2_u32.to_le_bytes());
        assert_eq!(&value[16..], &[0x51, 0x21]);
        Ok(())
    }

    #[test]
    fn parses_rpc_authentication_methods() -> AnyResult<()> {
        assert_eq!(parse_rpc_auth("none".to_owned())?, RpcAuth::None);
        assert_eq!(
            parse_rpc_auth("bitcoin:secret".to_owned())?,
            RpcAuth::UserPass("bitcoin".to_owned(), "secret".to_owned())
        );
        assert_eq!(
            parse_rpc_auth("/tmp/.cookie".to_owned())?,
            RpcAuth::Cookie(PathBuf::from("/tmp/.cookie"))
        );
        assert!(parse_rpc_auth(":secret".to_owned()).is_err());
        assert!(parse_rpc_auth("bitcoin:".to_owned()).is_err());
        Ok(())
    }

    #[test]
    fn skips_outputs_bitcoin_core_excludes_from_the_utxo_set() {
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

        assert!(!should_index_output(0, &spendable));
        assert!(should_index_output(1, &spendable));
        assert!(!should_index_output(1, &op_return));
        assert!(!should_index_output(1, &oversized));
    }

    #[test]
    fn aggregates_operation_timings() {
        let mut timing = OperationTiming::default();
        timing.record(Duration::from_millis(2));
        timing.record(Duration::from_millis(5));
        let mut other = OperationTiming::default();
        other.record(Duration::from_millis(3));

        timing.merge(other);

        assert_eq!(timing.count, 3);
        assert_eq!(timing.total, Duration::from_millis(10));
        assert_eq!(timing.max, Duration::from_millis(5));
        assert!((timing.average_micros() - 3_333.333_333).abs() < 0.001);
    }
}
