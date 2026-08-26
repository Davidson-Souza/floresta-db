mod ring;

use std::error::Error as StdError;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bitcoin::consensus::encode::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::{Block, OutPoint, TxOut};
use corepc_client::client_sync::{Auth, v31::Client};
use db_experiment::{Config, Database, Mode, PutResult};
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

    verify_core_utxo_set(&control, tip_hash, tip_height, indexed_outputs)?;
    if arguments.checkpoint {
        database.checkpoint()?;
    }
    database.sync()?;
    println!(
        "blocks={} outputs={} inputs={} utxos={} bytes={} elapsed={:.3}s throughput={:.0} blocks/s",
        producer_stats.blocks,
        producer_stats.outputs,
        consumer_stats.inputs,
        indexed_outputs,
        producer_stats.bytes,
        elapsed.as_secs_f64(),
        blocks_per_second(producer_stats.blocks, elapsed)
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
    while let Some(lease) = ring.claim_fetch()? {
        let hash = rpc
            .request(|| client.get_block_hash(lease.height))?
            .block_hash()?;
        let block = rpc.request(|| client.get_block(hash))?;
        if block.block_hash() != hash {
            return Err(io::Error::other("RPC returned a block with the wrong hash").into());
        }
        let encoded = serialize(&block);
        ring.write_fetch(lease, &encoded)?;
        wait_for_height(output_next, lease.height, ring)?;
        let outputs = add_block_outputs(database, ring, &block, lease.height, live_outputs)?;
        output_next
            .compare_exchange(
                lease.height,
                lease.height + 1,
                Ordering::Release,
                Ordering::Acquire,
            )
            .map_err(|_| io::Error::other("output indexing frontier changed unexpectedly"))?;
        ring.publish_fetch(lease, encoded.len())?;
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
    while let Some(lease) = ring.claim_consume()? {
        let encoded = ring.read_consume(lease)?;
        let block: Block = deserialize(&encoded)?;
        let inputs = remove_block_inputs(database, &block, live_outputs)?;
        ring.finish_consume(lease)?;
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
            let may_overwrite = transaction.is_coinbase() && database.contains(&key)?;
            if may_overwrite {
                wait_for_consumed_height(ring, height)?;
            }
            let value = output_value(output, height_u32)?;
            match database.put(&key, &value)? {
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
) -> AnyResult<u64> {
    let mut removed = 0_u64;
    for transaction in &block.txdata {
        for input in &transaction.input {
            if input.previous_output == OutPoint::null() {
                continue;
            }
            let key = outpoint_key(input.previous_output);
            if !database.delete(&key)? {
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

#[allow(clippy::cast_precision_loss)]
fn blocks_per_second(blocks: u64, elapsed: Duration) -> f64 {
    blocks as f64 / elapsed.as_secs_f64()
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
        mut request: impl FnMut() -> corepc_client::client_sync::Result<T>,
    ) -> AnyResult<T> {
        for attempt in 0..=RPC_RETRIES {
            match request() {
                Ok(value) => {
                    std::thread::sleep(self.delay);
                    return Ok(value);
                }
                Err(error) if attempt < RPC_RETRIES => {
                    eprintln!(
                        "RPC request failed (attempt {}/{}): {error}",
                        attempt + 1,
                        RPC_RETRIES + 1
                    );
                    std::thread::sleep(RPC_RETRY_DELAY);
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
struct ProducerStats {
    blocks: u64,
    outputs: u64,
    bytes: u64,
}

impl ProducerStats {
    fn merge(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.outputs = self.outputs.saturating_add(other.outputs);
        self.bytes = self.bytes.saturating_add(other.bytes);
    }
}

#[derive(Clone, Copy, Default)]
struct ConsumerStats {
    blocks: u64,
    inputs: u64,
}

impl ConsumerStats {
    fn merge(&mut self, other: Self) {
        self.blocks = self.blocks.saturating_add(other.blocks);
        self.inputs = self.inputs.saturating_add(other.inputs);
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
}
