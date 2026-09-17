// SPDX-License-Identifier: MIT OR Apache-2.0

//! Dependency-free throughput benchmarks for the public database API.

use floresta_db::{Config, Database, Mode, xxh64};
use std::error::Error as StdError;
use std::hint::black_box;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 8;
const BLOCK_SIZE: u64 = 1 << 20;
const DEFAULT_ITEMS: usize = 100_000;
const READ_ROUNDS: u64 = 3;
const HASH_ROUNDS: u64 = 16;

type AnyError = Box<dyn StdError + Send + Sync>;
type AnyResult<T> = std::result::Result<T, AnyError>;

fn main() -> AnyResult<()> {
    let items = parse_items()?;
    let data = BenchData::new(items)?;

    println!("floresta-db benchmark: items={items}");
    println!(
        "{:<30} {:>12} {:>16}",
        "benchmark", "elapsed_ms", "operations/s"
    );

    let hash_operations = checked_operations(items, HASH_ROUNDS)?;
    report("xxh64_scalar", hash_operations, bench_hash(&data));
    report(
        "write_only_put_batch",
        items_as_u64(items)?,
        bench_write_only_batch(&data)?,
    );
    report("scalar_get", items_as_u64(items)?, bench_scalar_get(&data)?);
    report(
        "batch_fetch",
        checked_operations(items, READ_ROUNDS)?,
        bench_batch_fetch(&data)?,
    );
    report(
        "batch_delete",
        items_as_u64(items)?,
        bench_batch_delete(&data)?,
    );
    report(
        "concurrent_add_batch",
        items_as_u64(items)?,
        bench_concurrent_add(&data)?,
    );
    report("checkpoint", items_as_u64(items)?, bench_checkpoint(&data)?);
    Ok(())
}

fn bench_hash(data: &BenchData) -> Duration {
    let started = Instant::now();
    let mut digest = 0_u64;
    for round in 0..HASH_ROUNDS {
        for key in &data.keys {
            digest ^= xxh64(key, round);
        }
    }
    black_box(digest);
    started.elapsed()
}

fn bench_write_only_batch(data: &BenchData) -> AnyResult<Duration> {
    let work = BenchDirectory::new("write")?;
    let database = Database::create(work.path(), inline_map_config(data.len())?)?;
    let started = Instant::now();
    let inserted = database.write_only()?.put_batch(entries(data))?;
    let elapsed = started.elapsed();
    ensure_count("write-only insert", inserted, data.len())?;
    database.close()?;
    Ok(elapsed)
}

fn bench_scalar_get(data: &BenchData) -> AnyResult<Duration> {
    let work = BenchDirectory::new("scalar-read")?;
    let database = populated_inline_map(work.path(), data)?;
    let started = Instant::now();
    let mut found = 0_usize;
    for key in &data.keys {
        found = found.saturating_add(usize::from(database.get(key)?.is_some()));
    }
    let elapsed = started.elapsed();
    ensure_count("scalar read", found, data.len())?;
    database.close()?;
    Ok(elapsed)
}

fn bench_batch_fetch(data: &BenchData) -> AnyResult<Duration> {
    let work = BenchDirectory::new("batch-read")?;
    let database = populated_inline_map(work.path(), data)?;
    let verification = database.batch_fetch(data.keys.iter().map(<[u8; KEY_SIZE]>::as_slice))?;
    ensure_count(
        "batch read verification",
        verification.iter().filter(|value| value.is_some()).count(),
        data.len(),
    )?;

    let started = Instant::now();
    for _round in 0..READ_ROUNDS {
        let values = database.batch_fetch(data.keys.iter().map(<[u8; KEY_SIZE]>::as_slice))?;
        black_box(values);
    }
    let elapsed = started.elapsed();
    database.close()?;
    Ok(elapsed)
}

fn bench_batch_delete(data: &BenchData) -> AnyResult<Duration> {
    let work = BenchDirectory::new("delete")?;
    let database = populated_inline_map(work.path(), data)?;
    let started = Instant::now();
    let deleted = database.batch_delete(data.keys.iter().map(<[u8; KEY_SIZE]>::as_slice))?;
    let elapsed = started.elapsed();
    ensure_count(
        "batch delete",
        deleted.iter().filter(|deleted| **deleted).count(),
        data.len(),
    )?;
    database.close()?;
    Ok(elapsed)
}

fn bench_concurrent_add(data: &BenchData) -> AnyResult<Duration> {
    let work = BenchDirectory::new("concurrent-add")?;
    let database = Database::create(work.path(), set_config(data.len())?)?;
    let workers = std::thread::available_parallelism()
        .map_or(1, std::num::NonZero::get)
        .min(8)
        .min(data.len());
    let chunk_size = data.len().div_ceil(workers);

    let started = Instant::now();
    let inserted = std::thread::scope(|scope| -> AnyResult<usize> {
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(workers)
            .map_err(|_| floresta_db::Error::OutOfMemory)?;
        for chunk in data.keys.chunks(chunk_size) {
            let database_ref = &database;
            handles.push(scope.spawn(move || {
                database_ref.add_batch(chunk.iter().map(<[u8; KEY_SIZE]>::as_slice))
            }));
        }
        let mut total = 0_usize;
        for handle in handles {
            let count = handle
                .join()
                .map_err(|_| io::Error::other("benchmark worker panicked"))??;
            total = total
                .checked_add(count)
                .ok_or_else(|| io::Error::other("benchmark insert count overflow"))?;
        }
        Ok(total)
    })?;
    let elapsed = started.elapsed();
    ensure_count("concurrent add", inserted, data.len())?;
    database.close()?;
    Ok(elapsed)
}

fn bench_checkpoint(data: &BenchData) -> AnyResult<Duration> {
    let work = BenchDirectory::new("checkpoint")?;
    let database = Database::create(work.path(), blob_map_config(data.len())?)?;
    let inserted = database.write_only()?.put_batch(entries(data))?;
    ensure_count("checkpoint setup", inserted, data.len())?;

    let started = Instant::now();
    black_box(database.checkpoint()?);
    let elapsed = started.elapsed();
    database.close()?;
    Ok(elapsed)
}

fn populated_inline_map(path: &Path, data: &BenchData) -> AnyResult<Database> {
    let database = Database::create(path, inline_map_config(data.len())?)?;
    let inserted = database.write_only()?.put_batch(entries(data))?;
    ensure_count("map setup", inserted, data.len())?;
    Ok(database)
}

fn entries(data: &BenchData) -> impl ExactSizeIterator<Item = (&[u8], &[u8])> + Clone + '_ {
    data.keys
        .iter()
        .zip(&data.values)
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
}

fn inline_map_config(items: usize) -> AnyResult<Config> {
    let mut config = base_config(Mode::Map, items)?;
    config.inline_value_size = VALUE_SIZE;
    config.blob_capacity = 0;
    Ok(config)
}

fn blob_map_config(items: usize) -> AnyResult<Config> {
    let mut config = base_config(Mode::Map, items)?;
    config.blob_capacity = storage_capacity(items, 16)?;
    Ok(config)
}

fn set_config(items: usize) -> AnyResult<Config> {
    base_config(Mode::Set, items)
}

fn base_config(mode: Mode, items: usize) -> AnyResult<Config> {
    let doubled = items
        .checked_mul(2)
        .ok_or_else(|| io::Error::other("benchmark bucket count overflow"))?;
    let buckets = doubled
        .max(1_024)
        .checked_next_power_of_two()
        .ok_or_else(|| io::Error::other("benchmark bucket count overflow"))?;
    let mut config = Config::new(mode, u64::try_from(buckets)?, KEY_SIZE);
    config.block_size = BLOCK_SIZE;
    config.body_capacity = storage_capacity(items, 128)?;
    Ok(config)
}

fn storage_capacity(items: usize, bytes_per_item: u64) -> AnyResult<u64> {
    let items = items_as_u64(items)?;
    let required = items
        .checked_mul(bytes_per_item)
        .and_then(|bytes| bytes.checked_add(BLOCK_SIZE))
        .ok_or_else(|| io::Error::other("benchmark capacity overflow"))?;
    let blocks = required.div_ceil(BLOCK_SIZE);
    blocks
        .checked_mul(BLOCK_SIZE)
        .ok_or_else(|| io::Error::other("benchmark capacity overflow").into())
}

fn checked_operations(items: usize, rounds: u64) -> AnyResult<u64> {
    items_as_u64(items)?
        .checked_mul(rounds)
        .ok_or_else(|| io::Error::other("benchmark operation count overflow").into())
}

fn items_as_u64(items: usize) -> AnyResult<u64> {
    Ok(u64::try_from(items)?)
}

fn ensure_count(name: &str, actual: usize, expected: usize) -> AnyResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{name} count mismatch: expected {expected}, got {actual}"
        ))
        .into())
    }
}

fn report(name: &str, operations: u64, elapsed: Duration) {
    let nanos = elapsed.as_nanos().max(1);
    let per_second = u128::from(operations).saturating_mul(1_000_000_000) / nanos;
    println!("{name:<30} {:>12} {:>16}", elapsed.as_millis(), per_second);
}

fn parse_items() -> AnyResult<usize> {
    let Some(argument) = std::env::args().nth(1) else {
        return Ok(DEFAULT_ITEMS);
    };
    let items = argument.parse::<usize>()?;
    if items == 0 {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "item count must be nonzero").into(),
        );
    }
    Ok(items)
}

struct BenchData {
    keys: Vec<[u8; KEY_SIZE]>,
    values: Vec<[u8; VALUE_SIZE]>,
}

impl BenchData {
    fn new(items: usize) -> AnyResult<Self> {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        keys.try_reserve_exact(items)
            .map_err(|_| floresta_db::Error::OutOfMemory)?;
        values
            .try_reserve_exact(items)
            .map_err(|_| floresta_db::Error::OutOfMemory)?;
        for index in 0..items {
            let index = u64::try_from(index)?;
            let encoded = index.to_le_bytes();
            let mut key = [0_u8; KEY_SIZE];
            key[..8].copy_from_slice(&encoded);
            key[8..].copy_from_slice(&xxh64(&encoded, 0x4245_4e43_484d_4152).to_le_bytes());
            keys.push(key);
            values.push(index.rotate_left(17).to_le_bytes());
        }
        Ok(Self { keys, values })
    }

    fn len(&self) -> usize {
        self.keys.len()
    }
}

struct BenchDirectory {
    path: PathBuf,
}

impl BenchDirectory {
    fn new(name: &str) -> io::Result<Self> {
        let path =
            std::env::temp_dir().join(format!("floresta-db-bench-{}-{name}", std::process::id()));
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BenchDirectory {
    fn drop(&mut self) {
        let _removed = std::fs::remove_dir_all(&self.path);
    }
}
