// SPDX-License-Identifier: MIT OR Apache-2.0

//! Measures concurrent insert/delete throughput with Bitcoin-like spend lifetimes.
//!
//! Run with `cargo run --release --example stress -- <blocks> <outputs-per-block>
//! <maximum-workers> <output-prefix>`. The example writes CSV data and an SVG chart.

use std::error::Error as StdError;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use floresta_db::{Config, Database, Error, Mode, Result};

const SPEND_PERCENT: u64 = 75;
const SPEND_WINDOW: usize = 100;
const BLOCK_SIZE: u64 = 1 << 20;

fn main() {
    if let Err(error) = run() {
        eprintln!("stress failure: {error}");
        std::process::exit(1);
    }
}

fn run() -> std::result::Result<(), Box<dyn StdError>> {
    let blocks = usize_argument(1, 200)?;
    let outputs_per_block = usize_argument(2, 100)?;
    let requested_threads = usize_argument(3, available_threads())?;
    let output = std::env::args()
        .nth(4)
        .map_or_else(|| PathBuf::from("stress"), PathBuf::from);
    let thread_counts = thread_counts(requested_threads)?;
    let mut results = Vec::new();
    results
        .try_reserve_exact(thread_counts.len())
        .map_err(|_| Error::OutOfMemory)?;

    for threads in thread_counts {
        let result = run_case(threads, blocks, outputs_per_block)?;
        println!(
            "threads={} operations={} throughput={:.0} ops/s p99={} ns",
            result.threads, result.operations, result.throughput, result.p99_nanos
        );
        results.push(result);
    }
    write_csv(&output.with_extension("csv"), &results)?;
    write_svg(&output.with_extension("svg"), &results)?;
    Ok(())
}

fn run_case(threads: usize, blocks: usize, outputs_per_block: usize) -> Result<CaseResult> {
    let path = stress_path(threads);
    let _ignored = std::fs::remove_dir_all(&path);
    let creations = threads
        .checked_mul(blocks)
        .and_then(|value| value.checked_mul(outputs_per_block))
        .ok_or(Error::InvalidConfig("stress operation count overflow"))?;
    let body_capacity = capacity_for(creations, 80)?;
    let blob_capacity = capacity_for(creations, 16)?;
    let mut config = Config::new(Mode::Map, next_power_of_two(creations / 2 + 1)?);
    config.block_size = BLOCK_SIZE;
    config.body_capacity = body_capacity;
    config.blob_capacity = blob_capacity;
    let database = Database::create(&path, config)?;

    let started = Instant::now();
    let worker_stats = std::thread::scope(|scope| -> Result<Vec<WorkerStats>> {
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(threads)
            .map_err(|_| Error::OutOfMemory)?;
        for thread in 0..threads {
            let database_ref = &database;
            handles
                .push(scope.spawn(move || worker(database_ref, thread, blocks, outputs_per_block)));
        }
        let mut stats = Vec::new();
        stats
            .try_reserve_exact(threads)
            .map_err(|_| Error::OutOfMemory)?;
        for handle in handles {
            stats.push(
                handle
                    .join()
                    .map_err(|_| Error::Corrupt("stress worker panicked"))??,
            );
        }
        Ok(stats)
    })?;
    let elapsed = started.elapsed();

    let mut operations = 0_u64;
    let mut samples = Vec::new();
    for stats in worker_stats {
        operations = operations.saturating_add(stats.operations);
        samples
            .try_reserve(stats.samples.len())
            .map_err(|_| Error::OutOfMemory)?;
        samples.extend(stats.samples);
    }
    samples.sort_unstable();
    let p50_nanos = percentile(&samples, 50);
    let p99_nanos = percentile(&samples, 99);
    database.checkpoint()?;
    drop(database);
    std::fs::remove_dir_all(path)?;
    Ok(CaseResult {
        threads,
        operations,
        elapsed,
        throughput: operations_per_second(operations, elapsed),
        p50_nanos,
        p99_nanos,
    })
}

fn worker(
    database: &Database,
    thread: usize,
    blocks: usize,
    outputs_per_block: usize,
) -> Result<WorkerStats> {
    let mut due = Vec::<Vec<[u8; 16]>>::new();
    due.try_reserve_exact(SPEND_WINDOW + 1)
        .map_err(|_| Error::OutOfMemory)?;
    for _index in 0..=SPEND_WINDOW {
        due.push(Vec::new());
    }
    let mut random = XorShift64::new((thread as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let mut operations = 0_u64;
    let mut sequence = 0_u64;
    let mut samples = Vec::new();

    for block in 0..blocks {
        let due_index = block % due.len();
        while let Some(key) = due
            .get_mut(due_index)
            .ok_or(Error::Corrupt("spend queue index is invalid"))?
            .pop()
        {
            let started = Instant::now();
            database.delete(&key)?;
            record_sample(&mut samples, operations, started.elapsed())?;
            operations = operations.saturating_add(1);
        }
        for _output in 0..outputs_per_block {
            let thread_bits = u64::try_from(thread)
                .map_err(|_| Error::InvalidConfig("thread index overflow"))?
                << 56;
            let encoded = (thread_bits | sequence).to_le_bytes();
            let mut key = [0_u8; 16];
            key[..8].copy_from_slice(&encoded);
            key[8..].copy_from_slice(&(thread_bits | sequence).rotate_left(29).to_le_bytes());
            sequence = sequence
                .checked_add(1)
                .ok_or(Error::CapacityExhausted("stress key sequence"))?;
            let started = Instant::now();
            database.put(&key, &block.to_le_bytes())?;
            record_sample(&mut samples, operations, started.elapsed())?;
            operations = operations.saturating_add(1);

            if random.next() % 100 < SPEND_PERCENT {
                let lifetime = usize::try_from(random.next() % SPEND_WINDOW as u64 + 1)
                    .map_err(|_| Error::Corrupt("spend lifetime overflow"))?;
                let spend_index = (block + lifetime) % due.len();
                due.get_mut(spend_index)
                    .ok_or(Error::Corrupt("spend queue index is invalid"))?
                    .try_reserve(1)
                    .map_err(|_| Error::OutOfMemory)?;
                due.get_mut(spend_index)
                    .ok_or(Error::Corrupt("spend queue index is invalid"))?
                    .push(key);
            }
        }
    }
    Ok(WorkerStats {
        operations,
        samples,
    })
}

fn record_sample(samples: &mut Vec<u64>, operation: u64, elapsed: Duration) -> Result<()> {
    if operation % 1_024 == 0 {
        samples.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        samples.push(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
    }
    Ok(())
}

fn capacity_for(objects: usize, bytes_per_object: u64) -> Result<u64> {
    let objects =
        u64::try_from(objects).map_err(|_| Error::InvalidConfig("stress object count overflow"))?;
    let requested = objects
        .checked_mul(bytes_per_object)
        .and_then(|value| value.checked_add(BLOCK_SIZE * 4))
        .ok_or(Error::InvalidConfig("stress capacity overflow"))?;
    let blocks = requested
        .checked_add(BLOCK_SIZE - 1)
        .ok_or(Error::InvalidConfig("stress capacity overflow"))?
        / BLOCK_SIZE;
    blocks
        .checked_mul(BLOCK_SIZE)
        .ok_or(Error::InvalidConfig("stress capacity overflow"))
}

fn next_power_of_two(value: usize) -> Result<u64> {
    let value = u64::try_from(value).map_err(|_| Error::InvalidConfig("bucket count overflow"))?;
    value
        .checked_next_power_of_two()
        .ok_or(Error::InvalidConfig("bucket count overflow"))
}

fn percentile(samples: &[u64], percentile: usize) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let index = (samples.len() - 1).saturating_mul(percentile) / 100;
    samples.get(index).copied().unwrap_or(0)
}

#[allow(clippy::cast_precision_loss)]
fn operations_per_second(operations: u64, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64()
}

fn thread_counts(maximum: usize) -> Result<Vec<usize>> {
    if maximum == 0 || maximum > 255 {
        return Err(Error::InvalidConfig(
            "stress threads must be between 1 and 255",
        ));
    }
    let mut counts = Vec::new();
    let mut count = 1_usize;
    while count < maximum {
        counts.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
        counts.push(count);
        count = count.saturating_mul(2);
    }
    counts.try_reserve(1).map_err(|_| Error::OutOfMemory)?;
    counts.push(maximum);
    Ok(counts)
}

fn available_threads() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}

fn usize_argument(index: usize, default: usize) -> std::result::Result<usize, io::Error> {
    let Some(value) = std::env::args().nth(index) else {
        return Ok(default);
    };
    value.parse::<usize>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("argument {index} is not an integer: {error}"),
        )
    })
}

fn write_csv(path: &Path, results: &[CaseResult]) -> io::Result<()> {
    let mut file = File::create(path)?;
    writeln!(
        file,
        "threads,operations,elapsed_seconds,throughput_ops_s,p50_ns,p99_ns"
    )?;
    for result in results {
        writeln!(
            file,
            "{},{},{:.6},{:.3},{},{}",
            result.threads,
            result.operations,
            result.elapsed.as_secs_f64(),
            result.throughput,
            result.p50_nanos,
            result.p99_nanos
        )?;
    }
    Ok(())
}

#[allow(clippy::cast_precision_loss)]
fn write_svg(path: &Path, results: &[CaseResult]) -> io::Result<()> {
    let mut file = File::create(path)?;
    let maximum = results
        .iter()
        .map(|result| result.throughput)
        .fold(1.0_f64, f64::max);
    let denominator = results.len().saturating_sub(1).max(1) as f64;
    writeln!(
        file,
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="960" height="540" viewBox="0 0 960 540">"#
    )?;
    writeln!(file, r##"<rect width="960" height="540" fill="#111827"/>"##)?;
    writeln!(
        file,
        r##"<text x="64" y="42" fill="#f9fafb" font-family="monospace" font-size="24">UTXO CAS throughput</text>"##
    )?;
    writeln!(
        file,
        r##"<path d="M72 472H920M72 72V472" stroke="#64748b" stroke-width="2"/>"##
    )?;
    let mut previous = None;
    for (index, result) in results.iter().enumerate() {
        let x = 72.0 + index as f64 / denominator * 848.0;
        let y = 472.0 - result.throughput / maximum * 380.0;
        if let Some((old_x, old_y)) = previous {
            writeln!(
                file,
                r##"<line x1="{old_x:.2}" y1="{old_y:.2}" x2="{x:.2}" y2="{y:.2}" stroke="#22d3ee" stroke-width="4"/>"##
            )?;
        }
        writeln!(
            file,
            r##"<circle cx="{x:.2}" cy="{y:.2}" r="6" fill="#f59e0b"/>"##
        )?;
        writeln!(
            file,
            r##"<text x="{x:.2}" y="500" text-anchor="middle" fill="#cbd5e1" font-family="monospace" font-size="14">{}</text>"##,
            result.threads
        )?;
        previous = Some((x, y));
    }
    writeln!(
        file,
        r##"<text x="496" y="528" text-anchor="middle" fill="#94a3b8" font-family="monospace" font-size="14">worker threads</text></svg>"##
    )?;
    Ok(())
}

fn stress_path(threads: usize) -> PathBuf {
    std::env::temp_dir().join(format!(
        "floresta-db-stress-{}-{threads}",
        std::process::id()
    ))
}

struct WorkerStats {
    operations: u64,
    samples: Vec<u64>,
}

struct CaseResult {
    threads: usize,
    operations: u64,
    elapsed: Duration,
    throughput: f64,
    p50_nanos: u64,
    p99_nanos: u64,
}

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }
}
