<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# floresta-db

A dependency-free-by-default, Linux x86-64 CAS-only concurrent storage engine for Floresta and Bitcoin-style data.

## Properties

- Fixed-width keys and separate-chaining bucket lists.
- Optional values for map mode; set mode has no blob file.
- In-tree XXH64 with four-key AVX2 batch hashing and a scalar fallback.
- Acquire-only bucket and link reads.
- Direct CAS unlinking under an explicit unique-deletion contract.
- Stable maximum `mmap` reservations whose backing files grow block-by-block.
- Tagged CAS LIFO free lists that reuse empty blocks before file growth.
- The active persisted bucket-head bank is pinned with Linux `mlock`.
- Concurrent per-bucket checkpoints during append-only writes.
- No default Cargo dependencies; the Bitcoin Core load test is feature-gated.

The database supports one process with many threads. Every shared state mutation uses `compare_exchange`; readers use acquire loads. Filesystem calls, page faults, checkpoints, and process startup are outside the lock-free progress guarantee.

`Database::create`, `open`, and `open_runtime` fail if `mlock` cannot pin the active head bank. Configure `RLIMIT_MEMLOCK` above `align_up(bucket_count * 8, 4096)` plus any other process locks. The default loader head bank is exactly 8 MiB, so a process limited to 8 MiB may need a higher limit or a smaller `DB_LOAD_BUCKETS`.

Deletion and replacement deliberately use no reader-tracking system. The caller must guarantee unique ownership of a key being removed and must prevent reads, replacements, deletions, or checkpoints from retaining an offset in the affected bucket while the removal runs. An empty block can be reused immediately after its unlink CAS succeeds.

## Example

```rust
use floresta_db::{Config, Database, Mode};

let outpoint = [0_u8; 36];
let serialized_output = b"serialized output";
let mut config = Config::new(Mode::Map, 1 << 20, outpoint.len());
config.body_capacity = 8 << 30;
config.blob_capacity = 8 << 30;

let database = Database::create("utxo.db", config)?;
database.put(&outpoint, serialized_output)?;
let output = database.get(&outpoint)?;
assert_eq!(output.as_deref(), Some(serialized_output.as_slice()));
database.delete(&outpoint)?;
database.checkpoint()?;
# Ok::<(), floresta_db::Error>(())
```

`Database::open` restores the newest valid checkpoint into fresh mutable runtime files. Mutations after the checkpoint may be lost after a crash. A concurrent checkpoint contains every append completed before checkpoint invocation; overlapping appends may or may not be included. Deletions and replacements must not overlap checkpoint capture.

## Locality-Optimized Batches

`Database::add_batch` and `WriteOnlyWriter::put_batch` are optimized for append-only construction. They validate the complete batch, SIMD-hash keys four at a time, sort entries by bucket, privately chain every same-bucket group, and publish that group with one successful head CAS. Buckets are committed in ascending order. These paths do not search for duplicate keys; duplicates remain in the chain, with the last duplicate in a batch observed first.

`Database::batch_fetch` and `Database::batch_delete` use the same hash-and-sort pipeline. They visit each requested bucket once and restore results to input order. `batch_fetch` permits duplicate requests; `batch_delete` rejects them and inherits the unique-deletion and quiescence contract.


## Validation

```text
cargo +nightly fmt --check
cargo +nightly clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc
cargo doc --no-deps --all-features
cargo +nightly miri test
```

The real `mmap`, file-growth, and `fallocate` integration tests are disabled under Miri; pure layout and hashing tests still run there. Valgrind can run the compiled unit-test executable:

```text
cargo test --no-run
valgrind --tool=memcheck --leak-check=full target/debug/deps/floresta_db-<hash>
```

## Fuzzing

Install [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) once, then run the `database` target:

```text
cargo install cargo-fuzz
cargo +nightly fuzz run database
```

The target in `fuzz/fuzz_targets/database.rs` decodes inputs into scalar and batched puts, fetches, and deletes plus checkpoint/reopen operations. Every result is checked against a stacked `BTreeMap` reference model so retained duplicates are observable.

If ASan reports that its shadow range overlaps the executable on a hardened kernel, build the fuzz target as non-PIE:

```text
RUSTFLAGS="-C link-arg=-no-pie" cargo +nightly fuzz run database
```

## Stress Evaluation

The stress example gives 75% of generated outputs a spend lifetime from 1 through 100 blocks. Each worker owns an independent block stream, avoiding a benchmark barrier in the database hot path.

```text
cargo run --release --example stress -- 1000 100 16 stress
```

Arguments are blocks, outputs per block, maximum workers, and output prefix. The runner tests powers of two through the requested worker count and writes `stress.csv` plus a dependency-free `stress.svg` throughput chart.

## Bitcoin Core Load and Swift Sync Hints

The feature-gated `bitcoin-load` example reads an existing active chain through `libbitcoinkernel`; no RPC server or flat-file ring is used. Building this feature requires Bitcoin Core's native build dependencies, including CMake, a C++ compiler, and Boost. Stop any process that exclusively locks the selected Bitcoin Core data directory before running it.

```text
cargo run --release --features bitcoin-load --example bitcoin-load -- \
  DATA_DIR BLOCKS_DIR [mainnet|testnet|testnet4|signet|regtest] \
  [TIP|tip] [ADD_THREADS] [REMOVE_THREADS] [RANGE_SIZE] [WORK_DIR]
```

Adder and remover pools independently claim small block ranges through CAS counters. Every adder publishes its completed height into a shared progress set and notifies a condition variable. Removers only claim ranges ending at or below the current minimum safe height; they wait on the condition variable only when no unclaimed safe block remains.

Progress is emitted as line-oriented `key=value` records. Kernel initialization, every range claim/completion, each worker reaching tip, index completion, hints scanning every 10,000 blocks, and hints encoding are logged immediately with `stage` and `event` fields.

Eligible outputs use a 12-byte key: the first 64 bits of the internal txid representation plus little-endian `vout`. The inline value is the eligible output's zero-based index within its block. Genesis, `OP_RETURN` outputs, and scripts larger than 10,000 bytes are not eligible. On mainnet only, the overwritten BIP30 coinbases at heights 91,722 and 91,812 are also excluded.

At completion, per-block eligible counts are folded into global offsets—for example, `[1, 4]` becomes `[0, 1, 5]`. A second `libbitcoinkernel` pass resolves the surviving local indices, rejects detected 64-bit txid collisions, and writes `WORK_DIR/swiftsync.hints` with the `hintsfile` crate.

The work directory must not already exist. `tip` selects the active-chain tip observed at startup; an explicit height makes runs repeatable. Tuning variables are `DB_LOAD_BUCKETS`, `DB_LOAD_BODY_GIB`, and `DB_LOAD_BLOCK_MIB`.
