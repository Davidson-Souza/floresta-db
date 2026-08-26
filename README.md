# db-experiment

A dependency-free, Linux x86-64 experiment in CAS-only concurrent storage for Bitcoin-style data.

## Properties

- Fixed-width keys and separate-chaining bucket lists.
- Optional values for map mode; set mode has no blob file.
- XXH64 hashing implemented in-tree.
- Acquire-only reader loads plus CAS-published hazard pointers.
- Mark-before-unlink deletion and replacement cleanup.
- Sparse, fixed-capacity `mmap` files with per-block CAS allocation/count state.
- Online `FALLOC_FL_PUNCH_HOLE` reclamation after hazard-safe retirement.
- Concurrent per-bucket checkpoints into alternating immutable generations.
- No default Cargo dependencies; the Bitcoin Core load test is feature-gated.

The database supports one process with many threads. Every shared state mutation uses `compare_exchange`; readers use acquire loads and hazard CAS operations. Filesystem calls, page faults, checkpoints, and process startup are outside the lock-free progress guarantee.

## Example

```rust
use db_experiment::{Config, Database, Mode};

let mut config = Config::new(Mode::Map, 1 << 20, 36);
config.body_capacity = 8 << 30;
config.blob_capacity = 8 << 30;

let database = Database::create("utxo.db", config)?;
database.put(&outpoint, &serialized_output)?;
let output = database.get(&outpoint)?;
database.delete(&outpoint)?;
database.checkpoint()?;
# Ok::<(), db_experiment::Error>(())
```

`Database::open` restores the newest valid checkpoint into fresh mutable runtime files. Mutations after the checkpoint may be lost after a crash. A concurrent checkpoint contains every operation completed before checkpoint invocation; overlapping operations may or may not be included.

## Validation

```text
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo +nightly miri test
cargo run --release --bin fuzz -- 1 100000
```

The real mmap and hole-punch integration tests are disabled under Miri; pure layout, hashing, and hazard-pointer tests still run there. Valgrind can run the compiled unit-test executable:

```text
cargo test --no-run
valgrind --tool=memcheck --leak-check=full target/debug/deps/db_experiment-<hash>
```

## Stress Evaluation

The stress runner gives 75% of generated outputs a spend lifetime from 1 through 100 blocks. Each worker owns an independent block stream, avoiding a benchmark barrier in the database hot path.

```text
cargo run --release --bin stress -- 1000 100 16 stress
```

Arguments are blocks, outputs per block, maximum workers, and output prefix. The runner tests powers of two through the requested worker count and writes `stress.csv` plus a dependency-free `stress.svg` throughput chart.

## Bitcoin Core Load Test

The optional `bitcoin-load` binary fetches real blocks with a producer pool, stores them in a bounded flat-file ring, indexes outputs in height order, and removes spent inputs with a consumer pool. It skips the genesis output and scripts Bitcoin Core excludes from its UTXO set. At completion it compares the live output count with `gettxoutsetinfo` at the exact selected block, which requires a synced `coinstatsindex` at that height.

```text
set -a && source .env && set +a
cargo run --release --features bitcoin-load --bin bitcoin-load -- \
  COOKIE_FILE|USER:PASSWORD|none RPC_URL [TIP|tip] [FETCH_THREADS] \
  [SPEND_THREADS] [RING_SLOTS] [WORK_DIR]
```

The work directory must not already exist. `tip` selects the node tip observed at startup; an explicit height makes repeatable runs possible while the chain advances.

Tuning variables are `DB_LOAD_SLOT_MIB`, `DB_LOAD_BUCKETS`, `DB_LOAD_BODY_GIB`, `DB_LOAD_BLOB_GIB`, `DB_LOAD_BLOCK_MIB`, and `DB_LOAD_RPC_DELAY_MS`. Set `DB_LOAD_CHECKPOINT` to any value to create a final checkpoint. RPC pacing defaults to 15 ms per worker because some HTTP servers do not advertise keep-alive and can otherwise exhaust ephemeral ports during long runs.

The final performance report uses machine-readable `key=value` fields. It includes block, payload, and database-operation rates; RPC calls, retries, transport time, pacing, and retry waits; producer ordering and ring waits; serialization and deserialization; aggregate `contains`, `put`, and `delete` latency; verification, checkpoint, and sync duration; and logical versus physically allocated storage. Each timing has an operation count, summed worker time, average latency, and maximum latency. Stage timings include their nested database operations, and summed times may exceed wall time because workers run concurrently.

A signet integration run through block 100,000 processed 100,001 blocks, 3,433,983 indexed outputs, and 2,223,210 spent inputs. Its final 1,210,773 UTXOs matched Bitcoin Core exactly.
