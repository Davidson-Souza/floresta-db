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
- No Cargo dependencies.

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
