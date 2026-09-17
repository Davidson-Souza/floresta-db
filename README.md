<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# floresta-db

`floresta-db` is a fixed-width, concurrent key-value store for Bitcoin-style indexes. It is designed for one Linux x86-64 process with many threads, large sparse files, append-heavy construction, and explicit checkpoint-based recovery. The default crate has no external dependencies.

It is a good fit when:

- keys have one fixed width known when the database is created;
- the workload is dominated by parallel inserts, point reads, and ordered batch work;
- the application can coordinate destructive operations such as replacement and deletion;
- losing mutations made after the last checkpoint is acceptable after a crash.

It is not a general SQL engine, a multi-process database, or a transactional store with per-write crash durability.

## Quick start

```rust
use floresta_db::{Config, Database, Mode, PutResult};

let path = "utxo.db";
let key = [0_u8; 36];
let value = b"serialized output";

let mut config = Config::new(Mode::Map, 1 << 20, key.len());
config.body_capacity = 8 << 30;
config.blob_capacity = 8 << 30;

let database = Database::create(path, config)?;
assert_eq!(database.put(&key, value)?, PutResult::Inserted);
assert_eq!(database.get(&key)?.as_deref(), Some(value.as_slice()));

// A checkpoint is the crash-recovery boundary.
database.checkpoint()?;
database.close()?;

let reopened = Database::open(path)?;
assert_eq!(reopened.get(&key)?.as_deref(), Some(value.as_slice()));
# Ok::<(), floresta_db::Error>(())
```

Set mode stores keys without values:

```rust
use floresta_db::{Config, Database, Mode};

let database = Database::create("set.db", Config::new(Mode::Set, 1 << 16, 32))?;
let key = [7_u8; 32];
database.add(&key)?;
assert!(database.contains(&key)?);
# Ok::<(), floresta_db::Error>(())
```

The create path must not exist. Bucket count, key width, block size, capacities, and value mode become part of the persistent layout.

## Data model and APIs

`Mode::Set` stores fixed-width keys. `Mode::Map` stores fixed-width keys plus either:

- fixed values of up to eight bytes directly inside each node; or
- variable-width values in a separate blob file.

The main scalar operations are:

- `add` / `contains` / `delete` for sets;
- `put` / `get` / `delete` for maps;
- `sync` to flush current mappings;
- `checkpoint` and `open` for crash recovery;
- `close` and `open_runtime` for a clean runtime-file reopen.

Reads return owned values rather than slices into mapped storage.

### Batch construction

Batch APIs hash and group work by bucket so each bucket is visited or published once per group:

- `add_batch` appends set keys;
- `WriteOnlyWriter::put_batch` builds maps without duplicate lookups;
- `batch_fetch` resolves requests and restores input order;
- `batch_delete` removes the first matching node for each unique requested key.

Append-only batch writers deliberately retain duplicate keys. `batch_fetch` accepts duplicate requests; `batch_delete` rejects them.

## Concurrency contract

The supported topology is one process with many threads. Append-only writes and reads use atomic publication and validation; there is no global write lock.

Deletion and replacing `put` calls are different. The database intentionally has no hazard pointers, epochs, or reader-tracking layer. Before removing a key, the caller must guarantee:

1. unique ownership of that logical key;
2. no reader, competing remover/replacer, or checkpoint can retain an offset in the affected bucket;
3. the operation's higher-level ordering makes the key eligible for removal.

Per-bucket delete locks serialize internal deleters, but they do not replace this ownership and quiescence contract. Once a node is unlinked, an empty block may be reused immediately.

This design keeps the append/read hot path small and fast. Applications that need arbitrary reads concurrent with arbitrary deletes need a reclamation layer above the database or a different storage engine.

## Durability and recovery

A checkpoint is the recovery boundary. `Database::open` validates both checkpoint generations, selects the newest valid one, and rebuilds fresh mutable runtime files. Mutations completed after that checkpoint can be lost after a crash.

A checkpoint may overlap append-only writes. It contains every append completed before capture began; overlapping appends may or may not be included. Deletions and replacements must not overlap checkpoint capture.

`Database::sync` flushes runtime mappings but does not create a recoverable checkpoint generation.

Inline-value maps are intended for clean runtime reopen and do not support checkpoint creation. Close them cleanly and use `Database::open_runtime`.

## Storage assumptions

- Linux x86-64 with 4 KiB pages.
- Sparse `mmap` files with stable maximum virtual reservations.
- Backing files grow one allocation block at a time.
- Bucket heads rely on the operating system page cache; no `mlock` limit is required.
- Filesystem calls, page faults, file growth, startup, and checkpoints are outside the lock-free progress guarantee.
- Capacities are configured maxima, not eagerly allocated disk usage.

Important configuration fields:

- `bucket_count`: more buckets shorten collision chains at the cost of a larger head table;
- `body_capacity`: maximum space reserved for fixed-width nodes;
- `blob_capacity`: maximum external value space for non-inline maps;
- `block_size`: file-growth and block-reuse granularity;
- `inline_value_size`: zero for blob values, or one through eight bytes inline.

## Strong points

- CAS publication with acquire-validated reads.
- Locality-oriented batch hashing and bucket ordering.
- Four-key AVX2 XXH64 with a scalar fallback.
- One metadata reservation CAS for many fixed-width batch nodes.
- Tagged LIFO free lists that reuse empty blocks before growing files.
- Checksummed nodes, values, headers, manifests, and checkpoint fallback.
- Stable sparse mappings: growth does not invalidate published offsets.
- Dependency-free default build.

The tradeoff is deliberate: the engine gets these properties by narrowing the deployment and concurrency model rather than hiding coordination behind a general-purpose transaction layer.

## Examples

### Stress runner

```text
cargo run --release --example stress -- 1000 100 16 stress
```

Arguments are block count, outputs per block, maximum worker count, and output prefix. It writes `stress.csv` and `stress.svg`.

### Bitcoin Core load and Swift Sync hints

The optional loader reads an existing active chain through `libbitcoinkernel` and builds a compact UTXO index plus `swiftsync.hints`:

```text
cargo run --release --features bitcoin-load --example bitcoin-load -- \
  DATA_DIR BLOCKS_DIR [mainnet|testnet|testnet4|signet|regtest] \
  [TIP|tip] [ADD_THREADS] [REMOVE_THREADS] [RANGE_SIZE] [WORK_DIR]
```

The work directory must not exist. Building this feature requires CMake, a C++ compiler, and Boost. Stop any process that exclusively locks the selected Bitcoin Core data directory before running it.

After indexing, the example closes the database and destructively uses its body file as external-sort workspace for hints generation. `WORK_DIR/index` is therefore not a reopenable database after a successful loader run. Tuning variables are `DB_LOAD_BUCKETS`, `DB_LOAD_BODY_GIB`, and `DB_LOAD_BLOCK_MIB`.

## Validation

```text
cargo +nightly fmt --check
cargo +nightly clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc
cargo doc --no-deps --all-features
cargo +nightly miri test
```

Real mapping, growth, and allocation tests do not run under Miri; pure layout and hashing tests do. For native memory checking:

```text
cargo test --no-run
valgrind --tool=memcheck --leak-check=full target/debug/deps/floresta_db-<hash>
```

The model-based fuzz target checks scalar and batched operations plus checkpoint/reopen behavior against a `BTreeMap` model:

```text
cargo install cargo-fuzz
cargo +nightly fuzz run database
```
