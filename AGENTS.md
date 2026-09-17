<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# Repository Guidelines

## Project Overview

`floresta-db` is a Rust 2024 CAS-only concurrent storage engine for fixed-width, Bitcoin-style data. It provides map and set modes over growable sparse `mmap` files, with separate-chaining buckets, tagged LIFO free lists, and checkpoint-based recovery. The default crate has no external dependencies.

The supported deployment model is one Linux x86-64 process with many threads. Filesystem calls, page faults, startup, and checkpoints are outside the lock-free progress guarantee. A checkpoint is the recovery boundary: `Database::open` restores the newest valid checkpoint, so later mutations may be lost after a crash.

## Architecture & Data Flow

- `src/lib.rs` exposes the configuration limits, `Config`, `Mode`, `Database`, `PutResult`, `WriteOnlyWriter`, `Error`, `Result`, and `xxh64`.
- `Database` in `src/table.rs` owns mapped files, in-memory atomic bucket heads, block allocators, and checkpoint generation state. There is no global mutable state or service container.
- Create flow: validate `Config`, reserve stable maximum mappings, create one-page `body` and count files, and create `blobs` only for non-inline map values.
- Replacing map write flow: validate input -> allocate/write privately -> publish with a head CAS -> directly unlink the uniquely owned old node -> decrement block counts -> push newly empty sealed blocks onto the free list.
- Batch pipeline: validate every key, hash four equal-width keys per AVX2 vector when available, sort by `(bucket, hash/input order)`, then visit bucket heads in ascending order. Append-only groups use one successful head CAS per bucket. `batch_fetch` and `batch_delete` traverse each requested bucket once and restore results to input order.
- Scalar read flow: traverse with acquire loads, validate link stability and node/value checksums, then copy values out rather than exposing mapped slices.
- `src/allocator.rs` uses packed per-block allocation/count state plus a tagged CAS LIFO free list. Allocation pops a free block before advancing the high-water mark and growing backing files.
- `src/checkpoint.rs` copies buckets into alternating immutable snapshot generations, writes validated manifests, and rebuilds fresh mutable runtime files on open. Checkpoints may overlap append-only writes, not removal operations. `Database::sync` flushes runtime mappings but does not create a checkpoint generation.

Concurrency invariants are architectural: shared state changes use `compare_exchange`, readers use acquire loads, and free-list heads carry ABA tags. There is intentionally no reader-tracking or deferred-reclamation layer. `delete` and replacing `put` require caller-provided unique ownership and quiescence for reads/checkpoints that might retain an affected bucket offset. Empty blocks may be reused immediately after unlink. Do not weaken this contract or alter atomic orderings without proving publication, unlink, reuse, and file-growth behavior together.

## Key Directories

- `src/`: library implementation; tests live inline beside each module.
- `examples/`: runnable stress and feature-gated Bitcoin Core load examples.
- `fuzz/`: independent `cargo-fuzz` package and the `database` fuzz target.

There is no separate `tests/` tree and no generated source directory.

## Development Commands

Run from the repository root:

```text
cargo build --all-features
cargo +nightly fmt --check
cargo +nightly clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc
cargo doc --no-deps --all-features
cargo +nightly miri test
cargo +nightly fuzz run database
cargo run --release --example stress -- 1000 100 16 stress
```

The stress example writes `stress.csv` and `stress.svg`. For the optional loader:

```text
cargo run --release --features bitcoin-load --example bitcoin-load -- \
  DATA_DIR BLOCKS_DIR signet tip 4 4 32 NEW_WORK_DIR
```

Use a new work directory; the loader intentionally fails if it already exists. It reads block files through `libbitcoinkernel` and writes `swiftsync.hints` with the `hintsfile` crate. Building it requires CMake, a C++ compiler, and Boost. Install `cargo-fuzz` with `cargo install cargo-fuzz` before running its nightly fuzz command.

## Code Conventions & Common Patterns

- Prefix every authored file with `SPDX-License-Identifier: MIT OR Apache-2.0`; do not alter Cargo-generated lockfile headers.
- Use standard `rustfmt`. Follow `snake_case` functions/modules, `CamelCase` types, and `SCREAMING_SNAKE_CASE` constants, with blank lines between logical fields, variants, and items.
- Put attributes before their item's Rustdoc. Every module needs a brief `//!` architecture description; every externally reachable item needs meaningful Rustdoc and a compiling example.
- Keep visibility narrow. Export only interfaces downstream users need.
- Return the crate's `Result<T, E = Error>` and propagate with `?`. Prefer exact error variants and context over panics; Clippy denies `unwrap`, `expect`, and `panic`.
- Document public fallible APIs with `# Errors`. Add `# Safety` wherever unsafe contracts exist.
- Prefer safe Rust. Keep unavoidable unsafe code inside narrow syscall/mapping boundaries, validate range and alignment first, and add a concrete `// SAFETY:` explanation. `unsafe_op_in_unsafe_fn` is denied.
- This is synchronous threaded code, not async code. Use scoped threads and atomics for concurrency; do not add an async runtime for synchronous storage paths.
- Dependency injection is explicit: pass `Config`, paths, and borrowed storage objects. State belongs to `Database` and small focused structs rather than globals or a framework container.
- Avoid allocations and copies in hot paths. Private bytes must be fully initialized before publication. `Database::get` returns an owned `Vec<u8>` rather than exposing mapped storage.
- Keep on-disk offsets and alignment in `src/layout.rs`; serialization and checksums belong in `src/node.rs`. Format changes must consider `FORMAT_VERSION`, checkpoint recovery, old manifests, and corruption validation.
- Preserve the dependency-free default. Optional integrations belong behind Cargo features, as `bitcoin-load` does.

## Important Files

- `Cargo.toml`: Rust version, features, optional dependencies, example gate, and lint policy.
- `Cargo.lock`: committed resolved dependency graph; do not hand-edit it.
- `src/lib.rs`: platform guard and public API surface.
- `src/table.rs`: primary `Database` API and bucket-list mutation/read paths.
- `src/checkpoint.rs`: `Database::open`, checkpoint capture, manifests, validation, and recovery.
- `src/allocator.rs`: CAS block allocator, object counts, tagged free list, and high-water growth.
- `src/mapped_file.rs` / `src/sys.rs`: stable growable mapping wrapper and Linux syscall boundary.
- `src/layout.rs` / `src/node.rs`: persistent layout, tags, serialization, and checksums.
- `src/hash.rs`: canonical scalar XXH64 plus the four-key AVX2 implementation and scalar fallback.
- `src/config.rs` / `src/error.rs`: validated configuration and canonical errors.
- `fuzz/Cargo.toml` / `fuzz/fuzz_targets/database.rs`: `cargo-fuzz` package and model-based state-machine target.
- `README.md`: current usage, validation, stress, and Bitcoin-load runbook.
- `PLAN.md`: original design intent. Verify it against current code; for example, the implementation uses separate chaining rather than the plan's early linear-probing description.

## Runtime/Tooling Preferences

- Use Cargo with Rust 1.85 or newer; the crate uses edition 2024. No `rust-toolchain` file pins a compiler. Nightly is needed for Miri, formatting/Clippy parity with Floresta, and `cargo-fuzz`.
- The crate deliberately fails compilation outside Linux x86-64 and expects 4 KiB pages plus Linux `mmap`, `madvise`, `msync`, and `fallocate` allocation behavior.
- Database create/open advises the persisted head mapping for random access but does not pin pages; deployments do not require a raised `RLIMIT_MEMLOCK`.
- Default features are empty. `bitcoin`, `bitcoinkernel`, and `hintsfile` are optional and enabled only by `bitcoin-load`.
- `.env` is gitignored. Loader tuning variables are `DB_LOAD_BUCKETS`, `DB_LOAD_BODY_GIB`, and `DB_LOAD_BLOCK_MIB`.
- `target/`, fuzz artifacts/corpora, loader work directories, and root `*.data`, `*.csv`, and `*.svg` outputs are generated and ignored.
- No CI, container, custom rustfmt, or standalone Clippy configuration is present. Treat the README validation sequence as the local quality gate.

## Testing & QA

Tests use Rust's built-in harness in module-local `mod tests` blocks. Names are descriptive `snake_case`; fallible tests commonly return `Result<()>` to use `?`. Reuse the existing `test_path`, `test_paths`, `test_directory`, and config helper patterns, create artifacts under `std::env::temp_dir()` with process-specific names, and clean them up explicitly.

Pure layout and hash tests run under Miri. Real mapping, file-growth, allocator, table, and checkpoint tests use `#[cfg(all(test, not(miri)))]`, so Miri is not a substitute for native `cargo test`. For memory checking:

```text
cargo test --no-run
valgrind --tool=memcheck --leak-check=full target/debug/deps/floresta_db-<hash>
```

Match tests to the changed invariant: append publication, unique direct unlinking, free-list LIFO/ABA behavior, reuse before growth, clean reopen, checkpoint fallback, and corruption handling. Use `cargo-fuzz` or the stress example for state-machine and concurrency changes. Critical paths should have complete behavioral coverage; other code should retain meaningful normal, boundary, error, concurrent, corruption, and recovery coverage.
