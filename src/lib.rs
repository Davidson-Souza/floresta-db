// SPDX-License-Identifier: MIT OR Apache-2.0

//! A CAS-only concurrent, memory-mapped database for Floresta.
//!
//! The crate stores fixed-width keys in a separately chained hash table. Writers
//! publish fully initialized nodes with compare-and-swap operations. Batch APIs
//! SIMD-hash keys and process buckets in ascending order. Unique deletions unlink
//! nodes directly, and empty blocks enter a tagged CAS free list for reuse before
//! mapped backing files grow.
//!
//! Storage is supported on 64-bit little-endian Linux and macOS targets.
//! Windows targets compile for API portability but storage operations return
//! [`Error::Unsupported`] rather than weaken the stable-growth contract.
//!
//! # Example
//!
//! ```no_run
//! use floresta_db::{Config, Database, Mode};
//!
//! let path = "/var/lib/floresta/utxo";
//! let mut config = Config::new(Mode::Map, 1 << 20, 36);
//! config.body_capacity = 8 << 30;
//! config.blob_capacity = 8 << 30;
//!
//! let database = Database::create(path, config)?;
//! database.put(&[0; 36], b"serialized output")?;
//! database.checkpoint()?;
//! # Ok::<(), floresta_db::Error>(())
//! ```

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod allocator;
mod checkpoint;
mod config;
mod error;
mod hash;
mod layout;
mod mapped_file;
mod node;
mod platform;
mod table;

pub use config::{Config, DEFAULT_HASH_SEED, MAX_INLINE_VALUE_SIZE, MAX_KEY_SIZE, Mode};
pub use error::{Error, Result};
pub use hash::xxh64;
pub use table::{Database, PutResult, WriteOnlyWriter};

#[cfg(not(target_pointer_width = "64"))]
compile_error!("floresta-db requires a 64-bit target");
#[cfg(not(target_has_atomic = "64"))]
compile_error!("floresta-db requires lock-free 64-bit atomics");
#[cfg(not(target_endian = "little"))]
compile_error!("floresta-db currently requires a little-endian target");
