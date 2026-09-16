// SPDX-License-Identifier: MIT OR Apache-2.0

//! A CAS-only concurrent, memory-mapped database for Floresta.
//!
//! The crate stores fixed-width keys in a separately chained hash table. Writers
//! publish fully initialized nodes with compare-and-swap operations. Batch APIs
//! SIMD-hash keys and process buckets in ascending order. Unique deletions unlink
//! nodes directly, and empty blocks enter a tagged CAS free list for reuse before
//! mapped backing files grow.
//!
//! The database supports one Linux x86-64 process with many threads.
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

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]
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
mod sys;
mod table;

pub use config::{Config, DEFAULT_HASH_SEED, MAX_INLINE_VALUE_SIZE, MAX_KEY_SIZE, Mode};
pub use error::{Error, Result};
pub use hash::xxh64;
pub use table::{Database, PutResult, WriteOnlyWriter};

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
compile_error!("floresta-db currently supports only Linux x86-64");
