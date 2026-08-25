#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

mod allocator;
mod checkpoint;
mod config;
mod error;
mod hash;
mod hazard;
mod layout;
mod mapped_file;
mod node;
mod sys;
mod table;

pub use config::{Config, Mode};
pub use error::{Error, Result};
pub use hash::xxh64;
pub use table::{Database, PutResult};

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
compile_error!("db-experiment currently supports only Linux x86-64");
