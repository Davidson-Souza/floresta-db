#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

mod allocator;
mod config;
mod error;
mod hash;
mod layout;
mod mapped_file;
mod sys;

pub use config::{Config, Mode};
pub use error::{Error, Result};
pub use hash::xxh64;

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
compile_error!("db-experiment currently supports only Linux x86-64");
