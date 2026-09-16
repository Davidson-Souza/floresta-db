// SPDX-License-Identifier: MIT OR Apache-2.0

//! Exercises map mutations and checkpoint recovery against a `BTreeMap` model.
//!
//! Each input is decoded into fixed-width operations. The target compares every
//! observable result with the model and periodically reopens a checkpoint.

#![no_main]

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use floresta_db::{Config, Database, Mode};
use libfuzzer_sys::fuzz_target;

const RECORD_BYTES: usize = 17;
const MAX_OPERATIONS: usize = 1_024;
const FUZZ_CAPACITY: u64 = 1 << 20;
const BLOCK_SIZE: u64 = 4_096;

static NEXT_CASE: AtomicU64 = AtomicU64::new(0);

fuzz_target!(|input: &[u8]| {
    if input.len() < RECORD_BYTES {
        return;
    }

    let path = fuzz_path();
    let setup = remove_if_exists(&path);
    let outcome = setup
        .and_then(|()| exercise(input, &path).map_err(|error| io::Error::other(error.to_string())));
    let cleanup = remove_if_exists(&path);

    // A panic is cargo-fuzz's failure signal. Cleanup happens before reporting it.
    if let Err(error) = cleanup {
        panic!("failed to clean up fuzz database: {error}");
    }

    if let Err(error) = outcome {
        panic!("database diverged from the reference model: {error}");
    }
});

fn exercise(input: &[u8], path: &Path) -> Result<(), Box<dyn StdError>> {
    let mut config = Config::new(Mode::Map, 64, 8);
    config.body_capacity = FUZZ_CAPACITY;
    config.blob_capacity = FUZZ_CAPACITY;
    config.block_size = BLOCK_SIZE;
    config.max_threads = 1;

    let mut database = Database::create(path, config)?;
    let mut model = BTreeMap::<[u8; 8], [u8; 8]>::new();

    for (step, record) in input
        .chunks_exact(RECORD_BYTES)
        .take(MAX_OPERATIONS)
        .enumerate()
    {
        let mut key = [0_u8; 8];
        key.copy_from_slice(&record[1..9]);

        let mut value = [0_u8; 8];
        value.copy_from_slice(&record[9..17]);

        match record[0] % 7 {
            0 => {
                database.put(&key, &value)?;
                model.insert(key, value);
            }

            1 => {
                let expected = !model.contains_key(&key);
                let inserted = database.put_new(&key, &value)?;
                ensure_equal(step, "put_new result", inserted, expected)?;
                if inserted {
                    model.insert(key, value);
                }
            }

            2 => {
                let expected = model.remove(&key).is_some();
                let deleted = database.delete(&key)?;
                ensure_equal(step, "delete result", deleted, expected)?;
            }

            3 => compare_value(step, database.get(&key)?.as_deref(), model.get(&key))?,

            4 => {
                let expected = model.contains_key(&key);
                let present = database.contains(&key)?;
                ensure_equal(step, "contains result", present, expected)?;
            }

            5 => {
                database.checkpoint()?;
                drop(database);
                database = Database::open(path)?;
                verify_model(step, &database, &model)?;
            }

            6 => {
                database.reclaim()?;
            }

            _ => return Err(mismatch(step, "operation selector").into()),
        }
    }

    verify_model(MAX_OPERATIONS, &database, &model)
}

fn verify_model(
    step: usize,
    database: &Database,
    model: &BTreeMap<[u8; 8], [u8; 8]>,
) -> Result<(), Box<dyn StdError>> {
    for (key, expected) in model {
        compare_value(step, database.get(key)?.as_deref(), Some(expected))?;
    }

    Ok(())
}

fn compare_value(
    step: usize,
    actual: Option<&[u8]>,
    expected: Option<&[u8; 8]>,
) -> Result<(), io::Error> {
    let matches = match (actual, expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) => actual == *expected,
        _ => false,
    };

    if matches {
        Ok(())
    } else {
        Err(mismatch(step, "lookup result"))
    }
}

fn ensure_equal(
    step: usize,
    operation: &str,
    actual: bool,
    expected: bool,
) -> Result<(), io::Error> {
    if actual == expected {
        Ok(())
    } else {
        Err(mismatch(step, operation))
    }
}

fn mismatch(step: usize, operation: &str) -> io::Error {
    io::Error::other(format!(
        "operation {step}: {operation} differs from the reference model"
    ))
}

fn fuzz_path() -> PathBuf {
    let case = NEXT_CASE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("floresta-db-fuzz-{}-{case}", std::process::id()))
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
