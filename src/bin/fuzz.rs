use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::io;
use std::path::PathBuf;

use db_experiment::{Config, Database, Mode};

fn main() {
    if let Err(error) = run() {
        eprintln!("fuzz failure: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn StdError>> {
    let seed = argument(1, 1)?;
    let steps = argument(2, 100_000)?;
    let path = fuzz_path(seed);
    let _ignored = std::fs::remove_dir_all(&path);

    let mut config = Config::new(Mode::Map, 1_024, 8);
    config.block_size = 1 << 20;
    config.body_capacity = 64 << 20;
    config.blob_capacity = 64 << 20;
    config.max_threads = 4;
    let mut database = Database::create(&path, config)?;
    let mut model = BTreeMap::<[u8; 8], [u8; 8]>::new();
    let mut random = XorShift64::new(seed);

    for step in 0..steps {
        let key = (random.next() % 4_096).to_le_bytes();
        match random.next() % 10_000 {
            0..=4_499 => {
                let value = random.next().to_le_bytes();
                database.put(&key, &value)?;
                model.insert(key, value);
            }
            4_500..=6_499 => {
                let expected = model.remove(&key).is_some();
                let actual = database.delete(&key)?;
                if actual != expected {
                    return Err(
                        failure(step, "delete result differs from the reference model").into(),
                    );
                }
            }
            6_500..=9_989 => {
                let actual = database.get(&key)?;
                compare_value(step, actual.as_deref(), model.get(&key).copied())?;
            }
            _ => {
                database.checkpoint()?;
                drop(database);
                database = Database::open(&path)?;
            }
        }
    }

    for (key, expected) in &model {
        let actual = database.get(key)?;
        compare_value(steps, actual.as_deref(), Some(*expected))?;
    }
    drop(database);
    std::fs::remove_dir_all(path)?;
    println!("seed={seed} steps={steps} keys={} status=ok", model.len());
    Ok(())
}

fn compare_value(
    step: u64,
    actual: Option<&[u8]>,
    expected: Option<[u8; 8]>,
) -> Result<(), io::Error> {
    let matches = match (actual, expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) => actual == expected,
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(failure(step, "lookup differs from the reference model"))
    }
}

fn failure(step: u64, message: &str) -> io::Error {
    io::Error::other(format!("step {step}: {message}"))
}

fn argument(index: usize, default: u64) -> Result<u64, io::Error> {
    let Some(value) = std::env::args().nth(index) else {
        return Ok(default);
    };
    value.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("argument {index} is not an integer: {error}"),
        )
    })
}

fn fuzz_path(seed: u64) -> PathBuf {
    std::env::temp_dir().join(format!("db-experiment-fuzz-{}-{seed}", std::process::id()))
}

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x9e37_79b9_7f4a_7c15
            } else {
                seed
            },
        }
    }

    fn next(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }
}
