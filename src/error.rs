use std::fmt::{self, Display, Formatter};
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InvalidConfig(&'static str),
    InvalidKeyLength { expected: usize, actual: usize },
    CapacityExhausted(&'static str),
    Corrupt(&'static str),
    Busy(&'static str),
    Closed,
    Unsupported(&'static str),
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::InvalidConfig(message) => write!(formatter, "invalid configuration: {message}"),
            Self::InvalidKeyLength { expected, actual } => {
                write!(
                    formatter,
                    "invalid key length: expected {expected}, got {actual}"
                )
            }
            Self::CapacityExhausted(area) => write!(formatter, "{area} capacity exhausted"),
            Self::Corrupt(message) => write!(formatter, "database is corrupt: {message}"),
            Self::Busy(message) => write!(formatter, "database is busy: {message}"),
            Self::Closed => formatter.write_str("database is closed"),
            Self::Unsupported(message) => write!(formatter, "unsupported operation: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
