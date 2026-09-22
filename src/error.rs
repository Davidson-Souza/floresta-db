// SPDX-License-Identifier: MIT OR Apache-2.0

//! Errors returned by database operations.
//!
//! The module keeps I/O failures distinct from configuration, capacity,
//! corruption, lifecycle, and unsupported-operation failures.

use std::fmt::{self, Display, Formatter};
use std::io;

/// The result type returned by `floresta-db` operations.
///
/// The error parameter defaults to [`Error`] but can be replaced when a caller
/// needs to compose this alias with another precise error type.
///
/// # Examples
///
/// ```
/// use floresta_db::{Config, Mode, Result};
///
/// fn validate(config: &Config) -> Result<()> {
///     config.validate()
/// }
///
/// validate(&Config::new(Mode::Set, 64))?;
/// # Ok::<(), floresta_db::Error>(())
/// ```
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
/// A precise failure category for database configuration, storage, and lifecycle operations.
///
/// # Examples
///
/// ```
/// use floresta_db::Error;
///
/// let error = Error::InvalidKeyLength {
///     expected: 36,
///     actual: 32,
/// };
/// assert_eq!(error.to_string(), "invalid key length: expected 36, got 32");
/// ```
pub enum Error {
    /// An operating-system or filesystem operation failed.
    Io(
        /// The original I/O error.
        io::Error,
    ),

    /// The requested configuration cannot form a valid persistent layout.
    InvalidConfig(
        /// A description of the violated invariant.
        &'static str,
    ),

    /// A key does not match the width fixed at database creation.
    InvalidKeyLength {
        /// The configured key width.
        expected: usize,

        /// The supplied key width.
        actual: usize,
    },

    /// A required in-memory allocation failed.
    OutOfMemory,

    /// A fixed-capacity storage area has no remaining space.
    CapacityExhausted(
        /// The exhausted storage area.
        &'static str,
    ),

    /// Persistent data violates a format or checksum invariant.
    Corrupt(
        /// A description of the invalid state.
        &'static str,
    ),

    /// An exclusive lifecycle operation is already active.
    Busy(
        /// A description of the conflicting operation.
        &'static str,
    ),

    /// The requested operation used a closed lifecycle state.
    Closed,

    /// The selected mode or platform does not support the operation.
    Unsupported(
        /// A description of the unsupported operation.
        &'static str,
    ),
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
            Self::OutOfMemory => formatter.write_str("memory allocation failed"),
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
