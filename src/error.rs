//! Application-wide error types.
//!
//! Errors are grouped by layer so callers can decide whether to log-and-skip
//! (parse/classify) or treat as fatal (I/O during startup).

use std::io;

use thiserror::Error;

/// Top-level error enum used across the pipeline.
#[derive(Debug, Error)]
pub enum AppError {
    /// The datagram could not be split into valid key=value pairs, or a field
    /// value failed validation (bad number, invalid enum, etc.).
    #[error("parse error: {0}")]
    Parse(String),

    /// The key set did not match any known record type — usually a typo in a
    /// field name or a partial record from a concatenation glitch.
    #[error("unknown record type (keys: {0})")]
    UnknownRecord(String),

    /// `serde_json` failed to serialize a record that already passed validation.
    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),

    /// File open, write, or flush failure in a writer task.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, AppError>;
