//! Per-record-type file writer tasks.
//!
//! Each writer owns one `.jsonl` file and runs in its own Tokio task. Lines
//! arrive pre-serialized as JSON strings from the classifier via an `mpsc`
//! channel.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};
use tracing::{error, info};

use crate::error::AppError;
use crate::model::RecordType;

/// Counters for lines written and I/O failures — shared with `main` for stats.
#[derive(Debug, Default)]
pub struct WriterStats {
    pub written: AtomicU64,
    pub io_errors: AtomicU64,
}

/// Handle returned to `main` so it can await the writer task and read stats.
pub struct WriterHandle {
    pub join: JoinHandle<()>,
    pub stats: Arc<WriterStats>,
}

/// Spawn a dedicated writer task for one record type.
///
/// The task multiplexes three events via `tokio::select!`:
///   - incoming JSON lines from the classifier channel
///   - periodic flush (every 1s) to push kernel buffers to disk
///   - shutdown signal from `main`
pub fn spawn_writer(
    record_type: RecordType,
    output_dir: PathBuf,
    mut rx: mpsc::Receiver<String>,
    mut shutdown: watch::Receiver<bool>,
) -> WriterHandle {
    let stats = Arc::new(WriterStats::default());
    let stats_clone = Arc::clone(&stats);

    let join = tokio::spawn(async move {
        let path = output_dir.join(record_type.filename());
        let mut file = match open_append_file(&path).await {
            Ok(file) => file,
            Err(err) => {
                error!(
                    record_type = record_type.label(),
                    path = %path.display(),
                    error = %err,
                    "failed to open output file"
                );
                return;
            }
        };

        info!(
            record_type = record_type.label(),
            path = %path.display(),
            "writer task started"
        );

        // Periodic flush so data isn't stuck in kernel buffers indefinitely.
        let mut flush_interval = time::interval(Duration::from_secs(1));
        flush_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Shutdown requested by main (Ctrl-C path).
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                // Push buffered writes to disk on a timer.
                _ = flush_interval.tick() => {
                    if let Err(err) = file.flush().await {
                        stats_clone.io_errors.fetch_add(1, Ordering::Relaxed);
                        error!(
                            record_type = record_type.label(),
                            error = %err,
                            "flush failed"
                        );
                    }
                }
                // Next JSON line from the classifier.
                line = rx.recv() => {
                    match line {
                        Some(json_line) => {
                            // Write the full line in one syscall. With O_APPEND,
                            // writes under PIPE_BUF are atomic on POSIX systems.
                            let payload = format!("{json_line}\n");
                            if let Err(err) = file.write_all(payload.as_bytes()).await {
                                stats_clone.io_errors.fetch_add(1, Ordering::Relaxed);
                                error!(
                                    record_type = record_type.label(),
                                    error = %err,
                                    "write failed"
                                );
                            } else {
                                stats_clone.written.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        // Classifier dropped its sender — drain and exit.
                        None => break,
                    }
                }
            }
        }

        // Final flush on shutdown so no data is left in buffers.
        if let Err(err) = file.flush().await {
            stats_clone.io_errors.fetch_add(1, Ordering::Relaxed);
            error!(
                record_type = record_type.label(),
                error = %err,
                "final flush failed"
            );
        }

        info!(
            record_type = record_type.label(),
            written = stats_clone.written.load(Ordering::Relaxed),
            io_errors = stats_clone.io_errors.load(Ordering::Relaxed),
            "writer task stopped"
        );
    });

    WriterHandle { join, stats }
}

/// Open (or create) a file in append-only mode.
///
/// `append(true)` sets O_APPEND so every write goes to the end of the file
/// without needing to seek, and individual small writes are atomic.
async fn open_append_file(path: &std::path::Path) -> Result<File, AppError> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(AppError::Io)
}
