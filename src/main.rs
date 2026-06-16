//! Telemetry client — UDP multicast receiver that writes classified JSONL output.
//!
//! # Pipeline
//!
//! ```text
//! UDP socket → receiver task → classifier task → writer tasks (×4)
//!                  mpsc            mpsc (per type)
//! ```
//!
//! Each stage runs in its own Tokio task connected by channels. `main` handles
//! startup, periodic stats, graceful shutdown on Ctrl-C, and joining all tasks.

mod cli;
mod error;
mod model;
mod net;
mod parser;
mod writer;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use clap::Parser as _;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};
use tracing::{error, info, warn, Level};
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;
use crate::error::AppError;
use crate::model::{RecordType, TelemetryRecord};
use crate::writer::{spawn_writer, WriterHandle};

/// Channel buffer size. At ~100 msg/s this gives ~10s of headroom.
const CHANNEL_CAPACITY: usize = 1024;

/// Max UDP datagram size — matches the simulator's limit.
const MAX_DATAGRAM: usize = 512;

/// Shared counters across receiver and classifier tasks.
#[derive(Debug, Default)]
struct PipelineStats {
    received: AtomicU64,
    parsed: AtomicU64,
    skipped: AtomicU64,
    serialized: AtomicU64,
    serialize_errors: AtomicU64,
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    init_tracing();

    let cli = Cli::parse();
    tokio::fs::create_dir_all(&cli.output_dir).await?;

    info!(
        multicast = %cli.multicast_addr,
        port = cli.port,
        output_dir = %cli.output_dir.display(),
        "starting telemetry client"
    );

    // --- Network setup ---
    let socket = net::join_multicast(cli.multicast_addr, cli.port).await?;

    // Shutdown flag broadcast to all tasks via watch channel.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // --- Channel wiring ---
    // Raw datagram strings flow from receiver → classifier.
    let (raw_tx, raw_rx) = mpsc::channel::<String>(CHANNEL_CAPACITY);

    // One channel per record type: classifier → writer.
    let mut writer_senders: HashMap<RecordType, mpsc::Sender<String>> = HashMap::new();
    let mut writer_handles: Vec<WriterHandle> = Vec::new();

    for record_type in RecordType::ALL {
        let (tx, rx) = mpsc::channel::<String>(CHANNEL_CAPACITY);
        writer_senders.insert(record_type, tx);
        writer_handles.push(spawn_writer(
            record_type,
            cli.output_dir.clone(),
            rx,
            shutdown_rx.clone(),
        ));
    }

    // --- Spawn pipeline tasks ---
    let stats = Arc::new(PipelineStats::default());
    let receiver_handle = spawn_receiver(socket, raw_tx, shutdown_rx.clone(), Arc::clone(&stats));
    let classifier_handle = spawn_classifier(
        raw_rx,
        writer_senders,
        shutdown_rx.clone(),
        Arc::clone(&stats),
    );

    // --- Main loop: wait for Ctrl-C or log periodic stats ---
    let mut stats_interval = time::interval(Duration::from_secs(5));
    stats_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal received");
                break;
            }
            _ = stats_interval.tick() => {
                log_pipeline_stats(&stats, &writer_handles);
            }
        }
    }

    // --- Graceful shutdown ---
    // Signal all tasks to stop, then wait for them to drain and exit.
    let _ = shutdown_tx.send(true);
    log_pipeline_stats(&stats, &writer_handles);

    if let Err(err) = receiver_handle.await {
        error!(error = %err, "receiver join error");
    }
    if let Err(err) = classifier_handle.await {
        error!(error = %err, "classifier join error");
    }

    for handle in writer_handles {
        if let Err(err) = handle.join.await {
            error!(error = %err, "writer join error");
        }
    }

    info!("telemetry client stopped");
    Ok(())
}

/// Configure `tracing` subscriber. Respects `RUST_LOG` env var, defaults to INFO.
fn init_tracing() {
    let filter = EnvFilter::builder()
        .with_default_directive(Level::INFO.into())
        .from_env_lossy();

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

/// Receiver task: read UDP datagrams and forward raw lines to the classifier.
///
/// Uses `tokio::select!` to also respond to the shutdown signal without
/// blocking indefinitely on `recv_from`.
fn spawn_receiver(
    socket: tokio::net::UdpSocket,
    raw_tx: mpsc::Sender<String>,
    mut shutdown: watch::Receiver<bool>,
    stats: Arc<PipelineStats>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; MAX_DATAGRAM];

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                recv_result = socket.recv_from(&mut buf) => {
                    match recv_result {
                        Ok((len, _addr)) => {
                            stats.received.fetch_add(1, Ordering::Relaxed);
                            // Lossy UTF-8 is fine — bad bytes will fail parsing downstream.
                            let line = String::from_utf8_lossy(&buf[..len]).into_owned();
                            // Channel closed means classifier exited — stop receiving.
                            if raw_tx.send(line).await.is_err() {
                                break;
                            }
                        }
                        Err(err) => {
                            error!(error = %err, "udp recv failed");
                        }
                    }
                }
            }
        }

        info!(
            received = stats.received.load(Ordering::Relaxed),
            "receiver task stopped"
        );
    })
}

/// Classifier task: parse, validate, serialize, and route records to writers.
///
/// Bad records are logged and skipped — the task never panics or exits on
/// a single malformed datagram. On shutdown it drops its writer senders so
/// the writer tasks see channel close and drain their queues.
fn spawn_classifier(
    mut raw_rx: mpsc::Receiver<String>,
    writer_senders: HashMap<RecordType, mpsc::Sender<String>>,
    mut shutdown: watch::Receiver<bool>,
    stats: Arc<PipelineStats>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                line = raw_rx.recv() => {
                    match line {
                        Some(line) => {
                            match parser::process_line(&line) {
                                Ok((record, record_type)) => {
                                    stats.parsed.fetch_add(1, Ordering::Relaxed);
                                    match serialize_record(record) {
                                        Ok(json_line) => {
                                            stats.serialized.fetch_add(1, Ordering::Relaxed);
                                            if let Some(tx) = writer_senders.get(&record_type) {
                                                if tx.send(json_line).await.is_err() {
                                                    warn!(
                                                        record_type = record_type.label(),
                                                        "writer channel closed"
                                                    );
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            stats.serialize_errors.fetch_add(1, Ordering::Relaxed);
                                            error!(error = %err, raw = %line.trim(), "serialization failed");
                                        }
                                    }
                                }
                                // Key set didn't match any known record type.
                                Err(AppError::UnknownRecord(keys)) => {
                                    stats.skipped.fetch_add(1, Ordering::Relaxed);
                                    warn!(keys = %keys, raw = %line.trim(), "unknown record type");
                                }
                                // Bad format or invalid field value.
                                Err(AppError::Parse(reason)) => {
                                    stats.skipped.fetch_add(1, Ordering::Relaxed);
                                    warn!(reason = %reason, raw = %line.trim(), "parse failed");
                                }
                                Err(err) => {
                                    stats.skipped.fetch_add(1, Ordering::Relaxed);
                                    warn!(error = %err, raw = %line.trim(), "record rejected");
                                }
                            }
                        }
                        // Receiver dropped its sender — nothing more to process.
                        None => break,
                    }
                }
            }
        }

        // Drop senders so writer tasks receive None and exit after draining.
        drop(writer_senders);
        info!(
            parsed = stats.parsed.load(Ordering::Relaxed),
            skipped = stats.skipped.load(Ordering::Relaxed),
            serialized = stats.serialized.load(Ordering::Relaxed),
            serialize_errors = stats.serialize_errors.load(Ordering::Relaxed),
            "classifier task stopped"
        );
    })
}

/// Serialize a validated record to a compact JSON string (no trailing newline).
fn serialize_record(record: TelemetryRecord) -> Result<String, AppError> {
    serde_json::to_string(&record).map_err(AppError::from)
}

/// Log pipeline-wide and per-writer counters.
fn log_pipeline_stats(stats: &PipelineStats, writers: &[WriterHandle]) {
    info!(
        received = stats.received.load(Ordering::Relaxed),
        parsed = stats.parsed.load(Ordering::Relaxed),
        skipped = stats.skipped.load(Ordering::Relaxed),
        serialized = stats.serialized.load(Ordering::Relaxed),
        serialize_errors = stats.serialize_errors.load(Ordering::Relaxed),
        "pipeline stats"
    );

    for writer in writers {
        info!(
            written = writer.stats.written.load(Ordering::Relaxed),
            io_errors = writer.stats.io_errors.load(Ordering::Relaxed),
            "writer stats"
        );
    }
}
