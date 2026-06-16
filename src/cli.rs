//! Command-line argument parsing via `clap`.

use std::net::Ipv4Addr;
use std::path::PathBuf;

use clap::Parser;

/// Runtime configuration for the telemetry client.
#[derive(Debug, Parser)]
#[command(
    name = "telemetry-client",
    about = "UDP multicast telemetry JSONL writer"
)]
pub struct Cli {
    /// Directory where `.jsonl` output files are written.
    /// Created automatically if it does not exist.
    #[arg(long, default_value = "./out")]
    pub output_dir: PathBuf,

    /// Multicast group address to join.
    #[arg(long, default_value = "224.0.0.1")]
    pub multicast_addr: Ipv4Addr,

    /// UDP port the simulator broadcasts on.
    #[arg(long, default_value_t = 8904)]
    pub port: u16,
}
