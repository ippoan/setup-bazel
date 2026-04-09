mod r2;
mod sync;
mod watch;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "bazel-r2-sync", about = "Fast Bazel disk_cache sync with R2")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Restore disk_cache from R2
    Restore(SyncArgs),
    /// Save disk_cache to R2
    Save(SyncArgs),
    /// Watch disk_cache and upload new files in background
    Watch(WatchArgs),
}

#[derive(Parser, Clone)]
pub struct SyncArgs {
    /// S3-compatible endpoint URL
    #[arg(long, env = "R2_ENDPOINT")]
    endpoint: String,

    /// Bucket name
    #[arg(long, env = "R2_BUCKET")]
    bucket: String,

    /// Key prefix in the bucket
    #[arg(long, default_value = "disk-cache/")]
    prefix: String,

    /// Local disk_cache directory path
    #[arg(long)]
    local_path: String,

    /// Number of concurrent operations
    #[arg(long, default_value = "64")]
    concurrency: usize,
}

#[derive(Parser, Clone)]
pub struct WatchArgs {
    /// S3-compatible endpoint URL
    #[arg(long, env = "R2_ENDPOINT")]
    endpoint: String,

    /// Bucket name
    #[arg(long, env = "R2_BUCKET")]
    bucket: String,

    /// Key prefix in the bucket
    #[arg(long, default_value = "disk-cache/")]
    prefix: String,

    /// Local disk_cache directory path
    #[arg(long)]
    local_path: String,

    /// Number of concurrent operations
    #[arg(long, default_value = "64")]
    concurrency: usize,

    /// Polling interval in seconds
    #[arg(long, default_value = "5")]
    interval: u64,

    /// Path to write PID file
    #[arg(long)]
    pid_file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Restore(args) => sync::restore(&args).await,
        Command::Save(args) => sync::save(&args).await,
        Command::Watch(args) => watch::run(&args).await,
    }
}
