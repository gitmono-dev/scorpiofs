use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};
use scorpiofs::cli;

/// Antares build overlay manager.
///
/// Deprecated compatibility alias: prefer the unified `scorpio` binary
/// (`scorpio mount|umount|list|http-mount`, and `scorpio serve` for the
/// workspace daemon). This alias is retained for at least one minor release.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the configuration file (scorpio config).
    #[arg(long, default_value = "scorpio.toml")]
    config_path: String,
    /// Root path to place per-job upper layers (overrides config when set).
    #[arg(long)]
    upper_root: Option<PathBuf>,
    /// Root path to place per-job CL layers (overrides config when set).
    #[arg(long)]
    cl_root: Option<PathBuf>,
    /// Root path for per-job mountpoints (overrides config when set).
    #[arg(long)]
    mount_root: Option<PathBuf>,
    /// Path to persist mount state as TOML (overrides config when set).
    #[arg(long)]
    state_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Mount a new Antares job instance.
    Mount {
        /// Unique job identifier.
        job_id: String,
        /// Optional CL layer name; when set, creates a CL passthrough layer placeholder.
        #[arg(long)]
        cl: Option<String>,
    },
    /// Unmount a job instance.
    Umount {
        /// Job identifier to remove.
        job_id: String,
    },
    /// List tracked instances.
    List,
    /// Start HTTP daemon server.
    Serve {
        /// Address to bind to (e.g., "0.0.0.0:2726")
        #[arg(long, default_value = "0.0.0.0:2726")]
        bind: String,
    },
    /// Mount via HTTP daemon (recommended for build systems to ensure unified behavior).
    HttpMount {
        /// Unique job identifier (recommended). If omitted, the daemon will create a UUID-based mount.
        #[arg(long)]
        job_id: Option<String>,
        /// Monorepo path to mount (e.g., "/third-party/mega")
        path: String,
        /// Optional CL identifier
        #[arg(long)]
        cl: Option<String>,
        /// Daemon base URL (e.g., "http://127.0.0.1:2726")
        #[arg(long, default_value = "http://127.0.0.1:2726")]
        endpoint: String,
    },
}

#[tokio::main]
async fn main() {
    eprintln!(
        "note: the `antares` binary is a deprecated compatibility alias; prefer `scorpio <subcommand>`"
    );

    let cli = Cli::parse();

    let overrides = cli::antares_overrides(
        cli.upper_root.clone(),
        cli.cl_root.clone(),
        cli.mount_root.clone(),
        cli.state_file.clone(),
    );
    if let Err(code) = cli::init(&cli.config_path, None, overrides) {
        std::process::exit(code);
    }

    let code = match cli.command {
        Commands::Mount { job_id, cl } => cli::antares_mount(&job_id, cl.as_deref()).await,
        Commands::Umount { job_id } => cli::antares_umount(&job_id).await,
        Commands::List => cli::antares_list().await,
        Commands::Serve { bind } => match bind.parse::<SocketAddr>() {
            Ok(addr) => cli::antares_serve(addr).await,
            Err(e) => {
                eprintln!("Invalid bind address '{bind}': {e}");
                cli::exit::CONFIG
            }
        },
        Commands::HttpMount {
            job_id,
            path,
            cl,
            endpoint,
        } => cli::http_mount(job_id.as_deref(), &path, cl.as_deref(), &endpoint),
    };

    std::process::exit(code);
}
