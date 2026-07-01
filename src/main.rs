use std::{net::SocketAddr, path::PathBuf};

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use scorpiofs::{cli, doctor};

/// Scorpio: FUSE-based virtual filesystem with an Antares build overlay.
///
/// With no subcommand, `scorpio` runs the workspace daemon (`serve`), preserving
/// backward compatibility with `scorpio -c <cfg> --http-addr <addr>`.
#[derive(Parser, Debug)]
#[command(name = "scorpio", author, version, about, long_about = None)]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, default_value = "scorpio.toml", global = true)]
    config_path: String,

    /// HTTP bind address for the workspace daemon (Antares API lives under /antares/*).
    #[arg(long, default_value = "0.0.0.0:2725", global = true)]
    http_addr: SocketAddr,

    /// Log filter directive (e.g. "info", "scorpio=debug"). Overrides
    /// SCORPIO_LOG, RUST_LOG, and the config `log_level`.
    #[arg(long, global = true)]
    log_level: Option<String>,

    /// Override the Antares per-job upper-layer root.
    #[arg(long, global = true)]
    upper_root: Option<PathBuf>,
    /// Override the Antares per-job CL-layer root.
    #[arg(long, global = true)]
    cl_root: Option<PathBuf>,
    /// Override the Antares per-job mountpoint root.
    #[arg(long, global = true)]
    mount_root: Option<PathBuf>,
    /// Override the Antares state file path.
    #[arg(long, global = true)]
    state_file: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the workspace daemon (FUSE mount + HTTP API). Default when no subcommand is given.
    Serve,
    /// Mount an Antares job instance.
    Mount {
        /// Unique job identifier.
        job_id: String,
        /// Optional CL layer name.
        #[arg(long)]
        cl: Option<String>,
    },
    /// Unmount an Antares job instance.
    Umount {
        /// Job identifier to remove.
        job_id: String,
    },
    /// List tracked Antares instances.
    List,
    /// Mount via a running HTTP daemon (recommended for build systems).
    HttpMount {
        /// Unique job identifier (recommended).
        #[arg(long)]
        job_id: Option<String>,
        /// Monorepo path to mount (e.g. "/third-party/mega").
        path: String,
        /// Optional CL identifier.
        #[arg(long)]
        cl: Option<String>,
        /// Daemon base URL (the request goes to `{endpoint}/mounts`).
        #[arg(long, default_value = "http://127.0.0.1:2725/antares")]
        endpoint: String,
    },
    /// Inspect or validate configuration.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Run environment diagnostics (FUSE, /etc/fuse.conf, directories, mega server).
    Doctor,
    /// Generate a shell completion script (bash, zsh, fish, ...).
    Completions {
        /// Target shell.
        shell: Shell,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigAction {
    /// Write a configuration template to a file.
    Init {
        /// Output path.
        #[arg(default_value = "scorpio.toml")]
        path: String,
        /// Overwrite an existing file.
        #[arg(long)]
        force: bool,
    },
    /// Offline-validate a configuration file, reporting all problems.
    Validate,
    /// Print the effective (merged) configuration.
    Show,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let overrides = cli::antares_overrides(
        cli.upper_root.clone(),
        cli.cl_root.clone(),
        cli.mount_root.clone(),
        cli.state_file.clone(),
    );

    // These commands need neither a loaded config nor logging; handle them
    // before `cli::init` so they work even when the config is missing/invalid.
    match &cli.command {
        Some(Commands::Completions { shell }) => {
            let mut cmd = Cli::command();
            clap_complete::generate(*shell, &mut cmd, "scorpio", &mut std::io::stdout());
            return;
        }
        Some(Commands::Config {
            action: ConfigAction::Init { path, force },
        }) => {
            std::process::exit(cli::config_init(path, *force));
        }
        Some(Commands::Config {
            action: ConfigAction::Validate,
        }) => {
            std::process::exit(cli::config_validate(&cli.config_path, overrides.clone()));
        }
        _ => {}
    }

    if let Err(code) = cli::init(&cli.config_path, cli.log_level.as_deref(), overrides) {
        std::process::exit(code);
    }

    let code = match cli.command {
        None => {
            // Unconditional (not log-level gated) deprecation note for the
            // legacy flag-only invocation form.
            eprintln!(
                "note: running `scorpio` without a subcommand is deprecated; use `scorpio serve`"
            );
            cli::serve(cli.http_addr).await
        }
        Some(Commands::Serve) => cli::serve(cli.http_addr).await,
        Some(Commands::Mount { job_id, cl }) => cli::antares_mount(&job_id, cl.as_deref()).await,
        Some(Commands::Umount { job_id }) => cli::antares_umount(&job_id).await,
        Some(Commands::List) => cli::antares_list().await,
        Some(Commands::HttpMount {
            job_id,
            path,
            cl,
            endpoint,
        }) => cli::http_mount(job_id.as_deref(), &path, cl.as_deref(), &endpoint),
        Some(Commands::Config {
            action: ConfigAction::Show,
        }) => cli::config_show(),
        Some(Commands::Config { .. }) => {
            unreachable!("config init/validate handled before config init")
        }
        Some(Commands::Doctor) => doctor::run().await,
        Some(Commands::Completions { .. }) => unreachable!("handled before config init"),
    };

    std::process::exit(code);
}
