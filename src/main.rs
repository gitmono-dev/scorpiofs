use std::{net::SocketAddr, path::PathBuf};

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use scorpiofs::{cli, doctor};

/// Scorpio: fixed snapshot workspaces with private writable layers.
#[derive(Parser, Debug)]
#[command(name = "scorpio", author, version, about, long_about = None)]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, default_value = "scorpio.toml", global = true)]
    config_path: String,

    /// HTTP bind address for the v3 workspace daemon.
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
    /// Run the workspace HTTP control daemon. Mounts are created by explicit requests.
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
    /// Control workspaces through the daemon that owns their mounts.
    Workspace {
        /// Daemon base URL.
        #[arg(long, default_value = "http://127.0.0.1:2725", global = true)]
        endpoint: String,
        #[command(subcommand)]
        action: WorkspaceAction,
    },
    /// Inspect or validate configuration.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Run environment diagnostics (FUSE provider, directories, mega server).
    Doctor,
    /// Generate a shell completion script (bash, zsh, fish, ...).
    Completions {
        /// Target shell.
        shell: Shell,
    },
}

#[derive(Subcommand, Debug)]
enum WorkspaceAction {
    /// Create a new workspace; the previous workspace stays fixed.
    Create {
        /// Canonical monorepo scope, such as /project.
        scope: String,
        /// Fixed namespace view ID. Omit to resolve latest once.
        #[arg(long)]
        view_id: Option<String>,
        /// Start full hydration in the background.
        #[arg(long)]
        full: bool,
    },
    /// List workspaces owned by the daemon.
    List,
    /// Observe mount, hydration, dirty, lease and local-pin state.
    Status { id: String },
    /// Hydrate the existing fixed snapshot.
    Hydrate { id: String },
    /// Cancel and join the workspace's hydration task.
    CancelHydrate { id: String },
    /// Release this workspace's local pin.
    ReleaseLocalPin { id: String },
    /// Destroy a workspace. Dirty contents are preserved by default.
    Destroy {
        id: String,
        /// Explicitly discard dirty upper contents.
        #[arg(long)]
        discard_dirty: bool,
    },
}

impl WorkspaceAction {
    fn into_command(self) -> cli::WorkspaceCommand {
        match self {
            Self::Create {
                scope,
                view_id,
                full,
            } => cli::WorkspaceCommand::Create {
                scope,
                view_id,
                full,
            },
            Self::List => cli::WorkspaceCommand::List,
            Self::Status { id } => cli::WorkspaceCommand::Status { id },
            Self::Hydrate { id } => cli::WorkspaceCommand::Hydrate { id },
            Self::CancelHydrate { id } => cli::WorkspaceCommand::CancelHydrate { id },
            Self::ReleaseLocalPin { id } => cli::WorkspaceCommand::ReleaseLocalPin { id },
            Self::Destroy { id, discard_dirty } => {
                cli::WorkspaceCommand::Destroy { id, discard_dirty }
            }
        }
    }
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;

    #[test]
    fn legacy_commands_and_path_flags_remain_accepted_alongside_workspace_commands() {
        for args in [
            vec!["scorpio", "mount", "job", "--cl", "change"],
            vec!["scorpio", "umount", "job"],
            vec!["scorpio", "list"],
            vec!["scorpio", "http-mount", "/project", "--job-id", "job"],
            vec![
                "scorpio",
                "--upper-root",
                "/tmp/upper",
                "--cl-root",
                "/tmp/cl",
                "--mount-root",
                "/tmp/mounts",
                "--state-file",
                "/tmp/state",
                "list",
            ],
            vec!["scorpio", "workspace", "list"],
            vec!["scorpio", "serve"],
        ] {
            Cli::try_parse_from(args).unwrap();
        }
    }
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
    /// Emit runtime paths for the release installer.
    #[command(hide = true)]
    InstallerPaths,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Workspace clients require only the daemon URL. They must not initialize
    // local storage or construct a second lifecycle owner.
    let cli = match cli {
        Cli {
            command: Some(Commands::Workspace { endpoint, action }),
            ..
        } => {
            std::process::exit(cli::workspace_request(&endpoint, action.into_command()).await);
        }
        cli => cli,
    };
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
        Some(Commands::Config {
            action: ConfigAction::InstallerPaths,
        }) => {
            std::process::exit(cli::config_installer_paths(
                &cli.config_path,
                overrides.clone(),
            ));
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
        }) => cli::http_mount(job_id.as_deref(), &path, cl.as_deref(), &endpoint).await,
        Some(Commands::Workspace { .. }) => unreachable!("workspace handled before config init"),
        Some(Commands::Config {
            action: ConfigAction::Show,
        }) => cli::config_show(),
        Some(Commands::Config { .. }) => {
            unreachable!("config init/validate/installer-paths handled before config init")
        }
        Some(Commands::Doctor) => doctor::run().await,
        Some(Commands::Completions { .. }) => unreachable!("handled before config init"),
    };

    std::process::exit(code);
}
