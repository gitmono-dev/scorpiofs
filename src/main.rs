use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

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

    /// MST/2 snapshot service base URL. Overrides SCORPIO_MST2_BASE_URL and config.
    #[arg(long, global = true)]
    mst2_base_url: Option<String>,

    /// Persistent store root. Workspace and cache roots derive from this path.
    #[arg(long, global = true)]
    store_path: Option<String>,

    /// HTTP bind address for the v3 workspace daemon.
    #[arg(long, default_value = "0.0.0.0:2725", global = true)]
    http_addr: SocketAddr,

    /// Log filter directive (e.g. "info", "scorpio=debug"). Overrides
    /// SCORPIO_LOG, RUST_LOG, and the config `log_level`.
    #[arg(long, global = true)]
    log_level: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the workspace HTTP control daemon. Mounts are created by explicit requests.
    Serve {
        /// Write bounded workspace binding evidence to a new absolute JSONL file.
        #[arg(long, requires = "workspace_observation_run_id")]
        workspace_observation_jsonl: Option<PathBuf>,
        /// Canonical non-nil UUID identifying this observation run.
        #[arg(long, requires = "workspace_observation_jsonl")]
        workspace_observation_run_id: Option<String>,
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
            command: Commands::Workspace { endpoint, action },
            ..
        } => {
            std::process::exit(cli::workspace_request(&endpoint, action.into_command()).await);
        }
        cli => cli,
    };
    let mut overrides = HashMap::new();
    for (key, value) in [
        ("mst2_base_url", &cli.mst2_base_url),
        ("store_path", &cli.store_path),
    ] {
        if let Some(value) = value {
            overrides.insert(key.into(), value.clone());
        }
    }
    if let Some(value) = &cli.log_level {
        // Resolve the CLI value before parsing the persisted file so a stale
        // or malformed configured level cannot shadow the explicit override.
        overrides.insert("log_level".to_owned(), value.clone());
    }

    // These commands need neither a loaded config nor logging; handle them
    // before `cli::init` so they work even when the config is missing/invalid.
    match &cli.command {
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            clap_complete::generate(*shell, &mut cmd, "scorpio", &mut std::io::stdout());
            return;
        }
        Commands::Config {
            action: ConfigAction::Init { path, force },
        } => {
            std::process::exit(cli::config_init(path, *force));
        }
        Commands::Config {
            action: ConfigAction::Validate,
        } => {
            std::process::exit(cli::config_validate(&cli.config_path, overrides.clone()));
        }
        Commands::Config {
            action: ConfigAction::InstallerPaths,
        } => {
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
        Commands::Serve {
            workspace_observation_jsonl,
            workspace_observation_run_id,
        } => {
            let observation = workspace_observation_jsonl
                .zip(workspace_observation_run_id)
                .map(|(path, run_id)| cli::ObservationFileOptions { path, run_id });
            cli::serve_with_observation(cli.http_addr, observation).await
        }
        Commands::Workspace { .. } => unreachable!("workspace handled before config init"),
        Commands::Config {
            action: ConfigAction::Show,
        } => cli::config_show(),
        Commands::Config { .. } => {
            unreachable!("config init/validate/installer-paths handled before config init")
        }
        Commands::Doctor => doctor::run().await,
        Commands::Completions { .. } => unreachable!("handled before config init"),
    };

    std::process::exit(code);
}
