//! Shared command implementations for the `scorpio` and `antares` binaries.
//!
//! The two binaries are thin clap front-ends; the actual work lives here so the
//! unified `scorpio` CLI and the (deprecated) `antares` alias behave identically
//! and reuse the service/manager layer directly instead of self-calling over
//! HTTP.

use std::{
    collections::HashMap,
    ffi::OsStr,
    io::{self, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use asyncfuse::raw::logfs::LoggingFileSystem;
use tokio::sync::oneshot;

use crate::{
    antares::{AntaresManager, AntaresPaths},
    daemon::{antares::AntaresServiceImpl, daemon_main_with_profile},
    fuse::{
        logfuse::LogFuse, profile::start_profile_writer, profile::FuseProfileOptions, MegaFuse,
    },
    manager::{fetch::CheckHash, ScorpioManager},
    server::mount_filesystem,
    util::{config, logging},
};

/// Stable process exit codes shared by the CLIs (scripts depend on these).
pub mod exit {
    pub const SUCCESS: i32 = 0;
    pub const INTERNAL: i32 = 1;
    pub const CONFIG: i32 = 2;
    pub const MOUNT: i32 = 3;
    pub const BIND: i32 = 4;
}

/// Build the config CLI-override map from the optional Antares path flags.
///
/// Returning these as config overrides (rather than mutating `AntaresPaths`
/// after the fact) keeps the documented precedence `CLI > env > file > default`
/// intact and ensures runtime directories are created for the effective paths.
pub fn antares_overrides(
    upper_root: Option<PathBuf>,
    cl_root: Option<PathBuf>,
    mount_root: Option<PathBuf>,
    state_file: Option<PathBuf>,
) -> HashMap<String, String> {
    let mut overrides = HashMap::new();
    if let Some(p) = upper_root {
        overrides.insert("antares_upper_root".to_string(), p.display().to_string());
    }
    if let Some(p) = cl_root {
        overrides.insert("antares_cl_root".to_string(), p.display().to_string());
    }
    if let Some(p) = mount_root {
        overrides.insert("antares_mount_root".to_string(), p.display().to_string());
    }
    if let Some(p) = state_file {
        overrides.insert("antares_state_file".to_string(), p.display().to_string());
    }
    overrides
}

/// Build config overrides for the FUSE profile CLI flags.
pub fn fuse_profile_overrides(
    enabled: bool,
    path: Option<PathBuf>,
    agent: Option<String>,
    task: Option<String>,
    capacity: Option<usize>,
    flush_interval_ms: Option<u64>,
) -> HashMap<String, String> {
    let mut overrides = HashMap::new();
    if enabled {
        overrides.insert("fuse_profile_enabled".to_string(), "true".to_string());
    }
    if let Some(path) = path {
        overrides.insert("fuse_profile_path".to_string(), path.display().to_string());
    }
    if let Some(agent) = agent {
        overrides.insert("fuse_profile_agent".to_string(), agent);
    }
    if let Some(task) = task {
        overrides.insert("fuse_profile_task".to_string(), task);
    }
    if let Some(capacity) = capacity {
        overrides.insert("fuse_profile_capacity".to_string(), capacity.to_string());
    }
    if let Some(interval) = flush_interval_ms {
        overrides.insert(
            "fuse_profile_flush_interval_ms".to_string(),
            interval.to_string(),
        );
    }
    overrides
}

/// Load configuration (with CLI overrides) and initialize logging.
///
/// Must be called exactly once, before dispatching a command. Returns the
/// `CONFIG` exit code on failure.
pub fn init(
    config_path: &str,
    log_level: Option<&str>,
    overrides: HashMap<String, String>,
) -> Result<(), i32> {
    if let Err(e) = config::init_config_with(config_path, overrides) {
        // Logging is not up yet; this single bootstrap error goes to stderr.
        eprintln!("Failed to load config: {e}");
        return Err(exit::CONFIG);
    }
    logging::init(log_level, config::log_level());
    Ok(())
}

/// Run the workspace daemon: mount the FUSE workspace and serve the HTTP API
/// (including the nested Antares routes) until a shutdown signal. Assumes
/// [`init`] has already loaded configuration.
pub async fn serve(http_addr: SocketAddr) -> i32 {
    let mut manager = match ScorpioManager::from_toml(config::config_file()) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("failed to load state file '{}': {e}", config::config_file());
            return exit::CONFIG;
        }
    };
    manager.check().await;

    let fuse_interface = MegaFuse::new_from_manager(&manager).await;
    let mountpoint = OsStr::new(config::workspace());
    let mut profile_writer = None;
    let mut profile_context = None;
    let mount_result = if config::fuse_profile_enabled() {
        let options = FuseProfileOptions {
            path: config::fuse_profile_path().to_string(),
            mount_id: uuid::Uuid::new_v4().to_string(),
            agent: config::fuse_profile_agent().to_string(),
            task: config::fuse_profile_task().to_string(),
            capacity: config::fuse_profile_capacity(),
            flush_interval: Duration::from_millis(config::fuse_profile_flush_interval_ms()),
        };
        match start_profile_writer(options) {
            Ok((context, writer)) => {
                profile_context = Some(context.clone());
                profile_writer = Some(writer);
                mount_filesystem(LogFuse::new(fuse_interface.clone(), context), mountpoint).await
            }
            Err(e) => {
                tracing::error!("failed to start FUSE profile writer: {e}");
                return exit::CONFIG;
            }
        }
    } else {
        mount_filesystem(LoggingFileSystem::new(fuse_interface.clone()), mountpoint).await
    };
    let mut mount_handle = match mount_result {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("failed to mount workspace at {:?}: {e}", mountpoint);
            if let Some(writer) = profile_writer {
                let _ = writer.shutdown().await;
            }
            return exit::MOUNT;
        }
    };

    // Bind the HTTP listener up-front so a bind failure is a clean exit (code 4)
    // rather than a panic inside the daemon task.
    let listener = match tokio::net::TcpListener::bind(http_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("failed to bind HTTP address {http_addr}: {e}");
            let _ = mount_handle.unmount().await;
            if let Some(writer) = profile_writer {
                let _ = writer.shutdown().await;
            }
            return exit::BIND;
        }
    };
    tracing::info!("server running on {http_addr}");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let mut daemon_task = tokio::spawn(daemon_main_with_profile(
        Arc::new(fuse_interface),
        manager,
        shutdown_rx,
        listener,
        profile_context,
    ));

    let mut exit_code = exit::SUCCESS;
    let mut mount_finished = false;
    let mut daemon_finished = false;

    // Wait for whichever happens first: the FUSE session ends, the HTTP daemon
    // exits (e.g. a runtime server error), or a shutdown signal arrives.
    tokio::select! {
        res = &mut mount_handle => {
            mount_finished = true;
            if let Err(e) = res {
                tracing::error!("FUSE session ended with error: {e:?}");
                exit_code = exit::INTERNAL;
            }
        }
        res = &mut daemon_task => {
            daemon_finished = true;
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!("HTTP daemon server error: {e}");
                    exit_code = exit::INTERNAL;
                }
                Err(e) => {
                    tracing::error!("HTTP daemon task join failed: {e}");
                    exit_code = exit::INTERNAL;
                }
            }
        }
        _ = shutdown_signal() => {}
    }

    // Stop the HTTP server first (this triggers Antares shutdown cleanup), then
    // unmount the main workspace filesystem.
    let _ = shutdown_tx.send(());
    if !daemon_finished {
        match tokio::time::timeout(Duration::from_secs(20), &mut daemon_task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => {
                tracing::error!("HTTP daemon server error: {e}");
                exit_code = exit::INTERNAL;
            }
            Ok(Err(e)) => {
                tracing::error!("HTTP daemon task join failed: {e}");
                exit_code = exit::INTERNAL;
            }
            Err(_) => {
                tracing::warn!("HTTP daemon shutdown timed out; aborting task");
                daemon_task.abort();
                exit_code = exit::INTERNAL;
            }
        }
    }

    if !mount_finished {
        tracing::info!("unmounting workspace filesystem");
        let _ = mount_handle.unmount().await;
    }
    if let Some(writer) = profile_writer {
        if let Err(e) = writer.shutdown().await {
            tracing::error!("failed to flush FUSE profile: {e}");
            exit_code = exit::INTERNAL;
        }
    }
    exit_code
}

/// Mount an Antares job instance directly via the manager.
pub async fn antares_mount(job_id: &str, cl: Option<&str>) -> i32 {
    let manager = AntaresManager::new(AntaresPaths::from_global_config()).await;
    match manager.mount_job(job_id, cl).await {
        Ok(instance) => {
            println!("mounted job {job_id} at {}", instance.mountpoint.display());
            exit::SUCCESS
        }
        Err(err) => {
            eprintln!("failed to mount job {job_id}: {err}");
            exit::MOUNT
        }
    }
}

/// Unmount an Antares job instance.
pub async fn antares_umount(job_id: &str) -> i32 {
    let manager = AntaresManager::new(AntaresPaths::from_global_config()).await;
    match manager.umount_job(job_id).await {
        Ok(Some(_)) => {
            println!("unmounted job {job_id}");
            exit::SUCCESS
        }
        Ok(None) => {
            eprintln!("job {job_id} not found");
            exit::MOUNT
        }
        Err(err) => {
            eprintln!("failed to unmount job {job_id}: {err}");
            exit::MOUNT
        }
    }
}

/// List tracked Antares job instances.
pub async fn antares_list() -> i32 {
    let manager = AntaresManager::new(AntaresPaths::from_global_config()).await;
    let items = manager.list().await;
    if items.is_empty() {
        println!("no active jobs");
    } else {
        for it in items {
            let cl = it
                .cl_dir
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "(none)".to_string());
            println!(
                "job_id={} mount={} upper={} cl={}",
                it.job_id,
                it.mountpoint.display(),
                it.upper_dir.display(),
                cl
            );
        }
    }
    exit::SUCCESS
}

/// Run the standalone Antares HTTP daemon (the `antares serve` form).
pub async fn antares_serve(addr: SocketAddr) -> i32 {
    use crate::daemon::antares::AntaresDaemon;

    // Bind up-front so a bind failure maps to the dedicated exit code (4).
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("failed to bind Antares HTTP address {addr}: {e}");
            return exit::BIND;
        }
    };
    tracing::info!("starting Antares daemon on {addr}");

    let service = Arc::new(AntaresServiceImpl::new(None).await);
    let daemon = AntaresDaemon::new(service);
    if let Err(e) = daemon.serve_with_listener(listener).await {
        tracing::error!("daemon error: {e}");
        return exit::INTERNAL;
    }
    exit::SUCCESS
}

/// Mount via a running HTTP daemon (recommended for build systems). `endpoint`
/// is the base URL; the request is sent to `{endpoint}/mounts`.
pub async fn http_mount(job_id: Option<&str>, path: &str, cl: Option<&str>, endpoint: &str) -> i32 {
    // Use the async reqwest client: this runs inside the binaries' tokio
    // runtime, where reqwest::blocking would panic.
    let client = reqwest::Client::new();
    let url = format!("{}/mounts", endpoint.trim_end_matches('/'));
    let payload = serde_json::json!({
        "job_id": job_id,
        "path": path,
        "cl": cl,
    });

    match client
        .post(url)
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => match r.json::<serde_json::Value>().await {
            Ok(v) => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&v).unwrap_or_else(|_| v.to_string())
                );
                exit::SUCCESS
            }
            Err(e) => {
                eprintln!("failed to parse response json: {e}");
                exit::INTERNAL
            }
        },
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            eprintln!("http mount failed: status={status} body={body}");
            exit::MOUNT
        }
        Err(e) => {
            eprintln!("http mount request failed: {e}");
            exit::INTERNAL
        }
    }
}

/// `scorpio config init`: write a config template to `path`.
///
/// Does not require an existing/valid config. Refuses to overwrite unless
/// `force` is set.
pub fn config_init(path: &str, force: bool) -> i32 {
    if std::path::Path::new(path).exists() && !force {
        eprintln!("refusing to overwrite existing '{path}' (use --force)");
        return exit::CONFIG;
    }
    match std::fs::write(path, CONFIG_TEMPLATE) {
        Ok(()) => {
            println!("wrote config template to {path}");
            println!("edit base_url/lfs_url, then: scorpio --config-path {path} doctor");
            exit::SUCCESS
        }
        Err(e) => {
            eprintln!("failed to write '{path}': {e}");
            exit::CONFIG
        }
    }
}

/// `scorpio config validate`: offline-validate a config file, reporting all
/// problems. Does not load the process-wide config.
pub fn config_validate(config_path: &str, overrides: HashMap<String, String>) -> i32 {
    match config::validate_file(config_path, overrides) {
        Ok(()) => {
            println!("{config_path}: OK");
            exit::SUCCESS
        }
        Err(problems) => {
            eprintln!("{config_path}: {} problem(s) found:", problems.len());
            for p in &problems {
                eprintln!("  - {p}");
            }
            exit::CONFIG
        }
    }
}

/// `scorpio config show`: print the effective (merged) configuration. Assumes
/// [`init`] has already loaded configuration.
pub fn config_show() -> i32 {
    println!("{}", config::effective_config_dump());
    exit::SUCCESS
}

/// Emit effective runtime paths as NUL-delimited records for `install.sh`.
///
/// This deliberately avoids global config initialization because initialization
/// creates runtime directories, which would be unsafe during installer checks.
pub fn config_installer_paths(config_path: &str, overrides: HashMap<String, String>) -> i32 {
    let paths = match config::resolve_runtime_paths(config_path, overrides) {
        Ok(paths) => paths,
        Err(e) => {
            eprintln!("failed to resolve runtime paths from '{config_path}': {e}");
            return exit::CONFIG;
        }
    };
    let values = [
        paths.workspace,
        paths.store_path,
        paths.config_file,
        paths.antares_upper_root,
        paths.antares_cl_root,
        paths.antares_mount_root,
        paths.antares_state_file,
    ];
    let mut stdout = io::stdout().lock();
    for value in values {
        if value.as_bytes().contains(&0) {
            eprintln!("runtime path contains a NUL byte and cannot be installed");
            return exit::CONFIG;
        }
        if let Err(e) = stdout
            .write_all(value.as_bytes())
            .and_then(|_| stdout.write_all(&[0]))
        {
            eprintln!("failed to write installer runtime paths: {e}");
            return exit::INTERNAL;
        }
    }
    exit::SUCCESS
}

/// Template used by `scorpio config init`.
const CONFIG_TEMPLATE: &str = r#"# ScorpioFS configuration. Every key can be overridden by SCORPIO_<KEY> env vars
# and on the CLI; precedence is CLI > env > this file > built-in defaults.
base_url = "http://localhost:8000"
lfs_url = "http://localhost:8000/lfs"
workspace = "/tmp/scorpio-megadir/mount"
store_path = "/tmp/scorpio-megadir/store"
config_file = "config.toml"
git_author = "MEGA"
git_email = "admin@mega.org"
log_level = "info"
antares_upper_root = "/tmp/scorpio-megadir/antares/upper"
antares_cl_root = "/tmp/scorpio-megadir/antares/cl"
antares_mount_root = "/tmp/scorpio-megadir/antares/mnt"
antares_state_file = "/tmp/scorpio-megadir/antares/state.toml"

[fuse_profile]
enabled = false
path = "/tmp/scorpiofs-fuse-profile.tsv"
agent = "unlabeled"
task = ""
capacity = 262144
flush_interval_ms = 10
"#;

/// Wait for SIGTERM/SIGINT (Unix) or Ctrl-C (other platforms).
///
/// Signal-handler registration failures are logged and degrade gracefully
/// (falling back to a narrower signal set) instead of panicking.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to install SIGTERM handler ({e}); falling back to Ctrl-C");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to install SIGINT handler ({e}); waiting on SIGTERM only");
                sigterm.recv().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
