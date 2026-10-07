//! Commands for the v3 workspace daemon and its HTTP client.

use std::{
    collections::HashMap,
    io::{self, Write},
    net::SocketAddr,
    time::Duration,
};

use tokio::sync::oneshot;

use crate::{
    snapshot::Mst2Client,
    util::{config, logging},
    workspace::{WorkspaceConfig, WorkspaceService},
};

mod observation_sink;
pub use observation_sink::ObservationFileOptions;
use observation_sink::ObservationSink;

/// Stable process exit codes shared by the CLIs (scripts depend on these).
pub mod exit {
    pub const SUCCESS: i32 = 0;
    pub const INTERNAL: i32 = 1;
    pub const CONFIG: i32 = 2;
    pub const MOUNT: i32 = 3;
    pub const BIND: i32 = 4;
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

/// Run the workspace control daemon until a shutdown signal. Filesystems are
/// mounted by explicit requests, after selecting their fixed lower view.
/// Assumes [`init`] has already loaded configuration.
pub async fn serve(http_addr: SocketAddr) -> i32 {
    serve_with_observation(http_addr, None).await
}

/// Explicit diagnostic output. Ordinary serving creates no observation task,
/// file, additional resolve request, or diagnostic request header.
pub async fn serve_with_observation(
    http_addr: SocketAddr,
    observation: Option<ObservationFileOptions>,
) -> i32 {
    serve_with_diagnostics(http_addr, observation, false).await
}

pub async fn serve_with_diagnostics(
    http_addr: SocketAddr,
    observation: Option<ObservationFileOptions>,
    read_profile: bool,
) -> i32 {
    // Bind the HTTP listener up-front so a bind failure is a clean exit (code 4)
    // rather than a panic inside the daemon task.
    let listener = match tokio::net::TcpListener::bind(http_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("failed to bind HTTP address {http_addr}: {e}");
            return exit::BIND;
        }
    };
    tracing::info!("server running on {http_addr}");

    let (observer, mut sink) = match observation {
        None => (None, None),
        Some(options) => match ObservationSink::start(options) {
            Ok((observer, sink)) => (Some(observer), Some(sink)),
            Err(error) => {
                tracing::error!("workspace observation initialization failed: {error}");
                return exit::CONFIG;
            }
        },
    };

    let token = config::mst2_auth_token();
    let paths = config::runtime_paths();
    let client = Mst2Client::with_token(
        config::mst2_base_url(),
        (!token.is_empty()).then(|| token.to_owned()),
    );
    let mut workspace_config =
        WorkspaceConfig::new(paths.workspace_root.into(), paths.cache_root.into());
    workspace_config.read_profile = read_profile;
    let initialized = match &observer {
        Some(observer) => {
            WorkspaceService::new_with_observer(client, workspace_config, observer.clone())
        }
        None => WorkspaceService::new(client, workspace_config),
    };
    let service = match initialized {
        Ok(service) => service,
        Err(error) => {
            tracing::error!("workspace initialization failed: {error}");
            drop(observer);
            if let Some(sink) = sink {
                let _ = sink.finish(exit::CONFIG).await;
            }
            return exit::CONFIG;
        }
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let mut daemon_task = tokio::spawn(crate::workspace::http::serve(
        service.clone(),
        listener,
        shutdown_rx,
    ));

    let mut exit_code = exit::SUCCESS;
    let mut daemon_finished = false;

    // The daemon owns its explicitly created mounts. No workspace lifecycle
    // is initialized before the HTTP listener admits an explicit request.
    tokio::select! {
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
        _ = observation_failed(&mut sink) => {
            tracing::error!("workspace observation failure requires daemon shutdown");
            exit_code = exit::INTERNAL;
        }
    }

    // Drain HTTP requests before cleaning up their mounts: an admitted create
    // must finish recording its handle before shutdown drains the service.
    let _ = shutdown_tx.send(());
    if !daemon_finished {
        match tokio::time::timeout(Duration::from_secs(35), &mut daemon_task).await {
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
                let _ = daemon_task.await;
                let _ =
                    tokio::time::timeout(Duration::from_secs(15), service.shutdown_cleanup()).await;
                exit_code = exit::INTERNAL;
            }
        }
    }

    // HTTP and owned cleanup have joined. Any detached lifecycle owner still
    // holding the service keeps the producer alive, which rejects sink finish.
    drop(service);
    drop(observer);
    if let Some(sink) = sink {
        if !sink.finish(exit_code).await && exit_code == exit::SUCCESS {
            exit_code = exit::INTERNAL;
        }
    }
    exit_code
}

async fn observation_failed(sink: &mut Option<ObservationSink>) {
    match sink {
        Some(sink) => sink.failed().await,
        None => std::future::pending().await,
    }
}

/// Every workspace command goes through the daemon that owns its mount, reader,
/// private upper and lifecycle fence. The client never constructs a manager.
pub enum WorkspaceCommand {
    Create {
        scope: String,
        view_id: Option<String>,
        full: bool,
    },
    List,
    Status {
        id: String,
    },
    Hydrate {
        id: String,
    },
    CancelHydrate {
        id: String,
    },
    ReleaseLocalPin {
        id: String,
    },
    Destroy {
        id: String,
        discard_dirty: bool,
    },
}

pub async fn workspace_request(endpoint: &str, command: WorkspaceCommand) -> i32 {
    match send_workspace_request(endpoint, command).await {
        Ok(Some(response)) => {
            println!("{}", serde_json::to_string_pretty(&response).unwrap());
            exit::SUCCESS
        }
        Ok(None) => exit::SUCCESS,
        Err(error) => {
            eprintln!("workspace request failed: {error}");
            exit::INTERNAL
        }
    }
}

async fn send_workspace_request(
    endpoint: &str,
    command: WorkspaceCommand,
) -> Result<Option<serde_json::Value>, String> {
    use reqwest::{Method, StatusCode};
    use serde_json::json;

    let mut url = url::Url::parse(endpoint).map_err(|_| "invalid daemon URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("daemon URL must use HTTP(S) without credentials, query or fragment".into());
    }
    let (method, mut id, suffix, body) = match command {
        WorkspaceCommand::Create {
            scope,
            view_id,
            full,
        } => {
            crate::snapshot::auth::validate_scope(&scope).map_err(|e| e.to_string())?;
            let target = if let Some(view_id) = view_id {
                crate::snapshot::frames::parse_digest(&view_id).map_err(|e| e.to_string())?;
                json!({"kind": "view", "view_id": view_id})
            } else {
                json!({"kind": "latest"})
            };
            (
                Method::POST,
                None,
                None,
                Some(json!({
                    "target": target,
                    "scope": scope,
                    "delivery": if full { "full" } else { "lazy" },
                    "upper_policy": "private",
                })),
            )
        }
        WorkspaceCommand::List => (Method::GET, None, None, None),
        WorkspaceCommand::Status { id } => (Method::GET, Some(id), None, None),
        WorkspaceCommand::Hydrate { id } => (Method::POST, Some(id), Some("hydrate"), None),
        WorkspaceCommand::CancelHydrate { id } => {
            (Method::POST, Some(id), Some("hydrate/cancel"), None)
        }
        WorkspaceCommand::ReleaseLocalPin { id } => {
            (Method::POST, Some(id), Some("local-pin/release"), None)
        }
        WorkspaceCommand::Destroy { id, discard_dirty } => (
            Method::POST,
            Some(id),
            Some("destroy"),
            Some(json!({"discard_dirty": discard_dirty})),
        ),
    };
    if let Some(id) = &mut id {
        *id = uuid::Uuid::parse_str(id)
            .map_err(|_| "workspace ID must be a UUID")?
            .to_string();
    }
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| "invalid daemon URL path")?;
        segments.pop_if_empty().push("v3").push("workspaces");
        if let Some(id) = &id {
            segments.push(id);
        }
        if let Some(suffix) = suffix {
            segments.extend(suffix.split('/'));
        }
    }
    // Never replay a mutating request after a timeout or follow a redirect to
    // another lifecycle owner. The daemon retains admitted operations itself.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .map_err(|e| e.to_string())?;
    let mut request = client.request(method, url);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let mut response = request.send().await.map_err(|e| e.to_string())?;
    let status = response.status();
    const RESPONSE_CAP: usize = 4 * 1024 * 1024;
    if response
        .content_length()
        .is_some_and(|length| length > RESPONSE_CAP as u64)
    {
        return Err("workspace response exceeds 4 MiB".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        if chunk.len() > RESPONSE_CAP - bytes.len() {
            return Err("workspace response exceeds 4 MiB".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        return Err(format!(
            "status={status} body={}",
            String::from_utf8_lossy(&bytes)
        ));
    }
    if status == StatusCode::NO_CONTENT && bytes.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| format!("invalid response JSON: {e}"))
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
            println!("edit mst2_base_url, then: scorpio --config-path {path} doctor");
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
    let problems = config::validate_file(config_path, overrides);
    if problems.is_empty() {
        println!("{config_path}: OK");
        exit::SUCCESS
    } else {
        eprintln!("{config_path}: {} problem(s) found:", problems.len());
        for p in &problems {
            eprintln!("  - {p}");
        }
        exit::CONFIG
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
/// Resolution is read-only and uses the same derived roots as the daemon.
pub fn config_installer_paths(config_path: &str, overrides: HashMap<String, String>) -> i32 {
    let paths = match config::resolve_runtime_paths(config_path, overrides) {
        Ok(paths) => paths,
        Err(e) => {
            eprintln!("failed to resolve runtime paths from '{config_path}': {e}");
            return exit::CONFIG;
        }
    };
    let values = [paths.store_path, paths.workspace_root, paths.cache_root];
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
# precedence is CLI > env > this file > built-in defaults.
mst2_base_url = "http://127.0.0.1:19700"
mst2_auth_token = ""
store_path = "/tmp/scorpio-megadir/store"
log_level = "info"
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
