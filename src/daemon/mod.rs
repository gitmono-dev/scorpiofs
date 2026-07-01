use std::{path::PathBuf, sync::Arc};

use axum::{
    extract::{Path, Request, State},
    middleware::Next,
    response::Response,
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use git_internal::hash::ObjectHash;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

use crate::{
    fuse::MegaFuse,
    manager::{fetch::fetch, ScorpioManager, WorkDir},
    util::{config, GPath},
};
pub mod antares;
//mod git;

const SUCCESS: &str = "Success";
const FAIL: &str = "Fail";

#[derive(Debug, Deserialize, Serialize, Clone)]
struct MountRequest {
    path: String,
    cl: Option<String>, // cl is the mount request, used for buck2 temp mount.
}

/// Response structure for mount requests.
/// Returns immediately with a request ID for tracking the async operation.
#[derive(Debug, Deserialize, Serialize)]
struct MountResponse {
    status: String,     // Operation status: "Success" or "Fail"
    request_id: String, // Unique ID for tracking the mount task
    message: String,    // Human-readable status message
}
/// Mount task structure, used to track asynchronous mount operations.
/// Each task represents a mount request that can be executed in the background.
#[derive(Debug, Deserialize, Serialize, Clone)]
struct MountStatus {
    request_id: String,        // Unique identifier for the mount request
    status: String,            // Current task status: "fetching", "finished", or "error"
    mount_info: MountRequest,  // Original mount request containing path and cl info
    result: Option<MountInfo>, // Mount result populated when task completes successfully
}
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
struct MountInfo {
    hash: String,
    path: String,
    inode: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct MountsResponse {
    status: String,
    mounts: Vec<MountInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
struct UmountRequest {
    path: Option<String>,
    inode: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct UmountResponse {
    status: String,
    message: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ConfigResponse {
    status: String,
    config: ConfigInfo,
}

#[derive(Debug, Deserialize, Serialize)]
struct ConfigInfo {
    mega_url: String,
    mount_path: String,
    store_path: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ConfigRequest {
    mega_url: Option<String>,
    mount_path: Option<String>,
    store_path: Option<String>,
}

/// Response structure for mount task status queries.
/// Provides current task status and mount information when available.
#[derive(Debug, Deserialize, Serialize)]
struct SelectResponse {
    status: String,           // API call status: "Success" or "Fail"
    task_status: String,      // Task status: "fetching", "finished", "error", or "not_found"
    mount: Option<MountInfo>, // Mount information available when task is finished
    message: String,          // Human-readable status message
}
/// Liveness response for the root `GET /health` endpoint.
///
/// Intentionally lightweight: it reports only process-level liveness and never
/// performs remote (mega) or deep FUSE checks, and never leaks absolute
/// workspace/store paths. Deep checks belong in readiness / `scorpio doctor`.
#[derive(Debug, Serialize, Deserialize)]
struct HealthResponse {
    status: String,
    version: String,
    uptime_secs: u64,
    /// Number of tracked mounts. `null` if the manager lock is momentarily held
    /// by an in-flight mount (liveness must never block on it).
    mount_count: Option<usize>,
}

/// Application state shared across all request handlers.
/// Contains shared resources and task tracking for the daemon.
#[derive(Clone)]
struct ScoState {
    fuse: Arc<MegaFuse>,                      // Shared FUSE filesystem interface
    manager: Arc<Mutex<ScorpioManager>>,      // Shared workspace manager
    tasks: Arc<DashMap<String, MountStatus>>, // Thread-safe storage for async mount tasks
    started: std::time::Instant,              // Process start time, for uptime reporting
}

/// Resolve a mount request to an inode and whether it should be treated as a temporary mount.
///
/// - If the path exists, returns its inode and `temp_mount=false`.
/// - If the path doesn't exist and this is a temp mount request (buck2), creates a temp point.
/// - If the path doesn't exist and this is a normal mount, returns a descriptive error.
async fn resolve_mount_inode(
    state: &ScoState,
    req: &MountRequest,
    mono_path: &str,
) -> Result<(u64, bool), String> {
    let temp_request = req.cl.is_none();

    match state.fuse.get_inode(mono_path).await {
        Ok(inode) => Ok((inode, false)),
        Err(_) => {
            if temp_request {
                let inode = match state.fuse.dic.store.add_temp_point(mono_path).await {
                    Ok(inode) => inode,
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            if let Ok(crate::dicfuse::store::PathLookupStatus::ParentNotLoaded {
                                parent_path,
                            }) = state.fuse.dic.store.lookup_path_status(mono_path).await
                            {
                                return Err(format!(
                                    "Temp mount parent directory not loaded in dicfuse: {mono_path} (parent: {parent_path})"
                                ));
                            }
                        }
                        return Err(format!("Failed to add temp point for {mono_path}: {e}"));
                    }
                };
                return Ok((inode, true));
            }

            let status = state
                .fuse
                .dic
                .store
                .lookup_path_status(mono_path)
                .await
                .map_err(|e| format!("Mount path lookup failed in dicfuse: {mono_path}: {e}"))?;

            match status {
                crate::dicfuse::store::PathLookupStatus::Found(inode) => Ok((inode, false)),
                crate::dicfuse::store::PathLookupStatus::ParentNotLoaded { parent_path } => Err(
                    format!(
                        "Mount parent directory not loaded in dicfuse: {mono_path} (parent: {parent_path})"
                    ),
                ),
                crate::dicfuse::store::PathLookupStatus::NotFound => Err(format!(
                    "Mount path not found in dicfuse: {mono_path}"
                )),
            }
        }
    }
}
#[allow(unused)]
pub async fn daemon_main(
    fuse: Arc<MegaFuse>,
    manager: ScorpioManager,
    shutdown_rx: oneshot::Receiver<()>,
    listener: tokio::net::TcpListener,
) -> std::io::Result<()> {
    let inner = ScoState {
        fuse,
        manager: Arc::new(Mutex::new(manager)),
        tasks: Arc::new(DashMap::new()), // Initialize empty task tracking map
        started: std::time::Instant::now(),
    };

    // Legacy `/api/fs/*` and `/api/config` routes are deprecated in favor of the
    // Antares API (`/antares/*`). They keep working for at least one minor
    // release but are tagged with a `Deprecation: true` response header and a
    // warning log via the middleware below.
    let deprecated = Router::new()
        .route("/api/fs/mount", post(mount_handler))
        .route("/api/fs/mpoint", get(mounts_handler))
        .route("/api/fs/select/{request_id}", get(select_handler))
        .route("/api/fs/unmount", post(unmount_handler))
        .route("/api/config", get(config_handler))
        .route("/api/config", post(update_config_handler))
        // Note: git-related routes have been moved to `src/daemon/git.rs`
        // and are currently disabled here. To enable them, merge the
        // router returned by `daemon::git::router()` into this `app`.
        .layer(axum::middleware::from_fn(deprecation_middleware));

    let mut app = Router::new()
        .route("/health", get(health_handler))
        .merge(deprecated)
        .with_state(inner);

    // Antares route - create service with new Dicfuse instance
    let antares_service = Arc::new(antares::AntaresServiceImpl::new(None).await);
    let antares_service_for_shutdown = antares_service.clone();
    let antares_daemon = antares::AntaresDaemon::new(antares_service);
    let antares_router = antares_daemon.router();
    let app = app.nest("/antares", antares_router);

    // The listener is bound by the caller so bind failures surface as a clean
    // CLI exit code instead of a panic inside this task.
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
            tracing::info!("HTTP server shutdown requested; running Antares shutdown cleanup");
            match tokio::time::timeout(
                std::time::Duration::from_secs(15),
                antares_service_for_shutdown.shutdown_cleanup_impl(),
            )
            .await
            {
                Ok(Ok(())) => tracing::info!("Antares shutdown cleanup completed"),
                Ok(Err(e)) => tracing::warn!("Antares shutdown cleanup failed: {:?}", e),
                Err(_) => tracing::warn!("Antares shutdown cleanup timed out"),
            }
        })
        .await
}

/// Root liveness probe. Lightweight by design: reports process status, version,
/// uptime, and the number of tracked mounts, without remote/FUSE deep checks or
/// leaking absolute paths. Readiness/deep checks live under `/antares/...` and
/// `scorpio doctor`.
async fn health_handler(State(state): State<ScoState>) -> axum::Json<HealthResponse> {
    // Use `try_lock` so liveness never blocks: `perform_mount_task` holds this
    // mutex across remote fetch/download work, so an `await`ing `lock()` here
    // could stall the probe under load.
    let mount_count = state.manager.try_lock().ok().map(|m| m.works.len());
    axum::Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: state.started.elapsed().as_secs(),
        mount_count,
    })
}

/// Tags responses from deprecated routes with `Deprecation: true` (RFC 8594)
/// and emits a warning log so callers can discover and migrate off them.
async fn deprecation_middleware(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    tracing::warn!(
        "deprecated endpoint called: {method} {uri}; prefer the Antares API (/antares/*) — see docs/api.md"
    );
    let mut response = next.run(req).await;
    response.headers_mut().insert(
        axum::http::header::HeaderName::from_static("deprecation"),
        axum::http::HeaderValue::from_static("true"),
    );
    response
}

/// Asynchronous mount handler for clients.
/// Initiates a mount operation in the background and returns immediately with a tracking ID.
/// This allows clients to start multiple mount operations concurrently and check their status.
async fn mount_handler(
    State(state): State<ScoState>,
    req: axum::Json<MountRequest>,
) -> axum::Json<MountResponse> {
    // Generate a unique request ID for tracking this mount operation
    let request_id = Uuid::new_v4().to_string();

    // Create initial task record with "fetching" status
    let mount_status = MountStatus {
        request_id: request_id.clone(),
        status: "fetching".to_string(),
        mount_info: req.0.clone(),
        result: None,
    };

    // Store the task in the shared task map for status tracking
    state.tasks.insert(request_id.clone(), mount_status);

    // Perform the mount operation synchronously
    let mount_result = perform_mount_task(state.clone(), req.0.clone()).await;

    // Update the task status based on mount operation result
    if let Some(mut task) = state.tasks.get_mut(&request_id) {
        match mount_result {
            Ok(mount_info) => {
                task.status = "finished".to_string();
                task.result = Some(mount_info);
                axum::Json(MountResponse {
                    status: SUCCESS.to_string(),
                    request_id,
                    message: "Mount task completed".to_string(),
                })
            }
            Err(err) => {
                task.status = "error".to_string();
                let message =
                    if err.contains("already mounted") || err.contains("already checked-out") {
                        "please unmount".to_string()
                    } else {
                        format!("Mount failed: {}", err)
                    };

                axum::Json(MountResponse {
                    status: FAIL.to_string(),
                    request_id,
                    message,
                })
            }
        }
    } else {
        axum::Json(MountResponse {
            status: FAIL.to_string(),
            request_id,
            message: "task not found".to_string(),
        })
    }
}

/// Helper function to perform the actual mount operation.
/// This function contains the core mounting logic extracted from the original mount handler.
/// It handles both temporary mounts (for buck2) and regular mounts with proper error handling.
async fn perform_mount_task(state: ScoState, req: MountRequest) -> Result<MountInfo, String> {
    // Normalize the path format using GPath utility
    let mono_path = if let Some(cl) = &req.cl {
        format!("{}_{}", GPath::from(req.path.clone()), cl)
    } else {
        GPath::from(req.path.clone()).to_string()
    };

    // Resolve inode and determine temp mount behavior
    let (inode, temp_mount) = resolve_mount_inode(&state, &req, &mono_path).await?;

    // Check if the target is already mounted to prevent conflicts
    if state.fuse.is_mount(inode).await {
        return Err("The target is already mounted".to_string());
    }

    // Acquire manager lock and check for existing checkouts
    let mut ml = state.manager.lock().await;
    if let Err(mounted_path) = ml.check_before_mount(&mono_path) {
        return Err(format!("The {mounted_path} is already checked-out"));
    }

    let store_path = config::store_path();

    // Handle temporary mount case (typically for buck2)
    if temp_mount {
        let temp_hash = {
            let hasher = ObjectHash::new(mono_path.as_bytes());
            hasher.to_string()
        };

        let store_path = PathBuf::from(store_path).join(&temp_hash);

        // Perform the actual overlay mount
        state
            .fuse
            .overlay_mount(inode, store_path, false, None)
            .await
            .map_err(|e| format!("Failed to overlay mount: {e}"))?;

        let mount_info = MountInfo {
            hash: temp_hash.clone(),
            path: mono_path.clone(),
            inode,
        };

        // Update manager's work directory list
        ml.works.push(WorkDir {
            path: mono_path,
            node: inode,
            hash: temp_hash,
        });
        // Persist to the configured state file. On failure, roll back both the
        // in-memory entry and the overlay mount so memory, disk, and the actual
        // mount table stay consistent instead of pretending success.
        let state_file = config::config_file();
        // Convert the (`!Send`) `Box<dyn Error>` into an owned message inside the
        // match so it is dropped before the await below; otherwise the handler
        // future would be `!Send`.
        let persist_err = match ml.to_toml(state_file) {
            Ok(()) => None,
            Err(e) => Some(format!("Failed to persist mount state: {e}")),
        };
        if let Some(msg) = persist_err {
            ml.works.pop();
            drop(ml);
            tracing::error!("failed to persist mount state to '{state_file}': {msg}");
            if let Err(ue) = state.fuse.overlay_umount_byinode(inode).await {
                tracing::error!(
                    "failed to roll back overlay mount for inode {inode} after state persist failure: {ue}"
                );
            }
            return Err(msg);
        }

        return Ok(mount_info);
    }

    // Handle regular mount case - fetch repository information
    let work_dir = fetch(&mut ml, inode, mono_path.clone(), &req.path)
        .await
        .map_err(|e| format!("Failed to fetch: {e}"))?;

    let store_path = PathBuf::from(store_path).join(&work_dir.hash);

    // CL layer support removed: skip building CL layer if provided

    // Perform the final overlay mount with CL layer if applicable
    state
        .fuse
        .overlay_mount(inode, store_path, req.cl.is_some(), req.cl.as_deref())
        .await
        .map_err(|e| format!("Mount process error: {e}"))?;

    let mount_info = MountInfo {
        hash: work_dir.hash,
        path: work_dir.path,
        inode,
    };

    Ok(mount_info)
}

/// Query handler for mount task status.
/// Allows clients to check the progress of their asynchronous mount operations.
/// Requires a valid request_id as URL path parameter.
/// Automatically cleans up completed tasks from memory.
async fn select_handler(
    State(state): State<ScoState>,
    Path(request_id): Path<String>,
) -> axum::Json<SelectResponse> {
    // Search by request_id (now provided as URL path parameter)
    if let Some(task) = state.tasks.get(&request_id) {
        let response = SelectResponse {
            status: SUCCESS.to_string(),
            task_status: task.status.clone(),
            mount: task.result.clone(),
            message: "Task found".to_string(),
        };

        // Clean up completed tasks from memory to prevent memory leaks
        if task.status == "finished" || task.status == "error" {
            drop(task); // Release the reference before removing
            state.tasks.remove(&request_id);
        }

        axum::Json(response)
    } else {
        axum::Json(SelectResponse {
            status: FAIL.to_string(),
            task_status: "not_found".to_string(),
            mount: None,
            message: "Task not found".to_string(),
        })
    }
}

/// Get all mounted dictionary .
async fn mounts_handler(State(state): State<ScoState>) -> axum::Json<MountsResponse> {
    let manager = state.manager.lock().await;
    let re = manager
        .works
        .iter()
        .map(|word_dir| MountInfo {
            hash: word_dir.hash.clone(),
            path: word_dir.path.clone(),
            inode: word_dir.node,
        })
        .collect();

    axum::Json(MountsResponse {
        status: SUCCESS.into(),
        mounts: re,
    })
}

/// Unmounts filesystem and removes CL layer files
async fn unmount_handler(
    State(state): State<ScoState>,
    req: axum::Json<UmountRequest>,
) -> axum::Json<UmountResponse> {
    let handle;
    if let Some(inode) = req.inode {
        handle = state.fuse.overlay_umount_byinode(inode).await;
    } else if let Some(path) = &req.path {
        handle = state.fuse.overlay_umount_bypath(path).await;
    } else {
        return axum::Json(UmountResponse {
            status: FAIL.into(),
            message: "Need a inode or path.".to_string(),
        });
    }
    match handle {
        Ok(_) => {
            // Derive the canonical (normalized `GPath`) path of the resource that
            // was actually unmounted, so CL cleanup + state bookkeeping match the
            // keys stored in `works`. The unmount above prefers `inode` over
            // `path`, so resolve from `inode` first for consistency (and to avoid
            // unmounting A while removing bookkeeping for B). Avoid panics: if the
            // path can't be resolved, log and skip bookkeeping — the unmount
            // already succeeded.
            let path_str = if let Some(inode) = req.inode {
                match state.fuse.dic.store.find_path(inode).await {
                    Some(path) => Some(path.to_string()),
                    None => {
                        tracing::warn!(
                            "unmounted inode {inode} but could not resolve its path for cleanup"
                        );
                        None
                    }
                }
            } else {
                // Normalize so it matches the stored `GPath` keys (e.g. "/repo" -> "repo").
                req.path
                    .as_ref()
                    .map(|path| GPath::from(path.clone()).to_string())
            };

            if let Some(path_str) = path_str {
                // Try to get the CL link from the path and clean up CL layer
                if let Some(cl_pos) = path_str.rfind('_') {
                    let potential_cl_link = &path_str[cl_pos + 1..];
                    // Simple validation - CL links are usually not entire paths
                    if !potential_cl_link.contains('/') && !potential_cl_link.is_empty() {
                        let store_path = config::store_path();
                        let _ = state
                            .fuse
                            .remove_cl_layer_by_cl_link(store_path, potential_cl_link)
                            .await;
                    }
                }

                // The filesystem is already unmounted at this point; a state-file
                // update failure (or a benign "not tracked" path) is logged rather
                // than silently dropped, but does not undo the successful unmount.
                if let Err(e) = state.manager.lock().await.remove_workspace(&path_str).await {
                    tracing::warn!("failed to update mount state after unmounting {path_str}: {e}");
                }
            }

            axum::Json(UmountResponse {
                status: SUCCESS.into(),
                message: "Directory unmounted successfully".to_string(),
            })
        }
        Err(err) => axum::Json(UmountResponse {
            status: FAIL.into(),
            message: format!("Umount process error :{err}."),
        }),
    }
}

async fn config_handler() -> axum::Json<ConfigResponse> {
    let base_url = config::base_url();
    let workspace = config::workspace();
    let store_path = config::store_path();
    let config_info = ConfigInfo {
        mega_url: base_url.to_string(),
        mount_path: workspace.to_string(),
        store_path: store_path.to_string(),
    };

    axum::Json(ConfigResponse {
        status: SUCCESS.into(),
        config: config_info,
    })
}

/// Deprecated, non-functional configuration update endpoint.
///
/// This handler does NOT persist or apply any configuration; it only echoes the
/// request. It is tagged `Deprecation: true` (see `deprecation_middleware`).
/// Configuration changes should go through the config file + restart. The
/// request struct still uses the legacy `mega_url`/`mount_path` field names
/// (mapping to `base_url`/`workspace`); since the endpoint is deprecated and
/// non-functional, the names are kept for backward compatibility rather than
/// renamed. The `status` casing now matches `GET /api/config` ("Success").
async fn update_config_handler(
    State(_state): State<ScoState>,
    req: axum::Json<ConfigRequest>,
) -> axum::Json<ConfigResponse> {
    let config_info = ConfigInfo {
        mega_url: req.mega_url.clone().unwrap_or_default(),
        mount_path: req.mount_path.clone().unwrap_or_default(),
        store_path: req.store_path.clone().unwrap_or_default(),
    };

    axum::Json(ConfigResponse {
        status: SUCCESS.to_string(),
        config: config_info,
    })
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::dicfuse::Dicfuse;

    async fn make_state() -> ScoState {
        let tmp = tempdir().unwrap();
        let dic = Dicfuse::new_with_store_path(tmp.path().to_str().unwrap()).await;
        let mut fuse = MegaFuse::new().await;
        fuse.dic = Arc::new(dic);

        ScoState {
            fuse: Arc::new(fuse),
            manager: Arc::new(Mutex::new(ScorpioManager { works: vec![] })),
            tasks: Arc::new(DashMap::new()),
            started: std::time::Instant::now(),
        }
    }

    #[tokio::test]
    async fn test_health_handler_reports_liveness() {
        let state = make_state().await;
        let body = health_handler(State(state)).await.0;
        assert_eq!(body.status, "ok");
        assert_eq!(body.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(body.mount_count, Some(0));
        // Must not leak any absolute path in the liveness payload.
        let json = serde_json::to_string(&body).unwrap();
        assert!(
            !json.contains('/'),
            "health payload must not leak paths: {json}"
        );
    }

    #[tokio::test]
    async fn test_deprecation_header_present_only_on_legacy_routes() {
        use axum::body::Body;
        use tower::ServiceExt;

        let state = make_state().await;
        let deprecated = Router::new()
            .route("/api/config", get(config_handler))
            .layer(axum::middleware::from_fn(deprecation_middleware));
        let app = Router::new()
            .route("/health", get(health_handler))
            .merge(deprecated)
            .with_state(state);

        // `/health` is NOT deprecated.
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.headers().get("deprecation").is_none());

        // `/api/config` is deprecated → `Deprecation: true`.
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.headers().get("deprecation").unwrap(), "true");
    }

    #[tokio::test]
    async fn test_resolve_mount_inode_found_path() {
        let state = make_state().await;
        state.fuse.dic.store.insert_mock_item(1, 0, "", true).await;
        state
            .fuse
            .dic
            .store
            .insert_mock_item(2, 1, "repo", true)
            .await;

        let req = MountRequest {
            path: "/repo".to_string(),
            cl: None,
        };
        let (inode, temp_mount) = resolve_mount_inode(&state, &req, "repo").await.unwrap();
        assert_eq!(inode, 2);
        assert!(!temp_mount);
    }

    #[tokio::test]
    async fn test_resolve_mount_inode_temp_mount() {
        let state = make_state().await;
        state.fuse.dic.store.insert_mock_item(1, 0, "", true).await;
        state
            .fuse
            .dic
            .store
            .insert_mock_item(2, 1, "repo", true)
            .await;

        let req = MountRequest {
            path: "/repo/tmp".to_string(),
            cl: None,
        };
        let (inode, temp_mount) = resolve_mount_inode(&state, &req, "repo/tmp").await.unwrap();
        assert!(temp_mount);
        let inode_check = state
            .fuse
            .dic
            .store
            .get_inode_from_path("repo/tmp")
            .await
            .unwrap();
        assert_eq!(inode, inode_check);
    }

    #[tokio::test]
    async fn test_resolve_mount_inode_not_found_normal_mount() {
        let state = make_state().await;
        state.fuse.dic.store.insert_mock_item(1, 0, "", true).await;
        state
            .fuse
            .dic
            .store
            .insert_mock_item(2, 1, "repo", true)
            .await;

        let req = MountRequest {
            path: "/repo/missing".to_string(),
            cl: Some("cl123".to_string()),
        };
        let err = resolve_mount_inode(&state, &req, "repo/missing_cl123")
            .await
            .unwrap_err();
        // Check that error message indicates path issue in dicfuse
        assert!(err.contains("dicfuse"));
        assert!(err.contains("repo/missing_cl123"));
        // Accept either "not found" or "not loaded" error messages
        assert!(
            err.contains("Mount path not found in dicfuse")
                || err.contains("Mount parent directory not loaded in dicfuse")
        );
    }
}
