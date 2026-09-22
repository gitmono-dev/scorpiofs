//! Antares daemon HTTP interface for mount lifecycle management.
//!
//! Provides Axum routes to create, list, query, and delete FUSE mounts backed by
//! AntaresService implementations. Includes graceful shutdown with cleanup.

#[cfg(any(test, target_os = "macos"))]
use std::ffi::OsString;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    net::SocketAddr,
    os::unix::fs::FileTypeExt,
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[cfg(not(target_os = "macos"))]
use std::{ffi::CString, os::unix::ffi::OsStrExt};

use async_trait::async_trait;
use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::AsyncWriteExt,
    sync::RwLock,
    time::{sleep, timeout},
};
use uuid::Uuid;

use crate::{
    antares::fuse::AntaresFuse,
    daemon::lower_view::DicfuseLower,
    daemon::upper_fork::{fork_upper, ForkCopyError, ForkCopyStats},
    daemon::worktree_v2::{
        effective_changes, flatten_chain_into_upper, generation_of, lower_item_for,
        remove_committed_upper_entries,
        resolve_latest_revision, AttachWorktreeRequest, AttachWorktreeResponse,
        CommitFinalizeRequest, CommitFinalizeResponse, CommittedPath, EffectiveKind,
        RefreshDisposition, RefreshRequest, RefreshResponse, WorktreeStateV2,
    },
    dicfuse::store::DictionaryStore,
    dicfuse::{Dicfuse, DicfuseManager},
    snapshot::fuse::Mst2Fuse,
    snapshot::{Mst2Client, SnapshotReader},
    util::config,
};

/// Retention window requested for an MST/2 snapshot view backing a mount. The
/// server clamps to `1..=3600`; a mount outliving the window renews lazily.
const MST2_LEASE_SECONDS: u64 = 3600;

/// Build the MST/2 snapshot-view lower layer when `mst2_lower_enabled` is set
/// (spec 12 §1). Returns `None` in the default Dicfuse mode, so the legacy
/// reader stays the only path unless the operator opted in explicitly
/// (spec 15 §3: no silent fallback in either direction).
async fn mst2_lower_layer() -> Result<Option<Arc<Mst2Fuse>>, ServiceError> {
    if !config::mst2_lower_enabled() {
        return Ok(None);
    }
    let token = config::mst2_auth_token();
    let client = Mst2Client::with_token(
        config::mst2_base_url(),
        (!token.is_empty()).then(|| token.to_string()),
    );
    let reader = SnapshotReader::resolve(client, config::mst2_scope(), MST2_LEASE_SECONDS)
        .await
        .map_err(|e| {
            ServiceError::Internal(format!(
                "mst2 lower: resolve({}) failed: {e}",
                config::mst2_scope()
            ))
        })?;
    let fuse = Mst2Fuse::from_reader_lazy(reader, None)
        .await
        .map_err(|e| ServiceError::Internal(format!("mst2 lower: build view failed: {e}")))?;
    tracing::info!(
        scope = config::mst2_scope(),
        snapshot = ?fuse.snapshot_id(),
        "antares svc: serving MST/2 snapshot view as the lower layer"
    );
    Ok(Some(Arc::new(fuse)))
}

/// The lower projection a mount's effective diff must compare against: the
/// MST/2 snapshot view when the mount serves one, otherwise the Dicfuse
/// projection (P3; see `mst2-impl/P3-HASH-DOMAIN-DESIGN.md`).
fn lower_view_for(entry: &MountEntry) -> Arc<dyn crate::daemon::lower_view::LowerView> {
    use crate::daemon::lower_view::{DicfuseLower, Mst2Lower};
    match &entry.mst2_lower {
        Some(view) => Arc::new(Mst2Lower(view.clone())),
        None => Arc::new(DicfuseLower(entry.fuse.dic.store.clone())),
    }
}

/// MST/2-lowered mounts do not support the worktree-v2 mutations yet: their
/// lower moves by resolving a new snapshot, not by re-pinning the Dicfuse
/// projection, and the finalize/refresh plumbing for that is not in place.
/// Refuse explicitly rather than executing the Dicfuse semantics against the
/// wrong projection (spec 15 §3).
fn reject_mst2_mutation(entry: &MountEntry, op: &str) -> Result<(), ServiceError> {
    if entry.mst2_lower.is_some() {
        return Err(ServiceError::InvalidRequest(format!(
            "{op} is not supported on an MST/2-lowered mount yet; re-attach without \
             mst2_lower_enabled or wait for the snapshot-side finalize/refresh"
        )));
    }
    Ok(())
}

/// High-level HTTP daemon that exposes Antares orchestration capabilities.
pub struct AntaresDaemon<S: AntaresService> {
    service: Arc<S>,
    shutdown_timeout: Duration,
}

impl<S> AntaresDaemon<S>
where
    S: AntaresService + 'static,
{
    /// Construct a daemon backed by the given service.
    pub fn new(service: Arc<S>) -> Self {
        Self {
            service,
            shutdown_timeout: Duration::from_secs(10),
        }
    }

    /// Override the graceful shutdown timeout applied to the HTTP server.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Produce an Axum router with all routes wired to their handlers.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", get(Self::healthcheck))
            .route("/mounts", post(Self::create_mount))
            .route("/mounts", get(Self::list_mounts))
            .route("/mounts/by-job/{job_id}", get(Self::describe_mount_by_job))
            .route("/mounts/by-job/{job_id}", delete(Self::delete_mount_by_job))
            .route("/mounts/{mount_id}", get(Self::describe_mount))
            .route("/mounts/{mount_id}", delete(Self::delete_mount))
            .route("/mounts/{mount_id}/cl", post(Self::build_cl))
            .route("/mounts/{mount_id}/cl", delete(Self::clear_cl))
            .route("/mounts/{mount_id}/ready", get(Self::mount_ready))
            .route("/mounts/{mount_id}/changes", get(Self::mount_changes))
            .route("/mounts/{mount_id}/worktree", get(Self::worktree_state))
            .route(
                "/mounts/{mount_id}/worktree/base",
                post(Self::bind_worktree_base),
            )
            .route(
                "/mounts/{mount_id}/worktree/refresh-plan",
                post(Self::plan_worktree_refresh),
            )
            .route("/mounts/{mount_id}/fork", post(Self::fork_mount))
            // Worktree Control Protocol v2 (docs/scorpiofs-libra-complete-spec-v1.md).
            .route("/worktrees", post(Self::attach_worktree))
            .route("/worktrees/{mount_id}/state", get(Self::worktree_state_v2))
            .route(
                "/worktrees/{mount_id}/commit-finalize",
                post(Self::commit_finalize),
            )
            .route("/worktrees/{mount_id}/refresh", post(Self::refresh_lower))
            .with_state(self.service.clone())
    }

    /// Run the HTTP server until it receives a shutdown signal.
    /// Note: For graceful shutdown with mount cleanup, use `AntaresDaemon<AntaresServiceImpl>`.
    pub async fn serve(self, bind_addr: SocketAddr) -> Result<(), ApiError> {
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|e| {
                ApiError::Service(ServiceError::Internal(format!(
                    "failed to bind to {}: {}",
                    bind_addr, e
                )))
            })?;

        tracing::info!("Antares daemon listening on {}", bind_addr);
        self.serve_with_listener(listener).await
    }

    /// Serve on an already-bound listener. Lets callers bind up-front so a bind
    /// failure can be mapped to a distinct exit code instead of a generic error.
    pub async fn serve_with_listener(
        self,
        listener: tokio::net::TcpListener,
    ) -> Result<(), ApiError> {
        let router = self.router();
        let shutdown_timeout = self.shutdown_timeout;
        let service = self.service.clone();

        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("Received shutdown signal");
                match timeout(shutdown_timeout, service.shutdown_cleanup()).await {
                    Ok(Ok(())) => tracing::info!("Shutdown cleanup completed"),
                    Ok(Err(e)) => tracing::warn!("Shutdown cleanup failed: {:?}", e),
                    Err(_) => {
                        tracing::warn!("Shutdown cleanup timed out after {:?}", shutdown_timeout)
                    }
                }
            })
            .await
            .map_err(|e| {
                ApiError::Service(ServiceError::Internal(format!("server error: {}", e)))
            })?;

        Ok(())
    }

    /// Lightweight health/liveness probe.
    async fn healthcheck(State(service): State<Arc<S>>) -> Result<Json<HealthResponse>, ApiError> {
        Ok(Json(service.health_info().await))
    }

    async fn create_mount(
        State(service): State<Arc<S>>,
        Json(request): Json<CreateMountRequest>,
    ) -> Result<Json<MountCreated>, ApiError> {
        let start = Instant::now();
        let job_id = request.job_id.clone();
        let build_id = request.build_id.clone();
        let path = request.path.clone();
        let cl = request.cl.clone();
        tracing::info!(
            job_id = ?job_id,
            build_id = ?build_id,
            path = %path,
            cl = ?cl,
            "antares http: create_mount request"
        );

        let created = service.create_mount(request).await;
        match &created {
            Ok(created) => tracing::info!(
                mount_id = %created.mount_id,
                mountpoint = %created.mountpoint,
                elapsed_ms = start.elapsed().as_millis(),
                "antares http: create_mount success"
            ),
            Err(err) => tracing::warn!(
                elapsed_ms = start.elapsed().as_millis(),
                error = %err,
                "antares http: create_mount failed"
            ),
        }

        Ok(Json(created?))
    }

    async fn list_mounts(State(service): State<Arc<S>>) -> Result<Json<MountCollection>, ApiError> {
        let mounts = service.list_mounts().await?;
        Ok(Json(MountCollection { mounts }))
    }

    async fn describe_mount_by_job(
        State(service): State<Arc<S>>,
        AxumPath(job_id): AxumPath<String>,
    ) -> Result<Json<MountStatus>, ApiError> {
        let status = service.describe_mount_by_job(job_id).await?;
        Ok(Json(status))
    }

    async fn delete_mount_by_job(
        State(service): State<Arc<S>>,
        AxumPath(job_id): AxumPath<String>,
    ) -> Result<Json<MountStatus>, ApiError> {
        let start = Instant::now();
        tracing::info!(job_id = %job_id, "antares http: delete_mount_by_job request");
        let status = service.delete_mount_by_job(job_id).await;
        match &status {
            Ok(status) => tracing::info!(
                mount_id = %status.mount_id,
                state = ?status.state,
                elapsed_ms = start.elapsed().as_millis(),
                "antares http: delete_mount_by_job done"
            ),
            Err(err) => tracing::warn!(
                elapsed_ms = start.elapsed().as_millis(),
                error = %err,
                "antares http: delete_mount_by_job failed"
            ),
        }
        Ok(Json(status?))
    }

    async fn describe_mount(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
    ) -> Result<Json<MountStatus>, ApiError> {
        let status = service.describe_mount(mount_id).await?;
        Ok(Json(status))
    }

    async fn delete_mount(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
    ) -> Result<Json<MountStatus>, ApiError> {
        let start = Instant::now();
        tracing::info!(mount_id = %mount_id, "antares http: delete_mount request");
        let status = service.delete_mount(mount_id).await;
        match &status {
            Ok(status) => tracing::info!(
                mount_id = %status.mount_id,
                state = ?status.state,
                elapsed_ms = start.elapsed().as_millis(),
                "antares http: delete_mount done"
            ),
            Err(err) => tracing::warn!(
                mount_id = %mount_id,
                elapsed_ms = start.elapsed().as_millis(),
                error = %err,
                "antares http: delete_mount failed"
            ),
        }
        Ok(Json(status?))
    }

    async fn build_cl(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
        Json(request): Json<BuildClRequest>,
    ) -> Result<Json<MountStatus>, ApiError> {
        let start = Instant::now();
        let cl = request.cl;
        tracing::info!(mount_id = %mount_id, cl = %cl, "antares http: build_cl request");
        let status = service.build_cl(mount_id, cl).await;
        match &status {
            Ok(status) => tracing::info!(
                mount_id = %status.mount_id,
                state = ?status.state,
                elapsed_ms = start.elapsed().as_millis(),
                "antares http: build_cl done"
            ),
            Err(err) => tracing::warn!(
                mount_id = %mount_id,
                elapsed_ms = start.elapsed().as_millis(),
                error = %err,
                "antares http: build_cl failed"
            ),
        }
        Ok(Json(status?))
    }

    async fn clear_cl(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
    ) -> Result<Json<MountStatus>, ApiError> {
        let start = Instant::now();
        tracing::info!(mount_id = %mount_id, "antares http: clear_cl request");
        let status = service.clear_cl(mount_id).await;
        match &status {
            Ok(status) => tracing::info!(
                mount_id = %status.mount_id,
                state = ?status.state,
                elapsed_ms = start.elapsed().as_millis(),
                "antares http: clear_cl done"
            ),
            Err(err) => tracing::warn!(
                mount_id = %mount_id,
                elapsed_ms = start.elapsed().as_millis(),
                error = %err,
                "antares http: clear_cl failed"
            ),
        }
        Ok(Json(status?))
    }

    /// Check whether a mount is ready for heavy workloads.
    ///
    /// `ready=true` means Phase 1 (Dicfuse in-memory directory cache warmup)
    /// has completed. Phase 2 kernel-cache warmup may still be running in
    /// background as best-effort optimisation.
    ///
    /// Clients (e.g. Orion) should poll this endpoint before starting heavy
    /// filesystem workloads (buck2 builds) to avoid statx storms against cold
    /// FUSE caches.
    async fn mount_ready(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
    ) -> Result<Json<MountReadyResponse>, ApiError> {
        let resp = service.check_mount_ready(mount_id).await?;
        Ok(Json(resp))
    }

    /// Return paths represented in this mount's private writable upper layer.
    async fn mount_changes(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
    ) -> Result<Json<MountChangesResponse>, ApiError> {
        Ok(Json(service.changed_paths(mount_id).await?))
    }

    /// Return the Git-compatible worktree state for an interactive mount.
    async fn worktree_state(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
    ) -> Result<Json<WorktreeStateResponse>, ApiError> {
        Ok(Json(service.worktree_state(mount_id).await?))
    }

    /// Bind an otherwise clean, CL-free mount to the revision selected by Libra.
    async fn bind_worktree_base(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
        Json(request): Json<BindWorktreeBaseRequest>,
    ) -> Result<Json<WorktreeStateResponse>, ApiError> {
        Ok(Json(service.bind_worktree_base(mount_id, request).await?))
    }

    /// Check whether Libra may safely perform a later base-tree switch.
    async fn plan_worktree_refresh(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
        Json(request): Json<RefreshPlanRequest>,
    ) -> Result<Json<RefreshPlanResponse>, ApiError> {
        Ok(Json(
            service.plan_worktree_refresh(mount_id, request).await?,
        ))
    }

    /// Derive a new worktree mount from an existing one.
    async fn fork_mount(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
        Json(request): Json<ForkMountRequest>,
    ) -> Result<(StatusCode, Json<ForkMountResponse>), ApiError> {
        let response = service.fork_mount(mount_id, request).await?;
        Ok((StatusCode::CREATED, Json(response)))
    }

    /// Worktree v2: attach a worktree with its lower pinned from the first request.
    async fn attach_worktree(
        State(service): State<Arc<S>>,
        Json(request): Json<AttachWorktreeRequest>,
    ) -> Result<(StatusCode, Json<AttachWorktreeResponse>), ApiError> {
        let response = service.attach_worktree(request).await?;
        Ok((StatusCode::CREATED, Json(response)))
    }

    /// Worktree v2: effective diff of a mount.
    async fn worktree_state_v2(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
    ) -> Result<Json<WorktreeStateV2>, ApiError> {
        Ok(Json(service.worktree_state_v2(mount_id).await?))
    }

    /// Worktree v2: finalize a commit (pin lower, clean committed upper entries).
    async fn commit_finalize(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
        Json(request): Json<CommitFinalizeRequest>,
    ) -> Result<Json<CommitFinalizeResponse>, ApiError> {
        Ok(Json(service.commit_finalize(mount_id, request).await?))
    }

    /// Worktree v2: move the lower projection to a newer revision.
    async fn refresh_lower(
        State(service): State<Arc<S>>,
        AxumPath(mount_id): AxumPath<Uuid>,
        Json(request): Json<RefreshRequest>,
    ) -> Result<Json<RefreshResponse>, ApiError> {
        Ok(Json(service.refresh_lower(mount_id, request).await?))
    }
}

/// Asynchronous service boundary that the HTTP layer depends on.
#[async_trait]
pub trait AntaresService: Send + Sync {
    /// Create a new mount with auto-generated paths based on UUID
    async fn create_mount(&self, request: CreateMountRequest)
        -> Result<MountCreated, ServiceError>;
    async fn list_mounts(&self) -> Result<Vec<MountStatus>, ServiceError>;
    async fn describe_mount(&self, mount_id: Uuid) -> Result<MountStatus, ServiceError>;
    async fn delete_mount(&self, mount_id: Uuid) -> Result<MountStatus, ServiceError>;

    /// Describe a mount by build task identifier (job/build id).
    ///
    /// Default implementation scans `list_mounts()`; implementations may override
    /// for efficiency.
    async fn describe_mount_by_job(&self, job_id: String) -> Result<MountStatus, ServiceError> {
        let mounts = self.list_mounts().await?;
        mounts
            .into_iter()
            .find(|m| m.job_id.as_deref() == Some(job_id.as_str()))
            .ok_or(ServiceError::NotFoundTask(job_id))
    }

    /// Delete (unmount) a mount by build task identifier (job/build id).
    ///
    /// Default implementation resolves to a mount_id and delegates to `delete_mount()`.
    async fn delete_mount_by_job(&self, job_id: String) -> Result<MountStatus, ServiceError> {
        let status = self.describe_mount_by_job(job_id.clone()).await?;
        self.delete_mount(status.mount_id).await
    }
    /// Build or rebuild the CL layer for an existing mount
    async fn build_cl(&self, mount_id: Uuid, cl_link: String) -> Result<MountStatus, ServiceError>;
    /// Clear the CL layer for an existing mount
    async fn clear_cl(&self, mount_id: Uuid) -> Result<MountStatus, ServiceError>;

    /// Check whether a mount is ready for heavy I/O workloads (e.g. buck2).
    ///
    /// Returns `MountReadyResponse` with `ready=true` once Phase 1 completes.
    /// Background kernel warmup (Phase 2) is intentionally non-blocking.
    async fn check_mount_ready(&self, mount_id: Uuid) -> Result<MountReadyResponse, ServiceError>;

    /// List paths changed in the mount's private writable upper layer.
    async fn changed_paths(&self, mount_id: Uuid) -> Result<MountChangesResponse, ServiceError>;

    /// Return the lower-base binding and writable upper state used by Libra.
    async fn worktree_state(&self, _mount_id: Uuid) -> Result<WorktreeStateResponse, ServiceError> {
        Err(ServiceError::Unsupported(
            "worktree state is not implemented by this Antares service".into(),
        ))
    }

    /// Bind a clean mount to the immutable revision selected by the VCS owner.
    async fn bind_worktree_base(
        &self,
        _mount_id: Uuid,
        _request: BindWorktreeBaseRequest,
    ) -> Result<WorktreeStateResponse, ServiceError> {
        Err(ServiceError::Unsupported(
            "worktree base binding is not implemented by this Antares service".into(),
        ))
    }

    /// Produce a non-mutating preflight for a future lower-base switch.
    async fn plan_worktree_refresh(
        &self,
        _mount_id: Uuid,
        _request: RefreshPlanRequest,
    ) -> Result<RefreshPlanResponse, ServiceError> {
        Err(ServiceError::Unsupported(
            "worktree refresh planning is not implemented by this Antares service".into(),
        ))
    }

    /// Derive a new worktree mount from an existing one.
    ///
    /// Default implementation reports the capability as absent, following the same
    /// pattern as the other worktree operations: a client must be able to tell "this
    /// daemon cannot fork" from "this fork failed".
    async fn fork_mount(
        &self,
        _source_mount_id: Uuid,
        _request: ForkMountRequest,
    ) -> Result<ForkMountResponse, ServiceError> {
        Err(ServiceError::Unsupported(
            "fork is not implemented by this Antares service".into(),
        ))
    }

    /// Worktree Control Protocol v2: effective diff, commit finalize, lower switch.
    ///
    /// See `docs/scorpiofs-libra-complete-spec-v1.md` and `worktree_v2.rs` for the
    /// failure modes these operations eliminate over the v1 contract.
    async fn attach_worktree(
        &self,
        _request: AttachWorktreeRequest,
    ) -> Result<AttachWorktreeResponse, ServiceError> {
        Err(ServiceError::Unsupported(
            "worktree v2 attach is not implemented by this Antares service".into(),
        ))
    }

    async fn worktree_state_v2(&self, _mount_id: Uuid) -> Result<WorktreeStateV2, ServiceError> {
        Err(ServiceError::Unsupported(
            "worktree v2 state is not implemented by this Antares service".into(),
        ))
    }

    async fn commit_finalize(
        &self,
        _mount_id: Uuid,
        _request: CommitFinalizeRequest,
    ) -> Result<CommitFinalizeResponse, ServiceError> {
        Err(ServiceError::Unsupported(
            "commit finalize is not implemented by this Antares service".into(),
        ))
    }

    async fn refresh_lower(
        &self,
        _mount_id: Uuid,
        _request: RefreshRequest,
    ) -> Result<RefreshResponse, ServiceError> {
        Err(ServiceError::Unsupported(
            "lower refresh is not implemented by this Antares service".into(),
        ))
    }

    async fn health_info(&self) -> HealthResponse;
    async fn shutdown_cleanup(&self) -> Result<(), ServiceError>;
}

/// Request payload for provisioning a new mount.
/// Simplified API: only requires the monorepo path and optional CL identifier.
/// All internal paths (mountpoint, upper_dir, cl_dir) are auto-generated.
///
/// # Path Generation
/// Paths are auto-generated using UUID-based naming under configured root directories:
/// - `mountpoint`: `{antares_mount_root}/{uuid}` (e.g., `/var/lib/antares/mounts/550e8400-e29b-41d4-a716-446655440000`)
/// - `upper_dir`: `{antares_upper_root}/{uuid}` (e.g., `/var/lib/antares/upper/550e8400-e29b-41d4-a716-446655440000`)
/// - `cl_dir`: `{antares_cl_root}/{uuid}` (only if `cl` is provided)
///
/// The UUID is generated per mount request, ensuring unique paths for each mount instance.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateMountRequest {
    /// Optional build task identifier (job-level mount). When provided, Antares will treat
    /// mount creation as idempotent for the same task id.
    ///
    /// This is preferred in build systems to bind mount lifecycle to a task.
    #[serde(default)]
    pub job_id: Option<String>,
    /// Optional alternative task identifier (build-level). If both `job_id` and `build_id`
    /// are provided, `job_id` takes precedence.
    #[serde(default)]
    pub build_id: Option<String>,
    /// Monorepo path to mount (e.g., "/third-party/mega")
    pub path: String,
    /// Repository path represented by CL API file entries. Defaults to `path`
    /// for backwards compatibility.
    #[serde(default)]
    pub cl_path: Option<String>,
    /// Optional CL (changelist) identifier for the CL layer
    #[serde(default)]
    pub cl: Option<String>,
    /// Optional absolute filesystem mountpoint. When omitted, Antares allocates
    /// one under `antares_mount_root`; when present, this directory is mounted
    /// directly. The caller must provide an empty directory.
    #[serde(default)]
    pub mountpoint: Option<String>,
    /// **Internal only — not part of the HTTP contract.** A pre-populated upper
    /// directory to adopt instead of generating one.
    ///
    /// `fork` needs this because an upper layer is only imported into the FUSE layer
    /// when the session starts: a delta written into the upper *after* mounting is
    /// invisible through the mount (and can even make writes fail with `EEXIST`),
    /// while `GET /worktree` — which scans the directory directly — reports it,
    /// leaving the API and the filesystem disagreeing. So the child's delta has to be
    /// on disk before the mount exists.
    ///
    /// `skip_deserializing` is load-bearing: if a client could set this, mount
    /// creation would become arbitrary directory creation, and the failure paths call
    /// `remove_dir_all` on it.
    #[serde(default, skip_serializing, skip_deserializing)]
    pub upper_dir: Option<String>,
    /// **Internal only — not part of the HTTP contract.** Pin the mount's Dicfuse
    /// lower to this Mega *internal* commit OID instead of the moving trunk tip.
    ///
    /// The Worktree-v2 attach sets it so the projection is immutable from the first
    /// request; v1 mounts stay unpinned. Like `upper_dir`, `skip_deserializing` is
    /// load-bearing — a client able to pin an arbitrary revision would be able to
    /// serve content the mount's owner never asked for.
    #[serde(default, skip_serializing, skip_deserializing)]
    pub pinned_refs: Option<String>,
    /// **Internal only.** Sealed chain layers for a `chain`-fork child, nearest
    /// first (host paths). Same `skip_deserializing` rationale as `pinned_refs`:
    /// a client able to stack arbitrary directories below its view would read
    /// content its mount never projected.
    #[serde(default, skip_serializing, skip_deserializing)]
    pub sealed_chain: Vec<String>,
}

/// Request payload for building/rebuilding a CL layer.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BuildClRequest {
    /// CL (changelist) link identifier
    pub cl: String,
}

/// Response returned after mount creation succeeds.
/// Only contains the essential information the caller needs.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MountCreated {
    /// Unique identifier for this mount
    pub mount_id: Uuid,
    /// The actual filesystem path where the mount is accessible
    pub mountpoint: String,
}

/// Snapshot of a single mount's state.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MountStatus {
    pub mount_id: Uuid,
    /// Optional build task identifier (job/build id) associated with this mount.
    #[serde(default)]
    pub job_id: Option<String>,
    /// The monorepo path being mounted
    pub path: String,
    /// Optional CL identifier
    pub cl: Option<String>,
    /// Commit/revision selected by Libra for an interactive worktree mount.
    #[serde(default)]
    pub base_revision: Option<String>,
    /// The actual filesystem mountpoint
    pub mountpoint: String,
    pub layers: MountLayers,
    pub state: MountLifecycle,
    pub created_at_epoch_ms: u64,
    pub last_seen_epoch_ms: u64,
}

/// Convenience wrapper used by list endpoints.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MountCollection {
    pub mounts: Vec<MountStatus>,
}

/// Directory layout for a mount.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MountLayers {
    pub upper: String,
    pub cl: Option<String>,
    pub dicfuse: String,
}

/// Lifecycle indicator used in responses and service contracts.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum MountLifecycle {
    Provisioning,
    Mounted,
    /// Dicfuse directory tree has been fully pre-loaded; safe for heavy I/O
    /// workloads (e.g. buck2 builds) that would otherwise trigger a statx storm
    /// against cold caches.
    Ready,
    /// Mount is entering CL switch window; new control-plane operations should
    /// be rejected until remount finishes.
    Quiescing,
    Unmounting,
    Unmounted,
    Failed {
        reason: String,
    },
}

/// Response for the `/mounts/{mount_id}/ready` readiness probe.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MountReadyResponse {
    pub mount_id: Uuid,
    /// `true` when Phase 1 (Dicfuse memory cache warmup) has completed.
    /// Phase 2 kernel-cache warmup may still be running in background.
    pub ready: bool,
    /// Current lifecycle state of the mount.
    pub state: MountLifecycle,
}

/// Response for the `/mounts/{mount_id}/changes` endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MountChangesResponse {
    pub mount_id: Uuid,
    /// Stable fingerprint of the current changed-path set.
    pub generation: u64,
    pub changes: Vec<ChangedPath>,
}

/// Where a worktree sits inside its VCS repository, so that a process running *in*
/// the mount can find the repository it belongs to.
///
/// Deliberately tiny. The authoritative state (HEAD, index, refs, objects) lives
/// host-side under `commondir`, keyed by `worktree_id`, and must never be
/// materialized in the upper layer: what gets written into a mount is a *pointer*,
/// not a repository. ScorpioFS does not interpret either value.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct VcsPointer {
    /// Absolute path of the repository's shared state directory. The caller owns
    /// canonicalization — the value is written verbatim, and a VCS that compares it
    /// against its own canonical storage will reject a messy one.
    pub commondir: String,
    /// Stable identifier of this worktree within that repository.
    pub worktree_id: String,
}

/// Request used by Libra immediately after attaching a clean worktree mount.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BindWorktreeBaseRequest {
    /// Immutable commit/revision that Dicfuse is expected to project for this mount.
    pub base_revision: String,
    /// Optional. When present, the pointer files are written into the mount.
    ///
    /// Optional so an older client keeps working unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcs_pointer: Option<VcsPointer>,
}

/// Current ScorpioFS contribution to a Git-compatible worktree state.
///
/// HEAD, index, refs, commits, and conflict stages remain owned by Libra. The
/// response only reports the pinned lower-base identity and upper-layer delta.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorktreeStateResponse {
    pub mount_id: Uuid,
    pub path: String,
    pub base_revision: Option<String>,
    pub mount_state: MountLifecycle,
    pub dirty: bool,
    pub changes: MountChangesResponse,
}

/// Non-mutating request for deciding whether a VCS owner may switch bases.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RefreshPlanRequest {
    /// Optimistic lock: the revision Libra believes is mounted now.
    pub expected_base_revision: String,
    /// Resolved target revision. ScorpioFS does not resolve refs itself.
    pub target_revision: String,
    /// Reject dirty worktrees. Defaults to the safe Git-compatible behavior.
    #[serde(default = "default_require_clean")]
    pub require_clean: bool,
}

fn default_require_clean() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RefreshPlanDisposition {
    Ready,
    AlreadyAtTarget,
    Unbound,
    BaseMismatch,
    BlockedDirty,
}

/// Result of a refresh preflight. This endpoint never switches the FUSE lower layer.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RefreshPlanResponse {
    pub mount_id: Uuid,
    pub current_base_revision: Option<String>,
    pub target_revision: String,
    pub disposition: RefreshPlanDisposition,
    pub worktree: WorktreeStateResponse,
}

/// A path represented in the private writable upper layer.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChangedPath {
    pub kind: ChangeKind,
    /// Normalized path relative to the Antares mount root.
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Modified,
    Deleted,
}

/// How `fork` should produce the child worktree's layers.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ForkMode {
    /// Copy the parent's writable delta into a fresh upper layer. The child ends up
    /// with its own independent layers and the parent keeps working. Cost is
    /// proportional to the parent's *delta*, not to the repository size.
    #[default]
    Materialize,
    /// Share a frozen copy of the parent's upper layer as a lower layer instead of
    /// copying it, for an O(1) fork. **Not implemented**: a request is downgraded to
    /// `Materialize` and the response reports `mode_downgraded_from`, so a caller is
    /// never silently given a different mechanism than the one it asked for.
    Chain,
}

/// Request to derive a new worktree mount from an existing one.
///
/// The monorepo path is inherited from the source: a fork is a second worktree over
/// the *same* subtree, so there is nothing to choose. The child's mountpoint is
/// generated by the daemon, exactly as on `POST /mounts`; read it back from the
/// response (`path`) or from `GET /mounts/{id}`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ForkMountRequest {
    /// Optional task identifier, as on mount creation.
    #[serde(default)]
    pub job_id: Option<String>,
    /// Optional target mountpoint. When omitted, ScorpioFS allocates one below
    /// its configured mount root; Libra supplies the linked worktree path.
    #[serde(default)]
    pub mountpoint: Option<String>,
    /// Layer strategy. Defaults to `materialize`.
    #[serde(default)]
    pub mode: ForkMode,
    /// Carry the source's CL layer into the child. Off by default: a CL is a build
    /// baseline, not a worktree edit (see `docs/worktree-state-transitions.md`).
    #[serde(default)]
    pub inherit_cl: bool,
}

/// Result of a `fork`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ForkMountResponse {
    pub mount_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    pub path: String,
    /// The worktree this one was derived from.
    pub source_mount_id: Uuid,
    /// The mode actually used.
    pub mode: ForkMode,
    /// Set when the requested mode could not be honoured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode_downgraded_from: Option<ForkMode>,
    /// Lower layers, **nearest first**. Clients must not reorder this.
    pub lower_chain: Vec<String>,
    /// The source's base binding, inherited. `None` until `POST .../worktree/base`.
    pub base_revision: Option<String>,
    pub mount_state: MountLifecycle,
    /// Always `None` under `materialize`; reserved for `chain`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_frozen_layer: Option<String>,
    /// What the delta copy actually did. `None` under `chain`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copy_stats: Option<ForkCopyStats>,
}

/// Health check response payload.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthResponse {
    /// Version of the VCS worktree control protocol.
    pub protocol_version: u32,
    /// Stable service identifier used during capability negotiation.
    pub service: String,
    /// ScorpioFS package version.
    pub service_version: Option<String>,
    /// Versioned control-plane capabilities supported by this service.
    pub capabilities: Vec<String>,
    /// Service health status: "healthy" or "degraded"
    pub status: String,
    /// Current number of active mounts
    pub mount_count: usize,
    /// Service uptime in seconds
    pub uptime_secs: u64,
}

/// Error response body for JSON output.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ErrorBody {
    /// Human-readable error message
    pub error: String,
    /// Machine-readable error code
    pub code: String,
}

/// Service-level failures (implementation specific) that surface through the API.
#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("mount not found: {0}")]
    NotFound(Uuid),
    #[error("mount not found for task id: {0}")]
    NotFoundTask(String),
    #[error("failed to interact with fuse stack: {0}")]
    FuseFailure(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error("unexpected error: {0}")]
    Internal(String),
}

/// HTTP-facing errors mapped to responses.
#[derive(Debug, Error)]
pub enum ApiError {
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error("serde payload rejected: {0}")]
    BadPayload(String),
    #[error("server shutting down")]
    Shutdown,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status_code, error_code, message) = match &self {
            ApiError::Service(ServiceError::InvalidRequest(msg)) => {
                (StatusCode::BAD_REQUEST, "INVALID_REQUEST", msg.clone())
            }
            ApiError::Service(ServiceError::NotFound(id)) => (
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                format!("mount {} not found", id),
            ),
            ApiError::Service(ServiceError::NotFoundTask(task)) => (
                StatusCode::NOT_FOUND,
                "NOT_FOUND",
                format!("mount for task {} not found", task),
            ),
            ApiError::Service(ServiceError::FuseFailure(msg)) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "FUSE_ERROR", msg.clone())
            }
            ApiError::Service(ServiceError::Unsupported(msg)) => {
                (StatusCode::NOT_IMPLEMENTED, "UNSUPPORTED", msg.clone())
            }
            ApiError::Service(ServiceError::Internal(msg)) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                msg.clone(),
            ),
            ApiError::BadPayload(msg) => (StatusCode::BAD_REQUEST, "BAD_PAYLOAD", msg.clone()),
            ApiError::Shutdown => (
                StatusCode::SERVICE_UNAVAILABLE,
                "SHUTDOWN",
                "server is shutting down".into(),
            ),
        };

        let body = ErrorBody {
            error: message,
            code: error_code.to_string(),
        };

        (status_code, Json(body)).into_response()
    }
}

// ============================================================================
// Service Implementation
// ============================================================================

/// Internal entry tracking a single mount.
struct MountEntry {
    mount_id: Uuid,
    /// Optional build task identifier (job/build id) associated with this mount.
    job_id: Option<String>,
    /// The monorepo path being mounted
    path: String,
    /// Repository path represented by CL API file entries.
    cl_path: Option<String>,
    /// Optional CL identifier
    cl: Option<String>,
    /// Immutable revision selected by Libra for an interactive worktree.
    base_revision: Option<String>,
    /// Mega *internal* commit OID the Dicfuse lower is pinned to (from
    /// `/api/v1/latest-commit`). `None` = legacy mount whose lower tracks the
    /// moving trunk tip. Distinct from `base_revision`, which lives in the VCS
    /// client's identifier space (a git commit OID).
    pinned_refs: Option<String>,
    /// Sealed delta layers from `chain` forks, **nearest first** (host directory
    /// paths). They shadow the Dicfuse projection below them and are flattened
    /// into the upper at the next finalize/refresh.
    sealed_chain: Vec<String>,
    /// Auto-generated mountpoint path
    mountpoint: String,
    /// Auto-generated upper directory
    upper_dir: String,
    /// Auto-generated CL directory (if cl is provided)
    cl_dir: Option<String>,
    fuse: AntaresFuse,
    /// The MST/2 snapshot view backing this mount's lower projection, when the
    /// mount was created with `mst2_lower_enabled` (spec 12 §1). Not persisted:
    /// a restarted daemon refuses to restore such a mount rather than silently
    /// serving the Dicfuse projection instead (spec 15 §3 — explicit modes, no
    /// silent fallback).
    mst2_lower: Option<Arc<Mst2Fuse>>,
    state: MountLifecycle,
    created_at_epoch_ms: u64,
    last_seen_epoch_ms: u64,
    /// Signal for the background deep-preload task to stop early (e.g. on unmount).
    preload_cancel: Arc<AtomicBool>,
}

#[derive(Debug, Deserialize)]
struct CommonResult<T> {
    req_result: bool,
    data: Option<T>,
    err_message: String,
}

#[derive(Debug, Deserialize)]
struct ClFileEntry {
    path: String,
    sha: String,
    action: String,
}

impl MountEntry {
    /// Convert to public MountStatus for API responses.
    fn to_status(&self) -> MountStatus {
        MountStatus {
            mount_id: self.mount_id,
            job_id: self.job_id.clone(),
            path: self.path.clone(),
            cl: self.cl.clone(),
            base_revision: self.base_revision.clone(),
            mountpoint: self.mountpoint.clone(),
            layers: MountLayers {
                upper: self.upper_dir.clone(),
                cl: self.cl_dir.clone(),
                dicfuse: "shared".to_string(),
            },
            state: self.state.clone(),
            created_at_epoch_ms: self.created_at_epoch_ms,
            last_seen_epoch_ms: self.last_seen_epoch_ms,
        }
    }

    /// Update the last_seen timestamp.
    fn update_last_seen(&mut self) {
        self.last_seen_epoch_ms = current_epoch_ms();
    }
}

/// Directory inside a mount that holds the VCS pointer.
const VCS_POINTER_DIR: &str = ".libra";

/// Write (or verify) the VCS pointer inside a mount.
///
/// Idempotent for identical content, and refuses to silently rewrite a *different*
/// identity: two worktrees claiming the same directory would be worse than an error.
///
/// The two files are written into the mount, so they land in the writable upper layer.
/// `scan_layer_changes` skips this directory, so the pointer never shows up as a local
/// change — which is what lets the VCS layer treat the worktree as clean.
fn write_vcs_pointer(mountpoint: &Path, pointer: &VcsPointer) -> Result<(), ServiceError> {
    let commondir = pointer.commondir.trim();
    let worktree_id = pointer.worktree_id.trim();

    if commondir.is_empty() || worktree_id.is_empty() {
        return Err(ServiceError::InvalidRequest(
            "vcs_pointer.commondir and vcs_pointer.worktree_id cannot be empty".into(),
        ));
    }
    // One value per line. An embedded newline would forge an extra line rather than
    // describing a path.
    if commondir.contains('\n') || worktree_id.contains('\n') {
        return Err(ServiceError::InvalidRequest(
            "vcs_pointer values must not contain newlines".into(),
        ));
    }
    if !Path::new(commondir).is_absolute() {
        return Err(ServiceError::InvalidRequest(
            "vcs_pointer.commondir must be an absolute path".into(),
        ));
    }

    let dir = mountpoint.join(VCS_POINTER_DIR);
    std::fs::create_dir_all(&dir)
        .map_err(|e| ServiceError::Internal(format!("failed to create {}: {e}", dir.display())))?;

    for (name, value) in [("commondir", commondir), ("worktree_id", worktree_id)] {
        let path = dir.join(name);
        let wanted = format!("{value}\n");
        match std::fs::read_to_string(&path) {
            // Already correct: a repeated bind is a no-op.
            Ok(existing) if existing == wanted => continue,
            Ok(existing) => {
                return Err(ServiceError::InvalidRequest(format!(
                    "worktree pointer {} already reads {:?}; refusing to rewrite it to {:?}",
                    path.display(),
                    existing.trim_end(),
                    value
                )));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(ServiceError::Internal(format!(
                    "failed to read {}: {e}",
                    path.display()
                )));
            }
        }
        std::fs::write(&path, wanted).map_err(|e| {
            ServiceError::Internal(format!("failed to write {}: {e}", path.display()))
        })?;
    }

    Ok(())
}

/// OCI overlay whiteout prefix (libfuse-fs default on macOS).
const OCI_WHITEOUT_PREFIX: &str = ".wh.";
/// OCI opaque-directory marker; not a per-file delete.
const OCI_OPAQUE_MARKER: &str = ".wh..wh..opq";

#[cfg(any(test, target_os = "macos"))]
fn oci_whiteout_path(path: &Path) -> PathBuf {
    let name = path.file_name().unwrap_or_default();
    let mut marked = OsString::from(OCI_WHITEOUT_PREFIX);
    marked.push(name);
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(marked),
        _ => PathBuf::from(marked),
    }
}

fn classify_layer_entry(
    file_type: std::fs::FileType,
    relative: &Path,
) -> Option<(ChangeKind, String)> {
    let name = relative.file_name()?.to_str()?;
    if name == OCI_OPAQUE_MARKER {
        return None;
    }
    if let Some(hidden) = name.strip_prefix(OCI_WHITEOUT_PREFIX) {
        let mut logical = relative.to_path_buf();
        logical.set_file_name(hidden);
        return Some((ChangeKind::Deleted, logical.to_str()?.to_string()));
    }
    let path = relative.to_str()?.to_string();
    if file_type.is_char_device() {
        Some((ChangeKind::Deleted, path))
    } else {
        Some((ChangeKind::Modified, path))
    }
}

/// Get current time as milliseconds since UNIX epoch.
fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn scan_layer_changes(
    layer_dir: &Path,
    changes: &mut BTreeMap<String, ChangedPath>,
) -> Result<(), ServiceError> {
    let mut pending = vec![layer_dir.to_path_buf()];

    while let Some(directory) = pending.pop() {
        let entries = std::fs::read_dir(&directory).map_err(|error| {
            ServiceError::Internal(format!(
                "failed to read Antares upper directory {:?}: {}",
                directory, error
            ))
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                ServiceError::Internal(format!(
                    "failed to read an entry from Antares upper directory {:?}: {}",
                    directory, error
                ))
            })?;
            let path = entry.path();
            let relative = path.strip_prefix(layer_dir).map_err(|error| {
                ServiceError::Internal(format!(
                    "Antares layer entry {:?} escaped layer directory {:?}: {}",
                    path, layer_dir, error
                ))
            })?;
            if relative
                .components()
                .next()
                .is_some_and(|component| component.as_os_str() == ".libra")
            {
                continue;
            }

            let file_type = entry.file_type().map_err(|error| {
                ServiceError::Internal(format!(
                    "failed to inspect Antares upper entry {:?}: {}",
                    path, error
                ))
            })?;
            if file_type.is_dir() {
                pending.push(path);
                continue;
            }

            let Some((kind, logical_path)) = classify_layer_entry(file_type, relative) else {
                continue;
            };
            let changed = ChangedPath {
                kind,
                path: logical_path,
                source_path: None,
            };
            changes.insert(changed.path.clone(), changed);
        }
    }
    Ok(())
}

fn scan_mount_changes(
    mount_id: Uuid,
    upper_dir: &Path,
    cl_dir: Option<&Path>,
) -> Result<MountChangesResponse, ServiceError> {
    let mut by_path = BTreeMap::new();
    if let Some(cl_dir) = cl_dir {
        scan_layer_changes(cl_dir, &mut by_path)?;
    }
    scan_layer_changes(upper_dir, &mut by_path)?;
    let changes: Vec<_> = by_path.into_values().collect();

    let mut generation = 0xcbf29ce484222325_u64;
    for change in &changes {
        generation ^= match change.kind {
            ChangeKind::Modified => 1,
            ChangeKind::Deleted => 2,
        };
        generation = generation.wrapping_mul(0x100000001b3);
        for byte in change.path.as_bytes() {
            generation ^= u64::from(*byte);
            generation = generation.wrapping_mul(0x100000001b3);
        }
    }

    Ok(MountChangesResponse {
        mount_id,
        generation,
        changes,
    })
}

/// Type alias for path index: maps (monorepo_path, optional_cl, cl_path) to mount_id.
type PathIndex = Arc<RwLock<HashMap<(String, Option<String>, Option<String>), Uuid>>>;
/// Type alias for job index: maps a build task id (job_id/build_id) to mount_id.
type JobIndex = Arc<RwLock<HashMap<String, Uuid>>>;

/// Persisted mount state for recovery across restarts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedMountState {
    pub mount_id: Uuid,
    #[serde(default)]
    pub job_id: Option<String>,
    pub path: String,
    #[serde(default)]
    pub cl_path: Option<String>,
    pub cl: Option<String>,
    #[serde(default)]
    pub base_revision: Option<String>,
    /// Mega internal commit OID the Dicfuse lower is pinned to.
    #[serde(default)]
    pub pinned_refs: Option<String>,
    /// Sealed delta layers, nearest first (host paths). Flattened away at the
    /// next finalize/refresh; persisted so recovery rebuilds the same view.
    #[serde(default)]
    pub sealed_chain: Vec<String>,
    pub mountpoint: String,
    pub upper_dir: String,
    pub cl_dir: Option<String>,
    pub created_at_epoch_ms: u64,
    /// Whether this mount's lower projection is an MST/2 snapshot view. Such a
    /// mount is not restored across restarts: the view is resolved per process,
    /// and rebuilding it with the Dicfuse projection instead would silently
    /// serve a different base (spec 15 §3).
    #[serde(default)]
    pub mst2_lower: bool,
}

/// Persisted state file structure.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistedState {
    pub mounts: Vec<PersistedMountState>,
}

/// Selects which process owns durable mount lifecycle state.
///
/// `ScorpioFs` preserves the standalone daemon behavior. `External` is for an
/// embedding controller such as Libra: ScorpioFS keeps only the live FUSE
/// handles and never writes or recovers a state file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StateOwnership {
    #[default]
    ScorpioFs,
    External,
}

/// Concrete implementation of AntaresService.
pub struct AntaresServiceImpl {
    /// Shared Dicfuse instance for root path (read-only base layer).
    dicfuse: Arc<Dicfuse>,
    /// Cache of Dicfuse instances keyed by base_path for subdirectory mounts.
    /// This avoids creating duplicate instances for the same path.
    dicfuse_cache: Arc<RwLock<HashMap<String, Arc<Dicfuse>>>>,
    /// Active mounts indexed by UUID.
    mounts: Arc<RwLock<HashMap<Uuid, MountEntry>>>,
    /// Fast lookup for (path, cl) -> mount_id to avoid linear scans.
    path_index: PathIndex,
    /// Fast lookup for (job_id/build_id) -> mount_id for task-granularity mounts.
    job_index: JobIndex,
    /// Service start time for uptime calculation.
    start_time: Instant,
    /// Path to the state file for persistence.
    state_file: PathBuf,
    /// Durable state owner. External controllers must remain the sole writer.
    state_ownership: StateOwnership,
}

impl AntaresServiceImpl {
    /// Create a new service instance.
    ///
    /// # Arguments
    /// * `dicfuse` - Optional shared Dicfuse instance. If None, creates a new one.
    ///
    /// # Note
    /// Requires config to be initialized via `config::init_config()` before calling.
    pub async fn new(dicfuse: Option<Arc<Dicfuse>>) -> Self {
        Self::new_with_state_ownership(dicfuse, StateOwnership::ScorpioFs).await
    }

    /// Create a service whose durable desired state is owned by the embedding
    /// controller. The service never writes or recovers `antares_state_file`.
    pub async fn new_external_state(dicfuse: Option<Arc<Dicfuse>>) -> Self {
        Self::new_with_state_ownership(dicfuse, StateOwnership::External).await
    }

    async fn new_with_state_ownership(
        dicfuse: Option<Arc<Dicfuse>>,
        state_ownership: StateOwnership,
    ) -> Self {
        let dic = match dicfuse {
            Some(d) => d,
            None => DicfuseManager::global().await,
        };
        // Trigger import as early as possible so directory tree loading begins
        // before any mount requests arrive. Idempotent: no-op if already started.
        dic.start_import();
        let state_file = PathBuf::from(crate::util::config::antares_state_file());
        Self {
            dicfuse: dic,
            dicfuse_cache: Arc::new(RwLock::new(HashMap::new())),
            mounts: Arc::new(RwLock::new(HashMap::new())),
            path_index: Arc::new(RwLock::new(HashMap::new())),
            job_index: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
            state_file,
            state_ownership,
        }
    }

    /// Create a new service instance and recover previous mounts if available.
    ///
    /// # Arguments
    /// * `dicfuse` - Optional shared Dicfuse instance. If None, creates a new one.
    ///
    /// # Note
    /// Requires config to be initialized via `config::init_config()` before calling.
    pub async fn new_with_recovery(dicfuse: Option<Arc<Dicfuse>>) -> Self {
        let instance = Self::new(dicfuse).await;
        instance.recover_mounts().await;
        instance
    }

    fn normalize_mount_path(path: &str) -> String {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return String::new();
        }
        if trimmed == "/" {
            return "/".to_string();
        }
        let mut normalized = if trimmed.starts_with('/') {
            trimmed.to_string()
        } else {
            format!("/{trimmed}")
        };
        normalized = normalized.trim_end_matches('/').to_string();
        if normalized.is_empty() {
            "/".to_string()
        } else {
            normalized
        }
    }

    fn normalize_abs_path(path: &str) -> String {
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return "/".to_string();
        }
        if trimmed == "/" {
            return "/".to_string();
        }
        let mut normalized = if trimmed.starts_with('/') {
            trimmed.to_string()
        } else {
            format!("/{trimmed}")
        };
        normalized = normalized.trim_end_matches('/').to_string();
        if normalized.is_empty() {
            "/".to_string()
        } else {
            normalized
        }
    }

    fn relative_path_for_mount(entry_path: &str, mount_path: &str) -> Option<PathBuf> {
        let raw_entry = entry_path.trim();
        if raw_entry.is_empty() {
            return None;
        }

        let mount = Self::normalize_abs_path(mount_path);
        if !raw_entry.starts_with('/') {
            // The CL API returns paths relative to the repository selected by
            // mount_path. Keep entries that already carry the mount prefix
            // compatible with older servers, otherwise use them as-is.
            let mount_prefix = mount.trim_start_matches('/');
            let rel = raw_entry
                .strip_prefix(mount_prefix)
                .and_then(|path| path.strip_prefix('/'))
                .unwrap_or(raw_entry);
            return Self::validated_relative_path(rel);
        }

        let entry = Self::normalize_abs_path(raw_entry);
        if mount == "/" {
            let rel = entry.trim_start_matches('/');
            if rel.is_empty() {
                return None;
            }
            return Self::validated_relative_path(rel);
        }

        let prefix = format!("{}/", mount);
        let rel = entry.strip_prefix(&prefix)?;
        Self::validated_relative_path(rel)
    }

    fn validated_relative_path(rel: &str) -> Option<PathBuf> {
        let rel_path = Path::new(rel);
        let components = rel_path.components();
        for component in components {
            match component {
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return None;
                }
                Component::CurDir | Component::Normal(_) => {}
            }
        }
        Some(rel_path.to_path_buf())
    }

    fn http_client() -> Result<Client, ServiceError> {
        Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ServiceError::Internal(format!("failed to build http client: {}", e)))
    }

    fn cl_quiesce_grace_duration() -> Duration {
        const DEFAULT_MS: u64 = 150;
        match std::env::var("ANTARES_CL_QUIESCE_GRACE_MS") {
            Ok(raw) => match raw.trim().parse::<u64>() {
                Ok(ms) => Duration::from_millis(ms.clamp(0, 3_000)),
                Err(_) => {
                    tracing::warn!(
                        value = %raw,
                        default_ms = DEFAULT_MS,
                        "invalid ANTARES_CL_QUIESCE_GRACE_MS, using default"
                    );
                    Duration::from_millis(DEFAULT_MS)
                }
            },
            Err(_) => Duration::from_millis(DEFAULT_MS),
        }
    }

    /// Spawn a background deep-preload walk to warm FUSE kernel entry/attr caches.
    ///
    /// This is a **best-effort optimisation** — it does NOT block the mount from
    /// being marked `Ready`.  By the time we call this, Dicfuse's internal
    /// `load_dir_depth()` (Phase 1) has already populated the in-memory directory
    /// cache.  Without this walk, FUSE `statx` calls still reach the daemon but
    /// hit the Dicfuse memory cache (~1 ms each).  The walk pushes those entries
    /// into the Linux kernel's FUSE cache so subsequent `statx` calls are served
    /// at ~0 ms — a nice-to-have, not a prerequisite for correctness.
    fn spawn_deep_preload_task(
        &self,
        mount_id: Uuid,
        mountpoint: String,
        cancel: Arc<AtomicBool>,
        source: &'static str,
    ) {
        tokio::spawn(async move {
            let start = Instant::now();
            tracing::info!(
                mount_id = %mount_id,
                mountpoint = %mountpoint,
                source = source,
                "antares svc: starting background kernel cache warm (best-effort)"
            );

            let mp = mountpoint.clone();
            let walk_result =
                tokio::task::spawn_blocking(move || deep_preload_walk(&mp, &cancel)).await;

            match walk_result {
                Ok(Ok(stats)) => {
                    tracing::info!(
                        mount_id = %mount_id,
                        source = source,
                        entries_visited = stats.entries_visited,
                        metadata_touches = stats.metadata_touches,
                        budget_exhausted = stats.budget_exhausted,
                        elapsed_ms = start.elapsed().as_millis(),
                        "antares svc: kernel cache warm completed"
                    );
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        mount_id = %mount_id,
                        source = source,
                        error = %e,
                        elapsed_ms = start.elapsed().as_millis(),
                        "antares svc: kernel cache warm finished with errors"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        mount_id = %mount_id,
                        source = source,
                        error = %e,
                        "antares svc: kernel cache warm task panicked"
                    );
                }
            }
        });
    }

    async fn fetch_cl_files(&self, cl_link: &str) -> Result<Vec<ClFileEntry>, ServiceError> {
        let base_url = crate::util::config::base_url();
        let url = format!("{base_url}/api/v1/cl/{cl_link}/files-list");
        let client = Self::http_client()?;
        let resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| ServiceError::Internal(format!("failed to fetch CL files: {}", e)))?;
        if !resp.status().is_success() {
            return Err(ServiceError::Internal(format!(
                "failed to fetch CL files: HTTP {}",
                resp.status()
            )));
        }
        let body: CommonResult<Vec<ClFileEntry>> = resp.json().await.map_err(|e| {
            ServiceError::Internal(format!("failed to parse CL files response: {}", e))
        })?;
        if !body.req_result {
            return Err(ServiceError::Internal(format!(
                "CL files response error: {}",
                body.err_message
            )));
        }
        Ok(body.data.unwrap_or_default())
    }

    async fn download_blob_to_path(
        &self,
        client: &Client,
        oid: &str,
        dest: &Path,
    ) -> Result<(), ServiceError> {
        let base_url = crate::util::config::base_url();
        let clean_oid = oid.trim_start_matches("sha1:");
        let url = format!("{base_url}/api/v1/file/blob/{clean_oid}");
        let resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| ServiceError::Internal(format!("failed to download blob: {}", e)))?;
        if !resp.status().is_success() {
            return Err(ServiceError::Internal(format!(
                "failed to download blob {}: HTTP {}",
                clean_oid,
                resp.status()
            )));
        }

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ServiceError::Internal(format!(
                    "failed to create CL parent dir {:?}: {}",
                    parent, e
                ))
            })?;
        }

        let mut file = tokio::fs::File::create(dest).await.map_err(|e| {
            ServiceError::Internal(format!("failed to create file {:?}: {}", dest, e))
        })?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ServiceError::Internal(format!("failed to read blob data: {}", e)))?;
        file.write_all(&bytes).await.map_err(|e| {
            ServiceError::Internal(format!("failed to write file {:?}: {}", dest, e))
        })?;
        Ok(())
    }

    /// Remove the per-mount directories of an instance that has been unmounted
    /// for good. Every mount gets a fresh UUID, so nothing can reattach to
    /// these paths afterwards; leaving them behind leaks disk and litters
    /// `antares_mount_root` (visible on the host when the root is bind-mounted).
    ///
    /// The mountpoint is removed with `remove_dir`, never `remove_dir_all`: it
    /// must be an empty directory once the FUSE session is detached, so if a
    /// mount is unexpectedly still attached (`EBUSY` / `ENOTEMPTY`) we refuse to
    /// touch it rather than delete user-visible files through the mount. The
    /// private upper/CL layers are plain directories owned by this instance and
    /// are removed recursively. Failures are logged, not fatal.
    fn remove_mount_dirs(
        mount_id: Uuid,
        mountpoint: &Path,
        upper_dir: &Path,
        cl_dir: Option<&Path>,
    ) {
        if let Err(e) = std::fs::remove_dir(mountpoint) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    mount_id = %mount_id,
                    mountpoint = %mountpoint.display(),
                    error = %e,
                    "antares svc: mountpoint directory left in place after unmount"
                );
            }
        }
        for (layer, dir) in [("upper_dir", Some(upper_dir)), ("cl_dir", cl_dir)] {
            let Some(dir) = dir else { continue };
            match std::fs::remove_dir_all(dir) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!(
                    mount_id = %mount_id,
                    layer,
                    dir = %dir.display(),
                    error = %e,
                    "antares svc: failed to remove layer directory after unmount"
                ),
            }
        }
    }

    fn create_whiteout(path: &Path) -> Result<(), ServiceError> {
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                return Err(ServiceError::Internal(format!(
                    "failed to create whiteout parent {:?}: {}",
                    parent, e
                )));
            }
        }

        if path.exists() {
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_dir_all(path);
        }

        #[cfg(target_os = "macos")]
        {
            let dest = oci_whiteout_path(path);
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&dest)
                .map_err(|e| {
                    ServiceError::Internal(format!(
                        "failed to create OCI whiteout {:?}: {}",
                        dest, e
                    ))
                })?;
            Ok(())
        }

        #[cfg(not(target_os = "macos"))]
        {
            let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|e| {
                ServiceError::Internal(format!("invalid whiteout path {:?}: {}", path, e))
            })?;

            let mode = libc::S_IFCHR;
            let dev = 0;
            let res = unsafe { libc::mknod(c_path.as_ptr(), mode, dev) };
            if res != 0 {
                return Err(ServiceError::Internal(format!(
                    "failed to create whiteout {:?}: {}",
                    path,
                    std::io::Error::last_os_error()
                )));
            }
            Ok(())
        }
    }

    async fn build_cl_layer(
        &self,
        mount_path: &str,
        cl_path: &str,
        cl_link: &str,
        cl_dir: &Path,
    ) -> Result<(), ServiceError> {
        if cl_link.trim().is_empty() {
            return Err(ServiceError::InvalidRequest(
                "cl link cannot be empty".to_string(),
            ));
        }

        if cl_dir.exists() {
            tokio::fs::remove_dir_all(cl_dir).await.map_err(|e| {
                ServiceError::Internal(format!("failed to clear CL dir {:?}: {}", cl_dir, e))
            })?;
        }
        tokio::fs::create_dir_all(cl_dir).await.map_err(|e| {
            ServiceError::Internal(format!("failed to create CL dir {:?}: {}", cl_dir, e))
        })?;

        let files = self.fetch_cl_files(cl_link).await?;
        if files.is_empty() {
            return Ok(());
        }

        let normalized_mount_path = Self::normalize_abs_path(mount_path);
        let normalized_cl_path = Self::normalize_abs_path(cl_path);
        let cl_mount_relative = if normalized_mount_path == normalized_cl_path {
            PathBuf::new()
        } else {
            Self::relative_path_for_mount(&normalized_cl_path, &normalized_mount_path).ok_or_else(
                || {
                    ServiceError::InvalidRequest(format!(
                        "CL repository path `{}` is outside Antares mount path `{}`",
                        cl_path, mount_path
                    ))
                },
            )?
        };

        let client = Self::http_client()?;
        for file in files {
            let repo_relative_path =
                match Self::relative_path_for_mount(&file.path, &normalized_cl_path) {
                    Some(p) => p,
                    None => continue,
                };
            let dest = cl_dir.join(&cl_mount_relative).join(repo_relative_path);
            match file.action.as_str() {
                "new" | "modified" => {
                    self.download_blob_to_path(&client, &file.sha, &dest)
                        .await?;
                }
                "deleted" => {
                    Self::create_whiteout(&dest)?;
                }
                other => {
                    tracing::warn!(
                        "Unknown CL action '{}' for path {}, skipping",
                        other,
                        file.path
                    );
                }
            }
        }

        Ok(())
    }

    /// Get or create a Dicfuse instance for the given path.
    ///
    /// For root path ("/" or empty), returns the shared global instance.
    /// For subdirectory paths, returns a cached instance or creates a new one.
    /// This ensures that multiple mounts with the same base_path share the same
    /// Dicfuse instance, avoiding unnecessary duplication.
    ///
    /// IMPORTANT: For newly created instances, this method waits for the Dicfuse
    /// directory tree to be fully initialized before returning. This prevents
    /// FUSE mount failures due to root inode not being set up yet.
    ///
    /// # TODO(dicfuse-antares-integration)
    /// - Support incremental directory tree loading to reduce initial wait time
    /// - Add progress callback for long-running initialization
    /// - Consider lazy loading for very large subdirectory mounts
    async fn get_or_create_dicfuse(
        &self,
        path: &str,
        pinned_refs: Option<&str>,
    ) -> Result<Arc<Dicfuse>, ServiceError> {
        const INIT_TIMEOUT_SECS: u64 = 120;

        // A pinned mount must never share the path-keyed (unpinned) instances: its
        // projection is a fixed revision, theirs tracks the moving trunk tip.
        if let Some(refs) = pinned_refs {
            let dicfuse = DicfuseManager::for_base_path_and_refs(path, refs).await;
            if tokio::time::timeout(
                Duration::from_secs(INIT_TIMEOUT_SECS),
                dicfuse.store.wait_for_ready(),
            )
            .await
            .is_err()
            {
                return Err(ServiceError::FuseFailure(format!(
                    "pinned Dicfuse for {path} at {refs} did not become ready within \
                     {INIT_TIMEOUT_SECS}s"
                )));
            }
            return Ok(dicfuse);
        }

        // For root path, use the shared global instance (but ensure it's initialized first).
        if path.is_empty() || path == "/" {
            tracing::info!(
                "Waiting for shared Dicfuse instance to initialize for path: / (timeout: {}s)",
                INIT_TIMEOUT_SECS
            );
            match tokio::time::timeout(
                Duration::from_secs(INIT_TIMEOUT_SECS),
                self.dicfuse.store.wait_for_ready(),
            )
            .await
            {
                Ok(_) => {
                    tracing::info!("Shared Dicfuse initialized successfully for path: /");
                }
                Err(_) => {
                    tracing::error!(
                        "Shared Dicfuse initialization timed out for path: / after {}s",
                        INIT_TIMEOUT_SECS
                    );
                    return Err(ServiceError::FuseFailure(format!(
                        "Dicfuse initialization timed out for path '/' after {}s. \
                         Check network connectivity to the monorepo server.",
                        INIT_TIMEOUT_SECS
                    )));
                }
            }
            return Ok(self.dicfuse.clone());
        }

        // Normalize the path for consistent cache keys
        let normalized_path = path.trim_end_matches('/').to_string();

        // Check cache first - if found, it's already initialized
        {
            let cache = self.dicfuse_cache.read().await;
            if let Some(dicfuse) = cache.get(&normalized_path) {
                tracing::debug!(
                    "Using cached Dicfuse instance for path: {}",
                    normalized_path
                );
                return Ok(dicfuse.clone());
            }
        }

        // Not in cache, create new instance
        let new_dicfuse = DicfuseManager::for_base_path(&normalized_path).await;

        // CRITICAL: Wait for the Dicfuse directory tree to be fully loaded before
        // returning. Without this, FUSE mount may fail because the root inode
        // is not set up yet when import_arc hasn't completed.
        // TODO(dicfuse-antares-integration): If many concurrent requests initialize DIFFERENT
        // base paths, we may enqueue a large number of concurrent warmups (network + memory).
        // Consider adding a global semaphore/queue to cap concurrent initializations.
        tracing::info!(
            "Waiting for Dicfuse instance to initialize for path: {} (timeout: {}s)",
            normalized_path,
            INIT_TIMEOUT_SECS
        );
        match tokio::time::timeout(
            std::time::Duration::from_secs(INIT_TIMEOUT_SECS),
            new_dicfuse.store.wait_for_ready(),
        )
        .await
        {
            Ok(_) => {
                tracing::info!(
                    "Dicfuse initialized successfully for path: {}",
                    normalized_path
                );
            }
            Err(_) => {
                tracing::error!(
                    "Dicfuse initialization timed out for path: {} after {}s",
                    normalized_path,
                    INIT_TIMEOUT_SECS
                );
                return Err(ServiceError::FuseFailure(format!(
                    "Dicfuse initialization timed out for path '{}' after {}s. \
                     Check network connectivity to the monorepo server.",
                    normalized_path, INIT_TIMEOUT_SECS
                )));
            }
        }

        // Insert into cache
        {
            let mut cache = self.dicfuse_cache.write().await;
            // Double-check in case another task created it while we were waiting
            if let Some(dicfuse) = cache.get(&normalized_path) {
                return Ok(dicfuse.clone());
            }
            cache.insert(normalized_path.clone(), new_dicfuse.clone());
            tracing::info!(
                "Created and cached new Dicfuse instance for path: {}",
                normalized_path
            );
        }

        Ok(new_dicfuse)
    }

    /// Persist current mount state to file.
    /// Resolve the Dicfuse instance backing a mount's lower projection.
    ///
    /// Pinned mounts get their pinned instance; legacy mounts share the path-keyed
    /// unpinned instance. Callers must NOT cache the result across a finalize: the
    /// pin changes the instance.
    async fn lower_dicfuse_for(
        &self,
        path: &str,
        pinned_refs: Option<&str>,
    ) -> Result<Arc<Dicfuse>, ServiceError> {
        match pinned_refs {
            Some(refs) => {
                let dicfuse = DicfuseManager::for_base_path_and_refs(path, refs).await;
                if tokio::time::timeout(Duration::from_secs(120), dicfuse.store.wait_for_ready())
                    .await
                    .is_err()
                {
                    return Err(ServiceError::FuseFailure(format!(
                        "pinned Dicfuse for {path} at {refs} did not become ready"
                    )));
                }
                Ok(dicfuse)
            }
            None => self.get_or_create_dicfuse(path, None).await,
        }
    }

    /// Verify that a candidate lower actually serves what a VCS commit claims.
    ///
    /// Per-path optimistic lock: the committed hashes must be exactly what the new
    /// lower serves. This proves the push landed AND that no path was concurrently
    /// edited between staging and finalize.
    async fn verify_committed_paths(
        new_store: &DictionaryStore,
        committed: &[CommittedPath],
    ) -> Result<(), String> {
        for path in committed {
            let item = lower_item_for(new_store, &path.path).await;
            match (&path.kind, item) {
                (EffectiveKind::Deleted, None) => {}
                (EffectiveKind::Deleted, Some(item)) => {
                    return Err(format!(
                        "committed deletion of {} does not match the new revision \
                         (lower still serves blob {})",
                        path.path, item.hash
                    ));
                }
                (EffectiveKind::Added | EffectiveKind::Modified, None) => {
                    return Err(format!(
                        "committed path {} is absent from the new revision",
                        path.path
                    ));
                }
                (EffectiveKind::Added | EffectiveKind::Modified, Some(item)) => {
                    if let Some(expected) = &path.content_hash {
                        if item.hash != *expected {
                            return Err(format!(
                                "committed content of {} does not match the new revision \
                                 (commit {}, lower {})",
                                path.path, expected, item.hash
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Rebuild a mount's FUSE session over a new lower projection.
    ///
    /// The caller must have already unmounted the old session. The upper directory
    /// and mountpoint are reused unchanged; only the Dicfuse instance moves.
    async fn remount_with_lower(
        mountpoint: &Path,
        dicfuse: Arc<Dicfuse>,
        upper_dir: &Path,
        cl_dir: Option<&Path>,
        sealed_chain: &[String],
    ) -> Result<AntaresFuse, ServiceError> {
        let frozen = sealed_chain.iter().map(PathBuf::from).collect::<Vec<_>>();
        let mut fuse = AntaresFuse::new(
            mountpoint.to_path_buf(),
            dicfuse,
            upper_dir.to_path_buf(),
            cl_dir.map(PathBuf::from),
        )
        .await
        .and_then(|fuse| fuse.with_frozen_layers(frozen))
        .map_err(|e| ServiceError::FuseFailure(format!("failed to rebuild overlay: {e}")))?;
        fuse.mount()
            .await
            .map_err(|e| ServiceError::FuseFailure(format!("failed to remount: {e}")))?;
        Ok(fuse)
    }

    /// `chain` fork: seal the source's upper into a shared read-only layer, give the
    /// source a fresh upper (its view is byte-identical and it stays writable), and
    /// stack the sealed layer under the child — zero bytes copied.
    ///
    /// Requires the source to be pinned: a sealed layer over a moving trunk tip
    /// would give the child a base with no stable identity. The caller downgrades
    /// unpinned sources to `materialize` before getting here.
    #[allow(clippy::too_many_arguments)]
    async fn fork_mount_chain(
        &self,
        source_mount_id: Uuid,
        request: ForkMountRequest,
        source_path: String,
        source_upper: PathBuf,
        source_pinned: String,
        mut source_chain: Vec<String>,
        inherited_base: String,
        start: Instant,
    ) -> Result<ForkMountResponse, ServiceError> {
        let upper_root = PathBuf::from(crate::util::config::antares_upper_root());
        let frozen = upper_root.join(format!("sealed-{}", Uuid::new_v4()));
        let source_new_upper = upper_root.join(Uuid::new_v4().to_string());
        // Parent's VCS pointer target (host gitdir), captured while sealing.
        let mut source_pointer_target: Option<PathBuf> = None;

        // 1. Quiesce the source and seal its upper with one rename (same filesystem,
        //    atomic). Rollback restores the rename and the mount.
        {
            let mut mounts = self.mounts.write().await;
            let entry = mounts
                .get_mut(&source_mount_id)
                .ok_or(ServiceError::NotFound(source_mount_id))?;
            entry.state = MountLifecycle::Quiescing;
            if let Err(e) = entry.fuse.unmount().await {
                entry.state = MountLifecycle::Ready;
                return Err(ServiceError::FuseFailure(format!(
                    "chain fork: failed to quiesce the source mount: {e}"
                )));
            }
            if let Err(e) = std::fs::rename(&source_upper, &frozen) {
                entry.state = MountLifecycle::Ready;
                return Err(ServiceError::FuseFailure(format!(
                    "chain fork: failed to seal the source upper: {e}"
                )));
            }
            // The sealed layer must not carry the parent's VCS pointer: the child
            // inherits the layer read-only and would otherwise resolve the PARENT's
            // `.libra` (wrong index, wrong identity). Capture the pointer target so
            // the parent's rebuilt upper can serve the same link, then drop it from
            // the sealed layer.
            source_pointer_target = std::fs::read_link(frozen.join(".libra")).ok();
            let pointer_path = frozen.join(".libra");
            match std::fs::symlink_metadata(&pointer_path) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    if let Err(e) = std::fs::remove_file(&pointer_path) {
                        entry.state = MountLifecycle::Ready;
                        return Err(ServiceError::FuseFailure(format!(
                            "chain fork: failed to strip the VCS pointer from the sealed layer: {e}"
                        )));
                    }
                }
                Ok(meta) if meta.is_dir() => {
                    if let Err(e) = std::fs::remove_dir_all(&pointer_path) {
                        entry.state = MountLifecycle::Ready;
                        return Err(ServiceError::FuseFailure(format!(
                            "chain fork: failed to strip the VCS pointer dir from the sealed layer: {e}"
                        )));
                    }
                }
                _ => {}
            }
        }

        // 2. Rebuild the source: fresh empty upper over [frozen] + its old chain.
        //    Its view is unchanged and it keeps accepting writes; the child shares
        //    the same sealed layer read-only.
        let source_dicfuse =
            DicfuseManager::for_base_path_and_refs(&source_path, &source_pinned).await;
        if tokio::time::timeout(
            Duration::from_secs(180),
            source_dicfuse.store.wait_for_ready(),
        )
        .await
        .is_err()
        {
            // Roll back the seal: the source keeps its original upper, unchained.
            if let Err(rename_err) = std::fs::rename(&frozen, &source_upper) {
                tracing::error!(
                    "chain fork: rollback rename failed: {rename_err}; the source upper is now at {}",
                    frozen.display()
                );
            }
            {
                let mut mounts = self.mounts.write().await;
                if let Some(entry) = mounts.get_mut(&source_mount_id) {
                    entry.state = MountLifecycle::Ready;
                }
            }
            return Err(ServiceError::FuseFailure(
                "chain fork: the sealed Dicfuse projection did not become ready; the source was restored unchanged".into(),
            ));
        }

        let mut new_chain: Vec<String> = vec![frozen.to_string_lossy().to_string()];
        new_chain.extend(source_chain.iter().cloned());

        let (source_mountpoint, source_cl_dir) = {
            let mounts = self.mounts.read().await;
            match mounts.get(&source_mount_id) {
                Some(entry) => (
                    entry.mountpoint.clone(),
                    entry.cl_dir.clone().map(PathBuf::from),
                ),
                None => {
                    let _ = std::fs::rename(&frozen, &source_upper);
                    return Err(ServiceError::Internal(
                        "chain fork: source mount vanished mid-fork".into(),
                    ));
                }
            }
        };

        let source_fuse = match AntaresFuse::new(
            PathBuf::from(&source_mountpoint),
            source_dicfuse.clone(),
            source_new_upper.clone(),
            source_cl_dir.clone(),
        )
        .await
        .and_then(|fuse| fuse.with_frozen_layers(new_chain.iter().map(PathBuf::from).collect()))
        .map_err(|e| ServiceError::FuseFailure(format!("chain fork source rebuild: {e}")))
        {
            Ok(mut fuse) => match fuse.mount().await {
                Ok(()) => fuse,
                Err(e) => {
                    let _ = Self::remount_with_lower(
                        Path::new(&source_mountpoint),
                        source_dicfuse,
                        &source_upper,
                        source_cl_dir.as_deref(),
                        &source_chain,
                    )
                    .await;
                    let _ = std::fs::rename(&frozen, &source_upper);
                    return Err(ServiceError::FuseFailure(format!(
                        "chain fork: source remount failed: {e}"
                    )));
                }
            },
            Err(e) => {
                let _ = std::fs::rename(&frozen, &source_upper);
                return Err(ServiceError::FuseFailure(format!(
                    "chain fork: source rebuild failed: {e}"
                )));
            }
        };

        // Serve the same VCS pointer from the parent's new upper: the sealed layer
        // no longer carries it, and Libra expects `<worktree>/.libra` to resolve.
        if let Some(target) = &source_pointer_target {
            std::os::unix::fs::symlink(target, source_new_upper.join(".libra")).map_err(|e| {
                ServiceError::FuseFailure(format!(
                    "chain fork: failed to re-create the parent VCS pointer: {e}"
                ))
            })?;
        }

        {
            let mut mounts = self.mounts.write().await;
            if let Some(entry) = mounts.get_mut(&source_mount_id) {
                entry.fuse = source_fuse;
                entry.upper_dir = source_new_upper.to_string_lossy().to_string();
                entry.sealed_chain = new_chain.clone();
                entry.state = MountLifecycle::Ready;
                entry.last_seen_epoch_ms = current_epoch_ms();
            }
        }
        self.persist_state().await;

        // 3. The child: fresh upper over [frozen] + the source's old chain, the same
        //    pinned Dicfuse instance (shared through the manager cache), the same
        //    bound base. create_mount handles duplicates and job binding.
        let job_id = request
            .job_id
            .clone()
            .unwrap_or_else(|| format!("chain-fork-{}-{}", source_mount_id, Uuid::new_v4()));
        let created = match self
            .create_mount(CreateMountRequest {
                job_id: Some(job_id),
                build_id: None,
                path: source_path,
                cl_path: None,
                cl: None,
                mountpoint: request.mountpoint.clone(),
                upper_dir: None,
                pinned_refs: Some(source_pinned.clone()),
                sealed_chain: new_chain.clone(),
            })
            .await
        {
            Ok(created) => created,
            Err(e) => {
                // The source is already rebuilt and writable; only the child failed.
                return Err(ServiceError::Internal(format!(
                    "chain fork: child mount failed (source unchanged): {e}"
                )));
            }
        };

        // 4. Bind the child's base to the inherited revision (fresh clean mount).
        self.bind_worktree_base(
            created.mount_id,
            BindWorktreeBaseRequest {
                base_revision: inherited_base.clone(),
                vcs_pointer: None,
            },
        )
        .await?;

        // 5. Report the child's lower chain: the sealed dirs, nearest first, then
        //    the pinned Dicfuse projection.
        let mut lower_chain: Vec<String> = new_chain.clone();
        lower_chain.push(format!("dicfuse@{source_pinned}"));

        tracing::info!(
            source_mount_id = %source_mount_id,
            mount_id = %created.mount_id,
            sealed_layer = %frozen.display(),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "antares svc: fork_mount_chain success"
        );

        Ok(ForkMountResponse {
            mount_id: created.mount_id,
            job_id: request.job_id,
            path: created.mountpoint,
            source_mount_id,
            mode: ForkMode::Chain,
            mode_downgraded_from: None,
            lower_chain,
            // Inherited: a fork is a copy of the same revision, not a new one.
            base_revision: Some(inherited_base),
            mount_state: MountLifecycle::Ready,
            source_frozen_layer: Some(frozen.to_string_lossy().to_string()),
            copy_stats: None,
        })
    }

    async fn persist_state(&self) {
        if self.state_ownership == StateOwnership::External {
            return;
        }

        let mounts = self.mounts.read().await;
        let state = PersistedState {
            mounts: mounts
                .values()
                .filter(|e| matches!(e.state, MountLifecycle::Mounted | MountLifecycle::Ready))
                .map(|e| PersistedMountState {
                    mount_id: e.mount_id,
                    job_id: e.job_id.clone(),
                    path: e.path.clone(),
                    cl_path: e.cl_path.clone(),
                    cl: e.cl.clone(),
                    base_revision: e.base_revision.clone(),
                    pinned_refs: e.pinned_refs.clone(),
                    sealed_chain: e.sealed_chain.clone(),
                    mountpoint: e.mountpoint.clone(),
                    upper_dir: e.upper_dir.clone(),
                    cl_dir: e.cl_dir.clone(),
                    created_at_epoch_ms: e.created_at_epoch_ms,
                    mst2_lower: e.mst2_lower.is_some(),
                })
                .collect(),
        };
        drop(mounts);

        // Write state to file
        if let Some(parent) = self.state_file.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                tracing::warn!("Failed to create state directory: {}", e);
                return;
            }
        }

        match toml::to_string_pretty(&state) {
            Ok(content) => {
                if let Err(e) = std::fs::write(&self.state_file, content) {
                    tracing::warn!("Failed to write state file: {}", e);
                }
            }
            Err(e) => {
                tracing::warn!("Failed to serialize state: {}", e);
            }
        }
    }

    /// Recover mounts from persisted state file.
    async fn recover_mounts(&self) {
        if !self.state_file.exists() {
            tracing::debug!(
                "No state file found at {:?}, skipping recovery",
                self.state_file
            );
            return;
        }

        let content = match std::fs::read_to_string(&self.state_file) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("Failed to read state file: {}", e);
                return;
            }
        };

        let state: PersistedState = match toml::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Failed to parse state file: {}", e);
                tracing::error!(
                    "Failed to parse state file at {:?}: {}. Skipping mount recovery.",
                    self.state_file,
                    e
                );
                return;
            }
        };

        tracing::info!("Recovering {} mounts from state file", state.mounts.len());

        for persisted in state.mounts {
            // An MST/2-lowered mount is not restored: its view is resolved per
            // process, and rebuilding the mount with the Dicfuse projection
            // instead would silently serve a different base (spec 15 §3).
            if persisted.mst2_lower {
                tracing::warn!(
                    mount_id = %persisted.mount_id,
                    "not restoring an MST/2-lowered mount after restart; re-attach it \
                     (the Dicfuse projection would be a different base)"
                );
                continue;
            }

            // Check if mountpoint still exists
            let mountpoint = PathBuf::from(&persisted.mountpoint);
            if !mountpoint.exists() {
                tracing::info!(
                    "Skipping recovery of mount {} - mountpoint no longer exists",
                    persisted.mount_id
                );
                continue;
            }

            // Get or create Dicfuse instance (uses cache for subdirectory paths).
            // A pinned mount must come back pinned: an unpinned instance would serve
            // the moving trunk tip, silently breaking the lower_revision invariant.
            let dicfuse = match &persisted.pinned_refs {
                Some(refs) => {
                    let pinned =
                        DicfuseManager::for_base_path_and_refs(&persisted.path, refs).await;
                    match tokio::time::timeout(
                        Duration::from_secs(120),
                        pinned.store.wait_for_ready(),
                    )
                    .await
                    {
                        Ok(()) => pinned,
                        Err(_) => {
                            tracing::warn!(
                                "Pinned Dicfuse for {} at {} did not become ready during recovery",
                                persisted.mount_id,
                                refs
                            );
                            continue;
                        }
                    }
                }
                None => match self.get_or_create_dicfuse(&persisted.path, None).await {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!(
                            "Failed to get Dicfuse for {} during recovery: {}",
                            persisted.mount_id,
                            e
                        );
                        continue;
                    }
                },
            };

            let upper_dir = PathBuf::from(&persisted.upper_dir);
            let cl_dir = persisted.cl_dir.as_ref().map(PathBuf::from);

            // Try to create and mount AntaresFuse
            let frozen = persisted
                .sealed_chain
                .iter()
                .map(PathBuf::from)
                .collect::<Vec<_>>();
            match AntaresFuse::new(mountpoint.clone(), dicfuse, upper_dir, cl_dir.clone())
                .await
                .and_then(|fuse| fuse.with_frozen_layers(frozen))
            {
                Ok(mut fuse) => {
                    if let Err(e) = fuse.mount().await {
                        tracing::warn!(
                            "Failed to remount {} during recovery: {}",
                            persisted.mount_id,
                            e
                        );
                        continue;
                    }

                    // Create entry
                    let cl_path = persisted
                        .cl_path
                        .clone()
                        .unwrap_or_else(|| persisted.path.clone());
                    let entry = MountEntry {
                        mount_id: persisted.mount_id,
                        job_id: persisted.job_id.clone(),
                        path: persisted.path.clone(),
                        cl_path: Some(cl_path.clone()),
                        cl: persisted.cl.clone(),
                        base_revision: persisted.base_revision.clone(),
                        pinned_refs: persisted.pinned_refs.clone(),
                        sealed_chain: persisted.sealed_chain.clone(),
                        mountpoint: persisted.mountpoint.clone(),
                        upper_dir: persisted.upper_dir.clone(),
                        cl_dir: persisted.cl_dir.clone(),
                        fuse,
                        // Recovery only restores Dicfuse-lowered mounts; the
                        // MST/2 ones are skipped above and must be re-attached.
                        mst2_lower: None,
                        // Dicfuse is ready after AntaresFuse::new() completes import_arc.
                        state: MountLifecycle::Ready,
                        created_at_epoch_ms: persisted.created_at_epoch_ms,
                        last_seen_epoch_ms: current_epoch_ms(),
                        preload_cancel: Arc::new(AtomicBool::new(false)),
                    };

                    let mut mounts = self.mounts.write().await;
                    let mut index = self.path_index.write().await;
                    let mut job_index = self.job_index.write().await;
                    mounts.insert(persisted.mount_id, entry);
                    if let Some(job_id) = persisted.job_id {
                        job_index.insert(job_id, persisted.mount_id);
                    } else {
                        index.insert(
                            (persisted.path, persisted.cl, Some(cl_path)),
                            persisted.mount_id,
                        );
                    }

                    tracing::info!("Recovered mount {} at {:?}", persisted.mount_id, mountpoint);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to create AntaresFuse for recovery of {}: {}",
                        persisted.mount_id,
                        e
                    );
                }
            }
        }
    }

    /// Validate the create mount request.
    fn validate_request(request: &CreateMountRequest) -> Result<(), ServiceError> {
        if request.path.is_empty() {
            return Err(ServiceError::InvalidRequest("path cannot be empty".into()));
        }
        Ok(())
    }

    /// Check if a path+CL+CL-path combination is already mounted.
    async fn is_path_already_mounted(
        &self,
        path: &str,
        cl: Option<&str>,
        cl_path: Option<&str>,
    ) -> bool {
        let index = self.path_index.read().await;
        index.contains_key(&(
            path.to_string(),
            cl.map(|s| s.to_string()),
            cl_path.map(|s| s.to_string()),
        ))
    }

    /// Get service health information.
    pub async fn health_info_impl(&self) -> HealthResponse {
        let mounts = self.mounts.read().await;
        HealthResponse {
            protocol_version: 1,
            service: "scorpiofs".to_string(),
            service_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            capabilities: vec![
                "mount.v1".to_string(),
                "ready.v1".to_string(),
                "changes.v1".to_string(),
                "worktree-base.v1".to_string(),
                "refresh-plan.v1".to_string(),
                // Advertises that this daemon records upper-layer deletions in the OCI
                // whiteout form (`.wh.<name>`), so a deletion of a lower-layer file is
                // observable in `changes` without requiring `CAP_MKNOD`. A VCS client that
                // relies on deletions (e.g. a `sync` that must delete remote files) must
                // refuse to operate when this capability is absent rather than silently
                // miss deletions.
                "whiteout.oci.v1".to_string(),
                // Derive a new worktree mount from an existing one
                // (`POST /mounts/{mount_id}/fork`).
                "fork.v1".to_string(),
                // Worktree Control Protocol v2: effective diff, commit finalize,
                // lower refresh (docs/scorpiofs-libra-complete-spec-v1.md).
                "worktree.state.v2".to_string(),
                "worktree.attach.v2".to_string(),
                // Chain forks: sealed layers shared between parent and child.
                "worktree.fork-chain.v2".to_string(),
                "worktree.commit-finalize.v2".to_string(),
                "worktree.refresh.v2".to_string(),
            ],
            status: "healthy".to_string(),
            mount_count: mounts.len(),
            uptime_secs: self.start_time.elapsed().as_secs(),
        }
    }

    /// Cleanup all mounts during shutdown.
    pub async fn shutdown_cleanup_impl(&self) -> Result<(), ServiceError> {
        let mut mounts = self.mounts.write().await;
        let mut index = self.path_index.write().await;
        let mut job_index = self.job_index.write().await;

        for (mount_id, mut entry) in mounts.drain() {
            tracing::info!("Unmounting {} during shutdown", mount_id);
            entry.preload_cancel.store(true, Ordering::Relaxed);
            if let Err(e) = entry.fuse.unmount().await {
                tracing::warn!("Failed to unmount {} during shutdown: {}", mount_id, e);
                // Continue with other mounts even if one fails
            }
        }
        // All mounts drained; clear indices.
        // TODO(antares): If we ever decide to keep failed-unmount mounts in memory/state for
        // later retry, revisit index cleanup to avoid inconsistencies.
        index.clear();
        job_index.clear();
        Ok(())
    }
}

#[async_trait]
impl AntaresService for AntaresServiceImpl {
    async fn create_mount(
        &self,
        request: CreateMountRequest,
    ) -> Result<MountCreated, ServiceError> {
        let start = Instant::now();
        let mut request = request;
        request.path = Self::normalize_mount_path(&request.path);
        if let Some(cl_path) = request.cl_path.as_mut() {
            *cl_path = Self::normalize_mount_path(cl_path);
        }
        if request.cl_path.is_none() {
            request.cl_path = Some(request.path.clone());
        }

        // 1. Validate request
        Self::validate_request(&request)?;

        // Derive a task identifier (job/build id) if provided.
        let task_id: Option<String> = request
            .job_id
            .clone()
            .or(request.build_id.clone())
            .and_then(|s| {
                let trimmed = s.trim().to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            });

        tracing::info!(
            task_id = ?task_id,
            path = %request.path,
            cl = ?request.cl,
            "antares svc: create_mount start"
        );

        // 2. Idempotency / de-dup policy:
        // - If task_id is provided: treat create as idempotent for the same task id.
        //   This supports build-task-granularity mounts.
        // - If task_id is NOT provided: keep legacy behavior and reject duplicate (path, cl).
        if let Some(ref job_id) = task_id {
            // Fast path: already mounted for this task id -> return existing mount.
            if let Some(existing_id) = { self.job_index.read().await.get(job_id).cloned() } {
                let mut mounts = self.mounts.write().await;
                if let Some(entry) = mounts.get_mut(&existing_id) {
                    // Guard against job_id reuse with different request params.
                    if entry.path != request.path
                        || entry.cl != request.cl
                        || entry.cl_path != request.cl_path
                    {
                        return Err(ServiceError::InvalidRequest(format!(
                            "job_id/build_id '{}' already mounted with different path/cl",
                            job_id
                        )));
                    }
                    // If the mount is being torn down, do NOT treat this as an idempotent success.
                    // Otherwise we may return a mount_id that is about to be removed, causing
                    // follow-up describe/delete calls to 404.
                    if !matches!(entry.state, MountLifecycle::Mounted | MountLifecycle::Ready) {
                        return Err(ServiceError::InvalidRequest(format!(
                            "job_id/build_id '{}' is currently in state {:?}; retry after unmount completes",
                            job_id, entry.state
                        )));
                    }
                    entry.update_last_seen();
                    tracing::info!(
                        task_id = %job_id,
                        mount_id = %existing_id,
                        mountpoint = %entry.mountpoint,
                        elapsed_ms = start.elapsed().as_millis(),
                        "antares svc: create_mount idempotent hit"
                    );
                    return Ok(MountCreated {
                        mount_id: existing_id,
                        mountpoint: entry.mountpoint.clone(),
                    });
                } else {
                    // Stale index entry: remove and continue with fresh mount creation.
                    self.job_index.write().await.remove(job_id);
                }
            }
        } else if self
            .is_path_already_mounted(
                &request.path,
                request.cl.as_deref(),
                request.cl_path.as_deref(),
            )
            .await
        {
            return Err(ServiceError::InvalidRequest(format!(
                "path {} with cl {:?} is already mounted",
                request.path, request.cl
            )));
        }

        // 3. Generate UUID and auto-generate all paths
        let mount_id = Uuid::new_v4();
        let id_str = mount_id.to_string();

        // Get base paths from config
        let mount_root = crate::util::config::antares_mount_root();
        let upper_root = crate::util::config::antares_upper_root();
        let cl_root = crate::util::config::antares_cl_root();

        // Auto-generate paths based on UUID
        let mountpoint_str = request
            .mountpoint
            .clone()
            .unwrap_or_else(|| format!("{}/{}", mount_root, id_str));
        // `fork` supplies a pre-populated upper (see `CreateMountRequest::upper_dir`);
        // everything else gets a fresh one. The hint is validated to sit under the
        // configured upper root so a stray value cannot point the failure-path
        // `remove_dir_all` at an unrelated directory.
        let upper_dir_str = match request.upper_dir.as_deref() {
            Some(hint) => {
                let hint_path = Path::new(hint.trim_end_matches('/'));
                let root = Path::new(upper_root.trim_end_matches('/'));
                if hint_path.parent() != Some(root) {
                    return Err(ServiceError::InvalidRequest(format!(
                        "upper_dir must be a direct child of {}",
                        root.display()
                    )));
                }
                hint_path.to_string_lossy().to_string()
            }
            None => format!("{}/{}", upper_root, id_str),
        };
        let cl_dir_str = request
            .cl
            .as_ref()
            .map(|_| format!("{}/{}", cl_root, id_str));

        let mountpoint = PathBuf::from(&mountpoint_str);
        let upper_dir = PathBuf::from(&upper_dir_str);
        let cl_dir = cl_dir_str.as_ref().map(PathBuf::from);

        tracing::debug!(
            mount_id = %mount_id,
            task_id = ?task_id,
            mountpoint = %mountpoint_str,
            upper_dir = %upper_dir_str,
            cl_dir = ?cl_dir_str,
            "antares svc: create_mount paths generated"
        );

        if let (Some(cl_link), Some(ref cl_dir_str)) = (request.cl.as_deref(), cl_dir_str.as_ref())
        {
            let cl_dir_path = PathBuf::from(cl_dir_str);
            if let Err(err) = self
                .build_cl_layer(
                    &request.path,
                    request.cl_path.as_deref().unwrap_or(&request.path),
                    cl_link,
                    &cl_dir_path,
                )
                .await
            {
                let _ = std::fs::remove_dir_all(&mountpoint_str);
                let _ = std::fs::remove_dir_all(&upper_dir_str);
                let _ = std::fs::remove_dir_all(cl_dir_str);
                return Err(err);
            }
        }

        // 5. Get or create Dicfuse instance for this mount (uses cache for subdirectory paths)
        // If a specific base path is requested (not root), get from cache or create a dedicated
        // Dicfuse with path remapping. Otherwise, use the shared global instance.
        // This may take time for new subdirectory paths as it waits for import_arc to complete.
        let dicfuse = self
            .get_or_create_dicfuse(&request.path, request.pinned_refs.as_deref())
            .await?;

        // 6. Create AntaresFuse instance (may take time, not holding lock)
        let sealed = request
            .sealed_chain
            .iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        let mut fuse = AntaresFuse::new(mountpoint, dicfuse, upper_dir, cl_dir)
            .await
            .and_then(|fuse| fuse.with_frozen_layers(sealed))
            .map_err(|e| ServiceError::FuseFailure(format!("failed to create fuse: {}", e)))?;

        // MST/2 lower projection (spec 12 §1): when the operator enabled it,
        // the snapshot view takes the Dicfuse slot as the overlay's base layer.
        // The view is kept on the mount entry so the effective diff compares
        // against the projection that is actually being served.
        let mst2_lower = mst2_lower_layer().await?;
        if let Some(view) = &mst2_lower {
            fuse = fuse.with_lower_override(view.clone() as Arc<dyn libfuse_fs::unionfs::layer::Layer>);
        }

        // 7. Mount the filesystem
        fuse.mount()
            .await
            .map_err(|e| ServiceError::FuseFailure(format!("failed to mount: {}", e)))?;

        // 8. Record timestamps. We'll only construct MountEntry after passing the duplicate check
        // so we can rollback the FUSE mount safely on race losers.
        let now = current_epoch_ms();

        // 9. Insert into mounts map
        let mut mounts = self.mounts.write().await;
        let mut index = self.path_index.write().await;
        let mut job_index = self.job_index.write().await;

        // Double-check for races after acquiring write locks (match the policy above).
        if let Some(ref job_id) = task_id {
            if job_index.contains_key(job_id) {
                // IMPORTANT: rollback the freshly mounted FUSE session before returning.
                // Under concurrent POST /mounts for the same job_id/build_id, the losing request
                // may have already mounted a FUSE session but not yet inserted it into mounts /
                // job_index. Returning early here would leak an orphan mount that cannot be
                // tracked or cleaned up.
                let err = ServiceError::InvalidRequest(format!(
                    "job_id/build_id '{}' is already mounted",
                    job_id
                ));
                drop(mounts);
                drop(index);
                drop(job_index);

                tracing::warn!(
                    "create_mount duplicate task_id detected after mount; rolling back orphan mount {}",
                    mount_id
                );
                let _ = fuse.unmount().await;
                Self::remove_mount_dirs(
                    mount_id,
                    Path::new(&mountpoint_str),
                    Path::new(&upper_dir_str),
                    cl_dir_str.as_deref().map(Path::new),
                );
                return Err(err);
            }
        } else if index.contains_key(&(
            request.path.clone(),
            request.cl.clone(),
            request.cl_path.clone(),
        )) {
            // Same rollback logic as above for legacy (path, cl) duplicates.
            let err = ServiceError::InvalidRequest(format!(
                "path {} with cl {:?} is already mounted",
                request.path, request.cl
            ));
            drop(mounts);
            drop(index);
            drop(job_index);

            tracing::warn!(
                "create_mount duplicate (path, cl) detected after mount; rolling back orphan mount {}",
                mount_id
            );
            let _ = fuse.unmount().await;
            Self::remove_mount_dirs(
                mount_id,
                Path::new(&mountpoint_str),
                Path::new(&upper_dir_str),
                cl_dir_str.as_deref().map(Path::new),
            );
            return Err(err);
        }

        // Now it's safe to commit the mount into the in-memory state.
        let preload_cancel = Arc::new(AtomicBool::new(false));
        let entry = MountEntry {
            mount_id,
            job_id: task_id.clone(),
            path: request.path.clone(),
            cl_path: request.cl_path.clone(),
            cl: request.cl.clone(),
            base_revision: None,
            pinned_refs: request.pinned_refs.clone(),
            sealed_chain: request.sealed_chain.clone(),
            mountpoint: mountpoint_str.clone(),
            upper_dir: upper_dir_str.clone(),
            cl_dir: cl_dir_str.clone(),
            fuse,
            mst2_lower,
            state: MountLifecycle::Mounted,
            created_at_epoch_ms: now,
            last_seen_epoch_ms: now,
            preload_cancel: preload_cancel.clone(),
        };

        // Preserve path/cl for logging before moving into index
        let path_for_log = request.path.clone();
        let cl_for_log = request.cl.clone();

        let task_id_for_log = task_id.clone();

        mounts.insert(mount_id, entry);
        if let Some(job_id) = task_id {
            job_index.insert(job_id, mount_id);
        } else {
            index.insert(
                (
                    request.path.clone(),
                    request.cl.clone(),
                    request.cl_path.clone(),
                ),
                mount_id,
            );
        }

        tracing::info!(
            mount_id = %mount_id,
            task_id = ?task_id_for_log,
            path = %path_for_log,
            cl = ?cl_for_log,
            mountpoint = %mountpoint_str,
            upper_dir = %upper_dir_str,
            cl_dir = ?cl_dir_str,
            elapsed_ms = start.elapsed().as_millis(),
            "antares svc: create_mount success"
        );

        // IMPORTANT: release locks before persisting state.
        // `persist_state()` acquires `self.mounts.read()`. If we keep holding `mounts.write()`
        // here, the task deadlocks and the HTTP request never returns (curl hangs at step [1]).
        drop(mounts);
        drop(index);
        drop(job_index);

        // Persist state to file for recovery
        self.persist_state().await;

        // Transition to Ready immediately.
        //
        // By this point Dicfuse's `import_arc()` → `load_dir_depth()` (Phase 1) has
        // already populated the in-memory directory cache.  Any FUSE `statx` that
        // arrives now will hit the Dicfuse memory cache (~1 ms) instead of making a
        // network round-trip (~100 ms).  Waiting for `deep_preload_walk` (Phase 2)
        // to push those entries into the **kernel** FUSE cache would save ~1 ms per
        // statx but costs ~140 s of startup latency — unacceptable for CI.
        //
        // Phase 2 still runs in the background as a best-effort optimisation.
        {
            let mut mounts = self.mounts.write().await;
            if let Some(entry) = mounts.get_mut(&mount_id) {
                if matches!(entry.state, MountLifecycle::Mounted) {
                    entry.state = MountLifecycle::Ready;
                    entry.update_last_seen();
                    tracing::info!(
                        mount_id = %mount_id,
                        "antares svc: mount is Ready (Dicfuse cache warm, kernel cache warming in background)"
                    );
                }
            }
        }

        // Best-effort: warm FUSE kernel caches in the background.
        self.spawn_deep_preload_task(
            mount_id,
            mountpoint_str.clone(),
            preload_cancel.clone(),
            "create_mount",
        );

        Ok(MountCreated {
            mount_id,
            mountpoint: mountpoint_str,
        })
    }

    async fn list_mounts(&self) -> Result<Vec<MountStatus>, ServiceError> {
        let mounts = self.mounts.read().await;
        let list: Vec<MountStatus> = mounts.values().map(|e| e.to_status()).collect();
        Ok(list)
    }

    async fn describe_mount(&self, mount_id: Uuid) -> Result<MountStatus, ServiceError> {
        let mounts = self.mounts.read().await;
        let entry = mounts
            .get(&mount_id)
            .ok_or(ServiceError::NotFound(mount_id))?;
        Ok(entry.to_status())
    }

    async fn changed_paths(&self, mount_id: Uuid) -> Result<MountChangesResponse, ServiceError> {
        // Retain this read lock through the blocking scan. CL updates need the write
        // lock before entering Quiescing, so they cannot remove or recreate cl_dir
        // while scan_mount_changes is reading it.
        let mounts = self.mounts.read().await;
        let (upper_dir, cl_dir) = {
            let entry = mounts
                .get(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            if matches!(entry.state, MountLifecycle::Quiescing) {
                return Err(ServiceError::InvalidRequest(format!(
                    "mount {} is quiescing while its CL layer is reconfigured",
                    mount_id
                )));
            }
            (
                PathBuf::from(&entry.upper_dir),
                entry.cl_dir.as_deref().map(PathBuf::from),
            )
        };
        let scan_result = tokio::task::spawn_blocking(move || {
            scan_mount_changes(mount_id, &upper_dir, cl_dir.as_deref())
        })
        .await
        .map_err(|error| {
            ServiceError::Internal(format!(
                "Antares changed-path scan task failed for mount {}: {}",
                mount_id, error
            ))
        })?;
        drop(mounts);
        scan_result
    }

    async fn worktree_state(&self, mount_id: Uuid) -> Result<WorktreeStateResponse, ServiceError> {
        let (path, base_revision, mount_state, upper_dir) = {
            let mounts = self.mounts.read().await;
            let entry = mounts
                .get(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            (
                entry.path.clone(),
                entry.base_revision.clone(),
                entry.state.clone(),
                PathBuf::from(&entry.upper_dir),
            )
        };

        // A VCS worktree treats an optional CL layer as part of its supplied
        // base, not as a user edit. Only the private upper layer is dirty.
        let changes =
            tokio::task::spawn_blocking(move || scan_mount_changes(mount_id, &upper_dir, None))
                .await
                .map_err(|error| {
                    ServiceError::Internal(format!(
                        "Antares worktree-state scan task failed for mount {}: {}",
                        mount_id, error
                    ))
                })??;

        Ok(WorktreeStateResponse {
            mount_id,
            path,
            base_revision,
            mount_state,
            dirty: !changes.changes.is_empty(),
            changes,
        })
    }

    async fn bind_worktree_base(
        &self,
        mount_id: Uuid,
        request: BindWorktreeBaseRequest,
    ) -> Result<WorktreeStateResponse, ServiceError> {
        let base_revision = request.base_revision.trim();
        if base_revision.is_empty() {
            return Err(ServiceError::InvalidRequest(
                "base_revision cannot be empty".into(),
            ));
        }

        // Collect what validation needs first: the pointer write below is filesystem
        // I/O and should not run while holding the mount-table lock.
        let (existing_base, mountpoint, cl_present, mount_state) = {
            let mounts = self.mounts.read().await;
            let entry = mounts
                .get(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            (
                entry.base_revision.clone(),
                PathBuf::from(&entry.mountpoint),
                entry.cl.is_some(),
                entry.state.clone(),
            )
        };

        if let Some(existing) = &existing_base {
            if existing != base_revision {
                return Err(ServiceError::InvalidRequest(format!(
                    "mount {} is already bound to base revision {}",
                    mount_id, existing
                )));
            }
            // Repeating the same binding is idempotent, and re-asserts the pointer so a
            // caller whose first attempt lost the write can simply ask again.
            if let Some(pointer) = &request.vcs_pointer {
                write_vcs_pointer(&mountpoint, pointer)?;
            }
            return self.worktree_state(mount_id).await;
        }

        if !matches!(
            &mount_state,
            MountLifecycle::Mounted | MountLifecycle::Ready
        ) {
            return Err(ServiceError::InvalidRequest(format!(
                "mount {} is currently in state {:?}; cannot bind a Libra worktree base",
                mount_id, mount_state
            )));
        }
        if cl_present {
            return Err(ServiceError::InvalidRequest(
                "cannot bind a Libra worktree base to a mount with a CL layer".into(),
            ));
        }

        let state = self.worktree_state(mount_id).await?;
        if state.dirty {
            return Err(ServiceError::InvalidRequest(
                "cannot bind a worktree base after local upper-layer changes exist".into(),
            ));
        }

        // Pointer first, binding second: if the pointer cannot be written, no binding
        // has been advertised on the strength of state that is not on disk.
        if let Some(pointer) = &request.vcs_pointer {
            write_vcs_pointer(&mountpoint, pointer)?;
        }

        {
            let mut mounts = self.mounts.write().await;
            let entry = mounts
                .get_mut(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            // Re-check under the write lock: the mount may have moved on while the
            // pointer was being written.
            if !matches!(entry.state, MountLifecycle::Mounted | MountLifecycle::Ready) {
                return Err(ServiceError::InvalidRequest(format!(
                    "mount {} is currently in state {:?}; cannot bind a Libra worktree base",
                    mount_id, entry.state
                )));
            }
            if entry.cl.is_some() {
                return Err(ServiceError::InvalidRequest(
                    "cannot bind a Libra worktree base to a mount with a CL layer".into(),
                ));
            }
            match &entry.base_revision {
                Some(existing) if existing != base_revision => {
                    return Err(ServiceError::InvalidRequest(format!(
                        "mount {} is already bound to base revision {}",
                        mount_id, existing
                    )));
                }
                Some(_) => {}
                None => {
                    entry.base_revision = Some(base_revision.to_string());
                    entry.update_last_seen();
                }
            }
        }

        self.persist_state().await;
        self.worktree_state(mount_id).await
    }

    async fn plan_worktree_refresh(
        &self,
        mount_id: Uuid,
        request: RefreshPlanRequest,
    ) -> Result<RefreshPlanResponse, ServiceError> {
        let expected = request.expected_base_revision.trim();
        let target = request.target_revision.trim();
        if expected.is_empty() || target.is_empty() {
            return Err(ServiceError::InvalidRequest(
                "expected_base_revision and target_revision cannot be empty".into(),
            ));
        }

        let worktree = self.worktree_state(mount_id).await?;
        let disposition = match worktree.base_revision.as_deref() {
            None => RefreshPlanDisposition::Unbound,
            Some(current) if current != expected => RefreshPlanDisposition::BaseMismatch,
            Some(current) if current == target => RefreshPlanDisposition::AlreadyAtTarget,
            Some(_) if request.require_clean && worktree.dirty => {
                RefreshPlanDisposition::BlockedDirty
            }
            Some(_) => RefreshPlanDisposition::Ready,
        };

        Ok(RefreshPlanResponse {
            mount_id,
            current_base_revision: worktree.base_revision.clone(),
            target_revision: target.to_string(),
            disposition,
            worktree,
        })
    }

    async fn fork_mount(
        &self,
        source_mount_id: Uuid,
        request: ForkMountRequest,
    ) -> Result<ForkMountResponse, ServiceError> {
        let start = Instant::now();

        // Resolve what we need from the source, then drop the lock: the delta copy
        // below does blocking filesystem I/O and must not hold the mount table.
        //
        // The monorepo path is *inherited*: a fork is a second worktree over the same
        // subtree, so the caller does not get to point it somewhere else. Note that
        // `path` here is the monorepo path (as on `POST /mounts`), **not** a filesystem
        // location — the child's mountpoint is generated by `create_mount`.
        let (source_path, source_upper, source_cl, source_cl_path, source_base, source_pinned, source_chain) = {
            let mounts = self.mounts.read().await;
            let entry = mounts
                .get(&source_mount_id)
                .ok_or(ServiceError::NotFound(source_mount_id))?;

            if matches!(
                &entry.state,
                MountLifecycle::Quiescing | MountLifecycle::Unmounting | MountLifecycle::Unmounted
            ) {
                return Err(ServiceError::InvalidRequest(format!(
                    "source mount {} is in state {:?} and cannot be forked from",
                    source_mount_id, entry.state
                )));
            }

            (
                entry.path.clone(),
                PathBuf::from(&entry.upper_dir),
                entry.cl.clone(),
                entry.cl_path.clone(),
                entry.base_revision.clone(),
                entry.pinned_refs.clone(),
                entry.sealed_chain.clone(),
            )
        };

        let inherited_base = source_base.clone().ok_or_else(|| {
            ServiceError::InvalidRequest(
                "cannot fork a worktree without a bound base revision".into(),
            )
        })?;

        // Chain requires a pinned source: a sealed layer over a MOVING trunk tip
        // would make the child's base meaningless. Unpinned sources fall back to
        // materialize and the response says so.
        let (mode, mode_downgraded_from) = match (&request.mode, &source_pinned) {
            (ForkMode::Chain, None) => (ForkMode::Materialize, Some(ForkMode::Chain)),
            (ForkMode::Chain, Some(_)) => (ForkMode::Chain, None),
            (ForkMode::Materialize, _) => (ForkMode::Materialize, None),
        };
        // The child is a distinct mount: never inherit the source's job binding,
        // and never let the (path, cl) duplicate check reject a second fork.
        let job_id = request
            .job_id
            .clone()
            .unwrap_or_else(|| format!("fork-{}-{}", source_mount_id, Uuid::new_v4()));
        let sealed_fork = mode == ForkMode::Chain;

        if sealed_fork {
            return self
                .fork_mount_chain(
                    source_mount_id,
                    request.clone(),
                    source_path.clone(),
                    source_upper.clone(),
                    source_pinned
                        .clone()
                        .expect("checked above: chain requires a pinned source"),
                    source_chain.clone(),
                    inherited_base.clone(),
                    start,
                )
                .await;
        }

        // The child's delta must be on disk *before* the FUSE session starts, because
        // the upper layer is only imported at mount time. So it is copied into a
        // staging upper directory, which `create_mount` then adopts (the internal
        // `upper_dir` field) instead of generating an empty one.
        let staging = PathBuf::from(crate::util::config::antares_upper_root())
            .join(Uuid::new_v4().to_string());

        let (src, dst) = (source_upper.clone(), staging.clone());
        let copied = tokio::task::spawn_blocking(move || fork_upper(&src, &dst))
            .await
            .map_err(|e| ServiceError::Internal(format!("fork delta copy task failed: {e}")))?;

        let copy_stats = match copied {
            Ok(stats) => stats,
            Err(err) => {
                let _ = std::fs::remove_dir_all(&staging);
                return Err(match err {
                    ForkCopyError::SourceBusy { .. } => {
                        ServiceError::InvalidRequest(format!("fork aborted: {err}"))
                    }
                    ForkCopyError::UnsupportedEntry { .. } => ServiceError::InvalidRequest(
                        format!("fork cannot copy this worktree: {err}"),
                    ),
                    ForkCopyError::Io { .. } => {
                        ServiceError::Internal(format!("fork failed while copying: {err}"))
                    }
                });
            }
        };

        let inherit_cl = request.inherit_cl;
        let created = match self
            .create_mount(CreateMountRequest {
                job_id: Some(job_id),
                build_id: None,
                path: source_path,
                cl_path: if inherit_cl { source_cl_path } else { None },
                cl: if inherit_cl { source_cl } else { None },
                mountpoint: request.mountpoint.clone(),
                upper_dir: Some(staging.to_string_lossy().to_string()),
                // The child inherits the source's pin: a fork of a pinned worktree
                // must not silently fall back to the moving trunk tip.
                pinned_refs: source_pinned,
                sealed_chain: Vec::new(),
            })
            .await
        {
            Ok(created) => created,
            Err(err) => {
                // `create_mount` rolls back the paths it owns, but the staging
                // directory is ours to clean up.
                let _ = std::fs::remove_dir_all(&staging);
                return Err(err);
            }
        };

        {
            let mut mounts = self.mounts.write().await;
            let entry = mounts
                .get_mut(&created.mount_id)
                .ok_or(ServiceError::NotFound(created.mount_id))?;
            entry.base_revision = Some(inherited_base.clone());
            entry.update_last_seen();
        }
        self.persist_state().await;

        let status = self.describe_mount(created.mount_id).await?;

        // Nearest-first: the CL layer (a build baseline) sits above the shared
        // Dicfuse projection. Callers must not reorder this.
        let mut lower_chain = Vec::new();
        if let Some(cl) = status.layers.cl.clone() {
            lower_chain.push(cl);
        }
        lower_chain.push(status.layers.dicfuse.clone());

        tracing::info!(
            source_mount_id = %source_mount_id,
            mount_id = %created.mount_id,
            mode = ?mode,
            files = copy_stats.files,
            bytes = copy_stats.bytes,
            reflink_used = copy_stats.reflink_used,
            retries = copy_stats.retries,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "antares svc: fork_mount success"
        );

        Ok(ForkMountResponse {
            mount_id: created.mount_id,
            job_id: status.job_id.clone(),
            path: status.mountpoint.clone(),
            source_mount_id,
            mode,
            mode_downgraded_from,
            lower_chain,
            // Inherited: a fork is a copy of the same revision, not a new one.
            base_revision: Some(inherited_base),
            mount_state: status.state.clone(),
            // Reserved for `chain`; always absent under `materialize`.
            source_frozen_layer: None,
            copy_stats: Some(copy_stats),
        })
    }

    async fn attach_worktree(
        &self,
        request: AttachWorktreeRequest,
    ) -> Result<AttachWorktreeResponse, ServiceError> {
        let start = Instant::now();
        let repo_path = Self::normalize_mount_path(&request.repo_path);

        // Fail before creating anything if the mountpoint cannot serve a FUSE
        // session — the same rule v1's mount path enforces, just earlier.
        let mountpoint = PathBuf::from(Self::normalize_mount_path(&request.mountpoint));
        crate::server::prepare_mountpoint(&mountpoint).map_err(|e| {
            ServiceError::InvalidRequest(format!(
                "attach target {} is not usable as a mountpoint: {e}",
                mountpoint.display()
            ))
        })?;

        // Pin the lower now: an explicit internal OID from the client, or the
        // monorepo's latest commit for the path. Either way the projection is
        // immutable from the first request — no tip-drift window before bind.
        let lower_revision = match request.lower_revision.as_deref().map(str::trim) {
            Some(r) if !r.is_empty() => r.to_string(),
            _ => resolve_latest_revision(config::base_url(), &repo_path)
                .await
                .map_err(ServiceError::Internal)?,
        };

        let created = self
            .create_mount(CreateMountRequest {
                job_id: request.job_id.clone(),
                build_id: None,
                path: repo_path.clone(),
                cl_path: None,
                cl: None,
                mountpoint: Some(mountpoint.to_string_lossy().to_string()),
                upper_dir: None,
                pinned_refs: Some(lower_revision.clone()),
                sealed_chain: Vec::new(),
            })
            .await?;

        // Record the client-side binding on the fresh (clean) mount: the same
        // validation v1's bind endpoint runs, inlined for the single-call attach.
        // The pointer files stay the client's business — the daemon never writes
        // VCS metadata into a mount.
        if let Some(base) = request
            .base_revision
            .as_deref()
            .map(str::trim)
            .filter(|b| !b.is_empty())
        {
            self.bind_worktree_base(
                created.mount_id,
                BindWorktreeBaseRequest {
                    base_revision: base.to_string(),
                    vcs_pointer: None,
                },
            )
            .await?;
        }

        let generation = {
            let mounts = self.mounts.read().await;
            let entry = mounts
                .get(&created.mount_id)
                .ok_or(ServiceError::NotFound(created.mount_id))?;
            let chain_dirs = entry
                .sealed_chain
                .iter()
                .map(PathBuf::from)
                .collect::<Vec<_>>();
            let changes = effective_changes(lower_view_for(entry).as_ref(), Path::new(&entry.upper_dir), &chain_dirs)
                .await
                .map_err(|e| ServiceError::Internal(format!("effective scan failed: {e}")))?;
            generation_of(&changes)
        };

        tracing::info!(
            mount_id = %created.mount_id,
            worktree_id = ?request.worktree_id,
            repo_path = %repo_path,
            lower_revision = %lower_revision,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "antares svc: attach_worktree success"
        );

        Ok(AttachWorktreeResponse {
            mount_id: created.mount_id.to_string(),
            worktree_id: request.worktree_id,
            mountpoint: created.mountpoint,
            base_revision: request.base_revision,
            lower_revision,
            state: "ready".into(),
            generation,
        })
    }

    async fn worktree_state_v2(&self, mount_id: Uuid) -> Result<WorktreeStateV2, ServiceError> {
        let (path, upper_dir, base_revision, pinned_refs, sealed_chain, mount_state, mst2_lower) = {
            let mounts = self.mounts.read().await;
            let entry = mounts
                .get(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            (
                entry.path.clone(),
                PathBuf::from(&entry.upper_dir),
                entry.base_revision.clone(),
                entry.pinned_refs.clone(),
                entry.sealed_chain.clone(),
                entry.state.clone(),
                entry.mst2_lower.clone(),
            )
        };

        let dicfuse = self
            .lower_dicfuse_for(&path, pinned_refs.as_deref())
            .await?;
        let chain_dirs: Vec<PathBuf> = sealed_chain.iter().map(PathBuf::from).collect();
        let view: Arc<dyn crate::daemon::lower_view::LowerView> = match &mst2_lower {
            Some(view) => Arc::new(crate::daemon::lower_view::Mst2Lower(view.clone())),
            None => Arc::new(DicfuseLower(dicfuse.store.clone())),
        };
        let changes = effective_changes(view.as_ref(), &upper_dir, &chain_dirs)
            .await
            .map_err(|e| ServiceError::Internal(format!("effective scan failed: {e}")))?;
        let generation = generation_of(&changes);

        Ok(WorktreeStateV2 {
            mount_id: mount_id.to_string(),
            lower_revision: pinned_refs,
            base_revision,
            state: format!("{mount_state:?}").to_lowercase(),
            generation,
            dirty: !changes.is_empty(),
            changes,
        })
    }

    async fn commit_finalize(
        &self,
        mount_id: Uuid,
        request: CommitFinalizeRequest,
    ) -> Result<CommitFinalizeResponse, ServiceError> {
        let start = Instant::now();

        // Phase A — reads and builds only. Every failure below this point leaves
        // the mount, the upper layer, and the bound revision untouched.
        let (path, upper_dir, cl_dir, mountpoint, base_revision, pinned_refs, sealed_chain, mount_state, mst2_lower) = {
            let mounts = self.mounts.read().await;
            let entry = mounts
                .get(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            (
                entry.path.clone(),
                PathBuf::from(&entry.upper_dir),
                entry.cl_dir.clone().map(PathBuf::from),
                PathBuf::from(&entry.mountpoint),
                entry.base_revision.clone(),
                entry.pinned_refs.clone(),
                entry.sealed_chain.clone(),
                entry.state.clone(),
                entry.mst2_lower.is_some(),
            )
        };
        if !matches!(mount_state, MountLifecycle::Mounted | MountLifecycle::Ready) {
            return Err(ServiceError::InvalidRequest(format!(
                "mount {mount_id} is in state {mount_state:?}; cannot finalize a commit"
            )));
        }
        // MST/2-lowered mounts move their lower by resolving a new snapshot, not
        // by re-pinning the Dicfuse projection — refuse rather than run the
        // Dicfuse semantics against the wrong projection (P3; see
        // mst2-impl/P3-HASH-DOMAIN-DESIGN.md).
        if mst2_lower {
            return Err(ServiceError::InvalidRequest(
                "commit-finalize is not supported on an MST/2-lowered mount yet; \
                 re-attach without mst2_lower_enabled or wait for the snapshot-side \
                 finalize"
                    .into(),
            ));
        }

        let chain_dirs: Vec<PathBuf> = sealed_chain.iter().map(PathBuf::from).collect();
        let current = self
            .lower_dicfuse_for(&path, pinned_refs.as_deref())
            .await?;
        let changes = effective_changes(&DicfuseLower(current.store.clone()), &upper_dir, &chain_dirs)
            .await
            .map_err(|e| ServiceError::Internal(format!("effective scan failed: {e}")))?;
        let generation = generation_of(&changes);

        if let Some(expected) = request.expected_generation {
            if expected != generation {
                return Ok(CommitFinalizeResponse {
                    state: "conflict".into(),
                    code: Some("GENERATION_CHANGED".into()),
                    detail: Some(format!(
                        "state generation {expected} no longer matches {generation}; \
                         re-read state and re-commit"
                    )),
                    base_revision: base_revision.unwrap_or_default(),
                    lower_revision: pinned_refs,
                    generation,
                    cleaned_paths: Vec::new(),
                });
            }
        }
        if let Some(expected) = &request.expected_base_revision {
            match &base_revision {
                Some(actual) if actual != expected => {
                    return Ok(CommitFinalizeResponse {
                        state: "conflict".into(),
                        code: Some("BASE_MISMATCH".into()),
                        detail: Some(format!(
                            "bound base revision is {actual}, client expected {expected}"
                        )),
                        base_revision: base_revision.unwrap_or_default(),
                        lower_revision: pinned_refs,
                        generation,
                        cleaned_paths: Vec::new(),
                    });
                }
                _ => {}
            }
        }

        let new_refs = match request.new_base_revision.as_deref().map(str::trim) {
            Some(r) if !r.is_empty() => r.to_string(),
            _ => resolve_latest_revision(config::base_url(), &path)
                .await
                .map_err(ServiceError::Internal)?,
        };

        // A sealed chain is a delta against the OLD revision: keeping it across a
        // lower switch would let stale chain entries shadow the new projection.
        // Flatten it into the upper (nearest wins, upper wins over everything);
        // the chain's O(1) fork cost becomes a one-time O(uncommitted delta) here.
        let chain_empty = chain_dirs.is_empty();
        if !chain_empty {
            let dirs = chain_dirs.clone();
            let upper = upper_dir.clone();
            tokio::task::spawn_blocking(move || flatten_chain_into_upper(&dirs, &upper))
                .await
                .map_err(|e| ServiceError::Internal(format!("chain flatten task failed: {e}")))?
                .map_err(|e| ServiceError::Internal(format!("chain flatten failed: {e}")))?;
        }

        // Already at the requested revision: only the upper cleanup remains, which
        // makes a retried finalize idempotent. The chain is already flattened.
        if pinned_refs.as_deref() == Some(new_refs.as_str()) {
            let cleaned = remove_committed_upper_entries(&upper_dir, &request.committed_paths)
                .map_err(|e| ServiceError::Internal(format!("upper cleanup failed: {e}")))?;
            {
                let mut mounts = self.mounts.write().await;
                if let Some(entry) = mounts.get_mut(&mount_id) {
                    entry.sealed_chain = Vec::new();
                    entry.last_seen_epoch_ms = current_epoch_ms();
                }
            }
            self.persist_state().await;
            let changes = effective_changes(&DicfuseLower(current.store.clone()), &upper_dir, &[])
                .await
                .map_err(|e| ServiceError::Internal(format!("effective scan failed: {e}")))?;
            return Ok(CommitFinalizeResponse {
                state: "ready".into(),
                code: None,
                detail: None,
                base_revision: base_revision.unwrap_or_default(),
                lower_revision: pinned_refs,
                generation: generation_of(&changes),
                cleaned_paths: cleaned,
            });
        }

        let new_dicfuse = DicfuseManager::for_base_path_and_refs(&path, &new_refs).await;
        if tokio::time::timeout(
            Duration::from_secs(180),
            new_dicfuse.store.wait_for_ready(),
        )
        .await
        .is_err()
        {
            return Ok(CommitFinalizeResponse {
                state: "failed".into(),
                code: Some("SWITCH_FAILED".into()),
                detail: Some(format!(
                    "pinned lower for revision {new_refs} did not become ready"
                )),
                base_revision: base_revision.unwrap_or_default(),
                lower_revision: pinned_refs,
                generation,
                cleaned_paths: Vec::new(),
            });
        }

        if let Err(detail) =
            Self::verify_committed_paths(&new_dicfuse.store, &request.committed_paths).await
        {
            return Ok(CommitFinalizeResponse {
                state: "conflict".into(),
                code: Some("TREE_MISMATCH".into()),
                detail: Some(detail),
                base_revision: base_revision.unwrap_or_default(),
                lower_revision: pinned_refs,
                generation,
                cleaned_paths: Vec::new(),
            });
        }

        // Phase B — mutation. Quiesce, remove exactly the committed entries, then
        // remount over the new lower. Rollbacks restore the original chain: the
        // flatten was view-neutral, so the original stack serves the same bytes.
        let old_dicfuse = current;
        {
            let mut mounts = self.mounts.write().await;
            let entry = mounts
                .get_mut(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            entry.state = MountLifecycle::Quiescing;
            if let Err(e) = entry.fuse.unmount().await {
                entry.state = MountLifecycle::Ready;
                return Err(ServiceError::FuseFailure(format!(
                    "failed to quiesce mount {mount_id}: {e}"
                )));
            }
        }

        let cleaned = match remove_committed_upper_entries(&upper_dir, &request.committed_paths)
        {
            Ok(cleaned) => cleaned,
            Err(e) => {
                let _ = Self::remount_with_lower(
                    &mountpoint,
                    old_dicfuse,
                    &upper_dir,
                    cl_dir.as_deref(),
                    &sealed_chain,
                )
                .await;
                return Ok(CommitFinalizeResponse {
                    state: "failed".into(),
                    code: Some("SWITCH_FAILED".into()),
                    detail: Some(format!("upper cleanup failed: {e}")),
                    base_revision: base_revision.unwrap_or_default(),
                    lower_revision: pinned_refs,
                    generation,
                    cleaned_paths: Vec::new(),
                });
            }
        };

        let mut new_fuse = match Self::remount_with_lower(
            &mountpoint,
            new_dicfuse.clone(),
            &upper_dir,
            cl_dir.as_deref(),
            &[],
        )
        .await
        {
            Ok(fuse) => fuse,
            Err(e) => {
                let _ = Self::remount_with_lower(
                    &mountpoint,
                    old_dicfuse,
                    &upper_dir,
                    cl_dir.as_deref(),
                    &sealed_chain,
                )
                .await;
                return Ok(CommitFinalizeResponse {
                    state: "failed".into(),
                    code: Some("SWITCH_FAILED".into()),
                    detail: Some(format!(
                        "remount over revision {new_refs} failed after cleanup: {e}; \
                         worktree remounted on the previous projection"
                    )),
                    base_revision: base_revision.unwrap_or_default(),
                    lower_revision: pinned_refs,
                    generation,
                    cleaned_paths: Vec::new(),
                });
            }
        };

        {
            let mut mounts = self.mounts.write().await;
            let entry = mounts
                .get_mut(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            entry.fuse = new_fuse;
            entry.pinned_refs = Some(new_refs.clone());
            // Flattened above: the chain no longer applies to the new revision.
            entry.sealed_chain = Vec::new();
            entry.state = MountLifecycle::Ready;
            entry.last_seen_epoch_ms = current_epoch_ms();
        }
        self.persist_state().await;

        // Warm the committed paths into the new store so the first read after a
        // sync cannot race store initialization and come back empty.
        for committed in &request.committed_paths {
            let Some(item) = lower_item_for(&new_dicfuse.store, &committed.path).await else {
                continue;
            };
            if item.is_dir() {
                continue;
            }
            let ino = item.get_inode();
            let oid = item.hash.clone();
            let _ = new_dicfuse.store.fetch_file_content(ino, &oid).await;
        }

        let changes = effective_changes(&DicfuseLower(new_dicfuse.store.clone()), &upper_dir, &[])
            .await
            .map_err(|e| ServiceError::Internal(format!("effective scan failed: {e}")))?;
        let generation = generation_of(&changes);

        tracing::info!(
            mount_id = %mount_id,
            lower_revision = %new_refs,
            cleaned = cleaned.len(),
            dirty = !changes.is_empty(),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "antares svc: commit_finalize success"
        );

        Ok(CommitFinalizeResponse {
            state: "ready".into(),
            code: None,
            detail: None,
            base_revision: base_revision.unwrap_or_default(),
            lower_revision: Some(new_refs),
            generation,
            cleaned_paths: cleaned,
        })
    }

    async fn refresh_lower(
        &self,
        mount_id: Uuid,
        request: RefreshRequest,
    ) -> Result<RefreshResponse, ServiceError> {
        let start = Instant::now();
        let (path, upper_dir, cl_dir, mountpoint, base_revision, pinned_refs, sealed_chain, mount_state, mst2_lower) = {
            let mounts = self.mounts.read().await;
            let entry = mounts
                .get(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            (
                entry.path.clone(),
                PathBuf::from(&entry.upper_dir),
                entry.cl_dir.clone().map(PathBuf::from),
                PathBuf::from(&entry.mountpoint),
                entry.base_revision.clone(),
                entry.pinned_refs.clone(),
                entry.sealed_chain.clone(),
                entry.state.clone(),
                entry.mst2_lower.is_some(),
            )
        };
        if !matches!(mount_state, MountLifecycle::Mounted | MountLifecycle::Ready) {
            return Err(ServiceError::InvalidRequest(format!(
                "mount {mount_id} is in state {mount_state:?}; cannot refresh the lower"
            )));
        }
        // MST/2-lowered mounts move their lower by resolving a new snapshot, not
        // by re-pinning the Dicfuse projection — refuse rather than run the
        // Dicfuse semantics against the wrong projection (P3; see
        // mst2-impl/P3-HASH-DOMAIN-DESIGN.md).
        if mst2_lower {
            return Err(ServiceError::InvalidRequest(
                "refresh is not supported on an MST/2-lowered mount yet; the snapshot \
                 view is pinned at attach time and follows its own refresh path"
                    .into(),
            ));
        }

        let chain_dirs: Vec<PathBuf> = sealed_chain.iter().map(PathBuf::from).collect();
        let current = self
            .lower_dicfuse_for(&path, pinned_refs.as_deref())
            .await?;
        let changes = effective_changes(&DicfuseLower(current.store.clone()), &upper_dir, &chain_dirs)
            .await
            .map_err(|e| ServiceError::Internal(format!("effective scan failed: {e}")))?;
        let generation = generation_of(&changes);

        if request.require_clean && !changes.is_empty() {
            return Ok(RefreshResponse {
                disposition: RefreshDisposition::BlockedDirty,
                base_revision: base_revision.unwrap_or_default(),
                lower_revision: pinned_refs,
                generation,
                code: Some("BLOCKED_DIRTY".into()),
                detail: Some(format!(
                    "{} effective change(s) exist; commit or stash before refreshing",
                    changes.len()
                )),
            });
        }

        let target = match request.target_revision.as_deref().map(str::trim) {
            Some(r) if !r.is_empty() => r.to_string(),
            _ => resolve_latest_revision(config::base_url(), &path)
                .await
                .map_err(ServiceError::Internal)?,
        };

        if pinned_refs.as_deref() == Some(target.as_str()) {
            return Ok(RefreshResponse {
                disposition: RefreshDisposition::AlreadyAtTarget,
                base_revision: base_revision.unwrap_or_default(),
                lower_revision: pinned_refs,
                generation,
                code: None,
                detail: None,
            });
        }

        // Flatten before the switch: a sealed chain is a delta against the OLD
        // revision and must not shadow the new projection.
        if !chain_dirs.is_empty() {
            let dirs = chain_dirs.clone();
            let upper = upper_dir.clone();
            tokio::task::spawn_blocking(move || flatten_chain_into_upper(&dirs, &upper))
                .await
                .map_err(|e| ServiceError::Internal(format!("chain flatten task failed: {e}")))?
                .map_err(|e| ServiceError::Internal(format!("chain flatten failed: {e}")))?;
        }

        let new_dicfuse = DicfuseManager::for_base_path_and_refs(&path, &target).await;
        if tokio::time::timeout(
            Duration::from_secs(180),
            new_dicfuse.store.wait_for_ready(),
        )
        .await
        .is_err()
        {
            return Ok(RefreshResponse {
                disposition: RefreshDisposition::BaseMismatch,
                base_revision: base_revision.unwrap_or_default(),
                lower_revision: pinned_refs,
                generation,
                code: Some("SWITCH_FAILED".into()),
                detail: Some(format!("lower for revision {target} did not become ready")),
            });
        }

        let old_dicfuse = current;
        {
            let mut mounts = self.mounts.write().await;
            let entry = mounts
                .get_mut(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            entry.state = MountLifecycle::Quiescing;
            if let Err(e) = entry.fuse.unmount().await {
                entry.state = MountLifecycle::Ready;
                return Err(ServiceError::FuseFailure(format!(
                    "failed to quiesce mount {mount_id}: {e}"
                )));
            }
        }

        let mut new_fuse = match Self::remount_with_lower(
            &mountpoint,
            new_dicfuse.clone(),
            &upper_dir,
            cl_dir.as_deref(),
            &[],
        )
        .await
        {
            Ok(fuse) => fuse,
            Err(e) => {
                let _ = Self::remount_with_lower(
                    &mountpoint,
                    old_dicfuse,
                    &upper_dir,
                    cl_dir.as_deref(),
                    &sealed_chain,
                )
                .await;
                return Ok(RefreshResponse {
                    disposition: RefreshDisposition::BaseMismatch,
                    base_revision: base_revision.unwrap_or_default(),
                    lower_revision: pinned_refs,
                    generation,
                    code: Some("SWITCH_FAILED".into()),
                    detail: Some(format!(
                        "remount over revision {target} failed: {e}; \
                         worktree remounted on the previous projection"
                    )),
                });
            }
        };

        {
            let mut mounts = self.mounts.write().await;
            let entry = mounts
                .get_mut(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            entry.fuse = new_fuse;
            entry.pinned_refs = Some(target.clone());
            // Flattened above: the chain no longer applies to the new revision.
            entry.sealed_chain = Vec::new();
            entry.state = MountLifecycle::Ready;
            entry.last_seen_epoch_ms = current_epoch_ms();
        }
        self.persist_state().await;

        let changes = effective_changes(&DicfuseLower(new_dicfuse.store.clone()), &upper_dir, &[])
            .await
            .map_err(|e| ServiceError::Internal(format!("effective scan failed: {e}")))?;
        let generation = generation_of(&changes);

        tracing::info!(
            mount_id = %mount_id,
            lower_revision = %target,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "antares svc: refresh_lower success"
        );

        Ok(RefreshResponse {
            disposition: RefreshDisposition::Switched,
            base_revision: base_revision.unwrap_or_default(),
            lower_revision: Some(target),
            generation,
            code: None,
            detail: None,
        })
    }

    async fn delete_mount(&self, mount_id: Uuid) -> Result<MountStatus, ServiceError> {
        let start = Instant::now();
        // Acquire write locks to update state
        let mut mounts = self.mounts.write().await;
        let index = self.path_index.write().await;

        // Get mutable reference to entry (don't remove yet)
        let entry = mounts
            .get_mut(&mount_id)
            .ok_or(ServiceError::NotFound(mount_id))?;

        if matches!(
            entry.state,
            MountLifecycle::Quiescing | MountLifecycle::Unmounting
        ) {
            return Err(ServiceError::InvalidRequest(format!(
                "mount {} is currently in state {:?}; retry after switch/unmount completes",
                mount_id, entry.state
            )));
        }

        // Cancel any in-flight deep-preload walk so it stops quickly.
        entry.preload_cancel.store(true, Ordering::Relaxed);

        // Set state to Unmounting while still in the map
        entry.state = MountLifecycle::Unmounting;
        entry.update_last_seen();

        // Store path/cl for index removal, then take ownership of fuse for unmount
        let path = entry.path.clone();
        let cl = entry.cl.clone();
        let job_id = entry.job_id.clone();
        let job_id_for_log = job_id.clone();
        let sealed_chain = entry.sealed_chain.clone();
        tracing::info!(
            mount_id = %mount_id,
            task_id = ?job_id_for_log,
            path = %path,
            cl = ?cl,
            mountpoint = %entry.mountpoint,
            "antares svc: delete_mount start"
        );
        let mountpoint = PathBuf::from(&entry.mountpoint);
        let upper_dir = PathBuf::from(&entry.upper_dir);
        let cl_dir = entry.cl_dir.as_ref().map(PathBuf::from);
        let mut fuse = std::mem::replace(&mut entry.fuse, {
            // Create a placeholder AntaresFuse to replace (will be removed anyway if unmount succeeds)
            // This is safe because we're about to remove the entry on success, or restore fuse on failure
            AntaresFuse::new(
                mountpoint.clone(),
                self.dicfuse.clone(),
                upper_dir.clone(),
                cl_dir.clone(),
            )
            .await
            .map_err(|e| {
                ServiceError::Internal(format!("failed to create placeholder fuse: {}", e))
            })?
        });

        // Release locks before potentially slow unmount operation
        drop(mounts);
        drop(index);

        // Unmount the filesystem
        let unmount_result = fuse.unmount().await;

        // Reacquire locks to update state and remove if needed
        let mut mounts = self.mounts.write().await;
        let mut index = self.path_index.write().await;
        let mut job_index = self.job_index.write().await;

        let entry = match mounts.get_mut(&mount_id) {
            Some(entry) => entry,
            None => {
                tracing::error!(
                    "Mount entry {} missing during unmount; possible race or state bug",
                    mount_id
                );
                drop(mounts);
                drop(index);
                drop(job_index);
                return Err(ServiceError::Internal(format!(
                    "Mount entry {} not found during unmount; this should not happen",
                    mount_id
                )));
            }
        };

        if let Err(e) = unmount_result {
            tracing::error!(
                mount_id = %mount_id,
                task_id = ?job_id_for_log,
                elapsed_ms = start.elapsed().as_millis(),
                error = %e,
                "antares svc: delete_mount unmount failed"
            );
            // Put fuse back since unmount failed
            entry.fuse = fuse;
            entry.state = MountLifecycle::Failed {
                reason: format!("unmount failed: {}", e),
            };
            entry.update_last_seen();
            // Do not remove from mounts or index; keep for tracking failed unmounts
            let status = entry.to_status();
            drop(mounts);
            drop(index);
            drop(job_index);
            return Ok(status);
        } else {
            entry.state = MountLifecycle::Unmounted;
            entry.update_last_seen();
            // Remove from mounts and index only after successful unmount
            let cl_path = entry.cl_path.clone();
            let status = entry.to_status();
            mounts.remove(&mount_id);
            if let Some(job_id) = job_id {
                job_index.remove(&job_id);
            } else {
                index.remove(&(path, cl, cl_path));
            }
            // Sealed layers of a chain fork are shared: reclaim one only when no
            // surviving mount still references it. Computed before the locks drop.
            let orphaned = if sealed_chain.is_empty() {
                Vec::new()
            } else {
                let referenced: std::collections::HashSet<&str> = mounts
                    .values()
                    .flat_map(|e| e.sealed_chain.iter().map(String::as_str))
                    .collect();
                sealed_chain
                    .iter()
                    .filter(|d| !referenced.contains(d.as_str()))
                    .cloned()
                    .collect()
            };
            drop(mounts);
            drop(index);
            drop(job_index);
            tracing::info!(
                mount_id = %mount_id,
                task_id = ?job_id_for_log,
                elapsed_ms = start.elapsed().as_millis(),
                "antares svc: delete_mount success"
            );

            // The instance is gone from the maps and cannot be reattached, so
            // reclaim its mountpoint and private layers (outside the locks).
            Self::remove_mount_dirs(mount_id, &mountpoint, &upper_dir, cl_dir.as_deref());
            for dir in orphaned {
                if let Err(e) = tokio::fs::remove_dir_all(&dir).await {
                    tracing::warn!("sealed layer {} cleanup failed: {e}", dir);
                }
            }

            // Persist state to file for recovery
            self.persist_state().await;

            Ok(status)
        }
    }

    async fn build_cl(&self, mount_id: Uuid, cl_link: String) -> Result<MountStatus, ServiceError> {
        let mut mounts = self.mounts.write().await;
        let index = self.path_index.write().await;

        let entry = mounts
            .get_mut(&mount_id)
            .ok_or(ServiceError::NotFound(mount_id))?;
        if !matches!(entry.state, MountLifecycle::Mounted | MountLifecycle::Ready) {
            return Err(ServiceError::InvalidRequest(format!(
                "mount {} is currently in state {:?}; cannot build CL",
                mount_id, entry.state
            )));
        }
        if entry.base_revision.is_some() {
            return Err(ServiceError::InvalidRequest(
                "cannot modify a CL layer after binding a Libra worktree base".into(),
            ));
        }

        let cl_root = crate::util::config::antares_cl_root();
        let cl_dir_str = format!("{}/{}", cl_root, mount_id);
        let cl_dir_path = PathBuf::from(&cl_dir_str);
        let quiesce_grace = Self::cl_quiesce_grace_duration();
        let path = entry.path.clone();
        let job_id = entry.job_id.clone();
        let old_cl = entry.cl.clone();
        let cl_path = entry.cl_path.clone();
        let mountpoint = PathBuf::from(&entry.mountpoint);
        let upper_dir = PathBuf::from(&entry.upper_dir);
        let existing_cl_dir = entry.cl_dir.as_ref().map(PathBuf::from);
        let dicfuse = entry.fuse.dic.clone();
        // Cancel any in-flight deep-preload walk before unmounting.
        entry.preload_cancel.store(true, Ordering::Relaxed);
        // Enter a short quiescing window so control-plane operations reject this mount
        // while we prepare to remount with the new CL layer.
        entry.state = MountLifecycle::Quiescing;
        entry.update_last_seen();
        let mut old_fuse = std::mem::replace(&mut entry.fuse, {
            AntaresFuse::new(
                mountpoint.clone(),
                self.dicfuse.clone(),
                upper_dir.clone(),
                existing_cl_dir.clone(),
            )
            .await
            .map_err(|e| {
                ServiceError::Internal(format!("failed to create placeholder fuse: {}", e))
            })?
        });

        drop(mounts);
        drop(index);
        if !quiesce_grace.is_zero() {
            tracing::info!(
                mount_id = %mount_id,
                grace_ms = quiesce_grace.as_millis(),
                "antares svc: build_cl quiescing before remount"
            );
            sleep(quiesce_grace).await;
        }

        if let Err(e) = old_fuse.unmount().await {
            tracing::error!("Failed to unmount {}: {}", mount_id, e);
            let mut mounts = self.mounts.write().await;
            let index = self.path_index.write().await;
            let _job_index = self.job_index.write().await;
            if let Some(entry) = mounts.get_mut(&mount_id) {
                entry.fuse = old_fuse;
                entry.state = MountLifecycle::Failed {
                    reason: format!("unmount failed: {}", e),
                };
                entry.update_last_seen();
            }
            drop(mounts);
            drop(index);
            drop(_job_index);
            return Err(ServiceError::FuseFailure(format!("unmount failed: {}", e)));
        }

        if let Err(e) = self
            .build_cl_layer(
                &path,
                cl_path.as_deref().unwrap_or(&path),
                &cl_link,
                &cl_dir_path,
            )
            .await
        {
            tracing::error!("Failed to build CL layer for {}: {}", mount_id, e);
            let remount_result = old_fuse.mount().await;
            let mut mounts = self.mounts.write().await;
            let index = self.path_index.write().await;
            let _job_index = self.job_index.write().await;
            if let Some(entry) = mounts.get_mut(&mount_id) {
                entry.fuse = old_fuse;
                entry.state = if let Err(remount_err) = remount_result {
                    MountLifecycle::Failed {
                        reason: format!("remount after CL failure: {}", remount_err),
                    }
                } else {
                    MountLifecycle::Mounted
                };
                entry.update_last_seen();
            }
            drop(mounts);
            drop(index);
            drop(_job_index);
            return Err(e);
        }

        let mut new_fuse = AntaresFuse::new(
            mountpoint.clone(),
            dicfuse,
            upper_dir.clone(),
            Some(cl_dir_path.clone()),
        )
        .await
        .map_err(|e| ServiceError::FuseFailure(format!("failed to create fuse: {}", e)))?;
        if let Err(e) = new_fuse.mount().await {
            tracing::error!("Failed to remount {} with CL: {}", mount_id, e);
            let remount_result = old_fuse.mount().await;
            let mut mounts = self.mounts.write().await;
            let index = self.path_index.write().await;
            let _job_index = self.job_index.write().await;
            if let Some(entry) = mounts.get_mut(&mount_id) {
                entry.fuse = old_fuse;
                entry.state = if let Err(remount_err) = remount_result {
                    MountLifecycle::Failed {
                        reason: format!("remount after CL failure: {}", remount_err),
                    }
                } else {
                    MountLifecycle::Mounted
                };
                entry.update_last_seen();
            }
            drop(mounts);
            drop(index);
            drop(_job_index);
            return Err(ServiceError::FuseFailure(format!(
                "failed to mount CL view: {}",
                e
            )));
        }

        let mut mounts = self.mounts.write().await;
        let mut index = self.path_index.write().await;
        let _job_index = self.job_index.write().await;
        let entry = mounts
            .get_mut(&mount_id)
            .ok_or(ServiceError::NotFound(mount_id))?;

        // Reset the cancel flag and assign a fresh one for the next preload cycle.
        let new_cancel = Arc::new(AtomicBool::new(false));
        entry.fuse = new_fuse;
        entry.cl = Some(cl_link.clone());
        entry.cl_dir = Some(cl_dir_str);
        // Transition directly to Ready — Dicfuse cache is already warm.
        entry.state = MountLifecycle::Ready;
        entry.preload_cancel = new_cancel.clone();
        entry.update_last_seen();

        if job_id.is_none() && old_cl != entry.cl {
            let path = entry.path.clone();
            index.remove(&(path.clone(), old_cl, entry.cl_path.clone()));
            index.insert((path, entry.cl.clone(), entry.cl_path.clone()), mount_id);
        }

        let mountpoint_for_preload = entry.mountpoint.clone();
        let status = entry.to_status();
        tracing::info!(
            "Built CL layer for mount {} with link {}",
            mount_id,
            cl_link
        );
        drop(mounts);
        drop(index);
        drop(_job_index);

        self.persist_state().await;

        // Best-effort: re-warm kernel FUSE caches after remount.
        self.spawn_deep_preload_task(mount_id, mountpoint_for_preload, new_cancel, "build_cl");

        Ok(status)
    }

    async fn clear_cl(&self, mount_id: Uuid) -> Result<MountStatus, ServiceError> {
        let mut mounts = self.mounts.write().await;
        let index = self.path_index.write().await;

        let entry = mounts
            .get_mut(&mount_id)
            .ok_or(ServiceError::NotFound(mount_id))?;
        if !matches!(entry.state, MountLifecycle::Mounted | MountLifecycle::Ready) {
            return Err(ServiceError::InvalidRequest(format!(
                "mount {} is currently in state {:?}; cannot clear CL",
                mount_id, entry.state
            )));
        }
        if entry.base_revision.is_some() {
            return Err(ServiceError::InvalidRequest(
                "cannot modify a CL layer after binding a Libra worktree base".into(),
            ));
        }

        if entry.cl.is_none() {
            return Err(ServiceError::InvalidRequest(
                "mount has no CL layer to clear".into(),
            ));
        }

        let path = entry.path.clone();
        let job_id = entry.job_id.clone();
        let old_cl = entry.cl.clone();
        let cl_path = entry.cl_path.clone();
        let quiesce_grace = Self::cl_quiesce_grace_duration();
        let mountpoint = PathBuf::from(&entry.mountpoint);
        let upper_dir = PathBuf::from(&entry.upper_dir);
        let existing_cl_dir = entry.cl_dir.as_ref().map(PathBuf::from);
        let dicfuse = entry.fuse.dic.clone();
        // Cancel any in-flight deep-preload walk before unmounting.
        entry.preload_cancel.store(true, Ordering::Relaxed);
        // Enter a short quiescing window so control-plane operations reject this mount
        // while we prepare to remount without CL.
        entry.state = MountLifecycle::Quiescing;
        entry.update_last_seen();
        let mut old_fuse = std::mem::replace(&mut entry.fuse, {
            AntaresFuse::new(
                mountpoint.clone(),
                self.dicfuse.clone(),
                upper_dir.clone(),
                existing_cl_dir.clone(),
            )
            .await
            .map_err(|e| {
                ServiceError::Internal(format!("failed to create placeholder fuse: {}", e))
            })?
        });

        drop(mounts);
        drop(index);
        if !quiesce_grace.is_zero() {
            tracing::info!(
                mount_id = %mount_id,
                grace_ms = quiesce_grace.as_millis(),
                "antares svc: clear_cl quiescing before remount"
            );
            sleep(quiesce_grace).await;
        }

        if let Err(e) = old_fuse.unmount().await {
            tracing::error!("Failed to unmount {}: {}", mount_id, e);
            let mut mounts = self.mounts.write().await;
            let index = self.path_index.write().await;
            let _job_index = self.job_index.write().await;
            if let Some(entry) = mounts.get_mut(&mount_id) {
                entry.fuse = old_fuse;
                entry.state = MountLifecycle::Failed {
                    reason: format!("unmount failed: {}", e),
                };
                entry.update_last_seen();
            }
            drop(mounts);
            drop(index);
            drop(_job_index);
            return Err(ServiceError::FuseFailure(format!("unmount failed: {}", e)));
        }

        if let Some(cl_dir) = &existing_cl_dir {
            if cl_dir.exists() {
                if let Err(e) = std::fs::remove_dir_all(cl_dir) {
                    tracing::warn!("Failed to remove CL directory {:?}: {}", cl_dir, e);
                }
            }
        }

        let mut new_fuse = AntaresFuse::new(mountpoint.clone(), dicfuse, upper_dir.clone(), None)
            .await
            .map_err(|e| ServiceError::FuseFailure(format!("failed to create fuse: {}", e)))?;
        if let Err(e) = new_fuse.mount().await {
            tracing::error!("Failed to remount {} without CL: {}", mount_id, e);
            let remount_result = old_fuse.mount().await;
            let mut mounts = self.mounts.write().await;
            let index = self.path_index.write().await;
            let _job_index = self.job_index.write().await;
            if let Some(entry) = mounts.get_mut(&mount_id) {
                entry.fuse = old_fuse;
                entry.state = if let Err(remount_err) = remount_result {
                    MountLifecycle::Failed {
                        reason: format!("remount after clear CL failure: {}", remount_err),
                    }
                } else {
                    MountLifecycle::Mounted
                };
                entry.update_last_seen();
            }
            drop(mounts);
            drop(index);
            drop(_job_index);
            return Err(ServiceError::FuseFailure(format!(
                "failed to remount without CL: {}",
                e
            )));
        }

        let mut mounts = self.mounts.write().await;
        let mut index = self.path_index.write().await;
        let _job_index = self.job_index.write().await;
        let entry = mounts
            .get_mut(&mount_id)
            .ok_or(ServiceError::NotFound(mount_id))?;

        let new_cancel = Arc::new(AtomicBool::new(false));
        entry.fuse = new_fuse;
        entry.cl = None;
        entry.cl_dir = None;
        // Transition directly to Ready — Dicfuse cache is already warm.
        entry.state = MountLifecycle::Ready;
        entry.preload_cancel = new_cancel.clone();
        entry.update_last_seen();

        if job_id.is_none() {
            index.remove(&(path.clone(), old_cl, cl_path.clone()));
            index.insert((path, None, cl_path), mount_id);
        }

        let mountpoint_for_preload = entry.mountpoint.clone();
        let status = entry.to_status();
        tracing::info!("Cleared CL layer for mount {}", mount_id);
        drop(mounts);
        drop(index);
        drop(_job_index);

        self.persist_state().await;

        // Best-effort: re-warm kernel FUSE caches after remount.
        self.spawn_deep_preload_task(mount_id, mountpoint_for_preload, new_cancel, "clear_cl");

        Ok(status)
    }

    async fn check_mount_ready(&self, mount_id: Uuid) -> Result<MountReadyResponse, ServiceError> {
        let mounts = self.mounts.read().await;
        let entry = mounts
            .get(&mount_id)
            .ok_or(ServiceError::NotFound(mount_id))?;
        let ready = entry.state == MountLifecycle::Ready;
        Ok(MountReadyResponse {
            mount_id,
            ready,
            state: entry.state.clone(),
        })
    }

    async fn health_info(&self) -> HealthResponse {
        self.health_info_impl().await
    }

    async fn shutdown_cleanup(&self) -> Result<(), ServiceError> {
        self.shutdown_cleanup_impl().await
    }
}

#[derive(Debug, Clone, Copy)]
enum DeepPreloadMode {
    ScanOnly,
    Full,
    Hotset,
    DirsOnly,
}

impl DeepPreloadMode {
    fn as_str(self) -> &'static str {
        match self {
            DeepPreloadMode::ScanOnly => "scan_only",
            DeepPreloadMode::Full => "full",
            DeepPreloadMode::Hotset => "hotset",
            DeepPreloadMode::DirsOnly => "dirs_only",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DeepPreloadStats {
    entries_visited: usize,
    metadata_touches: usize,
    budget_exhausted: bool,
}

fn deep_preload_mode() -> DeepPreloadMode {
    match std::env::var("ANTARES_DEEP_PRELOAD_MODE") {
        Ok(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "scan" | "scan_only" | "readdirplus" => DeepPreloadMode::ScanOnly,
                "full" => DeepPreloadMode::Full,
                "dirs" | "dirs_only" => DeepPreloadMode::DirsOnly,
                "hotset" | "" => DeepPreloadMode::Hotset,
                _ => {
                    tracing::warn!(
                        value = %raw,
                        "invalid ANTARES_DEEP_PRELOAD_MODE, expected one of: scan|hotset|full|dirs"
                    );
                    DeepPreloadMode::ScanOnly
                }
            }
        }
        Err(_) => DeepPreloadMode::ScanOnly,
    }
}

fn deep_preload_should_touch_metadata(
    mode: DeepPreloadMode,
    file_type: &std::fs::FileType,
    path: &Path,
) -> bool {
    match mode {
        DeepPreloadMode::ScanOnly => false,
        DeepPreloadMode::Full => true,
        DeepPreloadMode::DirsOnly => file_type.is_dir(),
        DeepPreloadMode::Hotset => {
            if file_type.is_dir() {
                return true;
            }
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if matches!(
                name,
                "BUCK"
                    | "BUCK.v2"
                    | "BUILD"
                    | "BUILD.bazel"
                    | "PACKAGE"
                    | "TARGETS"
                    | "TARGETS.v2"
                    | "WORKSPACE"
                    | "WORKSPACE.bazel"
                    | ".buckconfig"
            ) {
                return true;
            }
            matches!(
                path.extension().and_then(|s| s.to_str()),
                Some("bzl") | Some("bxl")
            )
        }
    }
}

fn deep_preload_worker_count() -> usize {
    let default_workers = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(2, 8);
    match std::env::var("ANTARES_DEEP_PRELOAD_WORKERS") {
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(n) => n.clamp(1, 64),
            Err(_) => {
                tracing::warn!(
                    value = %raw,
                    default_workers,
                    "invalid ANTARES_DEEP_PRELOAD_WORKERS, using default"
                );
                default_workers
            }
        },
        Err(_) => default_workers,
    }
}

fn deep_preload_max_duration() -> Option<Duration> {
    const DEFAULT_MS: u64 = 8_000;
    match std::env::var("ANTARES_DEEP_PRELOAD_MAX_MS") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(ms) => Some(Duration::from_millis(ms.min(120_000))),
            Err(_) => {
                tracing::warn!(
                    value = %raw,
                    default_ms = DEFAULT_MS,
                    "invalid ANTARES_DEEP_PRELOAD_MAX_MS, using default"
                );
                Some(Duration::from_millis(DEFAULT_MS))
            }
        },
        Err(_) => Some(Duration::from_millis(DEFAULT_MS)),
    }
}

fn deep_preload_max_depth() -> usize {
    const DEFAULT_DEPTH: usize = 4;
    match std::env::var("ANTARES_DEEP_PRELOAD_MAX_DEPTH") {
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(depth) => depth.min(64),
            Err(_) => {
                tracing::warn!(
                    value = %raw,
                    default_depth = DEFAULT_DEPTH,
                    "invalid ANTARES_DEEP_PRELOAD_MAX_DEPTH, using default"
                );
                DEFAULT_DEPTH
            }
        },
        Err(_) => DEFAULT_DEPTH,
    }
}

/// Walk a directory tree with bounded parallelism to warm FUSE kernel caches.
///
/// Strategy is configurable:
/// - `scan` (default): traverse directories only (readdir/readdirplus-driven)
/// - `hotset`: touch metadata for directories + Buck hot files
/// - `dirs`: touch metadata only for directories
/// - `full`: touch metadata for every entry (most expensive)
/// - `ANTARES_DEEP_PRELOAD_MAX_MS`: cap total background warmup time
fn deep_preload_walk(root: &str, cancel: &AtomicBool) -> std::io::Result<DeepPreloadStats> {
    use std::fs;

    #[derive(Default)]
    struct WalkState {
        queue: VecDeque<(PathBuf, usize)>,
        in_flight: usize,
        done: bool,
    }

    // Bounded by default, but override-able for host-specific tuning.
    let workers = deep_preload_worker_count();
    let mode = deep_preload_mode();
    let max_depth = deep_preload_max_depth();
    let max_duration = deep_preload_max_duration();
    tracing::info!(
        root = root,
        workers,
        mode = mode.as_str(),
        max_depth,
        max_ms = max_duration.map(|d| d.as_millis()),
        "deep_preload_walk: start"
    );
    let started_at = Instant::now();
    let root_path = PathBuf::from(root);
    let total_entries = Arc::new(AtomicUsize::new(0));
    let total_touches = Arc::new(AtomicUsize::new(0));
    let budget_exhausted = Arc::new(AtomicBool::new(false));
    let state = Arc::new((
        Mutex::new(WalkState {
            queue: VecDeque::from([(root_path, 0)]),
            in_flight: 0,
            done: false,
        }),
        Condvar::new(),
    ));

    thread::scope(|scope| {
        for _ in 0..workers {
            let state = Arc::clone(&state);
            let total_entries = Arc::clone(&total_entries);
            let total_touches = Arc::clone(&total_touches);
            let budget_exhausted = Arc::clone(&budget_exhausted);
            scope.spawn(move || {
                let mut local_entries = 0usize;
                let mut local_touches = 0usize;

                loop {
                    if let Some(max_dur) = max_duration {
                        if started_at.elapsed() >= max_dur {
                            budget_exhausted.store(true, Ordering::Relaxed);
                            let (lock, cv) = &*state;
                            let mut guard = lock.lock().expect("deep_preload_walk lock poisoned");
                            guard.done = true;
                            cv.notify_all();
                            break;
                        }
                    }
                    if cancel.load(Ordering::Relaxed) {
                        let (lock, cv) = &*state;
                        let mut guard = lock.lock().expect("deep_preload_walk lock poisoned");
                        guard.done = true;
                        cv.notify_all();
                        break;
                    }

                    let dir = {
                        let (lock, cv) = &*state;
                        let mut guard = lock.lock().expect("deep_preload_walk lock poisoned");
                        loop {
                            if guard.done {
                                break None;
                            }
                            if let Some(dir) = guard.queue.pop_front() {
                                guard.in_flight += 1;
                                break Some(dir);
                            }
                            if guard.in_flight == 0 {
                                guard.done = true;
                                cv.notify_all();
                                break None;
                            }
                            guard = cv.wait(guard).expect("deep_preload_walk lock poisoned");
                        }
                    };

                    let Some((dir, depth)) = dir else {
                        break;
                    };

                    let mut discovered_dirs: Vec<(PathBuf, usize)> = Vec::new();
                    let entries = match fs::read_dir(&dir) {
                        Ok(entries) => entries,
                        Err(e) => {
                            tracing::warn!(dir = ?dir, error = %e, "deep_preload_walk: read_dir failed");
                            let (lock, cv) = &*state;
                            let mut guard = lock.lock().expect("deep_preload_walk lock poisoned");
                            guard.in_flight = guard.in_flight.saturating_sub(1);
                            if guard.queue.is_empty() && guard.in_flight == 0 {
                                guard.done = true;
                            }
                            cv.notify_all();
                            continue;
                        }
                    };

                    for entry in entries {
                        if let Some(max_dur) = max_duration {
                            if started_at.elapsed() >= max_dur {
                                budget_exhausted.store(true, Ordering::Relaxed);
                                break;
                            }
                        }
                        if cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let entry = match entry {
                            Ok(e) => e,
                            Err(e) => {
                                tracing::warn!(dir = ?dir, error = %e, "deep_preload_walk: entry error");
                                continue;
                            }
                        };

                        let path = entry.path();
                        let file_type = match entry.file_type() {
                            Ok(ft) => ft,
                            Err(e) => {
                                tracing::warn!(
                                    path = ?path,
                                    error = %e,
                                    "deep_preload_walk: file_type error"
                                );
                                continue;
                            }
                        };
                        local_entries += 1;

                        if file_type.is_dir() && depth < max_depth {
                            discovered_dirs.push((path.clone(), depth + 1));
                        }

                        if deep_preload_should_touch_metadata(mode, &file_type, &path) {
                            // Touch metadata to warm FUSE attr cache for selected hot paths.
                            let _ = entry.metadata();
                            local_touches += 1;
                        }
                    }

                    let (lock, cv) = &*state;
                    let mut guard = lock.lock().expect("deep_preload_walk lock poisoned");
                    for subdir in discovered_dirs {
                        guard.queue.push_back(subdir);
                    }
                    guard.in_flight = guard.in_flight.saturating_sub(1);
                    if guard.queue.is_empty() && guard.in_flight == 0 {
                        guard.done = true;
                    }
                    cv.notify_all();
                }

                total_entries.fetch_add(local_entries, Ordering::Relaxed);
                total_touches.fetch_add(local_touches, Ordering::Relaxed);
            });
        }
    });

    let stats = DeepPreloadStats {
        entries_visited: total_entries.load(Ordering::Relaxed),
        metadata_touches: total_touches.load(Ordering::Relaxed),
        budget_exhausted: budget_exhausted.load(Ordering::Relaxed),
    };
    if stats.budget_exhausted {
        tracing::info!(
            root = root,
            visited = stats.entries_visited,
            metadata_touches = stats.metadata_touches,
            "deep_preload_walk: time budget exhausted"
        );
    }
    if cancel.load(Ordering::Relaxed) {
        tracing::info!(
            root = root,
            visited = stats.entries_visited,
            metadata_touches = stats.metadata_touches,
            "deep_preload_walk: cancelled"
        );
    }
    Ok(stats)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use futures::future::join_all;
    use tower::ServiceExt;

    use super::*;

    /// Mock service for testing HTTP layer without actual FUSE operations
    struct MockAntaresService {
        mounts: Arc<RwLock<HashMap<Uuid, MountStatus>>>,
    }

    impl MockAntaresService {
        fn new() -> Self {
            Self {
                mounts: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl AntaresService for MockAntaresService {
        async fn create_mount(
            &self,
            request: CreateMountRequest,
        ) -> Result<MountCreated, ServiceError> {
            if request.path.is_empty() {
                return Err(ServiceError::InvalidRequest("path cannot be empty".into()));
            }

            let task_id = request.job_id.clone().or(request.build_id.clone());

            // Idempotency / de-dup policy:
            // - If task_id is provided: idempotent per task id.
            // - Otherwise: legacy behavior, reject duplicate (path, cl).
            if let Some(ref job_id) = task_id {
                let mounts = self.mounts.read().await;
                if let Some(existing) = mounts
                    .values()
                    .find(|m| m.job_id.as_deref() == Some(job_id))
                {
                    if existing.path != request.path || existing.cl != request.cl {
                        return Err(ServiceError::InvalidRequest(format!(
                            "job_id/build_id '{}' already mounted with different path/cl",
                            job_id
                        )));
                    }
                    if !matches!(
                        existing.state,
                        MountLifecycle::Mounted | MountLifecycle::Ready
                    ) {
                        return Err(ServiceError::InvalidRequest(format!(
                            "job_id/build_id '{}' is currently in state {:?}; retry after unmount completes",
                            job_id, existing.state
                        )));
                    }
                    return Ok(MountCreated {
                        mount_id: existing.mount_id,
                        mountpoint: existing.mountpoint.clone(),
                    });
                }
            } else {
                let mounts = self.mounts.read().await;
                if mounts
                    .values()
                    .any(|m| m.path == request.path && m.cl == request.cl)
                {
                    return Err(ServiceError::InvalidRequest(format!(
                        "path {} with cl {:?} is already mounted",
                        request.path, request.cl
                    )));
                }
            }

            // Auto-generate paths based on UUID
            let mount_id = Uuid::new_v4();
            let id_str = mount_id.to_string();
            let mountpoint = format!("/tmp/mock_mnt/{}", id_str);
            let upper_dir = format!("/tmp/mock_upper/{}", id_str);
            let cl_dir = request
                .cl
                .as_ref()
                .map(|_| format!("/tmp/mock_cl/{}", id_str));

            let status = MountStatus {
                mount_id,
                job_id: task_id.clone(),
                path: request.path,
                cl: request.cl,
                base_revision: None,
                mountpoint: mountpoint.clone(),
                layers: MountLayers {
                    upper: upper_dir,
                    cl: cl_dir,
                    dicfuse: "mock".into(),
                },
                state: MountLifecycle::Ready,
                created_at_epoch_ms: 0,
                last_seen_epoch_ms: 0,
            };
            self.mounts.write().await.insert(mount_id, status);

            Ok(MountCreated {
                mount_id,
                mountpoint,
            })
        }

        async fn list_mounts(&self) -> Result<Vec<MountStatus>, ServiceError> {
            Ok(self.mounts.read().await.values().cloned().collect())
        }

        async fn describe_mount(&self, mount_id: Uuid) -> Result<MountStatus, ServiceError> {
            self.mounts
                .read()
                .await
                .get(&mount_id)
                .cloned()
                .ok_or(ServiceError::NotFound(mount_id))
        }

        async fn delete_mount(&self, mount_id: Uuid) -> Result<MountStatus, ServiceError> {
            self.mounts
                .write()
                .await
                .remove(&mount_id)
                .map(|mut s| {
                    s.state = MountLifecycle::Unmounted;
                    s
                })
                .ok_or(ServiceError::NotFound(mount_id))
        }

        async fn changed_paths(
            &self,
            mount_id: Uuid,
        ) -> Result<MountChangesResponse, ServiceError> {
            if !self.mounts.read().await.contains_key(&mount_id) {
                return Err(ServiceError::NotFound(mount_id));
            }
            Ok(MountChangesResponse {
                mount_id,
                generation: 0,
                changes: Vec::new(),
            })
        }

        async fn build_cl(
            &self,
            mount_id: Uuid,
            cl_link: String,
        ) -> Result<MountStatus, ServiceError> {
            let mut mounts = self.mounts.write().await;
            let status = mounts
                .get_mut(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            if !matches!(
                status.state,
                MountLifecycle::Mounted | MountLifecycle::Ready
            ) {
                return Err(ServiceError::InvalidRequest(format!(
                    "mount {} is currently in state {:?}; cannot build CL",
                    mount_id, status.state
                )));
            }
            status.cl = Some(cl_link);
            status.layers.cl = Some(format!("/tmp/mock_cl/{}", mount_id));
            Ok(status.clone())
        }

        async fn clear_cl(&self, mount_id: Uuid) -> Result<MountStatus, ServiceError> {
            let mut mounts = self.mounts.write().await;
            let status = mounts
                .get_mut(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            if !matches!(
                status.state,
                MountLifecycle::Mounted | MountLifecycle::Ready
            ) {
                return Err(ServiceError::InvalidRequest(format!(
                    "mount {} is currently in state {:?}; cannot clear CL",
                    mount_id, status.state
                )));
            }
            if status.cl.is_none() {
                return Err(ServiceError::InvalidRequest(
                    "mount has no CL layer to clear".into(),
                ));
            }
            status.cl = None;
            status.layers.cl = None;
            Ok(status.clone())
        }

        async fn health_info(&self) -> HealthResponse {
            let mounts = self.mounts.read().await;
            HealthResponse {
                protocol_version: 1,
                service: "scorpiofs".to_string(),
                service_version: Some(env!("CARGO_PKG_VERSION").to_string()),
                capabilities: vec![
                    "mount.v1".to_string(),
                    "ready.v1".to_string(),
                    "changes.v1".to_string(),
                ],
                status: "healthy".to_string(),
                mount_count: mounts.len(),
                uptime_secs: 0,
            }
        }

        async fn check_mount_ready(
            &self,
            mount_id: Uuid,
        ) -> Result<MountReadyResponse, ServiceError> {
            let mounts = self.mounts.read().await;
            let status = mounts
                .get(&mount_id)
                .ok_or(ServiceError::NotFound(mount_id))?;
            Ok(MountReadyResponse {
                mount_id,
                ready: status.state == MountLifecycle::Ready,
                state: status.state.clone(),
            })
        }

        async fn shutdown_cleanup(&self) -> Result<(), ServiceError> {
            self.mounts.write().await.clear();
            Ok(())
        }
    }

    fn create_test_router() -> Router {
        let service = Arc::new(MockAntaresService::new());
        let daemon = AntaresDaemon::new(service);
        daemon.router()
    }

    #[test]
    fn changed_path_scan_ignores_libra_metadata_and_sorts_paths() {
        let root = tempfile::tempdir().unwrap();
        let cl = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("src")).unwrap();
        std::fs::create_dir_all(cl.path().join("src")).unwrap();
        std::fs::write(root.path().join("src/z.rs"), "z").unwrap();
        std::fs::write(root.path().join("src/a.rs"), "a").unwrap();
        std::fs::write(root.path().join("src/shared.rs"), "upper").unwrap();
        std::fs::write(cl.path().join("src/shared.rs"), "cl").unwrap();
        std::fs::write(cl.path().join("src/cl-only.rs"), "cl").unwrap();
        std::fs::write(root.path().join(".libra"), "gitdir: /tmp/metadata").unwrap();

        let mount_id = Uuid::new_v4();
        let response = scan_mount_changes(mount_id, root.path(), Some(cl.path())).unwrap();

        assert_eq!(response.mount_id, mount_id);
        assert_eq!(
            response
                .changes
                .iter()
                .map(|change| change.path.as_str())
                .collect::<Vec<_>>(),
            vec!["src/a.rs", "src/cl-only.rs", "src/shared.rs", "src/z.rs"]
        );
        assert!(response
            .changes
            .iter()
            .all(|change| change.kind == ChangeKind::Modified));
    }

    #[test]
    fn oci_whiteout_path_prefixes_basename() {
        assert_eq!(
            oci_whiteout_path(Path::new("/tmp/src/foo.rs")),
            PathBuf::from("/tmp/src/.wh.foo.rs")
        );
    }

    #[test]
    fn changed_path_scan_treats_oci_whiteout_as_delete() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("src")).unwrap();
        std::fs::write(root.path().join("src/.wh.gone.rs"), "").unwrap();
        std::fs::write(root.path().join("src/.wh..wh..opq"), "").unwrap();
        std::fs::write(root.path().join("src/keep.rs"), "k").unwrap();

        let response = scan_mount_changes(Uuid::new_v4(), root.path(), None).unwrap();
        let mut changes: Vec<_> = response
            .changes
            .iter()
            .map(|change| (change.path.as_str(), change.kind.clone()))
            .collect();
        changes.sort_by(|a, b| a.0.cmp(b.0));

        assert_eq!(
            changes,
            vec![
                ("src/gone.rs", ChangeKind::Deleted),
                ("src/keep.rs", ChangeKind::Modified),
            ]
        );
    }

    #[test]
    fn remove_mount_dirs_reclaims_mountpoint_and_layers() {
        let root = tempfile::tempdir().unwrap();
        let mount_id = Uuid::new_v4();
        let mountpoint = root.path().join("mnt").join(mount_id.to_string());
        let upper = root.path().join("upper").join(mount_id.to_string());
        let cl = root.path().join("cl").join(mount_id.to_string());
        std::fs::create_dir_all(&mountpoint).unwrap();
        std::fs::create_dir_all(upper.join("src")).unwrap();
        std::fs::write(upper.join("src/edit.rs"), "upper").unwrap();
        std::fs::create_dir_all(cl.join("src")).unwrap();
        std::fs::write(cl.join("src/cl.rs"), "cl").unwrap();

        AntaresServiceImpl::remove_mount_dirs(mount_id, &mountpoint, &upper, Some(&cl));

        assert!(!mountpoint.exists(), "empty mountpoint must be removed");
        assert!(!upper.exists(), "private upper layer must be removed");
        assert!(!cl.exists(), "private CL layer must be removed");
        // The per-mount roots themselves are left alone.
        assert!(root.path().join("mnt").exists());
        assert!(root.path().join("upper").exists());

        // Idempotent: nothing to do and nothing to fail on a second call.
        AntaresServiceImpl::remove_mount_dirs(mount_id, &mountpoint, &upper, Some(&cl));
    }

    #[test]
    fn remove_mount_dirs_never_deletes_through_a_populated_mountpoint() {
        // If the FUSE session were unexpectedly still attached, the mountpoint
        // would not be an empty directory. Its contents must survive untouched
        // while the private layers are still reclaimed.
        let root = tempfile::tempdir().unwrap();
        let mount_id = Uuid::new_v4();
        let mountpoint = root.path().join("mnt").join(mount_id.to_string());
        let upper = root.path().join("upper").join(mount_id.to_string());
        std::fs::create_dir_all(mountpoint.join("still-visible")).unwrap();
        std::fs::write(mountpoint.join("still-visible/file"), "keep").unwrap();
        std::fs::create_dir_all(&upper).unwrap();

        AntaresServiceImpl::remove_mount_dirs(mount_id, &mountpoint, &upper, None);

        assert!(mountpoint.join("still-visible/file").exists());
        assert_eq!(
            std::fs::read_to_string(mountpoint.join("still-visible/file")).unwrap(),
            "keep"
        );
        assert!(!upper.exists());
    }

    #[tokio::test]
    async fn test_mount_changes_route() {
        let app = create_test_router();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mounts")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"job_id":"vcs-job","path":"/project"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: MountCreated = serde_json::from_slice(&body).unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/mounts/{}/changes", created.mount_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let changes: MountChangesResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(changes.mount_id, created.mount_id);
        assert!(changes.changes.is_empty());
    }

    #[tokio::test]
    async fn test_healthcheck() {
        let app = create_test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let health: HealthResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(health.status, "healthy");
    }

    #[tokio::test]
    async fn test_create_mount_success() {
        let app = create_test_router();

        // Simplified request: only path and optional cl
        let body = serde_json::json!({
            "path": "/third-party/mega"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mounts")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: MountCreated = serde_json::from_slice(&body).unwrap();
        // Mountpoint is auto-generated with UUID
        assert!(created.mountpoint.starts_with("/tmp/mock_mnt/"));
    }

    #[tokio::test]
    async fn test_mount_by_job_and_delete_by_job() {
        let app = create_test_router();

        let body = serde_json::json!({
            "job_id": "job-1",
            "path": "/third-party/mega",
            "cl": "CL123"
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mounts")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Describe by job_id
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/mounts/by-job/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: MountStatus = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.job_id.as_deref(), Some("job-1"));
        assert_eq!(status.path, "/third-party/mega");
        assert_eq!(status.cl.as_deref(), Some("CL123"));

        // Delete by job_id
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/mounts/by-job/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let deleted: MountStatus = serde_json::from_slice(&body).unwrap();
        assert_eq!(deleted.job_id.as_deref(), Some("job-1"));
        assert!(matches!(deleted.state, MountLifecycle::Unmounted));

        // Now describe should be 404.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/mounts/by-job/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_list_mounts_empty() {
        let app = create_test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/mounts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let collection: MountCollection = serde_json::from_slice(&body).unwrap();
        assert!(collection.mounts.is_empty());
    }

    #[tokio::test]
    async fn test_describe_nonexistent_mount_returns_404() {
        let app = create_test_router();
        let fake_id = Uuid::new_v4();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/mounts/{}", fake_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_error_response_format() {
        let app = create_test_router();
        let fake_id = Uuid::new_v4();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/mounts/{}", fake_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let error: ErrorBody = serde_json::from_slice(&body).unwrap();

        assert_eq!(error.code, "NOT_FOUND");
        assert!(error.error.contains(&fake_id.to_string()));
    }

    #[tokio::test]
    async fn test_empty_path_rejected() {
        let app = create_test_router();

        let body = serde_json::json!({
            "path": ""
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mounts")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let error: ErrorBody = serde_json::from_slice(&body).unwrap();
        assert_eq!(error.code, "INVALID_REQUEST");
    }

    #[tokio::test]
    async fn test_create_mount_with_cl() {
        let app = create_test_router();

        // Request with CL identifier
        let body = serde_json::json!({
            "path": "/third-party/mega",
            "cl": "CL12345"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mounts")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_concurrent_mount_requests() {
        let service = Arc::new(MockAntaresService::new());

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let svc = service.clone();
                tokio::spawn(async move {
                    svc.create_mount(CreateMountRequest {
                        job_id: None,
                        build_id: None,
                        cl_path: None,
                        path: format!("/project/path{}", i),
                        cl: None,

                        upper_dir: None,

                        mountpoint: None,

                        pinned_refs: None,
                    
                        sealed_chain: Vec::new(),
                    })
                    .await
                })
            })
            .collect();

        for h in handles {
            assert!(h.await.unwrap().is_ok());
        }

        // All 10 mounts should exist
        let mounts = service.list_mounts().await.unwrap();
        assert_eq!(mounts.len(), 10);
    }

    #[tokio::test]
    async fn test_duplicate_path_cl_rejected() {
        let service = Arc::new(MockAntaresService::new());

        let request = CreateMountRequest {
            job_id: None,
            build_id: None,
            cl_path: None,
            path: "/third-party/mega".into(),
            cl: Some("CL123".into()),

            upper_dir: None,

            mountpoint: None,

            pinned_refs: None,
        
            sealed_chain: Vec::new(),
        };

        // First mount should succeed
        let result1 = service.create_mount(request.clone()).await;
        assert!(result1.is_ok());

        // Second mount with same path+cl should fail
        let result2 = service.create_mount(request).await;
        assert!(matches!(result2, Err(ServiceError::InvalidRequest(_))));
    }

    #[tokio::test]
    async fn test_job_id_idempotent() {
        let service = Arc::new(MockAntaresService::new());

        let request = CreateMountRequest {
            job_id: Some("job-123".into()),
            build_id: None,
            cl_path: None,
            path: "/third-party/mega".into(),
            cl: Some("CL123".into()),

            upper_dir: None,

            mountpoint: None,

            pinned_refs: None,
        
            sealed_chain: Vec::new(),
        };

        let first = service.create_mount(request.clone()).await.unwrap();
        let second = service.create_mount(request).await.unwrap();

        assert_eq!(first.mount_id, second.mount_id);
        assert_eq!(first.mountpoint, second.mountpoint);
    }

    #[tokio::test]
    async fn test_job_id_idempotent_rejected_when_unmounting() {
        let service = Arc::new(MockAntaresService::new());

        let request = CreateMountRequest {
            job_id: Some("job-123".into()),
            build_id: None,
            cl_path: None,
            path: "/third-party/mega".into(),
            cl: Some("CL123".into()),

            upper_dir: None,

            mountpoint: None,

            pinned_refs: None,
        
            sealed_chain: Vec::new(),
        };

        let first = service.create_mount(request.clone()).await.unwrap();

        // Simulate a concurrent teardown where job_id is still present but mount is unmounting.
        {
            let mut mounts = service.mounts.write().await;
            let s = mounts.get_mut(&first.mount_id).unwrap();
            s.state = MountLifecycle::Unmounting;
        }

        let second = service.create_mount(request).await;
        assert!(matches!(second, Err(ServiceError::InvalidRequest(_))));
    }

    #[tokio::test]
    async fn test_same_path_cl_different_job_id_allowed() {
        let service = Arc::new(MockAntaresService::new());

        let req1 = CreateMountRequest {
            job_id: Some("job-a".into()),
            build_id: None,
            cl_path: None,
            path: "/third-party/mega".into(),
            cl: Some("CL123".into()),

            upper_dir: None,

            mountpoint: None,

            pinned_refs: None,
        
            sealed_chain: Vec::new(),
        };
        let req2 = CreateMountRequest {
            job_id: Some("job-b".into()),
            build_id: None,
            cl_path: None,
            path: "/third-party/mega".into(),
            cl: Some("CL123".into()),

            upper_dir: None,

            mountpoint: None,

            pinned_refs: None,
        
            sealed_chain: Vec::new(),
        };

        let r1 = service.create_mount(req1).await;
        let r2 = service.create_mount(req2).await;
        assert!(r1.is_ok());
        assert!(r2.is_ok());

        let mounts = service.list_mounts().await.unwrap();
        assert_eq!(mounts.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_mount_success() {
        let service = Arc::new(MockAntaresService::new());

        // Create a mount
        let created = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/third-party/mega".into(),
                cl: None,

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await
            .unwrap();

        let mount_id = created.mount_id;

        // Delete it
        let deleted = service.delete_mount(mount_id).await.unwrap();
        assert!(matches!(deleted.state, MountLifecycle::Unmounted));

        // Verify it's gone
        let result = service.describe_mount(mount_id).await;
        assert!(matches!(result, Err(ServiceError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_same_path_different_cl_allowed() {
        let service = Arc::new(MockAntaresService::new());

        // Mount with CL1
        let result1 = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/third-party/mega".into(),
                cl: Some("CL1".into()),

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await;
        assert!(result1.is_ok());

        // Mount with CL2 (same path, different CL) should succeed
        let result2 = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/third-party/mega".into(),
                cl: Some("CL2".into()),

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await;
        assert!(result2.is_ok());

        // Should have 2 mounts
        let mounts = service.list_mounts().await.unwrap();
        assert_eq!(mounts.len(), 2);
    }

    /// Test concurrent mount creation to verify thread safety.
    /// This validates that multiple Antares instances can safely share
    /// the same service and create mounts concurrently.
    #[tokio::test]
    async fn test_concurrent_mount_creation() {
        let service = Arc::new(MockAntaresService::new());

        // Spawn 10 concurrent mount creation tasks
        let mut handles = Vec::new();
        for i in 0..10 {
            let svc = service.clone();
            let handle = tokio::spawn(async move {
                let request = CreateMountRequest {
                    job_id: None,
                    build_id: None,
                    cl_path: None,
                    path: format!("/concurrent-path-{}", i),
                    cl: None,

                    upper_dir: None,

                    mountpoint: None,

                    pinned_refs: None,
                
                    sealed_chain: Vec::new(),
                };
                svc.create_mount(request).await
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = join_all(handles).await;

        // All should succeed
        let mut success_count = 0;
        for result in results {
            match result {
                Ok(Ok(_)) => success_count += 1,
                Ok(Err(e)) => panic!("Mount creation failed: {:?}", e),
                Err(e) => panic!("Task panicked: {:?}", e),
            }
        }
        assert_eq!(success_count, 10, "All 10 concurrent mounts should succeed");

        // Verify all mounts are listed
        let mounts = service.list_mounts().await.unwrap();
        assert_eq!(
            mounts.len(),
            10,
            "Should have 10 mounts after concurrent creation"
        );

        // Verify paths are unique
        let paths: std::collections::HashSet<_> = mounts.iter().map(|m| m.path.clone()).collect();
        assert_eq!(paths.len(), 10, "All paths should be unique");
    }

    /// Test concurrent operations on the same mount.
    #[tokio::test]
    async fn test_concurrent_operations_same_mount() {
        let service = Arc::new(MockAntaresService::new());

        // Create a mount
        let request = CreateMountRequest {
            job_id: None,
            build_id: None,
            cl_path: None,
            path: "/test-concurrent-ops".to_string(),
            cl: None,

            upper_dir: None,

            mountpoint: None,

            pinned_refs: None,
        
            sealed_chain: Vec::new(),
        };
        let created = service.create_mount(request).await.unwrap();
        let mount_id = created.mount_id;

        // Spawn multiple concurrent describe operations
        let mut handles = Vec::new();
        for _ in 0..20 {
            let svc = service.clone();
            let id = mount_id;
            let handle = tokio::spawn(async move { svc.describe_mount(id).await });
            handles.push(handle);
        }

        // All describe operations should succeed
        let results: Vec<_> = join_all(handles).await;
        for result in results {
            assert!(
                result.is_ok() && result.unwrap().is_ok(),
                "All describe operations should succeed"
            );
        }
    }

    /// Test build_cl API - successfully add CL layer to mount
    #[tokio::test]
    async fn test_build_cl_success() {
        let service = Arc::new(MockAntaresService::new());

        // Create a mount without CL
        let created = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/third-party/mega".into(),
                cl: None,

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await
            .unwrap();

        let mount_id = created.mount_id;

        // Build CL layer
        let status = service.build_cl(mount_id, "CL123".into()).await.unwrap();
        assert_eq!(status.cl, Some("CL123".into()));
        assert!(status.layers.cl.is_some());
    }

    #[tokio::test]
    async fn test_build_cl_rejected_when_unmounting() {
        let service = Arc::new(MockAntaresService::new());

        let created = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/third-party/mega".into(),
                cl: None,

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await
            .unwrap();

        {
            let mut mounts = service.mounts.write().await;
            let s = mounts.get_mut(&created.mount_id).unwrap();
            s.state = MountLifecycle::Unmounting;
        }

        let result = service.build_cl(created.mount_id, "CL123".into()).await;
        assert!(matches!(result, Err(ServiceError::InvalidRequest(_))));
    }

    #[tokio::test]
    async fn test_build_cl_rejected_when_quiescing() {
        let service = Arc::new(MockAntaresService::new());

        let created = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/third-party/mega".into(),
                cl: None,

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await
            .unwrap();

        {
            let mut mounts = service.mounts.write().await;
            let s = mounts.get_mut(&created.mount_id).unwrap();
            s.state = MountLifecycle::Quiescing;
        }

        let result = service.build_cl(created.mount_id, "CL123".into()).await;
        assert!(matches!(result, Err(ServiceError::InvalidRequest(_))));
    }

    /// Test build_cl API - mount not found
    #[tokio::test]
    async fn test_build_cl_not_found() {
        let service = Arc::new(MockAntaresService::new());
        let fake_id = Uuid::new_v4();

        let result = service.build_cl(fake_id, "CL123".into()).await;
        assert!(matches!(result, Err(ServiceError::NotFound(_))));
    }

    /// Test clear_cl API - successfully clear CL layer
    #[tokio::test]
    async fn test_clear_cl_success() {
        let service = Arc::new(MockAntaresService::new());

        // Create a mount with CL
        let created = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/third-party/mega".into(),
                cl: Some("CL123".into()),

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await
            .unwrap();

        let mount_id = created.mount_id;

        // Clear CL layer
        let status = service.clear_cl(mount_id).await.unwrap();
        assert_eq!(status.cl, None);
        assert!(status.layers.cl.is_none());
    }

    /// Test clear_cl API - no CL layer to clear
    #[tokio::test]
    async fn test_clear_cl_no_layer() {
        let service = Arc::new(MockAntaresService::new());

        // Create a mount without CL
        let created = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/third-party/mega".into(),
                cl: None,

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await
            .unwrap();

        let mount_id = created.mount_id;

        // Try to clear non-existent CL layer
        let result = service.clear_cl(mount_id).await;
        assert!(matches!(result, Err(ServiceError::InvalidRequest(_))));
    }

    #[tokio::test]
    async fn test_clear_cl_rejected_when_quiescing() {
        let service = Arc::new(MockAntaresService::new());

        let created = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/third-party/mega".into(),
                cl: Some("CL123".into()),

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await
            .unwrap();

        {
            let mut mounts = service.mounts.write().await;
            let s = mounts.get_mut(&created.mount_id).unwrap();
            s.state = MountLifecycle::Quiescing;
        }

        let result = service.clear_cl(created.mount_id).await;
        assert!(matches!(result, Err(ServiceError::InvalidRequest(_))));
    }

    /// Test HTTP endpoint for build_cl
    #[tokio::test]
    async fn test_http_build_cl() {
        let service = Arc::new(MockAntaresService::new());

        // First create a mount
        let created = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/test/path".into(),
                cl: None,

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await
            .unwrap();

        let daemon = AntaresDaemon::new(service);
        let app = daemon.router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/mounts/{}/cl", created.mount_id))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"cl":"CL456"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: MountStatus = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.cl, Some("CL456".into()));
    }

    /// Test HTTP endpoint for clear_cl
    #[tokio::test]
    async fn test_http_clear_cl() {
        let service = Arc::new(MockAntaresService::new());

        // First create a mount with CL
        let created = service
            .create_mount(CreateMountRequest {
                job_id: None,
                build_id: None,
                cl_path: None,
                path: "/test/path".into(),
                cl: Some("CL123".into()),

                upper_dir: None,

                mountpoint: None,

                pinned_refs: None,
            
                sealed_chain: Vec::new(),
            })
            .await
            .unwrap();

        let daemon = AntaresDaemon::new(service);
        let app = daemon.router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/mounts/{}/cl", created.mount_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: MountStatus = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.cl, None);
    }
}
