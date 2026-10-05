use std::{sync::Arc, time::Instant};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;

use super::{CreateWorkspace, DestroyWorkspace, WorkspaceError, WorkspaceService, WorkspaceStatus};

impl IntoResponse for WorkspaceError {
    fn into_response(self) -> Response {
        let status = match self.code {
            "WORKSPACE_NOT_FOUND" => StatusCode::NOT_FOUND,
            "WORKSPACE_BUSY" => StatusCode::SERVICE_UNAVAILABLE,
            "WORKSPACE_DIRTY" | "WORKSPACE_NOT_READY" => StatusCode::CONFLICT,
            "INVALID_CONFIG" => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        #[derive(Serialize)]
        struct ErrorBody {
            code: &'static str,
            message: String,
        }
        (
            status,
            Json(ErrorBody {
                code: self.code,
                message: self.message,
            }),
        )
            .into_response()
    }
}

/// Only the v3 control contract is installed. Requests cannot select an old
/// worktree revision, dictionary lower, CL overlay, or refresh in place.
pub fn router(service: Arc<WorkspaceService>) -> Router {
    Router::new()
        .route("/v3/workspaces", post(create).get(list))
        .route("/v3/workspaces/{id}", get(status))
        .route("/v3/workspaces/{id}/hydrate", post(hydrate))
        .route("/v3/workspaces/{id}/hydrate/cancel", post(cancel))
        .route("/v3/workspaces/{id}/local-pin/release", post(release))
        .route("/v3/workspaces/{id}/destroy", post(destroy))
        .with_state(service)
        .merge(
            Router::new()
                .route("/health", get(health))
                .with_state(Instant::now()),
        )
}

async fn create(
    State(service): State<Arc<WorkspaceService>>,
    Json(request): Json<CreateWorkspace>,
) -> Result<Json<WorkspaceStatus>, WorkspaceError> {
    Ok(Json(service.create(request).await?))
}

async fn list(
    State(service): State<Arc<WorkspaceService>>,
) -> Result<Json<Vec<WorkspaceStatus>>, WorkspaceError> {
    Ok(Json(service.list().await?))
}

async fn status(
    State(service): State<Arc<WorkspaceService>>,
    Path(id): Path<String>,
) -> Result<Json<WorkspaceStatus>, WorkspaceError> {
    Ok(Json(service.status(&id).await?))
}

async fn hydrate(
    State(service): State<Arc<WorkspaceService>>,
    Path(id): Path<String>,
) -> Result<Json<WorkspaceStatus>, WorkspaceError> {
    Ok(Json(service.start_hydrate(&id).await?))
}

async fn cancel(
    State(service): State<Arc<WorkspaceService>>,
    Path(id): Path<String>,
) -> Result<Json<WorkspaceStatus>, WorkspaceError> {
    Ok(Json(service.cancel_hydrate(&id).await?))
}

async fn release(
    State(service): State<Arc<WorkspaceService>>,
    Path(id): Path<String>,
) -> Result<Json<crate::snapshot::ReleaseLocalPinReceipt>, WorkspaceError> {
    Ok(Json(service.release_local_pin(&id).await?))
}

async fn destroy(
    State(service): State<Arc<WorkspaceService>>,
    Path(id): Path<String>,
    Json(policy): Json<DestroyWorkspace>,
) -> Result<StatusCode, WorkspaceError> {
    service.destroy(&id, policy).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn health(State(started): State<Instant>) -> Json<serde_json::Value> {
    Json(
        serde_json::json!({"status":"ok", "version":env!("CARGO_PKG_VERSION"), "uptime_secs":started.elapsed().as_secs(), "mount_count":null}),
    )
}

/// Drain HTTP callers, then join admitted owned operations and retire all
/// mounts. Cleanup failure is returned to the launcher, never reported success.
pub async fn serve(
    service: Arc<WorkspaceService>,
    listener: tokio::net::TcpListener,
    shutdown: tokio::sync::oneshot::Receiver<()>,
) -> std::io::Result<()> {
    let result = axum::serve(listener, router(service.clone()))
        .with_graceful_shutdown(async move {
            let _ = shutdown.await;
        })
        .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        service.shutdown_cleanup(),
    )
    .await
    .map_err(|_| std::io::Error::other("workspace cleanup timed out"))?
    .map_err(|error| std::io::Error::other(error.to_string()))?;
    result
}
