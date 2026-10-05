//! Workspace control daemon. Mount creation belongs to the Antares service;
//! startup does not construct a separate full-repository dictionary mount.

use std::{sync::Arc, time::Instant};

use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use tokio::sync::oneshot;

pub mod antares;
pub mod lower_view;
pub mod upper_fork;
pub mod worktree_v2;

use antares::{AntaresDaemon, AntaresService};

#[derive(Serialize)]
struct ProcessHealth {
    status: &'static str,
    version: &'static str,
    uptime_secs: u64,
    // This process probe never waits for mount/remote work. Detailed mount
    // state is reported by the service under /antares.
    mount_count: Option<usize>,
}

async fn process_health(State(started): State<Instant>) -> Json<ProcessHealth> {
    Json(ProcessHealth {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        uptime_secs: started.elapsed().as_secs(),
        mount_count: None,
    })
}

/// The root surface exposes process health and the workspace service only.
/// The retired /api/fs/* and mutable /api/config handlers are removed.
pub fn daemon_router<S: AntaresService + 'static>(service: Arc<S>) -> Router {
    Router::new()
        .route("/health", get(process_health))
        .with_state(Instant::now())
        .nest("/antares", AntaresDaemon::new(service).router())
}

/// Serve on the caller's bound listener, drain admitted HTTP requests, then
/// clean up the mounts owned by this service.
pub async fn daemon_main<S: AntaresService + 'static>(
    service: Arc<S>,
    shutdown_rx: oneshot::Receiver<()>,
    listener: tokio::net::TcpListener,
) -> std::io::Result<()> {
    let result = axum::serve(listener, daemon_router(service.clone()))
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
            tracing::info!("workspace HTTP shutdown requested");
        })
        .await;
    let cleanup = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        service.shutdown_cleanup(),
    )
    .await;
    match cleanup {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Err(std::io::Error::other(format!(
                "mount cleanup failed: {error}"
            )));
        }
        Err(_) => return Err(std::io::Error::other("mount cleanup timed out")),
    }
    result
}
