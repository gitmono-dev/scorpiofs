//! Workspace control daemon. Mount creation belongs to the Antares service;
//! startup does not construct a separate full-repository dictionary mount.
//! The explicit legacy entry remains available for existing dictionary callers.

use std::{sync::Arc, time::Instant};

use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use tokio::sync::oneshot;

pub mod antares;
mod legacy;
pub use legacy::daemon_main;
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

/// The workspace surface exposes process health and the workspace service.
/// Deprecated dictionary routes remain on the explicit legacy entry only.
pub fn daemon_router<S: AntaresService + 'static>(service: Arc<S>) -> Router {
    Router::new()
        .route("/health", get(process_health))
        .with_state(Instant::now())
        .nest("/antares", AntaresDaemon::new(service).router())
}

/// Serve on the caller's bound listener, drain admitted HTTP requests, then
/// clean up the mounts owned by this service.
pub async fn workspace_daemon_main<S: AntaresService + 'static>(
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::{antares::*, *};

    struct FailingCleanup {
        admitted: tokio::sync::Notify,
        release: tokio::sync::Notify,
        drained: AtomicBool,
        cleanup_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl AntaresService for FailingCleanup {
        async fn create_mount(&self, _: CreateMountRequest) -> Result<MountCreated, ServiceError> {
            unreachable!()
        }
        async fn list_mounts(&self) -> Result<Vec<MountStatus>, ServiceError> {
            self.admitted.notify_one();
            self.release.notified().await;
            self.drained.store(true, Ordering::SeqCst);
            Ok(vec![])
        }
        async fn describe_mount(&self, _: uuid::Uuid) -> Result<MountStatus, ServiceError> {
            unreachable!()
        }
        async fn delete_mount(&self, _: uuid::Uuid) -> Result<MountStatus, ServiceError> {
            unreachable!()
        }
        async fn build_cl(&self, _: uuid::Uuid, _: String) -> Result<MountStatus, ServiceError> {
            unreachable!()
        }
        async fn clear_cl(&self, _: uuid::Uuid) -> Result<MountStatus, ServiceError> {
            unreachable!()
        }
        async fn check_mount_ready(
            &self,
            _: uuid::Uuid,
        ) -> Result<MountReadyResponse, ServiceError> {
            unreachable!()
        }
        async fn changed_paths(&self, _: uuid::Uuid) -> Result<MountChangesResponse, ServiceError> {
            unreachable!()
        }
        async fn health_info(&self) -> HealthResponse {
            unreachable!("process health must not call the mount service")
        }
        async fn shutdown_cleanup(&self) -> Result<(), ServiceError> {
            assert!(self.drained.load(Ordering::SeqCst));
            self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
            Err(ServiceError::FuseFailure("fixture unmount denial".into()))
        }
    }

    #[tokio::test]
    async fn shutdown_drains_an_admitted_request_and_reports_cleanup_failure() {
        let service = Arc::new(FailingCleanup {
            admitted: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            drained: AtomicBool::new(false),
            cleanup_calls: AtomicUsize::new(0),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mut server = tokio::spawn(workspace_daemon_main(
            service.clone(),
            shutdown_rx,
            listener,
        ));
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let health = client.get(format!("{base}/health")).send().await.unwrap();
        assert!(health.status().is_success());
        let request = tokio::spawn(async move {
            client
                .get(format!("{base}/antares/mounts"))
                .send()
                .await
                .unwrap()
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            service.admitted.notified(),
        )
        .await
        .unwrap();
        shutdown_tx.send(()).unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut server)
                .await
                .is_err()
        );
        assert_eq!(service.cleanup_calls.load(Ordering::SeqCst), 0);
        service.release.notify_one();
        let response = request.await.unwrap();
        assert!(response.status().is_success());
        // Consume the response so the connection can finish graceful draining.
        let _: serde_json::Value = response.json().await.unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("fixture unmount denial"));
        assert_eq!(service.cleanup_calls.load(Ordering::SeqCst), 1);
    }
}
