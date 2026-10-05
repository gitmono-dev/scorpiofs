//! Legacy JSON release receipts must identify the exact requested lease.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{http::StatusCode, routing::delete, Json, Router};
use scorpiofs::snapshot::{Mst2Client, SnapshotErrorCode};
use serde_json::{json, Value};

const LEASE: &str = "dedicated-lease";

struct Server {
    client: Mst2Client,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(app: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { client, task }
    }

    async fn json(body: Value, status: StatusCode) -> Self {
        Self::start(Router::new().route(
            "/api/v2/snapshots/leases/{lease}",
            delete(move || {
                let body = body.clone();
                async move { (status, Json(body)) }
            }),
        ))
        .await
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn omitted_misbound_and_malformed_release_receipts_are_errors() {
    let cases = [
        json!({}),
        json!({"released": false}),
        json!({"lease_id": LEASE}),
        json!({"lease_id": "another-lease", "released": true}),
        json!({"lease_id": "another-lease", "released": false}),
        json!({"lease_id": null, "released": false}),
        json!({"lease_id": LEASE, "released": null}),
        json!({"lease_id": LEASE, "released": "false"}),
        json!({"lease_id": LEASE, "released": 0}),
        json!([]),
    ];
    for body in cases {
        let server = Server::json(body, StatusCode::OK).await;
        let error = server.client.release_lease(LEASE).await.unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
        assert_eq!(server.client.retry_count(), 0);
    }
}

#[tokio::test]
async fn active_and_idempotent_receipts_remain_distinct_and_recover_after_bad_response() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let server = Server::start(Router::new().route(
        "/api/v2/snapshots/leases/{lease}",
        delete(move || {
            let body = match observed.fetch_add(1, Ordering::SeqCst) {
                0 => json!({"lease_id": LEASE}),
                1 => json!({"lease_id": LEASE, "released": true}),
                _ => json!({"lease_id": LEASE, "released": false}),
            };
            async move { Json(body) }
        }),
    ))
    .await;
    assert_eq!(
        server.client.release_lease(LEASE).await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    assert!(server.client.release_lease(LEASE).await.unwrap());
    assert!(!server.client.release_lease(LEASE).await.unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(server.client.retry_count(), 0);
}

#[tokio::test]
async fn typed_release_server_errors_are_preserved() {
    let server = Server::json(
        json!({"error": {"code": "SCOPE_FORBIDDEN", "message": "denied"}}),
        StatusCode::FORBIDDEN,
    )
    .await;
    let error = server.client.release_lease(LEASE).await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::ScopeForbidden);
    assert_eq!(error.http_status, 403);
    assert_eq!(server.client.retry_count(), 0);
}
