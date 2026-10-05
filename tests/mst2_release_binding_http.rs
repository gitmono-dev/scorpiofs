//! Canonical no-content release and closed, identity-bound legacy receipts.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{
    extract::Path,
    http::{HeaderMap, StatusCode},
    routing::delete,
    Json, Router,
};
use scorpiofs::snapshot::{LeaseReleaseOutcome, Mst2Client, SnapshotErrorCode};
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
        json!({"lease_id": LEASE, "released": true, "unexpected": false}),
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

#[tokio::test]
async fn canonical_204_acknowledges_repeated_release_with_bound_credentials() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let server = Server::start(Router::new().route(
        "/api/v2/snapshots/leases/{lease}",
        delete(move |Path(lease): Path<String>, headers: HeaderMap| {
            assert_eq!(lease, LEASE);
            assert_eq!(headers["authorization"], "Bearer release-actor");
            assert_eq!(headers["x-mega-snapshot-lease"], LEASE);
            observed.fetch_add(1, Ordering::SeqCst);
            async { StatusCode::NO_CONTENT }
        }),
    ))
    .await;
    server.client.set_token(Some("release-actor".into()));
    server.client.bind_lease(LEASE);
    for _ in 0..2 {
        assert_eq!(
            server.client.release_lease_outcome(LEASE).await.unwrap(),
            LeaseReleaseOutcome::Acknowledged
        );
    }
    assert!(server.client.release_lease(LEASE).await.unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(server.client.retry_count(), 0);
}

#[tokio::test]
async fn legacy_outcome_reports_removal_while_other_success_statuses_are_rejected() {
    for (released, expected) in [
        (true, LeaseReleaseOutcome::Removed),
        (false, LeaseReleaseOutcome::AlreadyReleased),
    ] {
        let server = Server::json(
            json!({"lease_id": LEASE, "released": released}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(
            server.client.release_lease_outcome(LEASE).await.unwrap(),
            expected
        );
    }
    for status in [
        StatusCode::CREATED,
        StatusCode::ACCEPTED,
        StatusCode::PARTIAL_CONTENT,
    ] {
        let server = Server::json(json!({"lease_id": LEASE, "released": true}), status).await;
        let error = server
            .client
            .release_lease_outcome(LEASE)
            .await
            .unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
        assert_eq!(error.http_status, status.as_u16());
        assert_eq!(server.client.retry_count(), 0);
    }
}

#[tokio::test]
async fn no_content_release_rejects_nonzero_payload_framing() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Axum/Hyper removes invalid 204 payload headers. An independent TCP peer
    // puts the actual invalid response on the wire instead of repairing it.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 8192];
        let mut used = 0;
        while !request[..used].windows(4).any(|tail| tail == b"\r\n\r\n") {
            let read = stream.read(&mut request[used..]).await.unwrap();
            assert_ne!(read, 0);
            used += read;
            assert!(used < request.len());
        }
        assert!(request[..used].starts_with(b"DELETE /api/v2/snapshots/leases/dedicated-lease "));
        stream
            .write_all(
                b"HTTP/1.1 204 No Content\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx",
            )
            .await
            .unwrap();
    });
    let server = Server { client, task };
    let error = server
        .client
        .release_lease_outcome(LEASE)
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
    assert_eq!(error.http_status, 204);
    assert_eq!(server.client.retry_count(), 0);
}
