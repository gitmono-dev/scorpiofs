//! Actual HTTP boundaries for renewal contracts and opaque lease paths.

use std::sync::{Arc, Mutex};

use axum::{
    body::Body,
    extract::{OriginalUri, Path},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{delete, post},
    Json, Router,
};
use scorpiofs::snapshot::{LeaseReleaseOutcome, Mst2Client, SnapshotErrorCode};
use serde_json::{json, Value};

const LEASE: &str = "fixture-lease-100";
type Requests = Arc<Mutex<Vec<(String, HeaderMap, Value)>>>;
struct Server {
    client: Mst2Client,
    requests: Requests,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(response: String) -> Self {
        let requests = Requests::default();
        let renew_requests = requests.clone();
        let release_requests = requests.clone();
        let app = Router::new()
            .route(
                "/api/v2/snapshots/leases/{lease}/renew",
                post(
                    move |OriginalUri(uri): OriginalUri,
                          Path(lease): Path<String>,
                          headers: HeaderMap,
                          Json(body): Json<Value>| {
                        let response = response.clone();
                        let requests = renew_requests.clone();
                        async move {
                            requests.lock().unwrap().push((
                                lease,
                                headers,
                                json!({"uri":uri.to_string(),"body":body}),
                            ));
                            Response::builder()
                                .header("content-type", "application/json")
                                .body(Body::from(response))
                                .unwrap()
                        }
                    },
                ),
            )
            .route(
                "/api/v2/snapshots/leases/{lease}",
                delete(
                    move |OriginalUri(uri): OriginalUri,
                          Path(lease): Path<String>,
                          headers: HeaderMap| {
                        let requests = release_requests.clone();
                        async move {
                            requests.lock().unwrap().push((
                                lease,
                                headers,
                                json!({"uri":uri.to_string()}),
                            ));
                            StatusCode::NO_CONTENT
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            client,
            requests,
            task,
        }
    }
}
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/mst2_lease_response_0_2_1.json")).unwrap()
}

#[tokio::test]
async fn canonical_lease_fixture_and_explicit_legacy_keep_their_json_shapes() {
    let canonical = fixture();
    let server =
        Server::start(include_str!("fixtures/mst2_lease_response_0_2_1.json").into()).await;
    assert_eq!(
        server.client.renew_lease(LEASE, 60).await.unwrap(),
        canonical
    );
    assert_eq!(
        server.requests.lock().unwrap()[0].2["body"],
        json!({"lease_seconds":60})
    );
    let mut legacy = canonical;
    legacy
        .as_object_mut()
        .unwrap()
        .remove("authorization_epoch");
    legacy["extension"] = json!({"legacy":true});
    let server = Server::start(legacy.to_string()).await;
    assert_eq!(
        server.client.renew_lease(LEASE, 3600).await.unwrap(),
        legacy
    );
}

#[tokio::test]
async fn canonical_marker_presence_requires_a_closed_complete_lease_response() {
    for key in [
        "lease_id",
        "snapshot_id",
        "lease_expires_at",
        "authorization_epoch",
    ] {
        let mut value = fixture();
        value[key] = Value::Null;
        let server = Server::start(value.to_string()).await;
        assert_eq!(
            server
                .client
                .renew_lease(LEASE, 600)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::IntegrityError
        );
        if key != "authorization_epoch" {
            let mut value = fixture();
            value.as_object_mut().unwrap().remove(key);
            let server = Server::start(value.to_string()).await;
            assert_eq!(
                server
                    .client
                    .renew_lease(LEASE, 600)
                    .await
                    .unwrap_err()
                    .code,
                SnapshotErrorCode::IntegrityError
            );
        }
    }
    let mut value = fixture();
    value["extension"] = json!(true);
    let server = Server::start(value.to_string()).await;
    assert_eq!(
        server
            .client
            .renew_lease(LEASE, 600)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
}

#[tokio::test]
async fn renewal_ids_counters_and_actual_timestamps_are_checked() {
    for (key, bad) in [
        ("lease_id", "another-lease"),
        ("snapshot_id", "sha256:ABC"),
        ("authorization_epoch", "01"),
        ("authorization_epoch", "9223372036854775808"),
        ("authorization_epoch", "-1"),
        ("lease_expires_at", "2026-02-30T00:00:00Z"),
        ("lease_expires_at", "2026-09-15T12:20:00-00:00"),
        ("lease_expires_at", "tomorrow"),
    ] {
        let mut value = fixture();
        value[key] = json!(bad);
        let server = Server::start(value.to_string()).await;
        assert_eq!(
            server
                .client
                .renew_lease(LEASE, 600)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::IntegrityError,
            "{key}:{bad}"
        );
    }
    for epoch in ["0", "9223372036854775807"] {
        let mut value = fixture();
        value["authorization_epoch"] = json!(epoch);
        value["lease_expires_at"] = json!("2026-09-15T20:20:00.123456789+08:00");
        let server = Server::start(value.to_string()).await;
        assert_eq!(server.client.renew_lease(LEASE, 600).await.unwrap(), value);
    }
}

#[tokio::test]
async fn opaque_lease_paths_round_trip_without_query_fragment_or_traversal() {
    for lease in [
        "a/b?c#d%2F\\ e",
        "%2e%2e",
        "../other",
        "租约🔒",
        "line\nfeed",
        "nul\0byte",
    ] {
        let mut value = fixture();
        value["lease_id"] = json!(lease);
        let server = Server::start(value.to_string()).await;
        server.client.set_token(Some("lease-actor".into()));
        // The opaque path and bearer authority are independent; do not place
        // arbitrary control characters into an HTTP lease header.
        assert_eq!(server.client.renew_lease(lease, 600).await.unwrap(), value);
        assert_eq!(
            server.client.release_lease_outcome(lease).await.unwrap(),
            LeaseReleaseOutcome::Acknowledged
        );
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for (returned, headers, body) in requests.iter() {
            assert_eq!(returned, lease);
            assert_eq!(headers["authorization"], "Bearer lease-actor");
            assert!(!headers.contains_key("x-mega-snapshot-lease"));
            let uri = body["uri"].as_str().unwrap();
            assert!(!uri.contains('?'));
            assert!(!uri.contains('#'));
            assert!(uri.starts_with("/api/v2/snapshots/leases/"));
        }
    }
}

#[tokio::test]
async fn unrepresentable_ids_and_invalid_suggestions_fail_before_http() {
    let server = Server::start(fixture().to_string()).await;
    for id in [String::new(), ".".into(), "..".into(), "🔒".repeat(513)] {
        assert_eq!(
            server.client.renew_lease(&id, 600).await.unwrap_err().code,
            SnapshotErrorCode::InvalidRequest
        );
        assert_eq!(
            server
                .client
                .release_lease_outcome(&id)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::InvalidRequest
        );
    }
    for seconds in [0, 59, 3601, u64::MAX] {
        assert_eq!(
            server
                .client
                .renew_lease(LEASE, seconds)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::InvalidRequest
        );
    }
    assert!(server.requests.lock().unwrap().is_empty());
    let lease = "🔒".repeat(512);
    let mut value = fixture();
    value["lease_id"] = json!(lease);
    let server = Server::start(value.to_string()).await;
    assert!(server.client.renew_lease(&lease, 600).await.is_ok());
    assert_eq!(server.requests.lock().unwrap()[0].0, lease);
}

#[tokio::test]
async fn renewal_reuses_recursive_duplicate_and_json_body_byte_bounds() {
    let response = include_str!("fixtures/mst2_lease_response_0_2_1.json").replace(
        "\"authorization_epoch\": \"7\"",
        "\"authorization_epoch\": \"7\", \"authorization_epoch\": \"8\"",
    );
    let server = Server::start(response).await;
    assert_eq!(
        server
            .client
            .renew_lease(LEASE, 600)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    let server = Server::start(" ".repeat(1_048_577)).await;
    assert_eq!(
        server
            .client
            .renew_lease(LEASE, 600)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
}
