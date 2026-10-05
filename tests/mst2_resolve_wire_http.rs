//! Check selected resolve contracts through the actual HTTP client and reader.

use std::sync::{Arc, Mutex};

use axum::{
    body::Body,
    http::HeaderMap,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use scorpiofs::snapshot::{Mst2Client, SnapshotErrorCode, SnapshotReader};
use serde_json::{json, Value};

const SCOPE: &str = "/project/app";
type Requests = Arc<Mutex<Vec<(HeaderMap, Value)>>>;
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
    async fn start(body: String) -> Self {
        let requests = Requests::default();
        let captured = requests.clone();
        let app = Router::new()
            .route(
                "/api/v2/snapshots/capabilities",
                get(|| async {
                    Json(json!({"protocol_versions":[2],"metadata_codecs":[1],
                    "frame_encodings":["identity"],
                    "features":{"resolve":true,"directory":true,"leases":true}}))
                }),
            )
            .route(
                "/api/v2/snapshots/resolve",
                post(move |headers: HeaderMap, Json(request): Json<Value>| {
                    let captured = captured.clone();
                    let body = body.clone();
                    async move {
                        captured.lock().unwrap().push((headers, request));
                        Response::builder()
                            .header("content-type", "application/json")
                            .body(Body::from(body))
                            .unwrap()
                    }
                }),
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
    serde_json::from_str(include_str!("fixtures/mst2_resolve_response_0_2_1.json")).unwrap()
}
async fn rejected(value: Value) {
    let server = Server::start(value.to_string()).await;
    assert_eq!(
        server.client.resolve(SCOPE, 600).await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
}

#[tokio::test]
async fn frozen_resolve_contract_preserves_the_request_and_fixed_reader_authority() {
    let server =
        Server::start(include_str!("fixtures/mst2_resolve_response_0_2_1.json").into()).await;
    let resolved = server.client.resolve(SCOPE, 600).await.unwrap();
    let expected = fixture();
    assert_eq!(
        resolved.descriptor.snapshot_id,
        expected["descriptor"]["snapshot_id"]
    );
    assert_eq!(resolved.publication_sequence, "100");
    assert_eq!(resolved.authorization_epoch, "7");
    assert_eq!(resolved.lease_expires_at, expected["lease_expires_at"]);
    assert_eq!(
        server.requests.lock().unwrap()[0].1,
        serde_json::from_str::<Value>(include_str!("fixtures/mst2_resolve_request_0_2_1.json"))
            .unwrap()
    );

    let mut live = fixture();
    live["lease_expires_at"] = json!("2099-01-01T00:00:00Z");
    let server = Server::start(live.to_string()).await;
    server.client.set_token(Some("frozen-actor".into()));
    server.client.bind_lease("previous-lease");
    let reader = SnapshotReader::resolve(server.client.clone(), SCOPE, 600)
        .await
        .unwrap();
    assert_eq!(reader.snapshot_id(), resolved.descriptor.snapshot_id);
    assert_eq!(reader.authorized_context().authorization_epoch(), 7);
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0["authorization"], "Bearer frozen-actor");
    assert!(!requests[0].0.contains_key("x-mega-snapshot-lease"));
}

#[tokio::test]
async fn canonical_resolve_requires_all_closed_fields_without_legacy_fallback() {
    for key in [
        "descriptor",
        "publication_sequence",
        "writer_epoch",
        "lease_id",
        "lease_expires_at",
        "authorization_epoch",
        "resolved_at",
        "delivery",
    ] {
        for null in [false, true] {
            let mut value = fixture();
            if null {
                value[key] = Value::Null;
            } else {
                value.as_object_mut().unwrap().remove(key);
            }
            rejected(value).await;
        }
    }
    let mut value = fixture();
    value["extra"] = json!(true);
    rejected(value).await;
    for field in [
        "schema_version",
        "metadata_codec",
        "instance_id",
        "namespace_view_id",
        "scope",
        "materialization_policy",
        "fs_semantics",
        "access_projection",
        "metadata_root",
        "snapshot_id",
    ] {
        let mut value = fixture();
        value["descriptor"].as_object_mut().unwrap().remove(field);
        rejected(value).await;
    }
    for (field, bad) in [
        ("extra", json!(true)),
        (
            "metadata_root",
            json!(format!("sha256:{}", "ff".repeat(32))),
        ),
        ("snapshot_id", json!(format!("sha256:{}", "ff".repeat(32)))),
        ("schema_version", json!(3)),
    ] {
        let mut value = fixture();
        value["descriptor"][field] = bad;
        rejected(value).await;
    }
    let server = Server::start(fixture().to_string()).await;
    assert_eq!(
        server.client.resolve("/other", 600).await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    for bad in [json!("lazy"), json!("FULL"), json!(true)] {
        let mut value = fixture();
        value["delivery"] = bad;
        rejected(value).await;
    }
}

#[tokio::test]
async fn resolve_counters_lease_ids_and_actual_timestamps_are_semantically_checked() {
    for field in [
        "publication_sequence",
        "writer_epoch",
        "authorization_epoch",
    ] {
        for bad in [
            json!(0),
            json!(true),
            json!(""),
            json!("00"),
            json!("-1"),
            json!("1.0"),
            json!("+1"),
            json!("9223372036854775808"),
        ] {
            let mut value = fixture();
            value[field] = bad;
            rejected(value).await;
        }
        for good in ["0", "9223372036854775807"] {
            let mut value = fixture();
            value[field] = json!(good);
            let server = Server::start(value.to_string()).await;
            assert!(server.client.resolve(SCOPE, 600).await.is_ok());
        }
    }
    for bad in [json!(""), json!("a".repeat(513)), json!(12)] {
        let mut value = fixture();
        value["lease_id"] = bad;
        rejected(value).await;
    }
    for field in ["lease_expires_at", "resolved_at"] {
        for bad in [
            json!(""),
            json!("2026-02-30T12:00:00Z"),
            json!("2026-09-15"),
            json!("2026-09-15T25:00:00Z"),
            json!("2026-09-15T12:00:00-00:00"),
            json!("2026-09-15 12:00:00Z"),
            json!("2026-09-15@12:00:00Z"),
            json!("2026-09-15\n12:00:00Z"),
            json!("2026-09-16T02:28:60Z"),
            json!(true),
        ] {
            let mut value = fixture();
            value[field] = bad;
            rejected(value).await;
        }
    }
    for expiry in ["2026-09-15T12:00:00Z", "2026-09-15T11:59:59Z"] {
        let mut value = fixture();
        value["lease_expires_at"] = json!(expiry);
        rejected(value).await;
    }
    let mut value = fixture();
    value["resolved_at"] = json!("2026-09-15T20:00:00.123456789+08:00");
    value["lease_expires_at"] = json!("2026-09-15T12:00:00.223456789Z");
    let server = Server::start(value.to_string()).await;
    assert!(
        server.client.resolve(SCOPE, 600).await.is_ok(),
        "actual grant need not equal suggested seconds"
    );
    let mut value = fixture();
    value["resolved_at"] = json!("2026-09-15t12:00:00z");
    value["lease_expires_at"] = json!("2026-09-15t12:20:00z");
    let server = Server::start(value.to_string()).await;
    assert!(server.client.resolve(SCOPE, 600).await.is_ok());
}

#[tokio::test]
async fn optional_offline_hints_are_closed_bound_data_and_never_an_export_permission() {
    let mut value = fixture();
    value["offline_grant"] = json!({"grant_id":"grant", "snapshot_id":value["descriptor"]["snapshot_id"],
        "actor_domain_id":"actor", "expires_at":"2026-09-15T13:00:00Z", "policy":"trusted_local_export_v1"});
    let server = Server::start(value.to_string()).await;
    assert!(server.client.resolve(SCOPE, 600).await.is_ok());
    for field in [
        "grant_id",
        "snapshot_id",
        "actor_domain_id",
        "expires_at",
        "policy",
    ] {
        let mut bad = value.clone();
        bad["offline_grant"].as_object_mut().unwrap().remove(field);
        rejected(bad).await;
    }
    for (field, bad) in [
        ("extra", json!(true)),
        ("grant_id", json!("")),
        ("actor_domain_id", json!("a".repeat(513))),
        ("snapshot_id", json!(format!("sha256:{}", "ff".repeat(32)))),
        ("expires_at", json!("invalid")),
        ("policy", json!("allow_all")),
    ] {
        let mut changed = value.clone();
        changed["offline_grant"][field] = bad;
        rejected(changed).await;
    }
    value["offline_grant"] = Value::Null;
    rejected(value).await;
}

#[tokio::test]
async fn explicit_legacy_and_json_budgets_remain_compatible_and_requests_fail_before_http() {
    let mut value = fixture();
    for key in ["writer_epoch", "resolved_at", "delivery"] {
        value.as_object_mut().unwrap().remove(key);
    }
    value["deployment_extension"] = json!(true);
    let server = Server::start(value.to_string()).await;
    assert!(server.client.resolve(SCOPE, 600).await.is_ok());
    for seconds in [0, 59, 3601, u64::MAX] {
        assert_eq!(
            server
                .client
                .resolve(SCOPE, seconds)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::InvalidRequest
        );
    }
    for path in ["relative", "/a//b", "/a/../b", "/a/", "/a\0b"] {
        assert!(server.client.resolve(path, 600).await.is_err());
    }
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    let duplicate = include_str!("fixtures/mst2_resolve_response_0_2_1.json").replacen(
        "\"writer_epoch\": \"1\"",
        "\"writer_epoch\": \"1\", \"writer_epoch\": \"1\"",
        1,
    );
    let server = Server::start(duplicate).await;
    assert_eq!(
        server.client.resolve(SCOPE, 600).await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    let server = Server::start(" ".repeat(1_048_577)).await;
    assert_eq!(
        server.client.resolve(SCOPE, 600).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
}
