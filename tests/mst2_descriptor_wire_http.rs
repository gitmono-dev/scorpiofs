//! Frozen 0.2.1 descriptor identity over real HTTP, without lease acquisition.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{
    extract::Path,
    http::{HeaderMap, StatusCode},
    routing::get,
    Json, Router,
};
use scorpiofs::snapshot::{Mst2Client, SnapshotErrorCode};
use serde_json::{json, Value};

const SID: &str = "sha256:f94af7bddec0603b40dbf5c2b17952bbfef30093af9f5653734ac05983cbbfee";
const OTHER_SID: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn canonical() -> Value {
    serde_json::from_str(include_str!("fixtures/mst2_descriptor_0_2_1.json")).unwrap()
}

fn wrapper(descriptor: Value) -> Value {
    json!({"snapshot_id": SID, "descriptor": descriptor,
        "lease_id": "legacy-hint", "lease_expires_at": "2026-09-15T12:10:00Z"})
}

struct Server {
    client: Mst2Client,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(body: Value, status: StatusCode) -> Self {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let app = Router::new().route(
            "/api/v2/snapshots/{sid}/descriptor",
            get(move |Path(sid): Path<String>, headers: HeaderMap| {
                assert!(sid == SID || sid == OTHER_SID);
                if let Some(lease) = headers.get("x-mega-snapshot-lease") {
                    assert_eq!(
                        lease, "original-lease",
                        "wrapper hints never rebind credentials"
                    );
                }
                observed.fetch_add(1, Ordering::SeqCst);
                let body = body.clone();
                async move { (status, Json(body)) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            client,
            calls,
            task,
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn frozen_bare_and_legacy_shapes_preserve_wire_and_normalize_the_same_identity() {
    for body in [canonical(), wrapper(canonical())] {
        let server = Server::start(body.clone(), StatusCode::OK).await;
        server.client.bind_lease("original-lease");
        assert_eq!(server.client.descriptor(SID).await.unwrap(), body);
        let descriptor = server.client.verified_descriptor(SID).await.unwrap();
        assert_eq!(descriptor.snapshot_id, SID);
        assert_eq!(descriptor.scope, "/project/app");
        assert_eq!(
            descriptor.metadata_root,
            canonical()["metadata_root"].as_str().unwrap()
        );
        assert_eq!(server.calls.load(Ordering::SeqCst), 2);
        assert_eq!(server.client.retry_count(), 0);
    }
}

#[tokio::test]
async fn request_wrapper_and_inner_snapshot_ids_must_all_match() {
    let server = Server::start(canonical(), StatusCode::OK).await;
    assert_eq!(
        server.client.descriptor(OTHER_SID).await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    let mut bad_outer = wrapper(canonical());
    bad_outer["snapshot_id"] = json!(OTHER_SID);
    let mut bad_inner = canonical();
    bad_inner["snapshot_id"] = json!(OTHER_SID);
    for body in [bad_outer, wrapper(bad_inner)] {
        let server = Server::start(body, StatusCode::OK).await;
        assert_eq!(
            server
                .client
                .verified_descriptor(SID)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::IntegrityError
        );
        assert_eq!(server.calls.load(Ordering::SeqCst), 1);
        assert_eq!(server.client.retry_count(), 0);
    }
}

#[tokio::test]
async fn closed_schemas_reject_missing_null_unknown_and_mixed_fields_without_fallback() {
    let valid = canonical();
    let mut cases = vec![json!([]), json!(null)];
    for field in valid.as_object().unwrap().keys() {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        cases.push(missing.clone());
        cases.push(wrapper(missing));
        let mut null = valid.clone();
        null[field] = Value::Null;
        cases.push(null);
    }
    let mut extra = valid.clone();
    extra["extra"] = json!(1);
    cases.push(extra.clone());
    cases.push(wrapper(extra));
    for field in ["snapshot_id", "descriptor", "lease_id", "lease_expires_at"] {
        let mut missing = wrapper(valid.clone());
        missing.as_object_mut().unwrap().remove(field);
        cases.push(missing);
        let mut null = wrapper(valid.clone());
        null[field] = Value::Null;
        cases.push(null);
    }
    let mut mixed = valid.clone();
    mixed["descriptor"] = valid.clone();
    cases.push(mixed);
    let mut extra = wrapper(valid);
    extra["metadata_root"] = canonical()["metadata_root"].clone();
    cases.push(extra);
    for body in cases {
        let server = Server::start(body, StatusCode::OK).await;
        assert_eq!(
            server.client.descriptor(SID).await.unwrap_err().code,
            SnapshotErrorCode::IntegrityError
        );
        assert_eq!(server.client.retry_count(), 0);
    }
}

#[tokio::test]
async fn changed_descriptor_facts_cannot_keep_a_valid_snapshot_identity() {
    for (field, value) in [
        ("schema_version", json!(3)),
        ("metadata_codec", json!(2)),
        ("materialization_policy", json!(2)),
        ("fs_semantics", json!(2)),
        ("access_projection", json!(1)),
        ("instance_id", json!("00000000-0000-0000-0000-000000000000")),
        ("namespace_view_id", json!(OTHER_SID)),
        ("metadata_root", json!(OTHER_SID)),
        ("scope", json!("/project/another")),
    ] {
        let mut body = canonical();
        body[field] = value;
        let server = Server::start(body, StatusCode::OK).await;
        let code = server
            .client
            .verified_descriptor(SID)
            .await
            .unwrap_err()
            .code;
        assert!(matches!(
            code,
            SnapshotErrorCode::IntegrityError | SnapshotErrorCode::DigestMismatch
        ));
        assert_eq!(server.client.retry_count(), 0);
    }
}

#[tokio::test]
async fn typed_descriptor_errors_remain_errors() {
    let server = Server::start(
        json!({"error":{"code":"SNAPSHOT_GONE", "message":"gone"}}),
        StatusCode::GONE,
    )
    .await;
    let error = server.client.verified_descriptor(SID).await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::SnapshotGone);
    assert_eq!(error.http_status, 410);
    assert_eq!(server.calls.load(Ordering::SeqCst), 1);
}
