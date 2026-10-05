//! Path preconditions belong to every caller, including a waiter that would
//! otherwise join an existing download. These tests do not assume membership
//! proofs or use a bare hash as authorization.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use mst2_codec::descriptor::ServingDescriptor;
use scorpiofs::snapshot::{
    durable::digest_of,
    range::{ChunkedFile, OBJECT_CAP},
    FetchCoordinator, Mst2Client, SnapshotErrorCode, SnapshotFile, SnapshotReader,
};
use serde_json::{json, Value};
use tokio::{sync::Notify, task::JoinHandle};

const SCOPE: &str = "/project";
const INSTANCE: &str = "11111111-2222-4333-8444-555555555557";
const CONTENT: &[u8] = b"verified fixture content";

#[tokio::test]
async fn foreign_or_legacy_store_is_rejected_before_recovery_mutates_its_marker() {
    let fixture = Arc::new(Fixture::default());
    let server = serve(fixture).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), SCOPE, 600)
        .await
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    for bound in [false, true] {
        let root = temp.path().join(if bound { "foreign" } else { "legacy" });
        let content = temp.path().join(if bound {
            "foreign-blobs"
        } else {
            "legacy-blobs"
        });
        if bound {
            reader.authorized_context().bind_view_cache(&root).unwrap();
            let path = root.join("authority.json");
            let mut identity: Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            identity["domain"] = "different-actor".into();
            std::fs::write(path, serde_json::to_vec(&identity).unwrap()).unwrap();
        } else {
            std::fs::create_dir_all(&root).unwrap();
        }
        let marker = b"malformed complete marker owned elsewhere";
        std::fs::write(root.join("DURABLE_COMPLETE"), marker).unwrap();
        let result = scorpiofs::snapshot::DurableStore::open_for_reader(&root, &content, &reader);
        assert!(matches!(result, Err(error) if error.code == SnapshotErrorCode::ScopeForbidden));
        assert_eq!(
            std::fs::read(root.join("DURABLE_COMPLETE")).unwrap(),
            marker
        );
        assert!(!root.join("NEEDS_REPAIR").exists());
        assert!(!root.join(".hydrate.lock").exists());
        assert!(!content.exists());
    }
}

#[derive(Default)]
struct Fixture {
    blob_requests: AtomicUsize,
    map_requests: AtomicUsize,
    blob_started: Notify,
    blob_release: Notify,
}

async fn capabilities() -> Json<Value> {
    Json(json!({
        "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true, "raw_blob": true, "chunk_reads": true}
    }))
}

async fn resolve() -> Json<Value> {
    let descriptor = ServingDescriptor {
        instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
        namespace_view_id: [0x22; 32],
        scope: SCOPE.into(),
        metadata_root: [0x01; 32],
    };
    Json(json!({
        "descriptor": {
            "schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE,
            "namespace_view_id": format!("sha256:{}", hex::encode(descriptor.namespace_view_id)),
            "scope": SCOPE, "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
            "metadata_root": format!("sha256:{}", hex::encode(descriptor.metadata_root)),
            "snapshot_id": format!("sha256:{}", hex::encode(descriptor.snapshot_id().unwrap()))
        },
        "lease_id": "paths-fixture-lease", "lease_expires_at": "2099-01-01T00:00:00Z",
        "publication_sequence": "1", "authorization_epoch": "1"
    }))
}

async fn blob(State(fixture): State<Arc<Fixture>>) -> &'static [u8] {
    fixture.blob_requests.fetch_add(1, Ordering::SeqCst);
    fixture.blob_started.notify_one();
    fixture.blob_release.notified().await;
    CONTENT
}

async fn chunk_map(State(fixture): State<Arc<Fixture>>) -> StatusCode {
    fixture.map_requests.fetch_add(1, Ordering::SeqCst);
    StatusCode::INTERNAL_SERVER_ERROR
}

struct Server {
    url: String,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(fixture: Arc<Fixture>) -> Server {
    let app = Router::new()
        .route("/api/v2/snapshots/capabilities", get(capabilities))
        .route("/api/v2/snapshots/resolve", post(resolve))
        .route("/api/v2/snapshots/{snapshot}/blob", get(blob))
        .route("/api/v2/snapshots/{snapshot}/chunk-map", get(chunk_map))
        .with_state(fixture);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    Server {
        url,
        task: tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
    }
}

fn invalid_paths() -> Vec<(String, SnapshotErrorCode)> {
    vec![
        ("../secret".into(), SnapshotErrorCode::ScopeInvalid),
        ("/a//b".into(), SnapshotErrorCode::ScopeInvalid),
        ("/a/./b".into(), SnapshotErrorCode::ScopeInvalid),
        ("x\0y".into(), SnapshotErrorCode::ScopeInvalid),
        ("x".repeat(256), SnapshotErrorCode::LimitExceeded),
        // Each relative path is at its own limit; adding /project exceeds
        // the full absolute path's component or byte budget.
        (vec!["x"; 256].join("/"), SnapshotErrorCode::LimitExceeded),
        (
            vec!["x".repeat(255); 16].join("/"),
            SnapshotErrorCode::LimitExceeded,
        ),
    ]
}

#[tokio::test]
async fn invalid_waiter_paths_cannot_join_a_valid_in_flight_content_request() {
    let fixture = Arc::new(Fixture::default());
    let server = serve(fixture.clone()).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), SCOPE, 600)
        .await
        .unwrap();
    let coordinator = FetchCoordinator::new(reader, 1);
    let file = SnapshotFile {
        rel_path: "file.txt".into(),
        fs_kind: "regular".into(),
        size: CONTENT.len() as u64,
        content_digest: digest_of(CONTENT),
    };
    let leader_coordinator = coordinator.clone();
    let leader_file = file.clone();
    let leader = tokio::spawn(async move { leader_coordinator.fetch(leader_file, false).await });
    tokio::time::timeout(Duration::from_secs(5), fixture.blob_started.notified())
        .await
        .unwrap();
    for (path, expected) in invalid_paths() {
        let invalid = SnapshotFile {
            rel_path: path.clone(),
            ..file.clone()
        };
        let error = tokio::time::timeout(Duration::from_secs(1), coordinator.fetch(invalid, false))
            .await
            .expect("invalid waiter joined the blocked valid download")
            .unwrap_err();
        assert_eq!(error.code, expected, "{path:?}");
    }
    assert_eq!(fixture.blob_requests.load(Ordering::SeqCst), 1);
    fixture.blob_release.notify_one();
    let bytes = tokio::time::timeout(Duration::from_secs(5), leader)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        bytes.as_slice(),
        CONTENT,
        "path rejection affected the valid leader"
    );
    assert_eq!(fixture.blob_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn large_file_open_rejects_invalid_composed_paths_before_chunk_map_rpc() {
    let fixture = Arc::new(Fixture::default());
    let server = serve(fixture.clone()).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), SCOPE, 600)
        .await
        .unwrap();
    for (path, expected) in invalid_paths() {
        let error =
            match ChunkedFile::open(&reader, &path, &digest_of(CONTENT), OBJECT_CAP + 1).await {
                Ok(_) => panic!("invalid range-read path was accepted: {path:?}"),
                Err(error) => error,
            };
        assert_eq!(error.code, expected, "{path:?}");
    }
    assert_eq!(
        fixture.map_requests.load(Ordering::SeqCst),
        0,
        "invalid path reached the range transport"
    );
}
