//! Whole-file APIs fail with a typed local limit before unbounded allocation.
//! Large files remain supported by the separately bounded range-read path.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use axum::{
    extract::{Path, Query, State},
    routing::{get, post},
    Json, Router,
};
use mst2_codec::{chunkmap::ChunkMap, descriptor::ServingDescriptor};
use scorpiofs::snapshot::{
    client::MAX_BUFFERED_FILE_BYTES, Mst2Client, SnapshotErrorCode, SnapshotReader,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DIGEST: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const INSTANCE: &str = "11111111-2222-4333-8444-555555555557";

async fn capabilities() -> Json<Value> {
    Json(json!({
        "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true,
                     "raw_blob": true, "objects": true, "chunk_reads": true}
    }))
}

async fn resolve() -> Json<Value> {
    let descriptor = ServingDescriptor {
        instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
        namespace_view_id: [0x22; 32],
        scope: "/project".into(),
        metadata_root: [0x01; 32],
    };
    Json(json!({
        "descriptor": {
            "schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE,
            "namespace_view_id": format!("sha256:{}", hex::encode(descriptor.namespace_view_id)),
            "scope": "/project", "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
            "metadata_root": format!("sha256:{}", hex::encode(descriptor.metadata_root)),
            "snapshot_id": format!("sha256:{}", hex::encode(descriptor.snapshot_id().unwrap()))
        },
        "lease_id": "budget-lease", "lease_expires_at": "2099-01-01T00:00:00Z",
        "publication_sequence": "1", "authorization_epoch": "1"
    }))
}

#[derive(Default)]
struct Counters {
    maps: AtomicUsize,
    leaves: AtomicUsize,
    objects: AtomicUsize,
}

async fn wrong_map(
    State(counters): State<Arc<Counters>>,
    Path(snapshot): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    counters.maps.fetch_add(1, Ordering::SeqCst);
    let map = ChunkMap::new([0xaa; 32], 8 * 1024 * 1024 * 1024 * 1024, [0x11; 32]).unwrap();
    Json(json!({
        "snapshot_id": snapshot, "path": query["path"], "schema_version": 2,
        "file_content_id": DIGEST, "map_id": format!("sha256:{}", hex::encode(map.map_id())),
        "file_size": map.file_size.to_string(), "chunk_size": 1024 * 1024,
        "chunk_count": map.chunk_count.to_string(), "page_count": map.page_count.to_string(),
        "pages_root": format!("sha256:{}", hex::encode(map.pages_root))
    }))
}

async fn leaf(State(counters): State<Arc<Counters>>) -> Json<Value> {
    counters.leaves.fetch_add(1, Ordering::SeqCst);
    Json(json!({}))
}

async fn objects(State(counters): State<Arc<Counters>>) -> Json<Value> {
    counters.objects.fetch_add(1, Ordering::SeqCst);
    Json(json!({}))
}

#[tokio::test]
async fn buffered_frame_limit_rejects_before_content_request_and_wrong_map_before_leaf_allocation()
{
    let counters = Arc::new(Counters::default());
    let app = Router::new()
        .route("/api/v2/snapshots/capabilities", get(capabilities))
        .route("/api/v2/snapshots/resolve", post(resolve))
        .route("/api/v2/snapshots/{snapshot}/chunk-map", get(wrong_map))
        .route("/api/v2/snapshots/{snapshot}/chunk-map/pages", get(leaf))
        .route("/api/v2/snapshots/{snapshot}/objects", post(objects))
        .with_state(counters.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let reader = SnapshotReader::resolve(
        Mst2Client::new(format!("http://{address}")),
        "/project",
        600,
    )
    .await
    .unwrap();
    for size in [
        MAX_BUFFERED_FILE_BYTES + 1,
        8 * 1024 * 1024 * 1024 * 1024,
        u64::MAX,
    ] {
        assert_eq!(
            reader
                .read_file_frames("file", DIGEST, size)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::LimitExceeded
        );
    }
    assert_eq!(counters.maps.load(Ordering::SeqCst), 0);
    assert_eq!(counters.objects.load(Ordering::SeqCst), 0);
    assert_eq!(
        reader
            .read_file_frames("file", DIGEST, 1024 * 1024)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(counters.maps.load(Ordering::SeqCst), 1);
    assert_eq!(counters.leaves.load(Ordering::SeqCst), 0);
    server.abort();
    let _ = server.await;
}

/// Raw HTTP keeps misleading length/framing observable instead of letting a
/// framework normalize it. Chunked framing is the actual body boundary.
async fn raw_blob(headers: &str, blocks: usize) -> (Mst2Client, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let headers = headers.to_string();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        while !request.windows(4).any(|x| x == b"\r\n\r\n") {
            let n = socket.read(&mut buf).await.unwrap();
            if n == 0 {
                return;
            }
            request.extend_from_slice(&buf[..n]);
        }
        if socket
            .write_all(format!("HTTP/1.1 200 OK\r\nConnection: close\r\n{headers}\r\n").as_bytes())
            .await
            .is_err()
        {
            return;
        }
        let block = vec![b'x'; 1024 * 1024];
        for _ in 0..blocks {
            if socket.write_all(b"100000\r\n").await.is_err()
                || socket.write_all(&block).await.is_err()
                || socket.write_all(b"\r\n").await.is_err()
            {
                return;
            }
        }
        let _ = socket.write_all(b"0\r\n\r\n").await;
    });
    (Mst2Client::new(format!("http://{address}")), task)
}

#[tokio::test]
async fn raw_blob_counts_actual_chunked_bytes_with_absent_or_false_content_length() {
    for headers in [
        "Transfer-Encoding: chunked\r\n",
        "Transfer-Encoding: chunked\r\nContent-Length: 1\r\n",
    ] {
        let (client, server) = raw_blob(headers, 65).await;
        let error = client
            .blob_verified("budget", "/file", DIGEST)
            .await
            .unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::LimitExceeded);
        assert!(client.received_bytes() > MAX_BUFFERED_FILE_BYTES);
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn raw_blob_rejects_oversized_length_without_collecting_the_body() {
    let (client, server) = raw_blob(
        &format!("Content-Length: {}\r\n", MAX_BUFFERED_FILE_BYTES + 1),
        0,
    )
    .await;
    assert_eq!(
        client
            .blob_verified("budget", "/file", DIGEST)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(client.received_bytes(), 0);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn bounded_blob_still_verifies_whole_content_digest() {
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    digest.update(&vec![b'x'; 1024 * 1024]);
    let wanted = format!("sha256:{}", hex::encode(digest.finish().as_ref()));
    let (client, server) = raw_blob("Transfer-Encoding: chunked\r\n", 1).await;
    let bytes = client
        .blob_verified("budget", "/file", &wanted)
        .await
        .unwrap();
    assert_eq!(bytes.len(), 1024 * 1024);
    assert!(bytes.iter().all(|x| *x == b'x'));
    server.abort();
    let _ = server.await;
}
