//! Exercise range budgets and cache eviction through verified HTTP responses.
//! The sparse fixture proves chunk bytes and leaves, not whole-file COMPLETE.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use mst2_codec::{
    chunkmap::{
        leaf_proof, merkle_root, ChunkLeaf, ChunkMap, ProofSide, CHUNKS_PER_PAGE, CHUNK_SIZE,
    },
    descriptor::ServingDescriptor,
    treeframe::{ChunkPayload, EndPayload},
};
use scorpiofs::snapshot::{
    client::MAX_BUFFERED_FILE_BYTES, ChunkedFile, Mst2Client, SnapshotErrorCode, SnapshotReader,
};
use serde_json::{json, Value};

const INSTANCE: &str = "11111111-2222-4333-8444-555555555558";
const CONTENT: [u8; 32] = [0xaa; 32];
const NO_CORRUPTION: u64 = u64::MAX;
const CHUNK: u64 = CHUNK_SIZE as u64;

fn digest(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

fn id(hash: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(hash))
}

fn pattern(index: u64) -> u8 {
    (index % 4 + 1) as u8
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for part in bytes.chunks(3) {
        let bits = ((part[0] as u32) << 16)
            | ((part.get(1).copied().unwrap_or(0) as u32) << 8)
            | part.get(2).copied().unwrap_or(0) as u32;
        out.push(ALPHABET[(bits >> 18) as usize] as char);
        out.push(ALPHABET[((bits >> 12) & 63) as usize] as char);
        out.push(if part.len() > 1 {
            ALPHABET[((bits >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if part.len() > 2 {
            ALPHABET[(bits & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

struct Fixture {
    map: ChunkMap,
    leaves: Vec<ChunkLeaf>,
    hashes: Vec<[u8; 32]>,
    chunk_requests: Mutex<Vec<u64>>,
    leaf_requests: Mutex<Vec<u64>>,
    corrupt_chunk: AtomicU64,
    corrupt_leaf: AtomicU64,
}

async fn capabilities() -> Json<Value> {
    Json(json!({
        "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true, "chunk_reads": true}
    }))
}

async fn resolve() -> Json<Value> {
    let descriptor = ServingDescriptor {
        instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
        namespace_view_id: [0x22; 32],
        scope: "/project".into(),
        metadata_root: [1; 32],
    };
    Json(json!({
        "descriptor": {
            "schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE,
            "namespace_view_id": id(&descriptor.namespace_view_id), "scope": "/project",
            "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
            "metadata_root": id(&descriptor.metadata_root), "snapshot_id": id(&descriptor.snapshot_id().unwrap())
        },
        "lease_id": "range-budget-lease", "lease_expires_at": "2099-01-01T00:00:00Z",
        "publication_sequence": "1", "authorization_epoch": "1"
    }))
}

async fn map(
    State(fixture): State<Arc<Fixture>>,
    Path(snapshot): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let map = &fixture.map;
    Json(json!({
        "snapshot_id": snapshot, "path": query["path"], "schema_version": 2,
        "file_content_id": id(&CONTENT), "map_id": id(&map.map_id()),
        "file_size": map.file_size.to_string(), "chunk_size": CHUNK_SIZE,
        "chunk_count": map.chunk_count.to_string(), "page_count": map.page_count.to_string(),
        "pages_root": id(&map.pages_root)
    }))
}

async fn leaf(
    State(fixture): State<Arc<Fixture>>,
    Path(snapshot): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let page: u64 = query["page"].parse().unwrap();
    fixture.leaf_requests.lock().unwrap().push(page);
    let mut leaf = fixture.leaves[page as usize].clone();
    if fixture.corrupt_leaf.load(Ordering::SeqCst) == page {
        leaf.chunk_sha256[0][0] ^= 1;
    }
    let proof: Vec<_> = leaf_proof(&fixture.hashes, page)
        .unwrap()
        .into_iter()
        .map(|step| {
            json!({
                "side": match step.side { ProofSide::Left => "left", ProofSide::Right => "right" },
                "sibling_pages": step.sibling_pages.to_string(), "digest": id(&step.digest)
            })
        })
        .collect();
    Json(json!({
        "snapshot_id": snapshot, "path": query["path"], "map_id": id(&fixture.map.map_id()),
        "page_count": fixture.map.page_count.to_string(),
        "leaf": {"page_index": page.to_string(), "count": leaf.chunk_sha256.len().to_string(),
                 "data_base64": base64(&leaf.encode().unwrap())},
        "proof": proof
    }))
}

async fn chunks(
    State(fixture): State<Arc<Fixture>>,
    Path(snapshot): Path<String>,
    body: Bytes,
) -> Response {
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let index: u64 = items[0]["chunk_index"].as_str().unwrap().parse().unwrap();
    assert_eq!(items[0]["path"], "file");
    assert_eq!(items[0]["map_id"], id(&fixture.map.map_id()));
    assert_eq!(items[0]["expected_digest"], id(&CONTENT));
    fixture.chunk_requests.lock().unwrap().push(index);
    let mut bytes = vec![pattern(index); fixture.map.chunk_len(index).unwrap() as usize];
    if fixture.corrupt_chunk.load(Ordering::SeqCst) == index {
        bytes[0] ^= 1;
    }
    let mut wire = ChunkPayload {
        map_id: fixture.map.map_id(),
        file_content_id: CONTENT,
        chunk_index: index,
        chunk_bytes: bytes,
    }
    .encode(19, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: fixture.map.chunk_len(index).unwrap(),
            request_body_sha256: digest(&body),
        }
        .encode(19, 1),
    );
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", snapshot)
        .header("x-mega-request-digest", id(&digest(&body)))
        .body(axum::body::Body::from(wire))
        .unwrap()
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn open(size: u64) -> (Server, Arc<Fixture>, SnapshotReader, ChunkedFile) {
    let count = size.div_ceil(CHUNK);
    let full: Vec<_> = (0..4)
        .map(|i| digest(&vec![pattern(i); CHUNK_SIZE as usize]))
        .collect();
    let all: Vec<_> = (0..count)
        .map(|i| {
            let len = (size - i * CHUNK).min(CHUNK);
            if len == CHUNK {
                full[(i % 4) as usize]
            } else {
                digest(&vec![pattern(i); len as usize])
            }
        })
        .collect();
    let leaves: Vec<_> = all
        .chunks(CHUNKS_PER_PAGE)
        .enumerate()
        .map(|(page, values)| ChunkLeaf {
            page_index: page as u64,
            chunk_sha256: values.to_vec(),
        })
        .collect();
    let hashes: Vec<_> = leaves
        .iter()
        .map(|leaf| leaf.leaf_hash().unwrap())
        .collect();
    let fixture = Arc::new(Fixture {
        map: ChunkMap::new(CONTENT, size, merkle_root(&hashes).unwrap()).unwrap(),
        leaves,
        hashes,
        chunk_requests: Mutex::new(Vec::new()),
        leaf_requests: Mutex::new(Vec::new()),
        corrupt_chunk: AtomicU64::new(NO_CORRUPTION),
        corrupt_leaf: AtomicU64::new(NO_CORRUPTION),
    });
    let app = Router::new()
        .route("/api/v2/snapshots/capabilities", get(capabilities))
        .route("/api/v2/snapshots/resolve", post(resolve))
        .route("/api/v2/snapshots/{snapshot}/chunk-map", get(map))
        .route("/api/v2/snapshots/{snapshot}/chunk-map/pages", get(leaf))
        .route("/api/v2/snapshots/{snapshot}/chunks", post(chunks))
        .with_state(fixture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = Server(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    }));
    let reader = SnapshotReader::resolve(Mst2Client::new(base), "/project", 600)
        .await
        .unwrap();
    let file = ChunkedFile::open(&reader, "file", &id(&CONTENT), size)
        .await
        .unwrap();
    (server, fixture, reader, file)
}

fn assert_bytes(bytes: &[u8], offset: u64) {
    // Expected bytes depend only on the requested absolute position, not caches.
    for (i, byte) in bytes.iter().enumerate() {
        assert_eq!(*byte, pattern((offset + i as u64) / CHUNK));
    }
}

#[tokio::test]
async fn output_limit_is_checked_before_content_and_after_eof_clamping() {
    let size = 80 * CHUNK + 7;
    let (_server, fixture, reader, file) = open(size).await;
    for length in [MAX_BUFFERED_FILE_BYTES + 1, u64::MAX] {
        assert_eq!(
            file.read_range(0, length).await.unwrap_err().code,
            SnapshotErrorCode::LimitExceeded
        );
    }
    assert!(fixture.leaf_requests.lock().unwrap().is_empty());
    assert!(fixture.chunk_requests.lock().unwrap().is_empty());
    assert!(file
        .read_range(u64::MAX, u64::MAX)
        .await
        .unwrap()
        .is_empty());
    let tail = file.read_range(size - 3, u64::MAX).await.unwrap();
    assert_eq!(tail.len(), 3);
    assert_bytes(&tail, size - 3);
    assert_eq!(fixture.chunk_requests.lock().unwrap().as_slice(), &[80]);
    let result = ChunkedFile::open(&reader, "file", &id(&CONTENT), size + 1).await;
    assert_eq!(
        result.err().unwrap().code,
        SnapshotErrorCode::DigestMismatch
    );
}

#[tokio::test]
async fn output_larger_than_cache_stays_correct_and_evicted_chunk_is_reverified() {
    let (_server, fixture, _reader, file) = open(80 * CHUNK).await;
    let bytes = file.read_range(0, 20 * CHUNK).await.unwrap();
    assert_bytes(&bytes, 0);
    assert_eq!(bytes.len() as u64, 20 * CHUNK);
    assert_eq!(fixture.chunk_requests.lock().unwrap().len(), 20);
    file.read_range(19 * CHUNK, 7).await.unwrap();
    assert_eq!(fixture.chunk_requests.lock().unwrap().len(), 20);
    assert!(!file.fully_cached().await);
    fixture.corrupt_chunk.store(0, Ordering::SeqCst);
    assert_eq!(
        file.read_range(0, 7).await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    fixture.corrupt_chunk.store(NO_CORRUPTION, Ordering::SeqCst);
    assert_bytes(&file.read_range(0, 7).await.unwrap(), 0);
    assert_eq!(
        fixture
            .chunk_requests
            .lock()
            .unwrap()
            .iter()
            .filter(|i| **i == 0)
            .count(),
        3
    );
    assert_eq!(fixture.leaf_requests.lock().unwrap().as_slice(), &[0]);
}

#[tokio::test]
async fn leaf_cache_refreshes_hits_and_rechecks_proof_after_eviction() {
    let page_bytes = CHUNKS_PER_PAGE as u64 * CHUNK;
    let (_server, fixture, _reader, file) = open(17 * page_bytes).await;
    for page in 0..16 {
        assert_bytes(
            &file.read_range(page * page_bytes, 1).await.unwrap(),
            page * page_bytes,
        );
    }
    // A second chunk on page zero refreshes that leaf without a leaf HTTP call.
    file.read_range(CHUNK, 1).await.unwrap();
    file.read_range(16 * page_bytes, 1).await.unwrap();
    assert_eq!(fixture.leaf_requests.lock().unwrap().len(), 17);
    // Page one was least recently used; both it and its old chunk were evicted.
    fixture.corrupt_leaf.store(1, Ordering::SeqCst);
    assert_eq!(
        file.read_range(page_bytes, 1).await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    fixture.corrupt_leaf.store(NO_CORRUPTION, Ordering::SeqCst);
    assert_bytes(&file.read_range(page_bytes, 1).await.unwrap(), page_bytes);
    assert_eq!(
        fixture
            .leaf_requests
            .lock()
            .unwrap()
            .iter()
            .filter(|i| **i == 1)
            .count(),
        3
    );
}

#[tokio::test]
async fn concurrent_reads_remain_correct_while_evicting_each_others_chunks() {
    let (_server, fixture, _reader, file) = open(80 * CHUNK).await;
    let file = &file;
    let reads = (0..8).map(|i| async move {
        let offset = i * 8 * CHUNK + 13;
        let bytes = file.read_range(offset, 3 * CHUNK + 7).await.unwrap();
        assert_eq!(bytes.len() as u64, 3 * CHUNK + 7);
        assert_bytes(&bytes, offset);
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        futures::future::join_all(reads),
    )
    .await
    .unwrap();
    assert_eq!(fixture.chunk_requests.lock().unwrap().len(), 32);
    // At most sixteen verified chunks remain, observable by refetching an old one.
    let before = fixture.chunk_requests.lock().unwrap().len();
    for index in (0..8).map(|i| i * 8) {
        file.read_range(index * CHUNK, 1).await.unwrap();
    }
    assert!(fixture.chunk_requests.lock().unwrap().len() > before);
}
