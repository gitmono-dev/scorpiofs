//! Valid chunk proofs cannot excuse a misbound legacy response envelope.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{routing::get, Json, Router};
use mst2_codec::chunkmap::{ChunkLeaf, ChunkMap, CHUNK_SIZE};
use scorpiofs::snapshot::{frames::VerifiedChunkMap, Mst2Client, SnapshotErrorCode};
use serde_json::{json, Value};

const SID: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PATH: &str = "file";

fn id(bytes: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(bytes))
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
    content: String,
    map: ChunkMap,
    leaf: ChunkLeaf,
}

impl Fixture {
    fn new() -> Self {
        let body = vec![0x51; CHUNK_SIZE as usize + 7];
        let hash = |bytes: &[u8]| {
            ring::digest::digest(&ring::digest::SHA256, bytes)
                .as_ref()
                .try_into()
                .unwrap()
        };
        let leaf = ChunkLeaf {
            page_index: 0,
            chunk_sha256: body.chunks(CHUNK_SIZE as usize).map(hash).collect(),
        };
        let map = ChunkMap::new(hash(&body), body.len() as u64, leaf.leaf_hash().unwrap()).unwrap();
        Self {
            content: id(&map.file_content_id),
            map,
            leaf,
        }
    }

    fn map_body(&self) -> Value {
        json!({
            "snapshot_id": SID, "path": PATH, "schema_version": 2,
            "file_content_id": self.content, "file_size": self.map.file_size.to_string(),
            "chunk_size": CHUNK_SIZE, "chunk_count": self.map.chunk_count.to_string(),
            "page_count": self.map.page_count.to_string(), "pages_root": id(&self.map.pages_root),
            "map_id": id(&self.map.map_id())
        })
    }

    fn leaf_body(&self) -> Value {
        json!({
            "snapshot_id": SID, "path": PATH, "map_id": id(&self.map.map_id()),
            "page_count": self.map.page_count.to_string(),
            "leaf": {"page_index": "0", "count": "2", "data_base64": base64(&self.leaf.encode().unwrap())},
            "proof": []
        })
    }

    fn verified(&self) -> VerifiedChunkMap {
        VerifiedChunkMap {
            file_content_id: self.content.clone(),
            map_id: id(&self.map.map_id()),
            file_size: self.map.file_size,
            chunk_count: self.map.chunk_count,
            page_count: self.map.page_count,
            pages_root: self.map.pages_root,
        }
    }
}

struct Server {
    client: Mst2Client,
    maps: Arc<AtomicUsize>,
    leaves: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(map: Value, leaf: Value) -> Self {
        let maps = Arc::new(AtomicUsize::new(0));
        let leaves = Arc::new(AtomicUsize::new(0));
        let map_calls = maps.clone();
        let leaf_calls = leaves.clone();
        let app = Router::new()
            .route(
                "/api/v2/snapshots/{sid}/chunk-map",
                get(move || {
                    map_calls.fetch_add(1, Ordering::SeqCst);
                    let map = map.clone();
                    async move { Json(map) }
                }),
            )
            .route(
                "/api/v2/snapshots/{sid}/chunk-map/pages",
                get(move || {
                    leaf_calls.fetch_add(1, Ordering::SeqCst);
                    let leaf = leaf.clone();
                    async move { Json(leaf) }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            client,
            maps,
            leaves,
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
async fn map_snapshot_path_and_profile_are_required_and_bound() {
    let fixture = Fixture::new();
    for (field, wrong) in [
        ("snapshot_id", json!("another-snapshot")),
        ("path", json!("another-file")),
        ("schema_version", json!(1)),
        ("schema_version", json!("2")),
    ] {
        let mut map = fixture.map_body();
        map[field] = wrong;
        let server = Server::start(map, fixture.leaf_body()).await;
        assert_eq!(
            server
                .client
                .chunk_map(SID, PATH, &fixture.content)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::IntegrityError
        );
    }
    for field in ["snapshot_id", "path", "schema_version"] {
        let mut map = fixture.map_body();
        map.as_object_mut().unwrap().remove(field);
        let server = Server::start(map, fixture.leaf_body()).await;
        assert_eq!(
            server
                .client
                .chunk_map(SID, PATH, &fixture.content)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::IntegrityError
        );
    }
    let server = Server::start(fixture.map_body(), fixture.leaf_body()).await;
    assert_eq!(
        server
            .client
            .chunk_map(SID, PATH, "invalid-digest")
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(server.maps.load(Ordering::SeqCst), 0);
    let map = server
        .client
        .chunk_map(SID, PATH, &fixture.content)
        .await
        .unwrap();
    assert_eq!(map.map_id, id(&fixture.map.map_id()));
    assert_eq!(server.client.retry_count(), 0);
}

#[tokio::test]
async fn a_valid_leaf_and_proof_cannot_bless_a_wrong_or_incomplete_envelope() {
    let fixture = Fixture::new();
    let mut cases = Vec::new();
    for (field, wrong) in [
        ("snapshot_id", json!("another-snapshot")),
        ("path", json!("another-file")),
        ("map_id", json!(id(&[0xbb; 32]))),
        ("page_count", json!("2")),
    ] {
        let mut leaf = fixture.leaf_body();
        leaf[field] = wrong;
        cases.push(leaf);
    }
    for (field, wrong) in [("page_index", "1"), ("count", "1")] {
        let mut leaf = fixture.leaf_body();
        leaf["leaf"][field] = json!(wrong);
        cases.push(leaf);
    }
    for field in ["snapshot_id", "path", "map_id", "proof"] {
        let mut leaf = fixture.leaf_body();
        leaf.as_object_mut().unwrap().remove(field);
        cases.push(leaf);
    }
    let mut leaf = fixture.leaf_body();
    leaf["proof"] = Value::Null;
    cases.push(leaf);
    let mut leaf = fixture.leaf_body();
    leaf["proof"] = json!({});
    cases.push(leaf);
    for leaf in cases {
        let server = Server::start(fixture.map_body(), leaf).await;
        assert_eq!(
            server
                .client
                .chunk_map_page(SID, PATH, &fixture.content, &fixture.verified(), 0)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::IntegrityError
        );
        assert_eq!(server.client.retry_count(), 0);
    }
    for field in ["page_count", "page_index", "count"] {
        for bad in [Value::Null, json!(0), json!("01")] {
            let mut leaf = fixture.leaf_body();
            if field == "page_count" {
                leaf[field] = bad;
            } else {
                leaf["leaf"][field] = bad;
            }
            let server = Server::start(fixture.map_body(), leaf).await;
            assert_eq!(
                server
                    .client
                    .chunk_map_page(SID, PATH, &fixture.content, &fixture.verified(), 0)
                    .await
                    .unwrap_err()
                    .code,
                SnapshotErrorCode::ScopeInvalid
            );
        }
        let mut leaf = fixture.leaf_body();
        if field == "page_count" {
            leaf.as_object_mut().unwrap().remove(field);
        } else {
            leaf["leaf"].as_object_mut().unwrap().remove(field);
        }
        let server = Server::start(fixture.map_body(), leaf).await;
        assert_eq!(
            server
                .client
                .chunk_map_page(SID, PATH, &fixture.content, &fixture.verified(), 0)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::ScopeInvalid
        );
    }
    let server = Server::start(fixture.map_body(), fixture.leaf_body()).await;
    assert_eq!(
        server
            .client
            .chunk_map_page(SID, PATH, &fixture.content, &fixture.verified(), 0)
            .await
            .unwrap()
            .chunk_sha256,
        fixture.leaf.chunk_sha256
    );
}

#[tokio::test]
async fn forged_local_maps_and_out_of_range_pages_fail_before_network() {
    let fixture = Fixture::new();
    let server = Server::start(fixture.map_body(), fixture.leaf_body()).await;
    let mut map = fixture.verified();
    map.chunk_count += 1;
    assert_eq!(
        server
            .client
            .chunk_map_page(SID, PATH, &fixture.content, &map, 0)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(
        server
            .client
            .chunk_map_page(SID, PATH, &fixture.content, &fixture.verified(), 1)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::ScopeInvalid
    );
    assert_eq!(
        server
            .client
            .chunk_map_page(SID, PATH, &id(&[0xcc; 32]), &fixture.verified(), 0)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(server.leaves.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn protocol_counts_are_canonical_and_the_counter_ceiling_precedes_map_limits() {
    let fixture = Fixture::new();
    for (count, code) in [
        ("01", SnapshotErrorCode::ScopeInvalid),
        ("+1", SnapshotErrorCode::ScopeInvalid),
        ("9223372036854775807", SnapshotErrorCode::DigestMismatch),
        ("9223372036854775808", SnapshotErrorCode::LimitExceeded),
        ("18446744073709551616", SnapshotErrorCode::LimitExceeded),
    ] {
        let mut map = fixture.map_body();
        map["file_size"] = json!(count);
        let server = Server::start(map, fixture.leaf_body()).await;
        assert_eq!(
            server
                .client
                .chunk_map(SID, PATH, &fixture.content)
                .await
                .unwrap_err()
                .code,
            code
        );
    }
    let map = ChunkMap::new(
        fixture.map.file_content_id,
        8 * 1024 * 1024 * 1024 * 1024,
        [0xdd; 32],
    )
    .unwrap();
    let mut body = fixture.map_body();
    body["file_size"] = json!(map.file_size.to_string());
    body["chunk_count"] = json!(map.chunk_count.to_string());
    body["page_count"] = json!(map.page_count.to_string());
    body["pages_root"] = json!(id(&map.pages_root));
    body["map_id"] = json!(id(&map.map_id()));
    let server = Server::start(body, fixture.leaf_body()).await;
    assert_eq!(
        server
            .client
            .chunk_map(SID, PATH, &fixture.content)
            .await
            .unwrap()
            .file_size,
        8 * 1024 * 1024 * 1024 * 1024
    );
    assert_eq!(server.leaves.load(Ordering::SeqCst), 0);
}
