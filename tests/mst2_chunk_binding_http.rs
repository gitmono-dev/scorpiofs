//! Valid chunk proofs cannot excuse a misbound legacy response envelope.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{extract::RawQuery, routing::get, Json, Router};
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
    queries: Arc<std::sync::Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(map: Value, leaf: Value) -> Self {
        let maps = Arc::new(AtomicUsize::new(0));
        let leaves = Arc::new(AtomicUsize::new(0));
        let map_calls = maps.clone();
        let leaf_calls = leaves.clone();
        let queries = Arc::new(std::sync::Mutex::new(Vec::new()));
        let leaf_queries = queries.clone();
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
                get(move |RawQuery(query): RawQuery| {
                    leaf_calls.fetch_add(1, Ordering::SeqCst);
                    leaf_queries.lock().unwrap().push(query.unwrap_or_default());
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
            queries,
            task,
        }
    }
}

fn canonical_map(fixture: &Fixture) -> Value {
    let mut map = fixture.map_body();
    let fields = map.as_object_mut().unwrap();
    let sid = fields.remove("snapshot_id").unwrap();
    let path = fields.remove("path").unwrap();
    json!({"snapshot_id": sid, "path": path, "map": map})
}

fn canonical_leaf(fixture: &Fixture) -> Value {
    json!({
        "map_id": id(&fixture.map.map_id()), "page_index": "0",
        "leaf_base64": base64(&fixture.leaf.encode().unwrap()), "proof": []
    })
}

#[tokio::test]
async fn chunk_leaf_base64_rejects_illegal_padding_and_nonzero_unused_bits() {
    for count in 1..=3 {
        let leaf = ChunkLeaf {
            page_index: 0,
            chunk_sha256: vec![[0x51; 32]; count],
        };
        let map = ChunkMap::new(
            [42; 32],
            (count as u64 - 1) * CHUNK_SIZE as u64 + 7,
            leaf.leaf_hash().unwrap(),
        )
        .unwrap();
        let fixture = Fixture {
            content: id(&map.file_content_id),
            map,
            leaf,
        };
        let valid = canonical_leaf(&fixture);
        let encoded = valid["leaf_base64"].as_str().unwrap();
        let server = Server::start(Value::Null, valid.clone()).await;
        assert_eq!(
            server
                .client
                .chunk_map_page_canonical(SID, PATH, &fixture.content, &fixture.verified(), 0)
                .await
                .unwrap()
                .chunk_sha256,
            fixture.leaf.chunk_sha256
        );
        let mut bad = Vec::new();
        for position in [8, 9] {
            let mut illegal = encoded.as_bytes().to_vec();
            assert_eq!(illegal[position], b'A');
            illegal[position] = b'=';
            bad.push(String::from_utf8(illegal).unwrap());
        }
        if encoded.ends_with('=') {
            const ALPHABET: &[u8] =
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            let padding = if encoded.ends_with("==") { 2 } else { 1 };
            let mut unused_bits = encoded.as_bytes().to_vec();
            let index = unused_bits.len() - padding - 1;
            let symbol = ALPHABET
                .iter()
                .position(|byte| *byte == unused_bits[index])
                .unwrap();
            unused_bits[index] = ALPHABET[symbol + 1];
            bad.push(String::from_utf8(unused_bits).unwrap());
        }
        for encoded in bad {
            let mut canonical = valid.clone();
            canonical["leaf_base64"] = json!(encoded);
            let mut legacy = fixture.leaf_body();
            legacy["leaf"]["count"] = json!(count.to_string());
            legacy["leaf"]["data_base64"] = canonical["leaf_base64"].clone();
            let server = Server::start(Value::Null, canonical).await;
            assert!(server
                .client
                .chunk_map_page_canonical(SID, PATH, &fixture.content, &fixture.verified(), 0)
                .await
                .is_err());
            let server = Server::start(Value::Null, legacy).await;
            assert!(server
                .client
                .chunk_map_page(SID, PATH, &fixture.content, &fixture.verified(), 0)
                .await
                .is_err());
        }
    }
}

#[tokio::test]
async fn canonical_page_proof_is_bound_to_nonzero_index_and_tree_shape() {
    let left = ChunkLeaf {
        page_index: 0,
        chunk_sha256: vec![[1; 32]; 256],
    };
    let right = ChunkLeaf {
        page_index: 1,
        chunk_sha256: vec![[2; 32]],
    };
    let root =
        mst2_codec::chunkmap::merkle_root(&[left.leaf_hash().unwrap(), right.leaf_hash().unwrap()])
            .unwrap();
    let map = ChunkMap::new([42; 32], 256 * CHUNK_SIZE as u64 + 7, root).unwrap();
    let content = id(&map.file_content_id);
    let verified = VerifiedChunkMap {
        file_content_id: content.clone(),
        map_id: id(&map.map_id()),
        file_size: map.file_size,
        chunk_count: map.chunk_count,
        page_count: map.page_count,
        pages_root: root,
    };
    let valid = json!({
        "map_id": verified.map_id, "page_index": "1", "leaf_base64": base64(&right.encode().unwrap()),
        "proof": [{"side":"left","sibling_pages":"1","digest":id(&left.leaf_hash().unwrap())}]
    });
    let server = Server::start(Value::Null, valid.clone()).await;
    assert_eq!(
        server
            .client
            .chunk_map_page_canonical(SID, PATH, &content, &verified, 1)
            .await
            .unwrap()
            .chunk_sha256,
        right.chunk_sha256
    );
    for (field, wrong) in [
        ("side", json!("right")),
        ("sibling_pages", json!("2")),
        ("digest", json!(id(&[9; 32]))),
    ] {
        let mut body = valid.clone();
        body["proof"][0][field] = wrong;
        let server = Server::start(Value::Null, body).await;
        assert!(server
            .client
            .chunk_map_page_canonical(SID, PATH, &content, &verified, 1)
            .await
            .is_err());
    }
    let mut body = valid;
    body["proof"] = json!([]);
    let server = Server::start(Value::Null, body).await;
    assert!(server
        .client
        .chunk_map_page_canonical(SID, PATH, &content, &verified, 1)
        .await
        .is_err());
}

#[tokio::test]
async fn frozen_canonical_map_and_page_use_exact_query_and_independent_codec_identity() {
    let map: Value =
        serde_json::from_str(include_str!("fixtures/mst2_chunk_map_0_2_1.json")).unwrap();
    let leaf: Value =
        serde_json::from_str(include_str!("fixtures/mst2_chunk_page_0_2_1.json")).unwrap();
    let sid = map["snapshot_id"].as_str().unwrap();
    let path = map["path"].as_str().unwrap();
    let digest = map["map"]["file_content_id"].as_str().unwrap();
    let server = Server::start(map.clone(), leaf.clone()).await;
    let verified = server.client.chunk_map(sid, path, digest).await.unwrap();
    assert_eq!(verified.map_id, leaf["map_id"].as_str().unwrap());
    assert_eq!(verified.file_size, 300000);
    let page = server
        .client
        .chunk_map_page_canonical(sid, path, digest, &verified, 0)
        .await
        .unwrap();
    assert_eq!(page.page_index, 0);
    assert_eq!(
        page.chunk_sha256,
        vec![scorpiofs::snapshot::frames::parse_digest(digest).unwrap()]
    );
    let query = server.queries.lock().unwrap()[0].clone();
    let pairs: std::collections::BTreeMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    assert_eq!(
        pairs,
        std::collections::BTreeMap::from([
            ("path".into(), path.into()),
            ("map_id".into(), verified.map_id),
            ("page_index".into(), "0".into())
        ])
    );
    assert_eq!(server.leaves.load(Ordering::SeqCst), 1);
    assert_eq!(server.client.retry_count(), 0);
}

#[tokio::test]
async fn canonical_maps_reject_mixed_unknown_missing_null_and_changed_identity_fields() {
    let fixture = Fixture::new();
    let valid = canonical_map(&fixture);
    let mut cases = vec![json!(null), json!([])];
    for field in ["snapshot_id", "path", "map"] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        cases.push(missing);
        let mut null = valid.clone();
        null[field] = Value::Null;
        cases.push(null);
    }
    let mut mixed = fixture.map_body();
    mixed["map"] = valid["map"].clone();
    cases.push(mixed);
    let mut failed_canonical = fixture.map_body();
    failed_canonical["map"] = Value::Null;
    cases.push(failed_canonical);
    let mut unknown = valid.clone();
    unknown["future"] = json!(1);
    cases.push(unknown);
    let mut unknown = valid.clone();
    unknown["map"]["future"] = json!(1);
    cases.push(unknown);
    let mut legacy_unknown = fixture.map_body();
    legacy_unknown["future"] = json!(1);
    cases.push(legacy_unknown);
    for (field, changed) in [
        ("schema_version", json!("2")),
        ("chunk_size", json!(1)),
        ("file_content_id", json!(id(&[0xcc; 32]))),
        ("file_size", json!("01")),
        ("chunk_count", json!("1")),
        ("page_count", json!("2")),
        ("pages_root", json!(id(&[0xdd; 32]))),
        ("map_id", json!(id(&[0xee; 32]))),
    ] {
        let mut changed_map = valid.clone();
        changed_map["map"][field] = changed;
        cases.push(changed_map);
    }
    for field in [
        "schema_version",
        "file_content_id",
        "file_size",
        "chunk_size",
        "chunk_count",
        "page_count",
        "pages_root",
        "map_id",
    ] {
        let mut missing = valid.clone();
        missing["map"].as_object_mut().unwrap().remove(field);
        cases.push(missing);
    }
    for body in cases {
        let server = Server::start(body.clone(), canonical_leaf(&fixture)).await;
        assert!(
            server
                .client
                .chunk_map(SID, PATH, &fixture.content)
                .await
                .is_err(),
            "accepted {body}"
        );
        assert_eq!(server.maps.load(Ordering::SeqCst), 1);
        assert_eq!(server.leaves.load(Ordering::SeqCst), 0);
        assert_eq!(server.client.retry_count(), 0);
    }
    let server = Server::start(valid, canonical_leaf(&fixture)).await;
    assert_eq!(
        server
            .client
            .chunk_map(SID, PATH, &fixture.content)
            .await
            .unwrap()
            .file_size,
        fixture.map.file_size
    );
}

#[tokio::test]
async fn canonical_pages_reject_mixed_contracts_proof_fields_and_bounded_encoding() {
    let fixture = Fixture::new();
    let valid = canonical_leaf(&fixture);
    let mut cases = Vec::new();
    for field in ["map_id", "page_index", "leaf_base64", "proof"] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        cases.push(missing);
        let mut null = valid.clone();
        null[field] = Value::Null;
        cases.push(null);
    }
    for (field, changed) in [
        ("map_id", json!(id(&[0xdd; 32]))),
        ("page_index", json!("1")),
        ("page_index", json!(0)),
        ("page_index", json!("00")),
        ("future", json!(1)),
        ("leaf_base64", json!("a".repeat(200))),
        (
            "proof",
            json!([{"side":"left","sibling_pages":"1","digest":id(&[1;32]),"future":true}]),
        ),
        (
            "proof",
            json!(vec![
                json!({"side":"left","sibling_pages":"1","digest":id(&[1;32])});
                33
            ]),
        ),
    ] {
        let mut changed_leaf = valid.clone();
        changed_leaf[field] = changed;
        cases.push(changed_leaf);
    }
    let mut mixed = fixture.leaf_body();
    mixed["leaf_base64"] = valid["leaf_base64"].clone();
    cases.push(mixed);
    cases.push(fixture.leaf_body());
    let mut wrong_leaf = fixture.leaf.clone();
    wrong_leaf.page_index = 1;
    let mut wrong = valid.clone();
    wrong["leaf_base64"] = json!(base64(&wrong_leaf.encode().unwrap()));
    cases.push(wrong);
    for body in cases {
        let server = Server::start(canonical_map(&fixture), body.clone()).await;
        assert!(
            server
                .client
                .chunk_map_page_canonical(SID, PATH, &fixture.content, &fixture.verified(), 0)
                .await
                .is_err(),
            "accepted {body}"
        );
        assert_eq!(server.leaves.load(Ordering::SeqCst), 1);
        assert_eq!(server.client.retry_count(), 0);
    }
    for body in [
        {
            let mut v = fixture.leaf_body();
            v["future"] = json!(1);
            v
        },
        {
            let mut v = fixture.leaf_body();
            v["leaf"]["future"] = json!(1);
            v
        },
    ] {
        let server = Server::start(fixture.map_body(), body).await;
        assert!(server
            .client
            .chunk_map_page(SID, PATH, &fixture.content, &fixture.verified(), 0)
            .await
            .is_err());
    }
    let server = Server::start(canonical_map(&fixture), valid).await;
    assert_eq!(
        server
            .client
            .chunk_map_page_canonical(SID, PATH, &fixture.content, &fixture.verified(), 0)
            .await
            .unwrap()
            .chunk_sha256,
        fixture.leaf.chunk_sha256
    );
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
