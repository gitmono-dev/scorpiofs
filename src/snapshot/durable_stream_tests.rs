//! Online streaming CAS transactions through canonical HTTP metadata/chunks.
//! Local completion/reopen prove integrity; no offline authorization is granted.

use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    body::Bytes,
    extract::{Path as HttpPath, Query, State},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use mst2_codec::{
    chunkmap::{
        leaf_proof, merkle_root, ChunkLeaf, ChunkMap, ProofSide, CHUNKS_PER_PAGE, CHUNK_SIZE,
    },
    descriptor::ServingDescriptor,
    metapage::{page_id, Entry, EntryKind, Page},
    treeframe::{ChunkPayload, EndPayload, MetaPayload, ObjectPayload},
};
use serde_json::{json, Value};
use tokio::sync::Notify;

use super::{durability_tests::FaultGuard, *};

// This batch fixture exercises the coordinator's fixed final allocation while
// the existing single-chunk fixture below keeps testing streaming CAS writes.
async fn owned_chunks(
    State(fixture): State<Arc<Fixture>>,
    HttpPath(snapshot): HttpPath<String>,
    body: Bytes,
) -> Response {
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    fixture.chunks.fetch_add(1, Ordering::SeqCst);
    let mut wire = Vec::new();
    let mut logical = 0;
    for (sequence, item) in items.iter().rev().enumerate() {
        let index: u64 = item["chunk_index"].as_str().unwrap().parse().unwrap();
        assert_eq!(item["path"], "/large.bin");
        assert_eq!(item["map_id"], id(&fixture.map.map_id()));
        assert_eq!(item["expected_digest"], id(&fixture.advertised));
        let length = fixture.map.chunk_len(index).unwrap();
        let mut bytes = vec![pattern(index); length as usize];
        if fixture.corrupt.load(Ordering::SeqCst) == index {
            bytes[0] ^= 1;
        }
        logical += length;
        wire.extend(
            ChunkPayload {
                map_id: fixture.map.map_id(),
                file_content_id: fixture.advertised,
                chunk_index: index,
                chunk_bytes: bytes,
            }
            .encode(31, sequence as u64)
            .unwrap(),
        );
    }
    wire.extend(
        EndPayload {
            request_item_count: items.len() as u32,
            unique_unit_count: items.len() as u32,
            logical_bytes: logical,
            request_body_sha256: hash(&body),
        }
        .encode(31, items.len() as u64),
    );
    response(&snapshot, &body, wire)
}

async fn open_owned(
    fixture: Fixture,
) -> (
    Server,
    Arc<Fixture>,
    Arc<crate::snapshot::FetchCoordinator>,
    SnapshotFile,
) {
    let fixture = Arc::new(fixture);
    let app = Router::new()
        .route("/api/v2/snapshots/capabilities", get(capabilities))
        .route("/api/v2/snapshots/resolve", post(resolve))
        .route("/api/v2/snapshots/{snapshot}/chunk-map", get(map))
        .route("/api/v2/snapshots/{snapshot}/chunk-map/pages", get(leaf))
        .route("/api/v2/snapshots/{snapshot}/chunks", post(owned_chunks))
        .with_state(fixture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = Server(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    }));
    let reader = SnapshotReader::resolve(crate::snapshot::Mst2Client::new(base), "/project", 600)
        .await
        .unwrap();
    let closure = ValidatedSnapshotClosure::from_pages(
        reader.descriptor(),
        std::collections::BTreeMap::from([(
            id(&fixture.descriptor.metadata_root),
            fixture.page.clone(),
        )]),
    )
    .unwrap();
    let file = closure.files()[0].clone();
    let coordinator = crate::snapshot::FetchCoordinator::with_verified_closure_and_budgets(
        reader,
        &closure,
        1,
        crate::snapshot::FetchCoordinatorLimits::default(),
        crate::snapshot::ContentBudgetLimits::new(16 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    (server, fixture, coordinator, file)
}

#[tokio::test]
async fn accounted_multi_chunk_reversed_response_uses_final_owner_and_verifies_whole_digest() {
    let size = 2 * CHUNK_SIZE as u64 + 17;
    let (_server, fixture, coordinator, file) = open_owned(Fixture::new(size, false)).await;
    let owner = coordinator.fetch_owned(file, true).await.unwrap();
    assert_eq!(owner.len() as u64, size);
    for index in 0..3 {
        let start = index * CHUNK_SIZE as usize;
        let end = owner.len().min(start + CHUNK_SIZE as usize);
        assert!(owner.as_bytes()[start..end]
            .iter()
            .all(|byte| *byte == pattern(index as u64)));
    }
    assert_eq!(hash(owner.as_bytes()), fixture.whole);
    assert_eq!(
        fixture.chunks.load(Ordering::SeqCst),
        1,
        "all three chunks kept their existing batch request"
    );
    assert_eq!(coordinator.counts().pending_jobs, 0);
    let charged = coordinator.content_usage().output_bytes;
    assert!(charged >= size as usize && charged < size as usize + 1024);
    drop(owner);
    assert_eq!(coordinator.content_usage().output_bytes, 0);
    tokio::time::timeout(Duration::from_secs(2), async {
        while coordinator.content_usage().construction_bytes != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn accounted_chunks_with_bad_leaf_or_wrong_whole_identity_publish_no_owner() {
    let size = 2 * CHUNK_SIZE as u64 + 17;
    for wrong_whole in [false, true] {
        let (_server, fixture, coordinator, file) =
            open_owned(Fixture::new(size, wrong_whole)).await;
        if !wrong_whole {
            fixture.corrupt.store(1, Ordering::SeqCst);
        }
        assert_eq!(
            coordinator.fetch_owned(file, true).await.unwrap_err().code,
            SnapshotErrorCode::DigestMismatch
        );
        assert_eq!(coordinator.content_usage().output_bytes, 0);
        tokio::time::timeout(Duration::from_secs(2), async {
            while coordinator.content_usage().construction_bytes != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn accounted_fixed_view_mismatch_rejects_large_map_before_chunk_collection() {
    let mut fixture = Fixture::new(2 * CHUNK_SIZE as u64 + 17, false);
    fixture.map = ChunkMap::new(
        fixture.advertised,
        128 * 1024 * 1024,
        fixture.map.pages_root,
    )
    .unwrap();
    let (_server, fixture, coordinator, file) = open_owned(fixture).await;
    assert_eq!(
        coordinator.fetch_owned(file, true).await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(fixture.maps.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.chunks.load(Ordering::SeqCst), 0);
    assert_eq!(coordinator.content_usage().output_bytes, 0);
}

const NONE: u64 = u64::MAX;
const INSTANCE: &str = "11111111-2222-4333-8444-555555555559";
const SMALL: &[u8] = b"small content fetched in an OBJECT request";

fn hash(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

fn id(value: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(value))
}

fn pattern(index: u64) -> u8 {
    (index % 7 + 1) as u8
}

fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for part in bytes.chunks(3) {
        let bits = ((part[0] as u32) << 16)
            | ((part.get(1).copied().unwrap_or(0) as u32) << 8)
            | part.get(2).copied().unwrap_or(0) as u32;
        out.push(TABLE[(bits >> 18) as usize] as char);
        out.push(TABLE[((bits >> 12) & 63) as usize] as char);
        out.push(if part.len() > 1 {
            TABLE[((bits >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if part.len() > 2 {
            TABLE[(bits & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

struct Fixture {
    descriptor: ServingDescriptor,
    page: Vec<u8>,
    map: ChunkMap,
    leaves: Vec<ChunkLeaf>,
    leaf_hashes: Vec<[u8; 32]>,
    whole: [u8; 32],
    advertised: [u8; 32],
    chunk_reads: bool,
    chunks: AtomicUsize,
    maps: AtomicUsize,
    raw: AtomicUsize,
    objects: AtomicUsize,
    mixed: bool,
    corrupt: AtomicU64,
    block: AtomicU64,
    blocked: Notify,
    release: Notify,
}

impl Fixture {
    fn new(size: u64, wrong_whole: bool) -> Self {
        let mut whole = Context::new(&SHA256);
        let mut chunk_hashes = Vec::new();
        for index in 0..size.div_ceil(CHUNK_SIZE as u64) {
            let length = (size - index * CHUNK_SIZE as u64).min(CHUNK_SIZE as u64);
            let bytes = vec![pattern(index); length as usize];
            whole.update(&bytes);
            chunk_hashes.push(hash(&bytes));
        }
        let whole: [u8; 32] = whole.finish().as_ref().try_into().unwrap();
        let mut advertised = whole;
        if wrong_whole {
            advertised[0] ^= 1;
        }
        let leaves: Vec<_> = chunk_hashes
            .chunks(CHUNKS_PER_PAGE)
            .enumerate()
            .map(|(page, hashes)| ChunkLeaf {
                page_index: page as u64,
                chunk_sha256: hashes.to_vec(),
            })
            .collect();
        let leaf_hashes: Vec<_> = leaves
            .iter()
            .map(|leaf| leaf.leaf_hash().unwrap())
            .collect();
        let map = ChunkMap::new(advertised, size, merkle_root(&leaf_hashes).unwrap()).unwrap();
        let page = Page::build(&[Entry::file(
            EntryKind::Regular,
            b"large.bin",
            size,
            advertised,
        )])
        .unwrap();
        let descriptor = ServingDescriptor {
            instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
            namespace_view_id: [0x42; 32],
            scope: "/project".into(),
            metadata_root: page_id(&page),
        };
        Self {
            descriptor,
            page,
            map,
            leaves,
            leaf_hashes,
            whole,
            advertised,
            chunk_reads: true,
            chunks: AtomicUsize::new(0),
            maps: AtomicUsize::new(0),
            raw: AtomicUsize::new(0),
            objects: AtomicUsize::new(0),
            mixed: false,
            corrupt: AtomicU64::new(NONE),
            block: AtomicU64::new(NONE),
            blocked: Notify::new(),
            release: Notify::new(),
        }
    }

    fn with_alias_and_small(mut self) -> Self {
        self.page = Page::build(&[
            Entry::file(
                EntryKind::Executable,
                b"large-alias.bin",
                self.map.file_size,
                self.advertised,
            ),
            Entry::file(
                EntryKind::Regular,
                b"large.bin",
                self.map.file_size,
                self.advertised,
            ),
            Entry::file(
                EntryKind::Regular,
                b"small.txt",
                SMALL.len() as u64,
                hash(SMALL),
            ),
        ])
        .unwrap();
        self.descriptor.metadata_root = page_id(&self.page);
        self.mixed = true;
        self
    }
}

async fn capabilities(State(fixture): State<Arc<Fixture>>) -> Json<Value> {
    Json(
        json!({"protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true,
                     "metadata_pages": true, "objects": true, "chunk_reads": fixture.chunk_reads, "raw_blob": true}}),
    )
}

async fn resolve(State(fixture): State<Arc<Fixture>>) -> Json<Value> {
    let d = &fixture.descriptor;
    Json(json!({
        "descriptor": {"schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE,
            "namespace_view_id": id(&d.namespace_view_id), "scope": "/project", "materialization_policy": 1,
            "fs_semantics": 1, "access_projection": 0, "metadata_root": id(&d.metadata_root),
            "snapshot_id": id(&d.snapshot_id().unwrap())},
        "lease_id": "stream-lease", "lease_expires_at": "2099-01-01T00:00:00Z",
        "publication_sequence": "1", "authorization_epoch": "1"
    }))
}

fn response(snapshot: &str, body: &[u8], wire: Vec<u8>) -> Response {
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", snapshot)
        .header("x-mega-request-digest", id(&hash(body)))
        .body(axum::body::Body::from(wire))
        .unwrap()
}

async fn metadata(
    State(fixture): State<Arc<Fixture>>,
    HttpPath(snapshot): HttpPath<String>,
    body: Bytes,
) -> Response {
    let request: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(request["items"].as_array().unwrap().len(), 1);
    assert_eq!(request["items"][0]["directory_path"], "/");
    assert_eq!(request["items"][0]["route"], json!([]));
    assert_eq!(
        request["items"][0]["expected_digest"],
        id(&fixture.descriptor.metadata_root)
    );
    let mut wire = MetaPayload {
        pages: vec![(fixture.descriptor.metadata_root, fixture.page.clone())],
    }
    .encode(21, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: fixture.page.len() as u64,
            request_body_sha256: hash(&body),
        }
        .encode(21, 1),
    );
    response(&snapshot, &body, wire)
}

async fn map(
    State(fixture): State<Arc<Fixture>>,
    HttpPath(snapshot): HttpPath<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    fixture.maps.fetch_add(1, Ordering::SeqCst);
    let m = &fixture.map;
    Json(
        json!({"snapshot_id": snapshot, "path": query["path"], "schema_version": 2,
        "file_content_id": id(&fixture.advertised), "map_id": id(&m.map_id()),
        "file_size": m.file_size.to_string(), "chunk_size": CHUNK_SIZE, "chunk_count": m.chunk_count.to_string(),
        "page_count": m.page_count.to_string(), "pages_root": id(&m.pages_root)}),
    )
}

async fn leaf(
    State(fixture): State<Arc<Fixture>>,
    HttpPath(snapshot): HttpPath<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    let page: u64 = query["page"].parse().unwrap();
    let proof: Vec<_> = leaf_proof(&fixture.leaf_hashes, page)
        .unwrap()
        .into_iter()
        .map(|step| {
            json!({
        "side": match step.side { ProofSide::Left => "left", ProofSide::Right => "right" },
        "sibling_pages": step.sibling_pages.to_string(), "digest": id(&step.digest)})
        })
        .collect();
    Json(
        json!({"snapshot_id": snapshot, "path": query["path"], "map_id": id(&fixture.map.map_id()),
        "page_count": fixture.map.page_count.to_string(),
        "leaf": {"page_index": page.to_string(), "count": fixture.leaves[page as usize].chunk_sha256.len().to_string(),
        "data_base64": base64(&fixture.leaves[page as usize].encode().unwrap())}, "proof": proof}),
    )
}

async fn chunks(
    State(fixture): State<Arc<Fixture>>,
    HttpPath(snapshot): HttpPath<String>,
    body: Bytes,
) -> Response {
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let index: u64 = items[0]["chunk_index"].as_str().unwrap().parse().unwrap();
    assert!(
        items[0]["path"] == "large.bin" || (fixture.mixed && items[0]["path"] == "large-alias.bin")
    );
    assert_eq!(items[0]["map_id"], id(&fixture.map.map_id()));
    assert_eq!(items[0]["expected_digest"], id(&fixture.advertised));
    fixture.chunks.fetch_add(1, Ordering::SeqCst);
    if fixture.block.load(Ordering::SeqCst) == index {
        fixture.blocked.notify_one();
        fixture.release.notified().await;
    }
    let length = fixture.map.chunk_len(index).unwrap();
    let mut bytes = vec![pattern(index); length as usize];
    if fixture.corrupt.load(Ordering::SeqCst) == index {
        bytes[0] ^= 1;
    }
    let mut wire = ChunkPayload {
        map_id: fixture.map.map_id(),
        file_content_id: fixture.advertised,
        chunk_index: index,
        chunk_bytes: bytes,
    }
    .encode(21, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: length,
            request_body_sha256: hash(&body),
        }
        .encode(21, 1),
    );
    response(&snapshot, &body, wire)
}

async fn objects(
    State(fixture): State<Arc<Fixture>>,
    HttpPath(snapshot): HttpPath<String>,
    body: Bytes,
) -> Response {
    assert!(fixture.mixed);
    let request: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(request["items"].as_array().unwrap().len(), 1);
    assert_eq!(request["items"][0]["path"], "/small.txt");
    assert_eq!(request["items"][0]["expected_digest"], id(&hash(SMALL)));
    fixture.objects.fetch_add(1, Ordering::SeqCst);
    let mut wire = ObjectPayload {
        objects: vec![(hash(SMALL), SMALL.to_vec())],
    }
    .encode(21, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: SMALL.len() as u64,
            request_body_sha256: hash(&body),
        }
        .encode(21, 1),
    );
    response(&snapshot, &body, wire)
}

async fn raw(State(fixture): State<Arc<Fixture>>) -> Response {
    fixture.raw.fetch_add(1, Ordering::SeqCst);
    if fixture.chunk_reads {
        return Response::builder()
            .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::empty())
            .unwrap();
    }
    let mut bytes = Vec::new();
    for index in 0..fixture.map.chunk_count {
        bytes.extend(vec![
            pattern(index);
            fixture.map.chunk_len(index).unwrap() as usize
        ]);
    }
    Response::builder()
        .header("content-type", "application/octet-stream")
        .body(axum::body::Body::from(bytes))
        .unwrap()
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn open(
    size: u64,
    wrong_whole: bool,
    temp: &Path,
) -> (Server, Arc<Fixture>, SnapshotReader, DurableStore) {
    open_fixture(Fixture::new(size, wrong_whole), temp).await
}

async fn open_fixture(
    fixture: Fixture,
    temp: &Path,
) -> (Server, Arc<Fixture>, SnapshotReader, DurableStore) {
    let fixture = Arc::new(fixture);
    let app = Router::new()
        .route("/api/v2/snapshots/capabilities", get(capabilities))
        .route("/api/v2/snapshots/resolve", post(resolve))
        .route(
            "/api/v2/snapshots/{snapshot}/metadata/pages",
            post(metadata),
        )
        .route("/api/v2/snapshots/{snapshot}/chunk-map", get(map))
        .route("/api/v2/snapshots/{snapshot}/chunk-map/pages", get(leaf))
        .route("/api/v2/snapshots/{snapshot}/chunks", post(chunks))
        .route("/api/v2/snapshots/{snapshot}/objects", post(objects))
        .route("/api/v2/snapshots/{snapshot}/blob", get(raw))
        .with_state(fixture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = Server(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    }));
    let reader = SnapshotReader::resolve(crate::snapshot::Mst2Client::new(base), "/project", 600)
        .await
        .unwrap();
    let store =
        DurableStore::open_for_reader(temp.join("view"), temp.join("cas"), &reader).unwrap();
    (server, fixture, reader, store)
}

fn unpublished(store: &DurableStore, digest: &[u8; 32]) {
    assert!(!store.root().join(COMPLETE_MARKER).exists());
    assert!(!store.blob_path(&id(digest)).unwrap().exists());
    assert!(read_optional(&store.root().join(JOURNAL_FILE))
        .unwrap()
        .is_none_or(|bytes| bytes.is_empty()));
    assert!(!fs::read_dir(store.content_dir()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp.")
    }));
}

async fn hydrate_parallel(
    store: &DurableStore,
    reader: &SnapshotReader,
    closure: &ValidatedSnapshotClosure,
    core: &str,
) -> Result<HydrateReport, SnapshotError> {
    let source = reader.clone();
    let fetch = move |file: SnapshotFile| {
        let source = source.clone();
        Box::pin(async move {
            let bytes = if source.capabilities().features.chunk_reads {
                source
                    .read_file_frames(&file.rel_path, &file.content_digest, file.size)
                    .await?
            } else {
                source
                    .read_file(&file.rel_path, &file.content_digest)
                    .await?
            };
            Ok(Arc::new(bytes))
        }) as futures::future::BoxFuture<'static, Result<Arc<Vec<u8>>, SnapshotError>>
    };
    match core {
        "concurrent" => {
            store
                .hydrate_snapshot_concurrent(reader, closure, 2, fetch)
                .await
        }
        "batch" => {
            let source = reader.clone();
            store
                .hydrate_snapshot_batches(
                    reader,
                    closure,
                    2,
                    2,
                    move |files| {
                        let source = source.clone();
                        Box::pin(async move {
                            let items: Vec<_> = files
                                .iter()
                                .map(|file| {
                                    (format!("/{}", file.rel_path), file.content_digest.clone())
                                })
                                .collect();
                            let objects = source
                                .client()
                                .objects(source.snapshot_id(), &items, source.encoding_hint())
                                .await?;
                            files
                                .into_iter()
                                .map(|file| {
                                    let digest = crate::snapshot::frames::parse_digest(
                                        &file.content_digest,
                                    )?;
                                    Ok((
                                        file.content_digest,
                                        Arc::new(objects.get(&digest).unwrap().clone()),
                                    ))
                                })
                                .collect()
                        })
                    },
                    fetch,
                )
                .await
        }
        _ => panic!("unknown parallel hydration core"),
    }
}

async fn previous_complete(store: &DurableStore, temp: &Path) -> (DurableStore, Vec<u8>, String) {
    let bytes = b"content of a different already committed snapshot";
    let page = Page::build(&[Entry::file(
        EntryKind::Regular,
        b"previous.txt",
        bytes.len() as u64,
        hash(bytes),
    )])
    .unwrap();
    let descriptor = ServingDescriptor {
        instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
        namespace_view_id: [0x42; 32],
        scope: "/project".into(),
        metadata_root: page_id(&page),
    }
    .encode()
    .unwrap();
    let closure = ValidatedSnapshotClosure::from_canonical_pages(
        &descriptor,
        BTreeMap::from([(id(&page_id(&page)), page)]),
    )
    .unwrap();
    let view = ViewMeta {
        snapshot_id: closure.snapshot_id().into(),
        namespace_view_id: closure.descriptor().namespace_view_id.clone(),
        scope: closure.descriptor().scope.clone(),
        lease_id: "previous-local-integrity".into(),
    };
    let previous =
        DurableStore::open_with_content(temp.join("previous-view"), store.content_dir()).unwrap();
    previous
        .hydrate_snapshot_with(&view, &closure, |_| std::future::ready(Ok(bytes.to_vec())))
        .await
        .unwrap();
    let marker = fs::read(previous.root().join(COMPLETE_MARKER)).unwrap();
    (previous, marker, id(&hash(bytes)))
}

fn previous_unchanged(previous: &DurableStore, marker: &[u8], digest: &str) {
    assert_eq!(
        fs::read(previous.root().join(COMPLETE_MARKER)).unwrap(),
        marker
    );
    assert!(previous.is_snapshot_complete().unwrap());
    assert!(previous
        .verify_blob(
            digest,
            b"content of a different already committed snapshot".len() as u64
        )
        .unwrap());
}

#[tokio::test]
async fn parallel_legacy_without_chunk_reads_preserves_buffered_raw_fallback() {
    for core in ["batch", "concurrent"] {
        let temp = tempfile::tempdir().unwrap();
        let size = 300 * 1024;
        let mut fixture = Fixture::new(size, false);
        fixture.chunk_reads = false;
        let (_server, fixture, reader, store) = open_fixture(fixture, temp.path()).await;
        let closure = reader.snapshot_closure().await.unwrap();
        let report = hydrate_parallel(&store, &reader, &closure, core)
            .await
            .unwrap();
        assert_eq!(
            (report.fetched, report.total_files, report.bytes_total),
            (1, 1, size)
        );
        assert_eq!(fixture.raw.load(Ordering::SeqCst), 1, "{core}");
        assert_eq!(fixture.maps.load(Ordering::SeqCst), 0, "{core}");
        assert_eq!(fixture.chunks.load(Ordering::SeqCst), 0, "{core}");
        assert!(store.is_snapshot_complete().unwrap(), "{core}");
    }
}

#[tokio::test]
async fn parallel_verified_chunks_with_wrong_whole_identity_never_commit() {
    for core in ["batch", "concurrent"] {
        let temp = tempfile::tempdir().unwrap();
        let (_server, fixture, reader, store) = open(3 * 1024 * 1024 + 7, true, temp.path()).await;
        let closure = reader.snapshot_closure().await.unwrap();
        assert_eq!(
            hydrate_parallel(&store, &reader, &closure, core)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch,
            "{core}"
        );
        assert_eq!(
            fixture.chunks.load(Ordering::SeqCst) as u64,
            fixture.map.chunk_count
        );
        unpublished(&store, &fixture.advertised);
        assert!(
            !store.blob_path(&id(&fixture.whole)).unwrap().exists(),
            "{core}"
        );
    }
}

#[tokio::test]
async fn parallel_streams_mix_small_objects_and_large_aliases_with_old_complete_isolated() {
    for core in ["batch", "concurrent"] {
        let temp = tempfile::tempdir().unwrap();
        let size = 65 * 1024 * 1024 + 7;
        let fixture = Fixture::new(size, false).with_alias_and_small();
        let (_server, fixture, reader, store) = open_fixture(fixture, temp.path()).await;
        let (previous, marker, old_digest) = previous_complete(&store, temp.path()).await;
        let closure = reader.snapshot_closure().await.unwrap();
        let report = hydrate_parallel(&store, &reader, &closure, core)
            .await
            .unwrap();
        assert_eq!(
            (
                report.fetched,
                report.resumed,
                report.total_files,
                report.bytes_total
            ),
            (2, 0, 3, 2 * size + SMALL.len() as u64),
            "{core}"
        );
        assert_eq!(fixture.maps.load(Ordering::SeqCst), 1, "{core}");
        assert_eq!(
            fixture.chunks.load(Ordering::SeqCst) as u64,
            fixture.map.chunk_count,
            "{core}"
        );
        assert_eq!(fixture.objects.load(Ordering::SeqCst), 1, "{core}");
        assert_eq!(fixture.raw.load(Ordering::SeqCst), 0, "{core}");
        assert!(store.is_snapshot_complete().unwrap(), "{core}");
        assert_eq!(store.snapshot_manifest().unwrap().files(), closure.files());
        assert_eq!(store.read_journal().unwrap().len(), 3, "{core}");
        previous_unchanged(&previous, &marker, &old_digest);
        let reopened = DurableStore::open_with_content(store.root(), store.content_dir()).unwrap();
        assert!(reopened.is_snapshot_complete().unwrap(), "{core}");
        assert_eq!(
            reopened
                .pread_blob(&id(&fixture.whole), size - 4, 20)
                .unwrap()
                .unwrap(),
            vec![pattern(65); 4]
        );
        let warm = hydrate_parallel(&reopened, &reader, &closure, core)
            .await
            .unwrap();
        assert_eq!((warm.fetched, warm.resumed), (0, 3), "{core}");
        assert_eq!(fixture.maps.load(Ordering::SeqCst), 1, "{core}");
        assert_eq!(
            fixture.chunks.load(Ordering::SeqCst) as u64,
            fixture.map.chunk_count,
            "{core}"
        );
        assert_eq!(fixture.objects.load(Ordering::SeqCst), 1, "{core}");
    }
}

#[tokio::test]
async fn parallel_late_corruption_never_commits_and_can_retry_without_touching_old_complete() {
    for core in ["batch", "concurrent"] {
        let temp = tempfile::tempdir().unwrap();
        let (_server, fixture, reader, store) = open(6 * 1024 * 1024 + 7, false, temp.path()).await;
        let (previous, marker, old_digest) = previous_complete(&store, temp.path()).await;
        let closure = reader.snapshot_closure().await.unwrap();
        fixture.corrupt.store(5, Ordering::SeqCst);
        assert_eq!(
            hydrate_parallel(&store, &reader, &closure, core)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch,
            "{core}"
        );
        assert_eq!(fixture.chunks.load(Ordering::SeqCst), 6, "{core}");
        unpublished(&store, &fixture.advertised);
        previous_unchanged(&previous, &marker, &old_digest);
        fixture.corrupt.store(NONE, Ordering::SeqCst);
        hydrate_parallel(&store, &reader, &closure, core)
            .await
            .unwrap();
        assert!(store.is_snapshot_complete().unwrap(), "{core}");
    }
}

#[tokio::test]
async fn parallel_stream_cancellation_removes_temp_releases_transaction_and_can_retry() {
    for core in ["batch", "concurrent"] {
        let temp = tempfile::tempdir().unwrap();
        let (_server, fixture, reader, store) = open(6 * 1024 * 1024 + 7, false, temp.path()).await;
        let closure = reader.snapshot_closure().await.unwrap();
        fixture.block.store(3, Ordering::SeqCst);
        let mut hydrate = Box::pin(hydrate_parallel(&store, &reader, &closure, core));
        tokio::select! {
            result = &mut hydrate => panic!("{core} unexpectedly completed: {result:?}"),
            _ = fixture.blocked.notified() => {}
            _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => panic!("{core} never reached blocked chunk"),
        }
        assert_eq!(fixture.chunks.load(Ordering::SeqCst), 4, "{core}");
        drop(hydrate);
        unpublished(&store, &fixture.advertised);
        assert!(store.try_transaction().unwrap().is_some(), "{core}");
        fixture.block.store(NONE, Ordering::SeqCst);
        fixture.release.notify_one();
        hydrate_parallel(&store, &reader, &closure, core)
            .await
            .unwrap();
        assert!(store.is_snapshot_complete().unwrap(), "{core}");
    }
}

#[tokio::test]
async fn parallel_stream_sync_failures_never_complete_and_can_retry() {
    for core in ["batch", "concurrent"] {
        for phase in ["object-file-sync", "directory-sync"] {
            let temp = tempfile::tempdir().unwrap();
            let (_server, fixture, reader, store) =
                open(3 * 1024 * 1024 + 7, false, temp.path()).await;
            let closure = reader.snapshot_closure().await.unwrap();
            let fault = FaultGuard::install(store.content_dir(), phase, false);
            let error = hydrate_parallel(&store, &reader, &closure, core)
                .await
                .unwrap_err();
            drop(fault);
            assert_eq!(error.code, SnapshotErrorCode::Internal, "{core} {phase}");
            assert!(
                error.message.contains("injected durability I/O failure"),
                "{error:?}"
            );
            assert_eq!(
                fixture.chunks.load(Ordering::SeqCst) as u64,
                fixture.map.chunk_count
            );
            assert!(
                !store.root().join(COMPLETE_MARKER).exists(),
                "{core} {phase}"
            );
            assert!(store.read_journal().unwrap().is_empty(), "{core} {phase}");
            assert!(!fs::read_dir(store.content_dir()).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".tmp.")
            }));
            hydrate_parallel(&store, &reader, &closure, core)
                .await
                .unwrap();
            assert!(store.is_snapshot_complete().unwrap(), "{core} {phase}");
        }
    }
}

#[tokio::test]
async fn legacy_server_without_chunk_reads_hydrates_raw_blob() {
    let temp = tempfile::tempdir().unwrap();
    let size = 300 * 1024;
    let mut fixture = Fixture::new(size, false);
    fixture.chunk_reads = false;
    let (_server, fixture, reader, store) = open_fixture(fixture, temp.path()).await;
    assert!(size > crate::snapshot::OBJECT_CAP);
    assert!(!reader.capabilities().features.chunk_reads);
    let report = store.hydrate_snapshot(&reader).await.unwrap();
    assert_eq!(
        (report.fetched, report.total_files, report.bytes_total),
        (1, 1, size)
    );
    assert_eq!(fixture.raw.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.maps.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.chunks.load(Ordering::SeqCst), 0);
    assert!(store.is_snapshot_complete().unwrap());
    assert_eq!(
        store.read_blob(&id(&fixture.whole), size).unwrap(),
        vec![pattern(0); size as usize]
    );
}

#[tokio::test]
async fn range_profile_rejects_above_eight_tib_before_request_and_accepts_boundary_map() {
    let temp = tempfile::tempdir().unwrap();
    let size = 8 * 1024 * 1024 * 1024 * 1024;
    // Construct only the canonical map: opening a range handle must not load
    // millions of chunk digests, request leaf pages, or hydrate the file.
    let mut fixture = Fixture::new(1, false);
    fixture.map = ChunkMap::new(fixture.advertised, size, [0xaa; 32]).unwrap();
    fixture.leaves.clear();
    fixture.leaf_hashes.clear();
    fixture.page = Page::build(&[Entry::file(
        EntryKind::Regular,
        b"large.bin",
        size,
        fixture.advertised,
    )])
    .unwrap();
    fixture.descriptor.metadata_root = page_id(&fixture.page);
    let (_server, fixture, reader, _store) = open_fixture(fixture, temp.path()).await;
    let digest = id(&fixture.advertised);
    match crate::snapshot::ChunkedFile::open(&reader, "large.bin", &digest, size + 1).await {
        Ok(_) => panic!("over-profile range handle was accepted"),
        Err(error) => assert_eq!(error.code, SnapshotErrorCode::LimitExceeded),
    }
    assert_eq!(fixture.maps.load(Ordering::SeqCst), 0);
    let handle = crate::snapshot::ChunkedFile::open(&reader, "large.bin", &digest, size)
        .await
        .unwrap();
    assert_eq!(handle.size, size);
    assert_eq!(handle.map_id(), id(&fixture.map.map_id()));
    assert_eq!(fixture.maps.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.chunks.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.raw.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn large_online_stream_is_complete_reopens_and_resumes_without_fetching() {
    let temp = tempfile::tempdir().unwrap();
    let size = 65 * 1024 * 1024 + 7;
    let (_server, fixture, reader, store) = open(size, false, temp.path()).await;
    assert!(size > crate::snapshot::client::MAX_BUFFERED_FILE_BYTES);
    let report = store.hydrate_snapshot(&reader).await.unwrap();
    assert_eq!(
        (
            report.fetched,
            report.resumed,
            report.total_files,
            report.bytes_total
        ),
        (1, 0, 1, size)
    );
    assert_eq!(
        fixture.chunks.load(Ordering::SeqCst) as u64,
        fixture.map.chunk_count
    );
    assert_eq!(fixture.raw.load(Ordering::SeqCst), 0);
    assert!(store.is_snapshot_complete().unwrap());
    assert_eq!(fixture.whole, fixture.advertised);
    let reopened = DurableStore::open_with_content(store.root(), store.content_dir()).unwrap();
    assert!(reopened.is_snapshot_complete().unwrap());
    let offset = CHUNK_SIZE as u64 - 3;
    let bytes = reopened
        .pread_blob(&id(&fixture.whole), offset, 8)
        .unwrap()
        .unwrap();
    assert_eq!(
        bytes,
        vec![
            pattern(0),
            pattern(0),
            pattern(0),
            pattern(1),
            pattern(1),
            pattern(1),
            pattern(1),
            pattern(1)
        ]
    );
    assert_eq!(
        reopened
            .pread_blob(&id(&fixture.whole), size - 4, 20)
            .unwrap()
            .unwrap(),
        vec![pattern(65); 4]
    );
    let fetched = fixture.chunks.load(Ordering::SeqCst);
    let maps = fixture.maps.load(Ordering::SeqCst);
    let resumed = reopened.hydrate_snapshot(&reader).await.unwrap();
    assert_eq!((resumed.fetched, resumed.resumed), (0, 1));
    assert_eq!(fixture.chunks.load(Ordering::SeqCst), fetched);
    assert_eq!(fixture.maps.load(Ordering::SeqCst), maps);
    assert!(reopened.is_snapshot_complete().unwrap());
}

#[tokio::test]
async fn verified_chunks_without_whole_file_identity_never_publish_cas_or_journal() {
    let temp = tempfile::tempdir().unwrap();
    let (_server, fixture, reader, store) = open(3 * 1024 * 1024 + 7, true, temp.path()).await;
    assert_eq!(
        store.hydrate_snapshot(&reader).await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(
        fixture.chunks.load(Ordering::SeqCst) as u64,
        fixture.map.chunk_count
    );
    assert_eq!(fixture.raw.load(Ordering::SeqCst), 0);
    unpublished(&store, &fixture.advertised);
    assert!(!store.blob_path(&id(&fixture.whole)).unwrap().exists());
}

#[tokio::test]
async fn late_corrupt_chunk_preserves_existing_cas_and_never_commits_new_view() {
    let temp = tempfile::tempdir().unwrap();
    let (_server, fixture, reader, store) = open(6 * 1024 * 1024 + 7, false, temp.path()).await;
    let old = b"verified bytes from an existing unrelated complete file";
    let old_id = digest_of(old);
    write_atomic(store.content_dir(), &blob_name(&old_id), old).unwrap();
    fixture.corrupt.store(5, Ordering::SeqCst);
    assert_eq!(
        store.hydrate_snapshot(&reader).await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(fixture.chunks.load(Ordering::SeqCst), 6);
    unpublished(&store, &fixture.advertised);
    assert_eq!(store.read_blob(&old_id, old.len() as u64).unwrap(), old);
}

#[tokio::test]
async fn cancellation_removes_unpublished_temp_and_releases_transaction() {
    let temp = tempfile::tempdir().unwrap();
    let (_server, fixture, reader, store) = open(6 * 1024 * 1024 + 7, false, temp.path()).await;
    fixture.block.store(3, Ordering::SeqCst);
    let mut hydrate = Box::pin(store.hydrate_snapshot(&reader));
    tokio::select! {
        result = &mut hydrate => panic!("hydration unexpectedly completed: {result:?}"),
        _ = fixture.blocked.notified() => {}
        _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => panic!("stream never reached late blocked chunk"),
    }
    assert_eq!(fixture.chunks.load(Ordering::SeqCst), 4);
    drop(hydrate);
    unpublished(&store, &fixture.advertised);
    assert!(store.try_transaction().unwrap().is_some());
    fixture.block.store(NONE, Ordering::SeqCst);
    fixture.release.notify_one();
    let report = store.hydrate_snapshot(&reader).await.unwrap();
    assert_eq!(report.fetched, 1);
    assert!(store.is_snapshot_complete().unwrap());
}

#[tokio::test]
async fn streaming_content_sync_failures_do_not_journal_or_complete_the_file() {
    for phase in ["object-file-sync", "directory-sync"] {
        let temp = tempfile::tempdir().unwrap();
        let (_server, fixture, reader, store) = open(3 * 1024 * 1024 + 7, false, temp.path()).await;
        let fault = FaultGuard::install(store.content_dir(), phase, false);
        let error = store.hydrate_snapshot(&reader).await.unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::Internal, "{phase}");
        assert!(
            error.message.contains("injected durability I/O failure"),
            "{error:?}"
        );
        assert_eq!(
            fixture.chunks.load(Ordering::SeqCst) as u64,
            fixture.map.chunk_count
        );
        drop(fault);
        assert!(!store.root().join(COMPLETE_MARKER).exists(), "{phase}");
        assert!(read_optional(&store.root().join(JOURNAL_FILE))
            .unwrap()
            .is_none_or(|bytes| bytes.is_empty()));
        assert!(!fs::read_dir(store.content_dir()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp.")
        }));
        if phase == "object-file-sync" {
            assert!(!store.blob_path(&id(&fixture.advertised)).unwrap().exists());
        } else {
            assert!(store.blob_path(&id(&fixture.advertised)).unwrap().exists());
        }
        // A synced file may already be at its CAS name after directory fsync
        // fails. Retry re-verifies it; its presence grants no completion claim.
        store.hydrate_snapshot(&reader).await.unwrap();
        assert!(store.is_snapshot_complete().unwrap());
    }
}
