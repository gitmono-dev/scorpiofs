//! Fixed HTTP fixtures with hand-written expected logical paths. The oracle
//! never uses either production manifest walker to construct its answers.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsStr,
    net::TcpListener as StdTcpListener,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use asyncfuse::raw::prelude::{Filesystem, Request};
use axum::{
    body::{Body, Bytes},
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures::StreamExt;
use libfuse_fs::unionfs::layer::Layer;
use mst2_codec::{
    chunkmap::{ChunkLeaf, ChunkMap, CHUNK_SIZE},
    descriptor::ServingDescriptor,
    metapage::{page_id, BranchChild, Entry, EntryKind, Page},
    treeframe::{ChunkPayload, EndPayload, MetaPayload, ObjectPayload},
};
use scorpiofs::snapshot::{
    capabilities::CapabilityAdvertisement, durable::digest_of, frames::parse_digest,
    fuse::Mst2Fuse, ContentBudgetLimits, DurableStore, FetchCoordinator, IncrementalSync,
    MetadataProofLimits, Mst2Client, ResolveDelivery, ResolveRequest, ResolveTarget, ScopeCache,
    SnapshotErrorCode, SnapshotFile, SnapshotReader,
};
use serde_json::{json, Value};
use tokio::sync::Notify;

const INSTANCE_ID: &str = "11111111-2222-4333-8444-555555555555";
const NAMESPACE_VIEW_ID: [u8; 32] = [0x22; 32];

#[derive(Default)]
struct Fixture {
    canonical: bool,
    raw_only: bool,
    lease_expiry: Option<String>,
    renewal_gone: bool,
    pause_renewal: bool,
    renewal_requests: AtomicUsize,
    root: [u8; 32],
    pages: HashMap<[u8; 32], Vec<u8>>,
    routes: HashMap<(String, Vec<u8>), [u8; 32]>,
    expected: Vec<SnapshotFile>,
    blobs: HashMap<String, Vec<u8>>,
    requests: Mutex<Vec<Vec<String>>>,
    omit: Mutex<Option<[u8; 32]>>,
    extra_route_page: Mutex<Option<[u8; 32]>>,
    blob_requests: AtomicUsize,
    frame_content: AtomicBool,
    object_requests: Mutex<Vec<Vec<String>>>,
    omit_object: AtomicBool,
    object_bad_end: AtomicBool,
    pause_objects: AtomicBool,
    object_started: Notify,
    object_release: Notify,
    object_barrier: Option<Arc<tokio::sync::Barrier>>,
    pause_metadata: AtomicBool,
    metadata_started: Notify,
    metadata_release: Notify,
    pause_blob: AtomicBool,
    fail_blob_once: AtomicBool,
    blob_started: Notify,
    blob_release: Notify,
    map_requests: Mutex<Vec<String>>,
    leaf_requests: Mutex<Vec<String>>,
    chunk_requests: Mutex<Vec<(String, u64)>>,
    range_fault: AtomicUsize,
    pause_chunks: AtomicBool,
    chunk_started: Notify,
    chunk_release: Notify,
}

fn id_string(id: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(id))
}

fn file_entry(name: &str, kind: EntryKind, bytes: &[u8]) -> Entry {
    Entry::file(
        kind,
        name.as_bytes(),
        bytes.len() as u64,
        parse_digest(&digest_of(bytes)).unwrap(),
    )
}

impl Fixture {
    fn page(&mut self, path: &str, route: Vec<u8>, page: Page) -> [u8; 32] {
        let bytes = page.encode().unwrap();
        let id = page_id(&bytes);
        self.pages.insert(id, bytes);
        self.routes.insert((path.to_string(), route), id);
        id
    }

    fn leaf(&mut self, path: &str, mut entries: Vec<Entry>) -> [u8; 32] {
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        self.page(path, vec![], Page::Leaf { entries })
    }

    fn expect_file(&mut self, path: &str, kind: &str, bytes: &[u8]) {
        self.expected.push(SnapshotFile {
            rel_path: path.into(),
            fs_kind: kind.into(),
            size: bytes.len() as u64,
            content_digest: digest_of(bytes),
        });
        self.blobs.insert(format!("/{path}"), bytes.to_vec());
    }

    fn snapshot_id(&self) -> String {
        id_string(
            &ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str(INSTANCE_ID).unwrap().as_bytes(),
                namespace_view_id: NAMESPACE_VIEW_ID,
                scope: "/project".into(),
                metadata_root: self.root,
            }
            .snapshot_id()
            .unwrap(),
        )
    }

    fn requested_ids(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .cloned()
            .collect()
    }
}

// The stable nested subtree is unchanged across versions and directory moves.
fn nested_fixture(stable_name: &str, changed: &[u8]) -> Fixture {
    let mut f = Fixture::default();
    let deep = f.leaf(
        &format!("/{stable_name}/nested"),
        vec![file_entry("deep.txt", EntryKind::Executable, b"deep")],
    );
    let stable = f.leaf(
        &format!("/{stable_name}"),
        vec![
            file_entry("f.txt", EntryKind::Regular, b"stable"),
            Entry::dir(b"nested", deep),
        ],
    );
    let other = f.leaf(
        "/other",
        vec![file_entry("tail.txt", EntryKind::Symlink, changed)],
    );
    f.root = f.leaf(
        "/",
        vec![
            Entry::dir(stable_name.as_bytes(), stable),
            Entry::dir(b"other", other),
        ],
    );
    f.expect_file(&format!("{stable_name}/f.txt"), "regular", b"stable");
    f.expect_file(
        &format!("{stable_name}/nested/deep.txt"),
        "executable",
        b"deep",
    );
    f.expect_file("other/tail.txt", "symlink", changed);
    f
}

fn identical_directories_fixture() -> Fixture {
    let mut f = Fixture::default();
    let same = f.leaf("/left", vec![file_entry("x", EntryKind::Regular, b"same")]);
    f.routes.insert(("/right".into(), vec![]), same);
    f.root = f.leaf(
        "/",
        vec![Entry::dir(b"left", same), Entry::dir(b"right", same)],
    );
    f.expect_file("left/x", "regular", b"same");
    f.expect_file("right/x", "regular", b"same");
    f
}

// Expected paths are written from the fixture's declared logical namespace,
// never obtained from either production walker.
fn complete_fixture(stable_name: &str, changed: &[u8]) -> Fixture {
    let mut f = nested_fixture(stable_name, changed);
    let old_root = f.root;
    let stable = f.routes[&(format!("/{stable_name}"), vec![])];
    let deep = f.routes[&(format!("/{stable_name}/nested"), vec![])];
    let other = f.routes[&("/other".into(), vec![])];
    f.routes.insert(("/alias".into(), vec![]), stable);
    f.routes.insert(("/alias/nested".into(), vec![]), deep);
    f.expect_file("alias/f.txt", "regular", b"stable");
    f.expect_file("alias/nested/deep.txt", "executable", b"deep");
    let empty = f.leaf("/empty-a", vec![]);
    f.routes.insert(("/empty-b".into(), vec![]), empty);
    let mut wide = wide_fixture();
    for ((path, route), id) in wide.routes.drain() {
        assert_eq!(path, "/");
        f.routes.insert(("/wide".into(), route), id);
    }
    f.pages.extend(wide.pages.drain());
    for file in wide.expected {
        let body = file.rel_path.as_bytes().to_vec();
        f.expect_file(&format!("wide/{}", file.rel_path), "regular", &body);
    }
    f.root = f.leaf(
        "/",
        vec![
            Entry::dir(stable_name.as_bytes(), stable),
            Entry::dir(b"alias", stable),
            Entry::dir(b"empty-a", empty),
            Entry::dir(b"empty-b", empty),
            Entry::dir(b"other", other),
            Entry::dir(b"wide", wide.root),
        ],
    );
    f.pages.remove(&old_root);
    f
}

fn wide_fixture() -> Fixture {
    let mut f = Fixture::default();
    let mut children = Vec::new();
    for label in *b"abc" {
        let mut entries = Vec::new();
        for i in 0..64 {
            let name = format!("{}{i:03}.txt", label as char);
            let data = name.as_bytes();
            entries.push(file_entry(&name, EntryKind::Regular, data));
            f.expect_file(&name, "regular", data);
        }
        let id = f.page("/", vec![label], Page::Leaf { entries });
        children.push(BranchChild {
            label,
            subtree_entries: 64,
            child_page_id: id,
        });
    }
    f.root = f.page(
        "/",
        vec![],
        Page::Branch {
            prefix: vec![],
            terminal: None,
            children,
        },
    );
    f
}

async fn capabilities(State(f): State<Arc<Fixture>>) -> Json<Value> {
    if f.canonical {
        let mut value: Value =
            serde_json::from_str(include_str!("fixtures/mst2_capabilities_0_2_1.json")).unwrap();
        if f.raw_only {
            value["features"]["small_objects"] = json!(false);
            value["features"]["chunk_reads"] = json!(false);
        }
        return Json(value);
    }
    Json(json!({
        "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true, "metadata_pages": true,
            "objects": f.frame_content.load(Ordering::SeqCst),
            "chunk_reads": f.frame_content.load(Ordering::SeqCst)}
    }))
}

async fn resolve(State(f): State<Arc<Fixture>>, body: Bytes) -> Json<Value> {
    let mut response = json!({
        "descriptor": {
            "schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE_ID,
            "namespace_view_id": id_string(&NAMESPACE_VIEW_ID), "scope": "/project",
            "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
            "metadata_root": id_string(&f.root), "snapshot_id": f.snapshot_id()
        },
        "lease_id": "fixture-lease", "lease_expires_at": f.lease_expiry.as_deref().unwrap_or("2099-01-01T00:00:00Z"),
        "publication_sequence": "1", "authorization_epoch": "1"
    });
    if f.canonical {
        // The owned v3 path uses the shipped typed latest/lazy request. Full
        // metadata acquisition below remains distinct from body hydration.
        let request: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            request,
            json!({
                "target": {"kind": "latest"}, "scope": "/project", "delivery": "lazy",
                "lease_seconds": 600, "supported_metadata_codecs": [1]
            })
        );
        response["writer_epoch"] = json!("1");
        response["resolved_at"] = json!("2026-09-15T00:00:00Z");
        response["delivery"] = json!("lazy");
    }
    Json(response)
}

async fn renew(State(f): State<Arc<Fixture>>) -> (StatusCode, Json<Value>) {
    f.renewal_requests.fetch_add(1, Ordering::SeqCst);
    if f.pause_renewal {
        return std::future::pending().await;
    }
    let (status, code) = if f.renewal_gone {
        (StatusCode::GONE, "SNAPSHOT_GONE")
    } else {
        (StatusCode::FORBIDDEN, "SCOPE_FORBIDDEN")
    };
    (
        status,
        Json(
            json!({"error": {"code": code, "message": "fixture retention failure",
            "request_id": "fixture-renewal", "retryable": false}}),
        ),
    )
}

async fn metadata(
    State(f): State<Arc<Fixture>>,
    AxumPath(snapshot_id): AxumPath<String>,
    body: Bytes,
) -> Response {
    let req: Value = serde_json::from_slice(&body).unwrap();
    let items = req["items"].as_array().unwrap();
    let mut pages = Vec::new();
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for item in items {
        let path = item["directory_path"].as_str().unwrap();
        let route: Vec<u8> = serde_json::from_value(item["route"].clone()).unwrap();
        let id = parse_digest(item["expected_digest"].as_str().unwrap()).unwrap();
        assert_eq!(
            f.routes.get(&(path.to_string(), route.clone())),
            Some(&id),
            "request must use the fixed logical directory route"
        );
        ids.push(id_string(&id));
        // Match production pages_along_route: ancestors are repeated as
        // witnesses even when a previous request already fetched them.
        for depth in 0..=route.len() {
            let witness = f.routes[&(path.to_string(), route[..depth].to_vec())];
            if Some(witness) != *f.omit.lock().unwrap() && seen.insert(witness) {
                pages.push((witness, f.pages[&witness].clone()));
            }
        }
    }
    f.requests.lock().unwrap().push(ids);
    if f.pause_metadata.load(Ordering::SeqCst) {
        f.metadata_started.notify_one();
        f.metadata_release.notified().await;
    }
    if items
        .iter()
        .any(|item| !item["route"].as_array().unwrap().is_empty())
    {
        if let Some(id) = *f.extra_route_page.lock().unwrap() {
            if seen.insert(id) {
                pages.push((id, f.pages[&id].clone()));
            }
        }
    }
    // A legal response deliberately differs from request order and removes
    // repeated physical pages, exercising path-independent attribution.
    pages.reverse();
    let mut wire = Vec::new();
    let mut sequence = 0;
    let logical_bytes = pages.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    if !pages.is_empty() {
        wire.extend(
            MetaPayload {
                pages: pages.clone(),
            }
            .encode(7, sequence)
            .unwrap(),
        );
        sequence += 1;
    }
    wire.extend(
        EndPayload {
            request_item_count: items.len() as u32,
            unique_unit_count: pages.len() as u32,
            logical_bytes,
            request_body_sha256: parse_digest(&digest_of(&body)).unwrap(),
        }
        .encode(7, sequence),
    );
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", snapshot_id)
        .header("x-mega-request-digest", digest_of(&body))
        .body(Body::from(wire))
        .unwrap()
}

async fn blob(
    State(f): State<Arc<Fixture>>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    f.blob_requests.fetch_add(1, Ordering::SeqCst);
    if f.fail_blob_once.swap(false, Ordering::SeqCst) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": {"code": "SCOPE_FORBIDDEN", "message": "fixture denial"}})),
        )
            .into_response();
    }
    if f.pause_blob.load(Ordering::SeqCst) {
        f.blob_started.notify_one();
        f.blob_release.notified().await;
    }
    let bytes = &f.blobs[&query["path"]];
    assert_eq!(query["expected_digest"], digest_of(bytes));
    bytes.clone().into_response()
}

async fn objects(
    State(f): State<Arc<Fixture>>,
    AxumPath(snapshot_id): AxumPath<String>,
    body: Bytes,
) -> Response {
    let req: Value = serde_json::from_slice(&body).unwrap();
    let items = req["items"].as_array().unwrap();
    let mut seen = HashSet::new();
    let mut objects = Vec::new();
    for item in items {
        let bytes = &f.blobs[item["path"].as_str().unwrap()];
        assert_eq!(item["expected_digest"], digest_of(bytes));
        let digest = parse_digest(&digest_of(bytes)).unwrap();
        assert!(
            seen.insert(digest),
            "batch must deduplicate logical aliases"
        );
        objects.push((digest, bytes.clone()));
    }
    assert!(!items.is_empty() && items.len() <= 128);
    f.object_requests.lock().unwrap().push(
        items
            .iter()
            .map(|item| item["expected_digest"].as_str().unwrap().to_string())
            .collect(),
    );
    if let Some(barrier) = &f.object_barrier {
        barrier.wait().await;
    }
    if f.pause_objects.load(Ordering::SeqCst) {
        f.object_started.notify_one();
        f.object_release.notified().await;
    }
    if f.omit_object.swap(false, Ordering::SeqCst) {
        objects.pop();
    }
    let logical_bytes = objects.iter().map(|(_, data)| data.len() as u64).sum();
    let mut wire = Vec::new();
    let sequence = if objects.is_empty() {
        0
    } else {
        wire.extend(
            ObjectPayload {
                objects: objects.clone(),
            }
            .encode(8, 0)
            .unwrap(),
        );
        1
    };
    let mut request_digest = parse_digest(&digest_of(&body)).unwrap();
    if f.object_bad_end.load(Ordering::SeqCst) {
        request_digest[0] ^= 1;
    }
    wire.extend(
        EndPayload {
            request_item_count: items.len() as u32,
            unique_unit_count: objects.len() as u32,
            logical_bytes,
            request_body_sha256: request_digest,
        }
        .encode(8, sequence),
    );
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", snapshot_id)
        .header("x-mega-request-digest", digest_of(&body))
        .body(Body::from(wire))
        .unwrap()
}

fn fixture_chunk_map(bytes: &[u8]) -> (ChunkMap, ChunkLeaf) {
    let leaf = ChunkLeaf {
        page_index: 0,
        chunk_sha256: bytes
            .chunks(CHUNK_SIZE as usize)
            .map(|chunk| parse_digest(&digest_of(chunk)).unwrap())
            .collect(),
    };
    assert!(leaf.chunk_sha256.len() <= 256);
    let map = ChunkMap::new(
        parse_digest(&digest_of(bytes)).unwrap(),
        bytes.len() as u64,
        leaf.leaf_hash().unwrap(),
    )
    .unwrap();
    (map, leaf)
}

fn fixture_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for part in bytes.chunks(3) {
        let bits = u32::from(part[0]) << 16
            | u32::from(part.get(1).copied().unwrap_or(0)) << 8
            | u32::from(part.get(2).copied().unwrap_or(0));
        result.push(ALPHABET[(bits >> 18) as usize] as char);
        result.push(ALPHABET[((bits >> 12) & 63) as usize] as char);
        result.push(if part.len() > 1 {
            ALPHABET[((bits >> 6) & 63) as usize] as char
        } else {
            '='
        });
        result.push(if part.len() > 2 {
            ALPHABET[(bits & 63) as usize] as char
        } else {
            '='
        });
    }
    result
}

async fn range_map(
    State(f): State<Arc<Fixture>>,
    AxumPath(sid): AxumPath<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Json<Value> {
    assert_eq!(sid, f.snapshot_id());
    assert_eq!(headers["x-mega-snapshot-lease"], "fixture-lease");
    assert_eq!(query.len(), 2);
    let bytes = &f.blobs[&query["path"]];
    assert_eq!(query["expected_digest"], digest_of(bytes));
    f.map_requests.lock().unwrap().push(query["path"].clone());
    let (map, _) = fixture_chunk_map(bytes);
    let mut value = json!({"snapshot_id":sid,"path":query["path"],"map":{
        "schema_version":2,"file_content_id":id_string(&map.file_content_id),
        "file_size":map.file_size.to_string(),"chunk_size":CHUNK_SIZE,
        "chunk_count":map.chunk_count.to_string(),"page_count":map.page_count.to_string(),
        "pages_root":id_string(&map.pages_root),"map_id":id_string(&map.map_id())}});
    if f.range_fault.load(Ordering::SeqCst) == 4 {
        value["map"]["map_id"] = json!(id_string(&[0x91; 32]));
    }
    Json(value)
}

async fn range_leaf(
    State(f): State<Arc<Fixture>>,
    AxumPath(sid): AxumPath<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Json<Value> {
    assert_eq!(sid, f.snapshot_id());
    assert_eq!(headers["x-mega-snapshot-lease"], "fixture-lease");
    assert_eq!(query.len(), 3);
    assert_eq!(query["page_index"], "0");
    let (map, leaf) = fixture_chunk_map(&f.blobs[&query["path"]]);
    assert_eq!(query["map_id"], id_string(&map.map_id()));
    f.leaf_requests.lock().unwrap().push(query["path"].clone());
    let mut encoded = leaf.encode().unwrap();
    if f.range_fault.load(Ordering::SeqCst) == 5 {
        *encoded.last_mut().unwrap() ^= 1;
    }
    Json(json!({"map_id":id_string(&map.map_id()),"page_index":"0",
        "leaf_base64":fixture_base64(&encoded),"proof":[]}))
}

async fn range_chunks(
    State(f): State<Arc<Fixture>>,
    AxumPath(sid): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    assert_eq!(sid, f.snapshot_id());
    assert_eq!(headers["x-mega-snapshot-lease"], "fixture-lease");
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let item = &items[0];
    let path = item["path"].as_str().unwrap();
    let bytes = &f.blobs[path];
    let (map, _) = fixture_chunk_map(bytes);
    assert_eq!(item["expected_digest"], digest_of(bytes));
    assert_eq!(item["map_id"], id_string(&map.map_id()));
    let index = item["chunk_index"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let start = index as usize * CHUNK_SIZE as usize;
    let chunk = &bytes[start..(start + CHUNK_SIZE as usize).min(bytes.len())];
    f.chunk_requests.lock().unwrap().push((path.into(), index));
    if f.pause_chunks.load(Ordering::SeqCst) {
        f.chunk_started.notify_one();
        f.chunk_release.notified().await;
    }
    let mode = f.range_fault.load(Ordering::SeqCst);
    let mut payload = ChunkPayload {
        map_id: map.map_id(),
        file_content_id: map.file_content_id,
        chunk_index: index,
        chunk_bytes: chunk.to_vec(),
    };
    if mode == 1 {
        payload.chunk_bytes[0] ^= 1;
    }
    if mode == 3 {
        payload.map_id[0] ^= 1;
    }
    let mut wire = payload.encode(19, 0).unwrap();
    let mut end_digest = parse_digest(&digest_of(&body)).unwrap();
    if mode == 2 || mode == 7 && index > 0 {
        end_digest[0] ^= 1;
    }
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: chunk.len() as u64,
            request_body_sha256: end_digest,
        }
        .encode(19, 1),
    );
    if mode == 6 {
        wire.push(0);
    }
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", sid)
        .header("x-mega-request-digest", digest_of(&body))
        .body(Body::from(wire))
        .unwrap()
}

struct HttpFixture {
    fixture: Arc<Fixture>,
    base: String,
    listener: StdTcpListener,
    task: tokio::task::JoinHandle<()>,
}

impl HttpFixture {
    async fn start_canonical(mut fixture: Fixture) -> Self {
        fixture.canonical = true;
        Self::start(fixture).await
    }

    async fn start(fixture: Fixture) -> Self {
        let fixture = Arc::new(fixture);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .into_std()
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = Self::serve(&listener, fixture.clone());
        Self {
            fixture,
            base,
            listener,
            task,
        }
    }

    fn serve(listener: &StdTcpListener, fixture: Arc<Fixture>) -> tokio::task::JoinHandle<()> {
        let listener = tokio::net::TcpListener::from_std(listener.try_clone().unwrap()).unwrap();
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(capabilities))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .route("/api/v2/snapshots/leases/{lease}/renew", post(renew))
            .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
            .route("/api/v2/snapshots/{sid}/blob", get(blob))
            .route("/api/v2/snapshots/{sid}/objects", post(objects))
            .route("/api/v2/snapshots/{sid}/chunk-map", get(range_map))
            .route("/api/v2/snapshots/{sid}/chunk-map/pages", get(range_leaf))
            .route("/api/v2/snapshots/{sid}/chunks", post(range_chunks))
            .with_state(fixture.clone());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() })
    }

    async fn restart(mut self, fixture: Fixture) -> Self {
        self.task.abort();
        let _ = (&mut self.task).await;
        // Keep the listening socket bound while replacing the server. V1 and
        // V2 share the deployment URL, independently of ephemeral port reuse.
        self.fixture = Arc::new(fixture);
        self.task = Self::serve(&self.listener, self.fixture.clone());
        self
    }

    async fn reader(&self) -> SnapshotReader {
        SnapshotReader::resolve(Mst2Client::new(&self.base), "/project", 600)
            .await
            .unwrap()
    }

    async fn canonical_reader(&self) -> SnapshotReader {
        assert!(self.fixture.canonical);
        let reader = SnapshotReader::resolve_request(
            Mst2Client::new(&self.base),
            &ResolveRequest {
                target: ResolveTarget::Latest,
                scope: "/project".into(),
                delivery: ResolveDelivery::Lazy,
                lease_seconds: 600,
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            reader.capability_advertisement(),
            CapabilityAdvertisement::Canonical(_)
        ));
        reader
    }
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn assert_manifest(actual: &[SnapshotFile], expected: &[SnapshotFile]) {
    let index = |files: &[SnapshotFile]| {
        files
            .iter()
            .map(|f| {
                (
                    f.rel_path.clone(),
                    (f.fs_kind.clone(), f.size, f.content_digest.clone()),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(index(actual), index(expected));
    assert_eq!(
        actual.len(),
        expected.len(),
        "duplicate logical files are also invalid"
    );
}

fn owned_fuse_page_cache(
    root: &std::path::Path,
    reader: &SnapshotReader,
) -> (Arc<DurableStore>, ScopeCache) {
    let store = Arc::new(
        DurableStore::open_for_workspace(root, &uuid::Uuid::new_v4().to_string(), reader).unwrap(),
    );
    let scope = store.content_dir().parent().unwrap();
    reader.authorized_context().bind_scope_cache(scope).unwrap();
    let cache = ScopeCache::open(scope).unwrap();
    (store, cache)
}

fn cached_page_path(cache: &ScopeCache, id: &[u8; 32]) -> std::path::PathBuf {
    cache.dir().join("pages").join(hex::encode(id))
}

#[tokio::test]
async fn owned_lazy_fuse_reuses_synced_metadata_pages_without_expanding_unrelated_directories() {
    let http = HttpFixture::start_canonical(complete_fixture("a", b"target-one")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, cache) = owned_fuse_page_cache(root.path(), &reader);
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    assert_manifest(closure.files(), &http.fixture.expected);
    http.fixture.requests.lock().unwrap().clear();
    let fs = Mst2Fuse::from_reader_lazy(reader, Some(store))
        .await
        .unwrap();

    // This path loads a single directory. A missing unrelated subtree must
    // neither be requested nor make this complete listing fail.
    let unrelated = http.fixture.routes[&("/other".into(), vec![])];
    std::fs::remove_file(cached_page_path(&cache, &unrelated)).unwrap();
    *http.fixture.omit.lock().unwrap() = Some(unrelated);
    assert_eq!(fs.directory_entries("/a").await.unwrap().len(), 2);
    assert!(http.fixture.requested_ids().is_empty());
    cache
        .put_page(&id_string(&unrelated), &http.fixture.pages[&unrelated])
        .unwrap();
    *http.fixture.omit.lock().unwrap() = None;
    for directory in [
        "",
        "a",
        "a/nested",
        "alias",
        "alias/nested",
        "empty-a",
        "empty-b",
        "other",
        "wide",
    ] {
        let entries = fs.directory_entries(directory).await.unwrap();
        if directory.starts_with("empty-") {
            assert!(entries.is_empty());
        }
        if directory == "wide" {
            assert_eq!(entries.len(), 192);
        }
    }
    let a = fs
        .lookup(Request::default(), 1, OsStr::new("a"))
        .await
        .unwrap()
        .attr
        .ino;
    let alias = fs
        .lookup(Request::default(), 1, OsStr::new("alias"))
        .await
        .unwrap()
        .attr
        .ino;
    assert_ne!(a, alias, "logical aliases retain separate inodes");
    assert_ne!(
        fs.lookup(Request::default(), a, OsStr::new("f.txt"))
            .await
            .unwrap()
            .attr
            .ino,
        fs.lookup(Request::default(), alias, OsStr::new("f.txt"))
            .await
            .unwrap()
            .attr
            .ino,
    );
    assert!(http.fixture.requested_ids().is_empty());
}

#[tokio::test]
async fn owned_lazy_fuse_fetches_only_missing_corrupt_and_oversized_page_hints() {
    let http = HttpFixture::start_canonical(complete_fixture("a", b"target-one")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, cache) = owned_fuse_page_cache(root.path(), &reader);
    IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    let missing = http.fixture.routes[&("/wide".into(), vec![b'b'])];
    let corrupt = http.fixture.routes[&("/wide".into(), vec![b'c'])];
    let oversized = http.fixture.routes[&("/a".into(), vec![])];
    std::fs::remove_file(cached_page_path(&cache, &missing)).unwrap();
    std::fs::write(cached_page_path(&cache, &corrupt), b"broken page").unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(cached_page_path(&cache, &oversized))
        .unwrap()
        .set_len((mst2_codec::metapage::PAGE_MAX_BYTES + 1) as u64)
        .unwrap();
    http.fixture.requests.lock().unwrap().clear();
    let fs = Mst2Fuse::from_reader_lazy(reader, Some(store))
        .await
        .unwrap();
    assert!(http.fixture.requested_ids().is_empty());
    assert_eq!(fs.directory_entries("wide").await.unwrap().len(), 192);
    let mut actual = http.fixture.requested_ids();
    actual.sort();
    let mut expected = vec![id_string(&missing), id_string(&corrupt)];
    expected.sort();
    assert_eq!(actual, expected, "cached radix witnesses are not requested");
    http.fixture.requests.lock().unwrap().clear();
    assert_eq!(fs.directory_entries("a").await.unwrap().len(), 2);
    assert_eq!(http.fixture.requested_ids(), [id_string(&oversized)]);
}

#[tokio::test]
async fn owned_lazy_fuse_does_not_publish_a_partial_directory_after_bad_wire() {
    let http = HttpFixture::start_canonical(complete_fixture("a", b"target-one")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, cache) = owned_fuse_page_cache(root.path(), &reader);
    IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    let missing = http.fixture.routes[&("/wide".into(), vec![b'b'])];
    std::fs::remove_file(cached_page_path(&cache, &missing)).unwrap();
    *http.fixture.omit.lock().unwrap() = Some(missing);
    let fs = Mst2Fuse::from_reader_lazy(reader, Some(store))
        .await
        .unwrap();
    http.fixture.requests.lock().unwrap().clear();
    for _ in 0..2 {
        assert_eq!(
            fs.directory_entries("wide").await.unwrap_err().code,
            SnapshotErrorCode::DigestMismatch
        );
    }
    assert_eq!(
        http.fixture.requested_ids(),
        [id_string(&missing), id_string(&missing)]
    );
    *http.fixture.omit.lock().unwrap() = None;
    http.fixture.requests.lock().unwrap().clear();
    assert_eq!(fs.directory_entries("wide").await.unwrap().len(), 192);
    assert_eq!(http.fixture.requested_ids(), [id_string(&missing)]);
}

#[tokio::test]
async fn owned_lazy_fuse_rejects_another_authority_before_reading_page_hints() {
    let http = HttpFixture::start_canonical(complete_fixture("a", b"target-one")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, cache) = owned_fuse_page_cache(root.path(), &reader);
    IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    let other = HttpFixture::start_canonical(complete_fixture("a", b"target-one")).await;
    let other_reader = other.canonical_reader().await;
    assert_eq!(reader.snapshot_id(), other_reader.snapshot_id());
    assert_ne!(
        reader.authorized_context().cache_domain(),
        other_reader.authorized_context().cache_domain()
    );
    let error = Mst2Fuse::from_reader_lazy(other_reader, Some(store))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, SnapshotErrorCode::ScopeForbidden);
    assert!(other.fixture.requested_ids().is_empty());
}

#[tokio::test]
async fn lazy_fuse_without_an_owned_store_keeps_the_wire_page_path() {
    let http = HttpFixture::start_canonical(complete_fixture("a", b"target-one")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (_owned, cache) = owned_fuse_page_cache(root.path(), &reader);
    IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    let ordinary = Arc::new(
        DurableStore::open_for_reader(
            cache.dir().join("ordinary-view"),
            cache.dir().join("blobs"),
            &reader,
        )
        .unwrap(),
    );
    assert!(ordinary.workspace_binding().unwrap().is_none());
    for store in [None, Some(ordinary)] {
        http.fixture.requests.lock().unwrap().clear();
        let fs = Mst2Fuse::from_reader_lazy(reader.clone(), store)
            .await
            .unwrap();
        assert_eq!(
            http.fixture.requested_ids(),
            [id_string(&http.fixture.root)]
        );
        assert_eq!(fs.directory_entries("a").await.unwrap().len(), 2);
        assert_eq!(http.fixture.requested_ids().len(), 2);
    }
}

#[tokio::test]
async fn owned_cached_pages_still_enforce_namespace_node_bounds() {
    let http = HttpFixture::start_canonical(complete_fixture("a", b"target-one")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, cache) = owned_fuse_page_cache(root.path(), &reader);
    IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    http.fixture.requests.lock().unwrap().clear();
    let error = Mst2Fuse::from_reader_lazy_with_limits(
        reader,
        Some(store),
        MetadataProofLimits {
            max_cached_nodes: 1,
            ..MetadataProofLimits::default()
        },
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.code, SnapshotErrorCode::ProofBudgetExceeded);
    assert!(http.fixture.requested_ids().is_empty());
}

#[tokio::test]
async fn owned_cached_pages_do_not_bypass_the_complete_directory_count_proof() {
    let mut fixture = complete_fixture("a", b"target-one");
    let old_wide = fixture.routes[&("/wide".into(), vec![])];
    let (mut wide, _) = Page::decode(&fixture.pages[&old_wide]).unwrap();
    match &mut wide {
        Page::Branch { children, .. } => children[0].subtree_entries += 1,
        _ => panic!("wide fixture has a radix root"),
    }
    let bad_wide = fixture.page("/wide", vec![], wide);
    let old_root = fixture.root;
    let (mut root_page, _) = Page::decode(&fixture.pages[&old_root]).unwrap();
    match &mut root_page {
        Page::Leaf { entries } => {
            entries
                .iter_mut()
                .find(|e| e.name.as_slice() == b"wide")
                .unwrap()
                .child_root = bad_wide
        }
        _ => panic!("scope fixture has a leaf root"),
    }
    fixture.root = fixture.page("/", vec![], root_page);
    fixture.pages.remove(&old_root);
    fixture.pages.remove(&old_wide);
    let http = HttpFixture::start_canonical(fixture).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, cache) = owned_fuse_page_cache(root.path(), &reader);
    // These individually hash-valid bytes are deliberately just cache hints.
    // The fixed-root directory proof must still reject their false count.
    for (id, bytes) in &http.fixture.pages {
        cache.put_page(&id_string(id), bytes).unwrap();
    }
    let fs = Mst2Fuse::from_reader_lazy(reader, Some(store))
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            fs.directory_entries("wide").await.unwrap_err().code,
            SnapshotErrorCode::IntegrityError
        );
    }
    assert!(http.fixture.requested_ids().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn owned_lazy_fuse_propagates_page_open_errors_without_wire_fallback() {
    let http = HttpFixture::start_canonical(complete_fixture("a", b"target-one")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, cache) = owned_fuse_page_cache(root.path(), &reader);
    IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    let fs = Mst2Fuse::from_reader_lazy(reader, Some(store))
        .await
        .unwrap();
    let id = http.fixture.routes[&("/a".into(), vec![])];
    let path = cached_page_path(&cache, &id);
    std::fs::remove_file(&path).unwrap();
    let target = root.path().join("outside-page");
    std::fs::write(&target, &http.fixture.pages[&id]).unwrap();
    std::os::unix::fs::symlink(&target, &path).unwrap();
    http.fixture.requests.lock().unwrap().clear();
    assert_eq!(
        fs.directory_entries("a").await.unwrap_err().code,
        SnapshotErrorCode::Internal
    );
    assert!(http.fixture.requested_ids().is_empty());
}

fn small_cas_fixture(link_target: &[u8]) -> Fixture {
    let mut fixture = Fixture::default();
    let body = vec![0x6a; 8192];
    let large = vec![0x8b; 2 * 1024 * 1024];
    let big_small = vec![0xa7; 246 * 1024];
    let mut entries = vec![
        file_entry("alpha", EntryKind::Regular, &body),
        file_entry("big-small", EntryKind::Regular, &big_small),
        file_entry("empty", EntryKind::Regular, b""),
        file_entry("exec", EntryKind::Executable, &body),
        file_entry("large", EntryKind::Regular, &large),
        file_entry("link", EntryKind::Symlink, link_target),
    ];
    fixture.expect_file("alpha", "regular", &body);
    fixture.expect_file("big-small", "regular", &big_small);
    fixture.expect_file("empty", "regular", b"");
    fixture.expect_file("exec", "executable", &body);
    fixture.expect_file("large", "regular", &large);
    fixture.expect_file("link", "symlink", link_target);
    for index in 0..64 {
        let name = format!("alias{index:03}");
        entries.push(file_entry(&name, EntryKind::Regular, &body));
        fixture.expect_file(&name, "regular", &body);
    }
    fixture.root = fixture.leaf("/", entries);
    fixture
}

fn cas_path(store: &DurableStore, body: &[u8]) -> std::path::PathBuf {
    store
        .content_dir()
        .join(hex::encode(parse_digest(&digest_of(body)).unwrap()))
}

async fn owned_small_cas_view(
    http: &HttpFixture,
    reader: &SnapshotReader,
    root: &std::path::Path,
    fill_cas: bool,
) -> (Arc<DurableStore>, Arc<Mst2Fuse>) {
    let (store, cache) = owned_fuse_page_cache(root, reader);
    let closure = IncrementalSync::new(reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    // Reuse the real complete metadata proof exactly as full hydration seeds
    // membership. These controlled CAS fixtures make no COMPLETE/pin claim.
    reader.seed_content_membership(&closure).unwrap();
    if fill_cas {
        for body in http.fixture.blobs.values() {
            std::fs::write(cas_path(&store, body), body).unwrap();
        }
    }
    let fs = Arc::new(
        Mst2Fuse::from_reader_lazy(reader.clone(), Some(store.clone()))
            .await
            .unwrap(),
    );
    (store, fs)
}

async fn root_file_inode(fs: &Mst2Fuse, name: &str) -> u64 {
    fs.lookup(Request::default(), 1, OsStr::new(name))
        .await
        .unwrap()
        .attr
        .ino
}

async fn small_read(
    fs: &Mst2Fuse,
    name: &str,
    offset: u64,
    size: u32,
) -> asyncfuse::raw::reply::ReplyData {
    let inode = root_file_inode(fs, name).await;
    fs.read(Request::default(), inode, inode, offset, size)
        .await
        .unwrap()
}

#[tokio::test]
async fn owned_cas_aliases_share_the_actual_reply_owner_and_last_bytes_keep_credit() {
    let http = HttpFixture::start_canonical(small_cas_fixture(b"alpha")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
    http.fixture.requests.lock().unwrap().clear();
    let slots = reader.content_usage().output_bytes;
    let alpha = root_file_inode(&fs, "alpha").await;
    let first = fs
        .read(Request::default(), alpha, alpha, 0, 8192)
        .await
        .unwrap();
    let paid = reader.content_usage().output_bytes - slots;
    assert!(paid >= 8192);
    assert_eq!(reader.content_usage().construction_bytes, 0);
    // All 64 aliases and the executable must now work from the same owner.
    // Removing its local object detects a second CAS read or body fallback.
    std::fs::remove_file(cas_path(&store, &http.fixture.blobs["/alpha"])).unwrap();
    let mut inodes = HashSet::from([alpha]);
    for name in (0..64)
        .map(|index| format!("alias{index:03}"))
        .chain(["exec".into()])
    {
        let inode = root_file_inode(&fs, &name).await;
        assert!(inodes.insert(inode), "content sharing never merges inodes");
        let reply = fs
            .read(Request::default(), inode, inode, 17, 31)
            .await
            .unwrap();
        assert_eq!(reply.data.as_ptr(), first.data.as_ptr().wrapping_add(17));
        assert_eq!(reply.data.as_ref(), &[0x6a; 31]);
    }
    assert_eq!(reader.content_usage().output_bytes, slots + paid);
    assert!(
        http.fixture.requested_ids().is_empty(),
        "seeded membership avoids metadata RPC"
    );
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
    let clone = first.data.clone();
    let last = clone.slice(23..41);
    drop(first);
    drop(clone);
    drop(fs);
    assert_eq!(reader.content_usage().output_bytes, paid);
    assert_eq!(last.as_ref(), &[0x6a; 18]);
    drop(last);
    assert_eq!(reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn owned_cas_readlink_rejects_nul_and_empty_or_eof_read_retains_no_payload() {
    let http = HttpFixture::start_canonical(small_cas_fixture(b"alpha")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
    let baseline = reader.content_usage().output_bytes;
    for (name, offset, size) in [
        ("empty", 0, 1),
        ("alpha", 0, 0),
        ("alpha", 8192, 1),
        ("large", u64::MAX, u32::MAX),
    ] {
        assert!(small_read(&fs, name, offset, size).await.data.is_empty());
        assert_eq!(reader.content_usage().output_bytes, baseline);
    }
    let link = root_file_inode(&fs, "link").await;
    let first = fs.readlink(Request::default(), link).await.unwrap();
    let second = fs.readlink(Request::default(), link).await.unwrap();
    assert_eq!(first.data.as_ref(), b"alpha");
    assert_eq!(first.data.as_ptr(), second.data.as_ptr());
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());

    let bad = HttpFixture::start_canonical(small_cas_fixture(b"al\0pha")).await;
    let bad_reader = bad.canonical_reader().await;
    let bad_root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&bad, &bad_reader, bad_root.path(), true).await;
    let baseline = bad_reader.content_usage().output_bytes;
    let link = root_file_inode(&fs, "link").await;
    for _ in 0..2 {
        assert!(fs.readlink(Request::default(), link).await.is_err());
        assert_eq!(bad_reader.content_usage().output_bytes, baseline);
    }
    assert!(bad.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(bad.fixture.blob_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn owned_cas_integrity_and_regular_file_errors_never_fall_back_to_http() {
    for fault in ["hash", "size", "directory"] {
        let http = HttpFixture::start_canonical(small_cas_fixture(b"alpha")).await;
        let reader = http.canonical_reader().await;
        let root = tempfile::tempdir().unwrap();
        let (store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
        let body = &http.fixture.blobs["/alpha"];
        let path = cas_path(&store, body);
        match fault {
            "hash" => std::fs::write(&path, vec![0x6b; body.len()]).unwrap(),
            "size" => std::fs::write(&path, &body[..body.len() - 1]).unwrap(),
            _ => {
                std::fs::remove_file(&path).unwrap();
                std::fs::create_dir(&path).unwrap();
            }
        }
        let baseline = reader.content_usage().output_bytes;
        let inode = root_file_inode(&fs, "alpha").await;
        for _ in 0..2 {
            assert!(fs
                .read(Request::default(), inode, inode, 0, 1)
                .await
                .is_err());
            assert_eq!(reader.content_usage().output_bytes, baseline);
            assert_eq!(reader.content_usage().construction_bytes, 0);
        }
        assert!(http.fixture.object_requests.lock().unwrap().is_empty());
        assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
        if fault == "directory" {
            std::fs::remove_dir(&path).unwrap();
        }
        std::fs::write(&path, body).unwrap();
        assert_eq!(small_read(&fs, "alpha", 0, 1).await.data.as_ref(), [0x6a]);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn owned_cas_final_symlink_is_an_error_even_when_its_target_matches() {
    let http = HttpFixture::start_canonical(small_cas_fixture(b"alpha")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
    let body = &http.fixture.blobs["/alpha"];
    let path = cas_path(&store, body);
    std::fs::remove_file(&path).unwrap();
    let outside = root.path().join("outside-blob");
    std::fs::write(&outside, body).unwrap();
    std::os::unix::fs::symlink(&outside, &path).unwrap();
    let baseline = reader.content_usage().output_bytes;
    let inode = root_file_inode(&fs, "alpha").await;
    assert!(fs
        .read(Request::default(), inode, inode, 0, 1)
        .await
        .is_err());
    assert_eq!(reader.content_usage().output_bytes, baseline);
    assert_eq!(std::fs::read(outside).unwrap().as_slice(), body.as_slice());
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn owned_cas_missing_object_uses_paid_transport_and_bad_end_publishes_no_cache() {
    let http = HttpFixture::start_canonical(small_cas_fixture(b"alpha")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), false).await;
    let baseline = reader.content_usage().output_bytes;
    let inode = root_file_inode(&fs, "alpha").await;
    http.fixture.object_bad_end.store(true, Ordering::SeqCst);
    for _ in 0..2 {
        assert!(fs
            .read(Request::default(), inode, inode, 0, 1)
            .await
            .is_err());
        assert_eq!(reader.content_usage().output_bytes, baseline);
    }
    assert_eq!(http.fixture.object_requests.lock().unwrap().len(), 2);
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
    http.fixture.object_bad_end.store(false, Ordering::SeqCst);
    let first = small_read(&fs, "alpha", 0, 8192).await;
    let alias = small_read(&fs, "alias000", 7, 19).await;
    assert_eq!(alias.data.as_ptr(), first.data.as_ptr().wrapping_add(7));
    assert_eq!(http.fixture.object_requests.lock().unwrap().len(), 3);
    drop(first);
    drop(alias);
    drop(fs);
    assert_eq!(reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn owned_cas_missing_object_can_use_accounted_raw_transport_without_reply_copy() {
    let mut fixture = small_cas_fixture(b"alpha");
    fixture.raw_only = true;
    let http = HttpFixture::start_canonical(fixture).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), false).await;
    let first = small_read(&fs, "alpha", 0, 8192).await;
    let alias = small_read(&fs, "alias000", 31, 7).await;
    assert_eq!(alias.data.as_ptr(), first.data.as_ptr().wrapping_add(31));
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 1);
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    drop(fs);
    let paid = reader.content_usage().output_bytes;
    assert!(paid >= 8192);
    drop(first);
    assert!(reader.content_usage().output_bytes > 0);
    drop(alias);
    assert_eq!(reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn owned_cas_capacity_failure_does_not_try_a_compatibility_body_or_false_eof() {
    let http = HttpFixture::start_canonical(small_cas_fixture(b"alpha")).await;
    let reader = http
        .canonical_reader()
        .await
        .with_content_limits(ContentBudgetLimits::new(192 * 1024, 64 * 1024).unwrap());
    let root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
    let baseline = reader.content_usage().output_bytes;
    let inode = root_file_inode(&fs, "big-small").await;
    for _ in 0..2 {
        assert!(fs
            .read(Request::default(), inode, inode, 0, 1)
            .await
            .is_err());
        assert_eq!(reader.content_usage().output_bytes, baseline);
        assert_eq!(reader.content_usage().construction_bytes, 0);
    }
    assert!(fs
        .read(Request::default(), inode, inode, 246 * 1024, 1)
        .await
        .unwrap()
        .data
        .is_empty());
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
}

fn owned_large_cas_fixture(aliases: usize, changed: bool) -> Fixture {
    let mut fixture = Fixture::default();
    let mut body = vec![0x31; 2 * CHUNK_SIZE as usize + 7];
    body[CHUNK_SIZE as usize..2 * CHUNK_SIZE as usize].fill(0x72);
    body[2 * CHUNK_SIZE as usize..].fill(0xe4);
    let stable = body.clone();
    if changed {
        body[17] ^= 1;
    }
    let mut entries = vec![file_entry("stable", EntryKind::Regular, &stable)];
    fixture.expect_file("stable", "regular", &stable);
    for index in 0..aliases {
        let name = format!("range{index:03}");
        entries.push(file_entry(&name, EntryKind::Regular, &body));
        fixture.expect_file(&name, "regular", &body);
    }
    fixture.root = fixture.leaf("/", entries);
    fixture
}

fn assert_no_range_wire(http: &HttpFixture) {
    assert!(http.fixture.map_requests.lock().unwrap().is_empty());
    assert!(http.fixture.leaf_requests.lock().unwrap().is_empty());
    assert!(http.fixture.chunk_requests.lock().unwrap().is_empty());
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn owned_cas_range_local_cold_warm_cross_chunk_tail_and_held_reply() {
    let http = HttpFixture::start_canonical(owned_large_cas_fixture(1, false)).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
    let inode = root_file_inode(&fs, "range000").await;
    let baseline = reader.content_usage();
    let first = fs
        .read(Request::default(), inode, inode, 0, 4096)
        .await
        .unwrap();
    assert_eq!(first.data.as_ref(), &[0x31; 4096]);
    assert!(reader.content_usage().output_bytes > baseline.output_bytes + 4096);
    assert_eq!(
        reader.content_usage().construction_bytes,
        baseline.construction_bytes
    );
    let body = &http.fixture.blobs["/range000"];
    let mut meters = scorpiofs::snapshot::LocalCasRangeMeters::default();
    assert_eq!(
        store
            .read_indexed_blob_range_with_meters(
                &digest_of(body),
                body.len() as u64,
                0,
                13,
                &mut meters
            )
            .unwrap()
            .unwrap(),
        &[0x31; 13]
    );
    assert!(meters.index_hit && !meters.index_built);
    assert_eq!(meters.bytes_read, CHUNK_SIZE as u64);
    assert_eq!(meters.whole_sha256_bytes, 0);
    assert_eq!(meters.chunk_sha256_bytes, CHUNK_SIZE as u64);
    let cross = fs
        .read(Request::default(), inode, inode, CHUNK_SIZE as u64 - 3, 6)
        .await
        .unwrap();
    assert_eq!(cross.data.as_ref(), &[0x31, 0x31, 0x31, 0x72, 0x72, 0x72]);
    let tail = fs
        .read(
            Request::default(),
            inode,
            inode,
            2 * CHUNK_SIZE as u64,
            u32::MAX,
        )
        .await
        .unwrap();
    assert_eq!(tail.data.as_ref(), &[0xe4; 7]);
    drop(cross);
    drop(tail);
    let cloned = first.data.clone();
    let held = cloned.slice(17..31);
    assert_eq!(held.as_ptr(), first.data.as_ptr().wrapping_add(17));
    assert_no_range_wire(&http);
    drop(fs);
    drop(first);
    drop(cloned);
    assert!(reader.content_usage().output_bytes > 0);
    assert_eq!(held.as_ref(), &[0x31; 14]);
    drop(held);
    assert_eq!(reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn owned_cas_range_cold_tail_and_warm_covering_damage_never_fall_back() {
    let http = HttpFixture::start_canonical(owned_large_cas_fixture(1, false)).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
    let body = &http.fixture.blobs["/range000"];
    let path = cas_path(&store, body);
    let inode = root_file_inode(&fs, "range000").await;
    let baseline = reader.content_usage();
    let mut damaged = body.clone();
    *damaged.last_mut().unwrap() ^= 1;
    std::fs::write(&path, &damaged).unwrap();
    for _ in 0..2 {
        assert!(fs
            .read(Request::default(), inode, inode, 0, 1)
            .await
            .is_err());
        assert_eq!(reader.content_usage(), baseline);
    }
    std::fs::write(&path, body).unwrap();
    drop(
        fs.read(Request::default(), inode, inode, 0, 1)
            .await
            .unwrap(),
    );
    std::fs::write(&path, &damaged).unwrap();
    assert_eq!(
        fs.read(Request::default(), inode, inode, 0, 1)
            .await
            .unwrap()
            .data
            .as_ref(),
        &[0x31]
    );
    assert!(store
        .read_verified_blob_range(&digest_of(body), body.len() as u64, 0, 1)
        .is_err());
    damaged = body.clone();
    damaged[8192] ^= 1;
    std::fs::write(&path, &damaged).unwrap();
    assert!(fs
        .read(Request::default(), inode, inode, 0, 1)
        .await
        .is_err());
    assert_eq!(reader.content_usage(), baseline);
    assert_no_range_wire(&http);
}

#[cfg(unix)]
#[tokio::test]
async fn owned_cas_range_wrong_size_directory_and_symlink_are_terminal() {
    use std::os::unix::fs::symlink;
    let http = HttpFixture::start_canonical(owned_large_cas_fixture(1, false)).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
    let body = &http.fixture.blobs["/range000"];
    let path = cas_path(&store, body);
    let inode = root_file_inode(&fs, "range000").await;
    let baseline = reader.content_usage();
    std::fs::write(&path, &body[..body.len() - 1]).unwrap();
    assert!(fs
        .read(Request::default(), inode, inode, 0, 1)
        .await
        .is_err());
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(fs
        .read(Request::default(), inode, inode, 0, 1)
        .await
        .is_err());
    std::fs::remove_dir(&path).unwrap();
    let outside = root.path().join("outside");
    std::fs::write(&outside, body).unwrap();
    for target in [&outside, &root.path().join("missing")] {
        symlink(target, &path).unwrap();
        assert!(fs
            .read(Request::default(), inode, inode, 0, 1)
            .await
            .is_err());
        std::fs::remove_file(&path).unwrap();
        assert_eq!(reader.content_usage(), baseline);
    }
    assert_no_range_wire(&http);
}

#[tokio::test]
async fn owned_cas_range_missing_uses_canonical_chunks_bounded_cache_and_local_precedence() {
    let http = HttpFixture::start_canonical(owned_large_cas_fixture(17, false)).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, fs) = owned_small_cas_view(&http, &reader, root.path(), false).await;
    let inode = root_file_inode(&fs, "range000").await;
    let first = fs
        .read(Request::default(), inode, inode, 0, 13)
        .await
        .unwrap();
    assert_eq!(first.data.as_ref(), &[0x31; 13]);
    drop(
        fs.read(Request::default(), inode, inode, 0, 13)
            .await
            .unwrap(),
    );
    assert_eq!(
        http.fixture.map_requests.lock().unwrap().as_slice(),
        ["/range000"]
    );
    assert_eq!(
        http.fixture.leaf_requests.lock().unwrap().as_slice(),
        ["/range000"]
    );
    assert_eq!(
        http.fixture.chunk_requests.lock().unwrap().as_slice(),
        [("/range000".to_string(), 0u64)]
    );
    drop(
        fs.read(Request::default(), inode, inode, CHUNK_SIZE as u64 - 3, 6)
            .await
            .unwrap(),
    );
    assert_eq!(http.fixture.chunk_requests.lock().unwrap().len(), 2);
    for index in 1..17 {
        let next = root_file_inode(&fs, &format!("range{index:03}")).await;
        assert_ne!(inode, next);
        drop(fs.read(Request::default(), next, next, 0, 1).await.unwrap());
    }
    assert_eq!(http.fixture.map_requests.lock().unwrap().len(), 17);
    assert_eq!(http.fixture.leaf_requests.lock().unwrap().len(), 17);
    drop(
        fs.read(Request::default(), inode, inode, 0, 1)
            .await
            .unwrap(),
    );
    assert_eq!(
        http.fixture.map_requests.lock().unwrap().len(),
        18,
        "oldest of 16 range entries was evicted"
    );
    let chunk_calls = http.fixture.chunk_requests.lock().unwrap().len();
    let body = &http.fixture.blobs["/range000"];
    std::fs::write(cas_path(&store, body), body).unwrap();
    drop(
        fs.read(Request::default(), inode, inode, 2 * CHUNK_SIZE as u64, 7)
            .await
            .unwrap(),
    );
    assert_eq!(
        http.fixture.chunk_requests.lock().unwrap().len(),
        chunk_calls,
        "CAS precedes a cached network handle"
    );
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    let held = first.data.slice(3..7);
    drop(first);
    drop(fs);
    assert_eq!(held.as_ref(), &[0x31; 4]);
    assert!(reader.content_usage().output_bytes > 0);
    drop(held);
    assert_eq!(reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn owned_cas_range_bad_map_leaf_chunk_identity_end_and_eof_publish_nothing() {
    for mode in 1..=7 {
        let http = HttpFixture::start_canonical(owned_large_cas_fixture(1, false)).await;
        let reader = http.canonical_reader().await;
        let root = tempfile::tempdir().unwrap();
        let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), false).await;
        let inode = root_file_inode(&fs, "range000").await;
        let baseline = reader.content_usage();
        http.fixture.range_fault.store(mode, Ordering::SeqCst);
        for _ in 0..2 {
            assert!(
                fs.read(Request::default(), inode, inode, 0, CHUNK_SIZE + 1)
                    .await
                    .is_err(),
                "fault {mode}"
            );
            assert_eq!(
                reader.content_usage(),
                baseline,
                "failed range/table owner retained for fault {mode}"
            );
        }
        assert_eq!(
            http.fixture.map_requests.lock().unwrap().len(),
            2,
            "failure cannot publish a range entry"
        );
        assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
        http.fixture.range_fault.store(0, Ordering::SeqCst);
        assert_eq!(
            fs.read(Request::default(), inode, inode, 0, 13)
                .await
                .unwrap()
                .data
                .as_ref(),
            &[0x31; 13]
        );
    }
}

#[tokio::test]
async fn owned_cas_range_output_and_construction_admission_reject_before_fallback() {
    for fill_cas in [false, true] {
        for (output, construction, requested) in [
            (192 * 1024, 2 * 1024 * 1024, 256 * 1024),
            (2 * 1024 * 1024, 64 * 1024, 13),
        ] {
            let http = HttpFixture::start_canonical(owned_large_cas_fixture(1, false)).await;
            let reader = http
                .canonical_reader()
                .await
                .with_content_limits(ContentBudgetLimits::new(output, construction).unwrap());
            let root = tempfile::tempdir().unwrap();
            let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), fill_cas).await;
            let inode = root_file_inode(&fs, "range000").await;
            let baseline = reader.content_usage();
            assert!(fs
                .read(Request::default(), inode, inode, 0, requested)
                .await
                .is_err());
            assert_eq!(reader.content_usage(), baseline);
            assert!(fs
                .read(Request::default(), inode, inode, u64::MAX, u32::MAX)
                .await
                .unwrap()
                .data
                .is_empty());
            assert!(fs
                .read(Request::default(), inode, inode, 0, 0)
                .await
                .unwrap()
                .data
                .is_empty());
            assert_no_range_wire(&http);
        }
    }
}

#[tokio::test]
async fn owned_cas_range_current_lease_gates_local_warm_cached_wire_zero_and_eof() {
    for fill_cas in [true, false] {
        let mut fixture = owned_large_cas_fixture(1, false);
        fixture.lease_expiry = expiring_cas_fixture().lease_expiry;
        let http = HttpFixture::start_canonical(fixture).await;
        let reader = http.canonical_reader().await;
        let root = tempfile::tempdir().unwrap();
        let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), fill_cas).await;
        let inode = root_file_inode(&fs, "range000").await;
        let first = fs
            .read(Request::default(), inode, inode, 0, 13)
            .await
            .unwrap();
        let map_calls = http.fixture.map_requests.lock().unwrap().len();
        let chunk_calls = http.fixture.chunk_requests.lock().unwrap().len();
        let baseline = reader.content_usage();
        wait_for_cas_revocation(&http, &reader).await;
        for (offset, size) in [(0, 1), (0, 0), (u64::MAX, u32::MAX)] {
            assert_eq!(
                i32::from(
                    fs.read(Request::default(), inode, inode, offset, size)
                        .await
                        .unwrap_err()
                ),
                -libc::EACCES
            );
        }
        assert_eq!(reader.content_usage(), baseline);
        assert_eq!(http.fixture.map_requests.lock().unwrap().len(), map_calls);
        assert_eq!(
            http.fixture.chunk_requests.lock().unwrap().len(),
            chunk_calls
        );
        assert_eq!(first.data.as_ref(), &[0x31; 13]);
    }
}

#[tokio::test]
async fn owned_cas_range_lease_failure_during_chunk_rejects_entry_and_reply() {
    let mut fixture = owned_large_cas_fixture(1, false);
    fixture.lease_expiry = expiring_cas_fixture().lease_expiry;
    fixture.pause_chunks.store(true, Ordering::SeqCst);
    let http = HttpFixture::start_canonical(fixture).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), false).await;
    let inode = root_file_inode(&fs, "range000").await;
    let baseline = reader.content_usage();
    let reading = fs.clone();
    let task =
        tokio::spawn(async move { reading.read(Request::default(), inode, inode, 0, 13).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        http.fixture.chunk_started.notified(),
    )
    .await
    .unwrap();
    wait_for_cas_revocation(&http, &reader).await;
    http.fixture.chunk_release.notify_one();
    assert_eq!(i32::from(task.await.unwrap().unwrap_err()), -libc::EACCES);
    assert_eq!(reader.content_usage(), baseline);
    assert_eq!(http.fixture.map_requests.lock().unwrap().len(), 1);
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn owned_cas_range_cancelled_wire_read_releases_actual_owners() {
    let fixture = owned_large_cas_fixture(1, false);
    fixture.pause_chunks.store(true, Ordering::SeqCst);
    let http = HttpFixture::start_canonical(fixture).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), false).await;
    let inode = root_file_inode(&fs, "range000").await;
    let baseline = reader.content_usage();
    let reading = fs.clone();
    let task =
        tokio::spawn(async move { reading.read(Request::default(), inode, inode, 0, 13).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        http.fixture.chunk_started.notified(),
    )
    .await
    .unwrap();
    assert!(reader.content_usage().output_bytes > baseline.output_bytes);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(reader.content_usage(), baseline);
    http.fixture.pause_chunks.store(false, Ordering::SeqCst);
    http.fixture.chunk_release.notify_one();
    drop(
        fs.read(Request::default(), inode, inode, 0, 13)
            .await
            .unwrap(),
    );
    assert_eq!(http.fixture.map_requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn owned_cas_range_commit_switch_keeps_old_bytes_and_reuses_shared_cas_facts() {
    let http = HttpFixture::start_canonical(owned_large_cas_fixture(1, false)).await;
    let old_reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (old_store, old_fs) = owned_small_cas_view(&http, &old_reader, root.path(), true).await;
    let old_inode = root_file_inode(&old_fs, "range000").await;
    let old_reply = old_fs
        .read(Request::default(), old_inode, old_inode, 0, 32)
        .await
        .unwrap();
    let http = http
        .restart({
            let mut f = owned_large_cas_fixture(1, true);
            f.canonical = true;
            f
        })
        .await;
    let reader = http.canonical_reader().await;
    assert_ne!(old_reader.snapshot_id(), reader.snapshot_id());
    let (store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
    assert_eq!(old_store.content_dir(), store.content_dir());
    let stable = root_file_inode(&fs, "stable").await;
    drop(
        fs.read(Request::default(), stable, stable, 0, 32)
            .await
            .unwrap(),
    );
    let inode = root_file_inode(&fs, "range000").await;
    let reply = fs
        .read(Request::default(), inode, inode, 0, 32)
        .await
        .unwrap();
    assert_eq!(reply.data[17], 0x30);
    assert_eq!(old_reply.data[17], 0x31);
    assert_eq!(
        old_fs
            .read(Request::default(), old_inode, old_inode, 0, 32)
            .await
            .unwrap()
            .data,
        old_reply.data
    );
    let body = &http.fixture.blobs["/stable"];
    let mut meters = scorpiofs::snapshot::LocalCasRangeMeters::default();
    store
        .read_indexed_blob_range_with_meters(
            &digest_of(body),
            body.len() as u64,
            0,
            13,
            &mut meters,
        )
        .unwrap();
    assert!(meters.index_hit && !meters.index_built);
    assert_no_range_wire(&http);
}

#[tokio::test]
async fn owned_cas_range_unseeded_aliases_prove_selected_paths_without_unrelated_fetch() {
    let body = vec![0x6d; 2 * CHUNK_SIZE as usize + 7];
    let mut fixture = Fixture::default();
    let selected = fixture.leaf("/a", vec![file_entry("large", EntryKind::Regular, &body)]);
    fixture.routes.insert(("/alias".into(), vec![]), selected);
    fixture.root = fixture.leaf(
        "/",
        vec![
            Entry::dir(b"a", selected),
            Entry::dir(b"alias", selected),
            Entry::dir(b"unavailable", [0x93; 32]),
        ],
    );
    fixture.expect_file("a/large", "regular", &body);
    fixture.expect_file("alias/large", "regular", &body);
    let http = HttpFixture::start_canonical(fixture).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, _cache) = owned_fuse_page_cache(root.path(), &reader);
    std::fs::write(cas_path(&store, &body), &body).unwrap();
    let fs = Mst2Fuse::from_reader_lazy(reader, Some(store))
        .await
        .unwrap();
    let mut files = Vec::new();
    for name in ["a", "alias"] {
        let parent = root_file_inode(&fs, name).await;
        let inode = fs
            .lookup(Request::default(), parent, OsStr::new("large"))
            .await
            .unwrap()
            .attr
            .ino;
        files.push(inode);
        http.fixture.requests.lock().unwrap().clear();
        assert_eq!(
            fs.read(Request::default(), inode, inode, 0, 17)
                .await
                .unwrap()
                .data
                .as_ref(),
            &[0x6d; 17]
        );
        assert_eq!(
            http.fixture.requested_ids(),
            [id_string(&http.fixture.root), id_string(&selected)]
        );
    }
    assert_ne!(files[0], files[1]);
    assert_no_range_wire(&http);
}

fn expiring_cas_fixture() -> Fixture {
    let mut fixture = small_cas_fixture(b"alpha");
    let expiry = time::OffsetDateTime::now_utc() + time::Duration::seconds(6);
    fixture.lease_expiry = Some(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        expiry.year(),
        u8::from(expiry.month()),
        expiry.day(),
        expiry.hour(),
        expiry.minute(),
        expiry.second()
    ));
    fixture
}

async fn wait_for_cas_revocation(http: &HttpFixture, reader: &SnapshotReader) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while http.fixture.renewal_requests.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        reader.ensure_lease().await.unwrap_err().code,
        SnapshotErrorCode::ScopeForbidden
    );
}

async fn assert_owned_cached_metadata_failure(
    fixture: Fixture,
    terminal: SnapshotErrorCode,
    errno: i32,
) {
    let http = HttpFixture::start_canonical(fixture).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
    let alpha = root_file_inode(&fs, "alpha").await;
    let before = fs
        .getattr(Request::default(), alpha, None, 0)
        .await
        .unwrap();
    assert_eq!(before.attr.size, 8192);
    assert!(fs.getattr(Request::default(), 1, None, 0).await.is_ok());
    assert_eq!(
        i32::from(
            fs.lookup(Request::default(), 1, OsStr::new("missing"))
                .await
                .err()
                .unwrap()
        ),
        -libc::ENOENT
    );
    for offset in [0, i64::MAX] {
        let reply = fs.readdir(Request::default(), 1, 1, offset).await.unwrap();
        let mut entries = std::pin::pin!(reply.entries);
        while let Some(entry) = entries.next().await {
            entry.unwrap();
        }
        let reply = fs
            .readdirplus(Request::default(), 1, 1, offset as u64, 0)
            .await
            .unwrap();
        let mut entries = std::pin::pin!(reply.entries);
        while let Some(entry) = entries.next().await {
            entry.unwrap();
        }
    }
    assert!(fs.opendir(Request::default(), 1, 0).await.is_ok());
    assert!(fs.statfs(Request::default(), 1).await.is_ok());
    assert!(!Layer::is_opaque(fs.as_ref(), Request::default(), 1)
        .await
        .unwrap());
    assert_eq!(
        Layer::getattr_with_mapping(fs.as_ref(), alpha, None, false)
            .await
            .unwrap()
            .0
            .st_size,
        8192
    );
    assert!(matches!(
        fs.path_state("alpha").await.unwrap(),
        scorpiofs::snapshot::SnapshotPathState::Present(_)
    ));
    assert!(matches!(
        fs.path_state("").await.unwrap(),
        scorpiofs::snapshot::SnapshotPathState::Present(_)
    ));
    assert!(matches!(
        fs.path_state("missing").await.unwrap(),
        scorpiofs::snapshot::SnapshotPathState::AbsentProven
    ));
    assert!(!fs.directory_entries("").await.unwrap().is_empty());
    let link = root_file_inode(&fs, "link").await;
    let large = root_file_inode(&fs, "large").await;
    let first = fs
        .read(Request::default(), alpha, alpha, 0, 8192)
        .await
        .unwrap();
    drop(fs.readlink(Request::default(), link).await.unwrap());
    drop(
        fs.read(Request::default(), large, large, 17, 19)
            .await
            .unwrap(),
    );
    let metadata = http.fixture.requested_ids();
    let usage = reader.content_usage();
    tokio::time::timeout(Duration::from_secs(8), async {
        while http.fixture.renewal_requests.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(reader.ensure_lease().await.unwrap_err().code, terminal);
    })
    .await
    .unwrap();
    assert_eq!(reader.local_lease_status().unwrap_err().code, terminal);
    for inode in [1, alpha] {
        assert_eq!(
            i32::from(
                fs.getattr(Request::default(), inode, None, 0)
                    .await
                    .err()
                    .unwrap()
            ),
            -errno
        );
        assert_eq!(
            Layer::getattr_with_mapping(fs.as_ref(), inode, None, false)
                .await
                .err()
                .unwrap()
                .raw_os_error(),
            Some(errno)
        );
    }
    for name in ["alpha", "missing"] {
        assert_eq!(
            i32::from(
                fs.lookup(Request::default(), 1, OsStr::new(name))
                    .await
                    .err()
                    .unwrap()
            ),
            -errno
        );
    }
    for offset in [0, i64::MAX] {
        assert_eq!(
            i32::from(
                fs.readdir(Request::default(), 1, 1, offset)
                    .await
                    .err()
                    .unwrap()
            ),
            -errno
        );
        assert_eq!(
            i32::from(
                fs.readdirplus(Request::default(), 1, 1, offset as u64, 0)
                    .await
                    .err()
                    .unwrap()
            ),
            -errno
        );
    }
    assert_eq!(
        i32::from(fs.opendir(Request::default(), 1, 0).await.err().unwrap()),
        -errno
    );
    assert_eq!(
        i32::from(fs.statfs(Request::default(), 1).await.err().unwrap()),
        -errno
    );
    assert_eq!(
        i32::from(
            Layer::is_opaque(fs.as_ref(), Request::default(), 1)
                .await
                .err()
                .unwrap()
        ),
        -errno
    );
    for path in ["", "/", "alpha", "missing"] {
        assert_eq!(fs.path_state(path).await.unwrap_err().code, terminal);
    }
    for path in ["", "/", "missing"] {
        assert_eq!(fs.directory_entries(path).await.unwrap_err().code, terminal);
    }
    for inode in [alpha, large] {
        assert_eq!(
            i32::from(
                fs.open(Request::default(), inode, libc::O_RDONLY as u32)
                    .await
                    .unwrap_err()
            ),
            -errno
        );
        for (offset, size) in [(0, 1), (0, 0), (u64::MAX, u32::MAX)] {
            assert_eq!(
                i32::from(
                    fs.read(Request::default(), inode, inode, offset, size)
                        .await
                        .unwrap_err()
                ),
                -errno
            );
        }
    }
    assert_eq!(
        i32::from(fs.readlink(Request::default(), link).await.unwrap_err()),
        -errno
    );
    assert_eq!(
        first.data.as_ref(),
        &[0x6a; 8192],
        "delivered bytes retain their owner"
    );
    assert_eq!(before.attr.size, 8192, "delivered metadata is immutable");
    assert!(fs
        .fsync(Request::default(), alpha, alpha, false)
        .await
        .is_ok());
    assert!(fs
        .release(Request::default(), alpha, alpha, 0, 0, false)
        .await
        .is_ok());
    assert!(fs.releasedir(Request::default(), 1, 1, 0).await.is_ok());
    assert_eq!(http.fixture.requested_ids(), metadata);
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
    assert_eq!(reader.content_usage(), usage);
}

#[tokio::test]
async fn owned_cached_metadata_rejects_revocation_without_fetching_content_or_metadata() {
    assert_owned_cached_metadata_failure(
        expiring_cas_fixture(),
        SnapshotErrorCode::ScopeForbidden,
        libc::EACCES,
    )
    .await;
}

#[tokio::test]
async fn owned_cached_metadata_and_content_report_stale_after_real_renewal_410() {
    let mut fixture = expiring_cas_fixture();
    fixture.renewal_gone = true;
    assert_owned_cached_metadata_failure(fixture, SnapshotErrorCode::SnapshotGone, libc::ESTALE)
        .await;
}

#[tokio::test]
async fn owned_cached_metadata_and_content_report_stale_after_actual_deadline() {
    let mut fixture = expiring_cas_fixture();
    fixture.pause_renewal = true;
    assert_owned_cached_metadata_failure(fixture, SnapshotErrorCode::LeaseExpired, libc::ESTALE)
        .await;
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn owned_revoked_metadata_still_allows_paused_local_diff_but_cannot_load_another_directory() {
    use std::os::unix::fs::PermissionsExt;

    use scorpiofs::{
        snapshot::upper_diff::{scan_upper, DiffLimits, UpperChangeKind},
        util::mutation_fence::MutationFence,
    };

    let mut fixture = complete_fixture("a", b"fresh");
    fixture.lease_expiry = expiring_cas_fixture().lease_expiry;
    let http = HttpFixture::start_canonical(fixture).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), false).await;
    assert!(!fs.directory_entries("a").await.unwrap().is_empty());
    let metadata = http.fixture.requested_ids();
    let usage = reader.content_usage();
    wait_for_cas_revocation(&http, &reader).await;
    assert_eq!(
        fs.directory_entries("a").await.unwrap_err().code,
        SnapshotErrorCode::ScopeForbidden
    );
    let upper = tempfile::tempdir().unwrap();
    std::fs::set_permissions(upper.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let fence = MutationFence::new(1);
    let pause = fence.pause().await.unwrap();
    assert!(scan_upper(&fs, upper.path(), &pause, DiffLimits::default())
        .await
        .unwrap()
        .is_clean());

    std::fs::create_dir(upper.path().join("a")).unwrap();
    std::fs::set_permissions(
        upper.path().join("a"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    std::fs::write(upper.path().join("a/f.txt"), b"private edit").unwrap();
    std::fs::set_permissions(
        upper.path().join("a/f.txt"),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    let dirty = scan_upper(&fs, upper.path(), &pause, DiffLimits::default())
        .await
        .unwrap();
    assert!(dirty.changes.iter().any(|change| {
        change.rel_path == "a/f.txt" && change.kind == UpperChangeKind::Modified
    }));
    std::fs::write(upper.path().join(".wh..wh..opq"), b"").unwrap();
    let opaque = scan_upper(&fs, upper.path(), &pause, DiffLimits::default())
        .await
        .unwrap();
    assert!(opaque
        .changes
        .iter()
        .any(|change| { change.rel_path == "other" && change.kind == UpperChangeKind::Deleted }));
    std::fs::remove_file(upper.path().join(".wh..wh..opq")).unwrap();

    // Its directory identity is recorded by the root, but its contents have
    // not been loaded. Even verified local page hints require the live lease.
    std::fs::create_dir(upper.path().join("other")).unwrap();
    std::fs::set_permissions(
        upper.path().join("other"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    std::fs::write(upper.path().join("other/tail.txt"), b"local").unwrap();
    assert_eq!(
        scan_upper(&fs, upper.path(), &pause, DiffLimits::default())
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::ScopeForbidden
    );
    std::fs::remove_file(upper.path().join("other/tail.txt")).unwrap();
    std::fs::write(upper.path().join("other/.wh..wh..opq"), b"").unwrap();
    assert_eq!(
        scan_upper(&fs, upper.path(), &pause, DiffLimits::default())
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::ScopeForbidden
    );
    assert_eq!(http.fixture.requested_ids(), metadata);
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
    assert_eq!(reader.content_usage(), usage);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn owned_directory_revoked_during_wire_load_keeps_the_directory_unpublished() {
    use std::os::unix::fs::PermissionsExt;

    use scorpiofs::{
        snapshot::upper_diff::{scan_upper, DiffLimits},
        util::mutation_fence::MutationFence,
    };

    let mut fixture = complete_fixture("a", b"fresh");
    fixture.lease_expiry = expiring_cas_fixture().lease_expiry;
    let http = HttpFixture::start_canonical(fixture).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, _cache) = owned_fuse_page_cache(root.path(), &reader);
    let fs = Arc::new(
        Mst2Fuse::from_reader_lazy(reader.clone(), Some(store))
            .await
            .unwrap(),
    );
    let a = root_file_inode(&fs, "a").await;
    http.fixture.pause_metadata.store(true, Ordering::SeqCst);
    let read_fs = fs.clone();
    let reading = tokio::spawn(async move {
        read_fs
            .lookup(Request::default(), a, OsStr::new("f.txt"))
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(2),
        http.fixture.metadata_started.notified(),
    )
    .await
    .unwrap();
    wait_for_cas_revocation(&http, &reader).await;
    http.fixture.metadata_release.notify_one();
    assert_eq!(
        i32::from(reading.await.unwrap().err().unwrap()),
        -libc::EACCES
    );
    let metadata = http.fixture.requested_ids();
    let upper = tempfile::tempdir().unwrap();
    std::fs::set_permissions(upper.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::create_dir(upper.path().join("a")).unwrap();
    std::fs::set_permissions(
        upper.path().join("a"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    std::fs::write(upper.path().join("a/f.txt"), b"local").unwrap();
    let fence = MutationFence::new(1);
    let pause = fence.pause().await.unwrap();
    assert_eq!(
        scan_upper(&fs, upper.path(), &pause, DiffLimits::default())
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::ScopeForbidden,
        "a failed in-flight page must not turn into recorded metadata"
    );
    assert_eq!(http.fixture.requested_ids(), metadata);
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn owned_cas_current_lease_gates_open_cached_read_readlink_small_and_large_eof() {
    let http = HttpFixture::start_canonical(expiring_cas_fixture()).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
    let alpha = root_file_inode(&fs, "alpha").await;
    let link = root_file_inode(&fs, "link").await;
    let large = root_file_inode(&fs, "large").await;
    let first = fs
        .read(Request::default(), alpha, alpha, 0, 8192)
        .await
        .unwrap();
    drop(fs.readlink(Request::default(), link).await.unwrap());
    assert_eq!(
        fs.read(Request::default(), large, large, 37, 19)
            .await
            .unwrap()
            .data
            .as_ref(),
        &[0x8b; 19]
    );
    wait_for_cas_revocation(&http, &reader).await;
    assert!(fs
        .open(Request::default(), alpha, libc::O_RDONLY as u32)
        .await
        .is_err());
    for inode in [alpha, large] {
        for (offset, size) in [(0, 1), (0, 0), (u64::MAX, u32::MAX)] {
            assert_eq!(
                i32::from(
                    fs.read(Request::default(), inode, inode, offset, size)
                        .await
                        .unwrap_err()
                ),
                -libc::EACCES
            );
        }
    }
    assert_eq!(
        i32::from(fs.readlink(Request::default(), link).await.unwrap_err()),
        -libc::EACCES
    );
    assert_eq!(
        first.data.as_ref(),
        &[0x6a; 8192],
        "already delivered bytes retain their owner"
    );
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn owned_cas_lease_failure_during_a_body_rejects_before_cache_and_reply_publication() {
    let fixture = expiring_cas_fixture();
    fixture.pause_objects.store(true, Ordering::SeqCst);
    let http = HttpFixture::start_canonical(fixture).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), false).await;
    let baseline = reader.content_usage().output_bytes;
    let inode = root_file_inode(&fs, "alpha").await;
    let reading = fs.clone();
    let task =
        tokio::spawn(async move { reading.read(Request::default(), inode, inode, 0, 1).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        http.fixture.object_started.notified(),
    )
    .await
    .unwrap();
    wait_for_cas_revocation(&http, &reader).await;
    http.fixture.object_release.notify_one();
    assert_eq!(i32::from(task.await.unwrap().unwrap_err()), -libc::EACCES);
    assert_eq!(reader.content_usage().output_bytes, baseline);
    assert_eq!(reader.content_usage().construction_bytes, 0);
    assert_eq!(http.fixture.object_requests.lock().unwrap().len(), 1);
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn canonical_nonowned_store_keeps_its_existing_cas_and_reply_copy_path() {
    let http = HttpFixture::start_canonical(small_cas_fixture(b"alpha")).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(
        DurableStore::open_for_reader(root.path().join("view"), root.path().join("cas"), &reader)
            .unwrap(),
    );
    assert!(store.workspace_binding().unwrap().is_none());
    let body = &http.fixture.blobs["/alpha"];
    std::fs::write(cas_path(&store, body), body).unwrap();
    let fs = Mst2Fuse::from_reader_lazy(reader.clone(), Some(store))
        .await
        .unwrap();
    let first = small_read(&fs, "alpha", 0, 31).await;
    let second = small_read(&fs, "alpha", 0, 31).await;
    assert_eq!(first.data.as_ref(), second.data.as_ref());
    assert_ne!(first.data.as_ptr(), second.data.as_ptr());
    assert_eq!(reader.content_usage().output_bytes, 0);
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn owned_cas_unseeded_lazy_reads_prove_each_alias_without_walking_unrelated_paths() {
    let fixture = complete_fixture("a", b"target-one");
    let unrelated = fixture.routes[&("/other".into(), vec![])];
    *fixture.omit.lock().unwrap() = Some(unrelated);
    let http = HttpFixture::start_canonical(fixture).await;
    let reader = http.canonical_reader().await;
    let root = tempfile::tempdir().unwrap();
    let (store, _cache) = owned_fuse_page_cache(root.path(), &reader);
    let body = &http.fixture.blobs["/a/f.txt"];
    std::fs::write(cas_path(&store, body), body).unwrap();
    let fs = Mst2Fuse::from_reader_lazy(reader, Some(store.clone()))
        .await
        .unwrap();
    let a = root_file_inode(&fs, "a").await;
    let file = fs
        .lookup(Request::default(), a, OsStr::new("f.txt"))
        .await
        .unwrap()
        .attr
        .ino;
    http.fixture.requests.lock().unwrap().clear();
    let first = fs.read(Request::default(), file, file, 0, 6).await.unwrap();
    let expected = [
        id_string(&http.fixture.root),
        id_string(&http.fixture.routes[&("/a".into(), vec![])]),
    ];
    assert_eq!(http.fixture.requested_ids(), expected);
    std::fs::remove_file(cas_path(&store, body)).unwrap();
    let alias = root_file_inode(&fs, "alias").await;
    let alias_file = fs
        .lookup(Request::default(), alias, OsStr::new("f.txt"))
        .await
        .unwrap()
        .attr
        .ino;
    assert_ne!(file, alias_file);
    http.fixture.requests.lock().unwrap().clear();
    let second = fs
        .read(Request::default(), alias_file, alias_file, 1, 3)
        .await
        .unwrap();
    assert_eq!(second.data.as_ptr(), first.data.as_ptr().wrapping_add(1));
    assert_eq!(
        http.fixture.requested_ids(),
        expected,
        "the alias proves its own logical path before sharing bytes"
    );
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
}

async fn pin(cache: &ScopeCache, reader: &SnapshotReader, fixture: &Fixture) {
    let snapshot_id = reader.snapshot_id();
    let dir = cache.dir().join(snapshot_id.trim_start_matches("sha256:"));
    reader.authorized_context().bind_view_cache(&dir).unwrap();
    reader
        .authorized_context()
        .bind_scope_cache(&cache.dir().join("blobs"))
        .unwrap();
    let store =
        scorpiofs::snapshot::DurableStore::open_with_content(&dir, cache.dir().join("blobs"))
            .unwrap();
    let view = scorpiofs::snapshot::ViewMeta {
        snapshot_id: snapshot_id.into(),
        namespace_view_id: id_string(&[0x55; 32]),
        scope: "/project".into(),
        lease_id: "fixture-lease".into(),
    };
    store
        .hydrate_with(&view, &fixture.expected, |file| {
            let data = fixture.blobs[&format!("/{}", file.rel_path)].clone();
            async move { Ok(data) }
        })
        .await
        .unwrap();
    store.pin(&view).unwrap();
}

async fn hydrate_full(
    cache: &ScopeCache,
    reader: &SnapshotReader,
    closure: &scorpiofs::snapshot::ValidatedSnapshotClosure,
) -> scorpiofs::snapshot::HydrateReport {
    let dir = cache
        .dir()
        .join(reader.snapshot_id().trim_start_matches("sha256:"));
    let store =
        scorpiofs::snapshot::DurableStore::open_for_reader(&dir, cache.dir().join("blobs"), reader)
            .unwrap();
    let report = store
        .hydrate_snapshot_from_closure(reader, closure)
        .await
        .unwrap();
    assert_eq!(
        report.completion_kind,
        scorpiofs::snapshot::CompletionKind::FullSnapshot
    );
    assert!(store.is_snapshot_complete().unwrap());
    assert_eq!(
        store.snapshot_manifest().unwrap().directories(),
        closure.directories()
    );
    report
}

async fn hydrate_full_batches(
    store: &scorpiofs::snapshot::DurableStore,
    reader: &SnapshotReader,
    closure: &scorpiofs::snapshot::ValidatedSnapshotClosure,
) -> Result<scorpiofs::snapshot::HydrateReport, scorpiofs::snapshot::SnapshotError> {
    let source = reader.clone();
    store
        .hydrate_snapshot_content_batches(
            reader,
            closure,
            2,
            2,
            move |batch| {
                let source = source.clone();
                Box::pin(async move { source.read_content_batch(&batch).await })
            },
            |_| Box::pin(async { panic!("all HTTP batch fixture files are small") }),
        )
        .await
}

#[tokio::test]
async fn full_snapshot_http_batches_preserve_concurrency_alias_reuse_and_metadata() {
    let mut fixture = complete_fixture("a", b"target-one");
    fixture.frame_content.store(true, Ordering::SeqCst);
    // Both batches must be in flight together; a serial implementation fails
    // the bounded test instead of quietly losing the tuning knob.
    fixture.object_barrier = Some(Arc::new(tokio::sync::Barrier::new(2)));
    let http = HttpFixture::start(fixture).await;
    let reader = http.reader().await;
    assert!(reader.capabilities().features.objects);
    let temp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(temp.path()).unwrap();
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    let before_metadata = http.fixture.requested_ids();
    let store = scorpiofs::snapshot::DurableStore::open_for_reader(
        cache.dir().join("full-batches"),
        cache.dir().join("blobs"),
        &reader,
    )
    .unwrap();
    let report = tokio::time::timeout(
        Duration::from_secs(5),
        hydrate_full_batches(&store, &reader, &closure),
    )
    .await
    .unwrap()
    .unwrap();
    let unique: HashSet<_> = http
        .fixture
        .expected
        .iter()
        .map(|f| &f.content_digest)
        .collect();
    assert_eq!(report.fetched, unique.len() as u64);
    assert_eq!(report.total_files, http.fixture.expected.len() as u64);
    assert_eq!(
        report.bytes_total,
        http.fixture.expected.iter().map(|f| f.size).sum::<u64>()
    );
    assert_eq!(
        report.completion_kind,
        scorpiofs::snapshot::CompletionKind::FullSnapshot
    );
    let batches = http.fixture.object_requests.lock().unwrap().clone();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), unique.len());
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
    assert_eq!(http.fixture.requested_ids(), before_metadata);
    let local = store.snapshot_manifest().unwrap();
    assert_manifest(local.files(), &http.fixture.expected);
    assert_eq!(local.directories(), closure.directories());
    assert_eq!(local.pages(), closure.pages());
    let resumed = hydrate_full_batches(&store, &reader, &closure)
        .await
        .unwrap();
    assert_eq!(resumed.fetched, 0);
    assert_eq!(resumed.resumed, http.fixture.expected.len() as u64);
    assert_eq!(http.fixture.object_requests.lock().unwrap().len(), 2);
    assert!(store.is_snapshot_complete().unwrap());
}

#[tokio::test]
async fn full_snapshot_http_batch_failure_revokes_complete_and_can_resume() {
    let fixture = identical_directories_fixture();
    fixture.frame_content.store(true, Ordering::SeqCst);
    let http = HttpFixture::start(fixture).await;
    let reader = http.reader().await;
    let temp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(temp.path()).unwrap();
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    let store = scorpiofs::snapshot::DurableStore::open_for_reader(
        cache.dir().join("full-batch-failure"),
        cache.dir().join("blobs"),
        &reader,
    )
    .unwrap();
    hydrate_full_batches(&store, &reader, &closure)
        .await
        .unwrap();
    let digest = &http.fixture.expected[0].content_digest;
    std::fs::write(
        store
            .content_dir()
            .join(digest.trim_start_matches("sha256:")),
        b"corrupt",
    )
    .unwrap();
    http.fixture.omit_object.store(true, Ordering::SeqCst);
    assert_eq!(
        hydrate_full_batches(&store, &reader, &closure)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    assert!(!store.root().join("DURABLE_COMPLETE").exists());
    assert!(!store.is_complete().unwrap());
    assert_eq!(
        store.snapshot_manifest().unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    let retry = hydrate_full_batches(&store, &reader, &closure)
        .await
        .unwrap();
    assert_eq!(retry.fetched, 1, "retry deduplicates both logical paths");
    assert_eq!(retry.repaired, 2);
    assert!(store.is_snapshot_complete().unwrap());
    assert_manifest(
        store.snapshot_manifest().unwrap().files(),
        &http.fixture.expected,
    );
}

#[tokio::test]
async fn full_snapshot_http_raw_concurrent_fallback_preserves_metadata_and_resume() {
    assert_raw_concurrent_hydration(false).await;
}

#[tokio::test]
async fn full_snapshot_http_seeded_coordinator_preserves_zero_extra_metadata_rpc() {
    assert_raw_concurrent_hydration(true).await;
}

async fn assert_raw_concurrent_hydration(seeded: bool) {
    let http = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    assert!(!reader.capabilities().features.objects);
    let temp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(temp.path()).unwrap();
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    let before_metadata = http.fixture.requested_ids();
    let store = scorpiofs::snapshot::DurableStore::open_for_reader(
        cache.dir().join("full-raw"),
        cache.dir().join("blobs"),
        &reader,
    )
    .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let coordinator = if seeded {
        FetchCoordinator::with_verified_closure(reader.clone(), &closure, 3).unwrap()
    } else {
        FetchCoordinator::new(reader.clone(), 3)
    };
    let report = tokio::time::timeout(
        Duration::from_secs(5),
        store.hydrate_snapshot_concurrent_with_body(&reader, &closure, 3, move |file| {
            let barrier = barrier.clone();
            let coordinator = coordinator.clone();
            Box::pin(async move {
                barrier.wait().await;
                coordinator.fetch_owned(file, false).await
            })
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.fetched, 3);
    assert_eq!(
        report.completion_kind,
        scorpiofs::snapshot::CompletionKind::FullSnapshot
    );
    let after_metadata = http.fixture.requested_ids();
    assert_eq!(&after_metadata[..before_metadata.len()], &before_metadata);
    if seeded {
        // The already proved incremental closure supplies membership facts;
        // hydration must not issue a second metadata RPC.
        assert_eq!(after_metadata, before_metadata);
    } else {
        // Without a seed, OnceCell shares one fixed-root proof across callers.
        let mut membership_pages = after_metadata[before_metadata.len()..].to_vec();
        membership_pages.sort();
        let mut expected_pages = closure.pages().keys().cloned().collect::<Vec<_>>();
        expected_pages.sort();
        assert_eq!(membership_pages, expected_pages);
    }
    assert!(http.fixture.object_requests.lock().unwrap().is_empty());
    assert_eq!(store.snapshot_manifest().unwrap().pages(), closure.pages());
    let resumed = store
        .hydrate_snapshot_concurrent_with_body::<_, Vec<u8>>(&reader, &closure, 3, |_| {
            Box::pin(async { panic!("cache hits must not fetch") })
        })
        .await
        .unwrap();
    assert_eq!(resumed.fetched, 0);
    assert_eq!(resumed.resumed, 3);
    assert_eq!(http.fixture.requested_ids(), after_metadata);
}

#[tokio::test]
async fn full_snapshot_cache_preserves_empty_alias_radix_and_reports_full_proof() {
    let http = HttpFixture::start(complete_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let mut sync = IncrementalSync::new(&reader, &cache);
    let closure = sync.sync_snapshot().await.unwrap();
    assert_manifest(closure.files(), &http.fixture.expected);
    assert_eq!(
        closure
            .directories()
            .iter()
            .map(|d| d.rel_path.as_str())
            .collect::<Vec<_>>(),
        [
            "",
            "a",
            "a/nested",
            "alias",
            "alias/nested",
            "empty-a",
            "empty-b",
            "other",
            "wide"
        ]
    );
    assert_eq!(closure.pages().len(), http.fixture.pages.len());
    assert_eq!(sync.meters().closure_index_reads, 1);
    assert_eq!(sync.meters().closure_index_writes, 1);
    assert_eq!(sync.meters().pin_set_reads, 1);
    assert_eq!(sync.meters().acquisition_file_entries, 0);
    assert_eq!(
        sync.closure_meters().proof_page_hashes,
        closure.pages().len() as u64
    );
    assert_eq!(sync.closure_meters().proof_logical_directories, 9);
    assert_eq!(
        sync.closure_meters().proof_logical_files,
        http.fixture.expected.len() as u64
    );
    // The file-only API still constructs its manifest during acquisition.
    // The full API's proof must produce identical files and subtree records
    // without first building that temporary manifest.
    let file_tmp = tempfile::tempdir().unwrap();
    let file_cache = ScopeCache::open(file_tmp.path()).unwrap();
    let mut file_sync = IncrementalSync::new(&reader, &file_cache);
    let files = file_sync.sync().await.unwrap();
    assert_manifest(&files, &http.fixture.expected);
    assert_eq!(
        file_sync.meters().acquisition_file_entries,
        files.len() as u64
    );
    for root in closure.directories().iter().map(|d| &d.directory_root) {
        let mut proven = cache.record_for(root).unwrap();
        let mut acquired = file_cache.record_for(root).unwrap();
        proven.files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        acquired.files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        assert_eq!(proven, acquired);
    }
    let before = http.fixture.requested_ids();
    hydrate_full(&cache, &reader, &closure).await;
    assert_eq!(
        http.fixture.requested_ids(),
        before,
        "hydrate must not perform a second metadata walk"
    );
    http.fixture.requests.lock().unwrap().clear();
    let mut warm = IncrementalSync::new(&reader, &cache);
    let next = warm.sync_snapshot().await.unwrap();
    assert_eq!(next.pages(), closure.pages());
    assert_eq!(next.directories(), closure.directories());
    assert_eq!(next.files(), closure.files());
    assert!(http.fixture.requested_ids().is_empty());
    assert_eq!(warm.meters().fetched_pages, 0);
    assert_eq!(warm.meters().acquisition_file_entries, 0);
    assert_eq!(
        warm.meters().traversal_nodes,
        0,
        "acquisition reused the pinned root hint"
    );
    assert_eq!(warm.meters().closure_index_reads, 1);
    assert_eq!(warm.meters().closure_index_writes, 0);
    assert!(
        warm.closure_meters().collector_route_visits >= 9,
        "full proof still expands logical aliases"
    );
    assert_eq!(
        warm.meters().page_rehashes,
        closure.pages().len() as u64,
        "owned bytes are not re-read from disk for the proof collector"
    );
    assert_eq!(
        warm.closure_meters().proof_page_hashes,
        closure.pages().len() as u64
    );
}

#[tokio::test]
async fn full_snapshot_ignores_forged_files_and_repairs_truncated_or_extra_page_hints() {
    let http = HttpFixture::start(complete_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    hydrate_full(&cache, &reader, &closure).await;
    let foreign = Page::build(&[file_entry("foreign", EntryKind::Regular, b"foreign")]).unwrap();
    let foreign_id = id_string(&page_id(&foreign));
    cache.put_page(&foreign_id, &foreign).unwrap();
    let root_id = id_string(&http.fixture.root);
    let mut records: HashMap<String, scorpiofs::snapshot::ClosureRecord> =
        serde_json::from_slice(&std::fs::read(cache.dir().join("closures.json")).unwrap()).unwrap();
    for record in records.values_mut() {
        record.files = vec![SnapshotFile {
            rel_path: "forged.txt".into(),
            fs_kind: "regular".into(),
            size: 999,
            content_digest: id_string(&[0x88; 32]),
        }];
        record.total_entries = u64::MAX;
    }
    records.get_mut(&root_id).unwrap().page_ids = vec![root_id.clone(), foreign_id.clone()];
    std::fs::write(
        cache.dir().join("closures.json"),
        serde_json::to_vec(&records).unwrap(),
    )
    .unwrap();
    http.fixture.requests.lock().unwrap().clear();
    let mut repair = IncrementalSync::new(&reader, &cache);
    let fixed = repair.sync_snapshot().await.unwrap();
    assert_manifest(fixed.files(), &http.fixture.expected);
    assert_eq!(fixed.directories(), closure.directories());
    assert_eq!(fixed.pages(), closure.pages());
    assert!(!fixed.pages().contains_key(&foreign_id));
    assert!(
        http.fixture.requested_ids().is_empty(),
        "dependencies omitted by the hint still exist in the cache"
    );
    assert!(repair.closure_meters().repaired_records > 0);
    assert_eq!(repair.meters().acquisition_file_entries, 0);
    let root = cache.record_for(&root_id).unwrap();
    assert_manifest(&root.files, &http.fixture.expected);
    assert_eq!(
        root.total_entries,
        (fixed.files().len() + fixed.directories().len() - 1) as u64
    );
    assert_eq!(root.page_ids.len(), fixed.pages().len());
    assert!(!root.page_ids.contains(&foreign_id));
    let alias = cache
        .record_for(&id_string(&http.fixture.routes[&("/alias".into(), vec![])]))
        .unwrap();
    assert_eq!(
        alias
            .files
            .iter()
            .map(|f| f.rel_path.as_str())
            .collect::<Vec<_>>(),
        ["f.txt", "nested/deep.txt"]
    );
}

#[tokio::test]
async fn full_snapshot_commit_update_and_rename_keep_old_view_and_reuse_content() {
    let first = HttpFixture::start(complete_fixture("a", b"target-one")).await;
    let old = first.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let old_closure = IncrementalSync::new(&old, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    hydrate_full(&cache, &old, &old_closure).await;
    let next = first.restart(complete_fixture("a", b"target-two")).await;
    let reader = next.reader().await;
    let mut sync = IncrementalSync::new(&reader, &cache);
    let closure = sync.sync_snapshot().await.unwrap();
    assert_manifest(closure.files(), &next.fixture.expected);
    assert_eq!(
        sync.meters().fetched_pages,
        2,
        "only changed root and changed other directory"
    );
    assert!(sync.meters().reused_subtrees > 0);
    assert_eq!(hydrate_full(&cache, &reader, &closure).await.fetched, 1);
    let wrong_store = scorpiofs::snapshot::DurableStore::open_for_reader(
        cache.dir().join("wrong-view"),
        cache.dir().join("blobs"),
        &reader,
    )
    .unwrap();
    assert_eq!(
        wrong_store
            .hydrate_snapshot_from_closure(&reader, &old_closure)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::ScopeForbidden
    );
    assert_eq!(
        hydrate_full_batches(&wrong_store, &reader, &old_closure)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::ScopeForbidden
    );
    assert_eq!(
        wrong_store
            .hydrate_snapshot_concurrent_with_body::<_, Vec<u8>>(&reader, &old_closure, 4, |_| {
                Box::pin(async { panic!("mismatched fixed closure must be rejected before fetch") })
            })
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::ScopeForbidden
    );
    assert!(!wrong_store.is_complete().unwrap());
    let renamed = next.restart(complete_fixture("moved", b"target-two")).await;
    let latest = renamed.reader().await;
    let mut moved = IncrementalSync::new(&latest, &cache);
    let closure = moved.sync_snapshot().await.unwrap();
    assert_manifest(closure.files(), &renamed.fixture.expected);
    assert_eq!(moved.meters().fetched_pages, 1);
    assert_eq!(hydrate_full(&cache, &latest, &closure).await.fetched, 0);
    let old_store = scorpiofs::snapshot::DurableStore::open_with_content(
        cache
            .dir()
            .join(old.snapshot_id().trim_start_matches("sha256:")),
        cache.dir().join("blobs"),
    )
    .unwrap();
    let reopened = old_store.snapshot_manifest().unwrap();
    assert_eq!(reopened.files(), old_closure.files());
    assert_eq!(reopened.directories(), old_closure.directories());
}

#[tokio::test]
async fn full_snapshot_root_proof_failure_keeps_the_previous_index_transaction() {
    let first = HttpFixture::start(wide_fixture()).await;
    let old = first.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let closure = IncrementalSync::new(&old, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    hydrate_full(&cache, &old, &closure).await;
    let previous = std::fs::read(cache.dir().join("closures.json")).unwrap();
    let mut malformed = wide_fixture();
    let original_root = malformed.root;
    let (mut page, _) = Page::decode(&malformed.pages[&original_root]).unwrap();
    if let Page::Branch { children, .. } = &mut page {
        children[0].subtree_entries += 1;
    } else {
        panic!("wide root must be a branch")
    }
    malformed.root = malformed.page("/", vec![], page);
    malformed.pages.remove(&original_root);
    let next = first.restart(malformed).await;
    let reader = next.reader().await;
    let mut sync = IncrementalSync::new(&reader, &cache);
    assert_eq!(
        sync.sync_snapshot().await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(sync.meters().closure_index_reads, 1);
    assert_eq!(sync.meters().closure_index_writes, 0);
    assert_eq!(
        std::fs::read(cache.dir().join("closures.json")).unwrap(),
        previous
    );
    assert!(cache
        .record_for(reader.descriptor().metadata_root.as_str())
        .is_none());
    // The failed transaction releases its OS lock without poisoning retry.
    cache
        .put_record(
            &cache
                .record_for(old.descriptor().metadata_root.as_str())
                .unwrap(),
        )
        .unwrap();
}

#[tokio::test]
async fn full_snapshot_truncated_hint_cannot_hide_a_missing_wire_dependency() {
    let http = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    hydrate_full(&cache, &reader, &closure).await;
    let mut root = cache.record_for(&id_string(&http.fixture.root)).unwrap();
    root.page_ids = vec![root.root_page_id.clone()];
    cache.put_record(&root).unwrap();
    let previous = std::fs::read(cache.dir().join("closures.json")).unwrap();
    let missing = http.fixture.routes[&("/a/nested".into(), vec![])];
    std::fs::remove_file(cache.dir().join("pages").join(hex::encode(missing))).unwrap();
    *http.fixture.omit.lock().unwrap() = Some(missing);
    let mut sync = IncrementalSync::new(&reader, &cache);
    assert_eq!(
        sync.sync_snapshot().await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(sync.meters().closure_index_writes, 0);
    assert_eq!(
        std::fs::read(cache.dir().join("closures.json")).unwrap(),
        previous
    );
}

#[tokio::test]
async fn cold_nested_manifest_has_exact_paths_and_descendant_closure_pages() {
    let http = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let mut sync = IncrementalSync::new(&reader, &cache);
    assert_manifest(&sync.sync().await.unwrap(), &http.fixture.expected);
    assert_eq!(sync.meters().fetched_pages, 4);
    assert_eq!(sync.meters().traversal_nodes, 4);
    assert_eq!(sync.meters().closure_index_reads, 1);
    assert_eq!(
        sync.meters().closure_index_writes,
        1,
        "all four records publish together"
    );
    assert_eq!(sync.meters().pin_set_reads, 1);
    assert_eq!(
        sync.meters().page_rehashes,
        0,
        "cold pages are initially verified on receipt"
    );
    assert_eq!(
        sync.meters().closure_index_write_bytes,
        std::fs::metadata(cache.dir().join("closures.json"))
            .unwrap()
            .len()
    );
    let root = cache.record_for(&id_string(&http.fixture.root)).unwrap();
    assert_eq!(root.total_entries, 6, "three directories and three files");
    assert_eq!(root.page_ids.len(), http.fixture.pages.len());
    for id in http.fixture.pages.keys() {
        assert!(root.page_ids.contains(&id_string(id)));
    }
    assert_manifest(
        &reader.file_manifest_pages().await.unwrap(),
        &http.fixture.expected,
    );
}

#[tokio::test]
async fn mixed_reused_and_new_children_preserve_both_manifests() {
    let first = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let old = first.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    assert_manifest(
        &IncrementalSync::new(&old, &cache).sync().await.unwrap(),
        &first.fixture.expected,
    );
    pin(&cache, &old, &first.fixture).await;
    let next = first.restart(nested_fixture("a", b"target-two")).await;
    let reader = next.reader().await;
    assert_ne!(old.snapshot_id(), reader.snapshot_id());
    assert_eq!(
        old.authorized_context().cache_domain(),
        reader.authorized_context().cache_domain()
    );
    let mut sync = IncrementalSync::new(&reader, &cache);
    assert_manifest(&sync.sync().await.unwrap(), &next.fixture.expected);
    assert_eq!(sync.meters().reused_subtrees, 1);
    assert_eq!(
        sync.meters().reused_pages,
        2,
        "stable parent and nested child"
    );
    assert_eq!(sync.meters().fetched_pages, 2);
    assert_eq!(sync.meters().traversal_nodes, 2);
    assert_eq!(sync.meters().closure_index_reads, 1);
    assert_eq!(
        sync.meters().closure_index_writes,
        1,
        "pin transfer and new records share one commit"
    );
    assert_eq!(sync.meters().pin_set_reads, 1);
    assert_eq!(sync.meters().page_rehashes, 2);
    assert_eq!(sync.meters().unique_page_rehashes, 2);
    assert_eq!(
        cache
            .record_for(&id_string(&next.fixture.root))
            .unwrap()
            .page_ids
            .len(),
        4
    );
}

#[tokio::test]
async fn moved_subtree_rebases_once_and_keeps_its_descendant_pages() {
    let first = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let old = first.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    IncrementalSync::new(&old, &cache).sync().await.unwrap();
    pin(&cache, &old, &first.fixture).await;
    let next = first.restart(nested_fixture("moved", b"target-one")).await;
    let reader = next.reader().await;
    assert_ne!(old.snapshot_id(), reader.snapshot_id());
    assert_eq!(
        old.authorized_context().cache_domain(),
        reader.authorized_context().cache_domain()
    );
    let mut sync = IncrementalSync::new(&reader, &cache);
    assert_manifest(&sync.sync().await.unwrap(), &next.fixture.expected);
    assert_eq!(
        sync.meters().fetched_pages,
        1,
        "only the changed scope root"
    );
    assert_eq!(sync.meters().reused_subtrees, 2);
    assert_eq!(sync.meters().traversal_nodes, 1);
    assert_manifest(
        &reader.file_manifest_pages().await.unwrap(),
        &next.fixture.expected,
    );
}

#[tokio::test]
async fn identical_pages_from_another_deployment_cannot_reuse_a_scope_cache() {
    let first = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let old = first.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    assert_manifest(
        &IncrementalSync::new(&old, &cache).sync().await.unwrap(),
        &first.fixture.expected,
    );
    pin(&cache, &old, &first.fixture).await;
    let previous_records = std::fs::read(cache.dir().join("closures.json")).unwrap();

    // The first listener stays bound: this second port represents a genuinely
    // different deployment even though it advertises identical snapshot bytes.
    let other = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let reader = other.reader().await;
    assert_ne!(first.base, other.base);
    assert_eq!(old.snapshot_id(), reader.snapshot_id());
    assert_ne!(
        old.authorized_context().cache_domain(),
        reader.authorized_context().cache_domain()
    );
    let mut sync = IncrementalSync::new(&reader, &cache);
    assert_eq!(
        sync.sync().await.unwrap_err().code,
        SnapshotErrorCode::ScopeForbidden
    );
    assert_eq!(sync.meters().reused_subtrees, 0);
    assert_eq!(sync.meters().reused_pages, 0);
    assert_eq!(sync.meters().fetched_pages, 0);
    assert_eq!(sync.meters().traversal_nodes, 0);
    assert!(other.fixture.requested_ids().is_empty());
    assert_eq!(
        std::fs::read(cache.dir().join("closures.json")).unwrap(),
        previous_records
    );
}

#[tokio::test]
async fn identical_page_ids_expand_under_every_logical_directory() {
    let http = HttpFixture::start(identical_directories_fixture()).await;
    let reader = http.reader().await;
    assert_manifest(
        &reader.file_manifest_pages().await.unwrap(),
        &http.fixture.expected,
    );
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let mut sync = IncrementalSync::new(&reader, &cache);
    assert_manifest(&sync.sync().await.unwrap(), &http.fixture.expected);
    assert_eq!(sync.meters().fetched_pages, 2, "two physical pages");
    assert_eq!(
        sync.meters().traversal_nodes,
        3,
        "three logical page visits"
    );
    pin(&cache, &reader, &http.fixture).await;
    let mut warm = IncrementalSync::new(&reader, &cache);
    assert_manifest(&warm.sync().await.unwrap(), &http.fixture.expected);
    assert_eq!(warm.meters().fetched_pages, 0);
    assert_eq!(warm.meters().traversal_nodes, 0);
    assert_eq!(warm.meters().closure_index_reads, 1);
    assert_eq!(
        warm.meters().closure_index_writes,
        0,
        "same pinned view needs no index mutation"
    );
    assert_eq!(warm.meters().pin_set_reads, 1);
    assert_eq!(warm.meters().page_rehashes, 2);
    assert_eq!(warm.meters().unique_page_rehashes, 2);
}

#[tokio::test]
async fn missing_descendant_page_refetches_only_that_page_without_trusting_root_record() {
    let http = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    IncrementalSync::new(&reader, &cache).sync().await.unwrap();
    pin(&cache, &reader, &http.fixture).await;
    let missing = http.fixture.routes[&("/a/nested".into(), vec![])];
    std::fs::remove_file(cache.dir().join("pages").join(hex::encode(missing))).unwrap();
    http.fixture.requests.lock().unwrap().clear();
    let mut repair = IncrementalSync::new(&reader, &cache);
    assert_manifest(&repair.sync().await.unwrap(), &http.fixture.expected);
    assert_eq!(repair.meters().fetched_pages, 1);
    assert_eq!(http.fixture.requested_ids(), vec![id_string(&missing)]);
    assert_eq!(
        repair.meters().traversal_nodes,
        3,
        "root, a, and its missing child; other is reused"
    );
    assert_eq!(
        cache
            .record_for(&id_string(&http.fixture.root))
            .unwrap()
            .page_ids
            .len(),
        4
    );
}

#[tokio::test]
async fn missing_server_page_is_an_error_and_never_writes_a_closure_record() {
    let http = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let missing = http.fixture.routes[&("/a/nested".into(), vec![])];
    *http.fixture.omit.lock().unwrap() = Some(missing);
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let mut sync = IncrementalSync::new(&reader, &cache);
    let err = sync.sync().await.unwrap_err();
    assert_eq!(err.code, SnapshotErrorCode::DigestMismatch);
    assert!(cache.record_for(&id_string(&http.fixture.root)).is_none());
    assert!(
        !cache.dir().join("closures.json").exists(),
        "even completed sibling records stay uncommitted on failure"
    );
    assert_eq!(sync.meters().closure_index_reads, 1);
    assert_eq!(sync.meters().closure_index_writes, 0);
    assert_eq!(
        reader.file_manifest_pages().await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
}

#[tokio::test]
async fn wide_branch_manifest_and_lazy_fuse_expose_every_file() {
    let http = HttpFixture::start(wide_fixture()).await;
    let reader = http.reader().await;
    assert_manifest(
        &reader.file_manifest_pages().await.unwrap(),
        &http.fixture.expected,
    );
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    assert_manifest(
        &IncrementalSync::new(&reader, &cache).sync().await.unwrap(),
        &http.fixture.expected,
    );
    let fs = Mst2Fuse::from_reader_lazy(reader, None).await.unwrap();
    let reply = fs
        .readdirplus(Request::default(), 1, 1, 0, 1 << 20)
        .await
        .unwrap();
    let mut stream = std::pin::pin!(reply.entries);
    let mut names = HashSet::new();
    while let Some(entry) = stream.next().await {
        let entry = entry.unwrap();
        names.insert(entry.name.to_string_lossy().to_string());
    }
    for expected in &http.fixture.expected {
        assert!(
            names.contains(&expected.rel_path),
            "missing readdirplus name {}",
            expected.rel_path
        );
        let entry = fs
            .lookup(Request::default(), 1, OsStr::new(&expected.rel_path))
            .await
            .unwrap();
        assert_eq!(entry.attr.size, expected.size);
    }
    assert_eq!(
        names.len(),
        http.fixture.expected.len() + 2,
        "dot entries plus all files"
    );
}

#[tokio::test]
async fn unrelated_route_page_is_rejected_even_with_a_valid_digest() {
    let mut fixture = wide_fixture();
    let extra = fixture.leaf(
        "/unrequested",
        vec![file_entry("secret", EntryKind::Regular, b"neighbor")],
    );
    *fixture.extra_route_page.lock().unwrap() = Some(extra);
    let http = HttpFixture::start(fixture).await;
    let reader = http.reader().await;
    assert_eq!(
        reader.file_manifest_pages().await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    assert_eq!(
        IncrementalSync::new(&reader, &cache)
            .sync()
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    assert!(cache.record_for(&id_string(&http.fixture.root)).is_none());
}

#[tokio::test]
async fn malformed_closure_page_id_cannot_read_write_or_remove_an_outside_file() {
    let http = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path().join("cache")).unwrap();
    IncrementalSync::new(&reader, &cache).sync().await.unwrap();
    pin(&cache, &reader, &http.fixture).await;
    let sentinel = tmp.path().join("sentinel");
    std::fs::write(&sentinel, b"must remain untouched").unwrap();
    let invalid = "sha256:../../sentinel";
    let mut record = cache.record_for(&id_string(&http.fixture.root)).unwrap();
    record.page_ids.insert(0, invalid.into());
    // Use the current policy and a live pin so the corrupted record reaches
    // the id verifier; policy mismatch must not mask this regression.
    cache.put_record(&record).unwrap();
    assert!(cache.read_page_verified(invalid).unwrap().is_none());
    assert_eq!(
        cache.put_page(invalid, b"overwrite").unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    let malformed_utf8 = "é".repeat(32); // 64 bytes, not an ASCII digest.
    assert!(cache.read_page_verified(&malformed_utf8).unwrap().is_none());
    assert_manifest(
        &IncrementalSync::new(&reader, &cache).sync().await.unwrap(),
        &http.fixture.expected,
    );
    assert_eq!(std::fs::read(&sentinel).unwrap(), b"must remain untouched");
    assert!(!cache
        .record_for(&id_string(&http.fixture.root))
        .unwrap()
        .page_ids
        .contains(&invalid.to_string()));
}

#[tokio::test]
async fn cancelling_first_fetch_preserves_another_waiter_and_single_source_request() {
    let fixture = identical_directories_fixture();
    fixture.pause_blob.store(true, Ordering::SeqCst);
    let http = HttpFixture::start(fixture).await;
    let reader = http.reader().await;
    let coordinator = FetchCoordinator::new(reader, 2);
    let file = http.fixture.expected[0].clone();
    let leader = tokio::spawn({
        let c = coordinator.clone();
        let f = file.clone();
        async move { c.fetch_owned(f, false).await }
    });
    tokio::time::timeout(Duration::from_secs(3), http.fixture.blob_started.notified())
        .await
        .unwrap();
    // Register the other waiter before cancelling the first caller. If the
    // last live caller cancels, its flight must end rather than be reused.
    let mut waiter = Box::pin(coordinator.fetch_owned(file, false));
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    leader.abort();
    assert!(leader.await.unwrap_err().is_cancelled());
    http.fixture.blob_release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.as_slice(), b"same");
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_fetch_releases_singleflight_key_for_retry() {
    let fixture = identical_directories_fixture();
    fixture.fail_blob_once.store(true, Ordering::SeqCst);
    let http = HttpFixture::start(fixture).await;
    let coordinator = FetchCoordinator::new(http.reader().await, 2);
    let file = http.fixture.expected[0].clone();
    assert_eq!(
        coordinator
            .fetch_owned(file.clone(), false)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::ScopeForbidden
    );
    let bytes = tokio::time::timeout(Duration::from_secs(3), coordinator.fetch_owned(file, false))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes.as_slice(), b"same");
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 2);
}

// Runtime isolation alone does not isolate the process-wide CAS running gate.
// Run each pool-queue case in its own process so its two reads can both reach
// spawn_blocking without holding up unrelated lease tests in this binary.
fn run_cas_pool_test_in_isolated_process(test_name: &str) -> bool {
    const MARKER: &str = "SCORPIO_CAS_POOL_TEST";
    if std::env::var(MARKER).ok().as_deref() == Some(test_name) {
        return false;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture"])
        .env(MARKER, test_name)
        .status()
        .unwrap();
    assert!(
        status.success(),
        "isolated CAS pool test failed: {test_name}"
    );
    true
}

// This occupies the isolated runtime's only blocking thread. The controller
// releases it even if an assertion unwinds, so no test strands a worker.
struct BlockingPoolHold {
    gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
    worker: Option<tokio::task::JoinHandle<()>>,
}

impl BlockingPoolHold {
    async fn new() -> Self {
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let blocking = gate.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            let mut released = blocking.0.lock().unwrap();
            while !*released {
                released = blocking.1.wait(released).unwrap();
            }
        });
        let hold = Self {
            gate,
            worker: Some(worker),
        };
        tokio::time::timeout(Duration::from_secs(3), ready)
            .await
            .unwrap()
            .unwrap();
        hold
    }

    fn release(&self) {
        *self.gate.0.lock().unwrap() = true;
        self.gate.1.notify_all();
    }

    async fn finish(&mut self) {
        self.release();
        self.worker.take().unwrap().await.unwrap();
    }
}

impl Drop for BlockingPoolHold {
    fn drop(&mut self) {
        self.release();
    }
}

#[test]
fn owned_cas_pool_queue_preserves_metadata_responsiveness_and_rejects_real_lease_failures() {
    if run_cas_pool_test_in_isolated_process(
        "owned_cas_pool_queue_preserves_metadata_responsiveness_and_rejects_real_lease_failures",
    ) {
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        for (terminal, errno) in [
            (SnapshotErrorCode::ScopeForbidden, libc::EACCES),
            (SnapshotErrorCode::SnapshotGone, libc::ESTALE),
            (SnapshotErrorCode::LeaseExpired, libc::ESTALE),
        ] {
            let mut fixture = expiring_cas_fixture();
            fixture.renewal_gone = terminal == SnapshotErrorCode::SnapshotGone;
            fixture.pause_renewal = terminal == SnapshotErrorCode::LeaseExpired;
            let http = HttpFixture::start_canonical(fixture).await;
            let reader = http.canonical_reader().await;
            let root = tempfile::tempdir().unwrap();
            let (_store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
            let alpha = root_file_inode(&fs, "alpha").await;
            let large = root_file_inode(&fs, "large").await;
            let metadata = http.fixture.requested_ids();
            let baseline = reader.content_usage();
            let mut hold = BlockingPoolHold::new().await;
            let mut small_read = Box::pin(fs.read(Request::default(), alpha, alpha, 0, 19));
            let mut large_read = Box::pin(fs.read(Request::default(), large, large, 17, 19));
            assert!(futures::poll!(small_read.as_mut()).is_pending());
            let small_pending = reader.content_usage();
            assert!(small_pending.output_bytes > baseline.output_bytes);
            assert!(futures::poll!(large_read.as_mut()).is_pending());
            let pending = reader.content_usage();
            assert!(pending.output_bytes > small_pending.output_bytes);
            assert_eq!(pending.construction_bytes, baseline.construction_bytes);
            let attr = tokio::time::timeout(
                Duration::from_secs(1),
                fs.getattr(Request::default(), alpha, None, 0),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(attr.attr.size, 8192);
            // Actual HTTP renewal and the actual std::Instant grant clock
            // keep progressing while both CAS closures wait in the pool.
            tokio::time::timeout(Duration::from_secs(8), async {
                while http.fixture.renewal_requests.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(reader.ensure_lease().await.unwrap_err().code, terminal);
            })
            .await
            .unwrap();
            assert_eq!(reader.content_usage(), pending);
            hold.finish().await;
            assert_eq!(i32::from(small_read.await.unwrap_err()), -errno);
            assert_eq!(i32::from(large_read.await.unwrap_err()), -errno);
            assert_eq!(reader.content_usage(), baseline);
            assert_eq!(http.fixture.requested_ids(), metadata);
            assert!(http.fixture.object_requests.lock().unwrap().is_empty());
            assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
        }
    });
}

#[test]
fn owned_cas_cancelled_pool_queue_keeps_reply_credits_until_cleanup_and_can_retry() {
    if run_cas_pool_test_in_isolated_process(
        "owned_cas_cancelled_pool_queue_keeps_reply_credits_until_cleanup_and_can_retry",
    ) {
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let http = HttpFixture::start_canonical(small_cas_fixture(b"alpha")).await;
        let reader = http.canonical_reader().await;
        let root = tempfile::tempdir().unwrap();
        let (store, fs) = owned_small_cas_view(&http, &reader, root.path(), true).await;
        let alpha = root_file_inode(&fs, "alpha").await;
        let large = root_file_inode(&fs, "large").await;
        let metadata = http.fixture.requested_ids();
        let baseline = reader.content_usage();
        let mut hold = BlockingPoolHold::new().await;
        let mut small_read = Box::pin(fs.read(Request::default(), alpha, alpha, 0, 19));
        let mut large_read = Box::pin(fs.read(Request::default(), large, large, 17, 19));
        assert!(futures::poll!(small_read.as_mut()).is_pending());
        let small_pending = reader.content_usage();
        assert!(small_pending.output_bytes > baseline.output_bytes);
        assert!(futures::poll!(large_read.as_mut()).is_pending());
        let pending = reader.content_usage();
        assert!(pending.output_bytes > small_pending.output_bytes);
        assert_eq!(pending.construction_bytes, baseline.construction_bytes);
        drop(small_read);
        drop(large_read);
        assert_eq!(reader.content_usage(), pending);
        std::fs::remove_file(cas_path(&store, &http.fixture.blobs["/alpha"])).unwrap();
        hold.finish().await;
        tokio::time::timeout(Duration::from_secs(3), async {
            while reader.content_usage() != baseline {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(http.fixture.object_requests.lock().unwrap().is_empty());
        assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
        assert_eq!(http.fixture.requested_ids(), metadata);
        // Neither cancelled closure published a payload. A live retry with
        // the CAS object now absent must use the verified owned wire path.
        let reply = fs
            .read(Request::default(), alpha, alpha, 0, 19)
            .await
            .unwrap();
        assert_eq!(reply.data.as_ref(), &[0x6a; 19]);
        assert_eq!(http.fixture.object_requests.lock().unwrap().len(), 1);
        assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
        assert_eq!(http.fixture.requested_ids(), metadata);
    });
}
