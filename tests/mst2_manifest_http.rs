//! Fixed HTTP fixtures with hand-written expected logical paths. The oracle
//! never uses either production manifest walker to construct its answers.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsStr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use asyncfuse::raw::prelude::{Filesystem, Request};
use axum::{
    body::Bytes,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures::StreamExt;
use mst2_codec::{
    metapage::{page_id, BranchChild, Entry, EntryKind, Page},
    treeframe::{EndPayload, MetaPayload},
};
use scorpiofs::snapshot::{
    durable::digest_of, frames::parse_digest, fuse::Mst2Fuse, FetchCoordinator, IncrementalSync,
    Mst2Client, ScopeCache, SnapshotErrorCode, SnapshotFile, SnapshotReader,
};
use serde_json::{json, Value};
use tokio::sync::Notify;

#[derive(Default)]
struct Fixture {
    root: [u8; 32],
    pages: HashMap<[u8; 32], Vec<u8>>,
    routes: HashMap<(String, Vec<u8>), [u8; 32]>,
    expected: Vec<SnapshotFile>,
    blobs: HashMap<String, Vec<u8>>,
    requests: Mutex<Vec<Vec<String>>>,
    omit: Mutex<Option<[u8; 32]>>,
    extra_route_page: Mutex<Option<[u8; 32]>>,
    blob_requests: AtomicUsize,
    pause_blob: AtomicBool,
    fail_blob_once: AtomicBool,
    blob_started: Notify,
    blob_release: Notify,
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
        id_string(&self.root)
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

async fn capabilities() -> Json<Value> {
    Json(json!({
        "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true, "metadata_pages": true}
    }))
}

async fn resolve(State(f): State<Arc<Fixture>>) -> Json<Value> {
    Json(json!({
        "descriptor": {
            "schema_version": 2, "metadata_codec": 1, "instance_id": "fixture",
            "namespace_view_id": "fixed-fixture-view", "scope": "/project",
            "materialization_policy": 1, "fs_semantics": 1, "access_projection": 1,
            "metadata_root": id_string(&f.root), "snapshot_id": f.snapshot_id()
        },
        "lease_id": "fixture-lease", "lease_expires_at": "2099-01-01T00:00:00Z",
        "publication_sequence": "1"
    }))
}

async fn metadata(State(f): State<Arc<Fixture>>, body: Bytes) -> Response {
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
    ([("content-type", "application/octet-stream")], wire).into_response()
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

struct HttpFixture {
    fixture: Arc<Fixture>,
    base: String,
    task: tokio::task::JoinHandle<()>,
}

impl HttpFixture {
    async fn start(fixture: Fixture) -> Self {
        let fixture = Arc::new(fixture);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(capabilities))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
            .route("/api/v2/snapshots/{sid}/blob", get(blob))
            .with_state(fixture.clone());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            fixture,
            base,
            task,
        }
    }

    async fn reader(&self) -> SnapshotReader {
        SnapshotReader::resolve(Mst2Client::new(&self.base), "/project", 600)
            .await
            .unwrap()
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

fn pin(cache: &ScopeCache, snapshot_id: &str) {
    let dir = cache.dir().join(snapshot_id.trim_start_matches("sha256:"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("pin.json"), serde_json::to_vec(&json!({
        "snapshot_id": snapshot_id, "scope": "/project", "lease_id": "fixture-lease", "pinned_at_unix": 1
    })).unwrap()).unwrap();
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
    pin(&cache, old.snapshot_id());
    let next = HttpFixture::start(nested_fixture("a", b"target-two")).await;
    let reader = next.reader().await;
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
    pin(&cache, old.snapshot_id());
    let next = HttpFixture::start(nested_fixture("moved", b"target-one")).await;
    let reader = next.reader().await;
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
    pin(&cache, reader.snapshot_id());
    let mut warm = IncrementalSync::new(&reader, &cache);
    assert_manifest(&warm.sync().await.unwrap(), &http.fixture.expected);
    assert_eq!(warm.meters().fetched_pages, 0);
    assert_eq!(warm.meters().traversal_nodes, 0);
}

#[tokio::test]
async fn missing_descendant_page_refetches_only_that_page_without_trusting_root_record() {
    let http = HttpFixture::start(nested_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    IncrementalSync::new(&reader, &cache).sync().await.unwrap();
    pin(&cache, reader.snapshot_id());
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
            .record_for(reader.snapshot_id())
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
    let err = IncrementalSync::new(&reader, &cache)
        .sync()
        .await
        .unwrap_err();
    assert_eq!(err.code, SnapshotErrorCode::DigestMismatch);
    assert!(cache.record_for(&id_string(&http.fixture.root)).is_none());
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
    pin(&cache, reader.snapshot_id());
    let sentinel = tmp.path().join("sentinel");
    std::fs::write(&sentinel, b"must remain untouched").unwrap();
    let invalid = "sha256:../../sentinel";
    let mut record = cache.record_for(reader.snapshot_id()).unwrap();
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
        .record_for(reader.snapshot_id())
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
        async move { c.fetch(f, false).await }
    });
    tokio::time::timeout(Duration::from_secs(3), http.fixture.blob_started.notified())
        .await
        .unwrap();
    leader.abort();
    let _ = leader.await;
    // Poll until registered as a waiter before releasing the source. This
    // makes the cancellation regression independent of task scheduling.
    let mut waiter = Box::pin(coordinator.fetch(file, false));
    assert!(futures::poll!(waiter.as_mut()).is_pending());
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
            .fetch(file.clone(), false)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::ScopeForbidden
    );
    let bytes = tokio::time::timeout(Duration::from_secs(3), coordinator.fetch(file, false))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes.as_slice(), b"same");
    assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 2);
}
