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
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures::StreamExt;
use mst2_codec::{
    descriptor::ServingDescriptor,
    metapage::{page_id, BranchChild, Entry, EntryKind, Page},
    treeframe::{EndPayload, MetaPayload, ObjectPayload},
};
use scorpiofs::snapshot::{
    durable::digest_of, frames::parse_digest, fuse::Mst2Fuse, FetchCoordinator, IncrementalSync,
    Mst2Client, ScopeCache, SnapshotErrorCode, SnapshotFile, SnapshotReader,
};
use serde_json::{json, Value};
use tokio::sync::Notify;

const INSTANCE_ID: &str = "11111111-2222-4333-8444-555555555555";
const NAMESPACE_VIEW_ID: [u8; 32] = [0x22; 32];

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
    frame_content: AtomicBool,
    object_requests: Mutex<Vec<Vec<String>>>,
    omit_object: AtomicBool,
    object_barrier: Option<Arc<tokio::sync::Barrier>>,
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
    Json(json!({
        "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true, "metadata_pages": true,
            "objects": f.frame_content.load(Ordering::SeqCst),
            "chunk_reads": f.frame_content.load(Ordering::SeqCst)}
    }))
}

async fn resolve(State(f): State<Arc<Fixture>>) -> Json<Value> {
    Json(json!({
        "descriptor": {
            "schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE_ID,
            "namespace_view_id": id_string(&NAMESPACE_VIEW_ID), "scope": "/project",
            "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
            "metadata_root": id_string(&f.root), "snapshot_id": f.snapshot_id()
        },
        "lease_id": "fixture-lease", "lease_expires_at": "2099-01-01T00:00:00Z",
        "publication_sequence": "1", "authorization_epoch": "1"
    }))
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
    wire.extend(
        EndPayload {
            request_item_count: items.len() as u32,
            unique_unit_count: objects.len() as u32,
            logical_bytes,
            request_body_sha256: parse_digest(&digest_of(&body)).unwrap(),
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

struct HttpFixture {
    fixture: Arc<Fixture>,
    base: String,
    listener: StdTcpListener,
    task: tokio::task::JoinHandle<()>,
}

impl HttpFixture {
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
            .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
            .route("/api/v2/snapshots/{sid}/blob", get(blob))
            .route("/api/v2/snapshots/{sid}/objects", post(objects))
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
    let client = reader.client().clone();
    let sid = reader.snapshot_id().to_string();
    store
        .hydrate_snapshot_batches(
            reader,
            closure,
            2,
            2,
            move |batch| {
                let client = client.clone();
                let sid = sid.clone();
                Box::pin(async move {
                    let items: Vec<_> = batch
                        .iter()
                        .map(|f| (format!("/{}", f.rel_path), f.content_digest.clone()))
                        .collect();
                    let got = client.objects(&sid, &items, None).await?;
                    Ok(got
                        .into_iter()
                        .map(|(id, bytes)| (id_string(&id), Arc::new(bytes)))
                        .collect())
                })
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
        store.hydrate_snapshot_concurrent(&reader, &closure, 3, move |file| {
            let barrier = barrier.clone();
            let coordinator = coordinator.clone();
            Box::pin(async move {
                barrier.wait().await;
                coordinator.fetch(file, false).await
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
        .hydrate_snapshot_concurrent(&reader, &closure, 3, |_| {
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
    assert_eq!(
        sync.closure_meters().proof_page_hashes,
        closure.pages().len() as u64
    );
    assert_eq!(sync.closure_meters().proof_logical_directories, 9);
    assert_eq!(
        sync.closure_meters().proof_logical_files,
        http.fixture.expected.len() as u64
    );
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
            .hydrate_snapshot_concurrent(&reader, &old_closure, 4, |_| Box::pin(async {
                panic!("mismatched fixed closure must be rejected before fetch")
            }))
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
