//! Count admission and cancellation against actual fixed-view HTTP requests.
//! Tests in this target serialize because admission is process-wide.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use mst2_codec::{
    descriptor::ServingDescriptor,
    metapage::{page_id, Entry, EntryKind, Page},
    treeframe::{EndPayload, MetaPayload},
};
use scorpiofs::snapshot::{
    coordinator::{MAX_PROCESS_ACTIVE_CALLERS, MAX_PROCESS_PENDING_JOBS},
    durable::digest_of,
    FetchCoordinator, FetchCoordinatorCounts, FetchCoordinatorLimits, Mst2Client,
    SnapshotErrorCode, SnapshotFile, SnapshotReader, ValidatedSnapshotClosure,
};
use serde_json::{json, Value};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

static TEST_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());
const INSTANCE: &str = "11111111-2222-4333-8444-555555555561";

fn hash(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

fn id(bytes: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(bytes))
}

fn content(path: &str) -> Vec<u8> {
    if matches!(path.trim_start_matches('/'), "a" | "b") {
        b"shared verified bytes".to_vec()
    } else {
        format!("verified bytes for {}", path.trim_start_matches('/')).into_bytes()
    }
}

fn file(path: &str) -> SnapshotFile {
    let bytes = content(path);
    SnapshotFile {
        rel_path: path.into(),
        fs_kind: if path.trim_start_matches('/') == "b" {
            "executable"
        } else {
            "regular"
        }
        .into(),
        size: bytes.len() as u64,
        content_digest: digest_of(&bytes),
    }
}

struct Fixture {
    descriptor: ServingDescriptor,
    page: Vec<u8>,
    metadata_pages: bool,
    corrupt_metadata: AtomicBool,
    corrupt_blob: AtomicBool,
    body_mode: AtomicUsize,
    batch_segment: AtomicUsize,
    block_metadata: AtomicBool,
    metadata_requests: AtomicUsize,
    requests: Mutex<Vec<String>>,
    blob_release: Semaphore,
    metadata_release: Semaphore,
}

impl Fixture {
    fn new(metadata_pages: bool) -> Self {
        let mut entries = vec![
            Entry::file(
                EntryKind::Regular,
                b"a",
                content("a").len() as u64,
                hash(&content("a")),
            ),
            Entry::file(
                EntryKind::Executable,
                b"b",
                content("b").len() as u64,
                hash(&content("b")),
            ),
        ];
        for n in 0..64 {
            let path = format!("f{n:02}");
            let bytes = content(&path);
            entries.push(Entry::file(
                EntryKind::Regular,
                path.as_bytes(),
                bytes.len() as u64,
                hash(&bytes),
            ));
        }
        let page = Page::build(&entries).unwrap();
        Self {
            descriptor: ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
                namespace_view_id: [0x52; 32],
                scope: "/project".into(),
                metadata_root: page_id(&page),
            },
            page,
            metadata_pages,
            corrupt_metadata: AtomicBool::new(false),
            corrupt_blob: AtomicBool::new(false),
            body_mode: AtomicUsize::new(0),
            batch_segment: AtomicUsize::new(0),
            block_metadata: AtomicBool::new(false),
            metadata_requests: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            blob_release: Semaphore::new(0),
            metadata_release: Semaphore::new(0),
        }
    }

    fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("event-driven work did not reach its expected state");
}

async fn capabilities(State(f): State<Arc<Fixture>>) -> Json<Value> {
    Json(
        json!({"protocol_versions":[2],"metadata_codecs":[1],"frame_encodings":["identity"],"features":{"resolve":true,"directory":true,"leases":true,"metadata_pages":f.metadata_pages,"raw_blob":true}}),
    )
}

async fn resolve(State(f): State<Arc<Fixture>>) -> Json<Value> {
    let d = &f.descriptor;
    Json(
        json!({"descriptor":{"schema_version":2,"metadata_codec":1,"instance_id":INSTANCE,"namespace_view_id":id(&d.namespace_view_id),"scope":"/project","materialization_policy":1,"fs_semantics":1,"access_projection":0,"metadata_root":id(&d.metadata_root),"snapshot_id":id(&d.snapshot_id().unwrap())},"lease_id":"limits-lease","lease_expires_at":"2099-01-01T00:00:00Z","publication_sequence":"1","authorization_epoch":"1"}),
    )
}

async fn metadata(
    State(f): State<Arc<Fixture>>,
    Path(snapshot): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    assert_eq!(snapshot, id(&f.descriptor.snapshot_id().unwrap()));
    assert_eq!(headers["x-mega-snapshot-lease"], "limits-lease");
    let request: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(request["items"].as_array().unwrap().len(), 1);
    assert_eq!(request["items"][0]["directory_path"], "/");
    assert_eq!(request["items"][0]["route"], json!([]));
    assert_eq!(
        request["items"][0]["expected_digest"],
        id(&f.descriptor.metadata_root)
    );
    f.metadata_requests.fetch_add(1, Ordering::SeqCst);
    if f.block_metadata.load(Ordering::SeqCst) {
        f.metadata_release.acquire().await.unwrap().forget();
    }
    let mut page = f.page.clone();
    if f.corrupt_metadata.load(Ordering::SeqCst) {
        *page.last_mut().unwrap() ^= 1;
    }
    let mut wire = MetaPayload {
        pages: vec![(page_id(&page), page)],
    }
    .encode(24, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: f.page.len() as u64,
            request_body_sha256: hash(&body),
        }
        .encode(24, 1),
    );
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", snapshot)
        .header("x-mega-request-digest", id(&hash(&body)))
        .body(axum::body::Body::from(wire))
        .unwrap()
}

async fn blob(
    State(f): State<Arc<Fixture>>,
    Path(snapshot): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    assert_eq!(snapshot, id(&f.descriptor.snapshot_id().unwrap()));
    assert_eq!(headers["x-mega-snapshot-lease"], "limits-lease");
    let path = query["path"].clone();
    f.requests.lock().unwrap().push(path.clone());
    if path == "/absent" {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error":{"code":"PATH_NOT_FOUND","message":"not in fixed view"}})),
        )
            .into_response();
    }
    let mut bytes = content(&path);
    assert_eq!(query["expected_digest"], digest_of(&bytes));
    f.blob_release.acquire().await.unwrap().forget();
    if f.corrupt_blob.load(Ordering::SeqCst) {
        bytes[0] ^= 1;
    }
    match f.body_mode.load(Ordering::SeqCst) {
        1 => {
            bytes.push(0);
            return bytes.into_response();
        }
        2 => bytes.push(0),
        3 => {
            bytes.pop();
        }
        _ => return bytes.into_response(),
    }
    Response::new(axum::body::Body::from_stream(futures::stream::iter([
        Ok::<_, std::io::Error>(Bytes::from(bytes)),
    ])))
}

async fn objects(
    State(f): State<Arc<Fixture>>,
    Path(snapshot): Path<String>,
    body: Bytes,
) -> Response {
    assert!(
        body.len() <= 128 * 1024,
        "owned request writer exceeded its admitted fixed capacity"
    );
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    f.requests.lock().unwrap().push("objects".into());
    let mode = f.body_mode.load(Ordering::SeqCst);
    let mut objects = Vec::new();
    for item in items.iter().rev() {
        let path = item["path"].as_str().unwrap();
        let bytes = content(path);
        assert_eq!(item["expected_digest"], digest_of(&bytes));
        if !objects.iter().any(|(id, _)| *id == hash(&bytes)) {
            objects.push((hash(&bytes), bytes));
        }
    }
    let logical = objects.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    let units = objects.len();
    if mode == 20 {
        objects.pop();
    }
    if mode == 21 {
        objects.push((hash(b"foreign"), b"foreign".to_vec()));
    }
    let mut wire = if objects.is_empty() {
        Vec::new()
    } else {
        mst2_codec::treeframe::ObjectPayload {
            objects: objects.clone(),
        }
        .encode(29, 0)
        .unwrap()
    };
    let mut end = EndPayload {
        request_item_count: items.len() as u32,
        unique_unit_count: units as u32,
        logical_bytes: logical,
        request_body_sha256: hash(&body),
    };
    if mode == 22 || (mode == 27 && f.batch_segment.fetch_add(1, Ordering::SeqCst) >= 1) {
        end.request_body_sha256[0] ^= 1;
    }
    let mut sequence = u64::from(!wire.is_empty());
    if mode == 25 {
        wire.extend(
            mst2_codec::treeframe::ObjectPayload { objects }
                .encode(29, sequence)
                .unwrap(),
        );
        sequence += 1;
    }
    if mode == 24 {
        wire.extend(
            mst2_codec::treeframe::ErrorPayload {
                code: "INTEGRITY_ERROR".into(),
                retryable: false,
                request_id: "batch-late".into(),
            }
            .encode(29, sequence)
            .unwrap(),
        );
    } else {
        wire.extend(end.encode(29, sequence));
    }
    if mode == 23 {
        wire.push(0);
    }
    let response_body = if mode == 26 {
        use futures::StreamExt;
        axum::body::Body::from_stream(
            futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from(wire))])
                .chain(futures::stream::pending()),
        )
    } else {
        axum::body::Body::from(wire)
    };
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", snapshot)
        .header("x-mega-request-digest", id(&hash(&body)))
        .body(response_body)
        .unwrap()
}

struct Server {
    url: String,
    fixture: Arc<Fixture>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(metadata_pages: bool) -> Self {
        Self::start_fixture(Fixture::new(metadata_pages)).await
    }
    async fn start_fixture(fixture: Fixture) -> Self {
        let fixture = Arc::new(fixture);
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(capabilities))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .route(
                "/api/v2/snapshots/{snapshot}/metadata/pages",
                post(metadata),
            )
            .route("/api/v2/snapshots/{snapshot}/blob", get(blob))
            .route("/api/v2/snapshots/{snapshot}/objects", post(objects))
            .with_state(fixture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self {
            url: format!("http://{}", listener.local_addr().unwrap()),
            fixture,
            task: tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
        }
    }
    async fn reader(&self) -> SnapshotReader {
        SnapshotReader::resolve(Mst2Client::new(&self.url), "/project", 600)
            .await
            .unwrap()
    }
    async fn seeded(&self, running: usize, jobs: usize, callers: usize) -> Arc<FetchCoordinator> {
        let reader = self.reader().await;
        let closure = reader.snapshot_closure().await.unwrap();
        coordinator(reader, &closure, running, jobs, callers)
    }
}

#[tokio::test]
async fn owned_batch_long_authorized_paths_split_requests_and_late_segment_failure_publishes_nothing(
) {
    let _serial = TEST_LOCK.lock().await;
    let components: Vec<String> = (0..8)
        .map(|index| char::from(b'a' + index).to_string().repeat(255))
        .collect();
    let prefix = components.join("/");
    let files: Vec<_> = (0..128)
        .map(|index| file(&format!("{prefix}/f{index:03}")))
        .collect();
    let leaf = Page::build(
        &files
            .iter()
            .enumerate()
            .map(|(index, file)| {
                Entry::file(
                    EntryKind::Regular,
                    format!("f{index:03}").as_bytes(),
                    file.size,
                    hash(&content(&file.rel_path)),
                )
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let mut pages = std::collections::BTreeMap::from([(id(&page_id(&leaf)), leaf.clone())]);
    let mut root = leaf;
    for component in components.iter().rev() {
        root = Page::build(&[Entry::dir(component.as_bytes(), page_id(&root))]).unwrap();
        pages.insert(id(&page_id(&root)), root.clone());
    }
    let mut fixture = Fixture::new(true);
    fixture.descriptor.metadata_root = page_id(&root);
    fixture.page = root;
    let s = Server::start_fixture(fixture).await;
    let reader = s.reader().await;
    reader
        .seed_content_membership(
            &ValidatedSnapshotClosure::from_pages(reader.descriptor(), pages).unwrap(),
        )
        .unwrap();
    let batch = reader.read_content_batch(&files).await.unwrap();
    assert_eq!(batch.len(), 128);
    assert!(
        s.fixture.request_count() >= 2,
        "long escaping-aware request writer split before sending oversized JSON"
    );
    for file in &files {
        assert_eq!(
            batch.get(&file.content_digest).unwrap().as_bytes(),
            content(&file.rel_path)
        );
    }
    drop(batch);
    assert_eq!(reader.content_usage().output_bytes, 0);
    until(|| reader.content_usage().construction_bytes == 0).await;
    // A valid first segment followed by an invalid END in a later response must
    // discard the entire unpublished batch, including earlier segment bodies.
    s.fixture.body_mode.store(27, Ordering::SeqCst);
    s.fixture.batch_segment.store(0, Ordering::SeqCst);
    assert!(reader.read_content_batch(&files).await.is_err());
    assert_eq!(s.fixture.batch_segment.load(Ordering::SeqCst), 2);
    assert_eq!(reader.content_usage().output_bytes, 0);
    until(|| reader.content_usage().construction_bytes == 0).await;
}

fn coordinator(
    reader: SnapshotReader,
    closure: &ValidatedSnapshotClosure,
    running: usize,
    jobs: usize,
    callers: usize,
) -> Arc<FetchCoordinator> {
    FetchCoordinator::with_verified_closure_and_limits(
        reader,
        closure,
        running,
        FetchCoordinatorLimits::new(jobs, callers).unwrap(),
    )
    .unwrap()
}

fn start(
    c: &Arc<FetchCoordinator>,
    path: &str,
) -> tokio::task::JoinHandle<Result<Arc<Vec<u8>>, scorpiofs::snapshot::SnapshotError>> {
    let c = c.clone();
    let file = file(path);
    tokio::spawn(async move { c.fetch(file, false).await })
}

async fn idle(c: &FetchCoordinator) {
    until(|| {
        c.counts()
            == FetchCoordinatorCounts {
                pending_jobs: 0,
                active_callers: 0,
            }
    })
    .await;
}

#[tokio::test]
async fn distinct_jobs_reject_at_local_cap_and_finish_with_verified_bytes() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let c = s.seeded(1, 2, 4).await;
    let first = start(&c, "a");
    until(|| s.fixture.request_count() == 1).await;
    let second = start(&c, "f00");
    until(|| c.counts().pending_jobs == 2).await;
    assert_eq!(
        c.fetch(file("f01"), false).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(s.fixture.request_count(), 1);
    assert_eq!(c.counts().active_callers, 2);
    s.fixture.blob_release.add_permits(2);
    assert_eq!(first.await.unwrap().unwrap().as_slice(), content("a"));
    assert_eq!(second.await.unwrap().unwrap().as_slice(), content("f00"));
    idle(&c).await;
    assert_eq!(s.fixture.request_count(), 2);
}

#[tokio::test]
async fn aliases_share_one_job_and_cancelled_waiters_free_the_caller_slot() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let c = s.seeded(1, 1, 2).await;
    let first = start(&c, "a");
    until(|| s.fixture.request_count() == 1).await;
    let mut alias = Box::pin(c.fetch(file("b"), false));
    assert!(futures::poll!(alias.as_mut()).is_pending());
    assert_eq!(
        c.counts(),
        FetchCoordinatorCounts {
            pending_jobs: 1,
            active_callers: 2
        }
    );
    assert_eq!(
        c.fetch(file("b"), false).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    drop(alias);
    assert_eq!(c.counts().active_callers, 1);
    assert_eq!(
        c.fetch(file("f00"), false).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    let mut alias = Box::pin(c.fetch(file("/b"), false));
    assert!(futures::poll!(alias.as_mut()).is_pending());
    s.fixture.blob_release.add_permits(1);
    let a = first.await.unwrap().unwrap();
    let b = alias.await.unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(s.fixture.request_count(), 1);
    idle(&c).await;
}

#[tokio::test]
async fn last_queued_waiter_cancels_without_http_and_original_caller_can_cancel() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let c = s.seeded(1, 2, 4).await;
    let first = start(&c, "a");
    until(|| s.fixture.request_count() == 1).await;
    let queued = start(&c, "f00");
    until(|| c.counts().pending_jobs == 2).await;
    queued.abort();
    assert!(queued.await.unwrap_err().is_cancelled());
    until(|| c.counts().pending_jobs == 1).await;
    let mut alias = Box::pin(c.fetch(file("b"), false));
    assert!(futures::poll!(alias.as_mut()).is_pending());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    s.fixture.blob_release.add_permits(1);
    assert_eq!(alias.await.unwrap().as_slice(), content("b"));
    idle(&c).await;
    assert_eq!(*s.fixture.requests.lock().unwrap(), vec!["/a"]);
    s.fixture.blob_release.add_permits(1);
    assert_eq!(
        c.fetch(file("f00"), false).await.unwrap().as_slice(),
        content("f00")
    );
    idle(&c).await;
}

#[tokio::test]
async fn cancelled_running_generation_cannot_remove_immediate_same_key_retry() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let c = s.seeded(2, 2, 4).await;
    let first = start(&c, "a");
    until(|| s.fixture.request_count() == 1).await;
    let mut alias = Box::pin(c.fetch(file("b"), false));
    assert!(futures::poll!(alias.as_mut()).is_pending());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    drop(alias);
    // Poll on this single-thread runtime before yielding: the replacement is
    // inserted while the cancelled old leader still owns its job permit.
    let mut retry = Box::pin(c.fetch(file("b"), false));
    assert!(futures::poll!(retry.as_mut()).is_pending());
    assert_eq!(
        c.counts(),
        FetchCoordinatorCounts {
            pending_jobs: 2,
            active_callers: 1
        }
    );
    until(|| s.fixture.request_count() == 2).await;
    until(|| c.counts().pending_jobs == 1).await;
    assert_eq!(c.counts().active_callers, 1);
    s.fixture.blob_release.add_permits(2);
    assert_eq!(retry.await.unwrap().as_slice(), content("b"));
    idle(&c).await;
}

#[tokio::test]
async fn cancellation_before_first_leader_poll_is_latched_and_issues_no_http() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let c = s.seeded(1, 1, 1).await;
    let mut fetch = Box::pin(c.fetch(file("a"), false));
    assert!(futures::poll!(fetch.as_mut()).is_pending());
    assert_eq!(
        c.counts(),
        FetchCoordinatorCounts {
            pending_jobs: 1,
            active_callers: 1
        }
    );
    // No executor yield has occurred since spawning the leader. It has not
    // subscribed to cancellation or started its HTTP future yet.
    drop(fetch);
    assert_eq!(
        c.counts(),
        FetchCoordinatorCounts {
            pending_jobs: 1,
            active_callers: 0
        }
    );
    idle(&c).await;
    assert_eq!(s.fixture.request_count(), 0);
    s.fixture.blob_release.add_permits(1);
    assert_eq!(
        c.fetch(file("a"), false).await.unwrap().as_slice(),
        content("a")
    );
    idle(&c).await;
}

#[tokio::test]
async fn membership_waits_take_caller_slots_and_failed_proofs_and_bodies_release_admission() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    s.fixture.block_metadata.store(true, Ordering::SeqCst);
    let c = FetchCoordinator::new_with_limits(
        s.reader().await,
        1,
        FetchCoordinatorLimits::new(1, 1).unwrap(),
    );
    let proof = start(&c, "a");
    until(|| s.fixture.metadata_requests.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        c.counts(),
        FetchCoordinatorCounts {
            pending_jobs: 0,
            active_callers: 1
        }
    );
    assert_eq!(
        c.fetch(file("b"), false).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(s.fixture.metadata_requests.load(Ordering::SeqCst), 1);
    assert_eq!(s.fixture.request_count(), 0);
    proof.abort();
    assert!(proof.await.unwrap_err().is_cancelled());
    idle(&c).await;
    s.fixture.block_metadata.store(false, Ordering::SeqCst);
    s.fixture.corrupt_metadata.store(true, Ordering::SeqCst);
    let bad = c.fetch(file("a"), false).await.unwrap_err();
    assert!(matches!(
        bad.code,
        SnapshotErrorCode::DigestMismatch | SnapshotErrorCode::IntegrityError
    ));
    idle(&c).await;
    s.fixture.corrupt_metadata.store(false, Ordering::SeqCst);
    s.fixture.corrupt_blob.store(true, Ordering::SeqCst);
    s.fixture.blob_release.add_permits(1);
    assert_eq!(
        c.fetch(file("a"), false).await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    idle(&c).await;
    s.fixture.corrupt_blob.store(false, Ordering::SeqCst);
    s.fixture.blob_release.add_permits(1);
    assert_eq!(
        c.fetch(file("a"), false).await.unwrap().as_slice(),
        content("a")
    );
    idle(&c).await;
}

#[tokio::test]
async fn legacy_jobs_remain_independent_and_cancellation_releases_admission() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(false).await;
    let c = FetchCoordinator::new_with_limits(
        s.reader().await,
        2,
        FetchCoordinatorLimits::new(2, 3).unwrap(),
    );
    let a = start(&c, "a");
    let b = start(&c, "b");
    until(|| s.fixture.request_count() == 2).await;
    assert_eq!(
        c.fetch(file("a"), false).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    a.abort();
    assert!(a.await.unwrap_err().is_cancelled());
    assert_eq!(
        c.counts(),
        FetchCoordinatorCounts {
            pending_jobs: 1,
            active_callers: 1
        }
    );
    s.fixture.blob_release.add_permits(2);
    assert_eq!(b.await.unwrap().unwrap().as_slice(), content("b"));
    idle(&c).await;
    assert_eq!(s.fixture.metadata_requests.load(Ordering::SeqCst), 0);
    assert_eq!(s.fixture.request_count(), 2);
}

#[tokio::test]
async fn process_job_and_caller_caps_apply_across_coordinators_and_cleanup_restores_capacity() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let reader = s.reader().await;
    let closure = reader.snapshot_closure().await.unwrap();
    let mut coordinators = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let c = coordinator(reader.clone(), &closure, 1, 64, 256);
        for n in 0..64 {
            tasks.push(start(&c, &format!("f{n:02}")));
        }
        coordinators.push(c);
    }
    until(|| FetchCoordinator::process_counts().pending_jobs == MAX_PROCESS_PENDING_JOBS).await;
    until(|| s.fixture.request_count() == 4).await;
    let extra = coordinator(reader.clone(), &closure, 1, 64, 256);
    assert_eq!(
        extra.fetch(file("a"), false).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(
        extra.counts(),
        FetchCoordinatorCounts {
            pending_jobs: 0,
            active_callers: 0
        }
    );
    assert_eq!(
        FetchCoordinator::process_counts(),
        FetchCoordinatorCounts {
            pending_jobs: 256,
            active_callers: 256
        }
    );
    assert_eq!(s.fixture.request_count(), 4);
    for task in &tasks {
        task.abort();
    }
    for task in tasks.drain(..) {
        assert!(task.await.unwrap_err().is_cancelled());
    }
    until(|| {
        FetchCoordinator::process_counts()
            == FetchCoordinatorCounts {
                pending_jobs: 0,
                active_callers: 0,
            }
    })
    .await;
    for c in &coordinators {
        idle(c).await;
    }
    // Four independent shared downloads, with 256 legal callers each.
    for c in &coordinators {
        for _ in 0..256 {
            tasks.push(start(c, "a"));
        }
    }
    until(|| FetchCoordinator::process_counts().active_callers == MAX_PROCESS_ACTIVE_CALLERS).await;
    until(|| s.fixture.request_count() == 8).await;
    assert_eq!(FetchCoordinator::process_counts().pending_jobs, 4);
    assert_eq!(
        extra.fetch(file("b"), false).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(
        extra.counts(),
        FetchCoordinatorCounts {
            pending_jobs: 0,
            active_callers: 0
        }
    );
    assert_eq!(s.fixture.request_count(), 8);
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        assert!(task.await.unwrap_err().is_cancelled());
    }
    until(|| {
        FetchCoordinator::process_counts()
            == FetchCoordinatorCounts {
                pending_jobs: 0,
                active_callers: 0,
            }
    })
    .await;
    // Failed global acquisition returned the extra coordinator's local permits.
    // Release old server handlers too; they are not client-owned job permits.
    s.fixture.blob_release.add_permits(9);
    assert_eq!(
        extra.fetch(file("a"), false).await.unwrap().as_slice(),
        content("a")
    );
    idle(&extra).await;
    assert_eq!(s.fixture.request_count(), 9);
}

#[test]
fn runtime_shutdown_drops_actual_queued_and_running_job_owners() {
    let _serial = TEST_LOCK.blocking_lock();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (s, c, first, queued) = runtime.block_on(async {
        let s = Server::start(true).await;
        let c = s.seeded(1, 2, 4).await;
        let first = start(&c, "a");
        until(|| s.fixture.request_count() == 1).await;
        let queued = start(&c, "f00");
        until(|| c.counts().pending_jobs == 2).await;
        (s, c, first, queued)
    });
    assert_eq!(
        c.counts(),
        FetchCoordinatorCounts {
            pending_jobs: 2,
            active_callers: 2
        }
    );
    drop(runtime);
    assert_eq!(
        c.counts(),
        FetchCoordinatorCounts {
            pending_jobs: 0,
            active_callers: 0
        }
    );
    assert_eq!(
        FetchCoordinator::process_counts(),
        FetchCoordinatorCounts {
            pending_jobs: 0,
            active_callers: 0
        }
    );
    drop((s, first, queued));
}

#[tokio::test]
async fn invalid_limits_and_extreme_concurrency_are_bounded_without_constructor_panics() {
    let _serial = TEST_LOCK.lock().await;
    for (jobs, callers) in [(0, 1), (1, 0), (65, 1), (1, 257), (usize::MAX, usize::MAX)] {
        assert_eq!(
            FetchCoordinatorLimits::new(jobs, callers).unwrap_err().code,
            SnapshotErrorCode::InvalidRequest
        );
    }
    let s = Server::start(true).await;
    for running in [0, usize::MAX] {
        let c = s.seeded(running, 1, 1).await;
        s.fixture.blob_release.add_permits(1);
        assert_eq!(
            c.fetch(file("a"), false).await.unwrap().as_slice(),
            content("a")
        );
        idle(&c).await;
    }
}

async fn small_budget_coordinator(s: &Server) -> Arc<FetchCoordinator> {
    let reader = s.reader().await;
    let closure = reader.snapshot_closure().await.unwrap();
    FetchCoordinator::with_verified_closure_and_budgets(
        reader,
        &closure,
        1,
        FetchCoordinatorLimits::new(4, 8).unwrap(),
        scorpiofs::snapshot::ContentBudgetLimits::new(1024, 2 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}

fn start_owned(
    c: &Arc<FetchCoordinator>,
    path: &str,
) -> tokio::task::JoinHandle<
    Result<Arc<scorpiofs::snapshot::VerifiedContent>, scorpiofs::snapshot::SnapshotError>,
> {
    let c = c.clone();
    let file = file(path);
    tokio::spawn(async move { c.fetch_owned(file, false).await })
}

#[tokio::test]
async fn returned_aliases_and_unreceived_task_results_keep_capacity_until_last_arc() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let c = small_budget_coordinator(&s).await;
    let first = start_owned(&c, "a");
    until(|| s.fixture.request_count() == 1).await;
    let waiter = start_owned(&c, "b");
    until(|| c.counts().active_callers == 2).await;
    s.fixture.blob_release.add_permits(1);
    let owner = first.await.unwrap().unwrap();
    let identity = Arc::downgrade(&owner);
    idle(&c).await;
    assert_eq!(c.content_usage().output_bytes, 1024);
    let clone = owner.clone();
    assert_eq!(c.content_usage().output_bytes, 1024);
    assert_eq!(
        c.fetch_owned(file("f00"), false).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(
        s.fixture.request_count(),
        1,
        "capacity rejection sent no extra body HTTP"
    );
    drop((owner, clone));
    assert_eq!(
        c.content_usage().output_bytes,
        1024,
        "unreceived JoinHandle result is still an actual owner"
    );
    let alias = waiter.await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&identity.upgrade().unwrap(), &alias));
    assert_eq!(alias.as_slice(), content("b"));
    assert_eq!(c.content_usage().output_bytes, 1024);
    drop(alias);
    assert_eq!(c.content_usage().output_bytes, 0);
    s.fixture.blob_release.add_permits(1);
    let next = c.fetch_owned(file("f00"), false).await.unwrap();
    assert_eq!(next.as_slice(), content("f00"));
    drop(next);
    idle(&c).await;
    assert_eq!(c.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn sized_raw_rejects_length_digest_and_stream_overflow_without_publishing() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let c = small_budget_coordinator(&s).await;
    for mode in 1..=3 {
        s.fixture.body_mode.store(mode, Ordering::SeqCst);
        s.fixture.blob_release.add_permits(1);
        assert_eq!(
            c.fetch_owned(file("a"), false).await.unwrap_err().code,
            SnapshotErrorCode::DigestMismatch
        );
        idle(&c).await;
        assert_eq!(c.content_usage().output_bytes, 0);
        assert_eq!(c.content_usage().construction_bytes, 0);
    }
    s.fixture.body_mode.store(0, Ordering::SeqCst);
    s.fixture.corrupt_blob.store(true, Ordering::SeqCst);
    s.fixture.blob_release.add_permits(1);
    assert_eq!(
        c.fetch_owned(file("a"), false).await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    idle(&c).await;
    assert_eq!(c.content_usage().output_bytes, 0);
    s.fixture.corrupt_blob.store(false, Ordering::SeqCst);
    s.fixture.blob_release.add_permits(1);
    assert_eq!(
        c.fetch_owned(file("a"), false).await.unwrap().as_slice(),
        content("a")
    );
    idle(&c).await;
    assert_eq!(c.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn queued_and_running_cancellation_drop_actual_fixed_output_owners() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let c = small_budget_coordinator(&s).await;
    let first = start_owned(&c, "a");
    until(|| s.fixture.request_count() == 1).await;
    assert_eq!(c.content_usage().output_bytes, 1024);
    let queued = start_owned(&c, "f00");
    until(|| c.counts().pending_jobs == 2).await;
    assert_eq!(
        c.content_usage().output_bytes,
        1024,
        "queued leader did not allocate output"
    );
    queued.abort();
    assert!(queued.await.unwrap_err().is_cancelled());
    until(|| c.counts().pending_jobs == 1).await;
    assert_eq!(c.content_usage().output_bytes, 1024);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    idle(&c).await;
    assert_eq!(c.content_usage().output_bytes, 0);
    assert_eq!(c.content_usage().construction_bytes, 0);
    assert_eq!(s.fixture.request_count(), 1);
}

#[tokio::test]
async fn direct_reader_clones_share_budget_and_reject_forged_fixed_view_facts_before_body_http() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let reader = s.reader().await.with_content_limits(
        scorpiofs::snapshot::ContentBudgetLimits::new(1024, 2 * 1024 * 1024).unwrap(),
    );
    let closure = reader.snapshot_closure().await.unwrap();
    reader.seed_content_membership(&closure).unwrap();
    let clone = reader.clone();
    assert_eq!(
        clone
            .read_content(&file("absent"), false)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::PathNotFound
    );
    let mut wrong_size = file("a");
    wrong_size.size += 1;
    assert_eq!(
        clone
            .read_content(&wrong_size, false)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    let mut wrong_digest = file("a");
    wrong_digest.content_digest = digest_of(b"forged");
    assert_eq!(
        clone
            .read_content(&wrong_digest, false)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    let mut wrong_kind = file("a");
    wrong_kind.fs_kind = "symlink".into();
    assert_eq!(
        clone
            .read_content(&wrong_kind, false)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    let mut wrong_path = file("a");
    wrong_path.rel_path = "../a".into();
    assert!(clone.read_content(&wrong_path, false).await.is_err());
    assert_eq!(s.fixture.request_count(), 0);
    s.fixture.blob_release.add_permits(1);
    let owner = reader.read_content(&file("a"), false).await.unwrap();
    assert_eq!(clone.content_usage().output_bytes, 1024);
    assert_eq!(
        clone
            .read_content(&file("b"), false)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(s.fixture.request_count(), 1);
    drop(owner);
    assert_eq!(reader.content_usage().output_bytes, 0);
    s.fixture.blob_release.add_permits(1);
    assert_eq!(
        clone
            .read_content(&file("b"), false)
            .await
            .unwrap()
            .as_bytes(),
        content("b")
    );
}

#[tokio::test]
async fn owned_object_batch_charges_table_and_aliases_once_until_actual_owners_drop() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let reader = s.reader().await.with_content_limits(
        scorpiofs::snapshot::ContentBudgetLimits::new(4096, 8 * 1024 * 1024).unwrap(),
    );
    let closure = reader.snapshot_closure().await.unwrap();
    reader.seed_content_membership(&closure).unwrap();
    let mut absent_alias = file("a");
    absent_alias.rel_path = "absent".into();
    assert_eq!(
        reader
            .read_content_batch(&[file("a"), absent_alias])
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::PathNotFound
    );
    let mut forged_alias = file("b");
    forged_alias.fs_kind = "regular".into();
    assert_eq!(
        reader
            .read_content_batch(&[file("a"), forged_alias])
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(
        s.fixture.request_count(),
        0,
        "each alias authorizes before deduplication"
    );
    assert_eq!(reader.content_usage().output_bytes, 0);
    let alias_only = reader
        .read_content_batch(&[file("a"), file("b")])
        .await
        .unwrap();
    assert_eq!(alias_only.len(), 1);
    assert_eq!(
        reader.content_usage().output_bytes,
        2048,
        "one table plus one unique content"
    );
    drop(alias_only);
    assert_eq!(reader.content_usage().output_bytes, 0);
    let batch = reader
        .read_content_batch(&[file("a"), file("b"), file("f00")])
        .await
        .unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(
        batch.get(&file("a").content_digest).unwrap().as_bytes(),
        content("a")
    );
    assert_eq!(
        reader.content_usage().output_bytes,
        3072,
        "one table and two whole-content owners"
    );
    let retained = batch.get(&file("a").content_digest).unwrap().clone();
    let cloned = retained.clone();
    assert_eq!(reader.content_usage().output_bytes, 3072);
    drop(batch);
    assert_eq!(
        reader.content_usage().output_bytes,
        1024,
        "table and unretained content dropped before credits"
    );
    drop(retained);
    assert_eq!(reader.content_usage().output_bytes, 1024);
    drop(cloned);
    assert_eq!(reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn owned_batch_pre_http_capacity_and_late_integrity_failures_release_all_builders() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let reader = s.reader().await.with_content_limits(
        scorpiofs::snapshot::ContentBudgetLimits::new(2048, 8 * 1024 * 1024).unwrap(),
    );
    let closure = reader.snapshot_closure().await.unwrap();
    reader.seed_content_membership(&closure).unwrap();
    assert_eq!(
        reader
            .read_content_batch(&[file("a"), file("f00")])
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(
        s.fixture.request_count(),
        0,
        "all outputs admit before body HTTP"
    );
    assert_eq!(reader.content_usage().output_bytes, 0);
    for mode in 20..=25 {
        s.fixture.body_mode.store(mode, Ordering::SeqCst);
        assert!(reader
            .read_content_batch(&[file("a"), file("b")])
            .await
            .is_err());
        assert_eq!(reader.content_usage().output_bytes, 0);
        until(|| reader.content_usage().construction_bytes == 0).await;
    }
    let requests = s.fixture.request_count();
    assert_eq!(
        reader
            .read_content_batch(&vec![file("a"); 129])
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(s.fixture.request_count(), requests);
}

#[tokio::test]
async fn owned_batch_end_without_eof_and_actual_cancellation_keep_then_release_all_owners() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let reader = s.reader().await.with_content_limits(
        scorpiofs::snapshot::ContentBudgetLimits::new(4096, 8 * 1024 * 1024).unwrap(),
    );
    reader
        .seed_content_membership(&reader.snapshot_closure().await.unwrap())
        .unwrap();
    s.fixture.body_mode.store(26, Ordering::SeqCst);
    let task = tokio::spawn({
        let reader = reader.clone();
        async move { reader.read_content_batch(&[file("a"), file("f00")]).await }
    });
    until(|| s.fixture.request_count() == 1).await;
    assert!(!task.is_finished());
    assert_eq!(reader.content_usage().output_bytes, 3072);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    until(|| {
        reader.content_usage().output_bytes == 0 && reader.content_usage().construction_bytes == 0
    })
    .await;
    s.fixture.body_mode.store(0, Ordering::SeqCst);
    assert_eq!(
        reader
            .read_content_batch(&[file("a"), file("f00")])
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn legacy_and_owned_aliases_keep_distinct_result_types_and_shared_count_admission() {
    let _serial = TEST_LOCK.lock().await;
    let s = Server::start(true).await;
    let c = small_budget_coordinator(&s).await;
    let legacy = start(&c, "a");
    until(|| s.fixture.request_count() == 1).await;
    let owned = start_owned(&c, "b");
    until(|| c.counts().pending_jobs == 2).await;
    assert_eq!(c.counts().active_callers, 2);
    s.fixture.blob_release.add_permits(2);
    let legacy: Arc<Vec<u8>> = legacy.await.unwrap().unwrap();
    let owned = owned.await.unwrap().unwrap();
    assert_eq!(legacy.as_slice(), content("a"));
    assert_eq!(owned.as_slice(), content("b"));
    idle(&c).await;
    assert_eq!(s.fixture.request_count(), 2);
    assert_eq!(c.content_usage().output_bytes, 1024);
    drop(owned);
    assert_eq!(c.content_usage().output_bytes, 0);
    assert_eq!(legacy.as_slice(), content("a"));
}
