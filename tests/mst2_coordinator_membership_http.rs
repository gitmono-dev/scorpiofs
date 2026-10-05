//! Every single-flight caller needs its own fixed-root membership proof.
//! The HTTP oracle has two legal paths with identical bytes and distinct modes.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
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
    durable::digest_of, FetchCoordinator, Mst2Client, SnapshotErrorCode, SnapshotFile,
    SnapshotReader,
};
use serde_json::{json, Value};
use tokio::sync::{Notify, Semaphore};

const CONTENT: &[u8] = b"same bytes at separately proven paths";
const INSTANCE: &str = "11111111-2222-4333-8444-555555555560";

fn hash(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

fn id(value: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(value))
}

fn file(path: &str) -> SnapshotFile {
    SnapshotFile {
        rel_path: path.into(),
        fs_kind: if path.trim_start_matches('/') == "b" {
            "executable"
        } else {
            "regular"
        }
        .into(),
        size: CONTENT.len() as u64,
        content_digest: digest_of(CONTENT),
    }
}

fn expiry_in_three_seconds() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3;
    let mut days = seconds / 86_400;
    let leap = |year: u64| {
        year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
    };
    let mut year = 1970;
    loop {
        let length = if leap(year) { 366 } else { 365 };
        if days < length {
            break;
        }
        days -= length;
        year += 1;
    }
    let lengths = [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1;
    for length in lengths {
        if days < length {
            break;
        }
        days -= length;
        month += 1;
    }
    format!(
        "{year:04}-{month:02}-{:02}T{:02}:{:02}:{:02}Z",
        days + 1,
        seconds % 86_400 / 3600,
        seconds % 3600 / 60,
        seconds % 60
    )
}

struct Fixture {
    descriptor: ServingDescriptor,
    page: Vec<u8>,
    metadata_pages: bool,
    expiry: String,
    corrupt: AtomicBool,
    pause_metadata: AtomicBool,
    pause_blob: AtomicBool,
    metadata_requests: AtomicUsize,
    blob_requests: Mutex<Vec<(String, String)>>,
    metadata_started: Notify,
    blob_started: Notify,
    renewal_started: Notify,
    metadata_release: Semaphore,
    blob_release: Semaphore,
}

impl Fixture {
    fn new(metadata_pages: bool) -> Self {
        let page = Page::build(&[
            Entry::file(
                EntryKind::Regular,
                b"a",
                CONTENT.len() as u64,
                hash(CONTENT),
            ),
            Entry::file(
                EntryKind::Executable,
                b"b",
                CONTENT.len() as u64,
                hash(CONTENT),
            ),
        ])
        .unwrap();
        Self {
            descriptor: ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
                namespace_view_id: [0x51; 32],
                scope: "/project".into(),
                metadata_root: page_id(&page),
            },
            page,
            metadata_pages,
            expiry: "2099-01-01T00:00:00Z".into(),
            corrupt: AtomicBool::new(false),
            pause_metadata: AtomicBool::new(false),
            pause_blob: AtomicBool::new(false),
            metadata_requests: AtomicUsize::new(0),
            blob_requests: Mutex::new(Vec::new()),
            metadata_started: Notify::new(),
            blob_started: Notify::new(),
            renewal_started: Notify::new(),
            metadata_release: Semaphore::new(0),
            blob_release: Semaphore::new(0),
        }
    }

    async fn wait_blobs(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.blob_requests.lock().unwrap().len() < count {
                self.blob_started.notified().await;
            }
        })
        .await
        .unwrap();
    }
}

async fn capabilities(State(f): State<Arc<Fixture>>) -> Json<Value> {
    Json(json!({
        "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true,
                     "metadata_pages": f.metadata_pages, "raw_blob": true}
    }))
}

async fn resolve(State(f): State<Arc<Fixture>>) -> Json<Value> {
    let d = &f.descriptor;
    Json(json!({
        "descriptor": {"schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE,
            "namespace_view_id": id(&d.namespace_view_id), "scope": "/project",
            "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
            "metadata_root": id(&d.metadata_root), "snapshot_id": id(&d.snapshot_id().unwrap())},
        "lease_id": "membership-lease", "lease_expires_at": f.expiry,
        "publication_sequence": "1", "authorization_epoch": "1"
    }))
}

async fn renew(State(f): State<Arc<Fixture>>) -> Response {
    f.renewal_started.notify_one();
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": {"code": "LEASE_UNKNOWN", "message": "lease revoked"}})),
    )
        .into_response()
}

async fn metadata(
    State(f): State<Arc<Fixture>>,
    Path(snapshot): Path<String>,
    body: Bytes,
) -> Response {
    assert_eq!(snapshot, id(&f.descriptor.snapshot_id().unwrap()));
    let request: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(request["items"].as_array().unwrap().len(), 1);
    assert_eq!(request["items"][0]["directory_path"], "/");
    assert_eq!(request["items"][0]["route"], json!([]));
    assert_eq!(
        request["items"][0]["expected_digest"],
        id(&f.descriptor.metadata_root)
    );
    f.metadata_requests.fetch_add(1, Ordering::SeqCst);
    f.metadata_started.notify_one();
    if f.pause_metadata.load(Ordering::SeqCst) {
        f.metadata_release.acquire().await.unwrap().forget();
    }
    let mut page = f.page.clone();
    if f.corrupt.load(Ordering::SeqCst) {
        *page.last_mut().unwrap() ^= 1;
    }
    let mut wire = MetaPayload {
        pages: vec![(page_id(&page), page)],
    }
    .encode(23, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: f.page.len() as u64,
            request_body_sha256: hash(&body),
        }
        .encode(23, 1),
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
    assert_eq!(headers["x-mega-snapshot-lease"], "membership-lease");
    let path = query["path"].clone();
    let credential = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    f.blob_requests
        .lock()
        .unwrap()
        .push((path.clone(), credential));
    f.blob_started.notify_one();
    if !matches!(path.as_str(), "/a" | "/b") {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": {"code": "PATH_NOT_FOUND", "message": "not in fixed view"}})),
        )
            .into_response();
    }
    assert_eq!(query["expected_digest"], digest_of(CONTENT));
    if f.pause_blob.load(Ordering::SeqCst) {
        f.blob_release.acquire().await.unwrap().forget();
    }
    CONTENT.into_response()
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
    async fn start(fixture: Fixture) -> Self {
        let fixture = Arc::new(fixture);
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(capabilities))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .route("/api/v2/snapshots/leases/{lease}/renew", post(renew))
            .route(
                "/api/v2/snapshots/{snapshot}/metadata/pages",
                post(metadata),
            )
            .route("/api/v2/snapshots/{snapshot}/blob", get(blob))
            .with_state(fixture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self {
            url: format!("http://{}", listener.local_addr().unwrap()),
            fixture,
            task: tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
        }
    }

    async fn reader(&self, token: Option<&str>) -> SnapshotReader {
        SnapshotReader::resolve(
            Mst2Client::with_token(&self.url, token.map(str::to_string)),
            "/project",
            600,
        )
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn each_alias_proves_its_metadata_before_content_can_merge() {
    let fixture = Fixture::new(true);
    fixture.pause_blob.store(true, Ordering::SeqCst);
    let server = Server::start(fixture).await;
    let coordinator = FetchCoordinator::new(server.reader(None).await, 2);
    let leader = tokio::spawn({
        let c = coordinator.clone();
        async move { c.fetch(file("a"), false).await }
    });
    server.fixture.wait_blobs(1).await;
    let mut invalid = Vec::new();
    invalid.push((file("absent"), SnapshotErrorCode::PathNotFound));
    invalid.push((file("/absent"), SnapshotErrorCode::PathNotFound));
    let mut wrong = file("a");
    wrong.content_digest = digest_of(b"different");
    invalid.push((wrong, SnapshotErrorCode::DigestMismatch));
    let mut wrong = file("a");
    wrong.size += 1;
    invalid.push((wrong, SnapshotErrorCode::DigestMismatch));
    let mut wrong = file("b");
    wrong.fs_kind = "regular".into();
    invalid.push((wrong, SnapshotErrorCode::DigestMismatch));
    for (file, expected) in invalid {
        let error = tokio::time::timeout(Duration::from_secs(1), coordinator.fetch(file, false))
            .await
            .expect("unproven caller joined a blocked download")
            .unwrap_err();
        assert_eq!(error.code, expected);
    }
    let mut waiter = Box::pin(coordinator.fetch(file("/b"), false));
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    server.fixture.blob_release.add_permits(1);
    let a = leader.await.unwrap().unwrap();
    let b = waiter.await.unwrap();
    assert_eq!(a.as_slice(), CONTENT);
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(server.fixture.blob_requests.lock().unwrap().len(), 1);
    assert_eq!(server.fixture.metadata_requests.load(Ordering::SeqCst), 1);
    server.fixture.pause_blob.store(false, Ordering::SeqCst);
    let mut legacy_regular = file("a");
    legacy_regular.fs_kind = "file".into();
    assert_eq!(
        coordinator
            .fetch(legacy_regular, false)
            .await
            .unwrap()
            .as_slice(),
        CONTENT
    );
    assert_eq!(server.fixture.metadata_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_first_caller_preserves_a_proven_alias_waiter() {
    let fixture = Fixture::new(true);
    fixture.pause_blob.store(true, Ordering::SeqCst);
    let server = Server::start(fixture).await;
    let coordinator = FetchCoordinator::new(server.reader(None).await, 2);
    let first = tokio::spawn({
        let c = coordinator.clone();
        async move { c.fetch(file("a"), false).await }
    });
    server.fixture.wait_blobs(1).await;
    let mut waiter = Box::pin(coordinator.fetch(file("b"), false));
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    server.fixture.blob_release.add_permits(1);
    assert_eq!(waiter.await.unwrap().as_slice(), CONTENT);
    assert_eq!(server.fixture.blob_requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cached_membership_does_not_let_a_waiter_join_with_a_failed_lease() {
    let mut fixture = Fixture::new(true);
    fixture.expiry = expiry_in_three_seconds();
    fixture.pause_blob.store(true, Ordering::SeqCst);
    let server = Server::start(fixture).await;
    let reader = server.reader(None).await;
    let coordinator = FetchCoordinator::new(reader.clone(), 2);
    let first = tokio::spawn({
        let c = coordinator.clone();
        async move { c.fetch(file("a"), false).await }
    });
    server.fixture.wait_blobs(1).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        server.fixture.renewal_started.notified(),
    )
    .await
    .unwrap();
    // Synchronize with the in-progress renewer before testing the waiter.
    assert_eq!(
        reader.ensure_lease().await.unwrap_err().code,
        SnapshotErrorCode::LeaseUnknown
    );
    let error = tokio::time::timeout(Duration::from_secs(1), coordinator.fetch(file("b"), false))
        .await
        .expect("waiter reused a leader despite its own failed lease")
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::LeaseUnknown);
    assert_eq!(server.fixture.metadata_requests.load(Ordering::SeqCst), 1);
    assert_eq!(server.fixture.blob_requests.lock().unwrap().len(), 1);
    server.fixture.blob_release.add_permits(1);
    let _ = first.await.unwrap();
}

#[tokio::test]
async fn corrupt_root_proof_does_not_fetch_or_poison_membership_retry() {
    let fixture = Fixture::new(true);
    fixture.corrupt.store(true, Ordering::SeqCst);
    let server = Server::start(fixture).await;
    let coordinator = FetchCoordinator::new(server.reader(None).await, 1);
    let error = coordinator.fetch(file("a"), false).await.unwrap_err();
    assert!(matches!(
        error.code,
        SnapshotErrorCode::DigestMismatch | SnapshotErrorCode::IntegrityError
    ));
    assert!(server.fixture.blob_requests.lock().unwrap().is_empty());
    server.fixture.corrupt.store(false, Ordering::SeqCst);
    assert_eq!(
        coordinator
            .fetch(file("a"), false)
            .await
            .unwrap()
            .as_slice(),
        CONTENT
    );
    assert_eq!(server.fixture.metadata_requests.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancelled_membership_initialization_is_retried_from_the_fixed_root() {
    let fixture = Fixture::new(true);
    fixture.pause_metadata.store(true, Ordering::SeqCst);
    let server = Server::start(fixture).await;
    let coordinator = FetchCoordinator::new(server.reader(None).await, 1);
    let first = tokio::spawn({
        let c = coordinator.clone();
        async move { c.fetch(file("a"), false).await }
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        server.fixture.metadata_started.notified(),
    )
    .await
    .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(server.fixture.blob_requests.lock().unwrap().is_empty());
    server.fixture.pause_metadata.store(false, Ordering::SeqCst);
    server.fixture.metadata_release.add_permits(1);
    assert_eq!(
        coordinator
            .fetch(file("b"), false)
            .await
            .unwrap()
            .as_slice(),
        CONTENT
    );
    assert_eq!(server.fixture.metadata_requests.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn credentials_cannot_share_another_coordinators_download_or_membership() {
    let fixture = Fixture::new(true);
    fixture.pause_blob.store(true, Ordering::SeqCst);
    let server = Server::start(fixture).await;
    let a = server.reader(Some("actor-a")).await;
    let b = server.reader(Some("actor-b")).await;
    assert_ne!(
        a.authorized_context().cache_domain(),
        b.authorized_context().cache_domain()
    );
    let a = FetchCoordinator::new(a, 1);
    let b = FetchCoordinator::new(b, 1);
    let a = tokio::spawn(async move { a.fetch(file("a"), false).await });
    let b = tokio::spawn(async move { b.fetch(file("b"), false).await });
    server.fixture.wait_blobs(2).await;
    server.fixture.blob_release.add_permits(2);
    let a = a.await.unwrap().unwrap();
    let b = b.await.unwrap().unwrap();
    assert!(!Arc::ptr_eq(&a, &b));
    let requests = server.fixture.blob_requests.lock().unwrap();
    assert!(requests.contains(&("/a".into(), "Bearer actor-a".into())));
    assert!(requests.contains(&("/b".into(), "Bearer actor-b".into())));
    assert_eq!(server.fixture.metadata_requests.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn legacy_callers_keep_separate_online_path_requests_without_merging() {
    let fixture = Fixture::new(false);
    fixture.pause_blob.store(true, Ordering::SeqCst);
    let server = Server::start(fixture).await;
    let coordinator = FetchCoordinator::new(server.reader(None).await, 2);
    let first = tokio::spawn({
        let c = coordinator.clone();
        async move { c.fetch(file("a"), false).await }
    });
    server.fixture.wait_blobs(1).await;
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        coordinator.fetch(file("absent"), false),
    )
    .await
    .expect("legacy absent path reused another caller's result")
    .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::PathNotFound);
    let second = tokio::spawn({
        let c = coordinator.clone();
        async move { c.fetch(file("b"), false).await }
    });
    server.fixture.wait_blobs(3).await;
    server.fixture.blob_release.add_permits(2);
    assert_eq!(first.await.unwrap().unwrap().as_slice(), CONTENT);
    assert_eq!(second.await.unwrap().unwrap().as_slice(), CONTENT);
    assert_eq!(server.fixture.metadata_requests.load(Ordering::SeqCst), 0);
}
