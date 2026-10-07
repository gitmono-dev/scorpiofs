//! Public no-pages hydration uses a completed fixed-SID online walk, paid
//! ranges and whole-content verification, without claiming MTP2 membership.

use std::sync::{atomic::AtomicBool, Mutex};

use axum::{
    body::Body, extract::Request, http::StatusCode, middleware::Next, response::IntoResponse,
};
use futures::StreamExt;

use super::*;

const MIB: usize = 1024 * 1024;
const ONLINE_LEASE: &str = "completed-online-manifest-lease";
const DIRECTORY_OK: usize = 0;
const MISSING_SECOND_PAGE: usize = 1;
const MISSING_CHILD: usize = 2;
const LOOP_CURSOR: usize = 3;
const WRONG_ROOT: usize = 4;
const CHUNK_OK: usize = 0;
const SHORT_CHUNK: usize = 1;
const WRONG_CHUNK_HASH: usize = 2;
const WRONG_CHUNK_MAP: usize = 3;
const WRONG_CHUNK_CONTENT: usize = 4;
const WRONG_END: usize = 5;
const BEFORE_HEADERS: usize = 1;
const AFTER_CHUNK_BODY: usize = 2;

struct OnlineFixture {
    inner: Arc<Fixture>,
    descriptor: ServingDescriptor,
    empty_root: [u8; 32],
    directory_fault: AtomicUsize,
    directory_calls: Mutex<Vec<(String, Option<String>)>>,
    hold_directory: AtomicBool,
    directory_entered: Notify,
    directory_release: Notify,
    wrong_map_size: AtomicBool,
    hold_map: AtomicBool,
    map_entered: Notify,
    map_release: Notify,
    chunk_fault: AtomicUsize,
    hold_chunk: AtomicU64,
    chunk_stage: AtomicUsize,
    chunk_entered: Notify,
    chunk_release: Notify,
    partial_bodies: AtomicUsize,
    short_lease: bool,
    renewal_status: StatusCode,
    renewals: AtomicUsize,
}

impl OnlineFixture {
    fn new(size: u64, wrong_whole: bool) -> Self {
        let inner = Arc::new(Fixture::new(size, wrong_whole));
        let empty_root = page_id(&Page::build(&[]).unwrap());
        let root = Page::build(&[
            Entry::dir(b"a", empty_root),
            Entry::file(EntryKind::Regular, b"large.bin", size, inner.advertised),
        ])
        .unwrap();
        Self {
            inner,
            descriptor: ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
                namespace_view_id: [0x42; 32],
                scope: "/project".into(),
                metadata_root: page_id(&root),
            },
            empty_root,
            directory_fault: AtomicUsize::new(DIRECTORY_OK),
            directory_calls: Mutex::new(Vec::new()),
            hold_directory: AtomicBool::new(false),
            directory_entered: Notify::new(),
            directory_release: Notify::new(),
            wrong_map_size: AtomicBool::new(false),
            hold_map: AtomicBool::new(false),
            map_entered: Notify::new(),
            map_release: Notify::new(),
            chunk_fault: AtomicUsize::new(CHUNK_OK),
            hold_chunk: AtomicU64::new(NONE),
            chunk_stage: AtomicUsize::new(0),
            chunk_entered: Notify::new(),
            chunk_release: Notify::new(),
            partial_bodies: AtomicUsize::new(0),
            short_lease: false,
            renewal_status: StatusCode::FORBIDDEN,
            renewals: AtomicUsize::new(0),
        }
    }

    fn sid(&self) -> String {
        id(&self.descriptor.snapshot_id().unwrap())
    }

    fn release(&self) {
        self.hold_directory.store(false, Ordering::SeqCst);
        self.hold_map.store(false, Ordering::SeqCst);
        self.hold_chunk.store(NONE, Ordering::SeqCst);
        self.directory_release.notify_one();
        self.map_release.notify_one();
        self.chunk_release.notify_one();
    }

    fn assert_no_content(&self) {
        assert_eq!(self.inner.maps.load(Ordering::SeqCst), 0);
        assert_eq!(self.inner.chunks.load(Ordering::SeqCst), 0);
        assert_eq!(self.inner.raw.load(Ordering::SeqCst), 0);
        assert_eq!(self.inner.objects.load(Ordering::SeqCst), 0);
    }
}

struct ReleaseGuard(Arc<OnlineFixture>);
impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn short_expiry() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3;
    let expiry = time::OffsetDateTime::from_unix_timestamp(seconds as i64).unwrap();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        expiry.year(),
        u8::from(expiry.month()),
        expiry.day(),
        expiry.hour(),
        expiry.minute(),
        expiry.second()
    )
}

async fn online_capabilities() -> Json<Value> {
    Json(json!({
        "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true,
                     "metadata_pages": false, "objects": true, "chunk_reads": true, "raw_blob": true}
    }))
}

async fn online_resolve(State(f): State<Arc<OnlineFixture>>) -> Json<Value> {
    let d = &f.descriptor;
    Json(json!({
        "descriptor": {"schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE,
            "namespace_view_id": id(&d.namespace_view_id), "scope": "/project", "materialization_policy": 1,
            "fs_semantics": 1, "access_projection": 0, "metadata_root": id(&d.metadata_root),
            "snapshot_id": f.sid()},
        "lease_id": ONLINE_LEASE,
        "lease_expires_at": if f.short_lease { short_expiry() } else { "2099-01-01T00:00:00Z".into() },
        "publication_sequence": "1", "authorization_epoch": "1"
    }))
}

async fn lease_header(request: Request, next: Next) -> Response {
    if !matches!(
        request.uri().path(),
        "/api/v2/snapshots/capabilities" | "/api/v2/snapshots/resolve"
    ) {
        assert_eq!(request.headers()["x-mega-snapshot-lease"], ONLINE_LEASE);
    }
    next.run(request).await
}

fn missing(message: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error":{"code":"PATH_NOT_FOUND","message":message}})),
    )
        .into_response()
}

async fn online_directory(
    State(f): State<Arc<OnlineFixture>>,
    HttpPath(snapshot): HttpPath<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    assert_eq!(snapshot, f.sid());
    let path = &query["path"];
    let cursor = query.get("cursor").cloned();
    f.directory_calls
        .lock()
        .unwrap()
        .push((path.clone(), cursor.clone()));
    let fault = f.directory_fault.load(Ordering::SeqCst);
    if path == "/a" && fault == MISSING_CHILD {
        return missing("parent-advertised child is missing");
    }
    if path == "/" && cursor.is_some() {
        assert_eq!(cursor.as_deref(), Some("root-second"));
        if f.hold_directory.load(Ordering::SeqCst) {
            f.directory_entered.notify_one();
            f.directory_release.notified().await;
        }
        if fault == MISSING_SECOND_PAGE {
            return missing("advertised second root page is missing");
        }
    }
    let mut metadata_root = id(&f.descriptor.metadata_root);
    if fault == WRONG_ROOT {
        metadata_root = id(&[0x93; 32]);
    }
    let (directory_root, entries, count, start, next) = match (path.as_str(), cursor.as_deref()) {
        ("/", None) => (
            id(&f.descriptor.metadata_root),
            vec![json!({"name":"a","fs_kind":"directory","directory_root":id(&f.empty_root)})],
            "2",
            None,
            Some("root-second"),
        ),
        ("/", Some("root-second")) => (
            id(&f.descriptor.metadata_root),
            vec![
                json!({"name":"large.bin","fs_kind":"regular", "size":f.inner.map.file_size.to_string(),"content_digest":id(&f.inner.advertised)}),
            ],
            "2",
            Some("a"),
            if fault == LOOP_CURSOR {
                Some("root-second")
            } else {
                None
            },
        ),
        ("/a", None) => (id(&f.empty_root), vec![], "0", None, None),
        _ => panic!("unexpected directory request: {path:?} {cursor:?}"),
    };
    Json(json!({
        "snapshot_id":snapshot,"path":path,"metadata_root":metadata_root,
        "directory_root":directory_root,"node_class":"native_tree","lifecycle":"mutable",
        "range_start_exclusive":start,"entries":entries,"entry_count":count,
        "next_cursor":next,"proof_pages":[]
    }))
    .into_response()
}

async fn online_map(
    State(f): State<Arc<OnlineFixture>>,
    HttpPath(snapshot): HttpPath<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    assert_eq!(snapshot, f.sid());
    assert_eq!(query["path"], "/large.bin");
    assert_eq!(query["expected_digest"], id(&f.inner.advertised));
    let Json(mut value) =
        super::map(State(f.inner.clone()), HttpPath(snapshot), Query(query)).await;
    if f.wrong_map_size.load(Ordering::SeqCst) {
        let wrong = ChunkMap::new(
            f.inner.advertised,
            f.inner.map.file_size + 1,
            f.inner.map.pages_root,
        )
        .unwrap();
        value["file_size"] = json!(wrong.file_size.to_string());
        value["map_id"] = json!(id(&wrong.map_id()));
        value["chunk_count"] = json!(wrong.chunk_count.to_string());
        value["page_count"] = json!(wrong.page_count.to_string());
    }
    if f.hold_map.load(Ordering::SeqCst) {
        f.map_entered.notify_one();
        f.map_release.notified().await;
    }
    Json(value)
}

async fn online_leaf(
    State(f): State<Arc<OnlineFixture>>,
    HttpPath(snapshot): HttpPath<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    assert_eq!(snapshot, f.sid());
    super::leaf(State(f.inner.clone()), HttpPath(snapshot), Query(query)).await
}

async fn online_chunks(
    State(f): State<Arc<OnlineFixture>>,
    HttpPath(snapshot): HttpPath<String>,
    body: Bytes,
) -> Response {
    assert_eq!(snapshot, f.sid());
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let item = &items[0];
    let index: u64 = item["chunk_index"].as_str().unwrap().parse().unwrap();
    assert_eq!(item["path"], "/large.bin");
    assert_eq!(item["map_id"], id(&f.inner.map.map_id()));
    assert_eq!(item["expected_digest"], id(&f.inner.advertised));
    f.inner.chunks.fetch_add(1, Ordering::SeqCst);
    let held = f.hold_chunk.load(Ordering::SeqCst) == index;
    if held && f.chunk_stage.load(Ordering::SeqCst) == BEFORE_HEADERS {
        f.chunk_entered.notify_one();
        f.chunk_release.notified().await;
    }
    let length = f.inner.map.chunk_len(index).unwrap();
    let mut payload = ChunkPayload {
        map_id: f.inner.map.map_id(),
        file_content_id: f.inner.advertised,
        chunk_index: index,
        chunk_bytes: vec![pattern(index); length as usize],
    };
    let fault = if index == 1 {
        f.chunk_fault.load(Ordering::SeqCst)
    } else {
        CHUNK_OK
    };
    match fault {
        SHORT_CHUNK => {
            payload.chunk_bytes.pop();
        }
        WRONG_CHUNK_HASH => payload.chunk_bytes[0] ^= 1,
        WRONG_CHUNK_MAP => payload.map_id[0] ^= 1,
        WRONG_CHUNK_CONTENT => payload.file_content_id[0] ^= 1,
        _ => {}
    }
    let data = payload.encode(21, 0).unwrap();
    let mut end = EndPayload {
        request_item_count: 1,
        unique_unit_count: 1,
        logical_bytes: length,
        request_body_sha256: hash(&body),
    };
    if fault == WRONG_END {
        end.request_body_sha256[0] ^= 1;
    }
    let end = end.encode(21, 1);
    if held && f.chunk_stage.load(Ordering::SeqCst) == AFTER_CHUNK_BODY {
        // The second stream poll signals that actual CHUNK bytes have been
        // supplied to HTTP; END remains withheld, so no chunk can publish.
        let pending = f.clone();
        let stream = futures::stream::once(
            async move { Ok::<_, std::io::Error>(Bytes::from(data)) },
        )
        .chain(futures::stream::once(async move {
            pending.partial_bodies.fetch_add(1, Ordering::SeqCst);
            pending.chunk_entered.notify_one();
            pending.chunk_release.notified().await;
            Ok::<_, std::io::Error>(Bytes::from(end))
        }));
        return Response::builder()
            .header("content-type", "application/vnd.mega.treeframe;version=2")
            .header("x-mega-snapshot-id", snapshot)
            .header("x-mega-request-digest", id(&hash(&body)))
            .body(Body::from_stream(stream))
            .unwrap();
    }
    let mut wire = data;
    wire.extend(end);
    response(&snapshot, &body, wire)
}

async fn online_raw(State(f): State<Arc<OnlineFixture>>) -> Response {
    super::raw(State(f.inner.clone())).await
}

async fn online_objects(State(f): State<Arc<OnlineFixture>>) -> StatusCode {
    f.inner.objects.fetch_add(1, Ordering::SeqCst);
    StatusCode::INTERNAL_SERVER_ERROR
}

async fn online_renew(
    State(f): State<Arc<OnlineFixture>>,
    HttpPath(lease): HttpPath<String>,
    Json(body): Json<Value>,
) -> Response {
    assert_eq!(lease, ONLINE_LEASE);
    assert_eq!(body["lease_seconds"], 600);
    f.renewals.fetch_add(1, Ordering::SeqCst);
    let code = if f.renewal_status == StatusCode::GONE {
        "LEASE_EXPIRED"
    } else {
        "SCOPE_FORBIDDEN"
    };
    (
        f.renewal_status,
        Json(json!({"error":{"code":code,"message":"actual online renewal rejected"}})),
    )
        .into_response()
}

async fn open_online(
    fixture: OnlineFixture,
    temp: &Path,
    limits: crate::snapshot::ContentBudgetLimits,
) -> (Server, Arc<OnlineFixture>, SnapshotReader, DurableStore) {
    let fixture = Arc::new(fixture);
    let app = Router::new()
        .route("/api/v2/snapshots/capabilities", get(online_capabilities))
        .route("/api/v2/snapshots/resolve", post(online_resolve))
        .route("/api/v2/snapshots/leases/{lease}/renew", post(online_renew))
        .route(
            "/api/v2/snapshots/{snapshot}/directory",
            get(online_directory),
        )
        .route("/api/v2/snapshots/{snapshot}/chunk-map", get(online_map))
        .route(
            "/api/v2/snapshots/{snapshot}/chunk-map/pages",
            get(online_leaf),
        )
        .route("/api/v2/snapshots/{snapshot}/chunks", post(online_chunks))
        .route("/api/v2/snapshots/{snapshot}/objects", post(online_objects))
        .route("/api/v2/snapshots/{snapshot}/blob", get(online_raw))
        .layer(axum::middleware::from_fn(lease_header))
        .with_state(fixture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = Server(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap()
    }));
    let reader = SnapshotReader::resolve(crate::snapshot::Mst2Client::new(base), "/project", 600)
        .await
        .unwrap()
        .with_content_limits(limits);
    assert!(!reader.capabilities().features.metadata_pages);
    assert!(reader.capabilities().features.chunk_reads);
    let store =
        DurableStore::open_for_reader(temp.join("view"), temp.join("cas"), &reader).unwrap();
    (server, fixture, reader, store)
}

fn content_limits(output: usize) -> crate::snapshot::ContentBudgetLimits {
    crate::snapshot::ContentBudgetLimits::new(output, 8 * MIB).unwrap()
}

fn assert_walk(fixture: &OnlineFixture, walks: usize) {
    let expected: Vec<_> = (0..walks)
        .flat_map(|_| {
            [
                ("/".to_string(), None),
                ("/a".to_string(), None),
                ("/".to_string(), Some("root-second".to_string())),
            ]
        })
        .collect();
    assert_eq!(*fixture.directory_calls.lock().unwrap(), expected);
}

async fn settled_usage(reader: &SnapshotReader, output: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let usage = reader.content_usage();
            if usage.output_bytes == output && usage.construction_bytes == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled stream kept managed content credits");
}

#[tokio::test]
async fn no_pages_completed_manifest_keeps_tuple_and_reader_authority_boundaries() {
    let temp = tempfile::tempdir().unwrap();
    let size = 3 * MIB as u64 + 7;
    let (_server, fixture, reader, _store) = open_online(
        OnlineFixture::new(size, false),
        temp.path(),
        content_limits(8 * MIB),
    )
    .await;
    let manifest = crate::snapshot::online_file::CompletedOnlineManifest::walk(&reader)
        .await
        .unwrap();
    assert_walk(&fixture, 1);
    let file = &manifest.files()[0];
    for altered in [
        SnapshotFile {
            rel_path: "another.bin".into(),
            ..file.clone()
        },
        SnapshotFile {
            content_digest: id(&[0x79; 32]),
            ..file.clone()
        },
        SnapshotFile {
            size: file.size + 1,
            ..file.clone()
        },
        SnapshotFile {
            fs_kind: "executable".into(),
            ..file.clone()
        },
    ] {
        assert_eq!(
            manifest.range_file(&altered).unwrap_err().code,
            SnapshotErrorCode::IntegrityError
        );
    }
    // Private online authority remains distinct from the strict public open.
    assert_eq!(
        crate::snapshot::OwnedChunkedFile::open(
            &reader,
            &file.rel_path,
            &file.content_digest,
            file.size
        )
        .await
        .err()
        .unwrap()
        .code,
        SnapshotErrorCode::SnapshotNotReady,
    );
    fixture.assert_no_content();
    let token = manifest.range_file(file).unwrap();
    let foreign_temp = tempfile::tempdir().unwrap();
    let (_foreign_server, foreign_fixture, foreign_reader, _foreign_store) = open_online(
        OnlineFixture::new(size, false),
        foreign_temp.path(),
        content_limits(8 * MIB),
    )
    .await;
    assert_eq!(reader.snapshot_id(), foreign_reader.snapshot_id());
    assert_eq!(
        crate::snapshot::OwnedChunkedFile::open_online_path(&foreign_reader, token.clone())
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::IntegrityError,
    );
    foreign_fixture.assert_no_content();
    let range = crate::snapshot::OwnedChunkedFile::open_online_path(&reader, token)
        .await
        .unwrap();
    let bytes = range.read_range_owned(0, 32).await.unwrap();
    assert_eq!(bytes.as_bytes(), &[pattern(0); 32]);
    assert_eq!(fixture.inner.maps.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.inner.chunks.load(Ordering::SeqCst), 1);
    assert!(reader.content_membership.get().is_none());
    drop(range);
    drop(bytes);
    settled_usage(&reader, 0).await;
    settled_usage(&foreign_reader, 0).await;
}

#[tokio::test]
async fn no_pages_public_hydrate_streams_large_file_closure_reopens_and_resumes_without_content() {
    let temp = tempfile::tempdir().unwrap();
    let size = 65 * MIB as u64 + 7;
    let (_server, fixture, reader, store) = open_online(
        OnlineFixture::new(size, false),
        temp.path(),
        content_limits(24 * MIB),
    )
    .await;
    assert!(size > crate::snapshot::client::MAX_BUFFERED_FILE_BYTES);
    let report = store.hydrate(&reader).await.unwrap();
    assert_eq!(
        (
            report.fetched,
            report.resumed,
            report.total_files,
            report.bytes_total
        ),
        (1, 0, 1, size)
    );
    assert_walk(&fixture, 1);
    assert_eq!(fixture.inner.maps.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.inner.chunks.load(Ordering::SeqCst) as u64,
        fixture.inner.map.chunk_count
    );
    assert_eq!(fixture.inner.raw.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.inner.objects.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.completion_kind().unwrap(),
        Some(CompletionKind::FileClosure)
    );
    assert!(store.is_complete().unwrap());
    assert!(!store.is_snapshot_complete().unwrap());
    assert_eq!(store.manifest().unwrap()[0].rel_path, "large.bin");
    assert!(store
        .verify_blob(
            &id(&fixture.inner.whole),
            size,
            CasVerificationReason::CompletionAudit
        )
        .unwrap());
    assert!(reader.content_membership.get().is_none());
    settled_usage(&reader, 0).await;
    let reopened =
        DurableStore::open_for_reader(store.root(), store.content_dir(), &reader).unwrap();
    assert_eq!(
        reopened.completion_kind().unwrap(),
        Some(CompletionKind::FileClosure)
    );
    assert_eq!(
        reopened
            .read_verified_blob_range(&id(&fixture.inner.whole), size, size - 4, 20)
            .unwrap()
            .unwrap(),
        vec![pattern(65); 4]
    );
    let warm = reopened.hydrate(&reader).await.unwrap();
    assert_eq!(
        (
            warm.fetched,
            warm.resumed,
            warm.total_files,
            warm.bytes_total
        ),
        (0, 1, 1, size)
    );
    assert_walk(&fixture, 2);
    assert_eq!(fixture.inner.maps.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.inner.chunks.load(Ordering::SeqCst) as u64,
        fixture.inner.map.chunk_count
    );
    assert_eq!(fixture.inner.raw.load(Ordering::SeqCst), 0);
    assert!(reader.content_membership.get().is_none());
}

#[tokio::test]
async fn no_pages_missing_second_page_child_loop_and_wrong_root_never_start_content() {
    for fault in [MISSING_SECOND_PAGE, MISSING_CHILD, LOOP_CURSOR, WRONG_ROOT] {
        let temp = tempfile::tempdir().unwrap();
        let (_server, fixture, reader, store) = open_online(
            OnlineFixture::new(3 * MIB as u64 + 7, false),
            temp.path(),
            content_limits(8 * MIB),
        )
        .await;
        fixture.directory_fault.store(fault, Ordering::SeqCst);
        let error = store.hydrate(&reader).await.unwrap_err();
        assert_eq!(
            error.code,
            if matches!(fault, MISSING_SECOND_PAGE | MISSING_CHILD) {
                SnapshotErrorCode::PathNotFound
            } else {
                SnapshotErrorCode::IntegrityError
            },
            "fault {fault}"
        );
        fixture.assert_no_content();
        unpublished(&store, &fixture.inner.advertised);
        assert!(store.try_transaction().unwrap().is_some());
        assert!(reader.content_membership.get().is_none());
        settled_usage(&reader, 0).await;
        fixture
            .directory_fault
            .store(DIRECTORY_OK, Ordering::SeqCst);
        let report = store.hydrate(&reader).await.unwrap();
        assert_eq!(report.fetched, 1);
        assert_eq!(
            store.completion_kind().unwrap(),
            Some(CompletionKind::FileClosure)
        );
    }
}

#[tokio::test]
async fn no_pages_partial_directory_walk_cancellation_mints_no_stream_authority() {
    let temp = tempfile::tempdir().unwrap();
    let (_server, fixture, reader, store) = open_online(
        OnlineFixture::new(3 * MIB as u64 + 7, false),
        temp.path(),
        content_limits(8 * MIB),
    )
    .await;
    let _release = ReleaseGuard(fixture.clone());
    fixture.hold_directory.store(true, Ordering::SeqCst);
    let mut operation = Box::pin(store.hydrate(&reader));
    tokio::select! {
        result = &mut operation => panic!("partial walk unexpectedly completed: {result:?}"),
        _ = fixture.directory_entered.notified() => {},
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("second directory page was never requested"),
    }
    assert_walk(&fixture, 1);
    drop(operation);
    fixture.release();
    fixture.assert_no_content();
    unpublished(&store, &fixture.inner.advertised);
    assert!(store.try_transaction().unwrap().is_some());
    assert!(reader.content_membership.get().is_none());
    settled_usage(&reader, 0).await;
    store.hydrate(&reader).await.unwrap();
    assert_eq!(
        store.completion_kind().unwrap(),
        Some(CompletionKind::FileClosure)
    );
}

#[tokio::test]
async fn no_pages_map_chunk_and_end_errors_never_publish_and_retry_the_actual_stream() {
    for fault in 0..=WRONG_END {
        let temp = tempfile::tempdir().unwrap();
        let (_server, fixture, reader, store) = open_online(
            OnlineFixture::new(3 * MIB as u64 + 7, false),
            temp.path(),
            content_limits(8 * MIB),
        )
        .await;
        fixture.wrong_map_size.store(fault == 0, Ordering::SeqCst);
        fixture.chunk_fault.store(fault, Ordering::SeqCst);
        assert_eq!(
            store.hydrate(&reader).await.unwrap_err().code,
            SnapshotErrorCode::DigestMismatch,
            "fault {fault}"
        );
        assert_walk(&fixture, 1);
        assert_eq!(fixture.inner.maps.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture.inner.chunks.load(Ordering::SeqCst),
            if fault == 0 { 0 } else { 2 }
        );
        assert_eq!(fixture.inner.raw.load(Ordering::SeqCst), 0);
        unpublished(&store, &fixture.inner.advertised);
        assert!(store.try_transaction().unwrap().is_some());
        settled_usage(&reader, 0).await;
        fixture.wrong_map_size.store(false, Ordering::SeqCst);
        fixture.chunk_fault.store(CHUNK_OK, Ordering::SeqCst);
        let before = fixture.inner.chunks.load(Ordering::SeqCst);
        store.hydrate(&reader).await.unwrap();
        assert_eq!(fixture.inner.maps.load(Ordering::SeqCst), 2);
        assert_eq!(
            fixture.inner.chunks.load(Ordering::SeqCst) - before,
            fixture.inner.map.chunk_count as usize
        );
        assert_eq!(
            store.completion_kind().unwrap(),
            Some(CompletionKind::FileClosure)
        );
    }
}

#[tokio::test]
async fn no_pages_verified_chunks_with_wrong_whole_digest_never_complete() {
    let temp = tempfile::tempdir().unwrap();
    let (_server, fixture, reader, store) = open_online(
        OnlineFixture::new(3 * MIB as u64 + 7, true),
        temp.path(),
        content_limits(8 * MIB),
    )
    .await;
    assert_eq!(
        store.hydrate(&reader).await.unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(
        fixture.inner.chunks.load(Ordering::SeqCst) as u64,
        fixture.inner.map.chunk_count
    );
    assert_eq!(fixture.inner.raw.load(Ordering::SeqCst), 0);
    unpublished(&store, &fixture.inner.advertised);
    assert!(!store.blob_path(&id(&fixture.inner.whole)).unwrap().exists());
    settled_usage(&reader, 0).await;
}

#[tokio::test]
async fn no_pages_output_budget_rejects_before_chunk_body_and_low_budget_stream_keeps_progress() {
    let temp = tempfile::tempdir().unwrap();
    let (_server, fixture, reader, store) = open_online(
        OnlineFixture::new(6 * MIB as u64 + 7, false),
        temp.path(),
        content_limits(MIB + 1024),
    )
    .await;
    assert_eq!(
        store.hydrate(&reader).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_walk(&fixture, 1);
    assert_eq!(fixture.inner.maps.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.inner.chunks.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.inner.raw.load(Ordering::SeqCst), 0);
    unpublished(&store, &fixture.inner.advertised);
    settled_usage(&reader, 0).await;

    let temp = tempfile::tempdir().unwrap();
    let size = 20 * MIB as u64 + 7;
    let (_server, fixture, reader, store) = open_online(
        OnlineFixture::new(size, false),
        temp.path(),
        content_limits(3 * MIB),
    )
    .await;
    let report = store.hydrate(&reader).await.unwrap();
    assert_eq!((report.fetched, report.bytes_total), (1, size));
    assert_eq!(
        fixture.inner.chunks.load(Ordering::SeqCst) as u64,
        fixture.inner.map.chunk_count
    );
    assert_eq!(fixture.inner.maps.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.inner.raw.load(Ordering::SeqCst), 0);
    assert!(store
        .verify_blob(
            &id(&fixture.inner.whole),
            size,
            CasVerificationReason::CompletionAudit
        )
        .unwrap());
    assert_eq!(
        store.completion_kind().unwrap(),
        Some(CompletionKind::FileClosure)
    );
    settled_usage(&reader, 0).await;
}

#[tokio::test]
async fn no_pages_cancelled_header_and_body_streams_release_only_their_paid_owners() {
    for stage in [BEFORE_HEADERS, AFTER_CHUNK_BODY] {
        let temp = tempfile::tempdir().unwrap();
        let (_server, fixture, reader, store) = open_online(
            OnlineFixture::new(6 * MIB as u64 + 7, false),
            temp.path(),
            content_limits(8 * MIB),
        )
        .await;
        let _release = ReleaseGuard(fixture.clone());
        let (previous, marker, old_digest) = previous_complete(&store, temp.path()).await;
        let manifest = crate::snapshot::online_file::CompletedOnlineManifest::walk(&reader)
            .await
            .unwrap();
        let token = manifest.range_file(&manifest.files()[0]).unwrap();
        let range = crate::snapshot::OwnedChunkedFile::open_online_path(&reader, token)
            .await
            .unwrap();
        let retained = range.read_range_owned(0, 32).await.unwrap();
        assert_eq!(retained.as_bytes(), &[pattern(0); 32]);
        drop(range);
        let baseline = reader.content_usage().output_bytes;
        assert_eq!(baseline, 1024);
        settled_usage(&reader, baseline).await;
        let before = fixture.inner.chunks.load(Ordering::SeqCst);
        let before_wire = reader.client().received_bytes();
        fixture.hold_chunk.store(1, Ordering::SeqCst);
        fixture.chunk_stage.store(stage, Ordering::SeqCst);
        let mut operation = Box::pin(store.hydrate(&reader));
        tokio::select! {
            result = &mut operation => panic!("blocked stream unexpectedly completed: {result:?}"),
            _ = fixture.chunk_entered.notified() => {},
            _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("chunk gate was never reached"),
        }
        assert_eq!(fixture.inner.chunks.load(Ordering::SeqCst) - before, 2);
        assert_eq!(
            fixture.partial_bodies.load(Ordering::SeqCst),
            usize::from(stage == AFTER_CHUNK_BODY)
        );
        if stage == AFTER_CHUNK_BODY {
            let header = mst2_codec::treeframe::HEADER_LEN as u64;
            let minimum = 2 * (CHUNK_SIZE as u64 + header + 76) + header + 48;
            // Keep polling the operation until the client's actual wire meter
            // records both CHUNK frames; the second END remains withheld.
            tokio::select! {
                result = &mut operation => panic!("withheld END unexpectedly completed: {result:?}"),
                result = tokio::time::timeout(Duration::from_secs(5), async {
                    while reader.client().received_bytes() - before_wire < minimum {
                        tokio::task::yield_now().await;
                    }
                }) => result.expect("client did not receive the actual CHUNK body before cancellation"),
            }
            assert!(reader.client().received_bytes() - before_wire >= minimum);
        }
        let pending_usage = reader.content_usage();
        assert!(pending_usage.output_bytes > baseline);
        assert!(pending_usage.output_bytes <= 8 * MIB);
        assert!(pending_usage.construction_bytes <= 8 * MIB);
        drop(operation);
        fixture.release();
        settled_usage(&reader, baseline).await;
        assert_eq!(retained.as_bytes(), &[pattern(0); 32]);
        unpublished(&store, &fixture.inner.advertised);
        assert!(store.try_transaction().unwrap().is_some());
        previous_unchanged(&previous, &marker, &old_digest);
        drop(retained);
        settled_usage(&reader, 0).await;
        store.hydrate(&reader).await.unwrap();
        assert_eq!(
            store.completion_kind().unwrap(),
            Some(CompletionKind::FileClosure)
        );
        previous_unchanged(&previous, &marker, &old_digest);
    }
}

async fn renewal_rejected(
    fixture: &OnlineFixture,
    reader: &SnapshotReader,
    expected: SnapshotErrorCode,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.renewals.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real renewal endpoint was never reached");
    assert_eq!(reader.ensure_lease().await.unwrap_err().code, expected);
}

#[tokio::test]
async fn no_pages_manifest_and_pending_chunk_replies_reject_real_403_and_410_renewals() {
    for status in [StatusCode::FORBIDDEN, StatusCode::GONE] {
        for at_map in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            let mut config = OnlineFixture::new(3 * MIB as u64 + 7, false);
            config.short_lease = true;
            config.renewal_status = status;
            let (_server, fixture, reader, store) =
                open_online(config, temp.path(), content_limits(8 * MIB)).await;
            let _release = ReleaseGuard(fixture.clone());
            let expected = if status == StatusCode::FORBIDDEN {
                SnapshotErrorCode::ScopeForbidden
            } else {
                SnapshotErrorCode::LeaseExpired
            };
            fixture.hold_map.store(at_map, Ordering::SeqCst);
            if !at_map {
                fixture.hold_chunk.store(0, Ordering::SeqCst);
                fixture.chunk_stage.store(BEFORE_HEADERS, Ordering::SeqCst);
            }
            let mut operation = Box::pin(store.hydrate(&reader));
            let entered = if at_map {
                &fixture.map_entered
            } else {
                &fixture.chunk_entered
            };
            tokio::select! {
                result = &mut operation => panic!("held operation unexpectedly completed: {result:?}"),
                _ = entered.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("post-manifest network gate was never reached"),
            }
            assert_walk(&fixture, 1);
            assert_eq!(fixture.inner.maps.load(Ordering::SeqCst), 1);
            assert_eq!(
                fixture.inner.chunks.load(Ordering::SeqCst),
                usize::from(!at_map)
            );
            // Concurrently poll the operation so the actual HTTP response is
            // still pending while the real background renewal latches failure.
            tokio::select! {
                result = &mut operation => panic!("pending response escaped before release: {result:?}"),
                _ = renewal_rejected(&fixture, &reader, expected) => {},
            }
            fixture.release();
            assert_eq!(operation.await.unwrap_err().code, expected);
            assert_eq!(fixture.renewals.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.inner.raw.load(Ordering::SeqCst), 0);
            unpublished(&store, &fixture.inner.advertised);
            assert!(store.try_transaction().unwrap().is_some());
            assert!(reader.content_membership.get().is_none());
            settled_usage(&reader, 0).await;
        }
    }
}
