//! Actual workspace HTTP regressions for the single-file prefetch window.

use axum::http::StatusCode;
use tokio::sync::Mutex;

use super::*;
use crate::snapshot::{ContentBudgetLimits, FetchCoordinator};

const NO_CHUNK: usize = usize::MAX;
static PREFETCH_TESTS: Mutex<()> = Mutex::const_new(());

struct ChunkControls {
    fixture: Arc<Fixture>,
    held: AtomicUsize,
    seen: AtomicUsize,
    returned: AtomicUsize,
    fail_index: AtomicUsize,
    wrong_index: AtomicUsize,
    short_index: AtomicUsize,
    release: Notify,
}

impl ChunkControls {
    fn release_indices(&self, mask: usize) {
        self.held.fetch_and(!mask, Ordering::SeqCst);
        self.release.notify_waiters();
    }

    fn release_all(&self) {
        self.release_indices(usize::MAX);
    }
}

fn single_file_fixture() -> Fixture {
    let mut fixture = Fixture::new(true, false, false);
    fixture.pages.clear();
    fixture.bodies.clear();
    fixture.maps.clear();
    let mut bytes = Vec::new();
    for index in 0..4 {
        bytes.extend(vec![0x31 + index as u8; CHUNK_SIZE as usize]);
    }
    bytes.extend_from_slice(b"the-end");
    let leaf = ChunkLeaf {
        page_index: 0,
        chunk_sha256: bytes.chunks(CHUNK_SIZE as usize).map(hash).collect(),
    };
    let map = ChunkMap::new(hash(&bytes), bytes.len() as u64, leaf.leaf_hash().unwrap()).unwrap();
    let small = b"small object".to_vec();
    let root = Page::build(&[
        Entry::file(
            EntryKind::Regular,
            b"large0",
            bytes.len() as u64,
            hash(&bytes),
        ),
        Entry::file(
            EntryKind::Regular,
            b"small",
            small.len() as u64,
            hash(&small),
        ),
    ])
    .unwrap();
    fixture.descriptor.metadata_root = page_id(&root);
    fixture.pages.insert("/".into(), root);
    fixture.bodies.insert("/large0".into(), bytes);
    fixture.bodies.insert("/small".into(), small);
    fixture.maps.insert("/large0".into(), (map, leaf));
    fixture
}

async fn prefetch_chunks(State(control): State<Arc<ChunkControls>>, body: Bytes) -> Response {
    let fixture = &control.fixture;
    let value: Value = serde_json::from_slice(&body).unwrap();
    let items = value["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let item = &items[0];
    assert_eq!(item["path"], "/large0");
    let index: usize = item["chunk_index"].as_str().unwrap().parse().unwrap();
    assert!(index < 5);
    let bit = 1 << index;
    let (map, _) = &fixture.maps["/large0"];
    assert_eq!(item["expected_digest"], id(&map.file_content_id));
    assert_eq!(item["map_id"], id(&map.map_id()));
    fixture.chunk_calls.fetch_add(1, Ordering::SeqCst);
    let _active = active(&fixture.chunk_active, &fixture.chunk_peak);
    control.seen.fetch_or(bit, Ordering::SeqCst);
    loop {
        let release = control.release.notified();
        tokio::pin!(release);
        release.as_mut().enable();
        if control.held.load(Ordering::SeqCst) & bit == 0 {
            break;
        }
        release.await;
    }
    if control.fail_index.load(Ordering::SeqCst) == index {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"code":"INVALID_REQUEST","message":"late chunk failure","request_id":"prefetch-error","retryable":false}})),
        )
            .into_response();
    }
    let bytes = fixture.bodies["/large0"]
        .chunks(CHUNK_SIZE as usize)
        .nth(index)
        .unwrap();
    let mut response_bytes = bytes.to_vec();
    if control.short_index.load(Ordering::SeqCst) == index {
        response_bytes.pop().unwrap();
    }
    control.returned.fetch_or(bit, Ordering::SeqCst);
    fixture.frame(
        &body,
        ChunkPayload {
            map_id: map.map_id(),
            file_content_id: map.file_content_id,
            chunk_index: if control.wrong_index.load(Ordering::SeqCst) == index {
                (index + 1) as u64
            } else {
                index as u64
            },
            chunk_bytes: response_bytes,
        }
        .encode(7, 0)
        .unwrap(),
        1,
        1,
        bytes.len(),
    )
}

struct PrefetchServer {
    control: Arc<ChunkControls>,
    client: Mst2Client,
    task: tokio::task::JoinHandle<()>,
}

impl PrefetchServer {
    async fn new(held: usize) -> Self {
        let fixture = Arc::new(single_file_fixture());
        let control = Arc::new(ChunkControls {
            fixture: fixture.clone(),
            held: AtomicUsize::new(held),
            seen: AtomicUsize::new(0),
            returned: AtomicUsize::new(0),
            fail_index: AtomicUsize::new(NO_CHUNK),
            wrong_index: AtomicUsize::new(NO_CHUNK),
            short_index: AtomicUsize::new(NO_CHUNK),
            release: Notify::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let app = Router::new()
            .route(
                "/api/v2/snapshots/capabilities",
                get(caps).with_state(fixture.clone()),
            )
            .route(
                "/api/v2/snapshots/resolve",
                post(resolve).with_state(fixture.clone()),
            )
            .route(
                "/api/v2/snapshots/{sid}/metadata/pages",
                post(metadata).with_state(fixture.clone()),
            )
            .route(
                "/api/v2/snapshots/{sid}/objects",
                post(objects).with_state(fixture.clone()),
            )
            .route(
                "/api/v2/snapshots/{sid}/chunk-map",
                get(map).with_state(fixture.clone()),
            )
            .route(
                "/api/v2/snapshots/{sid}/chunk-map/pages",
                get(leaf).with_state(fixture.clone()),
            )
            .route(
                "/api/v2/snapshots/{sid}/chunks",
                post(prefetch_chunks).with_state(control.clone()),
            )
            .route(
                "/api/v2/snapshots/{sid}/blob",
                get(blob).with_state(fixture),
            );
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            control,
            client,
            task,
        }
    }

    async fn reader(&self) -> SnapshotReader {
        SnapshotReader::resolve_request(
            self.client.clone(),
            &crate::snapshot::ResolveRequest::latest("/project", 60),
        )
        .await
        .unwrap()
    }
}

impl Drop for PrefetchServer {
    fn drop(&mut self) {
        self.control.release_all();
        self.task.abort();
    }
}

async fn wait_for_prefetch(condition: impl Fn() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("real workspace chunk request condition did not become true");
}

fn store_for_prefetch(temp: &tempfile::TempDir, reader: &SnapshotReader) -> Arc<DurableStore> {
    Arc::new(
        DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555530",
            reader,
        )
        .unwrap(),
    )
}

fn large_cas_name(store: &DurableStore, fixture: &Fixture) -> std::path::PathBuf {
    store.content_dir().join(
        digest_of(&fixture.bodies["/large0"])
            .strip_prefix("sha256:")
            .unwrap(),
    )
}

fn assert_no_large_publication(store: &DurableStore, reader: &SnapshotReader, fixture: &Fixture) {
    assert!(!large_cas_name(store, fixture).exists());
    assert!(!store.root().join("DURABLE_COMPLETE").exists());
    assert!(!matches!(
        store.local_pin_state().unwrap(),
        LocalPinState::Complete(_)
    ));
    assert!(!store.is_snapshot_complete().unwrap());
    assert_eq!(reader.content_usage().output_bytes, 0);
    assert_eq!(reader.content_usage().construction_bytes, 0);
    for entry in std::fs::read_dir(store.content_dir()).unwrap() {
        assert!(!entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp."));
    }
}

async fn assert_two_pending_ranges(control: &ChunkControls, reader: &SnapshotReader, mask: usize) {
    wait_for_prefetch(|| {
        control.seen.load(Ordering::SeqCst) & mask == mask
            && control.fixture.chunk_active.load(Ordering::SeqCst) == 2
    })
    .await;
    assert_eq!(control.fixture.chunk_peak.load(Ordering::SeqCst), 2);
    let local = reader.content_usage();
    assert!(local.output_bytes >= 4 * 1024 * 1024 && local.output_bytes <= 128 * 1024 * 1024);
    assert!(local.construction_bytes > 0 && local.construction_bytes <= 32 * 1024 * 1024);
    let process = FetchCoordinator::process_content_usage();
    assert!(
        process.output_bytes >= local.output_bytes && process.output_bytes <= 512 * 1024 * 1024
    );
    assert!(
        process.construction_bytes >= local.construction_bytes
            && process.construction_bytes <= 64 * 1024 * 1024
    );
}

async fn assert_partial_window_written(store: &DurableStore) {
    wait_for_prefetch(|| {
        std::fs::read_dir(store.content_dir())
            .unwrap()
            .any(|entry| {
                let entry = entry.unwrap();
                entry.file_name().to_string_lossy().contains(".tmp.")
                    && entry.metadata().unwrap().len() == 2 * CHUNK_SIZE as u64
            })
    })
    .await;
}

#[tokio::test]
async fn workspace_single_large_prefetches_distinct_ranges_and_writes_in_order() {
    let _serial = PREFETCH_TESTS.lock().await;
    let server = PrefetchServer::new(0b11).await;
    let reader = server.reader().await;
    let temp = tempfile::tempdir().unwrap();
    let store = store_for_prefetch(&temp, &reader);
    let target = store.clone();
    let source = reader.clone();
    let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
    assert_two_pending_ranges(&server.control, &reader, 0b11).await;
    assert!(!large_cas_name(&store, &server.control.fixture).exists());
    assert!(!store.root().join("DURABLE_COMPLETE").exists());
    server.control.release_indices(0b10);
    wait_for_prefetch(|| {
        server.control.returned.load(Ordering::SeqCst) & 0b10 != 0
            && server.control.fixture.chunk_active.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(server.control.returned.load(Ordering::SeqCst) & 0b1, 0);
    assert!(!large_cas_name(&store, &server.control.fixture).exists());
    assert!(!store.root().join("DURABLE_COMPLETE").exists());
    server.control.release_all();
    let report = tokio::time::timeout(std::time::Duration::from_secs(30), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(report.complete);
    assert_eq!(report.fetched, 2);
    assert_eq!(server.control.fixture.chunk_calls.load(Ordering::SeqCst), 5);
    assert_eq!(server.control.fixture.chunk_peak.load(Ordering::SeqCst), 2);
    assert_eq!(server.control.fixture.map_calls.load(Ordering::SeqCst), 1);
    // Cold concurrent ranges may each fetch this one authenticated leaf.
    assert!((1..=2).contains(&server.control.fixture.leaf_calls.load(Ordering::SeqCst)));
    assert_full_snapshot(&store, &reader, &server.control.fixture);
    assert_eq!(reader.content_usage().output_bytes, 0);
    assert_eq!(reader.content_usage().construction_bytes, 0);
}

#[tokio::test]
async fn late_failure_wrong_index_or_short_range_drops_pending_peer_and_can_retry() {
    let _serial = PREFETCH_TESTS.lock().await;
    for mode in 0..3 {
        let server = PrefetchServer::new(0b1100).await;
        let reader = server.reader().await;
        let temp = tempfile::tempdir().unwrap();
        let store = store_for_prefetch(&temp, &reader);
        let target = store.clone();
        let source = reader.clone();
        let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
        assert_two_pending_ranges(&server.control, &reader, 0b1100).await;
        assert_partial_window_written(&store).await;
        let expected = match mode {
            0 => {
                server.control.fail_index.store(3, Ordering::SeqCst);
                SnapshotErrorCode::InvalidRequest
            }
            1 => {
                server.control.wrong_index.store(3, Ordering::SeqCst);
                SnapshotErrorCode::DigestMismatch
            }
            2 => {
                server.control.short_index.store(3, Ordering::SeqCst);
                SnapshotErrorCode::DigestMismatch
            }
            _ => unreachable!(),
        };
        server.control.release_indices(0b1000);
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, expected);
        assert_no_large_publication(&store, &reader, &server.control.fixture);
        server.control.fail_index.store(NO_CHUNK, Ordering::SeqCst);
        server.control.wrong_index.store(NO_CHUNK, Ordering::SeqCst);
        server.control.short_index.store(NO_CHUNK, Ordering::SeqCst);
        server.control.release_all();
        wait_for_prefetch(|| server.control.fixture.chunk_active.load(Ordering::SeqCst) == 0).await;
        assert!(bounded_hydrate(&store, &reader).await.complete);
        assert_full_snapshot(&store, &reader, &server.control.fixture);
        assert!(server.control.fixture.chunk_peak.load(Ordering::SeqCst) <= 2);
        assert_eq!(reader.content_usage().output_bytes, 0);
        assert_eq!(reader.content_usage().construction_bytes, 0);
    }
}

#[tokio::test]
async fn cancelled_late_prefetch_window_refunds_owners_and_temp_then_can_retry() {
    let _serial = PREFETCH_TESTS.lock().await;
    let server = PrefetchServer::new(0b1100).await;
    let reader = server.reader().await;
    let temp = tempfile::tempdir().unwrap();
    let store = store_for_prefetch(&temp, &reader);
    let target = store.clone();
    let source = reader.clone();
    let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
    assert_two_pending_ranges(&server.control, &reader, 0b1100).await;
    assert_partial_window_written(&store).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_no_large_publication(&store, &reader, &server.control.fixture);
    server.control.release_all();
    wait_for_prefetch(|| server.control.fixture.chunk_active.load(Ordering::SeqCst) == 0).await;
    assert!(bounded_hydrate(&store, &reader).await.complete);
    assert_full_snapshot(&store, &reader, &server.control.fixture);
    assert!(server.control.fixture.chunk_peak.load(Ordering::SeqCst) <= 2);
    assert_eq!(reader.content_usage().output_bytes, 0);
    assert_eq!(reader.content_usage().construction_bytes, 0);
}

#[tokio::test]
async fn lowered_limits_or_existing_paid_owner_keep_workspace_ranges_sequential() {
    let _serial = PREFETCH_TESTS.lock().await;
    for mode in 0..3 {
        let server = PrefetchServer::new(0b11).await;
        let reader = server.reader().await.with_content_limits(match mode {
            0 => ContentBudgetLimits::new(4 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
            1 => ContentBudgetLimits::new(128 * 1024 * 1024, 3 * 1024 * 1024).unwrap(),
            2 => ContentBudgetLimits::default(),
            _ => unreachable!(),
        });
        let owner = if mode == 2 {
            let closure = reader.snapshot_closure().await.unwrap();
            let small = closure
                .files()
                .iter()
                .find(|file| file.rel_path == "small")
                .unwrap();
            Some(reader.read_content(small, false).await.unwrap())
        } else {
            None
        };
        let held_output = reader.content_usage().output_bytes;
        assert_eq!(held_output > 0, mode == 2);
        let temp = tempfile::tempdir().unwrap();
        let store = store_for_prefetch(&temp, &reader);
        let target = store.clone();
        let source = reader.clone();
        let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
        wait_for_prefetch(|| server.control.seen.load(Ordering::SeqCst) & 0b1 != 0).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(server.control.seen.load(Ordering::SeqCst), 0b1);
        assert_eq!(
            server.control.fixture.chunk_active.load(Ordering::SeqCst),
            1
        );
        server.control.release_all();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(30), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .complete
        );
        assert_full_snapshot(&store, &reader, &server.control.fixture);
        assert_eq!(server.control.fixture.chunk_peak.load(Ordering::SeqCst), 1);
        assert_eq!(reader.content_usage().output_bytes, held_output);
        assert_eq!(reader.content_usage().construction_bytes, 0);
        drop(owner);
        assert_eq!(reader.content_usage().output_bytes, 0);
    }
}
