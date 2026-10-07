use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
};

use axum::{
    body::{Body, Bytes},
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use mst2_codec::{
    descriptor::ServingDescriptor,
    metapage::BranchChild,
    treeframe::{EndPayload, MetaPayload, ObjectPayload},
};
use serde_json::{json, Value};

use super::*;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const INSTANCE: &str = "11111111-2222-4333-8444-555555555564";
const BODY: &[u8] = b"proven path content";
fn hash(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}
fn id(hash: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(hash))
}

type Route = (String, Vec<u8>);
type Witnesses = BTreeMap<Route, Vec<Vec<u8>>>;
struct Fixture {
    descriptor: ServingDescriptor,
    witnesses: Witnesses,
    pages: BTreeMap<String, Vec<u8>>,
    requests: Mutex<Vec<Route>>,
    bodies: AtomicUsize,
    mode: AtomicUsize,
    paused: AtomicBool,
    started: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    expiry: String,
    epoch: AtomicUsize,
    renewals: AtomicUsize,
}
fn add_directory(
    directory: &str,
    entries: &[Entry],
    witnesses: &mut Witnesses,
    pages: &mut BTreeMap<String, Vec<u8>>,
) -> [u8; 32] {
    let root = Page::build(entries).unwrap();
    let mut routes = vec![Vec::new()];
    while let Some(route) = routes.pop() {
        let chain = Page::pages_along_route(entries, &route).unwrap();
        let last = chain.last().unwrap();
        if let Page::Branch { children, .. } = decode_page(last).unwrap() {
            for child in children {
                let mut next = route.clone();
                next.push(child.label);
                routes.push(next);
            }
        }
        pages.insert(id(&page_id(last)), last.clone());
        witnesses.insert((directory.into(), route), chain);
    }
    page_id(&root)
}
impl Fixture {
    fn wide() -> Self {
        let mut witnesses = BTreeMap::new();
        let mut pages = BTreeMap::new();
        let mut entries = vec![Entry::file(
            EntryKind::Executable,
            b"abc",
            BODY.len() as u64,
            hash(BODY),
        )];
        for index in 0..130 {
            entries.push(Entry::file(
                EntryKind::Regular,
                format!("abc{index:03}").as_bytes(),
                BODY.len() as u64,
                hash(BODY),
            ));
        }
        entries.push(Entry::file(
            EntryKind::Symlink,
            "é-link".as_bytes(),
            BODY.len() as u64,
            hash(BODY),
        ));
        entries.push(Entry::file(
            EntryKind::Regular,
            "ê-file".as_bytes(),
            BODY.len() as u64,
            hash(BODY),
        ));
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let child = add_directory("/wanted", &entries, &mut witnesses, &mut pages);
        let root = add_directory(
            "/",
            &[
                Entry::dir(b"unavailable", [0x88; 32]),
                Entry::dir(b"wanted", child),
            ],
            &mut witnesses,
            &mut pages,
        );
        Self::from_parts(root, witnesses, pages)
    }
    fn flat(count: usize) -> Self {
        let mut witnesses = BTreeMap::new();
        let mut pages = BTreeMap::new();
        let entries: Vec<_> = (0..count)
            .map(|index| {
                Entry::file(
                    EntryKind::Regular,
                    format!("file{index:03}").as_bytes(),
                    BODY.len() as u64,
                    hash(BODY),
                )
            })
            .collect();
        let root = add_directory("/", &entries, &mut witnesses, &mut pages);
        Self::from_parts(root, witnesses, pages)
    }
    fn deep_radix(depth: usize) -> (Self, String) {
        let name = format!("{}x", "a".repeat(depth));
        let leaf = Page::build(&[Entry::file(
            EntryKind::Regular,
            name.as_bytes(),
            BODY.len() as u64,
            hash(BODY),
        )])
        .unwrap();
        let mut chain = vec![leaf];
        for (count, level) in (1u64..).zip((0..depth).rev()) {
            let root = Page::Branch {
                prefix: vec![b'a'; level],
                terminal: None,
                children: vec![
                    BranchChild {
                        label: b'a',
                        subtree_entries: count,
                        child_page_id: page_id(chain.last().unwrap()),
                    },
                    BranchChild {
                        label: b'z',
                        subtree_entries: 1,
                        child_page_id: [0x99; 32],
                    },
                ],
            }
            .encode()
            .unwrap();
            chain.push(root);
        }
        chain.reverse();
        let witnesses = (0..chain.len())
            .map(|depth| (("/".into(), vec![b'a'; depth]), chain[..=depth].to_vec()))
            .collect();
        let pages = chain
            .iter()
            .map(|page| (id(&page_id(page)), page.clone()))
            .collect();
        (Self::from_parts(page_id(&chain[0]), witnesses, pages), name)
    }

    fn from_parts(root: [u8; 32], witnesses: Witnesses, pages: BTreeMap<String, Vec<u8>>) -> Self {
        Self {
            descriptor: ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
                namespace_view_id: [0x54; 32],
                scope: "/project".into(),
                metadata_root: root,
            },
            witnesses,
            pages,
            requests: Mutex::new(Vec::new()),
            bodies: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            paused: AtomicBool::new(false),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
            expiry: "2099-01-01T00:00:00Z".into(),
            epoch: AtomicUsize::new(1),
            renewals: AtomicUsize::new(0),
        }
    }
}
async fn capabilities() -> Json<Value> {
    Json(
        json!({"protocol_versions":[2],"metadata_codecs":[1],"frame_encodings":["identity"],"features":{"resolve":true,"directory":true,"leases":true,"metadata_pages":true,"objects":true,"raw_blob":true,"chunk_reads":true}}),
    )
}
async fn resolve(State(f): State<Arc<Fixture>>) -> Json<Value> {
    let d = &f.descriptor;
    Json(
        json!({"descriptor":{"schema_version":2,"metadata_codec":1,"instance_id":INSTANCE,"namespace_view_id":id(&d.namespace_view_id),"scope":d.scope,"materialization_policy":1,"fs_semantics":1,"access_projection":0,"metadata_root":id(&d.metadata_root),"snapshot_id":id(&d.snapshot_id().unwrap())},"lease_id":"proven-file-lease","lease_expires_at":f.expiry,"publication_sequence":"1","authorization_epoch":f.epoch.load(Ordering::SeqCst).to_string()}),
    )
}
async fn renew(State(f): State<Arc<Fixture>>) -> (StatusCode, Json<Value>) {
    f.renewals.fetch_add(1, Ordering::SeqCst);
    (
        StatusCode::FORBIDDEN,
        Json(json!({"error":{"code":"SCOPE_FORBIDDEN","message":"revoked proven file lease"}})),
    )
}
fn frame_response(f: &Fixture, request: &[u8], wire: Vec<u8>, pending: bool) -> Response {
    let body = if pending {
        use futures::StreamExt;
        Body::from_stream(
            futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from(wire))])
                .chain(futures::stream::pending()),
        )
    } else {
        Body::from(wire)
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header(
            "x-mega-snapshot-id",
            id(&f.descriptor.snapshot_id().unwrap()),
        )
        .header("x-mega-request-digest", id(&hash(request)))
        .body(body)
        .unwrap()
}
async fn metadata(State(f): State<Arc<Fixture>>, body: Bytes) -> Response {
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["items"].as_array().unwrap().len(), 1);
    let item = &value["items"][0];
    let directory = item["directory_path"].as_str().unwrap().to_owned();
    let route: Vec<u8> = serde_json::from_value(item["route"].clone()).unwrap();
    f.requests
        .lock()
        .unwrap()
        .push((directory.clone(), route.clone()));
    f.started.notify_one();
    if f.paused.load(Ordering::SeqCst) {
        f.release.acquire().await.unwrap().forget();
    }
    let chain = f
        .witnesses
        .get(&(directory, route))
        .expect("unrelated subtree request");
    assert_eq!(item["expected_digest"], id(&page_id(chain.last().unwrap())));
    let mut pages: Vec<_> = chain
        .iter()
        .map(|bytes| (page_id(bytes), bytes.clone()))
        .collect();
    match f.mode.load(Ordering::SeqCst) {
        1 => {
            let (claimed, bytes) = pages.last_mut().unwrap();
            *bytes.last_mut().unwrap() ^= 1;
            *claimed = page_id(bytes);
        }
        2 => {
            let extra = Page::build(&[]).unwrap();
            pages.push((page_id(&extra), extra));
        }
        3 => {
            pages.pop();
        }
        4 => {
            pages.push(pages.last().unwrap().clone());
        }
        _ => {}
    }
    let mut wire = Vec::new();
    let mut sequence = 0;
    for part in pages.chunks(64) {
        wire.extend(
            MetaPayload {
                pages: part.to_vec(),
            }
            .encode(41, sequence)
            .unwrap(),
        );
        sequence += 1;
    }
    let mut digest = hash(&body);
    if f.mode.load(Ordering::SeqCst) == 5 {
        digest[0] ^= 1;
    }
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: pages.len() as u32,
            logical_bytes: pages.iter().map(|(_, bytes)| bytes.len() as u64).sum(),
            request_body_sha256: digest,
        }
        .encode(41, sequence),
    );
    frame_response(&f, &body, wire, f.mode.load(Ordering::SeqCst) == 6)
}
async fn objects(State(f): State<Arc<Fixture>>, body: Bytes) -> Response {
    f.bodies.fetch_add(1, Ordering::SeqCst);
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["items"][0]["expected_digest"], id(&hash(BODY)));
    let mut wire = ObjectPayload {
        objects: vec![(hash(BODY), BODY.to_vec())],
    }
    .encode(42, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: BODY.len() as u64,
            request_body_sha256: hash(&body),
        }
        .encode(42, 1),
    );
    frame_response(&f, &body, wire, false)
}
async fn blob(
    State(f): State<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    f.bodies.fetch_add(1, Ordering::SeqCst);
    assert_eq!(query["expected_digest"], id(&hash(BODY)));
    BODY.into_response()
}
struct Server {
    reader: SnapshotReader,
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
            .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
            .route("/api/v2/snapshots/{sid}/objects", post(objects))
            .route("/api/v2/snapshots/{sid}/blob", get(blob))
            .with_state(fixture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let reader = SnapshotReader::resolve(super::super::Mst2Client::new(url), "/project", 600)
            .await
            .unwrap();
        Self {
            reader,
            fixture,
            task,
        }
    }
}

#[tokio::test]
async fn selective_membership_never_walks_the_unrelated_advertised_subtree_and_drives_owned_content(
) {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::wide()).await;
    for (path, kind) in [
        ("wanted/abc129", "regular"),
        ("wanted/abc", "executable"),
        ("wanted/é-link", "symlink"),
        ("wanted/ê-file", "regular"),
    ] {
        let token = server.reader.prove_file(path).await.unwrap();
        assert_eq!(token.file().rel_path, path);
        assert_eq!(token.file().fs_kind, kind);
        assert_eq!(token.file().size, BODY.len() as u64);
        assert_eq!(
            server
                .reader
                .read_proven_content(&token, true)
                .await
                .unwrap()
                .as_bytes(),
            BODY
        );
    }
    assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 4);
    assert!(server.reader.content_membership.get().is_none());
    assert!(server
        .fixture
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|(dir, _)| matches!(dir.as_str(), "/" | "/wanted")));
    let before = server.fixture.requests.lock().unwrap().len();
    let first = server.reader.prove_file("wanted/abc129").await.unwrap();
    let clone = server
        .reader
        .clone()
        .prove_file("/wanted/abc129")
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first, &clone));
    assert_eq!(server.fixture.requests.lock().unwrap().len(), before);
}

#[tokio::test]
async fn malformed_witness_end_and_actual_cancellation_publish_no_token_and_good_retry_works() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::flat(1)).await;
    for mode in 1..=5 {
        server.fixture.mode.store(mode, Ordering::SeqCst);
        assert!(
            server.reader.prove_file("file000").await.is_err(),
            "mode{mode}"
        );
        assert!(server
            .reader
            .path_membership
            .table
            .lock()
            .unwrap()
            .entries
            .iter()
            .flatten()
            .all(|cell| cell.token.get().is_none()));
    }
    server.fixture.mode.store(6, Ordering::SeqCst);
    let notified = server.fixture.started.notified();
    let task = tokio::spawn({
        let reader = server.reader.clone();
        async move { reader.prove_file("file000").await }
    });
    tokio::time::timeout(Duration::from_secs(5), notified)
        .await
        .unwrap();
    assert!(!task.is_finished());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        server
            .reader
            .path_membership
            .initializers
            .available_permits(),
        4
    );
    assert_eq!(
        server.reader.path_membership.callers.available_permits(),
        128
    );
    server.fixture.mode.store(0, Ordering::SeqCst);
    assert!(server.reader.prove_file("file000").await.is_ok());
    assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn same_path_waiters_share_the_actual_flight_and_cancelled_initializer_is_retried() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::flat(1)).await;
    server.fixture.paused.store(true, Ordering::SeqCst);
    let first = tokio::spawn({
        let reader = server.reader.clone();
        async move { reader.prove_file("file000").await }
    });
    tokio::time::timeout(Duration::from_secs(5), server.fixture.started.notified())
        .await
        .unwrap();
    let second = tokio::spawn({
        let reader = server.reader.clone();
        async move { reader.prove_file("file000").await }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.reader.path_membership.callers.available_permits() != 126 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(server.fixture.requests.lock().unwrap().len(), 1);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.fixture.requests.lock().unwrap().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.fixture.paused.store(false, Ordering::SeqCst);
    server.fixture.release.add_permits(2);
    let token = second.await.unwrap().unwrap();
    assert_eq!(token.file().rel_path, "file000");
    assert_eq!(
        server
            .reader
            .path_membership
            .initializers
            .available_permits(),
        4
    );
    assert_eq!(
        server.reader.path_membership.callers.available_permits(),
        128
    );
}

#[tokio::test]
async fn complete_seed_issues_no_metadata_and_foreign_domain_tokens_reject_before_body_http() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::flat(2)).await;
    let closure = super::super::ValidatedSnapshotClosure::from_pages(
        server.reader.descriptor(),
        server.fixture.pages.clone(),
    )
    .unwrap();
    server.reader.seed_content_membership(&closure).unwrap();
    let token = server.reader.prove_file("file000").await.unwrap();
    assert!(server.fixture.requests.lock().unwrap().is_empty());
    assert_eq!(
        server
            .reader
            .read_proven_content(&token, false)
            .await
            .unwrap()
            .as_bytes(),
        BODY
    );
    let foreign = Server::start(Fixture::flat(2)).await;
    assert_eq!(
        foreign
            .reader
            .read_proven_content(&token, true)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(foreign.fixture.bodies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn initializer_and_caller_admission_reject_before_http_and_restore_on_actual_abort() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::flat(8)).await;
    server.fixture.paused.store(true, Ordering::SeqCst);
    let mut tasks = Vec::new();
    for index in 0..4 {
        let reader = server.reader.clone();
        tasks.push(tokio::spawn(async move {
            reader.prove_file(&format!("file{index:03}")).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.fixture.requests.lock().unwrap().len() < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        server
            .reader
            .prove_file("file004")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(server.fixture.requests.lock().unwrap().len(), 4);
    for task in tasks {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    assert_eq!(
        server
            .reader
            .path_membership
            .initializers
            .available_permits(),
        4
    );
    let held = server
        .reader
        .path_membership
        .callers
        .clone()
        .try_acquire_many_owned(128)
        .unwrap();
    assert_eq!(
        server
            .reader
            .prove_file("file005")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(server.fixture.requests.lock().unwrap().len(), 4);
    drop(held);
    server.fixture.paused.store(false, Ordering::SeqCst);
    server.fixture.release.add_permits(4);
    assert!(server.reader.prove_file("file005").await.is_ok());
}

#[tokio::test]
async fn transferred_token_remains_valid_after_bounded_path_cell_eviction() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::flat(65)).await;
    let retained = server.reader.prove_file("file000").await.unwrap();
    for index in 1..65 {
        server
            .reader
            .prove_file(&format!("file{index:03}"))
            .await
            .unwrap();
    }
    assert_eq!(
        server
            .reader
            .path_membership
            .table
            .lock()
            .unwrap()
            .entries
            .iter()
            .flatten()
            .count(),
        CELLS
    );
    assert_eq!(
        server
            .reader
            .read_proven_content(&retained, true)
            .await
            .unwrap()
            .as_bytes(),
        BODY
    );
    assert_eq!(retained.file().rel_path, "file000");
}

#[test]
fn repeated_witness_work_is_admitted_cumulatively_and_old_witnesses_cannot_be_new_targets() {
    let mut work = ProofWork::new();
    work.bytes = MAX_PROOF_BYTES - PAGE_MAX_BYTES;
    assert!(work.admit(0).is_ok());
    assert_eq!(
        work.admit(1).unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    let mut work = ProofWork::new();
    work.visits = MAX_VISITS - 1;
    assert!(work.admit(0).is_ok());
    assert!(work.admit(1).is_err());
    let mut work = ProofWork::new();
    work.steps = MAX_STEPS;
    assert!(work.admit(0).is_err());
    let mut table = CellTable {
        entries: std::array::from_fn(|_| None),
        next: 0,
    };
    let held: Vec<_> = (0..CELLS)
        .map(|index| table.get(&format!("file{index}")).unwrap())
        .collect();
    assert!(table.get("overflow").is_err());
    drop(held);
    assert!(table.get("retry").is_ok());
}

#[tokio::test]
async fn a_hash_valid_child_with_wrong_partition_or_declared_count_is_not_membership() {
    let _serial = TEST_LOCK.lock().await;
    for (name, count) in [(b"wrong".as_slice(), 1), (b"abfile".as_slice(), 2)] {
        let leaf = Page::build(&[Entry::file(
            EntryKind::Regular,
            name,
            BODY.len() as u64,
            hash(BODY),
        )])
        .unwrap();
        let root = Page::Branch {
            prefix: b"a".to_vec(),
            terminal: None,
            children: vec![
                BranchChild {
                    label: b'b',
                    subtree_entries: count,
                    child_page_id: page_id(&leaf),
                },
                BranchChild {
                    label: b'c',
                    subtree_entries: 1,
                    child_page_id: [0x33; 32],
                },
            ],
        }
        .encode()
        .unwrap();
        let fixture = Fixture::from_parts(
            page_id(&root),
            BTreeMap::from([
                (("/".into(), vec![]), vec![root.clone()]),
                (("/".into(), vec![b'b']), vec![root, leaf]),
            ]),
            BTreeMap::new(),
        );
        let server = Server::start(fixture).await;
        assert_eq!(
            server
                .reader
                .prove_file("abfile")
                .await
                .unwrap_err()
                .snapshot_error()
                .unwrap()
                .code,
            SnapshotErrorCode::IntegrityError
        );
        assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn deep_selected_path_accounts_for_all_repeated_witnesses_before_the_next_http() {
    let _serial = TEST_LOCK.lock().await;
    let (fixture, name) = Fixture::deep_radix(140);
    let server = Server::start(fixture).await;
    assert_eq!(
        server
            .reader
            .prove_file(&name)
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    let requests = server.fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 90);
    assert_eq!(
        requests
            .iter()
            .map(|(_, route)| route.len() + 1)
            .sum::<usize>(),
        4095
    );
    assert!(requests
        .iter()
        .all(|(directory, route)| directory == "/" && route.iter().all(|label| *label == b'a')));
    drop(requests);
    assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 0);
    assert_eq!(
        server
            .reader
            .path_membership
            .initializers
            .available_permits(),
        4
    );
    assert!(server
        .reader
        .path_membership
        .table
        .lock()
        .unwrap()
        .entries
        .iter()
        .flatten()
        .all(|cell| cell.token.get().is_none()));
}

#[tokio::test]
async fn incomplete_old_witness_chain_never_substitutes_for_a_new_target() {
    let _serial = TEST_LOCK.lock().await;
    let (fixture, name) = Fixture::deep_radix(2);
    let server = Server::start(fixture).await;
    server.fixture.paused.store(true, Ordering::SeqCst);
    let task = tokio::spawn({
        let reader = server.reader.clone();
        async move { reader.prove_file(&name).await }
    });
    tokio::time::timeout(Duration::from_secs(5), server.fixture.started.notified())
        .await
        .unwrap();
    server.fixture.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.fixture.requests.lock().unwrap().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.fixture.mode.store(3, Ordering::SeqCst);
    server.fixture.release.add_permits(1);
    assert!(matches!(
        task.await
            .unwrap()
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::IntegrityError | SnapshotErrorCode::DigestMismatch
    ));
    assert_eq!(server.fixture.requests.lock().unwrap().len(), 2);
    assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shared_initializer_deadline_cancels_actual_http_and_restores_admission_for_retry() {
    let _serial = TEST_LOCK.lock().await;
    let mut server = Server::start(Fixture::flat(1)).await;
    Arc::get_mut(&mut server.reader.path_membership)
        .unwrap()
        .deadline = Duration::from_millis(50);
    server.fixture.paused.store(true, Ordering::SeqCst);
    assert_eq!(
        server
            .reader
            .prove_file("file000")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(server.fixture.requests.lock().unwrap().len(), 1);
    assert_eq!(
        server.reader.path_membership.callers.available_permits(),
        128
    );
    assert_eq!(
        server
            .reader
            .path_membership
            .initializers
            .available_permits(),
        4
    );
    server.fixture.paused.store(false, Ordering::SeqCst);
    server.fixture.release.add_permits(1);
    assert!(server.reader.prove_file("file000").await.is_ok());
}

fn expiry_in_three_seconds() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3;
    let mut days = seconds / 86400;
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
    let mut month = 1;
    for length in [
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
    ] {
        if days < length {
            break;
        }
        days -= length;
        month += 1;
    }
    format!(
        "{year:04}-{month:02}-{:02}T{:02}:{:02}:{:02}Z",
        days + 1,
        seconds / 3600 % 24,
        seconds / 60 % 60,
        seconds % 60
    )
}

#[tokio::test]
async fn cached_transferred_proofs_and_owned_reads_keep_current_lease_checks() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::flat(1);
    fixture.expiry = expiry_in_three_seconds();
    let server = Server::start(fixture).await;
    let token = server.reader.prove_file("file000").await.unwrap();
    let before = server.fixture.requests.lock().unwrap().len();
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.fixture.renewals.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        server.reader.ensure_lease().await.unwrap_err().code,
        SnapshotErrorCode::ScopeForbidden
    );
    let error = server.reader.prove_file("file000").await.unwrap_err();
    assert_eq!(error.http_status(), 403);
    let source = error.snapshot_error().unwrap();
    assert_eq!(source.code, SnapshotErrorCode::ScopeForbidden);
    assert_eq!(source.http_status, 403);
    assert_eq!(error.to_string(), source.to_string());
    assert_eq!(
        std::error::Error::source(&error).unwrap().to_string(),
        source.to_string()
    );
    assert_eq!(
        server
            .reader
            .read_proven_content(&token, true)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::ScopeForbidden
    );
    assert_eq!(server.fixture.requests.lock().unwrap().len(), before);
    assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn same_deployment_changed_credential_or_epoch_cannot_consume_an_existing_token() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::flat(1)).await;
    let token = server.reader.prove_file("file000").await.unwrap();
    let credential_reader = SnapshotReader::resolve(
        super::super::Mst2Client::with_token(
            server.reader.client().base(),
            Some("different-credential".into()),
        ),
        "/project",
        600,
    )
    .await
    .unwrap();
    assert_eq!(
        credential_reader
            .read_proven_content(&token, true)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    server.fixture.epoch.store(2, Ordering::SeqCst);
    let changed_epoch = SnapshotReader::resolve(
        super::super::Mst2Client::new(server.reader.client().base()),
        "/project",
        600,
    )
    .await
    .unwrap();
    assert_eq!(
        changed_epoch
            .read_proven_content(&token, true)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn actual_waiters_at_local_caller_limit_reject_before_a_second_metadata_request() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::flat(1)).await;
    server.fixture.paused.store(true, Ordering::SeqCst);
    let mut tasks = Vec::new();
    for _ in 0..128 {
        let reader = server.reader.clone();
        tasks.push(tokio::spawn(
            async move { reader.prove_file("file000").await },
        ));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.reader.path_membership.callers.available_permits() != 0
            || server.fixture.requests.lock().unwrap().is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        server
            .reader
            .prove_file("file000")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(server.fixture.requests.lock().unwrap().len(), 1);
    for task in tasks {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    assert_eq!(
        server.reader.path_membership.callers.available_permits(),
        128
    );
    assert_eq!(
        server
            .reader
            .path_membership
            .initializers
            .available_permits(),
        4
    );
    server.fixture.paused.store(false, Ordering::SeqCst);
    server.fixture.release.add_permits(1);
    assert!(server.reader.prove_file("file000").await.is_ok());
}

#[tokio::test]
async fn process_initializer_admission_is_shared_across_distinct_reader_instances() {
    let _serial = TEST_LOCK.lock().await;
    let mut servers = Vec::new();
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let server = Server::start(Fixture::flat(5)).await;
        server.fixture.paused.store(true, Ordering::SeqCst);
        for index in 0..4 {
            let reader = server.reader.clone();
            tasks.push(tokio::spawn(async move {
                reader.prove_file(&format!("file{index:03}")).await
            }));
        }
        servers.push(server);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while servers
            .iter()
            .map(|server| server.fixture.requests.lock().unwrap().len())
            .sum::<usize>()
            < 16
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let extra = Server::start(Fixture::flat(1)).await;
    assert_eq!(
        extra
            .reader
            .prove_file("file000")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert!(extra.fixture.requests.lock().unwrap().is_empty());
    for task in tasks {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    assert_eq!(process().initializers.available_permits(), 16);
    assert!(extra.reader.prove_file("file000").await.is_ok());
    for server in servers {
        server.fixture.paused.store(false, Ordering::SeqCst);
        server.fixture.release.add_permits(4);
    }
}

#[tokio::test]
async fn a_waiters_own_deadline_drops_its_credit_without_cancelling_the_live_leader() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::flat(1)).await;
    server.fixture.paused.store(true, Ordering::SeqCst);
    let leader = tokio::spawn({
        let reader = server.reader.clone();
        async move { reader.prove_file("file000").await }
    });
    tokio::time::timeout(Duration::from_secs(5), server.fixture.started.notified())
        .await
        .unwrap();
    assert_eq!(
        server
            .reader
            .prove_file_deadline("file000", Duration::from_millis(50))
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert!(!leader.is_finished());
    assert_eq!(server.fixture.requests.lock().unwrap().len(), 1);
    assert_eq!(
        server.reader.path_membership.callers.available_permits(),
        127
    );
    assert_eq!(
        server
            .reader
            .path_membership
            .initializers
            .available_permits(),
        3
    );
    server.fixture.paused.store(false, Ordering::SeqCst);
    server.fixture.release.add_permits(1);
    assert!(leader.await.unwrap().is_ok());
    assert_eq!(
        server.reader.path_membership.callers.available_permits(),
        128
    );
    assert_eq!(
        server
            .reader
            .path_membership
            .initializers
            .available_permits(),
        4
    );
}

#[tokio::test]
async fn selected_membership_rejects_wrong_kinds_missing_paths_and_serving_profile_sizes() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::flat(1)).await;
    assert_eq!(
        server
            .reader
            .prove_file("missing")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::PathNotFound
    );
    assert_eq!(
        server
            .reader
            .prove_file("file000/child")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::NotDirectory
    );
    let before = server.fixture.requests.lock().unwrap().len();
    assert!(server.reader.prove_file("../file000").await.is_err());
    assert_eq!(server.fixture.requests.lock().unwrap().len(), before);
    for (kind, size) in [
        (
            EntryKind::Regular,
            super::super::content_profile::MAX_FILE_SIZE + 1,
        ),
        (EntryKind::Symlink, 0),
        (EntryKind::Symlink, 4096),
    ] {
        let mut witnesses = BTreeMap::new();
        let mut pages = BTreeMap::new();
        let root = add_directory(
            "/",
            &[Entry::file(kind, b"file", size, hash(BODY))],
            &mut witnesses,
            &mut pages,
        );
        let server = Server::start(Fixture::from_parts(root, witnesses, pages)).await;
        assert_eq!(
            server
                .reader
                .prove_file("file")
                .await
                .unwrap_err()
                .snapshot_error()
                .unwrap()
                .code,
            SnapshotErrorCode::LimitExceeded
        );
        assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn scope_root_existing_directory_and_intermediate_symlink_have_distinct_typed_errors() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::wide()).await;
    for path in ["", "/"] {
        let error = server.reader.prove_file(path).await.unwrap_err();
        assert_eq!(error.http_status(), 0);
        assert!(error.snapshot_error().is_none());
        assert!(std::error::Error::source(&error).is_none());
        assert!(error.to_string().starts_with("NotFile: "));
        assert!(matches!(error, FileMembershipError::NotFile { .. }));
    }
    assert!(server.fixture.requests.lock().unwrap().is_empty());
    assert!(matches!(
        server.reader.prove_file("wanted").await.unwrap_err(),
        FileMembershipError::NotFile { .. }
    ));
    assert_eq!(
        server.fixture.requests.lock().unwrap().as_slice(),
        [("/".into(), Vec::new())]
    );
    assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 0);
    let mut witnesses = BTreeMap::new();
    let mut pages = BTreeMap::new();
    let root = add_directory(
        "/",
        &[Entry::file(
            EntryKind::Symlink,
            b"link",
            BODY.len() as u64,
            hash(BODY),
        )],
        &mut witnesses,
        &mut pages,
    );
    let server = Server::start(Fixture::from_parts(root, witnesses, pages)).await;
    assert_eq!(
        server
            .reader
            .prove_file("link/target")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::SymlinkTraversal
    );
    assert_eq!(
        server.fixture.requests.lock().unwrap().as_slice(),
        [("/".into(), Vec::new())]
    );
    assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn full_closure_seed_distinguishes_known_directories_from_absent_paths_without_http() {
    let _serial = TEST_LOCK.lock().await;
    let mut witnesses = BTreeMap::new();
    let mut pages = BTreeMap::new();
    let empty = add_directory("/empty", &[], &mut witnesses, &mut pages);
    let root = add_directory(
        "/",
        &[
            Entry::dir(b"empty", empty),
            Entry::file(EntryKind::Regular, b"file", BODY.len() as u64, hash(BODY)),
            Entry::file(EntryKind::Symlink, b"link", BODY.len() as u64, hash(BODY)),
        ],
        &mut witnesses,
        &mut pages,
    );
    let server = Server::start(Fixture::from_parts(root, witnesses, pages)).await;
    let closure = super::super::ValidatedSnapshotClosure::from_pages(
        server.reader.descriptor(),
        server.fixture.pages.clone(),
    )
    .unwrap();
    server.reader.seed_content_membership(&closure).unwrap();
    let error = server.reader.prove_file("empty").await.unwrap_err();
    assert_eq!(error.http_status(), 0);
    assert!(matches!(error, FileMembershipError::NotFile { .. }));
    assert_eq!(
        server
            .reader
            .prove_file("absent")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::PathNotFound
    );
    assert!(server.reader.prove_file("file").await.is_ok());
    assert_eq!(
        server
            .reader
            .prove_file("file/child")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::NotDirectory
    );
    assert_eq!(
        server
            .reader
            .prove_file("link/target")
            .await
            .unwrap_err()
            .snapshot_error()
            .unwrap()
            .code,
        SnapshotErrorCode::SymlinkTraversal
    );
    assert!(server.fixture.requests.lock().unwrap().is_empty());
    assert_eq!(server.fixture.bodies.load(Ordering::SeqCst), 0);
}
