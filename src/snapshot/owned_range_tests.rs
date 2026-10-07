use std::{
    collections::BTreeMap,
    ffi::OsStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use asyncfuse::raw::prelude::{Filesystem, Request};
use axum::{
    body::{Body, Bytes},
    extract::{Query, State},
    http::StatusCode,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use mst2_codec::{
    chunkmap::{ChunkLeaf, ChunkMap},
    descriptor::ServingDescriptor,
    metapage::{page_id, Entry, EntryKind, Page},
    treeframe::{ChunkPayload, EndPayload},
};
use serde_json::{json, Value};

use super::*;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const INSTANCE: &str = "11111111-2222-4333-8444-555555555563";

fn hash(bytes: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}
fn id(digest: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(digest))
}
fn base64(bytes: &[u8]) -> String {
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

struct Fixture {
    descriptor: ServingDescriptor,
    page: Vec<u8>,
    map: ChunkMap,
    leaf: ChunkLeaf,
    mode: AtomicUsize,
    map_requests: AtomicUsize,
    chunk_requests: AtomicUsize,
    renewal_requests: AtomicUsize,
    metadata_pages: bool,
    aliases: bool,
    manifest_size_delta: u64,
    directory_requests: AtomicUsize,
    leaf_requests: AtomicUsize,
    paths: Mutex<Vec<String>>,
    hold_stage: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    expiry: String,
}
impl Fixture {
    fn new(full_chunks: usize) -> Self {
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        let mut hashes = Vec::new();
        for index in 0..=full_chunks {
            let bytes = vec![
                index as u8;
                if index == full_chunks {
                    7
                } else {
                    CHUNK_SIZE as usize
                }
            ];
            digest.update(&bytes);
            hashes.push(hash(&bytes));
        }
        let leaf = ChunkLeaf {
            page_index: 0,
            chunk_sha256: hashes,
        };
        let map = ChunkMap::new(
            digest.finish().as_ref().try_into().unwrap(),
            full_chunks as u64 * CHUNK_SIZE as u64 + 7,
            leaf.leaf_hash().unwrap(),
        )
        .unwrap();
        let page = Page::build(&[Entry::file(
            EntryKind::Regular,
            b"file",
            map.file_size,
            map.file_content_id,
        )])
        .unwrap();
        Self {
            descriptor: ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
                namespace_view_id: [0x53; 32],
                scope: "/project".into(),
                metadata_root: page_id(&page),
            },
            page,
            map,
            leaf,
            mode: AtomicUsize::new(0),
            map_requests: AtomicUsize::new(0),
            chunk_requests: AtomicUsize::new(0),
            renewal_requests: AtomicUsize::new(0),
            metadata_pages: true,
            aliases: false,
            manifest_size_delta: 0,
            directory_requests: AtomicUsize::new(0),
            leaf_requests: AtomicUsize::new(0),
            paths: Mutex::new(Vec::new()),
            hold_stage: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            expiry: "2099-01-01T00:00:00Z".into(),
        }
    }
    fn bytes(&self, index: u64) -> Vec<u8> {
        vec![index as u8; self.map.chunk_len(index).unwrap() as usize]
    }
    async fn hold(&self, stage: usize) {
        if self.hold_stage.load(Ordering::SeqCst) == stage {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
    fn accepts_path(&self, path: &str) -> bool {
        path == "file" || self.aliases && path == "alias"
    }
}

async fn capabilities(State(f): State<Arc<Fixture>>) -> Json<Value> {
    Json(
        json!({"protocol_versions":[2],"metadata_codecs":[1],"frame_encodings":["identity"],"features":{"resolve":true,"directory":true,"leases":true,"metadata_pages":f.metadata_pages,"chunk_reads":true}}),
    )
}
async fn directory_response(
    State(f): State<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert!(!f.metadata_pages);
    assert_eq!(query["path"], "/");
    f.directory_requests.fetch_add(1, Ordering::SeqCst);
    let names: &[&str] = if f.aliases {
        &["alias", "file"]
    } else {
        &["file"]
    };
    let entries: Vec<_> = names.iter().map(|name| {
        json!({"name":name,"fs_kind":"regular","size":(f.map.file_size + f.manifest_size_delta).to_string(),"content_digest":id(&f.map.file_content_id)})
    }).collect();
    Json(
        json!({"snapshot_id":id(&f.descriptor.snapshot_id().unwrap()),"path":"/","metadata_root":id(&f.descriptor.metadata_root),"directory_root":id(&f.descriptor.metadata_root),"node_class":"native_tree","lifecycle":"mutable","range_start_exclusive":null,"entries":entries,"entry_count":names.len().to_string(),"next_cursor":null,"proof_pages":[]}),
    )
}
async fn resolve(State(f): State<Arc<Fixture>>) -> Json<Value> {
    let d = &f.descriptor;
    Json(
        json!({"descriptor":{"schema_version":2,"metadata_codec":1,"instance_id":INSTANCE,"namespace_view_id":id(&d.namespace_view_id),"scope":"/project","materialization_policy":1,"fs_semantics":1,"access_projection":0,"metadata_root":id(&d.metadata_root),"snapshot_id":id(&d.snapshot_id().unwrap())},"lease_id":"owned-range-lease","lease_expires_at":f.expiry,"publication_sequence":"1","authorization_epoch":"1"}),
    )
}
async fn renew(State(f): State<Arc<Fixture>>) -> (StatusCode, Json<Value>) {
    f.renewal_requests.fetch_add(1, Ordering::SeqCst);
    (
        StatusCode::FORBIDDEN,
        Json(json!({"error":{"code":"SCOPE_FORBIDDEN","message":"revoked range lease"}})),
    )
}
async fn map_response(
    State(f): State<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    f.map_requests.fetch_add(1, Ordering::SeqCst);
    assert!(f.accepts_path(query["path"].trim_start_matches('/')));
    assert_eq!(query["expected_digest"], id(&f.map.file_content_id));
    f.hold(1).await;
    Json(
        json!({"snapshot_id":id(&f.descriptor.snapshot_id().unwrap()),"path":query["path"],"schema_version":2,"file_content_id":id(&f.map.file_content_id),"file_size":f.map.file_size.to_string(),"chunk_size":CHUNK_SIZE,"chunk_count":f.map.chunk_count.to_string(),"page_count":f.map.page_count.to_string(),"pages_root":id(&f.map.pages_root),"map_id":id(&f.map.map_id())}),
    )
}
async fn leaf_response(
    State(f): State<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    f.leaf_requests.fetch_add(1, Ordering::SeqCst);
    assert_eq!(query["page"], "0");
    assert!(f.accepts_path(query["path"].trim_start_matches('/')));
    f.hold(2).await;
    Json(
        json!({"snapshot_id":id(&f.descriptor.snapshot_id().unwrap()),"path":query["path"],"map_id":id(&f.map.map_id()),"page_count":"1","leaf":{"page_index":"0","count":f.map.chunk_count.to_string(),"data_base64":base64(&f.leaf.encode().unwrap())},"proof":[]}),
    )
}
async fn chunks(State(f): State<Arc<Fixture>>, body: Bytes) -> Response<Body> {
    f.chunk_requests.fetch_add(1, Ordering::SeqCst);
    let request: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(request["items"].as_array().unwrap().len(), 1);
    let item = &request["items"][0];
    let path = item["path"].as_str().unwrap();
    assert!(path.starts_with('/') && f.accepts_path(&path[1..]));
    f.paths.lock().unwrap().push(path.into());
    assert_eq!(item["expected_digest"], id(&f.map.file_content_id));
    assert_eq!(item["map_id"], id(&f.map.map_id()));
    let index = item["chunk_index"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let bytes = f.bytes(index);
    let mode = f.mode.load(Ordering::SeqCst);
    let mut payload = ChunkPayload {
        map_id: f.map.map_id(),
        file_content_id: f.map.file_content_id,
        chunk_index: index,
        chunk_bytes: bytes.clone(),
    };
    match mode {
        1 => payload.map_id[0] ^= 1,
        2 => payload.file_content_id[0] ^= 1,
        3 => payload.chunk_index += 1,
        4 => {
            payload.chunk_bytes.pop();
        }
        5 => payload.chunk_bytes[0] ^= 1,
        _ => {}
    }
    let mut wire = if mode == 10 {
        Vec::new()
    } else {
        payload.encode(31, 0).unwrap()
    };
    let mut seq = u64::from(!wire.is_empty());
    if mode == 6 {
        wire.extend(payload.encode(31, seq).unwrap());
        seq += 1;
    }
    let mut end = EndPayload {
        request_item_count: 1,
        unique_unit_count: 1,
        logical_bytes: bytes.len() as u64,
        request_body_sha256: hash(&body),
    };
    if mode == 7 || mode == 11 && index > 0 {
        end.request_body_sha256[0] ^= 1;
    }
    wire.extend(end.encode(31, seq));
    if mode == 8 {
        wire.push(0);
    }
    f.hold(3).await;
    let response = if mode == 9 {
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
        .header("x-mega-request-digest", id(&hash(&body)))
        .body(response)
        .unwrap()
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
    async fn start(full_chunks: usize, limit: usize) -> Self {
        Self::start_fixture(Fixture::new(full_chunks), limit).await
    }
    async fn start_fixture(fixture: Fixture, limit: usize) -> Self {
        let fixture = Arc::new(fixture);
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(capabilities))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .route("/api/v2/snapshots/leases/{lease}/renew", post(renew))
            .route("/api/v2/snapshots/{sid}/directory", get(directory_response))
            .route("/api/v2/snapshots/{sid}/chunk-map", get(map_response))
            .route(
                "/api/v2/snapshots/{sid}/chunk-map/pages",
                get(leaf_response),
            )
            .route("/api/v2/snapshots/{sid}/chunks", post(chunks))
            .with_state(fixture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let reader = SnapshotReader::resolve(super::super::Mst2Client::new(url), "/project", 600)
            .await
            .unwrap()
            .with_content_limits(
                super::super::ContentBudgetLimits::new(limit, 8 * 1024 * 1024).unwrap(),
            );
        if fixture.metadata_pages {
            let closure = super::super::ValidatedSnapshotClosure::from_pages(
                reader.descriptor(),
                BTreeMap::from([(id(&fixture.descriptor.metadata_root), fixture.page.clone())]),
            )
            .unwrap();
            reader.seed_content_membership(&closure).unwrap();
        }
        Self {
            reader,
            fixture,
            task,
        }
    }
    async fn open(&self) -> OwnedChunkedFile {
        OwnedChunkedFile::open(
            &self.reader,
            "file",
            &id(&self.fixture.map.file_content_id),
            self.fixture.map.file_size,
        )
        .await
        .unwrap()
    }
    async fn online_tokens(&self) -> BTreeMap<String, Arc<OnlineSnapshotFile>> {
        let fs = crate::snapshot::fuse::Mst2Fuse::from_reader(self.reader.clone())
            .await
            .unwrap();
        let names: &[&str] = if self.fixture.aliases {
            &["alias", "file"]
        } else {
            &["file"]
        };
        let mut tokens = BTreeMap::new();
        for name in names {
            let entry = fs
                .lookup(
                    Request::default(),
                    super::super::fuse::ROOT_INODE,
                    OsStr::new(name),
                )
                .await
                .unwrap();
            let token = fs.online_file_for_test(entry.attr.ino).unwrap();
            assert_eq!(token.file().rel_path, *name);
            tokens.insert((*name).to_string(), token);
        }
        assert!(self.reader.content_membership.get().is_none());
        tokens
    }
}

async fn construction_idle(reader: &SnapshotReader) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while reader.content_usage().construction_bytes != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

// Always release a held HTTP response, including when an assertion panics.
// The operation under test observes a real renewal rejection, not an injected
// local lease failure or a synthetic transport response.
struct HeldResponse {
    fixture: Arc<Fixture>,
}
impl HeldResponse {
    fn new(fixture: &Arc<Fixture>, stage: usize) -> Self {
        fixture.hold_stage.store(stage, Ordering::SeqCst);
        Self {
            fixture: fixture.clone(),
        }
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.fixture.entered.notified())
            .await
            .unwrap();
    }
    fn release(&self) {
        self.fixture.hold_stage.store(0, Ordering::SeqCst);
        self.fixture.release.notify_one();
    }
}
impl Drop for HeldResponse {
    fn drop(&mut self) {
        self.release();
    }
}

async fn renewal_rejected(server: &Server) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.fixture.renewal_requests.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Wait for the real renewal task to latch the terminal 403 result.
    assert_eq!(
        server.reader.ensure_lease().await.unwrap_err().code,
        SnapshotErrorCode::ScopeForbidden
    );
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
        seconds / 3600 % 24,
        seconds / 60 % 60,
        seconds % 60
    )
}

#[tokio::test]
async fn authenticated_ranges_cover_boundaries_clamp_overflow_and_keep_actual_range_owners() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(2, 8 * 1024 * 1024).await;
    let file = server.open().await;
    let boundary = file
        .read_range_owned(CHUNK_SIZE as u64 - 2, 5)
        .await
        .unwrap();
    assert_eq!(boundary.as_bytes(), [0, 0, 1, 1, 1]);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 2);
    let end = file
        .read_range_owned(2 * CHUNK_SIZE as u64 + 3, u64::MAX)
        .await
        .unwrap();
    assert_eq!(end.as_bytes(), [2; 4]);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 3);
    let empty = file.read_range_owned(u64::MAX, u64::MAX).await.unwrap();
    assert!(empty.is_empty());
    let zero = file.read_range_owned(0, 0).await.unwrap();
    assert!(zero.is_empty());
    assert!(file.fully_cached().await);
    let clone = boundary.clone();
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 4096);
    drop(boundary);
    assert_eq!(server.reader.content_usage().output_bytes, 4096);
    drop(clone);
    drop(end);
    drop(empty);
    drop(zero);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
    construction_idle(&server.reader).await;
}

#[tokio::test]
async fn strict_range_open_rejects_forged_facts_and_range_admission_precedes_chunk_http() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(1, 1024).await;
    let digest = id(&server.fixture.map.file_content_id);
    assert_eq!(
        OwnedChunkedFile::open(
            &server.reader,
            "absent",
            &digest,
            server.fixture.map.file_size
        )
        .await
        .err()
        .unwrap()
        .code,
        SnapshotErrorCode::PathNotFound
    );
    assert_eq!(
        OwnedChunkedFile::open(
            &server.reader,
            "file",
            &id(&[9; 32]),
            server.fixture.map.file_size
        )
        .await
        .err()
        .unwrap()
        .code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(
        OwnedChunkedFile::open(
            &server.reader,
            "file",
            &digest,
            server.fixture.map.file_size + 1
        )
        .await
        .err()
        .unwrap()
        .code,
        SnapshotErrorCode::DigestMismatch
    );
    assert!(OwnedChunkedFile::open(
        &server.reader,
        "../file",
        &digest,
        server.fixture.map.file_size
    )
    .await
    .is_err());
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 0);
    let file = server.open().await;
    assert_eq!(
        file.read_range_owned(0, 1).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 0);
    assert_eq!(server.reader.content_usage().output_bytes, 1024);
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn readers_and_instances_share_quota_and_live_chunk_arc_survives_lru_eviction() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(17, 32 * 1024 * 1024).await;
    let file = server.open().await;
    let held = file.ensure_chunk(0).await.unwrap();
    for index in 1..=16 {
        file.ensure_chunk(index).await.unwrap();
    }
    assert_eq!(file.chunks.lock().await.len, 16);
    assert_eq!(
        server.reader.content_usage().output_bytes,
        17 * (CHUNK_SIZE as usize + 1024) + 1024
    );
    drop(held);
    assert_eq!(
        server.reader.content_usage().output_bytes,
        16 * (CHUNK_SIZE as usize + 1024) + 1024
    );
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
    let server = Server::start(2, CHUNK_SIZE as usize + 4096).await;
    let first = server.open().await;
    let second = OwnedChunkedFile::open(
        &server.reader.clone(),
        "/file",
        &id(&server.fixture.map.file_content_id),
        server.fixture.map.file_size,
    )
    .await
    .unwrap();
    let retained = first.read_range_owned(0, 1).await.unwrap();
    assert_eq!(
        second.read_range_owned(0, 1).await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 1);
    drop(first);
    assert_eq!(second.read_range_owned(0, 1).await.unwrap().as_bytes(), [0]);
    drop(second);
    drop(retained);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn bad_chunk_identity_integrity_end_and_actual_cancellation_publish_no_range() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(1, 8 * 1024 * 1024).await;
    let file = Arc::new(server.open().await);
    for mode in [1, 2, 3, 4, 5, 6, 7, 8, 10] {
        server.fixture.mode.store(mode, Ordering::SeqCst);
        assert!(file.read_range_owned(0, 1).await.is_err(), "mode{mode}");
        construction_idle(&server.reader).await;
        assert_eq!(
            server.reader.content_usage().output_bytes,
            1024,
            "mode{mode}"
        );
        assert_eq!(file.chunks.lock().await.len, 0);
    }
    server.fixture.mode.store(9, Ordering::SeqCst);
    let before = server.fixture.chunk_requests.load(Ordering::SeqCst);
    let task = tokio::spawn({
        let file = file.clone();
        async move { file.read_range_owned(0, 1).await }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.fixture.chunk_requests.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!task.is_finished());
    assert_eq!(
        server.reader.content_usage().output_bytes,
        CHUNK_SIZE as usize + 3072
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    construction_idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 1024);
    server.fixture.mode.store(0, Ordering::SeqCst);
    assert_eq!(file.read_range_owned(0, 1).await.unwrap().as_bytes(), [0]);
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn exact_64_mib_range_is_admitted_and_larger_output_rejects_before_chunk_http() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(65, 128 * 1024 * 1024).await;
    let file = server.open().await;
    let maximum = 64 * 1024 * 1024;
    assert_eq!(
        file.read_range_owned(0, maximum + 1)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 0);
    let owner = file.read_range_owned(0, maximum).await.unwrap();
    assert_eq!(owner.len(), maximum as usize);
    for (index, bytes) in owner.as_bytes().chunks(CHUNK_SIZE as usize).enumerate() {
        assert!(bytes.iter().all(|byte| *byte == index as u8));
    }
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 64);
    assert_eq!(file.chunks.lock().await.len, 16);
    assert!(!file.fully_cached().await);
    drop(file);
    assert_eq!(
        server.reader.content_usage().output_bytes,
        maximum as usize + 1024
    );
    drop(owner);
    construction_idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn eight_tib_map_shape_opens_without_allocating_or_fetching_file_payload() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(1);
    fixture.map = ChunkMap::new(
        fixture.map.file_content_id,
        super::super::range::MAX_FILE_SIZE,
        fixture.map.pages_root,
    )
    .unwrap();
    fixture.page = Page::build(&[Entry::file(
        EntryKind::Regular,
        b"file",
        fixture.map.file_size,
        fixture.map.file_content_id,
    )])
    .unwrap();
    fixture.descriptor.metadata_root = page_id(&fixture.page);
    let server = Server::start_fixture(fixture, 1024).await;
    let digest = id(&server.fixture.map.file_content_id);
    assert_eq!(
        OwnedChunkedFile::open(
            &server.reader,
            "file",
            &digest,
            super::super::range::MAX_FILE_SIZE + 1
        )
        .await
        .err()
        .unwrap()
        .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 0);
    let file = server.open().await;
    assert_eq!(file.size(), 8 * 1024 * 1024 * 1024 * 1024);
    assert_eq!(file.map.chunk_count, 8 * 1024 * 1024);
    assert_eq!(file.map.page_count, 32 * 1024);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 0);
    assert_eq!(server.reader.content_usage().output_bytes, 1024);
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn cached_and_eof_ranges_reject_a_revoked_current_lease() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(1);
    fixture.expiry = expiry_in_three_seconds();
    let server = Server::start_fixture(fixture, 8 * 1024 * 1024).await;
    let file = server.open().await;
    let before_revoke = file.read_range_owned(0, 1).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.fixture.renewal_requests.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Synchronize with the renewer so the rejection latch is already set.
    assert_eq!(
        server.reader.ensure_lease().await.unwrap_err().code,
        SnapshotErrorCode::ScopeForbidden
    );
    assert_eq!(
        file.read_range_owned(0, 1).await.unwrap_err().code,
        SnapshotErrorCode::ScopeForbidden
    );
    assert_eq!(
        file.read_range_owned(u64::MAX, 0).await.unwrap_err().code,
        SnapshotErrorCode::ScopeForbidden
    );
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 1);
    assert_eq!(before_revoke.as_bytes(), [0]);
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 1024);
    drop(before_revoke);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn a_late_cross_chunk_failure_publishes_no_range_and_reuses_the_proven_first_chunk() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(2, 8 * 1024 * 1024).await;
    let file = server.open().await;
    server.fixture.mode.store(11, Ordering::SeqCst);
    assert!(file
        .read_range_owned(CHUNK_SIZE as u64 - 1, 2)
        .await
        .is_err());
    construction_idle(&server.reader).await;
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 2);
    assert_eq!(file.chunks.lock().await.len, 1);
    assert_eq!(
        server.reader.content_usage().output_bytes,
        CHUNK_SIZE as usize + 2048
    );
    server.fixture.mode.store(0, Ordering::SeqCst);
    let owner = file
        .read_range_owned(CHUNK_SIZE as u64 - 1, 2)
        .await
        .unwrap();
    assert_eq!(owner.as_bytes(), [0, 1]);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 3);
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 1024);
    drop(owner);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn transferred_proof_opens_ranges_without_initializing_a_complete_closure() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(2, 8 * 1024 * 1024).await;
    let proof = server.reader.prove_file("file").await.unwrap();
    let consumer = SnapshotReader::resolve(
        super::super::Mst2Client::new(server.reader.client().base()),
        "/project",
        600,
    )
    .await
    .unwrap();
    assert!(consumer.content_membership.get().is_none());
    let file = OwnedChunkedFile::open_proven(&consumer, proof)
        .await
        .unwrap();
    assert_eq!(
        file.read_range_owned(CHUNK_SIZE as u64 - 2, 5)
            .await
            .unwrap()
            .as_bytes(),
        [0, 0, 1, 1, 1]
    );
    assert_eq!(
        file.read_range_owned(CHUNK_SIZE as u64 - 2, 5)
            .await
            .unwrap()
            .as_bytes(),
        [0, 0, 1, 1, 1]
    );
    assert!(file
        .read_range_owned(u64::MAX, u64::MAX)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 2);
    assert!(consumer.content_membership.get().is_none());
    let foreign = Server::start(2, 8 * 1024 * 1024).await;
    let foreign_proof = foreign.reader.prove_file("file").await.unwrap();
    assert_eq!(
        OwnedChunkedFile::open_proven(&consumer, foreign_proof)
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 1);
    drop(file);
    assert_eq!(consumer.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn online_manifest_ranges_keep_paid_owners_and_each_alias_wire_path() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(1);
    fixture.metadata_pages = false;
    fixture.aliases = true;
    let server = Server::start_fixture(fixture, 8 * 1024 * 1024).await;
    let tokens = server.online_tokens().await;
    assert_eq!(server.fixture.directory_requests.load(Ordering::SeqCst), 1);
    // The actual public manifest mount has been dropped. It does not seed a
    // fictitious complete closure or leave its payload cache reservation alive.
    assert_eq!(server.reader.content_usage().output_bytes, 0);
    let alias = OwnedChunkedFile::open_online_path(&server.reader, tokens["alias"].clone())
        .await
        .unwrap();
    let file = OwnedChunkedFile::open_online_path(&server.reader, tokens["file"].clone())
        .await
        .unwrap();
    let held_alias = alias.read_range_owned(0, 1).await.unwrap();
    let held_file = file.read_range_owned(0, 1).await.unwrap();
    assert_eq!(held_alias.as_bytes(), [0]);
    assert_eq!(held_file.as_bytes(), [0]);
    assert_eq!(alias.read_range_owned(0, 1).await.unwrap().as_bytes(), [0]);
    assert_eq!(file.read_range_owned(0, 1).await.unwrap().as_bytes(), [0]);
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 2);
    assert_eq!(server.fixture.leaf_requests.load(Ordering::SeqCst), 2);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 2);
    assert_eq!(*server.fixture.paths.lock().unwrap(), ["/alias", "/file"]);
    assert!(server.reader.content_membership.get().is_none());
    let last_alias = held_alias.clone();
    drop(alias);
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 2048);
    drop(held_alias);
    assert_eq!(server.reader.content_usage().output_bytes, 2048);
    assert_eq!(last_alias.as_bytes(), [0]);
    drop(last_alias);
    assert_eq!(server.reader.content_usage().output_bytes, 1024);
    drop(held_file);
    construction_idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn online_authority_rejects_foreign_domains_and_keeps_public_open_strict() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(1);
    fixture.metadata_pages = false;
    let server = Server::start_fixture(fixture, 8 * 1024 * 1024).await;
    let tokens = server.online_tokens().await;
    assert_eq!(
        OwnedChunkedFile::open(
            &server.reader,
            "file",
            &id(&server.fixture.map.file_content_id),
            server.fixture.map.file_size
        )
        .await
        .err()
        .unwrap()
        .code,
        SnapshotErrorCode::SnapshotNotReady
    );
    let mut foreign_fixture = Fixture::new(1);
    foreign_fixture.metadata_pages = false;
    let foreign = Server::start_fixture(foreign_fixture, 8 * 1024 * 1024).await;
    let foreign_tokens = foreign.online_tokens().await;
    assert_eq!(
        OwnedChunkedFile::open_online_path(&server.reader, foreign_tokens["file"].clone())
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 0);
    assert_eq!(foreign.fixture.map_requests.load(Ordering::SeqCst), 0);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
    assert!(server.reader.content_membership.get().is_none());
    // The actual no-pages public mount token still opens its own fixed path.
    let file = OwnedChunkedFile::open_online_path(&server.reader, tokens["file"].clone())
        .await
        .unwrap();
    assert_eq!(file.read_range_owned(0, 1).await.unwrap().as_bytes(), [0]);
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn online_manifest_size_is_checked_against_the_real_map_before_chunk_http() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(1);
    fixture.metadata_pages = false;
    fixture.manifest_size_delta = 1;
    let server = Server::start_fixture(fixture, 8 * 1024 * 1024).await;
    let tokens = server.online_tokens().await;
    assert_eq!(
        OwnedChunkedFile::open_online_path(&server.reader, tokens["file"].clone())
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(server.fixture.directory_requests.load(Ordering::SeqCst), 1);
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 1);
    assert_eq!(server.fixture.leaf_requests.load(Ordering::SeqCst), 0);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 0);
    assert!(server.reader.content_membership.get().is_none());
    construction_idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn online_bad_frames_and_cancelled_body_publish_no_range_or_chunk_owner() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(1);
    fixture.metadata_pages = false;
    let server = Server::start_fixture(fixture, 8 * 1024 * 1024).await;
    let tokens = server.online_tokens().await;
    let file = Arc::new(
        OwnedChunkedFile::open_online_path(&server.reader, tokens["file"].clone())
            .await
            .unwrap(),
    );
    for mode in [1, 2, 3, 4, 5, 6, 7, 8, 10] {
        server.fixture.mode.store(mode, Ordering::SeqCst);
        assert!(file.read_range_owned(0, 1).await.is_err(), "mode{mode}");
        construction_idle(&server.reader).await;
        assert_eq!(file.chunks.lock().await.len, 0, "mode{mode}");
        assert_eq!(
            server.reader.content_usage().output_bytes,
            1024,
            "mode{mode}"
        );
    }
    server.fixture.mode.store(9, Ordering::SeqCst);
    let held = HeldResponse::new(&server.fixture, 3);
    let task = tokio::spawn({
        let file = file.clone();
        async move { file.read_range_owned(0, 1).await }
    });
    held.entered().await;
    held.release();
    assert!(!task.is_finished());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    construction_idle(&server.reader).await;
    assert_eq!(file.chunks.lock().await.len, 0);
    assert_eq!(server.reader.content_usage().output_bytes, 1024);
    server.fixture.mode.store(0, Ordering::SeqCst);
    assert_eq!(file.read_range_owned(0, 1).await.unwrap().as_bytes(), [0]);
    assert!(server.reader.content_membership.get().is_none());
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn online_map_leaf_and_chunk_replies_cannot_publish_after_real_403_renewal() {
    let _serial = TEST_LOCK.lock().await;
    for stage in [1, 2, 3] {
        let mut fixture = Fixture::new(1);
        fixture.metadata_pages = false;
        fixture.expiry = expiry_in_three_seconds();
        let server = Server::start_fixture(fixture, 8 * 1024 * 1024).await;
        let tokens = server.online_tokens().await;
        if stage == 1 {
            let held = HeldResponse::new(&server.fixture, stage);
            let task = tokio::spawn({
                let reader = server.reader.clone();
                let token = tokens["file"].clone();
                async move { OwnedChunkedFile::open_online_path(&reader, token).await }
            });
            held.entered().await;
            renewal_rejected(&server).await;
            held.release();
            assert_eq!(
                task.await.unwrap().err().unwrap().code,
                SnapshotErrorCode::ScopeForbidden
            );
            assert_eq!(server.fixture.leaf_requests.load(Ordering::SeqCst), 0);
            assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 0);
            assert_eq!(server.reader.content_usage().output_bytes, 0);
        } else {
            let file = Arc::new(
                OwnedChunkedFile::open_online_path(&server.reader, tokens["file"].clone())
                    .await
                    .unwrap(),
            );
            let held = HeldResponse::new(&server.fixture, stage);
            let task = tokio::spawn({
                let file = file.clone();
                async move { file.read_range_owned(0, 1).await }
            });
            held.entered().await;
            renewal_rejected(&server).await;
            held.release();
            assert_eq!(
                task.await.unwrap().unwrap_err().code,
                SnapshotErrorCode::ScopeForbidden
            );
            assert_eq!(file.chunks.lock().await.len, 0);
            assert_eq!(file.leaves.lock().await.get(0).is_some(), stage == 3);
            assert_eq!(server.fixture.leaf_requests.load(Ordering::SeqCst), 1);
            assert_eq!(
                server.fixture.chunk_requests.load(Ordering::SeqCst),
                usize::from(stage == 3)
            );
            assert_eq!(server.reader.content_usage().output_bytes, 1024);
            drop(file);
            assert_eq!(server.reader.content_usage().output_bytes, 0);
        }
        assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 1);
        assert!(server.reader.content_membership.get().is_none());
        construction_idle(&server.reader).await;
    }
}

#[tokio::test]
async fn online_warm_zero_length_and_eof_ranges_reject_the_revoked_lease() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(1);
    fixture.metadata_pages = false;
    fixture.expiry = expiry_in_three_seconds();
    let server = Server::start_fixture(fixture, 8 * 1024 * 1024).await;
    let tokens = server.online_tokens().await;
    let file = OwnedChunkedFile::open_online_path(&server.reader, tokens["file"].clone())
        .await
        .unwrap();
    let before_revoke = file.read_range_owned(0, 1).await.unwrap();
    let output_before_revoke = server.reader.content_usage().output_bytes;
    renewal_rejected(&server).await;
    for (offset, length) in [(0, 1), (0, 0), (u64::MAX, u64::MAX)] {
        assert_eq!(
            file.read_range_owned(offset, length)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::ScopeForbidden
        );
    }
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 1);
    assert_eq!(server.fixture.leaf_requests.load(Ordering::SeqCst), 1);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        server.reader.content_usage().output_bytes,
        output_before_revoke
    );
    assert!(server.reader.content_membership.get().is_none());
    drop(file);
    assert_eq!(server.reader.content_usage().output_bytes, 1024);
    assert_eq!(before_revoke.as_bytes(), [0]);
    drop(before_revoke);
    construction_idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}
