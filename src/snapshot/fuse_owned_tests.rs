//! Actual Filesystem replies retain their paid owners through Bytes lifetimes.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use axum::{
    body::Body,
    extract::{Query, State as HttpState},
    http::StatusCode,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use mst2_codec::{
    chunkmap::{ChunkLeaf, ChunkMap, CHUNK_SIZE},
    descriptor::ServingDescriptor,
    metapage::{page_id, Entry, EntryKind, Page},
    treeframe::{ChunkPayload, EndPayload, MetaPayload, ObjectPayload},
};
use serde_json::{json, Value};

use super::*;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(target_os = "linux")]
#[path = "fuse_layer_tests.rs"]
mod layer_tests;
const INSTANCE: &str = "11111111-2222-4333-8444-555555555565";
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

struct Large {
    map: ChunkMap,
    leaf: ChunkLeaf,
}
impl Large {
    fn new() -> Self {
        Self::with_full_chunks(2)
    }
    fn with_full_chunks(full_chunks: u64) -> Self {
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        let mut chunk_sha256 = Vec::new();
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
            chunk_sha256.push(hash(&bytes));
        }
        let leaf = ChunkLeaf {
            page_index: 0,
            chunk_sha256,
        };
        let map = ChunkMap::new(
            digest.finish().as_ref().try_into().unwrap(),
            full_chunks * CHUNK_SIZE as u64 + 7,
            leaf.leaf_hash().unwrap(),
        )
        .unwrap();
        Self { map, leaf }
    }
    fn bytes(&self, index: u64) -> Vec<u8> {
        vec![index as u8; self.map.chunk_len(index).unwrap() as usize]
    }
}

struct Fixture {
    descriptor: ServingDescriptor,
    page: Vec<u8>,
    bodies: BTreeMap<String, Vec<u8>>,
    metadata: Mutex<Vec<String>>,
    requests: AtomicUsize,
    map_requests: AtomicUsize,
    leaf_requests: AtomicUsize,
    chunk_requests: AtomicUsize,
    emitted: Arc<AtomicUsize>,
    renewals: AtomicUsize,
    mode: AtomicUsize,
    release: tokio::sync::Semaphore,
    objects: bool,
    expiry: String,
    large: Option<Large>,
    nested_page: Option<Vec<u8>>,
}
impl Fixture {
    fn new(unavailable: bool, objects: bool) -> Self {
        let mut bodies: BTreeMap<String, Vec<u8>> = (0..17)
            .map(|index| (format!("file{index:03}"), vec![index as u8; 8192]))
            .collect();
        bodies.insert("big".into(), vec![0x66; 246 * 1024]);
        bodies.insert("empty".into(), Vec::new());
        bodies.insert("exec".into(), b"#!/bin/sh\n".to_vec());
        bodies.insert("link".into(), b"file000".to_vec());
        let mut entries: Vec<_> = bodies
            .iter()
            .map(|(name, body)| {
                Entry::file(
                    match name.as_str() {
                        "exec" => EntryKind::Executable,
                        "link" => EntryKind::Symlink,
                        _ => EntryKind::Regular,
                    },
                    name.as_bytes(),
                    body.len() as u64,
                    hash(body),
                )
            })
            .collect();
        if unavailable {
            entries.push(Entry::dir(b"unavailable", [0x88; 32]));
            entries.sort_by(|a, b| a.name.cmp(&b.name));
        }
        let page = Page::build(&entries).unwrap();
        Self {
            descriptor: ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
                namespace_view_id: [0x55; 32],
                scope: "/project".into(),
                metadata_root: page_id(&page),
            },
            page,
            bodies,
            metadata: Mutex::new(Vec::new()),
            requests: AtomicUsize::new(0),
            map_requests: AtomicUsize::new(0),
            leaf_requests: AtomicUsize::new(0),
            chunk_requests: AtomicUsize::new(0),
            emitted: Arc::new(AtomicUsize::new(0)),
            renewals: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            release: tokio::sync::Semaphore::new(0),
            objects,
            expiry: "2099-01-01T00:00:00Z".into(),
            large: None,
            nested_page: None,
        }
    }
    fn with_large(self) -> Self {
        self.with_large_file(Large::new())
    }
    fn with_large_file(mut self, large: Large) -> Self {
        let mut entries: Vec<_> = self
            .bodies
            .iter()
            .map(|(name, body)| {
                Entry::file(
                    match name.as_str() {
                        "exec" => EntryKind::Executable,
                        "link" => EntryKind::Symlink,
                        _ => EntryKind::Regular,
                    },
                    name.as_bytes(),
                    body.len() as u64,
                    hash(body),
                )
            })
            .collect();
        entries.extend((0..17).map(|index| {
            Entry::file(
                EntryKind::Regular,
                format!("range{index:03}").as_bytes(),
                large.map.file_size,
                large.map.file_content_id,
            )
        }));
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        self.page = Page::build(&entries).unwrap();
        self.descriptor.metadata_root = page_id(&self.page);
        self.large = Some(large);
        self
    }
    fn with_nested(mut self) -> Self {
        let bytes = vec![0x7b; 8192];
        let nested = Page::build(&[Entry::file(
            EntryKind::Regular,
            b"target",
            bytes.len() as u64,
            hash(&bytes),
        )])
        .unwrap();
        let mut entries: Vec<_> = self
            .bodies
            .iter()
            .map(|(name, body)| {
                Entry::file(
                    match name.as_str() {
                        "exec" => EntryKind::Executable,
                        "link" => EntryKind::Symlink,
                        _ => EntryKind::Regular,
                    },
                    name.as_bytes(),
                    body.len() as u64,
                    hash(body),
                )
            })
            .collect();
        entries.extend([
            Entry::dir(b"nested", page_id(&nested)),
            Entry::dir(b"unavailable", [0x88; 32]),
        ]);
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        self.page = Page::build(&entries).unwrap();
        self.descriptor.metadata_root = page_id(&self.page);
        self.bodies.insert("nested/target".into(), bytes);
        self.nested_page = Some(nested);
        self
    }
    fn response(&self, request: &[u8], wire: Vec<u8>, pending: bool) -> Response {
        use futures::StreamExt;
        let body = if pending {
            let emitted = self.emitted.clone();
            Body::from_stream(
                futures::stream::once(async move {
                    emitted.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, std::io::Error>(Bytes::from(wire))
                })
                .chain(futures::stream::pending()),
            )
        } else {
            Body::from(wire)
        };
        Response::builder()
            .header("content-type", "application/vnd.mega.treeframe;version=2")
            .header(
                "x-mega-snapshot-id",
                id(&self.descriptor.snapshot_id().unwrap()),
            )
            .header("x-mega-request-digest", id(&hash(request)))
            .body(body)
            .unwrap()
    }
}
async fn capabilities(HttpState(f): HttpState<Arc<Fixture>>) -> Json<Value> {
    Json(
        json!({"protocol_versions":[2],"metadata_codecs":[1],"frame_encodings":["identity"],"features":{"resolve":true,"directory":true,"leases":true,"metadata_pages":true,"objects":f.objects,"raw_blob":true,"chunk_reads":true}}),
    )
}
async fn resolve(HttpState(f): HttpState<Arc<Fixture>>) -> Json<Value> {
    let d = &f.descriptor;
    Json(
        json!({"descriptor":{"schema_version":2,"metadata_codec":1,"instance_id":INSTANCE,"namespace_view_id":id(&d.namespace_view_id),"scope":d.scope,"materialization_policy":1,"fs_semantics":1,"access_projection":0,"metadata_root":id(&d.metadata_root),"snapshot_id":id(&d.snapshot_id().unwrap())},"lease_id":"owned-fuse-lease","lease_expires_at":f.expiry,"publication_sequence":"1","authorization_epoch":"1"}),
    )
}
async fn renew(HttpState(f): HttpState<Arc<Fixture>>) -> (StatusCode, Json<Value>) {
    f.renewals.fetch_add(1, Ordering::SeqCst);
    (
        StatusCode::FORBIDDEN,
        Json(json!({"error":{"code":"SCOPE_FORBIDDEN","message":"revoked FUSE lease"}})),
    )
}
async fn metadata(HttpState(f): HttpState<Arc<Fixture>>, request: Bytes) -> Response {
    let value: Value = serde_json::from_slice(&request).unwrap();
    let items = value["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let directory = items[0]["directory_path"].as_str().unwrap();
    f.metadata.lock().unwrap().push(directory.into());
    let page = match directory {
        "/" => &f.page,
        "/nested" => f.nested_page.as_ref().unwrap(),
        _ => panic!("unrelated snapshot closure walk: {directory}"),
    };
    assert_eq!(items[0]["route"], json!([]));
    let mut wire = MetaPayload {
        pages: vec![(page_id(page), page.clone())],
    }
    .encode(51, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: page.len() as u64,
            request_body_sha256: hash(&request),
        }
        .encode(51, 1),
    );
    f.response(&request, wire, false)
}
async fn objects(HttpState(f): HttpState<Arc<Fixture>>, request: Bytes) -> Response {
    f.requests.fetch_add(1, Ordering::SeqCst);
    let mode = f.mode.load(Ordering::SeqCst);
    if mode == 4 {
        f.release.acquire().await.unwrap().forget();
    }
    let value: Value = serde_json::from_slice(&request).unwrap();
    let items = value["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    let name = items[0]["path"].as_str().unwrap().trim_start_matches('/');
    let body = &f.bodies[name];
    assert_eq!(items[0]["expected_digest"], id(&hash(body)));
    let mut wire = ObjectPayload {
        objects: vec![(hash(body), body.clone())],
    }
    .encode(52, 0)
    .unwrap();
    if mode == 1 {
        // Encode the valid OBJECT first, then corrupt its content while keeping
        // the outer frame digest valid. The actual client must reject the
        // inner object digest; the validating fixture encoder cannot forge it.
        *wire.last_mut().unwrap() ^= 1;
        let frame_digest = hash(&wire[mst2_codec::treeframe::HEADER_LEN..]);
        wire[32..64].copy_from_slice(&frame_digest);
    }
    let mut end = EndPayload {
        request_item_count: 1,
        unique_unit_count: 1,
        logical_bytes: body.len() as u64,
        request_body_sha256: hash(&request),
    };
    if mode == 2 {
        end.request_body_sha256[0] ^= 1;
    }
    wire.extend(end.encode(52, 1));
    f.response(&request, wire, mode == 3)
}
async fn raw(
    HttpState(f): HttpState<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    f.requests.fetch_add(1, Ordering::SeqCst);
    let bytes = &f.bodies[query["path"].trim_start_matches('/')];
    assert_eq!(query["expected_digest"], id(&hash(bytes)));
    Response::builder().body(Body::from(bytes.clone())).unwrap()
}
async fn map_response(
    HttpState(f): HttpState<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    f.map_requests.fetch_add(1, Ordering::SeqCst);
    let map = &f.large.as_ref().unwrap().map;
    assert!(query["path"].trim_start_matches('/').starts_with("range"));
    assert_eq!(query["expected_digest"], id(&map.file_content_id));
    let mut value = json!({"snapshot_id":id(&f.descriptor.snapshot_id().unwrap()),"path":query["path"],"schema_version":2,"file_content_id":id(&map.file_content_id),"file_size":map.file_size.to_string(),"chunk_size":CHUNK_SIZE,"chunk_count":map.chunk_count.to_string(),"page_count":map.page_count.to_string(),"pages_root":id(&map.pages_root),"map_id":id(&map.map_id())});
    if f.mode.load(Ordering::SeqCst) == 5 {
        value["file_size"] = json!((map.file_size + 1).to_string());
    }
    Json(value)
}
async fn leaf_response(
    HttpState(f): HttpState<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    f.leaf_requests.fetch_add(1, Ordering::SeqCst);
    let large = f.large.as_ref().unwrap();
    assert_eq!(query["page"], "0");
    Json(
        json!({"snapshot_id":id(&f.descriptor.snapshot_id().unwrap()),"path":query["path"],"map_id":id(&large.map.map_id()),"page_count":"1","leaf":{"page_index":"0","count":large.map.chunk_count.to_string(),"data_base64":base64(&large.leaf.encode().unwrap())},"proof":[]}),
    )
}
async fn chunks(HttpState(f): HttpState<Arc<Fixture>>, request: Bytes) -> Response {
    f.chunk_requests.fetch_add(1, Ordering::SeqCst);
    let mode = f.mode.load(Ordering::SeqCst);
    if mode == 4 {
        f.release.acquire().await.unwrap().forget();
    }
    let large = f.large.as_ref().unwrap();
    let value: Value = serde_json::from_slice(&request).unwrap();
    let items = value["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert!(items[0]["path"].as_str().unwrap().starts_with("/range"));
    assert_eq!(items[0]["expected_digest"], id(&large.map.file_content_id));
    assert_eq!(items[0]["map_id"], id(&large.map.map_id()));
    let index = items[0]["chunk_index"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    if mode == 10 && index >= 4 {
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("late chunk transport failure"))
            .unwrap();
    }
    let bytes = large.bytes(index);
    let mut payload = ChunkPayload {
        map_id: large.map.map_id(),
        file_content_id: large.map.file_content_id,
        chunk_index: index,
        chunk_bytes: bytes.clone(),
    };
    if mode == 1 {
        payload.chunk_bytes[0] ^= 1;
    }
    if mode == 6 {
        payload.map_id[0] ^= 1;
    }
    let mut wire = payload.encode(53, 0).unwrap();
    let mut end = EndPayload {
        request_item_count: 1,
        unique_unit_count: 1,
        logical_bytes: bytes.len() as u64,
        request_body_sha256: hash(&request),
    };
    if mode == 2 || mode == 7 && index > 0 || mode == 8 && index >= 4 {
        end.request_body_sha256[0] ^= 1;
    }
    wire.extend(end.encode(53, 1));
    f.response(&request, wire, mode == 3 || mode == 9 && index >= 4)
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
    async fn start(fixture: Fixture, limit: usize) -> Self {
        let fixture = Arc::new(fixture);
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(capabilities))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .route("/api/v2/snapshots/leases/{lease}/renew", post(renew))
            .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
            .route("/api/v2/snapshots/{sid}/objects", post(objects))
            .route("/api/v2/snapshots/{sid}/blob", get(raw))
            .route("/api/v2/snapshots/{sid}/chunk-map", get(map_response))
            .route(
                "/api/v2/snapshots/{sid}/chunk-map/pages",
                get(leaf_response),
            )
            .route("/api/v2/snapshots/{sid}/chunks", post(chunks))
            .with_state(fixture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client =
            crate::snapshot::Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let reader = SnapshotReader::resolve(client, "/project", 600)
            .await
            .unwrap()
            .with_content_limits(
                crate::snapshot::ContentBudgetLimits::new(limit, 8 * 1024 * 1024).unwrap(),
            );
        Self {
            reader,
            fixture,
            task,
        }
    }
    async fn view(&self, lazy: bool) -> Mst2Fuse {
        if lazy {
            Mst2Fuse::from_reader_lazy(self.reader.clone(), None)
                .await
                .unwrap()
        } else {
            Mst2Fuse::from_reader(self.reader.clone()).await.unwrap()
        }
    }
}
async fn inode(fs: &Mst2Fuse, name: &str) -> u64 {
    fs.lookup(Request::default(), ROOT_INODE, OsStr::new(name))
        .await
        .unwrap()
        .attr
        .ino
}
async fn read(fs: &Mst2Fuse, name: &str, offset: u64, size: u32) -> ReplyData {
    let inode = inode(fs, name).await;
    fs.read(Request::default(), inode, inode, offset, size)
        .await
        .unwrap()
}
async fn idle(reader: &SnapshotReader) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while reader.content_usage().construction_bytes != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_reply_clones_survive_small_cache_eviction_mount_drop_and_keep_quota() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true), 256 * 1024).await;
    let fs = server.view(false).await;
    let slots = server.reader.content_usage().output_bytes;
    let reply = read(&fs, "file000", 17, 31).await;
    let paid = server.reader.content_usage().output_bytes - slots;
    let cloned_reply = reply.clone();
    let last = cloned_reply.data.slice(1..8);
    assert_eq!(last.as_ptr(), reply.data.as_ptr().wrapping_add(1));
    assert_eq!(last.as_ref(), [0; 7]);
    for index in 1..17 {
        drop(read(&fs, &format!("file{index:03}"), 0, 1).await);
    }
    assert_eq!(
        fs.state
            .lock()
            .unwrap()
            .owned
            .as_ref()
            .unwrap()
            .contents
            .len(),
        16
    );
    assert!(fs.state.lock().unwrap().contents.is_empty());
    drop(reply);
    drop(cloned_reply);
    drop(fs);
    assert_eq!(server.reader.content_usage().output_bytes, paid);
    let fs = server.view(false).await;
    let big = inode(&fs, "big").await;
    let before = server.fixture.requests.load(Ordering::SeqCst);
    assert!(fs.read(Request::default(), big, big, 0, 1).await.is_err());
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), before);
    assert_eq!(last.as_ref(), [0; 7]);
    drop(last);
    assert_eq!(server.reader.content_usage().output_bytes, slots);
    assert_eq!(
        fs.read(Request::default(), big, big, 0, 1)
            .await
            .unwrap()
            .data
            .as_ref(),
        [0x66]
    );
    drop(fs);
    idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn lazy_actual_read_proves_only_target_and_eager_seed_avoids_repeat_metadata() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(true, true), 1024 * 1024).await;
    let fs = server.view(true).await;
    assert_eq!(read(&fs, "file000", 0, 4).await.data.as_ref(), [0; 4]);
    assert!(server.reader.content_membership.get().is_none());
    assert_eq!(
        server.fixture.metadata.lock().unwrap().as_slice(),
        ["/", "/"]
    );
    drop(read(&fs, "file000", 0, 1).await);
    assert_eq!(server.fixture.metadata.lock().unwrap().len(), 2);
    let server = Server::start(Fixture::new(false, true), 1024 * 1024).await;
    let fs = server.view(false).await;
    for name in ["file000", "exec", "link"] {
        let inode = inode(&fs, name).await;
        let reply = if name == "link" {
            fs.readlink(Request::default(), inode).await.unwrap()
        } else {
            read(&fs, name, 0, 4).await
        };
        assert!(!reply.data.is_empty());
    }
    assert_eq!(server.fixture.metadata.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn actual_raw_and_symlink_reply_owners_release_only_after_last_bytes() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, false), 1024 * 1024).await;
    let fs = server.view(false).await;
    let link = inode(&fs, "link").await;
    let reply = fs.readlink(Request::default(), link).await.unwrap();
    let clone = reply.clone();
    assert_eq!(clone.data.as_ptr(), reply.data.as_ptr());
    assert_eq!(clone.data.as_ref(), b"file000");
    drop(reply);
    drop(fs);
    assert_eq!(server.reader.content_usage().output_bytes, 2048);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 1);
    drop(clone);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn reply_metadata_admission_rejects_before_body_but_valid_empty_replies_have_no_owner() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true), 1024).await;
    let fs = server.view(false).await;
    let file = inode(&fs, "file000").await;
    assert!(fs.read(Request::default(), file, file, 0, 1).await.is_err());
    for (offset, size) in [(0, 0), (8192, 1), (u64::MAX, u32::MAX)] {
        assert!(fs
            .read(Request::default(), file, file, offset, size)
            .await
            .unwrap()
            .data
            .is_empty());
    }
    assert!(read(&fs, "empty", 0, 1).await.data.is_empty());
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
    assert_eq!(server.reader.content_usage().output_bytes, 1024);
}

#[tokio::test]
async fn failed_body_and_end_do_not_cache_or_retry_through_a_legacy_api() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true), 1024 * 1024).await;
    let fs = server.view(false).await;
    let file = inode(&fs, "file000").await;
    let slots = server.reader.content_usage().output_bytes;
    for mode in [1, 2] {
        server.fixture.mode.store(mode, Ordering::SeqCst);
        let before = server.fixture.requests.load(Ordering::SeqCst);
        assert_eq!(
            i32::from(
                fs.read(Request::default(), file, file, 0, 1)
                    .await
                    .unwrap_err()
            ),
            -libc::EIO
        );
        idle(&server.reader).await;
        assert_eq!(server.fixture.requests.load(Ordering::SeqCst), before + 1);
        assert_eq!(server.reader.content_usage().output_bytes, slots);
        assert_eq!(
            fs.state
                .lock()
                .unwrap()
                .owned
                .as_ref()
                .unwrap()
                .contents
                .len(),
            0
        );
        assert!(fs.state.lock().unwrap().contents.is_empty());
    }
    server.fixture.mode.store(0, Ordering::SeqCst);
    assert_eq!(read(&fs, "file000", 0, 1).await.data.as_ref(), [0]);
}

#[tokio::test]
async fn actual_fuse_future_cancellation_before_and_after_body_restores_only_its_owners() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true), 1024 * 1024).await;
    let fs = Arc::new(server.view(false).await);
    let retained = read(&fs, "file000", 0, 4).await;
    let baseline = server.reader.content_usage().output_bytes;
    for (mode, name) in [(4, "file001"), (3, "file002")] {
        server.fixture.mode.store(mode, Ordering::SeqCst);
        let file = inode(&fs, name).await;
        let before = server.fixture.requests.load(Ordering::SeqCst);
        let emitted = server.fixture.emitted.load(Ordering::SeqCst);
        let task = tokio::spawn({
            let fs = fs.clone();
            async move { fs.read(Request::default(), file, file, 0, 4).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.fixture.requests.load(Ordering::SeqCst) == before
                || mode == 3 && server.fixture.emitted.load(Ordering::SeqCst) == emitted
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !task.is_finished(),
            "actual pending HTTP unexpectedly published"
        );
        assert!(server.reader.content_usage().output_bytes > baseline);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        if mode == 4 {
            server.fixture.release.add_permits(1);
        }
        idle(&server.reader).await;
        assert_eq!(server.reader.content_usage().output_bytes, baseline);
        assert_eq!(retained.data.as_ref(), [0; 4]);
        assert_eq!(
            fs.state
                .lock()
                .unwrap()
                .owned
                .as_ref()
                .unwrap()
                .contents
                .len(),
            1
        );
    }
    server.fixture.mode.store(0, Ordering::SeqCst);
    assert_eq!(read(&fs, "file001", 0, 1).await.data.as_ref(), [1]);
    assert_eq!(read(&fs, "file002", 0, 1).await.data.as_ref(), [2]);
    drop(retained);
    drop(fs);
    idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn inode_tuple_and_unproven_public_manifest_cannot_authorize_a_body_or_empty_reply() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true), 1024 * 1024).await;
    for case in 0..4 {
        let fs = server.view(false).await;
        let file = inode(&fs, "file000").await;
        drop(fs.read(Request::default(), file, file, 0, 1).await.unwrap());
        {
            let mut state = fs.state.lock().unwrap();
            let Node::File(node) = state.nodes.get_mut(&file).unwrap() else {
                panic!("file fixture");
            };
            match case {
                0 => node.path = "file001".into(),
                1 => node.fs_kind = "symlink".into(),
                2 => node.size = 0,
                _ => node.digest = id(&[0x99; 32]),
            }
        }
        let before = server.fixture.requests.load(Ordering::SeqCst);
        for (offset, size) in [(0, 1), (u64::MAX, 0)] {
            assert_eq!(
                i32::from(
                    fs.read(Request::default(), file, file, offset, size)
                        .await
                        .unwrap_err()
                ),
                -libc::EIO
            );
        }
        if case == 1 {
            assert_eq!(
                i32::from(fs.readlink(Request::default(), file).await.unwrap_err()),
                -libc::EIO
            );
        }
        assert_eq!(server.fixture.requests.load(Ordering::SeqCst), before);
    }
    let server = Server::start(Fixture::new(false, true), 1024 * 1024).await;
    let fs = Mst2Fuse::build(
        Some(server.reader.clone()),
        None,
        vec![SnapshotFile {
            rel_path: "file000".into(),
            fs_kind: "regular".into(),
            size: 8192,
            content_digest: id(&[0x99; 32]),
        }],
    )
    .unwrap();
    let file = inode(&fs, "file000").await;
    assert!(fs.read(Request::default(), file, file, 0, 1).await.is_err());
    assert!(server.reader.content_membership.get().is_none());
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cached_content_readlink_and_true_eof_require_the_current_lease() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(false, true);
    let expiry = time::OffsetDateTime::now_utc() + time::Duration::seconds(4);
    fixture.expiry = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        expiry.year(),
        u8::from(expiry.month()),
        expiry.day(),
        expiry.hour(),
        expiry.minute(),
        expiry.second()
    );
    let server = Server::start(fixture, 1024 * 1024).await;
    let fs = server.view(false).await;
    let file = inode(&fs, "file000").await;
    let link = inode(&fs, "link").await;
    let prior = fs.read(Request::default(), file, file, 0, 4).await.unwrap();
    drop(fs.readlink(Request::default(), link).await.unwrap());
    tokio::time::timeout(Duration::from_secs(6), async {
        while server.fixture.renewals.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        server.reader.ensure_lease().await.unwrap_err().code,
        crate::snapshot::SnapshotErrorCode::ScopeForbidden
    );
    let before = server.fixture.requests.load(Ordering::SeqCst);
    for (offset, size) in [(0, 1), (0, 0), (8192, 1), (u64::MAX, u32::MAX)] {
        assert_eq!(
            i32::from(
                fs.read(Request::default(), file, file, offset, size)
                    .await
                    .unwrap_err()
            ),
            -libc::EACCES
        );
    }
    assert_eq!(
        i32::from(fs.readlink(Request::default(), link).await.unwrap_err()),
        -libc::EACCES
    );
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), before);
    assert_eq!(prior.data.as_ref(), [0; 4]);
}

#[tokio::test]
async fn actual_cross_chunk_reply_survives_range_reader_eviction_and_mount_drop() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true).with_large(), 32 * 1024 * 1024).await;
    let fs = server.view(false).await;
    let reply = read(&fs, "range000", CHUNK_SIZE as u64 - 2, 5).await;
    assert_eq!(reply.data.as_ref(), [0, 0, 1, 1, 1]);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 2);
    let clone = reply.clone();
    let last = clone.data.slice(1..4);
    assert_eq!(last.as_ptr(), reply.data.as_ptr().wrapping_add(1));
    for index in 1..17 {
        let eof = read(
            &fs,
            &format!("range{index:03}"),
            2 * CHUNK_SIZE as u64 + 3,
            u32::MAX,
        )
        .await;
        assert_eq!(eof.data.as_ref(), [2; 4]);
    }
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 17);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 18);
    {
        let state = fs.state.lock().unwrap();
        assert_eq!(state.owned.as_ref().unwrap().ranges.len(), 16);
        assert!(state.chunked.is_empty());
        assert!(state.contents.is_empty());
    }
    // The real first reader was evicted: opening this inode again requests its
    // map and both covering chunks. Its old reply continues owning its range.
    drop(read(&fs, "range000", CHUNK_SIZE as u64 - 2, 5).await);
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 18);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 20);
    drop(reply);
    drop(clone);
    drop(fs);
    idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 2048);
    assert_eq!(last.as_ref(), [0, 1, 1]);
    drop(last);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn actual_range_reply_retention_blocks_admission_until_the_last_bytes_drop() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true).with_large(), 5 * 1024).await;
    let fs = server.view(false).await;
    let reply = read(&fs, "range000", 2 * CHUNK_SIZE as u64, 7).await;
    assert_eq!(reply.data.as_ref(), [2; 7]);
    let retained = reply.data.clone();
    drop(reply);
    drop(fs);
    assert_eq!(server.reader.content_usage().output_bytes, 2048);
    let fs = server.view(false).await;
    let file = inode(&fs, "range001").await;
    let before = server.fixture.chunk_requests.load(Ordering::SeqCst);
    assert!(fs
        .read(Request::default(), file, file, 2 * CHUNK_SIZE as u64, 7)
        .await
        .is_err());
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), before);
    assert_eq!(server.reader.content_usage().output_bytes, 3072);
    assert_eq!(retained.as_ref(), [2; 7]);
    drop(retained);
    assert_eq!(
        read(&fs, "range001", 2 * CHUNK_SIZE as u64, 7)
            .await
            .data
            .as_ref(),
        [2; 7]
    );
    drop(fs);
    idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn malformed_actual_range_map_chunk_and_end_publish_no_reply_or_reader_cache() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true).with_large(), 8 * 1024 * 1024).await;
    let fs = server.view(false).await;
    let file = inode(&fs, "range000").await;
    let slots = server.reader.content_usage().output_bytes;
    for (mode, chunks) in [(5, 0), (1, 1), (2, 1), (6, 1), (7, 2)] {
        server.fixture.mode.store(mode, Ordering::SeqCst);
        let maps_before = server.fixture.map_requests.load(Ordering::SeqCst);
        let chunks_before = server.fixture.chunk_requests.load(Ordering::SeqCst);
        assert_eq!(
            i32::from(
                fs.read(Request::default(), file, file, CHUNK_SIZE as u64 - 2, 5)
                    .await
                    .unwrap_err()
            ),
            -libc::EIO
        );
        idle(&server.reader).await;
        assert_eq!(
            server.fixture.map_requests.load(Ordering::SeqCst),
            maps_before + 1
        );
        assert_eq!(
            server.fixture.chunk_requests.load(Ordering::SeqCst),
            chunks_before + chunks
        );
        assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
        assert_eq!(server.reader.content_usage().output_bytes, slots);
        let state = fs.state.lock().unwrap();
        assert_eq!(state.owned.as_ref().unwrap().ranges.len(), 0);
        assert!(state.chunked.is_empty());
    }
    server.fixture.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        read(&fs, "range000", CHUNK_SIZE as u64 - 2, 5)
            .await
            .data
            .as_ref(),
        [0, 0, 1, 1, 1]
    );
}

#[tokio::test]
async fn actual_range_cancellation_before_and_after_chunk_body_keeps_other_reply_paid() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true).with_large(), 8 * 1024 * 1024).await;
    let fs = Arc::new(server.view(false).await);
    let retained = read(&fs, "range000", 2 * CHUNK_SIZE as u64, 7).await;
    let baseline = server.reader.content_usage().output_bytes;
    for (mode, name) in [(4, "range001"), (3, "range002")] {
        server.fixture.mode.store(mode, Ordering::SeqCst);
        let file = inode(&fs, name).await;
        let before = server.fixture.chunk_requests.load(Ordering::SeqCst);
        let emitted = server.fixture.emitted.load(Ordering::SeqCst);
        let task = tokio::spawn({
            let fs = fs.clone();
            async move { fs.read(Request::default(), file, file, 0, 4).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.fixture.chunk_requests.load(Ordering::SeqCst) == before
                || mode == 3 && server.fixture.emitted.load(Ordering::SeqCst) == emitted
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !task.is_finished(),
            "actual pending chunk HTTP published a reply"
        );
        assert!(server.reader.content_usage().output_bytes > baseline);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        if mode == 4 {
            server.fixture.release.add_permits(1);
        }
        idle(&server.reader).await;
        assert_eq!(server.reader.content_usage().output_bytes, baseline);
        assert_eq!(retained.data.as_ref(), [2; 7]);
        assert_eq!(
            fs.state
                .lock()
                .unwrap()
                .owned
                .as_ref()
                .unwrap()
                .ranges
                .len(),
            1
        );
    }
    server.fixture.mode.store(0, Ordering::SeqCst);
    for name in ["range001", "range002"] {
        assert_eq!(read(&fs, name, 0, 4).await.data.as_ref(), [0; 4]);
    }
    drop(retained);
    drop(fs);
    idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn cached_actual_range_tuple_and_revoked_lease_reject_before_http_including_eof() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(false, true).with_large();
    let expiry = time::OffsetDateTime::now_utc() + time::Duration::seconds(4);
    fixture.expiry = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        expiry.year(),
        u8::from(expiry.month()),
        expiry.day(),
        expiry.hour(),
        expiry.minute(),
        expiry.second()
    );
    let server = Server::start(fixture, 8 * 1024 * 1024).await;
    let fs = server.view(false).await;
    let file = inode(&fs, "range000").await;
    let prior = read(&fs, "range000", 2 * CHUNK_SIZE as u64, 7).await;
    let original = match fs.node(file).unwrap() {
        Node::File(file) => file,
        _ => panic!("range inode"),
    };
    let before = (
        server.fixture.map_requests.load(Ordering::SeqCst),
        server.fixture.chunk_requests.load(Ordering::SeqCst),
    );
    for case in 0..4 {
        let mut node = original.clone();
        match case {
            0 => node.path = "range001".into(),
            1 => node.fs_kind = "executable".into(),
            2 => node.size += 1,
            _ => node.digest = id(&[0x99; 32]),
        }
        fs.state
            .lock()
            .unwrap()
            .nodes
            .insert(file, Node::File(node));
        for (offset, size) in [(0, 1), (u64::MAX, 0)] {
            assert_eq!(
                i32::from(
                    fs.read(Request::default(), file, file, offset, size)
                        .await
                        .unwrap_err()
                ),
                -libc::EIO
            );
        }
    }
    fs.state
        .lock()
        .unwrap()
        .nodes
        .insert(file, Node::File(original));
    tokio::time::timeout(Duration::from_secs(6), async {
        while server.fixture.renewals.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        server.reader.ensure_lease().await.unwrap_err().code,
        crate::snapshot::SnapshotErrorCode::ScopeForbidden
    );
    for (offset, size) in [
        (0, 1),
        (0, 0),
        (2 * CHUNK_SIZE as u64 + 7, 1),
        (u64::MAX, u32::MAX),
    ] {
        assert_eq!(
            i32::from(
                fs.read(Request::default(), file, file, offset, size)
                    .await
                    .unwrap_err()
            ),
            -libc::EACCES
        );
    }
    assert_eq!(
        (
            server.fixture.map_requests.load(Ordering::SeqCst),
            server.fixture.chunk_requests.load(Ordering::SeqCst)
        ),
        before
    );
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
    assert_eq!(prior.data.as_ref(), [2; 7]);
}

#[tokio::test]
async fn fixed_cache_construction_admission_failure_leaks_no_slots_or_reply_owners() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true), 3 * 1024).await;
    let fs = server.view(false).await;
    let link = inode(&fs, "link").await;
    let retained = fs.readlink(Request::default(), link).await.unwrap();
    drop(fs);
    assert_eq!(server.reader.content_usage().output_bytes, 2048);
    let fs = server.view(false).await;
    assert!(Mst2Fuse::from_reader(server.reader.clone()).await.is_err());
    assert_eq!(server.reader.content_usage().output_bytes, 3072);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 1);
    drop(fs);
    drop(retained);
    idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
    drop(server.view(false).await);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn actual_lazy_nested_read_uses_selected_ancestors_without_unrelated_closure() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true).with_nested(), 1024 * 1024).await;
    let fs = server.view(true).await;
    let nested = inode(&fs, "nested").await;
    let file = fs
        .lookup(Request::default(), nested, OsStr::new("target"))
        .await
        .unwrap()
        .attr
        .ino;
    assert_eq!(
        fs.read(Request::default(), file, file, 7, 4)
            .await
            .unwrap()
            .data
            .as_ref(),
        [0x7b; 4]
    );
    assert!(server.reader.content_membership.get().is_none());
    assert_eq!(
        server.fixture.metadata.lock().unwrap().as_slice(),
        ["/", "/nested", "/", "/nested"]
    );
    let before = server.fixture.metadata.lock().unwrap().len();
    assert_eq!(
        fs.read(Request::default(), file, file, 0, 1)
            .await
            .unwrap()
            .data
            .as_ref(),
        [0x7b]
    );
    assert_eq!(server.fixture.metadata.lock().unwrap().len(), before);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 1);
}
