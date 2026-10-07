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
    response::{IntoResponse, Response},
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
    map_not_ready: AtomicUsize,
    leaf_requests: AtomicUsize,
    chunk_requests: AtomicUsize,
    emitted: Arc<AtomicUsize>,
    renewals: AtomicUsize,
    renewal_status: u16,
    mode: AtomicUsize,
    release: tokio::sync::Semaphore,
    objects: bool,
    metadata_pages: bool,
    directory_requests: AtomicUsize,
    content_paths: Mutex<Vec<String>>,
    range_paths: Mutex<Vec<String>>,
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
            map_not_ready: AtomicUsize::new(0),
            leaf_requests: AtomicUsize::new(0),
            chunk_requests: AtomicUsize::new(0),
            emitted: Arc::new(AtomicUsize::new(0)),
            renewals: AtomicUsize::new(0),
            renewal_status: 403,
            mode: AtomicUsize::new(0),
            release: tokio::sync::Semaphore::new(0),
            objects,
            metadata_pages: true,
            directory_requests: AtomicUsize::new(0),
            content_paths: Mutex::new(Vec::new()),
            range_paths: Mutex::new(Vec::new()),
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
        json!({"protocol_versions":[2],"metadata_codecs":[1],"frame_encodings":["identity"],"features":{"resolve":true,"directory":true,"leases":true,"metadata_pages":f.metadata_pages,"objects":f.objects,"raw_blob":true,"chunk_reads":true}}),
    )
}
async fn directory(
    HttpState(f): HttpState<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert!(!f.metadata_pages);
    assert_eq!(query["path"], "/");
    assert_eq!(query["limit"], "256");
    assert!(!query.contains_key("cursor"));
    f.directory_requests.fetch_add(1, Ordering::SeqCst);
    let mut entries: Vec<Value> = f.bodies.iter().map(|(name, body)| json!({
        "name": name,
        "fs_kind": match name.as_str() { "exec" => "executable", "link" => "symlink", _ => "regular" },
        "size": body.len().to_string(), "content_digest": id(&hash(body)),
    })).collect();
    if let Some(large) = &f.large {
        entries.extend((0..17).map(|index| json!({
            "name":format!("range{index:03}"), "fs_kind":"regular",
            "size":large.map.file_size.to_string(), "content_digest":id(&large.map.file_content_id),
        })));
    }
    entries.sort_by(|a, b| a["name"].as_str().unwrap().cmp(b["name"].as_str().unwrap()));
    Json(
        json!({"snapshot_id": id(&f.descriptor.snapshot_id().unwrap()), "path":"/",
            "metadata_root": id(&f.descriptor.metadata_root), "directory_root":id(&f.descriptor.metadata_root),
            "node_class":"native_tree", "lifecycle":"immutable_release", "range_start_exclusive":null,
            "entry_count":entries.len().to_string(), "entries":entries, "next_cursor":null, "proof_pages":[],
        }),
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
        StatusCode::from_u16(f.renewal_status).unwrap(),
        Json(
            json!({"error":{"code":if f.renewal_status == 410 { "SNAPSHOT_GONE" } else { "SCOPE_FORBIDDEN" },"message":"revoked FUSE lease"}}),
        ),
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
    f.content_paths.lock().unwrap().push(name.into());
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
    f.content_paths
        .lock()
        .unwrap()
        .push(query["path"].trim_start_matches('/').into());
    assert_eq!(query["expected_digest"], id(&hash(bytes)));
    Response::builder().body(Body::from(bytes.clone())).unwrap()
}
async fn map_response(
    HttpState(f): HttpState<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    f.map_requests.fetch_add(1, Ordering::SeqCst);
    f.range_paths
        .lock()
        .unwrap()
        .push(query["path"].trim_start_matches('/').into());
    let map = &f.large.as_ref().unwrap().map;
    assert!(query["path"].trim_start_matches('/').starts_with("range"));
    assert_eq!(query["expected_digest"], id(&map.file_content_id));
    if f.map_not_ready
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
            remaining.checked_sub(1)
        })
        .is_ok()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":{"code":"METADATA_NOT_READY","message":"fixed map is being prepared","request_id":"fuse-map","retryable":false}})),
        )
            .into_response();
    }
    let mut value = json!({"snapshot_id":id(&f.descriptor.snapshot_id().unwrap()),"path":query["path"],"schema_version":2,"file_content_id":id(&map.file_content_id),"file_size":map.file_size.to_string(),"chunk_size":CHUNK_SIZE,"chunk_count":map.chunk_count.to_string(),"page_count":map.page_count.to_string(),"pages_root":id(&map.pages_root),"map_id":id(&map.map_id())});
    if f.mode.load(Ordering::SeqCst) == 5 {
        value["file_size"] = json!((map.file_size + 1).to_string());
    }
    Json(value).into_response()
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
    if mode == 11 && index >= 4 {
        f.release.acquire().await.unwrap().forget();
    }
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
            .route("/api/v2/snapshots/{sid}/directory", get(directory))
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

    async fn stored_view(&self) -> (tempfile::TempDir, Arc<DurableStore>, Mst2Fuse) {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            DurableStore::open_for_reader(
                temp.path().join("view"),
                temp.path().join("cas"),
                &self.reader,
            )
            .unwrap(),
        );
        let view = Mst2Fuse::from_reader_with_store(self.reader.clone(), store.clone())
            .await
            .unwrap();
        (temp, store, view)
    }
}

#[tokio::test]
async fn no_pages_public_constructors_keep_path_requests_independent_and_actual_replies_paid() {
    let _serial = TEST_LOCK.lock().await;
    for (stored, objects) in [(false, true), (true, true), (false, false), (true, false)] {
        let mut fixture = Fixture::new(false, objects);
        fixture.metadata_pages = false;
        fixture
            .bodies
            .insert("file001".into(), fixture.bodies["file000"].clone());
        let server = Server::start(fixture.with_large(), 32 * 1024 * 1024).await;
        let (_temp, _store, fs) = if stored {
            let (temp, store, fs) = server.stored_view().await;
            (Some(temp), Some(store), fs)
        } else {
            (None, None, server.view(false).await)
        };
        assert_eq!(server.fixture.directory_requests.load(Ordering::SeqCst), 1);
        assert!(server.fixture.metadata.lock().unwrap().is_empty());
        assert!(server.reader.content_membership.get().is_none());
        assert_eq!(
            fs.path_state("missing").await.unwrap_err().code,
            SnapshotErrorCode::SnapshotNotReady
        );
        let slots = server.reader.content_usage().output_bytes;
        let first = read(&fs, "file000", 17, 31).await;
        let alias = read(&fs, "file001", 17, 31).await;
        assert_eq!(first.data.as_ref(), alias.data.as_ref());
        assert_ne!(first.data.as_ptr(), alias.data.as_ptr());
        assert_eq!(
            server.fixture.content_paths.lock().unwrap().as_slice(),
            ["file000", "file001"]
        );
        let warmed = read(&fs, "file000", 17, 31).await;
        assert_eq!(warmed.data.as_ptr(), first.data.as_ptr());
        assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 2);
        let large = read(&fs, "range000", CHUNK_SIZE as u64 - 2, 5).await;
        let large_alias = read(&fs, "range001", CHUNK_SIZE as u64 - 2, 5).await;
        assert_eq!(large.data.as_ref(), [0, 0, 1, 1, 1]);
        assert_eq!(large.data.as_ref(), large_alias.data.as_ref());
        assert_eq!(
            server.fixture.range_paths.lock().unwrap().as_slice(),
            ["range000", "range001"]
        );
        assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 2);
        assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 4);
        let link = inode(&fs, "link").await;
        assert_eq!(
            fs.readlink(Request::default(), link)
                .await
                .unwrap()
                .data
                .as_ref(),
            b"file000"
        );
        assert!(server.reader.content_usage().output_bytes > slots);
        assert_eq!(server.reader.content_usage().construction_bytes, 0);
        drop(alias);
        drop(warmed);
        drop(large_alias);
        let last = first.data.slice(1..8);
        let tail = large.data.slice(1..4);
        drop(first);
        drop(large);
        drop(fs);
        assert!(server.reader.content_usage().output_bytes > 0);
        assert_eq!(last.as_ref(), [0; 7]);
        assert_eq!(tail.as_ref(), [0, 1, 1]);
        drop(last);
        drop(tail);
        assert_eq!(server.reader.content_usage().output_bytes, 0);
    }
}

#[tokio::test]
async fn legacy_pages_stored_constructor_uses_actual_full_proof_and_preserves_empty_directories() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(false, true);
    let empty = Page::build(&[]).unwrap();
    let mut entries: Vec<_> = fixture
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
    entries.push(Entry::dir(b"nested", page_id(&empty)));
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    fixture.page = Page::build(&entries).unwrap();
    fixture.descriptor.metadata_root = page_id(&fixture.page);
    fixture.nested_page = Some(empty);
    let server = Server::start(fixture, 8 * 1024 * 1024).await;
    assert!(matches!(
        server.reader.capability_advertisement(),
        crate::snapshot::capabilities::CapabilityAdvertisement::Legacy(_)
    ));
    let (_temp, store, fs) = server.stored_view().await;
    assert_eq!(
        server.fixture.metadata.lock().unwrap().as_slice(),
        ["/", "/nested"]
    );
    assert!(server.reader.content_membership.get().is_some());
    assert_eq!(server.fixture.directory_requests.load(Ordering::SeqCst), 0);
    assert_eq!(
        fs.path_state("nested/missing").await.unwrap(),
        SnapshotPathState::AbsentProven
    );
    let baseline = server.fixture.metadata.lock().unwrap().len();
    std::fs::write(
        store
            .content_dir()
            .join(hex::encode(hash(&server.fixture.bodies["file000"]))),
        &server.fixture.bodies["file000"],
    )
    .unwrap();
    let reply = read(&fs, "file000", 0, 4).await;
    assert_eq!(reply.data.as_ref(), [0; 4]);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
    assert_eq!(server.fixture.metadata.lock().unwrap().len(), baseline);
    assert!(fs.state.lock().unwrap().online.is_none());
    assert!(fs.state.lock().unwrap().store_small.is_some());
    drop(reply);
    drop(fs);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn no_pages_raw_builder_tuple_changes_and_foreign_tokens_never_authorize_cached_or_empty_replies(
) {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(false, true);
    fixture.metadata_pages = false;
    let server = Server::start(fixture, 8 * 1024 * 1024).await;
    let raw = Mst2Fuse::build(
        Some(server.reader.clone()),
        None,
        server.reader.file_manifest().await.unwrap(),
    )
    .unwrap();
    let file = inode(&raw, "file000").await;
    for (offset, size) in [(0, 1), (u64::MAX, 0)] {
        assert_eq!(
            i32::from(
                raw.read(Request::default(), file, file, offset, size)
                    .await
                    .unwrap_err()
            ),
            -libc::EIO
        );
    }
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
    drop(raw);
    let mut foreign_fixture = Fixture::new(false, true);
    foreign_fixture.metadata_pages = false;
    let foreign = Server::start(foreign_fixture, 8 * 1024 * 1024).await;
    let foreign_fs = foreign.view(false).await;
    let foreign_file = foreign_fs
        .online_file_for_test(inode(&foreign_fs, "file000").await)
        .unwrap();
    for mutation in 0..5 {
        let fs = server.view(false).await;
        let file = inode(&fs, "file000").await;
        drop(fs.read(Request::default(), file, file, 0, 1).await.unwrap());
        let before = server.fixture.requests.load(Ordering::SeqCst);
        let Node::File(mut node) = fs.node(file).unwrap() else {
            panic!("file")
        };
        match mutation {
            0 => node.path = "file001".into(),
            1 => node.fs_kind = "symlink".into(),
            2 => node.size = 0,
            3 => node.digest = id(&[0x99; 32]),
            _ => node.online_file = Some(foreign_file.clone()),
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
        assert_eq!(server.fixture.requests.load(Ordering::SeqCst), before);
        drop(fs);
    }
    drop(foreign_fs);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
    assert_eq!(foreign.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn no_pages_held_replies_survive_actual_small_and_range_cache_eviction() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(false, true).with_large();
    fixture.metadata_pages = false;
    let server = Server::start(fixture, 32 * 1024 * 1024).await;
    let fs = server.view(false).await;
    let first = read(&fs, "file000", 17, 31).await;
    let large = read(&fs, "range000", CHUNK_SIZE as u64 - 2, 5).await;
    let small_pointer = first.data.as_ptr();
    let large_pointer = large.data.as_ptr();
    let last = first.data.clone().slice(1..8);
    let tail = large.data.clone().slice(1..4);
    for index in 1..17 {
        drop(read(&fs, &format!("file{index:03}"), 0, 1).await);
        drop(
            read(
                &fs,
                &format!("range{index:03}"),
                2 * CHUNK_SIZE as u64 + 3,
                u32::MAX,
            )
            .await,
        );
    }
    {
        let state = fs.state.lock().unwrap();
        let cache = state.online.as_ref().unwrap();
        assert_eq!(cache.contents.len(), 16);
        assert_eq!(cache.ranges.len(), 16);
    }
    drop(read(&fs, "file000", 0, 1).await);
    drop(read(&fs, "range000", CHUNK_SIZE as u64 - 2, 5).await);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 18);
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 18);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 20);
    drop(first);
    drop(large);
    drop(fs);
    assert_eq!(last.as_ptr(), small_pointer.wrapping_add(1));
    assert_eq!(tail.as_ptr(), large_pointer.wrapping_add(1));
    assert_eq!(last.as_ref(), [0; 7]);
    assert_eq!(tail.as_ref(), [0, 1, 1]);
    assert!(server.reader.content_usage().output_bytes > 8192);
    assert_eq!(server.reader.content_usage().construction_bytes, 0);
    drop(last);
    drop(tail);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn profiled_canonical_store_cache_still_proves_aliases_and_rejects_revoked_lease() {
    use crate::util::read_profile::{Metric, Phase, ReadProfile};

    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(false, true);
    fixture
        .bodies
        .insert("file001".into(), fixture.bodies["file000"].clone());
    fixture = fixture.with_large();
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
    let (_temp, store, fs) = server.stored_view().await;
    let body = &server.fixture.bodies["file000"];
    std::fs::write(store.content_dir().join(hex::encode(hash(body))), body).unwrap();
    let profile = ReadProfile::new();
    let fs = fs.with_read_profile(Some(profile.clone()));
    let file = inode(&fs, "file000").await;
    let alias = inode(&fs, "file001").await;
    let prior = fs
        .read(Request::default(), file, file, 17, 31)
        .await
        .unwrap();
    let twin = fs
        .read(Request::default(), alias, alias, 17, 31)
        .await
        .unwrap();
    assert_eq!(prior.data.as_ref(), &body[17..48]);
    assert_eq!(prior.data.as_ptr(), twin.data.as_ptr());
    let warm = profile.snapshot();
    assert!(!warm.overflow);
    assert_eq!(warm.metric(Metric::SmallCasCalls), 1);
    assert_eq!(warm.metric(Metric::SmallCasReadBytes), body.len() as u64);
    assert_eq!(
        warm.metric(Metric::SmallCasWholeHashBytes),
        body.len() as u64
    );
    assert_eq!(warm.metric(Metric::SmallCasAppendBytes), body.len() as u64);
    assert_eq!(warm.metric(Metric::OwnerCacheMiss), 1);
    assert_eq!(warm.metric(Metric::OwnerCacheHit), 1);
    assert_eq!(warm.phases[Phase::MembershipAndLease as usize].calls, 2);
    assert_eq!(warm.phases[Phase::CacheGet as usize].calls, 2);
    assert_eq!(warm.phases[Phase::CacheInsert as usize].calls, 2);
    assert!(warm.phases[Phase::ValidateProvenFile as usize].calls >= 3);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
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
    for (offset, size) in [(0, 1), (0, 0), (8192, 1), (u64::MAX, u32::MAX)] {
        assert_eq!(
            i32::from(
                fs.read(Request::default(), alias, alias, offset, size)
                    .await
                    .unwrap_err()
            ),
            -libc::EACCES
        );
    }
    let denied = profile.snapshot();
    assert_eq!(denied.metric(Metric::OwnerCacheHit), 1);
    assert_eq!(denied.metric(Metric::SmallCasCalls), 1);
    assert_eq!(denied.metric(Metric::ReplyOwners), 2);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
    assert_eq!(prior.data.as_ref(), &body[17..48]);
    drop(twin);
    drop(fs);
    assert!(server.reader.content_usage().output_bytes > 0);
    drop(prior);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
    assert_eq!(server.reader.content_usage().construction_bytes, 0);
}

#[tokio::test]
async fn online_stored_profiles_use_real_cas_owners_and_only_missing_objects_reach_wire() {
    let _serial = TEST_LOCK.lock().await;
    for metadata_pages in [false, true] {
        let mut fixture = Fixture::new(false, true).with_large();
        fixture.metadata_pages = metadata_pages;
        let server = Server::start(fixture, 32 * 1024 * 1024).await;
        let (_temp, store, fs) = server.stored_view().await;
        let small = &server.fixture.bodies["file000"];
        std::fs::write(store.content_dir().join(hex::encode(hash(small))), small).unwrap();
        let large = server.fixture.large.as_ref().unwrap();
        let body: Vec<_> = (0..large.map.chunk_count)
            .flat_map(|index| large.bytes(index))
            .collect();
        assert_eq!(hash(&body), large.map.file_content_id);
        let path = store
            .content_dir()
            .join(hex::encode(large.map.file_content_id));
        std::fs::write(&path, body).unwrap();
        let reply = read(&fs, "file000", 17, 31).await;
        let range = read(&fs, "range000", CHUNK_SIZE as u64 - 2, 5).await;
        assert_eq!(reply.data.as_ref(), [0; 31]);
        assert_eq!(range.data.as_ref(), [0, 0, 1, 1, 1]);
        assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
        assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 0);
        assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 0);
        assert_eq!(read(&fs, "file001", 0, 1).await.data.as_ref(), [1]);
        assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 1);
        // Large reads prefer local CAS even with an existing wire handle.
        // Removing this actual object is the only reason the next range
        // can create its independent wire handle.
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            read(&fs, "range001", 2 * CHUNK_SIZE as u64, 7)
                .await
                .data
                .as_ref(),
            [2; 7]
        );
        assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 1);
        assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 1);
        let small_last = reply.data.slice(1..8);
        let range_last = range.data.slice(1..4);
        drop(reply);
        drop(range);
        drop(fs);
        idle(&server.reader).await;
        assert_eq!(small_last.as_ref(), [0; 7]);
        assert_eq!(range_last.as_ref(), [0, 1, 1]);
        assert!(server.reader.content_usage().output_bytes > 8192);
        drop(small_last);
        drop(range_last);
        assert_eq!(server.reader.content_usage().output_bytes, 0);
    }
}

#[tokio::test]
async fn online_stored_corrupt_size_and_type_errors_never_fallback_or_publish_a_cache() {
    let _serial = TEST_LOCK.lock().await;
    for metadata_pages in [false, true] {
        for large in [false, true] {
            for damage in 0..4 {
                let mut fixture = Fixture::new(false, true).with_large();
                fixture.metadata_pages = metadata_pages;
                let server = Server::start(fixture, 8 * 1024 * 1024).await;
                let (_temp, store, fs) = server.stored_view().await;
                let (name, body) = if large {
                    let large = server.fixture.large.as_ref().unwrap();
                    (
                        "range000",
                        (0..large.map.chunk_count)
                            .flat_map(|index| large.bytes(index))
                            .collect::<Vec<_>>(),
                    )
                } else {
                    ("file000", server.fixture.bodies["file000"].clone())
                };
                let path = store.content_dir().join(hex::encode(hash(&body)));
                match damage {
                    0 => {
                        let mut bytes = body.clone();
                        *bytes.last_mut().unwrap() ^= 1;
                        std::fs::write(&path, bytes).unwrap();
                    }
                    1 => std::fs::write(&path, &body[..body.len() - 1]).unwrap(),
                    2 => {
                        let mut bytes = body.clone();
                        bytes.push(1);
                        std::fs::write(&path, bytes).unwrap();
                    }
                    _ => std::fs::create_dir(&path).unwrap(),
                }
                let file = inode(&fs, name).await;
                let baseline = server.reader.content_usage();
                for _ in 0..2 {
                    assert_eq!(
                        i32::from(
                            fs.read(Request::default(), file, file, 0, 13)
                                .await
                                .unwrap_err()
                        ),
                        -libc::EIO
                    );
                    idle(&server.reader).await;
                    assert_eq!(server.reader.content_usage(), baseline);
                    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
                    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 0);
                    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 0);
                    let state = fs.state.lock().unwrap();
                    if let Some(cache) = &state.online {
                        assert_eq!(cache.contents.len(), 0);
                        assert_eq!(cache.ranges.len(), 0);
                    }
                }
                if damage == 3 {
                    std::fs::remove_dir(&path).unwrap();
                }
                std::fs::write(&path, &body).unwrap();
                assert_eq!(
                    fs.read(Request::default(), file, file, 0, 13)
                        .await
                        .unwrap()
                        .data
                        .as_ref(),
                    &body[..13]
                );
                drop(fs);
                idle(&server.reader).await;
                assert_eq!(server.reader.content_usage().output_bytes, 0);
            }
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn online_stored_cas_symlinks_are_terminal_even_with_valid_target_bytes() {
    let _serial = TEST_LOCK.lock().await;
    for metadata_pages in [false, true] {
        for large in [false, true] {
            let mut fixture = Fixture::new(false, true).with_large();
            fixture.metadata_pages = metadata_pages;
            let server = Server::start(fixture, 8 * 1024 * 1024).await;
            let (_temp, store, fs) = server.stored_view().await;
            let (name, body) = if large {
                let large = server.fixture.large.as_ref().unwrap();
                (
                    "range000",
                    (0..large.map.chunk_count)
                        .flat_map(|index| large.bytes(index))
                        .collect::<Vec<_>>(),
                )
            } else {
                ("file000", server.fixture.bodies["file000"].clone())
            };
            let target = store.content_dir().join("real-target");
            std::fs::write(&target, &body).unwrap();
            std::os::unix::fs::symlink(&target, store.content_dir().join(hex::encode(hash(&body))))
                .unwrap();
            let file = inode(&fs, name).await;
            let baseline = server.reader.content_usage();
            for _ in 0..2 {
                assert_eq!(
                    i32::from(
                        fs.read(Request::default(), file, file, 0, 13)
                            .await
                            .unwrap_err()
                    ),
                    -libc::EIO
                );
                idle(&server.reader).await;
                assert_eq!(server.reader.content_usage(), baseline);
                assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
                assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 0);
                assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 0);
            }
        }
    }
}

#[tokio::test]
async fn no_pages_exhausted_reply_quota_rejects_before_wire_and_cas_but_valid_eof_is_empty() {
    let _serial = TEST_LOCK.lock().await;
    for stored in [false, true] {
        let mut fixture = Fixture::new(false, true).with_large();
        fixture.metadata_pages = false;
        let limit = 8 * 1024 * 1024;
        let server = Server::start(fixture, limit).await;
        let (_temp, store, fs) = if stored {
            let (temp, store, fs) = server.stored_view().await;
            (Some(temp), Some(store), fs)
        } else {
            (None, None, server.view(false).await)
        };
        let small = inode(&fs, "file000").await;
        let range = inode(&fs, "range000").await;
        if let Some(store) = &store {
            std::fs::write(
                store
                    .content_dir()
                    .join(hex::encode(hash(&server.fixture.bodies["file000"]))),
                &server.fixture.bodies["file000"],
            )
            .unwrap();
        }
        let baseline = server.reader.content_usage().output_bytes;
        let pressure = server
            .reader
            .content_scope
            .reserve(
                crate::snapshot::content::BudgetClass::Output,
                limit - baseline,
            )
            .unwrap();
        for file in [small, range] {
            assert!(fs.read(Request::default(), file, file, 0, 1).await.is_err());
            assert!(fs
                .read(Request::default(), file, file, 0, 0)
                .await
                .unwrap()
                .data
                .is_empty());
            assert!(fs
                .read(Request::default(), file, file, u64::MAX, u32::MAX)
                .await
                .unwrap()
                .data
                .is_empty());
        }
        assert_eq!(server.reader.content_usage().output_bytes, limit);
        assert_eq!(server.reader.content_usage().construction_bytes, 0);
        assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
        assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 0);
        drop(pressure);
        assert_eq!(read(&fs, "file000", 0, 1).await.data.as_ref(), [0]);
        assert_eq!(
            read(&fs, "range000", 2 * CHUNK_SIZE as u64, 7)
                .await
                .data
                .as_ref(),
            [2; 7]
        );
        assert_eq!(
            server.fixture.requests.load(Ordering::SeqCst),
            usize::from(!stored)
        );
        drop(fs);
        idle(&server.reader).await;
        assert_eq!(server.reader.content_usage().output_bytes, 0);
    }
}

#[tokio::test]
async fn no_pages_malformed_object_map_chunk_end_and_nul_symlink_never_publish() {
    let _serial = TEST_LOCK.lock().await;
    let mut fixture = Fixture::new(false, true);
    fixture.metadata_pages = false;
    fixture
        .bodies
        .insert("link".into(), b"bad\0target".to_vec());
    let server = Server::start(fixture.with_large(), 8 * 1024 * 1024).await;
    let fs = server.view(false).await;
    let small = inode(&fs, "file000").await;
    let range = inode(&fs, "range000").await;
    let link = inode(&fs, "link").await;
    let baseline = server.reader.content_usage();
    for mode in [1, 2] {
        server.fixture.mode.store(mode, Ordering::SeqCst);
        let before = server.fixture.requests.load(Ordering::SeqCst);
        assert_eq!(
            i32::from(
                fs.read(Request::default(), small, small, 0, 1)
                    .await
                    .unwrap_err()
            ),
            -libc::EIO
        );
        assert_eq!(server.fixture.requests.load(Ordering::SeqCst), before + 1);
        idle(&server.reader).await;
        assert_eq!(server.reader.content_usage(), baseline);
        assert_eq!(
            fs.state
                .lock()
                .unwrap()
                .online
                .as_ref()
                .unwrap()
                .contents
                .len(),
            0
        );
    }
    for (mode, expected_chunks) in [(5, 0), (1, 1), (2, 1), (6, 1), (7, 2)] {
        server.fixture.mode.store(mode, Ordering::SeqCst);
        let maps = server.fixture.map_requests.load(Ordering::SeqCst);
        let chunks = server.fixture.chunk_requests.load(Ordering::SeqCst);
        assert_eq!(
            i32::from(
                fs.read(Request::default(), range, range, CHUNK_SIZE as u64 - 2, 5)
                    .await
                    .unwrap_err()
            ),
            -libc::EIO
        );
        assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), maps + 1);
        assert_eq!(
            server.fixture.chunk_requests.load(Ordering::SeqCst),
            chunks + expected_chunks
        );
        idle(&server.reader).await;
        assert_eq!(server.reader.content_usage(), baseline);
        assert_eq!(
            fs.state
                .lock()
                .unwrap()
                .online
                .as_ref()
                .unwrap()
                .ranges
                .len(),
            0
        );
    }
    server.fixture.mode.store(0, Ordering::SeqCst);
    for _ in 0..2 {
        assert_eq!(
            i32::from(fs.readlink(Request::default(), link).await.unwrap_err()),
            -libc::EIO
        );
        idle(&server.reader).await;
        assert_eq!(server.reader.content_usage(), baseline);
        assert_eq!(
            fs.state
                .lock()
                .unwrap()
                .online
                .as_ref()
                .unwrap()
                .contents
                .len(),
            0
        );
    }
    assert_eq!(read(&fs, "file000", 0, 1).await.data.as_ref(), [0]);
    assert_eq!(
        read(&fs, "range000", CHUNK_SIZE as u64 - 2, 5)
            .await
            .data
            .as_ref(),
        [0, 0, 1, 1, 1]
    );
}

#[tokio::test]
async fn no_pages_actual_fuse_cancellation_keeps_other_small_and_range_replies_paid() {
    let _serial = TEST_LOCK.lock().await;
    for large in [false, true] {
        let mut fixture = Fixture::new(false, true).with_large();
        fixture.metadata_pages = false;
        let server = Server::start(fixture, 8 * 1024 * 1024).await;
        let fs = Arc::new(server.view(false).await);
        let retained = read(
            &fs,
            if large { "range000" } else { "file000" },
            if large { 2 * CHUNK_SIZE as u64 } else { 0 },
            7,
        )
        .await;
        let baseline = server.reader.content_usage().output_bytes;
        for (mode, name) in if large {
            [(4, "range001"), (3, "range002")]
        } else {
            [(4, "file001"), (3, "file002")]
        } {
            server.fixture.mode.store(mode, Ordering::SeqCst);
            let file = inode(&fs, name).await;
            let count = if large {
                &server.fixture.chunk_requests
            } else {
                &server.fixture.requests
            };
            let before = count.load(Ordering::SeqCst);
            let emitted = server.fixture.emitted.load(Ordering::SeqCst);
            let task = tokio::spawn({
                let fs = fs.clone();
                async move { fs.read(Request::default(), file, file, 0, 4).await }
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                while count.load(Ordering::SeqCst) == before
                    || mode == 3 && server.fixture.emitted.load(Ordering::SeqCst) == emitted
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(!task.is_finished());
            assert!(server.reader.content_usage().output_bytes > baseline);
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            if mode == 4 {
                server.fixture.release.add_permits(1);
            }
            idle(&server.reader).await;
            assert_eq!(server.reader.content_usage().output_bytes, baseline);
            assert_eq!(
                retained.data.as_ref(),
                if large { &[2; 7] } else { &[0; 7] }
            );
            let state = fs.state.lock().unwrap();
            let cache = state.online.as_ref().unwrap();
            assert_eq!(
                if large {
                    cache.ranges.len()
                } else {
                    cache.contents.len()
                },
                1
            );
        }
        drop(retained);
        drop(fs);
        idle(&server.reader).await;
        assert_eq!(server.reader.content_usage().output_bytes, 0);
    }
}

#[tokio::test]
async fn no_pages_cached_content_range_readlink_and_empty_replies_observe_real_403_and_410() {
    let _serial = TEST_LOCK.lock().await;
    for status in [403, 410] {
        let mut fixture = Fixture::new(false, true).with_large();
        fixture.metadata_pages = false;
        fixture.renewal_status = status;
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
        let small = inode(&fs, "file000").await;
        let range = inode(&fs, "range000").await;
        let link = inode(&fs, "link").await;
        let prior = read(&fs, "file000", 0, 4).await;
        let prior_range = read(&fs, "range000", 2 * CHUNK_SIZE as u64, 7).await;
        drop(fs.readlink(Request::default(), link).await.unwrap());
        tokio::time::timeout(Duration::from_secs(6), async {
            while server.fixture.renewals.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let expected_code = if status == 403 {
            SnapshotErrorCode::ScopeForbidden
        } else {
            SnapshotErrorCode::SnapshotGone
        };
        assert_eq!(
            server.reader.ensure_lease().await.unwrap_err().code,
            expected_code
        );
        let before = (
            server.fixture.requests.load(Ordering::SeqCst),
            server.fixture.map_requests.load(Ordering::SeqCst),
            server.fixture.chunk_requests.load(Ordering::SeqCst),
        );
        let errno = if status == 403 {
            libc::EACCES
        } else {
            libc::ESTALE
        };
        for file in [small, range] {
            for (offset, size) in [(0, 1), (0, 0), (u64::MAX, u32::MAX)] {
                assert_eq!(
                    i32::from(
                        fs.read(Request::default(), file, file, offset, size)
                            .await
                            .unwrap_err()
                    ),
                    -errno
                );
            }
            assert_eq!(
                i32::from(
                    fs.getattr(Request::default(), file, None, 0)
                        .await
                        .unwrap_err()
                ),
                -errno
            );
        }
        assert_eq!(
            i32::from(fs.readlink(Request::default(), link).await.unwrap_err()),
            -errno
        );
        assert_eq!(
            (
                server.fixture.requests.load(Ordering::SeqCst),
                server.fixture.map_requests.load(Ordering::SeqCst),
                server.fixture.chunk_requests.load(Ordering::SeqCst)
            ),
            before
        );
        assert_eq!(prior.data.as_ref(), [0; 4]);
        assert_eq!(prior_range.data.as_ref(), [2; 7]);
        drop(prior);
        drop(prior_range);
        drop(fs);
        idle(&server.reader).await;
        assert_eq!(server.reader.content_usage().output_bytes, 0);
    }
}

#[tokio::test]
async fn actual_symlink_nul_is_terminal_in_every_online_owner_profile() {
    let _serial = TEST_LOCK.lock().await;
    for metadata_pages in [false, true] {
        for stored in [false, true] {
            let mut fixture = Fixture::new(false, true);
            fixture.metadata_pages = metadata_pages;
            fixture
                .bodies
                .insert("link".into(), b"bad\0target".to_vec());
            let server = Server::start(fixture.with_large(), 8 * 1024 * 1024).await;
            let (_temp, _store, fs) = if stored {
                let (temp, store, fs) = server.stored_view().await;
                let body = &server.fixture.bodies["link"];
                std::fs::write(store.content_dir().join(hex::encode(hash(body))), body).unwrap();
                (Some(temp), Some(store), fs)
            } else {
                (None, None, server.view(false).await)
            };
            let link = inode(&fs, "link").await;
            let baseline = server.reader.content_usage();
            for _ in 0..2 {
                assert_eq!(
                    i32::from(fs.readlink(Request::default(), link).await.unwrap_err()),
                    -libc::EIO
                );
                idle(&server.reader).await;
                assert_eq!(server.reader.content_usage(), baseline);
            }
            assert_eq!(
                server.fixture.requests.load(Ordering::SeqCst),
                if stored { 0 } else { 2 }
            );
            drop(fs);
            assert_eq!(server.reader.content_usage().output_bytes, 0);
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
    assert!(fs.state.lock().unwrap().online.is_none());
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
        assert!(fs.state.lock().unwrap().online.is_none());
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
        assert!(state.online.is_none());
        assert!(state.store_small.is_none());
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
async fn actual_metadata_not_ready_returns_eagain_without_publishing_or_latching_a_range() {
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::new(false, true).with_large();
    fixture.map_not_ready.store(1, Ordering::SeqCst);
    let server = Server::start(fixture, 8 * 1024 * 1024).await;
    let fs = server.view(false).await;
    let file = inode(&fs, "range000").await;
    let baseline = server.reader.content_usage();
    assert_eq!(
        i32::from(
            fs.read(Request::default(), file, file, CHUNK_SIZE as u64 - 2, 5)
                .await
                .unwrap_err()
        ),
        -libc::EAGAIN
    );
    idle(&server.reader).await;
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 1);
    assert_eq!(server.fixture.leaf_requests.load(Ordering::SeqCst), 0);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 0);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
    assert_eq!(server.reader.content_usage(), baseline);
    assert_eq!(
        fs.state
            .lock()
            .unwrap()
            .owned
            .as_ref()
            .unwrap()
            .ranges
            .len(),
        0
    );

    server.fixture.map_not_ready.store(0, Ordering::SeqCst);
    let reply = fs
        .read(Request::default(), file, file, CHUNK_SIZE as u64 - 2, 5)
        .await
        .unwrap();
    assert_eq!(reply.data.as_ref(), [0, 0, 1, 1, 1]);
    assert_eq!(server.fixture.map_requests.load(Ordering::SeqCst), 2);
    assert_eq!(server.fixture.leaf_requests.load(Ordering::SeqCst), 1);
    assert_eq!(server.fixture.chunk_requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        server.reader.snapshot_id(),
        id(&server.fixture.descriptor.snapshot_id().unwrap())
    );
    drop(reply);
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
        assert!(state.online.is_none());
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
