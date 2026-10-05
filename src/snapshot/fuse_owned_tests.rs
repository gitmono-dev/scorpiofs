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
    descriptor::ServingDescriptor,
    metapage::{page_id, Entry, EntryKind, Page},
    treeframe::{EndPayload, MetaPayload, ObjectPayload},
};
use serde_json::{json, Value};

use super::*;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
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

struct Fixture {
    descriptor: ServingDescriptor,
    page: Vec<u8>,
    bodies: BTreeMap<String, Vec<u8>>,
    metadata: Mutex<Vec<String>>,
    requests: AtomicUsize,
    renewals: AtomicUsize,
    mode: AtomicUsize,
    release: tokio::sync::Semaphore,
    objects: bool,
    expiry: String,
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
            renewals: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            release: tokio::sync::Semaphore::new(0),
            objects,
            expiry: "2099-01-01T00:00:00Z".into(),
        }
    }
    fn response(&self, request: &[u8], wire: Vec<u8>, pending: bool) -> Response {
        use futures::StreamExt;
        let body = if pending {
            Body::from_stream(
                futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from(wire))])
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
    assert_eq!(directory, "/", "unrelated snapshot closure walk");
    assert_eq!(items[0]["route"], json!([]));
    let mut wire = MetaPayload {
        pages: vec![(page_id(&f.page), f.page.clone())],
    }
    .encode(51, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: f.page.len() as u64,
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
    let mut bytes = body.clone();
    if mode == 1 {
        bytes[0] ^= 1;
    }
    let mut wire = ObjectPayload {
        objects: vec![(hash(body), bytes)],
    }
    .encode(52, 0)
    .unwrap();
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
    assert_eq!(last.as_ptr(), unsafe { reply.data.as_ptr().add(1) });
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
