//! Canonical discovery must drive the real fixed reader and both transports.
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
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
use scorpiofs::snapshot::{
    capabilities::CapabilityAdvertisement,
    durable::digest_of,
    frames::{parse_digest, MetadataPageItem},
    CompletionKind, DurableStore, LocalPinState, Mst2Client, OwnedChunkedFile, ResolveDelivery,
    ResolveRequest, ResolveTarget, ScopeCache, SnapshotErrorCode, SnapshotReader,
};
use serde_json::{json, Value};

const INSTANCE: &str = "11111111-2222-4333-8444-555555555555";

fn id(bytes: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(bytes))
}
fn hash(bytes: &[u8]) -> [u8; 32] {
    parse_digest(&digest_of(bytes)).unwrap()
}
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for part in bytes.chunks(3) {
        let bits = ((part[0] as u32) << 16)
            | ((part.get(1).copied().unwrap_or(0) as u32) << 8)
            | part.get(2).copied().unwrap_or(0) as u32;
        out.push(ALPHABET[(bits >> 18) as usize] as char);
        out.push(ALPHABET[((bits >> 12) & 63) as usize] as char);
        out.push(if part.len() > 1 {
            ALPHABET[((bits >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if part.len() > 2 {
            ALPHABET[(bits & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

struct Fixture {
    caps: Mutex<Value>,
    resolve_override: Mutex<Option<Value>>,
    legacy_flat_map: Mutex<bool>,
    map_not_ready: AtomicUsize,
    calls: Mutex<Vec<(String, Value)>>,
    descriptor: ServingDescriptor,
    pages: BTreeMap<String, Vec<u8>>,
    bodies: BTreeMap<String, Vec<u8>>,
    leaf: ChunkLeaf,
    map: ChunkMap,
}
impl Fixture {
    fn new() -> Self {
        let mut caps: Value =
            serde_json::from_str(include_str!("fixtures/mst2_capabilities_0_2_1.json")).unwrap();
        caps["limits"]["max_request_items"] = json!(2);
        caps["limits"]["max_metadata_items"] = json!(2);
        caps["limits"]["max_directory_entries"] = json!(1);
        caps["limits"]["max_json_request_bytes"] = json!(350);
        caps["limits"]["max_json_response_bytes"] = json!(4096);
        caps["limits"]["small_batch_bytes"] = json!(524288);
        caps["limits"]["chunk_batch_bytes"] = json!(1048576);
        let mut bodies = BTreeMap::new();
        let mut pages = BTreeMap::new();
        let mut entries = Vec::new();
        for name in ["a", "b", "c"] {
            let body = format!("content-{name}").into_bytes();
            entries.push(Entry::file(
                EntryKind::Regular,
                name.as_bytes(),
                body.len() as u64,
                hash(&body),
            ));
            bodies.insert(format!("/{name}"), body.clone());
            let child = Page::Leaf {
                entries: vec![Entry::file(
                    EntryKind::Regular,
                    b"file",
                    body.len() as u64,
                    hash(&body),
                )],
            }
            .encode()
            .unwrap();
            pages.insert(format!("/d{name}"), child);
            bodies.insert(format!("/d{name}/file"), body);
        }
        for name in ["a", "b", "c"] {
            entries.push(Entry::dir(
                format!("d{name}").as_bytes(),
                page_id(&pages[&format!("/d{name}")]),
            ));
        }
        let large = vec![0x51; 2 * CHUNK_SIZE as usize + 7];
        let leaf = ChunkLeaf {
            page_index: 0,
            chunk_sha256: large.chunks(CHUNK_SIZE as usize).map(hash).collect(),
        };
        let map =
            ChunkMap::new(hash(&large), large.len() as u64, leaf.leaf_hash().unwrap()).unwrap();
        entries.push(Entry::file(
            EntryKind::Regular,
            b"large",
            large.len() as u64,
            hash(&large),
        ));
        bodies.insert("/large".into(), large);
        let root = Page::Leaf { entries }.encode().unwrap();
        let descriptor = ServingDescriptor {
            instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
            namespace_view_id: [0x22; 32],
            scope: "/project".into(),
            metadata_root: page_id(&root),
        };
        pages.insert("/".into(), root);
        Self {
            caps: Mutex::new(caps),
            resolve_override: Mutex::new(None),
            legacy_flat_map: Mutex::new(false),
            map_not_ready: AtomicUsize::new(0),
            calls: Mutex::new(vec![]),
            descriptor,
            pages,
            bodies,
            leaf,
            map,
        }
    }
    fn sid(&self) -> String {
        id(&self.descriptor.snapshot_id().unwrap())
    }
    fn record(&self, endpoint: &str, value: Value) {
        self.calls.lock().unwrap().push((endpoint.into(), value));
    }
    fn descriptor_json(&self) -> Value {
        json!({"schema_version":2,"metadata_codec":1,"instance_id":INSTANCE,"namespace_view_id":id(&self.descriptor.namespace_view_id),"scope":self.descriptor.scope,"materialization_policy":1,"fs_semantics":1,"access_projection":0,"metadata_root":id(&self.descriptor.metadata_root),"snapshot_id":self.sid()})
    }
    fn envelope(&self, delivery: &str) -> Value {
        json!({"descriptor":self.descriptor_json(),"publication_sequence":"7","writer_epoch":"3","lease_id":"canonical-lease","lease_expires_at":"2099-01-01T00:00:00Z","authorization_epoch":"11","resolved_at":"2026-10-05T00:00:00Z","delivery":delivery})
    }
    fn frame_response(
        &self,
        body: &[u8],
        mut wire: Vec<u8>,
        count: usize,
        units: usize,
        logical: usize,
    ) -> Response {
        wire.extend(
            EndPayload {
                request_item_count: count as u32,
                unique_unit_count: units as u32,
                logical_bytes: logical as u64,
                request_body_sha256: hash(body),
            }
            .encode(7, units as u64),
        );
        Response::builder()
            .header("content-type", "application/vnd.mega.treeframe;version=2")
            .header("x-mega-snapshot-id", self.sid())
            .header("x-mega-request-digest", digest_of(body))
            .body(Body::from(wire))
            .unwrap()
    }
}

async fn capabilities(State(f): State<Arc<Fixture>>) -> Json<Value> {
    f.record("capabilities", Value::Null);
    Json(f.caps.lock().unwrap().clone())
}
async fn resolve(
    State(f): State<Arc<Fixture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    f.record("resolve", body.clone());
    assert_eq!(body["supported_metadata_codecs"], json!([1]));
    let mut response = Json(
        f.resolve_override
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| f.envelope(body["delivery"].as_str().unwrap())),
    )
    .into_response();
    if let Some(id) = headers.get("x-request-id") {
        response.headers_mut().insert("x-request-id", id.clone());
    }
    response
}

async fn lease_header(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    if !matches!(
        request.uri().path(),
        "/api/v2/snapshots/capabilities" | "/api/v2/snapshots/resolve"
    ) {
        assert_eq!(
            request.headers()["x-mega-snapshot-lease"],
            "canonical-lease"
        );
    }
    next.run(request).await
}
async fn metadata(s: State<Arc<Fixture>>, p: Path<String>, body: Bytes) -> Response {
    frames(s, Path((p.0, "metadata/pages".into())), body).await
}
async fn frames(
    State(f): State<Arc<Fixture>>,
    Path((sid, endpoint)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    assert_eq!(sid, f.sid());
    let caps = f.caps.lock().unwrap().clone();
    assert!(body.len() <= caps["limits"]["max_json_request_bytes"].as_u64().unwrap() as usize);
    let request: Value = serde_json::from_slice(&body).unwrap();
    assert!(request.get("encoding").is_none());
    let items = request["items"].as_array().unwrap();
    let item_cap = if endpoint == "metadata/pages" {
        "max_metadata_items"
    } else {
        "max_request_items"
    };
    assert!(items.len() <= caps["limits"][item_cap].as_u64().unwrap() as usize);
    f.record(&endpoint, request.clone());
    let mut wire = Vec::new();
    let mut logical = 0;
    for (sequence, item) in items.iter().enumerate() {
        match endpoint.as_str() {
            "metadata/pages" => {
                assert_eq!(item["route"], json!([]));
                let data = &f.pages[item["directory_path"].as_str().unwrap()];
                assert_eq!(item["expected_digest"], id(&page_id(data)));
                logical += data.len();
                wire.extend(
                    MetaPayload {
                        pages: vec![(page_id(data), data.clone())],
                    }
                    .encode(7, sequence as u64)
                    .unwrap(),
                );
            }
            "objects" => {
                let path = format!(
                    "/{}",
                    item["path"].as_str().unwrap().trim_start_matches('/')
                );
                let data = &f.bodies[&path];
                assert_eq!(item["expected_digest"], digest_of(data));
                logical += data.len();
                wire.extend(
                    ObjectPayload {
                        objects: vec![(hash(data), data.clone())],
                    }
                    .encode(7, sequence as u64)
                    .unwrap(),
                );
            }
            "chunks" => {
                assert_eq!(
                    item["path"].as_str().unwrap().trim_start_matches('/'),
                    "large"
                );
                assert_eq!(item["expected_digest"], id(&f.map.file_content_id));
                assert_eq!(item["map_id"], id(&f.map.map_id()));
                let index: usize = item["chunk_index"].as_str().unwrap().parse().unwrap();
                let data = f.bodies["/large"]
                    .chunks(CHUNK_SIZE as usize)
                    .nth(index)
                    .unwrap();
                logical += data.len();
                wire.extend(
                    ChunkPayload {
                        map_id: f.map.map_id(),
                        file_content_id: f.map.file_content_id,
                        chunk_index: index as u64,
                        chunk_bytes: data.to_vec(),
                    }
                    .encode(7, sequence as u64)
                    .unwrap(),
                );
            }
            _ => panic!("unexpected endpoint"),
        }
    }
    if endpoint == "objects" || endpoint == "chunks" {
        let key = if endpoint == "objects" {
            "small_batch_bytes"
        } else {
            "chunk_batch_bytes"
        };
        assert!(logical <= caps["limits"][key].as_u64().unwrap() as usize);
    }
    f.frame_response(&body, wire, items.len(), items.len(), logical)
}
async fn map(
    State(f): State<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    assert_eq!(query["path"].trim_start_matches('/'), "large");
    assert_eq!(query["expected_digest"], id(&f.map.file_content_id));
    f.record("chunk-map", json!(query));
    if f.map_not_ready
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
            remaining.checked_sub(1)
        })
        .is_ok()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":{"code":"METADATA_NOT_READY","message":"fixed map is being prepared","request_id":"canonical-map","retryable":true}})),
        )
            .into_response();
    }
    let descriptor = json!({"schema_version":2,"file_content_id":id(&f.map.file_content_id),"file_size":f.map.file_size.to_string(),"chunk_size":CHUNK_SIZE,"chunk_count":f.map.chunk_count.to_string(),"page_count":"1","pages_root":id(&f.map.pages_root),"map_id":id(&f.map.map_id())});
    let mut body = json!({"snapshot_id":f.sid(),"path":query["path"],"map":descriptor});
    if *f.legacy_flat_map.lock().unwrap() {
        let descriptor = body.as_object_mut().unwrap().remove("map").unwrap();
        body.as_object_mut()
            .unwrap()
            .extend(descriptor.as_object().unwrap().clone());
    }
    Json(body).into_response()
}
async fn leaf(
    State(f): State<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(query.len(), 3);
    assert_eq!(query["path"].trim_start_matches('/'), "large");
    assert_eq!(query["map_id"], id(&f.map.map_id()));
    assert_eq!(query["page_index"], "0");
    f.record("chunk-map/pages", json!(query));
    Json(
        json!({"map_id":id(&f.map.map_id()),"page_index":"0","leaf_base64":base64(&f.leaf.encode().unwrap()),"proof":[]}),
    )
}
async fn directory(
    State(f): State<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Json<Value> {
    assert_eq!(query["limit"], "1");
    assert_eq!(query["path"], "/");
    f.record("directory", json!(query));
    let entries = ["a", "b", "c", "da", "db", "dc", "large"];
    let index = query
        .get("cursor")
        .map_or(0, |s| s.parse::<usize>().unwrap());
    let name = entries[index];
    let entry = if let Some(body) = f.bodies.get(&format!("/{name}")) {
        json!({"name":name,"fs_kind":"regular","size":body.len().to_string(),"content_digest":digest_of(body)})
    } else {
        json!({"name":name,"fs_kind":"directory","directory_root":id(&page_id(&f.pages[&format!("/{name}")]))})
    };
    Json(
        json!({"snapshot_id":f.sid(),"path":"/","metadata_root":id(&f.descriptor.metadata_root),"directory_root":id(&f.descriptor.metadata_root),"node_class":"native_tree","lifecycle":"mutable","range_start_exclusive":index.checked_sub(1).map(|i|entries[i]),"entries":[entry],"entry_count":"7","next_cursor":if index+1<entries.len(){Some((index+1).to_string())}else{None},"proof_pages":[{"digest":id(&f.descriptor.metadata_root),"data_base64":base64(&f.pages["/"])}]}),
    )
}
async fn blob(
    State(f): State<Arc<Fixture>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Vec<u8> {
    f.record("blob", json!(query));
    f.bodies[&format!("/{}", query["path"].trim_start_matches('/'))].clone()
}

struct Server {
    f: Arc<Fixture>,
    client: Mst2Client,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(f: Fixture) -> Self {
        let f = Arc::new(f);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(capabilities))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
            .route("/api/v2/snapshots/{sid}/{endpoint}", post(frames))
            .route("/api/v2/snapshots/{sid}/chunk-map", get(map))
            .route("/api/v2/snapshots/{sid}/chunk-map/pages", get(leaf))
            .route("/api/v2/snapshots/{sid}/directory", get(directory))
            .route("/api/v2/snapshots/{sid}/blob", get(blob))
            .layer(axum::middleware::from_fn(lease_header))
            .with_state(f.clone());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { f, client, task }
    }
    async fn reader(&self) -> SnapshotReader {
        SnapshotReader::resolve(self.client.clone(), "/project", 60)
            .await
            .unwrap()
    }
    fn calls(&self, endpoint: &str) -> Vec<Value> {
        self.f
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(e, _)| e == endpoint)
            .map(|(_, v)| v.clone())
            .collect()
    }
}

#[tokio::test]
async fn canonical_readers_share_verified_cas_with_independently_releasable_workspace_pins() {
    let s = Server::start(Fixture::new()).await;
    let first_reader = s.reader().await;
    let second_reader = s.reader().await;
    assert_eq!(first_reader.snapshot_id(), second_reader.snapshot_id());
    let temp = tempfile::tempdir().unwrap();
    let first = DurableStore::open_for_workspace(
        temp.path(),
        &uuid::Uuid::new_v4().to_string(),
        &first_reader,
    )
    .unwrap();
    let second = DurableStore::open_for_workspace(
        temp.path(),
        &uuid::Uuid::new_v4().to_string(),
        &second_reader,
    )
    .unwrap();
    assert_ne!(first.root(), second.root());
    assert_eq!(first.content_dir(), second.content_dir());
    first.hydrate_snapshot(&first_reader).await.unwrap();
    let resumed = second.hydrate_snapshot(&second_reader).await.unwrap();
    assert_eq!(resumed.fetched, 0);
    assert_eq!(first.manifest().unwrap(), second.manifest().unwrap());
    assert_eq!(
        first.local_pin_state().unwrap(),
        LocalPinState::Complete(CompletionKind::FullSnapshot)
    );
    let cache = ScopeCache::open(
        first_reader
            .authorized_context()
            .scope_cache_dir(temp.path()),
    )
    .unwrap();
    let first_release = first.release_local_pin().unwrap();
    assert!(!first.is_complete().unwrap());
    assert_eq!(first.release_local_pin().unwrap(), first_release);
    assert_eq!(
        cache.try_live_pins().unwrap(),
        vec![second_reader.snapshot_id()]
    );
    assert!(second.is_snapshot_complete().unwrap());
    let file = second.manifest().unwrap().remove(0);
    let bytes = second.read_blob(&file.content_digest, file.size).unwrap();
    assert_eq!(digest_of(&bytes), file.content_digest);
    second.release_local_pin().unwrap();
    assert!(cache.try_live_pins().unwrap().is_empty());
}

#[tokio::test]
async fn canonical_discovery_drives_real_closure_and_owned_object_batches() {
    let s = Server::start(Fixture::new()).await;
    let reader = s.reader().await;
    assert!(matches!(
        reader.capability_advertisement(),
        CapabilityAdvertisement::Canonical(_)
    ));
    assert!(reader.capabilities().features.objects);
    assert_eq!(reader.encoding_hint(), None);
    assert_eq!(reader.authorized_context().authorization_epoch(), 11);
    let closure = reader.snapshot_closure().await.unwrap();
    assert_eq!(closure.files().len(), 7);
    assert_eq!(
        s.calls("metadata/pages")
            .iter()
            .map(|v| v["items"].as_array().unwrap().len())
            .collect::<Vec<_>>(),
        [1, 2, 1]
    );
    reader.seed_content_membership(&closure).unwrap();
    let files: Vec<_> = closure
        .files()
        .iter()
        .filter(|f| matches!(f.rel_path.as_str(), "a" | "b" | "c"))
        .cloned()
        .collect();
    let batch = reader.read_content_batch(&files).await.unwrap();
    assert_eq!(batch.len(), 3);
    assert_eq!(
        s.calls("objects")
            .iter()
            .map(|v| v["items"].as_array().unwrap().len())
            .collect::<Vec<_>>(),
        [2, 1]
    );
    assert_eq!(s.calls("resolve").len(), 1);
    assert_eq!(reader.snapshot_id(), s.f.sid());
}

#[tokio::test]
async fn canonical_ranges_use_page_index_and_split_actual_chunks_by_advertised_bytes() {
    let s = Server::start(Fixture::new()).await;
    let reader = s.reader().await;
    let closure = reader.snapshot_closure().await.unwrap();
    reader.seed_content_membership(&closure).unwrap();
    let file = closure
        .files()
        .iter()
        .find(|f| f.rel_path == "large")
        .unwrap();
    let bytes = reader.read_content(file, true).await.unwrap();
    assert_eq!(bytes.as_bytes(), s.f.bodies["/large"]);
    let range = OwnedChunkedFile::open(&reader, "/large", &file.content_digest, file.size)
        .await
        .unwrap();
    assert_eq!(
        range
            .read_range_owned(CHUNK_SIZE as u64 - 2, 6)
            .await
            .unwrap()
            .as_bytes(),
        &[0x51; 6]
    );
    assert_eq!(
        s.calls("chunks")
            .iter()
            .map(|v| v["items"].as_array().unwrap().len())
            .collect::<Vec<_>>(),
        [1, 1, 1, 1, 1]
    );
    assert_eq!(s.calls("chunk-map/pages").len(), 2);
    assert_eq!(s.calls("resolve").len(), 1);
}

#[tokio::test]
async fn canonical_owned_range_recovers_metadata_not_ready_without_resolving_another_view() {
    let fixture = Fixture::new();
    fixture.map_not_ready.store(1, Ordering::SeqCst);
    let s = Server::start(fixture).await;
    let reader = s.reader().await;
    let closure = reader.snapshot_closure().await.unwrap();
    reader.seed_content_membership(&closure).unwrap();
    let file = closure
        .files()
        .iter()
        .find(|file| file.rel_path == "large")
        .unwrap();
    let baseline = reader.content_usage();
    let range = OwnedChunkedFile::open(&reader, "/large", &file.content_digest, file.size)
        .await
        .unwrap();
    let bytes = range
        .read_range_owned(CHUNK_SIZE as u64 - 2, 6)
        .await
        .unwrap();
    assert_eq!(bytes.as_bytes(), [0x51; 6]);
    assert_eq!(s.calls("chunk-map").len(), 2);
    assert_eq!(s.client.retry_count(), 1);
    assert_eq!(s.calls("chunk-map/pages").len(), 1);
    assert_eq!(s.calls("chunks").len(), 2);
    assert_eq!(s.calls("resolve").len(), 1);
    assert_eq!(reader.snapshot_id(), s.f.sid());
    assert_eq!(reader.lease_id(), "canonical-lease");
    drop(bytes);
    drop(range);
    assert_eq!(reader.content_usage(), baseline);
}

#[tokio::test]
async fn canonical_owned_range_metadata_not_ready_stops_at_the_attempt_budget_and_can_recover() {
    let fixture = Fixture::new();
    fixture.map_not_ready.store(usize::MAX, Ordering::SeqCst);
    let mut s = Server::start(fixture).await;
    s.client = s
        .client
        .clone()
        .with_request_timeout(Duration::from_secs(3));
    let reader = s.reader().await;
    let closure = reader.snapshot_closure().await.unwrap();
    reader.seed_content_membership(&closure).unwrap();
    let file = closure
        .files()
        .iter()
        .find(|file| file.rel_path == "large")
        .unwrap();
    let baseline = reader.content_usage();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        OwnedChunkedFile::open(&reader, "/large", &file.content_digest, file.size),
    )
    .await
    .unwrap()
    .err()
    .unwrap();
    assert_eq!(error.code, SnapshotErrorCode::MetadataNotReady);
    assert_eq!(error.http_status, 503);
    assert_eq!(s.calls("chunk-map").len(), 4);
    assert_eq!(s.client.retry_count(), 3);
    assert!(s.calls("chunk-map/pages").is_empty());
    assert!(s.calls("chunks").is_empty());
    assert_eq!(reader.content_usage(), baseline);

    s.f.map_not_ready.store(0, Ordering::SeqCst);
    let range = OwnedChunkedFile::open(&reader, "/large", &file.content_digest, file.size)
        .await
        .unwrap();
    let bytes = range
        .read_range_owned(2 * CHUNK_SIZE as u64, 7)
        .await
        .unwrap();
    assert_eq!(bytes.as_bytes(), [0x51; 7]);
    assert_eq!(s.calls("chunk-map").len(), 5);
    assert_eq!(s.client.retry_count(), 3);
    assert_eq!(s.calls("chunk-map/pages").len(), 1);
    assert_eq!(s.calls("chunks").len(), 1);
    assert_eq!(s.calls("resolve").len(), 1);
    assert_eq!(reader.snapshot_id(), s.f.sid());
    assert_eq!(reader.lease_id(), "canonical-lease");
    drop(bytes);
    drop(range);
    assert_eq!(reader.content_usage(), baseline);
}

#[tokio::test]
async fn typed_view_and_lazy_delivery_are_sent_and_bound_to_the_fixed_reader() {
    let s = Server::start(Fixture::new()).await;
    let request = ResolveRequest {
        target: ResolveTarget::View {
            view_id: id(&s.f.descriptor.namespace_view_id),
        },
        scope: "/project".into(),
        delivery: ResolveDelivery::Lazy,
        lease_seconds: 60,
    };
    let reader = SnapshotReader::resolve_request(s.client.clone(), &request)
        .await
        .unwrap();
    assert_eq!(reader.delivery(), ResolveDelivery::Lazy);
    assert_eq!(
        s.calls("resolve")[0]["target"],
        json!({"kind":"view","view_id":id(&s.f.descriptor.namespace_view_id)})
    );
    assert_eq!(s.calls("resolve")[0]["delivery"], "lazy");
    let mut wrong = request.clone();
    wrong.target = ResolveTarget::View {
        view_id: id(&[0x33; 32]),
    };
    assert_eq!(
        SnapshotReader::resolve_request(s.client.clone(), &wrong)
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    *s.f.resolve_override.lock().unwrap() = Some(s.f.envelope("full"));
    assert_eq!(
        SnapshotReader::resolve_request(s.client.clone(), &request)
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::IntegrityError
    );
}

#[tokio::test]
async fn canonical_reader_never_accepts_a_legacy_resolve_downgrade() {
    let f = Fixture::new();
    let mut old = f.envelope("full");
    for key in ["writer_epoch", "resolved_at", "delivery"] {
        old.as_object_mut().unwrap().remove(key);
    }
    *f.resolve_override.lock().unwrap() = Some(old);
    let s = Server::start(f).await;
    assert_eq!(
        SnapshotReader::resolve(s.client.clone(), "/project", 60)
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(s.calls("resolve").len(), 1);
}

#[tokio::test]
async fn discovered_features_paths_files_and_lookup_counts_fail_before_body_http() {
    let f = Fixture::new();
    f.caps.lock().unwrap()["features"]["raw_blob"] = json!(false);
    f.caps.lock().unwrap()["features"]["small_objects"] = json!(false);
    f.caps.lock().unwrap()["limits"]["max_path_bytes"] = json!(30);
    let s = Server::start(f).await;
    let reader = s.reader().await;
    assert_eq!(
        reader
            .lookup(&["/a".into(), "/b".into(), "/c".into()])
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(
        reader
            .directory_page(&format!("/{}", "x".repeat(30)), 1)
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    let closure = reader.snapshot_closure().await.unwrap();
    reader.seed_content_membership(&closure).unwrap();
    let file = closure.files().iter().find(|f| f.rel_path == "a").unwrap();
    for use_frames in [false, true] {
        assert_eq!(
            reader
                .read_content(file, use_frames)
                .await
                .err()
                .unwrap()
                .code,
            SnapshotErrorCode::SnapshotNotReady
        );
    }
    assert!(s.calls("objects").is_empty());
    assert!(s.calls("blob").is_empty());
    assert!(s.calls("directory").is_empty());
}

#[tokio::test]
async fn directory_limit_is_clamped_once_and_cursor_enumeration_keeps_that_limit() {
    let s = Server::start(Fixture::new()).await;
    let reader = s.reader().await;
    let page = reader.directory_page("/", 128).await.unwrap();
    assert_eq!(
        page.entries
            .iter()
            .map(|e| e.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c", "da", "db", "dc", "large"]
    );
    assert_eq!(s.calls("directory").len(), 7);
    assert_eq!(s.calls("resolve").len(), 1);
}

#[tokio::test]
async fn invalid_discovery_and_disabled_full_delivery_do_not_reach_resolve() {
    for mode in 0..3 {
        let f = Fixture::new();
        match mode {
            0 => f.caps.lock().unwrap()["limits"]["metadata_page_bytes"] = json!(8192),
            1 => f.caps.lock().unwrap()["features"]["strict_publication"] = json!(false),
            _ => f.caps.lock().unwrap()["features"]["full_hydration"] = json!(false),
        }
        let s = Server::start(f).await;
        assert!(SnapshotReader::resolve(s.client.clone(), "/project", 60)
            .await
            .is_err());
        assert!(s.calls("resolve").is_empty());
    }
}

#[tokio::test]
async fn discovery_is_immutable_per_reader_and_new_json_response_limit_is_enforced() {
    let s = Server::start(Fixture::new()).await;
    let reader = s.reader().await;
    s.f.caps.lock().unwrap()["limits"]["max_json_response_bytes"] = json!(64);
    // The earlier reader keeps 4096; a future resolve is bound to the new 64.
    assert_eq!(
        reader.directory_page("/", 128).await.unwrap().entries.len(),
        7
    );
    assert_eq!(
        SnapshotReader::resolve(s.client.clone(), "/project", 60)
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(reader.snapshot_id(), s.f.sid());
}

#[tokio::test]
async fn committed_file_over_discovered_limit_cannot_seed_or_read_owned_content() {
    let f = Fixture::new();
    f.caps.lock().unwrap()["limits"]["max_file_bytes"] = json!("3");
    let s = Server::start(f).await;
    let reader = s.reader().await;
    assert_eq!(
        reader.snapshot_closure().await.err().unwrap().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert!(s.calls("objects").is_empty());
    assert!(s.calls("chunks").is_empty());
}

#[tokio::test]
async fn canonical_frame_wire_limit_rejects_a_genuine_oversized_metadata_frame() {
    let f = Fixture::new();
    f.caps.lock().unwrap()["limits"]["frame_wire_bytes"] = json!(16);
    let s = Server::start(f).await;
    let reader = s.reader().await;
    let error = reader
        .client()
        .metadata_pages(
            reader.snapshot_id(),
            &[MetadataPageItem {
                directory_path: "/".into(),
                route: vec![],
                expected_digest: Some(id(&s.f.descriptor.metadata_root)),
            }],
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::LimitExceeded);
    assert_eq!(s.calls("metadata/pages").len(), 1);
}

#[tokio::test]
async fn canonical_observed_typed_resolve_receipt_belongs_to_the_actual_response() {
    let s = Server::start(Fixture::new()).await;
    let request = ResolveRequest {
        target: ResolveTarget::View {
            view_id: id(&s.f.descriptor.namespace_view_id),
        },
        scope: "/project".into(),
        delivery: ResolveDelivery::Lazy,
        lease_seconds: 60,
    };
    let (reader, receipt) =
        SnapshotReader::resolve_request_observed(s.client.clone(), &request, "canonical.view")
            .await
            .unwrap();
    assert_eq!(reader.snapshot_id(), s.f.sid());
    assert_eq!(reader.delivery(), ResolveDelivery::Lazy);
    assert_eq!(receipt.attempt_ids(), &["canonical.view:a1"]);
    assert_eq!(receipt.final_attempt_id(), "canonical.view:a1");
    assert_eq!(receipt.retry_count(), 0);
    assert_eq!(s.calls("resolve").len(), 1);
}

#[tokio::test]
async fn discovered_request_byte_limit_rejects_resolve_before_the_actual_post() {
    let f = Fixture::new();
    f.caps.lock().unwrap()["limits"]["max_json_request_bytes"] = json!(100);
    let s = Server::start(f).await;
    assert_eq!(
        SnapshotReader::resolve(s.client.clone(), "/project", 60)
            .await
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert!(s.calls("resolve").is_empty());
}

#[tokio::test]
async fn canonical_reader_rejects_legacy_flat_maps_before_page_or_chunk_requests() {
    let fixture = Fixture::new();
    *fixture.legacy_flat_map.lock().unwrap() = true;
    let s = Server::start(fixture).await;
    let reader = s.reader().await;
    let closure = reader.snapshot_closure().await.unwrap();
    reader.seed_content_membership(&closure).unwrap();
    let file = closure
        .files()
        .iter()
        .find(|f| f.rel_path == "large")
        .unwrap();
    assert_eq!(
        reader.read_content(file, true).await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    assert!(s.calls("chunk-map/pages").is_empty());
    assert!(s.calls("chunks").is_empty());
}
