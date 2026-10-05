//! Repeatable client-cost experiment over a versioned loopback HTTP fixture.
//! This fixture does not execute Mega2 Git ingest, publication, or retention.
//! Run with `cargo test --test mst2_commit_update_bench -- --nocapture`.
//! Optional MST2_UPDATE_BENCH_{ROUNDS,DIRS,FILES,BYTES,OUTPUT} controls scale
//! and a private JSONL result path. No result file is written by default.

// The structured diagnostic record exceeds serde_json's default macro depth.
#![recursion_limit = "256"]

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    io::Write,
    path::Path,
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{Body, Bytes},
    extract::{Path as HttpPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use mst2_codec::{
    descriptor::ServingDescriptor,
    metapage::{page_id, Entry, EntryKind, Page},
    treeframe::{EndPayload, MetaPayload, ObjectPayload},
};
use scorpiofs::snapshot::{
    durable::digest_of, frames::parse_digest, DurableStore, HydrateReport, IncrementalSync,
    Mst2Client, ScopeCache, SnapshotErrorCode, SnapshotFile, SnapshotReader, SyncMeters, ViewMeta,
};
use serde::Serialize;
use serde_json::{json, Value};

type Files = BTreeMap<String, Vec<u8>>;
type PageId = [u8; 32];

#[derive(Clone, Copy, Debug)]
enum Scenario {
    SingleFile,
    SameDirectoryBatch,
    MultipleDirectoryBatch,
    RenameAndMove,
    WideDirectorySplit,
    CacheMissing,
}

impl Scenario {
    const ALL: [Self; 6] = [
        Self::SingleFile,
        Self::SameDirectoryBatch,
        Self::MultipleDirectoryBatch,
        Self::RenameAndMove,
        Self::WideDirectorySplit,
        Self::CacheMissing,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::SingleFile => "single_file",
            Self::SameDirectoryBatch => "same_directory_batch",
            Self::MultipleDirectoryBatch => "multiple_directory_batch",
            Self::RenameAndMove => "rename_and_move",
            Self::WideDirectorySplit => "wide_directory_128_to_129_split",
            Self::CacheMissing => "missing_unchanged_page_and_body",
        }
    }

    fn next(self, old: &Files, scale: &Scale) -> Files {
        let mut next = old.clone();
        let mut change = |dir, file| {
            let path = format!("d{dir:03}/f{file:04}.txt");
            next.insert(path.clone(), body(&format!("v2:{path}"), scale.bytes));
        };
        match self {
            Self::SingleFile | Self::CacheMissing => change(0, 0),
            Self::SameDirectoryBatch => {
                for file in 0..16.min(scale.files) {
                    change(0, file);
                }
            }
            Self::MultipleDirectoryBatch => {
                for dir in 0..4.min(scale.dirs) {
                    for file in 0..4.min(scale.files) {
                        change(dir, file);
                    }
                }
            }
            Self::RenameAndMove => {
                for (path, bytes) in old.iter().filter(|(p, _)| p.starts_with("d000/")) {
                    next.remove(path);
                    next.insert(format!("moved/{path}"), bytes.clone());
                }
                let bytes = next.remove("d001/f0000.txt").unwrap();
                next.insert("d002/renamed.txt".into(), bytes);
            }
            Self::WideDirectorySplit => {
                let path = "wide/w0128.txt";
                next.insert(path.into(), body(&format!("v2:{path}"), scale.bytes));
            }
        }
        next
    }
}

#[derive(Serialize)]
struct Scale {
    rounds: usize,
    dirs: usize,
    files: usize,
    bytes: usize,
}

impl Scale {
    fn environment() -> Self {
        fn setting(name: &str, default: usize, min: usize, max: usize) -> usize {
            let value = std::env::var(format!("MST2_UPDATE_BENCH_{name}"))
                .map(|v| v.parse().expect("benchmark setting must be an integer"))
                .unwrap_or(default);
            assert!((min..=max).contains(&value), "invalid {name}: {value}");
            value
        }
        Self {
            rounds: setting("ROUNDS", 5, 5, 20),
            dirs: setting("DIRS", 16, 8, 32),
            files: setting("FILES", 64, 32, 128),
            bytes: setting("BYTES", 256, 128, 4096),
        }
    }

    fn baseline(&self) -> Files {
        let mut files = Files::new();
        for dir in 0..self.dirs {
            for file in 0..self.files {
                let path = format!("d{dir:03}/f{file:04}.txt");
                files.insert(path.clone(), body(&format!("v1:{path}"), self.bytes));
            }
        }
        for file in 0..128 {
            let path = format!("wide/w{file:04}.txt");
            files.insert(path.clone(), body(&format!("v1:{path}"), self.bytes));
        }
        files
    }
}

fn body(seed: &str, len: usize) -> Vec<u8> {
    seed.as_bytes().iter().copied().cycle().take(len).collect()
}

fn id_string(id: &PageId) -> String {
    format!("sha256:{}", hex::encode(id))
}

struct Version {
    files: Files,
    expected: Vec<SnapshotFile>,
    root: PageId,
    pages: HashMap<PageId, Vec<u8>>,
    routes: HashMap<(String, Vec<u8>), PageId>,
}

impl Version {
    fn build(files: Files) -> Self {
        let expected = files
            .iter()
            .map(|(path, bytes)| SnapshotFile {
                rel_path: path.clone(),
                fs_kind: "regular".into(),
                size: bytes.len() as u64,
                content_digest: digest_of(bytes),
            })
            .collect();
        let mut directories: HashMap<String, Vec<Entry>> = HashMap::new();
        directories.insert("/".into(), Vec::new());
        for (path, bytes) in &files {
            let (parent, name) = path.rsplit_once('/').unwrap();
            let mut current = format!("/{parent}");
            loop {
                directories.entry(current.clone()).or_default();
                let (parent, _) = current.rsplit_once('/').unwrap();
                if parent.is_empty() {
                    break;
                }
                current = parent.to_string();
            }
            directories
                .get_mut(&format!("/{parent}"))
                .unwrap()
                .push(Entry::file(
                    EntryKind::Regular,
                    name.as_bytes(),
                    bytes.len() as u64,
                    parse_digest(&digest_of(bytes)).unwrap(),
                ));
        }
        let mut paths: Vec<_> = directories.keys().cloned().collect();
        paths.sort_by_key(|p| std::cmp::Reverse(p.len()));
        let mut version = Self {
            files,
            expected,
            root: [0; 32],
            pages: HashMap::new(),
            routes: HashMap::new(),
        };
        for path in paths {
            let entries = directories.get_mut(&path).unwrap();
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            version.register_routes(&path, entries, Vec::new());
            let root = version.routes[&(path.clone(), Vec::new())];
            if path == "/" {
                version.root = root;
            } else {
                let (parent, name) = path.rsplit_once('/').unwrap();
                let parent = if parent.is_empty() { "/" } else { parent };
                directories
                    .get_mut(parent)
                    .unwrap()
                    .push(Entry::dir(name.as_bytes(), root));
            }
        }
        version
    }

    fn register_routes(&mut self, directory: &str, entries: &[Entry], route: Vec<u8>) {
        let bytes = Page::pages_along_route(entries, &route)
            .unwrap()
            .pop()
            .unwrap();
        let id = page_id(&bytes);
        let (page, _) = Page::decode(&bytes).unwrap();
        self.pages.insert(id, bytes);
        self.routes.insert((directory.into(), route.clone()), id);
        if let Page::Branch { children, .. } = page {
            for child in children {
                let mut next = route.clone();
                next.push(child.label);
                self.register_routes(directory, entries, next);
            }
        }
    }

    fn serving_descriptor(&self) -> ServingDescriptor {
        ServingDescriptor {
            instance_uuid: *uuid::Uuid::from_u128(1).as_bytes(),
            namespace_view_id: [0x22; 32],
            scope: "/project".into(),
            metadata_root: self.root,
        }
    }

    fn snapshot_id(&self) -> String {
        id_string(&self.serving_descriptor().snapshot_id().unwrap())
    }
}

#[derive(Default)]
struct Event {
    kind: &'static str,
    request_bytes: usize,
    response_bytes: usize,
    requested_pages: Vec<PageId>,
    response_pages: Vec<PageId>,
    content: Vec<(String, usize, bool)>,
}

#[derive(Default, Serialize)]
struct Wire {
    requests: usize,
    request_body_bytes: usize,
    response_body_bytes: usize,
    metadata_requests: usize,
    requested_pages: usize,
    response_pages: usize,
    distinct_response_pages: usize,
    metadata_body_bytes: usize,
    content_requests: usize,
    changed_content_units: usize,
    changed_content_bytes: usize,
    unchanged_content_units: usize,
    unchanged_content_bytes: usize,
    requested_page_ids: Vec<String>,
}

struct Fixture {
    versions: [Arc<Version>; 2],
    latest: AtomicUsize,
    leases: Mutex<HashMap<String, String>>,
    events: Mutex<Vec<Event>>,
    old_content: HashSet<String>,
}

impl Fixture {
    fn authorized(&self, sid: &str, headers: &HeaderMap) -> Result<Arc<Version>, Box<Response>> {
        let lease = headers
            .get("x-mega-snapshot-lease")
            .and_then(|v| v.to_str().ok());
        let valid = lease.and_then(|l| self.leases.lock().unwrap().get(l).cloned());
        if valid.as_deref() != Some(sid) {
            let response = self.json_response(
                "authorization_denial",
                0,
                json!({"error": {"code": "LEASE_EXPIRED", "message": "lease/snapshot mismatch"}}),
            );
            return Err(Box::new((StatusCode::GONE, response).into_response()));
        }
        self.versions
            .iter()
            .find(|v| v.snapshot_id() == sid)
            .cloned()
            .ok_or_else(|| Box::new((StatusCode::GONE, "unknown fixed snapshot").into_response()))
    }

    fn mark(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    fn wire(&self, mark: usize) -> Wire {
        let events = self.events.lock().unwrap();
        let mut wire = Wire::default();
        let mut unique = HashSet::new();
        for event in &events[mark..] {
            wire.requests += 1;
            wire.request_body_bytes += event.request_bytes;
            wire.response_body_bytes += event.response_bytes;
            if event.kind == "metadata" {
                wire.metadata_requests += 1;
                wire.requested_pages += event.requested_pages.len();
                wire.response_pages += event.response_pages.len();
                wire.metadata_body_bytes += event.response_bytes;
                wire.requested_page_ids
                    .extend(event.requested_pages.iter().map(id_string));
                unique.extend(event.response_pages.iter().copied());
            }
            if !event.content.is_empty() {
                wire.content_requests += 1;
            }
            for (_, bytes, changed) in &event.content {
                if *changed {
                    wire.changed_content_units += 1;
                    wire.changed_content_bytes += bytes;
                } else {
                    wire.unchanged_content_units += 1;
                    wire.unchanged_content_bytes += bytes;
                }
            }
        }
        wire.distinct_response_pages = unique.len();
        wire
    }

    fn json_response(&self, kind: &'static str, request_bytes: usize, value: Value) -> Response {
        let bytes = serde_json::to_vec(&value).unwrap();
        self.events.lock().unwrap().push(Event {
            kind,
            request_bytes,
            response_bytes: bytes.len(),
            ..Event::default()
        });
        ([("content-type", "application/json")], bytes).into_response()
    }

    fn treeframe_response(
        &self,
        snapshot_id: &str,
        request_body: &[u8],
        body: Vec<u8>,
    ) -> Response {
        Response::builder()
            .header("content-type", "application/vnd.mega.treeframe;version=2")
            .header("x-mega-snapshot-id", snapshot_id)
            .header("x-mega-request-digest", digest_of(request_body))
            .body(Body::from(body))
            .unwrap()
    }
}

async fn capabilities(State(f): State<Arc<Fixture>>) -> Response {
    f.json_response(
        "capabilities",
        0,
        json!({
            "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
            "features": {"resolve": true, "directory": true, "leases": true,
                         "metadata_pages": true, "raw_blob": true, "objects": true}
        }),
    )
}

async fn resolve(State(f): State<Arc<Fixture>>, body: Bytes) -> Response {
    let request: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(request["scope"], "/project");
    assert_eq!(request["target"]["kind"], "latest");
    let generation = f.latest.load(Ordering::SeqCst);
    let version = &f.versions[generation];
    let descriptor = version.serving_descriptor();
    let sid = version.snapshot_id();
    let mut leases = f.leases.lock().unwrap();
    let lease = format!("bench-lease-{}", leases.len() + 1);
    leases.insert(lease.clone(), sid.clone());
    drop(leases);
    f.json_response("resolve", body.len(), json!({
        "descriptor": {"schema_version": 2, "metadata_codec": 1,
            "instance_id": uuid::Uuid::from_bytes(descriptor.instance_uuid).to_string(),
            "namespace_view_id": id_string(&descriptor.namespace_view_id),
            "scope": "/project", "materialization_policy": 1, "fs_semantics": 1,
            "access_projection": 0, "metadata_root": id_string(&version.root), "snapshot_id": sid},
        "lease_id": lease, "lease_expires_at": lease_expiry(),
        "publication_sequence": (generation + 1).to_string(), "authorization_epoch": "1"
    }))
}

fn lease_expiry() -> String {
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 600;
    let days = (unix / 86400) as i64;
    let seconds = unix % 86400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        seconds / 3600,
        seconds % 3600 / 60,
        seconds % 60
    )
}

fn end(body: &[u8], count: usize, units: usize, bytes: u64, sequence: u64) -> Vec<u8> {
    EndPayload {
        request_item_count: count.try_into().unwrap(),
        unique_unit_count: units.try_into().unwrap(),
        logical_bytes: bytes,
        request_body_sha256: parse_digest(&digest_of(body)).unwrap(),
    }
    .encode(17, sequence)
}

async fn metadata(
    State(f): State<Arc<Fixture>>,
    HttpPath(sid): HttpPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let version = match f.authorized(&sid, &headers) {
        Ok(v) => v,
        Err(r) => return *r,
    };
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    let mut pages = BTreeMap::new();
    let mut requested_pages = Vec::new();
    for item in items {
        let directory = item["directory_path"].as_str().unwrap();
        let route: Vec<u8> = serde_json::from_value(item["route"].clone()).unwrap();
        let id = parse_digest(item["expected_digest"].as_str().unwrap()).unwrap();
        assert_eq!(
            version.routes.get(&(directory.into(), route.clone())),
            Some(&id)
        );
        requested_pages.push(id);
        for depth in 0..=route.len() {
            let witness = version.routes[&(directory.into(), route[..depth].to_vec())];
            pages.insert(witness, version.pages[&witness].clone());
        }
    }
    let mut pages: Vec<_> = pages.into_iter().collect();
    pages.reverse();
    let mut wire = Vec::new();
    for (sequence, batch) in pages.chunks(64).enumerate() {
        wire.extend(
            MetaPayload {
                pages: batch.to_vec(),
            }
            .encode(17, sequence as u64)
            .unwrap(),
        );
    }
    wire.extend(end(
        &body,
        items.len(),
        pages.len(),
        pages.iter().map(|(_, b)| b.len() as u64).sum(),
        pages.len().div_ceil(64) as u64,
    ));
    f.events.lock().unwrap().push(Event {
        kind: "metadata",
        request_bytes: body.len(),
        response_bytes: wire.len(),
        requested_pages,
        response_pages: pages.iter().map(|(id, _)| *id).collect(),
        ..Event::default()
    });
    f.treeframe_response(&sid, &body, wire)
}

async fn objects(
    State(f): State<Arc<Fixture>>,
    HttpPath(sid): HttpPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let version = match f.authorized(&sid, &headers) {
        Ok(v) => v,
        Err(r) => return *r,
    };
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    let mut objects = BTreeMap::new();
    for item in items {
        let path = item["path"].as_str().unwrap().trim_start_matches('/');
        let bytes = version.files.get(path).unwrap();
        let digest = digest_of(bytes);
        assert_eq!(item["expected_digest"], digest);
        objects.insert(parse_digest(&digest).unwrap(), bytes.clone());
    }
    let objects: Vec<_> = objects.into_iter().collect();
    let mut wire = ObjectPayload {
        objects: objects.clone(),
    }
    .encode(17, 0)
    .unwrap();
    wire.extend(end(
        &body,
        items.len(),
        objects.len(),
        objects.iter().map(|(_, b)| b.len() as u64).sum(),
        1,
    ));
    let content = objects
        .iter()
        .map(|(id, b)| {
            let digest = id_string(id);
            let changed = !f.old_content.contains(&digest);
            (digest, b.len(), changed)
        })
        .collect();
    f.events.lock().unwrap().push(Event {
        kind: "objects",
        request_bytes: body.len(),
        response_bytes: wire.len(),
        content,
        ..Event::default()
    });
    f.treeframe_response(&sid, &body, wire)
}

async fn blob(
    State(f): State<Arc<Fixture>>,
    HttpPath(sid): HttpPath<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let version = match f.authorized(&sid, &headers) {
        Ok(v) => v,
        Err(r) => return *r,
    };
    let bytes = version
        .files
        .get(query["path"].trim_start_matches('/'))
        .unwrap();
    let digest = digest_of(bytes);
    assert_eq!(query["expected_digest"], digest);
    f.events.lock().unwrap().push(Event {
        kind: "blob",
        response_bytes: bytes.len(),
        content: vec![(
            digest.clone(),
            bytes.len(),
            !f.old_content.contains(&digest),
        )],
        ..Event::default()
    });
    bytes.clone().into_response()
}

struct HttpFixture {
    state: Arc<Fixture>,
    client: Mst2Client,
    task: tokio::task::JoinHandle<()>,
}

impl HttpFixture {
    async fn start(old: Version, new: Version) -> Self {
        let old_content = old
            .expected
            .iter()
            .map(|f| f.content_digest.clone())
            .collect();
        let state = Arc::new(Fixture {
            versions: [Arc::new(old), Arc::new(new)],
            latest: AtomicUsize::new(0),
            leases: Mutex::new(HashMap::new()),
            events: Mutex::new(Vec::new()),
            old_content,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(capabilities))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
            .route("/api/v2/snapshots/{sid}/objects", post(objects))
            .route("/api/v2/snapshots/{sid}/blob", get(blob))
            .with_state(state.clone());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            state,
            client,
            task,
        }
    }

    async fn reader(&self) -> SnapshotReader {
        SnapshotReader::resolve(self.client.clone(), "/project", 600)
            .await
            .unwrap()
    }
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn assert_manifest(actual: &[SnapshotFile], expected: &[SnapshotFile]) {
    let index = |files: &[SnapshotFile]| {
        files
            .iter()
            .map(|f| {
                (
                    f.rel_path.clone(),
                    (f.fs_kind.clone(), f.size, f.content_digest.clone()),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(
        index(actual),
        index(expected),
        "independent input-file oracle"
    );
    assert_eq!(actual.len(), expected.len(), "duplicate logical paths");
}

async fn hydrate(
    cache: &ScopeCache,
    reader: &SnapshotReader,
    manifest: &[SnapshotFile],
) -> (DurableStore, HydrateReport) {
    let store = DurableStore::open_with_content(
        cache
            .dir()
            .join(reader.snapshot_id().trim_start_matches("sha256:")),
        cache.dir().join("blobs"),
    )
    .unwrap();
    let view = ViewMeta {
        snapshot_id: reader.snapshot_id().into(),
        namespace_view_id: reader.descriptor().namespace_view_id.clone(),
        scope: reader.descriptor().scope.clone(),
        lease_id: reader.lease_id().to_owned(),
    };
    let client = reader.client().clone();
    let sid = reader.snapshot_id().to_string();
    let report = store
        .hydrate_batches(
            &view,
            manifest,
            4,
            4,
            move |batch| {
                let client = client.clone();
                let sid = sid.clone();
                Box::pin(async move {
                    let items = batch
                        .iter()
                        .map(|f| (format!("/{}", f.rel_path), f.content_digest.clone()))
                        .collect::<Vec<_>>();
                    let got = client.objects(&sid, &items, None).await?;
                    batch
                        .iter()
                        .map(|f| {
                            let id = parse_digest(&f.content_digest)?;
                            let bytes = got
                                .get(&id)
                                .expect("fixture must return every requested digest");
                            Ok((f.content_digest.clone(), Arc::new(bytes.clone())))
                        })
                        .collect()
                })
            },
            {
                let reader = reader.clone();
                move |file| {
                    let reader = reader.clone();
                    Box::pin(async move {
                        Ok(Arc::new(
                            reader
                                .read_file(&file.rel_path, &file.content_digest)
                                .await?,
                        ))
                    })
                }
            },
        )
        .await
        .unwrap();
    store.pin(&view).unwrap();
    assert!(report.complete);
    assert!(store.is_complete().unwrap());
    (store, report)
}

#[derive(Default, Serialize)]
struct ProcessSample {
    cpu_ns: Option<u64>,
    io: BTreeMap<String, u64>,
}

impl ProcessSample {
    fn read() -> Self {
        #[cfg(target_os = "linux")]
        {
            let mut sample = Self::default();
            let mut clock = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // CLOCK_PROCESS_CPUTIME_ID measures this test plus its HTTP fixture.
            if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut clock) } == 0 {
                sample.cpu_ns = Some(clock.tv_sec as u64 * 1_000_000_000 + clock.tv_nsec as u64);
            }
            if let Ok(io) = fs::read_to_string("/proc/self/io") {
                for line in io.lines() {
                    if let Some((key, value)) = line.split_once(':') {
                        if let Ok(value) = value.trim().parse() {
                            sample.io.insert(key.into(), value);
                        }
                    }
                }
            }
            sample
        }
        #[cfg(not(target_os = "linux"))]
        Self::default()
    }

    fn delta(&self) -> Value {
        let end = Self::read();
        let io: BTreeMap<_, _> = end
            .io
            .iter()
            .map(|(k, v)| (k, v.saturating_sub(*self.io.get(k).unwrap_or(&0))))
            .collect();
        json!({"cpu_ns": self.cpu_ns.zip(end.cpu_ns).map(|(a,b)| b.saturating_sub(a)), "io": io})
    }
}

fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn local_sync_work(meters: SyncMeters) -> Value {
    json!({
        "closure_index_reads": meters.closure_index_reads,
        "closure_index_read_bytes": meters.closure_index_read_bytes,
        "closure_index_writes": meters.closure_index_writes,
        "closure_index_write_bytes": meters.closure_index_write_bytes,
        "pin_set_reads": meters.pin_set_reads,
        "page_rehashes": meters.page_rehashes,
        "unique_page_rehashes": meters.unique_page_rehashes,
        "page_rehash_bytes": meters.page_rehash_bytes,
    })
}

fn emit(value: &Value) {
    let line = serde_json::to_string(value).unwrap();
    println!("MST2_UPDATE_BENCH {line}");
    if let Ok(path) = std::env::var("MST2_UPDATE_BENCH_OUTPUT") {
        let mut output = open_output(Path::new(&path));
        writeln!(output, "{line}").unwrap();
    }
}

fn open_output(path: &Path) -> fs::File {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .unwrap();
    let parent = path
        .parent()
        .expect("result path needs a private parent directory")
        .canonicalize()
        .unwrap();
    assert!(
        !parent.starts_with(&repository),
        "benchmark results must stay outside the Git checkout"
    );
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            assert!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "benchmark output must be a regular file, not a symlink"
            );
            assert!(
                !path.canonicalize().unwrap().starts_with(&repository),
                "benchmark output target must stay outside the Git checkout"
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("cannot inspect benchmark output: {error}"),
    }
    let mut options = fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let output = options.open(path).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let metadata = output
            .metadata()
            .expect("cannot inspect opened benchmark output");
        assert_eq!(
            metadata.nlink(),
            1,
            "benchmark output must not be a hard link"
        );
    }
    output
}

#[cfg(unix)]
#[test]
fn output_symlinks_cannot_append_to_checkout_files() {
    let temp = tempfile::tempdir().unwrap();
    let tracked = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let original = fs::read(&tracked).unwrap();
    let output = temp.path().join("results.jsonl");
    std::os::unix::fs::symlink(&tracked, &output).unwrap();
    assert!(std::panic::catch_unwind(|| open_output(&output)).is_err());
    assert_eq!(fs::read(&tracked).unwrap(), original);
    fs::remove_file(&output).unwrap();

    let hardlink = temp.path().join("hardlink.jsonl");
    fs::hard_link(&tracked, &hardlink).unwrap();
    assert!(std::panic::catch_unwind(|| open_output(&hardlink)).is_err());
    assert_eq!(fs::read(&tracked).unwrap(), original);
    fs::remove_file(&hardlink).unwrap();

    writeln!(open_output(&output), "private result").unwrap();
    assert_eq!(fs::read_to_string(&output).unwrap(), "private result\n");
}

fn command_version(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_update_costs_preserve_views_and_separate_work() {
    let scale = Scale::environment();
    let baseline = scale.baseline();
    emit(
        &json!({"record": "environment", "backend": "reference_fixture_http",
        "scale": scale, "baseline_files": baseline.len(), "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH, "cpu_parallelism": std::thread::available_parallelism().map(|n| n.get()).ok(),
        "source_head": command_version("git", &["rev-parse", "HEAD"]), "rustc": command_version("rustc", &["--version"]),
        "hostname": command_version("hostname", &[]), "kernel": command_version("uname", &["-r"]),
        "benchmark_sha256": digest_of(include_bytes!("mst2_commit_update_bench.rs")),
        "lock_sha256": digest_of(&fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock")).unwrap()),
        "cache_conditions": "new private app cache for each scenario/round; V1 hydrated/pinned before V2; OS cache uncontrolled; prebuilt fixture",
        "counter_limits": "SyncMeters traversal counts decoded logical pages; local_sync_work counts index read attempts/bytes, published writes/bytes, pin scans, actual cached-page hash calls/bytes including repeats and corrupt pages, and unique hashed page ids; missing pages and initial network-page validation are excluded from rehash counts; pin COMPLETE audits and hydration CAS verification are not separately instrumented; process CPU/IO includes fixture",
        "reuse_policy": "a subtree requires a pin backed by a current COMPLETE dependency audit; deliberately missing old CAS content revokes its COMPLETE and forces metadata traversal using verified individual cached pages; reuse counters for this damage scenario are not comparable with the former pin-JSON policy",
        "server_publication": "NOT_RUN", "server_rebuilt_pages": null, "server_scan_traversal_nodes": null,
        "crash_gc_offline_authorization": "NOT_RUN", "durable_complete": "current client completion plus real pin; no power-loss claim"}),
    );
    let mut timings: BTreeMap<&str, Vec<(f64, f64, f64)>> = BTreeMap::new();
    for round in 0..scale.rounds {
        // Rotate case order; every case still gets independent application caches.
        for offset in 0..Scenario::ALL.len() {
            let scenario = Scenario::ALL[(round + offset) % Scenario::ALL.len()];
            let build_start = Instant::now();
            let old = Version::build(baseline.clone());
            let new = Version::build(scenario.next(&baseline, &scale));
            let unchanged_pages = new
                .pages
                .keys()
                .filter(|id| old.pages.contains_key(*id))
                .count();
            let new_page_ids = new.pages.len() - unchanged_pages;
            for (id, bytes) in &new.pages {
                if let Some(previous) = old.pages.get(id) {
                    assert_eq!(
                        previous, bytes,
                        "a shared page ID must mean identical bytes"
                    );
                }
            }
            if matches!(
                scenario,
                Scenario::SingleFile | Scenario::SameDirectoryBatch | Scenario::CacheMissing
            ) {
                assert_eq!(new_page_ids, 2, "changed directory plus its scope root");
            }
            let fixture_build_ms = ms(build_start);
            let http = HttpFixture::start(old, new).await;
            let tmp = tempfile::tempdir().unwrap();
            let cache = ScopeCache::open(tmp.path().join("warm")).unwrap();
            let baseline_mark = http.state.mark();
            let baseline_start = Instant::now();
            let old_reader = http.reader().await;
            let mut old_sync = IncrementalSync::new(&old_reader, &cache);
            let old_manifest = old_sync.sync().await.unwrap();
            let (old_store, old_hydrate) = hydrate(&cache, &old_reader, &old_manifest).await;
            let baseline_ms = ms(baseline_start);
            assert_manifest(&old_manifest, &http.state.versions[0].expected);
            let baseline_wire = http.state.wire(baseline_mark);
            assert_eq!(old_hydrate.fetched as usize, baseline.len());
            assert_eq!(old_sync.meters().closure_index_reads, 1);
            assert_eq!(old_sync.meters().closure_index_writes, 1);
            assert_eq!(old_sync.meters().pin_set_reads, 1);
            let old_complete_before_update = old_store.root().join("DURABLE_COMPLETE").exists();
            assert!(old_complete_before_update);
            http.state.latest.store(1, Ordering::SeqCst);

            let cold = ScopeCache::open(tmp.path().join("cold-v2")).unwrap();
            let cold_mark = http.state.mark();
            let cold_start = Instant::now();
            let cold_reader = http.reader().await;
            let cold_resolve_ms = ms(cold_start);
            let mut cold_sync = IncrementalSync::new(&cold_reader, &cold);
            let cold_manifest = cold_sync.sync().await.unwrap();
            let cold_metadata_ready_ms = ms(cold_start);
            assert_manifest(&cold_manifest, &http.state.versions[1].expected);
            let cold_wire = http.state.wire(cold_mark);
            assert_eq!(cold_sync.meters().closure_index_reads, 1);
            assert_eq!(cold_sync.meters().closure_index_writes, 1);
            assert_eq!(cold_sync.meters().pin_set_reads, 1);

            let mut missing_page = None;
            if matches!(scenario, Scenario::CacheMissing) {
                let id = http.state.versions[0].routes[&("/d002".into(), Vec::new())];
                fs::remove_file(cache.dir().join("pages").join(hex::encode(id))).unwrap();
                let missing_digest = digest_of(&baseline["d003/f0000.txt"]);
                fs::remove_file(
                    old_store
                        .content_dir()
                        .join(missing_digest.strip_prefix("sha256:").unwrap()),
                )
                .unwrap();
                missing_page = Some(id);
            }
            let warm_mark = http.state.mark();
            let process = ProcessSample::read();
            let update_start = Instant::now();
            let reader = http.reader().await;
            let latest_resolve_ms = ms(update_start);
            let resolve_process_work = process.delta();
            assert_ne!(old_reader.snapshot_id(), reader.snapshot_id());
            assert_ne!(old_reader.lease_id(), reader.lease_id());
            let mut sync = IncrementalSync::new(&reader, &cache);
            let manifest = sync.sync().await.unwrap();
            let metadata_ready_ms = ms(update_start);
            let metadata_process_work = process.delta();
            let metadata_wire = http.state.wire(warm_mark);
            let (store, report) = hydrate(&cache, &reader, &manifest).await;
            let durable_complete_ms = ms(update_start);
            timings.entry(scenario.name()).or_default().push((
                latest_resolve_ms,
                metadata_ready_ms,
                durable_complete_ms,
            ));
            let process_work = process.delta();
            // Observe repair evidence outside the measured work. The V2
            // hydration can restore the shared body, but it cannot restore
            // the V1 marker that its pin audit revoked before metadata sync.
            let old_complete_after_update = old_store.root().join("DURABLE_COMPLETE").exists();
            let old_pin_repair_reason =
                fs::read_to_string(old_store.root().join("NEEDS_REPAIR")).ok();
            assert_manifest(&manifest, &http.state.versions[1].expected);
            let update_wire = http.state.wire(warm_mark);
            let changed_files: Vec<_> = manifest
                .iter()
                .filter(|file| !http.state.old_content.contains(&file.content_digest))
                .collect();
            let changed_bytes: u64 = changed_files.iter().map(|file| file.size).sum();
            assert_eq!(update_wire.changed_content_bytes as u64, changed_bytes);
            let missing_bodies = usize::from(matches!(scenario, Scenario::CacheMissing));
            assert_eq!(
                update_wire.unchanged_content_units, missing_bodies,
                "unchanged bodies are sent only after deliberate CAS removal"
            );
            assert_eq!(
                report.fetched as usize,
                changed_files.len() + missing_bodies
            );
            assert_eq!(
                report.resumed as usize + report.fetched as usize,
                manifest.len()
            );
            assert_eq!(sync.meters().closure_index_reads, 1);
            assert_eq!(sync.meters().closure_index_writes, 1);
            assert_eq!(sync.meters().pin_set_reads, 1);
            if matches!(scenario, Scenario::CacheMissing) {
                assert!(
                    !old_complete_after_update,
                    "a damaged dependency revokes the old commit"
                );
                assert!(old_pin_repair_reason.as_deref().is_some_and(
                    |reason| reason.contains("completion dependency missing or corrupt")
                ));
                assert_eq!(
                    sync.meters().reused_subtrees,
                    0,
                    "a damaged old pin cannot authorize subtree reuse"
                );
                assert_eq!(
                    sync.meters().traversal_nodes,
                    cold_sync.meters().traversal_nodes,
                    "without an audited pin the client must walk the full metadata tree"
                );
                assert_eq!(metadata_wire.requested_pages, 3,
                    "individual cached pages still limit fetching to two changed pages and one missing page");
            } else {
                assert!(old_complete_after_update);
                assert!(old_pin_repair_reason.is_none());
                assert!(sync.meters().reused_subtrees > 0);
                assert!(sync.meters().traversal_nodes < cold_sync.meters().traversal_nodes);
            }
            assert!(metadata_wire.requested_pages < cold_wire.requested_pages);
            assert_eq!(
                sync.meters().fetched_pages as usize,
                metadata_wire.response_pages,
                "ancestor witness pages must also be counted"
            );
            if let Some(id) = missing_page {
                assert!(metadata_wire.requested_page_ids.contains(&id_string(&id)));
            }
            if matches!(scenario, Scenario::WideDirectorySplit) {
                let old_wide = http.state.versions[0].routes[&("/wide".into(), Vec::new())];
                let new_wide = http.state.versions[1].routes[&("/wide".into(), Vec::new())];
                assert!(matches!(
                    Page::decode(&http.state.versions[0].pages[&old_wide])
                        .unwrap()
                        .0,
                    Page::Leaf { .. }
                ));
                assert!(matches!(
                    Page::decode(&http.state.versions[1].pages[&new_wide])
                        .unwrap()
                        .0,
                    Page::Branch { .. }
                ));
            }

            // Independent correctness traffic is deliberately outside the timing/counters above.
            let correctness_mark = http.state.mark();
            let verify_start = Instant::now();
            assert_eq!(store.verify_all(&manifest).unwrap(), manifest.len() as u64);
            assert_manifest(&store.manifest().unwrap(), &http.state.versions[1].expected);
            assert_manifest(
                &old_reader.file_manifest_pages().await.unwrap(),
                &http.state.versions[0].expected,
            );
            let old_file = &http.state.versions[0].expected[0];
            let mismatch = reader
                .client()
                .blob_verified(
                    old_reader.snapshot_id(),
                    &format!("/{}", old_file.rel_path),
                    &old_file.content_digest,
                )
                .await
                .unwrap_err();
            assert_eq!(
                mismatch.code,
                SnapshotErrorCode::LeaseExpired,
                "the fixture must reject a new view's lease on an old view"
            );
            assert_eq!(
                old_reader
                    .read_file(&old_file.rel_path, &old_file.content_digest)
                    .await
                    .unwrap(),
                baseline[&old_file.rel_path]
            );
            let new_file = changed_files.first().copied().unwrap_or(&manifest[0]);
            assert_eq!(
                reader
                    .read_file(&new_file.rel_path, &new_file.content_digest)
                    .await
                    .unwrap(),
                http.state.versions[1].files[&new_file.rel_path]
            );
            let verification_ms = ms(verify_start);
            let meters = sync.meters();
            emit(
                &json!({"record": "round", "backend": "reference_fixture_http", "round": round + 1,
                "scenario": scenario.name(), "files": manifest.len(), "fixture_build_ms": fixture_build_ms,
                "reference_new_page_ids": new_page_ids, "reference_unchanged_page_ids": unchanged_pages,
                "old_snapshot": old_reader.snapshot_id(), "new_snapshot": reader.snapshot_id(),
                "baseline_hydrate_ms": baseline_ms, "baseline_wire": baseline_wire,
                "baseline_local_sync_work": local_sync_work(old_sync.meters()),
                "cold_latest_resolve_ms": cold_resolve_ms, "cold_metadata_ready_ms": cold_metadata_ready_ms,
                "cold_wire": cold_wire, "cold_traversal_nodes": cold_sync.meters().traversal_nodes,
                "cold_local_sync_work": local_sync_work(cold_sync.meters()),
                "latest_resolve_ms": latest_resolve_ms, "metadata_ready_ms": metadata_ready_ms,
                "durable_complete_ms": durable_complete_ms, "metadata_wire": metadata_wire, "update_wire": update_wire,
                "client_fetched_pages": meters.fetched_pages, "client_reused_pages": meters.reused_pages,
                "client_traversal_nodes": meters.traversal_nodes, "client_reused_subtrees": meters.reused_subtrees,
                "client_local_sync_work": local_sync_work(meters),
                "hydrate_manifest_logical_files": manifest.len(), "cas_verification_read_calls": null, "reuse_page_rehash_calls": meters.page_rehashes,
                "old_complete_before_update": old_complete_before_update, "old_complete_after_update": old_complete_after_update,
                "old_pin_repair_reason": old_pin_repair_reason,
                "hydrate_fetched": report.fetched, "hydrate_resumed": report.resumed, "changed_input_bytes": changed_bytes,
                "resolve_process_work": resolve_process_work, "metadata_ready_process_work": metadata_process_work,
                "process_work": process_work, "correctness_verification_ms": verification_ms,
                "correctness_wire": http.state.wire(correctness_mark), "correctness": "PASS",
                "server_commit_publication": "NOT_RUN", "server_rebuilt_pages": null, "server_scan_traversal_nodes": null}),
            );
        }
    }
    for (scenario, samples) in timings {
        let distribution = |select: fn(&(f64, f64, f64)) -> f64| {
            let mut sorted: Vec<_> = samples.iter().map(select).collect();
            sorted.sort_by(f64::total_cmp);
            let percentile =
                |percentage: usize| sorted[(sorted.len() * percentage).div_ceil(100) - 1];
            json!({"min": sorted[0], "p50": percentile(50), "p95": percentile(95), "max": sorted[sorted.len() - 1]})
        };
        emit(
            &json!({"record": "summary", "backend": "reference_fixture_http", "scenario": scenario,
            "rounds": samples.len(), "latest_resolve_ms": distribution(|s| s.0),
            "metadata_ready_ms": distribution(|s| s.1), "durable_complete_ms": distribution(|s| s.2)}),
        );
    }
}
