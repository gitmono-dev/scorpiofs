//! Exercise the shipped launcher, not just the service constructor. The kernel
//! case also runs inside the isolated Qlean VM with the exact release binary.
#![cfg(unix)]

use std::{
    fs::File,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
#[cfg(target_os = "linux")]
use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use axum::{
    body::{Body, Bytes},
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use mst2_codec::{
    descriptor::ServingDescriptor,
    metapage::{page_id, Entry, EntryKind, Page},
    treeframe::{EndPayload, MetaPayload},
};
use scorpiofs::snapshot::{durable::digest_of, frames::parse_digest};
use serde_json::{json, Value};

const INSTANCE: &str = "11111111-2222-4333-8444-555555555555";
const CONTENT: [&[u8]; 2] = [b"old fixed content", b"new fixed content"];

fn digest(id: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(id))
}

struct Version {
    root: [u8; 32],
    root_bytes: Vec<u8>,
    child: [u8; 32],
    child_bytes: Vec<u8>,
    sid: String,
}

impl Version {
    fn new(content: &[u8]) -> Self {
        let child_bytes = Page::Leaf { entries: vec![] }.encode().unwrap();
        let child = page_id(&child_bytes);
        let root_bytes = Page::Leaf {
            entries: vec![
                Entry::file(
                    EntryKind::Regular,
                    b".wh..wh..opq",
                    content.len() as u64,
                    parse_digest(&digest_of(content)).unwrap(),
                ),
                Entry::file(
                    EntryKind::Regular,
                    b".wh.base.txt",
                    content.len() as u64,
                    parse_digest(&digest_of(content)).unwrap(),
                ),
                Entry::file(
                    EntryKind::Regular,
                    b"base.txt",
                    content.len() as u64,
                    parse_digest(&digest_of(content)).unwrap(),
                ),
                Entry::dir(b"deep", child),
            ],
        }
        .encode()
        .unwrap();
        let root = page_id(&root_bytes);
        let sid = digest(
            &ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
                namespace_view_id: [0x22; 32],
                scope: "/project".into(),
                metadata_root: root,
            }
            .snapshot_id()
            .unwrap(),
        );
        Self {
            root,
            root_bytes,
            child,
            child_bytes,
            sid,
        }
    }
}

struct Fixture {
    versions: [Version; 2],
    latest: AtomicUsize,
    reject_resolve: AtomicBool,
    requests: Mutex<Vec<String>>,
    child_pages: AtomicUsize,
    blobs: AtomicUsize,
}

impl Fixture {
    fn new() -> Self {
        Self {
            versions: CONTENT.map(Version::new),
            latest: AtomicUsize::new(0),
            reject_resolve: AtomicBool::new(false),
            requests: Mutex::new(Vec::new()),
            child_pages: AtomicUsize::new(0),
            blobs: AtomicUsize::new(0),
        }
    }

    fn version(&self, sid: &str) -> usize {
        self.versions
            .iter()
            .position(|v| v.sid == sid)
            .expect("fixed SID")
    }
}

async fn record(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let state = request.extensions().get::<Arc<Fixture>>().unwrap();
    state
        .requests
        .lock()
        .unwrap()
        .push(request.uri().path().into());
    next.run(request).await
}

async fn capabilities() -> Json<Value> {
    Json(json!({
        "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true,
            "metadata_pages": true, "raw_blob": true}
    }))
}

async fn resolve(State(f): State<Arc<Fixture>>, Json(request): Json<Value>) -> Json<Value> {
    assert_eq!(request["target"], json!({"kind": "latest"}));
    assert_eq!(request["scope"], "/project");
    if f.reject_resolve.load(Ordering::SeqCst) {
        return Json(json!({"publication_sequence": null}));
    }
    let v = &f.versions[f.latest.load(Ordering::SeqCst)];
    Json(json!({
        "descriptor": {
            "schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE,
            "namespace_view_id": digest(&[0x22; 32]), "scope": "/project",
            "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
            "metadata_root": digest(&v.root), "snapshot_id": v.sid
        },
        "lease_id": "launcher-lease", "lease_expires_at": "2099-01-01T00:00:00Z",
        "publication_sequence": "1", "authorization_epoch": "1"
    }))
}

async fn metadata(
    State(f): State<Arc<Fixture>>,
    AxumPath(sid): AxumPath<String>,
    body: Bytes,
) -> Response {
    let v = &f.versions[f.version(&sid)];
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["route"], json!([]));
    let (id, bytes) = match items[0]["directory_path"].as_str().unwrap() {
        "/" => (v.root, &v.root_bytes),
        "/deep" => {
            f.child_pages.fetch_add(1, Ordering::SeqCst);
            (v.child, &v.child_bytes)
        }
        _ => panic!("unexpected metadata path"),
    };
    assert_eq!(items[0]["expected_digest"], digest(&id));
    let mut wire = MetaPayload {
        pages: vec![(id, bytes.clone())],
    }
    .encode(7, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: bytes.len() as u64,
            request_body_sha256: parse_digest(&digest_of(&body)).unwrap(),
        }
        .encode(7, 1),
    );
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", sid)
        .header("x-mega-request-digest", digest_of(&body))
        .body(Body::from(wire))
        .unwrap()
}

async fn blob(State(f): State<Arc<Fixture>>, AxumPath(sid): AxumPath<String>) -> Response {
    f.blobs.fetch_add(1, Ordering::SeqCst);
    CONTENT[f.version(&sid)].to_vec().into_response()
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn fixture_server(f: Arc<Fixture>) -> (String, Server) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/api/v2/snapshots/capabilities", get(capabilities))
        .route("/api/v2/snapshots/resolve", post(resolve))
        .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
        .route("/api/v2/snapshots/{sid}/blob", get(blob))
        .fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR })
        .layer(axum::middleware::from_fn(record))
        .layer(axum::Extension(f.clone()))
        .with_state(f);
    (
        base,
        Server(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap()
        })),
    )
}

struct Launcher {
    child: Child,
    temp: tempfile::TempDir,
    base: String,
}

impl Drop for Launcher {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if cfg!(target_os = "linux") {
            // Only exact mount paths owned by this fixture. A failing assertion
            // must not leave either mount behind in the VM.
            for name in ["mount-old", "mount-new"] {
                let _ = Command::new("fusermount3")
                    .args(["-u", "-z"])
                    .arg(self.temp.path().join(name))
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }
}

impl Launcher {
    fn start(upstream: &str, addr: std::net::SocketAddr) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let mut config = toml::Table::new();
        for (key, value) in [
            ("base_url", upstream),
            ("mst2_base_url", upstream),
            ("mst2_scope", "/project"),
        ] {
            config.insert(key.into(), value.into());
        }
        config.insert("mst2_lower_enabled".into(), true.into());
        for (key, name) in [
            ("workspace", "unused-root"),
            ("store_path", "dictionary"),
            ("config_file", "invalid-legacy-state.toml"),
            ("antares_upper_root", "upper"),
            ("antares_cl_root", "cl"),
            ("antares_mount_root", "mounts"),
            ("antares_state_file", "antares-state.toml"),
        ] {
            config.insert(key.into(), temp.path().join(name).to_str().unwrap().into());
        }
        // The retired workspace manager must not read even a corrupt state file.
        std::fs::write(
            temp.path().join("invalid-legacy-state.toml"),
            "this is not TOML",
        )
        .unwrap();
        let path = temp.path().join("scorpio.toml");
        std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
        let binary = std::env::var_os("SCORPIO_LAUNCHER_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_scorpio")));
        let log = File::create(temp.path().join("launcher.log")).unwrap();
        let child = Command::new(binary)
            .env_clear()
            .envs(
                std::env::vars_os()
                    .filter(|(key, _)| !key.to_string_lossy().starts_with("SCORPIO_")),
            )
            .arg("--config-path")
            .arg(path)
            .arg("serve")
            .arg("--http-addr")
            .arg(addr.to_string())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        Self {
            child,
            temp,
            base: format!("http://{addr}"),
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.temp.path().join("launcher.log")).unwrap()
    }

    async fn ready(&mut self, client: &reqwest::Client) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    self.child.try_wait().unwrap().is_none(),
                    "launcher exited: {}",
                    self.log()
                );
                if let Ok(response) = client.get(format!("{}/health", self.base)).send().await {
                    if response.status().is_success() {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("launcher did not become healthy: {}", self.log()));
    }

    async fn stop(&mut self) {
        assert_eq!(
            unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) },
            0
        );
        let status = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("launcher failed to stop: {}", self.log()));
        assert!(status.success(), "shutdown failed: {}", self.log());
    }

    fn assert_no_dictionary(&self) {
        assert_eq!(
            std::fs::read_dir(self.temp.path().join("dictionary"))
                .unwrap()
                .count(),
            0
        );
    }
}

fn address() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn launcher_starts_without_root_mount_or_dictionary_and_removes_legacy_routes() {
    let f = Arc::new(Fixture::new());
    f.reject_resolve.store(true, Ordering::SeqCst);
    let (upstream, _server) = fixture_server(f.clone()).await;
    let mut launcher = Launcher::start(&upstream, address());
    let client = client();
    launcher.ready(&client).await;
    assert!(f.requests.lock().unwrap().is_empty());
    launcher.assert_no_dictionary();
    let health: Value = client
        .get(format!("{}/health", launcher.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(health["version"], env!("CARGO_PKG_VERSION"));
    assert!(health["mount_count"].is_null());
    assert!(!health.to_string().contains('/'));
    for (method, path) in [
        (reqwest::Method::POST, "/api/fs/mount"),
        (reqwest::Method::GET, "/api/fs/mpoint"),
        (reqwest::Method::GET, "/api/fs/select/old"),
        (reqwest::Method::POST, "/api/fs/unmount"),
        (reqwest::Method::GET, "/api/config"),
        (reqwest::Method::POST, "/api/config"),
    ] {
        assert_eq!(
            client
                .request(method, format!("{}{path}", launcher.base))
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    let response = client
        .post(format!("{}/antares/mounts", launcher.base))
        .json(&json!({"job_id": "rejected", "path": "/project"}))
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
    assert_eq!(
        *f.requests.lock().unwrap(),
        [
            "/api/v2/snapshots/capabilities",
            "/api/v2/snapshots/resolve"
        ]
    );
    launcher.assert_no_dictionary();
    let mounts: Value = client
        .get(format!("{}/antares/mounts", launcher.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(mounts["mounts"], json!([]));
    launcher.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bind_failure_precedes_dictionary_and_workspace_initialization() {
    let f = Arc::new(Fixture::new());
    let (upstream, _server) = fixture_server(f.clone()).await;
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut launcher = Launcher::start(&upstream, occupied.local_addr().unwrap());
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = launcher.child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("bind failure must return promptly");
    assert_eq!(status.code(), Some(4), "{}", launcher.log());
    assert!(f.requests.lock().unwrap().is_empty());
    launcher.assert_no_dictionary();
}

#[cfg(target_os = "linux")]
fn mounted(path: &Path) -> bool {
    std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .any(|line| line.split_whitespace().nth(4) == path.to_str())
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires /dev/fuse; run inside the isolated Qlean VM"]
async fn explicit_snapshot_mounts_keep_old_handles_and_dirty_upper_on_shutdown() {
    let f = Arc::new(Fixture::new());
    let (upstream, _server) = fixture_server(f.clone()).await;
    let mut launcher = Launcher::start(&upstream, address());
    let client = client();
    launcher.ready(&client).await;
    let old = launcher.temp.path().join("mount-old");
    let new = launcher.temp.path().join("mount-new");
    let mut ids = Vec::new();
    let mut old_fd = None;
    for (index, path) in [&old, &new].into_iter().enumerate() {
        f.latest.store(index, Ordering::SeqCst);
        let response = client.post(format!("{}/antares/mounts", launcher.base))
            .json(&json!({"job_id": format!("snapshot-{index}"), "path": "/project", "mountpoint": path}))
            .send().await.unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert!(
            status.is_success(),
            "mount rejected: {body}; {}",
            launcher.log()
        );
        ids.push(body["mount_id"].as_str().unwrap().to_owned());
        assert!(mounted(path));
        if index == 0 {
            // Keep this handle and dirty upper alive before changing latest
            // and admitting the second snapshot.
            old_fd = Some(File::open(old.join("base.txt")).unwrap());
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(old.join("dirty.txt"))
                .unwrap();
            file.write_all(b"keep this dirty upper").unwrap();
            file.sync_all().unwrap();
            File::open(&old).unwrap().sync_all().unwrap();
        }
    }
    assert_ne!(ids[0], ids[1]);
    assert!(!mounted(&launcher.temp.path().join("unused-root")));
    launcher.assert_no_dictionary();
    // Give the retired background preload walk time to run. A nested leaf must
    // still be untouched until an explicit lookup needs it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(f.child_pages.load(Ordering::SeqCst), 0);
    assert_eq!(f.blobs.load(Ordering::SeqCst), 0);
    let mut old_fd = old_fd.unwrap();
    let mut bytes = Vec::new();
    old_fd.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, CONTENT[0]);
    assert_eq!(std::fs::read(new.join("base.txt")).unwrap(), CONTENT[1]);
    // These are ordinary committed filenames in the complete snapshot,
    // even though the writable upper uses OCI names for its private deltas.
    for name in [".wh..wh..opq", ".wh.base.txt"] {
        assert_eq!(std::fs::read(new.join(name)).unwrap(), CONTENT[1]);
    }
    let dirty = old.join("dirty.txt");
    assert_eq!(std::fs::read(&dirty).unwrap(), b"keep this dirty upper");
    old_fd.seek(SeekFrom::Start(0)).unwrap();
    bytes.clear();
    old_fd.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, CONTENT[0]);
    // Explicit child lookup still fetches and verifies the committed page.
    assert_eq!(std::fs::read_dir(new.join("deep")).unwrap().count(), 0);
    assert_eq!(f.child_pages.load(Ordering::SeqCst), 1);
    drop(old_fd);
    launcher.stop().await;
    assert!(!mounted(&old));
    assert!(!mounted(&new));
    assert_eq!(
        std::fs::read(
            launcher
                .temp
                .path()
                .join("upper")
                .join(&ids[0])
                .join("dirty.txt")
        )
        .unwrap(),
        b"keep this dirty upper"
    );
    launcher.assert_no_dictionary();
}
