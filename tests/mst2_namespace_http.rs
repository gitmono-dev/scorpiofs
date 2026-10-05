//! Real canonical HTTP metadata must prove absence without fetching bodies.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use mst2_codec::{
    descriptor::ServingDescriptor,
    metapage::{page_id, BranchChild, Entry, EntryKind, Page},
    treeframe::{EndPayload, MetaPayload},
};
use scorpiofs::snapshot::{
    durable::digest_of, frames::parse_digest, fuse::Mst2Fuse, MetadataProofLimits, Mst2Client,
    SnapshotErrorCode, SnapshotNodeIdentity, SnapshotPathState, SnapshotReader,
};
use serde_json::{json, Value};

type Routes = BTreeMap<(String, Vec<u8>), Vec<Vec<u8>>>;

fn digest(id: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(id))
}
fn file(name: &str, bytes: &[u8]) -> Entry {
    Entry::file(
        EntryKind::Regular,
        name.as_bytes(),
        bytes.len() as u64,
        parse_digest(&digest_of(bytes)).unwrap(),
    )
}
fn directory(routes: &mut Routes, path: &str, mut entries: Vec<Entry>) -> [u8; 32] {
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let root = page_id(&Page::build(&entries).unwrap());
    let mut pending = vec![Vec::new()];
    while let Some(route) = pending.pop() {
        let chain = Page::pages_along_route(&entries, &route).unwrap();
        if let Page::Branch { children, .. } = Page::decode(chain.last().unwrap()).unwrap().0 {
            for child in children {
                let mut next = route.clone();
                next.push(child.label);
                pending.push(next);
            }
        }
        routes.insert((path.to_owned(), route), chain);
    }
    root
}
struct Fixture {
    descriptor: ServingDescriptor,
    routes: Routes,
    calls: Mutex<Vec<Value>>,
    fault: Option<(&'static str, u16, &'static str)>,
    omit: Option<String>,
    path_limit: Option<u32>,
    component_limit: Option<u32>,
}
impl Fixture {
    fn with_routes(root: [u8; 32], routes: Routes) -> Self {
        Self {
            descriptor: ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555")
                    .unwrap()
                    .as_bytes(),
                namespace_view_id: [0x22; 32],
                scope: "/project".into(),
                metadata_root: root,
            },
            routes,
            calls: Mutex::new(Vec::new()),
            fault: None,
            omit: None,
            path_limit: None,
            component_limit: None,
        }
    }
    fn new() -> Self {
        let mut routes = Routes::new();
        let empty = directory(&mut routes, "/empty", vec![]);
        let used = directory(&mut routes, "/used", vec![file("data", b"old")]);
        let unused = directory(&mut routes, "/unused", vec![file("data", b"unused")]);
        let root = directory(
            &mut routes,
            "/",
            vec![
                Entry::dir(b"empty", empty),
                file("plain", b"plain"),
                Entry::file(
                    EntryKind::Symlink,
                    b"sym",
                    4,
                    parse_digest(&digest_of(b"used")).unwrap(),
                ),
                Entry::dir(b"unused", unused),
                Entry::dir(b"used", used),
            ],
        );
        Self::with_routes(root, routes)
    }
    fn wide() -> Self {
        let mut routes = Routes::new();
        let root = directory(
            &mut routes,
            "/",
            (0..192)
                .map(|i| {
                    file(
                        &format!("{}{i:03}", (b'a' + (i / 64) as u8) as char),
                        b"wide",
                    )
                })
                .collect(),
        );
        Self::with_routes(root, routes)
    }
    fn sid(&self) -> String {
        digest(&self.descriptor.snapshot_id().unwrap())
    }
}
async fn caps(State(f): State<Arc<Fixture>>) -> Json<Value> {
    let mut value: Value =
        serde_json::from_str(include_str!("fixtures/mst2_capabilities_0_2_1.json")).unwrap();
    value["limits"]["max_metadata_items"] = json!(2);
    if let Some(limit) = f.path_limit {
        value["limits"]["max_path_bytes"] = json!(limit);
    }
    if let Some(limit) = f.component_limit {
        value["limits"]["max_path_components"] = json!(limit);
    }
    Json(value)
}
async fn resolve(State(f): State<Arc<Fixture>>) -> Json<Value> {
    Json(
        json!({"descriptor":{"schema_version":2,"metadata_codec":1,"instance_id":uuid::Uuid::from_bytes(f.descriptor.instance_uuid).to_string(),"namespace_view_id":digest(&f.descriptor.namespace_view_id),"scope":"/project","materialization_policy":1,"fs_semantics":1,"access_projection":0,"metadata_root":digest(&f.descriptor.metadata_root),"snapshot_id":f.sid()},"publication_sequence":"7","writer_epoch":"3","lease_id":"namespace-lease","lease_expires_at":"2099-01-01T00:00:00Z","authorization_epoch":"11","resolved_at":"2026-10-05T00:00:00Z","delivery":"full"}),
    )
}
async fn metadata(State(f): State<Arc<Fixture>>, Path(sid): Path<String>, body: Bytes) -> Response {
    assert_eq!(sid, f.sid());
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    assert!(items.len() <= 2, "discovered metadata item cap");
    f.calls.lock().unwrap().extend(items.iter().cloned());
    if let Some((path, status, code)) = f.fault {
        if items.iter().any(|item| item["directory_path"] == path) {
            return Response::builder().status(status).header("content-type", "application/json").body(Body::from(json!({"error":{"code":code,"message":"fixed metadata failed","request_id":"namespace-request","retryable":false}}).to_string())).unwrap();
        }
    }
    let mut unique = BTreeMap::new();
    for item in items {
        let path = item["directory_path"].as_str().unwrap().to_owned();
        let route = item["route"]
            .as_array()
            .unwrap()
            .iter()
            .map(|label| label.as_u64().unwrap() as u8)
            .collect::<Vec<_>>();
        let chain = &f.routes[&(path, route)];
        assert_eq!(
            item["expected_digest"],
            digest(&page_id(chain.last().unwrap()))
        );
        for bytes in chain {
            let id = page_id(bytes);
            if f.omit
                .as_ref()
                .is_none_or(|omitted| *omitted != digest(&id))
            {
                unique.insert(id, bytes.clone());
            }
        }
    }
    let pages: Vec<_> = unique.into_iter().collect();
    let logical = pages.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    let mut wire = MetaPayload {
        pages: pages.clone(),
    }
    .encode(7, 0)
    .unwrap();
    wire.extend(
        EndPayload {
            request_item_count: items.len() as u32,
            unique_unit_count: pages.len() as u32,
            logical_bytes: logical,
            request_body_sha256: parse_digest(&digest_of(&body)).unwrap(),
        }
        .encode(7, 1),
    );
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", f.sid())
        .header("x-mega-request-digest", digest_of(&body))
        .body(Body::from(wire))
        .unwrap()
}
struct Server {
    fixture: Arc<Fixture>,
    client: Mst2Client,
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let router = Router::new()
            .route("/api/v2/snapshots/capabilities", get(caps))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
            .with_state(fixture.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self {
            fixture,
            client,
            task,
        }
    }
    async fn reader(&self) -> SnapshotReader {
        SnapshotReader::resolve(self.client.clone(), "/project", 60)
            .await
            .unwrap()
    }
    fn paths(&self) -> Vec<String> {
        self.fixture
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|item| item["directory_path"].as_str().unwrap().to_owned())
            .collect()
    }
}

#[tokio::test]
async fn typed_paths_prove_absence_preserve_directories_and_do_not_expand_unused_children() {
    let server = Server::start(Fixture::new()).await;
    let view = Mst2Fuse::from_reader_lazy(server.reader().await, None)
        .await
        .unwrap();
    assert_eq!(server.paths(), ["/"]);
    assert!(matches!(
        view.path_state("unused").await.unwrap(),
        SnapshotPathState::Present(SnapshotNodeIdentity::Directory { .. })
    ));
    assert_eq!(server.paths(), ["/"]);
    assert_eq!(
        view.path_state("used/missing").await.unwrap(),
        SnapshotPathState::AbsentProven
    );
    assert_eq!(server.paths(), ["/", "/used"]);
    assert_eq!(
        view.path_state("sym/data").await.unwrap_err().code,
        SnapshotErrorCode::SymlinkTraversal
    );
    assert_eq!(
        view.path_state("plain/child").await.unwrap_err().code,
        SnapshotErrorCode::NotDirectory
    );
    assert_eq!(
        view.path_state("../escape").await.unwrap_err().code,
        SnapshotErrorCode::ScopeInvalid
    );
    assert_eq!(
        view.directory_entries("")
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        ["empty", "plain", "sym", "unused", "used"]
    );
    assert!(view.directory_entries("empty").await.unwrap().is_empty());
    assert!(!server.paths().iter().any(|path| path == "/unused"));
}

#[tokio::test]
async fn typed_metadata_failures_never_turn_into_absence() {
    for (status, code, expected) in [
        (404, "PATH_NOT_FOUND", SnapshotErrorCode::PathNotFound),
        (
            503,
            "OBJECT_UNAVAILABLE",
            SnapshotErrorCode::ObjectUnavailable,
        ),
        (401, "UNAUTHENTICATED", SnapshotErrorCode::Unauthenticated),
        (403, "SCOPE_FORBIDDEN", SnapshotErrorCode::ScopeForbidden),
        (410, "LEASE_EXPIRED", SnapshotErrorCode::LeaseExpired),
        (
            503,
            "TEMPORARY_UNAVAILABLE",
            SnapshotErrorCode::TemporaryUnavailable,
        ),
    ] {
        let mut fixture = Fixture::new();
        fixture.fault = Some(("/used", status, code));
        let server = Server::start(fixture).await;
        let view = Mst2Fuse::from_reader_lazy(server.reader().await, None)
            .await
            .unwrap();
        let error = view.path_state("used/missing").await.unwrap_err();
        assert_eq!(error.code, expected);
        assert_eq!(error.http_status, status);
        assert_eq!(
            view.path_state("used/missing").await.unwrap_err().code,
            expected,
            "failed loads stay unproved"
        );
    }
}

#[tokio::test]
async fn lazy_namespace_rejects_noncanonical_partitions_and_false_child_counts() {
    for wrong_count in [false, true] {
        let left = Page::build(&[file("a", b"a")]).unwrap();
        let right = Page::build(&[file("b", b"b")]).unwrap();
        let root = Page::Branch {
            prefix: Vec::new(),
            terminal: None,
            children: vec![
                BranchChild {
                    label: b'a',
                    subtree_entries: if wrong_count { 2 } else { 1 },
                    child_page_id: page_id(&left),
                },
                BranchChild {
                    label: b'b',
                    subtree_entries: 1,
                    child_page_id: page_id(&right),
                },
            ],
        }
        .encode()
        .unwrap();
        let routes = BTreeMap::from([
            (("/".to_owned(), vec![]), vec![root.clone()]),
            (("/".to_owned(), vec![b'a']), vec![root.clone(), left]),
            (("/".to_owned(), vec![b'b']), vec![root.clone(), right]),
        ]);
        let server = Server::start(Fixture::with_routes(page_id(&root), routes)).await;
        let error = Mst2Fuse::from_reader_lazy(server.reader().await, None)
            .await
            .err()
            .expect("unproved tree accepted");
        assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
    }
}

#[tokio::test]
async fn wide_directory_requires_all_committed_pages_and_respects_local_budgets() {
    let server = Server::start(Fixture::wide()).await;
    let view = Mst2Fuse::from_reader_lazy(server.reader().await, None)
        .await
        .unwrap();
    assert_eq!(view.directory_entries("").await.unwrap().len(), 192);
    assert_eq!(
        view.path_state("absent").await.unwrap(),
        SnapshotPathState::AbsentProven
    );
    let mut fixture = Fixture::wide();
    fixture.omit = Some(digest(&page_id(
        fixture
            .routes
            .iter()
            .find(|((_, route), _)| !route.is_empty())
            .unwrap()
            .1
            .last()
            .unwrap(),
    )));
    let server = Server::start(fixture).await;
    assert!(Mst2Fuse::from_reader_lazy(server.reader().await, None)
        .await
        .is_err());
    for limits in [
        MetadataProofLimits {
            max_directory_pages: 1,
            ..Default::default()
        },
        MetadataProofLimits {
            max_directory_entries: 1,
            ..Default::default()
        },
        MetadataProofLimits {
            max_cached_nodes: 1,
            ..Default::default()
        },
    ] {
        let server = Server::start(Fixture::wide()).await;
        let error = Mst2Fuse::from_reader_lazy_with_limits(server.reader().await, None, limits)
            .await
            .err()
            .expect("budget ignored");
        assert!(matches!(
            error.code,
            SnapshotErrorCode::ProofBudgetExceeded | SnapshotErrorCode::LimitExceeded
        ));
    }
}

#[tokio::test]
async fn old_and_new_fixed_readers_keep_distinct_content_identities() {
    let old = Server::start(Fixture::new()).await;
    let mut fixture = Fixture::new();
    let used = directory(&mut fixture.routes, "/used", vec![file("data", b"new")]);
    let mut root = Page::decode(fixture.routes[&("/".to_owned(), vec![])].last().unwrap())
        .unwrap()
        .0;
    let Page::Leaf { entries } = &mut root else {
        unreachable!()
    };
    entries
        .iter_mut()
        .find(|entry| entry.name == b"used")
        .unwrap()
        .child_root = used;
    let bytes = root.encode().unwrap();
    fixture.descriptor.metadata_root = page_id(&bytes);
    fixture.routes.insert(("/".to_owned(), vec![]), vec![bytes]);
    let new = Server::start(fixture).await;
    let old_view = Mst2Fuse::from_reader_lazy(old.reader().await, None)
        .await
        .unwrap();
    let new_view = Mst2Fuse::from_reader_lazy(new.reader().await, None)
        .await
        .unwrap();
    assert_ne!(old_view.snapshot_id(), new_view.snapshot_id());
    let old_state = SnapshotPathState::Present(SnapshotNodeIdentity::Regular {
        size: 3,
        content_digest: digest_of(b"old"),
    });
    assert_eq!(old_view.path_state("used/data").await.unwrap(), old_state);
    assert_ne!(new_view.path_state("used/data").await.unwrap(), old_state);
    assert_eq!(old_view.path_state("used/data").await.unwrap(), old_state);
}

#[tokio::test]
async fn cached_namespace_still_checks_advertised_negative_path_limits() {
    let mut fixture = Fixture::new();
    fixture.path_limit = Some(12);
    let server = Server::start(fixture).await;
    let view = Mst2Fuse::from_reader_lazy(server.reader().await, None)
        .await
        .unwrap();
    assert_eq!(
        view.path_state("absent").await.unwrap(),
        SnapshotPathState::AbsentProven
    );
    assert_eq!(
        view.path_state("neverexistsxx").await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(server.paths(), ["/"]);
    let mut fixture = Fixture::new();
    fixture.component_limit = Some(1);
    let server = Server::start(fixture).await;
    let view = Mst2Fuse::from_reader_lazy(server.reader().await, None)
        .await
        .unwrap();
    assert_eq!(
        view.path_state("absent/child").await.unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(server.paths(), ["/"]);
}

#[cfg(unix)]
fn upper_file(path: &std::path::Path, bytes: &[u8], mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, bytes).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[cfg(unix)]
fn upper_directory(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[cfg(unix)]
fn upper_tempdir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    temp
}

#[cfg(unix)]
#[tokio::test]
async fn bounded_upper_diff_compares_modes_empty_directories_and_symlink_target_bytes() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    use scorpiofs::{
        snapshot::upper_diff::{scan_upper, DiffLimits, UpperChangeKind},
        util::mutation_fence::MutationFence,
    };
    let server = Server::start(Fixture::new()).await;
    let view = Arc::new(
        Mst2Fuse::from_reader_lazy(server.reader().await, None)
            .await
            .unwrap(),
    );
    let temp = upper_tempdir();
    let fence = MutationFence::new(4);
    let pause = fence.pause().await.unwrap();
    assert!(scan_upper(
        &view,
        &temp.path().canonicalize().unwrap(),
        &pause,
        DiffLimits::default()
    )
    .await
    .unwrap()
    .is_clean());
    upper_file(&temp.path().join("plain"), b"plain", 0o644);
    upper_directory(&temp.path().join("empty"));
    symlink("used", temp.path().join("sym")).unwrap();
    let diff = scan_upper(
        &view,
        &temp.path().canonicalize().unwrap(),
        &pause,
        DiffLimits::default(),
    )
    .await
    .unwrap();
    assert!(diff.is_clean());
    assert_eq!(diff.meters.hash_bytes, 9);
    std::fs::set_permissions(
        temp.path().join("plain"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    upper_directory(&temp.path().join("new-empty"));
    symlink("/missing-outside-target", temp.path().join("outside-link")).unwrap();
    let diff = scan_upper(
        &view,
        &temp.path().canonicalize().unwrap(),
        &pause,
        DiffLimits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        diff.changes
            .iter()
            .map(|change| (change.rel_path.as_str(), change.kind))
            .collect::<Vec<_>>(),
        [
            ("new-empty", UpperChangeKind::Added),
            ("outside-link", UpperChangeKind::Added),
            ("plain", UpperChangeKind::Modified)
        ]
    );
    assert!(!server.paths().iter().any(|path| path == "/unused"));
}

#[cfg(unix)]
#[tokio::test]
async fn upper_whiteouts_include_directories_and_opaque_ancestors_hide_nested_lower_children() {
    use scorpiofs::{
        snapshot::upper_diff::{scan_upper, DiffLimits, UpperChangeKind},
        util::mutation_fence::MutationFence,
    };
    let server = Server::start(Fixture::new()).await;
    let view = Arc::new(
        Mst2Fuse::from_reader_lazy(server.reader().await, None)
            .await
            .unwrap(),
    );
    let temp = upper_tempdir();
    let fence = MutationFence::new(4);
    let pause = fence.pause().await.unwrap();
    upper_file(&temp.path().join(".wh.plain"), b"", 0o644);
    upper_file(&temp.path().join(".wh.unused"), b"", 0o644);
    let diff = scan_upper(
        &view,
        &temp.path().canonicalize().unwrap(),
        &pause,
        DiffLimits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        diff.changes
            .iter()
            .map(|change| (change.rel_path.as_str(), change.kind))
            .collect::<Vec<_>>(),
        [
            ("plain", UpperChangeKind::Deleted),
            ("unused", UpperChangeKind::Deleted)
        ]
    );
    assert!(matches!(
        diff.changes[1].base,
        Some(SnapshotNodeIdentity::Directory { .. })
    ));
    assert_eq!(server.paths(), ["/"]);
    let temp = upper_tempdir();
    upper_file(&temp.path().join(".wh..wh..opq"), b"", 0o644);
    upper_directory(&temp.path().join("used"));
    let diff = scan_upper(
        &view,
        &temp.path().canonicalize().unwrap(),
        &pause,
        DiffLimits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        diff.changes
            .iter()
            .map(|change| (change.rel_path.as_str(), change.kind))
            .collect::<Vec<_>>(),
        [
            ("empty", UpperChangeKind::Deleted),
            ("plain", UpperChangeKind::Deleted),
            ("sym", UpperChangeKind::Deleted),
            ("unused", UpperChangeKind::Deleted),
            ("used/data", UpperChangeKind::Deleted)
        ]
    );
    upper_file(&temp.path().join("used/data"), b"old", 0o644);
    let diff = scan_upper(
        &view,
        &temp.path().canonicalize().unwrap(),
        &pause,
        DiffLimits::default(),
    )
    .await
    .unwrap();
    assert!(!diff
        .changes
        .iter()
        .any(|change| change.rel_path == "used/data"));
    assert!(!server.paths().iter().any(|path| path == "/unused"));
}

#[cfg(unix)]
#[tokio::test]
async fn upper_directory_replacing_lower_file_derives_child_absence_and_streams_large_files() {
    use scorpiofs::{
        snapshot::upper_diff::{scan_upper, DiffLimits, UpperChangeKind},
        util::mutation_fence::MutationFence,
    };
    let server = Server::start(Fixture::new()).await;
    let view = Arc::new(
        Mst2Fuse::from_reader_lazy(server.reader().await, None)
            .await
            .unwrap(),
    );
    let temp = upper_tempdir();
    let fence = MutationFence::new(4);
    let pause = fence.pause().await.unwrap();
    upper_directory(&temp.path().join("plain"));
    let bytes = vec![0x41; 2 * 1024 * 1024 + 7];
    upper_file(&temp.path().join("plain/new"), &bytes, 0o644);
    let diff = scan_upper(
        &view,
        &temp.path().canonicalize().unwrap(),
        &pause,
        DiffLimits::default(),
    )
    .await
    .unwrap();
    assert_eq!(diff.meters.hash_bytes, bytes.len() as u64);
    assert_eq!(
        diff.changes
            .iter()
            .map(|change| (change.rel_path.as_str(), change.kind))
            .collect::<Vec<_>>(),
        [
            ("plain", UpperChangeKind::Modified),
            ("plain/new", UpperChangeKind::Added)
        ]
    );
    assert_eq!(
        diff.changes[1].upper.as_ref().unwrap(),
        &scorpiofs::snapshot::upper_diff::UpperNodeIdentity::Regular {
            mode: 0o644,
            uid: scorpiofs::util::mount_owner::mount_owner().uid,
            gid: scorpiofs::util::mount_owner::mount_owner().gid,
            size: bytes.len() as u64,
            content_digest: digest_of(&bytes)
        }
    );
    assert_eq!(server.paths(), ["/"]);
    assert_eq!(
        scan_upper(
            &view,
            &temp.path().canonicalize().unwrap(),
            &pause,
            DiffLimits {
                max_bytes: 64 * 1024,
                ..Default::default()
            }
        )
        .await
        .unwrap_err()
        .code,
        SnapshotErrorCode::ProofBudgetExceeded
    );
    assert_eq!(
        scan_upper(
            &view,
            &temp.path().canonicalize().unwrap(),
            &pause,
            DiffLimits {
                max_depth: 0,
                ..Default::default()
            }
        )
        .await
        .unwrap_err()
        .code,
        SnapshotErrorCode::ProofBudgetExceeded
    );
    assert_eq!(
        scan_upper(
            &view,
            &temp.path().canonicalize().unwrap(),
            &pause,
            DiffLimits {
                max_nodes: 1,
                ..Default::default()
            }
        )
        .await
        .unwrap_err()
        .code,
        SnapshotErrorCode::ProofBudgetExceeded
    );
}

#[cfg(unix)]
#[tokio::test]
async fn upper_scan_proof_errors_conflicting_markers_and_unsupported_nodes_cannot_be_clean() {
    use scorpiofs::{
        snapshot::upper_diff::{scan_upper, DiffLimits},
        util::mutation_fence::MutationFence,
    };
    let mut fixture = Fixture::new();
    fixture.fault = Some(("/used", 503, "OBJECT_UNAVAILABLE"));
    let server = Server::start(fixture).await;
    let view = Arc::new(
        Mst2Fuse::from_reader_lazy(server.reader().await, None)
            .await
            .unwrap(),
    );
    let temp = upper_tempdir();
    let fence = MutationFence::new(4);
    let pause = fence.pause().await.unwrap();
    upper_directory(&temp.path().join("used"));
    upper_file(&temp.path().join("used/data"), b"old", 0o644);
    assert_eq!(
        scan_upper(
            &view,
            &temp.path().canonicalize().unwrap(),
            &pause,
            DiffLimits::default()
        )
        .await
        .unwrap_err()
        .code,
        SnapshotErrorCode::ObjectUnavailable
    );
    let temp = upper_tempdir();
    upper_file(&temp.path().join(".wh.plain"), b"", 0o644);
    upper_file(&temp.path().join("plain"), b"plain", 0o644);
    assert!(scan_upper(
        &view,
        &temp.path().canonicalize().unwrap(),
        &pause,
        DiffLimits::default()
    )
    .await
    .is_err());
    let temp = upper_tempdir();
    upper_file(&temp.path().join(".wh..wh..opq"), b"invalid", 0o644);
    assert!(scan_upper(
        &view,
        &temp.path().canonicalize().unwrap(),
        &pause,
        DiffLimits::default()
    )
    .await
    .is_err());
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let temp = upper_tempdir();
        let fifo = std::ffi::CString::new(temp.path().join("fifo").as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert_eq!(
            scan_upper(
                &view,
                &temp.path().canonicalize().unwrap(),
                &pause,
                DiffLimits::default()
            )
            .await
            .unwrap_err()
            .code,
            SnapshotErrorCode::UnsupportedEntry
        );
    }
}

#[cfg(unix)]
fn set_test_xattr(path: &std::path::Path) {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    let name = c"user.scorpiofs-test";
    #[cfg(target_os = "linux")]
    let result =
        unsafe { libc::setxattr(path.as_ptr(), name.as_ptr(), b"state".as_ptr().cast(), 5, 0) };
    #[cfg(target_os = "macos")]
    let result = unsafe {
        libc::setxattr(
            path.as_ptr(),
            name.as_ptr(),
            b"state".as_ptr().cast(),
            5,
            0,
            0,
        )
    };
    assert_eq!(result, 0, "fixture xattr could not be set");
}

#[cfg(unix)]
#[tokio::test]
async fn root_chmod_and_xattr_only_edits_cannot_be_destroyed_as_clean() {
    use std::os::unix::fs::PermissionsExt;

    use scorpiofs::{
        snapshot::upper_diff::{scan_upper, DiffLimits, UpperChangeKind},
        util::mutation_fence::MutationFence,
    };
    let server = Server::start(Fixture::new()).await;
    let view = Arc::new(
        Mst2Fuse::from_reader_lazy(server.reader().await, None)
            .await
            .unwrap(),
    );
    let fence = MutationFence::new(4);
    let pause = fence.pause().await.unwrap();
    let temp = upper_tempdir();
    let path = temp.path().canonicalize().unwrap();
    assert!(scan_upper(&view, &path, &pause, DiffLimits::default())
        .await
        .unwrap()
        .is_clean());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let diff = scan_upper(&view, &path, &pause, DiffLimits::default())
        .await
        .unwrap();
    assert_eq!(diff.changes.len(), 1);
    assert_eq!(diff.changes[0].rel_path, "");
    assert_eq!(diff.changes[0].kind, UpperChangeKind::Modified);
    let temp = upper_tempdir();
    let path = temp.path().canonicalize().unwrap();
    upper_file(&path.join("plain"), b"plain", 0o644);
    assert!(scan_upper(&view, &path, &pause, DiffLimits::default())
        .await
        .unwrap()
        .is_clean());
    set_test_xattr(&path.join("plain"));
    assert_eq!(
        scan_upper(&view, &path, &pause, DiffLimits::default())
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::UnsupportedEntry
    );
    let temp = upper_tempdir();
    let path = temp.path().canonicalize().unwrap();
    set_test_xattr(&path);
    assert_eq!(
        scan_upper(&view, &path, &pause, DiffLimits::default())
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::UnsupportedEntry
    );
}
