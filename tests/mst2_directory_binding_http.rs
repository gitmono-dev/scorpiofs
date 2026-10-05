//! JSON enumeration checks syntax and continuity without claiming MTP2 proof verification.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{
    routing::{get, post},
    Json, Router,
};
use mst2_codec::descriptor::ServingDescriptor;
use scorpiofs::snapshot::{Mst2Client, SnapshotErrorCode, SnapshotReader};
use serde_json::{json, Value};

const INSTANCE: &str = "6ab219b0-4275-45ba-9d7b-7b0b633018cd";

fn digest(byte: u8) -> String {
    format!("sha256:{}", hex::encode([byte; 32]))
}
fn sid() -> String {
    let descriptor = ServingDescriptor {
        instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
        namespace_view_id: [0x22; 32],
        scope: "/project".into(),
        metadata_root: [0x33; 32],
    };
    format!("sha256:{}", hex::encode(descriptor.snapshot_id().unwrap()))
}
fn file(name: &str) -> Value {
    json!({"name":name,"fs_kind":"regular","size":"1","content_digest":digest(0x44)})
}
fn page(entries: Vec<Value>, count: &str, after: Option<&str>, next: Option<&str>) -> Value {
    json!({"snapshot_id":sid(),"path":"/","metadata_root":digest(0x33),
        "directory_root":digest(0x33),"node_class":"native_tree","lifecycle":"mutable",
        "range_start_exclusive":after,"entries":entries,"entry_count":count,"next_cursor":next,
        "proof_pages":[{"digest":digest(0x33),"data_base64":""}]})
}
struct Server {
    client: Mst2Client,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn start(pages: Vec<Value>) -> Self {
        Self::start_checked(pages, None).await
    }
    async fn start_checked(
        pages: Vec<Value>,
        requests: Option<Vec<(String, u32, Option<String>)>>,
    ) -> Self {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(|| async { Json(json!({
                "protocol_versions":[2],"metadata_codecs":[1],"frame_encodings":["identity"],
                "features":{"resolve":true,"directory":true,"leases":true}
            })) }))
            .route("/api/v2/snapshots/resolve", post(|| async { Json(json!({
                "descriptor":{"schema_version":2,"metadata_codec":1,"instance_id":INSTANCE,
                    "namespace_view_id":digest(0x22),"scope":"/project","materialization_policy":1,
                    "fs_semantics":1,"access_projection":0,"metadata_root":digest(0x33),"snapshot_id":sid()},
                "lease_id":"directory-test-lease","lease_expires_at":"2099-01-01T00:00:00Z",
                "publication_sequence":"1","authorization_epoch":"1"
            })) }))
            .route("/api/v2/snapshots/{sid}/directory", get(move |axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>| {
                let index = observed.fetch_add(1, Ordering::SeqCst);
                if let Some(requests) = &requests {
                    let (path, limit, cursor) = &requests[index];
                    assert_eq!(query.get("path"), Some(path));
                    assert_eq!(query.get("limit"), Some(&limit.to_string()));
                    assert_eq!(query.get("cursor"), cursor.as_ref());
                }
                let body = pages[index.min(pages.len()-1)].clone();
                async move { Json(body) }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            client,
            calls,
            task,
        }
    }
    async fn reader(&self) -> SnapshotReader {
        SnapshotReader::resolve(self.client.clone(), "/project", 60)
            .await
            .unwrap()
    }
}

// The backend cursor uses standard padded base64 over its JSON payload,
// followed by a dot and a 64-hex authentication tag. Authentication belongs
// to the server: this fixture accepts only its exact issued opaque token.
fn standard_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::new();
    for chunk in bytes.chunks(3) {
        let word = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for (index, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            encoded.push(if index > chunk.len() {
                '='
            } else {
                ALPHABET[((word >> shift) & 63) as usize] as char
            });
        }
    }
    encoded
}

#[tokio::test]
async fn escaped_legal_paths_preserve_server_shaped_opaque_cursors_over_http() {
    let dir = format!("/{}", vec!["\u{1}".repeat(255); 10].join("/"));
    // These controls are valid UTF-8 basenames under the serving profile.
    // JSON escaping expands the legal 2560-byte path before base64 encoding.
    mst2_codec::descriptor::validate_scope(&format!("/project{dir}")).unwrap();
    let payload = serde_json::to_vec(&json!({
        "s": sid(), "p": format!("/project{dir}"), "l": 1, "a": "a",
    }))
    .unwrap();
    let cursor = format!("{}.{}", standard_base64(&payload), hex::encode([0xa5; 32]));
    let mut first = page(vec![file("a")], "2", None, Some(&cursor));
    let mut second = page(vec![file("b")], "2", Some("a"), None);
    first["path"] = json!(dir);
    second["path"] = json!(dir);
    let server = Server::start_checked(
        vec![first, second],
        Some(vec![(dir.clone(), 1, None), (dir.clone(), 1, Some(cursor))]),
    )
    .await;
    let result = server.reader().await.directory_page(&dir, 1).await.unwrap();
    assert_eq!(result.path, dir);
    assert_eq!(
        result
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(result.entry_count, "2");
    assert!(result.next_cursor.is_none());
    assert_eq!(server.calls.load(Ordering::SeqCst), 2);
    assert_eq!(server.client.retry_count(), 0);
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn ordered_pages_empty_directory_and_large_metadata_files_remain_valid() {
    for manifest in [false, true] {
        let server = Server::start(vec![
            page(vec![file("a")], "2", None, Some("opaque-one")),
            page(vec![file("é")], "2", Some("a"), None),
        ])
        .await;
        let reader = server.reader().await;
        if manifest {
            let files = reader.file_manifest_directory().await.unwrap();
            assert_eq!(
                files
                    .iter()
                    .map(|f| f.rel_path.as_str())
                    .collect::<Vec<_>>(),
                ["a", "é"]
            );
        } else {
            assert_eq!(
                reader.directory_page("/", 1).await.unwrap().entries.len(),
                2
            );
        }
        assert_eq!(server.calls.load(Ordering::SeqCst), 2);
    }
    let server = Server::start(vec![page(vec![], "0", None, None)]).await;
    assert!(server
        .reader()
        .await
        .file_manifest_directory()
        .await
        .unwrap()
        .is_empty());
    let mut large = file("large");
    large["size"] = json!((8_u64 * 1024 * 1024 * 1024 * 1024).to_string());
    let server = Server::start(vec![page(vec![large], "1", None, None)]).await;
    assert_eq!(
        server
            .reader()
            .await
            .file_manifest_directory()
            .await
            .unwrap()[0]
            .size,
        8_u64 * 1024 * 1024 * 1024 * 1024
    );
}

#[tokio::test]
async fn recursive_directories_preserve_kinds_and_accept_legal_provenance_hints() {
    let child = json!({"name":"d", "fs_kind":"directory", "directory_root":digest(0x55),
        "node_class":"import_tree", "lifecycle":"immutable_release"});
    let mut root = page(vec![child], "1", None, None);
    root["ancestor_chain"] = json!([]);
    root["source_context"] = json!({"source_snapshot_id":digest(0x66), "binding_id":null,
        "source_relative_path":""});
    let mut executable = file("executable");
    executable["fs_kind"] = json!("executable");
    let mut symlink = file("link");
    symlink["fs_kind"] = json!("symlink");
    symlink["size"] = json!("4095");
    let mut subtree = page(vec![executable, symlink], "2", None, None);
    subtree["path"] = json!("/d");
    subtree["directory_root"] = json!(digest(0x55));
    let server = Server::start(vec![root.clone(), subtree.clone()]).await;
    let files = server
        .reader()
        .await
        .file_manifest_directory()
        .await
        .unwrap();
    assert_eq!(
        files
            .iter()
            .map(|file| (file.rel_path.as_str(), file.fs_kind.as_str()))
            .collect::<Vec<_>>(),
        [("d/executable", "executable"), ("d/link", "symlink")]
    );
    assert_eq!(server.calls.load(Ordering::SeqCst), 2);
    subtree["directory_root"] = json!(digest(0x77));
    let server = Server::start(vec![root, subtree]).await;
    assert_eq!(
        server
            .reader()
            .await
            .file_manifest_directory()
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(server.calls.load(Ordering::SeqCst), 2);
    for (size, code) in [
        ("0", SnapshotErrorCode::IntegrityError),
        ("4096", SnapshotErrorCode::LimitExceeded),
    ] {
        let mut entry = file("link");
        entry["fs_kind"] = json!("symlink");
        entry["size"] = json!(size);
        let server = Server::start(vec![page(vec![entry], "1", None, None)]).await;
        assert_eq!(
            server
                .reader()
                .await
                .file_manifest_directory()
                .await
                .unwrap_err()
                .code,
            code
        );
    }
}

#[tokio::test]
async fn fixed_identity_limits_order_and_conditional_entry_fields_are_checked() {
    let mut cases = Vec::new();
    for (field, bad) in [
        ("snapshot_id", json!(digest(0x55))),
        ("path", json!("/wrong")),
        ("directory_root", json!(digest(0))),
        ("metadata_root", json!("sha256:ABC")),
        ("entry_count", json!("0")),
        ("range_start_exclusive", json!("previous")),
        ("next_cursor", json!("")),
    ] {
        let mut body = page(vec![file("a")], "1", None, None);
        body[field] = bad;
        cases.push(body);
    }
    cases.push(page(vec![file("b"), file("a")], "2", None, None));
    cases.push(page(vec![file("a"), file("a")], "2", None, None));
    for (field, bad) in [
        ("name", json!("../a")),
        ("name", json!("..")),
        ("name", json!("a\0")),
        ("name", json!("é".repeat(128))),
        ("fs_kind", json!("fifo")),
        ("directory_root", json!(digest(0x33))),
        ("content_digest", json!("sha256:bad")),
        ("fs_kind", json!("directory")),
    ] {
        let mut body = page(vec![file("a")], "1", None, None);
        body["entries"][0][field] = bad;
        cases.push(body);
    }
    let mut body = page(vec![file("a")], "1", None, None);
    body["entries"][0].as_object_mut().unwrap().remove("size");
    cases.push(body);
    for body in cases {
        let server = Server::start(vec![body]).await;
        assert_eq!(
            server
                .client
                .directory(&sid(), "/", 256, None)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::IntegrityError
        );
    }
    let server = Server::start(vec![page(vec![file("a"), file("b")], "2", None, None)]).await;
    assert_eq!(
        server
            .client
            .directory(&sid(), "/", 1, None)
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    for limit in [0, 257] {
        let server = Server::start(vec![page(vec![], "0", None, None)]).await;
        assert_eq!(
            server
                .client
                .directory(&sid(), "/", limit, None)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::ScopeInvalid
        );
        assert_eq!(server.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn malformed_sizes_and_missing_nullable_fields_never_become_success() {
    for (size, code) in [
        ("01", SnapshotErrorCode::ScopeInvalid),
        ("+1", SnapshotErrorCode::ScopeInvalid),
        ("9223372036854775808", SnapshotErrorCode::LimitExceeded),
        ("8796093022209", SnapshotErrorCode::LimitExceeded),
    ] {
        let mut body = page(vec![file("a")], "1", None, None);
        body["entries"][0]["size"] = json!(size);
        let server = Server::start(vec![body]).await;
        assert_eq!(
            server
                .reader()
                .await
                .file_manifest_directory()
                .await
                .unwrap_err()
                .code,
            code
        );
    }
    for field in ["range_start_exclusive", "next_cursor"] {
        let mut body = page(vec![], "0", None, None);
        body.as_object_mut().unwrap().remove(field);
        let server = Server::start(vec![body]).await;
        assert!(server
            .client
            .directory(&sid(), "/", 256, None)
            .await
            .is_err());
    }
    let mut body = page(vec![file("a")], "1", None, None);
    body["entries"][0]["size"] = Value::Null;
    let server = Server::start(vec![body]).await;
    assert!(server
        .reader()
        .await
        .file_manifest_directory()
        .await
        .is_err());
}

#[tokio::test]
async fn pagination_cannot_change_roots_counts_boundaries_or_claim_early_eof() {
    let first = page(vec![file("a")], "2", None, Some("opaque-one"));
    let second = page(vec![file("b")], "2", Some("a"), None);
    let mut bad_pages = Vec::new();
    for (field, bad) in [
        ("metadata_root", json!(digest(0x66))),
        ("directory_root", json!(digest(0x77))),
        ("node_class", json!("import_tree")),
        ("lifecycle", json!("immutable_release")),
        ("entry_count", json!("3")),
        ("range_start_exclusive", json!("x")),
        ("next_cursor", json!("opaque-one")),
    ] {
        let mut body = second.clone();
        body[field] = bad;
        bad_pages.push(body);
    }
    bad_pages.push(page(vec![file("a")], "2", Some("a"), None));
    bad_pages.push(page(vec![], "2", Some("a"), Some("different")));
    bad_pages.push(page(vec![], "2", Some("a"), None));
    bad_pages.push(page(vec![file("b"), file("c")], "2", Some("a"), None));
    for bad in bad_pages {
        for manifest in [false, true] {
            let server = Server::start(vec![first.clone(), bad.clone()]).await;
            let reader = server.reader().await;
            let result = if manifest {
                reader.file_manifest_directory().await.map(|_| ())
            } else {
                reader.directory_page("/", 256).await.map(|_| ())
            };
            assert!(result.is_err());
            assert_eq!(server.calls.load(Ordering::SeqCst), 2);
            assert_eq!(server.client.retry_count(), 0);
        }
    }
    let server = Server::start(vec![page(vec![file("a")], "2", None, None)]).await;
    assert_eq!(
        server
            .reader()
            .await
            .file_manifest_directory()
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
}
