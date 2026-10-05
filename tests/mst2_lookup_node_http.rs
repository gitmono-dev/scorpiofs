//! Found-node syntax and filesystem bounds over real HTTP. These fixtures
//! deliberately do not supply cryptographic lookup membership proofs.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{routing::post, Json, Router};
use scorpiofs::snapshot::{Mst2Client, SnapshotErrorCode};
use serde_json::{json, Value};

const SID: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn start(body: Value) -> Self {
        Self::serve(Router::new().route(
            "/api/v2/snapshots/{sid}/lookup",
            post(move || {
                let body = body.clone();
                async move { Json(body) }
            }),
        ))
        .await
    }
    async fn serve(app: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, task }
    }
    fn client(&self) -> Mst2Client {
        Mst2Client::new(&self.url)
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn file() -> Value {
    json!({"fs_kind":"regular","name":"a","size":"0","content_digest":SID})
}
fn directory() -> Value {
    json!({"fs_kind":"directory","directory_root":SID})
}
fn response(path: &str, node: Value) -> Value {
    json!({"snapshot_id":SID,"results":[{"path":path,"status":"found","node":node}]})
}

async fn reject(path: &str, node: Value, code: SnapshotErrorCode) {
    let server = Server::start(response(path, node)).await;
    let client = server.client();
    let error = client.lookup(SID, &[path.into()]).await.unwrap_err();
    assert_eq!(error.code, code);
    assert_eq!(error.http_status, 0);
    assert_eq!(client.retry_count(), 0);
}

#[tokio::test]
async fn valid_root_files_directory_classes_and_utf8_names_remain_accepted() {
    let mut cases = vec![("/".to_string(), directory())];
    for kind in ["regular", "executable", "symlink"] {
        let mut node = file();
        node["fs_kind"] = json!(kind);
        node["size"] = json!(if kind == "symlink" {
            "4095"
        } else {
            "8796093022208"
        });
        node["lifecycle"] = json!("immutable_release");
        cases.push(("/nested/a".into(), node));
    }
    for class in [
        "native_tree",
        "native_checkout_root",
        "import_root",
        "import_tree",
        "aggregate",
    ] {
        for lifecycle in ["mutable", "immutable_release"] {
            let mut node = directory();
            node["name"] = json!("a");
            node["node_class"] = json!(class);
            node["lifecycle"] = json!(lifecycle);
            cases.push(("/a".into(), node));
        }
    }
    let name = "猫".repeat(85);
    assert_eq!(name.len(), 255);
    let mut node = file();
    node["name"] = json!(name);
    cases.push((format!("/{name}"), node));
    for (path, node) in cases {
        let mut body = response(&path, node);
        // Navigation hints remain optional and cannot alter the checked node.
        body["ancestor_chain"] = json!([]);
        let server = Server::start(body).await;
        let got = server
            .client()
            .lookup(SID, std::slice::from_ref(&path))
            .await
            .unwrap();
        assert_eq!(got.results[0].path, path);
        assert_eq!(got.results[0].status, "found");
    }
}

#[tokio::test]
async fn wrong_names_kinds_and_conditional_fields_never_become_found_nodes() {
    let mut cases = Vec::new();
    let mut node = file();
    node.as_object_mut().unwrap().remove("name");
    cases.push(("/a".to_string(), node));
    for name in ["", "b", ".", "..", "a/b", "a\0b"] {
        let mut node = file();
        node["name"] = json!(name);
        cases.push(("/a".into(), node));
    }
    let mut node = file();
    let name = "猫".repeat(86);
    node["name"] = json!(name);
    cases.push((format!("/{name}"), node));
    cases.push(("/".into(), file()));
    let mut node = directory();
    node["name"] = json!("root");
    cases.push(("/".into(), node));
    for (field, value) in [
        ("fs_kind", json!("file")),
        ("content_digest", json!("sha256:AA")),
        ("directory_root", json!(SID)),
        ("node_class", json!("native_tree")),
        ("lifecycle", json!("deleted")),
    ] {
        let mut node = file();
        node[field] = value;
        cases.push(("/a".into(), node));
    }
    for field in ["size", "content_digest"] {
        let mut node = file();
        node.as_object_mut().unwrap().remove(field);
        cases.push(("/a".into(), node));
    }
    for (field, value) in [
        ("size", json!("0")),
        ("content_digest", json!(SID)),
        (
            "directory_root",
            json!(format!("sha256:{}", "00".repeat(32))),
        ),
        ("node_class", json!("unknown")),
    ] {
        let mut node = directory();
        node[field] = value;
        cases.push(("/".into(), node));
    }
    let mut node = directory();
    node.as_object_mut().unwrap().remove("directory_root");
    cases.push(("/".into(), node));
    for (path, node) in cases {
        reject(&path, node, SnapshotErrorCode::IntegrityError).await;
    }
}

#[tokio::test]
async fn counts_filesystem_caps_and_explicit_nulls_are_checked() {
    for size in ["", "00", "01", "+1", "-1", " 1", "1 ", "1.0"] {
        let mut node = file();
        node["size"] = json!(size);
        reject("/a", node, SnapshotErrorCode::ScopeInvalid).await;
    }
    for size in [
        "8796093022209",
        "9223372036854775808",
        "18446744073709551616",
    ] {
        let mut node = file();
        node["size"] = json!(size);
        reject("/a", node, SnapshotErrorCode::LimitExceeded).await;
    }
    for (size, code) in [
        ("0", SnapshotErrorCode::IntegrityError),
        ("4096", SnapshotErrorCode::LimitExceeded),
    ] {
        let mut node = file();
        node["fs_kind"] = json!("symlink");
        node["size"] = json!(size);
        reject("/a", node, code).await;
    }
    for field in [
        "name",
        "size",
        "content_digest",
        "directory_root",
        "node_class",
        "lifecycle",
    ] {
        let mut node = file();
        node[field] = Value::Null;
        reject("/a", node, SnapshotErrorCode::IntegrityError).await;
    }
    for status in ["found", "absent", "not_directory", "symlink_traversal"] {
        let server = Server::start(json!({"snapshot_id":SID,"results":[
            {"path":"/a","status":status,"node":null}
        ]}))
        .await;
        assert_eq!(
            server
                .client()
                .lookup(SID, &["/a".into()])
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::IntegrityError
        );
    }
}

#[tokio::test]
async fn path_count_limit_rejects_before_http_and_preserves_the_boundary() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let server = Server::serve(Router::new().route(
        "/api/v2/snapshots/{sid}/lookup",
        post(move |Json(body): Json<Value>| {
            observed.fetch_add(1, Ordering::SeqCst);
            let paths = body["paths"].as_array().unwrap();
            assert_eq!(paths.len(), 128);
            let results: Vec<_> = paths
                .iter()
                .map(|path| json!({"path":path,"status":"absent"}))
                .collect();
            async move { Json(json!({"snapshot_id":SID,"results":results})) }
        }),
    ))
    .await;
    let client = server.client();
    assert_eq!(
        client
            .lookup(SID, &vec!["/a".into(); 129])
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        client
            .lookup(SID, &vec!["/a".into(); 128])
            .await
            .unwrap()
            .results
            .len(),
        128
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(client.retry_count(), 0);
}
