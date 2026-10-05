//! Ordered lookup results must belong to the exact requested fixed snapshot.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use axum::{http::StatusCode, routing::post, Json, Router};
use scorpiofs::snapshot::{Mst2Client, SnapshotErrorCode};
use serde_json::{json, Value};

const SID: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(response: Value, status: StatusCode) -> Self {
        let app = Router::new().route(
            "/api/v2/snapshots/{sid}/lookup",
            post(move || {
                let response = response.clone();
                async move { (status, Json(response)) }
            }),
        );
        Self::serve(app).await
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

fn valid() -> Value {
    json!({"snapshot_id": SID, "results": [
        {"path": "/a", "status": "found", "node": {"fs_kind": "regular", "name": "a", "size": "1", "content_digest": SID}},
        {"path": "/b", "status": "absent"},
    ]})
}

#[tokio::test]
async fn forged_snapshot_dropped_extra_reordered_and_misbound_results_are_errors() {
    let mut cases = Vec::new();
    let mut body = valid();
    body["snapshot_id"] =
        json!("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    cases.push(body);
    let mut body = valid();
    body["results"].as_array_mut().unwrap().pop();
    cases.push(body);
    let mut body = valid();
    body["results"]
        .as_array_mut()
        .unwrap()
        .push(json!({"path":"/extra", "status":"absent"}));
    cases.push(body);
    let mut body = valid();
    body["results"].as_array_mut().unwrap().swap(0, 1);
    cases.push(body);
    for wrong_path in ["/a", "/elsewhere"] {
        let mut body = valid();
        body["results"][1]["path"] = json!(wrong_path);
        cases.push(body);
    }
    let mut body = valid();
    body["results"][0].as_object_mut().unwrap().remove("node");
    cases.push(body);
    let mut body = valid();
    body["results"][1]["node"] = json!({"fs_kind":"directory"});
    cases.push(body);
    for status in ["error", "FOUND", "storage_failed"] {
        let mut body = valid();
        body["results"][1]["status"] = json!(status);
        cases.push(body);
    }
    for body in cases {
        let server = Server::start(body, StatusCode::OK).await;
        let error = server
            .client()
            .lookup(SID, &["/a".into(), "/b".into()])
            .await
            .unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
        assert_eq!(error.http_status, 0);
    }
}

#[tokio::test]
async fn legitimate_ordered_statuses_duplicate_input_and_empty_lookup_are_preserved() {
    let server = Server::start(valid(), StatusCode::OK).await;
    let response = server
        .client()
        .lookup(SID, &["/a".into(), "/b".into()])
        .await
        .unwrap();
    assert_eq!(response.results[0].status, "found");
    assert_eq!(response.results[1].status, "absent");
    for status in ["not_directory", "symlink_traversal"] {
        let mut body = valid();
        body["results"][1]["status"] = json!(status);
        let server = Server::start(body, StatusCode::OK).await;
        assert_eq!(
            server
                .client()
                .lookup(SID, &["/a".into(), "/b".into()])
                .await
                .unwrap()
                .results[1]
                .status,
            status
        );
    }
    let mut body = valid();
    body["results"][1] = body["results"][0].clone();
    let server = Server::start(body, StatusCode::OK).await;
    assert_eq!(
        server
            .client()
            .lookup(SID, &["/a".into(), "/a".into()])
            .await
            .unwrap()
            .results
            .len(),
        2
    );
    let server = Server::start(json!({"snapshot_id":SID,"results":[]}), StatusCode::OK).await;
    assert!(server
        .client()
        .lookup(SID, &[])
        .await
        .unwrap()
        .results
        .is_empty());
}

#[tokio::test]
async fn failed_response_does_not_poison_later_lookup_or_change_typed_server_errors() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let app = Router::new().route(
        "/api/v2/snapshots/{sid}/lookup",
        post(move || {
            let mut body = valid();
            if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                body["results"] = json!([]);
            }
            async move { Json(body) }
        }),
    );
    let server = Server::serve(app).await;
    let client = server.client();
    let paths = ["/a".into(), "/b".into()];
    assert_eq!(
        client.lookup(SID, &paths).await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(client.lookup(SID, &paths).await.unwrap().results.len(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(client.retry_count(), 0);
    let server = Server::start(
        json!({"error":{"code":"SCOPE_FORBIDDEN","message":"denied"}}),
        StatusCode::FORBIDDEN,
    )
    .await;
    let error = server.client().lookup(SID, &paths).await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::ScopeForbidden);
    assert_eq!(error.http_status, 403);
}
