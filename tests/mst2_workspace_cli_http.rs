//! Exercise the shipped CLI over TCP without initializing local storage.
#![cfg(unix)]

use std::sync::{Arc, Mutex};

use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    Router,
};
use serde_json::{json, Value};

const ID: &str = "11111111-2222-4333-8444-555555555555";

#[derive(Clone, Debug)]
struct Call {
    method: String,
    path: String,
    body: Option<Value>,
}

type Calls = Arc<Mutex<Vec<Call>>>;

async fn record(State(calls): State<Calls>, request: Request<Body>) -> Response {
    let method = request.method().to_string();
    let path = request.uri().path().to_owned();
    let bytes = to_bytes(request.into_body(), 64 * 1024).await.unwrap();
    let body = (!bytes.is_empty()).then(|| serde_json::from_slice(&bytes).unwrap());
    calls.lock().unwrap().push(Call {
        method,
        path: path.clone(),
        body,
    });
    if path.ends_with("/destroy") {
        return StatusCode::NO_CONTENT.into_response();
    }
    axum::Json(json!({"workspace_id": ID, "metadata_ready": true})).into_response()
}

async fn cli(endpoint: &str, arguments: &[&str]) -> std::process::Output {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_scorpio"))
        // A workspace client does not need this file or create runtime dirs.
        .args([
            "--config-path",
            "/nonexistent-scorpio-cli-config",
            "workspace",
            "--endpoint",
            endpoint,
        ])
        .args(arguments)
        .output()
        .await
        .unwrap()
}

async fn server(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/daemon/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (endpoint, task)
}

#[tokio::test]
async fn shipped_cli_uses_canonical_v3_requests_and_preserves_destroy_policy() {
    let calls = Calls::default();
    let (endpoint, task) = server(Router::new().fallback(record).with_state(calls.clone())).await;
    let view = format!("sha256:{}", "ab".repeat(32));
    let commands = [
        vec!["create", "/project"],
        vec!["create", "/project", "--view-id", &view, "--full"],
        vec!["list"],
        vec!["status", ID],
        vec!["hydrate", ID],
        vec!["cancel-hydrate", ID],
        vec!["release-local-pin", ID],
        vec!["destroy", ID],
        vec!["destroy", ID, "--discard-dirty"],
    ];
    for command in commands {
        let output = cli(&endpoint, &command).await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if command[0] == "destroy" {
            assert!(output.stdout.is_empty());
        } else {
            let value: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(value["workspace_id"], ID);
        }
    }
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 9);
    let base = "/daemon/v3/workspaces";
    assert_eq!(calls[0].path, base);
    assert_eq!(calls[0].method, "POST");
    assert_eq!(
        calls[0].body,
        Some(json!({
            "target":{"kind":"latest"}, "scope":"/project",
            "delivery":"lazy", "upper_policy":"private"
        }))
    );
    assert_eq!(
        calls[1].body,
        Some(json!({
            "target":{"kind":"view", "view_id":view}, "scope":"/project",
            "delivery":"full", "upper_policy":"private"
        }))
    );
    for (index, suffix, method) in [
        (2, String::new(), "GET"),
        (3, format!("/{ID}"), "GET"),
        (4, format!("/{ID}/hydrate"), "POST"),
        (5, format!("/{ID}/hydrate/cancel"), "POST"),
        (6, format!("/{ID}/local-pin/release"), "POST"),
        (7, format!("/{ID}/destroy"), "POST"),
        (8, format!("/{ID}/destroy"), "POST"),
    ] {
        assert_eq!(calls[index].path, format!("{base}{suffix}"));
        assert_eq!(calls[index].method, method);
        if index < 7 {
            assert!(calls[index].body.is_none());
        }
    }
    assert_eq!(calls[7].body, Some(json!({"discard_dirty":false})));
    assert_eq!(calls[8].body, Some(json!({"discard_dirty":true})));
    task.abort();
}

#[tokio::test]
async fn invalid_identity_and_retired_commands_do_not_contact_the_daemon() {
    let calls = Calls::default();
    let (endpoint, task) = server(Router::new().fallback(record).with_state(calls.clone())).await;
    for command in [
        vec!["status", "../other"],
        vec!["create", "/project/../other"],
        vec!["create", "/project", "--view-id", "old-revision"],
        vec!["create", "/project", "--cl", "old-cl"],
    ] {
        assert!(!cli(&endpoint, &command).await.status.success());
    }
    for command in ["mount", "umount", "list", "http-mount"] {
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_scorpio"))
            .args(["--config-path", "/nonexistent-scorpio-cli-config", command])
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("unrecognized subcommand"));
    }
    for endpoint in [
        "file:///tmp/daemon",
        "http://user:pass@localhost",
        "http://localhost/?old=1",
    ] {
        assert!(!cli(endpoint, &["list"]).await.status.success());
    }
    assert!(calls.lock().unwrap().is_empty());
    task.abort();
}

#[tokio::test]
async fn failed_or_redirected_mutation_is_not_replayed() {
    for status in [StatusCode::CONFLICT, StatusCode::TEMPORARY_REDIRECT] {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = calls.clone();
        let router = Router::new().fallback(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                (
                    status,
                    [("location", "/other-owner")],
                    axum::Json(json!({"code":"WORKSPACE_DIRTY"})),
                )
            }
        });
        let (endpoint, task) = server(router).await;
        let output = cli(&endpoint, &["destroy", ID]).await;
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(status.as_str()));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        task.abort();
    }
}

#[tokio::test]
async fn malformed_and_oversized_responses_do_not_report_success() {
    for body in [b"not-json".to_vec(), vec![b' '; 4 * 1024 * 1024 + 1]] {
        let router = Router::new().fallback(move || {
            let body = body.clone();
            async move { Response::new(Body::from(body)) }
        });
        let (endpoint, task) = server(router).await;
        let output = cli(&endpoint, &["list"]).await;
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        task.abort();
    }
}
