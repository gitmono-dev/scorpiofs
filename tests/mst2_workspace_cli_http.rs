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
        assert_eq!(output.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&output.stderr).contains("unrecognized subcommand"));
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

fn config_cli(path: &std::path::Path) -> std::process::Command {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_scorpio"));
    command
        .env_clear()
        .envs(std::env::vars_os().filter(|(key, _)| !key.to_string_lossy().starts_with("SCORPIO_")))
        .arg("--config-path")
        .arg(path);
    command
}

#[test]
fn shipped_v3_config_precedence_redaction_and_paths_are_read_only() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("scorpio.toml");
    let store = temp.path().join("not-created/store");
    let legacy = temp.path().join("legacy-state.toml");
    std::fs::write(&legacy, b"invalid legacy TOML").unwrap();
    let mut config = toml::Table::new();
    config.insert("store_path".into(), store.to_str().unwrap().into());
    config.insert("mst2_base_url".into(), "https://file.example".into());
    config.insert("mst2_auth_token".into(), "private-fixture-token".into());
    config.insert("config_file".into(), legacy.to_str().unwrap().into());
    config.insert("base_url".into(), "https://retired.example".into());
    let input = toml::to_string(&config).unwrap();
    std::fs::write(&path, &input).unwrap();

    for (environment, argument, expected) in [
        (None, None, "https://file.example"),
        (Some("https://env.example"), None, "https://env.example"),
        (
            Some("https://env.example"),
            Some("https://cli.example"),
            "https://cli.example",
        ),
    ] {
        let mut command = config_cli(&path);
        if let Some(value) = environment {
            command.env("SCORPIO_MST2_BASE_URL", value);
        }
        if let Some(value) = argument {
            command.args(["--mst2-base-url", value]);
        }
        let output = command.args(["config", "show"]).output().unwrap();
        assert!(output.status.success(), "{output:?}");
        let output = String::from_utf8(output.stdout).unwrap();
        assert!(output.contains(expected), "{output}");
        assert!(output.contains("<redacted>"));
        assert!(!output.contains("private-fixture-token"));
        assert!(!output.contains("https://retired.example"));
    }
    let output = config_cli(&path)
        .args(["config", "installer-paths"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let paths: Vec<_> = output.stdout.split(|byte| *byte == 0).collect();
    assert_eq!(paths.len(), 4);
    assert_eq!(paths[0], store.to_str().unwrap().as_bytes());
    assert_eq!(
        paths[1],
        store.join("workspaces-v3").to_str().unwrap().as_bytes()
    );
    assert_eq!(
        paths[2],
        store.join("mst2-cache").to_str().unwrap().as_bytes()
    );
    assert!(paths[3].is_empty());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), input);
    assert_eq!(std::fs::read(&legacy).unwrap(), b"invalid legacy TOML");
    assert!(!store.parent().unwrap().exists());
}

#[test]
fn shipped_config_template_has_only_live_fields_and_validation_is_offline() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("scorpio.toml");
    let output = config_cli(&path)
        .arg("config")
        .arg("init")
        .arg(&path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let input = std::fs::read_to_string(&path).unwrap();
    let table: toml::Table = toml::from_str(&input).unwrap();
    assert_eq!(table.len(), 4);
    for key in [
        "store_path",
        "mst2_base_url",
        "mst2_auth_token",
        "log_level",
    ] {
        assert!(table.contains_key(key));
    }
    let output = config_cli(&path)
        .args(["--mst2-base-url", "file:///invalid", "config", "validate"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("mst2_base_url"));
    assert_eq!(std::fs::read_to_string(path).unwrap(), input);
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[test]
fn observation_flags_and_output_failures_preserve_existing_files_and_uncreated_storage() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let config = root.join("scorpio.toml");
    let store = root.join("absent-store");
    let mut table = toml::Table::new();
    table.insert("store_path".into(), store.to_str().unwrap().into());
    std::fs::write(&config, toml::to_string(&table).unwrap()).unwrap();
    let output = root.join("observations.jsonl");
    let existing = root.join("existing.jsonl");
    std::fs::write(&existing, b"previous evidence\n").unwrap();
    let foreign = tempfile::tempdir().unwrap();
    symlink(foreign.path(), root.join("linked-parent")).unwrap();

    for args in [
        vec!["--workspace-observation-jsonl", output.to_str().unwrap()],
        vec!["--workspace-observation-run-id", ID],
        vec![
            "--workspace-observation-jsonl",
            output.to_str().unwrap(),
            "--workspace-observation-run-id",
            "00000000-0000-0000-0000-000000000000",
        ],
        vec![
            "--workspace-observation-jsonl",
            existing.to_str().unwrap(),
            "--workspace-observation-run-id",
            ID,
        ],
        vec![
            "--workspace-observation-jsonl",
            "relative-observations.jsonl",
            "--workspace-observation-run-id",
            ID,
        ],
    ] {
        let result = config_cli(&config)
            .args(["serve", "--http-addr", "127.0.0.1:0"])
            .args(args)
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(2), "{result:?}");
        assert!(!store.exists());
        assert!(!output.exists());
        assert_eq!(std::fs::read(&existing).unwrap(), b"previous evidence\n");
    }
    let linked = root.join("linked-parent/foreign.jsonl");
    let result = config_cli(&config)
        .args([
            "serve",
            "--http-addr",
            "127.0.0.1:0",
            "--workspace-observation-jsonl",
        ])
        .arg(&linked)
        .args(["--workspace-observation-run-id", ID])
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(2), "{result:?}");
    assert!(!foreign.path().join("foreign.jsonl").exists());
    assert!(!store.exists());

    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let result = config_cli(&config)
        .args(["serve", "--http-addr"])
        .arg(occupied.local_addr().unwrap().to_string())
        .arg("--workspace-observation-jsonl")
        .arg(&output)
        .args(["--workspace-observation-run-id", ID])
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(4), "{result:?}");
    assert!(!output.exists());
    assert!(!store.exists());
}

#[test]
fn failed_workspace_initialization_writes_an_incomplete_observation_footer() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let store = root.join("store-is-a-file");
    std::fs::write(&store, b"preserved store blocker").unwrap();
    let config = root.join("scorpio.toml");
    let mut table = toml::Table::new();
    table.insert("store_path".into(), store.to_str().unwrap().into());
    std::fs::write(&config, toml::to_string(&table).unwrap()).unwrap();
    let output = root.join("observations.jsonl");
    let result = config_cli(&config)
        .args([
            "serve",
            "--http-addr",
            "127.0.0.1:0",
            "--workspace-observation-jsonl",
        ])
        .arg(&output)
        .args(["--workspace-observation-run-id", ID])
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(2), "{result:?}");
    let footer: Value = serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
    assert_eq!(footer["record"], "workspace_observation_footer");
    assert_eq!(footer["run_id"], ID);
    assert_eq!(footer["daemon_exit_code"], 2);
    assert_eq!(footer["complete"], false);
    assert_eq!(footer["producers_closed"], true);
    assert_eq!(footer["drained"], true);
    assert_eq!(footer["accepted_records"], 0);
    assert_eq!(footer["received_records"], 0);
    assert_eq!(footer["written_records"], 0);
    assert_eq!(std::fs::read(store).unwrap(), b"preserved store blocker");
}
