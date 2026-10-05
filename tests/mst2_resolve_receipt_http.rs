use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use mst2_codec::descriptor::ServingDescriptor;
use scorpiofs::snapshot::{Mst2Client, SnapshotErrorCode, SnapshotReader};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
    task::JoinHandle,
};

#[derive(Clone, Copy)]
enum Mode {
    Good,
    Missing,
    Wrong,
    Duplicate,
    Comma,
    Retry(usize),
    RetryMissing,
    WrongScope,
    Delay,
    Canonical,
    MalformedCanonical,
}

#[derive(Clone)]
struct Request {
    kind: &'static str,
    id: Option<String>,
    actor: Option<String>,
    lease: Option<String>,
    body: Value,
}

struct Fixture {
    mode: Mode,
    count: AtomicUsize,
    requests: Mutex<Vec<Request>>,
    pause: bool,
    entered: Notify,
    release: Notify,
}
impl Fixture {
    fn record(&self, kind: &'static str, headers: &HeaderMap, body: Value) {
        let header = |name| {
            headers
                .get(name)
                .map(|value| value.to_str().unwrap().into())
        };
        self.requests.lock().unwrap().push(Request {
            kind,
            id: header("x-request-id"),
            actor: header("authorization"),
            lease: header("x-mega-snapshot-lease"),
            body,
        });
    }
}

fn caps() -> Value {
    json!({"protocol_versions":[2],"metadata_codecs":[1],"frame_encodings":["identity"],"features":{"resolve":true,"directory":true,"leases":true}})
}
fn resolved(scope: &str, sequence: usize) -> Value {
    let instance = "11111111-2222-4333-8444-555555555556";
    let descriptor = ServingDescriptor {
        instance_uuid: *uuid::Uuid::parse_str(instance).unwrap().as_bytes(),
        namespace_view_id: [sequence as u8; 32],
        scope: scope.into(),
        metadata_root: [1; 32],
    };
    json!({"descriptor":{"schema_version":2,"metadata_codec":1,"instance_id":instance,"namespace_view_id":format!("sha256:{}",hex::encode(descriptor.namespace_view_id)),"scope":scope,"materialization_policy":1,"fs_semantics":1,"access_projection":0,"metadata_root":format!("sha256:{}",hex::encode(descriptor.metadata_root)),"snapshot_id":format!("sha256:{}",hex::encode(descriptor.snapshot_id().unwrap()))},"publication_sequence":sequence.to_string(),"authorization_epoch":"1","lease_id":format!("lease-{sequence}"),"lease_expires_at":"2099-01-01T00:00:00Z"})
}
async fn capabilities(State(f): State<Arc<Fixture>>, headers: HeaderMap) -> Json<Value> {
    f.record("capabilities", &headers, Value::Null);
    Json(caps())
}
async fn resolve(
    State(f): State<Arc<Fixture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    f.record("resolve", &headers, body.clone());
    let sequence = f.count.fetch_add(1, Ordering::SeqCst) + 1;
    if f.pause && sequence == 1 {
        f.entered.notify_one();
        f.release.notified().await;
    }
    if matches!(f.mode, Mode::Delay) {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let retry =
        matches!(f.mode,Mode::Retry(n) if sequence<=n) || matches!(f.mode, Mode::RetryMissing);
    let mut response = if retry {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":{"code":"TEMPORARY_UNAVAILABLE","message":"fixture retry"}})),
        )
            .into_response()
    } else {
        let scope = if matches!(f.mode, Mode::WrongScope) {
            "/different"
        } else {
            body["scope"].as_str().unwrap()
        };
        let mut value = resolved(scope, sequence);
        if matches!(f.mode, Mode::Canonical | Mode::MalformedCanonical) {
            value["writer_epoch"] = json!("1");
            value["resolved_at"] = json!("2026-10-05T00:00:00Z");
            value["delivery"] = if matches!(f.mode, Mode::Canonical) {
                json!("full")
            } else {
                Value::Null
            };
        }
        Json(value).into_response()
    };
    if !matches!(f.mode, Mode::Missing | Mode::RetryMissing) {
        let id = headers
            .get("x-request-id")
            .cloned()
            .unwrap_or(HeaderValue::from_static("default-server-id"));
        let echo = match f.mode {
            Mode::Wrong => HeaderValue::from_static("wrong-id"),
            Mode::Comma => {
                HeaderValue::from_str(&format!("{}, other", id.to_str().unwrap())).unwrap()
            }
            _ => id.clone(),
        };
        response.headers_mut().insert("x-request-id", echo);
        if matches!(f.mode, Mode::Duplicate) {
            response.headers_mut().append("x-request-id", id);
        }
    }
    response
}
struct Server {
    base: String,
    fixture: Arc<Fixture>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(mode: Mode, pause: bool) -> Self {
        let fixture = Arc::new(Fixture {
            mode,
            count: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            pause,
            entered: Notify::new(),
            release: Notify::new(),
        });
        let app = Router::new()
            .route("/api/v2/snapshots/capabilities", get(capabilities))
            .route("/api/v2/snapshots/resolve", post(resolve))
            .with_state(fixture.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            base,
            fixture,
            task,
        }
    }
}

#[tokio::test]
async fn actual_reader_receipt_binds_its_one_resolve_and_does_not_reach_other_requests() {
    let server = Server::start(Mode::Good, false).await;
    let client = Mst2Client::with_token(&server.base, Some("private-actor".into()));
    client.bind_lease("previous-lease-must-not-reach-resolve");
    let (reader, receipt) =
        SnapshotReader::resolve_observed(client, "/project", 60, "mst2:run:r1:v1:resolve")
            .await
            .unwrap();
    assert_eq!(receipt.logical_request_id(), "mst2:run:r1:v1:resolve");
    assert_eq!(receipt.attempt_ids(), ["mst2:run:r1:v1:resolve:a1"]);
    assert_eq!(receipt.final_attempt_id(), receipt.attempt_ids()[0]);
    assert_eq!(receipt.retry_count(), 0);
    assert_eq!(
        reader.snapshot_id(),
        resolved("/project", 1)["descriptor"]["snapshot_id"]
    );
    reader.client().capabilities().await.unwrap();
    let requests = server.fixture.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].kind, "capabilities");
    assert!(requests[0].id.is_none());
    assert_eq!(requests[1].kind, "resolve");
    assert_eq!(requests[1].id.as_deref(), Some(receipt.final_attempt_id()));
    assert!(requests[1].lease.is_none());
    assert_eq!(requests[2].kind, "capabilities");
    assert!(requests[2].id.is_none());
    assert_eq!(requests[2].lease.as_deref(), Some(reader.lease_id()));
    assert!(requests
        .iter()
        .all(|request| request.actor.as_deref() == Some("Bearer private-actor")));
    let wire = serde_json::to_value(&receipt).unwrap();
    assert_eq!(wire.as_object().unwrap().len(), 4);
    assert!(!wire.to_string().contains("private-actor"));
    assert!(!wire.to_string().contains(reader.lease_id()));
}

#[tokio::test]
async fn malformed_or_missing_echo_is_terminal_before_a_status_retry() {
    for mode in [
        Mode::Missing,
        Mode::Wrong,
        Mode::Duplicate,
        Mode::Comma,
        Mode::RetryMissing,
    ] {
        let server = Server::start(mode, false).await;
        let client = Mst2Client::new(&server.base);
        let error = client
            .resolve_observed("/project", 60, "echo-case")
            .await
            .unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
        assert_eq!(server.fixture.count.load(Ordering::SeqCst), 1);
        assert_eq!(client.retry_count(), 0);
        assert!(!error.message.contains("echo-case"));
    }
}

#[tokio::test]
async fn actual_status_retries_keep_body_actor_and_deadline_but_use_distinct_attempt_ids() {
    let server = Server::start(Mode::Retry(3), false).await;
    let client = Mst2Client::with_token(&server.base, Some("fixed-actor".into()));
    let (reader, receipt) =
        SnapshotReader::resolve_observed(client.clone(), "/project", 60, "retry:logical")
            .await
            .unwrap();
    assert_eq!(
        receipt.attempt_ids(),
        [
            "retry:logical:a1",
            "retry:logical:a2",
            "retry:logical:a3",
            "retry:logical:a4"
        ]
    );
    assert_eq!(receipt.final_attempt_id(), "retry:logical:a4");
    assert_eq!(receipt.retry_count(), 3);
    assert_eq!(client.retry_count(), 3);
    assert_eq!(reader.authorized_context().publication_sequence(), 4);
    let requests = server.fixture.requests.lock().unwrap();
    let actual: Vec<_> = requests
        .iter()
        .filter(|request| request.kind == "resolve")
        .collect();
    assert_eq!(actual.len(), 4);
    for (request, id) in actual.iter().zip(receipt.attempt_ids()) {
        assert_eq!(request.id.as_ref(), Some(id));
        assert_eq!(request.body, actual[0].body);
        assert_eq!(request.actor.as_deref(), Some("Bearer fixed-actor"));
        assert!(request.lease.is_none());
    }
}

#[tokio::test]
async fn trace_id_preflight_rejects_before_capabilities_and_accepts_the_exact_header_limit() {
    let server = Server::start(Mode::Good, false).await;
    for id in [
        String::new(),
        "x".repeat(126),
        "has space".into(),
        "newline\n".into(),
        "中文".into(),
    ] {
        let error =
            SnapshotReader::resolve_observed(Mst2Client::new(&server.base), "/project", 60, &id)
                .await
                .err()
                .unwrap();
        assert_eq!(error.code, SnapshotErrorCode::InvalidRequest);
    }
    assert!(server.fixture.requests.lock().unwrap().is_empty());
    let (_, receipt) = SnapshotReader::resolve_observed(
        Mst2Client::new(&server.base),
        "/project",
        60,
        &"x".repeat(125),
    )
    .await
    .unwrap();
    assert_eq!(receipt.final_attempt_id().len(), 128);
}

#[tokio::test]
async fn concurrent_readers_and_actor_rotation_do_not_share_a_mutable_last_request_id() {
    let server = Server::start(Mode::Good, true).await;
    let client = Mst2Client::with_token(&server.base, Some("old-actor".into()));
    let first_client = client.clone();
    let first = tokio::spawn(async move {
        SnapshotReader::resolve_observed(first_client, "/first", 60, "first-operation")
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), server.fixture.entered.notified())
        .await
        .unwrap();
    client.set_token(Some("new-actor".into()));
    let (second, second_receipt) =
        SnapshotReader::resolve_observed(client, "/second", 60, "second-operation")
            .await
            .unwrap();
    server.fixture.release.notify_one();
    let (first, first_receipt) = first.await.unwrap();
    first.client().capabilities().await.unwrap();
    second.client().capabilities().await.unwrap();
    assert_eq!(first_receipt.final_attempt_id(), "first-operation:a1");
    assert_eq!(second_receipt.final_attempt_id(), "second-operation:a1");
    assert_eq!(first.descriptor().scope, "/first");
    assert_eq!(second.descriptor().scope, "/second");
    let requests = server.fixture.requests.lock().unwrap();
    for (id, actor) in [
        (first_receipt.final_attempt_id(), "Bearer old-actor"),
        (second_receipt.final_attempt_id(), "Bearer new-actor"),
    ] {
        let request = requests
            .iter()
            .find(|request| request.id.as_deref() == Some(id))
            .unwrap();
        assert_eq!(request.actor.as_deref(), Some(actor));
        assert!(request.lease.is_none());
    }
    let later: Vec<_> = requests
        .iter()
        .filter(|request| request.lease.is_some())
        .collect();
    assert_eq!(later.len(), 2);
    assert!(later.iter().all(|request| request.id.is_none()));
    assert_eq!(later[0].actor.as_deref(), Some("Bearer old-actor"));
    assert_eq!(later[1].actor.as_deref(), Some("Bearer new-actor"));
}

#[tokio::test]
async fn default_resolve_keeps_legacy_header_behavior_and_observed_response_still_needs_authority()
{
    let server = Server::start(Mode::Missing, false).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.base), "/project", 60)
        .await
        .unwrap();
    assert_eq!(reader.descriptor().scope, "/project");
    assert!(server
        .fixture
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|request| request.id.is_none()));
    let server = Server::start(Mode::WrongScope, false).await;
    let error = SnapshotReader::resolve_observed(
        Mst2Client::new(&server.base),
        "/project",
        60,
        "wrong-scope",
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.code, SnapshotErrorCode::ScopeForbidden);
    assert_eq!(server.fixture.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn observed_attempt_response_wait_uses_the_original_request_deadline() {
    let server = Server::start(Mode::Delay, false).await;
    let client = Mst2Client::new(&server.base).with_request_timeout(Duration::from_millis(30));
    let started = tokio::time::Instant::now();
    let error = client
        .resolve_observed("/project", 60, "deadline")
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::TemporaryUnavailable);
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(server.fixture.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn observed_resolve_uses_checked_canonical_parser_without_fallback() {
    let server = Server::start(Mode::Canonical, false).await;
    let (reader, receipt) = SnapshotReader::resolve_observed(
        Mst2Client::new(&server.base),
        "/project",
        60,
        "canonical",
    )
    .await
    .unwrap();
    assert_eq!(
        reader.snapshot_id(),
        resolved("/project", 1)["descriptor"]["snapshot_id"]
    );
    assert_eq!(receipt.final_attempt_id(), "canonical:a1");
    let server = Server::start(Mode::MalformedCanonical, false).await;
    let error = Mst2Client::new(&server.base)
        .resolve_observed("/project", 60, "malformed")
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
    assert_eq!(server.fixture.count.load(Ordering::SeqCst), 1);
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let len = stream.read(&mut chunk).await.unwrap();
        assert!(len > 0);
        bytes.extend_from_slice(&chunk[..len]);
        assert!(bytes.len() <= 8192);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length: ")
                        .map(|value| value.parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if bytes.len() >= end + 4 + length {
                return bytes;
            }
        }
    }
}
async fn send_json(stream: &mut tokio::net::TcpStream, body: Value, id: Option<&str>) {
    let body = body.to_string();
    let id = id
        .map(|id| format!("X-Request-Id: {id}\r\n"))
        .unwrap_or_default();
    let response=format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{id}\r\n{body}",body.len());
    stream.write_all(response.as_bytes()).await.unwrap();
    stream.shutdown().await.unwrap();
}

#[tokio::test]
async fn response_lost_after_server_success_does_not_label_the_next_snapshot_with_the_old_attempt()
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut caps_stream, _) = listener.accept().await.unwrap();
        let request = String::from_utf8(read_request(&mut caps_stream).await).unwrap();
        assert!(request.starts_with("GET /api/v2/snapshots/capabilities "));
        assert!(!request.to_ascii_lowercase().contains("x-request-id:"));
        send_json(&mut caps_stream, caps(), None).await;
        let mut successes = Vec::new();
        for sequence in 1..=2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = String::from_utf8(read_request(&mut stream).await).unwrap();
            assert!(request.starts_with("POST /api/v2/snapshots/resolve "));
            let id = request
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("x-request-id: ")
                        .map(str::to_owned)
                })
                .unwrap();
            let body = resolved("/project", sequence);
            successes.push((id.clone(), body.clone()));
            if sequence == 1 {
                drop(stream);
            } else {
                send_json(&mut stream, body, Some(&id)).await;
            }
        }
        successes
    });
    let client = Mst2Client::new(base);
    let (reader, receipt) =
        SnapshotReader::resolve_observed(client.clone(), "/project", 60, "lost-response")
            .await
            .unwrap();
    let successes = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.attempt_ids(),
        ["lost-response:a1", "lost-response:a2"]
    );
    assert_eq!(receipt.final_attempt_id(), successes[1].0);
    assert_eq!(
        reader.snapshot_id(),
        successes[1].1["descriptor"]["snapshot_id"]
    );
    assert_ne!(
        reader.snapshot_id(),
        successes[0].1["descriptor"]["snapshot_id"]
    );
    assert_eq!(client.retry_count(), 1);
    assert_eq!(reader.authorized_context().publication_sequence(), 2);
}
