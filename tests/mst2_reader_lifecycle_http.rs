//! Independent HTTP fixtures for actor/lease isolation and lease lifetime.
//! Requested lease seconds deliberately exceed the server's actual grant.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use mst2_codec::descriptor::ServingDescriptor;
use scorpiofs::snapshot::{Mst2Client, SnapshotErrorCode, SnapshotReader};
use serde_json::{json, Value};
use tokio::{sync::Notify, task::JoinHandle};

const INSTANCE_ID: &str = "11111111-2222-4333-8444-555555555556";
const NAMESPACE_VIEW_ID: [u8; 32] = [0x22; 32];
const METADATA_ROOT: [u8; 32] = [0x01; 32];

#[derive(Clone, Copy)]
enum Expiry {
    After(Duration),
    OffsetAfter(Duration, i32),
    Text(&'static str),
    Missing,
}

#[derive(Clone, Copy, Default)]
enum RenewalIdentity {
    #[default]
    Correct,
    WrongLease,
    WrongSnapshot,
}

#[derive(Debug, Clone)]
struct RequestRecord {
    kind: &'static str,
    id: String,
    actor: Option<String>,
    lease: Option<String>,
    requested_seconds: Option<u64>,
}

#[derive(Clone)]
struct Binding {
    snapshot: String,
    actor: Option<String>,
}

struct Fixture {
    initial_expiry: Expiry,
    renewal_expiry: Expiry,
    renewal_identity: Mutex<RenewalIdentity>,
    resolve_epoch: &'static str,
    renewal_epoch: Mutex<Option<Value>>,
    fail_renewal: AtomicBool,
    canonical_retry_error: Option<(&'static str, StatusCode)>,
    plain_retry_errors: bool,
    malformed_renewal_json: AtomicBool,
    pause_next_resolve: AtomicBool,
    pause_renewal: AtomicBool,
    resolve_count: AtomicUsize,
    renewal_count: AtomicUsize,
    requests: Mutex<Vec<RequestRecord>>,
    bindings: Mutex<HashMap<String, Binding>>,
    resolve_started: Notify,
    resolve_release: Notify,
    renewal_changed: Notify,
    renewal_release: Notify,
}

impl Default for Fixture {
    fn default() -> Self {
        Self {
            initial_expiry: Expiry::After(Duration::from_secs(30)),
            renewal_expiry: Expiry::After(Duration::from_secs(30)),
            renewal_identity: Mutex::new(RenewalIdentity::Correct),
            resolve_epoch: "1",
            renewal_epoch: Mutex::new(None),
            fail_renewal: AtomicBool::new(false),
            canonical_retry_error: None,
            plain_retry_errors: false,
            malformed_renewal_json: AtomicBool::new(false),
            pause_next_resolve: AtomicBool::new(false),
            pause_renewal: AtomicBool::new(false),
            resolve_count: AtomicUsize::new(0),
            renewal_count: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            bindings: Mutex::new(HashMap::new()),
            resolve_started: Notify::new(),
            resolve_release: Notify::new(),
            renewal_changed: Notify::new(),
            renewal_release: Notify::new(),
        }
    }
}

impl Fixture {
    fn record(&self, kind: &'static str, id: &str, headers: &HeaderMap, seconds: Option<u64>) {
        self.requests.lock().unwrap().push(RequestRecord {
            kind,
            id: id.to_string(),
            actor: header(headers, "authorization"),
            lease: header(headers, "x-mega-snapshot-lease"),
            requested_seconds: seconds,
        });
    }

    fn count(&self, kind: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.kind == kind)
            .count()
    }

    async fn wait_for_renewals(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let changed = self.renewal_changed.notified();
                if self.renewal_count.load(Ordering::SeqCst) >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("actual server grant did not trigger renewal");
    }
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).map(|v| v.to_str().unwrap().to_string())
}

// Independent calendar formatter: subtract whole calendar years/months from
// Unix days rather than reusing the production parser's civil-date algorithm.
fn timestamp_after(after: Duration) -> String {
    timestamp_after_offset(after, 0)
}

fn timestamp_after_offset(after: Duration, offset_seconds: i32) -> String {
    let at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + after;
    let at = if offset_seconds >= 0 {
        at + Duration::from_secs(offset_seconds as u64)
    } else {
        at - Duration::from_secs(offset_seconds.unsigned_abs() as u64)
    };
    let mut days = at.as_secs() / 86_400;
    let mut year = 1970;
    let leap = |year: u64| {
        year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
    };
    loop {
        let count = if leap(year) { 366 } else { 365 };
        if days < count {
            break;
        }
        days -= count;
        year += 1;
    }
    let months = [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0;
    while days >= months[month] {
        days -= months[month];
        month += 1;
    }
    let time = at.as_secs() % 86_400;
    let mut timestamp = format!(
        "{year:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}",
        month + 1,
        days + 1,
        time / 3600,
        time / 60 % 60,
        time % 60,
        at.subsec_nanos()
    );
    if offset_seconds == 0 {
        timestamp.push('Z');
    } else {
        let offset = offset_seconds.unsigned_abs();
        timestamp.push_str(&format!(
            "{}{:02}:{:02}",
            if offset_seconds > 0 { '+' } else { '-' },
            offset / 3600,
            offset / 60 % 60,
        ));
    }
    timestamp
}

fn add_expiry(response: &mut Value, expiry: Expiry) {
    match expiry {
        Expiry::After(after) => response["lease_expires_at"] = timestamp_after(after).into(),
        Expiry::OffsetAfter(after, offset) => {
            response["lease_expires_at"] = timestamp_after_offset(after, offset).into()
        }
        Expiry::Text(value) => response["lease_expires_at"] = value.into(),
        Expiry::Missing => {}
    }
}

async fn capabilities(State(f): State<Arc<Fixture>>, headers: HeaderMap) -> Json<Value> {
    f.record("capabilities", "", &headers, None);
    Json(json!({
        "protocol_versions": [2], "metadata_codecs": [1], "frame_encodings": ["identity"],
        "features": {"resolve": true, "directory": true, "leases": true, "lookup": true}
    }))
}

async fn resolve(
    State(f): State<Arc<Fixture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    let scope = body["scope"].as_str().unwrap();
    f.record("resolve", scope, &headers, body["lease_seconds"].as_u64());
    if f.pause_next_resolve.swap(false, Ordering::SeqCst) {
        f.resolve_started.notify_one();
        f.resolve_release.notified().await;
    }
    let sequence = f.resolve_count.fetch_add(1, Ordering::SeqCst) + 1;
    let descriptor = ServingDescriptor {
        instance_uuid: *uuid::Uuid::parse_str(INSTANCE_ID).unwrap().as_bytes(),
        namespace_view_id: NAMESPACE_VIEW_ID,
        scope: scope.into(),
        metadata_root: METADATA_ROOT,
    };
    let snapshot = format!("sha256:{}", hex::encode(descriptor.snapshot_id().unwrap()));
    let lease = format!("lease-{sequence}");
    f.bindings.lock().unwrap().insert(
        lease.clone(),
        Binding {
            snapshot: snapshot.clone(),
            actor: header(&headers, "authorization"),
        },
    );
    let mut response = json!({
        "descriptor": {
            "schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE_ID,
            "namespace_view_id": format!("sha256:{}", hex::encode(NAMESPACE_VIEW_ID)), "scope": scope,
            "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
            "metadata_root": format!("sha256:{}", hex::encode(METADATA_ROOT)), "snapshot_id": snapshot
        },
        "lease_id": lease, "publication_sequence": sequence.to_string(), "authorization_epoch": f.resolve_epoch
    });
    add_expiry(&mut response, f.initial_expiry);
    Json(response)
}

async fn lookup(
    State(f): State<Arc<Fixture>>,
    Path(snapshot): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    f.record("lookup", &snapshot, &headers, None);
    let lease = header(&headers, "x-mega-snapshot-lease").ok_or(StatusCode::FORBIDDEN)?;
    let binding = f
        .bindings
        .lock()
        .unwrap()
        .get(&lease)
        .cloned()
        .ok_or(StatusCode::FORBIDDEN)?;
    if binding.snapshot != snapshot || binding.actor != header(&headers, "authorization") {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(Json(json!({
        "snapshot_id": snapshot,
        "results": body["paths"].as_array().unwrap().iter().map(|path| json!({
            "path": path, "status": "found", "node": {"fs_kind": "regular", "name": path.as_str().unwrap().rsplit('/').next().unwrap(), "size": "1", "content_digest": format!("sha256:{}", "aa".repeat(32))}
        })).collect::<Vec<_>>()
    })))
}

async fn renew(
    State(f): State<Arc<Fixture>>,
    Path(lease): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    f.record("renew", &lease, &headers, body["lease_seconds"].as_u64());
    let binding = f.bindings.lock().unwrap().get(&lease).cloned();
    let Some(binding) = binding else {
        return StatusCode::FORBIDDEN.into_response();
    };
    if binding.actor != header(&headers, "authorization")
        || header(&headers, "x-mega-snapshot-lease").as_deref() != Some(lease.as_str())
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    f.renewal_count.fetch_add(1, Ordering::SeqCst);
    f.renewal_changed.notify_one();
    if f.fail_renewal.load(Ordering::SeqCst) {
        if let Some((code, status)) = f.canonical_retry_error {
            return (
                status,
                Json(json!({"error": {
                    "code": code, "message": "fixture renewal is temporarily unavailable",
                    "request_id": "renewal-fixture", "retryable": true
                }})),
            )
                .into_response();
        }
        if f.plain_retry_errors {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": {"code": "TEMPORARY_UNAVAILABLE", "message": "fixture backend is temporarily unavailable"}
        }))).into_response();
    }
    if f.malformed_renewal_json.load(Ordering::SeqCst) {
        return ([("content-type", "application/json")], "{broken-json").into_response();
    }
    if f.pause_renewal.load(Ordering::SeqCst) {
        f.renewal_release.notified().await;
    }
    let (returned_lease, snapshot) = match *f.renewal_identity.lock().unwrap() {
        RenewalIdentity::Correct => (lease, binding.snapshot),
        RenewalIdentity::WrongLease => ("other-lease".into(), binding.snapshot),
        RenewalIdentity::WrongSnapshot => (lease, format!("sha256:{}", "ff".repeat(32))),
    };
    let mut response = json!({"lease_id": returned_lease, "snapshot_id": snapshot});
    if let Some(epoch) = f.renewal_epoch.lock().unwrap().clone() {
        response["authorization_epoch"] = epoch;
    }
    add_expiry(&mut response, f.renewal_expiry);
    Json(response).into_response()
}

struct Server {
    url: String,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(f: Arc<Fixture>) -> Server {
    let app = Router::new()
        .route("/api/v2/snapshots/capabilities", get(capabilities))
        .route("/api/v2/snapshots/resolve", post(resolve))
        .route("/api/v2/snapshots/{snapshot}/lookup", post(lookup))
        .route("/api/v2/snapshots/leases/{lease}/renew", post(renew))
        .with_state(f);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    Server {
        url: format!("http://{address}"),
        task: tokio::spawn(async move { axum::serve(listener, app).await.unwrap() }),
    }
}

async fn probe(reader: &SnapshotReader) {
    let found = reader.lookup(&["/probe".to_string()]).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].path, "/probe");
}

#[tokio::test]
async fn concurrent_resolves_keep_distinct_snapshot_and_lease_bindings() {
    let fixture = Arc::new(Fixture::default());
    let server = serve(fixture.clone()).await;
    let client = Mst2Client::with_token(&server.url, Some("actor-a".into()));
    client.bind_lease("previous-template-lease");
    let (a, b) = tokio::join!(
        SnapshotReader::resolve(client.clone(), "/alpha", 600),
        SnapshotReader::resolve(client.clone(), "/beta", 600),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(a.snapshot_id(), b.snapshot_id());
    assert_ne!(a.lease_id(), b.lease_id());
    tokio::join!(probe(&a), probe(&b));
    probe(&a.clone()).await;
    for request in fixture.requests.lock().unwrap().iter() {
        assert_eq!(request.actor.as_deref(), Some("Bearer actor-a"));
        if request.kind == "resolve" || request.kind == "capabilities" {
            assert!(
                request.lease.is_none(),
                "previous lease leaked into resolve"
            );
        } else if request.kind == "lookup" {
            let expected = if request.id == a.snapshot_id() {
                a.lease_id()
            } else {
                b.lease_id()
            };
            assert_eq!(request.lease.as_deref(), Some(expected));
        }
    }
}

#[tokio::test]
async fn rotation_during_resolve_freezes_the_actor_for_the_existing_reader() {
    let fixture = Arc::new(Fixture {
        pause_next_resolve: AtomicBool::new(true),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let client = Mst2Client::with_token(&server.url, Some("before-rotation".into()));
    let original_partition = client.credential_partition();
    let resolving_client = client.clone();
    let resolving = tokio::spawn(async move {
        SnapshotReader::resolve(resolving_client, "/alpha", 600)
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(5), fixture.resolve_started.notified())
        .await
        .unwrap();
    client.set_token(Some("after-rotation".into()));
    fixture.resolve_release.notify_one();
    let original = resolving.await.unwrap();
    let next = SnapshotReader::resolve(client.clone(), "/beta", 600)
        .await
        .unwrap();
    assert_eq!(original.client().credential_partition(), original_partition);
    assert_ne!(next.client().credential_partition(), original_partition);
    original.client().set_token(Some("unrelated-actor".into()));
    original.client().bind_lease("unrelated-lease");
    assert_eq!(original.client().credential_partition(), original_partition);
    tokio::join!(probe(&original), probe(&next));
    let requests = fixture.requests.lock().unwrap();
    let original_lookup = requests
        .iter()
        .find(|r| r.kind == "lookup" && r.id == original.snapshot_id())
        .unwrap();
    assert_eq!(
        original_lookup.actor.as_deref(),
        Some("Bearer before-rotation")
    );
    let next_lookup = requests
        .iter()
        .find(|r| r.kind == "lookup" && r.id == next.snapshot_id())
        .unwrap();
    assert_eq!(next_lookup.actor.as_deref(), Some("Bearer after-rotation"));
}

#[tokio::test]
async fn server_grants_control_both_initial_and_renewed_windows() {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::After(Duration::from_millis(1500)),
        renewal_expiry: Expiry::After(Duration::from_millis(1500)),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let client = Mst2Client::with_token(&server.url, Some("actor-a".into()));
    let reader = SnapshotReader::resolve(client.clone(), "/alpha", 600)
        .await
        .unwrap();
    client.set_token(Some("actor-b".into()));
    fixture.wait_for_renewals(2).await;
    probe(&reader).await;
    assert_eq!(
        fixture.resolve_count.load(Ordering::SeqCst),
        1,
        "reader re-resolved latest"
    );
    for request in fixture
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.kind == "renew")
    {
        assert_eq!(request.id, reader.lease_id());
        assert_eq!(request.actor.as_deref(), Some("Bearer actor-a"));
        assert_eq!(request.lease.as_deref(), Some(reader.lease_id()));
        assert_eq!(request.requested_seconds, Some(600));
    }
}

#[tokio::test]
async fn offset_initial_and_renewed_grants_preserve_the_same_fixed_authority() {
    for offset in [8 * 3600, -2 * 3600 - 30 * 60] {
        let fixture = Arc::new(Fixture {
            initial_expiry: Expiry::OffsetAfter(Duration::from_millis(1500), offset),
            renewal_expiry: Expiry::OffsetAfter(Duration::from_millis(1500), -offset),
            renewal_epoch: Mutex::new(Some(json!("1"))),
            ..Fixture::default()
        });
        let server = serve(fixture.clone()).await;
        let client = Mst2Client::with_token(&server.url, Some("offset-actor".into()));
        let reader = SnapshotReader::resolve(client.clone(), "/alpha", 600)
            .await
            .unwrap();
        let snapshot = reader.snapshot_id().to_string();
        let lease = reader.lease_id().to_string();
        client.set_token(Some("next-actor".into()));
        fixture.wait_for_renewals(2).await;
        probe(&reader).await;
        assert_eq!(reader.snapshot_id(), snapshot);
        assert_eq!(reader.lease_id(), lease);
        assert_eq!(reader.authorized_context().authorization_epoch(), 1);
        assert_eq!(fixture.resolve_count.load(Ordering::SeqCst), 1);
        let requests = fixture.requests.lock().unwrap();
        for request in requests.iter().filter(|request| request.kind == "renew") {
            assert_eq!(request.actor.as_deref(), Some("Bearer offset-actor"));
            assert_eq!(request.lease.as_deref(), Some(lease.as_str()));
            assert_eq!(request.requested_seconds, Some(600));
        }
    }
}

#[tokio::test]
async fn a_positive_offset_does_not_extend_a_failed_renewals_actual_grant() {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::OffsetAfter(Duration::from_millis(1500), 8 * 3600),
        fail_renewal: AtomicBool::new(true),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
        .await
        .unwrap();
    fixture.wait_for_renewals(1).await;
    tokio::time::sleep(Duration::from_millis(1700)).await;
    let clone = reader.clone();
    assert_eq!(
        reader.lookup(&["/probe".into()]).await.unwrap_err().code,
        SnapshotErrorCode::LeaseExpired
    );
    assert_eq!(
        clone.lookup(&["/probe".into()]).await.unwrap_err().code,
        SnapshotErrorCode::LeaseExpired
    );
    assert_eq!(fixture.resolve_count.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.count("lookup"), 0);
}

#[tokio::test]
async fn invalid_initial_expiry_fails_before_any_snapshot_request() {
    for (expiry, expected) in [
        (Expiry::Missing, SnapshotErrorCode::IntegrityError),
        (Expiry::Text("garbage"), SnapshotErrorCode::IntegrityError),
        (
            Expiry::Text("2099-01-01T00:00:00-00:00"),
            SnapshotErrorCode::IntegrityError,
        ),
        (
            Expiry::Text("1969-12-31T23:59:59Z"),
            SnapshotErrorCode::LeaseExpired,
        ),
        (
            Expiry::Text("2026-04-31T00:00:00Z"),
            SnapshotErrorCode::IntegrityError,
        ),
        (
            Expiry::Text("1970-01-01T00:00:00Z"),
            SnapshotErrorCode::LeaseExpired,
        ),
    ] {
        let fixture = Arc::new(Fixture {
            initial_expiry: expiry,
            ..Fixture::default()
        });
        let server = serve(fixture.clone()).await;
        let error = match SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600).await
        {
            Ok(_) => panic!("invalid initial expiry was accepted"),
            Err(error) => error,
        };
        assert_eq!(error.code, expected);
        assert_eq!(fixture.count("lookup"), 0);
        assert_eq!(fixture.count("renew"), 0);
    }
}

#[tokio::test]
async fn invalid_renewals_fail_closed_without_fetching_or_resolving_latest() {
    for (expiry, identity, expected) in [
        (
            Expiry::Missing,
            RenewalIdentity::Correct,
            SnapshotErrorCode::IntegrityError,
        ),
        (
            Expiry::Text("not-a-timestamp"),
            RenewalIdentity::Correct,
            SnapshotErrorCode::IntegrityError,
        ),
        (
            Expiry::Text("1970-01-01T00:00:00Z"),
            RenewalIdentity::Correct,
            SnapshotErrorCode::LeaseExpired,
        ),
        (
            Expiry::After(Duration::from_secs(30)),
            RenewalIdentity::WrongLease,
            SnapshotErrorCode::IntegrityError,
        ),
        (
            Expiry::After(Duration::from_secs(30)),
            RenewalIdentity::WrongSnapshot,
            SnapshotErrorCode::IntegrityError,
        ),
    ] {
        let fixture = Arc::new(Fixture {
            initial_expiry: Expiry::After(Duration::from_millis(1500)),
            renewal_expiry: expiry,
            renewal_identity: Mutex::new(identity),
            ..Fixture::default()
        });
        let server = serve(fixture.clone()).await;
        let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
            .await
            .unwrap();
        fixture.wait_for_renewals(1).await;
        for _ in 0..2 {
            let error = reader
                .lookup(&["/probe".to_string()])
                .await
                .expect_err("invalid renewal did not fail closed");
            assert_eq!(error.code, expected);
        }
        assert_eq!(
            fixture.count("lookup"),
            0,
            "source read proceeded after invalid renewal"
        );
        assert_eq!(
            fixture.resolve_count.load(Ordering::SeqCst),
            1,
            "failure silently re-resolved latest"
        );
        assert_eq!(
            fixture.renewal_count.load(Ordering::SeqCst),
            1,
            "invalid renewal was retried as a new grant"
        );
    }
}

#[tokio::test]
async fn only_the_last_reader_clone_stops_background_renewal() {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::After(Duration::from_millis(1500)),
        renewal_expiry: Expiry::After(Duration::from_millis(1500)),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let original = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
        .await
        .unwrap();
    let clone = original.clone();
    drop(original);
    fixture.wait_for_renewals(2).await;
    probe(&clone).await;
    drop(clone);
    let stopped_at = fixture.renewal_count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        fixture.renewal_count.load(Ordering::SeqCst),
        stopped_at,
        "last reader drop retained the renewer"
    );
}

#[tokio::test]
async fn last_reader_drop_cancels_renewal_while_the_server_response_is_blocked() {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::After(Duration::from_millis(1500)),
        renewal_expiry: Expiry::After(Duration::from_millis(1500)),
        pause_renewal: AtomicBool::new(true),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
        .await
        .unwrap();
    fixture.wait_for_renewals(1).await;
    drop(reader);
    fixture.pause_renewal.store(false, Ordering::SeqCst);
    fixture.renewal_release.notify_one();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        fixture.renewal_count.load(Ordering::SeqCst),
        1,
        "blocked renewal continued after last reader drop"
    );
}

#[tokio::test]
async fn exhausted_503_retries_return_a_typed_error_and_recover_within_the_original_grant() {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::After(Duration::from_secs(6)),
        fail_renewal: AtomicBool::new(true),
        // Exercise the fallback response path: a missing error envelope must
        // not discard the 503 status needed for bounded recovery.
        plain_retry_errors: true,
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let client = Mst2Client::new(&server.url);
    let reader = SnapshotReader::resolve(client.clone(), "/alpha", 600)
        .await
        .unwrap();
    fixture.wait_for_renewals(4).await;
    let error = reader
        .lookup(&["/probe".into()])
        .await
        .expect_err("failed renewal was silently ignored");
    assert_eq!(error.code, SnapshotErrorCode::Internal);
    assert_eq!(error.http_status, 503);
    assert_eq!(
        fixture.count("lookup"),
        0,
        "source read bypassed failed renewal"
    );
    assert!(
        client.retry_count() >= 3,
        "fixture did not exhaust the transport retry batch"
    );
    let failed_requests = fixture.renewal_count.load(Ordering::SeqCst);
    fixture.fail_renewal.store(false, Ordering::SeqCst);
    fixture.wait_for_renewals(failed_requests + 1).await;
    probe(&reader).await;
    assert_eq!(
        fixture.resolve_count.load(Ordering::SeqCst),
        1,
        "recovery changed the fixed view"
    );
    assert_eq!(fixture.count("lookup"), 1);
}

#[tokio::test]
async fn persistent_transient_failures_stop_at_the_original_deadline() {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::After(Duration::from_secs(6)),
        fail_renewal: AtomicBool::new(true),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
        .await
        .unwrap();
    fixture.wait_for_renewals(4).await;
    let error = reader
        .lookup(&["/probe".into()])
        .await
        .expect_err("temporary outage was treated as a successful renewal");
    assert_eq!(error.code, SnapshotErrorCode::TemporaryUnavailable);
    assert_eq!(error.http_status, 503);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let expired = reader
        .lookup(&["/probe".into()])
        .await
        .expect_err("requested seconds extended the old server grant");
    assert_eq!(expired.code, SnapshotErrorCode::LeaseExpired);
    let stopped = fixture.renewal_count.load(Ordering::SeqCst);
    assert!(
        (4..=20).contains(&stopped),
        "renewals did not stay within the bounded retry budget: {stopped}"
    );
    fixture.fail_renewal.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        fixture.renewal_count.load(Ordering::SeqCst),
        stopped,
        "expired lease continued renewing after backend recovery"
    );
    assert_eq!(
        reader.lookup(&["/probe".into()]).await.unwrap_err().code,
        SnapshotErrorCode::LeaseExpired
    );
    assert_eq!(fixture.count("lookup"), 0);
    assert_eq!(fixture.resolve_count.load(Ordering::SeqCst), 1);
}

async fn canonical_renewal_failure(
    code: &'static str,
    status: StatusCode,
    expected: SnapshotErrorCode,
    recover: bool,
) {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::After(Duration::from_secs(6)),
        fail_renewal: AtomicBool::new(true),
        canonical_retry_error: Some((code, status)),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let client = Mst2Client::new(&server.url);
    let reader = SnapshotReader::resolve(client.clone(), "/alpha", 600)
        .await
        .unwrap();
    let clone = reader.clone();
    fixture.wait_for_renewals(4).await;
    for view in [&reader, &clone] {
        let error = view.lookup(&["/probe".into()]).await.unwrap_err();
        assert_eq!(error.code, expected, "{code}");
        assert_eq!(error.http_status, status.as_u16());
    }
    assert_eq!(fixture.count("lookup"), 0);
    assert!(client.retry_count() >= 3);
    if recover {
        let failed = fixture.renewal_count.load(Ordering::SeqCst);
        fixture.fail_renewal.store(false, Ordering::SeqCst);
        fixture.wait_for_renewals(failed + 1).await;
        probe(&reader).await;
        probe(&clone).await;
        assert_eq!(fixture.count("lookup"), 2);
        assert_eq!(reader.snapshot_id(), clone.snapshot_id());
        assert_eq!(reader.lease_id(), clone.lease_id());
    } else {
        tokio::time::sleep(Duration::from_secs(3)).await;
        for view in [&reader, &clone] {
            assert_eq!(
                view.lookup(&["/probe".into()]).await.unwrap_err().code,
                SnapshotErrorCode::LeaseExpired,
                "{code} did not stop at the original grant"
            );
        }
        let stopped = fixture.renewal_count.load(Ordering::SeqCst);
        assert!((4..=20).contains(&stopped));
        fixture.fail_renewal.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(fixture.renewal_count.load(Ordering::SeqCst), stopped);
        assert_eq!(
            clone.lookup(&["/probe".into()]).await.unwrap_err().code,
            SnapshotErrorCode::LeaseExpired,
            "late recovery revived the expired lease"
        );
        assert_eq!(fixture.count("lookup"), 0);
    }
    assert_eq!(fixture.resolve_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn typed_rate_limit_and_metadata_outages_recover_without_changing_the_fixed_view() {
    tokio::join!(
        canonical_renewal_failure(
            "RATE_LIMITED",
            StatusCode::TOO_MANY_REQUESTS,
            SnapshotErrorCode::Internal,
            true
        ),
        canonical_renewal_failure(
            "METADATA_NOT_READY",
            StatusCode::SERVICE_UNAVAILABLE,
            SnapshotErrorCode::Internal,
            true
        )
    );
}

#[tokio::test]
async fn typed_rate_limit_and_metadata_outages_cannot_extend_or_revive_the_original_grant() {
    tokio::join!(
        canonical_renewal_failure(
            "RATE_LIMITED",
            StatusCode::TOO_MANY_REQUESTS,
            SnapshotErrorCode::Internal,
            false
        ),
        canonical_renewal_failure(
            "METADATA_NOT_READY",
            StatusCode::SERVICE_UNAVAILABLE,
            SnapshotErrorCode::Internal,
            false
        )
    );
}

#[tokio::test]
async fn wrong_renewal_identity_is_terminal_after_the_backend_is_corrected() {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::After(Duration::from_secs(3)),
        renewal_identity: Mutex::new(RenewalIdentity::WrongLease),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
        .await
        .unwrap();
    fixture.wait_for_renewals(1).await;
    let error = reader.lookup(&["/probe".into()]).await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
    *fixture.renewal_identity.lock().unwrap() = RenewalIdentity::Correct;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        reader.lookup(&["/probe".into()]).await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(
        fixture.renewal_count.load(Ordering::SeqCst),
        1,
        "identity failure was treated as a retryable outage"
    );
    assert_eq!(fixture.count("lookup"), 0);
    assert_eq!(fixture.resolve_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn changed_and_malformed_renewal_epochs_are_terminal_for_every_reader_clone() {
    let cases = [
        json!("0"),
        json!("2"),
        json!("01"),
        json!("+1"),
        json!(" 1"),
        json!(""),
        json!("１"),
        json!("9223372036854775808"),
        json!("18446744073709551616"),
        json!(null),
        json!(1),
        json!(true),
        json!({"epoch": "1"}),
    ];
    futures::future::join_all(cases.into_iter().map(|epoch| async move {
        let fixture = Arc::new(Fixture {
            initial_expiry: Expiry::After(Duration::from_secs(3)),
            renewal_epoch: Mutex::new(Some(epoch)),
            ..Fixture::default()
        });
        let server = serve(fixture.clone()).await;
        let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
            .await
            .unwrap();
        let fixed_domain = reader.authorized_context().cache_domain().id().to_string();
        let clone = reader.clone();
        fixture.wait_for_renewals(1).await;
        assert_eq!(
            reader.ensure_lease().await.unwrap_err().code,
            SnapshotErrorCode::IntegrityError
        );
        *fixture.renewal_epoch.lock().unwrap() = Some(json!("1"));
        assert_eq!(
            clone.lookup(&["/probe".into()]).await.unwrap_err().code,
            SnapshotErrorCode::IntegrityError
        );
        assert_eq!(reader.authorized_context().authorization_epoch(), 1);
        assert_eq!(
            reader.authorized_context().cache_domain().id(),
            fixed_domain
        );
        assert_eq!(fixture.renewal_count.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.count("lookup"), 0);
        assert_eq!(fixture.resolve_count.load(Ordering::SeqCst), 1);
        assert_eq!(reader.client().retry_count(), 0);
    }))
    .await;
}

#[tokio::test]
async fn matching_renewal_epochs_preserve_fixed_authority_at_the_counter_boundary() {
    for epoch in ["0", "1", "9223372036854775807"] {
        let fixture = Arc::new(Fixture {
            initial_expiry: Expiry::After(Duration::from_secs(3)),
            resolve_epoch: epoch,
            renewal_epoch: Mutex::new(Some(json!(epoch))),
            ..Fixture::default()
        });
        let server = serve(fixture.clone()).await;
        let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
            .await
            .unwrap();
        let fixed_domain = reader.authorized_context().cache_domain().id().to_string();
        fixture.wait_for_renewals(1).await;
        reader.ensure_lease().await.unwrap();
        probe(&reader).await;
        assert_eq!(
            reader
                .authorized_context()
                .authorization_epoch()
                .to_string(),
            epoch
        );
        assert_eq!(
            reader.authorized_context().cache_domain().id(),
            fixed_domain
        );
        assert_eq!(fixture.renewal_count.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.count("lookup"), 1);
        assert_eq!(fixture.resolve_count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn an_epoch_reporting_lease_cannot_downgrade_to_legacy_renewals() {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::After(Duration::from_secs(3)),
        renewal_expiry: Expiry::After(Duration::from_secs(3)),
        renewal_epoch: Mutex::new(Some(json!("1"))),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
        .await
        .unwrap();
    fixture.wait_for_renewals(1).await;
    reader.ensure_lease().await.unwrap();
    *fixture.renewal_epoch.lock().unwrap() = None;
    fixture.wait_for_renewals(2).await;
    assert_eq!(
        reader.ensure_lease().await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    *fixture.renewal_epoch.lock().unwrap() = Some(json!("1"));
    assert_eq!(
        reader.lookup(&["/probe".into()]).await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(fixture.renewal_count.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.count("lookup"), 0);
    assert_eq!(fixture.resolve_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn malformed_success_json_is_terminal_instead_of_a_network_outage() {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::After(Duration::from_secs(3)),
        malformed_renewal_json: AtomicBool::new(true),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
        .await
        .unwrap();
    fixture.wait_for_renewals(1).await;
    let error = reader.lookup(&["/probe".into()]).await.unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
    fixture
        .malformed_renewal_json
        .store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        reader.lookup(&["/probe".into()]).await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(
        fixture.renewal_count.load(Ordering::SeqCst),
        1,
        "invalid JSON became a recoverable grant"
    );
    assert_eq!(fixture.count("lookup"), 0);
}

#[tokio::test]
async fn renewal_waiting_for_http_is_cancelled_at_the_original_deadline() {
    let fixture = Arc::new(Fixture {
        initial_expiry: Expiry::After(Duration::from_millis(1500)),
        pause_renewal: AtomicBool::new(true),
        ..Fixture::default()
    });
    let server = serve(fixture.clone()).await;
    let reader = SnapshotReader::resolve(Mst2Client::new(&server.url), "/alpha", 600)
        .await
        .unwrap();
    fixture.wait_for_renewals(1).await;
    let error = tokio::time::timeout(Duration::from_secs(2), reader.lookup(&["/probe".into()]))
        .await
        .expect("source read hung on renewal after the original deadline")
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::LeaseExpired);
    fixture.pause_renewal.store(false, Ordering::SeqCst);
    fixture.renewal_release.notify_one();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(fixture.renewal_count.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.count("lookup"), 0);
    assert_eq!(
        reader.lookup(&["/probe".into()]).await.unwrap_err().code,
        SnapshotErrorCode::LeaseExpired
    );
}
