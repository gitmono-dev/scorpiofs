//! The v3 service fails before mounting when the canonical fixed root cannot
//! be proved. These cases run without probing or skipping for a FUSE device.
#![cfg(unix)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use mst2_codec::{
    descriptor::ServingDescriptor,
    metapage::{page_id, Page},
    treeframe::{EndPayload, MetaPayload},
};
use scorpiofs::{
    snapshot::{durable::digest_of, frames::parse_digest, DurableStore, Mst2Client},
    workspace::{
        http, CreateWorkspace, WorkspaceConfig, WorkspaceObservationError, WorkspaceObserver,
        WorkspaceResolveBinding, WorkspaceService,
    },
};
use serde_json::{json, Value};
use tokio::{sync::Semaphore, task::JoinHandle};

const INSTANCE: &str = "11111111-2222-4333-8444-555555555555";
const SCOPE: &str = "/project";

fn digest(id: &[u8; 32]) -> String {
    format!("sha256:{}", hex::encode(id))
}

#[derive(Clone, Copy)]
enum RootFailure {
    Denied,
    BadEnd,
}

struct Fixture {
    root: [u8; 32],
    page: Vec<u8>,
    sid: String,
    failure: RootFailure,
    gate: Option<Arc<Semaphore>>,
    entered: Semaphore,
    resolves: AtomicUsize,
    metadata: AtomicUsize,
    requests: Mutex<Vec<String>>,
    resolve_ids: Mutex<Vec<Option<String>>>,
}

impl Fixture {
    fn new(failure: RootFailure, gated: bool) -> Arc<Self> {
        let page = Page::Leaf { entries: vec![] }.encode().unwrap();
        let root = page_id(&page);
        let sid = digest(
            &ServingDescriptor {
                instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
                namespace_view_id: [0x22; 32],
                scope: SCOPE.into(),
                metadata_root: root,
            }
            .snapshot_id()
            .unwrap(),
        );
        Arc::new(Self {
            root,
            page,
            sid,
            failure,
            gate: gated.then(|| Arc::new(Semaphore::new(0))),
            entered: Semaphore::new(0),
            resolves: AtomicUsize::new(0),
            metadata: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            resolve_ids: Mutex::new(Vec::new()),
        })
    }

    async fn wait_for_resolve(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.acquire())
            .await
            .expect("the real resolve request did not arrive")
            .unwrap()
            .forget();
    }

    fn release_resolve(&self) {
        self.gate.as_ref().unwrap().add_permits(1);
    }

    fn assert_only_canonical_requests(&self) {
        let expected_metadata = format!("/api/v2/snapshots/{}/metadata/pages", self.sid);
        let requests = self.requests.lock().unwrap();
        assert!(requests.iter().any(|p| p == &expected_metadata));
        for path in requests.iter() {
            assert!(
                path == "/api/v2/snapshots/capabilities"
                    || path == "/api/v2/snapshots/resolve"
                    || path == &expected_metadata,
                "unexpected legacy, dictionary or content request: {path}"
            );
        }
    }
}

async fn record(
    State(f): State<Arc<Fixture>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    f.requests.lock().unwrap().push(request.uri().path().into());
    next.run(request).await
}

async fn capabilities() -> Json<Value> {
    Json(serde_json::from_str(include_str!("fixtures/mst2_capabilities_0_2_1.json")).unwrap())
}

async fn resolve(
    State(f): State<Arc<Fixture>>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Response {
    assert_eq!(request["target"], json!({"kind": "latest"}));
    assert_eq!(request["scope"], SCOPE);
    f.resolves.fetch_add(1, Ordering::SeqCst);
    let request_id = headers
        .get("x-request-id")
        .map(|value| value.to_str().unwrap().to_owned());
    f.resolve_ids.lock().unwrap().push(request_id.clone());
    f.entered.add_permits(1);
    if let Some(gate) = &f.gate {
        gate.acquire().await.unwrap().forget();
    }
    let mut response = Json(json!({
        "descriptor": {
            "schema_version": 2, "metadata_codec": 1, "instance_id": INSTANCE,
            "namespace_view_id": digest(&[0x22; 32]), "scope": SCOPE,
            "materialization_policy": 1, "fs_semantics": 1, "access_projection": 0,
            "metadata_root": digest(&f.root), "snapshot_id": f.sid
        },
        "lease_id": "workspace-v3-lease", "lease_expires_at": "2099-01-01T00:00:00Z",
        "publication_sequence": "1", "writer_epoch": "1", "authorization_epoch": "1",
        "resolved_at": "2026-01-01T00:00:00Z", "delivery": request["delivery"]
    }))
    .into_response();
    if let Some(id) = request_id {
        response
            .headers_mut()
            .insert("x-request-id", HeaderValue::from_str(&id).unwrap());
    }
    response
}

async fn metadata(State(f): State<Arc<Fixture>>, Path(sid): Path<String>, body: Bytes) -> Response {
    assert_eq!(sid, f.sid);
    f.metadata.fetch_add(1, Ordering::SeqCst);
    let request: Value = serde_json::from_slice(&body).unwrap();
    let items = request["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["directory_path"], "/");
    assert_eq!(items[0]["route"], json!([]));
    assert_eq!(items[0]["expected_digest"], digest(&f.root));
    if matches!(f.failure, RootFailure::Denied) {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"error":{
                    "code":"SCOPE_FORBIDDEN", "message":"root metadata denied",
                    "request_id":"workspace-root-denied", "retryable":false
                }})
                .to_string(),
            ))
            .unwrap();
    }
    let mut wire = MetaPayload {
        pages: vec![(f.root, f.page.clone())],
    }
    .encode(7, 0)
    .unwrap();
    // The page and descriptor are correct; only the terminal request binding
    // is damaged, so accepting the META before checking END would mount it.
    wire.extend(
        EndPayload {
            request_item_count: 1,
            unique_unit_count: 1,
            logical_bytes: f.page.len() as u64,
            request_body_sha256: [0; 32],
        }
        .encode(7, 1),
    );
    assert_ne!(parse_digest(&digest_of(&body)).unwrap(), [0; 32]);
    Response::builder()
        .header("content-type", "application/vnd.mega.treeframe;version=2")
        .header("x-mega-snapshot-id", sid)
        .header("x-mega-request-digest", digest_of(&body))
        .body(Body::from(wire))
        .unwrap()
}

struct Server {
    url: String,
    task: JoinHandle<()>,
}

impl Server {
    async fn start(router: Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self { url, task }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Harness {
    fixture: Arc<Fixture>,
    service: Arc<WorkspaceService>,
    client: reqwest::Client,
    api: Server,
    _upstream: Server,
    temp: tempfile::TempDir,
}

impl Harness {
    async fn new(failure: RootFailure, gated: bool, workspaces: usize, operations: usize) -> Self {
        Self::new_with_observer(failure, gated, workspaces, operations, None).await
    }

    async fn new_with_observer(
        failure: RootFailure,
        gated: bool,
        workspaces: usize,
        operations: usize,
        observer: Option<Arc<WorkspaceObserver>>,
    ) -> Self {
        let fixture = Fixture::new(failure, gated);
        let upstream = Server::start(
            Router::new()
                .route("/api/v2/snapshots/capabilities", get(capabilities))
                .route("/api/v2/snapshots/resolve", post(resolve))
                .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
                .fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR })
                .layer(axum::middleware::from_fn_with_state(
                    fixture.clone(),
                    record,
                ))
                .with_state(fixture.clone()),
        )
        .await;
        let temp = tempfile::tempdir().unwrap();
        let mut config =
            WorkspaceConfig::new(temp.path().join("workspaces"), temp.path().join("cache"));
        config.max_workspaces = workspaces;
        config.max_operations = operations;
        let client = Mst2Client::new(&upstream.url);
        let service = match observer {
            Some(observer) => WorkspaceService::new_with_observer(client, config, observer),
            None => WorkspaceService::new(client, config),
        }
        .unwrap();
        let api = Server::start(http::router(service.clone())).await;
        Self {
            fixture,
            service,
            client: reqwest_client(),
            api,
            _upstream: upstream,
            temp,
        }
    }

    async fn create(&self, body: Value) -> reqwest::Response {
        self.client
            .post(format!("{}/v3/workspaces", self.api.url))
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    async fn list(&self) -> Value {
        // An owned operation may still hold the only control permit after its
        // caller aborts. Retry only the public admission rejection, not the
        // failed create or the upstream request.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let response = self
                    .client
                    .get(format!("{}/v3/workspaces", self.api.url))
                    .send()
                    .await
                    .unwrap();
                if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                    let body: Value = response.json().await.unwrap();
                    assert_eq!(body["code"], "WORKSPACE_BUSY");
                    tokio::task::yield_now().await;
                    continue;
                }
                assert_eq!(response.status(), StatusCode::OK);
                return response.json::<Value>().await.unwrap();
            }
        })
        .await
        .expect("the owned creation never retired")
    }

    async fn destroy(&self, id: &str) {
        let response = self
            .client
            .post(format!("{}/v3/workspaces/{id}/destroy", self.api.url))
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    fn assert_no_mount_directory(&self) {
        assert_eq!(
            std::fs::read_dir(self.temp.path().join("workspaces"))
                .unwrap()
                .count(),
            0
        );
    }

    fn cancelled_create(
        &self,
    ) -> JoinHandle<
        Result<scorpiofs::workspace::WorkspaceStatus, scorpiofs::workspace::WorkspaceError>,
    > {
        let service = self.service.clone();
        tokio::spawn(async move {
            service
                .create(serde_json::from_value(request("lazy")).unwrap())
                .await
        })
    }
}

fn reqwest_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap()
}

fn request(delivery: &str) -> Value {
    json!({"target":{"kind":"latest"},"scope":SCOPE,"delivery":delivery,"upper_policy":"private"})
}

fn assert_failed(status: &Value, fixture: &Fixture) {
    assert_eq!(status["snapshot_id"], fixture.sid);
    assert_eq!(status["mount_state"], "failed");
    assert_eq!(status["metadata_ready"], false);
    assert_ne!(status["hydration_state"], "complete");
    assert_ne!(status["local_pin_state"], "complete_snapshot");
    assert!(status["last_error"].as_str().is_some_and(|s| !s.is_empty()));
}

fn assert_observed_binding(
    record: &WorkspaceResolveBinding,
    status: &Value,
    harness: &Harness,
    run: &str,
) {
    assert_failed(status, &harness.fixture);
    assert_eq!(record.revision, 1);
    assert_eq!(record.run_id, run);
    assert_eq!(
        record.workspace_id,
        status["workspace_id"].as_str().unwrap()
    );
    assert_eq!(record.generation, status["generation"].as_str().unwrap());
    for id in [&record.workspace_id, &record.generation] {
        let uuid = uuid::Uuid::parse_str(id).unwrap();
        assert!(!uuid.is_nil());
        assert_eq!(uuid.to_string(), *id);
    }
    let logical_id = format!("ws:{run}:{}", record.workspace_id);
    assert!(logical_id.len() <= 125);
    assert_eq!(record.logical_request_id, logical_id);
    assert_eq!(record.resolve_trace_receipt.logical_request_id, logical_id);
    assert_eq!(
        record.resolve_trace_receipt.attempt_ids,
        [format!("{logical_id}:a1")]
    );
    assert_eq!(
        record.resolve_trace_receipt.final_attempt_id,
        format!("{logical_id}:a1")
    );
    assert_eq!(record.resolve_trace_receipt.retry_count, 0);
    assert_eq!(record.instance_id, INSTANCE);
    assert_eq!(record.namespace_view_id, digest(&[0x22; 32]));
    assert_eq!(record.snapshot_id, harness.fixture.sid);
    assert_eq!(record.scope, SCOPE);
    assert_eq!(record.publication_sequence, 1);
    let descriptor = ServingDescriptor {
        instance_uuid: *uuid::Uuid::parse_str(INSTANCE).unwrap().as_bytes(),
        namespace_view_id: [0x22; 32],
        scope: SCOPE.into(),
        metadata_root: harness.fixture.root,
    }
    .encode()
    .unwrap();
    assert_eq!(record.descriptor_bytes_hex, hex::encode(&descriptor));
    assert_eq!(digest_of(&descriptor), record.snapshot_id);
    assert!(record
        .store
        .starts_with(harness.temp.path().join("cache").canonicalize().unwrap()));
    assert_eq!(
        record.store.file_name().unwrap(),
        record.workspace_id.as_str()
    );
    let owners = record.store.parent().unwrap();
    assert_eq!(owners.file_name().unwrap(), "owners");
    let view = owners.parent().unwrap();
    assert_eq!(
        view.file_name().unwrap(),
        record.snapshot_id.strip_prefix("sha256:").unwrap()
    );
    assert_eq!(record.content_store, view.parent().unwrap().join("blobs"));
    let store = DurableStore::open_with_content(&record.store, &record.content_store).unwrap();
    let binding = store.workspace_binding().unwrap().unwrap();
    assert_eq!(binding.workspace_id(), record.workspace_id);
    assert_eq!(binding.snapshot_id(), record.snapshot_id);
    let value = serde_json::to_value(record).unwrap();
    assert_eq!(value["record"], "workspace_resolve_binding");
    for absent in [
        "lease_id",
        "token",
        "last_error",
        "mount_state",
        "metadata_ready",
        "full_snapshot_complete",
    ] {
        assert!(value.get(absent).is_none());
    }
    let mut unknown = value.clone();
    unknown["metadata_ready"] = true.into();
    assert!(serde_json::from_value::<WorkspaceResolveBinding>(unknown).is_err());
    let mut unknown_receipt = value;
    unknown_receipt["resolve_trace_receipt"]["lease_id"] = "not-authority".into();
    assert!(serde_json::from_value::<WorkspaceResolveBinding>(unknown_receipt).is_err());
}

#[tokio::test]
async fn observed_creation_records_its_only_actual_resolve_and_independent_owners_without_claiming_mount_success(
) {
    let run = "11111111-2222-4333-8444-666666666666";
    let (observer, mut observations) = WorkspaceObserver::channel(run, 2).unwrap();
    let h =
        Harness::new_with_observer(RootFailure::Denied, false, 2, 2, Some(observer.clone())).await;
    for _ in 0..2 {
        assert_eq!(
            h.create(request("lazy")).await.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
    let statuses = h.list().await;
    let first = observations.try_recv().unwrap();
    let second = observations.try_recv().unwrap();
    assert_ne!(first.workspace_id, second.workspace_id);
    assert_ne!(first.generation, second.generation);
    assert_ne!(first.store, second.store);
    assert_eq!(first.content_store, second.content_store);
    for record in [&first, &second] {
        let status = statuses
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["workspace_id"] == record.workspace_id)
            .unwrap();
        assert_observed_binding(record, status, &h, run);
    }
    assert_eq!(
        h.fixture.resolves.load(Ordering::SeqCst),
        2,
        "observer must not resolve again for labels"
    );
    assert_eq!(
        *h.fixture.resolve_ids.lock().unwrap(),
        [
            Some(first.resolve_trace_receipt.final_attempt_id),
            Some(second.resolve_trace_receipt.final_attempt_id)
        ]
    );
    assert!(matches!(
        observations.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(observer.status().first_error, None);
    assert_eq!(observer.status().accepted_records, 2);
    for status in statuses.as_array().unwrap() {
        h.destroy(status["workspace_id"].as_str().unwrap()).await;
    }
    h.assert_no_mount_directory();
    h.fixture.assert_only_canonical_requests();
    drop(h);
    drop(observer);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), observations.recv())
            .await
            .unwrap()
            .is_none()
    );
    let final_status = observations.finish().unwrap();
    assert_eq!(final_status.accepted_records, 2);
    assert_eq!(final_status.received_records, 2);
}

#[tokio::test]
async fn ordinary_creation_has_no_observation_header_or_record() {
    let (observer, observations) =
        WorkspaceObserver::channel("11111111-2222-4333-8444-666666666666", 1).unwrap();
    let h = Harness::new(RootFailure::Denied, false, 1, 2).await;
    assert_eq!(
        h.create(request("lazy")).await.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(*h.fixture.resolve_ids.lock().unwrap(), [None]);
    assert_eq!(h.fixture.resolves.load(Ordering::SeqCst), 1);
    assert_eq!(observer.status().accepted_records, 0);
    let statuses = h.list().await;
    h.destroy(statuses[0]["workspace_id"].as_str().unwrap())
        .await;
    h.assert_no_mount_directory();
    drop(observer);
    assert_eq!(observations.finish().unwrap().received_records, 0);
}

#[tokio::test]
async fn full_observation_queue_is_sticky_but_preserves_failed_owners_and_cleanup_capacity() {
    let (observer, mut observations) =
        WorkspaceObserver::channel("11111111-2222-4333-8444-666666666666", 1).unwrap();
    let h =
        Harness::new_with_observer(RootFailure::Denied, false, 2, 2, Some(observer.clone())).await;
    for _ in 0..2 {
        assert_eq!(
            h.create(request("lazy")).await.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
    let statuses = h.list().await;
    assert_eq!(statuses.as_array().unwrap().len(), 2);
    assert_eq!(h.fixture.resolves.load(Ordering::SeqCst), 2);
    assert_eq!(observer.status().accepted_records, 1);
    assert_eq!(
        observer.status().first_error,
        Some(WorkspaceObservationError::QueueFull)
    );
    let recorded = observations.try_recv().unwrap();
    for status in statuses.as_array().unwrap() {
        assert_failed(status, &h.fixture);
        let owner_root = recorded
            .store
            .parent()
            .unwrap()
            .join(status["workspace_id"].as_str().unwrap());
        let store = DurableStore::open_with_content(owner_root, &recorded.content_store).unwrap();
        assert_eq!(
            store.workspace_binding().unwrap().unwrap().workspace_id(),
            status["workspace_id"].as_str().unwrap()
        );
        h.destroy(status["workspace_id"].as_str().unwrap()).await;
    }
    assert_eq!(h.list().await, json!([]));
    assert_eq!(
        h.create(request("lazy")).await.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        h.fixture.resolves.load(Ordering::SeqCst),
        3,
        "queue loss must not leak workspace capacity"
    );
    let last = h.list().await;
    assert_failed(&last[0], &h.fixture);
    h.destroy(last[0]["workspace_id"].as_str().unwrap()).await;
    assert_eq!(
        observations.try_recv().unwrap().workspace_id,
        last[0]["workspace_id"].as_str().unwrap()
    );
    assert_eq!(
        observer.status().first_error,
        Some(WorkspaceObservationError::QueueFull)
    );
    h.assert_no_mount_directory();
    drop(h);
    drop(observer);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), observations.recv())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        observations.finish(),
        Err(WorkspaceObservationError::QueueFull)
    );
}

#[tokio::test]
async fn dropped_observation_receiver_does_not_abandon_the_real_failed_workspace() {
    let (observer, observations) =
        WorkspaceObserver::channel("11111111-2222-4333-8444-666666666666", 1).unwrap();
    drop(observations);
    let h =
        Harness::new_with_observer(RootFailure::Denied, false, 1, 2, Some(observer.clone())).await;
    assert_eq!(
        h.create(request("lazy")).await.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    let statuses = h.list().await;
    assert_eq!(statuses.as_array().unwrap().len(), 1);
    assert_failed(&statuses[0], &h.fixture);
    assert_eq!(h.fixture.resolves.load(Ordering::SeqCst), 1);
    assert!(h.fixture.resolve_ids.lock().unwrap()[0].is_some());
    assert_eq!(observer.status().accepted_records, 0);
    assert_eq!(
        observer.status().first_error,
        Some(WorkspaceObservationError::ReceiverDropped)
    );
    h.destroy(statuses[0]["workspace_id"].as_str().unwrap())
        .await;
    assert_eq!(h.list().await, json!([]));
    h.assert_no_mount_directory();
    h.fixture.assert_only_canonical_requests();
}

#[tokio::test]
async fn root_denial_and_bad_end_keep_fixed_failed_workspaces_without_mounting_or_dictionary_fallback(
) {
    for (failure, delivery) in [(RootFailure::Denied, "lazy"), (RootFailure::BadEnd, "full")] {
        let h = Harness::new(failure, false, 1, 2).await;
        let response = h.create(request(delivery)).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let error: Value = response.json().await.unwrap();
        assert_eq!(error["code"], "SNAPSHOT_ERROR");
        let message = error["message"].as_str().unwrap();
        match failure {
            RootFailure::Denied => {
                assert!(message.contains("ScopeForbidden: root metadata denied"))
            }
            RootFailure::BadEnd => {
                assert!(message.contains("END request_body_sha256 does not match the body sent"))
            }
        }
        let statuses = h.list().await;
        let entries = statuses.as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_failed(&entries[0], &h.fixture);
        let id = entries[0]["workspace_id"].as_str().unwrap();
        let status = h
            .client
            .get(format!("{}/v3/workspaces/{id}", h.api.url))
            .send()
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        assert_failed(&status.json::<Value>().await.unwrap(), &h.fixture);
        h.assert_no_mount_directory();
        assert_eq!(h.fixture.resolves.load(Ordering::SeqCst), 1);
        assert_eq!(h.fixture.metadata.load(Ordering::SeqCst), 1);
        h.fixture.assert_only_canonical_requests();
        h.destroy(id).await;
        assert_eq!(h.list().await, json!([]));
    }
}

#[tokio::test]
async fn cancelled_caller_retains_creation_and_workspace_capacity_until_failed_entry_is_destroyed()
{
    let h = Harness::new(RootFailure::Denied, true, 1, 2).await;
    let caller = h.cancelled_create();
    h.fixture.wait_for_resolve().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let rejected = h.create(request("lazy")).await;
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: Value = rejected.json().await.unwrap();
    assert_eq!(error["code"], "WORKSPACE_BUSY");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("workspace capacity"));
    assert_eq!(h.fixture.resolves.load(Ordering::SeqCst), 1);
    h.fixture.release_resolve();
    let failed = h.list().await;
    assert_eq!(failed.as_array().unwrap().len(), 1);
    assert_failed(&failed[0], &h.fixture);
    let id = failed[0]["workspace_id"].as_str().unwrap();
    h.destroy(id).await;
    assert_eq!(h.list().await, json!([]));

    // Destroy releases the actual retained workspace permit, permitting a new
    // upstream resolve rather than permanently leaking capacity on failure.
    let next_service = h.service.clone();
    let next = tokio::spawn(async move {
        next_service
            .create(serde_json::from_value(request("lazy")).unwrap())
            .await
    });
    h.fixture.wait_for_resolve().await;
    h.fixture.release_resolve();
    assert!(next.await.unwrap().is_err());
    assert_eq!(h.fixture.resolves.load(Ordering::SeqCst), 2);
    let failed = h.list().await;
    h.destroy(failed[0]["workspace_id"].as_str().unwrap()).await;
    h.assert_no_mount_directory();
    h.fixture.assert_only_canonical_requests();
}

#[tokio::test]
async fn operation_capacity_follows_cancelled_owned_creation_and_rejected_create_returns_its_workspace_permit(
) {
    let h = Harness::new(RootFailure::Denied, true, 2, 1).await;
    let caller = h.cancelled_create();
    h.fixture.wait_for_resolve().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let rejected = h.create(request("lazy")).await;
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: Value = rejected.json().await.unwrap();
    assert_eq!(error["code"], "WORKSPACE_BUSY");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("operation capacity"));
    assert_eq!(h.fixture.resolves.load(Ordering::SeqCst), 1);
    h.fixture.release_resolve();
    let failed = h.list().await;
    assert_eq!(failed.as_array().unwrap().len(), 1);
    assert_failed(&failed[0], &h.fixture);

    // Keep the first failed entry to consume one workspace permit. A second
    // admitted create proves that the rejected one returned its temporary
    // workspace reservation when its operation admission failed.
    let next_service = h.service.clone();
    let next = tokio::spawn(async move {
        next_service
            .create(serde_json::from_value(request("lazy")).unwrap())
            .await
    });
    h.fixture.wait_for_resolve().await;
    h.fixture.release_resolve();
    assert!(next.await.unwrap().is_err());
    let failed = h.list().await;
    assert_eq!(failed.as_array().unwrap().len(), 2);
    for status in failed.as_array().unwrap() {
        h.destroy(status["workspace_id"].as_str().unwrap()).await;
    }
    assert_eq!(h.list().await, json!([]));
    h.fixture.assert_only_canonical_requests();
}

#[tokio::test]
async fn shutdown_waits_for_the_cancelled_callers_admitted_create_and_rejects_new_creations() {
    let h = Harness::new(RootFailure::Denied, true, 2, 2).await;
    let caller = h.cancelled_create();
    h.fixture.wait_for_resolve().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let service = h.service.clone();
    let (started, started_rx) = tokio::sync::oneshot::channel();
    let shutdown = tokio::spawn(async move {
        let cleanup = service.shutdown_cleanup();
        tokio::pin!(cleanup);
        assert!(futures::poll!(cleanup.as_mut()).is_pending());
        started.send(()).unwrap();
        cleanup.await
    });
    tokio::time::timeout(Duration::from_secs(5), started_rx)
        .await
        .unwrap()
        .unwrap();
    let error = h
        .service
        .create(serde_json::from_value::<CreateWorkspace>(request("lazy")).unwrap())
        .await
        .unwrap_err();
    assert_eq!(error.code, "WORKSPACE_BUSY");
    assert!(error.message.contains("shutting down"));
    assert!(
        !shutdown.is_finished(),
        "shutdown dropped an admitted owned operation"
    );
    assert_eq!(h.fixture.metadata.load(Ordering::SeqCst), 0);
    h.fixture.release_resolve();
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(h.fixture.metadata.load(Ordering::SeqCst), 1);
    assert_eq!(h.fixture.resolves.load(Ordering::SeqCst), 1);
    let rejected = h.create(request("lazy")).await;
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        rejected.json::<Value>().await.unwrap()["code"],
        "WORKSPACE_BUSY"
    );
    h.assert_no_mount_directory();
    h.fixture.assert_only_canonical_requests();
}

#[tokio::test]
async fn v3_json_rejects_revision_and_dictionary_selection_before_any_upstream_work() {
    let h = Harness::new(RootFailure::Denied, false, 2, 2).await;
    let mut revision = request("lazy");
    revision["base_revision"] = "old-git-revision".into();
    let mut dictionary_target = request("lazy");
    dictionary_target["target"] = json!({"kind":"dictionary"});
    let mut dictionary_mode = request("lazy");
    dictionary_mode["mode"] = "dictionary".into();
    let mut dictionary_upper = request("lazy");
    dictionary_upper["upper_policy"] = "dictionary".into();
    for invalid in [
        revision,
        dictionary_target,
        dictionary_mode,
        dictionary_upper,
    ] {
        assert_eq!(
            h.create(invalid).await.status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    assert_eq!(h.list().await, json!([]));
    assert!(h.fixture.requests.lock().unwrap().is_empty());
    h.assert_no_mount_directory();
}

#[tokio::test]
async fn failed_root_cannot_claim_a_host_created_empty_upper_and_cleanup_retries_after_its_removal()
{
    let h = Harness::new(RootFailure::Denied, false, 1, 2).await;
    assert_eq!(
        h.create(request("lazy")).await.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    let initial = h.list().await;
    assert_eq!(initial.as_array().unwrap().len(), 1);
    assert_failed(&initial[0], &h.fixture);
    assert_eq!(initial[0]["local_pin_state"], "incomplete");
    h.assert_no_mount_directory();
    let id = initial[0]["workspace_id"].as_str().unwrap();
    uuid::Uuid::parse_str(id).unwrap();
    let directory = h.temp.path().join("workspaces").join(id);
    let upper = directory.join("upper");
    let foreign = directory.join("foreign-owner.txt");
    // The actual service failed before creating its private directory. This
    // empty upper therefore belongs to the host, even with the expected name.
    // Emptiness alone must not supply the missing ownership witness.
    std::fs::create_dir(&directory).unwrap();
    std::fs::create_dir(&upper).unwrap();
    std::fs::write(&foreign, b"host ownership must be retained").unwrap();
    assert!(std::fs::read_dir(&upper).unwrap().next().is_none());

    let response = h
        .client
        .get(format!("{}/v3/workspaces/{id}", h.api.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let status: Value = response.json().await.unwrap();
    assert_failed(&status, &h.fixture);
    assert_eq!(status["dirty_state"], "unknown");
    assert_eq!(status["local_pin_state"], "incomplete");
    assert_eq!(status["generation"], initial[0]["generation"]);

    for policy in [json!({}), json!({"discard_dirty": true})] {
        let response = h
            .client
            .post(format!("{}/v3/workspaces/{id}/destroy", h.api.url))
            .json(&policy)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.json::<Value>().await.unwrap()["code"],
            "WORKSPACE_UNKNOWN"
        );
        assert!(upper.is_dir());
        assert!(std::fs::read_dir(&upper).unwrap().next().is_none());
        assert_eq!(
            std::fs::read(&foreign).unwrap(),
            b"host ownership must be retained"
        );
        let retained = h.list().await;
        assert_eq!(retained.as_array().unwrap().len(), 1);
        assert_eq!(retained[0]["workspace_id"], id);
        assert_eq!(retained[0]["generation"], initial[0]["generation"]);
        assert_eq!(retained[0]["local_pin_state"], "incomplete");
    }
    let rejected = h.create(request("lazy")).await;
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        rejected.json::<Value>().await.unwrap()["code"],
        "WORKSPACE_BUSY"
    );
    assert_eq!(h.fixture.resolves.load(Ordering::SeqCst), 1);

    // Only the fixture removes its foreign directory. The failed service can
    // then retire the original retained entry, revoke its own incomplete pin,
    // and return capacity without adopting or deleting host-owned state.
    std::fs::remove_file(&foreign).unwrap();
    std::fs::remove_dir(&upper).unwrap();
    std::fs::remove_dir(&directory).unwrap();
    h.destroy(id).await;
    assert_eq!(h.list().await, json!([]));
    assert_eq!(
        h.create(request("lazy")).await.status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    let next = h.list().await;
    assert_eq!(next.as_array().unwrap().len(), 1);
    assert_failed(&next[0], &h.fixture);
    assert_ne!(next[0]["workspace_id"], id);
    assert_eq!(h.fixture.resolves.load(Ordering::SeqCst), 2);
    assert_eq!(h.fixture.metadata.load(Ordering::SeqCst), 2);
    h.destroy(next[0]["workspace_id"].as_str().unwrap()).await;
    assert_eq!(h.list().await, json!([]));
    h.assert_no_mount_directory();
    h.fixture.assert_only_canonical_requests();
}
