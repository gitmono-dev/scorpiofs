//! Workspace full hydration uses fixed, bounded OBJECT and streaming lanes.

use crate::snapshot::{DurableStore, HydrateReport, SnapshotError, SnapshotReader};

pub(crate) async fn hydrate_workspace(
    store: &DurableStore,
    reader: &SnapshotReader,
) -> Result<HydrateReport, SnapshotError> {
    if !reader.capabilities().features.objects {
        return store.hydrate_snapshot(reader).await;
    }
    store.bind_reader(reader)?;
    let closure = reader.snapshot_closure().await?;
    let batches = reader.clone();
    let large = reader.clone();
    store
        .hydrate_snapshot_content_batches(
            reader,
            &closure,
            2,
            2,
            move |files| {
                let reader = batches.clone();
                Box::pin(async move { reader.read_content_batch(&files).await })
            },
            move |file| {
                let reader = large.clone();
                // The durable core bypasses this compatibility callback for
                // chunk-capable large files and streams verified ranges to CAS.
                Box::pin(async move { reader.read_content(&file, false).await })
            },
        )
        .await
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    };

    use axum::{
        body::{Body, Bytes},
        extract::{Path, Query, State},
        response::Response,
        routing::{get, post},
        Json, Router,
    };
    use mst2_codec::{
        chunkmap::{ChunkLeaf, ChunkMap, CHUNK_SIZE},
        descriptor::ServingDescriptor,
        metapage::{page_id, Entry, EntryKind, Page},
        treeframe::{ChunkPayload, EndPayload, MetaPayload, ObjectPayload},
    };
    use serde_json::{json, Value};
    use tokio::sync::{Barrier, Notify};

    use super::*;
    use crate::snapshot::{
        durable::digest_of, frames::parse_digest, CompletionKind, LocalPinState, Mst2Client,
    };

    fn hash(bytes: &[u8]) -> [u8; 32] {
        parse_digest(&digest_of(bytes)).unwrap()
    }
    fn id(bytes: &[u8; 32]) -> String {
        format!("sha256:{}", hex::encode(bytes))
    }
    fn lookup_path(path: &str) -> String {
        format!("/{}", path.trim_start_matches('/'))
    }
    fn base64(bytes: &[u8]) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for part in bytes.chunks(3) {
            let bits = ((part[0] as u32) << 16)
                | ((part.get(1).copied().unwrap_or(0) as u32) << 8)
                | part.get(2).copied().unwrap_or(0) as u32;
            out.push(TABLE[(bits >> 18) as usize] as char);
            out.push(TABLE[((bits >> 12) & 63) as usize] as char);
            out.push(if part.len() > 1 {
                TABLE[((bits >> 6) & 63) as usize] as char
            } else {
                '='
            });
            out.push(if part.len() > 2 {
                TABLE[(bits & 63) as usize] as char
            } else {
                '='
            });
        }
        out
    }
    struct Fixture {
        descriptor: ServingDescriptor,
        pages: BTreeMap<String, Vec<u8>>,
        bodies: BTreeMap<String, Vec<u8>>,
        maps: BTreeMap<String, (ChunkMap, ChunkLeaf)>,
        objects: bool,
        object_gate: Option<Barrier>,
        large_gate: Option<Barrier>,
        hold_objects: AtomicBool,
        objects_started: Notify,
        objects_release: Notify,
        object_calls: AtomicUsize,
        object_active: AtomicUsize,
        object_peak: AtomicUsize,
        chunk_calls: AtomicUsize,
        chunk_active: AtomicUsize,
        chunk_peak: AtomicUsize,
        raw_calls: AtomicUsize,
    }
    impl Fixture {
        fn new(objects: bool, gates: bool, include_large: bool) -> Self {
            let mut pages = BTreeMap::new();
            let mut bodies = BTreeMap::new();
            let mut maps = BTreeMap::new();
            let mut root = Vec::new();
            for directory in 0..3 {
                let mut entries = Vec::new();
                for file in 0..64 {
                    let name = format!("f{file:03}");
                    let body = format!("body-{directory}-{file}").into_bytes();
                    entries.push(Entry::file(
                        EntryKind::Regular,
                        name.as_bytes(),
                        body.len() as u64,
                        hash(&body),
                    ));
                    bodies.insert(format!("/d{directory}/{name}"), body);
                }
                let bytes = Page::build(&entries).unwrap();
                root.push(Entry::dir(
                    format!("d{directory}").as_bytes(),
                    page_id(&bytes),
                ));
                pages.insert(format!("/d{directory}"), bytes);
            }
            if include_large {
                for index in 0..2 {
                    let path = format!("/large{index}");
                    let bytes = vec![0x51 + index as u8; 2 * CHUNK_SIZE as usize + 7];
                    let leaf = ChunkLeaf {
                        page_index: 0,
                        chunk_sha256: bytes.chunks(CHUNK_SIZE as usize).map(hash).collect(),
                    };
                    let map =
                        ChunkMap::new(hash(&bytes), bytes.len() as u64, leaf.leaf_hash().unwrap())
                            .unwrap();
                    root.push(Entry::file(
                        EntryKind::Regular,
                        format!("large{index}").as_bytes(),
                        bytes.len() as u64,
                        hash(&bytes),
                    ));
                    maps.insert(path.clone(), (map, leaf));
                    bodies.insert(path, bytes);
                }
            }
            let bytes = Page::build(&root).unwrap();
            let root = page_id(&bytes);
            pages.insert("/".into(), bytes);
            Self {
                descriptor: ServingDescriptor {
                    instance_uuid: *uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555")
                        .unwrap()
                        .as_bytes(),
                    namespace_view_id: [0x22; 32],
                    scope: "/project".into(),
                    metadata_root: root,
                },
                pages,
                bodies,
                maps,
                objects,
                object_gate: (objects && gates).then(|| Barrier::new(2)),
                large_gate: (gates && include_large).then(|| Barrier::new(2)),
                hold_objects: AtomicBool::new(false),
                objects_started: Notify::new(),
                objects_release: Notify::new(),
                object_calls: AtomicUsize::new(0),
                object_active: AtomicUsize::new(0),
                object_peak: AtomicUsize::new(0),
                chunk_calls: AtomicUsize::new(0),
                chunk_active: AtomicUsize::new(0),
                chunk_peak: AtomicUsize::new(0),
                raw_calls: AtomicUsize::new(0),
            }
        }
        fn sid(&self) -> String {
            id(&self.descriptor.snapshot_id().unwrap())
        }
        fn frame(
            &self,
            request: &[u8],
            mut bytes: Vec<u8>,
            items: usize,
            units: usize,
            logical: usize,
        ) -> Response {
            bytes.extend(
                EndPayload {
                    request_item_count: items as u32,
                    unique_unit_count: units as u32,
                    logical_bytes: logical as u64,
                    request_body_sha256: hash(request),
                }
                .encode(7, 1),
            );
            Response::builder()
                .header("content-type", "application/vnd.mega.treeframe;version=2")
                .header("x-mega-snapshot-id", self.sid())
                .header("x-mega-request-digest", digest_of(request))
                .body(Body::from(bytes))
                .unwrap()
        }
    }
    struct Active<'a>(&'a AtomicUsize);
    impl Drop for Active<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    fn active<'a>(counter: &'a AtomicUsize, peak: &AtomicUsize) -> Active<'a> {
        let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(count, Ordering::SeqCst);
        Active(counter)
    }
    async fn caps(State(f): State<Arc<Fixture>>) -> Json<Value> {
        let mut value: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/mst2_capabilities_0_2_1.json"
        ))
        .unwrap();
        value["features"]["small_objects"] = json!(f.objects);
        Json(value)
    }
    async fn resolve(State(f): State<Arc<Fixture>>) -> Json<Value> {
        Json(
            json!({"descriptor":{"schema_version":2,"metadata_codec":1,"instance_id":uuid::Uuid::from_bytes(f.descriptor.instance_uuid).to_string(),"namespace_view_id":id(&f.descriptor.namespace_view_id),"scope":"/project","materialization_policy":1,"fs_semantics":1,"access_projection":0,"metadata_root":id(&f.descriptor.metadata_root),"snapshot_id":f.sid()},"publication_sequence":"7","writer_epoch":"3","lease_id":"hydrate-lease","lease_expires_at":"2099-01-01T00:00:00Z","authorization_epoch":"11","resolved_at":"2026-10-05T00:00:00Z","delivery":"full"}),
        )
    }
    async fn metadata(
        State(f): State<Arc<Fixture>>,
        Path(sid): Path<String>,
        body: Bytes,
    ) -> Response {
        assert_eq!(sid, f.sid());
        let value: Value = serde_json::from_slice(&body).unwrap();
        let items = value["items"].as_array().unwrap();
        let mut unique = BTreeMap::new();
        for item in items {
            assert_eq!(item["route"], json!([]));
            let bytes = &f.pages[item["directory_path"].as_str().unwrap()];
            assert_eq!(item["expected_digest"], id(&page_id(bytes)));
            unique.insert(page_id(bytes), bytes.clone());
        }
        let pages: Vec<_> = unique.into_iter().collect();
        let logical = pages.iter().map(|(_, bytes)| bytes.len()).sum();
        f.frame(
            &body,
            MetaPayload {
                pages: pages.clone(),
            }
            .encode(7, 0)
            .unwrap(),
            items.len(),
            pages.len(),
            logical,
        )
    }
    async fn objects(
        State(f): State<Arc<Fixture>>,
        Path(sid): Path<String>,
        body: Bytes,
    ) -> Response {
        assert_eq!(sid, f.sid());
        assert!(f.objects);
        f.object_calls.fetch_add(1, Ordering::SeqCst);
        let _active = active(&f.object_active, &f.object_peak);
        f.objects_started.notify_one();
        if let Some(gate) = &f.object_gate {
            gate.wait().await;
        }
        let release = f.objects_release.notified();
        tokio::pin!(release);
        release.as_mut().enable();
        if f.hold_objects.load(Ordering::SeqCst) {
            release.await;
        }
        let value: Value = serde_json::from_slice(&body).unwrap();
        let items = value["items"].as_array().unwrap();
        let mut units = Vec::new();
        for item in items {
            let bytes = &f.bodies[&lookup_path(item["path"].as_str().unwrap())];
            assert_eq!(item["expected_digest"], digest_of(bytes));
            units.push((hash(bytes), bytes.clone()));
        }
        let logical = units.iter().map(|(_, bytes)| bytes.len()).sum();
        f.frame(
            &body,
            ObjectPayload { objects: units }.encode(7, 0).unwrap(),
            items.len(),
            items.len(),
            logical,
        )
    }
    async fn map(
        State(f): State<Arc<Fixture>>,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> Json<Value> {
        let (map, _) = &f.maps[&lookup_path(&query["path"])];
        Json(
            json!({"snapshot_id":f.sid(),"path":query["path"],"map":{"schema_version":2,"file_content_id":id(&map.file_content_id),"file_size":map.file_size.to_string(),"chunk_size":CHUNK_SIZE,"chunk_count":map.chunk_count.to_string(),"page_count":"1","pages_root":id(&map.pages_root),"map_id":id(&map.map_id())}}),
        )
    }
    async fn leaf(
        State(f): State<Arc<Fixture>>,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> Json<Value> {
        let (map, leaf) = &f.maps[&lookup_path(&query["path"])];
        assert_eq!(query["map_id"], id(&map.map_id()));
        assert_eq!(query["page_index"], "0");
        Json(
            json!({"map_id":id(&map.map_id()),"page_index":"0","leaf_base64":base64(&leaf.encode().unwrap()),"proof":[]}),
        )
    }
    async fn chunks(State(f): State<Arc<Fixture>>, body: Bytes) -> Response {
        let value: Value = serde_json::from_slice(&body).unwrap();
        let items = value["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        let item = &items[0];
        let path = lookup_path(item["path"].as_str().unwrap());
        let (map, _) = &f.maps[&path];
        let index: usize = item["chunk_index"].as_str().unwrap().parse().unwrap();
        let bytes = f.bodies[&path]
            .chunks(CHUNK_SIZE as usize)
            .nth(index)
            .unwrap();
        assert_eq!(item["expected_digest"], id(&map.file_content_id));
        assert_eq!(item["map_id"], id(&map.map_id()));
        f.chunk_calls.fetch_add(1, Ordering::SeqCst);
        let _active = active(&f.chunk_active, &f.chunk_peak);
        if index == 0 {
            if let Some(gate) = &f.large_gate {
                gate.wait().await;
            }
        }
        f.frame(
            &body,
            ChunkPayload {
                map_id: map.map_id(),
                file_content_id: map.file_content_id,
                chunk_index: index as u64,
                chunk_bytes: bytes.to_vec(),
            }
            .encode(7, 0)
            .unwrap(),
            1,
            1,
            bytes.len(),
        )
    }
    async fn blob(
        State(f): State<Arc<Fixture>>,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> Response {
        f.raw_calls.fetch_add(1, Ordering::SeqCst);
        let bytes = &f.bodies[&lookup_path(&query["path"])];
        Response::builder()
            .header("content-type", "application/octet-stream")
            .body(Body::from(bytes.clone()))
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
            self.fixture.hold_objects.store(false, Ordering::SeqCst);
            self.fixture.objects_release.notify_waiters();
        }
    }
    impl Server {
        async fn new(fixture: Fixture) -> Self {
            let fixture = Arc::new(fixture);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
            let app = Router::new()
                .route("/api/v2/snapshots/capabilities", get(caps))
                .route("/api/v2/snapshots/resolve", post(resolve))
                .route("/api/v2/snapshots/{sid}/metadata/pages", post(metadata))
                .route("/api/v2/snapshots/{sid}/objects", post(objects))
                .route("/api/v2/snapshots/{sid}/chunk-map", get(map))
                .route("/api/v2/snapshots/{sid}/chunk-map/pages", get(leaf))
                .route("/api/v2/snapshots/{sid}/chunks", post(chunks))
                .route("/api/v2/snapshots/{sid}/blob", get(blob))
                .with_state(fixture.clone());
            let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self {
                fixture,
                client,
                task,
            }
        }
        async fn reader(&self) -> SnapshotReader {
            SnapshotReader::resolve_request(
                self.client.clone(),
                &crate::snapshot::ResolveRequest::latest("/project", 60),
            )
            .await
            .unwrap()
        }
    }

    #[tokio::test]
    async fn actual_http_hydration_uses_two_batch_and_two_streaming_lanes_then_shares_cas() {
        let server = Server::new(Fixture::new(true, true, true)).await;
        let reader = server.reader().await;
        let temp = tempfile::tempdir().unwrap();
        let first = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555501",
            &reader,
        )
        .unwrap();
        let report = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            hydrate_workspace(&first, &reader),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(report.completion_kind, CompletionKind::FullSnapshot);
        assert!(report.complete);
        assert_eq!(report.fetched, 194);
        assert_eq!(server.fixture.object_peak.load(Ordering::SeqCst), 2);
        assert_eq!(server.fixture.chunk_peak.load(Ordering::SeqCst), 2);
        assert_eq!(server.fixture.raw_calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.fixture.chunk_calls.load(Ordering::SeqCst), 6);
        let second_reader = server.reader().await;
        let second = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555502",
            &second_reader,
        )
        .unwrap();
        let before = (
            server.fixture.object_calls.load(Ordering::SeqCst),
            server.fixture.chunk_calls.load(Ordering::SeqCst),
        );
        let report = hydrate_workspace(&second, &second_reader).await.unwrap();
        assert!(report.complete);
        assert_eq!(report.fetched, 0);
        assert_eq!(report.resumed, 194);
        assert_eq!(
            (
                server.fixture.object_calls.load(Ordering::SeqCst),
                server.fixture.chunk_calls.load(Ordering::SeqCst)
            ),
            before
        );
        assert_eq!(
            first.local_pin_state().unwrap(),
            LocalPinState::Complete(CompletionKind::FullSnapshot)
        );
        assert_eq!(
            second.local_pin_state().unwrap(),
            LocalPinState::Complete(CompletionKind::FullSnapshot)
        );
    }

    #[tokio::test]
    async fn cancelling_a_real_pending_batch_cannot_publish_complete_and_can_retry() {
        let fixture = Fixture::new(true, false, false);
        fixture.hold_objects.store(true, Ordering::SeqCst);
        let server = Server::new(fixture).await;
        let reader = server.reader().await;
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            DurableStore::open_for_workspace(
                temp.path(),
                "11111111-2222-4333-8444-555555555503",
                &reader,
            )
            .unwrap(),
        );
        let source = reader.clone();
        let target = store.clone();
        let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.fixture.objects_started.notified(),
        )
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!store.is_snapshot_complete().unwrap());
        assert!(!matches!(
            store.local_pin_state().unwrap(),
            LocalPinState::Complete(_)
        ));
        assert_eq!(reader.content_usage().output_bytes, 0);
        assert_eq!(reader.content_usage().construction_bytes, 0);
        server.fixture.hold_objects.store(false, Ordering::SeqCst);
        server.fixture.objects_release.notify_waiters();
        assert!(hydrate_workspace(&store, &reader).await.unwrap().complete);
    }

    #[tokio::test]
    async fn absent_object_capability_uses_existing_hydration_without_object_requests() {
        let server = Server::new(Fixture::new(false, false, false)).await;
        let reader = server.reader().await;
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555504",
            &reader,
        )
        .unwrap();
        let report = hydrate_workspace(&store, &reader).await.unwrap();
        assert!(report.complete);
        assert_eq!(report.completion_kind, CompletionKind::FullSnapshot);
        assert_eq!(server.fixture.object_calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.fixture.raw_calls.load(Ordering::SeqCst), 192);
    }
}
