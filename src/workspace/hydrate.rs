//! Workspace full hydration uses fixed, bounded OBJECT and streaming lanes.

use crate::snapshot::{
    durable::{tag_hydration_error, HydrationSubstage},
    stage::{trace_async, trace_blocking, trace_sync},
    DurableStore, HydrateReport, IncrementalSync, ScopeCache, SnapshotError, SnapshotErrorCode,
    SnapshotReader,
};

pub(crate) async fn hydrate_workspace(
    store: &DurableStore,
    reader: &SnapshotReader,
) -> Result<HydrateReport, SnapshotError> {
    trace_sync("hydrate_bind", || store.bind_reader(reader))?;
    // The owned store has already checked the scope, authorization domain and
    // fixed SID. Reuse lives beside its shared CAS, never in another owner's
    // private metadata directory or a caller-selected unbound cache.
    if store.workspace_binding()?.is_none() {
        return Err(SnapshotError::new(
            SnapshotErrorCode::InvalidRequest,
            "workspace hydration requires an owned store",
        ));
    }
    let scope = store.content_dir().parent().ok_or_else(|| {
        SnapshotError::new(SnapshotErrorCode::Internal, "workspace CAS has no scope")
    })?;
    reader.authorized_context().bind_scope_cache(scope)?;
    let cache = ScopeCache::open(scope)?;
    let mut sync = IncrementalSync::new(reader, &cache);
    let closure = trace_async("hydrate_metadata_closure", sync.sync_snapshot())
        .await
        .map_err(|error| tag_hydration_error(error, HydrationSubstage::MetadataClosure))?;
    tracing::debug!(
        target: "scorpiofs::workspace::performance",
        metadata_sync = ?sync.meters(),
        full_root_proof = ?sync.closure_meters(),
        "workspace metadata acquired with full namespace proof"
    );
    // Retention proves metadata reachability independently of completeness.
    // It allows pressure recovery before full-body capacity is reserved.
    let owned_store = store.clone();
    let (closure, retained) = trace_blocking("hydrate_retain_metadata_root", move || {
        let retained = owned_store.retain_snapshot_root(&closure);
        (closure, retained)
    })
    .await
    .map_err(|_| SnapshotError::new(SnapshotErrorCode::Internal, "cache root worker failed"))?;
    retained?;
    if super::cache::pressure(scope.to_path_buf()).await? {
        // Busy or unknown sibling owners remain protected. The capacity
        // admission below still decides whether this hydration can proceed.
        if let Err(error) = super::cache::collect(scope.to_path_buf()).await {
            tracing::debug!(target: "scorpiofs::workspace::performance", code = ?error.code,
                "pre-hydration collection deferred");
        }
    }
    // sync_snapshot has dropped the scope-index transaction before hydration
    // acquires this owner's durable publication lock. Cached pages remain
    // hints; this exact fixed root has been proved including empty directories.
    if !reader.capabilities().features.objects {
        let result = trace_async(
            "hydrate_without_objects",
            store.hydrate_snapshot_from_closure(reader, &closure),
        )
        .await;
        store.trace_verification_meters("hydration_without_objects");
        return result
            .map_err(|error| tag_hydration_error(error, HydrationSubstage::HydrationCommit));
    }
    let batches = reader.clone();
    let large = reader.clone();
    let result = trace_async(
        "hydrate_content_and_commit",
        store.hydrate_snapshot_content_batches(
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
        ),
    )
    .await;
    store.trace_verification_meters("hydration");
    result.map_err(|error| tag_hydration_error(error, HydrationSubstage::HydrationCommit))
}

#[cfg(test)]
#[path = "."]
mod tests {
    #[path = "hydrate_prefetch_tests.rs"]
    mod ordered_prefetch;

    #[path = "hydrate_owned_proof_tests.rs"]
    mod owned_proof;

    #[path = "hydrate_full_proof_tests.rs"]
    mod full_proof;

    #[path = "cache_pressure_tests.rs"]
    mod cache_pressure;

    #[path = "lazy_metadata_learning_tests.rs"]
    mod lazy_metadata_learning;

    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    };

    use axum::{
        body::{Body, Bytes},
        extract::{Path, Query, State},
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::{get, post},
        Json, Router,
    };
    use mst2_codec::{
        chunkmap::{ChunkLeaf, ChunkMap, CHUNK_SIZE},
        descriptor::ServingDescriptor,
        metapage::{page_id, Entry, EntryKind, Page},
        treeframe::{ChunkPayload, EndPayload, MetaPayload, ObjectPayload, OBJECT_MAX_RAW},
    };
    use serde_json::{json, Value};
    use tokio::sync::{Barrier, Mutex, Notify};

    use super::*;
    use crate::snapshot::{
        durable::{digest_of, CasVerificationReason},
        frames::parse_digest,
        CompletionKind, ContentBudgetLimits, FetchCoordinator, LocalPinState, Mst2Client,
    };

    fn hash(bytes: &[u8]) -> [u8; 32] {
        parse_digest(&digest_of(bytes)).unwrap()
    }
    fn id(bytes: &[u8; 32]) -> String {
        format!("sha256:{}", hex::encode(bytes))
    }
    fn checked_wire_path(path: &str) -> Result<&str, (StatusCode, Json<Value>)> {
        if mst2_codec::descriptor::validate_scope(path).is_err() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(
                    json!({"error":{"code":"INVALID_REQUEST","message":"invalid scope-relative HTTP path","request_id":"strict-content-path","retryable":false}}),
                ),
            ));
        }
        Ok(path)
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
        authorization_epoch: u64,
        object_gate: Option<Barrier>,
        large_gate: Option<Barrier>,
        hold_objects: AtomicBool,
        objects_started: Notify,
        objects_release: Notify,
        fail_objects: AtomicBool,
        object_calls: AtomicUsize,
        object_active: AtomicUsize,
        object_peak: AtomicUsize,
        object_max_bytes: AtomicUsize,
        chunk_calls: AtomicUsize,
        map_calls: AtomicUsize,
        leaf_calls: AtomicUsize,
        chunk_active: AtomicUsize,
        chunk_peak: AtomicUsize,
        hold_chunks: AtomicBool,
        chunks_release: Notify,
        fail_chunks: AtomicBool,
        request_active: AtomicUsize,
        request_peak: AtomicUsize,
        raw_calls: AtomicUsize,
        metadata_calls: AtomicUsize,
        metadata_pages: AtomicUsize,
        metadata_wire_bytes: AtomicUsize,
        hold_metadata: AtomicBool,
        metadata_started: Notify,
        metadata_release: Notify,
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
                authorization_epoch: 11,
                object_gate: (objects && gates).then(|| Barrier::new(2)),
                large_gate: (gates && include_large).then(|| Barrier::new(2)),
                hold_objects: AtomicBool::new(false),
                objects_started: Notify::new(),
                objects_release: Notify::new(),
                fail_objects: AtomicBool::new(false),
                object_calls: AtomicUsize::new(0),
                object_active: AtomicUsize::new(0),
                object_peak: AtomicUsize::new(0),
                object_max_bytes: AtomicUsize::new(0),
                chunk_calls: AtomicUsize::new(0),
                map_calls: AtomicUsize::new(0),
                leaf_calls: AtomicUsize::new(0),
                chunk_active: AtomicUsize::new(0),
                chunk_peak: AtomicUsize::new(0),
                hold_chunks: AtomicBool::new(false),
                chunks_release: Notify::new(),
                fail_chunks: AtomicBool::new(false),
                request_active: AtomicUsize::new(0),
                request_peak: AtomicUsize::new(0),
                raw_calls: AtomicUsize::new(0),
                metadata_calls: AtomicUsize::new(0),
                metadata_pages: AtomicUsize::new(0),
                metadata_wire_bytes: AtomicUsize::new(0),
                hold_metadata: AtomicBool::new(false),
                metadata_started: Notify::new(),
                metadata_release: Notify::new(),
            }
        }
        fn near_budget_mix() -> Self {
            let mut fixture = Self::new(true, false, true);
            fixture.bodies.retain(|path, _| path.starts_with("/large"));
            let mut root = Vec::new();
            // 14 MiB of unique small objects fills two 7-MiB requests.
            // Each request must use legal bounded OBJECT frames.
            for (directory, files) in [24, 24, 8].into_iter().enumerate() {
                let mut entries = Vec::new();
                for file in 0..files {
                    let name = format!("f{file:03}");
                    let mut bytes = vec![directory as u8; 256 * 1024];
                    bytes[0] = file as u8;
                    entries.push(Entry::file(
                        EntryKind::Regular,
                        name.as_bytes(),
                        bytes.len() as u64,
                        hash(&bytes),
                    ));
                    fixture
                        .bodies
                        .insert(format!("/d{directory}/{name}"), bytes);
                }
                let bytes = Page::build(&entries).unwrap();
                root.push(Entry::dir(
                    format!("d{directory}").as_bytes(),
                    page_id(&bytes),
                ));
                fixture.pages.insert(format!("/d{directory}"), bytes);
            }
            for name in ["large0", "large1"] {
                let bytes = &fixture.bodies[&format!("/{name}")];
                root.push(Entry::file(
                    EntryKind::Regular,
                    name.as_bytes(),
                    bytes.len() as u64,
                    hash(bytes),
                ));
            }
            let bytes = Page::build(&root).unwrap();
            fixture.descriptor.metadata_root = page_id(&bytes);
            fixture.pages.insert("/".into(), bytes);
            fixture
        }
        fn update_version(objects: bool, version: u8) -> Self {
            let mut fixture = Self::new(objects, false, false);
            fixture.descriptor.namespace_view_id = [0x22 + version; 32];
            if version >= 1 {
                fixture
                    .bodies
                    .insert("/d0/f000".into(), b"new single-file bytes".to_vec());
                let entries: Vec<_> = fixture
                    .bodies
                    .iter()
                    .filter_map(|(path, bytes)| {
                        path.strip_prefix("/d0/").map(|name| {
                            Entry::file(
                                EntryKind::Regular,
                                name.as_bytes(),
                                bytes.len() as u64,
                                hash(bytes),
                            )
                        })
                    })
                    .collect();
                fixture
                    .pages
                    .insert("/d0".into(), Page::build(&entries).unwrap());
            }
            if version >= 2 {
                let bytes = fixture.pages.remove("/d1").unwrap();
                fixture.pages.insert("/moved".into(), bytes);
                let moved: Vec<_> = fixture
                    .bodies
                    .iter()
                    .filter_map(|(path, bytes)| {
                        path.strip_prefix("/d1/")
                            .map(|name| (path.clone(), format!("/moved/{name}"), bytes.clone()))
                    })
                    .collect();
                for (previous, next, bytes) in moved {
                    fixture.bodies.remove(&previous);
                    fixture.bodies.insert(next, bytes);
                }
            }
            let empty = Page::build(&[]).unwrap();
            fixture.pages.insert("/empty-a".into(), empty.clone());
            fixture.pages.insert("/empty-b".into(), empty);
            for name in ["alias-a", "alias-b"] {
                fixture
                    .pages
                    .insert(format!("/{name}"), fixture.pages["/d2"].clone());
                let aliases: Vec<_> = fixture
                    .bodies
                    .iter()
                    .filter_map(|(path, bytes)| {
                        path.strip_prefix("/d2/")
                            .map(|suffix| (format!("/{name}/{suffix}"), bytes.clone()))
                    })
                    .collect();
                fixture.bodies.extend(aliases);
            }
            let entries: Vec<_> = fixture
                .pages
                .iter()
                .filter(|(path, _)| path.as_str() != "/")
                .map(|(path, bytes)| Entry::dir(&path.as_bytes()[1..], page_id(bytes)))
                .collect();
            let root = Page::build(&entries).unwrap();
            fixture.descriptor.metadata_root = page_id(&root);
            fixture.pages.insert("/".into(), root);
            fixture
        }
        fn sid(&self) -> String {
            id(&self.descriptor.snapshot_id().unwrap())
        }
        fn frame(
            &self,
            request: &[u8],
            bytes: Vec<u8>,
            items: usize,
            units: usize,
            logical: usize,
        ) -> Response {
            self.frame_response(
                request,
                self.frame_bytes(request, bytes, items, units, logical),
            )
        }
        fn frame_bytes(
            &self,
            request: &[u8],
            mut bytes: Vec<u8>,
            items: usize,
            units: usize,
            logical: usize,
        ) -> Vec<u8> {
            bytes.extend(
                EndPayload {
                    request_item_count: items as u32,
                    unique_unit_count: units as u32,
                    logical_bytes: logical as u64,
                    request_body_sha256: hash(request),
                }
                .encode(7, 1),
            );
            bytes
        }
        fn frame_response(&self, request: &[u8], bytes: Vec<u8>) -> Response {
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
            json!({"descriptor":{"schema_version":2,"metadata_codec":1,"instance_id":uuid::Uuid::from_bytes(f.descriptor.instance_uuid).to_string(),"namespace_view_id":id(&f.descriptor.namespace_view_id),"scope":"/project","materialization_policy":1,"fs_semantics":1,"access_projection":0,"metadata_root":id(&f.descriptor.metadata_root),"snapshot_id":f.sid()},"publication_sequence":"7","writer_epoch":"3","lease_id":"hydrate-lease","lease_expires_at":"2099-01-01T00:00:00Z","authorization_epoch":f.authorization_epoch.to_string(),"resolved_at":"2026-10-05T00:00:00Z","delivery":"full"}),
        )
    }
    async fn metadata(
        State(f): State<Arc<Fixture>>,
        Path(sid): Path<String>,
        body: Bytes,
    ) -> Response {
        assert_eq!(sid, f.sid());
        f.metadata_calls.fetch_add(1, Ordering::SeqCst);
        let release = f.metadata_release.notified();
        tokio::pin!(release);
        release.as_mut().enable();
        f.metadata_started.notify_one();
        if f.hold_metadata.load(Ordering::SeqCst) {
            release.await;
        }
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
        let bytes = f.frame_bytes(
            &body,
            MetaPayload {
                pages: pages.clone(),
            }
            .encode(7, 0)
            .unwrap(),
            items.len(),
            pages.len(),
            logical,
        );
        f.metadata_pages.fetch_add(pages.len(), Ordering::SeqCst);
        f.metadata_wire_bytes
            .fetch_add(bytes.len(), Ordering::SeqCst);
        f.frame_response(&body, bytes)
    }
    async fn objects(
        State(f): State<Arc<Fixture>>,
        Path(sid): Path<String>,
        body: Bytes,
    ) -> Response {
        assert_eq!(sid, f.sid());
        assert!(f.objects);
        let _request = active(&f.request_active, &f.request_peak);
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
        if f.fail_objects.load(Ordering::SeqCst) {
            return rejected_content();
        }
        let value: Value = serde_json::from_slice(&body).unwrap();
        let items = value["items"].as_array().unwrap();
        assert!(items.len() <= 128);
        let mut units = Vec::new();
        for item in items {
            let path = match checked_wire_path(item["path"].as_str().unwrap()) {
                Ok(path) => path,
                Err(response) => return response.into_response(),
            };
            let bytes = &f.bodies[path];
            assert_eq!(item["expected_digest"], digest_of(bytes));
            units.push((hash(bytes), bytes.clone()));
        }
        let logical = units.iter().map(|(_, bytes)| bytes.len()).sum();
        assert!(logical <= 7 * 1024 * 1024);
        f.object_max_bytes.fetch_max(logical, Ordering::SeqCst);
        let mut bytes = Vec::new();
        let mut frame_objects = Vec::new();
        let mut raw_size = 4;
        let mut sequence = 0;
        for unit in units {
            let unit_size = 40 + unit.1.len();
            if raw_size + unit_size > OBJECT_MAX_RAW {
                bytes.extend(
                    ObjectPayload {
                        objects: std::mem::take(&mut frame_objects),
                    }
                    .encode(7, sequence)
                    .unwrap(),
                );
                sequence += 1;
                raw_size = 4;
            }
            raw_size += unit_size;
            frame_objects.push(unit);
        }
        if !frame_objects.is_empty() {
            bytes.extend(
                ObjectPayload {
                    objects: frame_objects,
                }
                .encode(7, sequence)
                .unwrap(),
            );
            sequence += 1;
        }
        bytes.extend(
            EndPayload {
                request_item_count: items.len() as u32,
                unique_unit_count: items.len() as u32,
                logical_bytes: logical as u64,
                request_body_sha256: hash(&body),
            }
            .encode(7, sequence),
        );
        f.frame_response(&body, bytes)
    }
    async fn map(
        State(f): State<Arc<Fixture>>,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> Response {
        let _request = active(&f.request_active, &f.request_peak);
        let path = match checked_wire_path(&query["path"]) {
            Ok(path) => path,
            Err(response) => return response.into_response(),
        };
        f.map_calls.fetch_add(1, Ordering::SeqCst);
        let (map, _) = &f.maps[path];
        assert_eq!(query["expected_digest"], id(&map.file_content_id));
        Json(
            json!({"snapshot_id":f.sid(),"path":path,"map":{"schema_version":2,"file_content_id":id(&map.file_content_id),"file_size":map.file_size.to_string(),"chunk_size":CHUNK_SIZE,"chunk_count":map.chunk_count.to_string(),"page_count":"1","pages_root":id(&map.pages_root),"map_id":id(&map.map_id())}}),
        ).into_response()
    }
    async fn leaf(
        State(f): State<Arc<Fixture>>,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> Response {
        let _request = active(&f.request_active, &f.request_peak);
        let path = match checked_wire_path(&query["path"]) {
            Ok(path) => path,
            Err(response) => return response.into_response(),
        };
        f.leaf_calls.fetch_add(1, Ordering::SeqCst);
        let (map, leaf) = &f.maps[path];
        assert_eq!(query["map_id"], id(&map.map_id()));
        assert_eq!(query["page_index"], "0");
        Json(
            json!({"map_id":id(&map.map_id()),"page_index":"0","leaf_base64":base64(&leaf.encode().unwrap()),"proof":[]}),
        ).into_response()
    }
    async fn chunks(State(f): State<Arc<Fixture>>, body: Bytes) -> Response {
        let _request = active(&f.request_active, &f.request_peak);
        let value: Value = serde_json::from_slice(&body).unwrap();
        let items = value["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        let item = &items[0];
        let path = match checked_wire_path(item["path"].as_str().unwrap()) {
            Ok(path) => path,
            Err(response) => return response.into_response(),
        };
        let (map, _) = &f.maps[path];
        let index: usize = item["chunk_index"].as_str().unwrap().parse().unwrap();
        let bytes = f.bodies[path]
            .chunks(CHUNK_SIZE as usize)
            .nth(index)
            .unwrap();
        assert_eq!(item["expected_digest"], id(&map.file_content_id));
        assert_eq!(item["map_id"], id(&map.map_id()));
        f.chunk_calls.fetch_add(1, Ordering::SeqCst);
        let _active = active(&f.chunk_active, &f.chunk_peak);
        let release = f.chunks_release.notified();
        tokio::pin!(release);
        release.as_mut().enable();
        if index == 0 {
            if let Some(gate) = &f.large_gate {
                gate.wait().await;
            }
        }
        if f.hold_chunks.load(Ordering::SeqCst) {
            release.await;
        }
        if f.fail_chunks.load(Ordering::SeqCst) {
            return rejected_content();
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
    fn rejected_content() -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"code":"INVALID_REQUEST","message":"rejected test content","request_id":"mixed-lane-failure","retryable":false}})),
        )
            .into_response()
    }
    async fn blob(
        State(f): State<Arc<Fixture>>,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> Response {
        let path = match checked_wire_path(&query["path"]) {
            Ok(path) => path,
            Err(response) => return response.into_response(),
        };
        f.raw_calls.fetch_add(1, Ordering::SeqCst);
        let bytes = &f.bodies[path];
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
            self.fixture.hold_chunks.store(false, Ordering::SeqCst);
            self.fixture.chunks_release.notify_waiters();
            self.fixture.hold_metadata.store(false, Ordering::SeqCst);
            self.fixture.metadata_release.notify_waiters();
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

    struct PublishedVersions {
        versions: Vec<Arc<Fixture>>,
        current: AtomicUsize,
    }
    impl PublishedVersions {
        fn current(&self) -> Arc<Fixture> {
            self.versions[self.current.load(Ordering::SeqCst)].clone()
        }
        fn snapshot(&self, sid: &str) -> Arc<Fixture> {
            self.versions
                .iter()
                .find(|version| version.sid() == sid)
                .expect("request must name an actual fixed published snapshot")
                .clone()
        }
    }
    async fn version_caps(State(f): State<Arc<PublishedVersions>>) -> Json<Value> {
        caps(State(f.current())).await
    }
    async fn version_resolve(State(f): State<Arc<PublishedVersions>>) -> Json<Value> {
        resolve(State(f.current())).await
    }
    async fn version_metadata(
        State(f): State<Arc<PublishedVersions>>,
        Path(sid): Path<String>,
        body: Bytes,
    ) -> Response {
        metadata(State(f.snapshot(&sid)), Path(sid), body).await
    }
    async fn version_objects(
        State(f): State<Arc<PublishedVersions>>,
        Path(sid): Path<String>,
        body: Bytes,
    ) -> Response {
        objects(State(f.snapshot(&sid)), Path(sid), body).await
    }
    async fn version_blob(
        State(f): State<Arc<PublishedVersions>>,
        Path(sid): Path<String>,
        query: Query<BTreeMap<String, String>>,
    ) -> Response {
        blob(State(f.snapshot(&sid)), query).await
    }
    struct VersionServer {
        fixture: Arc<PublishedVersions>,
        client: Mst2Client,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for VersionServer {
        fn drop(&mut self) {
            self.task.abort();
            for version in &self.fixture.versions {
                version.hold_metadata.store(false, Ordering::SeqCst);
                version.metadata_release.notify_waiters();
            }
        }
    }
    impl VersionServer {
        async fn new(versions: Vec<Fixture>) -> Self {
            let fixture = Arc::new(PublishedVersions {
                versions: versions.into_iter().map(Arc::new).collect(),
                current: AtomicUsize::new(0),
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = Mst2Client::new(format!("http://{}", listener.local_addr().unwrap()));
            let app = Router::new()
                .route("/api/v2/snapshots/capabilities", get(version_caps))
                .route("/api/v2/snapshots/resolve", post(version_resolve))
                .route(
                    "/api/v2/snapshots/{sid}/metadata/pages",
                    post(version_metadata),
                )
                .route("/api/v2/snapshots/{sid}/objects", post(version_objects))
                .route("/api/v2/snapshots/{sid}/blob", get(version_blob))
                .with_state(fixture.clone());
            let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self {
                fixture,
                client,
                task,
            }
        }
        async fn reader(&self, version: usize) -> SnapshotReader {
            self.fixture.current.store(version, Ordering::SeqCst);
            SnapshotReader::resolve_request(
                self.client.clone(),
                &crate::snapshot::ResolveRequest::latest("/project", 60),
            )
            .await
            .unwrap()
        }
    }
    async fn bounded_hydrate(store: &DurableStore, reader: &SnapshotReader) -> HydrateReport {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            hydrate_workspace(store, reader),
        )
        .await
        .expect("actual HTTP workspace hydration must complete within its test deadline")
        .unwrap()
    }
    fn assert_full_snapshot(store: &DurableStore, reader: &SnapshotReader, fixture: &Fixture) {
        assert_eq!(
            store.local_pin_state().unwrap(),
            LocalPinState::Complete(CompletionKind::FullSnapshot)
        );
        let closure = store.snapshot_manifest().unwrap();
        closure.matches_descriptor(reader.descriptor()).unwrap();
        let actual: BTreeMap<_, _> = closure
            .files()
            .iter()
            .map(|file| {
                assert_eq!(file.fs_kind, "regular");
                (
                    format!("/{}", file.rel_path),
                    (file.size, file.content_digest.clone()),
                )
            })
            .collect();
        let expected: BTreeMap<_, _> = fixture
            .bodies
            .iter()
            .map(|(path, bytes)| (path.clone(), (bytes.len() as u64, digest_of(bytes))))
            .collect();
        assert_eq!(actual, expected);
        let directories: BTreeSet<_> = closure
            .directories()
            .iter()
            .map(|directory| format!("/{}", directory.rel_path))
            .collect();
        assert_eq!(directories, fixture.pages.keys().cloned().collect());
        for (path, bytes) in &fixture.bodies {
            let file = &actual[path];
            assert_eq!(store.read_blob(&file.1, file.0).unwrap(), *bytes);
        }
    }

    const MIXED_OUTPUT_BYTES: usize = 128 * 1024 * 1024;
    const MIXED_CONSTRUCTION_BYTES: usize = 32 * 1024 * 1024;
    static MIXED_TESTS: Mutex<()> = Mutex::const_new(());

    async fn mixed_reader(server: &Server) -> SnapshotReader {
        server.reader().await.with_content_limits(
            ContentBudgetLimits::new(MIXED_OUTPUT_BYTES, MIXED_CONSTRUCTION_BYTES).unwrap(),
        )
    }

    async fn wait_for_content(condition: impl Fn() -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("bounded real HTTP content condition did not become true");
    }

    async fn assert_mixed_lanes_pending(fixture: &Fixture, reader: &SnapshotReader) {
        assert_mixed_lanes_pending_with_limits(
            fixture,
            reader,
            MIXED_OUTPUT_BYTES,
            MIXED_CONSTRUCTION_BYTES,
        )
        .await;
    }

    async fn assert_mixed_lanes_pending_with_limits(
        fixture: &Fixture,
        reader: &SnapshotReader,
        output_bytes: usize,
        construction_bytes: usize,
    ) {
        wait_for_content(|| {
            fixture.object_active.load(Ordering::SeqCst) == 2
                && fixture.chunk_active.load(Ordering::SeqCst) == 2
        })
        .await;
        assert_eq!(fixture.request_active.load(Ordering::SeqCst), 4);
        assert_eq!(fixture.object_peak.load(Ordering::SeqCst), 2);
        assert_eq!(fixture.chunk_peak.load(Ordering::SeqCst), 2);
        assert_eq!(fixture.request_peak.load(Ordering::SeqCst), 4);
        let local = reader.content_usage();
        assert!(local.output_bytes > 0 && local.output_bytes <= output_bytes);
        assert!(local.construction_bytes > 0 && local.construction_bytes <= construction_bytes);
        // These are the actual shared semaphore charges, not fixture byte
        // estimates or RSS. Other parallel tests may own additional credits.
        let process = FetchCoordinator::process_content_usage();
        assert!(process.output_bytes >= local.output_bytes);
        assert!(process.construction_bytes >= local.construction_bytes);
        assert!(process.output_bytes <= 512 * 1024 * 1024);
        assert!(process.construction_bytes <= 64 * 1024 * 1024);
    }

    fn assert_uncommitted_and_refunded(store: &DurableStore, reader: &SnapshotReader) {
        assert!(!store.is_snapshot_complete().unwrap());
        assert!(!store.root().join("DURABLE_COMPLETE").exists());
        assert!(!matches!(
            store.local_pin_state().unwrap(),
            LocalPinState::Complete(_)
        ));
        assert_eq!(reader.content_usage().output_bytes, 0);
        assert_eq!(reader.content_usage().construction_bytes, 0);
        for entry in std::fs::read_dir(store.content_dir()).unwrap() {
            assert!(!entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp."));
        }
    }

    #[tokio::test]
    async fn maximum_object_batches_overlap_chunks_with_default_charged_limits() {
        let _serial = MIXED_TESTS.lock().await;
        let fixture = Fixture::near_budget_mix();
        fixture.hold_objects.store(true, Ordering::SeqCst);
        fixture.hold_chunks.store(true, Ordering::SeqCst);
        let server = Server::new(fixture).await;
        let reader = mixed_reader(&server).await;
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            DurableStore::open_for_workspace(
                temp.path(),
                "11111111-2222-4333-8444-555555555523",
                &reader,
            )
            .unwrap(),
        );
        let source = reader.clone();
        let target = store.clone();
        let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
        assert_mixed_lanes_pending(&server.fixture, &reader).await;
        assert!(reader.content_usage().output_bytes >= 14 * 1024 * 1024);
        assert!(!store.root().join("DURABLE_COMPLETE").exists());
        server.fixture.hold_objects.store(false, Ordering::SeqCst);
        server.fixture.hold_chunks.store(false, Ordering::SeqCst);
        server.fixture.objects_release.notify_waiters();
        server.fixture.chunks_release.notify_waiters();
        let report = tokio::time::timeout(std::time::Duration::from_secs(30), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(report.complete);
        assert_eq!(report.fetched, 58);
        assert_eq!(server.fixture.object_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            server.fixture.object_max_bytes.load(Ordering::SeqCst),
            7 * 1024 * 1024
        );
        assert_eq!(server.fixture.chunk_calls.load(Ordering::SeqCst), 6);
        assert_full_snapshot(&store, &reader, &server.fixture);
        assert_eq!(reader.content_usage().output_bytes, 0);
        assert_eq!(reader.content_usage().construction_bytes, 0);
    }

    async fn assert_serial_maximum_batches(server: &Server, reader: &SnapshotReader) {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            DurableStore::open_for_workspace(
                temp.path(),
                "11111111-2222-4333-8444-555555555524",
                reader,
            )
            .unwrap(),
        );
        let source = reader.clone();
        let target = store.clone();
        let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
        wait_for_content(|| server.fixture.object_active.load(Ordering::SeqCst) == 2).await;
        // Give a concurrently scheduled large lane time to issue its map and
        // CHUNK requests. With the serial fallback it cannot start yet.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(server.fixture.map_calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.fixture.chunk_calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.fixture.request_active.load(Ordering::SeqCst), 2);
        assert!(!store.root().join("DURABLE_COMPLETE").exists());
        server.fixture.hold_objects.store(false, Ordering::SeqCst);
        server.fixture.objects_release.notify_waiters();
        let report = tokio::time::timeout(std::time::Duration::from_secs(30), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(report.complete);
        assert_eq!(report.fetched, 58);
        assert_eq!(server.fixture.object_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            server.fixture.object_max_bytes.load(Ordering::SeqCst),
            7 * 1024 * 1024
        );
        assert_eq!(server.fixture.chunk_calls.load(Ordering::SeqCst), 6);
        assert_eq!(server.fixture.object_peak.load(Ordering::SeqCst), 2);
        assert!(server.fixture.chunk_peak.load(Ordering::SeqCst) <= 2);
        assert!(server.fixture.request_peak.load(Ordering::SeqCst) <= 2);
        assert_full_snapshot(&store, reader, &server.fixture);
    }

    #[tokio::test]
    async fn lowered_output_or_construction_policy_keeps_maximum_batches_serial() {
        let _serial = MIXED_TESTS.lock().await;
        for (output, construction) in [
            (16 * 1024 * 1024, MIXED_CONSTRUCTION_BYTES),
            (MIXED_OUTPUT_BYTES, 8 * 1024 * 1024),
        ] {
            let fixture = Fixture::near_budget_mix();
            fixture.hold_objects.store(true, Ordering::SeqCst);
            let server = Server::new(fixture).await;
            let reader = server
                .reader()
                .await
                .with_content_limits(ContentBudgetLimits::new(output, construction).unwrap());
            assert_serial_maximum_batches(&server, &reader).await;
            assert_eq!(reader.content_usage().output_bytes, 0);
            assert_eq!(reader.content_usage().construction_bytes, 0);
        }
    }

    #[tokio::test]
    async fn existing_reader_credit_owners_keep_default_policy_hydration_serial() {
        let _serial = MIXED_TESTS.lock().await;
        for (output, construction) in [(112 * 1024 * 1024, 0), (0, 26 * 1024 * 1024)] {
            let fixture = Fixture::near_budget_mix();
            fixture.hold_objects.store(true, Ordering::SeqCst);
            let server = Server::new(fixture).await;
            let reader = mixed_reader(&server).await;
            let owners = reader
                .content_scope
                .reserve_test_capacity(output, construction)
                .unwrap();
            assert_serial_maximum_batches(&server, &reader).await;
            assert_eq!(reader.content_usage().output_bytes, output);
            assert_eq!(reader.content_usage().construction_bytes, construction);
            let process = FetchCoordinator::process_content_usage();
            assert!(process.output_bytes >= output);
            assert!(process.construction_bytes >= construction);
            drop(owners);
            assert_eq!(reader.content_usage().output_bytes, 0);
            assert_eq!(reader.content_usage().construction_bytes, 0);
        }
    }

    #[tokio::test]
    async fn process_credit_pressure_falls_back_and_release_allows_real_http_overlap() {
        // Global quotas belong to one OS process. Execute the pressure case
        // in its own test process so the ordinary parallel suite still runs
        // this regression without borrowing the other tests' headroom.
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(90),
            tokio::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("workspace::hydrate::tests::process_credit_pressure_worker")
                .arg("--test-threads=1")
                .arg("--nocapture")
                .env("SCORPIOFS_HYDRATION_PROCESS_PRESSURE_WORKER", "1")
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("isolated process pressure test exceeded its deadline")
        .expect("failed to launch the process pressure worker");
        assert!(
            output.status.success(),
            "pressure worker failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("HYDRATION_PROCESS_PRESSURE_VERIFIED"),
            "the isolated worker must execute all pressure and refund assertions"
        );
    }

    #[tokio::test]
    async fn process_credit_pressure_worker() {
        if std::env::var("SCORPIOFS_HYDRATION_PROCESS_PRESSURE_WORKER").as_deref() != Ok("1") {
            return;
        }
        let _serial = MIXED_TESTS.lock().await;
        let fixture = Fixture::near_budget_mix();
        fixture.hold_objects.store(true, Ordering::SeqCst);
        let server = Server::new(fixture).await;
        let reader = mixed_reader(&server).await;
        let other_a = mixed_reader(&server).await;
        let other_b = mixed_reader(&server).await;
        let before = FetchCoordinator::process_content_usage();
        let owner_a = other_a
            .content_scope
            .reserve_test_capacity(0, 29 * 1024 * 1024)
            .unwrap();
        let owner_b = other_b
            .content_scope
            .reserve_test_capacity(0, 29 * 1024 * 1024)
            .unwrap();
        assert_serial_maximum_batches(&server, &reader).await;
        assert_eq!(reader.content_usage().output_bytes, 0);
        assert_eq!(reader.content_usage().construction_bytes, 0);
        drop(owner_a);
        assert_eq!(other_a.content_usage().construction_bytes, 0);
        assert_eq!(other_b.content_usage().construction_bytes, 29 * 1024 * 1024);
        drop(owner_b);
        assert_eq!(other_b.content_usage().construction_bytes, 0);
        assert_eq!(FetchCoordinator::process_content_usage(), before);

        // A fresh owner has no CAS resume shortcuts, so after the unrelated
        // owners drop it must issue both real HTTP lanes concurrently.
        server.fixture.hold_objects.store(true, Ordering::SeqCst);
        server.fixture.hold_chunks.store(true, Ordering::SeqCst);
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            DurableStore::open_for_workspace(
                temp.path(),
                "11111111-2222-4333-8444-555555555525",
                &reader,
            )
            .unwrap(),
        );
        let source = reader.clone();
        let target = store.clone();
        let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
        assert_mixed_lanes_pending(&server.fixture, &reader).await;
        server.fixture.hold_objects.store(false, Ordering::SeqCst);
        server.fixture.hold_chunks.store(false, Ordering::SeqCst);
        server.fixture.objects_release.notify_waiters();
        server.fixture.chunks_release.notify_waiters();
        let report = tokio::time::timeout(std::time::Duration::from_secs(30), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(report.complete);
        assert_eq!(report.fetched, 58);
        assert_full_snapshot(&store, &reader, &server.fixture);
        assert_eq!(reader.content_usage().output_bytes, 0);
        assert_eq!(reader.content_usage().construction_bytes, 0);
        assert_eq!(FetchCoordinator::process_content_usage(), before);
        println!("HYDRATION_PROCESS_PRESSURE_VERIFIED");
    }

    #[tokio::test]
    async fn held_objects_do_not_delay_large_chunks_or_allow_early_completion() {
        let _serial = MIXED_TESTS.lock().await;
        let fixture = Fixture::new(true, false, true);
        fixture.hold_objects.store(true, Ordering::SeqCst);
        fixture.hold_chunks.store(true, Ordering::SeqCst);
        let server = Server::new(fixture).await;
        let reader = mixed_reader(&server).await;
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            DurableStore::open_for_workspace(
                temp.path(),
                "11111111-2222-4333-8444-555555555520",
                &reader,
            )
            .unwrap(),
        );
        let source = reader.clone();
        let target = store.clone();
        let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
        assert_mixed_lanes_pending(&server.fixture, &reader).await;
        assert!(!store.root().join("DURABLE_COMPLETE").exists());
        server.fixture.hold_chunks.store(false, Ordering::SeqCst);
        server.fixture.chunks_release.notify_waiters();
        let large_cas: Vec<_> = ["/large0", "/large1"]
            .into_iter()
            .map(|name| {
                let digest = digest_of(&server.fixture.bodies[name]);
                store
                    .content_dir()
                    .join(digest.strip_prefix("sha256:").unwrap())
            })
            .collect();
        wait_for_content(|| {
            server.fixture.chunk_calls.load(Ordering::SeqCst) == 6
                && server.fixture.chunk_active.load(Ordering::SeqCst) == 0
                && reader.content_usage().output_bytes < 1024 * 1024
                && large_cas.iter().all(|path| path.is_file())
        })
        .await;
        for name in ["/large0", "/large1"] {
            let bytes = &server.fixture.bodies[name];
            assert_eq!(
                store
                    .read_blob(&digest_of(bytes), bytes.len() as u64)
                    .unwrap(),
                *bytes
            );
        }
        assert!(!store.root().join("DURABLE_COMPLETE").exists());
        assert_eq!(server.fixture.object_active.load(Ordering::SeqCst), 2);
        server.fixture.hold_objects.store(false, Ordering::SeqCst);
        server.fixture.objects_release.notify_waiters();
        let report = tokio::time::timeout(std::time::Duration::from_secs(30), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(report.fetched, 194);
        assert_full_snapshot(&store, &reader, &server.fixture);
        assert_eq!(reader.content_usage().output_bytes, 0);
        assert_eq!(reader.content_usage().construction_bytes, 0);
    }

    #[tokio::test]
    async fn either_mixed_http_lane_failure_drops_its_pending_sibling_and_can_retry() {
        let _serial = MIXED_TESTS.lock().await;
        for fail_objects in [true, false] {
            let fixture = Fixture::new(true, false, true);
            fixture.hold_objects.store(true, Ordering::SeqCst);
            fixture.hold_chunks.store(true, Ordering::SeqCst);
            let server = Server::new(fixture).await;
            let reader = mixed_reader(&server).await;
            let temp = tempfile::tempdir().unwrap();
            let store = Arc::new(
                DurableStore::open_for_workspace(
                    temp.path(),
                    "11111111-2222-4333-8444-555555555521",
                    &reader,
                )
                .unwrap(),
            );
            let source = reader.clone();
            let target = store.clone();
            let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
            assert_mixed_lanes_pending(&server.fixture, &reader).await;
            if fail_objects {
                server.fixture.fail_objects.store(true, Ordering::SeqCst);
                server.fixture.hold_objects.store(false, Ordering::SeqCst);
                server.fixture.objects_release.notify_waiters();
            } else {
                server.fixture.fail_chunks.store(true, Ordering::SeqCst);
                server.fixture.hold_chunks.store(false, Ordering::SeqCst);
                server.fixture.chunks_release.notify_waiters();
            }
            let error = tokio::time::timeout(std::time::Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err();
            assert_eq!(error.code, SnapshotErrorCode::InvalidRequest);
            assert_uncommitted_and_refunded(&store, &reader);
            server.fixture.fail_objects.store(false, Ordering::SeqCst);
            server.fixture.fail_chunks.store(false, Ordering::SeqCst);
            server.fixture.hold_objects.store(false, Ordering::SeqCst);
            server.fixture.hold_chunks.store(false, Ordering::SeqCst);
            server.fixture.objects_release.notify_waiters();
            server.fixture.chunks_release.notify_waiters();
            wait_for_content(|| server.fixture.request_active.load(Ordering::SeqCst) == 0).await;
            assert!(bounded_hydrate(&store, &reader).await.complete);
            assert_full_snapshot(&store, &reader, &server.fixture);
            assert!(server.fixture.object_peak.load(Ordering::SeqCst) <= 2);
            assert!(server.fixture.chunk_peak.load(Ordering::SeqCst) <= 2);
            assert!(server.fixture.request_peak.load(Ordering::SeqCst) <= 4);
        }
    }

    #[tokio::test]
    async fn cancelling_both_real_http_lanes_refunds_bytes_and_keeps_retry_possible() {
        let _serial = MIXED_TESTS.lock().await;
        let fixture = Fixture::new(true, false, true);
        fixture.hold_objects.store(true, Ordering::SeqCst);
        fixture.hold_chunks.store(true, Ordering::SeqCst);
        let server = Server::new(fixture).await;
        let reader = mixed_reader(&server).await;
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            DurableStore::open_for_workspace(
                temp.path(),
                "11111111-2222-4333-8444-555555555522",
                &reader,
            )
            .unwrap(),
        );
        let source = reader.clone();
        let target = store.clone();
        let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
        assert_mixed_lanes_pending(&server.fixture, &reader).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_uncommitted_and_refunded(&store, &reader);
        server.fixture.hold_objects.store(false, Ordering::SeqCst);
        server.fixture.hold_chunks.store(false, Ordering::SeqCst);
        server.fixture.objects_release.notify_waiters();
        server.fixture.chunks_release.notify_waiters();
        wait_for_content(|| server.fixture.request_active.load(Ordering::SeqCst) == 0).await;
        assert!(bounded_hydrate(&store, &reader).await.complete);
        assert_full_snapshot(&store, &reader, &server.fixture);
        assert!(server.fixture.object_peak.load(Ordering::SeqCst) <= 2);
        assert!(server.fixture.chunk_peak.load(Ordering::SeqCst) <= 2);
        assert!(server.fixture.request_peak.load(Ordering::SeqCst) <= 4);
    }

    #[tokio::test]
    async fn real_http_new_sid_single_file_and_directory_move_reuse_metadata_with_full_proof() {
        let server = VersionServer::new(
            (0..3)
                .map(|version| Fixture::update_version(true, version))
                .collect(),
        )
        .await;
        let temp = tempfile::tempdir().unwrap();
        let cold_reader = server.reader(0).await;
        let cold = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555510",
            &cold_reader,
        )
        .unwrap();
        let report = bounded_hydrate(&cold, &cold_reader).await;
        assert!(report.complete);
        assert_eq!(report.completion_kind, CompletionKind::FullSnapshot);
        assert_full_snapshot(&cold, &cold_reader, &server.fixture.versions[0]);
        let initial = &server.fixture.versions[0];
        assert_eq!(initial.metadata_calls.load(Ordering::SeqCst), 2);
        assert_eq!(initial.metadata_pages.load(Ordering::SeqCst), 5);
        let cold_wire = initial.metadata_wire_bytes.load(Ordering::SeqCst);
        assert!(cold_wire > 0);

        // Same SID retains separate local owners. Releasing one cannot revoke
        // the survivor that backs cross-version metadata acquisition.
        let sibling = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555511",
            &cold_reader,
        )
        .unwrap();
        assert_ne!(cold.root(), sibling.root());
        assert_eq!(cold.content_dir(), sibling.content_dir());
        assert_eq!(bounded_hydrate(&sibling, &cold_reader).await.fetched, 0);
        assert_eq!(
            initial.metadata_wire_bytes.load(Ordering::SeqCst),
            cold_wire
        );
        cold.release_local_pin().unwrap();
        assert_full_snapshot(&sibling, &cold_reader, initial);

        let single_reader = server.reader(1).await;
        assert_ne!(single_reader.snapshot_id(), cold_reader.snapshot_id());
        assert!(DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555511",
            &single_reader,
        )
        .is_err());
        let single = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555512",
            &single_reader,
        )
        .unwrap();
        assert_eq!(bounded_hydrate(&single, &single_reader).await.fetched, 1);
        assert_full_snapshot(&single, &single_reader, &server.fixture.versions[1]);
        let changed = &server.fixture.versions[1];
        assert_eq!(changed.metadata_calls.load(Ordering::SeqCst), 2);
        assert_eq!(changed.metadata_pages.load(Ordering::SeqCst), 2);
        let single_wire = changed.metadata_wire_bytes.load(Ordering::SeqCst);
        assert!(single_wire > 0 && single_wire < cold_wire);

        let moved_reader = server.reader(2).await;
        assert_ne!(moved_reader.snapshot_id(), single_reader.snapshot_id());
        let moved = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555513",
            &moved_reader,
        )
        .unwrap();
        assert_eq!(bounded_hydrate(&moved, &moved_reader).await.fetched, 0);
        assert_full_snapshot(&moved, &moved_reader, &server.fixture.versions[2]);
        let renamed = &server.fixture.versions[2];
        assert_eq!(renamed.metadata_calls.load(Ordering::SeqCst), 1);
        assert_eq!(renamed.metadata_pages.load(Ordering::SeqCst), 1);
        let rename_wire = renamed.metadata_wire_bytes.load(Ordering::SeqCst);
        assert!(rename_wire > 0 && rename_wire < single_wire);
        assert_eq!(renamed.object_calls.load(Ordering::SeqCst), 0);
        assert_eq!(renamed.raw_calls.load(Ordering::SeqCst), 0);
        assert_full_snapshot(&sibling, &cold_reader, initial);
        assert_full_snapshot(&single, &single_reader, changed);
        eprintln!(
            "WORKSPACE_METADATA_HTTP_REUSE: cold_pages=5 cold_wire={cold_wire} single_pages=2 single_wire={single_wire} rename_pages=1 rename_wire={rename_wire}; every owner has audited FullSnapshot with complete empty/alias directories"
        );
    }

    #[tokio::test]
    async fn real_http_raw_hydration_reuses_pages_and_repairs_corrupt_cached_metadata() {
        let server = Server::new(Fixture::update_version(false, 0)).await;
        let reader = server.reader().await;
        let temp = tempfile::tempdir().unwrap();
        let first = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555514",
            &reader,
        )
        .unwrap();
        bounded_hydrate(&first, &reader).await;
        assert_full_snapshot(&first, &reader, &server.fixture);
        assert_eq!(server.fixture.metadata_pages.load(Ordering::SeqCst), 5);
        let second = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555515",
            &reader,
        )
        .unwrap();
        assert_eq!(bounded_hydrate(&second, &reader).await.fetched, 0);
        assert_full_snapshot(&second, &reader, &server.fixture);
        assert_eq!(server.fixture.metadata_calls.load(Ordering::SeqCst), 2);
        assert_eq!(server.fixture.metadata_pages.load(Ordering::SeqCst), 5);
        let before_wire = server.fixture.metadata_wire_bytes.load(Ordering::SeqCst);
        let scope = first.content_dir().parent().unwrap();
        let shared_page = scope
            .join("pages")
            .join(hex::encode(page_id(&server.fixture.pages["/d2"])));
        std::fs::write(&shared_page, b"corrupt shared page hint").unwrap();
        let repaired = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555516",
            &reader,
        )
        .unwrap();
        assert_eq!(bounded_hydrate(&repaired, &reader).await.fetched, 0);
        assert_full_snapshot(&repaired, &reader, &server.fixture);
        assert_eq!(server.fixture.metadata_calls.load(Ordering::SeqCst), 3);
        assert_eq!(server.fixture.metadata_pages.load(Ordering::SeqCst), 6);
        assert!(server.fixture.metadata_wire_bytes.load(Ordering::SeqCst) > before_wire);
        assert_eq!(
            std::fs::read(&shared_page).unwrap(),
            server.fixture.pages["/d2"]
        );
        assert_eq!(server.fixture.object_calls.load(Ordering::SeqCst), 0);
        // Aliased paths share bodies but remain separate logical namespace entries.
        assert_eq!(server.fixture.raw_calls.load(Ordering::SeqCst), 192);
    }

    #[tokio::test]
    async fn cancelling_real_metadata_request_unlocks_scope_without_complete_and_retries() {
        let fixture = Fixture::update_version(true, 0);
        fixture.hold_metadata.store(true, Ordering::SeqCst);
        let server = Server::new(fixture).await;
        let reader = server.reader().await;
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(
            DurableStore::open_for_workspace(
                temp.path(),
                "11111111-2222-4333-8444-555555555517",
                &reader,
            )
            .unwrap(),
        );
        let source = reader.clone();
        let target = store.clone();
        let task = tokio::spawn(async move { hydrate_workspace(&target, &source).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.fixture.metadata_started.notified(),
        )
        .await
        .unwrap();
        let scope = store.content_dir().parent().unwrap();
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(scope.join("closures.lock"))
            .unwrap();
        lock.try_lock()
            .expect("actual metadata HTTP must not hold the scope index lock");
        lock.unlock().unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        lock.try_lock()
            .expect("cancelled actual HTTP sync must release its index lock");
        lock.unlock().unwrap();
        assert!(!store.is_snapshot_complete().unwrap());
        assert!(!scope.join("closures.json").exists());
        server.fixture.hold_metadata.store(false, Ordering::SeqCst);
        server.fixture.metadata_release.notify_waiters();
        assert!(bounded_hydrate(&store, &reader).await.complete);
        assert_full_snapshot(&store, &reader, &server.fixture);
    }

    #[tokio::test]
    async fn authorization_epoch_isolates_identical_metadata_and_refuses_another_domain_owner() {
        let first = Fixture::update_version(true, 0);
        let mut other_epoch = Fixture::update_version(true, 0);
        other_epoch.descriptor.namespace_view_id = [0x24; 32];
        other_epoch.authorization_epoch = 12;
        let server = VersionServer::new(vec![first, other_epoch]).await;
        let temp = tempfile::tempdir().unwrap();
        let first_reader = server.reader(0).await;
        let first = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555518",
            &first_reader,
        )
        .unwrap();
        bounded_hydrate(&first, &first_reader).await;
        let other_reader = server.reader(1).await;
        let other = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555519",
            &other_reader,
        )
        .unwrap();
        assert_ne!(first.content_dir(), other.content_dir());
        assert_ne!(
            first_reader.authorized_context().cache_domain(),
            other_reader.authorized_context().cache_domain()
        );
        assert_eq!(
            hydrate_workspace(&first, &other_reader)
                .await
                .unwrap_err()
                .code,
            SnapshotErrorCode::ScopeForbidden
        );
        assert_eq!(
            server.fixture.versions[1]
                .metadata_calls
                .load(Ordering::SeqCst),
            0
        );
        bounded_hydrate(&other, &other_reader).await;
        assert_full_snapshot(&other, &other_reader, &server.fixture.versions[1]);
        assert_eq!(
            server.fixture.versions[1]
                .metadata_calls
                .load(Ordering::SeqCst),
            2
        );
        assert_eq!(
            server.fixture.versions[1]
                .metadata_pages
                .load(Ordering::SeqCst),
            5
        );
        assert_full_snapshot(&first, &first_reader, &server.fixture.versions[0]);
    }

    #[tokio::test]
    async fn strict_http_paths_cover_owned_and_proven_ranges_then_hydration() {
        let server = Server::new(Fixture::new(true, false, true)).await;
        let reader = server.reader().await;
        let bytes = &server.fixture.bodies["/large0"];
        assert!(bytes.len() > 2 * CHUNK_SIZE as usize);
        let digest = digest_of(bytes);
        // The actual HTTP fixture rejects a local manifest path. It never
        // normalizes a malformed request before echoing the accepted wire path.
        let rejected = reader
            .client()
            .chunk_map(reader.snapshot_id(), "large0", &digest)
            .await
            .unwrap_err();
        assert_eq!(rejected.code, SnapshotErrorCode::InvalidRequest);
        assert_eq!(rejected.http_status, 400);
        assert_eq!(server.fixture.map_calls.load(Ordering::SeqCst), 0);

        let range =
            crate::snapshot::OwnedChunkedFile::open(&reader, "large0", &digest, bytes.len() as u64)
                .await
                .unwrap();
        let offset = CHUNK_SIZE as usize - 3;
        assert_eq!(
            range
                .read_range_owned(offset as u64, 9)
                .await
                .unwrap()
                .as_bytes(),
            &bytes[offset..offset + 9]
        );
        let proof = reader.prove_file("large1").await.unwrap();
        let proven_range = crate::snapshot::OwnedChunkedFile::open_proven(&reader, proof)
            .await
            .unwrap();
        let second = &server.fixture.bodies["/large1"];
        assert_eq!(
            proven_range
                .read_range_owned(0, second.len() as u64)
                .await
                .unwrap()
                .as_bytes(),
            second
        );
        assert_eq!(
            range
                .read_range_owned(2 * CHUNK_SIZE as u64, 7)
                .await
                .unwrap()
                .as_bytes(),
            &bytes[2 * CHUNK_SIZE as usize..]
        );
        assert_eq!(server.fixture.map_calls.load(Ordering::SeqCst), 2);
        assert_eq!(server.fixture.leaf_calls.load(Ordering::SeqCst), 2);
        assert_eq!(server.fixture.chunk_calls.load(Ordering::SeqCst), 6);

        let small = reader.prove_file("d0/f000").await.unwrap();
        for use_frames in [true, false] {
            assert_eq!(
                reader
                    .read_proven_content(&small, use_frames)
                    .await
                    .unwrap()
                    .as_bytes(),
                &server.fixture.bodies["/d0/f000"]
            );
        }
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555540",
            &reader,
        )
        .unwrap();
        let before = server.fixture.chunk_calls.load(Ordering::SeqCst);
        let report = bounded_hydrate(&store, &reader).await;
        assert!(report.complete);
        assert_eq!(report.completion_kind, CompletionKind::FullSnapshot);
        assert_eq!(report.fetched, 194);
        assert_eq!(
            server.fixture.chunk_calls.load(Ordering::SeqCst) - before,
            6
        );
        assert_full_snapshot(&store, &reader, &server.fixture);
    }

    #[tokio::test]
    async fn actual_http_hydration_uses_two_batch_and_two_streaming_lanes_then_shares_cas() {
        let server = Server::new(Fixture::new(true, true, true)).await;
        let reader = server.reader().await;
        let temp = tempfile::tempdir().unwrap();
        let mut first = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555501",
            &reader,
        )
        .unwrap();
        assert!(first.verification_meters().is_none());
        let first_meters = first.enable_verification_meters();
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
        let unique_bytes: u64 = server
            .fixture
            .bodies
            .values()
            .map(|bytes| bytes.len() as u64)
            .sum();
        let cold_resume = first_meters.snapshot_for(CasVerificationReason::Resume);
        assert_eq!(cold_resume.calls, 194);
        assert_eq!(cold_resume.missing, 194);
        assert_eq!(cold_resume.read_bytes, 0);
        let cold_commit = first_meters.snapshot_for(CasVerificationReason::HydrationCommit);
        assert_eq!(cold_commit.calls, 194);
        assert_eq!(cold_commit.verified, 194);
        assert_eq!(cold_commit.read_bytes, unique_bytes);
        let second_reader = server.reader().await;
        let mut second = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555502",
            &second_reader,
        )
        .unwrap();
        let second_meters = second.enable_verification_meters();
        let before = (
            server.fixture.object_calls.load(Ordering::SeqCst),
            server.fixture.chunk_calls.load(Ordering::SeqCst),
        );
        let report = hydrate_workspace(&second, &second_reader).await.unwrap();
        assert!(report.complete);
        assert_eq!(report.fetched, 0);
        assert_eq!(report.resumed, 194);
        let warm_resume = second_meters.snapshot_for(CasVerificationReason::Resume);
        assert_eq!(warm_resume.calls, 194);
        assert_eq!(warm_resume.verified, 194);
        assert_eq!(warm_resume.read_bytes, unique_bytes);
        // Full metadata proof does not audit old owners. This owner's resume
        // and commit still independently hash every content dependency.
        assert_eq!(
            second_meters
                .snapshot_for(CasVerificationReason::CompletionAudit)
                .calls,
            0
        );
        assert_eq!(second_meters.snapshot().read_bytes, 2 * unique_bytes);
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
        assert_eq!(
            first_meters
                .snapshot_for(CasVerificationReason::CompletionAudit)
                .read_bytes,
            unique_bytes
        );
        assert_eq!(
            second_meters
                .snapshot_for(CasVerificationReason::CompletionAudit)
                .read_bytes,
            unique_bytes
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
    async fn absent_object_capability_hydrates_cached_full_closure_without_object_requests() {
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
        assert!(
            store.verification_meters().is_none(),
            "fallback stays unmetered by default"
        );
        assert!(report.complete);
        assert_eq!(report.completion_kind, CompletionKind::FullSnapshot);
        assert_eq!(server.fixture.object_calls.load(Ordering::SeqCst), 0);
        assert_eq!(server.fixture.raw_calls.load(Ordering::SeqCst), 192);
    }
}
