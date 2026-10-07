use super::*;

#[tokio::test]
async fn direct_full_proof_applies_discovered_file_limits_before_publishing_records() {
    for case in 0..3 {
        let old = HttpFixture::start(complete_fixture("a", b"previous")).await;
        let old_reader = old.reader().await;
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        IncrementalSync::new(&old_reader, &cache)
            .sync_snapshot()
            .await
            .unwrap();
        let index = std::fs::read(cache.dir().join("closures.json")).unwrap();
        let bytes = b"12345";
        let mut fixture = Fixture {
            canonical: true,
            ..Default::default()
        };
        let (limits, boundary_limits, message) = match case {
            0 => {
                fixture.root = fixture.leaf("/", vec![file_entry("x", EntryKind::Regular, bytes)]);
                fixture.expect_file("x", "regular", bytes);
                (
                    json!({"max_file_bytes": "4"}),
                    json!({"max_file_bytes": "5"}),
                    "file exceeds the discovered serving limit",
                )
            }
            1 => {
                let name = "abcdefghijk";
                fixture.root = fixture.leaf("/", vec![file_entry(name, EntryKind::Regular, bytes)]);
                fixture.expect_file(name, "regular", bytes);
                (
                    json!({"max_path_bytes": 10}),
                    json!({"max_path_bytes": 12}),
                    "path exceeds the discovered serving limit",
                )
            }
            _ => {
                let child = fixture.leaf("/d", vec![file_entry("x", EntryKind::Regular, bytes)]);
                fixture.root = fixture.leaf("/", vec![Entry::dir(b"d", child)]);
                fixture.expect_file("d/x", "regular", bytes);
                (
                    json!({"max_path_components": 1}),
                    json!({"max_path_components": 2}),
                    "path exceeds the discovered serving limit",
                )
            }
        };
        fixture.operational_limits = Some(limits);
        let mut http = old.restart(fixture).await;
        let reader = http.canonical_reader().await;
        let CapabilityAdvertisement::Canonical(caps) = reader.capability_advertisement() else {
            panic!("fixture must pass canonical discovery before metadata validation");
        };
        match case {
            0 => assert_eq!(caps.limits().max_file_bytes, 4),
            1 => assert_eq!(caps.limits().max_path_bytes, 10),
            _ => assert_eq!(caps.limits().max_path_components, 1),
        }
        // These are valid canonical pages under the hard serving profile.
        // Only the successfully discovered operational limit rejects them.
        let hard_closure = scorpiofs::snapshot::ValidatedSnapshotClosure::from_pages(
            reader.descriptor(),
            http.fixture
                .pages
                .iter()
                .map(|(id, page)| (id_string(id), page.clone()))
                .collect(),
        )
        .unwrap();
        assert_manifest(hard_closure.files(), &http.fixture.expected);
        let mut sync = IncrementalSync::new(&reader, &cache);
        let error = sync.sync_snapshot().await.unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::LimitExceeded);
        assert_eq!(error.message, message);
        assert!(!http.fixture.requested_ids().is_empty());
        assert_eq!(sync.meters().closure_index_writes, 0);
        assert_eq!(
            std::fs::read(cache.dir().join("closures.json")).unwrap(),
            index
        );
        assert_eq!(http.fixture.blob_requests.load(Ordering::SeqCst), 0);
        assert!(http.fixture.object_requests.lock().unwrap().is_empty());
        // Retry the same pages at the accepted boundary, including cached hints.
        // Rebuild the fixture data without carrying the previous HTTP task.
        let accepted = Fixture {
            canonical: true,
            operational_limits: Some(boundary_limits),
            root: http.fixture.root,
            pages: http.fixture.pages.clone(),
            routes: http.fixture.routes.clone(),
            expected: http.fixture.expected.clone(),
            blobs: http.fixture.blobs.clone(),
            ..Default::default()
        };
        http = http.restart(accepted).await;
        let reader = http.canonical_reader().await;
        let mut retry = IncrementalSync::new(&reader, &cache);
        let closure = tokio::time::timeout(Duration::from_secs(2), retry.sync_snapshot())
            .await
            .unwrap()
            .unwrap();
        assert_manifest(closure.files(), &http.fixture.expected);
        assert_eq!(retry.meters().fetched_pages, 0);
        assert_eq!(retry.meters().reused_pages, closure.pages().len() as u64);
        assert_eq!(retry.meters().closure_index_writes, 1);
        assert_eq!(
            retry.closure_meters().proof_page_hashes,
            closure.pages().len() as u64
        );
    }
}

fn assert_full_proof(
    sync: &IncrementalSync<'_>,
    closure: &scorpiofs::snapshot::ValidatedSnapshotClosure,
    fixture: &Fixture,
) {
    assert_manifest(closure.files(), &fixture.expected);
    assert_eq!(closure.pages().len(), fixture.pages.len());
    assert_eq!(sync.meters().pin_set_reads, 0);
    assert_eq!(sync.meters().traversal_nodes, 0);
    assert_eq!(sync.meters().acquisition_file_entries, 0);
    assert_eq!(sync.meters().reused_file_entries_copied, 0);
    assert_eq!(sync.meters().reused_subtrees, 0);
    assert_eq!(
        sync.closure_meters().proof_page_hashes,
        closure.pages().len() as u64
    );
    assert_eq!(
        sync.closure_meters().proof_logical_files,
        closure.files().len() as u64
    );
    assert_eq!(
        sync.closure_meters().proof_logical_directories,
        closure.directories().len() as u64
    );
    assert!(
        sync.closure_meters().collector_route_visits > closure.pages().len() as u64,
        "identical page ids must still expand every logical alias"
    );
}

#[tokio::test]
async fn full_proof_ignores_malformed_old_scope_child_but_reuse_and_cleanup_refuse_it() {
    let http = HttpFixture::start(complete_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    hydrate_full(&cache, &reader, &closure).await;
    let index = std::fs::read(cache.dir().join("closures.json")).unwrap();
    let malformed = cache.dir().join("aa".repeat(32));
    std::fs::write(&malformed, b"not a pin directory").unwrap();
    http.fixture.requests.lock().unwrap().clear();

    let mut full = IncrementalSync::new(&reader, &cache);
    let warm = full.sync_snapshot().await.unwrap();
    assert_full_proof(&full, &warm, &http.fixture);
    assert_eq!(warm.directories(), closure.directories());
    assert_eq!(warm.pages(), closure.pages());
    assert_eq!(full.meters().reused_pages, closure.pages().len() as u64);
    assert_eq!(full.meters().fetched_pages, 0);
    assert_eq!(
        full.closure_meters().collector_page_decodes,
        closure.pages().len() as u64
    );
    assert!(http.fixture.requested_ids().is_empty());
    assert_eq!(
        std::fs::read(cache.dir().join("closures.json")).unwrap(),
        index
    );

    let mut files = IncrementalSync::new(&reader, &cache);
    assert_eq!(
        files.sync().await.unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(files.meters().pin_set_reads, 1);
    assert_eq!(
        cache
            .drop_records_for_pin(reader.snapshot_id())
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    assert_eq!(
        std::fs::read(cache.dir().join("closures.json")).unwrap(),
        index
    );
    std::fs::remove_file(malformed).unwrap();
    let mut retry = IncrementalSync::new(&reader, &cache);
    assert_manifest(&retry.sync().await.unwrap(), &http.fixture.expected);
    assert_eq!(retry.meters().reused_subtrees, 1);
}

#[tokio::test]
async fn full_proof_uses_busy_old_pin_pages_while_file_only_reuse_and_cleanup_stay_conservative() {
    let http = HttpFixture::start(complete_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    hydrate_full(&cache, &reader, &closure).await;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(
            cache
                .dir()
                .join(reader.snapshot_id().trim_start_matches("sha256:"))
                .join(".hydrate.lock"),
        )
        .unwrap();
    lock.try_lock().unwrap();
    let index = std::fs::read(cache.dir().join("closures.json")).unwrap();
    http.fixture.requests.lock().unwrap().clear();

    let mut full = IncrementalSync::new(&reader, &cache);
    let warm = tokio::time::timeout(Duration::from_secs(2), full.sync_snapshot())
        .await
        .unwrap()
        .unwrap();
    assert_full_proof(&full, &warm, &http.fixture);
    assert_eq!(warm.directories(), closure.directories());
    assert_eq!(full.meters().reused_pages, closure.pages().len() as u64);
    assert_eq!(
        full.closure_meters().collector_page_decodes,
        closure.pages().len() as u64
    );
    assert!(http.fixture.requested_ids().is_empty());
    assert_eq!(cache.drop_records_for_pin(reader.snapshot_id()).unwrap(), 0);
    assert_eq!(
        std::fs::read(cache.dir().join("closures.json")).unwrap(),
        index
    );

    let mut files = IncrementalSync::new(&reader, &cache);
    assert_manifest(&files.sync().await.unwrap(), &http.fixture.expected);
    assert_eq!(files.meters().pin_set_reads, 1);
    assert_eq!(files.meters().reused_subtrees, 0);
    assert!(files.meters().traversal_nodes > 0);
    lock.unlock().unwrap();
    let mut retry = IncrementalSync::new(&reader, &cache);
    assert_manifest(&retry.sync().await.unwrap(), &http.fixture.expected);
    assert_eq!(retry.meters().reused_subtrees, 1);
}

#[tokio::test]
async fn full_metadata_proof_does_not_promote_damaged_old_completion_to_content_proof() {
    let http = HttpFixture::start(complete_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    hydrate_full(&cache, &reader, &closure).await;
    let pin = cache
        .dir()
        .join(reader.snapshot_id().trim_start_matches("sha256:"));
    let marker = pin.join("DURABLE_COMPLETE");
    std::fs::write(&marker, b"damaged old completion").unwrap();
    let index = std::fs::read(cache.dir().join("closures.json")).unwrap();
    let mut full = IncrementalSync::new(&reader, &cache);
    let warm = full.sync_snapshot().await.unwrap();
    assert_full_proof(&full, &warm, &http.fixture);
    assert_eq!(std::fs::read(&marker).unwrap(), b"damaged old completion");
    assert_eq!(
        std::fs::read(cache.dir().join("closures.json")).unwrap(),
        index
    );

    let mut files = IncrementalSync::new(&reader, &cache);
    assert_manifest(&files.sync().await.unwrap(), &http.fixture.expected);
    assert_eq!(files.meters().pin_set_reads, 1);
    assert_eq!(files.meters().reused_subtrees, 0);
    assert!(!marker.exists());
    assert!(pin.join("NEEDS_REPAIR").exists());
    assert_eq!(cache.drop_records_for_pin(reader.snapshot_id()).unwrap(), 0);
    let store = DurableStore::open_for_reader(&pin, cache.dir().join("blobs"), &reader).unwrap();
    assert!(!store.is_snapshot_complete().unwrap());
    assert!(
        store
            .hydrate_snapshot_from_closure(&reader, &warm)
            .await
            .unwrap()
            .complete
    );
    assert!(store.is_snapshot_complete().unwrap());
}

#[tokio::test]
async fn cancelling_full_proof_during_real_http_collect_preserves_index_and_releases_lock() {
    let first = HttpFixture::start(complete_fixture("a", b"target-one")).await;
    let old = first.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    IncrementalSync::new(&old, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    let index = std::fs::read(cache.dir().join("closures.json")).unwrap();
    let next = first
        .restart(complete_fixture("moved", b"target-two"))
        .await;
    let reader = next.reader().await;
    next.fixture.pause_metadata.store(true, Ordering::SeqCst);
    let source = reader.clone();
    let scope = cache.dir().to_path_buf();
    let task = tokio::spawn(async move {
        let cache = ScopeCache::open(scope).unwrap();
        IncrementalSync::new(&source, &cache).sync_snapshot().await
    });
    tokio::time::timeout(
        Duration::from_secs(2),
        next.fixture.metadata_started.notified(),
    )
    .await
    .unwrap();
    let probe = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(cache.dir().join("closures.lock"))
        .unwrap();
    assert!(matches!(
        probe.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        std::fs::read(cache.dir().join("closures.json")).unwrap(),
        index
    );
    probe.try_lock().unwrap();
    probe.unlock().unwrap();
    assert!(!cache
        .dir()
        .join(reader.snapshot_id().trim_start_matches("sha256:"))
        .join("DURABLE_COMPLETE")
        .exists());
    next.fixture.pause_metadata.store(false, Ordering::SeqCst);
    next.fixture.metadata_release.notify_waiters();
    let mut retry = IncrementalSync::new(&reader, &cache);
    let closure = tokio::time::timeout(Duration::from_secs(2), retry.sync_snapshot())
        .await
        .unwrap()
        .unwrap();
    assert_full_proof(&retry, &closure, &next.fixture);
    assert_eq!(retry.meters().closure_index_writes, 1);
}

#[tokio::test]
async fn full_proof_revoked_at_lock_or_wire_boundary_cannot_publish_index() {
    for waiting_for_lock in [true, false] {
        let first = HttpFixture::start(complete_fixture("a", b"target-one")).await;
        let old = first.reader().await;
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        IncrementalSync::new(&old, &cache)
            .sync_snapshot()
            .await
            .unwrap();
        let index = std::fs::read(cache.dir().join("closures.json")).unwrap();
        let mut fixture = complete_fixture("moved", b"target-two");
        fixture.lease_expiry = expiring_cas_fixture().lease_expiry;
        let next = first.restart(fixture).await;
        let reader = next.reader().await;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(cache.dir().join("closures.lock"))
            .unwrap();
        if waiting_for_lock {
            lock.try_lock().unwrap();
        } else {
            next.fixture.pause_metadata.store(true, Ordering::SeqCst);
        }
        let source = reader.clone();
        let scope = cache.dir().to_path_buf();
        let mut task = tokio::spawn(async move {
            let cache = ScopeCache::open(scope).unwrap();
            IncrementalSync::new(&source, &cache).sync_snapshot().await
        });
        if waiting_for_lock {
            assert!(tokio::time::timeout(Duration::from_millis(30), &mut task)
                .await
                .is_err());
            assert!(next.fixture.requested_ids().is_empty());
        } else {
            tokio::time::timeout(
                Duration::from_secs(2),
                next.fixture.metadata_started.notified(),
            )
            .await
            .unwrap();
        }
        wait_for_cas_revocation(&next, &reader).await;
        if waiting_for_lock {
            lock.unlock().unwrap();
        } else {
            next.fixture.pause_metadata.store(false, Ordering::SeqCst);
            next.fixture.metadata_release.notify_one();
        }
        let error = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::ScopeForbidden);
        assert_eq!(
            std::fs::read(cache.dir().join("closures.json")).unwrap(),
            index
        );
        lock.try_lock().unwrap();
        lock.unlock().unwrap();
        assert!(!cache
            .dir()
            .join(reader.snapshot_id().trim_start_matches("sha256:"))
            .join("DURABLE_COMPLETE")
            .exists());
    }
}
