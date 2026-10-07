use super::*;

#[tokio::test]
async fn complete_sync_reuses_pins_without_copying_cached_manifests() {
    let first = HttpFixture::start(complete_fixture("a", b"target-one")).await;
    let old = first.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let closure = IncrementalSync::new(&old, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    hydrate_full(&cache, &old, &closure).await;
    let index = std::fs::read(cache.dir().join("closures.json")).unwrap();
    let before = first.fixture.requested_ids();
    let mut warm = IncrementalSync::new(&old, &cache);
    let unchanged = warm.sync_snapshot().await.unwrap();
    assert_eq!(unchanged.pages(), closure.pages());
    assert_eq!(unchanged.directories(), closure.directories());
    assert_manifest(unchanged.files(), &first.fixture.expected);
    assert_eq!(warm.meters().reused_file_entries_copied, 0);
    assert_eq!(warm.meters().traversal_nodes, 0);
    assert_eq!(warm.meters().closure_index_writes, 0);
    assert_eq!(first.fixture.requested_ids(), before);
    assert_eq!(
        std::fs::read(cache.dir().join("closures.json")).unwrap(),
        index
    );
    assert_eq!(
        warm.closure_meters().proof_page_hashes,
        closure.pages().len() as u64
    );

    let next = first
        .restart(complete_fixture("moved", b"target-two"))
        .await;
    let reader = next.reader().await;
    let mut changed = IncrementalSync::new(&reader, &cache);
    let updated = changed.sync_snapshot().await.unwrap();
    assert_manifest(updated.files(), &next.fixture.expected);
    assert!(changed.meters().reused_subtrees > 0);
    assert_eq!(changed.meters().reused_file_entries_copied, 0);
    assert_eq!(
        changed.closure_meters().proof_page_hashes,
        updated.pages().len() as u64
    );
    let moved = cache
        .record_for(&id_string(&next.fixture.routes[&("/moved".into(), vec![])]))
        .unwrap();
    assert_eq!(moved.pin_ref, reader.snapshot_id());
    assert_eq!(moved.files.len(), 2);
    assert_eq!(moved.files[0].rel_path, "f.txt");
    assert_eq!(moved.files[1].rel_path, "nested/deep.txt");
    assert!(closure
        .files()
        .iter()
        .any(|file| file.rel_path == "a/f.txt"));
    assert!(!closure
        .files()
        .iter()
        .any(|file| file.rel_path.starts_with("moved/")));
}

#[tokio::test]
async fn file_only_sync_still_returns_the_cached_manifest() {
    let http = HttpFixture::start(complete_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    hydrate_full(&cache, &reader, &closure).await;
    let before = http.fixture.requested_ids();
    let mut sync = IncrementalSync::new(&reader, &cache);
    let files = sync.sync().await.unwrap();
    assert_manifest(&files, &http.fixture.expected);
    assert_eq!(sync.meters().reused_subtrees, 1);
    assert_eq!(sync.meters().traversal_nodes, 0);
    assert_eq!(sync.meters().reused_file_entries_copied, files.len() as u64);
    assert_eq!(sync.meters().closure_index_writes, 0);
    assert_eq!(http.fixture.requested_ids(), before);
}

#[tokio::test]
async fn complete_sync_repairs_untrusted_large_manifests_without_copying_them() {
    let http = HttpFixture::start(complete_fixture("a", b"target-one")).await;
    let reader = http.reader().await;
    let tmp = tempfile::tempdir().unwrap();
    let cache = ScopeCache::open(tmp.path()).unwrap();
    let closure = IncrementalSync::new(&reader, &cache)
        .sync_snapshot()
        .await
        .unwrap();
    hydrate_full(&cache, &reader, &closure).await;
    let mut records: HashMap<String, scorpiofs::snapshot::ClosureRecord> =
        serde_json::from_slice(&std::fs::read(cache.dir().join("closures.json")).unwrap()).unwrap();
    let forged = SnapshotFile {
        rel_path: "forged.txt".into(),
        fs_kind: "regular".into(),
        size: 999,
        content_digest: id_string(&[0x88; 32]),
    };
    for record in records.values_mut() {
        record.files = vec![forged.clone(); 1024];
        record.total_entries = u64::MAX;
    }
    std::fs::write(
        cache.dir().join("closures.json"),
        serde_json::to_vec(&records).unwrap(),
    )
    .unwrap();
    let before = http.fixture.requested_ids();
    let mut repair = IncrementalSync::new(&reader, &cache);
    let fixed = repair.sync_snapshot().await.unwrap();
    assert_manifest(fixed.files(), &http.fixture.expected);
    assert_eq!(fixed.pages(), closure.pages());
    assert_eq!(fixed.directories(), closure.directories());
    assert_eq!(repair.meters().reused_file_entries_copied, 0);
    assert!(repair.closure_meters().repaired_records > 0);
    assert_eq!(http.fixture.requested_ids(), before);
    let restored = cache.record_for(&id_string(&http.fixture.root)).unwrap();
    assert_manifest(&restored.files, &http.fixture.expected);
    assert_eq!(
        restored.total_entries,
        (fixed.files().len() + fixed.directories().len() - 1) as u64
    );
}
