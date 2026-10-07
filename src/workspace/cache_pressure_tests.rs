//! Small real HTTP roots exercise retention without claiming throughput wins.

use std::{ffi::OsStr, fs};

use asyncfuse::raw::prelude::{Filesystem, Request};

use super::*;
use crate::{
    snapshot::{fuse::Mst2Fuse, CacheLimits, MetadataProofLimits},
    workspace::cache,
};

fn limits() -> CacheLimits {
    CacheLimits {
        max_bytes: 64 * 1024 * 1024,
        proof_headroom_bytes: 8 * 1024 * 1024,
        max_entries: 8192,
        max_owners: 32,
        max_inventory_bytes: 4 * 1024 * 1024,
        max_metadata_bytes: 4 * 1024 * 1024,
        max_root_nodes: 8192,
        max_record_bytes: 1024 * 1024,
        max_scan_millis: 5000,
        max_delete_entries: 128,
        max_delete_bytes: 8 * 1024 * 1024,
    }
}

fn tiny_version(version: u8) -> Fixture {
    let mut fixture = Fixture::new(true, false, false);
    fixture.descriptor.namespace_view_id = [0x22 + version; 32];
    fixture.bodies = BTreeMap::from([
        ("/needed".into(), b"shared needed bytes".to_vec()),
        ("/changed".into(), format!("version-{version}").into_bytes()),
    ]);
    let empty = Page::build(&[]).unwrap();
    let mut entries: Vec<_> = fixture
        .bodies
        .iter()
        .map(|(path, bytes)| {
            Entry::file(
                EntryKind::Regular,
                &path.as_bytes()[1..],
                bytes.len() as u64,
                hash(bytes),
            )
        })
        .collect();
    entries.push(Entry::dir(b"empty", page_id(&empty)));
    entries.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    let root = Page::build(&entries).unwrap();
    fixture.descriptor.metadata_root = page_id(&root);
    fixture.pages = BTreeMap::from([("/".into(), root), ("/empty".into(), empty)]);
    fixture
}

fn cas_path(store: &DurableStore, body: &[u8]) -> std::path::PathBuf {
    store.content_dir().join(hex::encode(hash(body)))
}

fn managed_store(temp: &tempfile::TempDir, id: &str, reader: &SnapshotReader) -> Arc<DurableStore> {
    Arc::new(
        DurableStore::open_for_workspace_with_cache_limits(temp.path(), id, reader, limits())
            .unwrap(),
    )
}

#[tokio::test]
async fn metadata_only_promotion_preserves_live_lazy_reads_and_collects_retired_version() {
    let server = VersionServer::new(vec![tiny_version(0), tiny_version(1)]).await;
    let temp = tempfile::tempdir().unwrap();
    let old_reader = server.reader(0).await;
    let old = managed_store(&temp, "11111111-2222-4333-8444-555555555701", &old_reader);
    assert!(bounded_hydrate(&old, &old_reader).await.complete);
    let old_body = server.fixture.versions[0].bodies["/changed"].clone();
    let shared = server.fixture.versions[0].bodies["/needed"].clone();
    let old_path = cas_path(&old, &old_body);
    let shared_path = cas_path(&old, &shared);
    assert_eq!(fs::read(&old_path).unwrap(), old_body);
    let reader = server.reader(1).await;
    let live = managed_store(&temp, "11111111-2222-4333-8444-555555555702", &reader);
    assert!(!live.cache_retention_known().unwrap());
    old.release_local_pin().unwrap();
    // A pin release leaves actual cache use active. Unknown siblings also
    // prevent deletion; neither lease nor an incomplete pin is a GC proof.
    let scope = live.content_dir().parent().unwrap().to_path_buf();
    let before = fs::read(&old_path).unwrap();
    let first = cache::collect(scope.clone()).await;
    match first {
        Ok(report) => assert_eq!(report.deleted_entries, 0),
        Err(error) => assert_eq!(error.code, SnapshotErrorCode::SnapshotNotReady),
    }
    assert_eq!(fs::read(&old_path).unwrap(), before);
    // This test has no native mount or upper. It explicitly retires only the
    // old store after its release; production does this after native join.
    old.retire_cache_use().unwrap();
    let second = cache::collect(scope.clone()).await;
    match second {
        Ok(report) => assert_eq!(report.deleted_entries, 0),
        Err(error) => assert_eq!(error.code, SnapshotErrorCode::SnapshotNotReady),
    }
    assert_eq!(fs::read(&old_path).unwrap(), old_body);
    let current = &server.fixture.versions[1];
    let body_calls =
        current.object_calls.load(Ordering::SeqCst) + current.raw_calls.load(Ordering::SeqCst);
    cache::promote(live.clone(), &reader, limits())
        .await
        .unwrap();
    assert!(live.cache_retention_known().unwrap());
    assert_eq!(
        current.object_calls.load(Ordering::SeqCst) + current.raw_calls.load(Ordering::SeqCst),
        body_calls
    );
    assert!(!live.root().join("DURABLE_COMPLETE").exists());
    let report = cache::collect(scope).await.unwrap();
    assert!(report.deleted_entries > 0);
    assert!(!old_path.exists());
    assert_eq!(fs::read(&shared_path).unwrap(), shared);
    let fs = Mst2Fuse::from_reader_lazy_with_limits(
        reader,
        Some(live.clone()),
        MetadataProofLimits::default(),
    )
    .await
    .unwrap();
    let needed = fs
        .lookup(Request::default(), 1, OsStr::new("needed"))
        .await
        .unwrap()
        .attr
        .ino;
    let reply = fs
        .read(Request::default(), needed, needed, 0, shared.len() as u32)
        .await
        .unwrap();
    assert_eq!(reply.data.as_ref(), shared);
    assert_eq!(
        current.object_calls.load(Ordering::SeqCst) + current.raw_calls.load(Ordering::SeqCst),
        body_calls
    );
    let changed = fs
        .lookup(Request::default(), 1, OsStr::new("changed"))
        .await
        .unwrap()
        .attr
        .ino;
    let expected = current.bodies["/changed"].clone();
    let reply = fs
        .read(
            Request::default(),
            changed,
            changed,
            0,
            expected.len() as u32,
        )
        .await
        .unwrap();
    assert_eq!(reply.data.as_ref(), expected);
    assert_eq!(fs::read(cas_path(&live, &expected)).unwrap(), expected);
    assert!(!live.root().join("DURABLE_COMPLETE").exists());
}

#[tokio::test]
async fn cancelled_real_http_promotion_leaves_unknown_use_and_retry_keeps_body_unloaded() {
    let fixture = tiny_version(1);
    fixture.hold_metadata.store(true, Ordering::SeqCst);
    let server = Server::new(fixture).await;
    let reader = server.reader().await;
    let temp = tempfile::tempdir().unwrap();
    let store = managed_store(&temp, "11111111-2222-4333-8444-555555555703", &reader);
    let use_record = fs::read(store.root().join("CACHE_USE.json")).unwrap();
    let source = reader.clone();
    let target = store.clone();
    let task = tokio::spawn(async move { cache::promote(target, &source, limits()).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while server.fixture.metadata_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        fs::read(store.root().join("CACHE_USE.json")).unwrap(),
        use_record
    );
    assert!(!store.cache_retention_known().unwrap());
    assert!(!store.root().join("DURABLE_COMPLETE").exists());
    assert_eq!(server.fixture.object_calls.load(Ordering::SeqCst), 0);
    assert_eq!(server.fixture.raw_calls.load(Ordering::SeqCst), 0);
    server.fixture.hold_metadata.store(false, Ordering::SeqCst);
    server.fixture.metadata_release.notify_waiters();
    cache::promote(store.clone(), &reader, limits())
        .await
        .unwrap();
    assert!(store.cache_retention_known().unwrap());
    assert_eq!(server.fixture.object_calls.load(Ordering::SeqCst), 0);
    assert_eq!(server.fixture.raw_calls.load(Ordering::SeqCst), 0);
    assert!(!store.root().join("DURABLE_COMPLETE").exists());
}
