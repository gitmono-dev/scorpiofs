//! Real HTTP lazy reads learn shared hints without hydrating any body.

use std::{ffi::OsStr, fs, path::PathBuf, time::Duration};

use asyncfuse::raw::prelude::{Filesystem, Request};
use mst2_codec::metapage::BranchChild;

use super::*;
use crate::{
    snapshot::{
        content::{ContentBudget, PROCESS_CONSTRUCTION_BYTES},
        fuse::Mst2Fuse,
        incremental::page_write_hooks,
        CacheLimits, ScopeCache,
    },
    workspace::cache,
};

fn fixture(version: u8) -> Fixture {
    let mut fixture = Fixture::new(true, false, false);
    fixture.descriptor.namespace_view_id = [0x70 + version; 32];
    let stable = b"unchanged nested file".to_vec();
    let changed = format!("version-{version}").into_bytes();
    let child = Page::build(&[Entry::file(
        EntryKind::Regular,
        b"needed",
        stable.len() as u64,
        hash(&stable),
    )])
    .unwrap();
    let root = Page::build(&[
        Entry::file(
            EntryKind::Regular,
            b"changed",
            changed.len() as u64,
            hash(&changed),
        ),
        Entry::dir(b"stable", page_id(&child)),
    ])
    .unwrap();
    fixture.descriptor.metadata_root = page_id(&root);
    fixture.pages = BTreeMap::from([("/".into(), root), ("/stable".into(), child)]);
    fixture.bodies = BTreeMap::from([
        ("/changed".into(), changed),
        ("/stable/needed".into(), stable),
    ]);
    fixture
}

fn managed(temp: &tempfile::TempDir, owner: u128, reader: &SnapshotReader) -> Arc<DurableStore> {
    Arc::new(
        DurableStore::open_for_workspace_with_cache_limits(
            temp.path(),
            &uuid::Uuid::from_u128(owner).to_string(),
            reader,
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
            },
        )
        .unwrap(),
    )
}

fn scope(store: &DurableStore) -> PathBuf {
    store.content_dir().parent().unwrap().to_path_buf()
}

fn page_path(store: &DurableStore, bytes: &[u8]) -> PathBuf {
    scope(store).join("pages").join(hex::encode(page_id(bytes)))
}

fn metadata_only(store: &DurableStore, fixture: &Fixture) {
    assert!(!store.root().join("DURABLE_COMPLETE").exists());
    assert!(!store.cache_retention_known().unwrap());
    assert_eq!(fixture.object_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.raw_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.map_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.chunk_calls.load(Ordering::SeqCst), 0);
    assert!(fs::read_dir(store.content_dir()).unwrap().next().is_none());
}

async fn nested_file(view: &Mst2Fuse) -> u64 {
    let directory = view
        .lookup(Request::default(), 1, OsStr::new("stable"))
        .await
        .unwrap()
        .attr
        .ino;
    view.lookup(Request::default(), directory, OsStr::new("needed"))
        .await
        .unwrap()
        .attr
        .ino
}

#[tokio::test]
async fn cold_lazy_learning_reuses_unchanged_subtree_across_real_http_commit_update() {
    let server = VersionServer::new(vec![fixture(0), fixture(1)]).await;
    let temp = tempfile::tempdir().unwrap();
    let old_reader = server.reader(0).await;
    let old = managed(&temp, 741, &old_reader);
    let old_view = Mst2Fuse::from_reader_lazy(old_reader.clone(), Some(old.clone()))
        .await
        .unwrap();
    let old_fixture = &server.fixture.versions[0];
    assert_eq!(old_fixture.metadata_calls.load(Ordering::SeqCst), 1);
    assert_eq!(old_fixture.metadata_pages.load(Ordering::SeqCst), 1);
    let old_file = nested_file(&old_view).await;
    assert!(old_file > 1);
    assert_eq!(old_fixture.metadata_calls.load(Ordering::SeqCst), 2);
    assert_eq!(old_fixture.metadata_pages.load(Ordering::SeqCst), 2);
    let cache = ScopeCache::open(scope(&old)).unwrap();
    for bytes in old_fixture.pages.values() {
        assert_eq!(
            cache
                .read_page_verified_bounded(&id(&page_id(bytes)))
                .unwrap(),
            Some(bytes.clone())
        );
    }
    metadata_only(&old, old_fixture);
    drop(old_view);
    drop(old_reader);

    let reader = server.reader(1).await;
    let live = managed(&temp, 742, &reader);
    assert_ne!(live.root(), old.root());
    assert_eq!(live.content_dir(), old.content_dir());
    assert_ne!(reader.snapshot_id(), old_fixture.sid());
    let view = Mst2Fuse::from_reader_lazy(reader, Some(live.clone()))
        .await
        .unwrap();
    let current = &server.fixture.versions[1];
    assert_eq!(current.metadata_calls.load(Ordering::SeqCst), 1);
    assert_eq!(current.metadata_pages.load(Ordering::SeqCst), 1);
    let wire = current.metadata_wire_bytes.load(Ordering::SeqCst);
    assert!(wire > 0);
    assert!(nested_file(&view).await > 1);
    assert_eq!(current.metadata_calls.load(Ordering::SeqCst), 1);
    assert_eq!(current.metadata_pages.load(Ordering::SeqCst), 1);
    assert_eq!(current.metadata_wire_bytes.load(Ordering::SeqCst), wire);
    assert_eq!(
        fs::read(page_path(&live, &current.pages["/stable"])).unwrap(),
        current.pages["/stable"]
    );
    metadata_only(&live, current);
    metadata_only(&old, old_fixture);
}

#[tokio::test]
async fn noncanonical_directory_proof_learns_no_wire_page_and_preserves_safe_hints() {
    let mut fixture = fixture(0);
    let children: Vec<_> = [b'a', b'b']
        .into_iter()
        .map(|name| {
            Page::build(&[Entry::file(EntryKind::Regular, &[name], 1, hash(b"x"))]).unwrap()
        })
        .collect();
    // Hash-valid and structurally decodable, but two entries must use a
    // canonical leaf. Local child hints avoid broadening the HTTP fixture.
    let root = Page::Branch {
        prefix: Vec::new(),
        terminal: None,
        children: children
            .iter()
            .zip([b'a', b'b'])
            .map(|(bytes, label)| BranchChild {
                label,
                subtree_entries: 1,
                child_page_id: page_id(bytes),
            })
            .collect(),
    }
    .encode()
    .unwrap();
    fixture.descriptor.metadata_root = page_id(&root);
    fixture.pages = BTreeMap::from([("/".into(), root.clone())]);
    let server = Server::new(fixture).await;
    let reader = server.reader().await;
    let temp = tempfile::tempdir().unwrap();
    let store = managed(&temp, 743, &reader);
    let cache = ScopeCache::open(scope(&store)).unwrap();
    for bytes in &children {
        cache.put_page(&id(&page_id(bytes)), bytes).unwrap();
    }
    let result = Mst2Fuse::from_reader_lazy(reader, Some(store.clone())).await;
    assert_eq!(
        result.err().unwrap().code,
        SnapshotErrorCode::IntegrityError
    );
    assert!(!page_path(&store, &root).exists());
    for bytes in children {
        assert_eq!(fs::read(page_path(&store, &bytes)).unwrap(), bytes);
    }
    assert_eq!(server.fixture.metadata_calls.load(Ordering::SeqCst), 1);
    assert_eq!(server.fixture.metadata_pages.load(Ordering::SeqCst), 1);
    metadata_only(&store, &server.fixture);
}

#[tokio::test]
async fn cancelled_lazy_waiter_keeps_actual_page_store_fence_and_admission_until_exit() {
    // Fill the real process quota in a separate test process, so this test
    // cannot take headroom from unrelated parallel content regressions.
    let output = tokio::time::timeout(
        Duration::from_secs(90),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("workspace::hydrate::tests::lazy_metadata_learning::cancelled_lazy_page_learning_process_credit_worker")
            .arg("--test-threads=1")
            .arg("--nocapture")
            .env("SCORPIOFS_LAZY_PAGE_LEARNING_PRESSURE_WORKER", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("isolated lazy page learning pressure test exceeded its deadline")
    .expect("failed to launch the lazy page learning pressure worker");
    assert!(
        output.status.success(),
        "lazy page learning worker failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout)
        .contains("LAZY_PAGE_LEARNING_ACTUAL_CREDIT_LIFETIME_OK"));
}

#[tokio::test]
async fn cancelled_lazy_page_learning_process_credit_worker() {
    if std::env::var("SCORPIOFS_LAZY_PAGE_LEARNING_PRESSURE_WORKER").as_deref() != Ok("1") {
        return;
    }
    let first_fill = ContentBudget::new(ContentBudgetLimits::default())
        .reserve_test_capacity(0, 32 * 1024 * 1024)
        .unwrap();
    let second_fill = ContentBudget::new(ContentBudgetLimits::default())
        .reserve_test_capacity(0, 32 * 1024 * 1024 - 1024)
        .unwrap();
    let server = Server::new(fixture(0)).await;
    let reader = server
        .reader()
        .await
        .with_content_limits(ContentBudgetLimits::new(128 * 1024 * 1024, 1024).unwrap());
    let observed = reader.clone();
    let temp = tempfile::tempdir().unwrap();
    let store = managed(&temp, 744, &reader);
    let directory = scope(&store);
    let root_path = page_path(&store, &server.fixture.pages["/"]);
    let owner_path = store.root().to_path_buf();
    let use_before = fs::read(owner_path.join("CACHE_USE.json")).unwrap();
    let weak = Arc::downgrade(&store);
    let mut barrier = page_write_hooks::install(&root_path, false);
    let task = tokio::spawn(async move { Mst2Fuse::from_reader_lazy(reader, Some(store)).await });
    tokio::time::timeout(Duration::from_secs(10), barrier.entered())
        .await
        .unwrap();
    assert!(!root_path.exists());
    assert_eq!(
        fs::read_dir(directory.join("cache-writers"))
            .unwrap()
            .count(),
        1
    );
    assert_eq!(fs::read_dir(directory.join("pages")).unwrap().count(), 1);
    assert_eq!(observed.content_usage().construction_bytes, 1024);
    assert_eq!(
        FetchCoordinator::process_content_usage().construction_bytes,
        PROCESS_CONSTRUCTION_BYTES
    );
    task.abort();
    assert!(task.await.err().unwrap().is_cancelled());
    assert!(
        weak.upgrade().is_some(),
        "actual writer still owns the store"
    );
    assert_eq!(observed.content_usage().construction_bytes, 1024);
    assert_eq!(
        FetchCoordinator::process_content_usage().construction_bytes,
        PROCESS_CONSTRUCTION_BYTES
    );
    assert_eq!(
        cache::collect(directory.clone()).await.unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    assert_eq!(
        fs::read(owner_path.join("CACHE_USE.json")).unwrap(),
        use_before
    );
    assert!(!owner_path.join("DURABLE_COMPLETE").exists());
    assert!(!root_path.exists());
    assert_eq!(
        fs::read_dir(directory.join("cache-writers"))
            .unwrap()
            .count(),
        1
    );

    // A distinct reader has local headroom. The process credit retained by
    // the canceled actual writer prevents a second uncharged job or temp.
    let next_reader = server.reader().await;
    let next_observed = next_reader.clone();
    let next = managed(&temp, 747, &next_reader);
    let next_view = Mst2Fuse::from_reader_lazy(next_reader, Some(next.clone()))
        .await
        .unwrap();
    assert!(next_view
        .lookup(Request::default(), 1, OsStr::new("changed"))
        .await
        .is_ok());
    assert_eq!(server.fixture.metadata_calls.load(Ordering::SeqCst), 2);
    assert_eq!(server.fixture.metadata_pages.load(Ordering::SeqCst), 2);
    assert_eq!(next_observed.content_usage().construction_bytes, 0);
    assert_eq!(
        fs::read_dir(directory.join("cache-writers"))
            .unwrap()
            .count(),
        1
    );
    assert_eq!(fs::read_dir(directory.join("pages")).unwrap().count(), 1);
    assert!(!root_path.exists());
    metadata_only(&next, &server.fixture);

    barrier.release();
    tokio::time::timeout(Duration::from_secs(10), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fs::read(&root_path).unwrap(), server.fixture.pages["/"]);
    assert_eq!(observed.content_usage().construction_bytes, 0);
    assert_eq!(
        FetchCoordinator::process_content_usage().construction_bytes,
        PROCESS_CONSTRUCTION_BYTES - 1024
    );
    assert_eq!(
        fs::read_dir(directory.join("cache-writers"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(fs::read_dir(directory.join("pages")).unwrap().count(), 1);
    assert_eq!(
        fs::read(owner_path.join("CACHE_USE.json")).unwrap(),
        use_before
    );
    assert!(!owner_path.join("DURABLE_COMPLETE").exists());
    // The returned credit admits real learning for the next lazy directory.
    assert!(nested_file(&next_view).await > 1);
    assert_eq!(server.fixture.metadata_calls.load(Ordering::SeqCst), 3);
    assert_eq!(server.fixture.metadata_pages.load(Ordering::SeqCst), 3);
    assert_eq!(
        fs::read(page_path(&next, &server.fixture.pages["/stable"])).unwrap(),
        server.fixture.pages["/stable"]
    );
    assert_eq!(next_observed.content_usage().construction_bytes, 0);
    assert_eq!(
        FetchCoordinator::process_content_usage().construction_bytes,
        PROCESS_CONSTRUCTION_BYTES - 1024
    );
    metadata_only(&next, &server.fixture);
    drop(first_fill);
    drop(second_fill);
    assert_eq!(
        FetchCoordinator::process_content_usage().construction_bytes,
        0
    );
    println!("LAZY_PAGE_LEARNING_ACTUAL_CREDIT_LIFETIME_OK");
}

#[tokio::test]
async fn failed_learning_preserves_old_safe_pages_and_proved_view_then_retry_learns() {
    let server = VersionServer::new(vec![fixture(0), fixture(1)]).await;
    let temp = tempfile::tempdir().unwrap();
    let reader = server.reader(0).await;
    let old = managed(&temp, 745, &reader);
    let old_view = Mst2Fuse::from_reader_lazy(reader, Some(old.clone()))
        .await
        .unwrap();
    assert!(nested_file(&old_view).await > 1);
    drop(old_view);
    let previous = &server.fixture.versions[0];
    let old_pages: Vec<_> = previous
        .pages
        .values()
        .map(|bytes| (page_path(&old, bytes), bytes.clone()))
        .collect();
    let reader = server.reader(1).await;
    let live = managed(&temp, 746, &reader);
    let current = &server.fixture.versions[1];
    let new_root = page_path(&live, &current.pages["/"]);
    let use_before = fs::read(live.root().join("CACHE_USE.json")).unwrap();
    let mut barrier = page_write_hooks::install(&new_root, true);
    let target = live.clone();
    let source = reader.clone();
    let task = tokio::spawn(async move { Mst2Fuse::from_reader_lazy(source, Some(target)).await });
    tokio::time::timeout(Duration::from_secs(10), barrier.entered())
        .await
        .unwrap();
    // The actual first job consumed its registration. Dropping that old
    // barrier must leave a newly registered retry hook at the same path.
    let mut retry_barrier = page_write_hooks::install(&new_root, false);
    barrier.release();
    let view = task.await.unwrap().unwrap();
    assert!(nested_file(&view).await > 1);
    assert!(!new_root.exists());
    assert_eq!(current.metadata_calls.load(Ordering::SeqCst), 1);
    assert_eq!(current.metadata_pages.load(Ordering::SeqCst), 1);
    let wire = current.metadata_wire_bytes.load(Ordering::SeqCst);
    for (path, bytes) in &old_pages {
        assert_eq!(fs::read(path).unwrap(), *bytes);
    }
    assert_eq!(
        fs::read_dir(scope(&live).join("cache-writers"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        fs::read_dir(scope(&live).join("pages")).unwrap().count(),
        old_pages.len()
    );
    assert_eq!(
        fs::read(live.root().join("CACHE_USE.json")).unwrap(),
        use_before
    );
    metadata_only(&live, current);
    drop(view);
    let target = live.clone();
    let retry_task =
        tokio::spawn(async move { Mst2Fuse::from_reader_lazy(reader, Some(target)).await });
    tokio::time::timeout(Duration::from_secs(10), retry_barrier.entered())
        .await
        .unwrap();
    assert!(!new_root.exists());
    for (path, bytes) in &old_pages {
        assert_eq!(fs::read(path).unwrap(), *bytes);
    }
    retry_barrier.release();
    let retry = retry_task.await.unwrap().unwrap();
    assert!(nested_file(&retry).await > 1);
    assert_eq!(current.metadata_calls.load(Ordering::SeqCst), 2);
    assert_eq!(current.metadata_pages.load(Ordering::SeqCst), 2);
    assert!(current.metadata_wire_bytes.load(Ordering::SeqCst) > wire);
    assert_eq!(fs::read(new_root).unwrap(), current.pages["/"]);
    for (path, bytes) in old_pages {
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
    metadata_only(&live, current);
}
