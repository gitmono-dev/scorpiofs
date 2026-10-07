//! File-closure durability regressions. The crash cases terminate a child
//! process with SIGKILL; they do not simulate a power loss or storage cache loss.

use std::{
    cell::RefCell,
    fs::OpenOptions,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use super::*;

#[test]
fn duplicated_descriptor_cannot_extend_transaction_lifetime() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let guard = store.transaction().unwrap();
    let duplicate = guard.0.try_clone().unwrap();
    assert!(store.try_transaction().unwrap().is_none());
    drop(guard);
    let next = store.try_transaction().unwrap();
    assert!(
        next.is_some(),
        "a fork/dup descriptor cannot retain a completed transaction's lock"
    );
    drop(next);
    drop(duplicate);
}

struct Fault {
    path: PathBuf,
    phase: &'static str,
    after_complete_rename: bool,
    armed: bool,
}

thread_local! {
    static FAULT: RefCell<Option<Fault>> = const { RefCell::new(None) };
    static SYNC_COUNTS: RefCell<Option<SyncCounts>> = const { RefCell::new(None) };
}

#[derive(Clone)]
struct SyncCounts {
    root: PathBuf,
    // Captured immediately after the real sync_all returns successfully.
    journal_chunks: Vec<(usize, usize)>,
    journal_compactions: usize,
    files: Vec<PathBuf>,
}

pub(super) struct SyncCounter;

impl SyncCounter {
    pub(super) fn install(root: &Path) -> Self {
        SYNC_COUNTS.with(|counts| {
            assert!(counts.borrow().is_none());
            *counts.borrow_mut() = Some(SyncCounts {
                root: root.into(),
                journal_chunks: Vec::new(),
                journal_compactions: 0,
                files: Vec::new(),
            });
        });
        Self
    }

    fn counts(&self) -> SyncCounts {
        SYNC_COUNTS.with(|counts| counts.borrow().as_ref().unwrap().clone())
    }

    pub(super) fn synced_files(&self) -> Vec<PathBuf> {
        self.counts().files
    }
}

impl Drop for SyncCounter {
    fn drop(&mut self) {
        SYNC_COUNTS.with(|counts| *counts.borrow_mut() = None);
    }
}

pub(super) fn record_journal_sync(root: &Path, bytes: &[u8]) {
    SYNC_COUNTS.with(|counts| {
        if let Some(counts) = counts.borrow_mut().as_mut() {
            if counts.root == root {
                counts.journal_chunks.push((
                    bytes.len(),
                    bytes.iter().filter(|byte| **byte == b'\n').count(),
                ));
            }
        }
    });
}

pub(super) fn record_journal_compaction(root: &Path) {
    SYNC_COUNTS.with(|counts| {
        if let Some(counts) = counts.borrow_mut().as_mut() {
            if counts.root == root {
                counts.journal_compactions += 1;
            }
        }
    });
}

pub(super) fn record_file_sync(path: &Path) {
    SYNC_COUNTS.with(|counts| {
        if let Some(counts) = counts.borrow_mut().as_mut() {
            if path.starts_with(&counts.root) {
                counts.files.push(path.into());
            }
        }
    });
}

pub(in crate::snapshot) struct FaultGuard;

impl FaultGuard {
    pub(in crate::snapshot) fn install(
        path: &Path,
        phase: &'static str,
        after_complete_rename: bool,
    ) -> Self {
        FAULT.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(Fault {
                path: path.into(),
                phase,
                after_complete_rename,
                armed: !after_complete_rename,
            });
        });
        Self
    }
}

impl Drop for FaultGuard {
    fn drop(&mut self) {
        FAULT.with(|slot| *slot.borrow_mut() = None);
    }
}

pub(super) fn checkpoint(path: &Path, phase: &str) -> Result<(), SnapshotError> {
    let fail = FAULT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(fault) = slot.as_mut() else {
            return false;
        };
        if fault.path != path {
            return false;
        }
        if fault.after_complete_rename && phase == "complete-renamed" {
            fault.armed = true;
        }
        if fault.armed && fault.phase == phase {
            *slot = None; // The revocation path must still be able to persist.
            true
        } else {
            false
        }
    });
    if fail {
        return Err(io_err(io::Error::other("injected durability I/O failure")));
    }
    #[cfg(unix)]
    if std::env::var_os("SCORPIO_DURABLE_WORKER_ROOT").as_deref() == Some(path.as_os_str()) {
        if std::env::var("SCORPIO_DURABLE_CRASH_PHASE").as_deref() == Ok(phase) {
            // SIGKILL does not run unwinding, destructors or exit handlers.
            // SAFETY: only the dedicated test child opts into this hook.
            if unsafe { libc::kill(libc::getpid(), libc::SIGKILL) } != 0 {
                return Err(io_err(io::Error::last_os_error()));
            }
            // Darwin can return from kill before delivering the signal.
            // Wait for actual termination instead of racing a normal exit.
            loop {
                // SAFETY: the dedicated crash worker has a pending SIGKILL.
                unsafe {
                    libc::pause();
                }
            }
        }
        if std::env::var("SCORPIO_DURABLE_PAUSE_PHASE").as_deref() == Ok(phase) {
            fs::write(path.join("worker-ready"), b"ready").map_err(io_err)?;
            let deadline = Instant::now() + Duration::from_secs(10);
            while !path.join("worker-release").exists() {
                if Instant::now() >= deadline {
                    return Err(io_err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "test worker was not released",
                    )));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    Ok(())
}

fn view() -> ViewMeta {
    ViewMeta {
        snapshot_id: "sha256:fixed-snapshot".into(),
        namespace_view_id: "sha256:fixed-namespace".into(),
        scope: "/project".into(),
        lease_id: "lease-durable".into(),
    }
}

fn files() -> Vec<SnapshotFile> {
    vec![
        SnapshotFile {
            rel_path: "a.txt".into(),
            fs_kind: "regular".into(),
            content_digest: digest_of(b"a"),
            size: 1,
        },
        SnapshotFile {
            rel_path: "nested/b.txt".into(),
            fs_kind: "executable".into(),
            content_digest: digest_of(b"second body"),
            size: 11,
        },
    ]
}

fn body(file: &SnapshotFile) -> Vec<u8> {
    match file.rel_path.as_str() {
        "a.txt" => b"a".to_vec(),
        "nested/b.txt" => b"second body".to_vec(),
        other => panic!("unknown fixture {other}"),
    }
}

async fn hydrate(store: &DurableStore) -> HydrateReport {
    store
        .hydrate_with(&view(), &files(), |file| {
            let data = body(file);
            async move { Ok(data) }
        })
        .await
        .unwrap()
}

fn many_files(count: usize) -> Vec<SnapshotFile> {
    (0..count)
        .map(|index| {
            let rel_path = format!("file-{index:04}");
            SnapshotFile {
                size: rel_path.len() as u64,
                content_digest: digest_of(rel_path.as_bytes()),
                rel_path,
                fs_kind: "regular".into(),
            }
        })
        .collect()
}

#[tokio::test]
async fn warm_1151_file_resume_bounds_real_journal_syncs_and_syncs_every_blob() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let manifest = many_files(1151);
    // Seed unsynced bytes: the completion path must explicitly sync every
    // cache hit even though the fetch callbacks are never used.
    for file in &manifest {
        fs::write(
            store.blob_path(&file.content_digest).unwrap(),
            file.rel_path.as_bytes(),
        )
        .unwrap();
    }
    store
        .hydrate_with(&view(), &manifest, |_| async { panic!("seeded CAS hit") })
        .await
        .unwrap();
    assert!(store.is_complete().unwrap());

    let expected_synced: HashSet<_> = manifest
        .iter()
        .map(|file| store.blob_path(&file.content_digest).unwrap())
        .collect();
    for mode in ["sequential", "concurrent", "batches"] {
        let counter = SyncCounter::install(temp.path());
        let report = match mode {
            "sequential" => {
                store
                    .hydrate_with(&view(), &manifest, |_| async { panic!("warm source read") })
                    .await
            }
            "concurrent" => {
                store
                    .hydrate_concurrent(&view(), &manifest, 8, |_| {
                        Box::pin(async { panic!("warm source read") })
                    })
                    .await
            }
            "batches" => {
                store
                    .hydrate_batches(
                        &view(),
                        &manifest,
                        4,
                        4,
                        |_| Box::pin(async { panic!("warm objects read") }),
                        |_| Box::pin(async { panic!("warm large read") }),
                    )
                    .await
            }
            _ => unreachable!(),
        }
        .unwrap();
        let counts = counter.counts();
        drop(counter);
        assert_eq!(report.fetched, 0, "{mode}");
        assert_eq!(report.resumed, 1151, "{mode}");
        assert_eq!(report.total_files, 1151, "{mode}");
        assert!(report.complete, "{mode}");
        // The regression budget is independent of a predicted report field:
        // each event was recorded after an actual journal sync_all succeeded.
        assert!(
            counts.journal_chunks.len() <= 9,
            "{mode}: {} real journal syncs for 1151 files",
            counts.journal_chunks.len()
        );
        assert_eq!(counts.journal_compactions, 1, "{mode}");
        for (bytes, records) in &counts.journal_chunks {
            assert!(*bytes > 0 && *bytes <= 256 * 1024, "{mode}");
            assert!(*records > 0 && *records <= 128, "{mode}");
        }
        assert_eq!(counts.files.len(), 1151, "{mode}");
        assert_eq!(
            counts.files.into_iter().collect::<HashSet<_>>(),
            expected_synced,
            "{mode}: every unique CAS dependency must still be synced"
        );
        assert_eq!(store.read_journal().unwrap().len(), 1151, "{mode}");
        assert_eq!(
            fs::read(temp.path().join(JOURNAL_FILE))
                .unwrap()
                .iter()
                .filter(|byte| **byte == b'\n')
                .count(),
            1151,
            "{mode}: completed journal contains exactly one logical manifest"
        );
        assert_eq!(store.manifest().unwrap(), manifest, "{mode}");
        assert!(store.is_complete().unwrap(), "{mode}");
    }
}

#[tokio::test]
async fn concurrent_cold_appends_keep_complete_json_records_and_bounded_syncs() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let manifest = many_files(257);
    let counter = SyncCounter::install(temp.path());
    let report = store
        .hydrate_concurrent(&view(), &manifest, 16, |file| {
            Box::pin(async move {
                tokio::task::yield_now().await;
                Ok(Arc::new(file.rel_path.into_bytes()))
            })
        })
        .await
        .unwrap();
    let counts = counter.counts();
    drop(counter);
    assert_eq!(report.fetched, 257);
    assert_eq!(report.resumed, 0);
    assert!(counts.journal_chunks.len() <= 8);
    assert_eq!(counts.journal_compactions, 1);
    assert!(counts
        .journal_chunks
        .iter()
        .all(|(bytes, records)| *bytes <= 256 * 1024 && *records <= 128));
    let journal = store.read_journal().unwrap();
    assert_eq!(journal.len(), 257);
    for file in &manifest {
        assert_eq!(journal[&file.rel_path].digest, file.content_digest);
        assert_eq!(journal[&file.rel_path].size, file.size);
    }
    assert_eq!(store.manifest().unwrap(), manifest);
}

#[tokio::test]
async fn long_legal_paths_flush_at_the_byte_limit_before_the_record_limit() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let parent = vec!["x".repeat(255); 15].join("/");
    let manifest: Vec<_> = (0..128)
        .map(|index| SnapshotFile {
            rel_path: format!("{parent}/file-{index:04}"),
            fs_kind: "regular".into(),
            size: 1,
            content_digest: digest_of(b"a"),
        })
        .collect();
    let counter = SyncCounter::install(temp.path());
    store
        .hydrate_with(&view(), &manifest, |_| async { Ok(b"a".to_vec()) })
        .await
        .unwrap();
    let counts = counter.counts();
    drop(counter);
    assert!(counts
        .journal_chunks
        .iter()
        .any(|(bytes, records)| { *bytes > 128 * 1024 && *records < 128 }));
    assert!(counts
        .journal_chunks
        .iter()
        .all(|(bytes, records)| *bytes <= 256 * 1024 && *records <= 128));
    assert_eq!(store.read_journal().unwrap().len(), 128);
    assert_eq!(counts.files.len(), 1, "alias content is synced once");
    assert_eq!(store.manifest().unwrap(), manifest);
}

#[tokio::test]
async fn journal_write_and_sync_failures_never_publish_complete() {
    for phase in [
        "journal-write",
        "journal-file-sync",
        "journal-chunk-durable",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let fault = FaultGuard::install(temp.path(), phase, false);
        let error = store
            .hydrate_with(&view(), &files(), |file| {
                let bytes = body(file);
                async move { Ok(bytes) }
            })
            .await
            .unwrap_err();
        drop(fault);
        assert_eq!(error.code, SnapshotErrorCode::Internal, "{phase}");
        assert!(!store.is_complete().unwrap(), "{phase}");
        assert!(!temp.path().join(COMPLETE_MARKER).exists(), "{phase}");
        assert_eq!(store.verify_all(&files()).unwrap(), 2, "{phase}");
        let report = store
            .hydrate_with(&view(), &files(), |_| async {
                panic!("journal failure must not discard valid CAS content")
            })
            .await
            .unwrap();
        assert_eq!(report.fetched, 0, "{phase}");
        assert!(store.is_complete().unwrap(), "{phase}");
    }
}

#[tokio::test]
async fn missing_or_tampered_commit_dependencies_revoke_completion_on_reopen() {
    // This is an independent on-disk oracle: mutations happen after a
    // successful commit, without using the writer to construct bad records.
    for damage in [
        "blob-missing",
        "blob-truncated",
        "blob-tampered",
        "manifest-missing",
        "manifest-tampered",
        "pin-missing",
        "pin-tampered",
        "view-missing",
        "view-scope",
        "view-namespace",
        "marker-count",
        "marker-namespace",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        hydrate(&store).await;
        assert!(store.is_complete().unwrap());
        let blob = store.blob_path(&files()[1].content_digest).unwrap();
        match damage {
            "blob-missing" => fs::remove_file(blob).unwrap(),
            "blob-truncated" => fs::write(blob, b"second").unwrap(),
            "blob-tampered" => fs::write(blob, b"SECOND BODY").unwrap(),
            "manifest-missing" => fs::remove_file(temp.path().join(MANIFEST_FILE)).unwrap(),
            "manifest-tampered" => {
                let mut manifest = files();
                manifest.pop();
                fs::write(
                    temp.path().join(MANIFEST_FILE),
                    serde_json::to_vec(&manifest).unwrap(),
                )
                .unwrap();
            }
            "pin-missing" => fs::remove_file(temp.path().join(PIN_FILE)).unwrap(),
            "pin-tampered" => fs::write(temp.path().join(PIN_FILE), b"{}").unwrap(),
            "view-missing" => fs::remove_file(temp.path().join(VIEW_FILE)).unwrap(),
            "view-scope" | "view-namespace" => {
                let mut changed = view();
                if damage == "view-scope" {
                    changed.scope = "/other".into();
                } else {
                    changed.namespace_view_id = "sha256:other".into();
                }
                fs::write(
                    temp.path().join(VIEW_FILE),
                    serde_json::to_vec(&changed).unwrap(),
                )
                .unwrap();
            }
            "marker-count" | "marker-namespace" => {
                let path = temp.path().join(COMPLETE_MARKER);
                let mut record: serde_json::Value =
                    serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                if damage == "marker-count" {
                    record["files"] = serde_json::json!(1);
                } else {
                    record["namespace_view_id"] = serde_json::json!("sha256:other");
                }
                fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
            }
            _ => unreachable!(),
        }
        let reopened = DurableStore::open(temp.path()).unwrap();
        assert!(!reopened.is_complete().unwrap(), "{damage}");
        assert!(!reopened.is_pinned().unwrap(), "{damage}");
        assert!(!temp.path().join(COMPLETE_MARKER).exists(), "{damage}");
        assert!(temp.path().join(REPAIR_FILE).exists(), "{damage}");
        assert_eq!(
            reopened.manifest().unwrap_err().code,
            SnapshotErrorCode::SnapshotNotReady,
            "{damage}"
        );
    }
}

#[tokio::test]
async fn legacy_snapshot_only_marker_is_not_upgraded_without_verification() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    hydrate(&store).await;
    fs::write(
        temp.path().join(COMPLETE_MARKER),
        serde_json::to_vec(&serde_json::json!({
            "snapshot_id": view().snapshot_id,
            "namespace_view_id": view().namespace_view_id,
            "files": 2, "bytes": 12, "hydrated_at_unix": 1
        }))
        .unwrap(),
    )
    .unwrap();
    let reopened = DurableStore::open(temp.path()).unwrap();
    assert!(!reopened.is_complete().unwrap());
    let report = hydrate(&reopened).await;
    assert_eq!(report.fetched, 0);
    assert_eq!(report.resumed, 2);
    assert!(reopened.is_complete().unwrap());
    assert!(reopened.is_pinned().unwrap());
}

#[tokio::test]
async fn a_rebound_pin_digest_does_not_hide_wrong_identity_or_missing_dependencies() {
    for damage in ["namespace", "scope", "lease", "dependencies", "manifest"] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        hydrate(&store).await;
        let mut pin: PinRecord =
            serde_json::from_slice(&fs::read(temp.path().join(PIN_FILE)).unwrap()).unwrap();
        match damage {
            "namespace" => pin.namespace_view_id = "another-namespace".into(),
            "scope" => pin.scope = "/another".into(),
            "lease" => pin.lease_id = "another-lease".into(),
            "dependencies" => {
                pin.blobs.pop();
            }
            "manifest" => pin.manifest_digest = digest_of(b"a different manifest"),
            _ => unreachable!(),
        }
        let pin_bytes = serde_json::to_vec(&pin).unwrap();
        fs::write(temp.path().join(PIN_FILE), &pin_bytes).unwrap();
        let mut marker: CompleteMarker =
            serde_json::from_slice(&fs::read(temp.path().join(COMPLETE_MARKER)).unwrap()).unwrap();
        marker.pin_digest = digest_of(&pin_bytes);
        fs::write(
            temp.path().join(COMPLETE_MARKER),
            serde_json::to_vec(&marker).unwrap(),
        )
        .unwrap();
        assert!(!store.is_complete().unwrap(), "semantic binding: {damage}");
        assert!(!temp.path().join(COMPLETE_MARKER).exists());
    }
}

#[tokio::test]
async fn repair_failure_revokes_old_complete_in_all_three_hydration_cores() {
    for core in ["sequential", "concurrent", "batch"] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        hydrate(&store).await;
        // Use the same store handle: revocation cannot depend on an opener
        // detecting damage before the failed repair starts.
        fs::remove_file(store.blob_path(&files()[1].content_digest).unwrap()).unwrap();
        let unavailable = || {
            SnapshotError::new(
                SnapshotErrorCode::ObjectUnavailable,
                "source unavailable during repair",
            )
        };
        let result = match core {
            "sequential" => {
                store
                    .hydrate_with(&view(), &files(), |_| async { Err(unavailable()) })
                    .await
            }
            "concurrent" => {
                store
                    .hydrate_concurrent(&view(), &files(), 4, |_| {
                        Box::pin(async {
                            Err(SnapshotError::new(
                                SnapshotErrorCode::ObjectUnavailable,
                                "source unavailable",
                            ))
                        })
                    })
                    .await
            }
            "batch" => {
                store
                    .hydrate_batches(
                        &view(),
                        &files(),
                        4,
                        4,
                        |_| {
                            Box::pin(async {
                                Err(SnapshotError::new(
                                    SnapshotErrorCode::ObjectUnavailable,
                                    "batch unavailable",
                                ))
                            })
                        },
                        |_| {
                            Box::pin(async {
                                Err(SnapshotError::new(
                                    SnapshotErrorCode::ObjectUnavailable,
                                    "large unavailable",
                                ))
                            })
                        },
                    )
                    .await
            }
            _ => unreachable!(),
        };
        assert_eq!(
            result.unwrap_err().code,
            SnapshotErrorCode::ObjectUnavailable,
            "{core}"
        );
        assert!(!store.is_complete().unwrap(), "{core}");
        assert!(!temp.path().join(COMPLETE_MARKER).exists(), "{core}");
        assert!(
            !DurableStore::open(temp.path())
                .unwrap()
                .is_complete()
                .unwrap(),
            "{core}"
        );
    }
}

#[tokio::test]
async fn batch_aliases_commit_logical_totals_and_one_retained_content_dependency() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let manifest: Vec<_> = ["a", "nested/b", "nested/link"]
        .into_iter()
        .map(|path| SnapshotFile {
            rel_path: path.into(),
            fs_kind: "regular".into(),
            size: 4,
            content_digest: digest_of(b"same"),
        })
        .collect();
    let report = store
        .hydrate_batches(
            &view(),
            &manifest,
            4,
            4,
            |batch| {
                Box::pin(async move {
                    assert_eq!(batch.len(), 1, "one physical download for three paths");
                    Ok(HashMap::from([(
                        digest_of(b"same"),
                        Arc::new(b"same".to_vec()),
                    )]))
                })
            },
            |_| Box::pin(async { panic!("all fixture files are small") }),
        )
        .await
        .unwrap();
    assert_eq!(report.total_files, 3);
    assert_eq!(report.bytes_total, 12);
    assert_eq!(report.fetched, 1);
    assert_eq!(store.manifest().unwrap(), manifest);
    assert_eq!(store.read_journal().unwrap().len(), 3);
    let pin: PinRecord =
        serde_json::from_slice(&fs::read(temp.path().join(PIN_FILE)).unwrap()).unwrap();
    assert_eq!(
        pin.blobs,
        vec![BlobDependency {
            digest: digest_of(b"same"),
            size: 4
        }]
    );
    assert!(store.is_pinned().unwrap());
}

#[tokio::test]
async fn batch_alias_resume_rejects_conflicting_sizes_and_propagates_real_cas_io_errors() {
    let temp = tempfile::tempdir().unwrap();
    let mut store = DurableStore::open(temp.path()).unwrap();
    let meters = store.enable_verification_meters();
    let mut manifest: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|path| SnapshotFile {
            rel_path: path.into(),
            fs_kind: "regular".into(),
            size: 4,
            content_digest: digest_of(b"same"),
        })
        .collect();
    manifest[1].size = 5;
    let error = store
        .hydrate_batches(
            &view(),
            &manifest,
            2,
            2,
            |_| Box::pin(async { panic!("invalid aliases must never fetch") }),
            |_| Box::pin(async { panic!("invalid aliases must never fetch") }),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::IntegrityError);
    assert_eq!(meters.snapshot().calls, 0);
    assert!(!temp.path().join(COMPLETE_MARKER).exists());

    manifest[1].size = 4;
    // ENOTDIR is a real I/O error, distinct from an absent CAS blob. It
    // cannot become a missing hint or be cached for a second alias.
    fs::remove_dir(store.content_dir()).unwrap();
    fs::write(store.content_dir(), b"blocked directory").unwrap();
    let error = store
        .hydrate_batches(
            &view(),
            &manifest,
            2,
            2,
            |_| Box::pin(async { panic!("a CAS I/O error must not fetch") }),
            |_| Box::pin(async { panic!("a CAS I/O error must not fetch") }),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::Internal);
    assert_eq!(hydration_error_label(&error), Some("cas_resume_audit"));
    assert_eq!(meters.snapshot_for(CasVerificationReason::Resume).calls, 1);
    assert_eq!(meters.snapshot_for(CasVerificationReason::Resume).errors, 1);
    assert!(!temp.path().join(COMPLETE_MARKER).exists());
}

#[tokio::test]
async fn full_view_identity_conflicts_leave_the_original_commit_untouched() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    hydrate(&store).await;
    let original = fs::read(temp.path().join(COMPLETE_MARKER)).unwrap();
    for field in ["snapshot", "namespace", "scope"] {
        let mut different = view();
        match field {
            "snapshot" => different.snapshot_id = "different".into(),
            "namespace" => different.namespace_view_id = "different".into(),
            "scope" => different.scope = "/different".into(),
            _ => unreachable!(),
        }
        let error = store
            .hydrate_with(&different, &files(), |_| async {
                panic!("conflict must be detected before fetch")
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::DurableViewConflict);
        assert_eq!(
            fs::read(temp.path().join(COMPLETE_MARKER)).unwrap(),
            original
        );
        assert!(store.is_complete().unwrap());
    }
    store.pin(&view()).unwrap();
    assert_eq!(
        fs::read(temp.path().join(COMPLETE_MARKER)).unwrap(),
        original,
        "idempotent pin must preserve the commit binding"
    );
    let error = store
        .hydrate_with(&view(), &files()[..1], |_| async {
            panic!("a partial fixed-view closure must not fetch")
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::DurableViewConflict);
    assert_eq!(
        fs::read(temp.path().join(COMPLETE_MARKER)).unwrap(),
        original
    );
    assert!(store.is_complete().unwrap());
}

#[tokio::test]
async fn malformed_closures_are_rejected_before_any_fetch() {
    for fault in [
        "duplicate",
        "ancestor",
        "size-conflict",
        "traversal",
        "directory",
        "absolute",
        "empty-component",
        "backslash",
        "digest-prefix",
        "digest-upper",
        "size-overflow",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let mut manifest = files();
        match fault {
            "duplicate" => manifest.push(manifest[0].clone()),
            "ancestor" => manifest[1].rel_path = "a.txt/child".into(),
            "size-conflict" => {
                manifest[1].content_digest = manifest[0].content_digest.clone();
                manifest[1].size = 3;
            }
            "traversal" => manifest[0].rel_path = "../outside".into(),
            "directory" => manifest[0].fs_kind = "dir".into(),
            "absolute" => manifest[0].rel_path = "/a".into(),
            "empty-component" => manifest[0].rel_path = "a//b".into(),
            "backslash" => manifest[0].rel_path = "a\\b".into(),
            "digest-prefix" => manifest[0].content_digest = manifest[0].content_digest[7..].into(),
            "digest-upper" => {
                manifest[0].content_digest = manifest[0].content_digest.to_ascii_uppercase()
            }
            "size-overflow" => {
                manifest[0].size = u64::MAX;
                manifest[1].size = 2;
            }
            _ => unreachable!(),
        }
        let error = store
            .hydrate_with(&view(), &manifest, |_| async {
                panic!("invalid closure must not fetch")
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::IntegrityError, "{fault}");
        assert!(!store.is_complete().unwrap());
    }
}

#[tokio::test]
async fn resume_removes_a_torn_journal_tail_before_appending_new_records() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    hydrate(&store).await;
    let mut journal = OpenOptions::new()
        .append(true)
        .open(temp.path().join(JOURNAL_FILE))
        .unwrap();
    journal.write_all(br#"{"rel_path":"torn"#).unwrap();
    drop(journal);
    hydrate(&store).await;
    // A second resume must parse the new complete records too, rather than
    // encountering the former tail concatenated to a new JSON record.
    let report = hydrate(&DurableStore::open(temp.path()).unwrap()).await;
    assert_eq!(report.fetched, 0);
    assert_eq!(report.resumed, 2);
}

#[tokio::test]
async fn repeated_hydration_compacts_journal_to_one_logical_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    hydrate(&store).await;
    let first = fs::read(temp.path().join(JOURNAL_FILE)).unwrap();
    assert_eq!(
        first.iter().filter(|byte| **byte == b'\n').count(),
        files().len()
    );
    for _ in 0..4 {
        let report = hydrate(&DurableStore::open(temp.path()).unwrap()).await;
        assert_eq!(report.fetched, 0);
        assert_eq!(fs::read(temp.path().join(JOURNAL_FILE)).unwrap(), first);
        assert!(store.is_complete().unwrap());
    }
}

#[tokio::test]
async fn journal_compaction_failure_preserves_old_hints_and_revokes_complete() {
    for phase in ["journal-compact-write", "journal-compact-file-sync"] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        hydrate(&store).await;
        let first = fs::read(temp.path().join(JOURNAL_FILE)).unwrap();
        let fault = FaultGuard::install(temp.path(), phase, false);
        let error = store
            .hydrate_with(&view(), &files(), |_| async {
                panic!("warm dependencies must not fetch")
            })
            .await
            .unwrap_err();
        drop(fault);
        assert_eq!(error.code, SnapshotErrorCode::Internal);
        let hints = fs::read(temp.path().join(JOURNAL_FILE)).unwrap();
        assert!(hints.starts_with(&first));
        assert_eq!(store.read_journal().unwrap().len(), files().len());
        assert!(!store.is_complete().unwrap());
        assert!(!fs::read_dir(temp.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".journal.log.tmp.")
        }));
        hydrate(&store).await;
        assert_eq!(fs::read(temp.path().join(JOURNAL_FILE)).unwrap(), first);
    }
}

#[tokio::test]
async fn blob_sync_and_directory_sync_errors_cannot_publish_a_batch_complete() {
    for phase in ["object-file-sync", "directory-sync"] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let fault = FaultGuard::install(store.content_dir(), phase, false);
        let error = store
            .hydrate_batches(
                &view(),
                &files(),
                4,
                4,
                |batch| {
                    Box::pin(async move {
                        Ok(batch
                            .into_iter()
                            .map(|file| (file.content_digest.clone(), Arc::new(body(&file))))
                            .collect())
                    })
                },
                |_| Box::pin(async { panic!("no large files") }),
            )
            .await
            .unwrap_err();
        drop(fault);
        assert_eq!(error.code, SnapshotErrorCode::Internal, "{phase}");
        assert!(!store.is_complete().unwrap());
        assert!(!temp.path().join(COMPLETE_MARKER).exists());
    }
}

#[tokio::test]
async fn final_marker_directory_sync_failure_revokes_the_visible_record() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let fault = FaultGuard::install(temp.path(), "directory-sync", true);
    let error = store
        .hydrate_with(&view(), &files(), |file| {
            let bytes = body(file);
            async move { Ok(bytes) }
        })
        .await
        .unwrap_err();
    drop(fault);
    assert_eq!(error.code, SnapshotErrorCode::Internal);
    assert!(!temp.path().join(COMPLETE_MARKER).exists());
    assert!(!DurableStore::open(temp.path())
        .unwrap()
        .is_complete()
        .unwrap());
}

#[tokio::test]
async fn reused_content_is_synced_before_a_new_completion_commit() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    hydrate(&store).await;
    let blob = store.blob_path(&files()[0].content_digest).unwrap();
    let fault = FaultGuard::install(&blob, "file-sync", false);
    let error = store
        .hydrate_with(&view(), &files(), |_| async {
            panic!("verified cache hits must not fetch")
        })
        .await
        .unwrap_err();
    drop(fault);
    assert_eq!(error.code, SnapshotErrorCode::Internal);
    assert!(!store.is_complete().unwrap());
    assert_eq!(
        store.verify_all(&files()).unwrap(),
        2,
        "failed commit keeps shared content"
    );
    let resumed = hydrate(&store).await;
    assert_eq!(resumed.fetched, 0);
    assert!(store.is_complete().unwrap());
}

#[tokio::test]
async fn failure_after_pin_publication_cannot_leave_complete() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let fault = FaultGuard::install(temp.path(), "pin-durable", false);
    let error = store
        .hydrate_with(&view(), &files(), |file| {
            let bytes = body(file);
            async move { Ok(bytes) }
        })
        .await
        .unwrap_err();
    drop(fault);
    assert_eq!(error.code, SnapshotErrorCode::Internal);
    assert!(temp.path().join(PIN_FILE).exists());
    assert!(
        DurableStore::committed_snapshot_at(temp.path(), store.content_dir())
            .unwrap()
            .is_none(),
        "prepare pin must not be discoverable for incremental reuse"
    );
    assert!(!store.is_complete().unwrap());
    assert!(!store.is_pinned().unwrap());
    assert!(!DurableStore::open(temp.path())
        .unwrap()
        .is_complete()
        .unwrap());
}

#[cfg(unix)]
struct Worker(Child);

#[cfg(unix)]
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(unix)]
fn worker(root: &Path, env_key: &str, phase: &str) -> Worker {
    Worker(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "snapshot::durable::durability_tests::process_crash_worker",
                "--nocapture",
            ])
            .env("SCORPIO_DURABLE_WORKER_ROOT", root)
            .env(env_key, phase)
            // Child libtest summaries must not pollute the parent gate's target
            // counts. Panics still go to stderr with their phase in parent asserts.
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}

#[cfg(unix)]
#[test]
fn process_crash_worker() {
    let Some(root) = std::env::var_os("SCORPIO_DURABLE_WORKER_ROOT") else {
        return;
    };
    let store = DurableStore::open(PathBuf::from(root)).unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(hydrate(&store));
}

#[cfg(unix)]
#[test]
fn killed_process_recovers_only_committed_dependencies_at_each_publication_boundary() {
    use std::os::unix::process::ExitStatusExt;
    for phase in [
        "marker-revoked",
        "journal-buffered",
        "journal-written",
        "journal-chunk-durable",
        "content-durable",
        "journal-compact-write",
        "journal-compact-file-sync",
        "journal-compacted",
        "manifest-durable",
        "view-durable",
        "pin-durable",
        "complete-renamed",
        "complete-durable",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut child = worker(temp.path(), "SCORPIO_DURABLE_CRASH_PHASE", phase);
        let status = child.0.wait().unwrap();
        assert_eq!(
            status.signal(),
            Some(libc::SIGKILL),
            "fault point was not reached: {phase}: {status}"
        );
        let store = DurableStore::open(temp.path()).unwrap();
        let committed = matches!(phase, "complete-renamed" | "complete-durable");
        assert_eq!(store.is_complete().unwrap(), committed, "{phase}");
        assert_eq!(store.is_pinned().unwrap(), committed, "{phase}");
        assert_eq!(
            DurableStore::committed_snapshot_at(temp.path(), store.content_dir())
                .unwrap()
                .is_some(),
            committed,
            "cache pin discovery: {phase}"
        );
        if committed {
            assert_eq!(store.manifest().unwrap(), files());
            assert_eq!(store.verify_all(&files()).unwrap(), 2);
        }
        // Dead writers release the OS lock. Resume may reuse verified CAS
        // objects, but must complete the metadata/pin/commit protocol again.
        let report = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(hydrate(&store));
        assert!(report.complete);
        assert!(store.is_complete().unwrap());
        assert!(store.is_pinned().unwrap());
    }
}

#[cfg(unix)]
#[test]
fn lost_buffered_journal_hints_still_reverify_blob_bytes_after_process_crash() {
    use std::os::unix::process::ExitStatusExt;
    let temp = tempfile::tempdir().unwrap();
    let mut child = worker(
        temp.path(),
        "SCORPIO_DURABLE_CRASH_PHASE",
        "journal-buffered",
    );
    assert_eq!(child.0.wait().unwrap().signal(), Some(libc::SIGKILL));
    let store = DurableStore::open(temp.path()).unwrap();
    assert!(!temp.path().join(JOURNAL_FILE).exists());
    assert!(!store.is_complete().unwrap());
    let first = &files()[0];
    let blob = store.blob_path(&first.content_digest).unwrap();
    assert_eq!(fs::read(&blob).unwrap(), body(first));
    // A durable blob with a lost buffered hint is still verified, never
    // credited merely because it survived the killed writer.
    fs::write(blob, b"x").unwrap();
    let report = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(hydrate(&store));
    assert_eq!(report.fetched, 2);
    assert_eq!(report.repaired, 1);
    assert_eq!(report.resumed, 0);
    assert!(store.is_complete().unwrap());
    assert_eq!(store.read_journal().unwrap().len(), 2);
}

#[cfg(unix)]
#[test]
fn another_process_cannot_validate_or_revoke_a_record_during_publication() {
    let temp = tempfile::tempdir().unwrap();
    let mut child = worker(
        temp.path(),
        "SCORPIO_DURABLE_PAUSE_PHASE",
        "complete-renamed",
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while !temp.path().join("worker-ready").exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "worker exited before commit boundary"
        );
        assert!(
            Instant::now() < deadline,
            "worker did not reach commit boundary"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let store = DurableStore::open(temp.path()).unwrap();
    assert!(temp.path().join(COMPLETE_MARKER).exists());
    assert!(
        !store.is_complete().unwrap(),
        "active transaction cannot be accepted"
    );
    assert_eq!(
        store.manifest().unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    assert!(
        temp.path().join(COMPLETE_MARKER).exists(),
        "the reader must not invalidate the active writer's marker"
    );
    fs::write(temp.path().join("worker-release"), b"release").unwrap();
    assert!(child.0.wait().unwrap().success());
    assert!(store.is_complete().unwrap());
    assert!(store.is_pinned().unwrap());
}
