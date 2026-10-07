//! Real file/directory syncs and failure recovery for small OBJECT batches.

use std::process::{Command, Stdio};

use futures::FutureExt;

use super::{
    durability_tests::{FaultGuard, SyncCounter},
    *,
};

type FixtureContent = Arc<HashMap<String, Arc<Vec<u8>>>>;

fn fixture() -> (ViewMeta, Vec<SnapshotFile>, FixtureContent) {
    let view = ViewMeta {
        snapshot_id: "sha256:cas-batch-snapshot".into(),
        namespace_view_id: "sha256:cas-batch-namespace".into(),
        scope: "/project".into(),
        lease_id: "cas-batch-lease".into(),
    };
    let mut raw = HashMap::new();
    let files = (0..128)
        .map(|index| {
            let bytes = vec![index as u8; 8192];
            let digest = digest_of(&bytes);
            raw.insert(digest.clone(), Arc::new(bytes));
            SnapshotFile {
                rel_path: format!("file-{index:04}"),
                fs_kind: "regular".into(),
                content_digest: digest,
                size: 8192,
            }
        })
        .collect();
    (view, files, Arc::new(raw))
}

async fn hydrate(
    store: &DurableStore,
    view: &ViewMeta,
    files: &[SnapshotFile],
    raw: FixtureContent,
) -> Result<HydrateReport, SnapshotError> {
    store
        .hydrate_batches_with_body(
            view,
            files,
            2,
            2,
            move |batch| {
                let raw = raw.clone();
                async move {
                    Ok(batch
                        .into_iter()
                        .map(|file| {
                            let bytes = raw[&file.content_digest].clone();
                            (file.content_digest, bytes)
                        })
                        .collect::<HashMap<_, _>>())
                }
                .boxed()
            },
            |_| {
                std::future::ready(Err::<Arc<Vec<u8>>, _>(SnapshotError::new(
                    SnapshotErrorCode::ScopeInvalid,
                    "small fixture cannot fetch large content",
                )))
                .boxed()
            },
        )
        .await
}

#[tokio::test]
async fn one_batch_syncs_every_object_then_one_directory_before_journal() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let (view, files, raw) = fixture();
    let counter = SyncCounter::install(temp.path());
    let report = hydrate(&store, &view, &files, raw.clone()).await.unwrap();
    assert_eq!(report.fetched, 128);
    assert_eq!(counter.synced_cas_objects().len(), 128);
    assert_eq!(
        counter.synced_files().len(),
        128,
        "full final dependency audit remains"
    );
    assert_eq!(
        counter
            .synced_directories()
            .iter()
            .filter(|path| *path == store.content_dir())
            .count(),
        2,
        "one batch directory sync plus the unchanged final dependency directory sync"
    );
    let events = counter.cas_batch_events();
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == "batch-directory-sync")
            .count(),
        1
    );
    let batch = events
        .iter()
        .position(|event| *event == "batch-directory-sync")
        .unwrap();
    let journal = events
        .iter()
        .position(|event| *event == "journal-sync")
        .unwrap();
    assert_eq!(batch, 128);
    assert!(journal > batch);
    for file in &files {
        assert_eq!(
            fs::read(store.blob_path(&file.content_digest).unwrap()).unwrap(),
            **raw.get(&file.content_digest).unwrap()
        );
    }
    assert!(store.is_complete().unwrap());
    drop(counter);
    let resumed = hydrate(&store, &view, &files, raw).await.unwrap();
    assert_eq!(resumed.fetched, 0);
    assert_eq!(resumed.resumed, 128);
    assert!(store.is_complete().unwrap());
}

#[tokio::test]
async fn batch_io_errors_never_publish_complete_and_reopen_repairs_corruption() {
    for (phase, content, after_rename) in [
        ("object-file-sync", true, false),
        ("object-rename", true, false),
        ("object-batch-directory-sync", true, false),
        ("directory-sync", true, false),
        ("journal-file-sync", false, false),
        ("directory-sync", false, true),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let (view, files, raw) = fixture();
        let target = if content {
            store.content_dir()
        } else {
            store.root()
        };
        let fault = FaultGuard::install(target, phase, after_rename);
        let error = hydrate(&store, &view, &files, raw.clone())
            .await
            .unwrap_err();
        drop(fault);
        assert_eq!(error.code, SnapshotErrorCode::Internal, "{phase}");
        assert!(!store.is_complete().unwrap(), "{phase}");
        assert!(!store.root().join(COMPLETE_MARKER).exists(), "{phase}");
        assert!(
            !fs::read_dir(store.content_dir()).unwrap().any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".tmp."))
        );
        let damaged = files
            .iter()
            .find(|file| store.blob_path(&file.content_digest).unwrap().exists());
        if let Some(file) = damaged {
            fs::write(store.blob_path(&file.content_digest).unwrap(), b"damaged").unwrap();
        }
        let reopened = DurableStore::open(temp.path()).unwrap();
        let report = hydrate(&reopened, &view, &files, raw.clone())
            .await
            .unwrap();
        if damaged.is_some() {
            assert_eq!(report.repaired, 1, "{phase}");
        }
        assert_eq!(reopened.verify_all(&files).unwrap(), 128);
        assert!(reopened.is_complete().unwrap(), "{phase}");
    }
}

#[tokio::test]
async fn invalid_later_object_leaves_no_batch_journal_or_complete() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let (view, mut files, raw) = fixture();
    files.sort_by(|left, right| left.content_digest.cmp(&right.content_digest));
    let mut damaged = raw.as_ref().clone();
    damaged.insert(
        files.last().unwrap().content_digest.clone(),
        Arc::new(b"corrupt".to_vec()),
    );
    let error = hydrate(&store, &view, &files, Arc::new(damaged))
        .await
        .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::DigestMismatch);
    assert!(store.read_journal().unwrap().is_empty());
    assert!(!store.is_complete().unwrap());
    let report = hydrate(&store, &view, &files, raw).await.unwrap();
    assert_eq!(report.fetched, 1);
    assert_eq!(report.resumed, 127);
    assert!(store.is_complete().unwrap());
}

#[test]
fn cas_batch_crash_worker() {
    let Some(root) = std::env::var_os("SCORPIO_CAS_BATCH_WORKER_ROOT") else {
        return;
    };
    let store = DurableStore::open(PathBuf::from(root)).unwrap();
    let (view, files, raw) = fixture();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(hydrate(&store, &view, &files, raw))
        .unwrap();
}

#[test]
fn killed_batch_before_journal_reopens_without_accepting_partial_commit() {
    use std::os::unix::process::ExitStatusExt;

    for phase in [
        "object-file-sync",
        "object-rename",
        "object-batch-directory-sync",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "snapshot::durable::cas_batch_durability_tests::cas_batch_crash_worker",
                "--nocapture",
            ])
            .env("SCORPIO_CAS_BATCH_WORKER_ROOT", temp.path())
            .env("SCORPIO_DURABLE_WORKER_ROOT", store.content_dir())
            .env("SCORPIO_DURABLE_CRASH_PHASE", phase)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let status = child.wait().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL), "{phase}: {status}");
        let reopened = DurableStore::open(temp.path()).unwrap();
        assert!(reopened.read_journal().unwrap().is_empty());
        assert!(!reopened.is_complete().unwrap());
        let (view, files, raw) = fixture();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(hydrate(&reopened, &view, &files, raw))
            .unwrap();
        assert_eq!(reopened.verify_all(&files).unwrap(), 128);
        assert!(reopened.is_complete().unwrap(), "{phase}");
    }
}
