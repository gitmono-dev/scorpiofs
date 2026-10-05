//! Root-derived full snapshot durability; no remote/offline authority is inferred.

use mst2_codec::{
    descriptor::ServingDescriptor,
    metapage::{page_id, Entry, EntryKind, Page},
};

use super::{durability_tests::FaultGuard, *};

fn add_directory_pages(entries: &[Entry], pages: &mut BTreeMap<String, Vec<u8>>) -> [u8; 32] {
    let root = Page::build(entries).unwrap();
    let id = page_id(&root);
    let mut frontier = vec![Vec::new()];
    while let Some(route) = frontier.pop() {
        let witness = Page::pages_along_route(entries, &route).unwrap();
        let last = witness.last().unwrap();
        if let Page::Branch { children, .. } = Page::decode(last).unwrap().0 {
            for child in children {
                let mut child_route = route.clone();
                child_route.push(child.label);
                frontier.push(child_route);
            }
        }
        for bytes in witness {
            pages.insert(format!("sha256:{}", hex::encode(page_id(&bytes))), bytes);
        }
    }
    id
}

fn fixture(files: usize) -> (ValidatedSnapshotClosure, HashMap<String, Vec<u8>>, ViewMeta) {
    let mut pages = BTreeMap::new();
    let empty = add_directory_pages(&[], &mut pages);
    let mut entries = vec![Entry::dir(b"nested-empty", empty)];
    let mut raw = HashMap::new();
    for index in 0..files {
        let bytes = format!("fixture content {index}").into_bytes();
        let digest = digest_of(&bytes);
        let id = crate::snapshot::frames::parse_digest(&digest).unwrap();
        let name = format!("f{index:04}");
        entries.push(Entry::file(
            EntryKind::Regular,
            name.as_bytes(),
            bytes.len() as u64,
            id,
        ));
        raw.insert(digest, bytes);
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let shared = add_directory_pages(&entries, &mut pages);
    let root = add_directory_pages(
        &[
            Entry::dir(b"alias-a", shared),
            Entry::dir(b"alias-b", shared),
            Entry::dir(b"empty", empty),
        ],
        &mut pages,
    );
    let descriptor = ServingDescriptor {
        instance_uuid: *uuid::Uuid::from_u128(1).as_bytes(),
        namespace_view_id: [3; 32],
        scope: "/project".into(),
        metadata_root: root,
    }
    .encode()
    .unwrap();
    let closure = ValidatedSnapshotClosure::from_canonical_pages(&descriptor, pages).unwrap();
    let view = ViewMeta {
        snapshot_id: closure.snapshot_id().into(),
        namespace_view_id: closure.descriptor().namespace_view_id.clone(),
        scope: closure.descriptor().scope.clone(),
        lease_id: "local-integrity-test".into(),
    };
    (closure, raw, view)
}

async fn hydrate_full(store: &DurableStore, files: usize) -> HydrateReport {
    let (closure, raw, view) = fixture(files);
    store
        .hydrate_snapshot_with(&view, &closure, |file| {
            std::future::ready(Ok(raw[&file.content_digest].clone()))
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn full_snapshot_reopen_preserves_empty_aliases_and_all_radix_pages() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let (expected, _, _) = fixture(130);
    assert!(
        expected.pages().len() > 3,
        "fixture must exercise radix children"
    );
    let report = hydrate_full(&store, 130).await;
    assert_eq!(report.completion_kind, CompletionKind::FullSnapshot);
    assert_eq!(
        report.total_files, 260,
        "logical alias paths are both retained"
    );
    drop(store);
    let store = DurableStore::open(temp.path()).unwrap();
    assert!(store.is_snapshot_complete().unwrap());
    let reopened = store.snapshot_manifest().unwrap();
    assert_eq!(reopened.descriptor_bytes(), expected.descriptor_bytes());
    assert_eq!(reopened.pages(), expected.pages());
    assert_eq!(reopened.files(), expected.files());
    assert_eq!(reopened.directories(), expected.directories());
    assert!(reopened
        .directories()
        .iter()
        .any(|dir| dir.rel_path == "alias-a/nested-empty"));
    assert!(reopened
        .directories()
        .iter()
        .any(|dir| dir.rel_path == "alias-b/nested-empty"));
    crate::snapshot::fuse::Mst2Fuse::from_snapshot_store(std::sync::Arc::new(store)).unwrap();
}

#[tokio::test]
async fn file_only_completion_cannot_be_reopened_as_a_full_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let (closure, raw, view) = fixture(2);
    let report = store
        .hydrate_with(&view, closure.files(), |file| {
            std::future::ready(Ok(raw[&file.content_digest].clone()))
        })
        .await
        .unwrap();
    assert_eq!(report.completion_kind, CompletionKind::FileClosure);
    assert_eq!(
        store.completion_kind().unwrap(),
        Some(CompletionKind::FileClosure)
    );
    assert!(!store.is_snapshot_complete().unwrap());
    assert_eq!(
        store.snapshot_manifest().unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    assert!(
        store.is_complete().unwrap(),
        "file-only compatibility is explicit"
    );
    hydrate_full(&store, 2).await;
    assert!(store.is_snapshot_complete().unwrap());
    assert_eq!(
        store
            .hydrate_with(&view, closure.files(), |file| {
                std::future::ready(Ok(raw[&file.content_digest].clone()))
            })
            .await
            .unwrap_err()
            .code,
        SnapshotErrorCode::DurableViewConflict
    );
    assert!(
        store.is_snapshot_complete().unwrap(),
        "file-only cannot revoke a stronger commitment"
    );
}

#[tokio::test]
async fn full_snapshot_accepts_linux_backslashes_and_rejects_nul_symlink_targets() {
    for (kind, raw, valid) in [
        (EntryKind::Regular, b"linux name".as_slice(), true),
        (EntryKind::Symlink, b"relative\\target".as_slice(), true),
        (EntryKind::Symlink, b"bad\0target".as_slice(), false),
    ] {
        let mut pages = BTreeMap::new();
        let root = add_directory_pages(
            &[Entry::file(
                kind,
                b"file\\name",
                raw.len() as u64,
                crate::snapshot::frames::parse_digest(&digest_of(raw)).unwrap(),
            )],
            &mut pages,
        );
        let descriptor = ServingDescriptor {
            instance_uuid: *uuid::Uuid::from_u128(1).as_bytes(),
            namespace_view_id: [3; 32],
            scope: "/project\\name".into(),
            metadata_root: root,
        }
        .encode()
        .unwrap();
        let closure = ValidatedSnapshotClosure::from_canonical_pages(&descriptor, pages).unwrap();
        let view = ViewMeta {
            snapshot_id: closure.snapshot_id().into(),
            namespace_view_id: closure.descriptor().namespace_view_id.clone(),
            scope: closure.descriptor().scope.clone(),
            lease_id: "local-test".into(),
        };
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let result = store
            .hydrate_snapshot_with(&view, &closure, |_| std::future::ready(Ok(raw.to_vec())))
            .await;
        if valid {
            assert_eq!(
                result.unwrap().completion_kind,
                CompletionKind::FullSnapshot
            );
            let reopened = DurableStore::open(temp.path()).unwrap();
            assert!(reopened.is_snapshot_complete().unwrap());
            assert_eq!(
                reopened.snapshot_manifest().unwrap().files()[0].rel_path,
                "file\\name"
            );
        } else {
            assert_eq!(result.unwrap_err().code, SnapshotErrorCode::IntegrityError);
            assert!(!store.is_snapshot_complete().unwrap());
            assert!(!temp.path().join(COMPLETE_MARKER).exists());
        }
    }
}

#[tokio::test]
async fn full_snapshot_missing_or_changed_metadata_revokes_completion() {
    for damage in [
        "descriptor",
        "index",
        "pin",
        "missing-empty-page",
        "changed-radix-page",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        hydrate_full(&store, 130).await;
        match damage {
            "descriptor" => {
                fs::write(temp.path().join(DESCRIPTOR_FILE), b"broken descriptor").unwrap()
            }
            "index" => fs::remove_file(temp.path().join(METADATA_INDEX_FILE)).unwrap(),
            "pin" => fs::remove_file(temp.path().join(PIN_FILE)).unwrap(),
            "missing-empty-page" => {
                let empty = Page::build(&[]).unwrap();
                let id = format!("sha256:{}", hex::encode(page_id(&empty)));
                fs::remove_file(store.metadata_page_path(&id).unwrap()).unwrap();
            }
            "changed-radix-page" => {
                let (closure, _, _) = fixture(130);
                let (id, _) = closure
                    .pages()
                    .iter()
                    .find(|(_, bytes)| {
                        matches!(Page::decode(bytes).unwrap().0, Page::Branch { .. })
                    })
                    .unwrap();
                fs::write(
                    store.metadata_page_path(id).unwrap(),
                    Page::build(&[]).unwrap(),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        drop(store);
        let reopened = DurableStore::open(temp.path()).unwrap();
        assert!(!reopened.is_snapshot_complete().unwrap(), "{damage}");
        assert!(!temp.path().join(COMPLETE_MARKER).exists(), "{damage}");
        assert!(temp.path().join(REPAIR_FILE).exists(), "{damage}");
        assert!(reopened.snapshot_manifest().is_err(), "{damage}");
    }
}

#[tokio::test]
async fn independent_views_with_identical_page_ids_keep_private_metadata_dependencies() {
    let temp = tempfile::tempdir().unwrap();
    let a =
        DurableStore::open_with_content(temp.path().join("a"), temp.path().join("blobs")).unwrap();
    let b =
        DurableStore::open_with_content(temp.path().join("b"), temp.path().join("blobs")).unwrap();
    hydrate_full(&a, 2).await;
    hydrate_full(&b, 2).await;
    let (closure, _, _) = fixture(2);
    let page_id = closure.pages().keys().next().unwrap();
    assert_ne!(
        a.metadata_page_path(page_id).unwrap(),
        b.metadata_page_path(page_id).unwrap()
    );
    fs::remove_file(a.metadata_page_path(page_id).unwrap()).unwrap();
    assert!(!a.is_snapshot_complete().unwrap());
    assert!(b.is_snapshot_complete().unwrap());
    assert_eq!(b.snapshot_manifest().unwrap().pages(), closure.pages());
    hydrate_full(&a, 2).await;
    assert!(a.is_snapshot_complete().unwrap());
    assert!(b.is_snapshot_complete().unwrap());
}

#[tokio::test]
async fn rewritten_index_checksums_cannot_hide_a_directory_or_a_file() {
    for damage in ["omit-empty-directory", "omit-file"] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        hydrate_full(&store, 2).await;
        let mut marker: SnapshotCompleteMarker = decode_commit(
            &fs::read(temp.path().join(COMPLETE_MARKER)).unwrap(),
            COMPLETE_MARKER,
        )
        .unwrap();
        let mut pin: SnapshotPinRecord =
            decode_commit(&fs::read(temp.path().join(PIN_FILE)).unwrap(), PIN_FILE).unwrap();
        if damage == "omit-empty-directory" {
            let mut index: MetadataIndex = decode_commit(
                &fs::read(temp.path().join(METADATA_INDEX_FILE)).unwrap(),
                METADATA_INDEX_FILE,
            )
            .unwrap();
            index.directories.retain(|dir| dir.rel_path != "empty");
            let bytes = encode_record(&index).unwrap();
            marker.directories = index.directories.len() as u64;
            marker.metadata_index_digest = digest_of(&bytes);
            pin.metadata_index_digest = marker.metadata_index_digest.clone();
            fs::write(temp.path().join(METADATA_INDEX_FILE), bytes).unwrap();
        } else {
            let mut files: Vec<SnapshotFile> = decode_commit(
                &fs::read(temp.path().join(MANIFEST_FILE)).unwrap(),
                MANIFEST_FILE,
            )
            .unwrap();
            files.pop();
            let bytes = encode_record(&files).unwrap();
            let (blobs, total) = validate_manifest(&files).unwrap();
            pin.blobs = blobs;
            marker.files = files.len() as u64;
            marker.bytes = total;
            marker.manifest_digest = digest_of(&bytes);
            pin.manifest_digest = marker.manifest_digest.clone();
            fs::write(temp.path().join(MANIFEST_FILE), bytes).unwrap();
        }
        let pin_bytes = encode_record(&pin).unwrap();
        marker.pin_digest = digest_of(&pin_bytes);
        fs::write(temp.path().join(PIN_FILE), pin_bytes).unwrap();
        fs::write(
            temp.path().join(COMPLETE_MARKER),
            encode_record(&marker).unwrap(),
        )
        .unwrap();
        assert!(
            !store.is_snapshot_complete().unwrap(),
            "root-derived closure: {damage}"
        );
        assert!(temp.path().join(REPAIR_FILE).exists());
    }
}

#[tokio::test]
async fn full_snapshot_metadata_sync_failures_never_publish_complete() {
    for phase in [
        "metadata-pages-durable",
        "descriptor-durable",
        "metadata-index-durable",
        "pin-durable",
        "complete-durable",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let (closure, raw, view) = fixture(2);
        let fault = FaultGuard::install(temp.path(), phase, false);
        let error = store
            .hydrate_snapshot_with(&view, &closure, |file| {
                std::future::ready(Ok(raw[&file.content_digest].clone()))
            })
            .await
            .unwrap_err();
        drop(fault);
        assert_eq!(error.code, SnapshotErrorCode::Internal, "{phase}");
        assert!(!store.is_snapshot_complete().unwrap(), "{phase}");
        assert!(!temp.path().join(COMPLETE_MARKER).exists(), "{phase}");
    }
    for (phase, after_rename) in [
        ("object-file-sync", false),
        ("directory-sync", false),
        ("directory-sync", true),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let (closure, raw, view) = fixture(2);
        let target = if after_rename {
            temp.path().to_path_buf()
        } else {
            temp.path().join(METADATA_DIR)
        };
        let fault = FaultGuard::install(&target, phase, after_rename);
        assert!(
            store
                .hydrate_snapshot_with(&view, &closure, |file| {
                    std::future::ready(Ok(raw[&file.content_digest].clone()))
                })
                .await
                .is_err(),
            "{phase}"
        );
        drop(fault);
        assert!(!store.is_snapshot_complete().unwrap());
        assert!(!temp.path().join(COMPLETE_MARKER).exists());
    }
}

#[cfg(unix)]
#[test]
fn snapshot_process_crash_worker() {
    let Some(root) = std::env::var_os("SCORPIO_SNAPSHOT_WORKER_ROOT") else {
        return;
    };
    let store = DurableStore::open(PathBuf::from(root)).unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(hydrate_full(&store, 2));
}

#[cfg(unix)]
#[test]
fn full_snapshot_sigkill_reopens_only_committed_root_graphs() {
    use std::os::unix::process::ExitStatusExt;
    for phase in [
        "marker-revoked",
        "content-durable",
        "metadata-pages-durable",
        "descriptor-durable",
        "metadata-index-durable",
        "pin-durable",
        "complete-renamed",
        "complete-durable",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut worker = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "snapshot::durable::snapshot_durability_tests::snapshot_process_crash_worker",
                "--nocapture",
            ])
            .env("SCORPIO_SNAPSHOT_WORKER_ROOT", temp.path())
            .env("SCORPIO_DURABLE_WORKER_ROOT", temp.path())
            .env("SCORPIO_DURABLE_CRASH_PHASE", phase)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap();
        let status = worker.wait().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL), "{phase}: {status}");
        let store = DurableStore::open(temp.path()).unwrap();
        let committed = matches!(phase, "complete-renamed" | "complete-durable");
        assert_eq!(store.is_snapshot_complete().unwrap(), committed, "{phase}");
        assert_eq!(store.snapshot_manifest().is_ok(), committed, "{phase}");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(hydrate_full(&store, 2));
        assert!(store.is_snapshot_complete().unwrap());
    }
}
