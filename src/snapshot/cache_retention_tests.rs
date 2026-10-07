use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Barrier,
};

use mst2_codec::{
    descriptor::ServingDescriptor,
    metapage::{page_id, Entry, EntryKind, Page},
};

use super::*;
use crate::snapshot::{auth::AuthorizedSnapshotContext, ScopeCache, ViewMeta};

struct ReleaseActualJob(Option<std::sync::mpsc::Sender<()>>);
impl ReleaseActualJob {
    fn release(mut self) {
        let _ = self.0.take().unwrap().send(());
    }
}
impl Drop for ReleaseActualJob {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test]
async fn cancelled_real_collector_waiter_keeps_fence_and_live_use_until_actual_job_exit() {
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 81);
    store.retain_snapshot_root(&closure).unwrap();
    let live = blob(&store, b"live");
    let stale = blob(&store, b"stale");
    store.release_local_pin().unwrap();
    let use_before = fs::read(store.root().join(USE_RECORD)).unwrap();
    let mut barrier = collection_hooks::install(&scope(&store));
    let directory = scope(&store);
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let actual = tokio::task::spawn_blocking(move || {
        let result = collect_scope(&directory);
        let _ = finished_tx.send(result);
    });
    tokio::time::timeout(Duration::from_secs(10), barrier.entered())
        .await
        .unwrap();
    drop(actual);
    assert_eq!(
        store.retire_cache_use().unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    assert_eq!(fs::read(store.root().join(USE_RECORD)).unwrap(), use_before);
    assert!(live.exists() && stale.exists());
    barrier.release();
    let result = tokio::time::timeout(Duration::from_secs(10), finished_rx)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(result.deleted_entries > 0);
    assert!(
        live.exists() && !stale.exists(),
        "released but unretired use remains protected inside detached collector"
    );
    store.retire_cache_use().unwrap();
    assert!(collect_scope(&scope(&store)).unwrap().deleted_entries > 0);
    assert!(!live.exists());
}

fn limits() -> CacheLimits {
    CacheLimits {
        max_bytes: 16 * 1024 * 1024,
        proof_headroom_bytes: 4 * 1024 * 1024,
        max_entries: 10_000,
        max_owners: 32,
        max_inventory_bytes: 16 * 1024 * 1024,
        max_metadata_bytes: 4 * 1024 * 1024,
        max_root_nodes: 10_000,
        max_record_bytes: 1024 * 1024,
        max_scan_millis: 5000,
        max_delete_entries: 512,
        max_delete_bytes: 16 * 1024 * 1024,
    }
}

fn root(files: &[(&str, &[u8])]) -> (AuthorizedSnapshotContext, ValidatedSnapshotClosure) {
    let entries: Vec<_> = files
        .iter()
        .map(|(name, bytes)| {
            Entry::file(
                EntryKind::Regular,
                name.as_bytes(),
                bytes.len() as u64,
                parse_digest(&durable::digest_of(bytes)).unwrap(),
            )
        })
        .collect();
    let page = Page::build(&entries).unwrap();
    let descriptor = ServingDescriptor {
        instance_uuid: *uuid::Uuid::from_u128(1).as_bytes(),
        namespace_view_id: [7; 32],
        scope: "/code".into(),
        metadata_root: page_id(&page),
    }
    .encode()
    .unwrap();
    let closure = ValidatedSnapshotClosure::from_canonical_pages(
        &descriptor,
        BTreeMap::from([(format!("sha256:{}", hex::encode(page_id(&page))), page)]),
    )
    .unwrap();
    let context = AuthorizedSnapshotContext::new(
        "http://retention.invalid",
        "same-actor",
        "/code",
        closure.descriptor().clone(),
        "1",
        "1",
    )
    .unwrap();
    (context, closure)
}

fn managed(
    temp: &tempfile::TempDir,
    context: &AuthorizedSnapshotContext,
    id: u128,
) -> DurableStore {
    let scope = context.scope_cache_dir(temp.path());
    context.bind_scope_cache(&scope).unwrap();
    configure_scope(&scope, limits()).unwrap();
    DurableStore::open_workspace_context(
        temp.path(),
        &uuid::Uuid::from_u128(id).to_string(),
        context,
    )
    .unwrap()
}

fn blob(store: &DurableStore, bytes: &[u8]) -> PathBuf {
    let digest = durable::digest_of(bytes);
    let name = hex::encode(parse_digest(&digest).unwrap());
    durable::write_atomic(store.content_dir(), &name, bytes).unwrap();
    store.content_dir().join(name)
}

fn scope(store: &DurableStore) -> PathBuf {
    store.content_dir().parent().unwrap().to_path_buf()
}

#[test]
fn regular_cas_leaf_keeps_managed_ancestor_fence_and_unmanaged_io_semantics() {
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 131);
    store.retain_snapshot_root(&closure).unwrap();
    let live = blob(&store, b"live");
    let directory = scope(&store);
    assert_eq!(managed_scope(&live).unwrap(), Some(directory.clone()));
    let held = io_guard(&live).unwrap().unwrap();
    assert_eq!(
        collect_scope(&directory).unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    assert_eq!(fs::read(&live).unwrap(), b"live");
    drop(held);
    assert!(collect_scope(&directory).is_ok());

    // Even a non-directory descendant must not hide a managed ancestor.
    assert_eq!(
        managed_scope(&live.join("child")).unwrap(),
        Some(directory.clone())
    );
    let policy_before = fs::read(directory.join(POLICY)).unwrap();
    fs::write(directory.join(POLICY), b"{}").unwrap();
    assert_eq!(
        managed_scope(&live).unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    fs::write(directory.join(POLICY), policy_before).unwrap();

    let unmanaged = tempfile::tempdir().unwrap();
    let file = unmanaged.path().join("actual-file");
    fs::write(&file, b"ordinary").unwrap();
    assert!(managed_scope(&file).unwrap().is_none());
    assert!(io_guard(&file).unwrap().is_none());
    assert_eq!(fs::read(&file).unwrap(), b"ordinary");
}

fn torn_control_temp(directory: &Path, final_name: &str, bytes: &[u8]) -> PathBuf {
    let path = directory.join(format!(".{final_name}.tmp.{}", uuid::Uuid::new_v4()));
    let mut file = secure_fs::open_create_new(&path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
    drop(file);
    durable::sync_dir(directory).unwrap();
    path // The actual partial regular file left by an interrupted raw_atomic.
}

#[test]
fn torn_control_crash_recovery_keeps_authority_live_use_and_valid_writer_declaration() {
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 101);
    store.retain_snapshot_root(&closure).unwrap();
    let live = blob(&store, b"live");
    let directory = scope(&store);
    let id = uuid::Uuid::new_v4().to_string();
    let final_name = "c".repeat(64);
    let body_temp_name = format!(".{final_name}.tmp.{id}");
    let body_temp = store.content_dir().join(&body_temp_name);
    let valid_intent = directory.join(INTENTS).join(format!("{id}.json"));
    raw_atomic(
        valid_intent.parent().unwrap(),
        valid_intent.file_name().unwrap().to_str().unwrap(),
        &WriterIntent {
            revision: 1,
            operation_id: id,
            relative_directory: "blobs".into(),
            final_name,
            temporary_name: body_temp_name,
            maximum_bytes: 64,
        },
    )
    .unwrap();
    fs::write(&body_temp, b"unpublished-body").unwrap();
    let controls = [
        torn_control_temp(&directory, POLICY, b"{\"revision\":1,\"limits\":"),
        torn_control_temp(&directory, LEDGER, b"{\"revision\":1,\"bytes\":0,"),
        torn_control_temp(
            &directory.join(INTENTS),
            &format!("{}.json", uuid::Uuid::new_v4()),
            b"{\"relative_directory\":\"blobs\",\"final_name\":",
        ),
        torn_control_temp(
            &directory.join(INTENTS),
            &format!("{}.hydrate.json", uuid::Uuid::new_v4()),
            b"{\"revision\":1,\"binding\":",
        ),
    ];
    let mut charges: Charges =
        read_record(&directory.join(LEDGER), CONTROL_BYTES as usize).unwrap();
    charges.bytes = limits().max_bytes - limits().proof_headroom_bytes;
    {
        let _fence = lock(&directory, FENCE, false).unwrap();
        let _admission = lock(&directory, ADMISSION_LOCK, false).unwrap();
        raw_atomic(&directory, LEDGER, &charges).unwrap();
    }
    assert_eq!(
        WriteAdmission::begin(store.content_dir(), &"d".repeat(64), 64)
            .err()
            .unwrap()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    let policy_before = fs::read(directory.join(POLICY)).unwrap();
    let authority_before = fs::read(directory.join("authority.json")).unwrap();
    let use_before = fs::read(store.root().join(USE_RECORD)).unwrap();
    let intent_before = fs::read(&valid_intent).unwrap();
    let report = collect_scope(&directory).unwrap();
    assert_eq!(report.deleted_entries, controls.len());
    assert!(controls.iter().all(|path| !path.exists()));
    assert_eq!(fs::read(directory.join(POLICY)).unwrap(), policy_before);
    assert_eq!(
        fs::read(directory.join("authority.json")).unwrap(),
        authority_before
    );
    assert_eq!(fs::read(store.root().join(USE_RECORD)).unwrap(), use_before);
    assert_eq!(fs::read(&valid_intent).unwrap(), intent_before);
    assert_eq!(fs::read(&body_temp).unwrap(), b"unpublished-body");
    assert_eq!(fs::read(&live).unwrap(), b"live");
    let recovered: Charges = read_record(&directory.join(LEDGER), CONTROL_BYTES as usize).unwrap();
    assert!(recovered.bytes > 0 && recovered.bytes < charges.bytes);
    drop(
        WriteAdmission::begin(store.content_dir(), &"d".repeat(64), 64)
            .unwrap()
            .unwrap(),
    );
    let retry = collect_scope(&directory).unwrap();
    assert_eq!(
        retry.deleted_entries, 2,
        "only the independently valid final intent authorizes its body temp"
    );
    assert!(!valid_intent.exists() && !body_temp.exists());
    assert_eq!(fs::read(&live).unwrap(), b"live");
}

#[test]
fn torn_intent_control_never_declares_an_unpublished_body_temporary() {
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 102);
    store.retain_snapshot_root(&closure).unwrap();
    let live = blob(&store, b"live");
    let directory = scope(&store);
    let id = uuid::Uuid::new_v4().to_string();
    let body_temp = store
        .content_dir()
        .join(format!(".{}.tmp.{id}", "e".repeat(64)));
    fs::write(&body_temp, b"unpublished").unwrap();
    let control = torn_control_temp(
        &directory.join(INTENTS),
        &format!("{id}.json"),
        b"{\"revision\":1,\"relative_directory\":\"blobs\",",
    );
    let ledger_before = fs::read(directory.join(LEDGER)).unwrap();
    assert!(collect_scope(&directory).is_err());
    assert!(control.exists() && body_temp.exists() && live.exists());
    assert_eq!(fs::read(directory.join(LEDGER)).unwrap(), ledger_before);
    fs::remove_file(&body_temp).unwrap(); // An external repair removes the unknown entry.
    assert_eq!(collect_scope(&directory).unwrap().deleted_entries, 1);
    assert!(!control.exists() && live.exists());
}

#[test]
fn unknown_names_corrupt_current_controls_symlink_oversize_and_busy_block_all_recovery() {
    use std::os::unix::fs::symlink;
    for (offset, failure) in [
        "unknown-name",
        "noncanonical-uuid",
        "unknown-intent",
        "wrong-location",
        "corrupt-authority",
        "corrupt-ledger",
        "corrupt-policy",
        "symlink",
        "oversize",
        "busy",
    ]
    .iter()
    .enumerate()
    {
        let temp = tempfile::tempdir().unwrap();
        let (context, closure) = root(&[("live", b"live")]);
        let store = managed(&temp, &context, 110 + offset as u128);
        store.retain_snapshot_root(&closure).unwrap();
        let live = blob(&store, b"live");
        let stale = blob(&store, b"stale");
        let directory = scope(&store);
        let control = torn_control_temp(&directory, LEDGER, b"{");
        let mut held = None;
        match *failure {
            "unknown-name" => {
                torn_control_temp(&directory, "unknown.json", b"{");
            }
            "noncanonical-uuid" => {
                let id = uuid::Uuid::from_u128(0xabcdef).to_string().to_uppercase();
                fs::write(directory.join(format!(".{POLICY}.tmp.{id}")), b"{").unwrap();
            }
            "unknown-intent" => {
                torn_control_temp(&directory.join(INTENTS), "not-a-uuid.json", b"{");
            }
            "wrong-location" => {
                torn_control_temp(store.content_dir(), LEDGER, b"{");
            }
            "corrupt-authority" => {
                fs::write(directory.join("authority.json"), b"{}").unwrap();
            }
            "corrupt-ledger" => {
                fs::write(directory.join(LEDGER), b"{}").unwrap();
            }
            "corrupt-policy" => {
                fs::write(directory.join(POLICY), b"{}").unwrap();
            }
            "symlink" => {
                let outside = temp.path().join("outside");
                fs::write(&outside, b"outside").unwrap();
                fs::remove_file(&control).unwrap();
                symlink(&outside, &control).unwrap();
            }
            "oversize" => {
                secure_fs::open_write(&control)
                    .unwrap()
                    .set_len(CONTROL_BYTES + 1)
                    .unwrap();
            }
            "busy" => {
                let file = secure_fs::open_regular_nonblocking(&control).unwrap();
                file.try_lock().unwrap();
                held = Some(file);
            }
            _ => unreachable!(),
        }
        let policy_before = fs::read(directory.join(POLICY)).unwrap();
        let ledger_before = fs::read(directory.join(LEDGER)).unwrap();
        let use_before = fs::read(store.root().join(USE_RECORD)).unwrap();
        assert!(collect_scope(&directory).is_err(), "{failure}");
        assert!(
            control.exists() && stale.exists() && live.exists(),
            "{failure}"
        );
        assert_eq!(
            fs::read(directory.join(POLICY)).unwrap(),
            policy_before,
            "{failure}"
        );
        assert_eq!(
            fs::read(directory.join(LEDGER)).unwrap(),
            ledger_before,
            "{failure}"
        );
        assert_eq!(
            fs::read(store.root().join(USE_RECORD)).unwrap(),
            use_before,
            "{failure}"
        );
        drop(held);
    }
}

#[tokio::test]
async fn actual_control_recovery_rejects_a_replaced_temporary_lifetime_before_any_unlink() {
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 121);
    store.retain_snapshot_root(&closure).unwrap();
    let live = blob(&store, b"live");
    let directory = scope(&store);
    let control = torn_control_temp(&directory, LEDGER, b"torn");
    let policy_before = fs::read(directory.join(POLICY)).unwrap();
    let ledger_before = fs::read(directory.join(LEDGER)).unwrap();
    let use_before = fs::read(store.root().join(USE_RECORD)).unwrap();
    let mut barrier = collection_hooks::install_before_unlink(&directory);
    let collector_directory = directory.clone();
    let actual = tokio::task::spawn_blocking(move || collect_scope(&collector_directory));
    tokio::time::timeout(Duration::from_secs(10), barrier.entered())
        .await
        .unwrap();
    fs::remove_file(&control).unwrap();
    let mut replacement = secure_fs::open_create_new(&control).unwrap();
    replacement.write_all(b"torn").unwrap();
    replacement.sync_all().unwrap();
    drop(replacement);
    barrier.release();
    assert!(tokio::time::timeout(Duration::from_secs(10), actual)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert_eq!(fs::read(&control).unwrap(), b"torn");
    assert_eq!(fs::read(directory.join(POLICY)).unwrap(), policy_before);
    assert_eq!(fs::read(directory.join(LEDGER)).unwrap(), ledger_before);
    assert_eq!(fs::read(store.root().join(USE_RECORD)).unwrap(), use_before);
    assert_eq!(fs::read(&live).unwrap(), b"live");
    assert_eq!(collect_scope(&directory).unwrap().deleted_entries, 1);
    assert!(!control.exists() && live.exists());
}

#[tokio::test]
async fn same_policy_reopen_and_new_commit_owner_preserve_a_held_actual_writer() {
    let temp = tempfile::tempdir().unwrap();
    let (old_context, _) = root(&[("old", b"old")]);
    let (new_context, _) = root(&[("new", b"new")]);
    let old = managed(&temp, &old_context, 91);
    let directory = old.content_dir().to_path_buf();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release = ReleaseActualJob(Some(release_tx));
    let actual = tokio::task::spawn_blocking(move || {
        let admission = WriteAdmission::begin(&directory, &"a".repeat(64), 64)
            .unwrap()
            .unwrap();
        let mut file = secure_fs::open_create_new(&admission.temporary_path()).unwrap();
        file.write_all(b"in-progress").unwrap();
        file.sync_all().unwrap();
        let intent = admission
            .scope
            .join(INTENTS)
            .join(admission.intent_name.as_ref().unwrap());
        started_tx
            .send((admission.temporary_path(), intent))
            .unwrap();
        release_rx.recv().unwrap();
        drop(file);
        drop(admission);
    });
    let (temporary, intent) = tokio::time::timeout(Duration::from_secs(10), started_rx)
        .await
        .unwrap()
        .unwrap();
    let directory = scope(&old);
    let policy_before = fs::read(directory.join(POLICY)).unwrap();
    let charges_before = fs::read(directory.join(LEDGER)).unwrap();
    let intent_before = fs::read(&intent).unwrap();
    let use_before = fs::read(old.root().join(USE_RECORD)).unwrap();
    configure_scope(&directory, limits()).unwrap();
    assert_eq!(fs::read(directory.join(POLICY)).unwrap(), policy_before);
    assert_eq!(fs::read(directory.join(LEDGER)).unwrap(), charges_before);
    assert_eq!(fs::read(&intent).unwrap(), intent_before);
    assert_eq!(fs::read(&temporary).unwrap(), b"in-progress");
    let mut different = limits();
    different.max_bytes += 1024 * 1024;
    assert_eq!(
        configure_scope(&directory, different).unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    assert_eq!(fs::read(directory.join(LEDGER)).unwrap(), charges_before);
    let new = managed(&temp, &new_context, 92);
    assert_eq!(scope(&new), directory);
    assert_ne!(new.root(), old.root());
    assert_eq!(fs::read(directory.join(POLICY)).unwrap(), policy_before);
    assert_eq!(fs::read(old.root().join(USE_RECORD)).unwrap(), use_before);
    assert_eq!(fs::read(&intent).unwrap(), intent_before);
    assert_eq!(fs::read(&temporary).unwrap(), b"in-progress");
    release.release();
    tokio::time::timeout(Duration::from_secs(10), actual)
        .await
        .unwrap()
        .unwrap();
    assert!(!temporary.exists() && !intent.exists());
}

#[test]
fn existing_policy_reuse_keeps_strict_validation_while_a_managed_writer_is_held() {
    let temp = tempfile::tempdir().unwrap();
    let (context, _) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 93);
    let directory = scope(&store);
    let admission = WriteAdmission::begin(store.content_dir(), &"b".repeat(64), 64)
        .unwrap()
        .unwrap();
    let mut file = secure_fs::open_create_new(&admission.temporary_path()).unwrap();
    file.write_all(b"held").unwrap();
    let authority = directory.join("authority.json");
    let authority_before = fs::read(&authority).unwrap();
    let policy_before = fs::read(directory.join(POLICY)).unwrap();
    let ledger_before = fs::read(directory.join(LEDGER)).unwrap();
    let intent = directory
        .join(INTENTS)
        .join(admission.intent_name.as_ref().unwrap());
    let intent_before = fs::read(&intent).unwrap();
    fs::write(&authority, b"{}").unwrap();
    assert_eq!(
        configure_scope(&directory, limits()).unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    fs::write(&authority, &authority_before).unwrap();
    fs::write(directory.join(POLICY), b"{}").unwrap();
    assert_eq!(
        configure_scope(&directory, limits()).unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    fs::write(directory.join(POLICY), &policy_before).unwrap();
    configure_scope(&directory, limits()).unwrap();
    assert_eq!(fs::read(directory.join(LEDGER)).unwrap(), ledger_before);
    assert_eq!(fs::read(&intent).unwrap(), intent_before);
    assert_eq!(fs::read(admission.temporary_path()).unwrap(), b"held");
    drop(file);
    drop(admission);
}

#[test]
fn released_live_use_survives_and_retired_old_unique_objects_are_actually_collected() {
    let temp = tempfile::tempdir().unwrap();
    let (old_context, old_root) = root(&[("old", b"old-only"), ("shared", b"shared")]);
    let (new_context, new_root) = root(&[("new", b"new-only"), ("shared", b"shared")]);
    let old = managed(&temp, &old_context, 11);
    let new = managed(&temp, &new_context, 12);
    old.retain_snapshot_root(&old_root).unwrap();
    new.retain_snapshot_root(&new_root).unwrap();
    let old_blob = blob(&old, b"old-only");
    let new_blob = blob(&new, b"new-only");
    let shared_blob = blob(&new, b"shared");
    let cache = ScopeCache::open(scope(&new)).unwrap();
    for closure in [&old_root, &new_root] {
        for (id, bytes) in closure.pages() {
            cache.put_page(id, bytes).unwrap();
        }
    }
    let old_page = scope(&old).join("pages").join(hex::encode(
        parse_digest(old_root.pages().keys().next().unwrap()).unwrap(),
    ));
    old.release_local_pin().unwrap();
    let kept = collect_scope(&scope(&old)).unwrap();
    assert_eq!(
        kept.deleted_entries, 0,
        "release does not retire mounted/dirty use"
    );
    assert!(old_blob.exists() && shared_blob.exists() && old_page.exists());
    old.retire_cache_use().unwrap();
    let removed = collect_scope(&scope(&new)).unwrap();
    assert!(removed.deleted_entries >= 3 && removed.deleted_bytes >= b"old-only".len() as u64);
    assert!(!old_blob.exists() && !old_page.exists());
    assert!(new_blob.exists() && shared_blob.exists());
    assert_eq!(
        new.read_blob(&durable::digest_of(b"shared"), 6).unwrap(),
        b"shared"
    );
    assert!(
        !new.root().join("DURABLE_COMPLETE").exists(),
        "metadata retention grants no body completeness"
    );
    assert!(
        old.retain_snapshot_root(&old_root).is_err(),
        "late promotion cannot resurrect a retired use"
    );
    let reopened = managed(&temp, &old_context, 11);
    assert!(
        !reopened.cache_retention_known().unwrap(),
        "explicit reopen starts a fresh unknown use"
    );
    assert_eq!(
        collect_scope(&scope(&new)).unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    reopened.retain_snapshot_root(&old_root).unwrap();
    assert!(collect_scope(&scope(&new)).is_ok());
}

#[test]
fn unknown_zero_delete_then_real_canonical_promotion_and_exact_idempotence() {
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 21);
    let live = blob(&store, b"live");
    let stale = blob(&store, b"stale");
    let charges_before = fs::read(scope(&store).join(LEDGER)).unwrap();
    assert_eq!(
        collect_scope(&scope(&store)).unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    assert!(stale.exists() && live.exists());
    assert_eq!(
        fs::read(scope(&store).join(LEDGER)).unwrap(),
        charges_before,
        "failed proof cannot reset accounting"
    );
    store.retain_snapshot_root(&closure).unwrap();
    let first = fs::read(scope(&store).join(LEDGER)).unwrap();
    store.retain_snapshot_root(&closure).unwrap();
    assert_eq!(
        fs::read(scope(&store).join(LEDGER)).unwrap(),
        first,
        "exact verified promotion does not write twice"
    );
    let page = store
        .root()
        .join(RETENTION_DIR)
        .join("pages")
        .join(hex::encode(
            parse_digest(closure.pages().keys().next().unwrap()).unwrap(),
        ));
    fs::write(&page, b"corrupt").unwrap();
    assert!(collect_scope(&scope(&store)).is_err());
    assert!(stale.exists());
    store.retain_snapshot_root(&closure).unwrap();
    let report = collect_scope(&scope(&store)).unwrap();
    assert_eq!(report.deleted_bytes, 5);
    assert!(!stale.exists() && live.exists());
}

#[tokio::test]
async fn cancellation_keeps_actual_blocking_fd_and_peak_reservation_until_retirement() {
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 31);
    store.retain_snapshot_root(&closure).unwrap();
    let stale = blob(&store, b"stale");
    let transaction = store.transaction().unwrap();
    let budget = HydrationBudget::reserve(&store, closure.files(), Some(&closure))
        .unwrap()
        .unwrap();
    let directory = store.content_dir().to_path_buf();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release = ReleaseActualJob(Some(release_tx));
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let actual = tokio::task::spawn_blocking(move || {
        let admission =
            WriteAdmission::begin_with_budget(&directory, &"a".repeat(64), 64, Some(budget))
                .unwrap()
                .unwrap();
        let mut file = secure_fs::open_create_new(&admission.temporary_path()).unwrap();
        file.write_all(b"unpublished").unwrap();
        started_tx.send(admission.temporary_path()).unwrap();
        release_rx.recv().unwrap();
        drop(file);
        drop(admission);
        finished_tx.send(()).unwrap();
    });
    let temporary = started_rx.await.unwrap();
    drop(actual); // Cancels the waiter, never the actual blocking job.
    drop(transaction);
    assert_eq!(
        collect_scope(&scope(&store)).unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    assert!(temporary.exists() && stale.exists());
    assert!(fs::read_dir(scope(&store).join(INTENTS)).unwrap().count() >= 2);
    release.release();
    finished_rx.await.unwrap();
    assert!(!temporary.exists());
    assert!(collect_scope(&scope(&store)).unwrap().deleted_entries >= 1);
    assert!(!stale.exists());
}

#[test]
fn same_digest_concurrent_physical_temps_are_charged_before_io() {
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 41);
    store.retain_snapshot_root(&closure).unwrap();
    let capacity = limits().max_bytes - limits().proof_headroom_bytes;
    let mut charges: Charges =
        read_record(&scope(&store).join(LEDGER), CONTROL_BYTES as usize).unwrap();
    charges.bytes = capacity - (100 + CONTROL_BYTES + 3 * ENTRY_CHARGE);
    raw_atomic(&scope(&store), LEDGER, &charges).unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let admitted = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|threads| {
        for _ in 0..2 {
            let barrier = barrier.clone();
            let admitted = admitted.clone();
            let directory = store.content_dir().to_path_buf();
            threads.spawn(move || {
                let reservation = WriteAdmission::begin(&directory, &"b".repeat(64), 100);
                let held = if let Ok(Some(reservation)) = reservation {
                    let mut file =
                        secure_fs::open_create_new(&reservation.temporary_path()).unwrap();
                    file.write_all(&[0u8; 100]).unwrap();
                    admitted.fetch_add(1, Ordering::SeqCst);
                    Some((file, reservation))
                } else {
                    None
                };
                barrier.wait();
                drop(held);
            });
        }
        barrier.wait();
    });
    assert_eq!(admitted.load(Ordering::SeqCst), 1);
    let after: Charges = read_record(&scope(&store).join(LEDGER), CONTROL_BYTES as usize).unwrap();
    assert_eq!(after.bytes, capacity);
}

#[test]
fn durable_exact_orphan_declaration_recovers_temp_before_its_intent() {
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 51);
    store.retain_snapshot_root(&closure).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let name = "c".repeat(64);
    let temporary_name = format!(".{name}.tmp.{id}");
    let temporary = store.content_dir().join(&temporary_name);
    let intent_path = scope(&store).join(INTENTS).join(format!("{id}.json"));
    raw_atomic(
        intent_path.parent().unwrap(),
        intent_path.file_name().unwrap().to_str().unwrap(),
        &WriterIntent {
            revision: 1,
            operation_id: id,
            relative_directory: "blobs".into(),
            final_name: name,
            temporary_name,
            maximum_bytes: 64,
        },
    )
    .unwrap();
    fs::write(&temporary, b"orphan").unwrap(); // Exact durable files left by a killed writer.
    let report = collect_scope(&scope(&store)).unwrap();
    assert_eq!(report.deleted_entries, 2);
    assert!(!temporary.exists() && !intent_path.exists());
}

#[test]
fn malformed_symlink_busy_oversized_and_small_inventory_budget_all_delete_zero() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 61);
    store.retain_snapshot_root(&closure).unwrap();
    let stale = blob(&store, b"stale");
    let bad_intent = scope(&store)
        .join(INTENTS)
        .join(format!("{}.json", uuid::Uuid::new_v4()));
    fs::write(&bad_intent, b"{}").unwrap();
    assert!(collect_scope(&scope(&store)).is_err());
    assert!(stale.exists());
    fs::remove_file(bad_intent).unwrap();
    let outside = temp.path().join("outside");
    fs::write(&outside, b"outside").unwrap();
    let link = store.content_dir().join("d".repeat(64));
    symlink(&outside, &link).unwrap();
    assert!(collect_scope(&scope(&store)).is_err());
    assert!(stale.exists() && outside.exists());
    fs::remove_file(link).unwrap();
    let held = secure_fs::open_regular_nonblocking(&stale).unwrap();
    held.try_lock().unwrap();
    assert!(collect_scope(&scope(&store)).is_err());
    assert!(stale.exists());
    drop(held);
    let huge = store.content_dir().join("e".repeat(64));
    File::create(&huge)
        .unwrap()
        .set_len(limits().max_bytes + 1)
        .unwrap();
    assert!(collect_scope(&scope(&store)).is_err());
    assert!(stale.exists());
    fs::remove_file(huge).unwrap();
    let mut constrained = limits();
    constrained.max_inventory_bytes = 1;
    raw_atomic(
        &scope(&store),
        POLICY,
        &CapacityPolicy {
            revision: 1,
            limits: constrained,
        },
    )
    .unwrap();
    assert_eq!(
        collect_scope(&scope(&store)).unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert!(stale.exists());
}

#[test]
fn prepared_unlink_rejects_a_recreated_same_digest_lifetime_and_parent_symlink() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let name = "f".repeat(64);
    let path = temp.path().join(&name);
    fs::write(&path, b"same").unwrap();
    let identity =
        secure_fs::RegularIdentity::from_metadata(&fs::symlink_metadata(&path).unwrap()).unwrap();
    let prepared = secure_fs::prepare_current_regular(temp.path(), &name, identity)
        .unwrap()
        .unwrap();
    fs::remove_file(&path).unwrap();
    fs::write(&path, b"same").unwrap();
    assert!(prepared.remove().is_err());
    assert_eq!(fs::read(&path).unwrap(), b"same");
    let directory_link = temp.path().join("linked");
    symlink(temp.path(), &directory_link).unwrap();
    let current =
        secure_fs::RegularIdentity::from_metadata(&fs::symlink_metadata(&path).unwrap()).unwrap();
    assert!(secure_fs::prepare_current_regular(&directory_link, &name, current).is_err());
    assert!(path.exists());
}

#[tokio::test]
async fn upfront_capacity_rejection_preserves_complete_and_retention_never_skips_body_sha() {
    let temp = tempfile::tempdir().unwrap();
    let (context, closure) = root(&[("live", b"live")]);
    let store = managed(&temp, &context, 71);
    let view = ViewMeta {
        snapshot_id: closure.snapshot_id().into(),
        namespace_view_id: closure.descriptor().namespace_view_id.clone(),
        scope: "/code".into(),
        lease_id: "lease-fixture".into(),
    };
    store
        .hydrate_snapshot_with(&view, &closure, |_| {
            std::future::ready(Ok(b"live".to_vec()))
        })
        .await
        .unwrap();
    let marker = fs::read(store.root().join("DURABLE_COMPLETE")).unwrap();
    let mut charges: Charges =
        read_record(&scope(&store).join(LEDGER), CONTROL_BYTES as usize).unwrap();
    charges.bytes = limits().max_bytes - limits().proof_headroom_bytes;
    raw_atomic(&scope(&store), LEDGER, &charges).unwrap();
    let rejected = store
        .hydrate_snapshot_with(&view, &closure, |_| {
            std::future::ready(Ok(b"live".to_vec()))
        })
        .await
        .unwrap_err();
    assert_eq!(rejected.code, SnapshotErrorCode::LimitExceeded);
    assert_eq!(
        fs::read(store.root().join("DURABLE_COMPLETE")).unwrap(),
        marker
    );
    assert!(
        collect_scope(&scope(&store)).is_ok(),
        "fresh inventory resets conservative overcharges without a body audit"
    );
    let path = store.content_dir().join(hex::encode(
        parse_digest(&durable::digest_of(b"live")).unwrap(),
    ));
    fs::write(path, b"evil").unwrap();
    assert!(store.cache_retention_known().unwrap());
    assert!(
        store.manifest().is_err(),
        "known root never grants body completeness"
    );
}
