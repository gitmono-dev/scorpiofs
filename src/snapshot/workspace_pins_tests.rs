use std::{collections::BTreeMap, sync::Arc};

use mst2_codec::{
    descriptor::ServingDescriptor,
    metapage::{page_id, Entry, EntryKind, Page},
};

use super::*;
use crate::snapshot::{
    durable::durability_tests::FaultGuard, ClosureRecord, ScopeCache, ValidatedSnapshotClosure,
};

struct Fixture {
    temp: tempfile::TempDir,
    context: AuthorizedSnapshotContext,
    closure: ValidatedSnapshotClosure,
    view: ViewMeta,
    bytes: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        Self::with_namespace(3)
    }

    fn with_namespace(namespace: u8) -> Self {
        let bytes = b"independently retained workspace bytes".to_vec();
        let empty = Page::build(&[]).unwrap();
        let root = Page::build(&[
            Entry::dir(b"empty", page_id(&empty)),
            Entry::file(
                EntryKind::Regular,
                b"file",
                bytes.len() as u64,
                parse_digest(&durable::digest_of(&bytes)).unwrap(),
            ),
        ])
        .unwrap();
        let descriptor = ServingDescriptor {
            instance_uuid: *uuid::Uuid::from_u128(1).as_bytes(),
            namespace_view_id: [namespace; 32],
            scope: "/project".into(),
            metadata_root: page_id(&root),
        }
        .encode()
        .unwrap();
        let pages = [empty, root]
            .into_iter()
            .map(|bytes| (format!("sha256:{}", hex::encode(page_id(&bytes))), bytes))
            .collect::<BTreeMap<_, _>>();
        let closure = ValidatedSnapshotClosure::from_canonical_pages(&descriptor, pages).unwrap();
        let context = AuthorizedSnapshotContext::new(
            "http://workspace-fixture.invalid",
            "actor-a",
            "/project",
            closure.descriptor().clone(),
            "1",
            "1",
        )
        .unwrap();
        let view = ViewMeta {
            snapshot_id: closure.snapshot_id().into(),
            namespace_view_id: closure.descriptor().namespace_view_id.clone(),
            scope: "/project".into(),
            lease_id: "fixture-live-lease".into(),
        };
        Self {
            temp: tempfile::tempdir().unwrap(),
            context,
            closure,
            view,
            bytes,
        }
    }
    fn owner(&self, id: u128) -> DurableStore {
        DurableStore::open_workspace_context(
            self.temp.path(),
            &uuid::Uuid::from_u128(id).to_string(),
            &self.context,
        )
        .unwrap()
    }
    fn cache(&self) -> ScopeCache {
        ScopeCache::open(self.context.scope_cache_dir(self.temp.path())).unwrap()
    }
    async fn hydrate(&self, store: &DurableStore) {
        store
            .hydrate_snapshot_with(&self.view, &self.closure, |_| {
                std::future::ready(Ok(self.bytes.clone()))
            })
            .await
            .unwrap();
    }
    fn add_record(&self) {
        self.cache()
            .put_record(&ClosureRecord {
                auth_domain: self.context.cache_domain().id().into(),
                metadata_codec: 1,
                policy_revision: 1,
                root_page_id: "workspace-test-root".into(),
                page_ids: vec![],
                files: self.closure.files().to_vec(),
                total_entries: 1,
                pin_ref: self.closure.snapshot_id().into(),
            })
            .unwrap();
    }
}

fn assert_full(store: &DurableStore, f: &Fixture) {
    let closure = store.snapshot_manifest().unwrap();
    assert_eq!(closure.descriptor_bytes(), f.closure.descriptor_bytes());
    assert_eq!(closure.pages(), f.closure.pages());
    assert_eq!(closure.directories(), f.closure.directories());
    assert_eq!(closure.files(), f.closure.files());
}

#[tokio::test]
async fn opt_in_release_meter_counts_other_owner_audits_and_corrupt_body_reads() {
    let f = Fixture::new();
    let mut first = f.owner(2);
    let second = f.owner(3);
    let third = f.owner(4);
    for store in [&first, &second, &third] {
        f.hydrate(store).await;
        assert!(store.verification_meters().is_none());
    }
    f.add_record();
    let meters = first.enable_verification_meters();
    let receipt = first.release_local_pin().unwrap();
    assert_eq!(meters.snapshot().calls, 2);
    assert_eq!(meters.snapshot().verified, 2);
    assert_eq!(meters.snapshot().read_bytes, 2 * f.bytes.len() as u64);
    assert_eq!(
        meters
            .snapshot_for(durable::CasVerificationReason::CompletionAudit)
            .calls,
        2
    );
    assert!(second.verification_meters().is_none());
    assert!(third.verification_meters().is_none());
    assert!(f.cache().record_for("workspace-test-root").is_some());

    let path = first.content_dir().join(hex::encode(
        parse_digest(&f.closure.files()[0].content_digest).unwrap(),
    ));
    fs::write(&path, vec![0x5a; f.bytes.len()]).unwrap();
    assert_eq!(first.release_local_pin().unwrap(), receipt);
    let damaged = meters.snapshot();
    assert_eq!(damaged.digest_mismatches, 2);
    assert_eq!(damaged.read_bytes, 4 * f.bytes.len() as u64);
    assert!(
        f.cache().record_for("workspace-test-root").is_some(),
        "Unknown owners block pruning even when the caller's own release succeeds"
    );
    fs::write(&path, &f.bytes).unwrap();
    // The inventory revoked damaged completion proofs. Restored bytes alone
    // cannot restore those proofs; each owner must explicitly hydrate again.
    f.hydrate(&second).await;
    f.hydrate(&third).await;
    assert_eq!(first.release_local_pin().unwrap(), receipt);
    assert_eq!(meters.snapshot().verified, 4);
    assert!(f.cache().record_for("workspace-test-root").is_some());
}

#[tokio::test]
async fn same_snapshot_owners_keep_independent_complete_closures_and_shared_content() {
    let f = Fixture::new();
    let first = f.owner(2);
    let second = f.owner(3);
    assert_ne!(first.root(), second.root());
    assert_eq!(first.content_dir(), second.content_dir());
    f.hydrate(&first).await;
    f.hydrate(&second).await;
    assert_full(&first, &f);
    assert_full(&second, &f);
    assert_eq!(
        first.local_pin_state().unwrap(),
        LocalPinState::Complete(CompletionKind::FullSnapshot)
    );
    assert_eq!(
        f.cache().try_live_pins().unwrap(),
        vec![f.closure.snapshot_id()]
    );
    f.add_record();
    let receipt = first.release_local_pin().unwrap();
    assert_eq!(first.release_local_pin().unwrap(), receipt);
    assert!(!first.is_complete().unwrap());
    assert!(first.manifest().is_err());
    assert!(first.snapshot_manifest().is_err());
    assert_full(&second, &f);
    assert_eq!(
        second
            .read_blob(&f.closure.files()[0].content_digest, f.bytes.len() as u64)
            .unwrap(),
        f.bytes
    );
    assert!(f.cache().record_for("workspace-test-root").is_some());
    assert_eq!(
        f.cache().try_live_pins().unwrap(),
        vec![f.closure.snapshot_id()]
    );
    second.release_local_pin().unwrap();
    assert!(f.cache().try_live_pins().unwrap().is_empty());
    assert!(f.cache().record_for("workspace-test-root").is_none());
    assert!(
        second
            .content_dir()
            .join(hex::encode(
                parse_digest(&f.closure.files()[0].content_digest).unwrap()
            ))
            .exists(),
        "release revokes guarantees and metadata hints; it does not GC shared bodies"
    );
}

#[tokio::test]
async fn revocation_failures_reopen_and_continue_one_operation_at_each_durable_boundary() {
    for phase in [
        "directory-sync",
        "release-intent-durable",
        "release-complete-revoked",
        "release-pin-removed",
        "release-registry-released",
        "release-index-pruned",
        "release-durable",
    ] {
        let f = Fixture::new();
        let store = f.owner(2);
        f.hydrate(&store).await;
        let fault = FaultGuard::install(store.root(), phase, false);
        assert!(store.release_local_pin().is_err(), "{phase}");
        drop(fault);
        let before = match store.local_pin_state().unwrap() {
            LocalPinState::Revoking { operation_id } | LocalPinState::Revoked { operation_id } => {
                operation_id
            }
            other => panic!(
                "{phase}: failed release must not leave an audited complete guarantee: {other:?}"
            ),
        };
        assert!(!store.is_complete().unwrap(), "{phase}");
        assert!(store.manifest().is_err(), "{phase}");
        assert!(store.snapshot_manifest().is_err(), "{phase}");
        let reopened = f.owner(2);
        assert!(!reopened.is_complete().unwrap(), "{phase}");
        let receipt = reopened.release_local_pin().unwrap();
        assert_eq!(receipt.operation_id, before, "{phase}");
        assert_eq!(
            reopened.local_pin_state().unwrap(),
            LocalPinState::Revoked {
                operation_id: before
            }
        );
        assert!(!reopened.root().join("pin.json").exists());
        assert!(!reopened.root().join("DURABLE_COMPLETE").exists());
    }
}

#[tokio::test]
async fn released_marker_cannot_be_resurrected_or_bypass_any_completion_entry() {
    let f = Fixture::new();
    let store = f.owner(2);
    f.hydrate(&store).await;
    let marker = fs::read(store.root().join("DURABLE_COMPLETE")).unwrap();
    let pin = fs::read(store.root().join("pin.json")).unwrap();
    store.release_local_pin().unwrap();
    fs::write(store.root().join("DURABLE_COMPLETE"), marker).unwrap();
    fs::write(store.root().join("pin.json"), pin).unwrap();
    assert!(!store.is_complete().unwrap());
    assert!(!store.is_pinned().unwrap());
    assert_eq!(store.completion_kind().unwrap(), None);
    assert!(!store.is_snapshot_complete().unwrap());
    assert!(store.manifest().is_err());
    assert!(store.snapshot_manifest().is_err());
    assert!(f.cache().try_live_pins().unwrap().is_empty());
    store.release_local_pin().unwrap();
    assert!(!store.root().join("DURABLE_COMPLETE").exists());
}

#[tokio::test]
async fn an_explicit_new_hydration_reverifies_shared_content_after_release() {
    let f = Fixture::new();
    let store = f.owner(2);
    f.hydrate(&store).await;
    let receipt = store.release_local_pin().unwrap();
    let content = store.content_dir().join(hex::encode(
        parse_digest(&f.closure.files()[0].content_digest).unwrap(),
    ));
    fs::write(&content, vec![b'x'; f.bytes.len()]).unwrap();
    let report = store
        .hydrate_snapshot_with(&f.view, &f.closure, |_| {
            std::future::ready(Ok(f.bytes.clone()))
        })
        .await
        .unwrap();
    assert_eq!(report.fetched, 1);
    assert_full(&store, &f);
    assert_eq!(
        store.local_pin_state().unwrap(),
        LocalPinState::Complete(CompletionKind::FullSnapshot)
    );
    assert_ne!(
        store.release_local_pin().unwrap().operation_id,
        receipt.operation_id
    );
}

#[tokio::test]
async fn failed_fresh_hydration_does_not_republish_a_released_complete_marker() {
    let f = Fixture::new();
    let store = f.owner(2);
    f.hydrate(&store).await;
    store.release_local_pin().unwrap();
    let fault = FaultGuard::install(store.root(), "workspace-registry-durable", false);
    assert!(store
        .hydrate_snapshot_with(&f.view, &f.closure, |_| std::future::ready(Ok(f
            .bytes
            .clone())))
        .await
        .is_err());
    drop(fault);
    assert!(!store.is_complete().unwrap());
    assert_eq!(store.local_pin_state().unwrap(), LocalPinState::Incomplete);
    assert!(f.cache().try_live_pins().unwrap().is_empty());
    assert!(store.snapshot_manifest().is_err());
}

#[tokio::test]
async fn active_hydration_and_release_share_the_real_owner_transaction_lock() {
    let f = Fixture::new();
    let store = Arc::new(f.owner(2));
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let task = {
        let store = store.clone();
        let entered = entered.clone();
        let resume = resume.clone();
        let view = f.view.clone();
        let closure = f.closure.clone();
        let bytes = f.bytes.clone();
        tokio::spawn(async move {
            store
                .hydrate_snapshot_with(&view, &closure, |_| {
                    let entered = entered.clone();
                    let resume = resume.clone();
                    let bytes = bytes.clone();
                    async move {
                        entered.notify_one();
                        resume.notified().await;
                        Ok(bytes)
                    }
                })
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    assert_eq!(store.local_pin_state().unwrap(), LocalPinState::Unknown);
    assert_eq!(
        store.release_local_pin().unwrap_err().code,
        SnapshotErrorCode::SnapshotNotReady
    );
    assert!(!store.root().join(REVOKE_FILE).exists());
    resume.notify_one();
    task.await.unwrap().unwrap();
    assert!(store.is_snapshot_complete().unwrap());
    store.release_local_pin().unwrap();
    assert!(!store.is_complete().unwrap());
}

#[tokio::test]
async fn cancelled_new_hydration_cannot_restore_the_old_guarantee() {
    let f = Fixture::new();
    let store = Arc::new(f.owner(2));
    f.hydrate(&store).await;
    store.release_local_pin().unwrap();
    let content = store.content_dir().join(hex::encode(
        parse_digest(&f.closure.files()[0].content_digest).unwrap(),
    ));
    fs::remove_file(content).unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let task = {
        let store = store.clone();
        let entered = entered.clone();
        let view = f.view.clone();
        let closure = f.closure.clone();
        tokio::spawn(async move {
            store
                .hydrate_snapshot_with(&view, &closure, |_| {
                    let entered = entered.clone();
                    async move {
                        entered.notify_one();
                        std::future::pending::<Result<Vec<u8>, SnapshotError>>().await
                    }
                })
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(store.local_pin_state().unwrap(), LocalPinState::Incomplete);
    assert!(!store.is_complete().unwrap());
    assert!(f.cache().try_live_pins().unwrap().is_empty());
    store.release_local_pin().unwrap();
}

#[tokio::test]
async fn cleanup_preserves_busy_or_corrupt_other_owner_pins() {
    let f = Fixture::new();
    let first = f.owner(2);
    let second = f.owner(3);
    f.hydrate(&first).await;
    f.hydrate(&second).await;
    f.add_record();
    let guard = second.transaction().unwrap();
    first.release_local_pin().unwrap();
    assert!(f.cache().record_for("workspace-test-root").is_some());
    drop(guard);
    fs::write(second.root().join(OWNER_FILE), b"broken owner").unwrap();
    assert_eq!(
        f.cache()
            .drop_records_for_pin(f.closure.snapshot_id())
            .unwrap_err()
            .code,
        SnapshotErrorCode::IntegrityError
    );
    assert!(f.cache().record_for("workspace-test-root").is_some());
    assert!(second
        .content_dir()
        .join(hex::encode(
            parse_digest(&f.closure.files()[0].content_digest).unwrap()
        ))
        .exists());
}

#[tokio::test]
async fn damaged_complete_and_repair_state_remain_conservative_retention_evidence() {
    let f = Fixture::new();
    let first = f.owner(2);
    let second = f.owner(3);
    f.hydrate(&first).await;
    f.hydrate(&second).await;
    f.add_record();
    fs::write(second.root().join("manifest.json"), b"broken manifest").unwrap();
    assert_eq!(
        f.cache()
            .drop_records_for_pin(f.closure.snapshot_id())
            .unwrap(),
        0
    );
    assert!(matches!(second.audit_pin().unwrap(), PinAudit::Unknown));
    assert!(f.cache().record_for("workspace-test-root").is_some());
    assert!(!second.is_complete().unwrap());
    assert!(!second.root().join("DURABLE_COMPLETE").exists());
    first.release_local_pin().unwrap();
    assert!(f.cache().try_live_pins().unwrap().is_empty());
    assert!(
        f.cache().record_for("workspace-test-root").is_some(),
        "a repaired-away marker is Unknown, not evidence for pruning"
    );
    second.release_local_pin().unwrap();
    assert!(f.cache().record_for("workspace-test-root").is_none());
}

#[tokio::test]
async fn a_legitimate_completion_bundle_cannot_be_grafted_into_another_fixed_owner() {
    let f = Fixture::new();
    let other = Fixture::with_namespace(4);
    assert_ne!(f.closure.snapshot_id(), other.closure.snapshot_id());
    assert_eq!(f.context.cache_domain(), other.context.cache_domain());
    let first = f.owner(2);
    let second = DurableStore::open_workspace_context(
        f.temp.path(),
        &uuid::Uuid::from_u128(3).to_string(),
        &other.context,
    )
    .unwrap();
    f.hydrate(&first).await;
    other.hydrate(&second).await;
    assert_eq!(first.content_dir(), second.content_dir());
    assert_full(&first, &f);
    assert_full(&second, &other);
    f.add_record();
    for name in [
        "view.json",
        "manifest.json",
        "pin.json",
        "descriptor.bin",
        "metadata.json",
        "journal.log",
        "DURABLE_COMPLETE",
    ] {
        fs::copy(second.root().join(name), first.root().join(name)).unwrap();
    }
    for entry in fs::read_dir(second.root().join("metadata")).unwrap() {
        let entry = entry.unwrap();
        fs::copy(
            entry.path(),
            first.root().join("metadata").join(entry.file_name()),
        )
        .unwrap();
    }
    assert_eq!(
        first.workspace_binding().unwrap().unwrap().snapshot_id(),
        f.closure.snapshot_id()
    );
    assert_eq!(
        first.local_pin_state().unwrap_err().code,
        SnapshotErrorCode::IntegrityError
    );
    assert!(first.is_complete().is_err());
    assert!(first.is_pinned().is_err());
    assert!(first.completion_kind().is_err());
    assert!(first.is_snapshot_complete().is_err());
    assert!(first.manifest().is_err());
    assert!(first.snapshot_manifest().is_err());
    assert!(first.audit_pin().is_err());
    assert!(DurableStore::open_workspace_context(
        f.temp.path(),
        &uuid::Uuid::from_u128(2).to_string(),
        &f.context
    )
    .is_err());
    assert!(f.cache().try_live_pins().is_err());
    for sid in [f.closure.snapshot_id(), other.closure.snapshot_id()] {
        assert!(f.cache().drop_records_for_pin(sid).is_err());
    }
    assert!(f.cache().record_for("workspace-test-root").is_some());
    assert_full(&second, &other);
}

#[tokio::test]
async fn a_missing_or_corrupt_fixed_view_is_never_proven_absence_for_an_owned_commit() {
    for corrupt in [false, true] {
        let f = Fixture::new();
        let store = f.owner(2);
        f.hydrate(&store).await;
        f.add_record();
        let view = store.root().join("view.json");
        if corrupt {
            fs::write(view, b"broken view").unwrap();
        } else {
            fs::remove_file(view).unwrap();
        }
        assert!(store.local_pin_state().is_err());
        assert!(store.is_complete().is_err());
        assert!(store.snapshot_manifest().is_err());
        assert!(f.cache().try_live_pins().is_err());
        assert!(f
            .cache()
            .drop_records_for_pin(f.closure.snapshot_id())
            .is_err());
        assert!(f.cache().record_for("workspace-test-root").is_some());
    }
}

#[tokio::test]
async fn missing_registry_or_owner_records_cannot_reconstruct_old_retention_claims() {
    for released in [false, true] {
        for missing_owner in [false, true] {
            let f = Fixture::new();
            let store = f.owner(2);
            f.hydrate(&store).await;
            if released {
                store.release_local_pin().unwrap();
            }
            f.add_record();
            let binding = store.workspace_binding().unwrap().unwrap();
            let path = if missing_owner {
                store.root().join(OWNER_FILE)
            } else {
                binding
                    .validate_store(&store)
                    .unwrap()
                    .join(REGISTRY_DIR)
                    .join(format!("{}.json", binding.workspace_id()))
            };
            fs::remove_file(&path).unwrap();
            assert!(DurableStore::open_workspace_context(
                f.temp.path(),
                binding.workspace_id(),
                &f.context
            )
            .is_err());
            assert!(
                !path.exists(),
                "open must not recreate a lost retention record"
            );
            assert!(f.cache().try_live_pins().is_err());
            assert!(f
                .cache()
                .drop_records_for_pin(f.closure.snapshot_id())
                .is_err());
            assert!(f.cache().record_for("workspace-test-root").is_some());
            assert!(store.is_complete().is_err());
        }
    }
}

#[tokio::test]
async fn released_but_busy_or_damaged_owners_still_prevent_hint_cleanup() {
    let f = Fixture::new();
    let store = f.owner(2);
    f.hydrate(&store).await;
    store.release_local_pin().unwrap();
    f.add_record();
    let guard = store.transaction().unwrap();
    assert_eq!(
        f.cache()
            .drop_records_for_pin(f.closure.snapshot_id())
            .unwrap(),
        0
    );
    assert!(f.cache().record_for("workspace-test-root").is_some());
    drop(guard);
    fs::write(store.root().join(REVOKE_FILE), b"broken revocation").unwrap();
    assert!(f
        .cache()
        .drop_records_for_pin(f.closure.snapshot_id())
        .is_err());
    assert!(f.cache().record_for("workspace-test-root").is_some());
}

#[tokio::test]
async fn a_released_registry_without_its_tombstone_cannot_authorize_cleanup_or_a_new_operation() {
    let f = Fixture::new();
    let store = f.owner(2);
    f.hydrate(&store).await;
    store.release_local_pin().unwrap();
    f.add_record();
    fs::remove_file(store.root().join(REVOKE_FILE)).unwrap();
    assert!(store.release_local_pin().is_err());
    assert!(store.local_pin_state().is_err());
    assert!(store.is_complete().is_err());
    assert!(f
        .cache()
        .drop_records_for_pin(f.closure.snapshot_id())
        .is_err());
    assert!(f.cache().record_for("workspace-test-root").is_some());
}

#[tokio::test]
async fn the_same_uuid_cannot_switch_snapshot_or_cross_registered_authority() {
    let f = Fixture::new();
    let store = f.owner(2);
    f.hydrate(&store).await;
    let owner = store.workspace_binding().unwrap().unwrap();
    let mut descriptor = f.closure.descriptor().clone();
    descriptor.namespace_view_id = format!("sha256:{}", "04".repeat(32));
    let serving = ServingDescriptor {
        instance_uuid: *uuid::Uuid::parse_str(&descriptor.instance_id)
            .unwrap()
            .as_bytes(),
        namespace_view_id: [4; 32],
        scope: descriptor.scope.clone(),
        metadata_root: parse_digest(&descriptor.metadata_root).unwrap(),
    };
    descriptor.snapshot_id = format!("sha256:{}", hex::encode(serving.snapshot_id().unwrap()));
    let context = AuthorizedSnapshotContext::new(
        "http://workspace-fixture.invalid",
        "actor-a",
        "/project",
        descriptor,
        "1",
        "1",
    )
    .unwrap();
    assert!(
        DurableStore::open_workspace_context(f.temp.path(), owner.workspace_id(), &context)
            .is_err()
    );
    assert!(store.is_snapshot_complete().unwrap());
    let other = AuthorizedSnapshotContext::new(
        "http://workspace-fixture.invalid",
        "actor-b",
        "/project",
        f.closure.descriptor().clone(),
        "1",
        "1",
    )
    .unwrap();
    let isolated =
        DurableStore::open_workspace_context(f.temp.path(), owner.workspace_id(), &other).unwrap();
    assert_ne!(store.content_dir(), isolated.content_dir());
    let scope = owner.validate_store(&store).unwrap();
    let path = scope
        .join(REGISTRY_DIR)
        .join(format!("{}.json", owner.workspace_id()));
    let mut registration: Registration = read_record(&path).unwrap().unwrap();
    registration.binding.auth_domain = other.cache_domain().id().into();
    fs::write(path, encode(&registration).unwrap()).unwrap();
    assert!(f.cache().try_live_pins().is_err());
    assert!(store.is_complete().is_err());
    assert!(isolated.local_pin_state().is_ok());
}

#[cfg(unix)]
#[tokio::test]
async fn registry_paths_do_not_follow_an_owner_symlink_outside_the_scope() {
    let f = Fixture::new();
    let store = f.owner(2);
    f.hydrate(&store).await;
    let root = store.root().to_path_buf();
    let moved = f.temp.path().join("outside-owner");
    fs::rename(&root, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &root).unwrap();
    assert!(f.cache().try_live_pins().is_err());
    assert!(store.release_local_pin().is_err());
    assert!(moved.join("DURABLE_COMPLETE").exists());
}
