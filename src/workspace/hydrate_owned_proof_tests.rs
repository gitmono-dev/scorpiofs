//! Actual HTTP owners and real durable hydration, including update/recovery.

use std::{collections::HashMap, fs};

use super::*;
use crate::snapshot::{
    durable::{owned_proof_tests::*, CasVerificationReason, ViewMeta},
    SnapshotFile,
};

async fn one_owner(
    reader: &SnapshotReader,
) -> (SnapshotFile, Arc<crate::snapshot::VerifiedContent>) {
    let closure = reader.snapshot_closure().await.unwrap();
    let file = closure.files()[0].clone();
    let batch = reader
        .read_content_batch(std::slice::from_ref(&file))
        .await
        .unwrap();
    let owner = batch.get(&file.content_digest).unwrap().clone();
    drop(batch);
    (file, owner)
}

#[tokio::test]
async fn real_workspace_update_reuses_owned_proof_but_keeps_full_cas_commit_audit() {
    let server = VersionServer::new(
        (0..2)
            .map(|version| Fixture::update_version(true, version))
            .collect(),
    )
    .await;
    let temp = tempfile::tempdir().unwrap();
    let cold_reader = server.reader(0).await;
    let cold = DurableStore::open_for_workspace(
        temp.path(),
        "11111111-2222-4333-8444-555555555601",
        &cold_reader,
    )
    .unwrap();
    let counter = BatchProofCounter::install(cold.content_dir());
    let report = bounded_hydrate(&cold, &cold_reader).await;
    assert_eq!(report.fetched, 192);
    assert_eq!(counter.counts().raw_hashes, 0);
    assert_eq!(counter.counts().verified_reuses, report.fetched);
    assert_eq!(counter.counts().verified_reuse_bytes, report.bytes_total);
    assert_full_snapshot(&cold, &cold_reader, &server.fixture.versions[0]);

    counter.reset();
    let changed_reader = server.reader(1).await;
    let mut changed = DurableStore::open_for_workspace(
        temp.path(),
        "11111111-2222-4333-8444-555555555602",
        &changed_reader,
    )
    .unwrap();
    let meters = changed.enable_verification_meters();
    let report = bounded_hydrate(&changed, &changed_reader).await;
    let changed_bytes = server.fixture.versions[1].bodies["/d0/f000"].len() as u64;
    assert_eq!(report.fetched, 1);
    assert_eq!(report.resumed, 191);
    assert_eq!(
        counter.counts(),
        BatchProofCounts {
            verified_reuses: 1,
            verified_reuse_bytes: changed_bytes,
            ..Default::default()
        }
    );
    let commit = meters.snapshot_for(CasVerificationReason::HydrationCommit);
    assert_eq!(commit.calls, 192);
    assert_eq!(commit.verified, 192);
    assert_eq!(commit.read_bytes, report.bytes_total);
    assert_eq!(changed_reader.content_usage().output_bytes, 0);
    assert_eq!(changed_reader.content_usage().construction_bytes, 0);
    assert_full_snapshot(&changed, &changed_reader, &server.fixture.versions[1]);

    // A published in-memory proof cannot certify later disk corruption.
    let file = changed
        .snapshot_manifest()
        .unwrap()
        .files()
        .iter()
        .find(|file| file.rel_path == "d0/f000")
        .unwrap()
        .clone();
    fs::write(
        changed
            .content_dir()
            .join(file.content_digest.trim_start_matches("sha256:")),
        vec![0xff; file.size as usize],
    )
    .unwrap();
    drop(changed);
    let reopened = DurableStore::open_for_workspace(
        temp.path(),
        "11111111-2222-4333-8444-555555555602",
        &changed_reader,
    )
    .unwrap();
    assert!(!reopened.is_snapshot_complete().unwrap());
    assert!(!reopened.root().join("DURABLE_COMPLETE").exists());
    counter.reset();
    let repaired = bounded_hydrate(&reopened, &changed_reader).await;
    assert_eq!(repaired.fetched, 1);
    assert_eq!(repaired.repaired, 1);
    assert_eq!(counter.counts().verified_reuses, 1);
    assert_eq!(counter.counts().raw_hashes, 0);
    assert_full_snapshot(&reopened, &changed_reader, &server.fixture.versions[1]);
}

#[tokio::test]
async fn actual_http_owner_rejects_wrong_digest_and_size_before_cas_or_journal() {
    let server = Server::new(Fixture::new(true, false, false)).await;
    let reader = server.reader().await;
    let (file, owner) = one_owner(&reader).await;
    for wrong_digest in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let counter = BatchProofCounter::install(store.content_dir());
        let mut requested = file.clone();
        if wrong_digest {
            requested.content_digest = digest_of(b"another fixed file");
        } else {
            requested.size += 1;
        }
        let error = hydrate_offered_owner(&store, &requested, owner.clone(), None)
            .await
            .unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::DigestMismatch);
        assert_eq!(counter.counts(), BatchProofCounts::default());
        assert_eq!(fs::read_dir(store.content_dir()).unwrap().count(), 0);
        assert!(!store.root().join("journal.log").exists());
        assert!(!store.root().join("DURABLE_COMPLETE").exists());
        assert!(!store.is_complete().unwrap());
        assert!(reader.content_usage().output_bytes > 0);
        drop(counter);
    }
    drop(owner);
    assert_eq!(reader.content_usage().output_bytes, 0);
    assert_eq!(reader.content_usage().construction_bytes, 0);
}

#[tokio::test]
async fn verified_owner_directory_sync_failure_cannot_journal_or_complete_and_reopen_repairs() {
    let server = Server::new(Fixture::new(true, false, false)).await;
    let reader = server.reader().await;
    let (file, owner) = one_owner(&reader).await;
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let counter = BatchProofCounter::install(store.content_dir());
    let error = hydrate_offered_owner(
        &store,
        &file,
        owner.clone(),
        Some("object-batch-directory-sync"),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, SnapshotErrorCode::Internal);
    assert_eq!(counter.counts().verified_reuses, 1);
    assert_eq!(counter.counts().raw_hashes, 0);
    assert!(!store.root().join("journal.log").exists());
    assert!(!store.root().join("DURABLE_COMPLETE").exists());
    let blob = store
        .content_dir()
        .join(file.content_digest.trim_start_matches("sha256:"));
    assert!(blob.exists());
    fs::write(&blob, vec![0xff; file.size as usize]).unwrap();
    drop(store);

    let mut reopened = DurableStore::open(temp.path()).unwrap();
    let meters = reopened.enable_verification_meters();
    let report = hydrate_offered_owner(&reopened, &file, owner.clone(), None)
        .await
        .unwrap();
    assert_eq!(report.fetched, 1);
    assert_eq!(report.repaired, 1);
    assert_eq!(
        meters
            .snapshot_for(CasVerificationReason::Resume)
            .digest_mismatches,
        1
    );
    let commit = meters.snapshot_for(CasVerificationReason::HydrationCommit);
    assert_eq!(commit.calls, 1);
    assert_eq!(commit.read_bytes, file.size);
    assert!(reopened.is_complete().unwrap());
    assert_eq!(fs::read(blob).unwrap().as_slice(), owner.as_bytes());
    drop(owner);
    assert_eq!(reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn raw_callback_keeps_full_hash_even_when_its_bytes_come_from_an_actual_verified_owner() {
    async fn unexpected_large_fetch() -> Result<Arc<Vec<u8>>, SnapshotError> {
        panic!("small fixture cannot fetch large content")
    }

    let server = Server::new(Fixture::new(true, false, false)).await;
    let reader = server.reader().await;
    let (file, owner) = one_owner(&reader).await;
    let temp = tempfile::tempdir().unwrap();
    let store = DurableStore::open(temp.path()).unwrap();
    let counter = BatchProofCounter::install(store.content_dir());
    let view = ViewMeta {
        snapshot_id: "sha256:raw-owner-callback".into(),
        namespace_view_id: "sha256:raw-owner-namespace".into(),
        scope: "/project".into(),
        lease_id: "raw-owner-lease".into(),
    };
    let source = owner.clone();
    let report = store
        .hydrate_batches_with_body(
            &view,
            std::slice::from_ref(&file),
            1,
            1,
            move |files| {
                let owner = source.clone();
                Box::pin(
                    async move { Ok(HashMap::from([(files[0].content_digest.clone(), owner)])) },
                )
            },
            |_| {
                Box::pin(unexpected_large_fetch())
                    as futures::future::BoxFuture<'static, Result<Arc<Vec<u8>>, SnapshotError>>
            },
        )
        .await
        .unwrap();
    assert_eq!(report.fetched, 1);
    assert_eq!(
        counter.counts(),
        BatchProofCounts {
            raw_hashes: 1,
            raw_hash_bytes: file.size,
            ..Default::default()
        }
    );
    assert!(store.is_complete().unwrap());
    drop(owner);
    assert_eq!(reader.content_usage().output_bytes, 0);
}
