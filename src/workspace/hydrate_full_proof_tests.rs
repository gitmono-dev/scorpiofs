use std::fs;

use super::*;

#[tokio::test]
async fn held_metadata_allows_another_owner_to_collect_before_serialized_index_publication() {
    let fixture = Fixture::update_version(true, 0);
    fixture.hold_metadata.store(true, Ordering::SeqCst);
    let server = Server::new(fixture).await;
    let reader = server.reader().await;
    let temp = tempfile::tempdir().unwrap();
    let first = Arc::new(
        DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555612",
            &reader,
        )
        .unwrap(),
    );
    let second = Arc::new(
        DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555613",
            &reader,
        )
        .unwrap(),
    );
    assert_eq!(first.content_dir(), second.content_dir());
    let first_source = reader.clone();
    let first_target = first.clone();
    let first_task =
        tokio::spawn(async move { hydrate_workspace(&first_target, &first_source).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        server.fixture.metadata_started.notified(),
    )
    .await
    .unwrap();
    let second_source = reader.clone();
    let second_target = second.clone();
    let mut second_task =
        tokio::spawn(async move { hydrate_workspace(&second_target, &second_source).await });
    let concurrent = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while server.fixture.metadata_calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    if concurrent.is_err() {
        first_task.abort();
        second_task.abort();
        let _ = first_task.await;
        let _ = second_task.await;
        panic!("another owner remained blocked behind metadata HTTP while sharing the scope index");
    }
    let scope = first.content_dir().parent().unwrap();
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(scope.join("closures.lock"))
        .unwrap();
    lock.try_lock()
        .expect("held metadata collectors must not own the publication lock");
    lock.unlock().unwrap();
    first_task.abort();
    assert!(first_task.await.unwrap_err().is_cancelled());
    assert!(!first.is_snapshot_complete().unwrap());
    assert!(!second.is_snapshot_complete().unwrap());
    assert!(!scope.join("closures.json").exists());
    server.fixture.hold_metadata.store(false, Ordering::SeqCst);
    server.fixture.metadata_release.notify_waiters();
    let finished = tokio::time::timeout(std::time::Duration::from_secs(30), &mut second_task).await;
    if finished.is_err() {
        second_task.abort();
        let _ = second_task.await;
        panic!("surviving owner could not publish its proved root and complete hydration");
    }
    let report = finished.unwrap().unwrap().unwrap();
    assert!(report.complete);
    assert_eq!(report.completion_kind, CompletionKind::FullSnapshot);
    assert!(!first.is_snapshot_complete().unwrap());
    assert!(scope.join("closures.json").exists());
    assert_full_snapshot(&second, &reader, &server.fixture);
    let reopened = DurableStore::open_for_workspace(
        temp.path(),
        "11111111-2222-4333-8444-555555555613",
        &reader,
    )
    .unwrap();
    assert_full_snapshot(&reopened, &reader, &server.fixture);
    assert!(bounded_hydrate(&first, &reader).await.complete);
    assert_full_snapshot(&first, &reader, &server.fixture);
}

#[tokio::test]
async fn actual_workspace_full_proof_skips_busy_old_owner_but_keeps_resume_and_commit_cas_audits() {
    for objects in [true, false] {
        let server = Server::new(Fixture::new(objects, false, false)).await;
        let reader = server.reader().await;
        let temp = tempfile::tempdir().unwrap();
        let mut old = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555610",
            &reader,
        )
        .unwrap();
        assert!(bounded_hydrate(&old, &reader).await.complete);
        assert_full_snapshot(&old, &reader, &server.fixture);
        let marker = fs::read(old.root().join("DURABLE_COMPLETE")).unwrap();
        let old_meters = old.enable_verification_meters();
        let mut new = DurableStore::open_for_workspace(
            temp.path(),
            "11111111-2222-4333-8444-555555555611",
            &reader,
        )
        .unwrap();
        let meters = new.enable_verification_meters();
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(old.root().join(".hydrate.lock"))
            .unwrap();
        lock.try_lock().unwrap();
        let before_requests = (
            server.fixture.metadata_calls.load(Ordering::SeqCst),
            server.fixture.object_calls.load(Ordering::SeqCst),
            server.fixture.raw_calls.load(Ordering::SeqCst),
        );
        let dependencies = server
            .fixture
            .bodies
            .values()
            .map(|bytes| (digest_of(bytes), bytes.len() as u64))
            .collect::<BTreeMap<_, _>>();
        let count = dependencies.len() as u64;
        let unique_bytes = dependencies.values().sum::<u64>();
        let report = bounded_hydrate(&new, &reader).await;
        assert!(report.complete);
        assert_eq!(report.completion_kind, CompletionKind::FullSnapshot);
        assert_eq!(report.fetched, 0);
        assert_eq!(report.resumed, server.fixture.bodies.len() as u64);
        assert_eq!(old_meters.snapshot().calls, 0);
        assert_eq!(
            meters
                .snapshot_for(CasVerificationReason::CompletionAudit)
                .calls,
            0
        );
        let resume = meters.snapshot_for(CasVerificationReason::Resume);
        assert_eq!(resume.calls, count);
        assert_eq!(resume.verified, count);
        assert_eq!(resume.read_bytes, unique_bytes);
        let commit = meters.snapshot_for(CasVerificationReason::HydrationCommit);
        assert_eq!(commit.calls, count);
        assert_eq!(commit.verified, count);
        assert_eq!(commit.read_bytes, unique_bytes);
        assert_eq!(meters.snapshot().read_bytes, 2 * unique_bytes);
        assert_eq!(
            (
                server.fixture.metadata_calls.load(Ordering::SeqCst),
                server.fixture.object_calls.load(Ordering::SeqCst),
                server.fixture.raw_calls.load(Ordering::SeqCst),
            ),
            before_requests
        );
        assert_eq!(
            fs::read(old.root().join("DURABLE_COMPLETE")).unwrap(),
            marker
        );
        lock.unlock().unwrap();
        assert_full_snapshot(&new, &reader, &server.fixture);
        let audit = meters.snapshot_for(CasVerificationReason::CompletionAudit);
        assert!(audit.calls >= count);
        assert!(audit.read_bytes >= unique_bytes);
        assert_eq!(meters.snapshot_for(CasVerificationReason::Resume), resume);
        assert_eq!(
            meters.snapshot_for(CasVerificationReason::HydrationCommit),
            commit
        );
        assert_full_snapshot(&old, &reader, &server.fixture);
    }
}
