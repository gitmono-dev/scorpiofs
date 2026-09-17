//! Live-stack e2e for the lease lifecycle (link Phase A-1).
//!
//! Three properties are checked against the running server:
//!  1. a short-lived lease is renewed proactively, so an operation that
//!     starts well after the initial window still succeeds;
//!  2. revoking the lease turns source operations into a *typed* failure
//!     (never a silent empty result or a hang);
//!  3. content already hydrated locally keeps being served after the lease
//!     is gone — the completed mount does not depend on the server.
//!
//! Ignored by default; requires the running monoengine stack.

use std::sync::Arc;

use scorpiofs::snapshot::{DurableStore, Mst2Client, SnapshotReader};

fn base_url() -> String {
    std::env::var("MST2_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:19700".to_string())
}

#[tokio::test]
#[ignore]
async fn short_lease_is_renewed_proactively_and_revocation_is_typed() {
    let client = Mst2Client::new(base_url());

    // 3s lease: the 5s sleep below outlives it, so success proves renewal.
    let reader = SnapshotReader::resolve(client.clone(), "/project", 3)
        .await
        .expect("resolve");

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let manifest = reader
        .file_manifest()
        .await
        .expect("manifest after the initial lease window (renewal must have happened)");
    assert!(!manifest.is_empty());
    let f = manifest.first().unwrap().clone();
    let bytes = reader
        .read_file(&f.rel_path, &f.content_digest)
        .await
        .expect("content read after the initial lease window");
    assert_eq!(bytes.len() as u64, f.size);

    // A healthy path performs no transport retries.
    assert_eq!(client.retry_count(), 0, "healthy path must not retry");

    // Client contract: if *our* retention claim cannot be renewed, the next
    // source operation must fail with a typed lease error instead of
    // silently proceeding on a claim the server no longer honours. (A
    // snapshot stays readable while any lease covers it — the server-side
    // 410 path is covered by the t08 oracle in an isolated snapshot — so
    // this asserts the client's own obligation: re-resolve, don't guess.)
    let victim = SnapshotReader::resolve(client.clone(), "/project", 3)
        .await
        .expect("second resolve");
    assert!(
        client
            .release_lease(&victim.lease_id)
            .await
            .expect("release victim lease"),
        "the victim lease must be removable"
    );
    // Let the deadline close in so the pre-operation renewal is attempted.
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    let err = victim
        .file_manifest()
        .await
        .expect_err("a claim that cannot be renewed must fail the operation");
    assert!(
        matches!(
            err.code,
            scorpiofs::snapshot::SnapshotErrorCode::LeaseUnknown
                | scorpiofs::snapshot::SnapshotErrorCode::LeaseExpired
        ),
        "expected a lease error, got {:?}: {}",
        err.code,
        err.message
    );
}

#[tokio::test]
#[ignore]
async fn hydrated_content_survives_lease_revocation() {
    let client = Mst2Client::new(base_url());
    let reader = SnapshotReader::resolve(client.clone(), "/project", 600)
        .await
        .expect("resolve");

    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(DurableStore::open(tmp.path()).unwrap());
    let report = store.hydrate(&reader).await.expect("hydrate");
    assert!(report.complete && report.total_files > 0);
    assert!(store.is_complete().unwrap());

    // Revoke the lease, then drop the reader so no renewal can happen.
    let lease = reader.lease_id.clone();
    assert!(client.release_lease(&lease).await.expect("release"));
    drop(reader);

    // Every hydrated byte is still readable and re-verifies: the local
    // store never needed the lease, and revocation does not damage it.
    let manifest = store.manifest().expect("manifest from the store");
    assert_eq!(store.verify_all(&manifest).unwrap(), manifest.len() as u64);
    let f = manifest.first().unwrap();
    let bytes = store
        .read_blob(&f.content_digest, f.size)
        .expect("content from the local CAS");
    assert_eq!(bytes.len() as u64, f.size);
}
