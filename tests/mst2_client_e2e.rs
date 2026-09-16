//! End-to-end client test against a live monoengine stack.
//!
//! Ignored by default (requires the storage-only stack at MST2_BASE_URL,
//! default http://127.0.0.1:19700). Run with:
//!   MST2_BASE_URL=http://127.0.0.1:19700 \
//!     cargo test --test mst2_client_e2e -- --ignored --nocapture

use scorpiofs::snapshot::{Mst2Client, SnapshotReader};

const SCOPE: &str = "/project";

#[tokio::test]
#[ignore = "requires a live monoengine stack with [mst2].enabled"]
async fn resolve_walk_and_read_verified() {
    let base = std::env::var("MST2_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:19700".into());
    let client = Mst2Client::new(base);

    // capabilities gate
    let caps = client.capabilities().await.expect("capabilities");
    assert!(caps.features.resolve && caps.features.directory);

    // resolve once; the view is pinned
    let reader = SnapshotReader::resolve(client, SCOPE, 600).await.expect("resolve");
    assert!(reader.snapshot_id().starts_with("sha256:"));
    assert_eq!(reader.descriptor.scope, SCOPE);
    let snapshot_id = reader.snapshot_id().to_string();

    // manifest walk
    let files = reader.file_manifest().await.expect("manifest");
    assert!(files.len() >= 3, "scope must contain seeded files");
    let hashes: std::collections::HashSet<_> = files.iter().map(|f| &f.content_digest).collect();
    assert_eq!(hashes.len(), files.len(), "duplicate file paths");

    // read one file with server + local digest verification
    let f = files.first().expect("at least one file").clone();
    let bytes = reader
        .read_file(&f.rel_path, &f.content_digest)
        .await
        .expect("verified blob");
    assert_eq!(bytes.len() as u64, f.size);

    // lookup four-state outcomes
    let results = reader
        .lookup(&vec![format!("/{}", f.rel_path), "/definitely-missing-xyz".to_string()])
        .await
        .expect("lookup");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].status, "found");
    assert_eq!(results[1].status, "absent");

    // digest tampering is rejected (server-side enforcement)
    let tampered = reader
        .read_file(&f.rel_path, &("sha256:".to_string() + &"0".repeat(64)))
        .await;
    assert!(matches!(
        tampered.map_err(|e| e.code),
        Err(scorpiofs::snapshot::SnapshotErrorCode::DigestMismatch)
    ));

    // unknown snapshot id -> typed error, not a hang
    let bad_client = Mst2Client::new(
        std::env::var("MST2_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:19700".into()),
    );
    let err = bad_client
        .directory("sha256:" + &"1".repeat(64), "/", 256, None)
        .await
        .expect_err("unknown snapshot must error");
    assert_eq!(err.code, scorpiofs::snapshot::SnapshotErrorCode::SnapshotUnknown);

    let _ = snapshot_id;
}
