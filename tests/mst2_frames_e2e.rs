//! Live-stack e2e for the frame transport (spec 04 §9 / 06 / 07):
//! OBJECT batch for small files, Chunk Map + CHUNK frames for >256 KiB,
//! descriptor/lease lifecycle. Ignored by default; requires the running
//! monoengine storage-only stack with the t08 oracle fixture pushed.

use scorpiofs::snapshot::{Mst2Client, SnapshotReader};

const CHUNK: usize = 1 << 20;

#[tokio::test]
#[ignore]
async fn frame_transport_verifies_objects_chunks_and_leases() {
    let base =
        std::env::var("MST2_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:19700".to_string());
    let client = Mst2Client::new(base);
    let scope = std::env::var("MST2_SCOPE").unwrap_or_else(|_| "/project".to_string());

    let caps = client.capabilities().await.expect("capabilities");
    assert!(
        caps.features.objects,
        "objects capability must be advertised"
    );
    assert!(
        caps.features.chunk_reads,
        "chunk_reads capability must be advertised"
    );
    assert!(caps.features.metadata_pages);
    assert!(
        caps.frame_encodings.iter().any(|e| e == "zstd"),
        "zstd must be advertised alongside identity: {:?}",
        caps.frame_encodings
    );

    let reader = SnapshotReader::resolve(client.clone(), &scope, 600)
        .await
        .expect("resolve");
    let sid = reader.snapshot_id().to_string();
    let lease = reader.lease_id.clone();

    // descriptor round-trip without touching latest
    let d = client.descriptor(&sid).await.expect("descriptor");
    assert_eq!(d["descriptor"]["snapshot_id"].as_str(), Some(sid.as_str()));

    // The manifest locates the t08 fixture files.
    let manifest = reader.file_manifest().await.expect("manifest");
    let by_path: std::collections::HashMap<_, _> =
        manifest.iter().map(|f| (f.rel_path.as_str(), f)).collect();

    // Small file through the OBJECT frame path.
    let small = by_path
        .get("t08/a.txt")
        .expect("t08/a.txt seeded by the t08 oracle");
    let bytes = reader
        .read_file_frames(&small.rel_path, &small.content_digest, small.size)
        .await
        .expect("small file over OBJECT frames");
    assert_eq!(bytes, b"t08 alpha\n");
    assert_eq!(bytes.len() as u64, small.size);

    // The same object over an explicitly zstd-negotiated stream: the
    // codec transparently decompresses and re-verifies content identity.
    let zstd_map = client
        .objects(
            &sid,
            &[(format!("/{}", small.rel_path), small.content_digest.clone())],
            Some("zstd"),
        )
        .await
        .expect("zstd objects batch");
    let zstd_cid = scorpiofs::snapshot::frames::parse_digest(&small.content_digest).unwrap();
    assert_eq!(zstd_map.get(&zstd_cid).unwrap().as_slice(), b"t08 alpha\n");

    // An unsupported encoding is a typed client error.
    let bad = client
        .objects(
            &sid,
            &[(format!("/{}", small.rel_path), small.content_digest.clone())],
            Some("gzip"),
        )
        .await;
    assert!(matches!(
        bad.map_err(|e| e.code),
        Err(scorpiofs::snapshot::SnapshotErrorCode::ScopeInvalid)
    ));

    // Empty file: a zero-length OBJECT unit must verify, never error as a
    // short read or masquerade as missing content.
    if let Some(empty) = by_path.get("t08/empty.txt") {
        assert_eq!(empty.size, 0);
        let bytes = reader
            .read_file_frames(&empty.rel_path, &empty.content_digest, 0)
            .await
            .expect("empty file over OBJECT frames");
        assert!(bytes.is_empty());
    }

    // Large file through chunk-map + CHUNK frames with whole-file rehash.
    let large = by_path
        .get("t08/chunked.bin")
        .expect("t08/chunked.bin seeded by the t08 oracle");
    assert!(
        large.size > 256 * 1024,
        "fixture must exceed the OBJECT cap"
    );
    let assembled = reader
        .read_file_frames(&large.rel_path, &large.content_digest, large.size)
        .await
        .expect("large file over CHUNK frames");
    assert_eq!(assembled.len() as u64, large.size);
    assert_eq!(assembled.len(), 2 * CHUNK + 7);
    // Deterministic pattern written by the oracle: byte i = (73i+11) % 256.
    for (i, b) in assembled.iter().enumerate() {
        assert_eq!(*b, ((73u64 * i as u64 + 11) % 256) as u8);
    }

    // A wrong digest over the frame path is refused, not served.
    let tampered = reader
        .read_file_frames(
            &small.rel_path,
            &("sha256:".to_string() + &"0".repeat(64)),
            small.size,
        )
        .await;
    assert!(matches!(
        tampered.map_err(|e| e.code),
        Err(scorpiofs::snapshot::SnapshotErrorCode::DigestMismatch)
    ));

    // Lease lifecycle: renew extends, release is idempotent, and a released
    // lease cannot be renewed. The snapshot stays readable via the lease
    // captured at the start of the test until released.
    let renewed = client.renew_lease(&lease, 600).await.expect("lease renew");
    assert_eq!(renewed["lease_id"].as_str(), Some(lease.as_str()));
    assert_eq!(renewed["snapshot_id"].as_str(), Some(sid.as_str()));

    // Release only this dedicated lease; resolve a second one first so the
    // release/404 checks do not depend on the test's first lease.
    let res2 = client.resolve(&scope, 600).await.expect("second resolve");
    let lease2 = res2.lease_id;
    let released = client.release_lease(&lease2).await.expect("release");
    assert!(released, "first release removes the active lease");
    let released_again = client
        .release_lease(&lease2)
        .await
        .expect("idempotent release");
    assert!(!released_again);
    let gone = client.renew_lease(&lease2, 600).await;
    assert!(matches!(
        gone.map_err(|e| (e.code, e.http_status)),
        Err((scorpiofs::snapshot::SnapshotErrorCode::LeaseUnknown, 404))
    ));
}

/// Spec 11 §6 acceptance: the binary page walk (metadata/pages, batched and
/// parent-committed-id-bound) must produce exactly the entry set the JSON
/// `directory` transport produces for the same fixed view.
#[tokio::test]
#[ignore]
async fn page_navigation_matches_json_directory() {
    let base =
        std::env::var("MST2_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:19700".to_string());
    let client = Mst2Client::new(base);
    let scope = std::env::var("MST2_SCOPE").unwrap_or_else(|_| "/project".to_string());
    let reader = SnapshotReader::resolve(client.clone(), &scope, 600)
        .await
        .expect("resolve");
    assert!(reader.capabilities().features.metadata_pages);

    let by_pages = reader.file_manifest_pages().await.expect("page walk");
    let by_json = reader
        .file_manifest_directory()
        .await
        .expect("json directory walk");

    let key = |f: &scorpiofs::snapshot::SnapshotFile| {
        (
            f.rel_path.clone(),
            f.fs_kind.clone(),
            f.size,
            f.content_digest.clone(),
        )
    };
    let mut pages_set: Vec<_> = by_pages.iter().map(key).collect();
    pages_set.sort();
    let mut json_set: Vec<_> = by_json.iter().map(key).collect();
    json_set.sort();
    assert!(
        !pages_set.is_empty(),
        "the view must not read as empty through pages"
    );
    assert_eq!(pages_set, json_set, "page walk must match JSON directory");
}
