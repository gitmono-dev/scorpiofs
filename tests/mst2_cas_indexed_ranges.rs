//! Cold reads prove a whole-file identity; warm reads prove covering chunks.
//! This integrity API does not authorize a mount or an offline view.

use std::{fs, path::PathBuf};

use scorpiofs::snapshot::{
    durable::digest_of, DurableStore, LocalCasRangeMeters, SnapshotErrorCode,
};

const CHUNK: usize = 1024 * 1024;

fn open_store(root: &std::path::Path) -> DurableStore {
    let content = root.join("cas");
    fs::create_dir(&content).unwrap();
    DurableStore::open_with_content(root, content).unwrap()
}

fn fixture() -> (tempfile::TempDir, DurableStore, Vec<u8>, String, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let store = open_store(temp.path());
    let body: Vec<_> = (0..2 * CHUNK + 7)
        .map(|index| (index % 251) as u8)
        .collect();
    let digest = digest_of(&body);
    let path = store
        .content_dir()
        .join(digest.strip_prefix("sha256:").unwrap());
    fs::write(&path, &body).unwrap();
    (temp, store, body, digest, path)
}

#[test]
fn cold_whole_scan_and_warm_actual_chunk_work_match_requested_geometry() {
    let (_temp, store, body, digest, _path) = fixture();
    let mut meters = LocalCasRangeMeters::default();
    assert_eq!(
        store
            .read_indexed_blob_range_with_meters(&digest, body.len() as u64, 0, 4096, &mut meters)
            .unwrap()
            .unwrap(),
        body[..4096]
    );
    assert_eq!(meters.bytes_read, body.len() as u64);
    assert_eq!(meters.whole_sha256_bytes, body.len() as u64);
    assert_eq!(meters.chunk_sha256_bytes, body.len() as u64);
    assert!(meters.index_built && !meters.index_hit && !meters.strict_fallback);
    for (offset, length, work) in [
        (0, 4096, CHUNK),
        (CHUNK - 3, 11, 2 * CHUNK),
        (body.len() - 5, usize::MAX, 7),
    ] {
        let end = offset.saturating_add(length).min(body.len());
        assert_eq!(
            store
                .read_indexed_blob_range_with_meters(
                    &digest,
                    body.len() as u64,
                    offset as u64,
                    length,
                    &mut meters
                )
                .unwrap()
                .unwrap(),
            body[offset..end]
        );
        assert_eq!(meters.bytes_read, work as u64);
        assert_eq!(meters.chunk_sha256_bytes, work as u64);
        assert_eq!(meters.whole_sha256_bytes, 0);
        assert!(meters.index_hit && !meters.index_built);
    }
    assert_eq!(
        store
            .read_verified_blob_range_with_meters(&digest, body.len() as u64, 0, 4096, &mut meters)
            .unwrap()
            .unwrap(),
        body[..4096]
    );
    assert_eq!(meters.bytes_read, body.len() as u64);
    assert_eq!(meters.whole_sha256_bytes, body.len() as u64);
    assert_eq!(meters.chunk_sha256_bytes, 0);
    assert!(!meters.index_hit && !meters.index_built);
}

#[test]
fn a_failed_cold_tail_proof_is_not_published_and_can_retry() {
    let (_temp, store, body, digest, path) = fixture();
    let mut corrupt = body.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&path, corrupt).unwrap();
    let mut meters = LocalCasRangeMeters::default();
    for _ in 0..2 {
        assert_eq!(
            store
                .read_indexed_blob_range_with_meters(&digest, body.len() as u64, 0, 13, &mut meters)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
        assert!(!meters.index_hit && !meters.index_built);
        assert_eq!(meters.whole_sha256_bytes, body.len() as u64);
    }
    fs::write(&path, &body).unwrap();
    assert_eq!(
        store
            .read_indexed_blob_range_with_meters(&digest, body.len() as u64, 0, 13, &mut meters)
            .unwrap()
            .unwrap(),
        body[..13]
    );
    assert!(meters.index_built && !meters.index_hit);
}

#[test]
fn warm_uncovered_tail_preserves_correct_prefix_but_bad_covering_bytes_fail() {
    let (_temp, store, body, digest, path) = fixture();
    store
        .read_indexed_blob_range(&digest, body.len() as u64, 0, 13)
        .unwrap();
    let mut corrupt = body.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&path, &corrupt).unwrap();
    assert_eq!(
        store
            .read_indexed_blob_range(&digest, body.len() as u64, 0, 13)
            .unwrap()
            .unwrap(),
        body[..13]
    );
    assert_eq!(
        store
            .read_indexed_blob_range(&digest, body.len() as u64, body.len() as u64 - 5, 13)
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(
        store
            .read_verified_blob_range(&digest, body.len() as u64, 0, 13)
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    // A byte in a covering chunk must be checked even when the caller did
    // not request that byte. Requested corruption fails by the same rule.
    for corrupt_at in [4, CHUNK - 1, CHUNK + 100] {
        let mut corrupt = body.clone();
        corrupt[corrupt_at] ^= 1;
        fs::write(&path, &corrupt).unwrap();
        let offset = if corrupt_at >= CHUNK { CHUNK - 3 } else { 0 };
        assert_eq!(
            store
                .read_indexed_blob_range(&digest, body.len() as u64, offset as u64, 13)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
    }
    fs::write(&path, &body).unwrap();
    assert_eq!(
        store
            .read_indexed_blob_range(&digest, body.len() as u64, body.len() as u64 - 5, 13)
            .unwrap()
            .unwrap(),
        body[body.len() - 5..]
    );
}

#[test]
fn facts_do_not_cross_cas_domains_or_fixed_size_and_digest() {
    let (_temp, store, body, digest, path) = fixture();
    store
        .read_indexed_blob_range(&digest, body.len() as u64, 0, 13)
        .unwrap();
    let other_temp = tempfile::tempdir().unwrap();
    let other = open_store(other_temp.path());
    let mut corrupt = body.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(
        other
            .content_dir()
            .join(digest.strip_prefix("sha256:").unwrap()),
        corrupt,
    )
    .unwrap();
    assert_eq!(
        other
            .read_indexed_blob_range(&digest, body.len() as u64, 0, 13)
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    for size in [body.len() as u64 - 1, body.len() as u64 + 1] {
        assert_eq!(
            store
                .read_indexed_blob_range(&digest, size, 0, 13)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
    }
    let wrong = digest_of(b"different identity");
    fs::copy(
        path,
        store
            .content_dir()
            .join(wrong.strip_prefix("sha256:").unwrap()),
    )
    .unwrap();
    assert_eq!(
        store
            .read_indexed_blob_range(&wrong, body.len() as u64, 0, 13)
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
}

#[test]
fn empty_missing_eof_and_output_limits_keep_precise_contracts() {
    let (_temp, store, body, digest, _path) = fixture();
    let empty = digest_of(b"");
    assert!(store
        .read_indexed_blob_range(&empty, 0, 0, usize::MAX)
        .unwrap()
        .is_none());
    fs::write(
        store
            .content_dir()
            .join(empty.strip_prefix("sha256:").unwrap()),
        [],
    )
    .unwrap();
    let mut meters = LocalCasRangeMeters::default();
    for offset in [0, 1, u64::MAX] {
        assert!(store
            .read_indexed_blob_range_with_meters(&empty, 0, offset, usize::MAX, &mut meters)
            .unwrap()
            .unwrap()
            .is_empty());
        assert_eq!(meters.bytes_read, 0);
    }
    store
        .read_indexed_blob_range(&digest, body.len() as u64, 0, 13)
        .unwrap();
    for (offset, len) in [
        (body.len() as u64, usize::MAX),
        (u64::MAX, usize::MAX),
        (0, 0),
    ] {
        assert!(store
            .read_indexed_blob_range_with_meters(
                &digest,
                body.len() as u64,
                offset,
                len,
                &mut meters
            )
            .unwrap()
            .unwrap()
            .is_empty());
        assert!(meters.index_hit);
        assert_eq!(meters.bytes_read, 0);
    }
    let oversized = 8 * 1024 * 1024 * 1024 * 1024 + 1;
    fs::remove_dir_all(store.content_dir()).unwrap();
    fs::write(store.content_dir(), b"not a directory").unwrap();
    assert_eq!(
        store
            .read_indexed_blob_range(&digest, oversized, 0, 0)
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
}

#[test]
fn indexed_large_file_preserves_the_fixed_output_cap() {
    use std::{fs::File, io::Write};

    use ring::digest::{Context, SHA256};
    let temp = tempfile::tempdir().unwrap();
    let store = open_store(temp.path());
    let size = 65 * 1024 * 1024 + 7;
    let source = temp.path().join("source");
    let mut file = File::create(&source).unwrap();
    let buffer = [0x62; 64 * 1024];
    let mut hash = Context::new(&SHA256);
    let mut remaining = size;
    while remaining != 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        file.write_all(&buffer[..count]).unwrap();
        hash.update(&buffer[..count]);
        remaining -= count as u64;
    }
    drop(file);
    let digest = format!("sha256:{}", hex::encode(hash.finish().as_ref()));
    fs::rename(
        source,
        store
            .content_dir()
            .join(digest.strip_prefix("sha256:").unwrap()),
    )
    .unwrap();
    let mut meters = LocalCasRangeMeters::default();
    assert_eq!(
        store
            .read_indexed_blob_range_with_meters(&digest, size, 0, 13, &mut meters)
            .unwrap()
            .unwrap(),
        [0x62; 13]
    );
    assert!(meters.index_built);
    assert_eq!(meters.bytes_read, size);
    assert!(meters.index_fact_charge_bytes >= 66 * 32);
    assert_eq!(
        store
            .read_indexed_blob_range_with_meters(&digest, size, 0, usize::MAX, &mut meters)
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    // Output admission now precedes index lookup as well as body work.
    assert_eq!(meters, LocalCasRangeMeters::default());
    assert_eq!(
        store
            .read_indexed_blob_range_with_meters(&digest, size, size - 5, usize::MAX, &mut meters)
            .unwrap()
            .unwrap(),
        [0x62; 5]
    );
    assert!(meters.index_hit);
    assert!(!meters.index_built);
    assert!(!meters.strict_fallback);
    assert!(meters.index_fact_charge_bytes >= 66 * 32);
    assert_eq!(meters.bytes_read, 7);
    assert_eq!(meters.chunk_sha256_bytes, 7);
    assert_eq!(meters.whole_sha256_bytes, 0);
}
