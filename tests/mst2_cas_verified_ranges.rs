//! Returned local ranges participate in the successful whole-file hash.
//! These primitives require no completion transaction, mount or Unix fsync.

use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use ring::digest::{Context, SHA256};
use scorpiofs::snapshot::{
    client::MAX_BUFFERED_FILE_BYTES, durable::digest_of, DurableStore, SnapshotErrorCode,
};

fn store(root: &Path) -> DurableStore {
    let content = root.join("cas");
    fs::create_dir(&content).unwrap();
    DurableStore::open_with_content(root, content).unwrap()
}

fn path(store: &DurableStore, digest: &str) -> PathBuf {
    store
        .content_dir()
        .join(digest.strip_prefix("sha256:").unwrap())
}

#[test]
fn verified_ranges_cross_scan_boundaries_and_clamp_to_fixed_eof() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path());
    let body: Vec<_> = (0..3 * 64 * 1024 + 7)
        .map(|index| (index % 251) as u8)
        .collect();
    let digest = digest_of(&body);
    fs::write(path(&store, &digest), &body).unwrap();
    for (offset, length) in [(0, 13), (64 * 1024 - 3, 11), (body.len() - 5, usize::MAX)] {
        let end = offset.saturating_add(length).min(body.len());
        assert_eq!(
            store
                .read_verified_blob_range(&digest, body.len() as u64, offset as u64, length)
                .unwrap()
                .unwrap(),
            body[offset..end]
        );
    }
    for (offset, length) in [
        (body.len() as u64, usize::MAX),
        (u64::MAX, usize::MAX),
        (0, 0),
    ] {
        assert!(store
            .read_verified_blob_range(&digest, body.len() as u64, offset, length)
            .unwrap()
            .unwrap()
            .is_empty());
    }
}

#[test]
fn same_size_corruption_in_requested_or_unread_tail_bytes_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path());
    let body = vec![0x51; 3 * 64 * 1024 + 7];
    let digest = digest_of(&body);
    for corrupt_at in [4, body.len() - 1] {
        let mut corrupt = body.clone();
        corrupt[corrupt_at] ^= 1;
        fs::write(path(&store, &digest), &corrupt).unwrap();
        assert_eq!(
            store
                .read_verified_blob_range(&digest, body.len() as u64, 0, 13)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
        // An empty result still cannot be reported as whole-file verified.
        assert_eq!(
            store
                .read_verified_blob_range(&digest, body.len() as u64, body.len() as u64, 13)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
    }
    fs::write(path(&store, &digest), &body).unwrap();
    assert_eq!(
        store
            .read_verified_blob_range(&digest, body.len() as u64, 0, 13)
            .unwrap()
            .unwrap(),
        body[..13]
    );
}

#[test]
fn fixed_size_and_digest_are_verified_before_returning_a_range() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path());
    let body = b"fixed view content";
    let digest = digest_of(body);
    fs::write(path(&store, &digest), body).unwrap();
    for size in [body.len() as u64 - 1, body.len() as u64 + 1] {
        assert_eq!(
            store
                .read_verified_blob_range(&digest, size, 0, 3)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
    }
    let wrong_digest = digest_of(b"different whole content");
    fs::write(path(&store, &wrong_digest), body).unwrap();
    assert_eq!(
        store
            .read_verified_blob_range(&wrong_digest, body.len() as u64, 0, 3)
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    assert_eq!(
        store
            .read_verified_blob_range("sha256:../../outside", body.len() as u64, 0, 3)
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
}

#[test]
fn serving_profile_rejects_above_eight_tib_before_cas_access() {
    const MAX_FILE_SIZE: u64 = 8 * 1024 * 1024 * 1024 * 1024;
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path());
    let digest = digest_of(b"boundary fixture");
    fs::write(path(&store, &digest), b"boundary fixture").unwrap();
    // The exact profile boundary is allowed to reach fixed-size validation;
    // no 8 TiB physical file is needed to prove admission at that boundary.
    assert_eq!(
        store
            .read_verified_blob_range(&digest, MAX_FILE_SIZE, 0, 1)
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    fs::remove_file(path(&store, &digest)).unwrap();
    fs::remove_dir(store.content_dir()).unwrap();
    fs::write(store.content_dir(), b"not a directory").unwrap();
    // Any CAS access would now fail with an I/O error. Oversized declarations
    // must instead fail in the preflight, including for an empty output.
    for size in [MAX_FILE_SIZE + 1, u64::MAX] {
        assert_eq!(
            store
                .read_verified_blob_range(&digest, size, u64::MAX, 0)
                .unwrap_err()
                .code,
            SnapshotErrorCode::LimitExceeded
        );
    }
}

#[test]
fn a_file_above_whole_buffer_cap_supports_verified_small_ranges() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path());
    let size = MAX_BUFFERED_FILE_BYTES + 5;
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
    fs::rename(source, path(&store, &digest)).unwrap();
    assert_eq!(
        store.read_blob(&digest, size).unwrap_err().code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(
        store
            .read_verified_blob_range(&digest, size, 0, usize::MAX)
            .unwrap_err()
            .code,
        SnapshotErrorCode::LimitExceeded
    );
    assert_eq!(
        store
            .read_verified_blob_range(&digest, size, size - 5, usize::MAX)
            .unwrap()
            .unwrap(),
        [0x62; 5]
    );
    assert!(store
        .read_verified_blob_range(&digest, size, size, usize::MAX)
        .unwrap()
        .unwrap()
        .is_empty());
}

#[test]
fn missing_and_verified_empty_content_remain_distinct() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path());
    let digest = digest_of(b"");
    assert!(store
        .read_verified_blob_range(&digest, 0, 0, usize::MAX)
        .unwrap()
        .is_none());
    fs::write(path(&store, &digest), []).unwrap();
    for offset in [0, 1, u64::MAX] {
        assert!(store
            .read_verified_blob_range(&digest, 0, offset, usize::MAX)
            .unwrap()
            .unwrap()
            .is_empty());
    }
    let wrong_digest = digest_of(b"not empty");
    fs::write(path(&store, &wrong_digest), []).unwrap();
    assert_eq!(
        store
            .read_verified_blob_range(&wrong_digest, 0, 0, 0)
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
}
