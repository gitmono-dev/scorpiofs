//! Local output budgets do not replace CAS integrity or offline authorization.

use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use ring::digest::{Context, SHA256};
use scorpiofs::snapshot::{
    client::MAX_BUFFERED_FILE_BYTES, durable::digest_of, DurableStore, SnapshotErrorCode,
};

// These read primitives need no completion transaction or directory fsync,
// so their regressions execute on Windows as well as Unix.
fn store(root: &Path) -> DurableStore {
    let content = root.join("cas");
    fs::create_dir(&content).unwrap();
    DurableStore::open_with_content(root, content).unwrap()
}

fn blob_path(store: &DurableStore, digest: &str) -> PathBuf {
    store
        .content_dir()
        .join(digest.strip_prefix("sha256:").unwrap())
}

#[test]
fn whole_cas_read_rejects_oversized_output_and_verifies_size_and_hash() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path());
    let body = vec![b'x'; 2 * 64 * 1024 + 7];
    let digest = digest_of(&body);
    for size in [MAX_BUFFERED_FILE_BYTES + 1, u64::MAX] {
        assert_eq!(
            store.read_blob(&digest, size).unwrap_err().code,
            SnapshotErrorCode::LimitExceeded
        );
    }
    // A normal absent digest retains its typed absence result.
    assert_eq!(
        store.read_blob(&digest, 7).unwrap_err().code,
        SnapshotErrorCode::PathNotFound
    );
    let path = blob_path(&store, &digest);
    fs::write(&path, &body).unwrap();
    assert_eq!(store.read_blob(&digest, body.len() as u64).unwrap(), body);
    assert_eq!(
        store
            .read_blob(&digest, body.len() as u64 - 1)
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
    let mut corrupt = body;
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&path, &corrupt).unwrap();
    assert_eq!(
        store
            .read_blob(&digest, corrupt.len() as u64)
            .unwrap_err()
            .code,
        SnapshotErrorCode::DigestMismatch
    );
}

#[test]
fn untrusted_large_cas_length_does_not_override_the_advertised_small_size() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path());
    let digest = digest_of(b"x");
    let file = File::create(blob_path(&store, &digest)).unwrap();
    file.set_len(256 * 1024 * 1024).unwrap();
    assert_eq!(
        store.read_blob(&digest, 1).unwrap_err().code,
        SnapshotErrorCode::DigestMismatch
    );
}

#[test]
fn cas_ranges_limit_clamped_output_and_keep_eof_and_missing_distinct() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path());
    let size = MAX_BUFFERED_FILE_BYTES + 5;
    let source = temp.path().join("range-source");
    let mut file = File::create(&source).unwrap();
    file.set_len(size).unwrap();
    file.seek(SeekFrom::Start(size - 5)).unwrap();
    file.write_all(b"abcde").unwrap();
    drop(file);
    // Hash the actual sparse file with bounded memory before assigning its
    // content address: its zero-filled prefix and tail are both committed.
    let mut hash = Context::new(&SHA256);
    let mut file = File::open(&source).unwrap();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    drop(file);
    let digest = format!("sha256:{}", hex::encode(hash.finish().as_ref()));
    assert!(store
        .read_verified_blob_range(&digest, size, 0, usize::MAX)
        .unwrap()
        .is_none());
    fs::rename(source, blob_path(&store, &digest)).unwrap();
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
        b"abcde"
    );
    assert!(store
        .read_verified_blob_range(&digest, size, size, usize::MAX)
        .unwrap()
        .unwrap()
        .is_empty());
    assert!(store
        .read_verified_blob_range(&digest, size, 0, 0)
        .unwrap()
        .unwrap()
        .is_empty());
}

#[test]
fn empty_cas_ranges_still_reject_wrong_size_and_corrupt_body() {
    let temp = tempfile::tempdir().unwrap();
    let store = store(temp.path());
    let body = b"zero output still verifies the addressed bytes";
    let size = body.len() as u64;
    let digest = digest_of(body);
    let path = blob_path(&store, &digest);
    fs::write(&path, body).unwrap();
    for wrong_size in [size - 1, size + 1] {
        for (offset, len) in [(0, 0), (size, usize::MAX)] {
            assert_eq!(
                store
                    .read_verified_blob_range(&digest, wrong_size, offset, len)
                    .unwrap_err()
                    .code,
                SnapshotErrorCode::DigestMismatch
            );
        }
    }
    let mut corrupt = *body;
    corrupt[0] ^= 1;
    fs::write(&path, corrupt).unwrap();
    for (offset, len) in [(0, 0), (size, usize::MAX)] {
        assert_eq!(
            store
                .read_verified_blob_range(&digest, size, offset, len)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
    }
}
