use std::{
    collections::HashMap,
    io::Write,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicU8, Ordering},
        Mutex,
    },
};

use super::*;
use crate::snapshot::identity::{ObjectFormat, SourceId};

type Key = (SourceId, ObjectKind, ObjectId);

#[derive(Default)]
struct FakeBackend {
    objects: Mutex<HashMap<Key, Bytes>>,
    calls: Mutex<Vec<(SourceSnapshot, ObjectKind, ObjectId, RelativePath)>>,
    failure: AtomicU8,
}

#[async_trait]
impl ObjectBackend for FakeBackend {
    async fn fetch(
        &self,
        source: &SourceSnapshot,
        kind: ObjectKind,
        oid: &ObjectId,
        source_path: &RelativePath,
        max_bytes: usize,
    ) -> Result<Bytes, SnapshotReadError> {
        self.calls
            .lock()
            .unwrap()
            .push((source.clone(), kind, oid.clone(), source_path.clone()));
        match self.failure.load(Ordering::SeqCst) {
            1 => return Err(SnapshotReadError::Forbidden),
            2 => return Err(SnapshotReadError::Expired),
            3 => {
                return Err(SnapshotReadError::Unavailable(
                    "injected network failure".into(),
                ))
            }
            _ => {}
        }
        let bytes = self
            .objects
            .lock()
            .unwrap()
            .get(&(source.source_id.clone(), kind, oid.clone()))
            .cloned()
            .ok_or_else(|| SnapshotReadError::Unavailable("missing retained object".into()))?;
        if bytes.len() > max_bytes {
            return Err(SnapshotReadError::ObjectTooLarge { limit: max_bytes });
        }
        Ok(bytes)
    }
}

/// Git itself computes fixture identities. The reader/hash verifier under test
/// does not generate the oracle's OIDs, and no objects are written to a repo.
fn git_oid(kind: &str, payload: &[u8]) -> ObjectId {
    let mut child = Command::new("git")
        .args(["hash-object", "--stdin", "-t", kind])
        .env("GIT_DEFAULT_HASH", "sha1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("snapshot fixtures require Git");
    child.stdin.take().unwrap().write_all(payload).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    ObjectId::new(String::from_utf8(output.stdout).unwrap().trim()).unwrap()
}

fn store(backend: &FakeBackend, source: &SourceId, kind: ObjectKind, payload: &[u8]) -> ObjectId {
    let oid = git_oid(kind.as_str(), payload);
    backend.objects.lock().unwrap().insert(
        (source.clone(), kind, oid.clone()),
        Bytes::copy_from_slice(payload),
    );
    oid
}

fn raw_entry(mode: &str, name: &[u8], oid: &ObjectId) -> Vec<u8> {
    let mut result = format!("{mode} ").into_bytes();
    result.extend(name);
    result.push(0);
    result.extend(hex::decode(oid.as_str()).unwrap());
    result
}

fn fixture(backend: &FakeBackend, content: &[u8]) -> SourceSnapshot {
    let id = SourceId::new("11111111-1111-4111-8111-111111111111").unwrap();
    let blob = store(backend, &id, ObjectKind::Blob, content);
    let subtree = store(
        backend,
        &id,
        ObjectKind::Tree,
        &raw_entry("100644", b"file.rs", &blob),
    );
    let link = store(backend, &id, ObjectKind::Blob, b"src/file.rs");
    let mut tree = raw_entry("120000", b"link", &link);
    tree.extend(raw_entry("100755", b"run.sh", &blob));
    tree.extend(raw_entry("40000", b"src", &subtree));
    let root = store(backend, &id, ObjectKind::Tree, &tree);
    let commit = format!("tree {root}\nauthor Fixture <fixture@example.test> 0 +0000\ncommitter Fixture <fixture@example.test> 0 +0000\n\nfixture\n");
    SourceSnapshot {
        source_id: id,
        scope_path: RepoPath::new("/project/a").unwrap(),
        object_format: ObjectFormat::Sha1,
        commit_oid: git_oid("commit", commit.as_bytes()),
        root_tree_oid: root,
    }
}

fn reader(source: &SourceSnapshot, backend: &Arc<FakeBackend>) -> SourceReader {
    SourceReader::new(source.clone(), backend.clone(), ReadLimits::default()).unwrap()
}

fn path(relative: &str) -> RepoPath {
    RepoPath::new(format!("/project/a/{relative}")).unwrap()
}

#[tokio::test]
async fn first_lazy_read_after_new_revision_still_reads_old_objects() {
    let backend = Arc::new(FakeBackend::default());
    let old = fixture(&backend, b"old");
    let old_reader = reader(&old, &backend);
    let new = fixture(&backend, b"new and longer");
    assert_ne!(old.commit_oid, new.commit_oid);
    assert!(backend.calls.lock().unwrap().is_empty());
    assert_eq!(
        old_reader
            .read_file(&path("src/file.rs"))
            .await
            .unwrap()
            .as_ref(),
        b"old"
    );
    assert!(backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|(source, _, _, _)| source == &old));
    assert_eq!(
        backend
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, _, path)| path.as_str())
            .collect::<Vec<_>>(),
        ["", "src", "src/file.rs"]
    );
    assert_eq!(
        reader(&new, &backend)
            .read_file(&path("src/file.rs"))
            .await
            .unwrap()
            .as_ref(),
        b"new and longer"
    );
    assert_eq!(
        old_reader
            .read_file(&path("src/file.rs"))
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn native_scope_is_not_applied_twice_and_prefix_neighbors_are_outside() {
    let backend = Arc::new(FakeBackend::default());
    let source = fixture(&backend, b"hello");
    let reader = reader(&source, &backend);
    let absolute = reader.lookup(&path("src/file.rs")).await.unwrap();
    let relative = reader
        .lookup_relative(&RelativePath::new("src/file.rs").unwrap())
        .await
        .unwrap();
    assert_eq!(absolute, relative);
    assert!(matches!(
        reader
            .lookup(&RepoPath::new("/project/ab/src/file.rs").unwrap())
            .await,
        Err(SnapshotReadError::OutsideScope)
    ));
    assert!(matches!(
        reader
            .lookup_relative(&RelativePath::new("project/a/src/file.rs").unwrap())
            .await,
        Err(SnapshotReadError::PathNotFound)
    ));
}

#[tokio::test]
async fn modes_and_symlink_target_are_preserved_without_implicit_traversal() {
    let backend = Arc::new(FakeBackend::default());
    let source = fixture(&backend, b"#!/bin/sh\n");
    let reader = reader(&source, &backend);
    reader.verify_root().await.unwrap();
    let entries = reader.list_dir(&source.scope_path).await.unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry.kind))
            .collect::<Vec<_>>(),
        [
            ("link", EntryKind::Symlink),
            ("run.sh", EntryKind::Executable),
            ("src", EntryKind::Directory)
        ]
    );
    assert_eq!(
        reader.read_link(&path("link")).await.unwrap().as_ref(),
        b"src/file.rs"
    );
    assert!(matches!(
        reader.read_file(&path("link")).await,
        Err(SnapshotReadError::NotFile)
    ));
    assert!(matches!(
        reader.lookup(&path("link/child")).await,
        Err(SnapshotReadError::NotDirectory)
    ));
}

#[tokio::test]
async fn failures_do_not_become_missing_paths_or_empty_successes() {
    let backend = Arc::new(FakeBackend::default());
    let source = fixture(&backend, b"hello");
    let reader = reader(&source, &backend);
    assert!(matches!(
        reader.lookup(&path("absent")).await,
        Err(SnapshotReadError::PathNotFound)
    ));
    backend.failure.store(1, Ordering::SeqCst);
    assert!(matches!(
        reader.read_file(&path("src/file.rs")).await,
        Err(SnapshotReadError::Forbidden)
    ));
    backend.failure.store(2, Ordering::SeqCst);
    assert!(matches!(
        reader.read_file(&path("src/file.rs")).await,
        Err(SnapshotReadError::Expired)
    ));
    backend.failure.store(3, Ordering::SeqCst);
    assert!(matches!(
        reader.read_file(&path("src/file.rs")).await,
        Err(SnapshotReadError::Unavailable(_))
    ));
    backend.failure.store(0, Ordering::SeqCst);
    backend.objects.lock().unwrap().remove(&(
        source.source_id.clone(),
        ObjectKind::Tree,
        source.root_tree_oid.clone(),
    ));
    assert!(matches!(
        reader.list_dir(&source.scope_path).await,
        Err(SnapshotReadError::Unavailable(_))
    ));
}

#[tokio::test]
async fn bad_bytes_are_rejected_and_git_like_file_prefix_is_preserved() {
    let backend = Arc::new(FakeBackend::default());
    let content = b"blob 3\0abc";
    let source = fixture(&backend, content);
    let reader = reader(&source, &backend);
    assert_eq!(
        reader
            .read_file(&path("src/file.rs"))
            .await
            .unwrap()
            .as_ref(),
        content
    );
    let entry = reader.lookup(&path("src/file.rs")).await.unwrap();
    assert!(matches!(
        verify_object(ObjectKind::Tree, &entry.oid, content),
        Err(SnapshotReadError::Integrity { .. })
    ));
    backend.objects.lock().unwrap().insert(
        (source.source_id.clone(), ObjectKind::Blob, entry.oid),
        Bytes::from_static(b"wrong"),
    );
    assert!(matches!(
        reader.read_file(&path("src/file.rs")).await,
        Err(SnapshotReadError::Integrity {
            kind: ObjectKind::Blob,
            ..
        })
    ));
}

#[tokio::test]
async fn zero_length_and_byte_limits_are_explicit() {
    let backend = Arc::new(FakeBackend::default());
    let empty = fixture(&backend, b"");
    assert!(reader(&empty, &backend)
        .read_file(&path("src/file.rs"))
        .await
        .unwrap()
        .is_empty());
    let source = fixture(&backend, b"more than four bytes");
    let limited = SourceReader::new(
        source,
        backend,
        ReadLimits {
            tree_bytes: 4096,
            blob_bytes: 4,
        },
    )
    .unwrap();
    assert!(matches!(
        limited.read_file(&path("src/file.rs")).await,
        Err(SnapshotReadError::ObjectTooLarge { limit: 4 })
    ));
}

#[tokio::test]
async fn same_oid_in_another_source_does_not_bypass_source_membership() {
    let backend = Arc::new(FakeBackend::default());
    let source = fixture(&backend, b"private");
    let mut other = source.clone();
    other.source_id = SourceId::new("22222222-2222-4222-8222-222222222222").unwrap();
    // A second source receives the trees, but has no authorized copy of the blob.
    let trees = backend
        .objects
        .lock()
        .unwrap()
        .iter()
        .filter(|((_, kind, _), _)| *kind == ObjectKind::Tree)
        .map(|((_, kind, oid), bytes)| {
            ((other.source_id.clone(), *kind, oid.clone()), bytes.clone())
        })
        .collect::<Vec<_>>();
    backend.objects.lock().unwrap().extend(trees);
    assert!(matches!(
        reader(&other, &backend)
            .read_file(&path("src/file.rs"))
            .await,
        Err(SnapshotReadError::Unavailable(_))
    ));
    assert_eq!(
        reader(&source, &backend)
            .read_file(&path("src/file.rs"))
            .await
            .unwrap()
            .as_ref(),
        b"private"
    );
}

#[test]
fn malformed_tree_names_and_truncated_objects_are_rejected() {
    let oid = ObjectId::new("1".repeat(40)).unwrap();
    for name in [b"".as_slice(), b".", b"..", b"a/b"] {
        assert!(matches!(
            decode_tree(&raw_entry("100644", name, &oid)),
            Err(SnapshotReadError::MalformedTree(_))
        ));
    }
    assert!(matches!(
        decode_tree(&raw_entry("100644", &[255], &oid)),
        Err(SnapshotReadError::Unsupported(_))
    ));
    let mut duplicate = raw_entry("100644", b"same", &oid);
    duplicate.extend(raw_entry("40000", b"same", &oid));
    assert!(matches!(
        decode_tree(&duplicate),
        Err(SnapshotReadError::MalformedTree("duplicate filename"))
    ));
    let mut truncated = raw_entry("100644", b"file", &oid);
    truncated.pop();
    assert!(matches!(
        decode_tree(&truncated),
        Err(SnapshotReadError::MalformedTree(_))
    ));
}
