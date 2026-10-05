use git_internal::{
    hash::{set_hash_kind_for_test, HashKind, ObjectHash},
    internal::object::{commit::Commit, tree::Tree, ObjectTrait},
};
use scorpiofs::{
    daemon::worktree_v2::git_blob_oid,
    manager::store::{BlobFsStore, CommitStore, ModifiedStore, TreeStore},
};

// git-internal 0.8.7 serde records, using object IDs independently verified by git hash-object.
const LEGACY_TREE: &str = r#"{"id":{"Sha1":[170,169,108,237,45,154,28,142,114,197,107,37,58,14,47,231,131,147,254,183]},"tree_items":[{"mode":"Blob","id":{"Sha1":[206,1,54,37,3,11,168,219,169,6,247,86,150,127,158,156,163,148,70,74]},"name":"hello.txt"}]}"#;
const LEGACY_COMMIT: &str = r#"{"id":{"Sha1":[124,173,109,7,176,52,251,136,27,222,88,124,90,27,152,36,42,195,240,118]},"tree_id":{"Sha1":[170,169,108,237,45,154,28,142,114,197,107,37,58,14,47,231,131,147,254,183]},"parent_commit_ids":[],"author":{"signature_type":"Author","name":"Test User","email":"test@example.com","timestamp":1700000000,"timezone":"+0000"},"committer":{"signature_type":"Committer","name":"Test User","email":"test@example.com","timestamp":1700000000,"timezone":"+0000"},"message":"compatibility\n"}"#;
const TREE_OID: &str = "aaa96ced2d9a1c8e72c56b253a0e2fe78393feb7";
const COMMIT_OID: &str = "7cad6d07b034fb881bde587c5a1b98242ac3f076";

#[test]
fn legacy_git_cache_survives_dependency_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let db = sled::open(dir.path().join("cache")).unwrap();
    db.insert("tree:v2:/", LEGACY_TREE.as_bytes()).unwrap();
    db.insert("commit:v2", LEGACY_COMMIT.as_bytes()).unwrap();
    db.flush().unwrap();
    drop(db);
    // Reading existing on-disk records must remain valid even on a non-SHA1 worker.
    let _guard = set_hash_kind_for_test(HashKind::Blake3);
    let db = sled::open(dir.path().join("cache")).unwrap();
    let tree = db.get_bypath(std::path::Path::new("/")).unwrap();
    assert_eq!(tree.id.to_string(), TREE_OID);
    assert_eq!(tree.tree_items[0].name, "hello.txt");
    assert_eq!(tree.tree_items[0].id.to_string(), git_blob_oid(b"hello\n"));
    let commit = db.get_commit().unwrap();
    assert_eq!(commit.id.to_string(), COMMIT_OID);
    assert_eq!(commit.tree_id, tree.id);
    assert_eq!(commit.message, "compatibility\n");
    assert_eq!(
        serde_json::to_value(&tree).unwrap(),
        serde_json::from_str::<serde_json::Value>(LEGACY_TREE).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&commit).unwrap(),
        serde_json::from_str::<serde_json::Value>(LEGACY_COMMIT).unwrap()
    );
    let parsed_tree = Tree::from_bytes(&tree.to_data().unwrap(), tree.id).unwrap();
    assert_eq!(parsed_tree.tree_items, tree.tree_items);
    let parsed_commit = Commit::from_bytes(&commit.to_data().unwrap(), commit.id).unwrap();
    assert_eq!(parsed_commit.to_data().unwrap(), commit.to_data().unwrap());
    db.insert_tree("/".into(), tree);
    db.store_commit(commit).unwrap();
    assert_eq!(db.db_tree_list().unwrap().len(), 1);
}

#[test]
fn git_blob_hash_and_store_remain_sha1_on_other_hash_workers() {
    for kind in [HashKind::Sha256, HashKind::Blake3] {
        let _guard = set_hash_kind_for_test(kind);
        assert_eq!(
            git_blob_oid(b"hello\n"),
            "ce013625030ba8dba906f756967f9e9ca394464a"
        );
        assert_eq!(
            git_blob_oid(b""),
            "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
        );
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let db = sled::open(root.join("index")).unwrap();
        let content = b"hello\n";
        let oid = git_blob_oid(content);
        root.add_blob_to_hash(&oid, content).unwrap();
        db.add_content("hello.txt".into(), oid.as_bytes()).unwrap();
        assert_eq!(root.get_blob_by_hash(&oid).unwrap(), content);
        let blobs = root.list_blobs(&db).unwrap();
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].data, content);
        assert_eq!(
            blobs[0].id,
            ObjectHash::from_hex_for_kind(HashKind::Sha1, &oid).unwrap()
        );
    }
}
