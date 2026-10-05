//! Compile this as an external consumer of the original five-field public DTO.

use scorpiofs::snapshot::types::LookupNode;
use serde_json::json;

#[test]
fn legacy_struct_literals_and_exhaustive_patterns_still_compile() {
    let node = LookupNode {
        fs_kind: "directory".into(),
        name: None,
        size: None,
        content_digest: None,
        directory_root: Some(format!("sha256:{}", "aa".repeat(32))),
    };
    let LookupNode {
        fs_kind,
        name,
        size,
        content_digest,
        directory_root,
    } = node.clone();
    assert_eq!(fs_kind, "directory");
    assert!(name.is_none() && size.is_none() && content_digest.is_none());
    assert_eq!(directory_root, node.directory_root);
}

#[test]
fn private_wire_hints_remain_checked_when_deserializing_the_public_dto() {
    let base =
        json!({"fs_kind":"directory", "directory_root":format!("sha256:{}", "aa".repeat(32))});
    for class in [
        "native_tree",
        "native_checkout_root",
        "import_root",
        "import_tree",
        "aggregate",
    ] {
        for lifecycle in ["mutable", "immutable_release"] {
            let mut wire = base.clone();
            wire["node_class"] = json!(class);
            wire["lifecycle"] = json!(lifecycle);
            let node: LookupNode = serde_json::from_value(wire).unwrap();
            assert_eq!(node.fs_kind, "directory");
        }
    }
    for (field, value) in [
        ("node_class", json!("unknown")),
        ("node_class", json!(null)),
        ("node_class", json!(false)),
        ("lifecycle", json!("deleted")),
        ("lifecycle", json!(null)),
        ("lifecycle", json!(1)),
    ] {
        let mut wire = base.clone();
        wire[field] = value;
        assert!(serde_json::from_value::<LookupNode>(wire).is_err());
    }
    for kind in ["regular", "executable", "symlink"] {
        let wire = json!({"fs_kind":kind, "name":"a", "size":"1", "content_digest":format!("sha256:{}", "aa".repeat(32)), "node_class":"native_tree"});
        assert!(serde_json::from_value::<LookupNode>(wire).is_err());
    }
}
