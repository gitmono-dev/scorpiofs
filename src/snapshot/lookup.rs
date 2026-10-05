//! Conditional JSON lookup node fields. These checks do not prove membership
//! or absence against the descriptor's metadata root.

use super::{
    frames::parse_count,
    range::MAX_FILE_SIZE,
    types::{LookupNode, SnapshotError, SnapshotErrorCode},
};

fn integrity() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::IntegrityError,
        "lookup node fields are inconsistent with the requested path or filesystem kind",
    )
}

fn name_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !matches!(name, "." | "..")
        && !name.contains(['/', '\0'])
}

fn digest_valid(digest: &str) -> bool {
    digest.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

pub(crate) fn validate_node(node: &LookupNode, path: &str) -> Result<(), SnapshotError> {
    if path == "/" {
        if node.name.is_some() || node.fs_kind != "directory" {
            return Err(integrity());
        }
    } else if !node
        .name
        .as_deref()
        .is_some_and(|name| name_valid(name) && Some(name) == path.rsplit('/').next())
    {
        return Err(integrity());
    }
    if node.node_class.as_deref().is_some_and(|class| {
        !matches!(
            class,
            "native_tree" | "native_checkout_root" | "import_root" | "import_tree" | "aggregate"
        )
    }) || node
        .lifecycle
        .as_deref()
        .is_some_and(|value| !matches!(value, "mutable" | "immutable_release"))
    {
        return Err(integrity());
    }
    match node.fs_kind.as_str() {
        "directory" => {
            if node.size.is_some()
                || node.content_digest.is_some()
                || !node.directory_root.as_deref().is_some_and(|digest| {
                    digest_valid(digest) && digest.as_bytes()[7..].iter().any(|byte| *byte != b'0')
                })
            {
                return Err(integrity());
            }
        }
        "regular" | "executable" | "symlink" => {
            if node.directory_root.is_some()
                || node.node_class.is_some()
                || !node.content_digest.as_deref().is_some_and(digest_valid)
            {
                return Err(integrity());
            }
            let size = parse_count(node.size.as_deref().ok_or_else(integrity)?, "lookup size")?;
            let cap = if node.fs_kind == "symlink" {
                4095
            } else {
                MAX_FILE_SIZE
            };
            if size > cap {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::LimitExceeded,
                    "lookup file size exceeds the filesystem profile",
                ));
            }
            if node.fs_kind == "symlink" && size == 0 {
                return Err(integrity());
            }
        }
        _ => return Err(integrity()),
    }
    Ok(())
}
