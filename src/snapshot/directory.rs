//! JSON directory syntax and pagination continuity. These checks do not
//! establish MTP2 membership or prove that a claimed EOF is cryptographic EOF.

use super::{
    content_profile::MAX_FILE_SIZE,
    frames::parse_count,
    types::{DirectoryResponse, SnapshotError, SnapshotErrorCode},
};

pub(crate) fn integrity() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::IntegrityError,
        "directory response is inconsistent with its fixed request or pagination",
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

fn directory_digest_valid(digest: &str) -> bool {
    digest_valid(digest) && digest.as_bytes()[7..].iter().any(|byte| *byte != b'0')
}

pub(crate) fn validate_page(
    page: &DirectoryResponse,
    sid: &str,
    path: &str,
    limit: u32,
    cursor: Option<&str>,
) -> Result<(), SnapshotError> {
    let count = parse_count(&page.entry_count, "directory entry_count")?;
    if page.snapshot_id != sid
        || page.path != path
        || page.entries.len() > limit as usize
        || count < page.entries.len() as u64
        || !directory_digest_valid(&page.metadata_root)
        || !directory_digest_valid(&page.directory_root)
        || (!matches!(
            page.node_class.as_str(),
            "native_tree" | "native_checkout_root" | "import_root" | "import_tree" | "aggregate"
        ))
        || !matches!(page.lifecycle.as_str(), "mutable" | "immutable_release")
        || (cursor.is_none() && page.range_start_exclusive.is_some())
        || page
            .range_start_exclusive
            .as_deref()
            .is_some_and(|name| !name_valid(name))
        || page
            .next_cursor
            .as_deref()
            .is_some_and(|next| next.is_empty() || Some(next) == cursor || page.entries.is_empty())
    {
        return Err(integrity());
    }
    let mut previous = page.range_start_exclusive.as_deref();
    for entry in &page.entries {
        if !name_valid(&entry.name)
            || previous.is_some_and(|name| name.as_bytes() >= entry.name.as_bytes())
        {
            return Err(integrity());
        }
        previous = Some(&entry.name);
        match entry.fs_kind.as_str() {
            "directory" => {
                if entry.size.is_some()
                    || entry.content_digest.is_some()
                    || !entry
                        .directory_root
                        .as_deref()
                        .is_some_and(directory_digest_valid)
                {
                    return Err(integrity());
                }
            }
            "regular" | "executable" | "symlink" => {
                if entry.directory_root.is_some()
                    || !entry.content_digest.as_deref().is_some_and(digest_valid)
                {
                    return Err(integrity());
                }
                let size = parse_count(entry.size.as_deref().ok_or_else(integrity)?, "file size")?;
                let cap = if entry.fs_kind == "symlink" {
                    4095
                } else {
                    MAX_FILE_SIZE
                };
                if size > cap {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::LimitExceeded,
                        "directory file size exceeds the filesystem profile",
                    ));
                }
                if entry.fs_kind == "symlink" && size == 0 {
                    return Err(integrity());
                }
            }
            _ => return Err(integrity()),
        }
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct Progress {
    root: Option<String>,
    count: Option<u64>,
    node_class: Option<String>,
    lifecycle: Option<String>,
    seen: u64,
    last: Option<String>,
}

impl Progress {
    pub(crate) fn for_root(root: &str) -> Self {
        Self {
            root: Some(root.to_owned()),
            ..Self::default()
        }
    }

    /// Validate before exposing entries or descending into a child directory.
    pub(crate) fn accept(
        &mut self,
        page: &DirectoryResponse,
        metadata_root: &str,
    ) -> Result<(), SnapshotError> {
        let count = parse_count(&page.entry_count, "directory entry_count")?;
        let seen = self
            .seen
            .checked_add(page.entries.len() as u64)
            .ok_or_else(integrity)?;
        if page.metadata_root != metadata_root
            || (page.path == "/" && page.directory_root != metadata_root)
            || self
                .root
                .as_ref()
                .is_some_and(|root| root != &page.directory_root)
            || self.count.is_some_and(|fixed| fixed != count)
            || self
                .node_class
                .as_ref()
                .is_some_and(|fixed| fixed != &page.node_class)
            || self
                .lifecycle
                .as_ref()
                .is_some_and(|fixed| fixed != &page.lifecycle)
            || page.range_start_exclusive != self.last
            || seen > count
            || (page.next_cursor.is_none() && seen != count)
        {
            return Err(integrity());
        }
        self.root = Some(page.directory_root.clone());
        self.count = Some(count);
        self.node_class = Some(page.node_class.clone());
        self.lifecycle = Some(page.lifecycle.clone());
        self.seen = seen;
        self.last = page.entries.last().map(|entry| entry.name.clone());
        Ok(())
    }
}
