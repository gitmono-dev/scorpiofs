//! A fixed-source read boundary. Backend adapters must authorize every request
//! against this descriptor. Paths are root-relative membership proofs, never
//! lookups through a moving ref or the current namespace registry.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

use super::identity::{ObjectId, RelativePath, RepoPath, SourceSnapshot, MAX_COMPONENT_BYTES};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    Tree,
    Blob,
}

impl ObjectKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tree => "tree",
            Self::Blob => "blob",
        }
    }
}

#[derive(Debug, Error)]
pub enum SnapshotReadError {
    #[error("path is outside the resolved source scope")]
    OutsideScope,
    #[error("path does not exist in the fixed source")]
    PathNotFound,
    #[error("expected a directory")]
    NotDirectory,
    #[error("expected a regular file")]
    NotFile,
    #[error("expected a symlink")]
    NotSymlink,
    #[error("source access denied")]
    Forbidden,
    #[error("snapshot retention lease expired")]
    Expired,
    #[error("object is unavailable: {0}")]
    Unavailable(String),
    #[error("Git {kind:?} hash mismatch for {oid}")]
    Integrity { kind: ObjectKind, oid: ObjectId },
    #[error("malformed Git tree: {0}")]
    MalformedTree(&'static str),
    #[error("unsupported snapshot entry: {0}")]
    Unsupported(&'static str),
    #[error("object exceeds configured byte limit {limit}")]
    ObjectTooLarge { limit: usize },
    #[error("object limits must be nonzero")]
    InvalidLimits,
}

#[async_trait]
pub trait ObjectBackend: Send + Sync {
    /// Return raw object payload (no Git header). An implementation must enforce
    /// max_bytes while receiving/allocating bytes, not merely after download.
    /// source also identifies authorization/scope/retention context: a global
    /// physical CAS hit must not bypass that check. No latest fallback is allowed.
    /// source_path must resolve to kind/OID beneath source.root_tree_oid. This
    /// permits a bounded-depth server membership/ACL check without enumerating
    /// the entire reachable object graph or trusting an arbitrary bare OID.
    async fn fetch(
        &self,
        source: &SourceSnapshot,
        kind: ObjectKind,
        oid: &ObjectId,
        source_path: &RelativePath,
        max_bytes: usize,
    ) -> Result<Bytes, SnapshotReadError>;
}

#[derive(Debug, Clone, Copy)]
pub struct ReadLimits {
    pub tree_bytes: usize,
    pub blob_bytes: usize,
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            tree_bytes: 16 * 1024 * 1024,
            blob_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Directory,
    File,
    Executable,
    Symlink,
    Gitlink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    pub name: String,
    pub kind: EntryKind,
    pub oid: ObjectId,
}

/// One immutable descriptor for this reader's entire lifetime. A caller must
/// resolve/attest/pin it before exposing a mount; this type doesn't publish or
/// acquire leases on its own. Updates construct another reader.
pub struct SourceReader {
    source: SourceSnapshot,
    backend: Arc<dyn ObjectBackend>,
    limits: ReadLimits,
}

impl SourceReader {
    pub fn new(
        source: SourceSnapshot,
        backend: Arc<dyn ObjectBackend>,
        limits: ReadLimits,
    ) -> Result<Self, SnapshotReadError> {
        if limits.tree_bytes == 0 || limits.blob_bytes == 0 {
            return Err(SnapshotReadError::InvalidLimits);
        }
        Ok(Self {
            source,
            backend,
            limits,
        })
    }

    pub fn source(&self) -> &SourceSnapshot {
        &self.source
    }

    async fn object(
        &self,
        kind: ObjectKind,
        oid: &ObjectId,
        source_path: &RelativePath,
    ) -> Result<Bytes, SnapshotReadError> {
        let limit = match kind {
            ObjectKind::Tree => self.limits.tree_bytes,
            ObjectKind::Blob => self.limits.blob_bytes,
        };
        let bytes = self
            .backend
            .fetch(&self.source, kind, oid, source_path, limit)
            .await?;
        if bytes.len() > limit {
            return Err(SnapshotReadError::ObjectTooLarge { limit });
        }
        verify_object(kind, oid, &bytes)?;
        Ok(bytes)
    }

    async fn tree(
        &self,
        oid: &ObjectId,
        source_path: &RelativePath,
    ) -> Result<Vec<TreeEntry>, SnapshotReadError> {
        let bytes = self.object(ObjectKind::Tree, oid, source_path).await?;
        decode_tree(&bytes)
    }

    pub async fn verify_root(&self) -> Result<(), SnapshotReadError> {
        self.tree(
            &self.source.root_tree_oid,
            &RelativePath::new("").expect("valid root path"),
        )
        .await
        .map(|_| ())
    }

    pub async fn lookup(&self, path: &RepoPath) -> Result<TreeEntry, SnapshotReadError> {
        let relative = path
            .relative_to(&self.source.scope_path)
            .ok_or(SnapshotReadError::OutsideScope)?;
        self.lookup_relative(&relative).await
    }

    pub async fn lookup_relative(
        &self,
        path: &RelativePath,
    ) -> Result<TreeEntry, SnapshotReadError> {
        let mut entry = TreeEntry {
            name: String::new(),
            kind: EntryKind::Directory,
            oid: self.source.root_tree_oid.clone(),
        };
        if path.as_str().is_empty() {
            return Ok(entry);
        }
        let mut walked = String::new();
        for name in path.as_str().split('/') {
            if entry.kind != EntryKind::Directory {
                return Err(if entry.kind == EntryKind::Gitlink {
                    SnapshotReadError::Unsupported("submodule traversal")
                } else {
                    SnapshotReadError::NotDirectory
                });
            }
            entry = self
                .tree(
                    &entry.oid,
                    &RelativePath::new(&walked).expect("prefix of a validated path"),
                )
                .await?
                .into_iter()
                .find(|entry| entry.name == name)
                .ok_or(SnapshotReadError::PathNotFound)?;
            if !walked.is_empty() {
                walked.push('/');
            }
            walked.push_str(name);
        }
        Ok(entry)
    }

    pub async fn list_dir(&self, path: &RepoPath) -> Result<Vec<TreeEntry>, SnapshotReadError> {
        let entry = self.lookup(path).await?;
        if entry.kind != EntryKind::Directory {
            return Err(SnapshotReadError::NotDirectory);
        }
        let relative = path
            .relative_to(&self.source.scope_path)
            .ok_or(SnapshotReadError::OutsideScope)?;
        self.tree(&entry.oid, &relative).await
    }

    /// This initial, bounded whole-object reader provides exact length through
    /// returned bytes. It never reports unknown stat size as zero; large-object
    /// streaming and an authenticated size-index adapter are separate work.
    pub async fn read_file(&self, path: &RepoPath) -> Result<Bytes, SnapshotReadError> {
        let entry = self.lookup(path).await?;
        match entry.kind {
            EntryKind::File | EntryKind::Executable => {
                let relative = path
                    .relative_to(&self.source.scope_path)
                    .ok_or(SnapshotReadError::OutsideScope)?;
                self.object(ObjectKind::Blob, &entry.oid, &relative).await
            }
            EntryKind::Gitlink => Err(SnapshotReadError::Unsupported("submodule hydration")),
            _ => Err(SnapshotReadError::NotFile),
        }
    }

    /// Return the raw symlink target without following it through live routing.
    pub async fn read_link(&self, path: &RepoPath) -> Result<Bytes, SnapshotReadError> {
        let entry = self.lookup(path).await?;
        if entry.kind != EntryKind::Symlink {
            return Err(SnapshotReadError::NotSymlink);
        }
        let relative = path
            .relative_to(&self.source.scope_path)
            .ok_or(SnapshotReadError::OutsideScope)?;
        self.object(ObjectKind::Blob, &entry.oid, &relative).await
    }
}

pub fn verify_object(
    kind: ObjectKind,
    oid: &ObjectId,
    bytes: &[u8],
) -> Result<(), SnapshotReadError> {
    // SHA-1 is for Git compatibility, never for authentication. Select the
    // algorithm explicitly rather than using git-internal's thread-local mode.
    let mut digest = ring::digest::Context::new(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY);
    digest.update(format!("{} {}\0", kind.as_str(), bytes.len()).as_bytes());
    digest.update(bytes);
    if hex::encode(digest.finish().as_ref()) != oid.as_str() {
        return Err(SnapshotReadError::Integrity {
            kind,
            oid: oid.clone(),
        });
    }
    Ok(())
}

fn decode_tree(mut bytes: &[u8]) -> Result<Vec<TreeEntry>, SnapshotReadError> {
    let mut entries = Vec::new();
    let mut names = HashSet::new();
    while !bytes.is_empty() {
        let space = bytes
            .iter()
            .position(|&b| b == b' ')
            .ok_or(SnapshotReadError::MalformedTree("missing mode separator"))?;
        let kind = match &bytes[..space] {
            b"40000" | b"040000" => EntryKind::Directory,
            b"100644" => EntryKind::File,
            b"100755" => EntryKind::Executable,
            b"120000" => EntryKind::Symlink,
            b"160000" => EntryKind::Gitlink,
            _ => return Err(SnapshotReadError::Unsupported("Git tree mode")),
        };
        bytes = &bytes[space + 1..];
        let nul = bytes
            .iter()
            .position(|&b| b == 0)
            .ok_or(SnapshotReadError::MalformedTree("missing name terminator"))?;
        let name = std::str::from_utf8(&bytes[..nul])
            .map_err(|_| SnapshotReadError::Unsupported("non-UTF-8 filename"))?;
        if name.is_empty()
            || name == "."
            || name == ".."
            || name.contains('/')
            || name.len() > MAX_COMPONENT_BYTES
        {
            return Err(SnapshotReadError::MalformedTree("invalid filename"));
        }
        if !names.insert(name.to_owned()) {
            return Err(SnapshotReadError::MalformedTree("duplicate filename"));
        }
        bytes = &bytes[nul + 1..];
        let raw_oid = bytes
            .get(..20)
            .ok_or(SnapshotReadError::MalformedTree("truncated SHA-1"))?;
        let oid = ObjectId::new(hex::encode(raw_oid)).expect("20 bytes produce a valid SHA-1 ID");
        entries.push(TreeEntry {
            name: name.to_owned(),
            kind,
            oid,
        });
        bytes = &bytes[20..];
    }
    // Directory iteration order is stable even if the source tree uses Git's
    // directory-slash ordering rather than bytewise filename ordering.
    entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
    Ok(entries)
}

#[cfg(test)]
mod tests;
