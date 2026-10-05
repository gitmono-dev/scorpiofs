//! Typed namespace facts from a fixed, completely verified directory radix.

use serde::Serialize;

/// Local admission bounds, separate from discovery's per-request maxima.
#[derive(Debug, Clone, Copy)]
pub struct MetadataProofLimits {
    pub max_directory_pages: usize,
    pub max_directory_entries: usize,
    pub max_cached_nodes: usize,
}

impl Default for MetadataProofLimits {
    fn default() -> Self {
        Self {
            max_directory_pages: 4096,
            max_directory_entries: 262_144,
            max_cached_nodes: 1_048_576,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotNodeKind {
    Directory,
    Regular,
    Executable,
    Symlink,
}

/// Directory identity is its committed root; file identity includes kind
/// and size. Modes are the linux-code-v1 synthesis, never Git permissions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SnapshotNodeIdentity {
    Directory { directory_root: String },
    Regular { size: u64, content_digest: String },
    Executable { size: u64, content_digest: String },
    Symlink { size: u64, content_digest: String },
}

impl SnapshotNodeIdentity {
    pub fn kind(&self) -> SnapshotNodeKind {
        match self {
            Self::Directory { .. } => SnapshotNodeKind::Directory,
            Self::Regular { .. } => SnapshotNodeKind::Regular,
            Self::Executable { .. } => SnapshotNodeKind::Executable,
            Self::Symlink { .. } => SnapshotNodeKind::Symlink,
        }
    }

    pub fn mode(&self) -> u32 {
        match self {
            Self::Directory { .. } | Self::Executable { .. } => 0o755,
            Self::Regular { .. } => 0o644,
            Self::Symlink { .. } => 0o777,
        }
    }

    pub fn content(&self) -> Option<(u64, &str)> {
        match self {
            Self::Directory { .. } => None,
            Self::Regular {
                size,
                content_digest,
            }
            | Self::Executable {
                size,
                content_digest,
            }
            | Self::Symlink {
                size,
                content_digest,
            } => Some((*size, content_digest)),
        }
    }
}

/// Errors remain Result::Err; an error never certifies absence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", content = "identity", rename_all = "snake_case")]
pub enum SnapshotPathState {
    Present(SnapshotNodeIdentity),
    AbsentProven,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SnapshotDirectoryEntry {
    pub name: String,
    pub identity: SnapshotNodeIdentity,
}
