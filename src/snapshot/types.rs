//! MST/2 snapshot client DTOs and errors (spec 03/04).

use serde::Deserialize;

/// Client-side snapshot error. Mirrors the server code set so callers can
/// distinguish absence/auth/not-found from transport failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotError {
    pub code: SnapshotErrorCode,
    pub message: String,
    pub http_status: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotErrorCode {
    ScopeInvalid,
    ScopeForbidden,
    ViewNotFound,
    SnapshotNotReady,
    SnapshotUnknown,
    PathNotFound,
    NotDirectory,
    UnsupportedEntry,
    LeaseUnknown,
    LeaseExpired,
    CursorInvalid,
    CursorStale,
    ProofBudgetExceeded,
    DigestMismatch,
    RangeNotSupported,
    SymlinkTraversal,
    /// A durable local store already holds a different fixed view; hydration
    /// refuses rather than mixing two views in one store.
    DurableViewConflict,
    Internal,
}

impl SnapshotError {
    pub fn new(code: SnapshotErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            http_status: 0,
        }
    }
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for SnapshotError {}

impl SnapshotErrorCode {
    pub fn from_server(code: &str) -> Self {
        match code {
            "SCOPE_INVALID" => Self::ScopeInvalid,
            "SCOPE_FORBIDDEN" => Self::ScopeForbidden,
            "VIEW_NOT_FOUND" => Self::ViewNotFound,
            "SNAPSHOT_NOT_READY" => Self::SnapshotNotReady,
            "SNAPSHOT_UNKNOWN" => Self::SnapshotUnknown,
            "PATH_NOT_FOUND" => Self::PathNotFound,
            "NOT_DIRECTORY" => Self::NotDirectory,
            "UNSUPPORTED_ENTRY" => Self::UnsupportedEntry,
            "LEASE_UNKNOWN" => Self::LeaseUnknown,
            "LEASE_EXPIRED" => Self::LeaseExpired,
            "CURSOR_INVALID" => Self::CursorInvalid,
            "CURSOR_STALE" => Self::CursorStale,
            "PROOF_BUDGET_EXCEEDED" => Self::ProofBudgetExceeded,
            "OBJECT_DIGEST_MISMATCH" => Self::DigestMismatch,
            "RANGE_NOT_SUPPORTED" => Self::RangeNotSupported,
            "SYMLINK_TRAVERSAL" => Self::SymlinkTraversal,
            _ => Self::Internal,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Capabilities {
    pub protocol_versions: Vec<u16>,
    pub metadata_codecs: Vec<u16>,
    pub frame_encodings: Vec<String>,
    pub features: CapabilityFeatures,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CapabilityFeatures {
    pub resolve: bool,
    pub directory: bool,
    pub leases: bool,
    #[serde(default)]
    pub lookup: bool,
    #[serde(default)]
    pub raw_blob: bool,
    #[serde(default)]
    pub objects: bool,
    #[serde(default)]
    pub chunk_reads: bool,
    #[serde(default)]
    pub full_hydration: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResolveResponse {
    pub descriptor: Descriptor,
    pub lease_id: String,
    #[serde(default)]
    pub lease_expires_at: String,
    pub publication_sequence: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Descriptor {
    pub schema_version: u16,
    pub metadata_codec: u16,
    pub instance_id: String,
    pub namespace_view_id: String,
    pub scope: String,
    pub materialization_policy: u16,
    pub fs_semantics: u16,
    pub access_projection: u16,
    pub metadata_root: String,
    pub snapshot_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DirectoryResponse {
    pub snapshot_id: String,
    pub path: String,
    pub metadata_root: String,
    pub directory_root: String,
    pub node_class: String,
    pub lifecycle: String,
    pub range_start_exclusive: Option<String>,
    pub entries: Vec<DirEntry>,
    pub entry_count: String,
    pub next_cursor: Option<String>,
    pub proof_pages: Vec<ProofPage>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub fs_kind: String,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub content_digest: Option<String>,
    #[serde(default)]
    pub directory_root: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProofPage {
    pub digest: String,
    pub data_base64: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LookupResponse {
    pub snapshot_id: String,
    pub results: Vec<LookupResult>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LookupResult {
    pub path: String,
    pub status: String,
    #[serde(default)]
    pub node: Option<LookupNode>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LookupNode {
    pub fs_kind: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub content_digest: Option<String>,
    #[serde(default)]
    pub directory_root: Option<String>,
}
