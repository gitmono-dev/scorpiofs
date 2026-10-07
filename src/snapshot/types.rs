//! MST/2 snapshot client DTOs and errors (spec 03/04).

use serde::{Deserialize, Serialize};

/// Resolve selects one namespace publication; later requests use its snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResolveTarget {
    Latest,
    View { view_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolveDelivery {
    Full,
    Lazy,
}

/// Explicit resolve semantics, including the requested completion scope.
#[derive(Debug, Clone, Serialize)]
pub struct ResolveRequest {
    pub target: ResolveTarget,
    pub scope: String,
    pub delivery: ResolveDelivery,
    pub lease_seconds: u64,
}

impl ResolveRequest {
    pub fn latest(scope: impl Into<String>, lease_seconds: u64) -> Self {
        Self {
            target: ResolveTarget::Latest,
            scope: scope.into(),
            delivery: ResolveDelivery::Full,
            lease_seconds,
        }
    }
}

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
    /// Malformed request (spec 14 §5 INVALID_REQUEST).
    InvalidRequest,
    /// Over a spec 14 §4 hard limit (body bytes, item counts).
    LimitExceeded,
    /// Missing or invalid credentials (spec 04 §1).
    Unauthenticated,
    ScopeForbidden,
    ViewNotFound,
    SnapshotNotReady,
    /// Fixed-view metadata is temporarily unavailable (METADATA_NOT_READY, 503).
    MetadataNotReady,
    /// The fixed view no longer exists (spec 14 §5 SNAPSHOT_GONE, 410).
    SnapshotGone,
    PathNotFound,
    NotDirectory,
    UnsupportedEntry,
    LeaseUnknown,
    LeaseExpired,
    CursorInvalid,
    CursorStale,
    ProofBudgetExceeded,
    DigestMismatch,
    /// A retained object is missing or violates the verified storage contract.
    IntegrityError,
    ObjectUnavailable,
    RangeNotSupported,
    SymlinkTraversal,
    /// A durable local store already holds a different fixed view; hydration
    /// refuses rather than mixing two views in one store.
    DurableViewConflict,
    /// A transport failure or temporary backend outage; retry within the
    /// still-valid fixed view rather than permanently rejecting the reader.
    TemporaryUnavailable,
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
            "INVALID_REQUEST" => Self::InvalidRequest,
            "LIMIT_EXCEEDED" => Self::LimitExceeded,
            "UNAUTHENTICATED" => Self::Unauthenticated,
            "SCOPE_FORBIDDEN" => Self::ScopeForbidden,
            "VIEW_NOT_FOUND" => Self::ViewNotFound,
            "SNAPSHOT_NOT_READY" => Self::SnapshotNotReady,
            "METADATA_NOT_READY" => Self::MetadataNotReady,
            "SNAPSHOT_GONE" => Self::SnapshotGone,
            "PATH_NOT_FOUND" => Self::PathNotFound,
            "NOT_DIRECTORY" => Self::NotDirectory,
            "UNSUPPORTED_ENTRY" => Self::UnsupportedEntry,
            "LEASE_UNKNOWN" => Self::LeaseUnknown,
            "LEASE_EXPIRED" => Self::LeaseExpired,
            "CURSOR_INVALID" => Self::CursorInvalid,
            "CURSOR_STALE" => Self::CursorStale,
            "PROOF_BUDGET_EXCEEDED" => Self::ProofBudgetExceeded,
            "EXPECTED_DIGEST_MISMATCH" => Self::DigestMismatch,
            "INTEGRITY_ERROR" => Self::IntegrityError,
            "OBJECT_UNAVAILABLE" => Self::ObjectUnavailable,
            // Pre-0.3 server builds used this spelling.
            "OBJECT_DIGEST_MISMATCH" => Self::DigestMismatch,
            "RANGE_NOT_SUPPORTED" => Self::RangeNotSupported,
            "SYMLINK_TRAVERSAL" => Self::SymlinkTraversal,
            "TEMPORARY_UNAVAILABLE" => Self::TemporaryUnavailable,
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
    pub metadata_pages: bool,
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
    /// Policy generation returned by resolve. Missing generations cannot
    /// establish an authorized cache domain.
    #[serde(default)]
    pub authorization_epoch: String,
    /// Optional server-issued authority for a trusted local export.  The
    /// response parser validates its shape, while callers must still verify
    /// the actor/domain binding and expiry before opening a local store.
    #[serde(default)]
    pub offline_grant: Option<OfflineGrant>,
}

/// Closed offline-export capability returned by canonical resolve.
///
/// Possessing a complete local CAS is not an authorization grant.  This DTO
/// is persisted only when it came from an authenticated resolve response and
/// must be checked against the fixed snapshot, actor domain and expiry on
/// every offline reopen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfflineGrant {
    pub grant_id: String,
    pub snapshot_id: String,
    pub actor_domain_id: String,
    pub expires_at: String,
    pub policy: String,
}

impl OfflineGrant {
    pub fn grant_id(&self) -> &str {
        &self.grant_id
    }

    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    pub fn actor_domain_id(&self) -> &str {
        &self.actor_domain_id
    }

    pub fn expires_at(&self) -> &str {
        &self.expires_at
    }

    pub fn policy(&self) -> &str {
        &self.policy
    }

    /// Validate the fixed-view binding carried by a grant.  The actor domain
    /// is optional here because the resolve layer has not yet opened the
    /// local cache; offline reopen passes the concrete cache/mount domain.
    pub(crate) fn validate_for(
        &self,
        snapshot_id: &str,
        actor_domain_id: Option<&str>,
    ) -> Result<(), SnapshotError> {
        if self.grant_id.is_empty()
            || self.grant_id.chars().count() > 512
            || self.actor_domain_id.is_empty()
            || self.actor_domain_id.chars().count() > 512
            || self.snapshot_id != snapshot_id
            || actor_domain_id.is_some_and(|expected| expected != self.actor_domain_id)
            || self.policy != "trusted_local_export_v1"
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeForbidden,
                "offline grant is bound to a different snapshot or actor domain",
            ));
        }
        crate::snapshot::resolve_wire::timestamp(&self.expires_at).map_err(|_| {
            SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "offline grant expiry is not RFC3339",
            )
        })?;
        Ok(())
    }

    pub(crate) fn expired(&self) -> bool {
        crate::snapshot::resolve_wire::timestamp(&self.expires_at)
            .map(|expiry| expiry <= time::OffsetDateTime::now_utc())
            .unwrap_or(true)
    }
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
    #[serde(deserialize_with = "required_nullable_string")]
    pub range_start_exclusive: Option<String>,
    pub entries: Vec<DirEntry>,
    pub entry_count: String,
    #[serde(deserialize_with = "required_nullable_string")]
    pub next_cursor: Option<String>,
    pub proof_pages: Vec<ProofPage>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub fs_kind: String,
    #[serde(default, deserialize_with = "optional_nonnull_string")]
    pub size: Option<String>,
    #[serde(default, deserialize_with = "optional_nonnull_string")]
    pub content_digest: Option<String>,
    #[serde(default, deserialize_with = "optional_nonnull_string")]
    pub directory_root: Option<String>,
}

fn required_nullable_string<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(deserializer)
}

fn optional_nonnull_string<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some)
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
    #[serde(default, deserialize_with = "optional_nonnull_lookup_node")]
    pub node: Option<LookupNode>,
}

fn optional_nonnull_lookup_node<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<LookupNode>, D::Error> {
    LookupNode::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone)]
pub struct LookupNode {
    pub fs_kind: String,
    pub name: Option<String>,
    pub size: Option<String>,
    pub content_digest: Option<String>,
    pub directory_root: Option<String>,
}

// Preserve LookupNode's original public struct-literal API. Provenance hints
// belong to the wire representation and are validated before being discarded.
#[derive(Deserialize)]
struct LookupNodeWire {
    fs_kind: String,
    #[serde(default, deserialize_with = "optional_nonnull_string")]
    name: Option<String>,
    #[serde(default, deserialize_with = "optional_nonnull_string")]
    size: Option<String>,
    #[serde(default, deserialize_with = "optional_nonnull_string")]
    content_digest: Option<String>,
    #[serde(default, deserialize_with = "optional_nonnull_string")]
    directory_root: Option<String>,
    #[serde(default, deserialize_with = "optional_nonnull_string")]
    node_class: Option<String>,
    #[serde(default, deserialize_with = "optional_nonnull_string")]
    lifecycle: Option<String>,
}

impl<'de> Deserialize<'de> for LookupNode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = LookupNodeWire::deserialize(deserializer)?;
        if wire.node_class.as_deref().is_some_and(|class| {
            wire.fs_kind != "directory"
                || !matches!(
                    class,
                    "native_tree"
                        | "native_checkout_root"
                        | "import_root"
                        | "import_tree"
                        | "aggregate"
                )
        }) || wire
            .lifecycle
            .as_deref()
            .is_some_and(|value| !matches!(value, "mutable" | "immutable_release"))
        {
            return Err(serde::de::Error::custom(
                "invalid lookup node provenance fields",
            ));
        }
        Ok(Self {
            fs_kind: wire.fs_kind,
            name: wire.name,
            size: wire.size,
            content_digest: wire.content_digest,
            directory_root: wire.directory_root,
        })
    }
}
