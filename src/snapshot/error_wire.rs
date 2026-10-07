//! Closed canonical errors and the explicit existing deployment contract.

use serde::Deserialize;
use serde_json::Value;

use super::{SnapshotError, SnapshotErrorCode};

/// SPEC error types for explicit canonical-envelope consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CanonicalSnapshotErrorCode {
    PathNotFound,
    NotDirectory,
    NotFile,
    SymlinkTraversal,
    Unauthenticated,
    ScopeForbidden,
    LeaseExpired,
    SnapshotGone,
    SnapshotNotReady,
    MetadataNotReady,
    ObjectUnavailable,
    IntegrityError,
    CursorStale,
    ExpectedDigestMismatch,
    MixedSourceBatch,
    UnsupportedEntry,
    UnsupportedCodec,
    ObjectTooLarge,
    LimitExceeded,
    ProofBudgetExceeded,
    InvalidRequest,
    RangeNotSupported,
    NamespaceConflict,
    PublicationConflict,
    ReleaseImmutable,
    RateLimited,
    TemporaryUnavailable,
    /// Compatibility spelling emitted by deployed servers for an otherwise
    /// retryable internal failure.
    Internal,
}

impl CanonicalSnapshotErrorCode {
    fn from_wire(code: &str) -> Option<Self> {
        Some(match code {
            "PATH_NOT_FOUND" => Self::PathNotFound,
            "NOT_DIRECTORY" => Self::NotDirectory,
            "NOT_FILE" => Self::NotFile,
            "SYMLINK_TRAVERSAL" => Self::SymlinkTraversal,
            "UNAUTHENTICATED" => Self::Unauthenticated,
            "SCOPE_FORBIDDEN" => Self::ScopeForbidden,
            "LEASE_EXPIRED" => Self::LeaseExpired,
            "SNAPSHOT_GONE" => Self::SnapshotGone,
            "SNAPSHOT_NOT_READY" => Self::SnapshotNotReady,
            "METADATA_NOT_READY" => Self::MetadataNotReady,
            "OBJECT_UNAVAILABLE" => Self::ObjectUnavailable,
            "INTEGRITY_ERROR" => Self::IntegrityError,
            "CURSOR_STALE" => Self::CursorStale,
            "EXPECTED_DIGEST_MISMATCH" => Self::ExpectedDigestMismatch,
            "MIXED_SOURCE_BATCH" => Self::MixedSourceBatch,
            "UNSUPPORTED_ENTRY" => Self::UnsupportedEntry,
            "UNSUPPORTED_CODEC" => Self::UnsupportedCodec,
            "OBJECT_TOO_LARGE" => Self::ObjectTooLarge,
            "LIMIT_EXCEEDED" => Self::LimitExceeded,
            "PROOF_BUDGET_EXCEEDED" => Self::ProofBudgetExceeded,
            "INVALID_REQUEST" => Self::InvalidRequest,
            "RANGE_NOT_SUPPORTED" => Self::RangeNotSupported,
            "NAMESPACE_CONFLICT" => Self::NamespaceConflict,
            "PUBLICATION_CONFLICT" => Self::PublicationConflict,
            "RELEASE_IMMUTABLE" => Self::ReleaseImmutable,
            "RATE_LIMITED" => Self::RateLimited,
            "TEMPORARY_UNAVAILABLE" => Self::TemporaryUnavailable,
            "INTERNAL" => Self::Internal,
            _ => return None,
        })
    }
}

/// Explicit canonical HTTP error parsing. This does not alter the legacy
/// request APIs or their error enum, nor does it grant retry/lease authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotError {
    pub code: CanonicalSnapshotErrorCode,
    pub message: String,
    pub request_id: String,
    pub retryable: bool,
    pub http_status: u16,
}

impl CanonicalSnapshotError {
    pub fn parse_response(bytes: &[u8], status: u16) -> Result<Self, SnapshotError> {
        let invalid = || SnapshotError {
            code: SnapshotErrorCode::IntegrityError,
            message: "invalid canonical HTTP error envelope or status binding".into(),
            http_status: status,
        };
        if bytes.len() > super::client::MAX_JSON_RESPONSE_BYTES {
            return Err(SnapshotError {
                code: SnapshotErrorCode::LimitExceeded,
                message: "canonical HTTP error exceeds the JSON response byte budget".into(),
                http_status: status,
            });
        }
        let envelope: CanonicalEnvelope =
            super::client::parse_json(bytes).map_err(|_| invalid())?;
        let error = envelope.error;
        if !(1..=512).contains(&error.request_id.chars().count())
            || error.message.chars().count() > 1024
            || expected_status(&error.code) != Some(status)
        {
            return Err(invalid());
        }
        let code = CanonicalSnapshotErrorCode::from_wire(&error.code).ok_or_else(invalid)?;
        Ok(Self {
            code,
            message: error.message,
            request_id: error.request_id,
            retryable: error.retryable,
            http_status: status,
        })
    }
}

#[derive(Deserialize)]
struct LegacyError {
    code: String,
    message: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalEnvelope {
    error: CanonicalError,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalError {
    code: String,
    message: String,
    request_id: String,
    retryable: bool,
}

fn expected_status(code: &str) -> Option<u16> {
    Some(match code {
        "PATH_NOT_FOUND" => 404,
        "NOT_DIRECTORY"
        | "NOT_FILE"
        | "SYMLINK_TRAVERSAL"
        | "CURSOR_STALE"
        | "EXPECTED_DIGEST_MISMATCH"
        | "NAMESPACE_CONFLICT"
        | "PUBLICATION_CONFLICT"
        | "RELEASE_IMMUTABLE" => 409,
        "UNAUTHENTICATED" => 401,
        "SCOPE_FORBIDDEN" => 403,
        "LEASE_EXPIRED" | "SNAPSHOT_GONE" => 410,
        "SNAPSHOT_NOT_READY"
        | "METADATA_NOT_READY"
        | "OBJECT_UNAVAILABLE"
        | "TEMPORARY_UNAVAILABLE" => 503,
        "INTEGRITY_ERROR" => 502,
        "MIXED_SOURCE_BATCH" | "UNSUPPORTED_ENTRY" | "UNSUPPORTED_CODEC" => 422,
        "OBJECT_TOO_LARGE" | "LIMIT_EXCEEDED" | "PROOF_BUDGET_EXCEEDED" => 413,
        "INVALID_REQUEST" | "RANGE_NOT_SUPPORTED" => 400,
        "RATE_LIMITED" => 429,
        // Explicit deployed spellings. These are compatibility facts, not
        // members of the canonical SPEC error-code set.
        "SCOPE_INVALID" | "CURSOR_INVALID" => 400,
        "VIEW_NOT_FOUND" | "LEASE_UNKNOWN" => 404,
        "OBJECT_DIGEST_MISMATCH" | "CONFLICT" => 409,
        "INTERNAL" => 500,
        _ => return None,
    })
}

pub(crate) fn parse(value: Value, status: u16) -> Result<SnapshotError, ()> {
    let detail = value.get("error").ok_or(())?;
    let canonical = detail.get("request_id").is_some() || detail.get("retryable").is_some();
    let (code, message) = if canonical {
        // Presence selects the contract, including a null/malformed hint.
        // Failed canonical decoding never gets a legacy parser retry.
        let envelope: CanonicalEnvelope = serde_json::from_value(value).map_err(|_| ())?;
        let error = envelope.error;
        if !(1..=512).contains(&error.request_id.chars().count())
            || error.message.chars().count() > 1024
            || expected_status(&error.code) != Some(status)
        {
            return Err(());
        }
        // Shape-check the server's hint. Transport retry/deadline policy is
        // independent; an error cannot grant unlimited retries or authority.
        let _retryable = error.retryable;
        (error.code, error.message)
    } else {
        let error: LegacyError = serde_json::from_value(detail.clone()).map_err(|_| ())?;
        if expected_status(&error.code).is_some_and(|expected| expected != status) {
            return Err(());
        }
        (error.code, error.message)
    };
    Ok(SnapshotError {
        code: SnapshotErrorCode::from_server(&code),
        message,
        http_status: status,
    })
}
