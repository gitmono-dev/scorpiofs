//! Closed canonical errors and the explicit existing deployment contract.

use serde::Deserialize;
use serde_json::Value;

use super::{SnapshotError, SnapshotErrorCode};

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
        "OBJECT_DIGEST_MISMATCH" => 409,
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
