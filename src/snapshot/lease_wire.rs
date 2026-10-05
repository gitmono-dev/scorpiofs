//! Lease wire validation never replaces the reader's fixed authority.

use serde::Deserialize;
use serde_json::Value;

use super::{Mst2Client, SnapshotError, SnapshotErrorCode};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalLease {
    lease_id: String,
    snapshot_id: String,
    lease_expires_at: String,
    authorization_epoch: String,
}

#[derive(Deserialize)]
struct LegacyLease {
    lease_id: String,
    snapshot_id: String,
    lease_expires_at: String,
}

fn invalid() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::IntegrityError,
        "lease renewal violates its selected contract or requested lease",
    )
}

pub(crate) fn url(
    client: &Mst2Client,
    lease_id: &str,
    renew: bool,
) -> Result<String, SnapshotError> {
    // WHATWG URLs normalize standalone dot segments. Reject those locally
    // rather than silently addressing another endpoint or lease. Other opaque
    // values, including slash, percent, query and fragment characters, are
    // encoded as exactly one path segment before URL parsing. URL setters
    // would otherwise silently discard literal tab/newline characters.
    if !super::resolve_wire::opaque(lease_id) || matches!(lease_id, "." | "..") {
        return Err(SnapshotError::new(
            SnapshotErrorCode::InvalidRequest,
            "lease identity cannot be represented as an opaque URL path segment",
        ));
    }
    let mut url =
        url::Url::parse(&format!("{}/api/v2/snapshots/leases", client.base())).map_err(|_| {
            SnapshotError::new(SnapshotErrorCode::InvalidRequest, "invalid lease base URL")
        })?;
    if url.cannot_be_a_base() || url.query().is_some() || url.fragment().is_some() {
        return Err(SnapshotError::new(
            SnapshotErrorCode::InvalidRequest,
            "invalid lease base URL",
        ));
    }
    let mut segment = String::with_capacity(lease_id.len() * 3);
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in lease_id.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            segment.push(byte as char);
        } else {
            segment.push('%');
            segment.push(HEX[(byte >> 4) as usize] as char);
            segment.push(HEX[(byte & 15) as usize] as char);
        }
    }
    url.set_path(&format!(
        "{}/{segment}{}",
        url.path(),
        if renew { "/renew" } else { "" }
    ));
    Ok(url.into())
}

pub(crate) fn validate(value: &Value, lease_id: &str) -> Result<(), SnapshotError> {
    // Presence selects the closed canonical contract, including null. A
    // malformed canonical response never receives a legacy parsing retry.
    if value.get("authorization_epoch").is_some() {
        let wire: CanonicalLease = serde_json::from_value(value.clone()).map_err(|_| invalid())?;
        if wire.lease_id != lease_id || !super::resolve_wire::opaque(&wire.lease_id) {
            return Err(invalid());
        }
        super::frames::parse_digest(&wire.snapshot_id).map_err(|_| invalid())?;
        super::resolve_wire::counter(&wire.authorization_epoch).map_err(|_| invalid())?;
        super::resolve_wire::timestamp(&wire.lease_expires_at).map_err(|_| invalid())?;
    } else {
        // Explicit deployed legacy responses lack the epoch and may include
        // extensions. They do not invent canonical epoch facts. The reader
        // still checks its fixed snapshot and retains epoch downgrade checks.
        let wire: LegacyLease = serde_json::from_value(value.clone()).map_err(|_| invalid())?;
        if wire.lease_id != lease_id {
            return Err(invalid());
        }
        super::frames::parse_digest(&wire.snapshot_id).map_err(|_| invalid())?;
        super::resolve_wire::timestamp(&wire.lease_expires_at).map_err(|_| invalid())?;
    }
    Ok(())
}
