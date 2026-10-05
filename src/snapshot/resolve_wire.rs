//! Resolve envelope validation. Wire facts do not create offline authority.

use serde::{Deserialize, Deserializer};
use serde_json::Value;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use super::{
    descriptor_wire::DescriptorWire, types::ResolveResponse, SnapshotError, SnapshotErrorCode,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalResolve {
    descriptor: DescriptorWire,
    publication_sequence: String,
    writer_epoch: String,
    lease_id: String,
    lease_expires_at: String,
    authorization_epoch: String,
    resolved_at: String,
    delivery: String,
    #[serde(default, deserialize_with = "present_grant")]
    offline_grant: Option<OfflineGrant>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OfflineGrant {
    grant_id: String,
    snapshot_id: String,
    actor_domain_id: String,
    expires_at: String,
    policy: String,
}

fn present_grant<'de, D: Deserializer<'de>>(decoder: D) -> Result<Option<OfflineGrant>, D::Error> {
    OfflineGrant::deserialize(decoder).map(Some)
}

fn invalid() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::IntegrityError,
        "resolve response violates its selected contract or requested view",
    )
}

pub(crate) fn counter(value: &str) -> Result<(), SnapshotError> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || value
            .parse::<u64>()
            .ok()
            .is_none_or(|n| n > i64::MAX as u64)
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn opaque(value: &str) -> bool {
    (1..=512).contains(&value.chars().count())
}

pub(crate) fn timestamp(value: &str) -> Result<OffsetDateTime, SnapshotError> {
    // RFC3339 -00:00 explicitly means an unknown local offset; it cannot
    // identify an actual lease expiry instant.
    if value.ends_with("-00:00") {
        return Err(invalid());
    }
    OffsetDateTime::parse(value, &Rfc3339).map_err(|_| invalid())
}

pub(crate) fn parse_request(
    value: Value,
    request: &super::ResolveRequest,
    require_canonical: bool,
) -> Result<ResolveResponse, SnapshotError> {
    let canonical = ["writer_epoch", "resolved_at", "delivery", "offline_grant"]
        .iter()
        .any(|key| value.get(key).is_some());
    if !canonical {
        if require_canonical {
            return Err(invalid());
        }
        // Explicit existing envelope. A malformed canonical envelope never
        // gets here, including one with a null canonical field.
        return serde_json::from_value(value).map_err(|_| invalid());
    }
    let wire: CanonicalResolve = serde_json::from_value(value).map_err(|_| invalid())?;
    let descriptor = wire.descriptor.into();
    super::auth::validate_descriptor(&descriptor).map_err(|_| invalid())?;
    let delivery = match request.delivery {
        super::ResolveDelivery::Full => "full",
        super::ResolveDelivery::Lazy => "lazy",
    };
    if descriptor.scope != request.scope
        || wire.delivery != delivery
        || matches!(&request.target, super::ResolveTarget::View { view_id }
            if descriptor.namespace_view_id != *view_id)
        || !opaque(&wire.lease_id)
    {
        return Err(invalid());
    }
    for value in [
        &wire.publication_sequence,
        &wire.writer_epoch,
        &wire.authorization_epoch,
    ] {
        counter(value)?;
    }
    let resolved = timestamp(&wire.resolved_at)?;
    if timestamp(&wire.lease_expires_at)? <= resolved {
        return Err(invalid());
    }
    if let Some(grant) = wire.offline_grant {
        if !opaque(&grant.grant_id)
            || !opaque(&grant.actor_domain_id)
            || grant.snapshot_id != descriptor.snapshot_id
            || grant.policy != "trusted_local_export_v1"
        {
            return Err(invalid());
        }
        timestamp(&grant.expires_at)?;
        // Syntactically valid server hints are intentionally discarded. Only
        // a separately trusted local export can authorize offline reads.
    }
    Ok(ResolveResponse {
        descriptor,
        publication_sequence: wire.publication_sequence,
        lease_id: wire.lease_id,
        lease_expires_at: wire.lease_expires_at,
        authorization_epoch: wire.authorization_epoch,
    })
}
