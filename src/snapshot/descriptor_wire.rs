//! Explicit canonical bare-descriptor and legacy wrapper contracts. Neither
//! descriptor retrieval nor legacy lease hints establishes authorization.

use serde::Deserialize;
use serde_json::Value;

use super::{Descriptor, SnapshotError, SnapshotErrorCode};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DescriptorWire {
    schema_version: u16,
    metadata_codec: u16,
    instance_id: String,
    namespace_view_id: String,
    scope: String,
    materialization_policy: u16,
    fs_semantics: u16,
    access_projection: u16,
    metadata_root: String,
    snapshot_id: String,
}

impl From<DescriptorWire> for Descriptor {
    fn from(wire: DescriptorWire) -> Self {
        Self {
            schema_version: wire.schema_version,
            metadata_codec: wire.metadata_codec,
            instance_id: wire.instance_id,
            namespace_view_id: wire.namespace_view_id,
            scope: wire.scope,
            materialization_policy: wire.materialization_policy,
            fs_semantics: wire.fs_semantics,
            access_projection: wire.access_projection,
            metadata_root: wire.metadata_root,
            snapshot_id: wire.snapshot_id,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyDescriptorWire {
    snapshot_id: String,
    descriptor: DescriptorWire,
    lease_id: String,
    lease_expires_at: String,
}

pub(crate) fn validate(value: &Value, requested: &str) -> Result<Descriptor, SnapshotError> {
    let invalid = || {
        SnapshotError::new(
            SnapshotErrorCode::IntegrityError,
            "invalid descriptor response contract",
        )
    };
    // Presence of the wrapper member selects that complete schema. A malformed
    // wrapper never falls back to parsing an apparent bare descriptor.
    let descriptor: Descriptor = if value.get("descriptor").is_some() {
        let wrapper: LegacyDescriptorWire =
            serde_json::from_value(value.clone()).map_err(|_| invalid())?;
        if wrapper.snapshot_id != requested
            || wrapper.lease_id.is_empty()
            || wrapper.lease_expires_at.is_empty()
        {
            return Err(invalid());
        }
        wrapper.descriptor.into()
    } else {
        let bare: DescriptorWire = serde_json::from_value(value.clone()).map_err(|_| invalid())?;
        bare.into()
    };
    if descriptor.snapshot_id != requested {
        return Err(invalid());
    }
    super::auth::validate_descriptor(&descriptor)?;
    Ok(descriptor)
}
