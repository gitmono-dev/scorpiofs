//! Explicit capability contracts. Discovery does not grant snapshot authority.

use serde::Deserialize;
use serde_json::Value;

use super::{types::Capabilities, SnapshotError, SnapshotErrorCode};

/// Canonical discovery or the deployment's existing legacy advertisement.
/// Legacy limits are not promoted into canonical limits. Snapshot readers
/// retain this advertisement and configure their private transport from it.
#[derive(Debug, Clone)]
pub enum CapabilityAdvertisement {
    Canonical(CanonicalCapabilities),
    Legacy(Capabilities),
}

/// A closed, profile-checked canonical advertisement returned by discovery.
#[derive(Debug, Clone)]
pub struct CanonicalCapabilities {
    frame_encodings: Vec<String>,
    features: CanonicalCapabilityFeatures,
    limits: CapabilityLimits,
}

impl CanonicalCapabilities {
    pub fn protocol_versions(&self) -> &[u16] {
        &[2]
    }

    pub fn metadata_codecs(&self) -> &[u16] {
        &[1]
    }

    pub fn frame_encodings(&self) -> &[String] {
        &self.frame_encodings
    }

    pub fn features(&self) -> &CanonicalCapabilityFeatures {
        &self.features
    }

    /// Server maxima. Readers enforce these in addition to local hard limits.
    pub fn limits(&self) -> &CapabilityLimits {
        &self.limits
    }
}

impl CapabilityAdvertisement {
    /// Local feature selection, never a lease, authorization or readiness fact.
    pub(crate) fn reader_capabilities(&self) -> Capabilities {
        match self {
            Self::Legacy(caps) => caps.clone(),
            Self::Canonical(caps) => Capabilities {
                protocol_versions: vec![2],
                metadata_codecs: vec![1],
                frame_encodings: vec!["identity".into()],
                features: super::types::CapabilityFeatures {
                    resolve: caps.features.strict_publication,
                    directory: caps.features.directory,
                    leases: true,
                    lookup: caps.features.lookup,
                    metadata_pages: caps.features.metadata_pages,
                    raw_blob: caps.features.raw_blob,
                    objects: caps.features.small_objects,
                    chunk_reads: caps.features.chunk_reads,
                    full_hydration: caps.features.full_hydration,
                },
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalCapabilityFeatures {
    pub strict_publication: bool,
    pub directory: bool,
    pub lookup: bool,
    pub metadata_pages: bool,
    pub raw_blob: bool,
    pub small_objects: bool,
    pub chunk_reads: bool,
    pub full_hydration: bool,
    pub region_hints: bool,
    pub offline_export: bool,
}

/// Canonical profile limits. Codec partition values are fixed; operational
/// maxima may only be lower than the SPEC's hard maxima.
#[derive(Debug, Clone)]
pub struct CapabilityLimits {
    pub max_file_bytes: u64,
    pub max_path_bytes: u32,
    pub max_path_components: u32,
    pub metadata_page_bytes: u32,
    pub metadata_leaf_entries: u32,
    pub max_json_request_bytes: u32,
    pub max_json_response_bytes: u32,
    pub max_directory_entries: u32,
    pub max_request_items: u32,
    pub max_metadata_items: u32,
    pub small_object_bytes: u32,
    pub small_batch_bytes: u32,
    pub object_frame_raw_bytes: u32,
    pub chunk_frame_raw_bytes: u32,
    pub frame_wire_bytes: u32,
    pub zstd_window_bytes: u32,
    pub chunk_size: u32,
    pub chunk_batch_bytes: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalWire {
    protocol_versions: Vec<u16>,
    metadata_codecs: Vec<u16>,
    frame_encodings: Vec<String>,
    features: CanonicalCapabilityFeatures,
    limits: LimitsWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsWire {
    max_file_bytes: String,
    max_path_bytes: u32,
    max_path_components: u32,
    metadata_page_bytes: u32,
    metadata_leaf_entries: u32,
    max_json_request_bytes: u32,
    max_json_response_bytes: u32,
    max_directory_entries: u32,
    max_request_items: u32,
    max_metadata_items: u32,
    small_object_bytes: u32,
    small_batch_bytes: u32,
    object_frame_raw_bytes: u32,
    chunk_frame_raw_bytes: u32,
    frame_wire_bytes: u32,
    zstd_window_bytes: u32,
    chunk_size: u32,
    chunk_batch_bytes: u32,
}

fn invalid() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::IntegrityError,
        "capability advertisement violates its selected wire contract",
    )
}

pub(crate) fn parse(value: Value) -> Result<CapabilityAdvertisement, SnapshotError> {
    let features = value.get("features");
    let legacy_marker = ["resolve", "leases", "objects"]
        .iter()
        .any(|key| features.and_then(|features| features.get(key)).is_some());
    let canonical_marker = ["strict_publication", "small_objects", "region_hints"]
        .iter()
        .any(|key| features.and_then(|features| features.get(key)).is_some())
        || value
            .get("limits")
            .is_some_and(|limits| limits.get("max_file_bytes").is_some());
    if !canonical_marker && legacy_marker {
        // Existing deployments include partial legacy limits and extensions.
        // Preserve that explicit contract without inventing canonical facts.
        return serde_json::from_value(value)
            .map(CapabilityAdvertisement::Legacy)
            .map_err(|_| invalid());
    }
    // Selection precedes parsing. Invalid canonical data never falls back.
    let wire: CanonicalWire = serde_json::from_value(value).map_err(|_| invalid())?;
    if wire.protocol_versions != [2]
        || wire.metadata_codecs != [1]
        || !(1..=2).contains(&wire.frame_encodings.len())
        || !wire.frame_encodings.iter().any(|value| value == "identity")
        || wire
            .frame_encodings
            .iter()
            .any(|value| value != "identity" && value != "zstd")
        || (wire.frame_encodings.len() == 2 && wire.frame_encodings[0] == wire.frame_encodings[1])
    {
        return Err(invalid());
    }
    let limits = wire.limits;
    let max_file_bytes = super::frames::parse_count(&limits.max_file_bytes, "max_file_bytes")
        .map_err(|_| invalid())?;
    if max_file_bytes > 8_796_093_022_208
        || [
            (limits.metadata_page_bytes, 16_384),
            (limits.metadata_leaf_entries, 128),
            (limits.small_object_bytes, 262_144),
            (limits.object_frame_raw_bytes, 1_048_576),
            (limits.chunk_frame_raw_bytes, 1_048_652),
            (limits.chunk_size, 1_048_576),
        ]
        .iter()
        .any(|(actual, fixed)| actual != fixed)
        || [
            (limits.max_path_bytes, 4096),
            (limits.max_path_components, 256),
            (limits.max_json_request_bytes, 131_072),
            (limits.max_json_response_bytes, 1_048_576),
            (limits.max_directory_entries, 256),
            (limits.max_request_items, 128),
            (limits.max_metadata_items, 64),
            (limits.small_batch_bytes, 8_388_608),
            (limits.frame_wire_bytes, 2_097_152),
            (limits.zstd_window_bytes, 8_388_608),
            (limits.chunk_batch_bytes, 134_217_728),
        ]
        .iter()
        .any(|(actual, maximum)| *actual == 0 || actual > maximum)
    {
        return Err(invalid());
    }
    Ok(CapabilityAdvertisement::Canonical(CanonicalCapabilities {
        frame_encodings: wire.frame_encodings,
        features: wire.features,
        limits: CapabilityLimits {
            max_file_bytes,
            max_path_bytes: limits.max_path_bytes,
            max_path_components: limits.max_path_components,
            metadata_page_bytes: limits.metadata_page_bytes,
            metadata_leaf_entries: limits.metadata_leaf_entries,
            max_json_request_bytes: limits.max_json_request_bytes,
            max_json_response_bytes: limits.max_json_response_bytes,
            max_directory_entries: limits.max_directory_entries,
            max_request_items: limits.max_request_items,
            max_metadata_items: limits.max_metadata_items,
            small_object_bytes: limits.small_object_bytes,
            small_batch_bytes: limits.small_batch_bytes,
            object_frame_raw_bytes: limits.object_frame_raw_bytes,
            chunk_frame_raw_bytes: limits.chunk_frame_raw_bytes,
            frame_wire_bytes: limits.frame_wire_bytes,
            zstd_window_bytes: limits.zstd_window_bytes,
            chunk_size: limits.chunk_size,
            chunk_batch_bytes: limits.chunk_batch_bytes,
        },
    }))
}
