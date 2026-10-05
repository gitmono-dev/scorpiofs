//! Explicit canonical and legacy chunk-map JSON contracts.

use serde_json::Value;

use super::{frames::parse_count, SnapshotError, SnapshotErrorCode};

fn binding_error() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::IntegrityError,
        "chunk-map JSON does not match its selected fixed-view contract",
    )
}

fn closed(value: &Value, fields: &[&str]) -> Result<(), SnapshotError> {
    let object = value.as_object().ok_or_else(binding_error)?;
    if object.keys().any(|key| !fields.contains(&key.as_str())) {
        return Err(binding_error());
    }
    Ok(())
}

pub(crate) fn map_descriptor<'a>(
    value: &'a Value,
    sid: &str,
    path: &str,
) -> Result<&'a Value, SnapshotError> {
    if value["snapshot_id"].as_str() != Some(sid) || value["path"].as_str() != Some(path) {
        return Err(binding_error());
    }
    const MAP_FIELDS: &[&str] = &[
        "schema_version",
        "file_content_id",
        "file_size",
        "chunk_size",
        "chunk_count",
        "page_count",
        "pages_root",
        "map_id",
    ];
    if value.get("map").is_some() {
        closed(value, &["snapshot_id", "path", "map"])?;
        let map = &value["map"];
        closed(map, MAP_FIELDS)?;
        Ok(map)
    } else {
        closed(
            value,
            &[
                "snapshot_id",
                "path",
                "schema_version",
                "file_content_id",
                "file_size",
                "chunk_size",
                "chunk_count",
                "page_count",
                "pages_root",
                "map_id",
            ],
        )?;
        Ok(value)
    }
}

pub(crate) fn map_leaf<'a>(
    value: &'a Value,
    sid: &str,
    path: &str,
    map_id: &str,
    page_count: u64,
    page_index: u64,
    expected_count: u64,
) -> Result<(&'a str, &'a [Value]), SnapshotError> {
    if value["map_id"].as_str() != Some(map_id) {
        return Err(binding_error());
    }
    let encoded = if value.get("leaf_base64").is_some() || value.get("page_index").is_some() {
        closed(value, &["map_id", "page_index", "leaf_base64", "proof"])?;
        if parse_count(value["page_index"].as_str().unwrap_or(""), "page_index")? != page_index {
            return Err(binding_error());
        }
        value["leaf_base64"].as_str().ok_or_else(binding_error)?
    } else {
        closed(
            value,
            &[
                "snapshot_id",
                "path",
                "map_id",
                "page_count",
                "leaf",
                "proof",
            ],
        )?;
        if value["snapshot_id"].as_str() != Some(sid) || value["path"].as_str() != Some(path) {
            return Err(binding_error());
        }
        let leaf = &value["leaf"];
        closed(leaf, &["page_index", "count", "data_base64"])?;
        if parse_count(value["page_count"].as_str().unwrap_or(""), "page_count")? != page_count
            || parse_count(leaf["page_index"].as_str().unwrap_or(""), "page_index")? != page_index
            || parse_count(leaf["count"].as_str().unwrap_or(""), "leaf count")? != expected_count
        {
            return Err(binding_error());
        }
        leaf["data_base64"].as_str().ok_or_else(binding_error)?
    };
    let max_encoded = (16 + 32 * expected_count).div_ceil(3) * 4;
    if encoded.len() as u64 > max_encoded {
        return Err(SnapshotError::new(
            SnapshotErrorCode::LimitExceeded,
            "chunk leaf exceeds the expected fixed page encoding budget",
        ));
    }
    let proof = value["proof"].as_array().ok_or_else(binding_error)?;
    if proof.len() > 32 {
        return Err(SnapshotError::new(
            SnapshotErrorCode::LimitExceeded,
            "chunk proof exceeds 32 steps",
        ));
    }
    for step in proof {
        closed(step, &["side", "sibling_pages", "digest"])?;
    }
    Ok((encoded, proof))
}
