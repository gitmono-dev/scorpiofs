//! Frame/navigation transport on top of [`Mst2Client`] (spec 04/06/07):
//! descriptor/lease lifecycle, HEAD, raw META pages, OBJECT batches,
//! chunk maps with Merkle proofs and CHUNK batches.
//!
//! Every byte returned here is verified before it leaves the module: frames
//! through the shared codec, pages through `page_id`, objects and chunks
//! through SHA-256 against the digest the fixed view advertised. A verified
//! chunk is still not a verified file — callers assembling chunks must
//! recompute the whole-file digest (spec 07 §7).

use std::collections::{HashMap, HashSet};

use mst2_codec::{
    chunkmap::{merkle_root, verify_leaf, ChunkLeaf, ChunkMap, ProofSide, CHUNK_SIZE},
    treeframe::{self, Frame},
};

use crate::snapshot::{client::Mst2Client, SnapshotError, SnapshotErrorCode};

const MAX_CHUNK_BATCH: usize = 128;
const MAX_OBJECT_BATCH: usize = 128;
const MAX_METADATA_BATCH: usize = 64;
const MAX_OBJECT_BATCH_BYTES: usize = 8 * 1024 * 1024;

// Account for JSON escaping and the complete envelope before sending any batch.
fn request_batches<'a, T>(
    items: &'a [T],
    encoding: Option<&str>,
    limit: usize,
    max_items: usize,
    item_value: impl Fn(&T) -> serde_json::Value,
) -> Result<Vec<&'a [T]>, SnapshotError> {
    let mut envelope = serde_json::json!({"items": []});
    if let Some(encoding) = encoding {
        envelope["encoding"] = encoding.into();
    }
    let overhead = serde_json::to_vec(&envelope).unwrap().len();
    let mut batches = Vec::new();
    let mut start = 0;
    let mut bytes = overhead;
    for (index, item) in items.iter().enumerate() {
        let item_bytes = serde_json::to_vec(&item_value(item)).unwrap().len();
        if overhead + item_bytes > limit {
            return Err(limit_err(
                "one TreeFrame item exceeds the JSON request byte limit",
            ));
        }
        let comma = usize::from(index > start);
        if bytes + comma + item_bytes > limit || index - start >= max_items {
            batches.push(&items[start..index]);
            start = index;
            bytes = overhead;
        }
        bytes += usize::from(index > start) + item_bytes;
    }
    if start < items.len() {
        batches.push(&items[start..]);
    }
    Ok(batches)
}

fn chunk_request_value(item: &ChunkRequest) -> serde_json::Value {
    serde_json::json!({
        "path": item.path,
        "expected_digest": item.expected_digest,
        "map_id": item.map_id,
        "chunk_index": item.chunk_index.to_string(),
    })
}

struct ResponseBudget {
    units: usize,
    raw_bytes: usize,
    wire_bytes: usize,
}

impl ResponseBudget {
    fn new(
        units: usize,
        logical_bytes: usize,
        unit_overhead: usize,
    ) -> Result<Self, SnapshotError> {
        let raw_bytes = units
            .checked_mul(unit_overhead)
            .and_then(|overhead| overhead.checked_add(logical_bytes))
            .and_then(|bytes| bytes.checked_add(treeframe::ERROR_MAX_BYTES))
            .ok_or_else(|| limit_err("TreeFrame response byte budget overflow"))?;
        // This client quota allows normal zstd overhead while bounding the
        // complete response by its request, even without Content-Length.
        let wire_bytes = units
            .checked_add(1)
            .and_then(|frames| frames.checked_mul(treeframe::HEADER_LEN))
            .and_then(|headers| raw_bytes.checked_mul(2)?.checked_add(headers))
            .ok_or_else(|| limit_err("TreeFrame response byte budget overflow"))?;
        Ok(Self {
            units,
            raw_bytes,
            wire_bytes,
        })
    }
}

fn limit_err(message: &str) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::LimitExceeded, message)
}

fn response_frames(
    client: &Mst2Client,
    raw: &[u8],
    context: &str,
    data_kind: u8,
    data_name: &str,
    budget: &ResponseBudget,
) -> Result<Vec<Frame>, SnapshotError> {
    let mut offset = 0usize;
    let mut frames = 0usize;
    let mut raw_total = 0usize;
    while offset < raw.len() {
        let header = raw
            .get(offset..)
            .and_then(|remaining| remaining.get(..treeframe::HEADER_LEN))
            .ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    "truncated TreeFrame header",
                )
            })?;
        let kind = header[6];
        if !matches!(kind, treeframe::KIND_END | treeframe::KIND_ERROR) && kind != data_kind {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("{context} contains a non-{data_name} data frame"),
            ));
        }
        let wire_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
        let raw_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let max_raw = match kind {
            treeframe::KIND_META => treeframe::META_MAX_RAW,
            treeframe::KIND_OBJECT => treeframe::OBJECT_MAX_RAW,
            treeframe::KIND_CHUNK => 76 + treeframe::CHUNK_MAX_LEN as usize,
            treeframe::KIND_END => 48,
            treeframe::KIND_ERROR => treeframe::ERROR_MAX_BYTES,
            _ => unreachable!("endpoint frame kind checked above"),
        };
        if raw_len > max_raw || wire_len > client.frame_wire_limit() {
            return Err(limit_err("TreeFrame payload exceeds its frame byte limit"));
        }
        if client.is_canonical() && header[7] != 0 {
            return Err(limit_err("reader selected identity TreeFrames"));
        }
        frames += 1;
        raw_total = raw_total
            .checked_add(raw_len)
            .ok_or_else(|| limit_err("TreeFrame raw response byte count overflow"))?;
        if frames > budget.units + 2 || raw_total > budget.raw_bytes {
            return Err(limit_err(
                "TreeFrame response exceeds its request's frame or raw byte budget",
            ));
        }
        offset = offset
            .checked_add(treeframe::HEADER_LEN)
            .and_then(|start| start.checked_add(wire_len))
            .filter(|end| *end <= raw.len())
            .ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    "truncated TreeFrame payload",
                )
            })?;
    }
    treeframe::parse_stream(raw).map_err(|error| frame_err(context, error))
}

fn b64_decode(s: &str) -> Result<Vec<u8>, SnapshotError> {
    // Standard alphabet, padded. No external base64 dependency.
    let dec = |c: u8| -> Option<u8> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        })
    };
    let invalid = || {
        SnapshotError::new(
            SnapshotErrorCode::IntegrityError,
            "invalid canonical padded base64",
        )
    };
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(invalid());
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let groups = bytes.as_chunks::<4>().0;
    for (index, chunk) in groups.iter().enumerate() {
        let a = dec(chunk[0]).ok_or_else(invalid)?;
        let b = dec(chunk[1]).ok_or_else(invalid)?;
        let final_group = index + 1 == groups.len();
        if chunk[2] == b'=' {
            if !final_group || chunk[3] != b'=' || b & 0x0f != 0 {
                return Err(invalid());
            }
            out.push((a << 2) | (b >> 4));
            continue;
        }
        let c = dec(chunk[2]).ok_or_else(invalid)?;
        if chunk[3] == b'=' {
            if !final_group || c & 0x03 != 0 {
                return Err(invalid());
            }
            out.push((a << 2) | (b >> 4));
            out.push((b & 0x0f) << 4 | (c >> 2));
            continue;
        }
        let d = dec(chunk[3]).ok_or_else(invalid)?;
        // All shifts stay within u8: masks keep the top bits bounded.
        out.push((a << 2) | (b >> 4));
        out.push((b & 0x0f) << 4 | (c >> 2));
        out.push((c & 0x03) << 6 | d);
    }
    Ok(out)
}

fn frame_err(context: &str, e: mst2_codec::CodecError) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::DigestMismatch, format!("{context}: {e}"))
}

/// Verified chunk-map binding for one file.
#[derive(Debug, Clone)]
pub struct VerifiedChunkMap {
    pub file_content_id: String,
    pub map_id: String,
    pub file_size: u64,
    pub chunk_count: u64,
    pub page_count: u64,
    pub pages_root: [u8; 32],
}

impl VerifiedChunkMap {
    /// Length of chunk `index` derived from the fixed profile map.
    pub fn chunk_len(&self, index: u64) -> Result<u64, SnapshotError> {
        let map = mst2_codec::chunkmap::ChunkMap::new(
            parse_digest(&self.file_content_id)?,
            self.file_size,
            self.pages_root,
        )
        .map_err(|error| {
            SnapshotError::new(SnapshotErrorCode::Internal, format!("chunk codec: {error}"))
        })?;
        map.chunk_len(index).map_err(|error| {
            SnapshotError::new(SnapshotErrorCode::Internal, format!("chunk codec: {error}"))
        })
    }
}

/// One verified chunk, in request order.
#[derive(Debug, Clone)]
pub struct ChunkUnit {
    pub map_id: [u8; 32],
    pub file_content_id: [u8; 32],
    pub chunk_index: u64,
    pub bytes: Vec<u8>,
}

/// Canonical 204 acknowledges idempotent release without reporting whether
/// the lease was active. Legacy 200 receipts preserve that distinction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseReleaseOutcome {
    Acknowledged,
    Removed,
    AlreadyReleased,
}

impl Mst2Client {
    pub(crate) fn snap_url(&self, tail: &str) -> String {
        format!("{}/api/v2/snapshots{tail}", self.base())
    }

    /// GET `/{sid}/descriptor`: validate the explicit bare or legacy wrapper
    /// contract and canonical identity, retaining the original JSON shape.
    /// This never re-resolves latest or establishes a new lease authority.
    pub async fn descriptor(&self, sid: &str) -> Result<serde_json::Value, SnapshotError> {
        let value = self
            .get_json(&self.snap_url(&format!("/{sid}/descriptor")))
            .await?;
        super::descriptor_wire::validate(&value, sid)?;
        if self.is_canonical() && value.get("descriptor").is_some() {
            return Err(chunk_binding_error());
        }
        Ok(value)
    }

    /// Retrieve a normalized descriptor with a verified fixed snapshot identity.
    /// Lease fields in a legacy wrapper remain hints, not reader authorization.
    pub async fn verified_descriptor(&self, sid: &str) -> Result<super::Descriptor, SnapshotError> {
        let value = self
            .get_json(&self.snap_url(&format!("/{sid}/descriptor")))
            .await?;
        if self.is_canonical() && value.get("descriptor").is_some() {
            return Err(chunk_binding_error());
        }
        super::descriptor_wire::validate(&value, sid)
    }

    pub async fn renew_lease(
        &self,
        lease_id: &str,
        lease_seconds: u64,
    ) -> Result<serde_json::Value, SnapshotError> {
        if !(60..=3600).contains(&lease_seconds) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::InvalidRequest,
                "renewal lease suggestion must be between 60 and 3600 seconds",
            ));
        }
        let url = super::lease_wire::url(self, lease_id, true)?;
        let value = self
            .post_json(&url, serde_json::json!({ "lease_seconds": lease_seconds }))
            .await?;
        super::lease_wire::validate(&value, lease_id)?;
        if self.is_canonical() && value.get("authorization_epoch").is_none() {
            return Err(chunk_binding_error());
        }
        Ok(value)
    }

    /// Idempotent lease release. Canonical 204 returns true for acknowledgement;
    /// legacy 200 returns its verified `released` flag. Use
    /// [`Self::release_lease_outcome`] when the removal distinction matters.
    pub async fn release_lease(&self, lease_id: &str) -> Result<bool, SnapshotError> {
        Ok(self.release_lease_outcome(lease_id).await? != LeaseReleaseOutcome::AlreadyReleased)
    }

    /// Release using the canonical 204 contract or the explicit legacy 200
    /// receipt contract selected by HTTP status, without parser fallback.
    pub async fn release_lease_outcome(
        &self,
        lease_id: &str,
    ) -> Result<LeaseReleaseOutcome, SnapshotError> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ReleaseReceipt {
            lease_id: String,
            released: bool,
        }

        let url = super::lease_wire::url(self, lease_id, false)?;
        let Some(v) = self.delete_release_json(&url).await? else {
            return Ok(LeaseReleaseOutcome::Acknowledged);
        };
        let invalid = || {
            SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "lease release receipt does not bind to the requested lease",
            )
        };
        let receipt: ReleaseReceipt = serde_json::from_value(v).map_err(|_| invalid())?;
        if receipt.lease_id != lease_id {
            return Err(invalid());
        }
        Ok(if receipt.released {
            LeaseReleaseOutcome::Removed
        } else {
            LeaseReleaseOutcome::AlreadyReleased
        })
    }

    /// HEAD blob: exact size and strong ETag without downloading content.
    pub async fn blob_head(
        &self,
        sid: &str,
        path: &str,
        expected_digest: Option<&str>,
    ) -> Result<(u64, String, String), SnapshotError> {
        self.require_feature("blob")?;
        self.validate_path(path)?;
        let mut url = self.snap_url(&format!("/{sid}/blob?path={}", urlencode(path)));
        if let Some(d) = expected_digest {
            url.push_str(&format!("&expected_digest={}", urlencode(d)));
        }
        self.head_blob(&url).await
    }

    /// POST `/{sid}/metadata/pages`; returns `(page_id hex, page bytes)` in
    /// frame order, rejecting duplicate pages and invalid END bindings. `encoding`
    /// negotiates `identity` (default) or `zstd` frame compression.
    pub async fn metadata_pages(
        &self,
        sid: &str,
        items: &[MetadataPageItem],
        encoding: Option<&str>,
    ) -> Result<Vec<([u8; 32], Vec<u8>)>, SnapshotError> {
        self.require_feature("metadata/pages")?;
        self.validate_encoding(encoding)?;
        for item in items {
            self.validate_path(&item.directory_path)?;
        }
        if items.is_empty() || items.len() > MAX_METADATA_BATCH {
            return Err(limit_err("metadata batch must hold 1..64 items"));
        }
        let batches = request_batches(
            items,
            encoding,
            self.request_byte_limit(),
            self.metadata_item_limit(),
            |item| serde_json::to_value(item).unwrap(),
        )?;
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for batch in batches {
            for page in self.metadata_pages_batch(sid, batch, encoding).await? {
                if seen.insert(page.0) {
                    out.push(page);
                }
            }
        }
        Ok(out)
    }

    async fn metadata_pages_batch(
        &self,
        sid: &str,
        items: &[MetadataPageItem],
        encoding: Option<&str>,
    ) -> Result<Vec<([u8; 32], Vec<u8>)>, SnapshotError> {
        if items.is_empty() || items.len() > MAX_METADATA_BATCH {
            return Err(limit_err("metadata batch must hold 1..64 items"));
        }
        let mut max_units = 0usize;
        for item in items {
            if item.route.len() > mst2_codec::metapage::MAX_DEPTH {
                return Err(limit_err("metadata radix depth exceeds 255"));
            }
            if let Some(expected) = &item.expected_digest {
                parse_digest(expected)?;
            }
            max_units += item.route.len() + 1;
        }
        let budget = ResponseBudget::new(
            max_units,
            max_units * mst2_codec::metapage::PAGE_MAX_BYTES,
            36 + 4,
        )?;
        let mut req = serde_json::json!({ "items": items });
        if let Some(enc) = encoding {
            req["encoding"] = serde_json::Value::String(enc.to_string());
        }
        let body = serde_json::to_vec(&req)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        let request_items = u32::try_from(items.len()).map_err(|_| {
            SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "metadata request item count overflow",
            )
        })?;
        let raw = self
            .post_treeframe(
                self.snap_url(&format!("/{sid}/metadata/pages")),
                body.clone(),
                sid,
                budget.wire_bytes,
            )
            .await?;
        let frames = response_frames(
            self,
            &raw,
            "metadata/pages stream",
            treeframe::KIND_META,
            "META",
            &budget,
        )?;
        // A terminated-with-ERROR stream is a failure, not an empty page set
        // (spec 04 §5: a failed directory load must never read as "no entries").
        let (expected_units, expected_bytes) = match frames.last() {
            Some(Frame::End(end)) => {
                check_end(&frames, &body, request_items)?;
                (end.unique_unit_count, end.logical_bytes)
            }
            Some(Frame::Error(e)) => {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::Internal,
                    format!(
                        "server rejected metadata/pages: {} (request_id {})",
                        e.code, e.request_id
                    ),
                ));
            }
            _ => {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    "metadata/pages stream missing END frame",
                ));
            }
        };
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let mut unique_units = 0u32;
        let mut logical_bytes = 0u64;
        for f in frames {
            match f {
                Frame::Meta(m) => {
                    for (page_id, bytes) in m.pages {
                        if !seen.insert(page_id) {
                            return Err(SnapshotError::new(
                                SnapshotErrorCode::DigestMismatch,
                                "metadata/pages repeated a page across frames",
                            ));
                        }
                        unique_units = unique_units.checked_add(1).ok_or_else(|| {
                            SnapshotError::new(
                                SnapshotErrorCode::LimitExceeded,
                                "metadata page count overflow",
                            )
                        })?;
                        logical_bytes =
                            logical_bytes
                                .checked_add(bytes.len() as u64)
                                .ok_or_else(|| {
                                    SnapshotError::new(
                                        SnapshotErrorCode::LimitExceeded,
                                        "metadata page byte count overflow",
                                    )
                                })?;
                        out.push((page_id, bytes));
                    }
                }
                Frame::End(_) => {}
                _ => {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        "metadata/pages stream contains a non-META data frame",
                    ));
                }
            }
        }
        // A route may return ancestor witness pages, and aliased routes can
        // share pages. Compare END with the actual unique pages, not items.
        if unique_units != expected_units || logical_bytes != expected_bytes {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "metadata/pages END page count or logical bytes do not match the stream",
            ));
        }
        check_metadata_members(items, &out)?;
        Ok(out)
    }

    /// POST `/{sid}/objects` for a batch of small files. Returns
    /// `content_id -> bytes`; all requested digests must be present.
    pub async fn objects(
        &self,
        sid: &str,
        items: &[(String, String)],
        encoding: Option<&str>,
    ) -> Result<HashMap<[u8; 32], Vec<u8>>, SnapshotError> {
        self.require_feature("objects")?;
        self.validate_encoding(encoding)?;
        for (path, _) in items {
            self.validate_path(path)?;
        }
        if items.is_empty() || items.len() > MAX_OBJECT_BATCH {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "objects batch must hold 1..128 items",
            ));
        }
        let batches = request_batches(
            items,
            encoding,
            self.request_byte_limit(),
            self.request_item_limit()
                .min((self.object_byte_limit() / treeframe::OBJECT_MAX_LEN as usize).max(1)),
            |(path, digest)| serde_json::json!({"path": path, "expected_digest": digest}),
        )?;
        let mut out = HashMap::new();
        let mut bytes = 0usize;
        for batch in batches {
            let objects = self.objects_batch(sid, batch, encoding).await?;
            bytes += objects
                .iter()
                .filter(|(id, _)| !out.contains_key(*id))
                .map(|(_, data)| data.len())
                .sum::<usize>();
            if bytes > MAX_OBJECT_BATCH_BYTES {
                return Err(limit_err(
                    "objects response exceeds the unique raw byte limit",
                ));
            }
            out.extend(objects);
        }
        Ok(out)
    }

    async fn objects_batch(
        &self,
        sid: &str,
        items: &[(String, String)],
        encoding: Option<&str>,
    ) -> Result<HashMap<[u8; 32], Vec<u8>>, SnapshotError> {
        if items.is_empty() || items.len() > MAX_OBJECT_BATCH {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "objects batch must hold 1..128 items",
            ));
        }
        let requested: HashSet<_> = items
            .iter()
            .map(|(_, digest)| parse_digest(digest))
            .collect::<Result<_, _>>()?;
        let budget = ResponseBudget::new(
            requested.len(),
            (requested.len() * treeframe::OBJECT_MAX_LEN as usize).min(self.object_byte_limit()),
            40 + 4,
        )?;
        let mut req = serde_json::json!({
            "items": items
                .iter()
                .map(|(p, d)| serde_json::json!({"path": p, "expected_digest": d}))
                .collect::<Vec<_>>(),
        });
        if let Some(enc) = encoding {
            req["encoding"] = serde_json::Value::String(enc.to_string());
        }
        let body = serde_json::to_vec(&req)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        let raw = self
            .post_treeframe(
                self.snap_url(&format!("/{sid}/objects")),
                body.clone(),
                sid,
                budget.wire_bytes,
            )
            .await?;
        let frames = response_frames(
            self,
            &raw,
            "objects stream",
            treeframe::KIND_OBJECT,
            "OBJECT",
            &budget,
        )?;
        let (expected_units, expected_bytes) = check_end(&frames, &body, items.len() as u32)?;

        let mut out = HashMap::new();
        let mut logical_bytes = 0u64;
        for f in frames {
            match f {
                Frame::Object(o) => {
                    for (cid, data) in o.objects {
                        if !requested.contains(&cid) || out.contains_key(&cid) {
                            return Err(SnapshotError::new(
                                SnapshotErrorCode::DigestMismatch,
                                "objects response contains an unrequested or duplicate content_id",
                            ));
                        }
                        logical_bytes += data.len() as u64;
                        if logical_bytes > self.object_byte_limit() as u64 {
                            return Err(limit_err(
                                "objects response exceeds the unique raw byte limit",
                            ));
                        }
                        out.insert(cid, data);
                    }
                }
                Frame::End(_) => {}
                _ => unreachable!("endpoint frame kinds and successful END were checked"),
            }
        }
        if out.len() != requested.len()
            || out.len() as u32 != expected_units
            || logical_bytes != expected_bytes
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "objects response unit set or END counts do not match the request",
            ));
        }
        Ok(out)
    }

    /// GET `/{sid}/chunk-map`, independently rebuilding and checking the
    /// MCM2 descriptor and the `map_id` binding.
    pub async fn chunk_map(
        &self,
        sid: &str,
        path: &str,
        expected_digest: &str,
    ) -> Result<VerifiedChunkMap, SnapshotError> {
        self.require_feature("chunk-map")?;
        self.validate_path(path)?;
        let want = parse_digest(expected_digest)?;
        let url = self.snap_url(&format!(
            "/{sid}/chunk-map?path={}&expected_digest={}",
            urlencode(path),
            urlencode(expected_digest)
        ));
        let response: serde_json::Value = self.get_json(&url).await?;
        if self.is_canonical() && response.get("map").is_none() {
            return Err(chunk_binding_error());
        }
        let v = super::chunk_wire::map_descriptor(&response, sid, path)?;
        if v["schema_version"].as_u64() != Some(2) {
            return Err(chunk_binding_error());
        }
        let file_content_id = parse_digest(v["file_content_id"].as_str().unwrap_or(""))?;
        // Bind the map to the file the view named (spec 07 §3, BODY-07): a
        // server returning a well-formed map for a *different* content id
        // must be rejected, never silently accepted.
        if file_content_id != want {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!(
                    "chunk map binds content {}, the view named {}",
                    hex32(&file_content_id),
                    hex32(&want)
                ),
            ));
        }
        let pages_root = parse_digest(v["pages_root"].as_str().unwrap_or(""))?;
        let map_id_want = parse_digest(v["map_id"].as_str().unwrap_or(""))?;
        let file_size = parse_count(v["file_size"].as_str().unwrap_or(""), "file_size")?;
        self.validate_file_size(file_size)?;
        // SPEC 07 limits content to 8 TiB independently of the wider JSON
        // counter domain and the codec's structural descriptor checks.
        const MAX_CHUNK_MAP_FILE_BYTES: u64 = 8 * 1024 * 1024 * 1024 * 1024;
        if file_size > MAX_CHUNK_MAP_FILE_BYTES {
            return Err(limit_err(
                "chunk-map file size exceeds the 8 TiB protocol limit",
            ));
        }
        let chunk_count = parse_count(v["chunk_count"].as_str().unwrap_or(""), "chunk_count")?;
        let page_count = parse_count(v["page_count"].as_str().unwrap_or(""), "page_count")?;
        let chunk_size = v["chunk_size"].as_u64().unwrap_or(0);
        if chunk_size != CHUNK_SIZE as u64 {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("chunk_size {chunk_size} is not the fixed 1MiB profile"),
            ));
        }
        // Rebuild the canonical 100-byte descriptor and its map_id ourselves.
        let map = ChunkMap::new(file_content_id, file_size, pages_root)
            .map_err(|e| frame_err("chunk-map descriptor", e))?;
        if map.chunk_count != chunk_count
            || map.page_count != page_count
            || map.map_id() != map_id_want
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "chunk-map descriptor fields are internally inconsistent",
            ));
        }
        Ok(VerifiedChunkMap {
            file_content_id: format!("sha256:{}", hex32(&file_content_id)),
            map_id: format!("sha256:{}", hex32(&map_id_want)),
            file_size,
            chunk_count,
            page_count,
            pages_root,
        })
    }

    /// GET `/{sid}/chunk-map/pages?page=N`, decoding the MCL2 leaf and
    /// checking its shape + bottom-up proof against `pages_root`.
    pub async fn chunk_map_page(
        &self,
        sid: &str,
        path: &str,
        expected_digest: &str,
        map: &VerifiedChunkMap,
        page_index: u64,
    ) -> Result<ChunkLeaf, SnapshotError> {
        self.chunk_map_page_contract(
            sid,
            path,
            expected_digest,
            map,
            page_index,
            self.is_canonical(),
        )
        .await
    }

    /// Request the canonical map_id/page_index query contract. This explicit
    /// entry point does not retry a rejected request with legacy parameters.
    pub async fn chunk_map_page_canonical(
        &self,
        sid: &str,
        path: &str,
        expected_digest: &str,
        map: &VerifiedChunkMap,
        page_index: u64,
    ) -> Result<ChunkLeaf, SnapshotError> {
        self.chunk_map_page_contract(sid, path, expected_digest, map, page_index, true)
            .await
    }

    async fn chunk_map_page_contract(
        &self,
        sid: &str,
        path: &str,
        expected_digest: &str,
        map: &VerifiedChunkMap,
        page_index: u64,
        canonical_query: bool,
    ) -> Result<ChunkLeaf, SnapshotError> {
        self.require_feature("chunk-map")?;
        self.validate_path(path)?;
        self.validate_file_size(map.file_size)?;
        let content_id = parse_digest(expected_digest)?;
        let canonical = ChunkMap::new(content_id, map.file_size, map.pages_root)
            .map_err(|error| frame_err("chunk-map descriptor", error))?;
        if parse_digest(&map.file_content_id)? != content_id
            || parse_digest(&map.map_id)? != canonical.map_id()
            || map.chunk_count != canonical.chunk_count
            || map.page_count != canonical.page_count
        {
            return Err(chunk_binding_error());
        }
        if page_index >= map.page_count {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "chunk-map page index is outside the fixed map",
            ));
        }
        let query = if canonical_query {
            format!(
                "path={}&map_id={}&page_index={page_index}",
                urlencode(path),
                urlencode(&map.map_id)
            )
        } else {
            format!(
                "path={}&expected_digest={}&page={page_index}",
                urlencode(path),
                urlencode(expected_digest)
            )
        };
        let url = self.snap_url(&format!("/{sid}/chunk-map/pages?{query}"));
        let v: serde_json::Value = self.get_json(&url).await?;
        let expect_count = ChunkLeaf::expected_count(map.chunk_count, page_index);
        if canonical_query && !(v.get("leaf_base64").is_some() || v.get("page_index").is_some()) {
            return Err(chunk_binding_error());
        }
        let (encoded_leaf, steps) = super::chunk_wire::map_leaf(
            &v,
            sid,
            path,
            &map.map_id,
            map.page_count,
            page_index,
            expect_count,
        )?;
        let leaf_bytes = b64_decode(encoded_leaf)?;
        let leaf = ChunkLeaf::decode(&leaf_bytes).map_err(|e| frame_err("chunk-map leaf", e))?;
        if leaf.page_index != page_index {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "chunk leaf page_index mismatch",
            ));
        }
        if leaf.chunk_sha256.len() as u64 != expect_count {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!(
                    "chunk leaf count {} != {expect_count}",
                    leaf.chunk_sha256.len()
                ),
            ));
        }
        let mut proof = Vec::new();
        for step in steps {
            let digest = parse_digest(step["digest"].as_str().unwrap_or(""))?;
            let sibling_pages = parse_count(
                step["sibling_pages"].as_str().unwrap_or(""),
                "sibling_pages",
            )?;
            let side = match step["side"].as_str() {
                Some("left") => ProofSide::Left,
                Some("right") => ProofSide::Right,
                _ => {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        "chunk proof side must be left|right",
                    ))
                }
            };
            proof.push(mst2_codec::chunkmap::ProofStep {
                side,
                sibling_pages,
                digest,
            });
        }
        let leaf_hash = leaf.leaf_hash().map_err(|e| frame_err("leaf hash", e))?;
        verify_leaf(
            map.page_count,
            page_index,
            leaf_hash,
            &proof,
            map.pages_root,
        )
        .map_err(|e| frame_err("chunk proof", e))?;
        Ok(leaf)
    }

    /// POST `/{sid}/chunks`; each returned chunk is frame-verified.
    pub async fn chunks(
        &self,
        sid: &str,
        items: &[ChunkRequest],
        encoding: Option<&str>,
    ) -> Result<Vec<ChunkUnit>, SnapshotError> {
        self.require_feature("chunks")?;
        self.validate_encoding(encoding)?;
        for item in items {
            self.validate_path(&item.path)?;
        }
        if items.is_empty() || items.len() > MAX_CHUNK_BATCH {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "chunks batch must hold 1..128 items",
            ));
        }
        let mut requested = HashMap::new();
        for item in items {
            let key = (parse_digest(&item.map_id)?, item.chunk_index);
            let file_id = parse_digest(&item.expected_digest)?;
            if requested
                .insert(key, file_id)
                .is_some_and(|previous| previous != file_id)
            {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::ScopeInvalid,
                    "a chunk unit cannot name different file content ids",
                ));
            }
        }
        let batches = request_batches(
            items,
            encoding,
            self.request_byte_limit(),
            self.request_item_limit()
                .min((self.chunk_byte_limit() / treeframe::CHUNK_MAX_LEN as usize).max(1)),
            chunk_request_value,
        )?;
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for batch in batches {
            for chunk in self.chunks_batch(sid, batch, encoding).await? {
                if seen.insert((chunk.map_id, chunk.chunk_index)) {
                    out.push(chunk);
                }
            }
        }
        Ok(out)
    }

    async fn chunks_batch(
        &self,
        sid: &str,
        items: &[ChunkRequest],
        encoding: Option<&str>,
    ) -> Result<Vec<ChunkUnit>, SnapshotError> {
        if items.is_empty() || items.len() > MAX_CHUNK_BATCH {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "chunks batch must hold 1..128 items",
            ));
        }
        let mut requested = HashMap::new();
        for item in items {
            let key = (parse_digest(&item.map_id)?, item.chunk_index);
            let file_id = parse_digest(&item.expected_digest)?;
            if requested
                .insert(key, file_id)
                .is_some_and(|previous| previous != file_id)
            {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::ScopeInvalid,
                    "a chunk unit cannot name different file content ids",
                ));
            }
        }
        let budget = ResponseBudget::new(
            requested.len(),
            (requested.len() * treeframe::CHUNK_MAX_LEN as usize).min(self.chunk_byte_limit()),
            76,
        )?;
        let mut req = serde_json::json!({
            "items": items
                .iter()
                .map(chunk_request_value)
                .collect::<Vec<_>>(),
        });
        if let Some(enc) = encoding {
            req["encoding"] = serde_json::Value::String(enc.to_string());
        }
        let body = serde_json::to_vec(&req)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        let raw = self
            .post_treeframe(
                self.snap_url(&format!("/{sid}/chunks")),
                body.clone(),
                sid,
                budget.wire_bytes,
            )
            .await?;
        let frames = response_frames(
            self,
            &raw,
            "chunks stream",
            treeframe::KIND_CHUNK,
            "CHUNK",
            &budget,
        )?;
        let (expected_units, expected_bytes) = check_end(&frames, &body, items.len() as u32)?;
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let mut logical_bytes = 0u64;
        for f in frames {
            match f {
                Frame::Chunk(c) => {
                    let key = (c.map_id, c.chunk_index);
                    if requested.get(&key) != Some(&c.file_content_id) || !seen.insert(key) {
                        return Err(SnapshotError::new(
                            SnapshotErrorCode::DigestMismatch,
                            "chunks response contains an unrequested or duplicate chunk unit",
                        ));
                    }
                    logical_bytes += c.chunk_bytes.len() as u64;
                    out.push(ChunkUnit {
                        map_id: c.map_id,
                        file_content_id: c.file_content_id,
                        chunk_index: c.chunk_index,
                        bytes: c.chunk_bytes,
                    });
                }
                Frame::End(_) => {}
                _ => unreachable!("endpoint frame kinds and successful END were checked"),
            }
        }
        if seen.len() != requested.len()
            || seen.len() as u32 != expected_units
            || logical_bytes != expected_bytes
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "chunks response unit set or END counts do not match the request",
            ));
        }
        self.count_units(out.len() as u64);
        Ok(out)
    }
}

/// One `/chunks` request member.
#[derive(Debug, Clone)]
pub struct ChunkRequest {
    pub path: String,
    pub expected_digest: String,
    pub map_id: String,
    pub chunk_index: u64,
}

/// One `/metadata/pages` request member.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetadataPageItem {
    pub directory_path: String,
    pub route: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_digest: Option<String>,
}

fn check_end(
    frames: &[Frame],
    request_body: &[u8],
    expect_items: u32,
) -> Result<(u32, u64), SnapshotError> {
    if let Some(Frame::Error(error)) = frames.last() {
        return Err(SnapshotError::new(
            SnapshotErrorCode::Internal,
            format!(
                "server rejected TreeFrame request: {} (request_id {})",
                error.code, error.request_id
            ),
        ));
    }
    let end = frames
        .last()
        .and_then(|f| match f {
            Frame::End(e) => Some(e),
            _ => None,
        })
        .ok_or_else(|| {
            SnapshotError::new(SnapshotErrorCode::DigestMismatch, "missing END frame")
        })?;
    if end.request_item_count != expect_items {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            format!(
                "END item count {} != request {}",
                end.request_item_count, expect_items
            ),
        ));
    }
    use ring::digest::{Context, SHA256};
    let mut cx = Context::new(&SHA256);
    cx.update(request_body);
    let got = cx.finish();
    if end.request_body_sha256.as_ref() != got.as_ref() {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            "END request_body_sha256 does not match the body sent",
        ));
    }
    Ok((end.unique_unit_count, end.logical_bytes))
}

fn check_metadata_members(
    items: &[MetadataPageItem],
    pages: &[([u8; 32], Vec<u8>)],
) -> Result<(), SnapshotError> {
    let page_ids: HashSet<_> = pages.iter().map(|(id, _)| *id).collect();
    for item in items {
        if let Some(expected) = &item.expected_digest {
            if !page_ids.contains(&parse_digest(expected)?) {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    "metadata/pages response is missing a requested terminal page",
                ));
            }
        }
    }
    if items.iter().all(|item| item.expected_digest.is_some()) {
        let mut parents: HashMap<_, Vec<_>> = HashMap::new();
        for (id, bytes) in pages {
            let (page, _) = mst2_codec::metapage::Page::decode(bytes)
                .map_err(|error| frame_err("metadata witness page", error))?;
            if let mst2_codec::metapage::Page::Branch { children, .. } = page {
                for child in children {
                    parents
                        .entry((child.label, child.child_page_id))
                        .or_default()
                        .push(*id);
                }
            }
        }
        let mut expected = HashSet::new();
        for item in items {
            let terminal = parse_digest(item.expected_digest.as_ref().unwrap())?;
            expected.insert(terminal);
            let mut frontier = HashSet::from([terminal]);
            for label in item.route.iter().rev() {
                let mut ancestors = HashSet::new();
                for child in frontier {
                    if let Some(ids) = parents.get(&(*label, child)) {
                        ancestors.extend(ids.iter().copied());
                    }
                }
                if ancestors.is_empty() {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        "metadata/pages response is missing a requested route witness",
                    ));
                }
                expected.extend(ancestors.iter().copied());
                frontier = ancestors;
            }
        }
        if expected != page_ids {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "metadata/pages response contains an unrequested page",
            ));
        }
    }
    // A routed terminal can share a physical page across directories. The
    // caller additionally binds these witnesses to its descriptor/known
    // root ids; structural reachability alone is not publisher authority.
    Ok(())
}

pub fn parse_digest(s: &str) -> Result<[u8; 32], SnapshotError> {
    let hex = s.strip_prefix("sha256:").ok_or_else(|| {
        SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            "digest must start with sha256:",
        )
    })?;
    let mut out = [0u8; 32];
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            "digest must contain exactly 64 lowercase hex digits",
        ));
    }
    hex::decode_to_slice(hex.as_bytes(), &mut out)
        .map_err(|_| SnapshotError::new(SnapshotErrorCode::DigestMismatch, "bad digest hex"))?;
    Ok(out)
}

pub(crate) fn parse_count(s: &str, field: &str) -> Result<u64, SnapshotError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) || (s.len() > 1 && s.starts_with('0'))
    {
        return Err(SnapshotError::new(
            SnapshotErrorCode::ScopeInvalid,
            format!("{field} must be a decimal string"),
        ));
    }
    let value = s.parse::<u64>().map_err(|_| {
        SnapshotError::new(
            SnapshotErrorCode::LimitExceeded,
            format!("{field} exceeds the protocol counter limit"),
        )
    })?;
    if value > i64::MAX as u64 {
        return Err(SnapshotError::new(
            SnapshotErrorCode::LimitExceeded,
            format!("{field} exceeds the protocol counter limit"),
        ));
    }
    Ok(value)
}

fn chunk_binding_error() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::IntegrityError,
        "chunk-map response does not bind to the requested fixed snapshot, path and map",
    )
}

pub fn hex32(b: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(64);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn urlencode(s: &str) -> String {
    // Same minimal encoder as client.rs paths (digests and `/`-paths).
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Re-exported for reader assembly: the pages root computed over leaf
/// hashes in order, independent of the server's value.
pub fn check_merkle_root(leaves: &[[u8; 32]], root: [u8; 32]) -> Result<(), SnapshotError> {
    let computed = merkle_root(leaves).map_err(|e| frame_err("merkle root", e))?;
    if computed != root {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            "pages_root does not match the delivered leaves",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn chunk_len_matches_the_map_rules() {
        use super::{parse_digest, VerifiedChunkMap};
        use mst2_codec::chunkmap::CHUNK_SIZE;

        let size = 2 * CHUNK_SIZE as u64 + 7;
        let id = "sha256:".to_string() + &"0".repeat(64);
        let map = VerifiedChunkMap {
            file_content_id: id.clone(),
            map_id: id,
            file_size: size,
            chunk_count: 3,
            page_count: 1,
            pages_root: [0u8; 32],
        };
        assert_eq!(map.chunk_len(0).unwrap(), CHUNK_SIZE as u64);
        assert_eq!(map.chunk_len(2).unwrap(), 7);
        assert!(map.chunk_len(3).is_err());
        assert!(parse_digest(&map.file_content_id).is_ok());
    }
    use super::*;

    #[test]
    fn parse_digest_rejects_unicode_without_panicking() {
        for payload in [
            format!("0\u{e9}{}", "0".repeat(61)),
            format!("\u{20ac}{}", "0".repeat(61)),
            format!("\u{1f600}{}", "0".repeat(60)),
            "\u{e9}".repeat(32),
        ] {
            assert_eq!(payload.len(), 64);
            let parsed = std::panic::catch_unwind(|| parse_digest(&format!("sha256:{payload}")));
            assert!(parsed.is_ok(), "a Unicode digest must return an error");
            assert_eq!(
                parsed.unwrap().unwrap_err().code,
                SnapshotErrorCode::DigestMismatch
            );
        }
    }

    #[test]
    fn parse_digest_requires_canonical_prefix_and_hex_length() {
        for invalid in [
            String::new(),
            "0".repeat(64),
            format!("SHA256:{}", "0".repeat(64)),
            format!("sha256:{}", "0".repeat(63)),
            format!("sha256:{}", "0".repeat(65)),
            format!("sha256:{}", "A".repeat(64)),
            format!("sha256:{}g", "0".repeat(63)),
            format!("sha256:{} ", "0".repeat(63)),
        ] {
            assert_eq!(
                parse_digest(&invalid).unwrap_err().code,
                SnapshotErrorCode::DigestMismatch
            );
        }
    }

    #[test]
    fn parse_digest_decodes_all_canonical_byte_values() {
        for start in (0..=224).step_by(32) {
            let bytes = std::array::from_fn(|i| (start + i) as u8);
            let digest = format!("sha256:{}", hex32(&bytes));
            assert_eq!(parse_digest(&digest).unwrap(), bytes);
        }
        assert_eq!(
            parse_digest(&format!("sha256:{}", "0".repeat(64))).unwrap(),
            [0; 32]
        );
        assert_eq!(
            parse_digest(&format!("sha256:{}", "f".repeat(64))).unwrap(),
            [255; 32]
        );
    }
}
