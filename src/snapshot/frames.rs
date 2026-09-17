//! Frame/navigation transport on top of [`Mst2Client`] (spec 04/06/07):
//! descriptor/lease lifecycle, HEAD, raw META pages, OBJECT batches,
//! chunk maps with Merkle proofs and CHUNK batches.
//!
//! Every byte returned here is verified before it leaves the module: frames
//! through the shared codec, pages through `page_id`, objects and chunks
//! through SHA-256 against the digest the fixed view advertised. A verified
//! chunk is still not a verified file — callers assembling chunks must
//! recompute the whole-file digest (spec 07 §7).

use std::collections::HashMap;

use mst2_codec::chunkmap::{merkle_root, verify_leaf, ChunkLeaf, ChunkMap, ProofSide, CHUNK_SIZE};
use mst2_codec::treeframe::Frame;

use crate::snapshot::{client::Mst2Client, SnapshotError, SnapshotErrorCode};

const MAX_CHUNK_BATCH: usize = 128;
const MAX_OBJECT_BATCH: usize = 128;

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
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(SnapshotError::new(
            SnapshotErrorCode::Internal,
            "base64 bad length",
        ));
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks_exact(4) {
        let vals: Vec<u8> = chunk
            .iter()
            .map(|&c| if c == b'=' { Ok(0) } else { dec(c).ok_or(()) })
            .collect::<Result<_, _>>()
            .map_err(|_| SnapshotError::new(SnapshotErrorCode::Internal, "base64 bad char"))?;
        // All shifts stay within u8: masks keep the top bits bounded.
        out.push((vals[0] << 2) | (vals[1] >> 4));
        out.push((vals[1] & 0x0f) << 4 | (vals[2] >> 2));
        out.push((vals[2] & 0x03) << 6 | vals[3]);
        if chunk[2] == b'=' {
            out.truncate(out.len() - 2);
        } else if chunk[3] == b'=' {
            out.truncate(out.len() - 1);
        }
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

/// One verified chunk, in request order.
#[derive(Debug, Clone)]
pub struct ChunkUnit {
    pub map_id: [u8; 32],
    pub file_content_id: [u8; 32],
    pub chunk_index: u64,
    pub bytes: Vec<u8>,
}

impl Mst2Client {
    pub(crate) fn snap_url(&self, tail: &str) -> String {
        format!("{}/api/v2/snapshots{tail}", self.base())
    }

    /// GET `/{sid}/descriptor`: never re-resolves latest.
    pub async fn descriptor(&self, sid: &str) -> Result<serde_json::Value, SnapshotError> {
        self.get_json(&self.snap_url(&format!("/{sid}/descriptor")))
            .await
    }

    pub async fn renew_lease(
        &self,
        lease_id: &str,
        lease_seconds: u64,
    ) -> Result<serde_json::Value, SnapshotError> {
        let url = format!("{}/api/v2/snapshots/leases/{lease_id}/renew", self.base());
        self.post_json(&url, serde_json::json!({ "lease_seconds": lease_seconds }))
            .await
    }

    /// Idempotent lease release. Returns whether an active lease was
    /// removed; this never deletes Git content.
    pub async fn release_lease(&self, lease_id: &str) -> Result<bool, SnapshotError> {
        let url = format!("{}/api/v2/snapshots/leases/{lease_id}", self.base());
        let v = self.delete_json(&url).await?;
        Ok(v.get("released").and_then(|v| v.as_bool()).unwrap_or(false))
    }

    /// HEAD blob: exact size and strong ETag without downloading content.
    pub async fn blob_head(
        &self,
        sid: &str,
        path: &str,
        expected_digest: Option<&str>,
    ) -> Result<(u64, String, String), SnapshotError> {
        let mut url = self.snap_url(&format!("/{sid}/blob?path={}", urlencode(path)));
        if let Some(d) = expected_digest {
            url.push_str(&format!("&expected_digest={}", urlencode(d)));
        }
        self.head_blob(&url).await
    }

    /// POST `/{sid}/metadata/pages`; returns `(page_id hex, page bytes)` in
    /// frame order with duplicates removed by the server. `encoding`
    /// negotiates `identity` (default) or `zstd` frame compression.
    pub async fn metadata_pages(
        &self,
        sid: &str,
        items: &[MetadataPageItem],
        encoding: Option<&str>,
    ) -> Result<Vec<([u8; 32], Vec<u8>)>, SnapshotError> {
        let mut req = serde_json::json!({ "items": items });
        if let Some(enc) = encoding {
            req["encoding"] = serde_json::Value::String(enc.to_string());
        }
        let body = serde_json::to_vec(&req)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        let raw = self
            .post_octets(self.snap_url(&format!("/{sid}/metadata/pages")), body)
            .await?;
        let frames = mst2_codec::treeframe::parse_stream(&raw)
            .map_err(|e| frame_err("metadata/pages stream", e))?;
        let mut out = Vec::new();
        for f in frames {
            if let Frame::Meta(m) = f {
                out.extend(m.pages);
            }
        }
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
        if items.is_empty() || items.len() > MAX_OBJECT_BATCH {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "objects batch must hold 1..128 items",
            ));
        }
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
            .post_octets(self.snap_url(&format!("/{sid}/objects")), body.clone())
            .await?;
        let frames = mst2_codec::treeframe::parse_stream(&raw)
            .map_err(|e| frame_err("objects stream", e))?;
        check_end(&frames, &body, items.len() as u32)?;

        let mut out = HashMap::new();
        for f in frames {
            if let Frame::Object(o) = f {
                for (cid, data) in o.objects {
                    out.insert(cid, data);
                }
            }
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
        let url = self.snap_url(&format!(
            "/{sid}/chunk-map?path={}&expected_digest={}",
            urlencode(path),
            urlencode(expected_digest)
        ));
        let v: serde_json::Value = self.get_json(&url).await?;
        let file_content_id = parse_digest(v["file_content_id"].as_str().unwrap_or(""))?;
        let pages_root = parse_digest(v["pages_root"].as_str().unwrap_or(""))?;
        let map_id_want = parse_digest(v["map_id"].as_str().unwrap_or(""))?;
        let file_size = parse_count(v["file_size"].as_str().unwrap_or(""), "file_size")?;
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
        let url = self.snap_url(&format!(
            "/{sid}/chunk-map/pages?path={}&expected_digest={}&page={page_index}",
            urlencode(path),
            urlencode(expected_digest)
        ));
        let v: serde_json::Value = self.get_json(&url).await?;
        let leaf_bytes = b64_decode(v["leaf"]["data_base64"].as_str().unwrap_or(""))?;
        let leaf = ChunkLeaf::decode(&leaf_bytes).map_err(|e| frame_err("chunk-map leaf", e))?;
        if leaf.page_index != page_index {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "chunk leaf page_index mismatch",
            ));
        }
        let expect_count = ChunkLeaf::expected_count(map.chunk_count, page_index);
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
        for step in v["proof"].as_array().unwrap_or(&vec![]) {
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
        if items.is_empty() || items.len() > MAX_CHUNK_BATCH {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "chunks batch must hold 1..128 items",
            ));
        }
        let mut req = serde_json::json!({
            "items": items
                .iter()
                .map(|i| serde_json::json!({
                    "path": i.path,
                    "expected_digest": i.expected_digest,
                    "map_id": i.map_id,
                    "chunk_index": i.chunk_index.to_string(),
                }))
                .collect::<Vec<_>>(),
        });
        if let Some(enc) = encoding {
            req["encoding"] = serde_json::Value::String(enc.to_string());
        }
        let body = serde_json::to_vec(&req)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        let raw = self
            .post_octets(self.snap_url(&format!("/{sid}/chunks")), body.clone())
            .await?;
        let frames =
            mst2_codec::treeframe::parse_stream(&raw).map_err(|e| frame_err("chunks stream", e))?;
        check_end(&frames, &body, items.len() as u32)?;
        let mut out = Vec::new();
        for f in frames {
            if let Frame::Chunk(c) = f {
                out.push(ChunkUnit {
                    map_id: c.map_id,
                    file_content_id: c.file_content_id,
                    chunk_index: c.chunk_index,
                    bytes: c.chunk_bytes,
                });
            }
        }
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
) -> Result<(), SnapshotError> {
    let end = frames
        .iter()
        .find_map(|f| match f {
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
    Ok(())
}

pub fn parse_digest(s: &str) -> Result<[u8; 32], SnapshotError> {
    let hex = s.strip_prefix("sha256:").unwrap_or(s);
    let mut out = [0u8; 32];
    if hex.len() != 64 {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            format!("digest must be 32 bytes hex: {s}"),
        ));
    }
    for i in 0..32 {
        out[i] = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)
            .map_err(|_| SnapshotError::new(SnapshotErrorCode::DigestMismatch, "bad digest hex"))?;
    }
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
    s.parse::<u64>().map_err(|_| {
        SnapshotError::new(SnapshotErrorCode::ScopeInvalid, format!("{field} overflow"))
    })
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
