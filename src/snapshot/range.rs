//! Range reads over the fixed view (spec 07 §6).
//!
//! A FUSE `read(offset, size)` must fetch only the chunks that cover the
//! requested bytes — reading the first 4 KiB of a 2 GiB file may not
//! download the file. This module owns that path for large files:
//!
//! * the chunk map is fetched once and independently verified
//!   (`map_id`, MCM2 field consistency) by the client;
//! * chunk-leaf pages are fetched on demand and checked against
//!   `pages_root` with their Merkle proof, so the digests used to verify
//!   chunk bytes are themselves authenticated;
//! * each chunk is verified against its leaf digest before it is cached or
//!   sliced — a short read never becomes zero-padded kernel data;
//! * the whole-file digest remains the hydration (`DURABLE_COMPLETE`)
//!   gate; per-chunk verification is what a range read can prove.
//!
//! Small files (≤ 256 KiB) are served whole through the OBJECT path: the
//! spec's small/large split, not a size threshold invented here.

use std::collections::HashMap;
use std::sync::Arc;

use mst2_codec::chunkmap::{CHUNKS_PER_PAGE, CHUNK_SIZE};

use crate::snapshot::{
    frames::{ChunkRequest, VerifiedChunkMap},
    reader::SnapshotReader,
    SnapshotError, SnapshotErrorCode,
};

/// Files at or below this size use the OBJECT path (spec 07 §2).
pub const OBJECT_CAP: u64 = 256 * 1024;

/// A large file read through its chunk map, with verified per-chunk caching.
pub struct ChunkedFile {
    reader: SnapshotReader,
    path: String,
    digest: String,
    pub size: u64,
    map: VerifiedChunkMap,
    /// Leaf digests per chunk-map page, verified against `pages_root`.
    leaves: tokio::sync::Mutex<HashMap<u64, Arc<Vec<[u8; 32]>>>>,
    /// Verified chunk bytes by index.
    chunks: tokio::sync::Mutex<HashMap<u64, Arc<Vec<u8>>>>,
}

impl ChunkedFile {
    /// Fetch and verify the chunk map for one path in the fixed view.
    pub async fn open(
        reader: &SnapshotReader,
        path: &str,
        digest: &str,
        size: u64,
    ) -> Result<Self, SnapshotError> {
        if size <= OBJECT_CAP {
            return Err(SnapshotError::new(
                SnapshotErrorCode::Internal,
                "small files are read whole, not through the chunk map",
            ));
        }
        let map = reader
            .client
            .chunk_map(reader.snapshot_id(), path, digest)
            .await?;
        if map.file_size != size {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("chunk map size {} != view size {size}", map.file_size),
            ));
        }
        Ok(ChunkedFile {
            reader: reader.clone(),
            path: path.to_string(),
            digest: digest.to_string(),
            size,
            map,
            leaves: tokio::sync::Mutex::new(HashMap::new()),
            chunks: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    pub fn map_id(&self) -> &str {
        &self.map.map_id
    }

    /// Bytes `[offset, offset+length)` of the file, clamped to EOF, with
    /// every covering chunk verified before it is sliced.
    pub async fn read_range(&self, offset: u64, length: u64) -> Result<Vec<u8>, SnapshotError> {
        if length == 0 || offset >= self.size {
            return Ok(Vec::new());
        }
        let end = offset.saturating_add(length).min(self.size);
        let (start_chunk, end_chunk) =
            mst2_codec::chunkmap::range_chunks(offset, end - offset, self.size)
                .map_err(codec_err)?;

        for index in start_chunk..=end_chunk {
            self.ensure_chunk(index).await?;
        }

        let cache = self.chunks.lock().await;
        let mut out = Vec::with_capacity((end - offset) as usize);
        for index in start_chunk..=end_chunk {
            let bytes = cache.get(&index).ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::Internal,
                    format!("chunk {index} missing after fetch"),
                )
            })?;
            let chunk_start = index * CHUNK_SIZE as u64;
            let from = if index == start_chunk {
                (offset - chunk_start) as usize
            } else {
                0
            };
            let to = if index == end_chunk {
                (end - chunk_start) as usize
            } else {
                bytes.len()
            };
            if from > to || to > bytes.len() {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::Internal,
                    format!(
                        "chunk {index}: slice {from}..{to} outside {} bytes",
                        bytes.len()
                    ),
                ));
            }
            out.extend_from_slice(&bytes[from..to]);
        }
        Ok(out)
    }

    /// True when every chunk of the file is already cached and verified.
    pub async fn fully_cached(&self) -> bool {
        self.chunks.lock().await.len() as u64 == self.map.chunk_count
    }

    /// Make chunk `index` available and verified, fetching the leaf page
    /// that authenticates it first when needed.
    async fn ensure_chunk(&self, index: u64) -> Result<(), SnapshotError> {
        if self.chunks.lock().await.contains_key(&index) {
            return Ok(());
        }
        let page = index / CHUNKS_PER_PAGE as u64;
        self.ensure_leaf_page(page).await?;
        let digests = self
            .leaves
            .lock()
            .await
            .get(&page)
            .cloned()
            .ok_or_else(|| {
                SnapshotError::new(SnapshotErrorCode::Internal, "leaf page missing after fetch")
            })?;
        let slot = (index % CHUNKS_PER_PAGE as u64) as usize;
        let want = digests.get(slot).copied().ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("leaf page {page} has no digest for chunk {index}"),
            )
        })?;

        let units = self
            .reader
            .client
            .chunks(
                self.reader.snapshot_id(),
                &[ChunkRequest {
                    path: self.path.clone(),
                    expected_digest: self.digest.clone(),
                    map_id: self.map.map_id.clone(),
                    chunk_index: index,
                }],
                self.reader.encoding_hint(),
            )
            .await?;
        let unit = units
            .into_iter()
            .find(|u| u.chunk_index == index)
            .ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    format!("no chunk {index} in the response"),
                )
            })?;
        // Independent verification: the bytes must hash to the digest the
        // authenticated leaf advertises, and match the map's length.
        let got = crate::snapshot::durable::digest_of(&unit.bytes);
        if got != format!("sha256:{}", crate::snapshot::frames::hex32(&want)) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("chunk {index} does not match its leaf digest"),
            ));
        }
        let expected_len = self.map.chunk_len(index)?;
        if unit.bytes.len() as u64 != expected_len {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!(
                    "chunk {index} is {} bytes, map says {expected_len}",
                    unit.bytes.len()
                ),
            ));
        }
        self.chunks.lock().await.insert(index, Arc::new(unit.bytes));
        Ok(())
    }

    /// Fetch and proof-check one leaf page (cached per page).
    async fn ensure_leaf_page(&self, page: u64) -> Result<(), SnapshotError> {
        if self.leaves.lock().await.contains_key(&page) {
            return Ok(());
        }
        let leaf = self
            .reader
            .client
            .chunk_map_page(
                self.reader.snapshot_id(),
                &self.path,
                &self.digest,
                &self.map,
                page,
            )
            .await?;
        self.leaves
            .lock()
            .await
            .insert(page, Arc::new(leaf.chunk_sha256));
        Ok(())
    }
}

impl VerifiedChunkMap {
    /// Length of chunk `index` derived from the map (spec 07 §4).
    pub fn chunk_len(&self, index: u64) -> Result<u64, SnapshotError> {
        let map = mst2_codec::chunkmap::ChunkMap::new(
            crate::snapshot::frames::parse_digest(&self.file_content_id)?,
            self.file_size,
            self.pages_root,
        )
        .map_err(codec_err)?;
        map.chunk_len(index).map_err(codec_err)
    }
}

fn codec_err(e: mst2_codec::CodecError) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::Internal, format!("chunk codec: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_cap_matches_the_profile_split() {
        // Spec 07 §2: ≤256 KiB uses OBJECT frames.
        assert_eq!(OBJECT_CAP, 262_144);
    }

    #[test]
    fn chunk_len_matches_the_map_rules() {
        use crate::snapshot::frames::parse_digest;
        // 2 chunks + 7 bytes: last chunk is the positive remainder.
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
}
