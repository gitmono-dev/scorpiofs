//! High-level fixed-view reader built on [`Mst2Client`].
//!
//! Resolves once, then walks the directory graph to produce a verified file
//! manifest of the fixed view. The view never moves (spec 03 §6); callers
//! bind paths, routes and handles to the snapshot id/generation.

use std::collections::HashMap;

use crate::snapshot::{
    client::Mst2Client,
    types::{Capabilities, Descriptor, DirEntry, LookupResult, SnapshotError, SnapshotErrorCode},
};

/// One resolved file in the fixed view.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotFile {
    /// Scope-relative path, leading `/` stripped.
    pub rel_path: String,
    pub fs_kind: String,
    pub size: u64,
    pub content_digest: String,
}

/// A fixed view plus everything needed to read its content.
#[derive(Clone)]
pub struct SnapshotReader {
    pub client: Mst2Client,
    pub descriptor: Descriptor,
    pub lease_id: String,
    caps: Capabilities,
}

impl SnapshotReader {
    /// Resolve `latest` for `scope` and return a bound reader.
    pub async fn resolve(
        client: Mst2Client,
        scope: &str,
        lease_seconds: u64,
    ) -> Result<Self, SnapshotError> {
        let caps = client.capabilities().await?;
        if !caps.features.resolve || !caps.features.directory {
            return Err(SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "deployment does not serve resolve/directory",
            ));
        }
        if !caps.metadata_codecs.contains(&1) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::ScopeInvalid,
                "server does not support metadata codec 1",
            ));
        }
        let res = client.resolve(scope, lease_seconds).await?;
        Ok(Self {
            client,
            descriptor: res.descriptor,
            lease_id: res.lease_id,
            caps,
        })
    }

    /// Capabilities captured at resolve time; callers gate frame
    /// transports on these rather than assuming the server profile.
    pub fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    pub fn snapshot_id(&self) -> &str {
        &self.descriptor.snapshot_id
    }

    /// Batch lookup of scope-relative paths.
    pub async fn lookup(&self, paths: &[String]) -> Result<Vec<LookupResult>, SnapshotError> {
        Ok(self.client.lookup(self.snapshot_id(), paths).await?.results)
    }

    /// Fetch one file's verified bytes (digest checked on both server and
    /// client sides).
    pub async fn read_file(&self, rel_path: &str, digest: &str) -> Result<Vec<u8>, SnapshotError> {
        let request_path = if rel_path.is_empty() || rel_path == "/" {
            "/".to_string()
        } else if rel_path.starts_with('/') {
            rel_path.to_string()
        } else {
            format!("/{rel_path}")
        };
        self.client
            .blob_verified(self.snapshot_id(), &request_path, digest)
            .await
    }

    /// Walk the whole scope via paginated `directory`, collecting files.
    ///
    /// Missing a page after a server-advertised cursor is an error; empty
    /// results are only accepted at real EOF (`next_cursor = null`).
    pub async fn file_manifest(&self) -> Result<Vec<SnapshotFile>, SnapshotError> {
        let mut out = Vec::new();
        self.walk_dir("/", &mut out).await?;
        Ok(out)
    }

    async fn walk_dir(&self, dir: &str, out: &mut Vec<SnapshotFile>) -> Result<(), SnapshotError> {
        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .client
                .directory(self.snapshot_id(), dir, 256, cursor.as_deref())
                .await?;
            for e in page.entries {
                let rel = if dir == "/" {
                    e.name.clone()
                } else {
                    format!("{}/{}", dir.trim_start_matches('/'), e.name)
                };
                if e.directory_root.is_some() {
                    Box::pin(self.walk_dir(&format!("/{rel}"), out)).await?;
                } else if let Some(digest) = e.content_digest {
                    let size = e.size.as_deref().unwrap_or("0").parse().map_err(|_| {
                        SnapshotError::new(
                            SnapshotErrorCode::Internal,
                            format!("non-numeric size for {rel}"),
                        )
                    })?;
                    out.push(SnapshotFile {
                        rel_path: rel,
                        fs_kind: e.fs_kind,
                        size,
                        content_digest: digest,
                    });
                } else {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::Internal,
                        format!("file entry {rel} missing content_digest"),
                    ));
                }
            }
            match page.next_cursor {
                None => return Ok(()),
                Some(c) => cursor = Some(c),
            }
        }
    }

    /// Fetch one file through the frame surface (spec 04 §9 / spec 07):
    /// OBJECT batch for files ≤256 KiB, Chunk Map + CHUNK frames above that.
    ///
    /// Every chunk is hash-checked against the map's leaf and the assembled
    /// file is re-hashed before it is returned — verified chunks alone never
    /// make a verified file (spec 07 §7).
    pub async fn read_file_frames(
        &self,
        rel_path: &str,
        digest: &str,
        size: u64,
    ) -> Result<Vec<u8>, SnapshotError> {
        let sid = self.snapshot_id();
        let request_path = if rel_path.starts_with('/') {
            rel_path.to_string()
        } else {
            format!("/{rel_path}")
        };
        if size <= 256 * 1024 {
            let map = self
                .client
                .objects(sid, &[(request_path, digest.to_string())])
                .await?;
            let want = crate::snapshot::frames::parse_digest(digest)?;
            let bytes = map.get(&want).cloned().ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    "objects response missing the requested unit",
                )
            })?;
            if bytes.len() as u64 != size {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    format!("{rel_path}: size {} != advertised {size}", bytes.len()),
                ));
            }
            if crate::snapshot::durable::digest_of(&bytes) != digest {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    format!("{rel_path}: whole-object rehash mismatch"),
                ));
            }
            return Ok(bytes);
        }

        // Large file: verify the map binding, every leaf proof, every chunk
        // hash, then the whole-file hash.
        let map = self.client.chunk_map(sid, &request_path, digest).await?;
        let mut chunk_hashes: Vec<[u8; 32]> = Vec::with_capacity(map.chunk_count as usize);
        for page_index in 0..map.page_count {
            let leaf = self
                .client
                .chunk_map_page(sid, &request_path, digest, &map, page_index)
                .await?;
            chunk_hashes.extend(leaf.chunk_sha256);
        }
        if chunk_hashes.len() as u64 != map.chunk_count {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "chunk map pages did not cover chunk_count",
            ));
        }
        let file_id = crate::snapshot::frames::parse_digest(digest)?;
        let length_map = mst2_codec::chunkmap::ChunkMap::new(file_id, size, map.pages_root)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;

        let map_id = map.map_id.clone();
        let mut out: Vec<Option<Vec<u8>>> = (0..map.chunk_count).map(|_| None).collect();
        let mut indices: Vec<u64> = (0..map.chunk_count).collect();
        while !indices.is_empty() {
            let take = 128.min(indices.len());
            let batch: Vec<u64> = indices.drain(..take).collect();
            let items: Vec<crate::snapshot::frames::ChunkRequest> = batch
                .iter()
                .map(|i| crate::snapshot::frames::ChunkRequest {
                    path: request_path.clone(),
                    expected_digest: digest.to_string(),
                    map_id: map_id.clone(),
                    chunk_index: *i,
                })
                .collect();
            for unit in self.client.chunks(sid, &items).await? {
                if unit.chunk_index >= map.chunk_count {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        "chunk index outside the map",
                    ));
                }
                let idx = unit.chunk_index as usize;
                let want_len = length_map.chunk_len(unit.chunk_index).map_err(|e| {
                    SnapshotError::new(SnapshotErrorCode::DigestMismatch, e.to_string())
                })?;
                if unit.bytes.len() as u64 != want_len {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        format!("chunk {idx} length {}", unit.bytes.len()),
                    ));
                }
                if crate::snapshot::durable::digest_of(&unit.bytes).as_str()
                    != format!(
                        "sha256:{}",
                        crate::snapshot::frames::hex32(&chunk_hashes[idx])
                    )
                {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        format!("chunk {idx} hash mismatch"),
                    ));
                }
                out[idx] = Some(unit.bytes);
            }
        }
        let total: usize = out
            .iter()
            .map(|c| c.as_ref().map(|v| v.len()).unwrap_or(0))
            .sum();
        if total as u64 != size {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "assembled size disagrees with the map",
            ));
        }
        let mut assembled = Vec::with_capacity(total);
        for c in out {
            assembled.extend_from_slice(&c.ok_or_else(|| {
                SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    "missing chunk after the response completed",
                )
            })?);
        }
        if crate::snapshot::durable::digest_of(&assembled) != digest {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("{rel_path}: assembled file rehash mismatch"),
            ));
        }
        Ok(assembled)
    }

    /// Convenience: manifest keyed by scope-relative path.
    pub async fn file_map(&self) -> Result<HashMap<String, SnapshotFile>, SnapshotError> {
        Ok(self
            .file_manifest()
            .await?
            .into_iter()
            .map(|f| (f.rel_path.clone(), f))
            .collect())
    }
}

/// Kept for potential future use of directory entry inspection.
#[allow(dead_code)]
fn _entry_marker(_e: &DirEntry) {}
