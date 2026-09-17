//! High-level fixed-view reader built on [`Mst2Client`].
//!
//! Resolves once, then walks the directory graph to produce a verified file
//! manifest of the fixed view. The view never moves (spec 03 §6); callers
//! bind paths, routes and handles to the snapshot id/generation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use crate::snapshot::{
    client::Mst2Client,
    types::{Capabilities, Descriptor, DirEntry, LookupResult, SnapshotError, SnapshotErrorCode},
};

/// One resolved file in the fixed view.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotFile {
    /// Scope-relative path, leading `/` stripped.
    pub rel_path: String,
    pub fs_kind: String,
    pub size: u64,
    pub content_digest: String,
}

/// Keeps the fixed view's retention lease alive across every operation that
/// reaches the server (spec 04 §4). The view itself never changes — only the
/// server-side retention claim does — so renewal is invisible to callers.
///
/// Renewal happens lazily: an operation that is about to reach the server
/// renews first when less than a third of the granted window remains. Reads
/// served from the local CAS never touch this path, so a completed mount
/// keeps working even if the lease lapses (regression-protected).
struct LeaseKeeper {
    /// Window requested (and the server's clamp is known: 1..=3600s).
    lease_seconds: u64,
    deadline: StdMutex<Instant>,
    /// One renewer at a time; the loser re-checks the deadline and proceeds.
    renewing: tokio::sync::Mutex<()>,
    /// Background renewer; aborted when the last reader clone is dropped.
    task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

impl LeaseKeeper {
    fn new(lease_seconds: u64) -> Self {
        let secs = lease_seconds.clamp(1, 3600);
        LeaseKeeper {
            lease_seconds: secs,
            deadline: StdMutex::new(Instant::now() + Duration::from_secs(secs)),
            renewing: tokio::sync::Mutex::new(()),
            task: StdMutex::new(None),
        }
    }

    fn deadline(&self) -> Instant {
        *self.deadline.lock().unwrap()
    }

    /// Renew at a third of the window remaining. The server refuses to
    /// renew an *already expired* lease, so proactive renewal (below) is
    /// what keeps a long operation alive; this lazy path is the safety net
    /// for an operation that starts close to the deadline.
    fn renew_threshold(&self) -> Duration {
        Duration::from_secs((self.lease_seconds / 3).max(1))
    }

    /// Renew if the deadline is close. A failed renewal is a typed error:
    /// the caller must not proceed to use a lapsed lease silently.
    async fn ensure(&self, client: &Mst2Client, lease_id: &str) -> Result<(), SnapshotError> {
        if Instant::now() + self.renew_threshold() < self.deadline() {
            return Ok(());
        }
        let _guard = self.renewing.lock().await;
        // Another task may have renewed while we waited.
        if Instant::now() + self.renew_threshold() < self.deadline() {
            return Ok(());
        }
        self.renew_now(client, lease_id).await
    }

    /// One unconditional renewal, updating the deadline.
    async fn renew_now(&self, client: &Mst2Client, lease_id: &str) -> Result<(), SnapshotError> {
        let renewed = client.renew_lease(lease_id, self.lease_seconds).await?;
        let expires = renewed
            .get("lease_expires_at")
            .and_then(|v| v.as_str())
            .and_then(parse_rfc3339_unix);
        let next = match expires {
            Some(unix) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let remaining = unix.saturating_sub(now).max(1);
                Instant::now() + Duration::from_secs(remaining)
            }
            // Unparseable expiry: fall back to the requested window (the
            // server never grants more than it was asked for).
            None => Instant::now() + Duration::from_secs(self.lease_seconds),
        };
        *self.deadline.lock().unwrap() = next;
        Ok(())
    }

    /// Start the background renewer. It renews every `lease/3` seconds for
    /// as long as any clone of the reader is alive, so an operation that
    /// outlives the initial window (a long hydrate) never lapses mid-way.
    /// A renewal failure stops the loop: the lease is gone, and the next
    /// source operation will surface the typed error.
    fn spawn(self: &Arc<Self>, client: Mst2Client, lease_id: String) {
        let keeper = Arc::clone(self);
        let handle = tokio::spawn(async move {
            let period = Duration::from_secs((keeper.lease_seconds / 3).max(1));
            loop {
                tokio::time::sleep(period).await;
                if let Err(e) = keeper.renew_now(&client, &lease_id).await {
                    tracing::warn!(error = %e, lease = %lease_id, "lease renewal failed; stopping renewer");
                    return;
                }
            }
        });
        *self.task.lock().unwrap() = Some(handle);
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        if let Some(h) = self.task.lock().unwrap().take() {
            h.abort();
        }
    }
}

/// Minimal RFC3339 (`YYYY-MM-DDTHH:MM:SSZ`) → unix seconds. Only the exact
/// shape this deployment emits is accepted; anything else returns `None` and
/// the caller falls back to the requested window.
fn parse_rfc3339_unix(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.len() != 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'Z'
    {
        return None;
    }
    let num = |from: usize, to: usize| -> Option<u64> {
        std::str::from_utf8(&b[from..to]).ok()?.parse::<u64>().ok()
    };
    let (y, mo, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (h, mi, sec) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    // days_from_civil (Howard Hinnant), matching runtime.rs' inverse.
    let y_adj = if mo <= 2 { y as i64 - 1 } else { y as i64 };
    let era = y_adj.div_euclid(400);
    let yoe = y_adj - era * 400;
    let mp = (mo as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    if days < 0 {
        return None;
    }
    Some(days as u64 * 86_400 + h * 3600 + mi * 60 + sec)
}

/// A fixed view plus everything needed to read its content.
#[derive(Clone)]
pub struct SnapshotReader {
    pub client: Mst2Client,
    pub descriptor: Descriptor,
    pub lease_id: String,
    caps: Capabilities,
    lease: Arc<LeaseKeeper>,
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
        let lease = Arc::new(LeaseKeeper::new(lease_seconds));
        // Keep the retention claim alive for as long as this reader lives
        // (a hydrate or mount may outlast the initial window). Outside a
        // runtime there is nothing to spawn onto; the lazy path in
        // `ensure_lease` still covers operations that start near expiry.
        if tokio::runtime::Handle::try_current().is_ok() {
            lease.spawn(client.clone(), res.lease_id.clone());
        }
        Ok(Self {
            client,
            descriptor: res.descriptor,
            lease_id: res.lease_id,
            caps,
            lease,
        })
    }

    /// Renew the view's retention lease if it is close to expiry. Called
    /// automatically before every operation that reaches the server; exposed
    /// so a mount can also keep it warm while idle.
    pub async fn ensure_lease(&self) -> Result<(), SnapshotError> {
        self.lease.ensure(&self.client, &self.lease_id).await
    }

    /// Capabilities captured at resolve time; callers gate frame
    /// transports on these rather than assuming the server profile.
    pub fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    /// Negotiated content encoding for frame responses, exposed for the
    /// range reader (which drives the client directly).
    pub fn encoding_hint(&self) -> Option<&'static str> {
        self.content_encoding()
    }

    /// Negotiated content encoding for frame responses: zstd when the
    /// deployment advertises it, otherwise identity (`None`).
    fn content_encoding(&self) -> Option<&'static str> {
        if self.caps.frame_encodings.iter().any(|e| e == "zstd") {
            Some("zstd")
        } else {
            None
        }
    }

    pub fn snapshot_id(&self) -> &str {
        &self.descriptor.snapshot_id
    }

    /// Batch lookup of scope-relative paths.
    pub async fn lookup(&self, paths: &[String]) -> Result<Vec<LookupResult>, SnapshotError> {
        self.ensure_lease().await?;
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
        self.ensure_lease().await?;
        self.client
            .blob_verified(self.snapshot_id(), &request_path, digest)
            .await
    }

    /// One directory, following the cursor to the end, so callers see every
    /// entry with the directory's own `directory_root` (spec 04 §6: the
    /// cursor chain is the complete enumeration, never a silent first page).
    pub async fn directory_page(
        &self,
        dir: &str,
        limit: u32,
    ) -> Result<crate::snapshot::types::DirectoryResponse, SnapshotError> {
        self.ensure_lease().await?;
        let mut cursor: Option<String> = None;
        let mut merged: Option<crate::snapshot::types::DirectoryResponse> = None;
        loop {
            let page = self
                .client
                .directory(self.snapshot_id(), dir, limit, cursor.as_deref())
                .await?;
            match &mut merged {
                None => merged = Some(page.clone()),
                Some(acc) => {
                    if acc.directory_root != page.directory_root {
                        return Err(SnapshotError::new(
                            SnapshotErrorCode::CursorStale,
                            "directory_root changed mid-enumeration",
                        ));
                    }
                    acc.entries.extend(page.entries.clone());
                    acc.next_cursor = page.next_cursor.clone();
                }
            }
            match page.next_cursor {
                None => return Ok(merged.expect("first page always merged")),
                Some(c) => cursor = Some(c),
            }
        }
    }

    /// Walk the whole scope via paginated `directory`, collecting files.
    ///
    /// Missing a page after a server-advertised cursor is an error; empty
    /// results are only accepted at real EOF (`next_cursor = null`).
    pub async fn file_manifest(&self) -> Result<Vec<SnapshotFile>, SnapshotError> {
        self.ensure_lease().await?;
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
        self.ensure_lease().await?;
        let sid = self.snapshot_id();
        let request_path = if rel_path.starts_with('/') {
            rel_path.to_string()
        } else {
            format!("/{rel_path}")
        };
        if size <= 256 * 1024 {
            let map = self
                .client
                .objects(
                    sid,
                    &[(request_path, digest.to_string())],
                    self.content_encoding(),
                )
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
            for unit in self
                .client
                .chunks(sid, &items, self.content_encoding())
                .await?
            {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_parser_matches_known_values() {
        assert_eq!(parse_rfc3339_unix("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_rfc3339_unix("2026-09-16T02:28:42Z"),
            Some(1_789_525_722)
        );
        assert_eq!(
            parse_rfc3339_unix("2024-02-29T23:59:59Z"),
            Some(1_709_251_199)
        );
    }

    #[test]
    fn rfc3339_parser_rejects_non_canonical_shapes() {
        for bad in [
            "",
            "2026-09-16T02:28:42",       // missing Z
            "2026-09-16 02:28:42Z",      // space separator
            "2026-13-01T00:00:00Z",      // month 13
            "2026-09-16T24:00:00Z",      // hour 24
            "2026-09-16T02:28:42+08:00", // offset form not emitted here
        ] {
            assert_eq!(parse_rfc3339_unix(bad), None, "{bad} must be rejected");
        }
    }

    #[test]
    fn lease_keeper_renews_only_near_expiry() {
        // A fresh keeper with a comfortable window does not need renewal;
        // with the window exhausted it does. `ensure` against a real client
        // is covered by the live e2e; here we pin the threshold arithmetic.
        let k = LeaseKeeper::new(60);
        assert!(Instant::now() + k.renew_threshold() < k.deadline());
        *k.deadline.lock().unwrap() = Instant::now() + Duration::from_secs(5);
        assert!(Instant::now() + k.renew_threshold() >= k.deadline());
        // The window is clamped to the server's 1..=3600 policy.
        assert_eq!(LeaseKeeper::new(99_999).lease_seconds, 3600);
        assert_eq!(LeaseKeeper::new(0).lease_seconds, 1);
    }
}
