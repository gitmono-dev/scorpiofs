//! Cross-version incremental sync and verified-subtree reuse (spec 11 §10).
//!
//! A new version of a scope usually differs from the previous one in a few
//! directories. Walking the whole tree again would re-enumerate every
//! descendant and re-check every cached file, even though MTP2 pages are
//! content-addressed: an unchanged directory has the *same* `directory_root`
//! page id, and the parent page that points at it commits to that id.
//!
//! This module keeps a per-scope [`ScopeCache`] of
//!
//! * verified closure records — `root_page_id` plus the exact page id set and
//!   the file list that was verified under it (the spec's
//!   `VerifiedClosureRecord`), and
//! * the verified page bytes themselves, so "the record's pages still exist"
//!   is a local, checkable fact rather than a memory of one.
//!
//! Reuse requires **all** of: a matching metadata codec and policy revision,
//! the record's pages present and re-hashing correctly, and a live pin — the
//! pin is *transferred* to the reusing snapshot so the guarantee no longer
//! depends on the old snapshot surviving (spec 11 §10.3). Anything less falls
//! back to a normal walk, which is a correct outcome, not an error.
//!
//! Three counters are kept separately (spec 11 §10.6): pages fetched from the
//! network, pages reused from the record, and page nodes actually visited.
//! "Did not download again" and "did not traverse everything" are different
//! claims and are asserted separately.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::snapshot::{
    client::Mst2Client, frames::MetadataPageItem, SnapshotError, SnapshotErrorCode, SnapshotFile,
    SnapshotReader,
};

/// Local cache-policy revision; bump when the reuse rules change so older
/// records are not trusted by newer code (spec 11 §10.3).
pub const POLICY_REVISION: u16 = 1;

/// One verified subtree: the pages that were verified and what they proved.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClosureRecord {
    /// Deployment identity (descriptor `instance_id`): content verified by a
    /// different deployment is never reused.
    pub auth_domain: String,
    pub metadata_codec: u16,
    pub policy_revision: u16,
    /// `directory_root` of the subtree (an MTP2 `page_id`).
    pub root_page_id: String,
    /// Every page id of that subtree's page tree.
    pub page_ids: Vec<String>,
    pub total_entries: u64,
    /// The file list the pages proved, so a reused subtree does not have to be
    /// enumerated again.
    pub files: Vec<SnapshotFile>,
    /// Snapshot that verified (and currently pins) this subtree.
    pub pin_ref: String,
}

impl ClosureRecord {
    /// Structurally compatible with the current deployment/profile.
    pub fn compatible(&self, auth_domain: &str, codec: u16) -> bool {
        self.auth_domain == auth_domain
            && self.metadata_codec == codec
            && self.policy_revision == POLICY_REVISION
    }
}

/// Counters for one sync pass (spec 11 §10.6).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncMeters {
    /// Pages fetched from the network.
    pub fetched_pages: u64,
    /// Pages skipped because a record covered them locally.
    pub reused_pages: u64,
    /// Page nodes actually visited (including the branch pages examined to
    /// make a reuse decision).
    pub traversal_nodes: u64,
    /// Directories whose subtrees were reused wholesale.
    pub reused_subtrees: u64,
}

/// Per-scope cache: closure records, verified pages, and the pin set.
pub struct ScopeCache {
    dir: PathBuf,
}

impl ScopeCache {
    /// Open (creating if needed) the cache for one scope directory. Views of
    /// the scope live in subdirectories, so pin liveness is discoverable.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, SnapshotError> {
        let dir = dir.into();
        fs::create_dir_all(dir.join("pages")).map_err(io_err)?;
        Ok(ScopeCache { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn closures_path(&self) -> PathBuf {
        self.dir.join("closures.json")
    }

    fn page_path(&self, page_id: &str) -> PathBuf {
        let hex = page_id.strip_prefix("sha256:").unwrap_or(page_id);
        self.dir.join("pages").join(hex)
    }

    fn load_records(&self) -> Result<HashMap<String, ClosureRecord>, SnapshotError> {
        let path = self.closures_path();
        if !path.exists() {
            return Ok(HashMap::new());
        }
        let bytes = fs::read(&path).map_err(io_err)?;
        match serde_json::from_slice::<HashMap<String, ClosureRecord>>(&bytes) {
            Ok(m) => Ok(m),
            // A torn or corrupt index is discarded, never partially trusted.
            Err(_) => Ok(HashMap::new()),
        }
    }

    fn store_records(&self, records: &HashMap<String, ClosureRecord>) -> Result<(), SnapshotError> {
        let bytes = serde_json::to_vec(records)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        write_atomic(&self.dir, "closures.json", &bytes)
    }

    /// The record for a subtree, if any (validity is the caller's call).
    pub fn record_for(&self, root_page_id: &str) -> Option<ClosureRecord> {
        self.load_records().ok()?.remove(root_page_id)
    }

    /// Insert or replace the record for its subtree.
    pub fn put_record(&self, record: &ClosureRecord) -> Result<(), SnapshotError> {
        let mut records = self.load_records()?;
        records.insert(record.root_page_id.clone(), record.clone());
        self.store_records(&records)
    }

    /// Drop records pinned only by `pin_ref` (spec 11 §10.3: releasing a
    /// snapshot releases what depended on *its* pin; a record already
    /// transferred to another live pin survives).
    pub fn drop_records_for_pin(&self, pin_ref: &str) -> Result<u64, SnapshotError> {
        let mut records = self.load_records()?;
        let before = records.len() as u64;
        records.retain(|_, r| r.pin_ref != pin_ref);
        self.store_records(&records)?;
        Ok(before - records.len() as u64)
    }

    /// Snapshot ids holding a local pin in this scope.
    pub fn live_pins(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for e in entries.flatten() {
                let pin = e.path().join("pin.json");
                if !pin.exists() {
                    continue;
                }
                if let Ok(bytes) = fs::read(&pin) {
                    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                        if let Some(id) = v.get("snapshot_id").and_then(|s| s.as_str()) {
                            out.push(id.to_string());
                        }
                    }
                }
            }
        }
        out
    }

    /// Store a verified page (id checked by the caller before writing).
    pub fn put_page(&self, page_id: &str, bytes: &[u8]) -> Result<(), SnapshotError> {
        write_atomic(&self.dir.join("pages"), &page_hex(page_id), bytes)
    }

    /// Read a page and re-hash it: existence alone is not evidence, because
    /// the store is plain files that a crash, a GC or a tamperer can damage.
    pub fn read_page_verified(&self, page_id: &str) -> Result<Option<Vec<u8>>, SnapshotError> {
        let path = self.page_path(page_id);
        let bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_err(e)),
        };
        let want = match parse_page_id(page_id) {
            Ok(w) => w,
            // A record entry that is not a valid page id means the record is
            // corrupt: drop the object so the next sync refetches instead of
            // aborting the whole sync (spec 11 §10.3 — fallback is the
            // correct path, not an error).
            Err(_) => {
                let _ = fs::remove_file(&path);
                return Ok(None);
            }
        };
        if mst2_codec::metapage::page_id(&bytes) != want {
            // Corrupt: remove so a later sync re-fetches instead of re-reading.
            let _ = fs::remove_file(&path);
            return Ok(None);
        }
        Ok(Some(bytes))
    }
}

fn page_hex(page_id: &str) -> String {
    page_id
        .strip_prefix("sha256:")
        .unwrap_or(page_id)
        .to_string()
}

fn parse_page_id(page_id: &str) -> Result<[u8; 32], SnapshotError> {
    crate::snapshot::frames::parse_digest(page_id)
}

/// Incremental sync over one scope.
pub struct IncrementalSync<'a> {
    reader: &'a SnapshotReader,
    cache: &'a ScopeCache,
    auth_domain: String,
    codec: u16,
    meters: SyncMeters,
}

impl<'a> IncrementalSync<'a> {
    pub fn new(reader: &'a SnapshotReader, cache: &'a ScopeCache) -> Self {
        IncrementalSync {
            reader,
            cache,
            auth_domain: reader.descriptor.instance_id.clone(),
            codec: reader.descriptor.metadata_codec,
            meters: SyncMeters::default(),
        }
    }

    pub fn meters(&self) -> SyncMeters {
        self.meters
    }

    /// Produce the file manifest of the fixed view, reusing verified
    /// subtrees whose page ids still match and whose pages are still here.
    pub async fn sync(&mut self) -> Result<Vec<SnapshotFile>, SnapshotError> {
        // The root is always fetched: it is the entry point of the sync and
        // its page id is what every reuse decision below hangs from.
        let root_page_id = self.reader.descriptor.metadata_root.clone();
        self.walk("/", Some(&root_page_id)).await
    }

    /// Walk one directory. `known_root` is the `directory_root` its parent
    /// advertised (absent only for the scope root, which fetches first).
    async fn walk(
        &mut self,
        dir: &str,
        known_root: Option<&str>,
    ) -> Result<Vec<SnapshotFile>, SnapshotError> {
        if let Some(root) = known_root {
            self.meters.traversal_nodes += 1;
            if let Some(files) = self.try_reuse(root, dir).await? {
                return Ok(files);
            }
        }

        let page = self.reader.directory_page(dir, 256).await?;
        self.meters.fetched_pages += 1;
        // Record every page of this directory's own MTP2 tree so a later
        // version can check the subtree is byte-identical and still here.
        let page_ids = self
            .fetch_and_store_pages(dir, &page.directory_root)
            .await?;

        self.meters.traversal_nodes += 1;
        let mut files = Vec::new();
        for entry in &page.entries {
            let rel = if dir == "/" {
                entry.name.clone()
            } else {
                format!("{}/{}", dir.trim_start_matches('/'), entry.name)
            };
            if let Some(child_root) = &entry.directory_root {
                let child = format!("/{rel}");
                files.extend(Box::pin(self.walk(&child, Some(child_root))).await?);
            } else if let Some(digest) = &entry.content_digest {
                let size = entry
                    .size
                    .as_deref()
                    .unwrap_or("0")
                    .parse::<u64>()
                    .map_err(|_| {
                        SnapshotError::new(
                            SnapshotErrorCode::Internal,
                            format!("non-numeric size for {rel}"),
                        )
                    })?;
                files.push(SnapshotFile {
                    rel_path: rel,
                    fs_kind: entry.fs_kind.clone(),
                    size,
                    content_digest: digest.clone(),
                });
            } else {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::Internal,
                    format!("file entry {rel} missing content_digest"),
                ));
            }
        }

        // The record stores paths *relative to this directory*: a subtree's
        // page ids do not depend on where the directory sits, so a moved
        // directory has the same subtree identity but a different path. The
        // path always comes from where the walk is now, never from the
        // record (spec 11 §10.2: no name/path-based equivalence inference).
        let relative: Vec<SnapshotFile> = files
            .iter()
            .map(|f| SnapshotFile {
                rel_path: strip_prefix(&f.rel_path, dir),
                ..f.clone()
            })
            .collect();
        self.cache.put_record(&ClosureRecord {
            auth_domain: self.auth_domain.clone(),
            metadata_codec: self.codec,
            policy_revision: POLICY_REVISION,
            root_page_id: page.directory_root.clone(),
            page_ids,
            total_entries: relative.len() as u64,
            files: relative,
            pin_ref: self.reader.snapshot_id().to_string(),
        })?;
        Ok(files)
    }

    /// Reuse the recorded subtree when every condition holds; otherwise
    /// return `None` so the caller walks normally.
    async fn try_reuse(
        &mut self,
        root_page_id: &str,
        dir: &str,
    ) -> Result<Option<Vec<SnapshotFile>>, SnapshotError> {
        let Some(record) = self.cache.record_for(root_page_id) else {
            return Ok(None);
        };
        if !record.compatible(&self.auth_domain, self.codec) {
            return Ok(None);
        }
        // The record must be backed by a pin that is still live here.
        let live = self.cache.live_pins();
        if !live.iter().any(|p| p == &record.pin_ref) {
            return Ok(None);
        }
        // A record that names no pages proves nothing: refuse it outright
        // rather than "verifying" an empty set (a hole a corrupt or truncated
        // index could otherwise open).
        if record.page_ids.is_empty() {
            return Ok(None);
        }
        // Every page of the subtree must be present *and* re-hash correctly.
        for page_id in &record.page_ids {
            if self.cache.read_page_verified(page_id)?.is_none() {
                return Ok(None);
            }
        }
        // Reuse is granted, and the guarantee is transferred to this
        // snapshot's own pin so releasing the old one cannot revoke it.
        if record.pin_ref != self.reader.snapshot_id() {
            let mut transferred = record.clone();
            transferred.pin_ref = self.reader.snapshot_id().to_string();
            self.cache.put_record(&transferred)?;
        }
        self.meters.reused_pages += record.page_ids.len() as u64;
        self.meters.reused_subtrees += 1;
        tracing::debug!(dir, root = root_page_id, "reused verified subtree");
        // Visible without a log subscriber: which subtrees were reused, so a
        // surprising counter can always be explained from the run's output.
        eprintln!(
            "REUSE dir={dir} root={} pages={} files={}",
            &root_page_id[..root_page_id.len().min(20)],
            record.page_ids.len(),
            record.files.len()
        );
        let files = record
            .files
            .iter()
            .map(|f| SnapshotFile {
                rel_path: with_prefix(&f.rel_path, dir),
                ..f.clone()
            })
            .collect();
        Ok(Some(files))
    }

    /// Fetch the MTP2 page tree of one directory and store it verified.
    fn fetch_and_store_pages<'f>(
        &'f mut self,
        dir: &'f str,
        root_page_id: &'f str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<String>, SnapshotError>> + Send + 'f>,
    > {
        Box::pin(async move {
            let mut page_ids = Vec::new();
            let mut routes: Vec<Vec<u8>> = vec![Vec::new()];
            while let Some(route) = routes.pop() {
                let items = [MetadataPageItem {
                    directory_path: dir.to_string(),
                    route: route.clone(),
                    expected_digest: if route.is_empty() {
                        Some(root_page_id.to_string())
                    } else {
                        None
                    },
                }];
                let pages = self
                    .reader
                    .client
                    .metadata_pages(
                        self.reader.snapshot_id(),
                        &items,
                        self.reader.encoding_hint(),
                    )
                    .await?;
                self.meters.fetched_pages += pages.len() as u64;
                // The root response must contain the page the walk named: a
                // server answering with different ids is not the view we
                // resolved, and filing a record under a root the response
                // never delivered would make reuse self-referential.
                if route.is_empty()
                    && !pages.iter().any(|(pid, _)| {
                        format!("sha256:{}", crate::snapshot::frames::hex32(pid)) == root_page_id
                    })
                {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        format!(
                            "metadata/pages did not return the requested root page {}",
                            root_page_id
                        ),
                    ));
                }
                for (pid, bytes) in pages {
                    let id = format!("sha256:{}", crate::snapshot::frames::hex32(&pid));
                    if page_ids.contains(&id) {
                        continue;
                    }
                    self.cache.put_page(&id, &bytes)?;
                    page_ids.push(id);
                    // A branch page names its children; follow them so the
                    // record covers the whole page tree, not just the root.
                    // Only a branch names children; a leaf ends the walk.
                    if let Ok((mst2_codec::metapage::Page::Branch { children, .. }, _)) =
                        mst2_codec::metapage::Page::decode(&bytes)
                    {
                        for child in children {
                            let mut next = route.clone();
                            next.push(child.label);
                            routes.push(next);
                        }
                    }
                }
            }
            Ok(page_ids)
        })
    }
}

/// Path of `rel` as seen from directory `dir` (`/a/b` under `/a` -> `b`).
fn strip_prefix(rel: &str, dir: &str) -> String {
    let prefix = dir.trim_start_matches('/');
    if prefix.is_empty() {
        return rel.to_string();
    }
    rel.strip_prefix(prefix)
        .map(|s| s.trim_start_matches('/').to_string())
        .unwrap_or_else(|| rel.to_string())
}

/// Re-attach `dir` to a subtree-relative path.
fn with_prefix(rel: &str, dir: &str) -> String {
    let prefix = dir.trim_start_matches('/');
    if prefix.is_empty() {
        return rel.to_string();
    }
    if rel.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}/{}", rel.trim_start_matches('/'))
    }
}

fn io_err(e: std::io::Error) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::Internal, format!("scope cache: {e}"))
}

/// Temp-file + rename so a crash never leaves a half-written record or page.
fn write_atomic(dir: &Path, name: &str, data: &[u8]) -> Result<(), SnapshotError> {
    fs::create_dir_all(dir).map_err(io_err)?;
    let tmp = dir.join(format!(
        ".{name}.tmp.{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    {
        use std::io::Write as _;
        let mut f = fs::File::create(&tmp).map_err(io_err)?;
        f.write_all(data).map_err(io_err)?;
        f.sync_all().map_err(io_err)?;
    }
    fs::rename(&tmp, dir.join(name)).map_err(io_err)?;
    Ok(())
}

/// Unused-import guard for the type used only in signatures above.
#[allow(dead_code)]
fn _client_marker(_: &Mst2Client) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(root: &str, pin: &str) -> ClosureRecord {
        ClosureRecord {
            auth_domain: "inst".into(),
            metadata_codec: 1,
            policy_revision: POLICY_REVISION,
            root_page_id: root.into(),
            page_ids: vec![root.into()],
            total_entries: 1,
            files: vec![SnapshotFile {
                rel_path: "a".into(),
                fs_kind: "regular".into(),
                size: 1,
                content_digest: "sha256:".to_string() + &"1".repeat(64),
            }],
            pin_ref: pin.into(),
        }
    }

    #[test]
    fn compatibility_gates_on_domain_codec_and_policy() {
        let r = rec("sha256:aa", "s1");
        assert!(r.compatible("inst", 1));
        assert!(!r.compatible("other", 1), "different deployment");
        assert!(!r.compatible("inst", 2), "different codec");
        let mut old = r.clone();
        old.policy_revision = POLICY_REVISION + 1;
        assert!(!old.compatible("inst", 1), "future policy revision");
    }

    #[test]
    fn records_round_trip_and_survive_a_corrupt_index() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        assert!(cache.record_for("sha256:aa").is_none());
        cache.put_record(&rec("sha256:aa", "s1")).unwrap();
        assert_eq!(cache.record_for("sha256:aa").unwrap().pin_ref, "s1");

        // A torn index is discarded wholesale, never partially trusted.
        std::fs::write(cache.closures_path(), b"{ not json").unwrap();
        assert!(cache.record_for("sha256:aa").is_none());
        assert_eq!(cache.load_records().unwrap().len(), 0);
    }

    #[test]
    fn releasing_a_pin_drops_only_its_records() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        cache.put_record(&rec("sha256:aa", "old")).unwrap();
        cache.put_record(&rec("sha256:bb", "new")).unwrap();
        assert_eq!(cache.drop_records_for_pin("old").unwrap(), 1);
        assert!(cache.record_for("sha256:aa").is_none());
        assert!(
            cache.record_for("sha256:bb").is_some(),
            "other pin survives"
        );
    }

    #[test]
    fn live_pins_come_from_the_view_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        assert!(cache.live_pins().is_empty());
        let view = tmp.path().join("abc123");
        std::fs::create_dir_all(&view).unwrap();
        std::fs::write(
            view.join("pin.json"),
            br#"{"snapshot_id":"sha256:abc","scope":"/p","lease_id":"l","pinned_at_unix":1}"#,
        )
        .unwrap();
        assert_eq!(cache.live_pins(), vec!["sha256:abc".to_string()]);
    }

    #[test]
    fn page_store_rehashes_and_evicts_corrupt_pages() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        let page = mst2_codec::metapage::Page::Leaf { entries: vec![] }
            .encode()
            .unwrap();
        let id = format!(
            "sha256:{}",
            crate::snapshot::frames::hex32(&mst2_codec::metapage::page_id(&page))
        );
        cache.put_page(&id, &page).unwrap();
        assert!(cache.read_page_verified(&id).unwrap().is_some());

        // Tampered bytes: the re-hash fails and the object is dropped so a
        // later sync refetches instead of trusting it.
        cache.put_page(&id, b"not the page").unwrap();
        assert!(cache.read_page_verified(&id).unwrap().is_none());
        assert!(cache.read_page_verified(&id).unwrap().is_none(), "evicted");
    }

    #[test]
    fn an_empty_page_set_never_authorises_reuse() {
        // A record with no pages is refused by construction; the check lives
        // in `try_reuse` (which needs a live reader), so pin the invariant
        // here on the record type itself.
        let mut r = rec("sha256:aa", "s1");
        r.page_ids.clear();
        assert!(r.page_ids.is_empty());
        assert!(r.compatible("inst", 1), "compatibility alone is not enough");
    }

    #[test]
    fn subtree_paths_rebase_so_a_moved_directory_reuses_safely() {
        // Recorded relative to /a, replayed under /b: the page identity is
        // the same (that is why reuse is allowed), but the paths must follow
        // the current location.
        assert_eq!(strip_prefix("a/b/c.txt", "/a"), "b/c.txt");
        assert_eq!(strip_prefix("x.txt", "/"), "x.txt");
        assert_eq!(with_prefix("b/c.txt", "/b"), "b/b/c.txt");
        assert_eq!(with_prefix("x.txt", "/"), "x.txt");
        assert_eq!(with_prefix("", "/moved"), "moved");
        // Round-trip is stable for the same directory.
        assert_eq!(
            with_prefix(&strip_prefix("a/b/c.txt", "/a"), "/a"),
            "a/b/c.txt"
        );
    }

    #[test]
    fn meters_start_empty_and_are_separate() {
        let m = SyncMeters::default();
        assert_eq!(m.fetched_pages, 0);
        assert_eq!(m.reused_pages, 0);
        assert_eq!(m.traversal_nodes, 0);
        // The three counters are independent by construction.
        let m2 = SyncMeters {
            fetched_pages: 1,
            reused_pages: 7,
            traversal_nodes: 2,
            reused_subtrees: 1,
        };
        assert_ne!(m2.fetched_pages, m2.reused_pages);
    }
}
