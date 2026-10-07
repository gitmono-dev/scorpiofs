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
//!
//! Full snapshot sync uses cached pages as hints and independently proves its
//! complete fixed root. It does not reuse record file lists or audit old pins.
//! The pin-backed record reuse rules above apply to the file-only `sync` API.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::snapshot::{
    closure::decode_page, frames::MetadataPageItem, reader::SnapshotPageSource, secure_fs,
    SnapshotClosureMeters, SnapshotError, SnapshotErrorCode, SnapshotFile, SnapshotReader,
    ValidatedSnapshotClosure,
};

/// Local cache-policy revision; bump when the reuse rules change so older
/// records are not trusted by newer code (spec 11 §10.3).
// Revision 2 records contain subtree-relative paths and every descendant
// page. Revision 1 records cannot prove either invariant and must be rebuilt.
pub const POLICY_REVISION: u16 = 2;

/// One verified subtree: the pages that were verified and what they proved.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClosureRecord {
    /// Fixed deployment/credential/authorization-epoch/profile partition.
    pub auth_domain: String,
    pub metadata_codec: u16,
    pub policy_revision: u16,
    /// `directory_root` of the subtree (an MTP2 `page_id`).
    pub root_page_id: String,
    /// Every page id of this directory and all descendant directories.
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
    /// Verified cached pages used by acquisition or the full root collector.
    pub reused_pages: u64,
    /// Acquisition page nodes visited, including reuse-decision branches.
    /// Full snapshot collector/proof work is recorded in closure_meters.
    pub traversal_nodes: u64,
    /// File rows constructed from acquired pages for the file-only API.
    /// Full snapshot acquisition leaves this at zero: its final independent
    /// root proof constructs the manifest. Cached records are not counted.
    pub acquisition_file_entries: u64,
    /// Cached manifest entries copied for the file-only reuse result. Full
    /// snapshot sync derives its manifest from the independent root proof.
    pub reused_file_entries_copied: u64,
    /// Directories whose subtrees were reused wholesale.
    pub reused_subtrees: u64,
    /// Index read attempts, including a missing or corrupt index. A sync
    /// loads the index once after acquiring its transaction lock.
    pub closure_index_reads: u64,
    /// Actual bytes read from the closure index (including corrupt bytes).
    pub closure_index_read_bytes: u64,
    /// Successfully published closure-index transactions.
    pub closure_index_writes: u64,
    /// Serialized bytes published by those transactions.
    pub closure_index_write_bytes: u64,
    /// Scope-directory scans for pins backed by a local COMPLETE dependency
    /// audit, not server authorization or GC-lease validation. These scans
    /// include whole-CAS verification; an optional store meter records its
    /// actual work under CompletionAudit independently of page reuse counts.
    pub pin_set_reads: u64,
    /// Actual local cached-page hash calls, including repeated checks of
    /// one page and checks that discover corrupt bytes. Missing pages and
    /// initial validation of network responses do not count here.
    pub page_rehashes: u64,
    /// Different cached page ids re-hashed during this sync.
    pub unique_page_rehashes: u64,
    /// Actual bytes passed to those local page hash calls, with repetition.
    pub page_rehash_bytes: u64,
}

/// Explicitly unlock before closing. A forked child or duplicated descriptor
/// may keep the same open-file description alive after this handle closes.
struct IndexLock {
    file: File,
}

impl Drop for IndexLock {
    fn drop(&mut self) {
        if let Err(error) = self.file.unlock() {
            tracing::warn!(%error, "failed to release local closure-index lock");
        }
    }
}

/// One scope-index read/modify/write transaction. The OS lock is held across
/// network awaits; dropping this value on error or cancellation explicitly
/// unlocks it without publishing pending records. Process exit closes its
/// descriptors and the OS releases the lock once all duplicates are closed.
struct ClosureTransaction {
    _lock: IndexLock,
    records: HashMap<String, ClosureRecord>,
    live_pins: HashSet<String>,
    dirty: bool,
}

// This transaction publishes hints derived from a complete fixed-root proof.
// It cannot supply the live-pin evidence required by file-only record reuse.
struct FullProofTransaction {
    _lock: IndexLock,
    records: HashMap<String, ClosureRecord>,
    dirty: bool,
}

impl FullProofTransaction {
    fn put_record(&mut self, record: ClosureRecord) {
        if self.records.get(&record.root_page_id) != Some(&record) {
            self.records.insert(record.root_page_id.clone(), record);
            self.dirty = true;
        }
    }

    fn commit(&self, cache: &ScopeCache, meters: &mut SyncMeters) -> Result<(), SnapshotError> {
        if self.dirty {
            cache.store_records_counted(&self.records, meters)?;
        }
        Ok(())
    }
}

struct ReusedSubtree {
    page_ids: Vec<String>,
    files: Vec<SnapshotFile>,
    total_entries: u64,
}

impl ClosureTransaction {
    fn put_record(&mut self, record: ClosureRecord) {
        if self.records.get(&record.root_page_id) != Some(&record) {
            self.records.insert(record.root_page_id.clone(), record);
            self.dirty = true;
        }
    }

    fn commit(&self, cache: &ScopeCache, meters: &mut SyncMeters) -> Result<(), SnapshotError> {
        if self.dirty {
            cache.store_records_counted(&self.records, meters)?;
        }
        Ok(())
    }
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
        secure_fs::create_dir_all_no_symlink(&dir.join("pages")).map_err(io_err)?;
        Ok(ScopeCache { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn closures_path(&self) -> PathBuf {
        self.dir.join("closures.json")
    }

    fn page_path(&self, page_id: &[u8; 32]) -> PathBuf {
        // Accept a parsed digest, never remote or journal text as a path.
        self.dir.join("pages").join(hex::encode(page_id))
    }

    fn load_records(&self) -> Result<HashMap<String, ClosureRecord>, SnapshotError> {
        self.load_records_counted(&mut SyncMeters::default())
    }

    fn load_records_counted(
        &self,
        meters: &mut SyncMeters,
    ) -> Result<HashMap<String, ClosureRecord>, SnapshotError> {
        meters.closure_index_reads += 1;
        let bytes = match secure_fs::read(&self.closures_path()) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(e) => return Err(io_err(e)),
        };
        meters.closure_index_read_bytes += bytes.len() as u64;
        match serde_json::from_slice::<HashMap<String, ClosureRecord>>(&bytes) {
            Ok(m) => Ok(m),
            // A torn or corrupt index is discarded, never partially trusted.
            Err(_) => Ok(HashMap::new()),
        }
    }

    fn store_records(&self, records: &HashMap<String, ClosureRecord>) -> Result<(), SnapshotError> {
        self.store_records_counted(records, &mut SyncMeters::default())
    }

    fn store_records_counted(
        &self,
        records: &HashMap<String, ClosureRecord>,
        meters: &mut SyncMeters,
    ) -> Result<(), SnapshotError> {
        let bytes = serde_json::to_vec(records)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        write_atomic(&self.dir, "closures.json", &bytes)?;
        // Persist the replacement directory entry as well as the temp-file
        // contents on supported Unix filesystems.
        #[cfg(unix)]
        File::open(&self.dir)
            .and_then(|dir| dir.sync_all())
            .map_err(io_err)?;
        meters.closure_index_writes += 1;
        meters.closure_index_write_bytes += bytes.len() as u64;
        Ok(())
    }

    fn try_index_lock(&self) -> Result<Option<IndexLock>, SnapshotError> {
        // Lock a stable inode, not closures.json which publication replaces.
        // No blocking lock call may stall the async task that owns the lock.
        let lock = secure_fs::open_rw_create(&self.dir.join("closures.lock")).map_err(io_err)?;
        match lock.try_lock() {
            Ok(()) => Ok(Some(IndexLock { file: lock })),
            Err(fs::TryLockError::WouldBlock) => Ok(None),
            Err(fs::TryLockError::Error(e)) => Err(io_err(e)),
        }
    }

    fn index_lock(&self) -> Result<IndexLock, SnapshotError> {
        self.try_index_lock()?.ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "another local closure-index transaction is active; retry later",
            )
        })
    }

    async fn sync_transaction(
        &self,
        meters: &mut SyncMeters,
    ) -> Result<ClosureTransaction, SnapshotError> {
        self.sync_transaction_with_pin_meters(meters, None).await
    }

    async fn full_proof_transaction(
        &self,
        meters: &mut SyncMeters,
    ) -> Result<FullProofTransaction, SnapshotError> {
        let lock = loop {
            if let Some(lock) = self.try_index_lock()? {
                break lock;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let records = self.load_records_counted(meters)?;
        // Page bytes are only hints. The caller must prove the complete current
        // descriptor root, so old owners and their content are not audited here.
        Ok(FullProofTransaction {
            _lock: lock,
            records,
            dirty: false,
        })
    }

    async fn sync_transaction_with_pin_meters(
        &self,
        meters: &mut SyncMeters,
        pin_meters: Option<&super::durable::CasVerificationMeters>,
    ) -> Result<ClosureTransaction, SnapshotError> {
        let lock = loop {
            if let Some(lock) = self.try_index_lock()? {
                break lock;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let records = self.load_records_counted(meters)?;
        meters.pin_set_reads += 1;
        let live_pins = super::stage::trace_sync("metadata_reuse_pin_inventory", || {
            self.pin_inventory(None, pin_meters).map(|inventory| {
                inventory
                    .into_iter()
                    .filter_map(|(_, audit)| {
                        if let super::workspace_pins::PinAudit::Active(id) = audit {
                            Some(id)
                        } else {
                            None
                        }
                    })
                    .collect()
            })
        })?;
        Ok(ClosureTransaction {
            _lock: lock,
            records,
            live_pins,
            dirty: false,
        })
    }

    /// The record for a subtree, if any (validity is the caller's call).
    pub fn record_for(&self, root_page_id: &str) -> Option<ClosureRecord> {
        self.load_records().ok()?.remove(root_page_id)
    }

    /// Insert or replace the record for its subtree. Returns
    /// `SnapshotNotReady` when another transaction owns the index; callers
    /// can retry without blocking an async executor thread.
    pub fn put_record(&self, record: &ClosureRecord) -> Result<(), SnapshotError> {
        let _lock = self.index_lock()?;
        let mut records = self.load_records()?;
        records.insert(record.root_page_id.clone(), record.clone());
        self.store_records(&records)
    }

    /// Drop records pinned only by `pin_ref` (spec 11 §10.3: releasing a
    /// snapshot releases what depended on *its* pin; a record already
    /// transferred to another live pin survives). Returns `SnapshotNotReady`
    /// while another index transaction is active; the caller may retry.
    pub fn drop_records_for_pin(&self, pin_ref: &str) -> Result<u64, SnapshotError> {
        self.drop_records_for_pin_impl(pin_ref, None, None)
    }

    // The caller holds the exact owner's transaction and has durably released
    // its registry row. Other owners are still independently lock/audit checked.
    pub(super) fn drop_records_for_released_owner(
        &self,
        owner: &super::workspace_pins::WorkspaceBinding,
        meters: Option<&super::durable::CasVerificationMeters>,
    ) -> Result<u64, SnapshotError> {
        self.drop_records_for_pin_impl(owner.snapshot_id(), Some(owner), meters)
    }

    fn drop_records_for_pin_impl(
        &self,
        pin_ref: &str,
        releasing: Option<&super::workspace_pins::WorkspaceBinding>,
        meters: Option<&super::durable::CasVerificationMeters>,
    ) -> Result<u64, SnapshotError> {
        let _lock = self.index_lock()?;
        // A snapshot may have several independent workspace owners. Unknown
        // ownership is insufficient evidence to remove its retention hints.
        if self
            .pin_inventory(releasing, meters)?
            .iter()
            .any(|(id, audit)| {
                id == pin_ref && !matches!(audit, super::workspace_pins::PinAudit::Inactive)
            })
        {
            return Ok(0);
        }
        let mut records = self.load_records()?;
        let before = records.len() as u64;
        records.retain(|_, r| r.pin_ref != pin_ref);
        self.store_records(&records)?;
        Ok(before - records.len() as u64)
    }

    /// Snapshot ids holding a local pin in this scope.
    pub fn live_pins(&self) -> Result<Vec<String>, SnapshotError> {
        self.try_live_pins()
    }

    pub fn try_live_pins(&self) -> Result<Vec<String>, SnapshotError> {
        let mut out: Vec<_> = self
            .pin_inventory(None, None)?
            .into_iter()
            .filter_map(|(_, audit)| {
                if let super::workspace_pins::PinAudit::Active(id) = audit {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    fn pin_inventory(
        &self,
        releasing: Option<&super::workspace_pins::WorkspaceBinding>,
        meters: Option<&super::durable::CasVerificationMeters>,
    ) -> Result<Vec<(String, super::workspace_pins::PinAudit)>, SnapshotError> {
        let mut out = super::workspace_pins::owner_inventory(&self.dir, releasing, meters)?;
        for entry in fs::read_dir(&self.dir).map_err(io_err)? {
            let entry = entry.map_err(io_err)?;
            let name = entry.file_name();
            let Some(hex) = name.to_str().filter(|name| {
                name.len() == 64
                    && name
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            }) else {
                continue;
            };
            let metadata = fs::symlink_metadata(entry.path()).map_err(io_err)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::IntegrityError,
                    "pin directory is not a real scope child",
                ));
            }
            // Owner roots are found exclusively through the fixed registry;
            // the containing SID directory itself may still hold an older pin.
            if !entry.path().join("pin.json").exists()
                && !entry.path().join("DURABLE_COMPLETE").exists()
            {
                continue;
            }
            let sid = format!("sha256:{hex}");
            let store = super::DurableStore {
                root: entry.path(),
                content: self.dir.join("blobs"),
                verification_meters: meters.cloned(),
            };
            let audit = store.audit_pin()?;
            if matches!(&audit, super::workspace_pins::PinAudit::Active(id) if id != &sid) {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::IntegrityError,
                    "pin snapshot differs from its scope child",
                ));
            }
            out.push((sid, audit));
        }
        Ok(out)
    }

    /// Store a verified page (digest/bytes checked by the caller). The id's
    /// shape is checked here before it can name any local file.
    pub fn put_page(&self, page_id: &str, bytes: &[u8]) -> Result<(), SnapshotError> {
        let id = parse_page_id(page_id)?;
        write_atomic(&self.dir.join("pages"), &hex::encode(id), bytes)
    }

    /// Read a page and re-hash it: existence alone is not evidence, because
    /// the store is plain files that a crash, a GC or a tamperer can damage.
    pub fn read_page_verified(&self, page_id: &str) -> Result<Option<Vec<u8>>, SnapshotError> {
        self.read_page_verified_counted(page_id, &mut SyncMeters::default(), &mut HashSet::new())
    }

    /// Read a digest-verified page hint with fixed, bounded scratch space.
    /// The caller must bind this cache to its authorization domain and prove
    /// membership under its fixed snapshot root; cached bytes grant neither.
    /// Missing, oversized or corrupt pages are misses. Other I/O errors,
    /// including a final symlink, propagate. Incremental sync meters are not
    /// changed by this separate v3 FUSE lookup.
    pub(crate) fn read_page_verified_bounded(
        &self,
        page_id: &str,
    ) -> Result<Option<Vec<u8>>, SnapshotError> {
        use std::io::Read;

        use mst2_codec::metapage::PAGE_MAX_BYTES;

        let want = match parse_page_id(page_id) {
            Ok(want) => want,
            Err(_) => return Ok(None),
        };
        let mut file = match secure_fs::open_regular_nonblocking(&self.page_path(&want)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_err(error)),
        };
        if file.metadata().map_err(io_err)?.len() > PAGE_MAX_BYTES as u64 {
            return Ok(None);
        }
        // The descriptor's size is only a preflight: the file can grow after
        // fstat. Read at most one byte beyond the page limit, without trusting
        // that sampled length or allocating from it.
        let mut buffer = [0u8; PAGE_MAX_BYTES + 1];
        let mut filled = 0;
        loop {
            match file.read(&mut buffer[filled..]) {
                Ok(0) => break,
                Ok(count) => {
                    filled += count;
                    if filled > PAGE_MAX_BYTES {
                        return Ok(None);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(io_err(error)),
            }
        }
        let bytes = &buffer[..filled];
        if mst2_codec::metapage::page_id(bytes) != want {
            return Ok(None);
        }
        Ok(Some(bytes.to_vec()))
    }

    fn read_page_verified_counted(
        &self,
        page_id: &str,
        meters: &mut SyncMeters,
        rehashed_page_ids: &mut HashSet<[u8; 32]>,
    ) -> Result<Option<Vec<u8>>, SnapshotError> {
        let want = match parse_page_id(page_id) {
            Ok(w) => w,
            // A corrupt record falls back to fetching. Do not even inspect
            // a path derived from its malformed id, much less remove it.
            Err(_) => return Ok(None),
        };
        let path = self.page_path(&want);
        let bytes = match secure_fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_err(e)),
        };
        meters.page_rehashes += 1;
        meters.page_rehash_bytes += bytes.len() as u64;
        rehashed_page_ids.insert(want);
        meters.unique_page_rehashes = rehashed_page_ids.len() as u64;
        if mst2_codec::metapage::page_id(&bytes) != want {
            // Corrupt: remove so a later sync re-fetches instead of re-reading.
            let _ = fs::remove_file(&path);
            return Ok(None);
        }
        Ok(Some(bytes))
    }
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
    reused_page_ids: HashSet<String>,
    rehashed_page_ids: HashSet<[u8; 32]>,
    closure_meters: SnapshotClosureMeters,
}

impl<'a> IncrementalSync<'a> {
    pub fn new(reader: &'a SnapshotReader, cache: &'a ScopeCache) -> Self {
        IncrementalSync {
            reader,
            cache,
            auth_domain: reader.authorized_context().cache_domain().id().to_string(),
            codec: reader.descriptor().metadata_codec,
            meters: SyncMeters::default(),
            reused_page_ids: HashSet::new(),
            rehashed_page_ids: HashSet::new(),
            closure_meters: SnapshotClosureMeters::default(),
        }
    }

    pub fn meters(&self) -> SyncMeters {
        self.meters
    }

    /// Root-proof work is additional to acquisition's `traversal_nodes`.
    pub fn closure_meters(&self) -> SnapshotClosureMeters {
        self.closure_meters
    }

    fn reset(&mut self) -> Result<(), SnapshotError> {
        self.reader
            .authorized_context()
            .bind_scope_cache(self.cache.dir())?;
        self.meters = SyncMeters::default();
        self.reused_page_ids.clear();
        self.rehashed_page_ids.clear();
        self.closure_meters = SnapshotClosureMeters::default();
        Ok(())
    }

    /// Reuse cached bytes, then prove the complete fixed root graph before
    /// publishing any index hints. File lists in records are never the truth
    /// of this API. Full proof still visits the entire logical namespace.
    pub async fn sync_snapshot(&mut self) -> Result<ValidatedSnapshotClosure, SnapshotError> {
        self.reset()?;
        if !self.reader.capabilities().features.metadata_pages {
            return Err(SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "complete snapshot sync requires metadata/pages",
            ));
        }
        self.reader.ensure_lease().await?;
        let reader = self.reader;
        let (pages, route_visits, collector_decodes) = reader.snapshot_pages_with(self).await?;
        let (closure, facts, mut meters) =
            ValidatedSnapshotClosure::with_subtree_facts(reader.descriptor(), pages)?;
        reader.validate_snapshot_files(closure.files())?;
        meters.collector_route_visits = route_visits;
        meters.collector_page_decodes = collector_decodes;
        let mut transaction = self.cache.full_proof_transaction(&mut self.meters).await?;
        reader.ensure_lease().await?;
        // Publish current reachable records only from the final root proof.
        // Cached page bytes are hints, never record file-list truth.
        // Unrelated old roots never enter this snapshot's dependency set.
        for fact in facts {
            let record = ClosureRecord {
                auth_domain: self.auth_domain.clone(),
                metadata_codec: self.codec,
                policy_revision: POLICY_REVISION,
                root_page_id: fact.root_page_id,
                page_ids: fact.page_ids,
                files: fact.files,
                total_entries: fact.total_entries,
                pin_ref: reader.snapshot_id().to_owned(),
            };
            if transaction
                .records
                .get(&record.root_page_id)
                .is_some_and(|old| old != &record)
            {
                meters.repaired_records += 1;
            }
            transaction.put_record(record);
        }
        reader.ensure_lease().await?;
        self.reused_page_ids
            .retain(|id| closure.pages().contains_key(id));
        self.meters.reused_pages = self.reused_page_ids.len() as u64;
        self.closure_meters = meters;
        transaction.commit(self.cache, &mut self.meters)?;
        Ok(closure)
    }

    /// Batched level-order sync: directories are fetched up to 64 per
    /// metadata_pages request (the endpoint's multi-item limit), collapsing
    /// the per-directory request storm that dominated cold-mount time on
    /// large trees. Closure records complete post-order — a record's file
    /// list spans its whole subtree, so it is written when the directory's
    /// page tree and all of its child directories have finished.
    pub async fn sync(&mut self) -> Result<Vec<SnapshotFile>, SnapshotError> {
        self.reset()?;
        let mut transaction = self.cache.sync_transaction(&mut self.meters).await?;
        let files = self.acquire(&mut transaction).await?;
        for file in &files {
            self.reader
                .authorized_context()
                .validate_relative_path(&file.rel_path)?;
        }
        transaction.commit(self.cache, &mut self.meters)?;
        Ok(files)
    }

    async fn acquire(
        &mut self,
        transaction: &mut ClosureTransaction,
    ) -> Result<Vec<SnapshotFile>, SnapshotError> {
        const PAGE_BATCH: usize = 64;

        enum Item {
            /// One page of a directory's own page tree. The route grows one
            /// label per round; the parent page was already fetched.
            Page {
                dir: String,
                route: Vec<u8>,
                expected: String,
            },
        }

        struct DirState {
            base: String,
            parent: Option<String>,
            root_page_id: String,
            page_ids: HashSet<String>,
            // Paths are relative to this directory, including completed
            // children. Prefixing happens once, when handed to the parent.
            files: Vec<SnapshotFile>,
            total_entries: u64,
            pending_pages: usize,
            pending_children: usize,
        }

        let mut files_out: Vec<SnapshotFile> = Vec::new();
        let mut states: HashMap<String, DirState> = HashMap::new();
        let mut frontier: VecDeque<Item> = VecDeque::new();
        let mut route_ids = HashMap::new();

        // Reuse hit -> the record covers the whole subtree: hand its files to
        // the parent (or the result) without fetching anything.
        macro_rules! finish_from_record {
            ($dir:expr, $expected:expr, $parent:expr, $base:expr) => {{
                if let Some(record) = self.try_reuse(&$expected, &$dir, transaction)? {
                    match &$parent {
                        Some(parent_path) => {
                            if let Some(ps) = states.get_mut(parent_path) {
                                ps.files
                                    .extend(record.files.into_iter().map(|f| SnapshotFile {
                                        rel_path: with_prefix(&f.rel_path, &$base),
                                        ..f
                                    }));
                                ps.page_ids.extend(record.page_ids);
                                ps.total_entries = ps
                                    .total_entries
                                    .checked_add(record.total_entries)
                                    .ok_or_else(|| {
                                        SnapshotError::new(
                                            SnapshotErrorCode::LimitExceeded,
                                            "entry count overflow",
                                        )
                                    })?;
                                ps.pending_children -= 1;
                            }
                        }
                        None => {
                            files_out = record.files;
                        }
                    }
                    true
                } else {
                    false
                }
            }};
        }

        // Enqueue a directory root: reuse first, otherwise create state and
        // fetch its root page in a later batch.
        macro_rules! enqueue_dir {
            ($dir:expr, $expected:expr, $parent:expr, $base:expr) => {{
                let dir: String = $dir;
                let expected: String = $expected;
                let parent: Option<String> = $parent;
                let base: String = $base;
                self.reader
                    .authorized_context()
                    .validate_relative_path(&dir)?;
                if let Some(parent_path) = &parent {
                    if let Some(ps) = states.get_mut(parent_path) {
                        ps.pending_children += 1;
                    }
                }
                if !finish_from_record!(dir, expected, parent, base) {
                    states.insert(
                        dir.clone(),
                        DirState {
                            base,
                            parent,
                            root_page_id: expected.clone(),
                            page_ids: HashSet::new(),
                            files: Vec::new(),
                            total_entries: 0,
                            pending_pages: 1,
                            pending_children: 0,
                        },
                    );
                    frontier.push_back(Item::Page {
                        dir,
                        route: Vec::new(),
                        expected,
                    });
                }
            }};
        }

        enqueue_dir!(
            "/".to_string(),
            self.reader.descriptor().metadata_root.clone(),
            None,
            String::new()
        );

        while !frontier.is_empty() {
            let take = frontier.len().min(PAGE_BATCH);
            let batch: Vec<Item> = frontier.drain(..take).collect();

            // A failed closure check falls back to a normal walk. Verified
            // individual pages remain usable: only missing pages need HTTP.
            let mut by_id: HashMap<String, Vec<u8>> = HashMap::new();
            let mut items = Vec::new();
            for it in &batch {
                let Item::Page {
                    dir,
                    route,
                    expected,
                } = it;
                self.reader
                    .authorized_context()
                    .validate_relative_path(dir)?;
                route_ids.insert((dir.clone(), route.clone()), expected.clone());
                if route.len() > mst2_codec::metapage::MAX_DEPTH {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::LimitExceeded,
                        "metadata radix depth exceeds 255",
                    ));
                }
                if by_id.contains_key(expected) {
                    continue;
                }
                if let Some(bytes) = self.cached_page(expected)? {
                    by_id.insert(expected.clone(), bytes);
                } else {
                    items.push(MetadataPageItem {
                        directory_path: dir.clone(),
                        route: route.clone(),
                        expected_digest: Some(expected.clone()),
                    });
                }
            }
            let pages = if items.is_empty() {
                Vec::new()
            } else {
                self.reader.ensure_lease().await?;
                self.reader
                    .client
                    .metadata_pages(
                        self.reader.snapshot_id(),
                        &items,
                        self.reader.encoding_hint(),
                    )
                    .await?
            };
            let mut allowed = HashSet::new();
            for item in &items {
                for depth in 0..=item.route.len() {
                    if let Some(id) =
                        route_ids.get(&(item.directory_path.clone(), item.route[..depth].to_vec()))
                    {
                        allowed.insert(id.clone());
                    }
                }
            }
            self.meters.fetched_pages += pages.len() as u64;
            // Responses may be deduplicated and returned in any order.
            for (pid, bytes) in pages {
                let id = format!("sha256:{}", crate::snapshot::frames::hex32(&pid));
                if !allowed.contains(&id) || mst2_codec::metapage::page_id(&bytes) != pid {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        format!("metadata/pages returned an invalid or unrequested page {id}"),
                    ));
                }
                self.cache.put_page(&id, &bytes)?;
                by_id.insert(id, bytes);
            }

            for it in &batch {
                let Item::Page {
                    dir,
                    route,
                    expected,
                } = it;
                self.meters.traversal_nodes += 1;
                let bytes = by_id.get(expected).ok_or_else(|| {
                    SnapshotError::new(
                        SnapshotErrorCode::DigestMismatch,
                        format!(
                            "metadata/pages did not return the requested page {expected} for {dir}"
                        ),
                    )
                })?;
                let page = decode_page(bytes)?;
                let st = states.get_mut(dir).ok_or_else(|| {
                    SnapshotError::new(
                        SnapshotErrorCode::Internal,
                        format!("missing walk state for {dir}"),
                    )
                })?;
                st.page_ids.insert(expected.clone());
                let mut child_dirs: Vec<(String, String, String)> = Vec::new(); // path, base, expected root
                let mut handle_entry = |e: &mst2_codec::metapage::Entry,
                                        child_dirs: &mut Vec<(String, String, String)>|
                 -> Result<(), SnapshotError> {
                    let name = std::str::from_utf8(&e.name)
                        .map_err(|_| {
                            SnapshotError::new(
                                SnapshotErrorCode::Internal,
                                "non-utf8 entry name in MTP2 page",
                            )
                        })?
                        .to_string();
                    let rel = if dir == "/" {
                        name.clone()
                    } else {
                        format!("{}/{}", dir.trim_start_matches('/'), name)
                    };
                    st.total_entries = st.total_entries.checked_add(1).ok_or_else(|| {
                        SnapshotError::new(SnapshotErrorCode::LimitExceeded, "entry count overflow")
                    })?;
                    match e.kind {
                        mst2_codec::metapage::EntryKind::Directory => {
                            child_dirs.push((
                                format!("/{rel}"),
                                name,
                                format!("sha256:{}", crate::snapshot::frames::hex32(&e.child_root)),
                            ));
                        }
                        kind => {
                            let fs_kind = match kind {
                                mst2_codec::metapage::EntryKind::Regular => "regular",
                                mst2_codec::metapage::EntryKind::Executable => "executable",
                                mst2_codec::metapage::EntryKind::Symlink => "symlink",
                                mst2_codec::metapage::EntryKind::Directory => unreachable!(),
                            };
                            st.files.push(SnapshotFile {
                                rel_path: name,
                                fs_kind: fs_kind.to_string(),
                                size: e.size,
                                content_digest: format!(
                                    "sha256:{}",
                                    crate::snapshot::frames::hex32(&e.content_id)
                                ),
                            });
                            self.meters.acquisition_file_entries += 1;
                        }
                    }
                    Ok(())
                };
                match &page {
                    mst2_codec::metapage::Page::Leaf { entries } => {
                        for e in entries {
                            handle_entry(e, &mut child_dirs)?;
                        }
                    }
                    mst2_codec::metapage::Page::Branch {
                        terminal, children, ..
                    } => {
                        if let Some(e) = terminal {
                            handle_entry(e, &mut child_dirs)?;
                        }
                        for c in children {
                            let mut next = route.clone();
                            next.push(c.label);
                            frontier.push_back(Item::Page {
                                dir: dir.clone(),
                                route: next,
                                expected: format!(
                                    "sha256:{}",
                                    crate::snapshot::frames::hex32(&c.child_page_id)
                                ),
                            });
                            st.pending_pages += 1;
                        }
                    }
                }
                st.pending_pages = st.pending_pages.saturating_sub(1);
                for (child_path, child_base, child_expected) in child_dirs {
                    enqueue_dir!(child_path, child_expected, Some(dir.clone()), child_base);
                }
            }

            // Completion cascade: a directory whose page tree and children
            // are done stages its record and hands its recursive file list
            // to its parent (which may complete in turn).
            loop {
                let done_dir = states
                    .iter()
                    .find(|(_, st)| st.pending_pages == 0 && st.pending_children == 0)
                    .map(|(d, _)| d.clone());
                let Some(dir) = done_dir else { break };
                let Some(st) = states.remove(&dir) else {
                    break;
                };

                let mut page_ids: Vec<String> = st.page_ids.into_iter().collect();
                page_ids.sort();
                // This file-only walk builds records from its verified pages.
                // Full snapshots build records in the separate root proof.
                transaction.put_record(ClosureRecord {
                    auth_domain: self.auth_domain.clone(),
                    metadata_codec: self.codec,
                    policy_revision: POLICY_REVISION,
                    root_page_id: st.root_page_id.clone(),
                    page_ids: page_ids.clone(),
                    total_entries: st.total_entries,
                    files: st.files.clone(),
                    pin_ref: self.reader.snapshot_id().to_string(),
                });

                match st.parent.clone() {
                    Some(parent_path) => {
                        if let Some(ps) = states.get_mut(&parent_path) {
                            ps.files.extend(st.files.into_iter().map(|f| SnapshotFile {
                                rel_path: with_prefix(&f.rel_path, &st.base),
                                ..f
                            }));
                            ps.page_ids.extend(page_ids);
                            ps.total_entries = ps
                                .total_entries
                                .checked_add(st.total_entries)
                                .ok_or_else(|| {
                                    SnapshotError::new(
                                        SnapshotErrorCode::LimitExceeded,
                                        "entry count overflow",
                                    )
                                })?;
                            ps.pending_children -= 1;
                        }
                    }
                    None => {
                        files_out = st.files;
                    }
                }
            }
        }

        self.meters.reused_pages = self.reused_page_ids.len() as u64;
        Ok(files_out)
    }

    /// Reuse the recorded subtree when every condition holds; otherwise
    /// return `None` so the caller walks normally.
    fn try_reuse(
        &mut self,
        root_page_id: &str,
        dir: &str,
        transaction: &mut ClosureTransaction,
    ) -> Result<Option<ReusedSubtree>, SnapshotError> {
        self.reader
            .authorized_context()
            .validate_relative_path(dir)?;
        let Some(record) = transaction.records.get_mut(root_page_id) else {
            return Ok(None);
        };
        if !record.compatible(&self.auth_domain, self.codec) {
            return Ok(None);
        }
        // The record must be backed by a pin that is still live here.
        if !transaction.live_pins.contains(&record.pin_ref) {
            return Ok(None);
        }
        // A record that names no pages proves nothing: refuse it outright
        // rather than "verifying" an empty set (a hole a corrupt or truncated
        // index could otherwise open).
        if record.page_ids.is_empty()
            || record.root_page_id != root_page_id
            || !record.page_ids.iter().any(|id| id == root_page_id)
        {
            return Ok(None);
        }
        // Every page of the subtree must be present *and* re-hash correctly.
        for page_id in &record.page_ids {
            if self.cached_page(page_id)?.is_none() {
                return Ok(None);
            }
        }
        self.meters.reused_file_entries_copied += record.files.len() as u64;
        let reused = ReusedSubtree {
            page_ids: record.page_ids.clone(),
            files: record.files.clone(),
            total_entries: record.total_entries,
        };
        self.reused_page_ids.extend(record.page_ids.iter().cloned());
        self.meters.reused_pages = self.reused_page_ids.len() as u64;
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
        // Keep the transaction's manifest intact; only its pin identity
        // changes. Full sync replaces hints from the final independent proof.
        if record.pin_ref != self.reader.snapshot_id() {
            record.pin_ref = self.reader.snapshot_id().to_string();
            transaction.dirty = true;
        }
        Ok(Some(reused))
    }
}

impl SnapshotPageSource for IncrementalSync<'_> {
    fn cached_page(&mut self, id: &str) -> Result<Option<Vec<u8>>, SnapshotError> {
        let bytes = self.cache.read_page_verified_counted(
            id,
            &mut self.meters,
            &mut self.rehashed_page_ids,
        )?;
        if bytes.is_some() {
            self.reused_page_ids.insert(id.to_owned());
        }
        Ok(bytes)
    }

    fn received_page(&mut self, id: &str, bytes: &[u8]) -> Result<(), SnapshotError> {
        self.cache.put_page(id, bytes)?;
        self.meters.fetched_pages += 1;
        Ok(())
    }
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
    secure_fs::create_dir_all_no_symlink(dir).map_err(io_err)?;
    let tmp = dir.join(format!(
        ".{name}.tmp.{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let result = (|| {
        use std::io::Write as _;
        let mut f = secure_fs::open_create_new(&tmp).map_err(io_err)?;
        f.write_all(data).map_err(io_err)?;
        f.sync_all().map_err(io_err)?;
        #[cfg(test)]
        tests::before_index_rename(dir, name)?;
        fs::rename(&tmp, dir.join(name)).map_err(io_err)
    })();
    if result.is_err() {
        // Leave the published index intact when preparation fails. A
        // leftover temporary file is not a reusable closure record.
        let _ = fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    thread_local! {
        static FAIL_INDEX_RENAME: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }

    struct RenameFailure;

    impl RenameFailure {
        fn install(dir: &Path) -> Self {
            FAIL_INDEX_RENAME.with(|slot| {
                assert!(slot.borrow().is_none());
                *slot.borrow_mut() = Some(dir.into());
            });
            Self
        }
    }

    impl Drop for RenameFailure {
        fn drop(&mut self) {
            FAIL_INDEX_RENAME.with(|slot| *slot.borrow_mut() = None);
        }
    }

    pub(super) fn before_index_rename(dir: &Path, name: &str) -> Result<(), SnapshotError> {
        let fail = FAIL_INDEX_RENAME.with(|slot| {
            let mut slot = slot.borrow_mut();
            if name == "closures.json" && slot.as_deref() == Some(dir) {
                *slot = None;
                true
            } else {
                false
            }
        });
        if fail {
            Err(io_err(std::io::Error::other(
                "injected closure-index publication failure",
            )))
        } else {
            Ok(())
        }
    }

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

    #[tokio::test]
    async fn live_pins_require_a_committed_dependency_audit() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        assert!(cache.live_pins().unwrap().is_empty());
        let id = format!("sha256:{}", "ab".repeat(32));
        let view = tmp.path().join(id.trim_start_matches("sha256:"));
        std::fs::create_dir_all(&view).unwrap();
        std::fs::write(
            view.join("pin.json"),
            serde_json::to_vec(&serde_json::json!({"snapshot_id": id, "scope": "/p", "lease_id": "l", "pinned_at_unix": 1})).unwrap(),
        )
        .unwrap();
        assert!(
            cache.live_pins().unwrap().is_empty(),
            "a prepare pin cannot authorize reuse"
        );
        let store =
            crate::snapshot::DurableStore::open_with_content(&view, tmp.path().join("blobs"))
                .unwrap();
        let meta = crate::snapshot::ViewMeta {
            snapshot_id: id.clone(),
            namespace_view_id: format!("sha256:{}", "55".repeat(32)),
            scope: "/p".into(),
            lease_id: "l".into(),
        };
        store
            .hydrate_with(&meta, &[], |_| async { Ok(Vec::new()) })
            .await
            .unwrap();
        assert_eq!(cache.live_pins().unwrap(), vec![id]);
        fs::remove_file(view.join("DURABLE_COMPLETE")).unwrap();
        assert!(
            cache.live_pins().unwrap().is_empty(),
            "an orphan pin cannot authorize reuse"
        );
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

    fn bounded_page_id(bytes: &[u8]) -> String {
        format!(
            "sha256:{}",
            hex::encode(mst2_codec::metapage::page_id(bytes))
        )
    }

    #[test]
    fn bounded_page_hints_verify_exact_bytes_and_treat_absence_or_corruption_as_misses() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        let page = mst2_codec::metapage::Page::Leaf { entries: vec![] }
            .encode()
            .unwrap();
        let id = bounded_page_id(&page);
        assert_eq!(cache.read_page_verified_bounded(&id).unwrap(), None);
        cache.put_page(&id, &page).unwrap();
        assert_eq!(
            cache.read_page_verified_bounded(&id).unwrap(),
            Some(page.clone())
        );
        cache.put_page(&id, b"corrupt").unwrap();
        assert_eq!(cache.read_page_verified_bounded(&id).unwrap(), None);
        assert_eq!(
            cache
                .read_page_verified_bounded("sha256:../../outside")
                .unwrap(),
            None
        );
    }

    #[test]
    fn bounded_page_hints_accept_the_limit_and_reject_larger_even_matching_digests() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        // Shape and snapshot membership remain the caller's responsibility.
        // This getter checks the byte bound and hash, before allocating output.
        let mut bytes = vec![0x5a; mst2_codec::metapage::PAGE_MAX_BYTES];
        let id = bounded_page_id(&bytes);
        cache.put_page(&id, &bytes).unwrap();
        assert_eq!(
            cache.read_page_verified_bounded(&id).unwrap(),
            Some(bytes.clone())
        );
        bytes.push(0x5a);
        let oversized_id = bounded_page_id(&bytes);
        cache.put_page(&oversized_id, &bytes).unwrap();
        assert_eq!(
            cache.read_page_verified_bounded(&oversized_id).unwrap(),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn bounded_page_hints_reject_oversized_sparse_files_without_reading_the_extent() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        let id = bounded_page_id(b"sparse");
        let path = cache.page_path(&parse_page_id(&id).unwrap());
        File::create(&path).unwrap().set_len(1u64 << 34).unwrap();
        assert_eq!(cache.read_page_verified_bounded(&id).unwrap(), None);
        assert_eq!(fs::metadata(path).unwrap().len(), 1u64 << 34);
    }

    #[cfg(unix)]
    #[test]
    fn bounded_page_hints_reject_a_final_symlink_and_propagate_nonregular_io_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        let bytes = b"outside";
        let id = bounded_page_id(bytes);
        let target = tmp.path().join("outside");
        fs::write(&target, bytes).unwrap();
        let path = cache.page_path(&parse_page_id(&id).unwrap());
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert_eq!(
            cache.read_page_verified_bounded(&id).unwrap_err().code,
            SnapshotErrorCode::Internal
        );
        assert_eq!(fs::read(&target).unwrap(), bytes);
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert_eq!(
            cache.read_page_verified_bounded(&id).unwrap_err().code,
            SnapshotErrorCode::Internal
        );
    }

    #[cfg(unix)]
    #[test]
    fn bounded_page_hints_reject_a_fifo_without_waiting_for_a_writer() {
        use std::{ffi::CString, os::unix::ffi::OsStrExt, sync::mpsc};

        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        let id = bounded_page_id(b"fifo");
        let path = cache.page_path(&parse_page_id(&id).unwrap());
        let fifo_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(
            unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) },
            0,
            "mkfifo failed: {}",
            std::io::Error::last_os_error()
        );
        let (sender, receiver) = mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let _ = sender.send(cache.read_page_verified_bounded(&id));
        });
        // A regression may block inside open forever. On timeout, unwinding
        // drops this join handle; the detached thread cannot hold up libtest's
        // process exit as a Tokio blocking task would hold up runtime shutdown.
        let result = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("FIFO cache hint blocked waiting for a writer");
        worker.join().unwrap();
        assert_eq!(result.unwrap_err().code, SnapshotErrorCode::Internal);
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
        assert_eq!(with_prefix("b/c.txt", "/b"), "b/b/c.txt");
        assert_eq!(with_prefix("x.txt", "/"), "x.txt");
        assert_eq!(with_prefix("", "/moved"), "moved");
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
            ..SyncMeters::default()
        };
        assert_ne!(m2.fetched_pages, m2.reused_pages);
    }

    #[tokio::test]
    async fn index_transaction_stages_records_and_publishes_once() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        cache.put_record(&rec("existing", "old")).unwrap();
        let initial_bytes = fs::read(cache.closures_path()).unwrap();
        let mut meters = SyncMeters::default();
        let mut transaction = cache.sync_transaction(&mut meters).await.unwrap();
        for n in 0..16 {
            transaction.put_record(rec(&format!("new-{n}"), "new"));
        }
        assert_eq!(fs::read(cache.closures_path()).unwrap(), initial_bytes);
        assert_eq!(
            cache.put_record(&rec("other", "other")).unwrap_err().code,
            SnapshotErrorCode::SnapshotNotReady
        );
        assert_eq!(
            cache.drop_records_for_pin("old").unwrap_err().code,
            SnapshotErrorCode::SnapshotNotReady
        );
        transaction.commit(&cache, &mut meters).unwrap();
        assert_eq!(meters.closure_index_reads, 1);
        assert_eq!(meters.closure_index_read_bytes, initial_bytes.len() as u64);
        assert_eq!(meters.closure_index_writes, 1);
        assert_eq!(
            meters.closure_index_write_bytes,
            fs::metadata(cache.closures_path()).unwrap().len()
        );
        assert_eq!(meters.pin_set_reads, 1);
        assert_eq!(cache.load_records().unwrap().len(), 17);
        drop(transaction);

        let mut meters = SyncMeters::default();
        let transaction = cache.sync_transaction(&mut meters).await.unwrap();
        transaction.commit(&cache, &mut meters).unwrap();
        assert_eq!(meters.closure_index_reads, 1);
        assert_eq!(
            meters.closure_index_writes, 0,
            "no dirty records to publish"
        );
    }

    #[tokio::test]
    async fn full_proof_index_wait_yields_and_cancellation_discards_records() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        cache.put_record(&rec("existing", "old")).unwrap();
        let original = fs::read(cache.closures_path()).unwrap();
        let blocker = cache.index_lock().unwrap();
        let mut meters = SyncMeters::default();
        assert!(tokio::time::timeout(
            Duration::from_millis(30),
            cache.full_proof_transaction(&mut meters),
        )
        .await
        .is_err());
        assert_eq!(meters.closure_index_reads, 0);
        assert_eq!(meters.pin_set_reads, 0);
        drop(blocker);

        let (ready, started) = tokio::sync::oneshot::channel();
        let task_dir = tmp.path().to_path_buf();
        let writer = tokio::spawn(async move {
            let cache = ScopeCache::open(task_dir).unwrap();
            let mut meters = SyncMeters::default();
            let mut transaction = cache.full_proof_transaction(&mut meters).await.unwrap();
            assert_eq!(meters.closure_index_reads, 1);
            assert_eq!(meters.pin_set_reads, 0);
            transaction.put_record(rec("cancelled", "new"));
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(transaction);
        });
        started.await.unwrap();
        assert!(cache.try_index_lock().unwrap().is_none());
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        assert_eq!(fs::read(cache.closures_path()).unwrap(), original);
        let transaction = tokio::time::timeout(
            Duration::from_secs(2),
            cache.full_proof_transaction(&mut SyncMeters::default()),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(transaction.records.contains_key("existing"));
        assert!(!transaction.records.contains_key("cancelled"));
        assert!(!transaction.dirty);
    }

    #[tokio::test]
    async fn full_proof_failed_publication_keeps_old_index_and_allows_retry() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        cache.put_record(&rec("existing", "old")).unwrap();
        let original = fs::read(cache.closures_path()).unwrap();
        let mut meters = SyncMeters::default();
        let mut transaction = cache.full_proof_transaction(&mut meters).await.unwrap();
        transaction.put_record(rec("existing", "new"));
        transaction.put_record(rec("fresh", "new"));
        let fault = RenameFailure::install(tmp.path());
        assert_eq!(
            transaction.commit(&cache, &mut meters).unwrap_err().code,
            SnapshotErrorCode::Internal
        );
        drop(fault);
        drop(transaction);
        assert_eq!(fs::read(cache.closures_path()).unwrap(), original);
        assert_eq!(meters.closure_index_writes, 0);
        assert_eq!(meters.closure_index_write_bytes, 0);
        assert_eq!(meters.pin_set_reads, 0);
        let mut transaction = cache
            .full_proof_transaction(&mut SyncMeters::default())
            .await
            .unwrap();
        assert_eq!(transaction.records["existing"].pin_ref, "old");
        assert!(!transaction.records.contains_key("fresh"));
        transaction.put_record(rec("fresh", "retry"));
        transaction
            .commit(&cache, &mut SyncMeters::default())
            .unwrap();
        drop(transaction);
        assert_eq!(cache.record_for("fresh").unwrap().pin_ref, "retry");
    }

    #[tokio::test]
    async fn index_wait_yields_and_cancellation_discards_pending_records() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        cache.put_record(&rec("existing", "old")).unwrap();
        let blocker = cache.index_lock().unwrap();
        let mut meters = SyncMeters::default();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(30),
                cache.sync_transaction(&mut meters),
            )
            .await
            .is_err(),
            "the blocked waiter must yield to this executor's timer"
        );
        assert_eq!(meters.closure_index_reads, 0);
        drop(blocker);

        let (ready, started) = tokio::sync::oneshot::channel();
        let task_dir = tmp.path().to_path_buf();
        let writer = tokio::spawn(async move {
            let cache = ScopeCache::open(task_dir).unwrap();
            let mut transaction = cache
                .sync_transaction(&mut SyncMeters::default())
                .await
                .unwrap();
            transaction.put_record(rec("cancelled", "new"));
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
            // Keep the transaction live throughout the cancellable await.
            drop(transaction);
        });
        started.await.unwrap();
        assert!(cache.try_index_lock().unwrap().is_none());
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        let transaction = tokio::time::timeout(
            Duration::from_secs(2),
            cache.sync_transaction(&mut SyncMeters::default()),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(transaction.records.contains_key("existing"));
        assert!(!transaction.records.contains_key("cancelled"));
        assert!(!transaction.dirty);
    }

    #[tokio::test]
    async fn failed_index_publication_keeps_old_records_and_releases_its_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        cache.put_record(&rec("existing", "old")).unwrap();
        let original = fs::read(cache.closures_path()).unwrap();
        let mut meters = SyncMeters::default();
        let mut transaction = cache.sync_transaction(&mut meters).await.unwrap();
        transaction.put_record(rec("existing", "new"));
        transaction.put_record(rec("fresh", "new"));
        let fault = RenameFailure::install(tmp.path());
        assert_eq!(
            transaction.commit(&cache, &mut meters).unwrap_err().code,
            SnapshotErrorCode::Internal
        );
        drop(fault);
        drop(transaction);
        assert_eq!(fs::read(cache.closures_path()).unwrap(), original);
        assert_eq!(meters.closure_index_writes, 0);
        assert_eq!(meters.closure_index_write_bytes, 0);
        assert!(!fs::read_dir(tmp.path()).unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".closures.json.tmp.")));

        let mut transaction = cache
            .sync_transaction(&mut SyncMeters::default())
            .await
            .unwrap();
        assert_eq!(transaction.records["existing"].pin_ref, "old");
        assert!(!transaction.records.contains_key("fresh"));
        transaction.put_record(rec("fresh", "retry"));
        transaction
            .commit(&cache, &mut SyncMeters::default())
            .unwrap();
        drop(transaction);
        assert_eq!(cache.record_for("fresh").unwrap().pin_ref, "retry");
    }

    #[tokio::test]
    async fn pin_release_orders_after_transfer_and_is_not_restored_by_later_batches() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        cache.put_record(&rec("existing", "old")).unwrap();
        cache.put_record(&rec("unrelated", "other")).unwrap();
        let mut transaction = cache
            .sync_transaction(&mut SyncMeters::default())
            .await
            .unwrap();
        transaction.put_record(rec("existing", "new"));
        transaction.put_record(rec("fresh", "new"));
        assert_eq!(
            cache.drop_records_for_pin("old").unwrap_err().code,
            SnapshotErrorCode::SnapshotNotReady
        );
        transaction
            .commit(&cache, &mut SyncMeters::default())
            .unwrap();
        drop(transaction);
        assert_eq!(cache.drop_records_for_pin("old").unwrap(), 0);
        assert_eq!(cache.record_for("existing").unwrap().pin_ref, "new");
        assert_eq!(cache.drop_records_for_pin("new").unwrap(), 2);

        let mut transaction = cache
            .sync_transaction(&mut SyncMeters::default())
            .await
            .unwrap();
        transaction.put_record(rec("later", "later"));
        transaction
            .commit(&cache, &mut SyncMeters::default())
            .unwrap();
        drop(transaction);
        assert!(cache.record_for("existing").is_none());
        assert!(cache.record_for("fresh").is_none());
        assert_eq!(cache.record_for("unrelated").unwrap().pin_ref, "other");
        assert_eq!(cache.record_for("later").unwrap().pin_ref, "later");
    }

    #[test]
    fn dropping_index_guard_unlocks_even_with_a_duplicate_descriptor_alive() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        let owner = cache.index_lock().unwrap();
        let duplicate = owner.file.try_clone().unwrap();
        assert!(cache.try_index_lock().unwrap().is_none());
        drop(owner);
        let _next = cache.index_lock().unwrap();
        // The original open-file description remains alive here. Acquiring
        // a fresh lock must not depend on waiting for this descriptor to die.
        drop(duplicate);
    }

    #[test]
    fn rehash_counters_include_repeats_and_corruption_but_not_missing_pages() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        let bytes = mst2_codec::metapage::Page::Leaf { entries: vec![] }
            .encode()
            .unwrap();
        let id = format!(
            "sha256:{}",
            hex::encode(mst2_codec::metapage::page_id(&bytes))
        );
        cache.put_page(&id, &bytes).unwrap();
        let mut meters = SyncMeters::default();
        let mut unique = HashSet::new();
        for _ in 0..2 {
            assert!(cache
                .read_page_verified_counted(&id, &mut meters, &mut unique)
                .unwrap()
                .is_some());
        }
        cache.put_page(&id, b"bad").unwrap();
        assert!(cache
            .read_page_verified_counted(&id, &mut meters, &mut unique)
            .unwrap()
            .is_none());
        assert!(cache
            .read_page_verified_counted(&id, &mut meters, &mut unique)
            .unwrap()
            .is_none());
        assert_eq!(meters.page_rehashes, 3);
        assert_eq!(meters.unique_page_rehashes, 1);
        assert_eq!(meters.page_rehash_bytes, bytes.len() as u64 * 2 + 3);
    }

    fn index_child(dir: &Path, mode: &str, worker: usize) -> std::process::Child {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "snapshot::incremental::tests::index_lock_child",
            ])
            .env("SCORPIOFS_INDEX_CHILD_DIR", dir)
            .env("SCORPIOFS_INDEX_CHILD_MODE", mode)
            .env("SCORPIOFS_INDEX_CHILD_WORKER", worker.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap()
    }

    #[test]
    fn index_writers_in_separate_processes_preserve_each_others_records() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        let mut workers = (0..4)
            .map(|worker| index_child(tmp.path(), "write", worker))
            .collect::<Vec<_>>();
        for worker in &mut workers {
            assert!(worker.wait().unwrap().success());
        }
        let records = cache.load_records().unwrap();
        assert_eq!(records.len(), 64, "all four writers' records survive");
        for worker in 0..4 {
            for n in 0..16 {
                assert!(records.contains_key(&format!("worker-{worker}-{n}")));
            }
        }
    }

    #[test]
    fn killed_index_owner_releases_the_os_lock_without_publishing() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = ScopeCache::open(tmp.path()).unwrap();
        cache.put_record(&rec("existing", "old")).unwrap();
        let mut owner = index_child(tmp.path(), "hold", 0);
        let ready = tmp.path().join("child-ready");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !ready.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let acquired = ready.exists();
        let unavailable = cache.try_index_lock().unwrap().is_none();
        owner.kill().unwrap();
        owner.wait().unwrap();
        assert!(acquired, "child must acquire the lock before termination");
        assert!(unavailable, "a separate process owns the same OS lock");
        let _lock = cache.index_lock().unwrap();
        assert!(cache.record_for("existing").is_some());
        assert!(cache.record_for("uncommitted").is_none());
    }

    #[test]
    #[ignore = "launched in a separate process by the index-lock regression tests"]
    fn index_lock_child() {
        let Some(dir) = std::env::var_os("SCORPIOFS_INDEX_CHILD_DIR") else {
            return;
        };
        let cache = ScopeCache::open(PathBuf::from(dir)).unwrap();
        let mode = std::env::var("SCORPIOFS_INDEX_CHILD_MODE").unwrap();
        if mode == "hold" {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            runtime.block_on(async {
                let mut transaction = cache
                    .sync_transaction(&mut SyncMeters::default())
                    .await
                    .unwrap();
                transaction.put_record(rec("uncommitted", "child"));
                fs::write(cache.dir().join("child-ready"), b"ready").unwrap();
                std::future::pending::<()>().await;
                drop(transaction);
            });
        } else {
            assert_eq!(mode, "write");
            let worker = std::env::var("SCORPIOFS_INDEX_CHILD_WORKER").unwrap();
            for n in 0..16 {
                let record = rec(&format!("worker-{worker}-{n}"), &worker);
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                loop {
                    match cache.put_record(&record) {
                        Ok(()) => break,
                        Err(error) if error.code == SnapshotErrorCode::SnapshotNotReady => {
                            assert!(std::time::Instant::now() < deadline);
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("unexpected writer error: {error}"),
                    }
                }
            }
        }
    }
}
