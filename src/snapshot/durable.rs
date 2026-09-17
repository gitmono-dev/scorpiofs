//! Durable local hydration of one fixed view (spec 11 client-side sync).
//!
//! A hydrated view is a content-addressed store plus an append-only journal:
//!
//! ```text
//! <root>/view.json            descriptor binding (snapshot id, scope, view id)
//! <root>/journal.log          one JSON record per completed file (fsync'd)
//! <root>/blobs/<hex>          file content, addressed by SHA-256
//! <root>/DURABLE_COMPLETE     marker written only after every file verified
//! <root>/pin.json             local pin (GC-lease governance is a later WP)
//! ```
//!
//! Two invariants drive the design:
//!
//! * **Completeness is proven, never assumed.** `DURABLE_COMPLETE` is written
//!   last, after every manifest file has been fetched, digest-checked and
//!   fsync'd. An interrupted hydration leaves the marker absent and is
//!   resumed, not accepted.
//! * **Resume re-verifies.** A journal entry is a hint, not proof: on resume
//!   each recorded blob is re-hashed before it counts as hydrated, so a
//!   truncated, torn or tampered CAS object is re-fetched rather than served.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};

use crate::snapshot::{SnapshotError, SnapshotErrorCode, SnapshotFile, SnapshotReader};

const VIEW_FILE: &str = "view.json";
const MANIFEST_FILE: &str = "manifest.json";
const JOURNAL_FILE: &str = "journal.log";
const COMPLETE_MARKER: &str = "DURABLE_COMPLETE";
const PIN_FILE: &str = "pin.json";
const BLOB_DIR: &str = "blobs";

/// The fixed-view binding a store was hydrated from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewMeta {
    pub snapshot_id: String,
    pub namespace_view_id: String,
    pub scope: String,
    pub lease_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct FileRecord {
    rel_path: String,
    digest: String,
    size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompleteMarker {
    snapshot_id: String,
    namespace_view_id: String,
    files: u64,
    bytes: u64,
    hydrated_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PinRecord {
    snapshot_id: String,
    scope: String,
    lease_id: String,
    pinned_at_unix: u64,
}

/// Outcome of one hydration pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydrateReport {
    pub snapshot_id: String,
    pub total_files: u64,
    /// Files downloaded from the server in this pass.
    pub fetched: u64,
    /// Files that were already hydrated and re-verified.
    pub resumed: u64,
    /// Resumed files whose CAS object failed re-verification and were refetched.
    pub repaired: u64,
    pub bytes_total: u64,
    pub complete: bool,
}

/// One snapshot's durable local store.
///
/// The per-view metadata (view/manifest/journal/marker/pin) lives under
/// `root`; the *content* is a content-addressed store that may be shared by
/// every view of one scope (`open_with_content`). Sharing is safe because a
/// blob is addressed by the digest of its bytes and re-verified before use,
/// and it is what makes a new version reuse content instead of downloading
/// it again (spec 11 §3/§10.2: key is auth_domain + digest + size).
pub struct DurableStore {
    root: PathBuf,
    content: PathBuf,
}

impl DurableStore {
    /// Open (creating if needed) the store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, SnapshotError> {
        let root = root.into();
        let content = root.join(BLOB_DIR);
        Self::open_with_content(root, content)
    }

    /// Open a view whose content comes from `content` (a scope-level cache
    /// shared with other views), while its own metadata stays under `root`.
    pub fn open_with_content(
        root: impl Into<PathBuf>,
        content: impl Into<PathBuf>,
    ) -> Result<Self, SnapshotError> {
        let root = root.into();
        let content = content.into();
        fs::create_dir_all(&root).map_err(io_err)?;
        fs::create_dir_all(&content).map_err(io_err)?;
        Ok(Self { root, content })
    }

    /// The content-addressed cache backing this view (shared across the
    /// views of one scope when opened with `open_with_content`).
    pub fn content_dir(&self) -> &Path {
        &self.content
    }

    /// `root/snapshots/<scope-slug>/<snapshot-hex>` — the conventional layout
    /// used by the mount example so restarts land in the same place.
    pub fn path_for(root: &Path, scope: &str, snapshot_id: &str) -> PathBuf {
        let slug: String = scope
            .trim_matches('/')
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let digest_hex = snapshot_id.strip_prefix("sha256:").unwrap_or(snapshot_id);
        root.join("snapshots").join(slug).join(digest_hex)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Path of one CAS object. The digest is validated as `sha256:<64 hex>`
    /// before it is ever turned into a path: it comes from server responses
    /// and the manifest file, and a crafted value (`../`, absolute, non-hex)
    /// must not be able to address a file outside the content directory.
    fn blob_path(&self, digest: &str) -> Result<PathBuf, SnapshotError> {
        let hex = digest.strip_prefix("sha256:").unwrap_or(digest);
        if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("malformed digest {digest:?}"),
            ));
        }
        Ok(self.content.join(hex))
    }

    /// True only when the marker exists *and* names this view. A marker that
    /// exists at all is proof that every file was verified at write time.
    pub fn is_complete(&self) -> Result<bool, SnapshotError> {
        let marker = self.root.join(COMPLETE_MARKER);
        if !marker.exists() {
            return Ok(false);
        }
        let bytes = fs::read(&marker).map_err(io_err)?;
        let parsed: CompleteMarker = serde_json::from_slice(&bytes).map_err(|e| {
            SnapshotError::new(
                SnapshotErrorCode::Internal,
                format!("corrupt {COMPLETE_MARKER}: {e}"),
            )
        })?;
        // A marker without a matching view.json binding is not proof.
        let Some(view) = self.stored_view()? else {
            return Ok(false);
        };
        Ok(parsed.snapshot_id == view.snapshot_id)
    }

    /// The view this store was hydrated from, if any.
    pub fn stored_view(&self) -> Result<Option<ViewMeta>, SnapshotError> {
        let path = self.root.join(VIEW_FILE);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path).map_err(io_err)?;
        let meta = serde_json::from_slice(&bytes).map_err(|e| {
            SnapshotError::new(
                SnapshotErrorCode::Internal,
                format!("corrupt {VIEW_FILE}: {e}"),
            )
        })?;
        Ok(Some(meta))
    }

    /// Pin this view locally. A pin only means something for a view that was
    /// actually hydrated into this store, so a missing or different binding is
    /// refused instead of writing a marker that would claim protection the
    /// store cannot back.
    pub fn pin(&self, view: &ViewMeta) -> Result<(), SnapshotError> {
        match self.stored_view()? {
            Some(stored) if stored.snapshot_id == view.snapshot_id => {}
            Some(stored) => {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DurableViewConflict,
                    format!(
                        "store at {} holds view {}, cannot pin {}",
                        self.root.display(),
                        stored.snapshot_id,
                        view.snapshot_id
                    ),
                ))
            }
            None => {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::SnapshotNotReady,
                    format!("{}: nothing hydrated here to pin", self.root.display()),
                ))
            }
        }
        let record = PinRecord {
            snapshot_id: view.snapshot_id.clone(),
            scope: view.scope.clone(),
            lease_id: view.lease_id.clone(),
            pinned_at_unix: now_unix(),
        };
        let bytes = serde_json::to_vec_pretty(&record)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        write_atomic(&self.root, PIN_FILE, &bytes)
    }

    pub fn is_pinned(&self) -> Result<bool, SnapshotError> {
        let path = self.root.join(PIN_FILE);
        if !path.exists() {
            return Ok(false);
        }
        let bytes = fs::read(&path).map_err(io_err)?;
        let rec: PinRecord = serde_json::from_slice(&bytes).map_err(|e| {
            SnapshotError::new(
                SnapshotErrorCode::Internal,
                format!("corrupt {PIN_FILE}: {e}"),
            )
        })?;
        match self.stored_view()? {
            Some(v) => Ok(rec.snapshot_id == v.snapshot_id),
            None => Ok(false),
        }
    }

    /// Hydrate (or resume hydrating) a fixed view from a live reader.
    ///
    /// The manifest is walked in full first: an incomplete listing is an
    /// error, never a partial hydration presented as complete.
    pub async fn hydrate(&self, reader: &SnapshotReader) -> Result<HydrateReport, SnapshotError> {
        let view = ViewMeta {
            snapshot_id: reader.snapshot_id().to_string(),
            namespace_view_id: reader.descriptor.namespace_view_id.clone(),
            scope: reader.descriptor.scope.clone(),
            lease_id: reader.lease_id.clone(),
        };
        let manifest = reader.file_manifest().await?;
        let report = self
            .hydrate_with(&view, &manifest, |f| {
                let digest = f.content_digest.clone();
                let path = f.rel_path.clone();
                async move { reader.read_file(&path, &digest).await }
            })
            .await?;
        self.pin(&view)?;
        Ok(report)
    }

    /// Hydration core, parameterised over the byte source so the write-ahead,
    /// resume and verification rules can be regression-tested without HTTP.
    pub async fn hydrate_with<F, Fut>(
        &self,
        view: &ViewMeta,
        manifest: &[SnapshotFile],
        fetch: F,
    ) -> Result<HydrateReport, SnapshotError>
    where
        F: Fn(&SnapshotFile) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>, SnapshotError>>,
    {
        // A different view must never be grafted onto existing durable state.
        if let Some(stored) = self.stored_view()? {
            if stored.snapshot_id != view.snapshot_id {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DurableViewConflict,
                    format!(
                        "store at {} holds view {}, refusing to hydrate {}",
                        self.root.display(),
                        stored.snapshot_id,
                        view.snapshot_id
                    ),
                ));
            }
        }

        // Structural check only: a corrupt journal is still an error, but the
        // reuse decision below is made against the CAS, not the journal.
        let _journal = self.read_journal()?;
        let mut fetched = 0u64;
        let mut resumed = 0u64;
        let mut repaired = 0u64;
        let mut bytes_total = 0u64;

        for f in manifest {
            // Content reuse (spec 11 §10.2): the CAS is content-addressed and
            // shared across the views of one scope, so a byte-identical file
            // from an earlier version is already here. A journal entry is not
            // required — but every hit is re-hashed before it is credited, and
            // a truncated or tampered object is repaired rather than served.
            match self.verify_blob(&f.content_digest, f.size) {
                Ok(true) => {
                    self.append_journal(&FileRecord {
                        rel_path: f.rel_path.clone(),
                        digest: f.content_digest.clone(),
                        size: f.size,
                    })?;
                    resumed += 1;
                    bytes_total += f.size;
                    continue;
                }
                Ok(false) => {
                    if self.blob_path(&f.content_digest)?.exists() {
                        // Present but wrong: drop it and refetch.
                        let _ = fs::remove_file(self.blob_path(&f.content_digest)?);
                        repaired += 1;
                    }
                }
                Err(e) => return Err(e),
            }

            let bytes = fetch(f).await?;
            // The store owns its own correctness: verify whatever the source
            // returned, regardless of whether the source claimed to verify.
            let got = digest_of(&bytes);
            if got != f.content_digest {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    format!("{}: expected {}, got {got}", f.rel_path, f.content_digest),
                ));
            }
            if bytes.len() as u64 != f.size {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    format!(
                        "{}: view advertises {} bytes, content is {}",
                        f.rel_path,
                        f.size,
                        bytes.len()
                    ),
                ));
            }
            write_atomic(&self.content, &blob_name(&f.content_digest), &bytes)?;
            self.append_journal(&FileRecord {
                rel_path: f.rel_path.clone(),
                digest: f.content_digest.clone(),
                size: f.size,
            })?;
            fetched += 1;
            bytes_total += f.size;
        }

        self.finish_hydration(view, manifest, bytes_total, fetched, resumed, repaired)
    }

    /// Concurrent hydration (spec 11 §7): identical verification/write-ahead
    /// rules as [`hydrate_with`], but files are fetched through `fetch` with
    /// up to `concurrency` leaders. The fetcher is expected to merge
    /// identical content itself (single-flight); here we merge the durable
    /// side as well so the same content id is written and journaled once
    /// while every logical path is still recorded.
    pub async fn hydrate_concurrent<F>(
        &self,
        view: &ViewMeta,
        manifest: &[SnapshotFile],
        concurrency: usize,
        fetch: F,
    ) -> Result<HydrateReport, SnapshotError>
    where
        F: Fn(
                SnapshotFile,
            ) -> futures::future::BoxFuture<
                'static,
                Result<std::sync::Arc<Vec<u8>>, SnapshotError>,
            > + Send
            + Sync
            + Clone
            + 'static,
    {
        if let Some(stored) = self.stored_view()? {
            if stored.snapshot_id != view.snapshot_id {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DurableViewConflict,
                    format!(
                        "store at {} holds view {}, refusing to hydrate {}",
                        self.root.display(),
                        stored.snapshot_id,
                        view.snapshot_id
                    ),
                ));
            }
        }
        let _journal = self.read_journal()?;
        let fetched = std::sync::atomic::AtomicU64::new(0);
        let resumed = std::sync::atomic::AtomicU64::new(0);
        let repaired = std::sync::atomic::AtomicU64::new(0);
        let bytes_total = std::sync::atomic::AtomicU64::new(0);
        let store = self;

        // Plan: journal credit is decided up front (same snapshot of the
        // journal for all tasks), then fetch+verify+write runs concurrently.
        use futures::stream::{StreamExt, TryStreamExt};
        // Every file goes through the same CAS check: content reuse is a
        // property of the shared store, not of this view's journal.
        let plan: Vec<SnapshotFile> = manifest.to_vec();

        futures::stream::iter(plan)
            .map(Ok::<_, SnapshotError>)
            .try_for_each_concurrent(concurrency.max(1), |f| {
                let fetched = &fetched;
                let resumed = &resumed;
                let repaired = &repaired;
                let bytes_total = &bytes_total;
                let fetch = fetch.clone();
                async move {
                    use std::sync::atomic::Ordering::Relaxed;
                    if store.verify_blob(&f.content_digest, f.size)? {
                        resumed.fetch_add(1, Relaxed);
                        bytes_total.fetch_add(f.size, Relaxed);
                        return Ok(());
                    }
                    if store.blob_path(&f.content_digest)?.exists() {
                        // Present but wrong: drop it and refetch.
                        let _ = fs::remove_file(store.blob_path(&f.content_digest)?);
                        repaired.fetch_add(1, Relaxed);
                    }
                    let bytes: std::sync::Arc<Vec<u8>> = fetch(f.clone()).await?;
                    // The store independently re-verifies, regardless of
                    // any verification the fetch path claimed.
                    let got = digest_of(&bytes);
                    if got != f.content_digest {
                        return Err(SnapshotError::new(
                            SnapshotErrorCode::DigestMismatch,
                            format!("{}: expected {}, got {got}", f.rel_path, f.content_digest),
                        ));
                    }
                    if bytes.len() as u64 != f.size {
                        return Err(SnapshotError::new(
                            SnapshotErrorCode::DigestMismatch,
                            format!(
                                "{}: view advertises {} bytes, content is {}",
                                f.rel_path,
                                f.size,
                                bytes.len()
                            ),
                        ));
                    }
                    write_atomic(&store.content, &blob_name(&f.content_digest), &bytes)?;
                    store.append_journal(&FileRecord {
                        rel_path: f.rel_path.clone(),
                        digest: f.content_digest.clone(),
                        size: f.size,
                    })?;
                    fetched.fetch_add(1, Relaxed);
                    bytes_total.fetch_add(f.size, Relaxed);
                    Ok(())
                }
            })
            .await?;

        let fetched = fetched.load(std::sync::atomic::Ordering::Relaxed);
        let resumed = resumed.load(std::sync::atomic::Ordering::Relaxed);
        let repaired = repaired.load(std::sync::atomic::Ordering::Relaxed);
        let bytes_total = bytes_total.load(std::sync::atomic::Ordering::Relaxed);
        store.finish_hydration(view, manifest, bytes_total, fetched, resumed, repaired)
    }

    /// Shared DURABLE_COMPLETE tail for the sequential and concurrent cores.
    fn finish_hydration(
        &self,
        view: &ViewMeta,
        manifest: &[SnapshotFile],
        bytes_total: u64,
        fetched: u64,
        resumed: u64,
        repaired: u64,
    ) -> Result<HydrateReport, SnapshotError> {
        // Persist the manifest before the marker: an offline reopen must
        // serve exactly the view that was hydrated, not a guess rebuilt from
        // the journal.
        let manifest_bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        write_atomic(&self.root, MANIFEST_FILE, &manifest_bytes)?;

        let marker = CompleteMarker {
            snapshot_id: view.snapshot_id.clone(),
            namespace_view_id: view.namespace_view_id.clone(),
            files: manifest.len() as u64,
            bytes: bytes_total,
            hydrated_at_unix: now_unix(),
        };
        let view_bytes = serde_json::to_vec_pretty(view)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        write_atomic(&self.root, VIEW_FILE, &view_bytes)?;
        let marker_bytes = serde_json::to_vec_pretty(&marker)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        write_atomic(&self.root, COMPLETE_MARKER, &marker_bytes)?;

        Ok(HydrateReport {
            snapshot_id: view.snapshot_id.clone(),
            total_files: manifest.len() as u64,
            fetched,
            resumed,
            repaired,
            bytes_total,
            complete: true,
        })
    }

    /// The persisted manifest of a completed hydration. Refuses to hand out a
    /// manifest when the completeness marker is absent or names another view.
    pub fn manifest(&self) -> Result<Vec<SnapshotFile>, SnapshotError> {
        if !self.is_complete()? {
            return Err(SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                format!("{}: no complete hydration to reopen", self.root.display()),
            ));
        }
        let bytes = fs::read(self.root.join(MANIFEST_FILE)).map_err(io_err)?;
        serde_json::from_slice(&bytes).map_err(|e| {
            SnapshotError::new(
                SnapshotErrorCode::Internal,
                format!("corrupt {MANIFEST_FILE}: {e}"),
            )
        })
    }

    /// Bounded range read of one CAS object: `None` when the object is not in
    /// the store, `Some(bytes)` (exactly `len`, clamped to EOF) when it is.
    ///
    /// This is what keeps a hydrated large file from being materialised whole
    /// on every FUSE read (spec 07 §6 / BODY-12): the digest is validated
    /// before it becomes a path, and the read is a bounded `pread`.
    pub fn pread_blob(
        &self,
        digest: &str,
        offset: u64,
        len: usize,
    ) -> Result<Option<Vec<u8>>, SnapshotError> {
        let path = self.blob_path(digest)?;
        let mut f = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_err(e)),
        };
        use std::io::{Read, Seek, SeekFrom};
        let file_len = f.metadata().map_err(io_err)?.len();
        if offset >= file_len {
            return Ok(Some(Vec::new()));
        }
        let avail = (file_len - offset) as usize;
        let want = len.min(avail);
        f.seek(SeekFrom::Start(offset)).map_err(io_err)?;
        let mut buf = vec![0u8; want];
        f.read_exact(&mut buf).map_err(io_err)?;
        Ok(Some(buf))
    }

    /// Re-verify every blob of `manifest` against its digest. Full re-hash —
    /// this is the durability check, not a fast path.
    pub fn verify_all(&self, manifest: &[SnapshotFile]) -> Result<u64, SnapshotError> {
        let mut verified = 0u64;
        for f in manifest {
            if !self.verify_blob(&f.content_digest, f.size)? {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DigestMismatch,
                    format!("{}: local copy does not match the view digest", f.rel_path),
                ));
            }
            verified += 1;
        }
        Ok(verified)
    }

    /// Read a hydrated file by CAS digest, re-verifying before returning it.
    pub fn read_blob(&self, digest: &str, expected_size: u64) -> Result<Vec<u8>, SnapshotError> {
        let path = self.blob_path(digest)?;
        let bytes = fs::read(&path).map_err(|e| {
            SnapshotError::new(
                SnapshotErrorCode::PathNotFound,
                format!("{}: {e}", path.display()),
            )
        })?;
        if digest_of(&bytes) != digest || bytes.len() as u64 != expected_size {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("{}: local copy does not match {digest}", path.display()),
            ));
        }
        Ok(bytes)
    }

    /// True when the blob exists, has the advertised size and hashes correctly.
    fn verify_blob(&self, digest: &str, expected_size: u64) -> Result<bool, SnapshotError> {
        let path = self.blob_path(digest)?;
        let meta = match fs::metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(io_err(e)),
        };
        if meta.len() != expected_size {
            return Ok(false);
        }
        let bytes = fs::read(&path).map_err(io_err)?;
        Ok(digest_of(&bytes) == digest)
    }

    /// Read the journal, tolerating a torn final line (crash mid-append) and
    /// nothing else: a record that cannot be parsed anywhere but the tail
    /// means the journal is not a trustworthy resume hint, so it is an error
    /// rather than a silently smaller set of hydrated files.
    fn read_journal(&self) -> Result<HashMap<String, FileRecord>, SnapshotError> {
        let path = self.root.join(JOURNAL_FILE);
        let bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(e) => return Err(io_err(e)),
        };
        let terminated = bytes.ends_with(b"\n");
        // Lossy is safe here: only a torn tail can hold a partial UTF-8
        // sequence, and the torn tail is the one line we drop.
        let text = String::from_utf8_lossy(&bytes);
        let mut lines: Vec<&str> = text.split('\n').collect();
        if !terminated {
            lines.pop();
        }
        let mut out = HashMap::new();
        for (idx, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<FileRecord>(line) {
                Ok(rec) => {
                    out.insert(rec.rel_path.clone(), rec);
                }
                Err(e) => {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::Internal,
                        format!("journal record {idx} unreadable ({e}): {line}"),
                    ));
                }
            }
        }
        Ok(out)
    }

    fn append_journal(&self, rec: &FileRecord) -> Result<(), SnapshotError> {
        let line = serde_json::to_string(rec)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join(JOURNAL_FILE))
            .map_err(io_err)?;
        f.write_all(line.as_bytes()).map_err(io_err)?;
        f.write_all(b"\n").map_err(io_err)?;
        f.sync_all().map_err(io_err)
    }
}

fn blob_name(digest: &str) -> String {
    digest.strip_prefix("sha256:").unwrap_or(digest).to_string()
}

/// Content digest in the view's wire form (`sha256:<hex>`).
pub fn digest_of(bytes: &[u8]) -> String {
    let mut cx = Context::new(&SHA256);
    cx.update(bytes);
    format!("sha256:{}", hex::encode(cx.finish().as_ref()))
}

/// Write `data` to `dir/name` atomically: temp file, fsync, rename, fsync dir.
/// A crash leaves either the old object or nothing — never a half-written one.
fn write_atomic(dir: &Path, name: &str, data: &[u8]) -> Result<(), SnapshotError> {
    fs::create_dir_all(dir).map_err(io_err)?;
    // Random per writer: concurrent hydrations of identical content may race
    // on the same final name but must not share a tmp path.
    let tmp = dir.join(format!(
        ".{name}.tmp.{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    {
        let mut f = File::create(&tmp).map_err(io_err)?;
        f.write_all(data).map_err(io_err)?;
        f.sync_all().map_err(io_err)?;
    }
    fs::rename(&tmp, dir.join(name)).map_err(io_err)?;
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn io_err(e: io::Error) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::Internal, format!("local store: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn file(rel: &str, content: &[u8]) -> (SnapshotFile, Vec<u8>) {
        (
            SnapshotFile {
                rel_path: rel.to_string(),
                fs_kind: "file".to_string(),
                size: content.len() as u64,
                content_digest: digest_of(content),
            },
            content.to_vec(),
        )
    }

    fn meta(snapshot_id: &str) -> ViewMeta {
        ViewMeta {
            snapshot_id: snapshot_id.to_string(),
            namespace_view_id: "sha256:view".to_string(),
            scope: "/project".to_string(),
            lease_id: "lease-1".to_string(),
        }
    }

    fn manifest_fixture() -> (Vec<SnapshotFile>, HashMap<String, Vec<u8>>) {
        let mut files = Vec::new();
        let mut content = HashMap::new();
        for (name, body) in [
            ("README.md", b"# project\n".as_slice()),
            ("src/main.rs", b"fn main() {}\n".as_slice()),
            ("assets/logo.bin", b"\x00\x01\x02\x03".as_slice()),
        ] {
            let (f, bytes) = file(name, body);
            content.insert(f.rel_path.clone(), bytes);
            files.push(f);
        }
        (files, content)
    }

    async fn hydrate_counted(
        store: &DurableStore,
        manifest: &[SnapshotFile],
        content: &HashMap<String, Vec<u8>>,
        calls: &RefCell<Vec<String>>,
    ) -> Result<HydrateReport, SnapshotError> {
        store
            .hydrate_with(&meta("sha256:snap"), manifest, |f| {
                calls.borrow_mut().push(f.rel_path.clone());
                let bytes = content.get(&f.rel_path).cloned().ok_or_else(|| {
                    SnapshotError::new(SnapshotErrorCode::PathNotFound, "missing fixture")
                });
                async move { bytes }
            })
            .await
    }

    #[tokio::test]
    async fn hydrate_then_resume_refetches_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let (manifest, content) = manifest_fixture();
        let store = DurableStore::open(tmp.path()).unwrap();

        let calls = RefCell::new(Vec::new());
        let first = hydrate_counted(&store, &manifest, &content, &calls)
            .await
            .unwrap();
        assert_eq!(first.fetched, 3);
        assert_eq!(first.resumed, 0);
        assert!(first.complete);
        assert_eq!(calls.borrow().len(), 3);
        assert!(store.is_complete().unwrap());

        // Re-open: journal + re-hash verification means zero network traffic.
        let store2 = DurableStore::open(tmp.path()).unwrap();
        let calls2 = RefCell::new(Vec::new());
        let second = hydrate_counted(&store2, &manifest, &content, &calls2)
            .await
            .unwrap();
        assert_eq!(second.fetched, 0);
        assert_eq!(second.resumed, 3);
        assert_eq!(second.repaired, 0);
        assert!(calls2.borrow().is_empty(), "resume must not refetch");
        assert_eq!(second.bytes_total, first.bytes_total);
    }

    #[tokio::test]
    async fn truncated_blob_is_repaired_not_served() {
        let tmp = tempfile::tempdir().unwrap();
        let (manifest, content) = manifest_fixture();
        let store = DurableStore::open(tmp.path()).unwrap();
        let calls = RefCell::new(Vec::new());
        hydrate_counted(&store, &manifest, &content, &calls)
            .await
            .unwrap();

        // Simulate a torn write: blob loses its tail but the journal still
        // claims the file is hydrated.
        let victim = &manifest[1];
        std::fs::write(store.blob_path(&victim.content_digest).unwrap(), b"fn main").unwrap();

        let store2 = DurableStore::open(tmp.path()).unwrap();
        let calls2 = RefCell::new(Vec::new());
        let report = hydrate_counted(&store2, &manifest, &content, &calls2)
            .await
            .unwrap();
        assert_eq!(calls2.borrow().len(), 1, "only the torn file is refetched");
        assert_eq!(calls2.borrow()[0], victim.rel_path);
        assert_eq!(report.repaired, 1);
        assert_eq!(report.fetched, 1);
        assert_eq!(report.resumed, 2);
        store2.verify_all(&manifest).unwrap();
    }

    #[tokio::test]
    async fn wrong_bytes_from_source_are_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let (manifest, _content) = manifest_fixture();
        let store = DurableStore::open(tmp.path()).unwrap();
        let report = store
            .hydrate_with(&meta("sha256:snap"), &manifest, |_f| async {
                Ok(b"not what the view promised".to_vec())
            })
            .await;
        let err = report.expect_err("digest mismatch must fail the hydration");
        assert_eq!(err.code, SnapshotErrorCode::DigestMismatch);
        assert!(
            !store.is_complete().unwrap(),
            "no marker after a failed pass"
        );
        assert!(!tmp.path().join(COMPLETE_MARKER).exists());
    }

    #[tokio::test]
    async fn view_conflict_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let (manifest, content) = manifest_fixture();
        let store = DurableStore::open(tmp.path()).unwrap();
        let calls = RefCell::new(Vec::new());
        hydrate_counted(&store, &manifest, &content, &calls)
            .await
            .unwrap();

        let other = ViewMeta {
            snapshot_id: "sha256:other".to_string(),
            ..meta("x")
        };
        let err = store
            .hydrate_with(&other, &manifest, |_f| async { Ok(Vec::new()) })
            .await
            .expect_err("must not graft a second view onto the store");
        assert_eq!(err.code, SnapshotErrorCode::DurableViewConflict);
        assert!(store.is_complete().unwrap(), "original marker untouched");
    }

    #[tokio::test]
    async fn malformed_digest_is_rejected_not_treated_as_a_path() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(tmp.path()).unwrap();
        // A crafted digest must never address a file outside the store: the
        // rejection happens before any filesystem call.
        for bad in [
            "sha256:../../victim.bin",
            "sha256:gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg", // non-hex
            "sha256:abcd",                                                             // too short
            "/etc/passwd", // no prefix, not hex
        ] {
            let err = store
                .verify_blob(bad, 0)
                .expect_err("malformed digest must be rejected");
            assert_eq!(err.code, SnapshotErrorCode::DigestMismatch, "{bad}");
        }
        // The canary outside the store was never created: the rejection
        // happened at digest validation, before any filesystem call.
        assert!(!tmp.path().join("victim.bin").exists());
        assert!(!store.content_dir().join("../../victim.bin").exists());
    }

    #[tokio::test]
    async fn hydrate_with_writes_where_reads_look() {
        // The documented layout: per-view metadata under `root`, shared
        // content in a separate directory. A hydration into that layout must
        // be readable through the same store (regression: the sequential
        // core once wrote to root/blobs while reads used the content dir).
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("view");
        let content = tmp.path().join("shared-blobs");
        let store = DurableStore::open_with_content(&root, &content).unwrap();
        let (manifest, content_map) = manifest_fixture();
        let calls = RefCell::new(Vec::new());
        let report = hydrate_counted(&store, &manifest, &content_map, &calls)
            .await
            .unwrap();
        assert!(report.complete);
        // Every blob must be readable through the store's own content dir.
        store.verify_all(&manifest).unwrap();
        for f in &manifest {
            assert!(
                content.join(blob_name(&f.content_digest)).exists(),
                "blob for {} missing from the shared content dir",
                f.rel_path
            );
        }
    }

    #[tokio::test]
    async fn reuse_is_content_addressed_not_journal_dependent() {
        let tmp = tempfile::tempdir().unwrap();
        let (manifest, content) = manifest_fixture();
        let store = DurableStore::open(tmp.path()).unwrap();
        let calls = RefCell::new(Vec::new());
        hydrate_counted(&store, &manifest, &content, &calls)
            .await
            .unwrap();

        // The journal is a resume hint, not the reuse authority: with the
        // content still verified in the CAS, a missing journal entry must
        // not cause a re-download (the shared store is content-addressed).
        std::fs::remove_file(tmp.path().join(JOURNAL_FILE)).unwrap();
        let calls2 = RefCell::new(Vec::new());
        let report = hydrate_counted(&store, &manifest, &content, &calls2)
            .await
            .unwrap();
        assert_eq!(report.fetched, 0, "CAS hits must not refetch");
        assert_eq!(report.resumed, 3);
        assert!(calls2.borrow().is_empty());

        // What *does* force a refetch is the content itself going away:
        // only the missing object is fetched, and the journal is rebuilt.
        let victim = &manifest[1];
        std::fs::remove_file(store.blob_path(&victim.content_digest).unwrap()).unwrap();
        let calls3 = RefCell::new(Vec::new());
        let report = hydrate_counted(&store, &manifest, &content, &calls3)
            .await
            .unwrap();
        assert_eq!(calls3.borrow().as_slice().len(), 1, "one missing object");
        assert_eq!(report.fetched, 1);
        assert_eq!(report.resumed, 2);
        store.verify_all(&manifest).unwrap();
    }

    #[tokio::test]
    async fn pin_requires_a_matching_hydrated_view() {
        let tmp = tempfile::tempdir().unwrap();
        let (manifest, content) = manifest_fixture();
        let store = DurableStore::open(tmp.path()).unwrap();
        assert!(!store.is_pinned().unwrap());
        // Nothing hydrated here: a pin would claim protection this store
        // cannot back, so it is refused.
        let err = store.pin(&meta("sha256:snap")).unwrap_err();
        assert_eq!(err.code, SnapshotErrorCode::SnapshotNotReady);

        let calls = RefCell::new(Vec::new());
        hydrate_counted(&store, &manifest, &content, &calls)
            .await
            .unwrap();
        store.pin(&meta("sha256:snap")).unwrap();
        assert!(store.is_pinned().unwrap());
        assert!(tmp.path().join(PIN_FILE).metadata().unwrap().len() > 0);
        // Pinning a different view onto this store is refused.
        let err = store.pin(&meta("sha256:other")).unwrap_err();
        assert_eq!(err.code, SnapshotErrorCode::DurableViewConflict);
        assert!(store.is_pinned().unwrap());

        // A pin recorded for one view does not cover another view's store.
        let other = DurableStore::open(tmp.path().join("other")).unwrap();
        let err = other.pin(&meta("sha256:snap")).unwrap_err();
        assert_eq!(err.code, SnapshotErrorCode::SnapshotNotReady);
    }

    #[test]
    fn read_blob_detects_tampering() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(tmp.path()).unwrap();
        let body = b"hello durable";
        let digest = digest_of(body);
        write_atomic(&store.root.join(BLOB_DIR), &blob_name(&digest), body).unwrap();
        assert_eq!(store.read_blob(&digest, body.len() as u64).unwrap(), body);

        std::fs::write(store.blob_path(&digest).unwrap(), b"hello durab1e").unwrap();
        let err = store
            .read_blob(&digest, body.len() as u64)
            .expect_err("tampered blob must not be served");
        assert_eq!(err.code, SnapshotErrorCode::DigestMismatch);
    }

    #[test]
    fn torn_journal_tail_is_tolerated_but_garbage_is_not() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(tmp.path()).unwrap();
        let rec = FileRecord {
            rel_path: "a.txt".into(),
            digest: digest_of(b"a"),
            size: 1,
        };
        store.append_journal(&rec).unwrap();

        // Torn tail: a partial JSON fragment without a newline. This is what a
        // crash between write and fsync leaves behind, so the earlier records
        // stay usable.
        let mut f = OpenOptions::new()
            .append(true)
            .open(tmp.path().join(JOURNAL_FILE))
            .unwrap();
        f.write_all(br#"{"rel_path":"b.tx"#).unwrap();
        drop(f);
        assert_eq!(store.read_journal().unwrap().len(), 1);

        // A record that is not the tail cannot be explained by a crash, so the
        // journal as a whole is rejected rather than silently truncated.
        let mut f = OpenOptions::new()
            .append(true)
            .open(tmp.path().join(JOURNAL_FILE))
            .unwrap();
        f.write_all(b"\nnot json at all\n").unwrap();
        f.write_all(b"{\"rel_path\":\"c.txt\",\"digest\":\"sha256:0\",\"size\":1}\n")
            .unwrap();
        drop(f);
        let err = store.read_journal().unwrap_err();
        assert_eq!(err.code, SnapshotErrorCode::Internal);
    }

    #[test]
    fn scope_and_snapshot_map_to_a_stable_path() {
        let root = Path::new("/var/lib/scorpio");
        let p = DurableStore::path_for(root, "/project", "sha256:abc123");
        assert!(p.ends_with("snapshots/project/abc123"));
        assert_eq!(DurableStore::path_for(root, "/project", "sha256:abc123"), p);
    }
}
