//! Durable local hydration of one fixed view (spec 11 client-side sync).
//!
//! A hydrated view is a content-addressed store plus an append-only journal:
//!
//! ```text
//! <root>/view.json            descriptor binding (snapshot id, scope, view id)
//! <root>/journal.log          one JSON record per completed file (fsync'd)
//! <root>/blobs/<hex>          file content, addressed by SHA-256
//! <root>/descriptor.bin       canonical descriptor for a full snapshot
//! <root>/metadata/<hex>       private canonical pages for a full snapshot
//! <root>/metadata.json        full snapshot page/directory closure index
//! <root>/DURABLE_COMPLETE     typed file-closure or full-snapshot commitment
//! <root>/pin.json             local retention bound to the committed closure
//! ```
//!
//! Two invariants drive the design:
//!
//! * **Completeness is proven, never assumed.** `DURABLE_COMPLETE` is written
//!   last, after every manifest file has been fetched, digest-checked and
//!   fsync'd. An interrupted hydration leaves the marker absent and is
//!   resumed, not accepted.
//! * **Reopen re-verifies.** The marker binds the view, manifest and pin, and
//!   every referenced CAS object is checked before completeness is accepted.
//! * **Resume re-verifies.** A journal entry is a hint, not proof: on resume
//!   each recorded blob is re-hashed before it counts as hydrated, so a
//!   truncated, torn or tampered CAS object is re-fetched rather than served.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::snapshot::{
    closure::{SnapshotDirectory, ValidatedSnapshotClosure},
    SnapshotError, SnapshotErrorCode, SnapshotFile, SnapshotReader,
};

const VIEW_FILE: &str = "view.json";
const MANIFEST_FILE: &str = "manifest.json";
const JOURNAL_FILE: &str = "journal.log";
const COMPLETE_MARKER: &str = "DURABLE_COMPLETE";
const PIN_FILE: &str = "pin.json";
const BLOB_DIR: &str = "blobs";
const TRANSACTION_LOCK: &str = ".hydrate.lock";
const REPAIR_FILE: &str = "NEEDS_REPAIR";
const VERIFICATION_REVISION: u32 = 2;
const SNAPSHOT_VERIFICATION_REVISION: u32 = 3;
const DESCRIPTOR_FILE: &str = "descriptor.bin";
const METADATA_INDEX_FILE: &str = "metadata.json";
const METADATA_DIR: &str = "metadata";

/// What a local completion record actually proves. Neither kind grants access.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompletionKind {
    #[default]
    FileClosure,
    FullSnapshot,
}

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
#[serde(deny_unknown_fields)]
struct CompleteMarker {
    #[serde(default)]
    verification_revision: u32,
    #[serde(default)]
    completion_kind: CompletionKind,
    snapshot_id: String,
    namespace_view_id: String,
    #[serde(default)]
    view_digest: String,
    #[serde(default)]
    manifest_digest: String,
    #[serde(default)]
    pin_digest: String,
    files: u64,
    bytes: u64,
    hydrated_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PinRecord {
    #[serde(default)]
    verification_revision: u32,
    #[serde(default)]
    pin_id: String,
    snapshot_id: String,
    #[serde(default)]
    namespace_view_id: String,
    scope: String,
    lease_id: String,
    #[serde(default)]
    view_digest: String,
    #[serde(default)]
    manifest_digest: String,
    #[serde(default)]
    blobs: Vec<BlobDependency>,
    pinned_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BlobDependency {
    digest: String,
    size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PageDependency {
    page_id: String,
    size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataIndex {
    verification_revision: u32,
    pages: Vec<PageDependency>,
    directories: Vec<SnapshotDirectory>,
}

struct MetadataCommit {
    descriptor_digest: String,
    metadata_index_digest: String,
    pages: Vec<PageDependency>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotPinRecord {
    verification_revision: u32,
    pin_id: String,
    snapshot_id: String,
    namespace_view_id: String,
    scope: String,
    lease_id: String,
    view_digest: String,
    manifest_digest: String,
    descriptor_digest: String,
    metadata_index_digest: String,
    pages: Vec<PageDependency>,
    blobs: Vec<BlobDependency>,
    pinned_at_unix: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotCompleteMarker {
    verification_revision: u32,
    completion_kind: CompletionKind,
    snapshot_id: String,
    namespace_view_id: String,
    view_digest: String,
    manifest_digest: String,
    descriptor_digest: String,
    metadata_index_digest: String,
    pin_digest: String,
    files: u64,
    directories: u64,
    pages: u64,
    bytes: u64,
    hydrated_at_unix: u64,
}

/// Outcome of one hydration pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydrateReport {
    pub snapshot_id: String,
    pub total_files: u64,
    /// Content fetch units downloaded in this pass. OBJECT batches merge
    /// paths sharing a digest, so this can be smaller than logical files.
    pub fetched: u64,
    /// Files that were already hydrated and re-verified.
    pub resumed: u64,
    /// Resumed files whose CAS object failed re-verification and were refetched.
    pub repaired: u64,
    pub bytes_total: u64,
    /// Complete for `completion_kind`, not necessarily a full snapshot.
    pub complete: bool,
    /// File-only APIs cannot claim a full metadata/content closure.
    pub completion_kind: CompletionKind,
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

/// Transaction lifetime owns the lock, even if fork/dup retains a descriptor.
struct TransactionGuard(File);

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error, "failed to release local hydration lock");
        }
    }
}

impl DurableStore {
    /// Check authority before the constructor can recover or alter a marker.
    pub fn open_for_reader(
        root: impl Into<PathBuf>,
        content: impl Into<PathBuf>,
        reader: &SnapshotReader,
    ) -> Result<Self, SnapshotError> {
        let root = root.into();
        let content = content.into();
        let context = reader.authorized_context();
        context.bind_view_cache(&root)?;
        context.bind_scope_cache(&content)?;
        let store = Self::open_with_content(root, content)?;
        store.bind_reader(reader)?;
        Ok(store)
    }

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
        create_dirs_durable(&root)?;
        create_dirs_durable(&content)?;
        let store = Self { root, content };
        // Recovery uses the same lock as publication: an active writer must
        // never have its in-progress state invalidated by another opener.
        if let Some(_transaction) = store.try_transaction()? {
            store.completed_manifest_locked()?;
        }
        Ok(store)
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

    /// Establish the reader's immutable authority before accessing local data.
    pub fn bind_reader(&self, reader: &SnapshotReader) -> Result<(), SnapshotError> {
        let context = reader.authorized_context();
        context.bind_view_cache(self.root())?;
        context.bind_scope_cache(self.content_dir())?;
        if let Some(view) = self.stored_view()? {
            if view.snapshot_id != reader.snapshot_id()
                || view.scope != reader.descriptor().scope
                || view.namespace_view_id != reader.descriptor().namespace_view_id
            {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::ScopeForbidden,
                    "stored view differs from the reader's authorized snapshot",
                ));
            }
        }
        Ok(())
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

    /// True when the recorded completion kind verifies. Revision 2 proves only
    /// files; use `is_snapshot_complete` for the full metadata/content contract.
    pub fn is_complete(&self) -> Result<bool, SnapshotError> {
        let Some(_transaction) = self.try_transaction()? else {
            return Ok(false);
        };
        Ok(self.completed_manifest_locked()?.is_some())
    }

    /// The exact audited guarantee, without treating a file list as a snapshot.
    pub fn completion_kind(&self) -> Result<Option<CompletionKind>, SnapshotError> {
        let Some(_transaction) = self.try_transaction()? else {
            return Ok(None);
        };
        if self.completed_manifest_locked()?.is_none() {
            return Ok(None);
        }
        let bytes = required_dependency(&self.root.join(COMPLETE_MARKER))?;
        Ok(Some(
            if completion_revision(&bytes)? == SNAPSHOT_VERIFICATION_REVISION {
                CompletionKind::FullSnapshot
            } else {
                CompletionKind::FileClosure
            },
        ))
    }

    /// True only for a root-verified descriptor, metadata graph and content.
    /// This is a local retention/integrity claim, never an authorization grant.
    pub fn is_snapshot_complete(&self) -> Result<bool, SnapshotError> {
        Ok(self.completion_kind()? == Some(CompletionKind::FullSnapshot))
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
        let _transaction = self.transaction()?;
        match self.stored_view()? {
            Some(stored) if stored == *view => {}
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
        // Hydration publishes its pin before its commit record. Repeating
        // pin() is idempotent; rewriting it would break the marker's binding.
        self.completed_manifest_locked()?.ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "a pin requires a complete, verified file closure",
            )
        })?;
        Ok(())
    }

    pub fn is_pinned(&self) -> Result<bool, SnapshotError> {
        self.is_complete()
    }

    /// A pin file written during prepare is not a committed retention claim.
    /// Cache discovery uses the same dependency audit and view lock as reopen.
    pub(crate) fn committed_snapshot_at(
        root: &Path,
        content: &Path,
    ) -> Result<Option<String>, SnapshotError> {
        if !root.join(COMPLETE_MARKER).exists() {
            return Ok(None);
        }
        let store = Self {
            root: root.to_path_buf(),
            content: content.to_path_buf(),
        };
        let Some(_transaction) = store.try_transaction()? else {
            return Ok(None);
        };
        if store.completed_manifest_locked()?.is_none() {
            return Ok(None);
        }
        Ok(store.stored_view()?.map(|view| view.snapshot_id))
    }

    /// Hydrate (or resume hydrating) a fixed view from a live reader.
    ///
    /// The manifest is walked in full first: an incomplete listing is an
    /// error, never a partial hydration presented as complete.
    pub async fn hydrate(&self, reader: &SnapshotReader) -> Result<HydrateReport, SnapshotError> {
        self.bind_reader(reader)?;
        let view = ViewMeta {
            snapshot_id: reader.snapshot_id().to_string(),
            namespace_view_id: reader.descriptor().namespace_view_id.clone(),
            scope: reader.descriptor().scope.clone(),
            lease_id: reader.lease_id().to_string(),
        };
        let manifest = reader.file_manifest().await?;
        self.hydrate_file_closure(
            &view,
            &manifest,
            None,
            |f| {
                let digest = f.content_digest.clone();
                let path = f.rel_path.clone();
                async move { reader.read_file(&path, &digest).await }
            },
            Some(reader),
        )
        .await
    }

    /// Hydrate the complete fixed graph selected by an authorized online reader.
    pub async fn hydrate_snapshot(
        &self,
        reader: &SnapshotReader,
    ) -> Result<HydrateReport, SnapshotError> {
        self.bind_reader(reader)?;
        let closure = reader.snapshot_closure().await?;
        let view = ViewMeta {
            snapshot_id: reader.snapshot_id().to_string(),
            namespace_view_id: reader.descriptor().namespace_view_id.clone(),
            scope: reader.descriptor().scope.clone(),
            lease_id: reader.lease_id().to_string(),
        };
        validate_snapshot_view(&view, &closure)?;
        self.hydrate_file_closure(
            &view,
            closure.files(),
            Some(&closure),
            |file| {
                let path = file.rel_path.clone();
                let digest = file.content_digest.clone();
                async move { reader.read_file(&path, &digest).await }
            },
            Some(reader),
        )
        .await
    }

    /// Local integrity primitive with an independently root-validated closure.
    /// The caller must establish its own authority; no offline grant is created.
    pub async fn hydrate_snapshot_with<F, Fut>(
        &self,
        view: &ViewMeta,
        closure: &ValidatedSnapshotClosure,
        fetch: F,
    ) -> Result<HydrateReport, SnapshotError>
    where
        F: Fn(&SnapshotFile) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>, SnapshotError>>,
    {
        validate_snapshot_view(view, closure)?;
        self.hydrate_file_closure(view, closure.files(), Some(closure), fetch, None)
            .await
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
        self.hydrate_file_closure(view, manifest, None, fetch, None)
            .await
    }

    async fn hydrate_file_closure<F, Fut>(
        &self,
        view: &ViewMeta,
        manifest: &[SnapshotFile],
        closure: Option<&ValidatedSnapshotClosure>,
        fetch: F,
        stream_reader: Option<&SnapshotReader>,
    ) -> Result<HydrateReport, SnapshotError>
    where
        F: Fn(&SnapshotFile) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>, SnapshotError>>,
    {
        let _transaction = self.prepare_hydration_for_kind(view, manifest, closure.is_some())?;
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
                        // Present but wrong: refetch and atomically replace it.
                        repaired += 1;
                    }
                }
                Err(e) => return Err(e),
            }

            if let Some(reader) = stream_reader.filter(|reader| {
                f.size > crate::snapshot::OBJECT_CAP && reader.capabilities().features.chunk_reads
            }) {
                write_reader_blob(&self.content, reader, f).await?;
            } else {
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
            }
            self.append_journal(&FileRecord {
                rel_path: f.rel_path.clone(),
                digest: f.content_digest.clone(),
                size: f.size,
            })?;
            fetched += 1;
            bytes_total += f.size;
        }

        match closure {
            Some(closure) => self.finish_hydration_commit(
                view,
                manifest,
                Some(closure),
                (fetched, resumed, repaired),
            ),
            None => self.finish_hydration(view, manifest, bytes_total, fetched, resumed, repaired),
        }
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
        let _transaction = self.prepare_hydration(view, manifest)?;
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
                        // Present but wrong: refetch and atomically replace it.
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

    /// Batched hydration: same verification, write-ahead, resume and journal
    /// rules as [`hydrate_concurrent`], but small files (≤ [`OBJECT_CAP`]) are
    /// fetched in OBJECT batches (≤128 unique digests, ≤7 MiB raw per request
    /// — spec 14 §4 batch limits with headroom) instead of one request per
    /// file. Large files still go through `fetch_large` (chunk-map + CHUNK).
    ///
    /// `fetch_batch` receives the batch's files (deduplicated by digest) and
    /// must return every requested digest; missing digests are an error, and
    /// every returned byte is re-verified here regardless of transport claims.
    pub async fn hydrate_batches<FBatch, FLarge>(
        &self,
        view: &ViewMeta,
        manifest: &[SnapshotFile],
        batch_concurrency: usize,
        large_concurrency: usize,
        fetch_batch: FBatch,
        fetch_large: FLarge,
    ) -> Result<HydrateReport, SnapshotError>
    where
        FBatch: Fn(
                Vec<SnapshotFile>,
            ) -> futures::future::BoxFuture<
                'static,
                Result<std::collections::HashMap<String, std::sync::Arc<Vec<u8>>>, SnapshotError>,
            > + Send
            + Sync
            + Clone
            + 'static,
        FLarge: Fn(
                SnapshotFile,
            ) -> futures::future::BoxFuture<
                'static,
                Result<std::sync::Arc<Vec<u8>>, SnapshotError>,
            > + Send
            + Sync
            + Clone
            + 'static,
    {
        use std::{collections::HashMap as BufMap, sync::atomic::Ordering::Relaxed};

        use futures::stream::{StreamExt, TryStreamExt};

        let _transaction = self.prepare_hydration(view, manifest)?;
        let fetched = std::sync::atomic::AtomicU64::new(0);
        let resumed = std::sync::atomic::AtomicU64::new(0);
        let repaired = std::sync::atomic::AtomicU64::new(0);
        let bytes_total = std::sync::atomic::AtomicU64::new(0);
        let store = self;

        // Phase 1: CAS reuse check decides what actually needs fetching.
        // Paths sharing a digest are journaled individually but fetched once.
        let mut need: Vec<SnapshotFile> = Vec::new();
        for f in manifest {
            match store.verify_blob(&f.content_digest, f.size) {
                Ok(true) => {
                    resumed.fetch_add(1, Relaxed);
                    bytes_total.fetch_add(f.size, Relaxed);
                    store.append_journal(&FileRecord {
                        rel_path: f.rel_path.clone(),
                        digest: f.content_digest.clone(),
                        size: f.size,
                    })?;
                }
                Ok(false) => {
                    if store.blob_path(&f.content_digest)?.exists() {
                        repaired.fetch_add(1, Relaxed);
                    }
                    need.push(f.clone());
                }
                Err(e) => return Err(e),
            }
        }

        // Phase 2: split small (OBJECT batch) from large (chunk path).
        const OBJECT_CAP: u64 = 256 * 1024;
        const BATCH_MAX_FILES: usize = 128;
        const BATCH_MAX_BYTES: u64 = 7 * 1024 * 1024;
        let (mut small, large): (Vec<SnapshotFile>, Vec<SnapshotFile>) =
            need.into_iter().partition(|f| f.size <= OBJECT_CAP);
        // Deduplicate small files by digest: one fetch unit per content.
        small.sort_by(|a, b| a.content_digest.cmp(&b.content_digest));
        small.dedup_by(|a, b| a.content_digest == b.content_digest);

        // Group into batches under the server's per-request limits.
        let mut batches: Vec<Vec<SnapshotFile>> = Vec::new();
        let mut cur: Vec<SnapshotFile> = Vec::new();
        let mut cur_bytes = 0u64;
        for f in small.drain(..) {
            if cur.len() >= BATCH_MAX_FILES || cur_bytes + f.size > BATCH_MAX_BYTES {
                batches.push(std::mem::take(&mut cur));
                cur_bytes = 0;
            }
            cur_bytes += f.size;
            cur.push(f);
        }
        if !cur.is_empty() {
            batches.push(cur);
        }

        // Phase 3: fetch batches concurrently; verify + write + journal.
        let fetched_b = &fetched;
        let bytes_b = &bytes_total;
        futures::stream::iter(batches)
            .map(Ok::<_, SnapshotError>)
            .try_for_each_concurrent(batch_concurrency.max(1), |batch| {
                let fetch_batch = fetch_batch.clone();
                async move {
                    let bytes = fetch_batch(batch.clone()).await?;
                    let mut by_digest: BufMap<String, std::sync::Arc<Vec<u8>>> = BufMap::new();
                    for (digest, data) in bytes {
                        by_digest.insert(digest, data);
                    }
                    let mut journal_lines: Vec<String> = Vec::new();
                    for f in &batch {
                        let data = by_digest.remove(&f.content_digest).ok_or_else(|| {
                            SnapshotError::new(
                                SnapshotErrorCode::DigestMismatch,
                                format!(
                                    "{}: objects batch did not return {}",
                                    f.rel_path, f.content_digest
                                ),
                            )
                        })?;
                        let got = digest_of(&data);
                        if got != f.content_digest {
                            return Err(SnapshotError::new(
                                SnapshotErrorCode::DigestMismatch,
                                format!("{}: expected {}, got {got}", f.rel_path, f.content_digest),
                            ));
                        }
                        if data.len() as u64 != f.size {
                            return Err(SnapshotError::new(
                                SnapshotErrorCode::DigestMismatch,
                                format!(
                                    "{}: view advertises {} bytes, content is {}",
                                    f.rel_path,
                                    f.size,
                                    data.len()
                                ),
                            ));
                        }
                        // The journal cannot make another file's data durable.
                        // Each CAS object is synced before the batch journal.
                        write_atomic(
                            &store.content,
                            &blob_name(&f.content_digest),
                            data.as_slice(),
                        )?;
                        journal_lines.push(
                            serde_json::to_string(&FileRecord {
                                rel_path: f.rel_path.clone(),
                                digest: f.content_digest.clone(),
                                size: f.size,
                            })
                            .map_err(|e| {
                                SnapshotError::new(SnapshotErrorCode::Internal, e.to_string())
                            })?,
                        );
                        fetched_b.fetch_add(1, Relaxed);
                        bytes_b.fetch_add(f.size, Relaxed);
                    }
                    store.append_journal_batch(&journal_lines)?;
                    sync_dir(store.content_dir())?;
                    Ok(())
                }
            })
            .await?;

        // Phase 4: large files, one chunked fetch per file, concurrent.
        let fetched_l = &fetched;
        let bytes_l = &bytes_total;
        futures::stream::iter(large)
            .map(Ok::<_, SnapshotError>)
            .try_for_each_concurrent(large_concurrency.max(1), |f| {
                let fetch_large = fetch_large.clone();
                async move {
                    let bytes: std::sync::Arc<Vec<u8>> = fetch_large(f.clone()).await?;
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
                    fetched_l.fetch_add(1, Relaxed);
                    bytes_l.fetch_add(f.size, Relaxed);
                    Ok(())
                }
            })
            .await?;

        let fetched = fetched.load(Relaxed);
        let resumed = resumed.load(Relaxed);
        let repaired = repaired.load(Relaxed);
        let bytes_total = bytes_total.load(Relaxed);
        store.finish_hydration(view, manifest, bytes_total, fetched, resumed, repaired)
    }

    // A file lock is held for the whole publication transaction, including
    // network awaits. It is nonblocking: unrelated async tasks never wait on
    // a short critical-section lock held by a downloading task. Dropping the
    // handle (including after process exit) releases the OS lock.
    fn try_transaction(&self) -> Result<Option<TransactionGuard>, SnapshotError> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join(TRANSACTION_LOCK))
            .map_err(io_err)?;
        match lock.try_lock() {
            Ok(()) => Ok(Some(TransactionGuard(lock))),
            Err(fs::TryLockError::WouldBlock) => Ok(None),
            Err(fs::TryLockError::Error(e)) => Err(io_err(e)),
        }
    }

    fn transaction(&self) -> Result<TransactionGuard, SnapshotError> {
        self.try_transaction()?.ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "another local hydration transaction is active",
            )
        })
    }

    fn prepare_hydration(
        &self,
        view: &ViewMeta,
        manifest: &[SnapshotFile],
    ) -> Result<TransactionGuard, SnapshotError> {
        self.prepare_hydration_for_kind(view, manifest, false)
    }

    fn prepare_hydration_for_kind(
        &self,
        view: &ViewMeta,
        manifest: &[SnapshotFile],
        full_snapshot: bool,
    ) -> Result<TransactionGuard, SnapshotError> {
        let transaction = self.transaction()?;
        validate_view_policy(view, full_snapshot)?;
        // Check identity before revoking anything: a conflicting caller must
        // leave the existing view's complete commitment untouched. A renewed
        // remote lease does not change the fixed view's identity.
        if let Some(stored) = self.stored_view()? {
            if stored.snapshot_id != view.snapshot_id
                || stored.namespace_view_id != view.namespace_view_id
                || stored.scope != view.scope
            {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DurableViewConflict,
                    format!(
                        "store at {} holds a different fixed view",
                        self.root.display()
                    ),
                ));
            }
        }
        validate_manifest_policy(manifest, full_snapshot)?;
        if !full_snapshot
            && read_optional(&self.root.join(COMPLETE_MARKER))?.is_some_and(|marker| {
                completion_revision(&marker)
                    .is_ok_and(|revision| revision == SNAPSHOT_VERIFICATION_REVISION)
            })
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DurableViewConflict,
                "file-only hydration cannot downgrade a full snapshot completion",
            ));
        }
        if let Some(mut previous) = self.bound_manifest()? {
            let mut incoming = manifest.to_vec();
            previous.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
            incoming.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
            if previous != incoming {
                return Err(SnapshotError::new(
                    SnapshotErrorCode::DurableViewConflict,
                    "a fixed view cannot be hydrated with a different file closure",
                ));
            }
        }
        // COMPLETE is a current claim, not a historical flag. Revoke it
        // durably before a repair can fail or be cancelled partway through.
        self.invalidate_complete()?;
        // Persist the fixed-view binding even for an interrupted first pass.
        // A later caller cannot graft a different scope/view onto its journal.
        let view_bytes = serde_json::to_vec_pretty(view)
            .map_err(|error| SnapshotError::new(SnapshotErrorCode::Internal, error.to_string()))?;
        write_atomic(&self.root, VIEW_FILE, &view_bytes)?;
        durability_checkpoint(&self.root, "marker-revoked")?;
        let _journal = self.read_journal()?;
        self.truncate_journal_tail()?;
        Ok(transaction)
    }

    fn bound_manifest(&self) -> Result<Option<Vec<SnapshotFile>>, SnapshotError> {
        let Some(marker_bytes) = read_optional(&self.root.join(COMPLETE_MARKER))? else {
            return Ok(None);
        };
        let manifest_digest = match completion_revision(&marker_bytes) {
            Ok(VERIFICATION_REVISION) => {
                let Ok(marker) = serde_json::from_slice::<CompleteMarker>(&marker_bytes) else {
                    return Ok(None);
                };
                marker.manifest_digest
            }
            Ok(SNAPSHOT_VERIFICATION_REVISION) => {
                let Ok(marker) = serde_json::from_slice::<SnapshotCompleteMarker>(&marker_bytes)
                else {
                    return Ok(None);
                };
                marker.manifest_digest
            }
            _ => return Ok(None),
        };
        let Some(bytes) = read_optional(&self.root.join(MANIFEST_FILE))? else {
            return Ok(None);
        };
        if digest_of(&bytes) != manifest_digest {
            return Ok(None);
        }
        Ok(Some(decode_commit(&bytes, MANIFEST_FILE)?))
    }

    fn invalidate_complete(&self) -> Result<(), SnapshotError> {
        match fs::remove_file(self.root.join(COMPLETE_MARKER)) {
            Ok(()) => sync_dir(&self.root),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_err(e)),
        }
    }

    // The lock must be held by the caller. Recovery does not rely on the
    // append-only journal: that is only a hint, not a committed dependency.
    fn completed_manifest_locked(&self) -> Result<Option<Vec<SnapshotFile>>, SnapshotError> {
        let Some(marker_bytes) = read_optional(&self.root.join(COMPLETE_MARKER))? else {
            return Ok(None);
        };
        match self.verify_commit(&marker_bytes) {
            Ok(manifest) => Ok(Some(manifest)),
            Err(error)
                if matches!(
                    error.code,
                    SnapshotErrorCode::IntegrityError | SnapshotErrorCode::DigestMismatch
                ) =>
            {
                self.invalidate_complete()?;
                write_atomic(&self.root, REPAIR_FILE, error.message.as_bytes())?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn verify_commit(&self, marker_bytes: &[u8]) -> Result<Vec<SnapshotFile>, SnapshotError> {
        if completion_revision(marker_bytes)? == SNAPSHOT_VERIFICATION_REVISION {
            return Ok(self.verify_snapshot_commit(marker_bytes)?.files().to_vec());
        }
        let marker: CompleteMarker = decode_commit(marker_bytes, COMPLETE_MARKER)?;
        if marker.verification_revision != VERIFICATION_REVISION
            || marker.completion_kind != CompletionKind::FileClosure
        {
            return Err(integrity_err("legacy or unsupported completion revision"));
        }
        let view_bytes = required_dependency(&self.root.join(VIEW_FILE))?;
        let manifest_bytes = required_dependency(&self.root.join(MANIFEST_FILE))?;
        let pin_bytes = required_dependency(&self.root.join(PIN_FILE))?;
        if digest_of(&view_bytes) != marker.view_digest
            || digest_of(&manifest_bytes) != marker.manifest_digest
            || digest_of(&pin_bytes) != marker.pin_digest
        {
            return Err(integrity_err("completion metadata digest mismatch"));
        }
        let view: ViewMeta = decode_commit(&view_bytes, VIEW_FILE)?;
        validate_view(&view)?;
        let manifest: Vec<SnapshotFile> = decode_commit(&manifest_bytes, MANIFEST_FILE)?;
        let pin: PinRecord = decode_commit(&pin_bytes, PIN_FILE)?;
        let (dependencies, bytes_total) = validate_manifest(&manifest)?;
        if marker.snapshot_id != view.snapshot_id
            || marker.namespace_view_id != view.namespace_view_id
            || marker.files != manifest.len() as u64
            || marker.bytes != bytes_total
            || pin.verification_revision != VERIFICATION_REVISION
            || pin.pin_id.is_empty()
            || pin.snapshot_id != view.snapshot_id
            || pin.namespace_view_id != view.namespace_view_id
            || pin.scope != view.scope
            || pin.lease_id != view.lease_id
            || pin.view_digest != marker.view_digest
            || pin.manifest_digest != marker.manifest_digest
            || pin.blobs != dependencies
        {
            return Err(integrity_err(
                "completion view, pin or file closure mismatch",
            ));
        }
        for blob in dependencies {
            if !self.verify_blob(&blob.digest, blob.size)? {
                return Err(integrity_err(format!(
                    "completion dependency missing or corrupt: {}",
                    blob.digest
                )));
            }
        }
        Ok(manifest)
    }

    fn metadata_page_path(&self, page_id: &str) -> Result<PathBuf, SnapshotError> {
        let id = crate::snapshot::frames::parse_digest(page_id)
            .map_err(|error| integrity_err(error.message))?;
        Ok(self.root.join(METADATA_DIR).join(hex::encode(id)))
    }

    // These pages are private copies of this view. No shared-page GC or pin
    // transfer is implied, and no path deletes another view's dependencies.
    fn persist_snapshot_metadata(
        &self,
        closure: &ValidatedSnapshotClosure,
    ) -> Result<MetadataCommit, SnapshotError> {
        let mut pages = Vec::with_capacity(closure.pages().len());
        for (page_id, bytes) in closure.pages() {
            let path = self.metadata_page_path(page_id)?;
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| integrity_err("invalid metadata page name"))?;
            write_atomic(&self.root.join(METADATA_DIR), name, bytes)?;
            pages.push(PageDependency {
                page_id: page_id.clone(),
                size: bytes.len() as u64,
            });
        }
        sync_dir(&self.root.join(METADATA_DIR))?;
        durability_checkpoint(&self.root, "metadata-pages-durable")?;
        write_atomic(&self.root, DESCRIPTOR_FILE, closure.descriptor_bytes())?;
        durability_checkpoint(&self.root, "descriptor-durable")?;
        let index = MetadataIndex {
            verification_revision: SNAPSHOT_VERIFICATION_REVISION,
            pages: pages.clone(),
            directories: closure.directories().to_vec(),
        };
        let bytes = encode_record(&index)?;
        write_atomic(&self.root, METADATA_INDEX_FILE, &bytes)?;
        durability_checkpoint(&self.root, "metadata-index-durable")?;
        Ok(MetadataCommit {
            descriptor_digest: digest_of(closure.descriptor_bytes()),
            metadata_index_digest: digest_of(&bytes),
            pages,
        })
    }

    fn verify_snapshot_commit(
        &self,
        marker_bytes: &[u8],
    ) -> Result<ValidatedSnapshotClosure, SnapshotError> {
        let marker: SnapshotCompleteMarker = decode_commit(marker_bytes, COMPLETE_MARKER)?;
        if marker.verification_revision != SNAPSHOT_VERIFICATION_REVISION
            || marker.completion_kind != CompletionKind::FullSnapshot
        {
            return Err(integrity_err("record does not prove a full snapshot"));
        }
        let view_bytes = required_dependency(&self.root.join(VIEW_FILE))?;
        let manifest_bytes = required_dependency(&self.root.join(MANIFEST_FILE))?;
        let pin_bytes = required_dependency(&self.root.join(PIN_FILE))?;
        let descriptor_bytes = required_dependency(&self.root.join(DESCRIPTOR_FILE))?;
        let index_bytes = required_dependency(&self.root.join(METADATA_INDEX_FILE))?;
        if digest_of(&view_bytes) != marker.view_digest
            || digest_of(&manifest_bytes) != marker.manifest_digest
            || digest_of(&pin_bytes) != marker.pin_digest
            || digest_of(&descriptor_bytes) != marker.descriptor_digest
            || digest_of(&index_bytes) != marker.metadata_index_digest
        {
            return Err(integrity_err(
                "snapshot completion metadata digest mismatch",
            ));
        }
        let view: ViewMeta = decode_commit(&view_bytes, VIEW_FILE)?;
        validate_view_policy(&view, true)?;
        let manifest: Vec<SnapshotFile> = decode_commit(&manifest_bytes, MANIFEST_FILE)?;
        let (blobs, bytes_total) = validate_manifest_policy(&manifest, true)?;
        let pin: SnapshotPinRecord = decode_commit(&pin_bytes, PIN_FILE)?;
        let index: MetadataIndex = decode_commit(&index_bytes, METADATA_INDEX_FILE)?;
        if index.verification_revision != SNAPSHOT_VERIFICATION_REVISION
            || marker.snapshot_id != view.snapshot_id
            || marker.namespace_view_id != view.namespace_view_id
            || marker.files != manifest.len() as u64
            || marker.bytes != bytes_total
            || marker.directories != index.directories.len() as u64
            || marker.pages != index.pages.len() as u64
            || pin.verification_revision != SNAPSHOT_VERIFICATION_REVISION
            || pin.pin_id.is_empty()
            || pin.snapshot_id != view.snapshot_id
            || pin.namespace_view_id != view.namespace_view_id
            || pin.scope != view.scope
            || pin.lease_id != view.lease_id
            || pin.view_digest != marker.view_digest
            || pin.manifest_digest != marker.manifest_digest
            || pin.descriptor_digest != marker.descriptor_digest
            || pin.metadata_index_digest != marker.metadata_index_digest
            || pin.pages != index.pages
            || pin.blobs != blobs
        {
            return Err(integrity_err(
                "snapshot view, pin or closure index mismatch",
            ));
        }
        let mut pages = BTreeMap::new();
        for dependency in &index.pages {
            let bytes = required_dependency(&self.metadata_page_path(&dependency.page_id)?)?;
            if bytes.len() as u64 != dependency.size
                || pages.insert(dependency.page_id.clone(), bytes).is_some()
            {
                return Err(integrity_err("metadata page size or identity repeated"));
            }
        }
        let closure = ValidatedSnapshotClosure::from_canonical_pages(&descriptor_bytes, pages)
            .map_err(|error| integrity_err(format!("metadata root graph: {}", error.message)))?;
        validate_snapshot_view(&view, &closure).map_err(|error| integrity_err(error.message))?;
        if closure.files() != manifest.as_slice()
            || closure.directories() != index.directories.as_slice()
        {
            return Err(integrity_err(
                "manifest does not match the descriptor's metadata graph",
            ));
        }
        for blob in blobs {
            if !self.verify_blob(&blob.digest, blob.size)? {
                return Err(integrity_err(format!(
                    "snapshot blob missing or corrupt: {}",
                    blob.digest
                )));
            }
        }
        self.verify_snapshot_links(&closure)?;
        Ok(closure)
    }

    fn verify_snapshot_links(
        &self,
        closure: &ValidatedSnapshotClosure,
    ) -> Result<(), SnapshotError> {
        for file in closure
            .files()
            .iter()
            .filter(|file| file.fs_kind == "symlink")
        {
            let bytes = self.read_blob(&file.content_digest, file.size)?;
            if bytes.contains(&0) {
                return Err(integrity_err("symlink target contains NUL"));
            }
        }
        Ok(())
    }

    /// Shared completion tail. This commits the file closure supplied by the
    /// current API; canonical descriptor, metadata pages and empty directories
    /// require a richer manifest API before full spec 11 closure can be claimed.
    fn finish_hydration(
        &self,
        view: &ViewMeta,
        manifest: &[SnapshotFile],
        _bytes_total: u64,
        fetched: u64,
        resumed: u64,
        repaired: u64,
    ) -> Result<HydrateReport, SnapshotError> {
        self.finish_hydration_commit(view, manifest, None, (fetched, resumed, repaired))
    }

    fn finish_hydration_commit(
        &self,
        view: &ViewMeta,
        manifest: &[SnapshotFile],
        closure: Option<&ValidatedSnapshotClosure>,
        counts: (u64, u64, u64),
    ) -> Result<HydrateReport, SnapshotError> {
        let (fetched, resumed, repaired) = counts;
        let (dependencies, bytes_total) = validate_manifest_policy(manifest, closure.is_some())?;
        if let Some(closure) = closure {
            validate_snapshot_view(view, closure)?;
            self.verify_snapshot_links(closure)?;
        }
        // Reuse is not a durability certificate. Sync every unique dependency
        // (including cache hits), then its directory, before metadata/pin.
        for blob in &dependencies {
            if !self.verify_blob(&blob.digest, blob.size)? {
                return Err(integrity_err(format!(
                    "hydration dependency missing or corrupt: {}",
                    blob.digest
                )));
            }
            sync_file(&self.blob_path(&blob.digest)?)?;
        }
        sync_dir(&self.content)?;
        durability_checkpoint(&self.root, "content-durable")?;
        // Keep exactly one hint for each logical path after a successful
        // pass. The old journal remains usable until the replacement commits.
        self.compact_journal(manifest)?;
        let manifest_bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        write_atomic(&self.root, MANIFEST_FILE, &manifest_bytes)?;
        durability_checkpoint(&self.root, "manifest-durable")?;

        let view_bytes = serde_json::to_vec_pretty(view)
            .map_err(|e| SnapshotError::new(SnapshotErrorCode::Internal, e.to_string()))?;
        write_atomic(&self.root, VIEW_FILE, &view_bytes)?;
        durability_checkpoint(&self.root, "view-durable")?;
        let view_digest = digest_of(&view_bytes);
        let manifest_digest = digest_of(&manifest_bytes);
        let completion_kind = if closure.is_some() {
            CompletionKind::FullSnapshot
        } else {
            CompletionKind::FileClosure
        };
        let metadata = closure
            .map(|closure| self.persist_snapshot_metadata(closure))
            .transpose()?;
        let pin_bytes = if let Some(metadata) = &metadata {
            encode_record(&SnapshotPinRecord {
                verification_revision: SNAPSHOT_VERIFICATION_REVISION,
                pin_id: uuid::Uuid::new_v4().to_string(),
                snapshot_id: view.snapshot_id.clone(),
                namespace_view_id: view.namespace_view_id.clone(),
                scope: view.scope.clone(),
                lease_id: view.lease_id.clone(),
                view_digest: view_digest.clone(),
                manifest_digest: manifest_digest.clone(),
                descriptor_digest: metadata.descriptor_digest.clone(),
                metadata_index_digest: metadata.metadata_index_digest.clone(),
                pages: metadata.pages.clone(),
                blobs: dependencies,
                pinned_at_unix: now_unix(),
            })?
        } else {
            encode_record(&PinRecord {
                verification_revision: VERIFICATION_REVISION,
                pin_id: uuid::Uuid::new_v4().to_string(),
                snapshot_id: view.snapshot_id.clone(),
                namespace_view_id: view.namespace_view_id.clone(),
                scope: view.scope.clone(),
                lease_id: view.lease_id.clone(),
                view_digest: view_digest.clone(),
                manifest_digest: manifest_digest.clone(),
                blobs: dependencies,
                pinned_at_unix: now_unix(),
            })?
        };
        write_atomic(&self.root, PIN_FILE, &pin_bytes)?;
        durability_checkpoint(&self.root, "pin-durable")?;
        match fs::remove_file(self.root.join(REPAIR_FILE)) {
            Ok(()) => sync_dir(&self.root)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err(e)),
        }

        let marker_bytes = if let Some(metadata) = metadata {
            let closure = closure.ok_or_else(|| integrity_err("snapshot closure missing"))?;
            encode_record(&SnapshotCompleteMarker {
                verification_revision: SNAPSHOT_VERIFICATION_REVISION,
                completion_kind,
                snapshot_id: view.snapshot_id.clone(),
                namespace_view_id: view.namespace_view_id.clone(),
                view_digest,
                manifest_digest,
                descriptor_digest: metadata.descriptor_digest,
                metadata_index_digest: metadata.metadata_index_digest,
                pin_digest: digest_of(&pin_bytes),
                files: manifest.len() as u64,
                directories: closure.directories().len() as u64,
                pages: metadata.pages.len() as u64,
                bytes: bytes_total,
                hydrated_at_unix: now_unix(),
            })?
        } else {
            encode_record(&CompleteMarker {
                verification_revision: VERIFICATION_REVISION,
                completion_kind,
                snapshot_id: view.snapshot_id.clone(),
                namespace_view_id: view.namespace_view_id.clone(),
                view_digest,
                manifest_digest,
                pin_digest: digest_of(&pin_bytes),
                files: manifest.len() as u64,
                bytes: bytes_total,
                hydrated_at_unix: now_unix(),
            })?
        };
        // The final rename+directory sync is the linearization point. An
        // error must not leave a visible COMPLETE from a failed repair.
        if let Err(error) = write_atomic(&self.root, COMPLETE_MARKER, &marker_bytes) {
            self.invalidate_complete()?;
            return Err(error);
        }
        if let Err(error) = durability_checkpoint(&self.root, "complete-durable") {
            self.invalidate_complete()?;
            return Err(error);
        }

        Ok(HydrateReport {
            snapshot_id: view.snapshot_id.clone(),
            total_files: manifest.len() as u64,
            fetched,
            resumed,
            repaired,
            bytes_total,
            complete: true,
            completion_kind,
        })
    }

    /// The persisted manifest of a completed hydration. Refuses to hand out a
    /// manifest when the completeness marker is absent or names another view.
    pub fn manifest(&self) -> Result<Vec<SnapshotFile>, SnapshotError> {
        let _transaction = self.transaction()?;
        if let Some(manifest) = self.completed_manifest_locked()? {
            return Ok(manifest);
        }
        let detail = read_optional(&self.root.join(REPAIR_FILE))?
            .map(|bytes| format!("; NEEDS_REPAIR: {}", String::from_utf8_lossy(&bytes)))
            .unwrap_or_default();
        Err(SnapshotError::new(
            SnapshotErrorCode::SnapshotNotReady,
            format!(
                "{}: no complete hydration to reopen{detail}",
                self.root.display()
            ),
        ))
    }

    /// Root-verifies the committed local graph without contacting a server.
    /// Integrity/retention evidence does not constitute an offline permission.
    pub fn snapshot_manifest(&self) -> Result<ValidatedSnapshotClosure, SnapshotError> {
        let _transaction = self.transaction()?;
        let Some(bytes) = read_optional(&self.root.join(COMPLETE_MARKER))? else {
            return Err(SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "no full snapshot completion record",
            ));
        };
        // Revision 2 remains explicitly file-only, including empty file lists.
        if completion_revision(&bytes).is_ok_and(|revision| revision == VERIFICATION_REVISION) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::SnapshotNotReady,
                "file-closure completion does not prove metadata completeness",
            ));
        }
        match self.verify_snapshot_commit(&bytes) {
            Ok(closure) => Ok(closure),
            Err(error)
                if matches!(
                    error.code,
                    SnapshotErrorCode::IntegrityError | SnapshotErrorCode::DigestMismatch
                ) =>
            {
                self.invalidate_complete()?;
                write_atomic(&self.root, REPAIR_FILE, error.message.as_bytes())?;
                Err(error)
            }
            Err(error) => Err(error),
        }
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
        let meta = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(io_err(e)),
        };
        if !meta.is_file() || meta.len() != expected_size {
            return Ok(false);
        }
        // Completion and resume must not collect a potentially 8 TiB CAS
        // object into memory. Read at most one byte beyond its advertised
        // size so growth cannot turn verification into an unbounded stream.
        let mut input = File::open(&path)
            .map_err(io_err)?
            .take(expected_size.saturating_add(1));
        let mut hash = Context::new(&SHA256);
        let mut buffer = [0u8; 64 * 1024];
        let mut read = 0u64;
        loop {
            let count = input.read(&mut buffer).map_err(io_err)?;
            if count == 0 {
                break;
            }
            read += count as u64;
            hash.update(&buffer[..count]);
        }
        Ok(read == expected_size
            && format!("sha256:{}", hex::encode(hash.finish().as_ref())) == digest)
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
        self.append_journal_batch(&[line])
    }

    /// Append journal lines with a single write + fsync. Used by the batched
    /// hydration. This syncs the journal itself, never the referenced blob
    /// files; the blob publishers and final closure audit sync those first.
    fn append_journal_batch(&self, lines: &[String]) -> Result<(), SnapshotError> {
        if lines.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for rec_line in lines {
            buf.extend_from_slice(rec_line.as_bytes());
            buf.extend_from_slice(b"\n");
        }
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join(JOURNAL_FILE))
            .map_err(io_err)?;
        f.write_all(&buf).map_err(io_err)?;
        f.sync_all().map_err(io_err)?;
        sync_dir(&self.root)
    }

    fn compact_journal(&self, manifest: &[SnapshotFile]) -> Result<(), SnapshotError> {
        let tmp = self.root.join(format!(
            ".{JOURNAL_FILE}.tmp.{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let result = (|| {
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp)
                .map_err(io_err)?;
            let mut output = io::BufWriter::with_capacity(256 * 1024, &file);
            durability_checkpoint(&self.root, "journal-compact-write")?;
            for file in manifest {
                serde_json::to_writer(
                    &mut output,
                    &FileRecord {
                        rel_path: file.rel_path.clone(),
                        digest: file.content_digest.clone(),
                        size: file.size,
                    },
                )
                .map_err(|error| {
                    SnapshotError::new(SnapshotErrorCode::Internal, error.to_string())
                })?;
                output.write_all(b"\n").map_err(io_err)?;
            }
            output.flush().map_err(io_err)?;
            durability_checkpoint(&self.root, "journal-compact-file-sync")?;
            file.sync_all().map_err(io_err)?;
            fs::rename(&tmp, self.root.join(JOURNAL_FILE)).map_err(io_err)?;
            sync_dir(&self.root)?;
            durability_checkpoint(&self.root, "journal-compacted")
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp);
        }
        result
    }

    fn truncate_journal_tail(&self) -> Result<(), SnapshotError> {
        let path = self.root.join(JOURNAL_FILE);
        let Some(bytes) = read_optional(&path)? else {
            return Ok(());
        };
        if bytes.ends_with(b"\n") || bytes.is_empty() {
            return Ok(());
        }
        let end = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |idx| idx + 1);
        let file = OpenOptions::new().write(true).open(&path).map_err(io_err)?;
        file.set_len(end as u64).map_err(io_err)?;
        file.sync_all().map_err(io_err)?;
        sync_dir(&self.root)
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

/// Write `data` to `dir/name`: unique temp, data fsync, rename, directory
/// fsync. No successful return is possible when either sync fails.
fn write_atomic(dir: &Path, name: &str, data: &[u8]) -> Result<(), SnapshotError> {
    create_dirs_durable(dir)?;
    // Random per writer: concurrent hydrations of identical content may race
    // on the same final name but must not share a tmp path.
    let tmp = dir.join(format!(
        ".{name}.tmp.{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let result = (|| {
        let mut f = File::create(&tmp).map_err(io_err)?;
        f.write_all(data).map_err(io_err)?;
        durability_checkpoint(dir, "object-file-sync")?;
        f.sync_all().map_err(io_err)?;
        fs::rename(&tmp, dir.join(name)).map_err(io_err)?;
        if name == COMPLETE_MARKER {
            durability_checkpoint(dir, "complete-renamed")?;
        }
        sync_dir(dir)
    })();
    if result.is_err() {
        // Only the unpublished temporary file is disposable. An already
        // renamed completion record is revoked by the transaction caller.
        match fs::remove_file(&tmp) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) => {} // Cleanup must not mask the original I/O failure.
        }
    }
    result
}

/// Only a verified and synced complete file is published at its CAS name.
/// Cancellation/errors leave no journal record or completion claim. A killed
/// process may leave an unpublished temp, as with the buffered atomic writer.
async fn write_reader_blob(
    dir: &Path,
    reader: &SnapshotReader,
    file: &SnapshotFile,
) -> Result<(), SnapshotError> {
    let source =
        crate::snapshot::ChunkedFile::open(reader, &file.rel_path, &file.content_digest, file.size)
            .await?;
    create_dirs_durable(dir)?;
    let name = blob_name(&file.content_digest);
    let temporary_path = dir.join(format!(
        ".{name}.tmp.{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let handle = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary_path)
        .map_err(io_err)?;
    let temporary = PendingBlob(temporary_path);
    let mut output = tokio::fs::File::from_std(handle);
    let mut hash = Context::new(&SHA256);
    let mut offset = 0;
    while offset < file.size {
        let length = (file.size - offset).min(mst2_codec::chunkmap::CHUNK_SIZE as u64);
        let bytes = source.read_range(offset, length).await?;
        if bytes.len() as u64 != length {
            return Err(integrity_err(
                "streamed chunk does not cover the expected file range",
            ));
        }
        hash.update(&bytes);
        output.write_all(&bytes).await.map_err(io_err)?;
        offset += length;
    }
    if format!("sha256:{}", hex::encode(hash.finish().as_ref())) != file.content_digest {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DigestMismatch,
            format!(
                "{}: streamed file does not match whole-content digest",
                file.rel_path
            ),
        ));
    }
    output.flush().await.map_err(io_err)?;
    durability_checkpoint(dir, "object-file-sync")?;
    output.sync_all().await.map_err(io_err)?;
    drop(output);
    fs::rename(&temporary.0, dir.join(name)).map_err(io_err)?;
    sync_dir(dir)
}

struct PendingBlob(PathBuf);

impl Drop for PendingBlob {
    fn drop(&mut self) {
        // Preserve the original error; only an unpublished temp is disposable.
        let _ = fs::remove_file(&self.0);
    }
}

fn sync_file(path: &Path) -> Result<(), SnapshotError> {
    durability_checkpoint(path, "file-sync")?;
    File::open(path).map_err(io_err)?.sync_all().map_err(io_err)
}

fn sync_dir(path: &Path) -> Result<(), SnapshotError> {
    durability_checkpoint(path, "directory-sync")?;
    File::open(path).map_err(io_err)?.sync_all().map_err(io_err)
}

// New directory names also need their parents persisted. Existing directory
// chains need no new entry sync until a file is published in them.
fn create_dirs_durable(path: &Path) -> Result<(), SnapshotError> {
    let mut missing = Vec::new();
    let mut cursor = path.to_path_buf();
    loop {
        match fs::metadata(&cursor) {
            Ok(meta) if meta.is_dir() => break,
            Ok(_) => {
                return Err(io_err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    "store path is not a directory",
                )))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                missing.push(cursor.clone());
                cursor = parent_dir(&cursor).to_path_buf();
            }
            Err(e) => return Err(io_err(e)),
        }
    }
    fs::create_dir_all(path).map_err(io_err)?;
    for directory in missing.iter().rev() {
        sync_dir(directory)?;
        sync_dir(parent_dir(directory))?;
    }
    Ok(())
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, SnapshotError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err(e)),
    }
}

fn required_dependency(path: &Path) -> Result<Vec<u8>, SnapshotError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if !meta.is_file() => {
            return Err(integrity_err("completion metadata is not a regular file"))
        }
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(integrity_err(format!(
                "completion dependency missing: {}",
                path.display()
            )))
        }
        Err(e) => return Err(io_err(e)),
    }
    read_optional(path)?
        .ok_or_else(|| integrity_err(format!("completion dependency missing: {}", path.display())))
}

fn decode_commit<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    name: &str,
) -> Result<T, SnapshotError> {
    serde_json::from_slice(bytes).map_err(|e| integrity_err(format!("corrupt {name}: {e}")))
}

fn encode_record<T: Serialize>(record: &T) -> Result<Vec<u8>, SnapshotError> {
    serde_json::to_vec_pretty(record)
        .map_err(|error| SnapshotError::new(SnapshotErrorCode::Internal, error.to_string()))
}

fn completion_revision(bytes: &[u8]) -> Result<u32, SnapshotError> {
    #[derive(Deserialize)]
    struct Header {
        verification_revision: u32,
    }
    Ok(decode_commit::<Header>(bytes, COMPLETE_MARKER)?.verification_revision)
}

fn validate_snapshot_view(
    view: &ViewMeta,
    closure: &ValidatedSnapshotClosure,
) -> Result<(), SnapshotError> {
    let descriptor = closure.descriptor();
    if view.snapshot_id != descriptor.snapshot_id
        || view.namespace_view_id != descriptor.namespace_view_id
        || view.scope != descriptor.scope
    {
        return Err(SnapshotError::new(
            SnapshotErrorCode::DurableViewConflict,
            "supplied view differs from the validated snapshot descriptor",
        ));
    }
    Ok(())
}

fn integrity_err(message: impl Into<String>) -> SnapshotError {
    SnapshotError::new(SnapshotErrorCode::IntegrityError, message)
}

fn validate_view(view: &ViewMeta) -> Result<(), SnapshotError> {
    validate_view_policy(view, false)
}

fn validate_view_policy(view: &ViewMeta, full_snapshot: bool) -> Result<(), SnapshotError> {
    if view.snapshot_id.is_empty()
        || view.namespace_view_id.is_empty()
        || view.lease_id.is_empty()
        || !view.scope.starts_with('/')
        || view.scope.len() > 4096
        || (view.scope != "/"
            && view.scope[1..].split('/').any(|part| {
                part.is_empty()
                    || part == "."
                    || part == ".."
                    || part.len() > 255
                    || part.contains('\0')
                    || (!full_snapshot && part.contains('\\'))
            }))
    {
        return Err(integrity_err("invalid fixed-view binding"));
    }
    Ok(())
}

// Only the supplied file closure is available here. Directory metadata and
// empty-directory entries are intentionally not inferred from file paths.
fn validate_manifest(
    manifest: &[SnapshotFile],
) -> Result<(Vec<BlobDependency>, u64), SnapshotError> {
    validate_manifest_policy(manifest, false)
}

fn validate_manifest_policy(
    manifest: &[SnapshotFile],
    full_snapshot: bool,
) -> Result<(Vec<BlobDependency>, u64), SnapshotError> {
    let mut paths = HashSet::new();
    let mut blobs = BTreeMap::new();
    let mut total = 0u64;
    for file in manifest {
        let components: Vec<_> = file.rel_path.split('/').collect();
        if file.rel_path.len() > 4096
            || components.len() > 256
            || components.iter().any(|part| {
                part.is_empty()
                    || *part == "."
                    || *part == ".."
                    || part.len() > 255
                    || part.contains('\0')
                    || (!full_snapshot && part.contains('\\'))
            })
            || !matches!(
                file.fs_kind.as_str(),
                "file" | "regular" | "executable" | "symlink"
            )
            || !paths.insert(file.rel_path.as_str())
        {
            return Err(integrity_err(format!(
                "invalid or duplicate file path/kind: {:?}",
                file.rel_path
            )));
        }
        let hex = file
            .content_digest
            .strip_prefix("sha256:")
            .ok_or_else(|| integrity_err("manifest digest lacks sha256 prefix"))?;
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(integrity_err("manifest digest is not canonical SHA-256"));
        }
        if let Some(size) = blobs.insert(file.content_digest.clone(), file.size) {
            if size != file.size {
                return Err(integrity_err(
                    "the same content digest has conflicting sizes",
                ));
            }
        }
        total = total
            .checked_add(file.size)
            .ok_or_else(|| integrity_err("manifest byte count overflow"))?;
    }
    for path in &paths {
        for (offset, _) in path.match_indices('/') {
            if paths.contains(&path[..offset]) {
                return Err(integrity_err("a manifest file is another file's ancestor"));
            }
        }
    }
    Ok((
        blobs
            .into_iter()
            .map(|(digest, size)| BlobDependency { digest, size })
            .collect(),
        total,
    ))
}

fn durability_checkpoint(_path: &Path, _phase: &str) -> Result<(), SnapshotError> {
    #[cfg(test)]
    durability_tests::checkpoint(_path, _phase)?;
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
#[path = "durable_tests.rs"]
mod durability_tests;

#[cfg(test)]
#[path = "durable_snapshot_tests.rs"]
mod snapshot_durability_tests;

#[cfg(all(test, unix))]
#[path = "durable_stream_tests.rs"]
mod streaming_durability_tests;

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

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
        assert!(
            store.is_complete().unwrap(),
            "completion marker exists={}, repair={:?}",
            store.root.join(COMPLETE_MARKER).exists(),
            fs::read_to_string(store.root.join(REPAIR_FILE))
        );

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
