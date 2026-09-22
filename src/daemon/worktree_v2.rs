//! Worktree Control Protocol v2: effective diff, commit finalize, lower switch.
//!
//! The v1 contract reported every upper-layer entry as a change and never moved the
//! lower projection, which produced two failures this module fixes:
//!
//! 1. **Phantom dirt.** An upper file whose content equals the base (edited back, or
//!    left by an earlier commit) was reported `modified` forever.
//! 2. **Stale upper shadowing a newer lower.** After a commit was pushed, the upper
//!    kept the committed content indefinitely, so a later remote update to the same
//!    path would be invisible behind the stale overlay.
//!
//! The fix is a per-path comparison of upper content against the *lower* projection
//! ([`effective_changes`]), plus a transactional finalize ([`CommitFinalizeRequest`])
//! that pins the lower to the committed revision and removes exactly the committed
//! upper entries. See `docs/scorpiofs-libra-complete-spec-v1.md`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::dicfuse::store::DictionaryStore;
use crate::dicfuse::tree_store::StorageItem;

/// Effective change kinds, in Git terms. `added`/`modified` compare upper content
/// against the lower projection; `deleted` is an OCI whiteout over a lower entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectiveKind {
    Added,
    Modified,
    Deleted,
}

/// One path whose effective content differs from the lower projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveChange {
    /// Path relative to the mount root, `/`-separated, no leading slash.
    pub path: String,
    pub kind: EffectiveKind,
    /// Git blob OID of the current upper content (`null` for deletions).
    pub content_hash: Option<String>,
    /// Git blob OID of the base (lower) content (`null` when the path is new).
    pub base_hash: Option<String>,
}

/// A path a VCS client says its commit has absorbed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedPath {
    /// Path relative to the mount root, `/`-separated, no leading slash.
    pub path: String,
    pub kind: EffectiveKind,
    /// Git blob OID the path was committed with. `None` for deletions. The finalize
    /// verifies this is exactly what the new lower serves, which both proves the
    /// commit landed and acts as a per-path optimistic lock against concurrent edits.
    #[serde(default)]
    pub content_hash: Option<String>,
}

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

/// Compute the git blob OID of `content`, matching what Mega's `content-hash`
/// entries contain for the same bytes.
///
/// Uses the process-wide hash kind (SHA-1 unless someone reconfigured it), which is
/// the format the monorepo server serves for the repositories this integration
/// targets. A length check against the lower's OID at comparison time catches a
/// mismatched configuration instead of silently reporting phantom changes.
pub fn git_blob_oid(content: &[u8]) -> String {
    git_internal::internal::object::blob::Blob::from_content_bytes(content.to_vec())
        .id
        .to_string()
}

// ---------------------------------------------------------------------------
// Upper-layer scanning
// ---------------------------------------------------------------------------

/// One physical entry found in the upper directory.
#[derive(Debug)]
struct UpperEntry {
    /// Path relative to the upper root, `/`-separated, no leading or trailing slash.
    rel_path: String,
    kind: UpperEntryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpperEntryKind {
    File,
    Symlink,
    /// OCI whiteout: the name it hides is `rel_path`.
    Whiteout,
}

const WHITEOUT_PREFIX: &str = ".wh.";
const WHITEOUT_OPAQUE: &str = ".wh..wh..opq";
/// VCS metadata inside the mount; a reconstructable pointer, never a worktree edit.
const VCS_POINTER_DIR: &str = ".libra";

fn classify_upper_entry(name: &str) -> Option<UpperEntryKind> {
    if name == WHITEOUT_OPAQUE {
        // Directory-wide marker: it does not name one path, and the entries it
        // hides are handled when the scanner walks the (now absent) children.
        return None;
    }
    if let Some(hidden) = name.strip_prefix(WHITEOUT_PREFIX) {
        if hidden.is_empty() {
            return None;
        }
        return Some(UpperEntryKind::Whiteout);
    }
    None
}

/// Walk the upper directory and return every physical entry.
///
/// Whiteouts are returned under the *hidden* name (`src/.wh.a.rs` scans as
/// `src/a.rs` with kind `Whiteout`). The VCS pointer directory is skipped at the
/// top level, mirroring the change scanner.
fn scan_upper_entries(upper_dir: &Path) -> std::io::Result<Vec<UpperEntry>> {
    let mut out = Vec::new();
    let mut pending = vec![PathBuf::new()];

    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(upper_dir.join(&dir))? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();

            if dir.as_os_str().is_empty() && name == VCS_POINTER_DIR {
                continue;
            }

            // file_type() does not follow symlinks: a link must be hashed as its
            // target bytes, never traversed.
            let file_type = entry.file_type()?;
            let rel = if dir.as_os_str().is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", dir.display(), name)
            };

            if file_type.is_dir() {
                pending.push(PathBuf::from(&rel));
                continue;
            }

            if let Some(kind) = classify_upper_entry(&name) {
                let hidden = if let Some(hidden) = name.strip_prefix(WHITEOUT_PREFIX) {
                    if dir.as_os_str().is_empty() {
                        hidden.to_string()
                    } else {
                        format!("{}/{}", dir.display(), hidden)
                    }
                } else {
                    rel.clone()
                };
                out.push(UpperEntry {
                    rel_path: hidden,
                    kind,
                });
                continue;
            }

            let kind = if file_type.is_symlink() {
                UpperEntryKind::Symlink
            } else {
                UpperEntryKind::File
            };
            out.push(UpperEntry {
                rel_path: rel,
                kind,
            });
        }
    }

    Ok(out)
}

/// Git blob OID of an upper entry's current content.
///
/// Symlinks hash their target bytes (git stores the target as the blob), matching
/// how the monorepo serves them.
fn upper_content_oid(upper_dir: &Path, entry: &UpperEntry) -> std::io::Result<String> {
    let bytes = match entry.kind {
        UpperEntryKind::Symlink => std::fs::read_link(upper_dir.join(&entry.rel_path))?
            .as_os_str()
            .as_encoded_bytes()
            .to_vec(),
        _ => std::fs::read(upper_dir.join(&entry.rel_path))?,
    };
    Ok(git_blob_oid(&bytes))
}

/// Resolve the lower projection's item for a mount-relative path.
///
/// The tree store loads lazily, so a miss on a deep path may just mean "the parent
/// directory was never fetched". This walks the ancestors top-down, forcing each
/// directory load, and retries before concluding the path is absent from the lower.
pub(crate) async fn lower_item_for(store: &DictionaryStore, rel_path: &str) -> Option<StorageItem> {
    let user_path = format!("/{rel_path}");
    if let Ok(item) = store.get_by_path(&user_path).await {
        return Some(item);
    }

    let parts: Vec<&str> = rel_path.split('/').filter(|p| !p.is_empty()).collect();
    if parts.len() <= 1 {
        return None;
    }

    // Ensure the tree root is loaded, then each ancestor down to the parent.
    let _ = store.ensure_dir_loaded(1).await;
    let mut walked = String::new();
    for part in &parts[..parts.len() - 1] {
        walked = format!("{walked}/{part}");
        match store.get_inode_from_path(&walked).await {
            Ok(inode) => {
                let _ = store.ensure_dir_loaded(inode).await;
            }
            Err(_) => {
                let _ = store.ensure_dir_loaded(1).await;
            }
        }
    }

    store.get_by_path(&user_path).await.ok()
}

/// The base content a chain layer or the projection serves for one path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ChainBase {
    /// The path exists with this git blob OID.
    Hash(String),
    /// A nearer layer whiteouts the path, or it is absent everywhere.
    Absent,
}

/// Look up `rel_path` through the sealed chain (nearest first) and finally the
/// Dicfuse projection, honoring per-layer whiteouts along the way.
///
/// The sealed layers are plain host directories (renamed former uppers), so every
/// lookup is a local filesystem probe — no FUSE, no network.
fn chain_base_for(chain: &[PathBuf], rel_path: &str) -> Option<ChainBase> {
    let rel = Path::new(rel_path);
    let name = rel.file_name()?.to_string_lossy().to_string();
    let parent = rel.parent().unwrap_or_else(|| Path::new(""));
    let whiteout_name = format!("{WHITEOUT_PREFIX}{name}");

    for layer in chain {
        let layer_parent = layer.join(parent);
        if layer_parent.join(&whiteout_name).is_file() {
            // A nearer chain layer hides the path for everything above it.
            return Some(ChainBase::Absent);
        }
        let entry = layer.join(rel);
        match fs::symlink_metadata(&entry) {
            Ok(meta) if meta.is_dir() => {
                // A directory cannot be the base of a file query; keep looking.
                continue;
            }
            Ok(meta) if meta.file_type().is_symlink() => {
                let bytes = fs::read_link(&entry).ok()?.as_os_str().as_encoded_bytes().to_vec();
                return Some(ChainBase::Hash(git_blob_oid(&bytes)));
            }
            Ok(_) => {
                let bytes = fs::read(&entry).ok()?;
                return Some(ChainBase::Hash(git_blob_oid(&bytes)));
            }
            Err(_) => continue,
        }
    }
    None
}

/// Compare the upper layer against the lower projection.
///
/// `chain` holds the sealed layers, nearest first. A call performs directory loads
/// for paths whose parents were not yet fetched, so the first scan of a cold deep
/// path may hit the network; later scans are local.
pub async fn effective_changes(
    store: &DictionaryStore,
    upper_dir: &Path,
    chain: &[PathBuf],
) -> std::io::Result<Vec<EffectiveChange>> {
    let mut by_path: BTreeMap<String, EffectiveChange> = BTreeMap::new();

    for entry in scan_upper_entries(upper_dir)? {
        // The base is the chain first (nearest layer wins, whiteouts honored);
        // the Dicfuse projection answers only what the chain does not.
        let base = match chain_base_for(chain, &entry.rel_path) {
            Some(base) => base,
            None => match lower_item_for(store, &entry.rel_path).await {
                Some(item) => ChainBase::Hash(item.hash.clone()),
                None => ChainBase::Absent,
            },
        };
        match entry.kind {
            UpperEntryKind::Whiteout => match base {
                ChainBase::Hash(base_hash) => {
                    by_path.insert(
                        entry.rel_path.clone(),
                        EffectiveChange {
                            path: entry.rel_path.clone(),
                            kind: EffectiveKind::Deleted,
                            content_hash: None,
                            base_hash: Some(base_hash),
                        },
                    );
                }
                // Whiteout over an absent base is a no-op; dropping it happens
                // during the next finalize, not here.
                ChainBase::Absent => {}
            },
            UpperEntryKind::File | UpperEntryKind::Symlink => {
                let content_hash = upper_content_oid(upper_dir, &entry)?;
                match base {
                    ChainBase::Hash(base_hash) if base_hash == content_hash => {
                        // Edited back to the base content: effectively clean. The
                        // redundant upper entry is left in place; finalize/compaction
                        // may drop it.
                        by_path.remove(&entry.rel_path);
                    }
                    ChainBase::Hash(base_hash) => {
                        by_path.insert(
                            entry.rel_path.clone(),
                            EffectiveChange {
                                path: entry.rel_path.clone(),
                                kind: EffectiveKind::Modified,
                                content_hash: Some(content_hash),
                                base_hash: Some(base_hash),
                            },
                        );
                    }
                    ChainBase::Absent => {
                        by_path.insert(
                            entry.rel_path.clone(),
                            EffectiveChange {
                                path: entry.rel_path.clone(),
                                kind: EffectiveKind::Added,
                                content_hash: Some(content_hash),
                                base_hash: None,
                            },
                        );
                    }
                }
            }
        }
    }

    Ok(by_path.into_values().collect())
}

/// Merge every sealed chain layer into the upper layer (nearest wins, upper wins
/// over everything), honoring whiteouts in both directions.
///
/// This runs when a chained mount's lower is about to move (finalize/refresh): a
/// chain is a delta against the OLD revision, so keeping it across a lower switch
/// would make stale chain entries shadow the new projection. Flattening converts
/// the O(1) fork cost into a one-time O(uncommitted delta) at finalize.
///
/// Returns the number of entries copied. Chain directories themselves are NOT
/// deleted here — other mounts may still reference them; unreferenced ones are
/// collected by the mount-delete path.
pub fn flatten_chain_into_upper(chain: &[PathBuf], upper_dir: &Path) -> std::io::Result<usize> {
    let mut copied = 0;

    // Farthest first, nearest last: a nearer layer's entry must overwrite a
    // farther layer's copy of the same path. The upper itself wins over all of
    // them — an existing upper entry is never touched.
    for layer in chain.iter().rev() {
        for entry in scan_upper_entries(layer)? {
            let upper_target = upper_dir.join(&entry.rel_path);
            if upper_target.exists() || upper_target.is_symlink() {
                continue;
            }
            let upper_parent = upper_target
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(""));
            fs::create_dir_all(&upper_parent)?;
            let physical_name = match entry.kind {
                UpperEntryKind::Whiteout => format!(
                    "{WHITEOUT_PREFIX}{}",
                    Path::new(&entry.rel_path)
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_default()
                ),
                _ => Path::new(&entry.rel_path)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default(),
            };
            let layer_entry = layer.join(parent_of(&entry.rel_path)).join(&physical_name);
            let meta = fs::symlink_metadata(&layer_entry)?;
            if meta.is_dir() {
                continue; // directories carry no content of their own
            }
            if meta.file_type().is_symlink() {
                let target = fs::read_link(&layer_entry)?;
                std::os::unix::fs::symlink(target, &upper_target)?;
            } else {
                fs::copy(&layer_entry, &upper_target)?;
            }
            copied += 1;
        }
    }

    Ok(copied)
}

fn parent_of(rel_path: &str) -> PathBuf {
    Path::new(rel_path)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from(""))
}

/// Stable fingerprint of an effective change set (FNV-1a over path/kind/hashes).
///
/// Order-independent: the changes are sorted by path before hashing, so the same
/// change set always produces the same generation regardless of how the caller
/// collected it. Libra uses this as an optimistic lock: it reads state, stages,
/// commits, then sends the generation back with the finalize; if anything changed
/// underneath, the finalize is refused before any destructive step.
pub fn generation_of(changes: &[EffectiveChange]) -> u64 {
    let mut ordered: Vec<&EffectiveChange> = changes.iter().collect();
    ordered.sort_by(|x, y| x.path.cmp(&y.path));

    let mut generation: u64 = 0xcbf29ce484222325;
    for change in ordered {
        for field in [
            change.path.as_str(),
            match change.kind {
                EffectiveKind::Added => "added",
                EffectiveKind::Modified => "modified",
                EffectiveKind::Deleted => "deleted",
            },
            change.content_hash.as_deref().unwrap_or(""),
            change.base_hash.as_deref().unwrap_or(""),
        ] {
            for byte in field.as_bytes() {
                generation ^= u64::from(*byte);
                generation = generation.wrapping_mul(0x100000001b3);
            }
            generation ^= 0xff;
            generation = generation.wrapping_mul(0x100000001b3);
        }
    }
    generation
}

// ---------------------------------------------------------------------------
// Mega revision resolution
// ---------------------------------------------------------------------------

/// Resolve the monorepo's current internal commit OID for `repo_path`.
///
/// Mega's tree/blob API addresses revisions by *internal* commit OID (the one
/// `/api/v1/latest-commit` returns), not by git commit OID. A VCS client that just
/// pushed a commit therefore cannot name its revision directly — it either passes
/// the OID it obtained from `latest-commit`, or leaves [`CommitFinalizeRequest::
/// new_base_revision`] empty and lets the daemon resolve it here.
pub async fn resolve_latest_revision(base_url: &str, repo_path: &str) -> Result<String, String> {
    let url = format!(
        "{}/api/v1/latest-commit?path={}",
        base_url.trim_end_matches('/'),
        // Mega's examples carry the leading slash (`?path=/project`); keep it.
        repo_path.trim_end_matches('/')
    );
    let response = reqwest::Client::new()
        .get(&url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| format!("latest-commit request failed for {repo_path}: {e}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "latest-commit returned HTTP {} for {repo_path}",
            response.status()
        ));
    }
    #[derive(serde::Deserialize)]
    struct LatestCommit {
        oid: String,
    }
    let parsed: LatestCommit = response
        .json()
        .await
        .map_err(|e| format!("latest-commit response parse failed for {repo_path}: {e}"))?;
    if parsed.oid.trim().is_empty() {
        return Err(format!(
            "latest-commit returned an empty OID for {repo_path}"
        ));
    }
    Ok(parsed.oid)
}

// ---------------------------------------------------------------------------
// Protocol payloads
// ---------------------------------------------------------------------------

/// `POST /antares/worktrees` — Worktree v2 attach.
///
/// One call provisions the mount with its lower **pinned from the first request**,
/// closing the v1 gap where `POST /mounts` served the moving trunk tip and the
/// base binding happened only afterwards. The daemon never writes VCS metadata
/// into the mount; the client owns `.libra` and writes it through the FUSE view.
#[derive(Debug, Clone, Deserialize)]
pub struct AttachWorktreeRequest {
    /// Client-side worktree identity, echoed back. The daemon keys mounts by
    /// `mount_id`; this field is provenance for the caller's registry.
    #[serde(default)]
    pub worktree_id: Option<String>,
    /// Monorepo path to project (e.g. `/project`).
    pub repo_path: String,
    /// Filesystem mountpoint. Must be absent or an empty directory.
    pub mountpoint: String,
    /// The revision the client binds (its own identifier space, e.g. a git commit
    /// OID). Recorded verbatim; it does NOT drive the lower pin. Empty/omitted
    /// leaves the mount unbound until a later v1 `worktree/base` or finalize.
    #[serde(default)]
    pub base_revision: Option<String>,
    /// Pin the lower to this Mega *internal* commit OID instead of the trunk tip
    /// at attach time. Empty/omitted resolves `latest-commit` for `repo_path`.
    #[serde(default)]
    pub lower_revision: Option<String>,
    /// Optional task identifier for idempotent job binding, as on v1 mounts.
    #[serde(default)]
    pub job_id: Option<String>,
}

/// `POST /antares/worktrees` response.
#[derive(Debug, Clone, Serialize)]
pub struct AttachWorktreeResponse {
    pub mount_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktree_id: Option<String>,
    pub mountpoint: String,
    /// Bound client-side revision (echoed, unchanged).
    pub base_revision: Option<String>,
    /// The internal revision the lower was pinned to.
    pub lower_revision: String,
    pub state: String,
    pub generation: u64,
}

/// `GET /antares/worktrees/{mount_id}/state`
#[derive(Debug, Clone, Serialize)]
pub struct WorktreeStateV2 {
    pub mount_id: String,
    /// The revision the lower projection is pinned to. `null` means the mount
    /// predates pinning and its lower tracks the moving trunk tip.
    pub lower_revision: Option<String>,
    /// The revision the VCS client bound (v1 `worktree/base`). May differ from
    /// `lower_revision` on legacy mounts; finalize aligns them.
    pub base_revision: Option<String>,
    pub state: String,
    pub generation: u64,
    pub dirty: bool,
    pub changes: Vec<EffectiveChange>,
}

/// `POST /antares/worktrees/{mount_id}/commit-finalize`
#[derive(Debug, Clone, Deserialize)]
pub struct CommitFinalizeRequest {
    /// The revision the client believes the lower is at. Verified before anything
    /// is touched.
    pub expected_base_revision: Option<String>,
    /// The revision to pin the lower to. Empty/omitted resolves the monorepo's
    /// latest commit for the mounted path.
    #[serde(default)]
    pub new_base_revision: Option<String>,
    /// Optimistic lock over the whole change set, from the state call.
    #[serde(default)]
    pub expected_generation: Option<u64>,
    /// Paths the commit absorbed; their upper entries are removed once the lower
    /// verifiably serves the committed content.
    #[serde(default)]
    pub committed_paths: Vec<CommittedPath>,
}

/// `POST /antares/worktrees/{mount_id}/commit-finalize` response.
#[derive(Debug, Clone, Serialize)]
pub struct CommitFinalizeResponse {
    pub state: String,
    /// Error code for a non-ready outcome: `GENERATION_CHANGED`, `TREE_MISMATCH`,
    /// `BASE_MISMATCH`, `SWITCH_FAILED`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub base_revision: String,
    pub lower_revision: Option<String>,
    pub generation: u64,
    /// Paths whose upper entries were removed (verified committed content only).
    pub cleaned_paths: Vec<String>,
}

/// `POST /antares/worktrees/{mount_id}/refresh`
#[derive(Debug, Clone, Deserialize)]
pub struct RefreshRequest {
    pub expected_base_revision: Option<String>,
    /// Revision to move the lower to. Empty/omitted resolves the latest.
    #[serde(default)]
    pub target_revision: Option<String>,
    /// Refuse when the effective diff is non-empty (Git-style overwrite protection).
    #[serde(default = "default_require_clean")]
    pub require_clean: bool,
}

fn default_require_clean() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshDisposition {
    Switched,
    AlreadyAtTarget,
    BlockedDirty,
    BaseMismatch,
}

/// `POST /antares/worktrees/{mount_id}/refresh` response.
#[derive(Debug, Clone, Serialize)]
pub struct RefreshResponse {
    pub disposition: RefreshDisposition,
    pub base_revision: String,
    pub lower_revision: Option<String>,
    pub generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

// ---------------------------------------------------------------------------
// Filesystem cleanup
// ---------------------------------------------------------------------------

/// Remove the upper-layer entries for a set of committed paths, then prune any
/// parent directories left empty (up to the upper root).
pub fn remove_committed_upper_entries(
    upper_dir: &Path,
    committed: &[CommittedPath],
) -> std::io::Result<Vec<String>> {
    let mut removed = Vec::new();
    let mut touched_parents: Vec<PathBuf> = Vec::new();

    for path in committed {
        let rel = Path::new(&path.path);
        let physical = match path.kind {
            EffectiveKind::Deleted => {
                let parent = rel.parent().unwrap_or_else(|| Path::new(""));
                let file_name = rel
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
                upper_dir
                    .join(parent)
                    .join(format!("{WHITEOUT_PREFIX}{file_name}"))
            }
            EffectiveKind::Added | EffectiveKind::Modified => upper_dir.join(rel),
        };

        match std::fs::symlink_metadata(&physical) {
            Ok(meta) => {
                if meta.is_dir() {
                    std::fs::remove_dir_all(&physical)?;
                } else {
                    std::fs::remove_file(&physical)?;
                }
                removed.push(path.path.clone());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Already gone: idempotent retry of an earlier finalize.
            }
            Err(e) => return Err(e),
        }

        if let Some(parent) = rel.parent() {
            if !parent.as_os_str().is_empty() {
                touched_parents.push(parent.to_path_buf());
            }
        }
    }

    // Deepest first so a chain of emptied directories collapses in one pass.
    touched_parents.sort_by_key(|p| std::cmp::Reverse(p.components().count()));
    for parent in touched_parents {
        let _ = std::fs::remove_dir(upper_dir.join(parent)); // fails harmlessly when non-empty
    }

    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_file(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, contents).unwrap();
    }

    #[test]
    fn git_blob_oid_matches_the_monorepo_server() {
        // The E2E worktree committed "formal-backend-sync\n" to src/alpha.txt and
        // Mega's content-hash API reported exactly this OID for the path — the
        // formula is the standard git blob header + content.
        assert_eq!(
            git_blob_oid(b"formal-backend-sync\n"),
            "d93a4449096163af2b8789689d1923f96b182671"
        );
        // Well-known git blob OIDs.
        assert_eq!(
            git_blob_oid(b"hello\n"),
            "ce013625030ba8dba906f756967f9e9ca394464a"
        );
        assert_eq!(
            git_blob_oid(b""),
            "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
        );
    }

    #[test]
    fn classifies_whiteouts_and_files() {
        assert_eq!(
            classify_upper_entry(".wh.gone.rs"),
            Some(UpperEntryKind::Whiteout)
        );
        assert_eq!(classify_upper_entry("normal.rs"), None);
        assert_eq!(classify_upper_entry(WHITEOUT_OPAQUE), None);
        assert_eq!(classify_upper_entry(".wh."), None);
    }

    #[test]
    fn generation_is_stable_and_order_independent() {
        let a = EffectiveChange {
            path: "src/a.rs".into(),
            kind: EffectiveKind::Modified,
            content_hash: Some("aaa".into()),
            base_hash: Some("bbb".into()),
        };
        let b = EffectiveChange {
            path: "src/b.rs".into(),
            kind: EffectiveKind::Added,
            content_hash: Some("ccc".into()),
            base_hash: None,
        };
        let one = generation_of(&[a.clone(), b.clone()]);
        let two = generation_of(&[b.clone(), a.clone()]);
        assert_eq!(
            one, two,
            "BTreeMap-fed inputs sort by path; order must not matter"
        );
        let changed = generation_of(std::slice::from_ref(&b));
        assert_ne!(one, changed);
    }

    #[test]
    fn generation_distinguishes_content_hash_from_base_hash() {
        let base = EffectiveChange {
            path: "p".into(),
            kind: EffectiveKind::Modified,
            content_hash: Some("c1".into()),
            base_hash: Some("b1".into()),
        };
        let same_base_diff_content = EffectiveChange {
            content_hash: Some("c2".into()),
            ..base.clone()
        };
        let same_content_diff_base = EffectiveChange {
            base_hash: Some("b2".into()),
            ..base.clone()
        };
        let g1 = generation_of(std::slice::from_ref(&base));
        assert_ne!(
            g1,
            generation_of(std::slice::from_ref(&same_base_diff_content))
        );
        assert_ne!(
            g1,
            generation_of(std::slice::from_ref(&same_content_diff_base))
        );
    }

    #[test]
    fn removes_files_whiteouts_and_prunes_empty_parents() {
        let tmp = tempfile::tempdir().unwrap();
        let upper = tmp.path();

        write_file(&upper.join("src/keep.rs"), "keep");
        write_file(&upper.join("src/deep/committed.rs"), "content");
        write_file(&upper.join("src/deep/.wh.gone.rs"), "");
        write_file(&upper.join("docs/.wh.gone.md"), "");

        let committed = vec![
            CommittedPath {
                path: "src/deep/committed.rs".into(),
                kind: EffectiveKind::Modified,
                content_hash: Some("x".into()),
            },
            CommittedPath {
                path: "src/deep/gone.rs".into(),
                kind: EffectiveKind::Deleted,
                content_hash: None,
            },
            CommittedPath {
                path: "docs/gone.md".into(),
                kind: EffectiveKind::Deleted,
                content_hash: None,
            },
        ];

        let removed = remove_committed_upper_entries(upper, &committed).unwrap();
        let mut sorted = removed.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec!["docs/gone.md", "src/deep/committed.rs", "src/deep/gone.rs"]
        );

        // Committed entries are gone.
        assert!(
            !upper.join("src/deep").exists(),
            "emptied parents are pruned"
        );
        assert!(!upper.join("docs").exists());
        // Sibling content survives.
        assert!(upper.join("src/keep.rs").exists());
    }

    #[test]
    fn removal_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let upper = tmp.path();
        write_file(&upper.join("a.rs"), "x");

        let committed = vec![CommittedPath {
            path: "a.rs".into(),
            kind: EffectiveKind::Modified,
            content_hash: None,
        }];
        remove_committed_upper_entries(upper, &committed).unwrap();
        // Second pass over an already-clean tree must not error.
        let removed = remove_committed_upper_entries(upper, &committed).unwrap();
        assert!(removed.is_empty());
    }
}
