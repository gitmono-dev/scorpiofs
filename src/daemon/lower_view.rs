//! Content-identity domains for the effective diff (P3, spec 12 §1).
//!
//! The lower projection answers content identity in its own domain: the
//! Dicfuse path speaks the mega content-hash (a git blob OID), while an MST/2
//! snapshot view speaks `sha256(raw bytes)` (spec 03 §1). The effective diff
//! must hash the *upper* layer in the same domain, otherwise "edited back to
//! base" and "modified" cannot be told apart.
//!
//! [`LowerView`] makes that domain explicit; [`DicfuseLower`] is the legacy
//! implementation, and the MST/2 one lives next to the snapshot view.

use std::sync::Arc;

use async_trait::async_trait;

use crate::daemon::worktree_v2::{git_blob_oid, lower_item_for};
use crate::dicfuse::store::DictionaryStore;
use crate::snapshot::fuse::Mst2Fuse;

/// Which content-identity domain a lower projection speaks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LowerHashKind {
    /// `sha1("blob <len>\0" + content)` — the mega content-hash, i.e. a git
    /// blob OID (bare 40 hex).
    GitBlobOid,
    /// `sha256:<64 hex>` over the raw bytes — the MST/2 `content_digest`
    /// (spec 03 §1). Symlinks hash their target bytes, matching spec 07 §1.
    Sha256Raw,
}

/// Content identity of `bytes` in `kind`'s domain.
pub fn hash_content(kind: LowerHashKind, bytes: &[u8]) -> String {
    match kind {
        LowerHashKind::GitBlobOid => git_blob_oid(bytes),
        LowerHashKind::Sha256Raw => crate::snapshot::durable::digest_of(bytes),
    }
}

/// A read-only lower projection as the effective diff sees it.
#[async_trait]
pub trait LowerView: Send + Sync {
    /// Content identity of `rel_path` as the lower serves it; `None` = the
    /// path is absent from this projection (deleted or never existed).
    async fn base_hash(&self, rel_path: &str) -> Option<String>;

    /// The domain [`Self::base_hash`] answers in.
    fn hash_kind(&self) -> LowerHashKind;
}

/// The Dicfuse projection as a [`LowerView`]: mega content-hash (git blob OID).
pub struct DicfuseLower(pub Arc<DictionaryStore>);

#[async_trait]
impl LowerView for DicfuseLower {
    async fn base_hash(&self, rel_path: &str) -> Option<String> {
        lower_item_for(&self.0, rel_path).await.map(|item| item.hash)
    }

    fn hash_kind(&self) -> LowerHashKind {
        LowerHashKind::GitBlobOid
    }
}

/// An MST/2 snapshot view as a [`LowerView`]: `sha256(raw)` content digests
/// (spec 03 §1). Symlinks answer the digest of their target bytes, which is
/// what the view stores and what the upper side hashes too (spec 07 §1).
pub struct Mst2Lower(pub Arc<Mst2Fuse>);

#[async_trait]
impl LowerView for Mst2Lower {
    async fn base_hash(&self, rel_path: &str) -> Option<String> {
        self.0.digest_for_path(rel_path).await
    }

    fn hash_kind(&self) -> LowerHashKind {
        LowerHashKind::Sha256Raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The git-blob domain must reproduce the value the monorepo server
    /// reports for the same content (the existing cross-check), and the sha256
    /// domain must use the view's wire form.
    #[test]
    fn both_domains_hash_their_wire_forms() {
        let content = b"formal-backend-sync\n";
        assert_eq!(
            hash_content(LowerHashKind::GitBlobOid, content),
            "d93a4449096163af2b8789689d1923f96b182671"
        );
        let sha = hash_content(LowerHashKind::Sha256Raw, content);
        assert!(sha.starts_with("sha256:"), "{sha}");
        assert_eq!(sha.len(), "sha256:".len() + 64);
    }

    #[test]
    fn empty_content_hashes_are_well_formed() {
        assert_eq!(
            hash_content(LowerHashKind::GitBlobOid, b""),
            "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
        );
        assert!(hash_content(LowerHashKind::Sha256Raw, b"").starts_with("sha256:"));
    }
}
