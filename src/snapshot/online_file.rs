//! Private fixed-SID path authority for a completed online directory manifest.
//!
//! This token is not an MTP2 membership proof. Only the public file-manifest
//! mount constructors mint it after their actual fixed-SID directory walk.
//! Each path retains an independent online request and current lease check.

use std::sync::Arc;

use super::{CacheDomain, SnapshotError, SnapshotErrorCode, SnapshotFile, SnapshotReader};

#[derive(Debug)]
pub(super) struct OnlineSnapshotFile {
    file: SnapshotFile,
    domain: CacheDomain,
    epoch: u64,
    snapshot: String,
    scope: String,
    root: String,
}

impl OnlineSnapshotFile {
    pub(super) fn from_manifest_file(
        reader: &SnapshotReader,
        file: SnapshotFile,
    ) -> Result<Arc<Self>, SnapshotError> {
        if reader.capabilities().features.metadata_pages {
            return Err(SnapshotError::new(
                SnapshotErrorCode::InvalidRequest,
                "online path authority cannot replace fixed-root membership",
            ));
        }
        let context = reader.authorized_context();
        context.validate_relative_path(&file.rel_path)?;
        reader.client().validate_path(&file.rel_path)?;
        reader.client().validate_file_size(file.size)?;
        super::frames::parse_digest(&file.content_digest)?;
        if !matches!(
            file.fs_kind.as_str(),
            "file" | "regular" | "executable" | "symlink"
        ) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::UnsupportedEntry,
                "online manifest entry is not a supported file",
            ));
        }
        reader.local_lease_status()?;
        Ok(Arc::new(Self {
            file,
            domain: context.cache_domain().clone(),
            epoch: context.authorization_epoch(),
            snapshot: reader.snapshot_id().to_string(),
            scope: reader.descriptor().scope.clone(),
            root: reader.descriptor().metadata_root.clone(),
        }))
    }

    pub(super) fn file(&self) -> &SnapshotFile {
        &self.file
    }

    pub(super) async fn validate(&self, reader: &SnapshotReader) -> Result<(), SnapshotError> {
        let context = reader.authorized_context();
        if reader.capabilities().features.metadata_pages
            || self.domain != *context.cache_domain()
            || self.epoch != context.authorization_epoch()
            || self.snapshot != reader.snapshot_id()
            || self.scope != reader.descriptor().scope
            || self.root != reader.descriptor().metadata_root
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "online file belongs to a different reader authority or fixed snapshot",
            ));
        }
        context.validate_relative_path(&self.file.rel_path)?;
        reader.ensure_lease().await
    }
}
