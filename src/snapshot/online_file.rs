//! Private fixed-SID path authority for a completed online directory manifest.
//!
//! This token is not an MTP2 membership proof. The mount and hydration
//! constructors mint it only after their actual fixed-SID directory walk.
//! Each path retains an independent online request and current lease check.

use std::{collections::HashMap, sync::Arc};

use super::{CacheDomain, SnapshotError, SnapshotErrorCode, SnapshotFile, SnapshotReader};

/// Hydration may stream only entries from this completed online walk. No
/// caller-supplied manifest constructor or fixed-root conversion is provided.
pub(super) struct CompletedOnlineManifest {
    files: Vec<SnapshotFile>,
    ranges: HashMap<String, Arc<OnlineSnapshotFile>>,
}

impl CompletedOnlineManifest {
    pub(super) async fn walk(reader: &SnapshotReader) -> Result<Self, SnapshotError> {
        if reader.capabilities().features.metadata_pages {
            return Err(SnapshotError::new(
                SnapshotErrorCode::InvalidRequest,
                "online manifest cannot replace fixed-root membership",
            ));
        }
        // Do not mint any authority before every directory/cursor succeeds.
        let files = reader.file_manifest_directory().await?;
        reader.ensure_lease().await?;
        let mut ranges = HashMap::new();
        for file in &files {
            if file.size > super::OBJECT_CAP {
                let authority = OnlineSnapshotFile::from_manifest_file(reader, file.clone())?;
                if ranges.insert(file.rel_path.clone(), authority).is_some() {
                    return Err(SnapshotError::new(
                        SnapshotErrorCode::IntegrityError,
                        "completed online manifest repeats a file path",
                    ));
                }
            }
        }
        reader.local_lease_status()?;
        Ok(Self { files, ranges })
    }

    pub(super) fn files(&self) -> &[SnapshotFile] {
        &self.files
    }

    pub(super) fn range_file(
        &self,
        file: &SnapshotFile,
    ) -> Result<Arc<OnlineSnapshotFile>, SnapshotError> {
        let authority = self.ranges.get(&file.rel_path).ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "file is not a large entry in the completed online manifest",
            )
        })?;
        if authority.file() != file {
            return Err(SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "stream tuple differs from the completed online manifest",
            ));
        }
        Ok(authority.clone())
    }
}

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
