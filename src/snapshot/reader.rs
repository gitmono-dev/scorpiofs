//! High-level fixed-view reader built on [`Mst2Client`].
//!
//! Resolves once, then walks the directory graph to produce a verified file
//! manifest of the fixed view. The view never moves (spec 03 §6); callers
//! bind paths, routes and handles to the snapshot id/generation.

use std::collections::HashMap;

use crate::snapshot::{
    client::Mst2Client,
    types::{Descriptor, DirEntry, LookupResult, SnapshotError, SnapshotErrorCode},
};

/// One resolved file in the fixed view.
#[derive(Debug, Clone)]
pub struct SnapshotFile {
    /// Scope-relative path, leading `/` stripped.
    pub rel_path: String,
    pub fs_kind: String,
    pub size: u64,
    pub content_digest: String,
}

/// A fixed view plus everything needed to read its content.
pub struct SnapshotReader {
    pub client: Mst2Client,
    pub descriptor: Descriptor,
    pub lease_id: String,
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
        })
    }

    pub fn snapshot_id(&self) -> &str {
        &self.descriptor.snapshot_id
    }

    /// Batch lookup of scope-relative paths.
    pub async fn lookup(
        &self,
        paths: &[String],
    ) -> Result<Vec<LookupResult>, SnapshotError> {
        Ok(self
            .client
            .lookup(self.snapshot_id(), paths)
            .await?
            .results)
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

    async fn walk_dir(
        &self,
        dir: &str,
        out: &mut Vec<SnapshotFile>,
    ) -> Result<(), SnapshotError> {
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
