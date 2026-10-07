use std::{collections::BTreeMap, sync::Arc};

use super::{SnapshotError, SnapshotErrorCode};

pub(super) type DirectoryEntries = Arc<[(String, u64)]>;

#[derive(Clone)]
pub(super) enum FixedDirectoryIndex {
    Building(BTreeMap<String, u64>),
    Ready(DirectoryEntries),
}

impl Default for FixedDirectoryIndex {
    fn default() -> Self {
        Self::Building(BTreeMap::new())
    }
}

impl FixedDirectoryIndex {
    pub(super) fn get(&self, name: &str) -> Option<&u64> {
        match self {
            Self::Building(entries) => entries.get(name),
            Self::Ready(entries) => entries
                .binary_search_by(|entry| entry.0.as_str().cmp(name))
                .ok()
                .map(|index| &entries[index].1),
        }
    }

    pub(super) fn contains_key(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    pub(super) fn insert(
        &mut self,
        name: String,
        inode: u64,
    ) -> Result<Option<u64>, SnapshotError> {
        match self {
            Self::Building(entries) => Ok(entries.insert(name, inode)),
            Self::Ready(_) => Err(SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "complete fixed directory index is immutable",
            )),
        }
    }

    pub(super) fn seal(&mut self) {
        if let Self::Building(entries) = self {
            let ordered: Vec<_> = std::mem::take(entries).into_iter().collect();
            *self = Self::Ready(ordered.into());
        }
    }

    pub(super) fn snapshot(&self) -> Result<DirectoryEntries, SnapshotError> {
        match self {
            Self::Ready(entries) => Ok(Arc::clone(entries)),
            Self::Building(_) => Err(SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "fixed directory index is not complete",
            )),
        }
    }
}
