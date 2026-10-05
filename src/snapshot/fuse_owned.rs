//! Owned payload caches and actual Bytes reply owners for modern online FUSE.
//! Inode/proof/native/kernel memory remains outside the payload capacity scope.

use std::{
    mem::{align_of, size_of},
    sync::{atomic::AtomicUsize, Arc},
};

use bytes::Bytes;

use super::{
    content::{BudgetClass, Reservation},
    OwnedChunkedFile, ProvenSnapshotFile, SnapshotError, SnapshotErrorCode, SnapshotReader,
    VerifiedContent, VerifiedRange,
};

const CACHE_ENTRIES: usize = 16;

#[derive(Clone)]
pub(crate) struct ContentEntry {
    pub(crate) inode: u64,
    pub(crate) proven: Arc<ProvenSnapshotFile>,
    pub(crate) content: Arc<VerifiedContent>,
}

#[derive(Clone)]
pub(crate) struct RangeEntry {
    pub(crate) inode: u64,
    pub(crate) proven: Arc<ProvenSnapshotFile>,
    pub(crate) range: Arc<OwnedChunkedFile>,
}

pub(crate) trait CacheEntry: Clone {
    fn inode(&self) -> u64;
}
impl CacheEntry for ContentEntry {
    fn inode(&self) -> u64 {
        self.inode
    }
}
impl CacheEntry for RangeEntry {
    fn inode(&self) -> u64 {
        self.inode
    }
}

/// Inline LRU slots. Eviction releases only this cache's actual owners.
pub(crate) struct FixedCache<T> {
    entries: [Option<T>; CACHE_ENTRIES],
    len: usize,
}
impl<T: CacheEntry> FixedCache<T> {
    fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
            len: 0,
        }
    }
    fn remove(&mut self, index: usize) -> T {
        let entry = self.entries[index].take().unwrap();
        for slot in index..self.len - 1 {
            self.entries[slot] = self.entries[slot + 1].take();
        }
        self.len -= 1;
        entry
    }
    pub(crate) fn get(&mut self, inode: u64) -> Option<T> {
        let index = self.entries[..self.len]
            .iter()
            .position(|entry| entry.as_ref().unwrap().inode() == inode)?;
        let entry = self.remove(index);
        self.entries[self.len] = Some(entry.clone());
        self.len += 1;
        Some(entry)
    }
    pub(crate) fn insert(&mut self, entry: T) {
        if let Some(index) = self.entries[..self.len]
            .iter()
            .position(|current| current.as_ref().unwrap().inode() == entry.inode())
        {
            drop(self.remove(index));
        }
        if self.len == CACHE_ENTRIES {
            drop(self.remove(0));
        }
        self.entries[self.len] = Some(entry);
        self.len += 1;
    }
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

pub(crate) struct OwnedFuseCache {
    pub(crate) contents: FixedCache<ContentEntry>,
    pub(crate) ranges: FixedCache<RangeEntry>,
    // Slots and their payload references drop before the slot reservation.
    _reservation: Reservation,
}
impl OwnedFuseCache {
    pub(crate) fn new(reader: &SnapshotReader) -> Result<Self, SnapshotError> {
        let reservation = reader
            .content_scope
            .reserve(BudgetClass::Output, size_of::<Self>())?;
        Ok(Self {
            contents: FixedCache::new(),
            ranges: FixedCache::new(),
            _reservation: reservation,
        })
    }
}

enum Payload {
    Content(Arc<VerifiedContent>),
    Range(Arc<VerifiedRange>),
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Content(owner) => owner.as_bytes(),
            Self::Range(owner) => owner.as_bytes(),
        }
    }
}

struct ReplyOwner {
    payload: Payload,
    start: usize,
    end: usize,
    // Actual payload Arc drops before these boxed-owner credits.
    _reservation: Reservation,
}
impl AsRef<[u8]> for ReplyOwner {
    fn as_ref(&self) -> &[u8] {
        &self.payload.as_ref()[self.start..self.end]
    }
}

/// Admission precedes body I/O; transferring it creates the actual Bytes owner.
pub(crate) struct ReplyAdmission(Reservation);
impl ReplyAdmission {
    pub(crate) fn new(reader: &SnapshotReader) -> Result<Self, SnapshotError> {
        // bytes1.12.1 boxes repr(C) Owned<T> = AtomicUsize + T. Include both
        // alignment gaps conservatively; clones/slices share that same box.
        let charge =
            size_of::<ReplyOwner>() + size_of::<AtomicUsize>() + 2 * align_of::<ReplyOwner>();
        reader
            .content_scope
            .reserve(BudgetClass::Output, charge)
            .map(Self)
    }
    pub(crate) fn content(
        self,
        owner: Arc<VerifiedContent>,
        start: usize,
        end: usize,
    ) -> Result<Bytes, SnapshotError> {
        self.publish(Payload::Content(owner), start, end)
    }
    pub(crate) fn range(self, owner: Arc<VerifiedRange>) -> Result<Bytes, SnapshotError> {
        let end = owner.len();
        self.publish(Payload::Range(owner), 0, end)
    }
    fn publish(self, payload: Payload, start: usize, end: usize) -> Result<Bytes, SnapshotError> {
        if start >= end || payload.as_ref().get(start..end).is_none() {
            return Err(SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "owned FUSE reply is not a nonempty verified slice",
            ));
        }
        Ok(Bytes::from_owner(ReplyOwner {
            payload,
            start,
            end,
            _reservation: self.0,
        }))
    }
}
