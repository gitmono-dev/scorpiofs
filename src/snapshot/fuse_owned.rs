//! Owned payload caches and actual Bytes reply owners for modern online FUSE.
//! Inode/proof/native/kernel memory remains outside the payload capacity scope.

use std::{
    mem::{align_of, size_of},
    sync::{atomic::AtomicUsize, Arc},
};

use bytes::Bytes;

use super::{
    cas_range::VerifiedCasRange,
    cas_worker::CasReadScope,
    content::{BudgetClass, ContentBudget, Reservation},
    fuse_store::StoreContent,
    online_file::OnlineSnapshotFile,
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

#[derive(Clone)]
pub(crate) struct OnlineContentEntry {
    pub(crate) inode: u64,
    pub(crate) content: StoreContent,
}
impl CacheEntry for OnlineContentEntry {
    fn inode(&self) -> u64 {
        self.inode
    }
}

#[derive(Clone)]
pub(crate) struct OnlineRangeEntry {
    pub(crate) inode: u64,
    pub(crate) file: Arc<OnlineSnapshotFile>,
    pub(crate) range: Arc<OwnedChunkedFile>,
}
impl CacheEntry for OnlineRangeEntry {
    fn inode(&self) -> u64 {
        self.inode
    }
}

/// No-pages online mounts retain path-specific owners and requests. Their
/// fixed manifest continuity is not a cryptographic membership proof.
pub(crate) struct OnlineFuseCache {
    pub(crate) contents: FixedCache<OnlineContentEntry>,
    pub(crate) ranges: FixedCache<OnlineRangeEntry>,
    pub(crate) workers: Arc<CasReadScope>,
    pub(crate) coordinator: Arc<super::FetchCoordinator>,
    _reservation: Reservation,
}
impl OnlineFuseCache {
    pub(crate) fn new(reader: &SnapshotReader) -> Result<Self, SnapshotError> {
        let reservation = reader
            .content_scope
            .reserve(BudgetClass::Output, size_of::<Self>())?;
        Ok(Self {
            contents: FixedCache::new(),
            ranges: FixedCache::new(),
            workers: reader.content_scope.cas_workers(),
            coordinator: super::FetchCoordinator::in_reader_content_scope(reader.clone(), 8),
            _reservation: reservation,
        })
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

/// The store path retains only bounded wire handles and path proofs. Local
/// range payloads live in actual replies, not in an inode payload cache.
pub(crate) struct StoreRangeCache {
    pub(crate) ranges: FixedCache<RangeEntry>,
    pub(crate) workers: Arc<CasReadScope>,
    _reservation: Reservation,
}

impl StoreRangeCache {
    pub(crate) fn new(reader: &SnapshotReader) -> Result<Self, SnapshotError> {
        let reservation = reader
            .content_scope
            .reserve(BudgetClass::Output, size_of::<Self>())?;
        Ok(Self {
            ranges: FixedCache::new(),
            workers: reader.content_scope.cas_workers(),
            _reservation: reservation,
        })
    }
}

enum Payload {
    Content(Arc<VerifiedContent>),
    Range(Arc<VerifiedRange>),
    CasRange(Arc<VerifiedCasRange>),
    Store(StoreContent),
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Content(owner) => owner.as_bytes(),
            Self::Range(owner) => owner.as_bytes(),
            Self::CasRange(owner) => owner.as_bytes(),
            Self::Store(owner) => owner.as_bytes(),
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
pub(crate) struct ReplyAdmission(
    Reservation,
    Option<Arc<crate::util::read_profile::ReadProfile>>,
);
impl ReplyAdmission {
    pub(crate) fn with_read_profile(
        mut self,
        profile: Option<Arc<crate::util::read_profile::ReadProfile>>,
    ) -> Self {
        self.1 = profile;
        self
    }
    pub(crate) fn new(reader: &SnapshotReader) -> Result<Self, SnapshotError> {
        Self::reserve(&reader.content_scope)
    }
    pub(super) fn reserve(budget: &ContentBudget) -> Result<Self, SnapshotError> {
        // bytes1.12.1 boxes repr(C) Owned<T> = AtomicUsize + T. Include both
        // alignment gaps conservatively; clones/slices share that same box.
        let charge =
            size_of::<ReplyOwner>() + size_of::<AtomicUsize>() + 2 * align_of::<ReplyOwner>();
        budget
            .reserve(BudgetClass::Output, charge)
            .map(|reservation| Self(reservation, None))
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
    pub(crate) fn cas_range(self, owner: Arc<VerifiedCasRange>) -> Result<Bytes, SnapshotError> {
        let end = owner.len();
        self.publish(Payload::CasRange(owner), 0, end)
    }
    pub(crate) fn store_content(
        self,
        owner: StoreContent,
        start: usize,
        end: usize,
    ) -> Result<Bytes, SnapshotError> {
        self.publish(Payload::Store(owner), start, end)
    }
    fn publish(self, payload: Payload, start: usize, end: usize) -> Result<Bytes, SnapshotError> {
        let _phase = crate::util::read_profile::phase(
            self.1.as_ref(),
            crate::util::read_profile::Phase::ReplyOwner,
        );
        if start >= end || payload.as_ref().get(start..end).is_none() {
            return Err(SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "owned FUSE reply is not a nonempty verified slice",
            ));
        }
        let bytes = Bytes::from_owner(ReplyOwner {
            payload,
            start,
            end,
            _reservation: self.0,
        });
        if let Some(profile) = &self.1 {
            profile.add_many(&[
                (crate::util::read_profile::Metric::ReplyOwners, 1),
                (
                    crate::util::read_profile::Metric::ReplyOwnerBytes,
                    bytes.len() as u64,
                ),
            ]);
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{
        cas_index::LocalCasRangeMeters,
        content::{ContentBudgetLimits, ContentBudgetUsage},
        durable::{digest_of, DurableStore},
        frames::parse_digest,
        OBJECT_CAP,
    };

    #[test]
    fn cas_reply_uses_the_actual_allocation_and_last_bytes_hold_payload_and_reply_credit() {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let body = vec![0x91; OBJECT_CAP as usize + 1];
        let digest = digest_of(&body);
        let path = store
            .content_dir()
            .join(hex::encode(parse_digest(&digest).unwrap()));
        std::fs::write(path, &body).unwrap();
        let budget = ContentBudget::new(ContentBudgetLimits::default());
        let owner = VerifiedCasRange::read(
            &store,
            &digest,
            body.len() as u64,
            0,
            4096,
            &budget,
            &mut LocalCasRangeMeters::default(),
        )
        .unwrap()
        .unwrap();
        let payload_charge = budget.usage().output_bytes;
        let pointer = owner.as_bytes().as_ptr();
        let reply = ReplyAdmission::reserve(&budget)
            .unwrap()
            .cas_range(owner.clone())
            .unwrap();
        assert_eq!(reply.as_ptr(), pointer);
        let paid = budget.usage();
        assert!(paid.output_bytes > payload_charge);
        assert_eq!(paid.construction_bytes, 0);
        drop(owner);
        drop(store);
        let clone = reply.clone();
        let last = clone.slice(17..31);
        drop(reply);
        drop(clone);
        assert_eq!(last.as_ptr(), pointer.wrapping_add(17));
        assert_eq!(last.as_ref(), &[0x91; 14]);
        assert_eq!(budget.usage(), paid);
        drop(last);
        assert_eq!(
            budget.usage(),
            ContentBudgetUsage {
                output_bytes: 0,
                construction_bytes: 0
            }
        );
    }
}
