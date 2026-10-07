//! Fixed-view, domain-local small-file owners for real v3 workspace stores.
//! The cache shares content bytes, never membership evidence or inode identity.

use std::{mem::size_of, sync::Arc};

use super::{
    cas_content::VerifiedCasContent,
    content::{BudgetClass, ContentBudget, Reservation},
    frames::parse_digest,
    SnapshotError, SnapshotErrorCode, VerifiedContent, OBJECT_CAP,
};
use crate::util::read_profile::{Metric, ReadProfile};

const CACHE_ENTRIES: usize = 2048;
const CACHE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentKey {
    digest: [u8; 32],
    size: u64,
}

impl ContentKey {
    pub(crate) fn new(digest: &str, size: u64) -> Result<Self, SnapshotError> {
        if size > OBJECT_CAP {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "workspace small-file cache received a large file",
            ));
        }
        Ok(Self {
            digest: parse_digest(digest)?,
            size,
        })
    }
}

#[derive(Clone)]
pub(crate) enum StoreContent {
    Cas(Arc<VerifiedCasContent>),
    Wire(Arc<VerifiedContent>),
}

impl StoreContent {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Cas(content) => content.as_bytes(),
            Self::Wire(content) => content.as_bytes(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.as_bytes().len()
    }
}

struct Entry {
    key: ContentKey,
    content: StoreContent,
    last_use: u64,
}

/// A bounded LRU. Output credits remain on the real payload owner when an
/// entry is evicted but a reply still holds it. These limits cover cache
/// retention; reader/process output quotas also cover in-flight/held owners.
pub(crate) struct StoreSmallCache {
    entries: Vec<Entry>,
    retained_bytes: usize,
    max_bytes: usize,
    clock: u64,
    // The table and all its owners drop before their table-capacity credits.
    _reservation: Reservation,
}

impl StoreSmallCache {
    pub(crate) fn new(budget: &ContentBudget) -> Result<Self, SnapshotError> {
        Self::with_limits(budget, CACHE_ENTRIES, CACHE_BYTES)
    }

    fn with_limits(
        budget: &ContentBudget,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Self, SnapshotError> {
        let charge = max_entries
            .checked_mul(size_of::<Entry>())
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .ok_or_else(capacity_error)?;
        let reservation = budget.reserve(BudgetClass::Output, charge)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(max_entries)
            .map_err(|_| capacity_error())?;
        if entries.capacity() != max_entries || max_entries == 0 || max_bytes == 0 {
            return Err(capacity_error());
        }
        Ok(Self {
            entries,
            retained_bytes: 0,
            max_bytes,
            clock: 0,
            _reservation: reservation,
        })
    }

    fn remove(&mut self, index: usize) -> Entry {
        let entry = self.entries.swap_remove(index);
        self.retained_bytes -= entry.content.len();
        entry
    }

    #[cfg(test)]
    pub(crate) fn get(&mut self, key: ContentKey) -> Option<StoreContent> {
        self.get_profiled(key, None)
    }

    pub(crate) fn get_profiled(
        &mut self,
        key: ContentKey,
        profile: Option<&ReadProfile>,
    ) -> Option<StoreContent> {
        let index = self.entries.iter().position(|entry| entry.key == key);
        if let Some(profile) = profile {
            profile.add_many(&[
                (
                    Metric::CacheGetProbes,
                    index.map_or(self.entries.len(), |i| i + 1) as u64,
                ),
                (
                    if index.is_some() {
                        Metric::OwnerCacheHit
                    } else {
                        Metric::OwnerCacheMiss
                    },
                    1,
                ),
            ]);
        }
        let index = index?;
        let stamp = self.tick();
        let entry = &mut self.entries[index];
        entry.last_use = stamp;
        Some(entry.content.clone())
    }

    fn tick(&mut self) -> u64 {
        if self.clock == u64::MAX {
            // An epoch reset keeps eviction bounded even after counter wrap.
            // Old entries tie; the next hit/insert becomes newer than all.
            for entry in &mut self.entries {
                entry.last_use = 0;
            }
            self.clock = 0;
        }
        self.clock += 1;
        self.clock
    }

    #[cfg(test)]
    pub(crate) fn insert(
        &mut self,
        key: ContentKey,
        content: StoreContent,
    ) -> Result<(), SnapshotError> {
        self.insert_profiled(key, content, None)
    }

    pub(crate) fn insert_profiled(
        &mut self,
        key: ContentKey,
        content: StoreContent,
        profile: Option<&ReadProfile>,
    ) -> Result<(), SnapshotError> {
        if content.len() as u64 != key.size {
            return Err(SnapshotError::new(
                SnapshotErrorCode::IntegrityError,
                "workspace content owner differs from its fixed cache key",
            ));
        }
        let found = self.entries.iter().position(|entry| entry.key == key);
        if let Some(profile) = profile {
            profile.add(
                Metric::CacheInsertProbes,
                found.map_or(self.entries.len(), |i| i + 1) as u64,
            );
        }
        if let Some(index) = found {
            drop(self.remove(index));
        }
        if content.len() > self.max_bytes {
            return Ok(());
        }
        while self.entries.len() == self.entries.capacity()
            || self.retained_bytes > self.max_bytes - content.len()
        {
            let oldest = self
                .entries
                .iter()
                .enumerate()
                .min_by_key(|(_, entry)| entry.last_use)
                .map(|(index, _)| index)
                .unwrap();
            drop(self.remove(oldest));
            if let Some(profile) = profile {
                profile.add(Metric::CacheEvictions, 1);
            }
        }
        self.retained_bytes += content.len();
        let last_use = self.tick();
        self.entries.push(Entry {
            key,
            content,
            last_use,
        });
        Ok(())
    }
}

fn capacity_error() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::LimitExceeded,
        "workspace small-file cache capacity admission failed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{durable::digest_of, ContentBudgetLimits, DurableStore};

    fn content(
        store: &DurableStore,
        budget: &ContentBudget,
        byte: u8,
        size: usize,
    ) -> (ContentKey, StoreContent) {
        let body = vec![byte; size];
        let digest = digest_of(&body);
        std::fs::write(
            store
                .content_dir()
                .join(hex::encode(parse_digest(&digest).unwrap())),
            &body,
        )
        .unwrap();
        (
            ContentKey::new(&digest, size as u64).unwrap(),
            StoreContent::Cas(
                VerifiedCasContent::read(store, &digest, size as u64, budget)
                    .unwrap()
                    .unwrap(),
            ),
        )
    }

    #[test]
    fn byte_eviction_keeps_held_owners_paid_and_aliases_share_the_same_allocation() {
        let root = tempfile::tempdir().unwrap();
        let store = DurableStore::open(root.path()).unwrap();
        let budget = ContentBudget::new(ContentBudgetLimits::default());
        let mut cache = StoreSmallCache::with_limits(&budget, 3, 16 * 1024).unwrap();
        let slots = budget.usage().output_bytes;
        let (a, body_a) = content(&store, &budget, 1, 8192);
        let held_paid = budget.usage().output_bytes - slots;
        cache.insert(a, body_a).unwrap();
        let held = cache.get(a).unwrap();
        let alias = cache.get(a).unwrap();
        assert_eq!(alias.as_bytes().as_ptr(), held.as_bytes().as_ptr());
        drop(alias);
        let (b, body_b) = content(&store, &budget, 2, 8192);
        cache.insert(b, body_b).unwrap();
        drop(cache.get(a));
        let (c, body_c) = content(&store, &budget, 3, 8192);
        cache.insert(c, body_c).unwrap();
        assert!(cache.get(b).is_none());
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.retained_bytes, 16 * 1024);
        // Make C newest; insertion of D must release the cache's A reference.
        drop(cache.get(c));
        let (d, body_d) = content(&store, &budget, 4, 8192);
        cache.insert(d, body_d).unwrap();
        assert!(cache.get(a).is_none());
        assert_eq!(held.as_bytes(), &[1; 8192]);
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.retained_bytes, 16 * 1024);
        drop(cache);
        assert_eq!(budget.usage().output_bytes, held_paid);
        assert_eq!(budget.usage().construction_bytes, 0);
        drop(held);
        assert_eq!(budget.usage().output_bytes, 0);
    }

    #[test]
    fn entry_eviction_is_independent_of_byte_capacity_and_hot_slots_do_not_move() {
        let root = tempfile::tempdir().unwrap();
        let store = DurableStore::open(root.path()).unwrap();
        let budget = ContentBudget::new(ContentBudgetLimits::default());
        let mut cache = StoreSmallCache::with_limits(&budget, 2, 1024 * 1024).unwrap();
        let (a, body_a) = content(&store, &budget, 1, 32);
        let (b, body_b) = content(&store, &budget, 2, 32);
        cache.insert(a, body_a).unwrap();
        cache.insert(b, body_b).unwrap();
        let first_slot = &cache.entries[0] as *const Entry;
        drop(cache.get(a));
        assert_eq!(&cache.entries[0] as *const Entry, first_slot);
        assert!(cache.entries[0].key == a);
        let (c, body_c) = content(&store, &budget, 3, 32);
        cache.insert(c, body_c).unwrap();
        assert!(cache.get(b).is_none());
        assert!(cache.get(a).is_some());
        assert!(cache.get(c).is_some());
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.retained_bytes, 64);
        drop(cache);
        assert_eq!(budget.usage().output_bytes, 0);
    }

    #[test]
    fn fixed_table_admission_and_digest_size_key_fail_without_retained_state() {
        let tiny = ContentBudget::new(ContentBudgetLimits::new(1024, 16 * 1024).unwrap());
        assert_eq!(
            StoreSmallCache::new(&tiny).err().unwrap().code,
            SnapshotErrorCode::LimitExceeded
        );
        assert_eq!(tiny.usage().output_bytes, 0);
        let root = tempfile::tempdir().unwrap();
        let store = DurableStore::open(root.path()).unwrap();
        let budget = ContentBudget::new(ContentBudgetLimits::default());
        let mut cache = StoreSmallCache::with_limits(&budget, 2, 1024).unwrap();
        let (key, body) = content(&store, &budget, 1, 32);
        cache.insert(key, body.clone()).unwrap();
        let another_size = ContentKey {
            digest: key.digest,
            size: 33,
        };
        assert!(cache.get(another_size).is_none());
        assert_eq!(
            cache.insert(another_size, body).unwrap_err().code,
            SnapshotErrorCode::IntegrityError
        );
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.retained_bytes, 32);
        drop(cache);
        assert_eq!(budget.usage().output_bytes, 0);
    }
}
