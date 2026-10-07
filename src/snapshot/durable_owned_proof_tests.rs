//! Test-only observations at the real durable batch validation branch.

use std::cell::RefCell;

use super::*;

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BatchProofCounts {
    pub raw_hashes: u64,
    pub raw_hash_bytes: u64,
    pub verified_reuses: u64,
    pub verified_reuse_bytes: u64,
}

struct CounterState {
    content_dir: PathBuf,
    counts: BatchProofCounts,
}

thread_local! {
    // These observations belong to current-thread hydration regression tests.
    static COUNTER: RefCell<Option<CounterState>> = const { RefCell::new(None) };
}

pub(crate) struct BatchProofCounter;

impl BatchProofCounter {
    pub(crate) fn install(content_dir: &Path) -> Self {
        COUNTER.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(CounterState {
                content_dir: content_dir.into(),
                counts: BatchProofCounts::default(),
            });
        });
        Self
    }

    pub(crate) fn counts(&self) -> BatchProofCounts {
        COUNTER.with(|slot| slot.borrow().as_ref().unwrap().counts)
    }

    pub(crate) fn reset(&self) {
        COUNTER
            .with(|slot| slot.borrow_mut().as_mut().unwrap().counts = BatchProofCounts::default());
    }
}

impl Drop for BatchProofCounter {
    fn drop(&mut self) {
        COUNTER.with(|slot| *slot.borrow_mut() = None);
    }
}

pub(super) fn record_raw_hash(content_dir: &Path, bytes: u64) {
    COUNTER.with(|slot| {
        if let Some(state) = slot.borrow_mut().as_mut() {
            if state.content_dir == content_dir {
                state.counts.raw_hashes += 1;
                state.counts.raw_hash_bytes += bytes;
            }
        }
    });
}

pub(super) fn record_verified_reuse(content_dir: &Path, bytes: u64) {
    COUNTER.with(|slot| {
        if let Some(state) = slot.borrow_mut().as_mut() {
            if state.content_dir == content_dir {
                state.counts.verified_reuses += 1;
                state.counts.verified_reuse_bytes += bytes;
            }
        }
    });
}

/// Deliberately offers an actual HTTP-published owner under the requested
/// key. This tests the core's binding checks without manufacturing an owner,
/// bypassing publication verification or adding a production constructor.
struct OfferedOwner(Arc<super::super::VerifiedContent>);

impl BorrowedBatch for OfferedOwner {
    fn content(&self, _digest: &str) -> Option<BorrowedBatchContent<'_>> {
        Some(BorrowedBatchContent::Verified(self.0.as_ref()))
    }
}

pub(crate) async fn hydrate_offered_owner(
    store: &DurableStore,
    file: &SnapshotFile,
    owner: Arc<super::super::VerifiedContent>,
    fault_phase: Option<&'static str>,
) -> Result<HydrateReport, SnapshotError> {
    let view = ViewMeta {
        snapshot_id: "sha256:owned-proof-fixture".into(),
        namespace_view_id: "sha256:owned-proof-namespace".into(),
        scope: "/project".into(),
        lease_id: "owned-proof-lease".into(),
    };
    let _fault = fault_phase
        .map(|phase| durability_tests::FaultGuard::install(store.content_dir(), phase, false));
    store
        .hydrate_batches_closure::<_, _, OfferedOwner, Vec<u8>>(
            &view,
            std::slice::from_ref(file),
            None,
            (1, 1),
            move |_| {
                let owner = owner.clone();
                Box::pin(async move { Ok(OfferedOwner(owner)) })
            },
            |_| Box::pin(async { panic!("small owner fixture must not fetch large content") }),
        )
        .await
}
