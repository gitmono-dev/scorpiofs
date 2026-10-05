//! Ownership of explicit owned-fetch output and managed construction buffers.
//! Native codec/HTTP/TLS buffers, metadata/error DTO heaps and caller copies
//! are separate. These are managed buffer capacity quotas, not RSS limits.

use std::{
    fmt,
    mem::size_of,
    sync::{Arc, OnceLock},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{SnapshotError, SnapshotErrorCode};

const CREDIT: usize = 1024;
pub const PROCESS_OUTPUT_BYTES: usize = 512 * 1024 * 1024;
pub const PROCESS_CONSTRUCTION_BYTES: usize = 64 * 1024 * 1024;
const COORDINATOR_OUTPUT_BYTES: usize = 128 * 1024 * 1024;
const COORDINATOR_CONSTRUCTION_BYTES: usize = 32 * 1024 * 1024;

/// Separate retained-output and managed-construction capacity limits.
/// Local policies may lower the hard caps and use whole 1-KiB credits.
#[derive(Debug, Clone, Copy)]
pub struct ContentBudgetLimits {
    output_bytes: usize,
    construction_bytes: usize,
}

impl ContentBudgetLimits {
    pub fn new(output_bytes: usize, construction_bytes: usize) -> Result<Self, SnapshotError> {
        if output_bytes == 0
            || construction_bytes == 0
            || output_bytes > COORDINATOR_OUTPUT_BYTES
            || construction_bytes > COORDINATOR_CONSTRUCTION_BYTES
            || !output_bytes.is_multiple_of(CREDIT)
            || !construction_bytes.is_multiple_of(CREDIT)
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::InvalidRequest,
                "content budgets must use positive 1-KiB credits within coordinator caps",
            ));
        }
        Ok(Self {
            output_bytes,
            construction_bytes,
        })
    }
}

impl Default for ContentBudgetLimits {
    fn default() -> Self {
        Self {
            output_bytes: COORDINATOR_OUTPUT_BYTES,
            construction_bytes: COORDINATOR_CONSTRUCTION_BYTES,
        }
    }
}

/// Charged 1-KiB capacity observations, sampled separately from admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentBudgetUsage {
    pub output_bytes: usize,
    pub construction_bytes: usize,
}

struct ProcessBudget {
    output: Arc<Semaphore>,
    construction: Arc<Semaphore>,
}

fn process_budget() -> &'static ProcessBudget {
    static BUDGET: OnceLock<ProcessBudget> = OnceLock::new();
    BUDGET.get_or_init(|| ProcessBudget {
        output: Arc::new(Semaphore::new(PROCESS_OUTPUT_BYTES / CREDIT)),
        construction: Arc::new(Semaphore::new(PROCESS_CONSTRUCTION_BYTES / CREDIT)),
    })
}

pub(crate) struct ContentBudget {
    output: Arc<Semaphore>,
    construction: Arc<Semaphore>,
    limits: ContentBudgetLimits,
}

#[derive(Clone, Copy)]
pub(crate) enum BudgetClass {
    Output,
    Construction,
}

impl ContentBudget {
    pub(crate) fn new(limits: ContentBudgetLimits) -> Arc<Self> {
        Arc::new(Self {
            output: Arc::new(Semaphore::new(limits.output_bytes / CREDIT)),
            construction: Arc::new(Semaphore::new(limits.construction_bytes / CREDIT)),
            limits,
        })
    }

    pub(crate) fn usage(&self) -> ContentBudgetUsage {
        ContentBudgetUsage {
            output_bytes: self.limits.output_bytes - self.output.available_permits() * CREDIT,
            construction_bytes: self.limits.construction_bytes
                - self.construction.available_permits() * CREDIT,
        }
    }

    pub(crate) fn process_usage() -> ContentBudgetUsage {
        let budget = process_budget();
        ContentBudgetUsage {
            output_bytes: PROCESS_OUTPUT_BYTES - budget.output.available_permits() * CREDIT,
            construction_bytes: PROCESS_CONSTRUCTION_BYTES
                - budget.construction.available_permits() * CREDIT,
        }
    }

    pub(crate) fn reserve(
        &self,
        class: BudgetClass,
        bytes: usize,
    ) -> Result<Reservation, SnapshotError> {
        let credits = bytes
            .checked_add(CREDIT - 1)
            .and_then(|n| u32::try_from(n / CREDIT).ok())
            .ok_or_else(capacity_limit)?;
        let process = process_budget();
        let (local, global) = match class {
            BudgetClass::Output => (&self.output, &process.output),
            BudgetClass::Construction => (&self.construction, &process.construction),
        };
        Reservation::acquire(local, global, credits)
    }
}

pub(crate) struct Reservation {
    _local: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

impl Reservation {
    fn acquire(
        local: &Arc<Semaphore>,
        global: &Arc<Semaphore>,
        credits: u32,
    ) -> Result<Self, SnapshotError> {
        let local = local
            .clone()
            .try_acquire_many_owned(credits)
            .map_err(|_| capacity_limit())?;
        let global = global
            .clone()
            .try_acquire_many_owned(credits)
            .map_err(|_| capacity_limit())?;
        Ok(Self {
            _local: local,
            _global: global,
        })
    }
}

fn capacity_limit() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::LimitExceeded,
        "content capacity admission limit reached",
    )
}

/// The payload is dropped before its capacity reservation. Mutation is bounded
/// by the one admitted allocation; no method exposes a mutable Vec or grows it.
pub(crate) struct AccountedBuffer {
    bytes: Vec<u8>,
    _reservation: Reservation,
}

impl AccountedBuffer {
    pub(crate) fn new(
        budget: &ContentBudget,
        class: BudgetClass,
        capacity: usize,
        extra: usize,
    ) -> Result<Self, SnapshotError> {
        let charge = capacity
            .checked_add(extra)
            .and_then(|n| n.checked_add(size_of::<Self>() + 2 * size_of::<usize>()))
            .ok_or_else(capacity_limit)?;
        let reservation = budget.reserve(class, charge)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| capacity_limit())?;
        // This helper uses standard Global with a single initial exact reserve.
        // Fail if a changed toolchain reports an unexpected capacity, rather
        // than silently growing an unaccounted allocation.
        if bytes.capacity() != capacity {
            return Err(SnapshotError::new(
                SnapshotErrorCode::Internal,
                "unexpected fixed buffer capacity",
            ));
        }
        Ok(Self {
            bytes,
            _reservation: reservation,
        })
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }
    pub(crate) fn capacity(&self) -> usize {
        self.bytes.capacity()
    }

    pub(crate) fn append(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        if bytes.len() > self.capacity() - self.len() {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "body exceeds its fixed expected size",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    pub(crate) fn initialize(&mut self) {
        self.bytes.resize(self.capacity(), 0);
    }

    pub(crate) fn write_at(&mut self, offset: usize, bytes: &[u8]) -> Result<(), SnapshotError> {
        let end = offset.checked_add(bytes.len()).ok_or_else(capacity_limit)?;
        let destination = self.bytes.get_mut(offset..end).ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "chunk exceeds its fixed output range",
            )
        })?;
        destination.copy_from_slice(bytes);
        Ok(())
    }
}

/// An immutable whole-file result produced by the accounted coordinator path.
/// Its capacity remains reserved through the last Arc, including queued results.
/// Creating a copy from its borrowed bytes is a caller-owned allocation.
pub struct VerifiedContent {
    buffer: AccountedBuffer,
}

impl VerifiedContent {
    pub fn as_bytes(&self) -> &[u8] {
        self.buffer.as_bytes()
    }
    pub fn as_slice(&self) -> &[u8] {
        self.as_bytes()
    }
    pub fn len(&self) -> usize {
        self.buffer.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn publish(
        buffer: AccountedBuffer,
        expected_size: usize,
        expected_digest: &[u8; 32],
        _receipt: super::owned_transport::ContentEofReceipt,
    ) -> Result<Arc<Self>, SnapshotError> {
        if buffer.len() != expected_size
            || ring::digest::digest(&ring::digest::SHA256, buffer.as_bytes()).as_ref()
                != expected_digest
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "content does not match its fixed size and whole digest",
            ));
        }
        Ok(Arc::new(Self { buffer }))
    }
}

impl AsRef<[u8]> for VerifiedContent {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Debug for VerifiedContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedContent")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_local_and_global_admission_returns_credits() {
        let local = Arc::new(Semaphore::new(2));
        let global = Arc::new(Semaphore::new(1));
        assert_eq!(
            Reservation::acquire(&local, &global, 2).err().unwrap().code,
            SnapshotErrorCode::LimitExceeded
        );
        assert_eq!(local.available_permits(), 2);
        assert_eq!(global.available_permits(), 1);
        let owner = Reservation::acquire(&local, &global, 1).unwrap();
        assert_eq!(local.available_permits(), 1);
        assert_eq!(global.available_permits(), 0);
        drop(owner);
        assert_eq!(local.available_permits(), 2);
        assert_eq!(global.available_permits(), 1);
    }

    #[test]
    fn admitted_exact_capacities_cover_boundary_sizes_without_growth() {
        let budget = ContentBudget::new(ContentBudgetLimits::default());
        for capacity in [
            0,
            1,
            1023,
            1024,
            1025,
            256 * 1024,
            1024 * 1024,
            1024 * 1024 + 76,
            2 * 1024 * 1024 + 64,
            64 * 1024 * 1024,
        ] {
            let buffer = AccountedBuffer::new(&budget, BudgetClass::Output, capacity, 0).unwrap();
            assert_eq!(buffer.capacity(), capacity);
            assert_eq!(buffer.len(), 0);
            assert!(budget.usage().output_bytes >= capacity);
            drop(buffer);
            assert_eq!(budget.usage().output_bytes, 0);
        }
    }

    #[test]
    fn fixed_capacity_admission_precedes_allocation_and_failure_returns_credits() {
        let budget = ContentBudget::new(ContentBudgetLimits::new(2 * CREDIT, CREDIT).unwrap());
        let mut first = AccountedBuffer::new(&budget, BudgetClass::Output, 20, 0).unwrap();
        assert_eq!(budget.usage().output_bytes, CREDIT);
        assert_eq!(first.capacity(), 20);
        first.append(&[0; 20]).unwrap();
        assert_eq!(
            first.append(&[0]).unwrap_err().code,
            SnapshotErrorCode::DigestMismatch
        );
        assert_eq!(first.capacity(), 20);
        let second = AccountedBuffer::new(&budget, BudgetClass::Output, 20, 0).unwrap();
        assert_eq!(budget.usage().output_bytes, 2 * CREDIT);
        assert_eq!(
            AccountedBuffer::new(&budget, BudgetClass::Output, 1, 0)
                .err()
                .unwrap()
                .code,
            SnapshotErrorCode::LimitExceeded
        );
        drop((first, second));
        assert_eq!(budget.usage().output_bytes, 0);
        assert_eq!(
            AccountedBuffer::new(&budget, BudgetClass::Output, usize::MAX, 0)
                .err()
                .unwrap()
                .code,
            SnapshotErrorCode::LimitExceeded
        );
        assert_eq!(budget.usage().output_bytes, 0);
    }

    #[test]
    fn output_and_construction_credits_are_independent() {
        let budget = ContentBudget::new(ContentBudgetLimits::new(CREDIT, CREDIT).unwrap());
        let output = AccountedBuffer::new(&budget, BudgetClass::Output, 1, 0).unwrap();
        let construction = AccountedBuffer::new(&budget, BudgetClass::Construction, 1, 0).unwrap();
        assert_eq!(
            budget.usage(),
            ContentBudgetUsage {
                output_bytes: CREDIT,
                construction_bytes: CREDIT
            }
        );
        drop(construction);
        assert_eq!(
            budget.usage(),
            ContentBudgetUsage {
                output_bytes: CREDIT,
                construction_bytes: 0
            }
        );
        drop(output);
        assert_eq!(
            budget.usage(),
            ContentBudgetUsage {
                output_bytes: 0,
                construction_bytes: 0
            }
        );
    }
}
