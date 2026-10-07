//! Admitted local CAS range bytes, without a fabricated HTTP END receipt.
//! Chunk facts prove integrity only; the caller must validate membership and
//! the current workspace lease before publishing these bytes.

use std::{fmt, mem::size_of, sync::Arc};

use super::{
    cas_index::{self, LocalCasRangeMeters, CHUNK_SIZE},
    content::{AccountedBuffer, BudgetClass, ContentBudget, Reservation},
    content_profile::{MAX_FILE_SIZE, OBJECT_CAP},
    durable::DurableStore,
    frames::parse_digest,
    SnapshotError, SnapshotErrorCode,
};

/// Fixed construction storage. The actual allocation drops before its credit;
/// callers receive only bounded mutable slices, never a growable Vec.
struct RangeScratch {
    bytes: Vec<u8>,
    _reservation: Reservation,
}

impl RangeScratch {
    fn new(budget: &ContentBudget, capacity: usize) -> Result<Self, SnapshotError> {
        let charge = capacity
            .checked_add(size_of::<Self>() + 2 * size_of::<usize>())
            .ok_or_else(capacity_limit)?;
        let reservation = budget.reserve(BudgetClass::Construction, charge)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| capacity_limit())?;
        if bytes.capacity() != capacity {
            return Err(SnapshotError::new(
                SnapshotErrorCode::Internal,
                "unexpected local CAS scratch capacity",
            ));
        }
        bytes.resize(capacity, 0);
        Ok(Self {
            bytes,
            _reservation: reservation,
        })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

pub(crate) struct VerifiedCasRange {
    buffer: AccountedBuffer,
}

#[cfg(test)]
type AdmittedPause = Option<Box<dyn FnOnce() + Send>>;
#[cfg(not(test))]
#[derive(Default)]
struct AdmittedPause {
    _private: (),
}

impl VerifiedCasRange {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn read(
        store: &DurableStore,
        digest: &str,
        expected_size: u64,
        offset: u64,
        requested: u64,
        budget: &ContentBudget,
        meters: &mut LocalCasRangeMeters,
    ) -> Result<Option<Arc<Self>>, SnapshotError> {
        Self::read_admitted(
            store,
            digest,
            expected_size,
            offset,
            requested,
            budget,
            meters,
            Default::default(),
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn read_paused(
        store: &DurableStore,
        digest: &str,
        expected_size: u64,
        offset: u64,
        requested: u64,
        budget: &ContentBudget,
        meters: &mut LocalCasRangeMeters,
        pause: impl FnOnce() + Send + 'static,
    ) -> Result<Option<Arc<Self>>, SnapshotError> {
        Self::read_admitted(
            store,
            digest,
            expected_size,
            offset,
            requested,
            budget,
            meters,
            Some(Box::new(pause)),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn read_admitted(
        store: &DurableStore,
        digest: &str,
        expected_size: u64,
        offset: u64,
        requested: u64,
        budget: &ContentBudget,
        meters: &mut LocalCasRangeMeters,
        _pause: AdmittedPause,
    ) -> Result<Option<Arc<Self>>, SnapshotError> {
        *meters = LocalCasRangeMeters::default();
        let fixed_digest = parse_digest(digest)?;
        if !(OBJECT_CAP + 1..=MAX_FILE_SIZE).contains(&expected_size) {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "owned local CAS range is outside the large-file profile",
            ));
        }
        // Clamp to the real fixed EOF before checking the output limit or
        // platform width. A huge request for a short tail remains bounded.
        let wanted = requested.min(expected_size.saturating_sub(offset));
        if wanted > super::client::MAX_BUFFERED_FILE_BYTES {
            return Err(capacity_limit());
        }
        let wanted = usize::try_from(wanted).map_err(|_| capacity_limit())?;
        let mut buffer =
            AccountedBuffer::new(budget, BudgetClass::Output, wanted, size_of::<Self>())?;
        let mut scratch = RangeScratch::new(budget, expected_size.min(CHUNK_SIZE) as usize)?;
        #[cfg(test)]
        if let Some(pause) = _pause {
            pause();
        }
        // Both allocations are admitted before CAS open or canonicalization.
        let path = store.content_dir().join(hex::encode(fixed_digest));
        if !cas_index::read_indexed_into(
            &path,
            store.content_dir(),
            digest,
            expected_size,
            offset,
            wanted,
            &mut buffer,
            scratch.as_mut_slice(),
            meters,
        )? {
            // Local credits are released as this call returns, before a
            // caller starts its independently admitted wire fallback.
            return Ok(None);
        }
        if buffer.len() != wanted {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "owned local CAS range differs from its fixed output length",
            ));
        }
        Ok(Some(Arc::new(Self { buffer })))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.buffer.as_bytes()
    }

    pub(crate) fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl fmt::Debug for VerifiedCasRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedCasRange")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

fn capacity_limit() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::LimitExceeded,
        "owned local CAS range capacity admission limit reached",
    )
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::*;
    use crate::snapshot::content::{ContentBudgetLimits, ContentBudgetUsage};

    fn fixture() -> (tempfile::TempDir, DurableStore, String, Vec<u8>) {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let mut body = vec![0x51; 2 * CHUNK_SIZE as usize + 7];
        body[CHUNK_SIZE as usize..2 * CHUNK_SIZE as usize].fill(0x72);
        body[2 * CHUNK_SIZE as usize..].fill(0x93);
        let digest = super::super::durable::digest_of(&body);
        fs::write(path(&store, &digest), &body).unwrap();
        (temp, store, digest, body)
    }

    fn path(store: &DurableStore, digest: &str) -> PathBuf {
        store
            .content_dir()
            .join(hex::encode(parse_digest(digest).unwrap()))
    }

    fn budget() -> Arc<ContentBudget> {
        ContentBudget::new(ContentBudgetLimits::default())
    }

    fn assert_unused(budget: &ContentBudget) {
        assert_eq!(
            budget.usage(),
            ContentBudgetUsage {
                output_bytes: 0,
                construction_bytes: 0,
            }
        );
    }

    fn read(
        store: &DurableStore,
        digest: &str,
        size: u64,
        offset: u64,
        requested: u64,
        budget: &ContentBudget,
        meters: &mut LocalCasRangeMeters,
    ) -> Arc<VerifiedCasRange> {
        VerifiedCasRange::read(store, digest, size, offset, requested, budget, meters)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn cold_whole_proof_and_warm_covering_work_retain_only_actual_output_owners() {
        let (_temp, store, digest, body) = fixture();
        let budget = budget();
        let _baseline = budget.reserve(BudgetClass::Output, 1024).unwrap();
        let baseline = budget.usage();
        let mut meters = LocalCasRangeMeters::default();
        let first = read(
            &store,
            &digest,
            body.len() as u64,
            0,
            4096,
            &budget,
            &mut meters,
        );
        assert_eq!(first.as_bytes(), &body[..4096]);
        assert!(meters.index_built && !meters.index_hit);
        assert_eq!(meters.bytes_read, body.len() as u64);
        assert_eq!(meters.whole_sha256_bytes, body.len() as u64);
        assert_eq!(meters.chunk_sha256_bytes, body.len() as u64);
        let retained = budget.usage();
        assert_eq!(retained.construction_bytes, baseline.construction_bytes);
        assert!(retained.output_bytes > baseline.output_bytes + 4096);
        let last = first.clone();
        drop(first);
        assert_eq!(budget.usage(), retained);
        assert_eq!(last.as_bytes(), &body[..4096]);
        drop(last);
        assert_eq!(budget.usage(), baseline);
        for (offset, requested, work) in [
            (0, 4096, CHUNK_SIZE),
            (CHUNK_SIZE - 3, 6, 2 * CHUNK_SIZE),
            (2 * CHUNK_SIZE, u64::MAX, 7),
        ] {
            let owner = read(
                &store,
                &digest,
                body.len() as u64,
                offset,
                requested,
                &budget,
                &mut meters,
            );
            let wanted = requested.min(body.len() as u64 - offset) as usize;
            assert_eq!(
                owner.as_bytes(),
                &body[offset as usize..offset as usize + wanted]
            );
            assert!(meters.index_hit && !meters.index_built);
            assert_eq!(meters.bytes_read, work);
            assert_eq!(meters.chunk_sha256_bytes, work);
            assert_eq!(meters.whole_sha256_bytes, 0);
            assert_eq!(
                budget.usage().construction_bytes,
                baseline.construction_bytes
            );
            drop(owner);
            assert_eq!(budget.usage(), baseline);
        }
    }

    #[test]
    fn cold_unread_tail_corruption_never_publishes_an_owner_or_fact_and_can_retry() {
        let (_temp, store, digest, body) = fixture();
        let budget = budget();
        let mut corrupt = body.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        fs::write(path(&store, &digest), &corrupt).unwrap();
        let mut meters = LocalCasRangeMeters::default();
        for _ in 0..2 {
            assert_eq!(
                VerifiedCasRange::read(
                    &store,
                    &digest,
                    body.len() as u64,
                    0,
                    1,
                    &budget,
                    &mut meters
                )
                .unwrap_err()
                .code,
                SnapshotErrorCode::DigestMismatch
            );
            assert_eq!(meters.whole_sha256_bytes, body.len() as u64);
            assert!(!meters.index_built && !meters.index_hit);
            assert_unused(&budget);
        }
        fs::write(path(&store, &digest), &body).unwrap();
        drop(read(
            &store,
            &digest,
            body.len() as u64,
            0,
            1,
            &budget,
            &mut meters,
        ));
        assert!(meters.index_built);
        assert_unused(&budget);
    }

    #[test]
    fn warm_unrequested_covering_byte_and_late_chunk_errors_refund_all_local_credits() {
        let (_temp, store, digest, body) = fixture();
        let budget = budget();
        let mut meters = LocalCasRangeMeters::default();
        drop(read(
            &store,
            &digest,
            body.len() as u64,
            0,
            1,
            &budget,
            &mut meters,
        ));
        for (changed, offset, wanted, work) in [
            (4096, 0, 1, CHUNK_SIZE),
            (
                CHUNK_SIZE as usize + 4096,
                CHUNK_SIZE - 1,
                2,
                2 * CHUNK_SIZE,
            ),
        ] {
            let mut corrupt = body.clone();
            corrupt[changed] ^= 1;
            fs::write(path(&store, &digest), &corrupt).unwrap();
            assert_eq!(
                VerifiedCasRange::read(
                    &store,
                    &digest,
                    body.len() as u64,
                    offset,
                    wanted,
                    &budget,
                    &mut meters
                )
                .unwrap_err()
                .code,
                SnapshotErrorCode::DigestMismatch
            );
            assert!(meters.index_hit);
            assert_eq!(meters.bytes_read, work);
            assert_eq!(meters.chunk_sha256_bytes, work);
            assert_eq!(meters.whole_sha256_bytes, 0);
            assert_unused(&budget);
            fs::write(path(&store, &digest), &body).unwrap();
        }
    }

    #[test]
    fn warm_fact_does_not_claim_integrity_of_unread_tail_but_strict_audit_detects_it() {
        let (_temp, store, digest, body) = fixture();
        let budget = budget();
        let mut meters = LocalCasRangeMeters::default();
        drop(read(
            &store,
            &digest,
            body.len() as u64,
            0,
            1,
            &budget,
            &mut meters,
        ));
        let mut corrupt = body.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        fs::write(path(&store, &digest), &corrupt).unwrap();
        let owner = read(
            &store,
            &digest,
            body.len() as u64,
            0,
            1,
            &budget,
            &mut meters,
        );
        assert_eq!(owner.as_bytes(), &body[..1]);
        assert!(meters.index_hit);
        assert_eq!(meters.bytes_read, CHUNK_SIZE);
        drop(owner);
        assert_eq!(
            store
                .read_verified_blob_range(&digest, body.len() as u64, 0, 1)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
        assert_unused(&budget);
    }

    #[test]
    fn missing_is_the_only_miss_and_size_or_type_errors_reset_meters_and_refund() {
        let (_temp, store, digest, body) = fixture();
        let budget = budget();
        fs::remove_file(path(&store, &digest)).unwrap();
        let mut meters = LocalCasRangeMeters {
            bytes_read: 99,
            index_hit: true,
            ..Default::default()
        };
        assert!(VerifiedCasRange::read(
            &store,
            &digest,
            body.len() as u64,
            0,
            1,
            &budget,
            &mut meters
        )
        .unwrap()
        .is_none());
        assert_eq!(meters, LocalCasRangeMeters::default());
        assert_unused(&budget);
        for bytes in [&body[..body.len() - 1], &body[..OBJECT_CAP as usize]] {
            fs::write(path(&store, &digest), bytes).unwrap();
            assert_eq!(
                VerifiedCasRange::read(
                    &store,
                    &digest,
                    body.len() as u64,
                    0,
                    1,
                    &budget,
                    &mut meters
                )
                .unwrap_err()
                .code,
                SnapshotErrorCode::DigestMismatch
            );
            assert_eq!(meters.bytes_read, 0);
            assert_unused(&budget);
        }
        fs::remove_file(path(&store, &digest)).unwrap();
        fs::create_dir(path(&store, &digest)).unwrap();
        assert_eq!(
            VerifiedCasRange::read(
                &store,
                &digest,
                body.len() as u64,
                0,
                1,
                &budget,
                &mut meters
            )
            .unwrap_err()
            .code,
            SnapshotErrorCode::Internal
        );
        assert_unused(&budget);
    }

    #[test]
    fn digest_profile_and_real_eof_clamp_precede_any_local_io() {
        let (_temp, store, digest, body) = fixture();
        let budget = budget();
        let mut meters = LocalCasRangeMeters {
            bytes_read: 99,
            ..Default::default()
        };
        for malformed in ["", "SHA256:00", "sha256:00", "sha256:../outside"] {
            assert_eq!(
                VerifiedCasRange::read(
                    &store,
                    malformed,
                    body.len() as u64,
                    0,
                    1,
                    &budget,
                    &mut meters
                )
                .unwrap_err()
                .code,
                SnapshotErrorCode::DigestMismatch
            );
            assert_eq!(meters, LocalCasRangeMeters::default());
            assert_unused(&budget);
        }
        for size in [0, OBJECT_CAP, MAX_FILE_SIZE + 1] {
            assert_eq!(
                VerifiedCasRange::read(&store, &digest, size, 0, 1, &budget, &mut meters)
                    .unwrap_err()
                    .code,
                SnapshotErrorCode::LimitExceeded
            );
            assert_eq!(meters.bytes_read, 0);
            assert_unused(&budget);
        }
        let limit = super::super::client::MAX_BUFFERED_FILE_BYTES;
        assert_eq!(
            VerifiedCasRange::read(
                &store,
                &digest,
                limit + 1,
                0,
                limit + 1,
                &budget,
                &mut meters
            )
            .unwrap_err()
            .code,
            SnapshotErrorCode::LimitExceeded
        );
        assert_eq!(meters.bytes_read, 0);
        let owner = read(
            &store,
            &digest,
            body.len() as u64,
            body.len() as u64 - 7,
            u64::MAX,
            &budget,
            &mut meters,
        );
        assert_eq!(owner.as_bytes(), &body[body.len() - 7..]);
        drop(owner);
        assert_unused(&budget);
    }

    #[test]
    fn unchanged_digest_reuses_facts_but_changed_digest_or_cas_domain_requires_whole_proof() {
        let (_temp, store, digest, body) = fixture();
        let budget = budget();
        let mut meters = LocalCasRangeMeters::default();
        drop(read(
            &store,
            &digest,
            body.len() as u64,
            0,
            1,
            &budget,
            &mut meters,
        ));
        assert!(meters.index_built);
        drop(read(
            &store,
            &digest,
            body.len() as u64,
            0,
            1,
            &budget,
            &mut meters,
        ));
        assert!(meters.index_hit);
        let mut changed = body.clone();
        *changed.last_mut().unwrap() ^= 1;
        let changed_digest = super::super::durable::digest_of(&changed);
        fs::write(path(&store, &changed_digest), &changed).unwrap();
        drop(read(
            &store,
            &changed_digest,
            changed.len() as u64,
            0,
            1,
            &budget,
            &mut meters,
        ));
        assert!(meters.index_built && !meters.index_hit);
        assert_eq!(meters.whole_sha256_bytes, changed.len() as u64);
        // New content must not replace the immutable old digest's fact.
        drop(read(
            &store,
            &digest,
            body.len() as u64,
            0,
            1,
            &budget,
            &mut meters,
        ));
        assert!(meters.index_hit);
        let other_temp = tempfile::tempdir().unwrap();
        let other_store = DurableStore::open(other_temp.path()).unwrap();
        fs::write(path(&other_store, &digest), &body).unwrap();
        drop(read(
            &other_store,
            &digest,
            body.len() as u64,
            0,
            1,
            &budget,
            &mut meters,
        ));
        assert!(meters.index_built && !meters.index_hit);
        assert_eq!(meters.whole_sha256_bytes, body.len() as u64);
        assert_unused(&budget);
    }

    #[cfg(unix)]
    #[test]
    fn output_and_scratch_admission_precede_valid_or_dangling_symlink_open() {
        use std::os::unix::fs::symlink;

        let (temp, store, digest, body) = fixture();
        let local = path(&store, &digest);
        fs::remove_file(&local).unwrap();
        let outside = temp.path().join("outside");
        fs::write(&outside, &body).unwrap();
        symlink(&outside, &local).unwrap();
        let mut meters = LocalCasRangeMeters::default();
        for target_exists in [true, false] {
            if !target_exists {
                fs::remove_file(&outside).unwrap();
            }
            for limits in [
                ContentBudgetLimits::new(4096, 2 * CHUNK_SIZE as usize).unwrap(),
                ContentBudgetLimits::new(8192, CHUNK_SIZE as usize).unwrap(),
            ] {
                let budget = ContentBudget::new(limits);
                // 4096 output needs owner credit; 1MiB scratch also needs
                // helper bookkeeping. Neither may inspect the symlink first.
                assert_eq!(
                    VerifiedCasRange::read(
                        &store,
                        &digest,
                        body.len() as u64,
                        0,
                        4096,
                        &budget,
                        &mut meters
                    )
                    .unwrap_err()
                    .code,
                    SnapshotErrorCode::LimitExceeded
                );
                assert_eq!(meters.bytes_read, 0);
                assert_unused(&budget);
            }
            let budget = budget();
            assert_eq!(
                VerifiedCasRange::read(
                    &store,
                    &digest,
                    body.len() as u64,
                    0,
                    4096,
                    &budget,
                    &mut meters
                )
                .unwrap_err()
                .code,
                SnapshotErrorCode::Internal
            );
            assert_eq!(meters.bytes_read, 0);
            assert_unused(&budget);
        }
    }

    #[cfg(unix)]
    #[test]
    fn fifo_without_writer_is_rejected_after_admission_without_blocking() {
        use std::{ffi::CString, os::unix::ffi::OsStrExt, sync::mpsc, time::Duration};

        let (_temp, store, digest, body) = fixture();
        let local = path(&store, &digest);
        fs::remove_file(&local).unwrap();
        let name = CString::new(local.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        for limits in [
            ContentBudgetLimits::new(4096, 2 * CHUNK_SIZE as usize).unwrap(),
            ContentBudgetLimits::new(8192, CHUNK_SIZE as usize).unwrap(),
        ] {
            let budget = ContentBudget::new(limits);
            let mut meters = LocalCasRangeMeters::default();
            assert_eq!(
                VerifiedCasRange::read(
                    &store,
                    &digest,
                    body.len() as u64,
                    0,
                    4096,
                    &budget,
                    &mut meters
                )
                .unwrap_err()
                .code,
                SnapshotErrorCode::LimitExceeded
            );
            assert_eq!(meters.bytes_read, 0);
            assert_unused(&budget);
        }
        let budget = budget();
        let worker_budget = budget.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let mut meters = LocalCasRangeMeters::default();
            let result = VerifiedCasRange::read(
                &store,
                &digest,
                body.len() as u64,
                0,
                4096,
                &worker_budget,
                &mut meters,
            );
            let _ = sender.send((result, meters));
        });
        let (result, meters) = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("local CAS range blocked waiting for a FIFO writer");
        worker.join().unwrap();
        assert_eq!(result.unwrap_err().code, SnapshotErrorCode::Internal);
        assert_eq!(meters.bytes_read, 0);
        assert_unused(&budget);
    }
}
