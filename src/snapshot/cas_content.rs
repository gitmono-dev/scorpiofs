//! Admitted, immutable small-object bytes verified from the local CAS.
//!
//! This proves byte integrity only. The caller must establish the workspace
//! domain, fixed-view membership and a live lease before publishing a reply.

use std::{
    fmt,
    io::{self, Read},
    mem::size_of,
    sync::Arc,
};

use super::{
    content::{AccountedBuffer, BudgetClass, ContentBudget},
    durable::DurableStore,
    frames::parse_digest,
    range::OBJECT_CAP,
    secure_fs, SnapshotError, SnapshotErrorCode,
};

const READ_SCRATCH_BYTES: usize = 16 * 1024;

/// A local CAS owner, distinct from content carrying an HTTP EOF receipt.
/// Output credits survive cache eviction while a reply retains this Arc.
pub(crate) struct VerifiedCasContent {
    buffer: AccountedBuffer,
}

impl VerifiedCasContent {
    pub(crate) fn read(
        store: &DurableStore,
        digest: &str,
        expected_size: u64,
        budget: &ContentBudget,
    ) -> Result<Option<Arc<Self>>, SnapshotError> {
        if expected_size > OBJECT_CAP {
            return Err(SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "local owned CAS read exceeds the small-object limit",
            ));
        }
        let expected_digest = parse_digest(digest)?;
        let capacity = usize::try_from(expected_size).map_err(|_| {
            SnapshotError::new(
                SnapshotErrorCode::LimitExceeded,
                "local owned CAS size exceeds platform capacity",
            )
        })?;
        // AccountedBuffer also charges its own allocation and Arc counters.
        // Admission precedes even opening a local object, including a miss.
        let mut buffer =
            AccountedBuffer::new(budget, BudgetClass::Output, capacity, size_of::<Self>())?;
        let _scratch_reservation = budget.reserve(BudgetClass::Construction, READ_SCRATCH_BYTES)?;
        let mut scratch = [0u8; READ_SCRATCH_BYTES];
        let path = store.content_dir().join(hex::encode(expected_digest));
        let mut input = match secure_fs::open_regular_nonblocking(&path) {
            Ok(input) => input,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error(error)),
        };
        if input.metadata().map_err(io_error)?.len() != expected_size {
            return Err(size_mismatch());
        }
        read_bounded(&mut input, &mut buffer, &mut scratch)?;
        if ring::digest::digest(&ring::digest::SHA256, buffer.as_bytes()).as_ref()
            != expected_digest.as_slice()
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                "local CAS bytes do not match their whole digest",
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

impl fmt::Debug for VerifiedCasContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedCasContent")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

fn read_bounded(
    input: &mut impl Read,
    buffer: &mut AccountedBuffer,
    scratch: &mut [u8],
) -> Result<(), SnapshotError> {
    // The extra byte detects growth after fstat without allocating or reading
    // an unbounded object. Only exact bytes enter the admitted output buffer.
    let mut input = input.take(buffer.capacity() as u64 + 1);
    loop {
        let count = match input.read(scratch) {
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(io_error(error)),
        };
        if count == 0 {
            break;
        }
        if count > buffer.capacity() - buffer.len() {
            return Err(size_mismatch());
        }
        buffer.append(&scratch[..count])?;
    }
    if buffer.len() != buffer.capacity() {
        return Err(size_mismatch());
    }
    Ok(())
}

fn size_mismatch() -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::DigestMismatch,
        "local CAS object differs from its fixed size",
    )
}

fn io_error(error: io::Error) -> SnapshotError {
    SnapshotError::new(
        SnapshotErrorCode::Internal,
        format!("local CAS read failed: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Cursor, path::PathBuf};

    use super::*;
    use crate::snapshot::content::{ContentBudgetLimits, ContentBudgetUsage};

    fn budget() -> Arc<ContentBudget> {
        ContentBudget::new(ContentBudgetLimits::default())
    }

    fn digest(bytes: &[u8]) -> String {
        format!(
            "sha256:{}",
            hex::encode(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref())
        )
    }

    fn path(store: &DurableStore, digest: &str) -> PathBuf {
        store
            .content_dir()
            .join(hex::encode(parse_digest(digest).unwrap()))
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

    #[test]
    fn verified_owner_retains_output_until_the_last_arc_drops() {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let body = vec![0x5a; 1024];
        let digest = digest(&body);
        fs::write(path(&store, &digest), &body).unwrap();
        let budget = budget();
        let _baseline_scratch = budget.reserve(BudgetClass::Construction, 1024).unwrap();
        let _baseline_output = budget.reserve(BudgetClass::Output, 1024).unwrap();
        let baseline = budget.usage();
        let owner = VerifiedCasContent::read(&store, &digest, body.len() as u64, &budget)
            .unwrap()
            .unwrap();
        assert_eq!(owner.as_bytes(), body);
        assert_eq!(owner.len(), body.len());
        let retained = budget.usage();
        assert_eq!(retained.construction_bytes, baseline.construction_bytes);
        // A whole credit of payload must also admit its owner allocation.
        assert!(retained.output_bytes > baseline.output_bytes + body.len());
        let reply_owner = Arc::clone(&owner);
        assert_eq!(budget.usage(), retained);
        drop(owner);
        assert_eq!(budget.usage(), retained);
        assert_eq!(reply_owner.as_bytes(), body);
        drop(reply_owner);
        assert_eq!(budget.usage(), baseline);
    }

    #[test]
    fn only_a_missing_object_is_a_cache_miss_and_returns_all_credits() {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let budget = budget();
        assert!(
            VerifiedCasContent::read(&store, &digest(b"missing"), 7, &budget)
                .unwrap()
                .is_none()
        );
        assert_unused(&budget);
        fs::create_dir(path(&store, &digest(b"directory"))).unwrap();
        let error =
            VerifiedCasContent::read(&store, &digest(b"directory"), 9, &budget).unwrap_err();
        assert_eq!(error.code, SnapshotErrorCode::Internal);
        assert_unused(&budget);
    }

    #[test]
    fn empty_and_exact_object_cap_are_verified_and_accounted() {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let budget = budget();
        for size in [0, OBJECT_CAP as usize] {
            let body = vec![0x7b; size];
            let digest = digest(&body);
            fs::write(path(&store, &digest), &body).unwrap();
            let owner = VerifiedCasContent::read(&store, &digest, size as u64, &budget)
                .unwrap()
                .unwrap();
            assert_eq!(owner.as_bytes(), body);
            assert_eq!(owner.len(), size);
            assert!(budget.usage().output_bytes > size);
            assert_eq!(budget.usage().construction_bytes, 0);
            drop(owner);
            assert_unused(&budget);
        }
    }

    #[test]
    fn same_length_tampering_is_an_error_and_returns_all_credits() {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let digest = digest(b"trusted");
        fs::write(path(&store, &digest), b"tampere").unwrap();
        let budget = budget();
        assert_eq!(
            VerifiedCasContent::read(&store, &digest, 7, &budget)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
        assert_unused(&budget);
    }

    #[test]
    fn truncated_or_oversized_descriptor_is_an_error_and_returns_all_credits() {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let digest = digest(b"trusted");
        let budget = budget();
        for body in [b"short".as_slice(), b"too long".as_slice()] {
            fs::write(path(&store, &digest), body).unwrap();
            assert_eq!(
                VerifiedCasContent::read(&store, &digest, 7, &budget)
                    .unwrap_err()
                    .code,
                SnapshotErrorCode::DigestMismatch
            );
            assert_unused(&budget);
        }
    }

    #[test]
    fn invalid_digest_and_above_cap_fail_before_io_or_admission() {
        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let digest = digest(b"object");
        fs::create_dir(path(&store, &digest)).unwrap();
        let budget = ContentBudget::new(ContentBudgetLimits::new(1024, 1024).unwrap());
        assert_eq!(
            VerifiedCasContent::read(&store, &digest, OBJECT_CAP + 1, &budget)
                .unwrap_err()
                .code,
            SnapshotErrorCode::LimitExceeded
        );
        for malformed in ["sha256:../outside", "SHA256:00", "sha256:00", ""] {
            assert_eq!(
                VerifiedCasContent::read(&store, malformed, 0, &budget)
                    .unwrap_err()
                    .code,
                SnapshotErrorCode::DigestMismatch
            );
            assert_unused(&budget);
        }
    }

    #[cfg(unix)]
    #[test]
    fn output_and_scratch_admission_precede_opening_a_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let body = vec![0x11; 1024];
        let digest = digest(&body);
        let target = temp.path().join("outside");
        fs::write(&target, &body).unwrap();
        symlink(&target, path(&store, &digest)).unwrap();
        for limits in [
            // Payload fits, but its retained owner needs another credit.
            ContentBudgetLimits::new(1024, READ_SCRATCH_BYTES).unwrap(),
            // Output fits, but managed scratch must be admitted before open.
            ContentBudgetLimits::new(4096, 1024).unwrap(),
        ] {
            let budget = ContentBudget::new(limits);
            assert_eq!(
                VerifiedCasContent::read(&store, &digest, body.len() as u64, &budget)
                    .unwrap_err()
                    .code,
                SnapshotErrorCode::LimitExceeded
            );
            assert_unused(&budget);
        }
        let budget = budget();
        assert_eq!(
            VerifiedCasContent::read(&store, &digest, body.len() as u64, &budget)
                .unwrap_err()
                .code,
            SnapshotErrorCode::Internal
        );
        assert_unused(&budget);
    }

    #[cfg(unix)]
    #[test]
    fn final_symlinks_are_errors_even_with_valid_or_missing_targets() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let body = b"valid target";
        let digest = digest(body);
        let target = temp.path().join("outside");
        fs::write(&target, body).unwrap();
        symlink(&target, path(&store, &digest)).unwrap();
        let budget = budget();
        for target_exists in [true, false] {
            if !target_exists {
                fs::remove_file(&target).unwrap();
            }
            assert_eq!(
                VerifiedCasContent::read(&store, &digest, body.len() as u64, &budget)
                    .unwrap_err()
                    .code,
                SnapshotErrorCode::Internal
            );
            assert_unused(&budget);
        }
    }

    #[cfg(unix)]
    #[test]
    fn final_fifo_without_a_writer_is_rejected_without_blocking() {
        use std::{ffi::CString, os::unix::ffi::OsStrExt, sync::mpsc, time::Duration};

        let temp = tempfile::tempdir().unwrap();
        let store = DurableStore::open(temp.path()).unwrap();
        let digest = digest(b"fifo");
        let fifo_path = CString::new(path(&store, &digest).as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) }, 0);
        let budget = budget();
        let worker_budget = Arc::clone(&budget);
        let (sender, receiver) = mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let _ = sender.send(VerifiedCasContent::read(&store, &digest, 4, &worker_budget));
        });
        // A blocking-open regression must fail this test without hanging its
        // runtime shutdown. A timeout drops the join handle during unwinding.
        let result = receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("CAS FIFO read blocked waiting for a writer");
        worker.join().unwrap();
        assert_eq!(result.unwrap_err().code, SnapshotErrorCode::Internal);
        assert_unused(&budget);
    }

    #[test]
    fn bounded_reader_detects_growth_with_only_one_sentinel_byte() {
        let budget = budget();
        let mut buffer = AccountedBuffer::new(&budget, BudgetClass::Output, 3, 0).unwrap();
        let mut input = Cursor::new(b"grew far beyond the expected size");
        let mut scratch = [0u8; READ_SCRATCH_BYTES];
        assert_eq!(
            read_bounded(&mut input, &mut buffer, &mut scratch)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
        assert_eq!(input.position(), 4);
        assert_eq!(buffer.len(), 0);
        drop(buffer);
        assert_unused(&budget);
    }

    #[test]
    fn bounded_reader_detects_truncation_after_descriptor_validation() {
        let budget = budget();
        let mut buffer = AccountedBuffer::new(&budget, BudgetClass::Output, 7, 0).unwrap();
        let mut input = Cursor::new(b"short");
        let mut scratch = [0u8; READ_SCRATCH_BYTES];
        assert_eq!(
            read_bounded(&mut input, &mut buffer, &mut scratch)
                .unwrap_err()
                .code,
            SnapshotErrorCode::DigestMismatch
        );
        drop(buffer);
        assert_unused(&budget);
    }

    #[test]
    fn bounded_reader_retries_interrupted_reads() {
        struct InterruptedOnce {
            interrupted: bool,
            bytes: Cursor<&'static [u8]>,
        }

        impl Read for InterruptedOnce {
            fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(io::ErrorKind::Interrupted.into());
                }
                self.bytes.read(output)
            }
        }

        let budget = budget();
        let mut buffer = AccountedBuffer::new(&budget, BudgetClass::Output, 7, 0).unwrap();
        let mut input = InterruptedOnce {
            interrupted: false,
            bytes: Cursor::new(b"trusted"),
        };
        let mut scratch = [0u8; READ_SCRATCH_BYTES];
        read_bounded(&mut input, &mut buffer, &mut scratch).unwrap();
        assert_eq!(buffer.as_bytes(), b"trusted");
        drop(buffer);
        assert_unused(&budget);
    }
}
