//! Single-flight, bounded-concurrency content fetching (spec 11 §5/§7).
//!
//! Waiters merge on content identity (`content_id + size`), never on path:
//! every caller proves membership against the fixed reader's committed
//! metadata root and checks its current lease before joining a download.
//! Deployments without metadata pages keep a separate online request for
//! every caller, preserving the server's per-path authorization check.
//! The leader's typed error is broadcast to every
//! waiter. The concurrency semaphore is the scheduling knob (spec 13): it
//! bounds simultaneous HTTP work without changing protocol semantics.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{oneshot, OnceCell, Semaphore};

use crate::snapshot::{
    SnapshotError, SnapshotErrorCode, SnapshotFile, SnapshotReader, ValidatedSnapshotClosure,
};

type FetchResult = Result<Arc<Vec<u8>>, Arc<SnapshotError>>;

struct FlightGuard {
    coordinator: Arc<FetchCoordinator>,
    key: String,
    completed: bool,
}

impl FlightGuard {
    fn complete(mut self, result: Result<Arc<Vec<u8>>, SnapshotError>) {
        let waiters = self
            .coordinator
            .inflight
            .lock()
            .unwrap()
            .remove(&self.key)
            .unwrap_or_default();
        self.completed = true;
        let shared = result.map_err(Arc::new);
        for waiter in waiters {
            let _ = waiter.send(shared.clone());
        }
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let waiters = self
            .coordinator
            .inflight
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.key)
            .unwrap_or_default();
        let error = Arc::new(SnapshotError::new(
            crate::snapshot::SnapshotErrorCode::Internal,
            "fetch task ended without a result",
        ));
        for waiter in waiters {
            let _ = waiter.send(Err(error.clone()));
        }
    }
}

/// Coordinates verified file fetches over one [`SnapshotReader`].
pub struct FetchCoordinator {
    reader: SnapshotReader,
    membership: OnceCell<HashMap<String, SnapshotFile>>,
    inflight: Mutex<HashMap<String, Vec<oneshot::Sender<FetchResult>>>>,
    semaphore: Arc<Semaphore>,
}

impl FetchCoordinator {
    /// `max_concurrent` bounds simultaneous leader downloads; waiters do
    /// not hold permits.
    pub fn new(reader: SnapshotReader, max_concurrent: usize) -> Arc<Self> {
        Self::with_membership(reader, max_concurrent, None)
    }

    /// Reuse a complete root proof already acquired for this fixed reader.
    /// Only file facts are retained; the closure cannot select a different
    /// descriptor or replace any caller's current lease/credential checks.
    pub fn with_verified_closure(
        reader: SnapshotReader,
        closure: &ValidatedSnapshotClosure,
        max_concurrent: usize,
    ) -> Result<Arc<Self>, SnapshotError> {
        closure.matches_descriptor(reader.descriptor())?;
        Ok(Self::with_membership(
            reader,
            max_concurrent,
            Some(membership_index(closure)),
        ))
    }

    fn with_membership(
        reader: SnapshotReader,
        max_concurrent: usize,
        membership: Option<HashMap<String, SnapshotFile>>,
    ) -> Arc<Self> {
        Arc::new(FetchCoordinator {
            reader,
            membership: OnceCell::new_with(membership),
            inflight: Mutex::new(HashMap::new()),
            semaphore: Arc::new(Semaphore::new(max_concurrent.max(1))),
        })
    }

    /// Fetch one file's verified bytes, merging concurrent identical
    /// requests. Every returned success has passed whole-file verification
    /// in the reader (object/chunk rehash).
    pub async fn fetch(
        self: &Arc<Self>,
        file: SnapshotFile,
        use_frames: bool,
    ) -> Result<Arc<Vec<u8>>, SnapshotError> {
        // A canonical path is not a membership proof. A caller cannot bypass
        // its own fixed-view check by naming another leader's content id.
        self.reader
            .authorized_context()
            .validate_relative_path(&file.rel_path)?;
        if !self.reader.capabilities().features.metadata_pages {
            // Without a root-verified proof, retain each caller's online
            // fixed-SID/path request. No other caller may supply its bytes
            // or its authorization result; concurrency is still bounded.
            self.reader.ensure_lease().await?;
            return self.lead(&file, use_frames).await;
        }
        self.validate_membership(&file).await?;
        self.reader.ensure_lease().await?;
        let key = format!("{}:{}", file.content_digest, file.size);
        let (tx, rx) = oneshot::channel();
        let leader = {
            let mut map = self.inflight.lock().unwrap();
            match map.get_mut(&key) {
                Some(waiters) => {
                    waiters.push(tx);
                    false
                }
                None => {
                    map.insert(key.clone(), vec![tx]);
                    true
                }
            }
        };
        if leader {
            let guard = FlightGuard {
                coordinator: self.clone(),
                key,
                completed: false,
            };
            // The first caller is a waiter too: dropping its future must not
            // cancel a download that another caller still needs.
            tokio::spawn(async move {
                let result = guard.coordinator.lead(&file, use_frames).await;
                guard.complete(result);
            });
        }
        rx.await
            .map_err(|_| {
                SnapshotError::new(
                    crate::snapshot::SnapshotErrorCode::Internal,
                    "fetch task disappeared without a result",
                )
            })?
            .map_err(|e| (*e).clone())
    }

    async fn validate_membership(&self, file: &SnapshotFile) -> Result<(), SnapshotError> {
        let files = self
            .membership
            .get_or_try_init(|| async {
                let closure = self.reader.snapshot_closure().await?;
                // Keep the fixed-root-derived path index, not duplicate page
                // bytes. A failed/cancelled initialization can be retried.
                Ok::<_, SnapshotError>(membership_index(&closure))
            })
            .await?;
        let path = file.rel_path.strip_prefix('/').unwrap_or(&file.rel_path);
        let expected = files.get(path).ok_or_else(|| {
            SnapshotError::new(
                SnapshotErrorCode::PathNotFound,
                format!("{} is not a file in the fixed snapshot", file.rel_path),
            )
        })?;
        let same_kind = file.fs_kind == expected.fs_kind
            || (file.fs_kind == "file" && expected.fs_kind == "regular");
        if file.content_digest != expected.content_digest
            || file.size != expected.size
            || !same_kind
        {
            return Err(SnapshotError::new(
                SnapshotErrorCode::DigestMismatch,
                format!("{} differs from its committed file metadata", file.rel_path),
            ));
        }
        Ok(())
    }

    async fn lead(
        &self,
        file: &SnapshotFile,
        use_frames: bool,
    ) -> Result<Arc<Vec<u8>>, SnapshotError> {
        let _permit = self.semaphore.clone().acquire_owned().await.map_err(|e| {
            SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::Internal,
                format!("fetch semaphore closed: {e}"),
            )
        })?;
        let bytes = if use_frames {
            self.reader
                .read_file_frames(&file.rel_path, &file.content_digest, file.size)
                .await?
        } else {
            self.reader
                .read_file(&file.rel_path, &file.content_digest)
                .await?
        };
        // The fetch paths verify content; the size must also match the view.
        if bytes.len() as u64 != file.size {
            return Err(SnapshotError::new(
                crate::snapshot::SnapshotErrorCode::DigestMismatch,
                format!(
                    "{}: fetched size {} != view size {}",
                    file.rel_path,
                    bytes.len(),
                    file.size
                ),
            ));
        }
        Ok(Arc::new(bytes))
    }
}

fn membership_index(closure: &ValidatedSnapshotClosure) -> HashMap<String, SnapshotFile> {
    closure
        .files()
        .iter()
        .cloned()
        .map(|file| (file.rel_path.clone(), file))
        .collect()
}
