//! Single-flight, bounded-concurrency content fetching (spec 11 §5/§7).
//!
//! Waiters merge on content identity (`content_id + size`), never on path:
//! each caller has already passed its own snapshot path-membership and
//! authorization checks before reaching here, so sharing downloaded bytes
//! never shares permission. The leader's typed error is broadcast to every
//! waiter. The concurrency semaphore is the scheduling knob (spec 13): it
//! bounds simultaneous HTTP work without changing protocol semantics.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{oneshot, Semaphore};

use crate::snapshot::{SnapshotError, SnapshotFile, SnapshotReader};

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
    inflight: Mutex<HashMap<String, Vec<oneshot::Sender<FetchResult>>>>,
    semaphore: Arc<Semaphore>,
}

impl FetchCoordinator {
    /// `max_concurrent` bounds simultaneous leader downloads; waiters do
    /// not hold permits.
    pub fn new(reader: SnapshotReader, max_concurrent: usize) -> Arc<Self> {
        Arc::new(FetchCoordinator {
            reader,
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
