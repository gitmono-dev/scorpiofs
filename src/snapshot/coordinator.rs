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

enum Role {
    Leader,
    Waiter(oneshot::Receiver<FetchResult>),
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
        let role = {
            let mut map = self.inflight.lock().unwrap();
            match map.get_mut(&key) {
                Some(waiters) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    Role::Waiter(rx)
                }
                None => {
                    map.insert(key.clone(), Vec::new());
                    Role::Leader
                }
            }
        };
        match role {
            Role::Waiter(rx) => rx
                .await
                .map_err(|_| {
                    SnapshotError::new(
                        crate::snapshot::SnapshotErrorCode::Internal,
                        "fetch leader disappeared without a result",
                    )
                })?
                .map_err(|e| (*e).clone()),
            Role::Leader => {
                let result = self.lead(&file, use_frames).await;
                let waiters = self
                    .inflight
                    .lock()
                    .unwrap()
                    .remove(&key)
                    .unwrap_or_default();
                for tx in waiters {
                    let shared = result
                        .as_ref()
                        .map(|b| b.clone())
                        .map_err(|e| Arc::new(e.clone()));
                    let _ = tx.send(shared);
                }
                result
            }
        }
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
