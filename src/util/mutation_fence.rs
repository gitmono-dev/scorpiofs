//! Coordinate native mutations with a stable upper scan and mount teardown.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::{
    OwnedRwLockReadGuard, OwnedRwLockWriteGuard, OwnedSemaphorePermit, RwLock, Semaphore,
};

/// An admission belongs to the actual operation, including cancellation recovery,
/// rather than the HTTP request or FUSE reply waiting for it.
#[derive(Clone)]
pub struct MutationFence {
    sealed: Arc<RwLock<bool>>,
    uncertain: Arc<AtomicBool>,
    capacity: Arc<Semaphore>,
    cleanup_capacity: Arc<Semaphore>,
}

impl MutationFence {
    pub fn new(max_in_flight: usize) -> Self {
        assert!(max_in_flight > 0);
        Self {
            sealed: Arc::new(RwLock::new(false)),
            uncertain: Arc::new(AtomicBool::new(false)),
            capacity: Arc::new(Semaphore::new(max_in_flight)),
            cleanup_capacity: Arc::new(Semaphore::new(max_in_flight)),
        }
    }

    pub(crate) async fn admit(&self, cleanup: bool) -> std::io::Result<AdmittedMutation> {
        // Bound queued as well as running operations, before retaining their data.
        let capacity = if cleanup {
            // Kernel RELEASE is not retried on EAGAIN. Give handle retirement
            // its own bounded lane, and wait instead of losing the handle.
            self.cleanup_capacity
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| std::io::Error::from_raw_os_error(libc::EIO))?
        } else {
            self.capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| std::io::Error::from_raw_os_error(libc::EAGAIN))?
        };
        let sealed = self.sealed.clone().read_owned().await;
        if *sealed && !cleanup {
            return Err(std::io::Error::from_raw_os_error(libc::EBUSY));
        }
        Ok(AdmittedMutation {
            _sealed: sealed,
            _capacity: capacity,
            uncertain: self.uncertain.clone(),
            completed: false,
        })
    }

    /// Wait for all admitted native operations and their recovery, then hold
    /// new mutations out while the caller scans the upper. Dropping the pause
    /// admits writes again unless the caller explicitly seals it.
    pub async fn pause(&self) -> std::io::Result<MutationPause> {
        let sealed = self.sealed.clone().write_owned().await;
        if self.uncertain.load(Ordering::Acquire) {
            return Err(std::io::Error::other(
                "native mutation or handle cleanup has an unknown outcome",
            ));
        }
        Ok(MutationPause { sealed })
    }

    /// Permanently reject new content mutations, after draining existing ones.
    /// Closing handles remains permitted so unmount can finish its cleanup.
    pub async fn seal(&self) -> std::io::Result<()> {
        self.pause().await?.seal();
        Ok(())
    }

    pub fn is_uncertain(&self) -> bool {
        self.uncertain.load(Ordering::Acquire)
    }

    pub(crate) fn mark_uncertain(&self) {
        self.uncertain.store(true, Ordering::Release);
    }
}

pub struct MutationPause {
    sealed: OwnedRwLockWriteGuard<bool>,
}

impl MutationPause {
    pub fn seal(&mut self) {
        *self.sealed = true;
    }
}

pub(crate) struct AdmittedMutation {
    _sealed: OwnedRwLockReadGuard<bool>,
    _capacity: OwnedSemaphorePermit,
    uncertain: Arc<AtomicBool>,
    completed: bool,
}

impl AdmittedMutation {
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for AdmittedMutation {
    fn drop(&mut self) {
        if !self.completed {
            // A panic or failed orphan-handle cleanup is not proof of a clean
            // upper, even though the task no longer holds the lock.
            self.uncertain.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn admission_capacity_and_seal_are_observable() {
        let fence = MutationFence::new(1);
        let admitted = fence.admit(false).await.unwrap();
        assert_eq!(
            fence.admit(false).await.err().unwrap().raw_os_error(),
            Some(libc::EAGAIN)
        );
        fence.admit(true).await.unwrap().complete();
        admitted.complete();
        fence.seal().await.unwrap();
        assert_eq!(
            fence.admit(false).await.err().unwrap().raw_os_error(),
            Some(libc::EBUSY)
        );
        fence.admit(true).await.unwrap().complete();
        assert!(!fence.is_uncertain());
    }

    #[tokio::test]
    async fn pause_drains_operations_and_reopens_after_rejected_destroy() {
        let fence = MutationFence::new(2);
        let admitted = fence.admit(false).await.unwrap();
        let pause = fence.pause();
        tokio::pin!(pause);
        assert!(futures::poll!(pause.as_mut()).is_pending());
        admitted.complete();
        let pause = pause.await.unwrap();
        let next = fence.admit(false);
        tokio::pin!(next);
        assert!(futures::poll!(next.as_mut()).is_pending());
        drop(pause);
        next.await.unwrap().complete();
        assert!(!fence.is_uncertain());
    }

    #[tokio::test]
    async fn incomplete_native_outcome_never_becomes_a_clean_pause() {
        let fence = MutationFence::new(1);
        drop(fence.admit(false).await.unwrap());
        assert!(fence.is_uncertain());
        assert!(fence.pause().await.is_err());
        assert!(fence.seal().await.is_err());
        // Closing a handle cannot retrospectively certify another operation.
        fence.admit(true).await.unwrap().complete();
        assert!(fence.pause().await.is_err());
    }
}
