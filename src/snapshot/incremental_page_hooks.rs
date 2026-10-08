//! Pause the actual admitted page writer after fsync, before publication.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{mpsc, Mutex, OnceLock},
};

use super::{SnapshotError, SnapshotErrorCode};

struct Hook {
    id: uuid::Uuid,
    entered: tokio::sync::oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
    fail: bool,
}

fn hooks() -> &'static Mutex<BTreeMap<PathBuf, Hook>> {
    static HOOKS: OnceLock<Mutex<BTreeMap<PathBuf, Hook>>> = OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub(crate) struct Barrier {
    path: PathBuf,
    id: uuid::Uuid,
    entered: Option<tokio::sync::oneshot::Receiver<()>>,
    release: Option<mpsc::Sender<()>>,
}

impl Barrier {
    pub(crate) async fn entered(&mut self) {
        self.entered.take().unwrap().await.unwrap();
    }

    pub(crate) fn release(mut self) {
        let _ = self.release.take().unwrap().send(());
    }
}

impl Drop for Barrier {
    fn drop(&mut self) {
        {
            let mut hooks = hooks().lock().unwrap();
            if hooks.get(&self.path).is_some_and(|hook| hook.id == self.id) {
                hooks.remove(&self.path);
            }
        }
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

pub(crate) fn install(path: &Path, fail: bool) -> Barrier {
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release) = mpsc::channel();
    let id = uuid::Uuid::new_v4();
    {
        let mut hooks = hooks().lock().unwrap();
        assert!(!hooks.contains_key(path));
        hooks.insert(
            path.to_path_buf(),
            Hook {
                id,
                entered,
                release,
                fail,
            },
        );
    }
    Barrier {
        path: path.to_path_buf(),
        id,
        entered: Some(entered_rx),
        release: Some(release_tx),
    }
}

pub(super) fn before_rename(path: &Path) -> Result<(), SnapshotError> {
    let hook = hooks().lock().unwrap().remove(path);
    if let Some(hook) = hook {
        let _ = hook.entered.send(());
        let _ = hook.release.recv();
        if hook.fail {
            return Err(SnapshotError::new(
                SnapshotErrorCode::Internal,
                "injected page publication failure",
            ));
        }
    }
    Ok(())
}
