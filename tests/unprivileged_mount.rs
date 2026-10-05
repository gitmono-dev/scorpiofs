#![cfg(target_os = "linux")]

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use asyncfuse::raw::prelude::{Filesystem, ReplyInit, ReplyLock, Request};
use scorpiofs::server::mount_filesystem;

struct InitProbe(Arc<AtomicBool>);

impl Filesystem for InitProbe {
    async fn init(&self, _request: Request) -> asyncfuse::Result<ReplyInit> {
        self.0.store(true, Ordering::SeqCst);
        Ok(ReplyInit::default())
    }

    async fn destroy(&self, _request: Request) {}

    async fn getlk(
        &self,
        _request: Request,
        _inode: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
    ) -> asyncfuse::Result<ReplyLock> {
        Err(libc::ENOSYS.into())
    }

    async fn setlk(
        &self,
        _request: Request,
        _inode: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        _block: bool,
    ) -> asyncfuse::Result<()> {
        Err(libc::ENOSYS.into())
    }
}

fn is_mounted(path: &std::path::Path) -> std::io::Result<bool> {
    Ok(std::fs::read_to_string("/proc/self/mountinfo")?
        .lines()
        .any(|line| line.split_whitespace().nth(4) == path.to_str()))
}

#[tokio::test]
#[ignore = "requires a non-root Linux user, /dev/fuse and configured FUSE helper"]
async fn ordinary_user_mount_negotiates_init_and_unmounts() {
    assert_ne!(
        unsafe { libc::geteuid() },
        0,
        "run this test as an ordinary user"
    );
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let effective = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:\t"))
        .unwrap();
    let capabilities = u64::from_str_radix(effective, 16).unwrap();
    assert_eq!(capabilities & (1 << 21), 0, "test must lack CAP_SYS_ADMIN");
    let directory = tempfile::tempdir().unwrap();
    let initialized = Arc::new(AtomicBool::new(false));
    let handle = mount_filesystem(InitProbe(initialized.clone()), directory.path().as_os_str())
        .await
        .expect("ordinary user must mount without CAP_SYS_ADMIN");
    let mounted = is_mounted(directory.path());
    let init_result = tokio::time::timeout(Duration::from_secs(5), async {
        while !initialized.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let unmount_result = tokio::time::timeout(Duration::from_secs(10), handle.unmount()).await;
    // Keep cleanup ahead of assertions even when INIT, mountinfo, or the
    // normal handle unmount fails. This exact temporary path is owned here.
    if !matches!(is_mounted(directory.path()), Ok(false)) {
        let mut cleanup = tokio::process::Command::new("fusermount3");
        cleanup.arg("-u").arg(directory.path()).kill_on_drop(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), cleanup.output()).await;
    }
    assert!(!is_mounted(directory.path()).expect("must verify final mountinfo"));
    unmount_result
        .expect("unmount must finish within its deadline")
        .expect("owned mount must unmount");
    assert!(mounted.expect("must read mounted path from mountinfo"));
    init_result.expect("kernel must negotiate INIT with this filesystem");
}
