//! Retain native mutation futures through request cancellation.

use std::{ffi::OsStr, future::Future, sync::Arc};

use asyncfuse::{
    notify::Notify,
    raw::{reply::*, Filesystem, Request},
    Inode, Result, SetAttr,
};
use bytes::Bytes;
use futures::Stream;
use tokio::sync::oneshot;

use super::mutation_fence::{AdmittedMutation, MutationFence};

/// The mounted filesystem and its control-plane owner share this exact fence.
/// Reads delegate directly; at most 64 mutation futures retain request data.
pub struct FencedFilesystem<FS> {
    inner: Arc<FS>,
    fence: MutationFence,
}

impl<FS> Clone for FencedFilesystem<FS> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            fence: self.fence.clone(),
        }
    }
}

impl<FS> FencedFilesystem<FS> {
    pub fn new(inner: FS) -> Self {
        Self {
            inner: Arc::new(inner),
            fence: MutationFence::new(64),
        }
    }

    pub fn fence(&self) -> &MutationFence {
        &self.fence
    }

    pub(crate) fn inner(&self) -> &Arc<FS> {
        &self.inner
    }
}

// The reply may be canceled after send succeeds but before receive is polled.
// Keep the admission and orphan-handle cleanup in the delivered value itself.
type FinishDelivery<T> = Box<dyn FnOnce(Option<Result<T>>) + Send>;

struct Delivered<T> {
    result: Option<Result<T>>,
    finish: Option<FinishDelivery<T>>,
}

impl<T> Delivered<T> {
    fn take(mut self) -> Result<T> {
        let result = self.result.take().expect("native result already taken");
        self.finish.take().expect("native completion already taken")(None);
        result
    }
}

impl<T> Drop for Delivered<T> {
    fn drop(&mut self) {
        if let Some(finish) = self.finish.take() {
            finish(self.result.take());
        }
    }
}

impl<FS: Filesystem + Send + Sync + 'static> FencedFilesystem<FS> {
    async fn run<T, F, Fut>(&self, admission: AdmittedMutation, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<FS>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.run_with_cleanup(admission, operation, |_, _| async { Ok(()) })
            .await
    }

    async fn run_with_cleanup<T, F, Fut, C, CFut>(
        &self,
        admission: AdmittedMutation,
        operation: F,
        cleanup: C,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<FS>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        C: FnOnce(Arc<FS>, Result<T>) -> CFut + Send + 'static,
        CFut: Future<Output = Result<()>> + Send + 'static,
    {
        let inner = self.inner.clone();
        let (send, receive) = oneshot::channel();
        // Dropping the caller does not abort this task or release its fence.
        tokio::spawn(async move {
            let result = operation(inner.clone()).await;
            let delivered = Delivered {
                result: Some(result),
                finish: Some(Box::new(move |orphan| {
                    if let Some(result) = orphan {
                        tokio::spawn(async move {
                            match cleanup(inner, result).await {
                                Ok(()) => admission.complete(),
                                Err(error) => tracing::error!(
                                    ?error,
                                    "orphan FUSE handle cleanup failed; mutation state is unknown"
                                ),
                            }
                        });
                    } else {
                        admission.complete();
                    }
                })),
            };
            let _ = send.send(delivered);
        });
        let delivered = receive
            .await
            .map_err(|_| asyncfuse::Errno::from(libc::EIO))?;
        delivered.take()
    }
}

fn native_error(error: std::io::Error) -> asyncfuse::Errno {
    error.raw_os_error().unwrap_or(libc::EIO).into()
}

impl<FS: Filesystem + Send + Sync + 'static> Filesystem for FencedFilesystem<FS> {
    async fn init(&self, req: Request) -> Result<ReplyInit> {
        self.inner.init(req).await
    }

    async fn init_with_notify(&self, req: Request, notify: Notify) -> Result<ReplyInit> {
        self.inner.init_with_notify(req, notify).await
    }

    async fn destroy(&self, req: Request) {
        match self.fence.pause().await {
            Ok(mut pause) => {
                pause.seal();
                self.inner().destroy(req).await;
            }
            Err(error) => {
                tracing::error!(%error, "native destroy refused an unknown mutation outcome")
            }
        }
    }

    async fn lookup(&self, req: Request, parent: Inode, name: &OsStr) -> Result<ReplyEntry> {
        self.inner.lookup(req, parent, name).await
    }

    async fn forget(&self, req: Request, inode: Inode, nlookup: u64) {
        self.inner.forget(req, inode, nlookup).await;
    }

    async fn getattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
    ) -> Result<ReplyAttr> {
        self.inner.getattr(req, inode, fh, flags).await
    }

    async fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        self.run(admission, move |inner| async move {
            inner.setattr(req, inode, fh, set_attr).await
        })
        .await
    }

    async fn readlink(&self, req: Request, inode: Inode) -> Result<ReplyData> {
        self.inner.readlink(req, inode).await
    }

    async fn symlink(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        link: &OsStr,
    ) -> Result<ReplyEntry> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        let link = link.to_owned();
        self.run(admission, move |inner| async move {
            inner.symlink(req, parent, &name, &link).await
        })
        .await
    }

    async fn mknod(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> Result<ReplyEntry> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        self.run(admission, move |inner| async move {
            inner.mknod(req, parent, &name, mode, rdev).await
        })
        .await
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> Result<ReplyEntry> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        self.run(admission, move |inner| async move {
            inner.mkdir(req, parent, &name, mode, umask).await
        })
        .await
    }

    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        self.run(admission, move |inner| async move {
            inner.unlink(req, parent, &name).await
        })
        .await
    }

    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        self.run(admission, move |inner| async move {
            inner.rmdir(req, parent, &name).await
        })
        .await
    }

    async fn rename(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        let new_name = new_name.to_owned();
        self.run(admission, move |inner| async move {
            inner
                .rename(req, parent, &name, new_parent, &new_name)
                .await
        })
        .await
    }

    async fn link(
        &self,
        req: Request,
        inode: Inode,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<ReplyEntry> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let new_name = new_name.to_owned();
        self.run(admission, move |inner| async move {
            inner.link(req, inode, new_parent, &new_name).await
        })
        .await
    }

    async fn open(&self, req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        self.run_with_cleanup(
            admission,
            move |inner| async move { inner.open(req, inode, flags).await },
            move |inner, result| async move {
                match result {
                    Ok(reply) => inner.release(req, inode, reply.fh, flags, 0, false).await,
                    Err(_) => Ok(()),
                }
            },
        )
        .await
    }

    async fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        self.inner.read(req, inode, fh, offset, size).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn write(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        data: &[u8],
        write_flags: u32,
        flags: u32,
    ) -> Result<ReplyWrite> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let data = data.to_owned();
        self.run(admission, move |inner| async move {
            inner
                .write(req, inode, fh, offset, &data, write_flags, flags)
                .await
        })
        .await
    }

    async fn statfs(&self, req: Request, inode: Inode) -> Result<ReplyStatFs> {
        self.inner.statfs(req, inode).await
    }

    async fn release(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> Result<()> {
        let admission = self.fence.admit(true).await.map_err(native_error)?;
        self.run(admission, move |inner| async move {
            inner
                .release(req, inode, fh, flags, lock_owner, flush)
                .await
        })
        .await
    }

    async fn fsync(&self, req: Request, inode: Inode, fh: u64, datasync: bool) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        self.run(admission, move |inner| async move {
            inner.fsync(req, inode, fh, datasync).await
        })
        .await
    }

    async fn setxattr(
        &self,
        req: Request,
        inode: Inode,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        let value = value.to_owned();
        self.run(admission, move |inner| async move {
            inner
                .setxattr(req, inode, &name, &value, flags, position)
                .await
        })
        .await
    }

    async fn getxattr(
        &self,
        req: Request,
        inode: Inode,
        name: &OsStr,
        size: u32,
    ) -> Result<ReplyXAttr> {
        self.inner.getxattr(req, inode, name, size).await
    }

    async fn listxattr(&self, req: Request, inode: Inode, size: u32) -> Result<ReplyXAttr> {
        self.inner.listxattr(req, inode, size).await
    }

    async fn removexattr(&self, req: Request, inode: Inode, name: &OsStr) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        self.run(admission, move |inner| async move {
            inner.removexattr(req, inode, &name).await
        })
        .await
    }

    async fn flush(&self, req: Request, inode: Inode, fh: u64, lock_owner: u64) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        self.run(admission, move |inner| async move {
            inner.flush(req, inode, fh, lock_owner).await
        })
        .await
    }

    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        self.run_with_cleanup(
            admission,
            move |inner| async move { inner.opendir(req, inode, flags).await },
            move |inner, result| async move {
                match result {
                    Ok(reply) => inner.releasedir(req, inode, reply.fh, flags).await,
                    Err(_) => Ok(()),
                }
            },
        )
        .await
    }

    async fn readdir<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<impl Stream<Item = Result<DirectoryEntry>> + Send + 'a>> {
        self.inner.readdir(req, parent, fh, offset).await
    }

    async fn releasedir(&self, req: Request, inode: Inode, fh: u64, flags: u32) -> Result<()> {
        let admission = self.fence.admit(true).await.map_err(native_error)?;
        self.run(admission, move |inner| async move {
            inner.releasedir(req, inode, fh, flags).await
        })
        .await
    }

    async fn fsyncdir(&self, req: Request, inode: Inode, fh: u64, datasync: bool) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        self.run(admission, move |inner| async move {
            inner.fsyncdir(req, inode, fh, datasync).await
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn getlk(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        r#type: u32,
        pid: u32,
    ) -> Result<ReplyLock> {
        self.inner
            .getlk(req, inode, fh, lock_owner, start, end, r#type, pid)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn setlk(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        r#type: u32,
        pid: u32,
        block: bool,
    ) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        self.run(admission, move |inner| async move {
            inner
                .setlk(req, inode, fh, lock_owner, start, end, r#type, pid, block)
                .await
        })
        .await
    }

    async fn access(&self, req: Request, inode: Inode, mask: u32) -> Result<()> {
        self.inner.access(req, inode, mask).await
    }

    async fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        self.run_with_cleanup(
            admission,
            move |inner| async move { inner.create(req, parent, &name, mode, flags).await },
            move |inner, result| async move {
                match result {
                    Ok(reply) => {
                        inner
                            .release(req, reply.attr.ino, reply.fh, flags, 0, false)
                            .await
                    }
                    Err(_) => Ok(()),
                }
            },
        )
        .await
    }

    async fn interrupt(&self, req: Request, unique: u64) -> Result<()> {
        self.inner.interrupt(req, unique).await
    }

    async fn bmap(
        &self,
        req: Request,
        inode: Inode,
        blocksize: u32,
        idx: u64,
    ) -> Result<ReplyBmap> {
        self.inner.bmap(req, inode, blocksize, idx).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn ioctl(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
        cmd: u32,
        arg: u64,
        in_data: &[u8],
        out_size: u32,
    ) -> Result<ReplyIoctl> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let in_data = in_data.to_owned();
        self.run(admission, move |inner| async move {
            inner
                .ioctl(req, inode, fh, flags, cmd, arg, &in_data, out_size)
                .await
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn poll(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        kh: Option<u64>,
        flags: u32,
        events: u32,
        notify: &Notify,
    ) -> Result<ReplyPoll> {
        self.inner
            .poll(req, inode, fh, kh, flags, events, notify)
            .await
    }

    async fn notify_reply(
        &self,
        req: Request,
        inode: Inode,
        offset: u64,
        data: Bytes,
    ) -> Result<()> {
        self.inner.notify_reply(req, inode, offset, data).await
    }

    async fn batch_forget(&self, req: Request, inodes: &[(Inode, u64)]) {
        self.inner.batch_forget(req, inodes).await;
    }

    async fn fallocate(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        self.run(admission, move |inner| async move {
            inner.fallocate(req, inode, fh, offset, length, mode).await
        })
        .await
    }

    async fn readdirplus<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: u64,
        lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<impl Stream<Item = Result<DirectoryEntryPlus>> + Send + 'a>>
    {
        self.inner
            .readdirplus(req, parent, fh, offset, lock_owner)
            .await
    }

    async fn rename2(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
        flags: u32,
    ) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        let new_name = new_name.to_owned();
        self.run(admission, move |inner| async move {
            inner
                .rename2(req, parent, &name, new_parent, &new_name, flags)
                .await
        })
        .await
    }

    async fn lseek(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        whence: u32,
    ) -> Result<ReplyLSeek> {
        self.inner.lseek(req, inode, fh, offset, whence).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn copy_file_range(
        &self,
        req: Request,
        inode: Inode,
        fh_in: u64,
        off_in: u64,
        inode_out: Inode,
        fh_out: u64,
        off_out: u64,
        length: u64,
        flags: u64,
    ) -> Result<ReplyCopyFileRange> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        self.run(admission, move |inner| async move {
            inner
                .copy_file_range(
                    req, inode, fh_in, off_in, inode_out, fh_out, off_out, length, flags,
                )
                .await
        })
        .await
    }

    #[cfg(target_os = "macos")]
    async fn setvolname(&self, req: Request, name: &OsStr) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let name = name.to_owned();
        self.run(admission, move |inner| async move {
            inner.setvolname(req, &name).await
        })
        .await
    }

    #[cfg(target_os = "macos")]
    async fn getxtimes(&self, req: Request, inode: Inode) -> Result<ReplyXTimes> {
        self.inner.getxtimes(req, inode).await
    }

    #[cfg(target_os = "macos")]
    async fn exchange(
        &self,
        req: Request,
        olddir: Inode,
        oldname: &OsStr,
        newdir: Inode,
        newname: &OsStr,
        options: u64,
    ) -> Result<()> {
        let admission = self.fence.admit(false).await.map_err(native_error)?;
        let oldname = oldname.to_owned();
        let newname = newname.to_owned();
        self.run(admission, move |inner| async move {
            inner
                .exchange(req, olddir, &oldname, newdir, &newname, options)
                .await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Mutex,
        },
        time::Duration,
    };

    use tokio::sync::Semaphore;

    use super::*;

    struct ControlledFs {
        started: Semaphore,
        proceed: Semaphore,
        release_started: Semaphore,
        release_proceed: Semaphore,
        release_count: AtomicUsize,
        release_fails: AtomicBool,
        panic_write: AtomicBool,
        content: Mutex<Vec<u8>>,
        destroyed: AtomicBool,
    }

    impl ControlledFs {
        fn new() -> Self {
            Self {
                started: Semaphore::new(0),
                proceed: Semaphore::new(0),
                release_started: Semaphore::new(0),
                release_proceed: Semaphore::new(0),
                release_count: AtomicUsize::new(0),
                release_fails: AtomicBool::new(false),
                panic_write: AtomicBool::new(false),
                content: Mutex::new(b"base".to_vec()),
                destroyed: AtomicBool::new(false),
            }
        }

        async fn wait_started(&self) {
            tokio::time::timeout(Duration::from_secs(5), self.started.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }

        async fn hold(&self) {
            self.started.add_permits(1);
            self.proceed.acquire().await.unwrap().forget();
        }
    }

    impl Filesystem for ControlledFs {
        async fn init(&self, _: Request) -> Result<ReplyInit> {
            Err(libc::ENOSYS.into())
        }

        async fn destroy(&self, _: Request) {
            self.destroyed.store(true, Ordering::Release);
        }

        async fn read(&self, _: Request, _: Inode, _: u64, _: u64, _: u32) -> Result<ReplyData> {
            Ok(ReplyData {
                data: Bytes::copy_from_slice(&self.content.lock().unwrap()),
            })
        }

        #[allow(clippy::too_many_arguments)]
        async fn write(
            &self,
            _: Request,
            _: Inode,
            _: u64,
            _: u64,
            data: &[u8],
            _: u32,
            _: u32,
        ) -> Result<ReplyWrite> {
            self.hold().await;
            assert!(
                !self.panic_write.load(Ordering::Acquire),
                "injected native panic"
            );
            if data == b"ENOSPC" {
                return Err(libc::ENOSPC.into());
            }
            *self.content.lock().unwrap() = data.to_vec();
            Ok(ReplyWrite {
                written: data.len() as u32,
            })
        }

        async fn open(&self, _: Request, _: Inode, flags: u32) -> Result<ReplyOpen> {
            self.hold().await;
            if flags & libc::O_TRUNC as u32 != 0 {
                self.content.lock().unwrap().clear();
            }
            Ok(ReplyOpen { fh: 99, flags: 0 })
        }

        async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
            self.open(req, inode, flags).await
        }

        async fn create(
            &self,
            _: Request,
            _: Inode,
            _: &OsStr,
            _: u32,
            _: u32,
        ) -> Result<ReplyCreated> {
            self.hold().await;
            *self.content.lock().unwrap() = b"created".to_vec();
            Ok(ReplyCreated {
                ttl: Duration::ZERO,
                attr: crate::util::file_attr::make_file_attr(
                    42,
                    7,
                    1,
                    asyncfuse::Timestamp::new(0, 0),
                    asyncfuse::Timestamp::new(0, 0),
                    asyncfuse::Timestamp::new(0, 0),
                    asyncfuse::FileType::RegularFile,
                    0o644,
                    1,
                    0,
                    0,
                    0,
                    4096,
                ),
                generation: 1,
                fh: 99,
                flags: 0,
            })
        }

        async fn release(
            &self,
            _: Request,
            inode: Inode,
            fh: u64,
            _: u32,
            _: u64,
            _: bool,
        ) -> Result<()> {
            assert!([2, 42].contains(&inode));
            assert_eq!(fh, 99);
            self.release_count.fetch_add(1, Ordering::AcqRel);
            self.release_started.add_permits(1);
            self.release_proceed.acquire().await.unwrap().forget();
            if self.release_fails.load(Ordering::Acquire) {
                Err(libc::EIO.into())
            } else {
                Ok(())
            }
        }

        async fn releasedir(&self, req: Request, inode: Inode, fh: u64, flags: u32) -> Result<()> {
            self.release(req, inode, fh, flags, 0, false).await
        }

        #[allow(clippy::too_many_arguments)]
        async fn getlk(
            &self,
            _: Request,
            _: Inode,
            _: u64,
            _: u64,
            _: u64,
            _: u64,
            _: u32,
            _: u32,
        ) -> Result<ReplyLock> {
            Err(libc::ENOSYS.into())
        }

        #[allow(clippy::too_many_arguments)]
        async fn setlk(
            &self,
            _: Request,
            _: Inode,
            _: u64,
            _: u64,
            _: u64,
            _: u64,
            _: u32,
            _: u32,
            _: bool,
        ) -> Result<()> {
            Err(libc::ENOSYS.into())
        }
    }

    async fn wait_pause(fs: &FencedFilesystem<ControlledFs>) {
        drop(
            tokio::time::timeout(Duration::from_secs(5), fs.fence.pause())
                .await
                .unwrap()
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn cancelled_write_retains_actual_operation_and_old_handle_fence() {
        let fs = FencedFilesystem::new(ControlledFs::new());
        let other = fs.clone();
        let caller = tokio::spawn(async move {
            other
                .write(Request::default(), 2, 99, 0, b"dirty", 0, 0)
                .await
        });
        fs.inner.wait_started().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let pause = fs.fence.pause();
        tokio::pin!(pause);
        assert!(futures::poll!(pause.as_mut()).is_pending());
        assert_eq!(
            fs.read(Request::default(), 2, 99, 0, 4)
                .await
                .unwrap()
                .data
                .as_ref(),
            b"base"
        );
        fs.inner.proceed.add_permits(1);
        let mut pause = tokio::time::timeout(Duration::from_secs(5), pause)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*fs.inner.content.lock().unwrap(), b"dirty");
        let write = fs.write(Request::default(), 2, 99, 0, b"late", 0, 0);
        tokio::pin!(write);
        assert!(futures::poll!(write.as_mut()).is_pending());
        pause.seal();
        drop(pause);
        assert_eq!(write.await.unwrap_err(), libc::EBUSY.into());
        assert_eq!(
            fs.read(Request::default(), 2, 99, 0, 5)
                .await
                .unwrap()
                .data
                .as_ref(),
            b"dirty"
        );
    }

    #[tokio::test]
    async fn cancelled_truncating_open_drains_its_orphan_release() {
        let fs = FencedFilesystem::new(ControlledFs::new());
        let other = fs.clone();
        let caller = tokio::spawn(async move {
            other
                .open(Request::default(), 2, libc::O_TRUNC as u32)
                .await
        });
        fs.inner.wait_started().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        fs.inner.proceed.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), fs.inner.release_started.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let pause = fs.fence.pause();
        tokio::pin!(pause);
        assert!(futures::poll!(pause.as_mut()).is_pending());
        assert!(fs.inner.content.lock().unwrap().is_empty());
        fs.inner.release_proceed.add_permits(1);
        drop(
            tokio::time::timeout(Duration::from_secs(5), pause)
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(fs.inner.release_count.load(Ordering::Acquire), 1);
        assert!(!fs.fence.is_uncertain());
    }

    #[tokio::test]
    async fn cancelled_create_and_directory_open_release_the_correct_handle() {
        for directory in [false, true] {
            let fs = FencedFilesystem::new(ControlledFs::new());
            let other = fs.clone();
            let caller = tokio::spawn(async move {
                if directory {
                    other.opendir(Request::default(), 2, 0).await.map(|_| ())
                } else {
                    other
                        .create(Request::default(), 1, OsStr::new("new"), 0o644, 0)
                        .await
                        .map(|_| ())
                }
            });
            fs.inner.wait_started().await;
            caller.abort();
            assert!(caller.await.unwrap_err().is_cancelled());
            fs.inner.proceed.add_permits(1);
            fs.inner.release_proceed.add_permits(1);
            wait_pause(&fs).await;
            assert_eq!(fs.inner.release_count.load(Ordering::Acquire), 1);
        }
    }

    #[tokio::test]
    async fn orphan_release_failure_and_native_panic_remain_unknown() {
        let fs = FencedFilesystem::new(ControlledFs::new());
        fs.inner.release_fails.store(true, Ordering::Release);
        let other = fs.clone();
        let caller = tokio::spawn(async move { other.open(Request::default(), 2, 0).await });
        fs.inner.wait_started().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        fs.inner.proceed.add_permits(1);
        fs.inner.release_proceed.add_permits(1);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), fs.fence.pause())
                .await
                .unwrap()
                .is_err()
        );
        assert!(fs.fence.is_uncertain());
        fs.destroy(Request::default()).await;
        assert!(!fs.inner.destroyed.load(Ordering::Acquire));

        let fs = FencedFilesystem::new(ControlledFs::new());
        fs.inner.panic_write.store(true, Ordering::Release);
        fs.inner.proceed.add_permits(1);
        assert_eq!(
            fs.write(Request::default(), 2, 99, 0, b"panic", 0, 0)
                .await
                .unwrap_err(),
            libc::EIO.into()
        );
        assert!(fs.fence.pause().await.is_err());
    }

    #[tokio::test]
    async fn mutation_budget_bounds_queued_data_without_serializing_writes() {
        let fs = FencedFilesystem::new(ControlledFs::new());
        let mut callers = Vec::new();
        for _ in 0..64 {
            let other = fs.clone();
            callers.push(tokio::spawn(async move {
                other
                    .write(Request::default(), 2, 99, 0, b"write", 0, 0)
                    .await
            }));
            fs.inner.wait_started().await;
        }
        assert_eq!(
            fs.write(Request::default(), 2, 99, 0, b"excess", 0, 0)
                .await
                .unwrap_err(),
            libc::EAGAIN.into()
        );
        fs.inner.proceed.add_permits(64);
        for caller in callers {
            assert_eq!(caller.await.unwrap().unwrap().written, 5);
        }
        wait_pause(&fs).await;
        assert!(!fs.fence.is_uncertain());
    }

    #[tokio::test]
    async fn native_error_and_successful_handle_are_not_reclassified() {
        let fs = FencedFilesystem::new(ControlledFs::new());
        fs.inner.proceed.add_permits(2);
        assert_eq!(
            fs.write(Request::default(), 2, 99, 0, b"ENOSPC", 0, 0)
                .await
                .unwrap_err(),
            libc::ENOSPC.into()
        );
        let open = fs.open(Request::default(), 2, 0).await.unwrap();
        assert_eq!(open.fh, 99);
        wait_pause(&fs).await;
        assert_eq!(fs.inner.release_count.load(Ordering::Acquire), 0);
        fs.fence.seal().await.unwrap();
        fs.inner.release_proceed.add_permits(1);
        fs.release(Request::default(), 2, open.fh, 0, 0, false)
            .await
            .unwrap();
        assert_eq!(fs.inner.release_count.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn all_content_mutation_entrypoints_reject_a_sealed_mount() {
        let fs = FencedFilesystem::new(ControlledFs::new());
        fs.fence.seal().await.unwrap();
        let req = Request::default();
        let name = OsStr::new("file");
        let mut errors = vec![
            fs.setattr(
                req,
                2,
                Some(99),
                SetAttr {
                    size: Some(0),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err(),
            fs.symlink(req, 1, name, name).await.unwrap_err(),
            fs.mknod(req, 1, name, 0, 0).await.unwrap_err(),
            fs.mkdir(req, 1, name, 0, 0).await.unwrap_err(),
            fs.unlink(req, 1, name).await.unwrap_err(),
            fs.rmdir(req, 1, name).await.unwrap_err(),
            fs.rename(req, 1, name, 1, name).await.unwrap_err(),
            fs.rename2(req, 1, name, 1, name, 0).await.unwrap_err(),
            fs.link(req, 2, 1, name).await.unwrap_err(),
            fs.open(req, 2, 0).await.unwrap_err(),
            fs.write(req, 2, 99, 0, b"data", 0, 0).await.unwrap_err(),
            fs.create(req, 1, name, 0, 0).await.unwrap_err(),
            fs.setxattr(req, 2, name, b"value", 0, 0).await.unwrap_err(),
            fs.removexattr(req, 2, name).await.unwrap_err(),
            fs.fallocate(req, 2, 99, 0, 1, 0).await.unwrap_err(),
            fs.copy_file_range(req, 2, 99, 0, 2, 99, 0, 1, 0)
                .await
                .unwrap_err(),
            fs.ioctl(req, 2, 99, 0, 0, 0, b"", 0).await.unwrap_err(),
            fs.flush(req, 2, 99, 0).await.unwrap_err(),
            fs.fsync(req, 2, 99, false).await.unwrap_err(),
            fs.fsyncdir(req, 1, 99, false).await.unwrap_err(),
            fs.setlk(req, 2, 99, 0, 0, 0, 0, 0, false)
                .await
                .unwrap_err(),
            fs.opendir(req, 1, 0).await.unwrap_err(),
        ];
        #[cfg(target_os = "macos")]
        errors.extend([
            fs.setvolname(req, name).await.unwrap_err(),
            fs.exchange(req, 1, name, 1, name, 0).await.unwrap_err(),
        ]);
        assert!(errors.drain(..).all(|error| error == libc::EBUSY.into()));
        assert!(!fs.fence.is_uncertain());
    }
}
