use std::{ffi::OsStr, sync::Arc};

use asyncfuse::{raw::prelude::*, Inode, Result, SetAttr};

use super::{
    profile::{base_event, observe_result, observe_unit, FuseProfileContext},
    MegaFuse,
};

/// A FUSE decorator that records operation latency and request characteristics.
///
/// The wrapped filesystem remains unchanged. Profiling work on the FUSE path is
/// limited to timing, filling a stack event, and pushing it to a bounded queue.
pub struct LogFuse<F> {
    inner: F,
    profile: Arc<FuseProfileContext>,
}

impl<F> LogFuse<F> {
    pub fn new(inner: F, profile: Arc<FuseProfileContext>) -> Self {
        Self { inner, profile }
    }

    pub fn into_inner(self) -> F {
        self.inner
    }
}

fn apply_metadata(
    mut event: super::profile::FuseProfileEvent,
    metadata: impl FnOnce(&mut super::profile::FuseProfileEvent),
) -> super::profile::FuseProfileEvent {
    metadata(&mut event);
    event
}

macro_rules! profile_result {
    ($self:ident, $op:ident, $req:ident, $metadata:expr, $call:expr, $inspect:expr) => {{
        let request = $req;
        observe_result(
            &$self.profile,
            stringify!($op),
            request,
            || apply_metadata(base_event(stringify!($op), request, None), $metadata),
            async { $call },
            $inspect,
        )
        .await
    }};
}

macro_rules! profile_unit {
    ($self:ident, $op:ident, $req:ident, $metadata:expr, $call:expr) => {{
        let request = $req;
        observe_unit(
            &$self.profile,
            stringify!($op),
            request,
            || apply_metadata(base_event(stringify!($op), request, None), $metadata),
            async { $call },
        )
        .await
    }};
}

impl LogFuse<MegaFuse> {
    pub fn megafuse(inner: MegaFuse, profile: Arc<FuseProfileContext>) -> Self {
        Self::new(inner, profile)
    }
}

impl<F: Filesystem + Sync> Filesystem for LogFuse<F> {
    async fn init(&self, req: Request) -> Result<ReplyInit> {
        profile_result!(
            self,
            init,
            req,
            |_| {},
            self.inner.init(req).await,
            |_, _| {}
        )
    }

    async fn destroy(&self, req: Request) {
        profile_unit!(self, destroy, req, |_| {}, self.inner.destroy(req).await)
    }

    async fn lookup(&self, req: Request, parent: Inode, name: &OsStr) -> Result<ReplyEntry> {
        profile_result!(
            self,
            lookup,
            req,
            |event| event.parent = Some(parent),
            self.inner.lookup(req, parent, name).await,
            |reply, event| event.inode = Some(reply.attr.ino)
        )
    }

    async fn forget(&self, req: Request, inode: Inode, nlookup: u64) {
        profile_unit!(
            self,
            forget,
            req,
            |event| {
                event.inode = Some(inode);
                event.requested_size = Some(nlookup);
            },
            self.inner.forget(req, inode, nlookup).await
        )
    }

    async fn getattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
    ) -> Result<ReplyAttr> {
        profile_result!(
            self,
            getattr,
            req,
            |event| {
                event.inode = Some(inode);
                event.requested_size = Some(u64::from(flags));
            },
            self.inner.getattr(req, inode, fh, flags).await,
            |reply, event| event.inode = Some(reply.attr.ino)
        )
    }

    async fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        profile_result!(
            self,
            setattr,
            req,
            |event| event.inode = Some(inode),
            self.inner.setattr(req, inode, fh, set_attr).await,
            |reply, event| event.inode = Some(reply.attr.ino)
        )
    }

    async fn readlink(&self, req: Request, inode: Inode) -> Result<ReplyData> {
        profile_result!(
            self,
            readlink,
            req,
            |event| event.inode = Some(inode),
            self.inner.readlink(req, inode).await,
            |reply, event| event.bytes = Some(reply.data.len() as u64)
        )
    }

    async fn symlink(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        link: &OsStr,
    ) -> Result<ReplyEntry> {
        profile_result!(
            self,
            symlink,
            req,
            |event| event.parent = Some(parent),
            self.inner.symlink(req, parent, name, link).await,
            |reply, event| event.inode = Some(reply.attr.ino)
        )
    }

    async fn mknod(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> Result<ReplyEntry> {
        profile_result!(
            self,
            mknod,
            req,
            |event| event.parent = Some(parent),
            self.inner.mknod(req, parent, name, mode, rdev).await,
            |reply, event| event.inode = Some(reply.attr.ino)
        )
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> Result<ReplyEntry> {
        profile_result!(
            self,
            mkdir,
            req,
            |event| event.parent = Some(parent),
            self.inner.mkdir(req, parent, name, mode, umask).await,
            |reply, event| event.inode = Some(reply.attr.ino)
        )
    }

    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> Result<()> {
        profile_result!(
            self,
            unlink,
            req,
            |event| event.parent = Some(parent),
            self.inner.unlink(req, parent, name).await,
            |_, _| {}
        )
    }

    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> Result<()> {
        profile_result!(
            self,
            rmdir,
            req,
            |event| event.parent = Some(parent),
            self.inner.rmdir(req, parent, name).await,
            |_, _| {}
        )
    }

    async fn rename(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<()> {
        profile_result!(
            self,
            rename,
            req,
            |event| event.parent = Some(parent),
            self.inner
                .rename(req, parent, name, new_parent, new_name)
                .await,
            |_, _| {}
        )
    }

    async fn link(
        &self,
        req: Request,
        inode: Inode,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> Result<ReplyEntry> {
        profile_result!(
            self,
            link,
            req,
            |event| {
                event.inode = Some(inode);
                event.parent = Some(new_parent);
            },
            self.inner.link(req, inode, new_parent, new_name).await,
            |reply, event| event.inode = Some(reply.attr.ino)
        )
    }

    async fn open(&self, req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
        profile_result!(
            self,
            open,
            req,
            |event| {
                event.inode = Some(inode);
                event.requested_size = Some(u64::from(flags));
            },
            self.inner.open(req, inode, flags).await,
            |reply, event| event.fh = Some(reply.fh)
        )
    }

    async fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        profile_result!(
            self,
            read,
            req,
            |event| {
                event.inode = Some(inode);
                event.fh = Some(fh);
                event.offset = Some(offset);
                event.requested_size = Some(u64::from(size));
            },
            self.inner.read(req, inode, fh, offset, size).await,
            |reply, event| event.bytes = Some(reply.data.len() as u64)
        )
    }

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
        profile_result!(
            self,
            write,
            req,
            |event| {
                event.inode = Some(inode);
                event.fh = Some(fh);
                event.offset = Some(offset);
                event.requested_size = Some(data.len() as u64);
            },
            self.inner
                .write(req, inode, fh, offset, data, write_flags, flags)
                .await,
            |reply, event| event.bytes = Some(u64::from(reply.written))
        )
    }

    async fn statfs(&self, req: Request, inode: Inode) -> Result<ReplyStatFs> {
        profile_result!(
            self,
            statfs,
            req,
            |event| event.inode = Some(inode),
            self.inner.statfs(req, inode).await,
            |_, _| {}
        )
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
        profile_result!(
            self,
            release,
            req,
            |event| {
                event.inode = Some(inode);
                event.fh = Some(fh);
                event.requested_size = Some(u64::from(flags));
            },
            self.inner
                .release(req, inode, fh, flags, lock_owner, flush)
                .await,
            |_, _| {}
        )
    }

    async fn fsync(&self, req: Request, inode: Inode, fh: u64, datasync: bool) -> Result<()> {
        profile_result!(
            self,
            fsync,
            req,
            |event| {
                event.inode = Some(inode);
                event.fh = Some(fh);
            },
            self.inner.fsync(req, inode, fh, datasync).await,
            |_, _| {}
        )
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
        profile_result!(
            self,
            setxattr,
            req,
            |event| {
                event.inode = Some(inode);
                event.requested_size = Some(value.len() as u64);
            },
            self.inner
                .setxattr(req, inode, name, value, flags, position)
                .await,
            |_, _| {}
        )
    }

    async fn getxattr(
        &self,
        req: Request,
        inode: Inode,
        name: &OsStr,
        size: u32,
    ) -> Result<ReplyXAttr> {
        profile_result!(
            self,
            getxattr,
            req,
            |event| {
                event.inode = Some(inode);
                event.requested_size = Some(u64::from(size));
            },
            self.inner.getxattr(req, inode, name, size).await,
            |reply, event| {
                event.bytes = Some(match reply {
                    ReplyXAttr::Size(size) => u64::from(*size),
                    ReplyXAttr::Data(data) => data.len() as u64,
                });
            }
        )
    }

    async fn listxattr(&self, req: Request, inode: Inode, size: u32) -> Result<ReplyXAttr> {
        profile_result!(
            self,
            listxattr,
            req,
            |event| {
                event.inode = Some(inode);
                event.requested_size = Some(u64::from(size));
            },
            self.inner.listxattr(req, inode, size).await,
            |reply, event| {
                event.bytes = Some(match reply {
                    ReplyXAttr::Size(size) => u64::from(*size),
                    ReplyXAttr::Data(data) => data.len() as u64,
                });
            }
        )
    }

    async fn removexattr(&self, req: Request, inode: Inode, name: &OsStr) -> Result<()> {
        profile_result!(
            self,
            removexattr,
            req,
            |event| event.inode = Some(inode),
            self.inner.removexattr(req, inode, name).await,
            |_, _| {}
        )
    }

    async fn flush(&self, req: Request, inode: Inode, fh: u64, lock_owner: u64) -> Result<()> {
        profile_result!(
            self,
            flush,
            req,
            |event| event.inode = Some(inode),
            self.inner.flush(req, inode, fh, lock_owner).await,
            |_, event| event.fh = Some(fh)
        )
    }

    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
        profile_result!(
            self,
            opendir,
            req,
            |event| {
                event.inode = Some(inode);
                event.requested_size = Some(u64::from(flags));
            },
            self.inner.opendir(req, inode, flags).await,
            |reply, event| event.fh = Some(reply.fh)
        )
    }

    async fn readdir<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<impl futures::Stream<Item = Result<DirectoryEntry>> + Send + 'a>>
    {
        profile_result!(
            self,
            readdir,
            req,
            |event| {
                event.parent = Some(parent);
                event.fh = Some(fh);
                event.offset = Some(offset as u64);
            },
            self.inner.readdir(req, parent, fh, offset).await,
            |_, _| {}
        )
    }

    async fn releasedir(&self, req: Request, inode: Inode, fh: u64, flags: u32) -> Result<()> {
        profile_result!(
            self,
            releasedir,
            req,
            |event| {
                event.inode = Some(inode);
                event.fh = Some(fh);
                event.requested_size = Some(u64::from(flags));
            },
            self.inner.releasedir(req, inode, fh, flags).await,
            |_, _| {}
        )
    }

    async fn fsyncdir(&self, req: Request, inode: Inode, fh: u64, datasync: bool) -> Result<()> {
        profile_result!(
            self,
            fsyncdir,
            req,
            |event| {
                event.inode = Some(inode);
                event.fh = Some(fh);
            },
            self.inner.fsyncdir(req, inode, fh, datasync).await,
            |_, _| {}
        )
    }

    async fn access(&self, req: Request, inode: Inode, mask: u32) -> Result<()> {
        profile_result!(
            self,
            access,
            req,
            |event| {
                event.inode = Some(inode);
                event.requested_size = Some(u64::from(mask));
            },
            self.inner.access(req, inode, mask).await,
            |_, _| {}
        )
    }

    async fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated> {
        profile_result!(
            self,
            create,
            req,
            |event| {
                event.parent = Some(parent);
                event.requested_size = Some(u64::from(flags));
            },
            self.inner.create(req, parent, name, mode, flags).await,
            |reply, event| {
                event.inode = Some(reply.attr.ino);
                event.fh = Some(reply.fh);
            }
        )
    }

    async fn interrupt(&self, req: Request, unique: u64) -> Result<()> {
        profile_result!(
            self,
            interrupt,
            req,
            |event| event.requested_size = Some(unique),
            self.inner.interrupt(req, unique).await,
            |_, _| {}
        )
    }

    async fn bmap(
        &self,
        req: Request,
        inode: Inode,
        blocksize: u32,
        idx: u64,
    ) -> Result<ReplyBmap> {
        profile_result!(
            self,
            bmap,
            req,
            |event| {
                event.inode = Some(inode);
                event.requested_size = Some(u64::from(blocksize));
                event.offset = Some(idx);
            },
            self.inner.bmap(req, inode, blocksize, idx).await,
            |_, _| {}
        )
    }

    async fn batch_forget(&self, req: Request, inodes: &[(u64, u64)]) {
        profile_unit!(
            self,
            batch_forget,
            req,
            |event| {
                event.entries = Some(inodes.len() as u64);
                event.requested_size = Some(inodes.iter().map(|(_, nlookup)| nlookup).sum());
            },
            self.inner.batch_forget(req, inodes).await
        )
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
        profile_result!(
            self,
            fallocate,
            req,
            |event| {
                event.inode = Some(inode);
                event.fh = Some(fh);
                event.offset = Some(offset);
                event.requested_size = Some(length);
            },
            self.inner
                .fallocate(req, inode, fh, offset, length, mode)
                .await,
            |_, _| {}
        )
    }

    async fn readdirplus<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: u64,
        lock_owner: u64,
    ) -> Result<
        ReplyDirectoryPlus<impl futures::Stream<Item = Result<DirectoryEntryPlus>> + Send + 'a>,
    > {
        profile_result!(
            self,
            readdirplus,
            req,
            |event| {
                event.parent = Some(parent);
                event.fh = Some(fh);
                event.offset = Some(offset);
            },
            self.inner
                .readdirplus(req, parent, fh, offset, lock_owner)
                .await,
            |_, _| {}
        )
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
        profile_result!(
            self,
            rename2,
            req,
            |event| {
                event.parent = Some(parent);
                event.requested_size = Some(u64::from(flags));
            },
            self.inner
                .rename2(req, parent, name, new_parent, new_name, flags)
                .await,
            |_, _| {}
        )
    }

    async fn lseek(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        whence: u32,
    ) -> Result<ReplyLSeek> {
        profile_result!(
            self,
            lseek,
            req,
            |event| {
                event.inode = Some(inode);
                event.fh = Some(fh);
                event.offset = Some(offset);
                event.requested_size = Some(u64::from(whence));
            },
            self.inner.lseek(req, inode, fh, offset, whence).await,
            |reply, event| event.offset = Some(reply.offset)
        )
    }

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
        profile_result!(
            self,
            getlk,
            req,
            |event| {
                event.inode = Some(inode);
                event.fh = Some(fh);
                event.offset = Some(start);
                event.bytes = Some(end.saturating_sub(start));
                event.requested_size = Some(u64::from(r#type));
            },
            self.inner
                .getlk(req, inode, fh, lock_owner, start, end, r#type, pid)
                .await,
            |_, _| {}
        )
    }

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
        profile_result!(
            self,
            setlk,
            req,
            |event| {
                event.inode = Some(inode);
                event.fh = Some(fh);
                event.offset = Some(start);
                event.bytes = Some(end.saturating_sub(start));
                event.requested_size = Some(u64::from(r#type));
            },
            self.inner
                .setlk(req, inode, fh, lock_owner, start, end, r#type, pid, block)
                .await,
            |_, _| {}
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fuse::profile::{start_profile_writer, FuseProfileOptions};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockFilesystem {
        reads: AtomicUsize,
        writes: AtomicUsize,
    }

    impl Filesystem for MockFilesystem {
        async fn init(&self, _req: Request) -> Result<ReplyInit> {
            Ok(ReplyInit::default())
        }

        async fn destroy(&self, _req: Request) {}

        async fn read(
            &self,
            _req: Request,
            _inode: Inode,
            _fh: u64,
            _offset: u64,
            _size: u32,
        ) -> Result<ReplyData> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Ok(ReplyData {
                data: Bytes::from_static(b"12345678"),
            })
        }

        async fn write(
            &self,
            _req: Request,
            _inode: Inode,
            _fh: u64,
            _offset: u64,
            data: &[u8],
            _write_flags: u32,
            _flags: u32,
        ) -> Result<ReplyWrite> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            Ok(ReplyWrite {
                written: data.len() as u32,
            })
        }

        async fn getlk(
            &self,
            _req: Request,
            _inode: Inode,
            _fh: u64,
            _lock_owner: u64,
            _start: u64,
            _end: u64,
            _type: u32,
            _pid: u32,
        ) -> Result<ReplyLock> {
            Err(libc::ENOSYS.into())
        }

        async fn setlk(
            &self,
            _req: Request,
            _inode: Inode,
            _fh: u64,
            _lock_owner: u64,
            _start: u64,
            _end: u64,
            _type: u32,
            _pid: u32,
            _block: bool,
        ) -> Result<()> {
            Err(libc::ENOSYS.into())
        }
    }

    fn operation_fields<'a>(output: &'a str, op: &str) -> Vec<&'a str> {
        output
            .lines()
            .find(|line| line.split('\t').nth(2) == Some(op))
            .unwrap_or_else(|| panic!("missing {op} operation in profile:\n{output}"))
            .split('\t')
            .collect()
    }

    #[tokio::test]
    async fn records_read_write_sizes_and_latency() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("profile.tsv");
        let (context, writer) = start_profile_writer(FuseProfileOptions {
            path: path.display().to_string(),
            mount_id: "mount-test".to_string(),
            agent: "test-agent".to_string(),
            task: "io-test".to_string(),
            capacity: 16,
            flush_interval: std::time::Duration::from_millis(1),
        })
        .unwrap();

        let inner = MockFilesystem {
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
        };
        let filesystem = LogFuse::new(inner, context);
        let request = Request {
            unique: 100,
            uid: 1000,
            gid: 1000,
            pid: 2000,
        };

        let read = filesystem.read(request, 11, 4, 128, 8).await.unwrap();
        assert_eq!(read.data.len(), 8);

        let write = filesystem
            .write(request, 11, 4, 256, b"abcdef", 0, 0)
            .await
            .unwrap();
        assert_eq!(write.written, 6);

        writer.shutdown().await.unwrap();
        let output = std::fs::read_to_string(path).unwrap();
        let read_fields = operation_fields(&output, "read");
        let write_fields = operation_fields(&output, "write");

        assert_eq!(read_fields[3], "100");
        assert_eq!(read_fields[4], "2000");
        assert_eq!(read_fields[7], "11");
        assert_eq!(read_fields[8], "4");
        assert_eq!(read_fields[10], "128");
        assert_eq!(read_fields[11], "8");
        assert_eq!(read_fields[12], "8");
        assert_eq!(read_fields[14], "ok");
        assert!(read_fields[15].parse::<u128>().is_ok());

        assert_eq!(write_fields[7], "11");
        assert_eq!(write_fields[8], "4");
        assert_eq!(write_fields[10], "256");
        assert_eq!(write_fields[11], "6");
        assert_eq!(write_fields[12], "6");
        assert_eq!(write_fields[14], "ok");
        assert!(write_fields[15].parse::<u128>().is_ok());

        let inner = filesystem.into_inner();
        assert_eq!(inner.reads.load(Ordering::Relaxed), 1);
        assert_eq!(inner.writes.load(Ordering::Relaxed), 1);
    }
}
