//! Actual modern HTTP snapshot lower and real writable Antares upper.

use std::path::PathBuf;

use libfuse_fs::{
    passthrough::{config::Config as UpperConfig, PassthroughFs},
    unionfs::{config::Config as OverlayConfig, OverlayFs},
    util::whiteout::WhiteoutFormat,
};

use super::*;

async fn overlay(server: &Server) -> (Arc<Mst2Fuse>, OverlayFs, tempfile::TempDir, PathBuf) {
    let lower = Arc::new(server.view(false).await);
    let temp = tempfile::tempdir().unwrap();
    let upper_path = temp.path().join("upper");
    std::fs::create_dir(&upper_path).unwrap();
    let upper = PassthroughFs::<()>::new(UpperConfig {
        root_dir: upper_path.clone(),
        do_import: true,
        writeback: false,
        whiteout_format: WhiteoutFormat::OciWhiteout,
        ..Default::default()
    })
    .unwrap();
    upper.import().await.unwrap();
    let overlay = OverlayFs::new(
        Some(Arc::new(upper)),
        vec![lower.clone()],
        OverlayConfig {
            do_import: true,
            ..Default::default()
        },
        1,
    )
    .unwrap();
    overlay.init(Request::default()).await.unwrap();
    (lower, overlay, temp, upper_path)
}

#[tokio::test]
async fn modern_copy_up_append_truncate_and_upper_fsync_preserve_the_open_fixed_lower() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true), 1024 * 1024).await;
    let (lower, overlay, _temp, upper) = overlay(&server).await;
    let req = Request::default();
    let file = overlay
        .lookup(req, ROOT_INODE, OsStr::new("file001"))
        .await
        .unwrap()
        .attr
        .ino;
    let old = overlay
        .open(req, file, libc::O_RDONLY as u32)
        .await
        .unwrap();
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 0);
    let slots = server.reader.content_usage().output_bytes;
    let retained = overlay.read(req, file, old.fh, 17, 31).await.unwrap();
    let paid = server.reader.content_usage().output_bytes - slots;
    assert_eq!(retained.data.as_ref(), [1; 31]);
    let flags = (libc::O_RDWR | libc::O_APPEND) as u32;
    let writable = overlay.open(req, file, flags).await.unwrap();
    // The actual O_APPEND handle must append even when the supplied offset is0.
    assert_eq!(
        overlay
            .write(req, file, writable.fh, 0, b"tail", 0, flags)
            .await
            .unwrap()
            .written,
        4
    );
    overlay.fsync(req, file, writable.fh, false).await.unwrap();
    let mut expected = vec![1; 8192];
    expected.extend_from_slice(b"tail");
    assert_eq!(std::fs::read(upper.join("file001")).unwrap(), expected);
    assert_eq!(
        overlay
            .read(req, file, old.fh, 8190, 8)
            .await
            .unwrap()
            .data
            .as_ref(),
        [1; 2]
    );
    overlay
        .release(req, file, writable.fh, flags, 0, false)
        .await
        .unwrap();
    let trunc_flags = (libc::O_RDWR | libc::O_TRUNC) as u32;
    let truncated = overlay.open(req, file, trunc_flags).await.unwrap();
    assert_eq!(std::fs::metadata(upper.join("file001")).unwrap().len(), 0);
    assert_eq!(
        overlay
            .write(req, file, truncated.fh, 0, b"new", 0, trunc_flags)
            .await
            .unwrap()
            .written,
        3
    );
    overlay.fsync(req, file, truncated.fh, true).await.unwrap();
    overlay
        .release(req, file, truncated.fh, trunc_flags, 0, false)
        .await
        .unwrap();
    let reopened = overlay
        .open(req, file, libc::O_RDONLY as u32)
        .await
        .unwrap();
    assert_eq!(
        overlay
            .read(req, file, reopened.fh, 0, 64)
            .await
            .unwrap()
            .data
            .as_ref(),
        b"new"
    );
    overlay
        .release(req, file, reopened.fh, 0, 0, false)
        .await
        .unwrap();
    assert_eq!(
        overlay
            .read(req, file, old.fh, 0, 4)
            .await
            .unwrap()
            .data
            .as_ref(),
        [1; 4]
    );
    overlay
        .release(req, file, old.fh, 0, 0, false)
        .await
        .unwrap();
    assert_eq!(read(&lower, "file001", 0, 4).await.data.as_ref(), [1; 4]);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 1);
    assert_eq!(server.fixture.metadata.lock().unwrap().len(), 1);
    drop(overlay);
    drop(lower);
    idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, paid);
    assert_eq!(retained.data.as_ref(), [1; 31]);
    drop(retained);
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn copied_up_modern_file_keeps_its_live_upper_handle_after_unlink_and_whiteout() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true), 1024 * 1024).await;
    let (lower, overlay, _temp, upper) = overlay(&server).await;
    let req = Request::default();
    let name = OsStr::new("file002");
    let file = overlay
        .lookup(req, ROOT_INODE, name)
        .await
        .unwrap()
        .attr
        .ino;
    let flags = libc::O_RDWR as u32;
    let opened = overlay.open(req, file, flags).await.unwrap();
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        overlay
            .write(req, file, opened.fh, 0, b"upper", 0, flags)
            .await
            .unwrap()
            .written,
        5
    );
    overlay.unlink(req, ROOT_INODE, name).await.unwrap();
    assert_eq!(
        i32::from(overlay.lookup(req, ROOT_INODE, name).await.unwrap_err()),
        -libc::ENOENT
    );
    assert!(!upper.join("file002").exists());
    assert!(upper.join(".wh.file002").exists());
    assert_eq!(
        overlay
            .read(req, file, opened.fh, 0, 5)
            .await
            .unwrap()
            .data
            .as_ref(),
        b"upper"
    );
    assert_eq!(
        overlay
            .write(req, file, opened.fh, 5, b"tail", 0, flags)
            .await
            .unwrap()
            .written,
        4
    );
    overlay.fsync(req, file, opened.fh, false).await.unwrap();
    assert_eq!(
        overlay
            .read(req, file, opened.fh, 0, 9)
            .await
            .unwrap()
            .data
            .as_ref(),
        b"uppertail"
    );
    overlay
        .release(req, file, opened.fh, flags, 0, false)
        .await
        .unwrap();
    assert_eq!(
        i32::from(overlay.lookup(req, ROOT_INODE, name).await.unwrap_err()),
        -libc::ENOENT
    );
    assert_eq!(read(&lower, "file002", 0, 9).await.data.as_ref(), [2; 9]);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 1);
    drop(overlay);
    drop(lower);
    idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}

#[tokio::test]
async fn editor_temp_fsync_rename_and_directory_fsync_replace_upper_without_mutating_lower() {
    let _serial = TEST_LOCK.lock().await;
    let server = Server::start(Fixture::new(false, true), 1024 * 1024).await;
    let (lower, overlay, _temp, upper) = overlay(&server).await;
    let req = Request::default();
    let original_name = OsStr::new("file003");
    let original = overlay
        .lookup(req, ROOT_INODE, original_name)
        .await
        .unwrap()
        .attr
        .ino;
    let old = overlay
        .open(req, original, libc::O_RDONLY as u32)
        .await
        .unwrap();
    let prior = overlay.read(req, original, old.fh, 0, 4).await.unwrap();
    let temporary_name = OsStr::new(".file003.editor-tmp");
    let flags = libc::O_RDWR as u32;
    let created = overlay
        .create(req, ROOT_INODE, temporary_name, 0o644, flags)
        .await
        .unwrap();
    let inode = created.attr.ino;
    assert_eq!(
        overlay
            .write(req, inode, created.fh, 0, b"replacement\n", 0, flags)
            .await
            .unwrap()
            .written,
        12
    );
    overlay.fsync(req, inode, created.fh, false).await.unwrap();
    overlay
        .release(req, inode, created.fh, flags, 0, false)
        .await
        .unwrap();
    overlay
        .rename(req, ROOT_INODE, temporary_name, ROOT_INODE, original_name)
        .await
        .unwrap();
    let directory = overlay.opendir(req, ROOT_INODE, 0).await.unwrap();
    overlay
        .fsyncdir(req, ROOT_INODE, directory.fh, false)
        .await
        .unwrap();
    overlay
        .releasedir(req, ROOT_INODE, directory.fh, 0)
        .await
        .unwrap();
    assert!(!upper.join(temporary_name).exists());
    assert_eq!(
        std::fs::read(upper.join(original_name)).unwrap(),
        b"replacement\n"
    );
    assert_eq!(
        i32::from(
            overlay
                .lookup(req, ROOT_INODE, temporary_name)
                .await
                .unwrap_err()
        ),
        -libc::ENOENT
    );
    let replaced = overlay
        .lookup(req, ROOT_INODE, original_name)
        .await
        .unwrap()
        .attr;
    assert_eq!(replaced.size, 12);
    let opened = overlay
        .open(req, replaced.ino, libc::O_RDONLY as u32)
        .await
        .unwrap();
    assert_eq!(
        overlay
            .read(req, replaced.ino, opened.fh, 0, 64)
            .await
            .unwrap()
            .data
            .as_ref(),
        b"replacement\n"
    );
    overlay
        .release(req, replaced.ino, opened.fh, 0, 0, false)
        .await
        .unwrap();
    assert_eq!(
        overlay
            .read(req, original, old.fh, 0, 4)
            .await
            .unwrap()
            .data
            .as_ref(),
        [3; 4]
    );
    overlay
        .release(req, original, old.fh, 0, 0, false)
        .await
        .unwrap();
    assert_eq!(read(&lower, "file003", 0, 4).await.data.as_ref(), [3; 4]);
    assert_eq!(prior.data.as_ref(), [3; 4]);
    assert_eq!(server.fixture.requests.load(Ordering::SeqCst), 1);
    drop(prior);
    drop(overlay);
    drop(lower);
    idle(&server.reader).await;
    assert_eq!(server.reader.content_usage().output_bytes, 0);
}
