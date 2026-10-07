use futures::StreamExt;
use libfuse_fs::unionfs::layer::Layer;

use super::*;

fn view() -> (Mst2Fuse, Arc<ReadProfile>) {
    let mut files: Vec<_> = (0..256)
        .map(|index| SnapshotFile {
            rel_path: format!("wide/file{index:03}"),
            fs_kind: "regular".into(),
            size: index + 1,
            content_digest: format!("sha256:{}", "12".repeat(32)),
        })
        .collect();
    for (name, kind, size) in [("exec", "executable", 7), ("link", "symlink", 6)] {
        files.push(SnapshotFile {
            rel_path: format!("wide/{name}"),
            fs_kind: kind.into(),
            size,
            content_digest: format!("sha256:{}", "34".repeat(32)),
        });
    }
    let profile = ReadProfile::new();
    let view = Mst2Fuse::build(None, None, files)
        .unwrap()
        .with_read_profile(Some(profile.clone()));
    (view, profile)
}

#[tokio::test]
async fn directory_reply_consumption_and_drop_share_one_fixed_index_without_building_suffixes() {
    let (view, profile) = view();
    let req = Request::default();
    let inode = view
        .lookup(req, ROOT_INODE, OsStr::new("wide"))
        .await
        .unwrap()
        .attr
        .ino;
    let index = {
        let state = view.state.lock().unwrap();
        let Node::Dir(directory) = &state.nodes[&inode] else {
            panic!("wide must remain a fixed directory");
        };
        directory.children.snapshot().unwrap()
    };
    assert_eq!(index.len(), 258);
    assert_eq!(Arc::strong_count(&index), 2);
    {
        let reply = view.readdir(req, inode, inode, 0).await.unwrap();
        assert_eq!(Arc::strong_count(&index), 3);
        let before = profile.snapshot();
        assert_eq!(before.metric(Metric::DirectoryReplyEntriesBuilt), 0);
        assert_eq!(before.metric(Metric::DirectoryReplyNameBytes), 0);
        drop(reply);
    }
    assert_eq!(Arc::strong_count(&index), 2);
    {
        let reply = view.readdirplus(req, inode, inode, 0, 0).await.unwrap();
        assert_eq!(
            profile
                .snapshot()
                .metric(Metric::DirectoryReplyEntriesBuilt),
            0
        );
        let mut entries = Box::pin(reply.entries);
        for (name, offset) in [(".", 1), ("..", 2), ("exec", 3)] {
            let entry = entries.next().await.unwrap().unwrap();
            assert_eq!(entry.name.to_str(), Some(name));
            assert_eq!(entry.offset, offset);
            assert_eq!(entry.inode, entry.attr.ino);
            if name == "exec" {
                assert_eq!(entry.attr.size, 7);
                assert_eq!(entry.attr.perm, 0o755);
            }
        }
        let partial = profile.snapshot();
        assert_eq!(partial.metric(Metric::DirectoryReplyEntriesBuilt), 3);
        assert_eq!(partial.metric(Metric::DirectoryReplyNameBytes), 7);
        assert!(!partial.directory_stream_delivery_measured);
        drop(entries);
    }
    assert_eq!(Arc::strong_count(&index), 2);
    assert_eq!(
        profile
            .snapshot()
            .metric(Metric::DirectoryReplyEntriesBuilt),
        3
    );
    {
        let reply = view.readdirplus(req, inode, inode, 259, 0).await.unwrap();
        assert_eq!(
            profile
                .snapshot()
                .metric(Metric::DirectoryReplyEntriesBuilt),
            3
        );
        let mut entries = Box::pin(reply.entries);
        let last = entries.next().await.unwrap().unwrap();
        assert_eq!(last.name.to_str(), Some("link"));
        assert_eq!(last.offset, 260);
        assert_eq!(last.attr.kind, FileType::Symlink);
        assert_eq!(last.attr.size, 6);
        assert!(entries.next().await.is_none());
    }
    assert_eq!(Arc::strong_count(&index), 2);
    assert_eq!(
        profile
            .snapshot()
            .metric(Metric::DirectoryReplyEntriesBuilt),
        4
    );
    {
        let reply = view.readdir(req, inode, inode, i64::MIN).await.unwrap();
        let mut entries = Box::pin(reply.entries);
        let dot = entries.next().await.unwrap().unwrap();
        assert_eq!(dot.name.to_str(), Some("."));
        assert_eq!(dot.offset, 1);
        assert_eq!(dot.kind, FileType::Directory);
    }
    assert_eq!(Arc::strong_count(&index), 2);
    assert_eq!(
        profile
            .snapshot()
            .metric(Metric::DirectoryReplyEntriesBuilt),
        5
    );
}

#[tokio::test]
async fn wide_directory_attributes_and_resume_cookies_keep_fixed_metadata_without_node_copies() {
    let (view, profile) = view();
    let req = Request::default();
    let wide = view
        .lookup(req, ROOT_INODE, OsStr::new("wide"))
        .await
        .unwrap();
    let inode = wide.attr.ino;
    assert_eq!(wide.attr.kind, FileType::Directory);
    assert_eq!(wide.attr.perm, 0o755);
    assert_eq!(wide.attr.nlink, 2);
    assert_eq!(view.opendir(req, inode, 0).await.unwrap().fh, inode);
    let listing = view.readdirplus(req, inode, inode, 0, 0).await.unwrap();
    let entries = listing.entries.collect::<Vec<_>>().await;
    assert_eq!(entries.len(), 260);
    for (index, entry) in entries.iter().enumerate() {
        let entry = entry.as_ref().unwrap();
        assert_eq!(entry.offset, index as i64 + 1);
        assert_eq!(entry.attr.ino, entry.inode);
        assert_eq!(entry.attr.kind, entry.kind);
        assert_eq!(entry.entry_ttl, TTL);
        assert_eq!(entry.attr_ttl, TTL);
        assert_eq!(entry.generation, 0);
        let attr = view.getattr(req, entry.inode, None, 0).await.unwrap().attr;
        assert_eq!(attr.kind, entry.attr.kind);
        assert_eq!(attr.size, entry.attr.size);
        assert_eq!(attr.perm, entry.attr.perm);
        let name = entry.name.to_str().unwrap();
        match name {
            "." => assert_eq!(entry.inode, inode),
            ".." => assert_eq!(entry.inode, ROOT_INODE),
            "exec" => {
                assert_eq!(entry.attr.kind, FileType::RegularFile);
                assert_eq!(entry.attr.size, 7);
                assert_eq!(entry.attr.perm, 0o755);
            }
            "link" => {
                assert_eq!(entry.attr.kind, FileType::Symlink);
                assert_eq!(entry.attr.size, 6);
                assert_eq!(entry.attr.perm, 0o777);
            }
            file => {
                let number = file.strip_prefix("file").unwrap().parse::<u64>().unwrap();
                assert_eq!(entry.attr.kind, FileType::RegularFile);
                assert_eq!(entry.attr.size, number + 1);
                assert_eq!(entry.attr.perm, 0o644);
            }
        }
    }
    let resumed = view.readdirplus(req, inode, inode, 129, 0).await.unwrap();
    let resumed = resumed.entries.collect::<Vec<_>>().await;
    assert_eq!(resumed.len(), entries.len() - 129);
    for (expected, actual) in entries[129..].iter().zip(resumed.iter()) {
        let expected = expected.as_ref().unwrap();
        let actual = actual.as_ref().unwrap();
        assert_eq!(actual.name, expected.name);
        assert_eq!(actual.inode, expected.inode);
        assert_eq!(actual.offset, expected.offset);
        assert_eq!(actual.attr.size, expected.attr.size);
    }
    let empty = view.readdirplus(req, inode, inode, 260, 0).await.unwrap();
    assert!(empty.entries.collect::<Vec<_>>().await.is_empty());
    assert_eq!(
        i32::from(view.metadata_attr(u64::MAX).unwrap_err()),
        -libc::ENOENT
    );
    let measured = profile.snapshot();
    assert!(!measured.overflow);
    assert_eq!(measured.metric(Metric::NodeDirectoryClones), 0);
    assert_eq!(measured.metric(Metric::NodeFileClones), 0);
    assert_eq!(measured.metric(Metric::SmallCasCalls), 0);
    assert_eq!(measured.metric(Metric::LargeCasCalls), 0);
}

#[tokio::test]
async fn overlay_mapping_and_opaque_queries_preserve_type_size_and_errno_without_node_copies() {
    let (view, profile) = view();
    let req = Request::default();
    let inode = view
        .lookup(req, ROOT_INODE, OsStr::new("wide"))
        .await
        .unwrap()
        .attr
        .ino;
    assert!(!view.is_opaque(req, inode).await.unwrap());
    for (name, mode, size) in [
        ("file000", libc::S_IFREG | 0o644, 1),
        ("exec", libc::S_IFREG | 0o755, 7),
        ("link", libc::S_IFLNK | 0o777, 6),
    ] {
        let file = view
            .lookup(req, inode, OsStr::new(name))
            .await
            .unwrap()
            .attr
            .ino;
        let (stat, ttl) = view.getattr_with_mapping(file, None, false).await.unwrap();
        assert_eq!(stat.st_ino, file);
        assert_eq!(stat.st_mode, mode);
        assert_eq!(stat.st_size, size);
        assert_eq!(ttl, TTL);
        assert_eq!(
            i32::from(view.is_opaque(req, file).await.unwrap_err()),
            -libc::ENOTDIR
        );
        assert_eq!(
            i32::from(view.opendir(req, file, 0).await.unwrap_err()),
            -libc::ENOTDIR
        );
    }
    let (stat, ttl) = view.getattr_with_mapping(inode, None, false).await.unwrap();
    assert_eq!(stat.st_mode, libc::S_IFDIR | 0o755);
    assert_eq!(stat.st_nlink, 2);
    assert_eq!(ttl, TTL);
    assert_eq!(
        view.getattr_with_mapping(u64::MAX, None, false)
            .await
            .err()
            .unwrap()
            .raw_os_error(),
        Some(libc::ENOENT)
    );
    let measured = profile.snapshot();
    assert_eq!(measured.metric(Metric::NodeDirectoryClones), 0);
    assert_eq!(measured.metric(Metric::NodeFileClones), 0);
    assert_eq!(measured.metric(Metric::SmallCasCalls), 0);
    assert_eq!(measured.metric(Metric::LargeCasCalls), 0);
}
