//! Integration tests for Antares mount management.
//!
//! These tests verify the `AntaresManager` and `AntaresFuse` functionality,
//! including custom mountpoint support via `mount_job_at`.
//!
//! ## Running Tests
//!
//! Unit tests (no root required):
//! ```bash
//! cargo test --test antares_test -- --test-threads=1
//! ```
//!
//! Integration tests with FUSE (requires root):
//! ```bash
//! sudo -E cargo test --test antares_test -- --ignored --nocapture --test-threads=1
//! ```

use scorpio::{
    antares::fuse::AntaresFuse,
    antares::{AntaresManager, AntaresPaths},
    util::config,
};
use serial_test::serial;
use std::path::PathBuf;
use tempfile::tempdir;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

/// Helper to initialize config, ignoring "already initialized" errors.
fn init_config() {
    if let Err(e) = config::init_config("./scorpio.toml") {
        if !e.contains("already initialized") {
            panic!("Failed to load config: {e}");
        }
    }
}

// =============================================================================
// AntaresManager Unit Tests (no root required)
// =============================================================================

#[tokio::test]
async fn test_mount_and_list_registers_instance() {
    init_config();

    let root = tempdir().unwrap();
    let paths = AntaresPaths::new(
        root.path().join("upper"),
        root.path().join("cl"),
        root.path().join("mnt"),
        root.path().join("state.toml"),
    );
    let manager = AntaresManager::new(paths).await;

    let instance = manager.mount_job("job1", Some("cl1")).await.unwrap();
    assert_eq!(instance.job_id, "job1");
    assert!(instance.mountpoint.starts_with(root.path().join("mnt")));

    // Verify state persistence
    let state_content = std::fs::read_to_string(root.path().join("state.toml")).unwrap();
    assert!(state_content.contains("job1"));

    // Verify listing
    let listed = manager.list().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].job_id, "job1");

    // Cleanup
    let removed = manager.umount_job("job1").await.unwrap();
    assert!(removed.is_some());
    assert!(manager.list().await.is_empty());
}

#[tokio::test]
async fn test_mount_job_at_custom_path() {
    init_config();

    let root = tempdir().unwrap();
    let custom_mount = root.path().join("custom_mountpoint");

    let paths = AntaresPaths::new(
        root.path().join("upper"),
        root.path().join("cl"),
        root.path().join("mnt"),
        root.path().join("state.toml"),
    );
    let manager = AntaresManager::new(paths).await;

    // Mount to custom path (outside of mount_root)
    let instance = manager
        .mount_job_at("job_custom", custom_mount.clone(), None)
        .await
        .unwrap();

    // Verify mountpoint is at custom location
    assert_eq!(instance.mountpoint, custom_mount);
    assert!(!instance.mountpoint.starts_with(root.path().join("mnt")));

    // Verify directory was created
    assert!(custom_mount.exists());

    // Verify state persistence includes custom mountpoint
    let state_content = std::fs::read_to_string(root.path().join("state.toml")).unwrap();
    assert!(state_content.contains("custom_mountpoint"));

    // Verify listing
    let listed = manager.list().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].mountpoint, custom_mount);

    // Cleanup
    let removed = manager.umount_job("job_custom").await.unwrap();
    assert!(removed.is_some());
}

#[tokio::test]
async fn test_mount_job_at_multiple_custom_paths() {
    init_config();

    let root = tempdir().unwrap();
    let paths = AntaresPaths::new(
        root.path().join("upper"),
        root.path().join("cl"),
        root.path().join("mnt"),
        root.path().join("state.toml"),
    );
    let manager = AntaresManager::new(paths).await;

    // Mount multiple jobs to different custom paths
    let custom1 = root.path().join("workspace1");
    let custom2 = root.path().join("deeply/nested/workspace2");

    let inst1 = manager
        .mount_job_at("job1", custom1.clone(), None)
        .await
        .unwrap();
    let inst2 = manager
        .mount_job_at("job2", custom2.clone(), Some("cl-123"))
        .await
        .unwrap();

    assert_eq!(inst1.mountpoint, custom1);
    assert_eq!(inst2.mountpoint, custom2);
    assert!(inst2.cl_dir.is_some()); // CL layer was requested

    // Verify both directories were created (including nested path)
    assert!(custom1.exists());
    assert!(custom2.exists());

    // Verify listing shows both
    let listed = manager.list().await;
    assert_eq!(listed.len(), 2);

    // Cleanup
    manager.umount_job("job1").await.unwrap();
    manager.umount_job("job2").await.unwrap();
    assert!(manager.list().await.is_empty());
}

#[tokio::test]
async fn test_mount_job_at_with_pathbuf() {
    init_config();

    let root = tempdir().unwrap();
    let paths = AntaresPaths::new(
        root.path().join("upper"),
        root.path().join("cl"),
        root.path().join("mnt"),
        root.path().join("state.toml"),
    );
    let manager = AntaresManager::new(paths).await;

    // Test with PathBuf
    let custom = root.path().join("custom_path");
    let inst = manager
        .mount_job_at("job_pathbuf", custom.clone(), None)
        .await
        .unwrap();
    assert_eq!(inst.mountpoint, custom);

    // Test with &str
    let custom_str = root.path().join("string_path");
    let inst2 = manager
        .mount_job_at("job_str", custom_str.clone(), None)
        .await
        .unwrap();
    assert_eq!(inst2.mountpoint, custom_str);

    manager.umount_job("job_pathbuf").await.unwrap();
    manager.umount_job("job_str").await.unwrap();
}

// =============================================================================
// FUSE Integration Tests (requires root)
// =============================================================================

/// Check if FUSE prerequisites are met.
fn fuse_prereqs_available() -> bool {
    let uid = unsafe { libc::geteuid() };
    if uid != 0 {
        println!("Skipping: requires root privileges");
        return false;
    }

    if !std::path::Path::new("/dev/fuse").exists() {
        println!("Skipping: /dev/fuse not available");
        return false;
    }

    if std::process::Command::new("fusermount")
        .arg("--version")
        .output()
        .is_err()
    {
        println!("Skipping: fusermount not found");
        return false;
    }

    true
}

/// Test that mount_job_at can mount to an arbitrary directory and file operations work.
///
/// Run with:
///   sudo -E cargo test --test antares_test test_fuse_mount_job_at_custom_path -- --exact --ignored --nocapture
#[tokio::test]
#[ignore]
#[serial]
async fn test_fuse_mount_job_at_custom_path() {
    let test_future = async {
        init_config();

        if !fuse_prereqs_available() {
            return;
        }

        let test_id = Uuid::new_v4();
        let base = PathBuf::from(format!("/tmp/antares_mount_at_test_{test_id}"));
        let _ = std::fs::remove_dir_all(&base);

        let custom_mount = base.join("my_custom_workspace");
        let paths = AntaresPaths::new(
            base.join("upper"),
            base.join("cl"),
            base.join("mnt"),
            base.join("state.toml"),
        );

        let manager = AntaresManager::new(paths).await;

        // Mount to custom path
        println!("Mounting job to custom path: {}", custom_mount.display());
        let config = manager
            .mount_job_at("test-job", custom_mount.clone(), None)
            .await
            .expect("mount_job_at should succeed");

        assert_eq!(config.mountpoint, custom_mount);
        println!(
            "✓ Job mounted at custom path: {}",
            config.mountpoint.display()
        );

        // Create AntaresFuse and mount it
        let dic = manager.dicfuse();
        let mut fuse = AntaresFuse::new(
            custom_mount.clone(),
            dic,
            config.upper_dir.clone(),
            config.cl_dir.clone(),
        )
        .await
        .expect("AntaresFuse::new should succeed");

        fuse.mount().await.expect("FUSE mount should succeed");
        println!("✓ FUSE filesystem mounted");

        sleep(Duration::from_millis(500)).await;

        // Test basic file operations
        println!("Testing file operations...");

        // Directory listing
        let read_result = tokio::fs::read_dir(&custom_mount).await;
        assert!(read_result.is_ok(), "should be able to read directory");
        println!("✓ Directory listing works");

        // File write
        let test_file = custom_mount.join("test_file.txt");
        tokio::fs::write(&test_file, b"Hello from custom mountpoint!")
            .await
            .unwrap();
        println!("✓ File write works");

        // File read
        let read_content = tokio::fs::read(&test_file).await.unwrap();
        assert_eq!(read_content, b"Hello from custom mountpoint!");
        println!("✓ File read works");

        // Subdirectory
        let subdir = custom_mount.join("subdir");
        tokio::fs::create_dir(&subdir).await.unwrap();
        let subfile = subdir.join("nested.txt");
        tokio::fs::write(&subfile, b"nested content").await.unwrap();
        let nested = tokio::fs::read(&subfile).await.unwrap();
        assert_eq!(nested, b"nested content");
        println!("✓ Subdirectory operations work");

        // Cleanup
        println!("Unmounting...");
        fuse.unmount().await.expect("unmount should succeed");
        manager
            .umount_job("test-job")
            .await
            .expect("manager umount should succeed");
        let _ = std::fs::remove_dir_all(&base);
        println!("✓ Test completed successfully");
    };

    match tokio::time::timeout(Duration::from_secs(120), test_future).await {
        Ok(_) => println!("✓ Test passed"),
        Err(_) => panic!("Test timed out after 120 seconds"),
    }
}

/// Test mounting multiple jobs to different custom paths with FUSE.
///
/// Run with:
///   sudo -E cargo test --test antares_test test_fuse_multiple_custom_mounts -- --exact --ignored --nocapture
#[tokio::test]
#[ignore]
#[serial]
async fn test_fuse_multiple_custom_mounts() {
    let test_future = async {
        init_config();

        if !fuse_prereqs_available() {
            return;
        }

        let test_id = Uuid::new_v4();
        let base = PathBuf::from(format!("/tmp/antares_multi_mount_test_{test_id}"));
        let _ = std::fs::remove_dir_all(&base);

        let paths = AntaresPaths::new(
            base.join("upper"),
            base.join("cl"),
            base.join("mnt"),
            base.join("state.toml"),
        );

        let manager = AntaresManager::new(paths).await;
        let dic = manager.dicfuse();

        let mount1 = base.join("workspace_a");
        let mount2 = base.join("workspace_b");

        // Mount first job
        let config1 = manager
            .mount_job_at("job-a", mount1.clone(), None)
            .await
            .unwrap();
        let mut fuse1 =
            AntaresFuse::new(mount1.clone(), dic.clone(), config1.upper_dir.clone(), None)
                .await
                .unwrap();
        fuse1.mount().await.unwrap();
        println!("✓ Job A mounted at {}", mount1.display());

        // Mount second job
        let config2 = manager
            .mount_job_at("job-b", mount2.clone(), Some("cl-test"))
            .await
            .unwrap();
        let mut fuse2 = AntaresFuse::new(
            mount2.clone(),
            dic.clone(),
            config2.upper_dir.clone(),
            config2.cl_dir.clone(),
        )
        .await
        .unwrap();
        fuse2.mount().await.unwrap();
        println!("✓ Job B mounted at {}", mount2.display());

        sleep(Duration::from_millis(500)).await;

        // Write different files to each mount
        let file1 = mount1.join("file_from_job_a.txt");
        let file2 = mount2.join("file_from_job_b.txt");

        tokio::fs::write(&file1, b"Written by job A").await.unwrap();
        tokio::fs::write(&file2, b"Written by job B").await.unwrap();

        // Verify isolation
        assert!(file1.exists());
        assert!(file2.exists());
        assert!(!mount1.join("file_from_job_b.txt").exists());
        assert!(!mount2.join("file_from_job_a.txt").exists());
        println!("✓ Mount isolation verified");

        // Verify both are tracked
        let listed = manager.list().await;
        assert_eq!(listed.len(), 2);
        println!("✓ Both mounts tracked");

        // Cleanup
        fuse1.unmount().await.unwrap();
        fuse2.unmount().await.unwrap();
        manager.umount_job("job-a").await.unwrap();
        manager.umount_job("job-b").await.unwrap();

        let _ = std::fs::remove_dir_all(&base);
        println!("✓ Test completed successfully");
    };

    match tokio::time::timeout(Duration::from_secs(120), test_future).await {
        Ok(_) => println!("✓ Test passed"),
        Err(_) => panic!("Test timed out"),
    }
}
