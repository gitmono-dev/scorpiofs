//! Platform-specific FUSE detection and unmount helpers.
//!
//! Linux uses `/dev/fuse` + `fusermount3`. macOS uses macFUSE's `mount_macfuse`;
//! FUSE-T is detected so `scorpio doctor` can explain that it is not supported.

use std::{
    io,
    path::{Path, PathBuf},
    process::Command,
};

/// Detected FUSE userspace / kernel provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuseProvider {
    LinuxFuse,
    MacFuse,
    /// FUSE-T is present but asyncfuse only speaks macFUSE.
    FuseTUnsupported,
    Unavailable,
}

impl FuseProvider {
    pub fn is_usable(self) -> bool {
        matches!(self, Self::LinuxFuse | Self::MacFuse)
    }
}

pub const MACFUSE_MOUNT: &str = "/Library/Filesystems/macfuse.fs/Contents/Resources/mount_macfuse";
pub const FUSET_FS: &str = "/Library/Filesystems/fuse-t.fs";

/// Resolve the host FUSE provider. Does not attempt a probe mount.
pub fn fuse_provider() -> FuseProvider {
    #[cfg(target_os = "linux")]
    {
        if Path::new("/dev/fuse").exists() {
            FuseProvider::LinuxFuse
        } else {
            FuseProvider::Unavailable
        }
    }
    #[cfg(target_os = "macos")]
    {
        if Path::new(MACFUSE_MOUNT).exists() {
            FuseProvider::MacFuse
        } else if Path::new(FUSET_FS).exists() {
            FuseProvider::FuseTUnsupported
        } else {
            FuseProvider::Unavailable
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        FuseProvider::Unavailable
    }
}

/// The FUSE unmount helper to invoke on Linux, resolved once.
///
/// Prefers fuse3's `fusermount3` and falls back to fuse2's `fusermount`.
#[cfg(target_os = "linux")]
pub(crate) fn fusermount_bin() -> &'static str {
    use std::sync::OnceLock;
    static BIN: OnceLock<&'static str> = OnceLock::new();
    BIN.get_or_init(|| {
        if binary_on_path("fusermount3") {
            "fusermount3"
        } else if binary_on_path("fusermount") {
            "fusermount"
        } else {
            "fusermount3"
        }
    })
}

#[cfg(target_os = "linux")]
pub(crate) fn binary_on_path(name: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(name).is_file()))
        .unwrap_or(false)
}

/// Unmount `path`. On Linux this is `fusermount -u` / `-uz`. On macOS this is
/// `/sbin/umount`, then `/sbin/umount -f` when `lazy` is set or the first call
/// fails because the mount is busy.
pub async fn unmount_path(path: impl AsRef<Path>, lazy: bool) -> io::Result<()> {
    let path = path.as_ref();
    #[cfg(target_os = "linux")]
    {
        unmount_linux(path, lazy).await
    }
    #[cfg(target_os = "macos")]
    {
        unmount_macos(path, lazy).await
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (path, lazy);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "FUSE unmount is not supported on this platform",
        ))
    }
}

#[cfg(target_os = "linux")]
async fn unmount_linux(path: &Path, lazy: bool) -> io::Result<()> {
    let mut cmd = tokio::process::Command::new(fusermount_bin());
    if lazy {
        cmd.arg("-uz");
    } else {
        cmd.arg("-u");
    }
    let output = cmd.arg(path).output().await?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if is_not_mounted_message(&stderr) {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "{} failed for {}: {}",
        fusermount_bin(),
        path.display(),
        stderr.trim()
    )))
}

#[cfg(target_os = "macos")]
async fn unmount_macos(path: &Path, lazy: bool) -> io::Result<()> {
    if !lazy {
        let output = tokio::process::Command::new("/sbin/umount")
            .arg(path)
            .output()
            .await?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if is_not_mounted_message(&stderr) {
            return Ok(());
        }
        tracing::warn!(
            path = %path.display(),
            error = %stderr.trim(),
            "umount failed; retrying with -f"
        );
    }
    let output = tokio::process::Command::new("/sbin/umount")
        .arg("-f")
        .arg(path)
        .output()
        .await?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if is_not_mounted_message(&stderr) {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "umount -f failed for {}: {}",
        path.display(),
        stderr.trim()
    )))
}

pub(crate) fn is_not_mounted_message(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    lower.contains("not mounted")
        || lower.contains("not currently mounted")
        || lower.contains("invalid argument")
}

/// Whether `path` currently appears in the host mount table.
pub fn is_mounted(path: impl AsRef<Path>) -> bool {
    let path = path.as_ref();
    let candidates = mount_path_candidates(path);
    #[cfg(target_os = "linux")]
    {
        for candidate in &candidates {
            let status = Command::new("findmnt")
                .args(["--mountpoint", "--noheadings"])
                .arg(candidate)
                .status();
            if matches!(status, Ok(s) if s.success()) {
                return true;
            }
        }
        false
    }
    #[cfg(target_os = "macos")]
    {
        let output = match Command::new("mount").output() {
            Ok(o) => o,
            Err(_) => return false,
        };
        if !output.status.success() {
            return false;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        mount_table_contains(&stdout, &candidates)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = candidates;
        false
    }
}

fn mount_path_candidates(path: &Path) -> Vec<PathBuf> {
    let mut out = vec![path.to_path_buf()];
    if let Ok(canon) = path.canonicalize() {
        if !out.contains(&canon) {
            out.push(canon);
        }
    }
    out
}

/// Parse BSD / macOS `mount` output (`DEVICE on TARGET (opts)`).
pub(crate) fn mount_line_target(line: &str) -> Option<&str> {
    let rest = line.split_once(" on ")?.1;
    Some(
        rest.rsplit_once(" (")
            .map(|(target, _)| target)
            .unwrap_or(rest)
            .trim(),
    )
}

pub(crate) fn mount_table_contains(stdout: &str, candidates: &[PathBuf]) -> bool {
    stdout.lines().any(|line| {
        let Some(target) = mount_line_target(line) else {
            return false;
        };
        candidates.iter().any(|c| c.as_os_str() == target)
    })
}

#[cfg(test)]
mod tests {
    use super::{is_not_mounted_message, mount_line_target, mount_table_contains};
    use std::path::PathBuf;

    #[test]
    fn parses_macos_mount_line() {
        let line =
            "macfuse#scorpio on /private/tmp/megadir-eli/mount (macfuse, local, synchronous)";
        assert_eq!(
            mount_line_target(line),
            Some("/private/tmp/megadir-eli/mount")
        );
    }

    #[test]
    fn mount_table_matches_exact_path_only() {
        let table = "\
/dev/disk3s1 on / (apfs, local)
macfuse#scorpio on /tmp/foo (macfuse, local)
";
        assert!(mount_table_contains(table, &[PathBuf::from("/tmp/foo")]));
        assert!(!mount_table_contains(
            table,
            &[PathBuf::from("/tmp/foo-bar")]
        ));
    }

    #[test]
    fn not_mounted_messages() {
        assert!(is_not_mounted_message("umount: /tmp/x: not mounted"));
        assert!(is_not_mounted_message(
            "umount: /tmp/x: not currently mounted"
        ));
        assert!(is_not_mounted_message("Invalid argument"));
        assert!(!is_not_mounted_message("permission denied"));
    }
}
