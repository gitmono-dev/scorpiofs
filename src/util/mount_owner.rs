//! Ownership for files surfaced through a ScorpioFS mount.
//!
//! A daemon started via `sudo` runs as root, but the mount serves a *user's*
//! worktree. Reporting the daemon's own uid/gid would make the union
//! filesystem's copy-up create root-owned upper nodes (the overlay preserves
//! the lower layer's ownership), after which the user cannot create files
//! inside a copied-up directory. The mount owner is therefore:
//!
//! 1. `SCORPIO_MOUNT_OWNER` (`uid` or `uid:gid`) when set — container
//!    orchestrators have no `SUDO_USER`: the daemon is PID 1 as root while
//!    the worktree belongs to a non-root runtime user, so the owner must be
//!    declared explicitly (see bench/infra runner manifests);
//! 2. else the effective ids when not root;
//! 3. else `SUDO_USER` (best-effort) when root — the local `sudo scorpio
//!    serve` path;
//! 4. else the effective ids (root).

use std::sync::OnceLock;

/// uid/gid reported for every node of a mount.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MountOwner {
    pub uid: u32,
    pub gid: u32,
}

impl Default for MountOwner {
    fn default() -> Self {
        resolve()
    }
}

/// Parse an explicit owner override: `"uid"` (gid = uid) or `"uid:gid"`.
fn parse_owner_override(spec: &str) -> Option<MountOwner> {
    let spec = spec.trim();
    if spec.is_empty() {
        return None;
    }
    let (uid_raw, gid_raw) = match spec.split_once(':') {
        Some((u, g)) => (u, Some(g)),
        None => (spec, None),
    };
    let uid: u32 = uid_raw.trim().parse().ok()?;
    let gid: u32 = match gid_raw {
        Some(g) => g.trim().parse().ok()?,
        None => uid,
    };
    Some(MountOwner { uid, gid })
}

fn resolve() -> MountOwner {
    let euid = unsafe { libc::geteuid() };
    let egid = unsafe { libc::getegid() };
    // Explicit override first: containers have no SUDO_USER to derive from.
    if let Some(spec) = std::env::var_os("SCORPIO_MOUNT_OWNER") {
        if let Some(owner) = spec.to_str().and_then(parse_owner_override) {
            return owner;
        }
    }
    if euid != 0 {
        return MountOwner { uid: euid, gid: egid };
    }
    let Some(user) = std::env::var_os("SUDO_USER") else {
        return MountOwner { uid: euid, gid: egid };
    };
    let Ok(cuser) = std::ffi::CString::new(user.as_os_str().as_encoded_bytes()) else {
        return MountOwner { uid: euid, gid: egid };
    };
    let pw = unsafe { libc::getpwnam(cuser.as_ptr()) };
    if pw.is_null() {
        return MountOwner { uid: euid, gid: egid };
    }
    let (uid, gid) = unsafe { ((*pw).pw_uid, (*pw).pw_gid) };
    MountOwner { uid, gid }
}

/// The uid/gid a mount should report. Resolved once per process.
pub fn mount_owner() -> MountOwner {
    static OWNER: OnceLock<MountOwner> = OnceLock::new();
    *OWNER.get_or_init(resolve)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_is_stable_across_calls() {
        assert_eq!(mount_owner(), mount_owner());
    }

    #[test]
    fn owner_override_parses_uid_and_uid_gid() {
        assert_eq!(
            parse_owner_override("1000"),
            Some(MountOwner { uid: 1000, gid: 1000 })
        );
        assert_eq!(
            parse_owner_override("1000:100"),
            Some(MountOwner { uid: 1000, gid: 100 })
        );
        assert_eq!(
            parse_owner_override(" 1000 : 100 "),
            Some(MountOwner { uid: 1000, gid: 100 })
        );
    }

    #[test]
    fn owner_override_rejects_garbage() {
        assert_eq!(parse_owner_override(""), None);
        assert_eq!(parse_owner_override("  "), None);
        assert_eq!(parse_owner_override("root"), None);
        assert_eq!(parse_owner_override("1000:"), None);
        assert_eq!(parse_owner_override(":100"), None);
        assert_eq!(parse_owner_override("1000:x"), None);
    }
}
