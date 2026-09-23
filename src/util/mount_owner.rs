//! Ownership for files surfaced through a ScorpioFS mount.
//!
//! A daemon started via `sudo` runs as root, but the mount serves a *user's*
//! worktree. Reporting the daemon's own uid/gid would make the union
//! filesystem's copy-up create root-owned upper nodes (the overlay preserves
//! the lower layer's ownership), after which the user cannot create files
//! inside a copied-up directory. The mount owner is therefore derived from
//! `SUDO_USER` (best-effort) and falls back to the effective ids.

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

fn resolve() -> MountOwner {
    let euid = unsafe { libc::geteuid() };
    let egid = unsafe { libc::getegid() };
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
}
