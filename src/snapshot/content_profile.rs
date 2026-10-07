//! Fixed MST/2 serving profile, shared by owned HTTP and local CAS readers.

/// Files at or below this size use OBJECT frames (spec 07 §2).
pub const OBJECT_CAP: u64 = 256 * 1024;
pub(crate) const MAX_FILE_SIZE: u64 = 8 * 1024 * 1024 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_cap_matches_the_profile_split() {
        assert_eq!(OBJECT_CAP, 262_144);
    }
}
