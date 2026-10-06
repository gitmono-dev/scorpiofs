//! Compile guard for the public commands used by existing Rust CLI front ends.
use std::{collections::HashMap, path::PathBuf};

type LegacyOverrides = fn(
    Option<PathBuf>,
    Option<PathBuf>,
    Option<PathBuf>,
    Option<PathBuf>,
) -> HashMap<String, String>;

#[test]
fn old_command_entry_points_remain_public() {
    let overrides: LegacyOverrides = scorpiofs::cli::antares_overrides;
    assert!(overrides(None, None, None, None).is_empty());
    let _ = scorpiofs::cli::antares_mount;
    let _ = scorpiofs::cli::antares_umount;
    let _ = scorpiofs::cli::antares_list;
    let _ = scorpiofs::cli::antares_serve;
    let _ = scorpiofs::cli::http_mount;
}
