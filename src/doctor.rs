//! `scorpio doctor` — environment diagnostics.
//!
//! Runs a series of lightweight, mostly-local checks and prints a human-readable
//! report to stdout. Critical failures (missing FUSE device, non-writable
//! runtime directories) yield a non-zero exit code; advisory issues (no
//! `user_allow_other`, unreachable mega server) are reported as warnings.
//!
//! Configuration must already be loaded (via `cli::init`) before calling
//! [`run`], so the directory/URL checks reflect the effective config.

use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use crate::{cli::exit, util::config};

#[derive(Clone, Copy, PartialEq)]
enum Status {
    Ok,
    Warn,
    Fail,
}

fn report(status: Status, name: &str, detail: &str) {
    let tag = match status {
        Status::Ok => "[ OK ]",
        Status::Warn => "[WARN]",
        Status::Fail => "[FAIL]",
    };
    println!("{tag} {name}: {detail}");
}

/// Run all diagnostics. Returns `exit::SUCCESS` when no critical check fails,
/// otherwise `exit::INTERNAL`.
pub async fn run() -> i32 {
    println!("scorpio doctor — environment diagnostics\n");
    let mut failures = 0u32;

    check_fuse(&mut failures);
    check_fuse_conf();
    check_directories(&mut failures);
    check_mega().await;

    println!();
    if failures == 0 {
        println!("All critical checks passed.");
        exit::SUCCESS
    } else {
        println!("{failures} critical check(s) failed.");
        exit::INTERNAL
    }
}

fn check_fuse(failures: &mut u32) {
    if Path::new("/dev/fuse").exists() {
        report(Status::Ok, "fuse device", "/dev/fuse is present");
    } else {
        report(
            Status::Fail,
            "fuse device",
            "/dev/fuse not found (load the `fuse` kernel module, or run in a FUSE-capable environment / container with --device /dev/fuse)",
        );
        *failures += 1;
    }

    match fs::read_to_string("/proc/filesystems") {
        // `/proc/filesystems` lists one fs per line; the name is the last field.
        // Match it exactly so "fuseblk"/"fusectl" don't count as "fuse".
        Ok(contents)
            if contents
                .lines()
                .any(|line| line.split_whitespace().last() == Some("fuse")) =>
        {
            report(Status::Ok, "fuse filesystem", "kernel supports fuse");
        }
        Ok(_) => report(
            Status::Warn,
            "fuse filesystem",
            "`fuse` not listed in /proc/filesystems (the module may load on demand)",
        ),
        Err(e) => report(
            Status::Warn,
            "fuse filesystem",
            &format!("could not read /proc/filesystems: {e}"),
        ),
    }
}

fn check_fuse_conf() {
    match fs::read_to_string("/etc/fuse.conf") {
        Ok(contents) => {
            let enabled = contents.lines().any(|l| {
                let t = l.trim();
                !t.starts_with('#') && t == "user_allow_other"
            });
            if enabled {
                report(Status::Ok, "/etc/fuse.conf", "user_allow_other is enabled");
            } else {
                report(
                    Status::Warn,
                    "/etc/fuse.conf",
                    "user_allow_other not enabled (only needed for allow_other mounts)",
                );
            }
        }
        Err(_) => report(
            Status::Warn,
            "/etc/fuse.conf",
            "not found (only needed for allow_other mounts)",
        ),
    }
}

fn check_directories(failures: &mut u32) {
    for (name, path) in [
        ("workspace", config::workspace()),
        ("store_path", config::store_path()),
        ("antares_upper_root", config::antares_upper_root()),
        ("antares_cl_root", config::antares_cl_root()),
        ("antares_mount_root", config::antares_mount_root()),
    ] {
        match check_writable(path) {
            Ok(()) => report(Status::Ok, name, &format!("{path} is writable")),
            Err(e) => {
                report(Status::Fail, name, &format!("{path}: {e}"));
                *failures += 1;
            }
        }
    }
}

fn check_writable(path: &str) -> Result<(), String> {
    let p = Path::new(path);
    if !p.exists() {
        return Err("does not exist".to_string());
    }
    if !p.is_dir() {
        return Err("not a directory".to_string());
    }
    // Probe writability by creating a fresh, uniquely-named file with O_EXCL
    // (`create_new`): this never truncates an existing file and won't follow a
    // symlink for the final component, so we only ever touch a file we created.
    static PROBE_COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = PROBE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let probe = p.join(format!(".scorpio-doctor-probe.{}.{n}", std::process::id()));
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        Ok(_) => {
            let _ = fs::remove_file(&probe);
            Ok(())
        }
        Err(e) => Err(format!("not writable: {e}")),
    }
}

async fn check_mega() {
    let base = config::base_url();
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            report(
                Status::Warn,
                "mega server",
                &format!("could not build HTTP client: {e}"),
            );
            return;
        }
    };
    match client.get(base).send().await {
        Ok(resp) => report(
            Status::Ok,
            "mega server",
            &format!("{base} reachable (HTTP {})", resp.status().as_u16()),
        ),
        Err(e) => report(
            Status::Warn,
            "mega server",
            &format!("{base} unreachable: {e}"),
        ),
    }
}
