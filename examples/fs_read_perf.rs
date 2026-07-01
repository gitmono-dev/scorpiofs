//! Filesystem read/stat micro-benchmark.
//!
//! Walks a directory tree and reports directory-load, `stat`, and read timings —
//! handy for comparing a native path against a ScorpioFS/FUSE mount.
//!
//! Run with:  `cargo run --release --example fs_read_perf -- <dir>`
//! (Generate a test tree first with `script/run.sh` or `script/run_1000_files.sh`.)

use std::env;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

fn collect_files_recursively(dir: &Path) -> Vec<PathBuf> {
    if dir.file_name() == Some(std::ffi::OsStr::new(".git")) {
        return Vec::new(); // skip .git
    }
    let mut files = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            } else if path.is_dir() {
                files.extend(collect_files_recursively(&path));
            }
        }
    }
    files
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("usage: {} <directory>", args[0]);
        std::process::exit(2);
    }

    let target_dir = Path::new(&args[1]);
    if !target_dir.is_dir() {
        eprintln!("error: {:?} is not a directory", target_dir);
        std::process::exit(2);
    }

    // Time the directory switch + recursive listing.
    let start_cd = Instant::now();
    if let Err(e) = env::set_current_dir(target_dir) {
        eprintln!("error: cannot enter {}: {e}", target_dir.display());
        std::process::exit(1);
    }
    let files = collect_files_recursively(Path::new("."));
    let duration_cd = start_cd.elapsed();

    // Time stat() on every file.
    let start_stat = Instant::now();
    let stat_ok = files.iter().filter_map(|f| fs::metadata(f).ok()).count();
    let duration_stat = start_stat.elapsed();

    // Time reading every file fully.
    let start_read = Instant::now();
    let mut total_bytes = 0u64;
    for f in &files {
        if let Ok(mut file) = File::open(f) {
            let mut buffer = Vec::new();
            if let Ok(bytes_read) = file.read_to_end(&mut buffer) {
                total_bytes += bytes_read as u64;
            }
        }
    }
    let duration_read = start_read.elapsed();

    println!("===== read performance =====");
    println!("files found:    {}", files.len());
    println!("stat succeeded: {stat_ok}");
    println!("list dir:       {duration_cd:.3?}");
    println!(
        "bytes read:     {:.2} MB",
        total_bytes as f64 / (1024.0 * 1024.0)
    );
    println!("stat time:      {duration_stat:.3?}");
    println!("read time:      {duration_read:.3?}");

    if duration_stat.as_secs_f64() > 0.0 {
        println!(
            "stat rate:      {:.2} files/s",
            files.len() as f64 / duration_stat.as_secs_f64()
        );
    }
    if duration_read.as_secs_f64() > 0.0 {
        let throughput = (total_bytes as f64 / (1024.0 * 1024.0)) / duration_read.as_secs_f64();
        println!("read throughput:{throughput:.2} MB/s");
    }
}
