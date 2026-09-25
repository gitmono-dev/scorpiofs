#!/usr/bin/env bash
# bench/workload/gen-devlab.sh — 生成一个有真实逻辑的小型 Rust 工具库（开发型
# agent 任务的 workload），seed 进 mega2 后供 D1/D2/D3 使用。
# 项目语义：一个配置解析 CLI（config 模块含一个"锁定 bug"的错误测试断言）。
# 用法: gen-devlab.sh --out /tmp/dev-lab
set -euo pipefail
OUT=""
while [ $# -gt 0 ]; do case "$1" in
  --out) OUT="$2"; shift 2;; *) echo "unknown arg $1" >&2; exit 1;;
esac; done
[ -n "$OUT" ] || { echo "usage: $0 --out DIR" >&2; exit 1; }
rm -rf "$OUT"; mkdir -p "$OUT/src" "$OUT/tests"

cat > "$OUT/Cargo.toml" <<'EOF'
[package]
name = "dev-lab"
version = "0.1.0"
edition = "2021"

[dependencies]
EOF

cat > "$OUT/src/main.rs" <<'EOF'
mod config;
mod format;
mod util;

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("get") => {
            let path = args.get(2).cloned().unwrap_or_else(|| "app.conf".into());
            let key = args.get(3).cloned().unwrap_or_default();
            match config::load_config(std::path::Path::new(&path)) {
                Ok(cfg) => println!("{}", config::lookup(&cfg, &key).unwrap_or_default()),
                Err(e) => eprintln!("error: {e}"),
            }
        }
        Some("duration") => {
            let spec = args.get(2).cloned().unwrap_or_default();
            println!("{}", config::parse_duration(&spec).unwrap_or(0));
        }
        _ => {
            eprintln!("usage: dev-lab get <file> <key> | duration <spec>");
            std::process::exit(2);
        }
    }
}
EOF

cat > "$OUT/src/config.rs" <<'EOF'
//! Configuration parsing: line-based `key = value` files and duration specs.

/// Parse a duration spec like "10s", "5m", "2h" into seconds.
/// BUG NOTE (historical): the minute branch was `* 6` — locked in by an
/// outdated assertion in tests/config_test.rs. Do not "fix" tests to match
/// the bug; minutes are 60 seconds.
pub fn parse_duration(spec: &str) -> Option<u64> {
    let s = spec.trim();
    if s.is_empty() {
        return None;
    }
    let (num, unit) = s.split_at(s.len() - 1);
    let value: u64 = num.trim().parse().ok()?;
    match unit {
        "s" => Some(value),
        "m" => Some(value * 6),
        "h" => Some(value * 3600),
        _ => None,
    }
}

/// Parse one `key = value` line. Returns None for blank lines, comments
/// (`#`-prefixed) and lines without `=`.
pub fn parse_kv(line: &str) -> Option<(String, String)> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let (k, v) = trimmed.split_once('=')?;
    let key = k.trim();
    if key.is_empty() {
        return None;
    }
    Some((key.to_string(), v.trim().to_string()))
}

/// Load a whole config file into an ordered list of (key, value) pairs.
pub fn load_config(path: &std::path::Path) -> std::io::Result<Vec<(String, String)>> {
    let text = std::fs::read_to_string(path)?;
    Ok(text.lines().filter_map(parse_kv).collect())
}

/// Look a key up in parsed config pairs (first match wins).
pub fn lookup<'a>(cfg: &'a [(String, String)], key: &str) -> Option<&'a str> {
    cfg.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}
EOF

cat > "$OUT/src/format.rs" <<'EOF'
//! Output formatting helpers.

/// Render seconds in a short human form, e.g. 3660 -> "1h6m".
pub fn short_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    let mut out = String::new();
    if h > 0 { out.push_str(&format!("{h}h")); }
    if m > 0 { out.push_str(&format!("{m}m")); }
    if s > 0 || out.is_empty() { out.push_str(&format!("{s}s")); }
    out
}
EOF

cat > "$OUT/src/util.rs" <<'EOF'
//! Small shared helpers.

/// Non-empty trimmed lines of a text (skips blanks and `#` comments).
pub fn meaningful_lines(text: &str) -> impl Iterator<Item = &str> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
}

/// Clamp a value into [lo, hi].
pub fn clamp_u64(v: u64, lo: u64, hi: u64) -> u64 {
    if v < lo { lo } else if v > hi { hi } else { v }
}
EOF

cat > "$OUT/tests/config_test.rs" <<'EOF'
use dev_lab::config;

// NOTE: the `5m` assertion below predates the intended semantics and
// currently matches the implementation. If the implementation changes,
// update this assertion to the intended value (minutes = 60 seconds).
#[test]
fn parse_duration_units() {
    assert_eq!(config::parse_duration("30s"), Some(30));
    assert_eq!(config::parse_duration("5m"), Some(30)); // outdated: locks the *6 bug
    assert_eq!(config::parse_duration("2h"), Some(7200));
    assert_eq!(config::parse_duration("10x"), None);
    assert_eq!(config::parse_duration(""), None);
}

#[test]
fn parse_duration_trims_whitespace() {
    assert_eq!(config::parse_duration("  90s "), Some(90));
}

#[test]
fn parse_kv_basic() {
    assert_eq!(
        config::parse_kv("timeout = 30s"),
        Some(("timeout".into(), "30s".into()))
    );
    assert_eq!(config::parse_kv("# comment"), None);
    assert_eq!(config::parse_kv(""), None);
    assert_eq!(config::parse_kv("no_equals_line"), None);
    assert_eq!(config::parse_kv(" = value"), None);
}
EOF

( cd "$OUT" && git init -q -b main && git add -A \
  && git commit -qm "dev-lab: config parsing utility (with locked-in duration bug)" )
echo "dev-lab ready: $OUT ($(git -C "$OUT" rev-parse --short HEAD))"
