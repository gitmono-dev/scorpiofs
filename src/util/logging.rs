//! Centralized logging/tracing initialization for the Scorpio binaries.
//!
//! All runtime diagnostics go through `tracing`. Because the global subscriber
//! installed here also captures records emitted via the `log` crate (the
//! `tracing-log` bridge is enabled by `try_init`), there is a single,
//! consistent log stream regardless of whether a given module uses `tracing::`
//! or `log::` macros.

use tracing_subscriber::EnvFilter;

/// Initialize the global tracing subscriber exactly once.
///
/// The filter directive is chosen by this precedence (highest first):
///
/// 1. `cli_level` — the `--log-level` CLI flag, when provided.
/// 2. `SCORPIO_LOG` environment variable.
/// 3. `RUST_LOG` environment variable.
/// 4. `config_level` — the `log_level` config-file value.
/// 5. `"info"` as a final fallback.
///
/// A malformed directive falls back to `"info"` rather than aborting. Calling
/// this more than once (or after another subscriber is installed, e.g. in
/// tests) is a no-op.
pub fn init(cli_level: Option<&str>, config_level: &str) {
    let directive = cli_level
        .map(str::to_string)
        .filter(|s| !s.trim().is_empty())
        .or_else(|| env_directive("SCORPIO_LOG"))
        .or_else(|| env_directive("RUST_LOG"))
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
            if config_level.trim().is_empty() {
                "info".to_string()
            } else {
                config_level.to_string()
            }
        });

    let filter = EnvFilter::try_new(&directive).unwrap_or_else(|e| {
        // Don't fail startup on a bad directive; warn (once stderr is the only
        // sink available pre-init) and use a sane default.
        eprintln!("invalid log filter {directive:?} ({e}); falling back to \"info\"");
        EnvFilter::new("info")
    });

    // Write diagnostics to stderr so stdout stays clean for command output
    // (e.g. `list`, `config show`, `http-mount`, and generated completions).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .try_init();
}

fn env_directive(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}
