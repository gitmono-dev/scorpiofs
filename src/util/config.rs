use std::{collections::HashMap, fs, path::Path, str::FromStr, sync::OnceLock};

// Configuration error type (using simple String for error messages)
pub type ConfigError = String;

// Result type for configuration operations
pub type ConfigResult<T> = Result<T, ConfigError>;

/// Strongly-typed runtime configuration.
///
/// String fields (paths, URLs) are stored as `String`; numeric/boolean/enum
/// knobs are stored as their concrete type so callers never re-parse at access
/// time. The struct lives for the whole process inside a `OnceLock`, which lets
/// the string accessors keep handing out `&'static str` references (see the
/// dual-`OnceLock` note below).
#[derive(Debug, Clone)]
pub struct ScorpioConfig {
    pub base_url: String,
    pub workspace: String,
    pub store_path: String,
    pub git_author: String,
    pub git_email: String,
    pub config_file: String,
    pub lfs_url: String,
    /// Default tracing filter directive (e.g. "info", "scorpio=debug"). Used by
    /// the binaries when no CLI flag / `SCORPIO_LOG` / `RUST_LOG` is set.
    pub log_level: String,
    pub dicfuse_readable: bool,
    pub load_dir_depth: usize,
    pub fetch_file_thread: usize,
    pub dicfuse_import_concurrency: usize,
    pub dicfuse_dir_sync_ttl_secs: u64,
    pub dicfuse_reply_ttl_secs: u64,
    pub dicfuse_fetch_dir_timeout_secs: u64,
    pub dicfuse_connect_timeout_secs: u64,
    pub dicfuse_fetch_dir_max_retries: u32,
    pub dicfuse_stat_mode: DicfuseStatMode,
    pub dicfuse_open_buff_max_bytes: u64,
    pub dicfuse_open_buff_max_files: usize,
    pub antares_load_dir_depth: usize,
    pub antares_dicfuse_dir_sync_ttl_secs: u64,
    pub antares_dicfuse_reply_ttl_secs: u64,
    pub antares_dicfuse_stat_mode: DicfuseStatMode,
    pub antares_dicfuse_open_buff_max_bytes: u64,
    pub antares_dicfuse_open_buff_max_files: usize,
    pub antares_upper_root: String,
    pub antares_cl_root: String,
    pub antares_mount_root: String,
    pub antares_state_file: String,
}

const DEFAULT_LOAD_DIR_DEPTH: usize = 3;
const DEFAULT_FETCH_FILE_THREAD: usize = 10;
const DEFAULT_ANTARES_SUBDIR: &str = "antares";
const DEFAULT_DICFUSE_IMPORT_CONCURRENCY: usize = 4;

// Dicfuse timeout/cache defaults are tuned for interactive usage.
// Antares uses a separate set of build-oriented defaults below.
//
// TODO(perf):
// - Re-tune TTL/timeout defaults from production lookup metrics.
// - Split timeout knobs by request class (tree listing vs blob fetch).
// - Support per-mount overrides instead of process-global defaults.

/// Directory refresh TTL for base Dicfuse mounts.
const DEFAULT_DICFUSE_DIR_SYNC_TTL_SECS: u64 = 5;

/// Kernel entry TTL for base Dicfuse mounts.
const DEFAULT_DICFUSE_REPLY_TTL_SECS: u64 = 2;

/// Per-request timeout for directory listing RPCs.
const DEFAULT_DICFUSE_FETCH_DIR_TIMEOUT_SECS: u64 = 10;

/// TCP connect timeout for Dicfuse HTTP requests.
const DEFAULT_DICFUSE_CONNECT_TIMEOUT_SECS: u64 = 3;

/// Retry count for transient directory listing failures.
const DEFAULT_DICFUSE_FETCH_DIR_MAX_RETRIES: u32 = 3;

const DEFAULT_DICFUSE_OPEN_BUFF_MAX_BYTES: u64 = 256 * 1024 * 1024; // 256MiB
const DEFAULT_DICFUSE_OPEN_BUFF_MAX_FILES: usize = 4096;

// Antares mounts are primarily used by build workloads.

/// Preload directory depth for Antares mounts.
const DEFAULT_ANTARES_LOAD_DIR_DEPTH: usize = 3;

/// Directory refresh TTL for Antares mounts.
const DEFAULT_ANTARES_DICFUSE_DIR_SYNC_TTL_SECS: u64 = 120;

/// Kernel entry TTL for Antares mounts.
const DEFAULT_ANTARES_DICFUSE_REPLY_TTL_SECS: u64 = 60;

const DEFAULT_ANTARES_DICFUSE_OPEN_BUFF_MAX_BYTES: u64 = 64 * 1024 * 1024; // 64MiB
const DEFAULT_ANTARES_DICFUSE_OPEN_BUFF_MAX_FILES: usize = 1024;

// Global configuration management.
//
// We intentionally use two separate `OnceLock`s instead of a single one:
//
// - `SCORPIO_CONFIG` holds the *explicit* configuration loaded via
//   `init_config()`. This is the source of truth in production.
// - `DEFAULT_CONFIG` is a fallback that is lazily initialized the first
//   time any accessor is called before `init_config()` has succeeded.
//
// Why two locks instead of "init defaults, then replace on init_config"?
//
// Some library consumers (e.g. orion's `warmup_dicfuse`) read a few
// accessors for diagnostic logging *before* they get a chance to call
// `init_config()`. With a single `OnceLock`, that early read would lock
// the defaults in place and the later `init_config(path)` would fail with
// "Configuration already initialized", silently leaving the wrong values
// (e.g. `base_url = http://localhost:8000`) active. Splitting the two
// states lets `init_config()` always take precedence while still keeping
// accessors `&'static str` (both lock contents live for the whole process).
static SCORPIO_CONFIG: OnceLock<ScorpioConfig> = OnceLock::new();
static DEFAULT_CONFIG: OnceLock<ScorpioConfig> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DicfuseStatMode {
    Fast,
    Accurate,
}

/// Built-in defaults used when a config field is absent/empty, and as the whole
/// configuration in the pre-`init_config` fallback path.
fn defaults() -> ScorpioConfig {
    let username = whoami::username();
    // Prefer a writable, container-friendly default. Users can still override via scorpio.toml.
    let base_path = format!("/tmp/megadir-{username}");
    ScorpioConfig {
        base_url: "http://localhost:8000".to_string(),
        workspace: format!("{base_path}/mount"),
        store_path: format!("{base_path}/store"),
        git_author: "MEGA".to_string(),
        git_email: "admin@mega.org".to_string(),
        config_file: "config.toml".to_string(),
        lfs_url: "http://localhost:8000/lfs".to_string(),
        log_level: "info".to_string(),
        dicfuse_readable: true,
        load_dir_depth: DEFAULT_LOAD_DIR_DEPTH,
        fetch_file_thread: DEFAULT_FETCH_FILE_THREAD,
        dicfuse_import_concurrency: DEFAULT_DICFUSE_IMPORT_CONCURRENCY,
        dicfuse_dir_sync_ttl_secs: DEFAULT_DICFUSE_DIR_SYNC_TTL_SECS,
        dicfuse_reply_ttl_secs: DEFAULT_DICFUSE_REPLY_TTL_SECS,
        dicfuse_fetch_dir_timeout_secs: DEFAULT_DICFUSE_FETCH_DIR_TIMEOUT_SECS,
        dicfuse_connect_timeout_secs: DEFAULT_DICFUSE_CONNECT_TIMEOUT_SECS,
        dicfuse_fetch_dir_max_retries: DEFAULT_DICFUSE_FETCH_DIR_MAX_RETRIES,
        dicfuse_stat_mode: DicfuseStatMode::Accurate,
        dicfuse_open_buff_max_bytes: DEFAULT_DICFUSE_OPEN_BUFF_MAX_BYTES,
        dicfuse_open_buff_max_files: DEFAULT_DICFUSE_OPEN_BUFF_MAX_FILES,
        antares_load_dir_depth: DEFAULT_ANTARES_LOAD_DIR_DEPTH,
        antares_dicfuse_dir_sync_ttl_secs: DEFAULT_ANTARES_DICFUSE_DIR_SYNC_TTL_SECS,
        antares_dicfuse_reply_ttl_secs: DEFAULT_ANTARES_DICFUSE_REPLY_TTL_SECS,
        antares_dicfuse_stat_mode: DicfuseStatMode::Fast,
        antares_dicfuse_open_buff_max_bytes: DEFAULT_ANTARES_DICFUSE_OPEN_BUFF_MAX_BYTES,
        antares_dicfuse_open_buff_max_files: DEFAULT_ANTARES_DICFUSE_OPEN_BUFF_MAX_FILES,
        antares_upper_root: format!("{base_path}/{DEFAULT_ANTARES_SUBDIR}/upper"),
        antares_cl_root: format!("{base_path}/{DEFAULT_ANTARES_SUBDIR}/cl"),
        antares_mount_root: format!("{base_path}/{DEFAULT_ANTARES_SUBDIR}/mnt"),
        antares_state_file: format!("{base_path}/{DEFAULT_ANTARES_SUBDIR}/state.toml"),
    }
}

/// Resolves a single configuration value across all sources, honoring the
/// precedence `CLI > environment (SCORPIO_*) > config file > default`.
///
/// The config file is read in two compatible forms:
/// - the new section format, e.g. `[dicfuse] load_dir_depth = 5`, and
/// - the legacy flat keys, e.g. `load_dir_depth = "5"` (string or native scalar).
///
/// When both forms set the same key, the section value wins and a deprecation
/// warning is logged for the flat key.
struct RawResolver {
    file: toml::Table,
    cli: HashMap<String, String>,
}

impl RawResolver {
    fn new(file: toml::Table, cli: HashMap<String, String>) -> Self {
        Self { file, cli }
    }

    /// Returns the highest-priority non-empty raw string for `flat_key`.
    ///
    /// `section`/`short` name the equivalent location in the new sectioned
    /// format; pass an empty `section` for keys that only exist as flat keys.
    ///
    /// A config-file value that is present but not a scalar (an array/table/
    /// datetime where a string/number/bool is expected) is a hard error rather
    /// than silently falling back to the default.
    fn get(&self, flat_key: &str, section: &str, short: &str) -> ConfigResult<Option<String>> {
        // 1. CLI overrides (highest priority).
        if let Some(v) = self.cli.get(flat_key) {
            if !v.trim().is_empty() {
                return Ok(Some(v.clone()));
            }
        }

        // 2. Environment variable: SCORPIO_<FLAT_KEY_UPPERCASE>.
        let env_key = format!("SCORPIO_{}", flat_key.to_ascii_uppercase());
        if let Ok(v) = std::env::var(&env_key) {
            if !v.trim().is_empty() {
                return Ok(Some(v));
            }
        }

        // 3. Config file: new section form, then legacy flat key.
        let section_val = self.section_get(section, short)?;
        let flat_val = self.flat_get(flat_key)?;
        if section_val.is_some() && flat_val.is_some() {
            tracing::warn!(
                "config key '{flat_key}' set both as a flat key and as [{section}].{short}; \
                 using the [{section}] value (flat keys are deprecated)"
            );
        }
        Ok(section_val.or(flat_val))
    }

    fn section_get(&self, section: &str, short: &str) -> ConfigResult<Option<String>> {
        if section.is_empty() {
            return Ok(None);
        }
        let Some(value) = self.file.get(section) else {
            return Ok(None);
        };
        // A malformed section (e.g. `server = "x"` instead of `[server]`) is
        // treated as absent; the flat-key fallback still applies.
        let Some(table) = value.as_table() else {
            return Ok(None);
        };
        match table.get(short) {
            Some(v) => extract_scalar(v, &format!("[{section}].{short}")),
            None => Ok(None),
        }
    }

    fn flat_get(&self, flat_key: &str) -> ConfigResult<Option<String>> {
        match self.file.get(flat_key) {
            Some(v) => extract_scalar(v, flat_key),
            None => Ok(None),
        }
    }
}

/// Converts a scalar TOML value to its string form.
///
/// - Empty/whitespace strings yield `Ok(None)` so an explicitly-empty value
///   falls through to the default.
/// - Non-scalar values (array/table/datetime) yield an `Err` naming the key, so
///   a present-but-wrong-type value is rejected at startup instead of being
///   silently defaulted.
fn extract_scalar(value: &toml::Value, key_desc: &str) -> ConfigResult<Option<String>> {
    let s = match value {
        toml::Value::String(s) => s.clone(),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Datetime(_) => {
            return Err(format!(
                "Invalid value for '{key_desc}': expected a scalar (string/number/bool), got a datetime"
            ))
        }
        toml::Value::Array(_) => {
            return Err(format!(
                "Invalid value for '{key_desc}': expected a scalar (string/number/bool), got an array"
            ))
        }
        toml::Value::Table(_) => {
            return Err(format!(
                "Invalid value for '{key_desc}': expected a scalar (string/number/bool), got a table"
            ))
        }
    };
    if s.trim().is_empty() {
        Ok(None)
    } else {
        Ok(Some(s))
    }
}

fn optional_string(
    r: &RawResolver,
    flat: &str,
    section: &str,
    short: &str,
    default: String,
) -> ConfigResult<String> {
    Ok(r.get(flat, section, short)?.unwrap_or(default))
}

fn parse_number<T>(
    r: &RawResolver,
    flat: &str,
    section: &str,
    short: &str,
    default: T,
) -> ConfigResult<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    match r.get(flat, section, short)? {
        Some(v) => v.trim().parse::<T>().map_err(|e| {
            format!(
                "Invalid value for '{flat}': {:?} is not a valid {} ({e})",
                v,
                std::any::type_name::<T>()
            )
        }),
        None => Ok(default),
    }
}

fn parse_bool(
    r: &RawResolver,
    flat: &str,
    section: &str,
    short: &str,
    default: bool,
) -> ConfigResult<bool> {
    match r.get(flat, section, short)? {
        Some(v) => match v.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            other => Err(format!(
                "Invalid boolean for '{flat}': expected 'true' or 'false', got {other:?}"
            )),
        },
        None => Ok(default),
    }
}

fn parse_stat_mode(
    r: &RawResolver,
    flat: &str,
    section: &str,
    short: &str,
    default: DicfuseStatMode,
) -> ConfigResult<DicfuseStatMode> {
    match r.get(flat, section, short)? {
        Some(v) => match v.trim().to_ascii_lowercase().as_str() {
            "fast" => Ok(DicfuseStatMode::Fast),
            "accurate" => Ok(DicfuseStatMode::Accurate),
            other => Err(format!(
                "Invalid stat mode for '{flat}': expected 'fast' or 'accurate', got {other:?}"
            )),
        },
        None => Ok(default),
    }
}

/// Resolve every field from the layered sources and parse it into its concrete
/// type. Type errors (a misspelled number/bool/enum) are reported here with the
/// offending field name, before the service starts.
fn build_config(r: &RawResolver) -> ConfigResult<ScorpioConfig> {
    let d = defaults();

    let cfg = ScorpioConfig {
        // base_url / lfs_url default to a local dev server when omitted; the
        // resolved value is still URL-validated below.
        base_url: optional_string(r, "base_url", "server", "base_url", d.base_url)?,
        lfs_url: optional_string(r, "lfs_url", "server", "lfs_url", d.lfs_url)?,
        workspace: optional_string(r, "workspace", "server", "workspace", d.workspace)?,
        store_path: optional_string(r, "store_path", "server", "store_path", d.store_path)?,
        git_author: optional_string(r, "git_author", "server", "git_author", d.git_author)?,
        git_email: optional_string(r, "git_email", "server", "git_email", d.git_email)?,
        config_file: optional_string(r, "config_file", "server", "config_file", d.config_file)?,
        log_level: optional_string(r, "log_level", "server", "log_level", d.log_level)?,
        dicfuse_readable: parse_bool(
            r,
            "dicfuse_readable",
            "dicfuse",
            "readable",
            d.dicfuse_readable,
        )?,
        load_dir_depth: parse_number(
            r,
            "load_dir_depth",
            "dicfuse",
            "load_dir_depth",
            d.load_dir_depth,
        )?,
        fetch_file_thread: parse_number(
            r,
            "fetch_file_thread",
            "dicfuse",
            "fetch_file_thread",
            d.fetch_file_thread,
        )?,
        dicfuse_import_concurrency: parse_number(
            r,
            "dicfuse_import_concurrency",
            "dicfuse",
            "import_concurrency",
            d.dicfuse_import_concurrency,
        )?,
        dicfuse_dir_sync_ttl_secs: parse_number(
            r,
            "dicfuse_dir_sync_ttl_secs",
            "dicfuse",
            "dir_sync_ttl_secs",
            d.dicfuse_dir_sync_ttl_secs,
        )?,
        dicfuse_reply_ttl_secs: parse_number(
            r,
            "dicfuse_reply_ttl_secs",
            "dicfuse",
            "reply_ttl_secs",
            d.dicfuse_reply_ttl_secs,
        )?,
        dicfuse_fetch_dir_timeout_secs: parse_number(
            r,
            "dicfuse_fetch_dir_timeout_secs",
            "dicfuse",
            "fetch_dir_timeout_secs",
            d.dicfuse_fetch_dir_timeout_secs,
        )?,
        dicfuse_connect_timeout_secs: parse_number(
            r,
            "dicfuse_connect_timeout_secs",
            "dicfuse",
            "connect_timeout_secs",
            d.dicfuse_connect_timeout_secs,
        )?,
        dicfuse_fetch_dir_max_retries: parse_number(
            r,
            "dicfuse_fetch_dir_max_retries",
            "dicfuse",
            "fetch_dir_max_retries",
            d.dicfuse_fetch_dir_max_retries,
        )?,
        dicfuse_stat_mode: parse_stat_mode(
            r,
            "dicfuse_stat_mode",
            "dicfuse",
            "stat_mode",
            d.dicfuse_stat_mode,
        )?,
        dicfuse_open_buff_max_bytes: parse_number(
            r,
            "dicfuse_open_buff_max_bytes",
            "dicfuse",
            "open_buff_max_bytes",
            d.dicfuse_open_buff_max_bytes,
        )?,
        dicfuse_open_buff_max_files: parse_number(
            r,
            "dicfuse_open_buff_max_files",
            "dicfuse",
            "open_buff_max_files",
            d.dicfuse_open_buff_max_files,
        )?,
        antares_load_dir_depth: parse_number(
            r,
            "antares_load_dir_depth",
            "antares",
            "load_dir_depth",
            d.antares_load_dir_depth,
        )?,
        antares_dicfuse_dir_sync_ttl_secs: parse_number(
            r,
            "antares_dicfuse_dir_sync_ttl_secs",
            "antares",
            "dir_sync_ttl_secs",
            d.antares_dicfuse_dir_sync_ttl_secs,
        )?,
        antares_dicfuse_reply_ttl_secs: parse_number(
            r,
            "antares_dicfuse_reply_ttl_secs",
            "antares",
            "reply_ttl_secs",
            d.antares_dicfuse_reply_ttl_secs,
        )?,
        antares_dicfuse_stat_mode: parse_stat_mode(
            r,
            "antares_dicfuse_stat_mode",
            "antares",
            "stat_mode",
            d.antares_dicfuse_stat_mode,
        )?,
        antares_dicfuse_open_buff_max_bytes: parse_number(
            r,
            "antares_dicfuse_open_buff_max_bytes",
            "antares",
            "open_buff_max_bytes",
            d.antares_dicfuse_open_buff_max_bytes,
        )?,
        antares_dicfuse_open_buff_max_files: parse_number(
            r,
            "antares_dicfuse_open_buff_max_files",
            "antares",
            "open_buff_max_files",
            d.antares_dicfuse_open_buff_max_files,
        )?,
        antares_upper_root: optional_string(
            r,
            "antares_upper_root",
            "antares",
            "upper_root",
            d.antares_upper_root,
        )?,
        antares_cl_root: optional_string(
            r,
            "antares_cl_root",
            "antares",
            "cl_root",
            d.antares_cl_root,
        )?,
        antares_mount_root: optional_string(
            r,
            "antares_mount_root",
            "antares",
            "mount_root",
            d.antares_mount_root,
        )?,
        antares_state_file: optional_string(
            r,
            "antares_state_file",
            "antares",
            "state_file",
            d.antares_state_file,
        )?,
    };

    validate(&cfg)?;
    Ok(cfg)
}

fn validate_url(field: &str, value: &str) -> ConfigResult<()> {
    let parsed = url::Url::parse(value)
        .map_err(|e| format!("Invalid URL for '{field}': {value:?} ({e})"))?;
    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(format!(
                "Invalid URL scheme for '{field}': expected http/https, got {other:?} in {value:?}"
            ))
        }
    }
    if parsed.host_str().map(|h| h.is_empty()).unwrap_or(true) {
        return Err(format!(
            "Invalid URL for '{field}': missing host in {value:?}"
        ));
    }
    Ok(())
}

/// Lightweight, start-up-time validation. Deep checks (path writability,
/// remote reachability) are intentionally left to `config validate --deep`
/// / `doctor` so that cold start stays fast.
fn validate(cfg: &ScorpioConfig) -> ConfigResult<()> {
    validate_url("base_url", &cfg.base_url)?;
    validate_url("lfs_url", &cfg.lfs_url)?;

    let required_paths = [
        ("workspace", &cfg.workspace),
        ("store_path", &cfg.store_path),
        ("git_author", &cfg.git_author),
        ("git_email", &cfg.git_email),
        ("config_file", &cfg.config_file),
        ("antares_upper_root", &cfg.antares_upper_root),
        ("antares_cl_root", &cfg.antares_cl_root),
        ("antares_mount_root", &cfg.antares_mount_root),
        ("antares_state_file", &cfg.antares_state_file),
    ];
    for (name, value) in required_paths {
        if value.trim().is_empty() {
            return Err(format!("Missing or empty required config: {name}"));
        }
    }

    // Numeric lower bounds: values that must be >= 1 to make sense.
    let positive = [
        ("load_dir_depth", cfg.load_dir_depth),
        ("fetch_file_thread", cfg.fetch_file_thread),
        ("dicfuse_import_concurrency", cfg.dicfuse_import_concurrency),
        (
            "dicfuse_open_buff_max_files",
            cfg.dicfuse_open_buff_max_files,
        ),
        ("antares_load_dir_depth", cfg.antares_load_dir_depth),
        (
            "antares_dicfuse_open_buff_max_files",
            cfg.antares_dicfuse_open_buff_max_files,
        ),
    ];
    for (name, value) in positive {
        if value < 1 {
            return Err(format!(
                "Invalid config '{name}': must be >= 1, got {value}"
            ));
        }
    }

    let positive_u64 = [
        (
            "dicfuse_fetch_dir_timeout_secs",
            cfg.dicfuse_fetch_dir_timeout_secs,
        ),
        (
            "dicfuse_connect_timeout_secs",
            cfg.dicfuse_connect_timeout_secs,
        ),
    ];
    for (name, value) in positive_u64 {
        if value < 1 {
            return Err(format!(
                "Invalid config '{name}': must be >= 1, got {value}"
            ));
        }
    }

    Ok(())
}

// --- Collect-all validation (for `scorpio config validate`) -----------------
//
// Unlike `build_config`/`validate` (which short-circuit on the first error so
// the daemon fails fast at startup), these helpers resolve every field and
// accumulate ALL problems so `config validate` can report them in one pass.

fn collect_str(
    r: &RawResolver,
    flat: &str,
    section: &str,
    short: &str,
    default: &str,
    errs: &mut Vec<String>,
) -> String {
    match r.get(flat, section, short) {
        Ok(Some(v)) => v,
        Ok(None) => default.to_string(),
        Err(e) => {
            errs.push(e);
            default.to_string()
        }
    }
}

fn collect_num<T>(
    r: &RawResolver,
    flat: &str,
    section: &str,
    short: &str,
    default: T,
    errs: &mut Vec<String>,
) -> T
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    match r.get(flat, section, short) {
        Ok(Some(v)) => match v.trim().parse::<T>() {
            Ok(x) => x,
            Err(e) => {
                errs.push(format!(
                    "Invalid value for '{flat}': {:?} is not a valid {} ({e})",
                    v,
                    std::any::type_name::<T>()
                ));
                default
            }
        },
        Ok(None) => default,
        Err(e) => {
            errs.push(e);
            default
        }
    }
}

fn collect_bool(
    r: &RawResolver,
    flat: &str,
    section: &str,
    short: &str,
    default: bool,
    errs: &mut Vec<String>,
) -> bool {
    match parse_bool(r, flat, section, short, default) {
        Ok(v) => v,
        Err(e) => {
            errs.push(e);
            default
        }
    }
}

fn collect_mode(
    r: &RawResolver,
    flat: &str,
    section: &str,
    short: &str,
    default: DicfuseStatMode,
    errs: &mut Vec<String>,
) -> DicfuseStatMode {
    match parse_stat_mode(r, flat, section, short, default) {
        Ok(v) => v,
        Err(e) => {
            errs.push(e);
            default
        }
    }
}

/// Validate a config file offline, returning **all** problems found.
///
/// Reports parse errors, type errors, range violations, malformed URLs, and
/// missing required fields. Used by `scorpio config validate`.
pub fn validate_file(
    path: &str,
    cli_overrides: HashMap<String, String>,
) -> Result<(), Vec<String>> {
    let content = fs::read_to_string(path)
        .map_err(|e| vec![format!("Config file not found at '{path}': {e}")])?;
    let file: toml::Table = toml::from_str(&content)
        .map_err(|e| vec![format!("Invalid config format in '{path}': {e}")])?;

    let r = RawResolver::new(file, cli_overrides);
    let d = defaults();
    let mut errs = Vec::new();

    // Resolve every field, accumulating type/parse errors. Fields that fail to
    // parse fall back to their (valid) default so a single bad field does not
    // mask later checks.
    let base_url = collect_str(&r, "base_url", "server", "base_url", &d.base_url, &mut errs);
    let lfs_url = collect_str(&r, "lfs_url", "server", "lfs_url", &d.lfs_url, &mut errs);
    let workspace = collect_str(
        &r,
        "workspace",
        "server",
        "workspace",
        &d.workspace,
        &mut errs,
    );
    let store_path = collect_str(
        &r,
        "store_path",
        "server",
        "store_path",
        &d.store_path,
        &mut errs,
    );
    let git_author = collect_str(
        &r,
        "git_author",
        "server",
        "git_author",
        &d.git_author,
        &mut errs,
    );
    let git_email = collect_str(
        &r,
        "git_email",
        "server",
        "git_email",
        &d.git_email,
        &mut errs,
    );
    let config_file = collect_str(
        &r,
        "config_file",
        "server",
        "config_file",
        &d.config_file,
        &mut errs,
    );
    let _log_level = collect_str(
        &r,
        "log_level",
        "server",
        "log_level",
        &d.log_level,
        &mut errs,
    );
    let _readable = collect_bool(
        &r,
        "dicfuse_readable",
        "dicfuse",
        "readable",
        d.dicfuse_readable,
        &mut errs,
    );
    let load_dir_depth = collect_num(
        &r,
        "load_dir_depth",
        "dicfuse",
        "load_dir_depth",
        d.load_dir_depth,
        &mut errs,
    );
    let fetch_file_thread = collect_num(
        &r,
        "fetch_file_thread",
        "dicfuse",
        "fetch_file_thread",
        d.fetch_file_thread,
        &mut errs,
    );
    let dicfuse_import_concurrency = collect_num(
        &r,
        "dicfuse_import_concurrency",
        "dicfuse",
        "import_concurrency",
        d.dicfuse_import_concurrency,
        &mut errs,
    );
    let dicfuse_connect_timeout_secs = collect_num(
        &r,
        "dicfuse_connect_timeout_secs",
        "dicfuse",
        "connect_timeout_secs",
        d.dicfuse_connect_timeout_secs,
        &mut errs,
    );
    let dicfuse_fetch_dir_timeout_secs = collect_num(
        &r,
        "dicfuse_fetch_dir_timeout_secs",
        "dicfuse",
        "fetch_dir_timeout_secs",
        d.dicfuse_fetch_dir_timeout_secs,
        &mut errs,
    );
    let dicfuse_open_buff_max_files = collect_num(
        &r,
        "dicfuse_open_buff_max_files",
        "dicfuse",
        "open_buff_max_files",
        d.dicfuse_open_buff_max_files,
        &mut errs,
    );
    let _dicfuse_stat_mode = collect_mode(
        &r,
        "dicfuse_stat_mode",
        "dicfuse",
        "stat_mode",
        d.dicfuse_stat_mode,
        &mut errs,
    );
    let antares_load_dir_depth = collect_num(
        &r,
        "antares_load_dir_depth",
        "antares",
        "load_dir_depth",
        d.antares_load_dir_depth,
        &mut errs,
    );
    let antares_dicfuse_open_buff_max_files = collect_num(
        &r,
        "antares_dicfuse_open_buff_max_files",
        "antares",
        "open_buff_max_files",
        d.antares_dicfuse_open_buff_max_files,
        &mut errs,
    );
    let _antares_stat_mode = collect_mode(
        &r,
        "antares_dicfuse_stat_mode",
        "antares",
        "stat_mode",
        d.antares_dicfuse_stat_mode,
        &mut errs,
    );
    let antares_upper_root = collect_str(
        &r,
        "antares_upper_root",
        "antares",
        "upper_root",
        &d.antares_upper_root,
        &mut errs,
    );
    let antares_cl_root = collect_str(
        &r,
        "antares_cl_root",
        "antares",
        "cl_root",
        &d.antares_cl_root,
        &mut errs,
    );
    let antares_mount_root = collect_str(
        &r,
        "antares_mount_root",
        "antares",
        "mount_root",
        &d.antares_mount_root,
        &mut errs,
    );
    let antares_state_file = collect_str(
        &r,
        "antares_state_file",
        "antares",
        "state_file",
        &d.antares_state_file,
        &mut errs,
    );

    // Remaining typed knobs without a range check: still type-checked here so a
    // malformed value cannot pass `config validate` yet fail at startup.
    let _ = collect_num(
        &r,
        "dicfuse_dir_sync_ttl_secs",
        "dicfuse",
        "dir_sync_ttl_secs",
        d.dicfuse_dir_sync_ttl_secs,
        &mut errs,
    );
    let _ = collect_num(
        &r,
        "dicfuse_reply_ttl_secs",
        "dicfuse",
        "reply_ttl_secs",
        d.dicfuse_reply_ttl_secs,
        &mut errs,
    );
    let _ = collect_num(
        &r,
        "dicfuse_fetch_dir_max_retries",
        "dicfuse",
        "fetch_dir_max_retries",
        d.dicfuse_fetch_dir_max_retries,
        &mut errs,
    );
    let _ = collect_num(
        &r,
        "dicfuse_open_buff_max_bytes",
        "dicfuse",
        "open_buff_max_bytes",
        d.dicfuse_open_buff_max_bytes,
        &mut errs,
    );
    let _ = collect_num(
        &r,
        "antares_dicfuse_dir_sync_ttl_secs",
        "antares",
        "dir_sync_ttl_secs",
        d.antares_dicfuse_dir_sync_ttl_secs,
        &mut errs,
    );
    let _ = collect_num(
        &r,
        "antares_dicfuse_reply_ttl_secs",
        "antares",
        "reply_ttl_secs",
        d.antares_dicfuse_reply_ttl_secs,
        &mut errs,
    );
    let _ = collect_num(
        &r,
        "antares_dicfuse_open_buff_max_bytes",
        "antares",
        "open_buff_max_bytes",
        d.antares_dicfuse_open_buff_max_bytes,
        &mut errs,
    );

    // URL format.
    if let Err(e) = validate_url("base_url", &base_url) {
        errs.push(e);
    }
    if let Err(e) = validate_url("lfs_url", &lfs_url) {
        errs.push(e);
    }

    // Required non-empty fields.
    let required = [
        ("workspace", &workspace),
        ("store_path", &store_path),
        ("git_author", &git_author),
        ("git_email", &git_email),
        ("config_file", &config_file),
        ("antares_upper_root", &antares_upper_root),
        ("antares_cl_root", &antares_cl_root),
        ("antares_mount_root", &antares_mount_root),
        ("antares_state_file", &antares_state_file),
    ];
    for (name, value) in required {
        if value.trim().is_empty() {
            errs.push(format!("Missing or empty required config: {name}"));
        }
    }

    // Numeric lower bounds (only for fields that parsed; failed parses already
    // reported and replaced by valid defaults).
    let positive = [
        ("load_dir_depth", load_dir_depth),
        ("fetch_file_thread", fetch_file_thread),
        ("dicfuse_import_concurrency", dicfuse_import_concurrency),
        ("dicfuse_open_buff_max_files", dicfuse_open_buff_max_files),
        ("antares_load_dir_depth", antares_load_dir_depth),
        (
            "antares_dicfuse_open_buff_max_files",
            antares_dicfuse_open_buff_max_files,
        ),
    ];
    for (name, value) in positive {
        if value < 1 {
            errs.push(format!(
                "Invalid config '{name}': must be >= 1, got {value}"
            ));
        }
    }
    for (name, value) in [
        ("dicfuse_connect_timeout_secs", dicfuse_connect_timeout_secs),
        (
            "dicfuse_fetch_dir_timeout_secs",
            dicfuse_fetch_dir_timeout_secs,
        ),
    ] {
        if value < 1 {
            errs.push(format!(
                "Invalid config '{name}': must be >= 1, got {value}"
            ));
        }
    }

    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs)
    }
}

/// Render the effective (merged) configuration for `scorpio config show`.
///
/// Reads the process-wide config (after `init_config`). No fields are currently
/// secret; absolute paths are shown in full. If token/secret fields are added
/// later, redact them here.
pub fn effective_config_dump() -> String {
    format!("{:#?}", get_config())
}

/// Create the runtime directories and ensure the state file exists.
///
/// Unlike the previous implementation, this never rewrites the user's main
/// config file (`scorpio.toml`). The main config is treated as read-only input;
/// only runtime outputs (mount/store dirs and the `config_file` state store)
/// are created on demand.
fn ensure_runtime_paths(cfg: &ScorpioConfig) -> ConfigResult<()> {
    for path in [
        cfg.workspace.as_str(),
        cfg.store_path.as_str(),
        cfg.antares_upper_root.as_str(),
        cfg.antares_cl_root.as_str(),
        cfg.antares_mount_root.as_str(),
    ] {
        create_dir_all(Path::new(path))?;
    }

    if let Some(parent) = Path::new(&cfg.antares_state_file).parent() {
        create_dir_all(parent)?;
    }

    // The `config_file` holds runtime mount state, not user config; create an
    // empty state store if missing so the first run is self-contained.
    if !Path::new(&cfg.config_file).exists() {
        fs::write(&cfg.config_file, "works = []\n")
            .map_err(|e| format!("Failed to create state file '{}': {e}", cfg.config_file))?;
    }
    Ok(())
}

fn create_dir_all(path: &Path) -> ConfigResult<()> {
    if let Err(e) = fs::create_dir_all(path) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(format!(
                "Failed to create directory {}: {e}",
                path.display()
            ));
        }
    }
    Ok(())
}

/// Initialize global configuration from a TOML file.
///
/// Equivalent to [`init_config_with`] with no CLI overrides. Environment
/// variables (`SCORPIO_*`) are still applied.
///
/// Returns `Err("Configuration already initialized")` if `init_config` has
/// previously succeeded — calling it twice is treated as a programmer error.
///
/// It is, however, safe to call `init_config()` *after* an accessor has
/// already triggered the lazy `DEFAULT_CONFIG` fallback in `get_config()`.
/// In that case the defaults remain in `DEFAULT_CONFIG` (kept alive so
/// previously handed-out `&'static str` references stay valid) and the
/// freshly loaded values are installed into `SCORPIO_CONFIG`. All subsequent
/// reads see the explicit values.
pub fn init_config(path: &str) -> ConfigResult<()> {
    init_config_with(path, HashMap::new())
}

/// Initialize global configuration from a TOML file, applying CLI overrides.
///
/// Resolution precedence is `CLI overrides > SCORPIO_* env > config file >
/// built-in defaults`. The main config file is never rewritten.
pub fn init_config_with(path: &str, cli_overrides: HashMap<String, String>) -> ConfigResult<()> {
    // Bail out before any filesystem side effects if config is already set.
    if SCORPIO_CONFIG.get().is_some() {
        return Err("Configuration already initialized".to_string());
    }

    let content =
        fs::read_to_string(path).map_err(|e| format!("Config file not found at '{path}': {e}"))?;

    let file: toml::Table =
        toml::from_str(&content).map_err(|e| format!("Invalid config format in '{path}': {e}"))?;

    let resolver = RawResolver::new(file, cli_overrides);
    let cfg = build_config(&resolver)?;
    ensure_runtime_paths(&cfg)?;

    SCORPIO_CONFIG
        .set(cfg)
        .map_err(|_| "Configuration already initialized".to_string())
}

/// Get reference to global configuration.
///
/// Resolution order:
/// 1. The explicit config installed by `init_config()` (production path).
/// 2. A lazily-built default config (`DEFAULT_CONFIG`) if no explicit config
///    has been installed yet. This keeps tests and ad-hoc usage runnable
///    without forcing every caller to call `init_config()`.
///
/// Critically, the default config is stored in its own `OnceLock`, so a later
/// successful `init_config()` call will still take effect: subsequent reads see
/// the explicit values via branch (1), and any `&'static str` references handed
/// out from branch (2) remain valid (both maps live for the program lifetime).
///
/// The fallback never panics: directory preparation failures are logged at
/// `warn` level instead of aborting the process.
fn get_config() -> &'static ScorpioConfig {
    if let Some(cfg) = SCORPIO_CONFIG.get() {
        return cfg;
    }

    DEFAULT_CONFIG.get_or_init(|| {
        tracing::debug!(
            "Scorpio config accessed before init_config(); using built-in defaults. \
             A later init_config() will override these values."
        );
        let cfg = defaults();
        if let Err(e) = ensure_runtime_paths(&cfg) {
            tracing::warn!("Failed to prepare default runtime paths: {e}");
        }
        cfg
    })
}

pub fn base_url() -> &'static str {
    get_config().base_url.as_str()
}

pub fn workspace() -> &'static str {
    get_config().workspace.as_str()
}

pub fn store_path() -> &'static str {
    get_config().store_path.as_str()
}

pub fn git_author() -> &'static str {
    get_config().git_author.as_str()
}

pub fn git_email() -> &'static str {
    get_config().git_email.as_str()
}

pub fn file_blob_endpoint() -> String {
    format!("{}/api/v1/file/blob", base_url())
}
pub fn tree_file_endpoint() -> String {
    format!("{}/api/v1/file/tree?path=/", base_url())
}
pub fn config_file() -> &'static str {
    get_config().config_file.as_str()
}
pub fn lfs_url() -> &'static str {
    get_config().lfs_url.as_str()
}
pub fn log_level() -> &'static str {
    get_config().log_level.as_str()
}
pub fn dicfuse_readable() -> bool {
    get_config().dicfuse_readable
}

pub fn antares_upper_root() -> &'static str {
    get_config().antares_upper_root.as_str()
}

pub fn antares_cl_root() -> &'static str {
    get_config().antares_cl_root.as_str()
}

pub fn antares_mount_root() -> &'static str {
    get_config().antares_mount_root.as_str()
}

pub fn antares_state_file() -> &'static str {
    get_config().antares_state_file.as_str()
}

pub fn load_dir_depth() -> usize {
    get_config().load_dir_depth
}

pub fn fetch_file_thread() -> usize {
    get_config().fetch_file_thread
}

pub fn dicfuse_import_concurrency() -> usize {
    get_config().dicfuse_import_concurrency
}

pub fn dicfuse_dir_sync_ttl_secs() -> u64 {
    get_config().dicfuse_dir_sync_ttl_secs
}

pub fn dicfuse_reply_ttl_secs() -> u64 {
    get_config().dicfuse_reply_ttl_secs
}

pub fn dicfuse_fetch_dir_timeout_secs() -> u64 {
    get_config().dicfuse_fetch_dir_timeout_secs
}

pub fn dicfuse_connect_timeout_secs() -> u64 {
    get_config().dicfuse_connect_timeout_secs
}

pub fn dicfuse_fetch_dir_max_retries() -> u32 {
    get_config().dicfuse_fetch_dir_max_retries
}

pub fn dicfuse_stat_mode() -> DicfuseStatMode {
    get_config().dicfuse_stat_mode
}

pub fn dicfuse_open_buff_max_bytes() -> u64 {
    get_config().dicfuse_open_buff_max_bytes
}

pub fn dicfuse_open_buff_max_files() -> usize {
    get_config().dicfuse_open_buff_max_files
}

pub fn antares_load_dir_depth() -> usize {
    get_config().antares_load_dir_depth
}

pub fn antares_dicfuse_dir_sync_ttl_secs() -> u64 {
    get_config().antares_dicfuse_dir_sync_ttl_secs
}

pub fn antares_dicfuse_reply_ttl_secs() -> u64 {
    get_config().antares_dicfuse_reply_ttl_secs
}

pub fn antares_dicfuse_stat_mode() -> DicfuseStatMode {
    get_config().antares_dicfuse_stat_mode
}

pub fn antares_dicfuse_open_buff_max_bytes() -> u64 {
    get_config().antares_dicfuse_open_buff_max_bytes
}

pub fn antares_dicfuse_open_buff_max_files() -> usize {
    get_config().antares_dicfuse_open_buff_max_files
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(src: &str) -> toml::Table {
        toml::from_str(src).expect("valid toml")
    }

    fn resolver(src: &str) -> RawResolver {
        RawResolver::new(table(src), HashMap::new())
    }

    #[test]
    fn test_get_url() {
        let config_content = r#"
        base_url = "http://localhost:8000"
        workspace = ""
        store_path = ""
        git_author = "MEGA"
        git_email = "admin@mega.org"
        config_file = "config.toml"
        lfs_url = "http://localhost:8000/lfs"
        dicfuse_readable = "true"
        load_dir_depth = "3"
        fetch_file_thread = "10"
        antares_upper_root = ""
        antares_cl_root = ""
        antares_mount_root = ""
        antares_state_file = ""
        "#;
        let config_path = "/tmp/scorpio.toml";
        std::fs::write(config_path, config_content).expect("Failed to write test config file");
        match init_config(config_path) {
            Ok(()) => {
                assert_eq!(base_url(), "http://localhost:8000");
                assert_eq!(
                    workspace(),
                    format!("/tmp/megadir-{}/mount", whoami::username())
                );
                assert_eq!(
                    store_path(),
                    format!("/tmp/megadir-{}/store", whoami::username())
                );
                assert_eq!(git_author(), "MEGA");
                assert_eq!(git_email(), "admin@mega.org");
                assert_eq!(
                    file_blob_endpoint(),
                    "http://localhost:8000/api/v1/file/blob"
                );
                assert_eq!(load_dir_depth(), 3);
                assert_eq!(fetch_file_thread(), 10);
                assert_eq!(config_file(), "config.toml");
                assert!(antares_upper_root().ends_with("/antares/upper"));
                assert!(antares_cl_root().ends_with("/antares/cl"));
                assert!(antares_mount_root().ends_with("/antares/mnt"));
                assert!(antares_state_file().ends_with("/antares/state.toml"));
            }
            Err(e) if e.contains("already initialized") => {
                // Other tests may have initialized the global config first; assert basic invariants.
                assert!(!base_url().is_empty());
                assert!(!workspace().is_empty());
                assert!(!store_path().is_empty());
                assert!(file_blob_endpoint().starts_with(base_url()));
            }
            Err(e) => panic!("Failed to load config: {e}"),
        }
    }

    #[test]
    fn empty_flat_value_falls_back_to_default() {
        let r = resolver(
            r#"
            base_url = "http://example.com"
            lfs_url = "http://example.com/lfs"
            workspace = ""
        "#,
        );
        let cfg = build_config(&r).expect("should build");
        assert!(cfg.workspace.ends_with("/mount"), "default workspace used");
        assert_eq!(cfg.base_url, "http://example.com");
    }

    #[test]
    fn native_scalar_types_are_accepted() {
        let r = resolver(
            r#"
            base_url = "http://example.com"
            lfs_url = "http://example.com/lfs"
            load_dir_depth = 7
            dicfuse_readable = false
        "#,
        );
        let cfg = build_config(&r).expect("should build");
        assert_eq!(cfg.load_dir_depth, 7);
        assert!(!cfg.dicfuse_readable);
    }

    #[test]
    fn section_form_is_supported() {
        let r = resolver(
            r#"
            base_url = "http://example.com"
            lfs_url = "http://example.com/lfs"

            [dicfuse]
            load_dir_depth = 9
            stat_mode = "fast"

            [antares]
            upper_root = "/srv/antares/upper"
        "#,
        );
        let cfg = build_config(&r).expect("should build");
        assert_eq!(cfg.load_dir_depth, 9);
        assert_eq!(cfg.dicfuse_stat_mode, DicfuseStatMode::Fast);
        assert_eq!(cfg.antares_upper_root, "/srv/antares/upper");
    }

    #[test]
    fn invalid_number_is_rejected_with_field_name() {
        let r = resolver(
            r#"
            base_url = "http://example.com"
            lfs_url = "http://example.com/lfs"
            load_dir_depth = "abc"
        "#,
        );
        let err = build_config(&r).unwrap_err();
        assert!(
            err.contains("load_dir_depth"),
            "error names the field: {err}"
        );
    }

    #[test]
    fn numeric_lower_bound_is_enforced() {
        let r = resolver(
            r#"
            base_url = "http://example.com"
            lfs_url = "http://example.com/lfs"
            load_dir_depth = "0"
        "#,
        );
        let err = build_config(&r).unwrap_err();
        assert!(
            err.contains("load_dir_depth") && err.contains(">= 1"),
            "{err}"
        );
    }

    #[test]
    fn invalid_stat_mode_is_rejected() {
        let r = resolver(
            r#"
            base_url = "http://example.com"
            lfs_url = "http://example.com/lfs"
            dicfuse_stat_mode = "bogus"
        "#,
        );
        let err = build_config(&r).unwrap_err();
        assert!(err.contains("dicfuse_stat_mode"), "{err}");
    }

    #[test]
    fn invalid_url_is_rejected() {
        let r = resolver(
            r#"
            base_url = "not-a-url"
            lfs_url = "http://example.com/lfs"
        "#,
        );
        let err = build_config(&r).unwrap_err();
        assert!(err.contains("base_url"), "{err}");
    }

    #[test]
    fn missing_url_uses_default() {
        let r = resolver(r#"workspace = "/tmp/ws""#);
        let cfg = build_config(&r).expect("missing base_url falls back to default");
        assert_eq!(cfg.base_url, "http://localhost:8000");
        assert_eq!(cfg.lfs_url, "http://localhost:8000/lfs");
    }

    #[test]
    fn non_scalar_value_is_rejected() {
        let r = resolver(
            r#"
            base_url = "http://example.com"
            lfs_url = "http://example.com/lfs"
            load_dir_depth = [3]
        "#,
        );
        let err = build_config(&r).unwrap_err();
        assert!(
            err.contains("load_dir_depth") && err.contains("array"),
            "present-but-non-scalar value must be rejected: {err}"
        );
    }

    #[test]
    fn cli_override_beats_file() {
        let mut cli = HashMap::new();
        cli.insert("base_url".to_string(), "http://cli.example".to_string());
        let r = RawResolver::new(
            table(
                r#"
                base_url = "http://file.example"
                lfs_url = "http://example.com/lfs"
            "#,
            ),
            cli,
        );
        let cfg = build_config(&r).expect("should build");
        assert_eq!(cfg.base_url, "http://cli.example");
    }

    #[test]
    fn validate_file_reports_all_problems() {
        let broken = r#"
            base_url = "not a url"
            lfs_url = "http://ok.example/lfs"
            load_dir_depth = "abc"
            fetch_file_thread = "0"
            dicfuse_stat_mode = "bogus"
            dicfuse_dir_sync_ttl_secs = "abc"
            antares_dicfuse_open_buff_max_bytes = "xyz"
        "#;
        let path = format!(
            "{}/scorpio-validate-{}.toml",
            std::env::temp_dir().display(),
            std::process::id()
        );
        std::fs::write(&path, broken).unwrap();

        let problems = validate_file(&path, HashMap::new()).unwrap_err();
        // Must collect ALL issues in one pass, not just the first.
        for field in [
            "base_url",
            "load_dir_depth",
            "dicfuse_stat_mode",
            "fetch_file_thread",
            // Typed knobs without a range check must still be type-validated, so a
            // config `config validate` accepts is one `init_config` accepts.
            "dicfuse_dir_sync_ttl_secs",
            "antares_dicfuse_open_buff_max_bytes",
        ] {
            assert!(
                problems.iter().any(|p| p.contains(field)),
                "expected a problem mentioning {field}: {problems:?}"
            );
        }
        assert!(
            problems.len() >= 6,
            "expected >=6 problems, got {problems:?}"
        );

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn validate_file_accepts_valid_config() {
        let ok = r#"
            base_url = "http://example.com"
            lfs_url = "http://example.com/lfs"
        "#;
        let path = format!(
            "{}/scorpio-validate-ok-{}.toml",
            std::env::temp_dir().display(),
            std::process::id()
        );
        std::fs::write(&path, ok).unwrap();
        assert!(validate_file(&path, HashMap::new()).is_ok());
        std::fs::remove_file(&path).ok();
    }
}
