use std::{collections::HashMap, fmt, fs, path::Path, sync::OnceLock};

pub type ConfigError = String;
pub type ConfigResult<T> = Result<T, ConfigError>;

/// The fixed-snapshot workspace daemon's runtime configuration.
#[derive(Clone)]
pub struct ScorpioConfig {
    pub store_path: String,
    pub mst2_base_url: String,
    pub mst2_auth_token: String,
    pub log_level: String,
}

impl fmt::Debug for ScorpioConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScorpioConfig")
            .field("store_path", &self.store_path)
            .field("mst2_base_url", &self.mst2_base_url)
            .field("mst2_auth_token", &"<redacted>")
            .field("log_level", &self.log_level)
            .finish()
    }
}

/// All persistent v3 roots derive from the configured store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePaths {
    pub store_path: String,
    pub workspace_root: String,
    pub cache_root: String,
}

impl ScorpioConfig {
    fn runtime_paths(&self) -> RuntimePaths {
        let store = Path::new(&self.store_path);
        RuntimePaths {
            store_path: self.store_path.clone(),
            workspace_root: store.join("workspaces-v3").to_string_lossy().into_owned(),
            cache_root: store.join("mst2-cache").to_string_lossy().into_owned(),
        }
    }
}

// Diagnostic default reads must permit later explicit initialization. Both
// values live for the process lifetime, keeping accessor references valid.
static SCORPIO_CONFIG: OnceLock<ScorpioConfig> = OnceLock::new();
static DEFAULT_CONFIG: OnceLock<ScorpioConfig> = OnceLock::new();

fn defaults() -> ScorpioConfig {
    let username = whoami::username().unwrap_or_else(|_| "unknown".to_string());
    ScorpioConfig {
        store_path: format!("/tmp/megadir-{username}/store"),
        mst2_base_url: "http://127.0.0.1:19700".to_string(),
        mst2_auth_token: String::new(),
        log_level: "info".to_string(),
    }
}

/// Precedence: CLI > SCORPIO_* env > sectioned TOML > flat TOML > defaults.
/// Retired keys are ignored without rewriting input or mapping old endpoints.
struct RawResolver {
    file: toml::Table,
    cli: HashMap<String, String>,
    environment: HashMap<String, String>,
}

impl RawResolver {
    fn new(file: toml::Table, cli: HashMap<String, String>) -> Self {
        let environment = [
            "store_path",
            "mst2_base_url",
            "mst2_auth_token",
            "log_level",
        ]
        .into_iter()
        .filter_map(|key| {
            std::env::var(format!("SCORPIO_{}", key.to_ascii_uppercase()))
                .ok()
                .map(|value| (key.to_owned(), value))
        })
        .collect();
        Self {
            file,
            cli,
            environment,
        }
    }

    fn get(&self, key: &str, section: &str, short: &str) -> ConfigResult<Option<String>> {
        if let Some(value) = self.cli.get(key).filter(|value| !value.trim().is_empty()) {
            return Ok(Some(value.clone()));
        }
        if let Some(value) = self
            .environment
            .get(key)
            .filter(|value| !value.trim().is_empty())
        {
            return Ok(Some(value.clone()));
        }
        if let Some(value) = self
            .file
            .get(section)
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get(short))
        {
            if let Some(value) = string_value(value, key)? {
                return Ok(Some(value));
            }
        }
        self.file
            .get(key)
            .map(|value| string_value(value, key))
            .unwrap_or(Ok(None))
    }
}

fn string_value(value: &toml::Value, key: &str) -> ConfigResult<Option<String>> {
    let value = value
        .as_str()
        .ok_or_else(|| format!("Invalid value for '{key}': expected a string"))?;
    Ok((!value.trim().is_empty()).then(|| value.to_owned()))
}

fn validate_url(value: &str) -> ConfigResult<()> {
    let parsed =
        url::Url::parse(value).map_err(|_| "Invalid URL for 'mst2_base_url'".to_string())?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err("Invalid URL for 'mst2_base_url': expected http/https with a host".into());
    }
    Ok(())
}

fn resolve_config(resolver: &RawResolver) -> (ScorpioConfig, Vec<ConfigError>) {
    let mut errors = Vec::new();
    let mut resolve = |key, section, short, default| match resolver.get(key, section, short) {
        Ok(value) => value.unwrap_or(default),
        Err(error) => {
            errors.push(error);
            default
        }
    };
    let default = defaults();
    let config = ScorpioConfig {
        store_path: resolve("store_path", "server", "store_path", default.store_path),
        mst2_base_url: resolve("mst2_base_url", "mst2", "base_url", default.mst2_base_url),
        mst2_auth_token: resolve(
            "mst2_auth_token",
            "mst2",
            "auth_token",
            default.mst2_auth_token,
        ),
        log_level: resolve("log_level", "server", "log_level", default.log_level),
    };
    if let Err(error) = validate_url(&config.mst2_base_url) {
        errors.push(error);
    }
    for (name, value) in [
        ("store_path", &config.store_path),
        ("mst2_auth_token", &config.mst2_auth_token),
        ("log_level", &config.log_level),
    ] {
        if value
            .chars()
            .any(|character| matches!(character, '\0' | '\r' | '\n'))
        {
            errors.push(format!(
                "Invalid value for '{name}': contains a control character"
            ));
        }
    }
    (config, errors)
}

fn resolver_from_file(path: &str, overrides: HashMap<String, String>) -> ConfigResult<RawResolver> {
    let content = fs::read_to_string(path)
        .map_err(|error| format!("Could not read config at '{path}': {error}"))?;
    let file =
        toml::from_str(&content).map_err(|_| format!("Invalid config format in '{path}'"))?;
    Ok(RawResolver::new(file, overrides))
}

fn parse_config(path: &str, overrides: HashMap<String, String>) -> ConfigResult<ScorpioConfig> {
    let (config, errors) = resolve_config(&resolver_from_file(path, overrides)?);
    match errors.into_iter().next() {
        Some(error) => Err(error),
        None => Ok(config),
    }
}

/// Validate every live field without creating directories or rewriting input.
pub fn validate_file(path: &str, overrides: HashMap<String, String>) -> Vec<ConfigError> {
    match resolver_from_file(path, overrides) {
        Ok(resolver) => resolve_config(&resolver).1,
        Err(error) => vec![error],
    }
}

pub fn effective_config_dump() -> String {
    format!("{:#?}", get_config())
}

pub fn resolve_runtime_paths(
    path: &str,
    overrides: HashMap<String, String>,
) -> ConfigResult<RuntimePaths> {
    Ok(parse_config(path, overrides)?.runtime_paths())
}

/// Pure configuration loading: the service creates v3 storage after HTTP bind.
pub fn init_config(path: &str) -> ConfigResult<()> {
    init_config_with(path, HashMap::new())
}

pub fn init_config_with(path: &str, overrides: HashMap<String, String>) -> ConfigResult<()> {
    if SCORPIO_CONFIG.get().is_some() {
        return Err("Configuration already initialized".into());
    }
    SCORPIO_CONFIG
        .set(parse_config(path, overrides)?)
        .map_err(|_| "Configuration already initialized".into())
}

fn get_config() -> &'static ScorpioConfig {
    SCORPIO_CONFIG
        .get()
        .unwrap_or_else(|| DEFAULT_CONFIG.get_or_init(defaults))
}

pub fn runtime_paths() -> RuntimePaths {
    get_config().runtime_paths()
}
pub fn store_path() -> &'static str {
    &get_config().store_path
}
pub fn mst2_base_url() -> &'static str {
    &get_config().mst2_base_url
}
pub fn mst2_auth_token() -> &'static str {
    &get_config().mst2_auth_token
}
pub fn log_level() -> &'static str {
    &get_config().log_level
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolver(file: toml::Table, cli: HashMap<String, String>) -> RawResolver {
        RawResolver {
            file,
            cli,
            environment: HashMap::new(),
        }
    }

    #[test]
    fn retired_endpoints_and_malformed_unused_keys_do_not_change_the_live_backend() {
        let file = toml::from_str(
            r#"base_url = "https://retired.example"
               lfs_url = []
               config_file = { ignored = true }
               mst2_lower_enabled = false
               [antares]
               upper_root = false"#,
        )
        .unwrap();
        let (config, errors) = resolve_config(&resolver(file, HashMap::new()));
        assert!(errors.is_empty());
        assert_eq!(config.mst2_base_url, "http://127.0.0.1:19700");
    }

    #[test]
    fn sectioned_live_fields_override_flat_keys_and_cli_overrides_both() {
        let file = toml::from_str(
            r#"mst2_base_url = "https://flat.example"
               store_path = "/flat"
               [mst2]
               base_url = "https://section.example"
               auth_token = "private-token"
               [server]
               store_path = "/section""#,
        )
        .unwrap();
        let overrides = HashMap::from([("store_path".into(), "/cli".into())]);
        let (config, errors) = resolve_config(&resolver(file, overrides));
        assert!(errors.is_empty());
        assert_eq!(config.store_path, "/cli");
        assert_eq!(config.mst2_base_url, "https://section.example");
        assert_eq!(config.mst2_auth_token, "private-token");
        assert!(!format!("{config:#?}").contains("private-token"));
    }

    #[test]
    fn live_validation_collects_errors_without_echoing_the_secret() {
        let file = toml::from_str(
            r#"mst2_base_url = "file:///private"
               store_path = []
               mst2_auth_token = "private-token\n"
               log_level = { invalid = true }"#,
        )
        .unwrap();
        let (_, errors) = resolve_config(&resolver(file, HashMap::new()));
        for key in [
            "mst2_base_url",
            "store_path",
            "mst2_auth_token",
            "log_level",
        ] {
            assert!(errors.iter().any(|error| error.contains(key)), "{errors:?}");
        }
        assert!(!format!("{errors:?}").contains("private-token"));
    }

    #[test]
    fn retained_config_resolution_is_read_only_and_derives_both_roots() {
        let temp = tempfile::tempdir().unwrap();
        let store = temp.path().join("not-created/store");
        let file = temp.path().join("scorpio.toml");
        let mut table = toml::Table::new();
        table.insert("store_path".into(), store.to_str().unwrap().into());
        table.insert("mst2_base_url".into(), "https://live.example".into());
        table.insert("config_file".into(), "invalid-legacy-state.toml".into());
        let input = toml::to_string(&table).unwrap();
        fs::write(&file, &input).unwrap();
        let overrides = HashMap::from([
            ("store_path".into(), store.to_str().unwrap().into()),
            ("mst2_base_url".into(), "https://live.example".into()),
            ("mst2_auth_token".into(), "private-fixture-token".into()),
            ("log_level".into(), "info".into()),
        ]);
        let paths = resolve_runtime_paths(file.to_str().unwrap(), overrides.clone()).unwrap();
        assert_eq!(paths.store_path, store.to_str().unwrap());
        assert_eq!(
            Path::new(&paths.workspace_root),
            store.join("workspaces-v3")
        );
        assert_eq!(Path::new(&paths.cache_root), store.join("mst2-cache"));
        assert!(validate_file(file.to_str().unwrap(), overrides).is_empty());
        assert_eq!(fs::read_to_string(file).unwrap(), input);
        assert!(!store.parent().unwrap().exists());
        assert!(!temp.path().join("invalid-legacy-state.toml").exists());
    }
}
