//! Unified configuration system for unirust components.
//!
//! Configuration is loaded with precedence: CLI args > Env vars > Config file > Defaults
//!
//! # Example config file (unirust.toml)
//! ```toml
//! profile = "billion-scale-high-performance"
//!
//! [shard]
//! listen = "0.0.0.0:50061"
//! id = 0
//! data_dir = "/var/lib/unirust"
//! backup_dir = "/var/backups/unirust/shard-0"
//!
//! [router]
//! listen = "0.0.0.0:50060"
//! shards = ["shard-0:50061", "shard-1:50061", "shard-2:50061", "shard-3:50061", "shard-4:50061"]
//! ```

mod defaults;
mod tuning;

pub use defaults::*;
pub use tuning::*;

use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

fn config_env_path(key: &str) -> Option<&'static str> {
    match key.to_ascii_uppercase().as_str() {
        "PROFILE" => Some("profile"),
        "SHARD_LISTEN" => Some("shard.listen"),
        "SHARD_ID" => Some("shard.id"),
        "SHARD_DATA_DIR" => Some("shard.data_dir"),
        "SHARD_BACKUP_DIR" => Some("shard.backup_dir"),
        "SHARD_ONTOLOGY" => Some("shard.ontology"),
        "SHARD_REPAIR" => Some("shard.repair"),
        "SHARD_CONFIG_VERSION" => Some("shard.config_version"),
        "SHARD_TLS_CERT" => Some("shard.tls_cert"),
        "SHARD_TLS_KEY" => Some("shard.tls_key"),
        "SHARD_TLS_CLIENT_CA" => Some("shard.tls_client_ca"),
        "ROUTER_LISTEN" => Some("router.listen"),
        "ROUTER_SHARDS_FILE" => Some("router.shards_file"),
        "ROUTER_ONTOLOGY" => Some("router.ontology"),
        "ROUTER_CONFIG_VERSION" => Some("router.config_version"),
        "ROUTER_SHARD_CONNECT_TIMEOUT_SECS" => Some("router.shard_connect_timeout_secs"),
        "ROUTER_SHARD_REQUEST_TIMEOUT_SECS" => Some("router.shard_request_timeout_secs"),
        "ROUTER_SHARD_TCP_KEEPALIVE_SECS" => Some("router.shard_tcp_keepalive_secs"),
        "ROUTER_CHECKPOINT_INTERVAL_SECS" => Some("router.checkpoint_interval_secs"),
        "ROUTER_TLS_CERT" => Some("router.tls_cert"),
        "ROUTER_TLS_KEY" => Some("router.tls_key"),
        "ROUTER_TLS_CLIENT_CA" => Some("router.tls_client_ca"),
        "ROUTER_SHARD_TLS_CA" => Some("router.shard_tls_ca"),
        "ROUTER_SHARD_TLS_CERT" => Some("router.shard_tls_cert"),
        "ROUTER_SHARD_TLS_KEY" => Some("router.shard_tls_key"),
        "STORAGE_BLOCK_CACHE_MB" => Some("storage.block_cache_mb"),
        "STORAGE_WRITE_BUFFER_MB" => Some("storage.write_buffer_mb"),
        "STORAGE_RATE_LIMIT_MBPS" => Some("storage.rate_limit_mbps"),
        "STORAGE_MAX_BACKGROUND_JOBS" => Some("storage.max_background_jobs"),
        "RECONCILIATION_KEY_COUNT_THRESHOLD" => Some("reconciliation.key_count_threshold"),
        "RECONCILIATION_MAX_STALENESS_SECS" => Some("reconciliation.max_staleness_secs"),
        "RECONCILIATION_IDLE_INGEST_RATE" => Some("reconciliation.idle_ingest_rate"),
        "RECONCILIATION_MIN_INTERVAL_SECS" => Some("reconciliation.min_interval_secs"),
        // Parsed separately because the documented value is a comma-separated list.
        "ROUTER_SHARDS" => None,
        _ => None,
    }
}

fn validate_config_environment() -> Result<(), ConfigError> {
    const CONFIG_NAMESPACES: [&str; 4] = [
        "UNIRUST_SHARD_",
        "UNIRUST_ROUTER_",
        "UNIRUST_STORAGE_",
        "UNIRUST_RECONCILIATION_",
    ];

    for (key, _) in std::env::vars_os() {
        let Some(key) = key.to_str() else {
            continue;
        };
        let normalized = key.to_ascii_uppercase();
        if CONFIG_NAMESPACES
            .iter()
            .any(|namespace| normalized.starts_with(namespace))
            && normalized != "UNIRUST_ROUTER_SHARDS"
            && config_env_path(normalized.trim_start_matches("UNIRUST_")).is_none()
        {
            return Err(ConfigError {
                message: format!("unknown Unirust configuration variable {key}"),
            });
        }
    }

    Ok(())
}

fn router_shards_from_env() -> Result<Option<Vec<String>>, ConfigError> {
    let Some(value) = std::env::var_os("UNIRUST_ROUTER_SHARDS") else {
        return Ok(None);
    };
    let value = value.into_string().map_err(|_| ConfigError {
        message: "UNIRUST_ROUTER_SHARDS must be valid UTF-8".to_string(),
    })?;
    let shards: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect();
    if shards.is_empty() {
        return Err(ConfigError {
            message: "UNIRUST_ROUTER_SHARDS must contain at least one address".to_string(),
        });
    }
    Ok(Some(shards))
}

fn validate_mtls_group(name: &str, paths: [Option<&PathBuf>; 3]) -> Result<(), ConfigError> {
    let configured = paths.iter().filter(|path| path.is_some()).count();
    if configured != 0 && configured != paths.len() {
        return Err(ConfigError {
            message: format!("{name} requires all three certificate paths together"),
        });
    }
    Ok(())
}

/// Main configuration for unirust components.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct UniConfig {
    /// Performance tuning profile
    pub profile: Profile,
    /// Shard configuration
    pub shard: ShardConfig,
    /// Router configuration
    pub router: RouterConfig,
    /// RocksDB storage tuning (advanced)
    pub storage: StorageConfig,
    /// Cross-shard reconciliation (advanced)
    pub reconciliation: ReconciliationConfig,
}

impl UniConfig {
    /// Load configuration with precedence: CLI args > Env > File > Defaults
    ///
    /// # Arguments
    /// * `config_path` - Optional path to TOML config file
    /// * `overrides` - CLI overrides to apply on top
    pub fn load(
        config_path: Option<&str>,
        overrides: ConfigOverrides,
    ) -> Result<Self, ConfigError> {
        validate_config_environment()?;
        let mut figment = Figment::new().merge(Serialized::defaults(UniConfig::default()));

        // Layer 1: Config file (if provided)
        if let Some(path) = config_path {
            figment = figment.merge(Toml::file(path));
        }

        // Layer 2: Environment variables with UNIRUST_ prefix
        figment = figment.merge(
            Env::prefixed("UNIRUST_")
                .filter_map(|key| config_env_path(key.as_str()).map(Into::into)),
        );
        if let Some(shards) = router_shards_from_env()? {
            figment = figment.merge(Serialized::defaults(ConfigOverrides {
                router: Some(RouterOverrides {
                    shards: Some(shards),
                    ..RouterOverrides::default()
                }),
                ..ConfigOverrides::default()
            }));
        }

        // Layer 3: CLI overrides
        figment = figment.merge(Serialized::defaults(overrides));

        let config: Self = figment.extract().map_err(ConfigError::from)?;
        validate_mtls_group(
            "shard mTLS",
            [
                config.shard.tls_cert.as_ref(),
                config.shard.tls_key.as_ref(),
                config.shard.tls_client_ca.as_ref(),
            ],
        )?;
        validate_mtls_group(
            "router mTLS",
            [
                config.router.tls_cert.as_ref(),
                config.router.tls_key.as_ref(),
                config.router.tls_client_ca.as_ref(),
            ],
        )?;
        validate_mtls_group(
            "router-to-shard mTLS",
            [
                config.router.shard_tls_ca.as_ref(),
                config.router.shard_tls_cert.as_ref(),
                config.router.shard_tls_key.as_ref(),
            ],
        )?;
        Ok(config)
    }

    /// Load from environment and optional config file only (no CLI overrides)
    pub fn from_env(config_path: Option<&str>) -> Result<Self, ConfigError> {
        Self::load(config_path, ConfigOverrides::default())
    }
}

/// Performance tuning profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Profile {
    /// Balanced settings for general workloads
    Balanced,
    /// Optimized for low-latency responses
    LowLatency,
    /// Optimized for maximum throughput
    HighThroughput,
    /// Optimized for bulk data ingestion
    BulkIngest,
    /// Reduced memory footprint
    MemorySaver,
    /// For billion-scale datasets with persistent storage
    BillionScale,
    /// For billion-scale datasets with larger caches (production default)
    #[default]
    BillionScaleHighPerformance,
}

impl Profile {
    /// Convert to the internal TuningProfile enum
    pub fn to_tuning_profile(self) -> TuningProfile {
        match self {
            Profile::Balanced => TuningProfile::Balanced,
            Profile::LowLatency => TuningProfile::LowLatency,
            Profile::HighThroughput => TuningProfile::HighThroughput,
            Profile::BulkIngest => TuningProfile::BulkIngest,
            Profile::MemorySaver => TuningProfile::MemorySaver,
            Profile::BillionScale => TuningProfile::BillionScale,
            Profile::BillionScaleHighPerformance => TuningProfile::BillionScaleHighPerformance,
        }
    }
}

/// Shard node configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShardConfig {
    /// Listen address
    pub listen: SocketAddr,
    /// Shard ID (0-indexed)
    pub id: u16,
    /// Data directory for persistence
    pub data_dir: Option<PathBuf>,
    /// Checkpoint root, ideally on storage independent from the data directory
    pub backup_dir: Option<PathBuf>,
    /// Path to ontology configuration file (JSON)
    pub ontology: Option<PathBuf>,
    /// Run repair on startup
    pub repair: bool,
    /// Config version for compatibility checking
    pub config_version: Option<String>,
    /// PEM server certificate for mutually authenticated TLS
    pub tls_cert: Option<PathBuf>,
    /// PEM server private key for mutually authenticated TLS
    pub tls_key: Option<PathBuf>,
    /// PEM CA used to require and verify client certificates
    pub tls_client_ca: Option<PathBuf>,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_SHARD_ADDR.parse().unwrap(),
            id: 0,
            data_dir: None,
            backup_dir: None,
            ontology: None,
            repair: false,
            config_version: None,
            tls_cert: None,
            tls_key: None,
            tls_client_ca: None,
        }
    }
}

/// Router node configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RouterConfig {
    /// Listen address
    pub listen: SocketAddr,
    /// Shard addresses (will be prefixed with http:// if no scheme)
    pub shards: Vec<String>,
    /// Path to file containing shard addresses (one per line)
    pub shards_file: Option<PathBuf>,
    /// Path to ontology configuration file (JSON)
    pub ontology: Option<PathBuf>,
    /// Config version for compatibility checking
    pub config_version: Option<String>,
    /// Maximum time to establish a shard connection
    pub shard_connect_timeout_secs: u64,
    /// Maximum time for one shard RPC
    pub shard_request_timeout_secs: u64,
    /// TCP keepalive interval for shard connections
    pub shard_tcp_keepalive_secs: u64,
    /// Interval between automatic coordinated checkpoints (0 disables)
    pub checkpoint_interval_secs: u64,
    /// PEM server certificate for mutually authenticated TLS
    pub tls_cert: Option<PathBuf>,
    /// PEM server private key for mutually authenticated TLS
    pub tls_key: Option<PathBuf>,
    /// PEM CA used to require and verify client certificates
    pub tls_client_ca: Option<PathBuf>,
    /// PEM CA used to verify shard server certificates
    pub shard_tls_ca: Option<PathBuf>,
    /// PEM router client certificate presented to shards
    pub shard_tls_cert: Option<PathBuf>,
    /// PEM router client private key presented to shards
    pub shard_tls_key: Option<PathBuf>,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_ROUTER_ADDR.parse().unwrap(),
            shards: default_router_shards(),
            shards_file: None,
            ontology: None,
            config_version: None,
            shard_connect_timeout_secs: DEFAULT_SHARD_CONNECT_TIMEOUT_SECS,
            shard_request_timeout_secs: DEFAULT_SHARD_REQUEST_TIMEOUT_SECS,
            shard_tcp_keepalive_secs: DEFAULT_SHARD_TCP_KEEPALIVE_SECS,
            checkpoint_interval_secs: DEFAULT_CHECKPOINT_INTERVAL_SECS,
            tls_cert: None,
            tls_key: None,
            tls_client_ca: None,
            shard_tls_ca: None,
            shard_tls_cert: None,
            shard_tls_key: None,
        }
    }
}

/// RocksDB storage configuration (advanced).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Block cache size in MB
    pub block_cache_mb: usize,
    /// Write buffer size in MB
    pub write_buffer_mb: usize,
    /// Rate limit in MB/s (0 = unlimited)
    pub rate_limit_mbps: usize,
    /// Maximum number of background compaction threads
    pub max_background_jobs: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            block_cache_mb: DEFAULT_BLOCK_CACHE_MB,
            write_buffer_mb: DEFAULT_WRITE_BUFFER_MB,
            rate_limit_mbps: 0,
            max_background_jobs: DEFAULT_BACKGROUND_JOBS,
        }
    }
}

/// Cross-shard reconciliation configuration (advanced).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReconciliationConfig {
    /// Number of dirty identity keys that triggers reconciliation
    pub key_count_threshold: usize,
    /// Maximum staleness before forced reconcile (seconds)
    pub max_staleness_secs: u64,
    /// Ingest rate below which pending keys are reconciled while idle
    pub idle_ingest_rate: f64,
    /// Minimum interval between reconciles (seconds)
    pub min_interval_secs: u64,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            key_count_threshold: DEFAULT_RECONCILE_KEY_COUNT_THRESHOLD,
            max_staleness_secs: DEFAULT_MAX_STALENESS_SECS,
            idle_ingest_rate: DEFAULT_RECONCILE_IDLE_INGEST_RATE,
            min_interval_secs: DEFAULT_MIN_RECONCILE_INTERVAL_SECS,
        }
    }
}

/// CLI overrides that take precedence over file and env config.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<Profile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard: Option<ShardOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub router: Option<RouterOverrides>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ShardOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen: Option<SocketAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup_dir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ontology: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repair: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_cert: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_key: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_client_ca: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RouterOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listen: Option<SocketAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shards: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shards_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ontology: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_interval_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_cert: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_key: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_client_ca: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_tls_ca: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_tls_cert: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_tls_key: Option<PathBuf>,
}

/// Configuration error.
#[derive(Debug)]
pub struct ConfigError {
    pub message: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "configuration error: {}", self.message)
    }
}

impl std::error::Error for ConfigError {}

impl From<figment::Error> for ConfigError {
    fn from(e: figment::Error) -> Self {
        Self {
            message: e.to_string(),
        }
    }
}

/// Helper to normalize shard addresses (add http:// if missing).
pub fn normalize_shard_addrs(addrs: &[String]) -> Vec<String> {
    addrs
        .iter()
        .filter(|a| !a.is_empty())
        .map(|addr| {
            if addr.starts_with("http://") || addr.starts_with("https://") {
                addr.clone()
            } else {
                format!("http://{}", addr)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = UniConfig::default();
        assert_eq!(config.profile, Profile::BillionScaleHighPerformance);
        assert_eq!(config.shard.id, 0);
        assert!(config.shard.backup_dir.is_none());
        assert_eq!(config.storage.block_cache_mb, DEFAULT_BLOCK_CACHE_MB);
        assert_eq!(
            config.router.shard_connect_timeout_secs,
            DEFAULT_SHARD_CONNECT_TIMEOUT_SECS
        );
        assert_eq!(
            config.router.shard_request_timeout_secs,
            DEFAULT_SHARD_REQUEST_TIMEOUT_SECS
        );
        assert_eq!(
            config.router.checkpoint_interval_secs,
            DEFAULT_CHECKPOINT_INTERVAL_SECS
        );
    }

    #[test]
    fn test_normalize_shard_addrs() {
        let addrs = vec![
            "localhost:50061".to_string(),
            "http://shard1:50061".to_string(),
            "".to_string(),
        ];
        let normalized = normalize_shard_addrs(&addrs);
        assert_eq!(normalized.len(), 2);
        assert_eq!(normalized[0], "http://localhost:50061");
        assert_eq!(normalized[1], "http://shard1:50061");
    }

    #[test]
    fn test_profile_serde() {
        let json = serde_json::to_string(&Profile::HighThroughput).unwrap();
        assert_eq!(json, "\"high-throughput\"");

        let profile: Profile = serde_json::from_str("\"low-latency\"").unwrap();
        assert_eq!(profile, Profile::LowLatency);
    }

    #[test]
    fn test_config_env_paths_preserve_field_underscores() {
        assert_eq!(
            config_env_path("ROUTER_CHECKPOINT_INTERVAL_SECS"),
            Some("router.checkpoint_interval_secs")
        );
        assert_eq!(
            config_env_path("SHARD_BACKUP_DIR"),
            Some("shard.backup_dir")
        );
        assert_eq!(
            config_env_path("RECONCILIATION_KEY_COUNT_THRESHOLD"),
            Some("reconciliation.key_count_threshold")
        );
        assert_eq!(config_env_path("ROUTER_UNKNOWN"), None);
    }
}
