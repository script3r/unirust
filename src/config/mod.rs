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
        let mut figment = Figment::new().merge(Serialized::defaults(UniConfig::default()));

        // Layer 1: Config file (if provided)
        if let Some(path) = config_path {
            figment = figment.merge(Toml::file(path));
        }

        // Layer 2: Environment variables with UNIRUST_ prefix
        figment = figment.merge(Env::prefixed("UNIRUST_").split("_"));

        // Layer 3: CLI overrides
        figment = figment.merge(Serialized::defaults(overrides));

        figment.extract().map_err(ConfigError::from)
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
    /// Path to ontology configuration file (JSON)
    pub ontology: Option<PathBuf>,
    /// Run repair on startup
    pub repair: bool,
    /// Config version for compatibility checking
    pub config_version: Option<String>,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_SHARD_ADDR.parse().unwrap(),
            id: 0,
            data_dir: None,
            ontology: None,
            repair: false,
            config_version: None,
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
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_ROUTER_ADDR.parse().unwrap(),
            shards: default_router_shards(),
            shards_file: None,
            ontology: None,
            config_version: None,
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
    /// Maximum staleness before forced reconcile (seconds)
    pub max_staleness_secs: u64,
    /// Minimum interval between reconciles (seconds)
    pub min_interval_secs: u64,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            max_staleness_secs: DEFAULT_MAX_STALENESS_SECS,
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
    pub ontology: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repair: Option<bool>,
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
        assert_eq!(config.storage.block_cache_mb, DEFAULT_BLOCK_CACHE_MB);
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
}
