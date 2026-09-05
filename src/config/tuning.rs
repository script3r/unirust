//! Streaming engine tuning configuration.
//!
//! These are internal tuning parameters for the entity resolution engine.
//! Select a [`TuningProfile`] for an embedded engine or [`super::Profile`] in node
//! configuration before adjusting individual parameters. Presets are workload
//! tradeoffs, not throughput or total-memory guarantees.

use crate::conflicts::ConflictAlgorithm;
use crate::dsu::PersistentDSUConfig;
use crate::index::TierConfig;

use super::defaults::*;

#[derive(Debug, Clone)]
pub struct StreamingTuning {
    pub candidate_cap: usize,
    pub adaptive_candidate_cap: bool,
    pub adaptive_high_threshold: usize,
    pub adaptive_mid_threshold: usize,
    pub adaptive_high_cap: usize,
    pub adaptive_mid_cap: usize,
    pub deferred_reconciliation: bool,
    pub hot_key_threshold: usize,
    /// Enable deterministic, hash-based candidate sampling above `sampling_threshold`.
    /// Selection is weighted by temporal overlap. Sampling can omit matching
    /// candidates and affect resolution; it is not equivalent to exhaustive linking.
    pub stochastic_sampling: bool,
    /// Threshold above which stochastic sampling kicks in.
    pub sampling_threshold: usize,
    /// Target number of candidates to sample when using stochastic sampling.
    pub sampling_target: usize,
    /// Configuration for persistent DSU (used when `use_persistent_dsu` is true)
    pub dsu_config: Option<PersistentDSUConfig>,
    /// Request a RocksDB-backed DSU when the store exposes a shared database.
    pub use_persistent_dsu: bool,
    /// Configuration for tiered index storage (used when `use_tiered_index` is true)
    pub tier_config: Option<TierConfig>,
    /// Request hot/warm index caches with RocksDB-backed cold buckets when available.
    pub use_tiered_index: bool,
    /// Shard ID for this node (used for boundary tracking in distributed mode)
    pub shard_id: u16,
    /// Whether to track boundary signatures for cross-shard reconciliation.
    /// Default false - enable only in distributed mode to reduce memory overhead.
    pub enable_boundary_tracking: bool,
    /// Reserved linker-state cache configuration. Limits are currently not enforced;
    /// supplying them emits a warning and retains correctness-critical state in memory.
    pub linker_state_config: Option<LinkerStateConfig>,
}

/// Configuration for conflict detection algorithms.
#[derive(Debug, Clone)]
pub struct ConflictTuning {
    /// The algorithm to use for conflict detection.
    /// - `SweepLine`: Sweep sorted temporal boundary events
    /// - `AtomicIntervals`: Evaluate observations in atomic intervals
    /// - `Auto`: Select using the fraction of distinct temporal boundaries
    pub algorithm: ConflictAlgorithmChoice,

    /// Threshold for auto-selection: if the ratio of unique boundaries
    /// to twice the descriptor count is below this, use AtomicIntervals.
    /// Default: 0.5. Shared boundaries are a heuristic, not a runtime guarantee.
    pub auto_overlap_threshold: f64,
}

/// Algorithm selection for conflict detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictAlgorithmChoice {
    /// Always use sweep-line algorithm
    SweepLine,
    /// Always use atomic intervals algorithm
    AtomicIntervals,
    /// Automatically select based on data characteristics
    #[default]
    Auto,
}

impl Default for ConflictTuning {
    fn default() -> Self {
        Self {
            algorithm: ConflictAlgorithmChoice::Auto,
            auto_overlap_threshold: 0.5,
        }
    }
}

impl ConflictTuning {
    /// Select atomic intervals for workloads with many shared temporal boundaries.
    pub fn high_overlap() -> Self {
        Self {
            algorithm: ConflictAlgorithmChoice::AtomicIntervals,
            auto_overlap_threshold: 0.5,
        }
    }

    /// Create tuning for diverse time boundaries
    pub fn diverse_boundaries() -> Self {
        Self {
            algorithm: ConflictAlgorithmChoice::SweepLine,
            auto_overlap_threshold: 0.5,
        }
    }

    /// Select the appropriate algorithm based on data characteristics.
    ///
    /// Returns the algorithm to use given the number of unique boundaries
    /// and total descriptors.
    pub fn select_algorithm(
        &self,
        unique_boundaries: usize,
        total_descriptors: usize,
    ) -> ConflictAlgorithm {
        match self.algorithm {
            ConflictAlgorithmChoice::SweepLine => ConflictAlgorithm::SweepLine,
            ConflictAlgorithmChoice::AtomicIntervals => ConflictAlgorithm::AtomicIntervals,
            ConflictAlgorithmChoice::Auto => {
                if total_descriptors == 0 {
                    return ConflictAlgorithm::SweepLine;
                }

                // Ratio of unique boundaries to all descriptor endpoints
                // Each descriptor contributes 2 boundaries (start, end)
                // If many share the same boundaries, the ratio is low
                let max_boundaries = total_descriptors * 2;
                let ratio = unique_boundaries as f64 / max_boundaries as f64;

                if ratio < self.auto_overlap_threshold {
                    // Prefer atomic intervals when many boundaries are shared.
                    ConflictAlgorithm::AtomicIntervals
                } else {
                    // Prefer the sweep line when boundaries are more diverse.
                    ConflictAlgorithm::SweepLine
                }
            }
        }
    }
}

/// Preset profiles that bundle common tuning choices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuningProfile {
    Balanced,
    LowLatency,
    HighThroughput,
    BulkIngest,
    MemorySaver,
    /// Request persistent DSU and tiered index caches when the store supports them.
    BillionScale,
    /// Request persistent backends with larger caches and candidate budgets.
    BillionScaleHighPerformance,
}

impl Default for StreamingTuning {
    fn default() -> Self {
        Self {
            candidate_cap: DEFAULT_CANDIDATE_CAP,
            adaptive_candidate_cap: true,
            adaptive_high_threshold: DEFAULT_ADAPTIVE_HIGH_THRESHOLD,
            adaptive_mid_threshold: DEFAULT_ADAPTIVE_MID_THRESHOLD,
            adaptive_high_cap: DEFAULT_ADAPTIVE_HIGH_CAP,
            adaptive_mid_cap: DEFAULT_ADAPTIVE_MID_CAP,
            deferred_reconciliation: true,
            hot_key_threshold: DEFAULT_HOT_KEY_THRESHOLD,
            stochastic_sampling: true,
            sampling_threshold: DEFAULT_SAMPLING_THRESHOLD,
            sampling_target: DEFAULT_SAMPLING_TARGET,
            dsu_config: None,
            use_persistent_dsu: false,
            tier_config: None,
            use_tiered_index: false,
            shard_id: 0,
            enable_boundary_tracking: false,
            linker_state_config: None,
        }
    }
}

impl StreamingTuning {
    pub fn from_profile(profile: TuningProfile) -> Self {
        match profile {
            TuningProfile::Balanced => Self::balanced(),
            TuningProfile::LowLatency => Self::low_latency(),
            TuningProfile::HighThroughput => Self::high_throughput(),
            TuningProfile::BulkIngest => Self::bulk_ingest(),
            TuningProfile::MemorySaver => Self::memory_saver(),
            TuningProfile::BillionScale => Self::billion_scale(),
            TuningProfile::BillionScaleHighPerformance => Self::billion_scale_high_performance(),
        }
    }

    pub fn balanced() -> Self {
        Self::default()
    }

    pub fn low_latency() -> Self {
        Self {
            candidate_cap: 1000,
            adaptive_candidate_cap: true,
            adaptive_high_threshold: 4000,
            adaptive_mid_threshold: 1000,
            adaptive_high_cap: 250,
            adaptive_mid_cap: 500,
            deferred_reconciliation: true,
            hot_key_threshold: 20_000,
            stochastic_sampling: true,
            sampling_threshold: 300,
            sampling_target: 100,
            dsu_config: None,
            use_persistent_dsu: false,
            tier_config: None,
            use_tiered_index: false,
            shard_id: 0,
            enable_boundary_tracking: false,
            linker_state_config: None,
        }
    }

    pub fn high_throughput() -> Self {
        Self {
            candidate_cap: 4000,
            adaptive_candidate_cap: true,
            adaptive_high_threshold: 20_000,
            adaptive_mid_threshold: 5000,
            adaptive_high_cap: 1500,
            adaptive_mid_cap: 2500,
            deferred_reconciliation: true,
            hot_key_threshold: 100_000,
            stochastic_sampling: true,
            sampling_threshold: 800,
            sampling_target: 400,
            dsu_config: None,
            use_persistent_dsu: false,
            tier_config: None,
            use_tiered_index: false,
            shard_id: 0,
            enable_boundary_tracking: false,
            linker_state_config: None,
        }
    }

    pub fn bulk_ingest() -> Self {
        Self {
            candidate_cap: 500,
            adaptive_candidate_cap: true,
            adaptive_high_threshold: 2000,
            adaptive_mid_threshold: 500,
            adaptive_high_cap: 200,
            adaptive_mid_cap: 300,
            deferred_reconciliation: true,
            hot_key_threshold: 10_000,
            stochastic_sampling: true,
            sampling_threshold: 200,
            sampling_target: 100,
            dsu_config: None,
            use_persistent_dsu: false,
            tier_config: None,
            use_tiered_index: false,
            shard_id: 0,
            enable_boundary_tracking: false,
            linker_state_config: None,
        }
    }

    pub fn memory_saver() -> Self {
        Self {
            candidate_cap: 500,
            adaptive_candidate_cap: false,
            adaptive_high_threshold: 0,
            adaptive_mid_threshold: 0,
            adaptive_high_cap: 0,
            adaptive_mid_cap: 0,
            deferred_reconciliation: true,
            hot_key_threshold: 5_000,
            stochastic_sampling: true,
            sampling_threshold: 200,
            sampling_target: 50,
            dsu_config: Some(PersistentDSUConfig::memory_saver()),
            use_persistent_dsu: false,
            tier_config: Some(TierConfig::memory_saver()),
            use_tiered_index: false,
            shard_id: 0,
            enable_boundary_tracking: false,
            linker_state_config: None,
        }
    }

    /// Request persistent DSU and tiered index backends with default cache sizes.
    /// Correctness-critical linker maps remain in memory and grow with the dataset.
    pub fn billion_scale() -> Self {
        Self {
            candidate_cap: DEFAULT_CANDIDATE_CAP,
            adaptive_candidate_cap: true,
            adaptive_high_threshold: DEFAULT_ADAPTIVE_HIGH_THRESHOLD,
            adaptive_mid_threshold: DEFAULT_ADAPTIVE_MID_THRESHOLD,
            adaptive_high_cap: DEFAULT_ADAPTIVE_HIGH_CAP,
            adaptive_mid_cap: DEFAULT_ADAPTIVE_MID_CAP,
            deferred_reconciliation: true,
            hot_key_threshold: 100_000,
            stochastic_sampling: true,
            sampling_threshold: DEFAULT_SAMPLING_THRESHOLD,
            sampling_target: DEFAULT_SAMPLING_TARGET,
            dsu_config: Some(PersistentDSUConfig::default()),
            use_persistent_dsu: true,
            tier_config: Some(TierConfig::default()),
            use_tiered_index: true,
            shard_id: 0,
            enable_boundary_tracking: false,
            // LinkerState's bounded LRU backend has no durable spill path yet.
            // Evicting cluster summaries or ID mappings changes resolution results,
            // so persistent profiles must remain unbounded until spill is implemented.
            linker_state_config: None,
        }
    }

    /// Request persistent backends with larger caches and candidate budgets.
    /// The name does not imply a supported dataset size or total-memory bound.
    pub fn billion_scale_high_performance() -> Self {
        Self {
            candidate_cap: 4000,
            adaptive_candidate_cap: true,
            adaptive_high_threshold: 20_000,
            adaptive_mid_threshold: 5000,
            adaptive_high_cap: 1500,
            adaptive_mid_cap: 2500,
            deferred_reconciliation: true,
            hot_key_threshold: 200_000,
            stochastic_sampling: true,
            sampling_threshold: 800,
            sampling_target: 400,
            dsu_config: Some(PersistentDSUConfig::high_performance()),
            use_persistent_dsu: true,
            tier_config: Some(TierConfig::high_performance()),
            use_tiered_index: true,
            shard_id: 0,
            enable_boundary_tracking: false,
            // Correctness-critical linker state cannot be evicted without durable spill.
            linker_state_config: None,
        }
    }

    /// Enable boundary tracking for distributed mode
    pub fn with_boundary_tracking(mut self, enable: bool) -> Self {
        self.enable_boundary_tracking = enable;
        self
    }

    /// Set the shard ID for distributed mode.
    pub fn with_shard_id(mut self, shard_id: u16) -> Self {
        self.shard_id = shard_id;
        self
    }
}

/// Reserved configuration for linker-state memory management.
///
/// Capacities are currently not enforced because correctness-critical evictions
/// require a durable spill/read-through backend. Supplying this configuration
/// retains unbounded state and emits a warning.
#[derive(Debug, Clone)]
pub struct LinkerStateConfig {
    /// Requested cluster-ID mapping capacity; currently not enforced.
    pub cluster_ids_capacity: usize,
    /// Requested global-ID mapping capacity; currently not enforced.
    pub global_ids_capacity: usize,
    /// Requested strong-ID summary capacity; currently not enforced.
    pub summaries_capacity: usize,
    /// Requested record-perspective capacity; currently not enforced.
    pub perspectives_capacity: usize,
    /// Reserved linker-state dirty-buffer threshold; currently unused.
    pub dirty_buffer_size: usize,
}

impl Default for LinkerStateConfig {
    fn default() -> Self {
        Self {
            cluster_ids_capacity: DEFAULT_CLUSTER_IDS_CAPACITY,
            global_ids_capacity: DEFAULT_GLOBAL_IDS_CAPACITY,
            summaries_capacity: DEFAULT_SUMMARIES_CAPACITY,
            perspectives_capacity: DEFAULT_PERSPECTIVES_CAPACITY,
            dirty_buffer_size: DEFAULT_DIRTY_BUFFER_SIZE,
        }
    }
}

impl LinkerStateConfig {
    /// Smaller requested capacities; current linker implementations do not enforce them.
    pub fn memory_saver() -> Self {
        Self {
            cluster_ids_capacity: 500_000,
            global_ids_capacity: 100_000,
            summaries_capacity: 50_000,
            perspectives_capacity: 500_000,
            dirty_buffer_size: 10_000,
        }
    }

    /// Larger requested capacities; current linker implementations do not enforce them.
    pub fn high_performance() -> Self {
        Self {
            cluster_ids_capacity: 20_000_000,
            global_ids_capacity: 5_000_000,
            summaries_capacity: 2_000_000,
            perspectives_capacity: 20_000_000,
            dirty_buffer_size: 500_000,
        }
    }

    /// Set requested capacities to `usize::MAX`. Linker state is currently retained
    /// regardless of these values because durable spill is not implemented.
    pub fn unlimited() -> Self {
        Self {
            cluster_ids_capacity: usize::MAX,
            global_ids_capacity: usize::MAX,
            summaries_capacity: usize::MAX,
            perspectives_capacity: usize::MAX,
            dirty_buffer_size: usize::MAX,
        }
    }
}
