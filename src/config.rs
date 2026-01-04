use crate::conflicts::ConflictAlgorithm;
use crate::dsu::PersistentDSUConfig;
use crate::index::TierConfig;

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
    /// Enable stochastic candidate sampling (SPER-inspired optimization).
    /// When candidates exceed sampling_threshold, sample with probability
    /// proportional to temporal overlap instead of hard cutoff.
    /// This maintains expected match quality while reducing computation.
    pub stochastic_sampling: bool,
    /// Threshold above which stochastic sampling kicks in.
    /// Default: 500 candidates
    pub sampling_threshold: usize,
    /// Target number of candidates to sample when using stochastic sampling.
    /// Default: 200
    pub sampling_target: usize,
    /// Configuration for persistent DSU (used when `use_persistent_dsu` is true)
    pub dsu_config: Option<PersistentDSUConfig>,
    /// Whether to use persistent DSU for billion-scale datasets
    pub use_persistent_dsu: bool,
    /// Configuration for tiered index storage (used when `use_tiered_index` is true)
    pub tier_config: Option<TierConfig>,
    /// Whether to use tiered index for billion-scale datasets
    pub use_tiered_index: bool,
    /// Shard ID for this node (used for boundary tracking in distributed mode)
    pub shard_id: u16,
    /// Whether to track boundary signatures for cross-shard reconciliation.
    /// Default false - enable only in distributed mode to reduce memory overhead.
    pub enable_boundary_tracking: bool,
    /// Configuration for linker state memory management (LRU caches).
    /// Default: None (uses HashMap, unlimited capacity).
    pub linker_state_config: Option<LinkerStateConfig>,
}

/// Configuration for conflict detection algorithms.
#[derive(Debug, Clone)]
pub struct ConflictTuning {
    /// The algorithm to use for conflict detection.
    /// - `SweepLine`: O(n log n), best for diverse time boundaries
    /// - `AtomicIntervals`: O(atoms × n), best for overlapping intervals
    /// - `Auto`: Automatically select based on overlap ratio heuristic
    pub algorithm: ConflictAlgorithmChoice,

    /// Threshold for auto-selection: if the ratio of unique boundaries
    /// to total descriptors is below this, use AtomicIntervals.
    /// Default: 0.5 (if < 50% unique boundaries, use atomic intervals)
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
    /// Create tuning optimized for high-overlap workloads (production default)
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

                // Ratio of unique boundaries to total descriptors
                // Each descriptor contributes 2 boundaries (start, end)
                // If many share the same boundaries, the ratio is low
                let max_boundaries = total_descriptors * 2;
                let ratio = unique_boundaries as f64 / max_boundaries as f64;

                if ratio < self.auto_overlap_threshold {
                    // High overlap detected - atomic intervals is faster
                    ConflictAlgorithm::AtomicIntervals
                } else {
                    // Low overlap - sweep line is faster
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
    /// Optimized for billion-scale datasets with persistent DSU
    BillionScale,
    /// High-performance billion-scale with larger caches
    BillionScaleHighPerformance,
}

impl Default for StreamingTuning {
    fn default() -> Self {
        Self {
            candidate_cap: 2000,
            adaptive_candidate_cap: true,
            adaptive_high_threshold: 10_000,
            adaptive_mid_threshold: 2_000,
            adaptive_high_cap: 500,
            adaptive_mid_cap: 1000,
            deferred_reconciliation: true,
            hot_key_threshold: 50_000,
            stochastic_sampling: true,
            sampling_threshold: 500,
            sampling_target: 200,
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

    /// Configuration optimized for billion-scale datasets with persistent DSU and tiered index
    pub fn billion_scale() -> Self {
        Self {
            candidate_cap: 2000,
            adaptive_candidate_cap: true,
            adaptive_high_threshold: 10_000,
            adaptive_mid_threshold: 2_000,
            adaptive_high_cap: 500,
            adaptive_mid_cap: 1000,
            deferred_reconciliation: true,
            hot_key_threshold: 100_000,
            stochastic_sampling: true,
            sampling_threshold: 500,
            sampling_target: 200,
            dsu_config: Some(PersistentDSUConfig::default()),
            use_persistent_dsu: true,
            tier_config: Some(TierConfig::default()),
            use_tiered_index: true,
            shard_id: 0,
            enable_boundary_tracking: false,
            linker_state_config: Some(LinkerStateConfig::default()),
        }
    }

    /// High-performance configuration for billion-scale with larger caches
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
            linker_state_config: Some(LinkerStateConfig::high_performance()),
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

/// Configuration for linker state memory management.
/// Controls LRU cache sizes for cluster IDs, summaries, and perspectives.
#[derive(Debug, Clone)]
pub struct LinkerStateConfig {
    /// Maximum number of cluster ID mappings to keep in memory.
    /// Default: 5_000_000 (~80MB with overhead)
    pub cluster_ids_capacity: usize,
    /// Maximum number of global cluster ID mappings to keep in memory.
    /// Default: 1_000_000 (~24MB with overhead)
    pub global_ids_capacity: usize,
    /// Maximum number of strong ID summaries to keep in memory.
    /// Default: 500_000 (~100MB depending on summary size)
    pub summaries_capacity: usize,
    /// Maximum number of record perspectives to keep in memory.
    /// Default: 5_000_000 (~160MB depending on perspective length)
    pub perspectives_capacity: usize,
    /// Size of dirty buffer before flushing to disk (if persistence enabled).
    /// Default: 100_000
    pub dirty_buffer_size: usize,
}

impl Default for LinkerStateConfig {
    fn default() -> Self {
        Self {
            cluster_ids_capacity: 5_000_000,
            global_ids_capacity: 1_000_000,
            summaries_capacity: 500_000,
            perspectives_capacity: 5_000_000,
            dirty_buffer_size: 100_000,
        }
    }
}

impl LinkerStateConfig {
    /// Configuration optimized for memory-constrained environments.
    /// Uses smaller caches (~200MB total).
    pub fn memory_saver() -> Self {
        Self {
            cluster_ids_capacity: 500_000,
            global_ids_capacity: 100_000,
            summaries_capacity: 50_000,
            perspectives_capacity: 500_000,
            dirty_buffer_size: 10_000,
        }
    }

    /// Configuration optimized for high-performance environments.
    /// Uses larger caches (~1GB total).
    pub fn high_performance() -> Self {
        Self {
            cluster_ids_capacity: 20_000_000,
            global_ids_capacity: 5_000_000,
            summaries_capacity: 2_000_000,
            perspectives_capacity: 20_000_000,
            dirty_buffer_size: 500_000,
        }
    }

    /// Unlimited capacity (disables LRU eviction).
    /// Only use for small datasets where everything fits in memory.
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
