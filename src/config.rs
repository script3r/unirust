use crate::conflicts::ConflictAlgorithm;

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
        }
    }
}
