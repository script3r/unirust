#[derive(Debug, Clone)]
pub struct StreamingTuning {
    pub candidate_cap: usize,
    pub adaptive_candidate_cap: bool,
    pub adaptive_high_threshold: usize,
    pub adaptive_mid_threshold: usize,
    pub adaptive_high_cap: usize,
    pub adaptive_mid_cap: usize,
    pub deferred_reconciliation: bool,
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
        }
    }
}
