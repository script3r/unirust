//! Default constants for unirust configuration.
//!
//! All magic numbers are centralized here with documentation.

// =============================================================================
// Network Defaults
// =============================================================================

/// Default shard listen address
pub const DEFAULT_SHARD_ADDR: &str = "127.0.0.1:50061";

/// Default router listen address
pub const DEFAULT_ROUTER_ADDR: &str = "127.0.0.1:50060";

// =============================================================================
// Storage Defaults (RocksDB)
// =============================================================================

/// Default block cache size in MB
/// Larger values improve read performance for frequently accessed data.
pub const DEFAULT_BLOCK_CACHE_MB: usize = 512;

/// Default write buffer size in MB
/// Larger values batch more writes before flushing to disk.
pub const DEFAULT_WRITE_BUFFER_MB: usize = 128;

/// Default number of background compaction jobs
/// Scale with available CPU cores for better compaction throughput.
pub const DEFAULT_BACKGROUND_JOBS: usize = 4;

/// Default memtable size in bytes (64MB)
pub const DEFAULT_MEMTABLE_SIZE: usize = 64 * 1024 * 1024;

/// Default max write buffer number
pub const DEFAULT_MAX_WRITE_BUFFER_NUMBER: i32 = 4;

/// Default level0 file number trigger for compaction
pub const DEFAULT_LEVEL0_FILE_NUM_COMPACTION_TRIGGER: i32 = 4;

// =============================================================================
// Reconciliation Defaults
// =============================================================================

/// Maximum staleness before forced reconcile (seconds)
/// Controls how stale cross-shard data can be before triggering reconciliation.
pub const DEFAULT_MAX_STALENESS_SECS: u64 = 60;

/// Minimum interval between reconciles (seconds)
/// Prevents reconciliation thrashing under high load.
pub const DEFAULT_MIN_RECONCILE_INTERVAL_SECS: u64 = 5;

// =============================================================================
// Partitioning Defaults
// =============================================================================

/// Default number of partitions when not specified
/// Uses number of CPU cores for optimal parallelism.
pub fn default_partition_count() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(8)
}

/// Default number of ingest worker threads
pub const DEFAULT_INGEST_WORKERS: usize = 32;

// =============================================================================
// Streaming Engine Defaults
// =============================================================================

/// Default candidate cap for entity matching
pub const DEFAULT_CANDIDATE_CAP: usize = 2000;

/// High threshold for adaptive candidate capping
pub const DEFAULT_ADAPTIVE_HIGH_THRESHOLD: usize = 10_000;

/// Mid threshold for adaptive candidate capping
pub const DEFAULT_ADAPTIVE_MID_THRESHOLD: usize = 2_000;

/// Candidate cap when above high threshold
pub const DEFAULT_ADAPTIVE_HIGH_CAP: usize = 500;

/// Candidate cap when above mid threshold
pub const DEFAULT_ADAPTIVE_MID_CAP: usize = 1000;

/// Hot key threshold for special handling of high-cardinality keys
pub const DEFAULT_HOT_KEY_THRESHOLD: usize = 50_000;

/// Threshold above which stochastic sampling activates
pub const DEFAULT_SAMPLING_THRESHOLD: usize = 500;

/// Target number of candidates when using stochastic sampling
pub const DEFAULT_SAMPLING_TARGET: usize = 200;

// =============================================================================
// WAL Defaults
// =============================================================================

/// Maximum delay before flushing partial WAL batch (microseconds)
pub const DEFAULT_WAL_COALESCE_DELAY_US: u64 = 100;

/// Maximum records to coalesce before WAL flush
pub const DEFAULT_WAL_COALESCE_RECORDS: usize = 1000;

/// WAL channel capacity for backpressure
pub const DEFAULT_WAL_CHANNEL_CAPACITY: usize = 1000;

// =============================================================================
// Linker State Cache Defaults (for billion-scale)
// =============================================================================

/// Default cluster IDs cache capacity
pub const DEFAULT_CLUSTER_IDS_CAPACITY: usize = 5_000_000;

/// Default global IDs cache capacity
pub const DEFAULT_GLOBAL_IDS_CAPACITY: usize = 1_000_000;

/// Default summaries cache capacity
pub const DEFAULT_SUMMARIES_CAPACITY: usize = 500_000;

/// Default perspectives cache capacity
pub const DEFAULT_PERSPECTIVES_CAPACITY: usize = 5_000_000;

/// Default dirty buffer size before flushing
pub const DEFAULT_DIRTY_BUFFER_SIZE: usize = 100_000;

// =============================================================================
// Batch Processing Defaults
// =============================================================================

/// Default batch size for record processing
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Default channel buffer size for async processing
pub const DEFAULT_CHANNEL_BUFFER: usize = 100;
