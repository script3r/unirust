//! Cache-line aligned atomic counters for HFT
//!
//! Eliminates false sharing by ensuring each counter is on its own cache line.
//! Typical cache line size is 64 bytes on x86_64 and ARM64.

use std::sync::atomic::{AtomicU64, Ordering};

/// Cache line size in bytes (64 for modern x86_64 and ARM64)
pub const CACHE_LINE_SIZE: usize = 64;

/// A single atomic counter aligned to its own cache line.
///
/// This prevents false sharing when multiple counters are updated
/// concurrently from different CPU cores.
#[repr(C, align(64))]
#[derive(Debug)]
pub struct AlignedCounter {
    value: AtomicU64,
    // Padding to fill the cache line
    _pad: [u8; CACHE_LINE_SIZE - 8],
}

impl AlignedCounter {
    /// Create a new counter initialized to zero
    #[inline]
    pub const fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
            _pad: [0u8; CACHE_LINE_SIZE - 8],
        }
    }

    /// Create a new counter with initial value
    #[inline]
    pub const fn with_value(val: u64) -> Self {
        Self {
            value: AtomicU64::new(val),
            _pad: [0u8; CACHE_LINE_SIZE - 8],
        }
    }

    /// Load the current value
    #[inline]
    pub fn load(&self, ordering: Ordering) -> u64 {
        self.value.load(ordering)
    }

    /// Store a value
    #[inline]
    pub fn store(&self, val: u64, ordering: Ordering) {
        self.value.store(val, ordering)
    }

    /// Atomically add to the counter, returning the previous value
    #[inline]
    pub fn fetch_add(&self, val: u64, ordering: Ordering) -> u64 {
        self.value.fetch_add(val, ordering)
    }

    /// Atomically subtract from the counter, returning the previous value
    #[inline]
    pub fn fetch_sub(&self, val: u64, ordering: Ordering) -> u64 {
        self.value.fetch_sub(val, ordering)
    }

    /// Atomically increment by 1
    #[inline]
    pub fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a reference to the underlying atomic
    #[inline]
    pub fn as_atomic(&self) -> &AtomicU64 {
        &self.value
    }
}

impl Default for AlignedCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for AlignedCounter {
    fn clone(&self) -> Self {
        Self::with_value(self.load(Ordering::Relaxed))
    }
}

/// High-performance metrics with cache-line aligned counters.
///
/// Each counter occupies its own cache line to prevent false sharing.
#[repr(C)]
#[derive(Debug)]
pub struct AlignedMetrics {
    /// Total records processed
    pub records_processed: AlignedCounter,
    /// Total batches processed
    pub batches_processed: AlignedCounter,
    /// Total clusters created
    pub clusters_created: AlignedCounter,
    /// Total merges performed
    pub merges_performed: AlignedCounter,
    /// Total conflicts detected
    pub conflicts_detected: AlignedCounter,
    /// Cache hits in hot path
    pub cache_hits: AlignedCounter,
    /// Cache misses in hot path
    pub cache_misses: AlignedCounter,
    /// Hot key exits (early termination)
    pub hot_key_exits: AlignedCounter,
    /// Queue depth (current pending)
    pub queue_depth: AlignedCounter,
    /// Total latency in microseconds
    pub total_latency_us: AlignedCounter,
    /// Maximum latency in microseconds
    pub max_latency_us: AlignedCounter,
    /// Dropped requests due to backpressure
    pub dropped_requests: AlignedCounter,
}

impl AlignedMetrics {
    /// Create new metrics initialized to zero
    pub const fn new() -> Self {
        Self {
            records_processed: AlignedCounter::new(),
            batches_processed: AlignedCounter::new(),
            clusters_created: AlignedCounter::new(),
            merges_performed: AlignedCounter::new(),
            conflicts_detected: AlignedCounter::new(),
            cache_hits: AlignedCounter::new(),
            cache_misses: AlignedCounter::new(),
            hot_key_exits: AlignedCounter::new(),
            queue_depth: AlignedCounter::new(),
            total_latency_us: AlignedCounter::new(),
            max_latency_us: AlignedCounter::new(),
            dropped_requests: AlignedCounter::new(),
        }
    }

    /// Record a batch completion with latency
    #[inline]
    pub fn record_batch(&self, record_count: u64, latency_us: u64) {
        self.records_processed
            .fetch_add(record_count, Ordering::Relaxed);
        self.batches_processed.increment();
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);

        // Update max latency using CAS loop
        let mut current_max = self.max_latency_us.load(Ordering::Relaxed);
        while latency_us > current_max {
            match self.max_latency_us.as_atomic().compare_exchange_weak(
                current_max,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        let batches = self.batches_processed.load(Ordering::Relaxed);
        let total_latency = self.total_latency_us.load(Ordering::Relaxed);

        MetricsSnapshot {
            records_processed: self.records_processed.load(Ordering::Relaxed),
            batches_processed: batches,
            clusters_created: self.clusters_created.load(Ordering::Relaxed),
            merges_performed: self.merges_performed.load(Ordering::Relaxed),
            conflicts_detected: self.conflicts_detected.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            hot_key_exits: self.hot_key_exits.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            avg_latency_us: if batches > 0 {
                total_latency / batches
            } else {
                0
            },
            max_latency_us: self.max_latency_us.load(Ordering::Relaxed),
            dropped_requests: self.dropped_requests.load(Ordering::Relaxed),
        }
    }

    /// Reset all metrics to zero
    pub fn reset(&self) {
        self.records_processed.store(0, Ordering::Relaxed);
        self.batches_processed.store(0, Ordering::Relaxed);
        self.clusters_created.store(0, Ordering::Relaxed);
        self.merges_performed.store(0, Ordering::Relaxed);
        self.conflicts_detected.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.hot_key_exits.store(0, Ordering::Relaxed);
        self.queue_depth.store(0, Ordering::Relaxed);
        self.total_latency_us.store(0, Ordering::Relaxed);
        self.max_latency_us.store(0, Ordering::Relaxed);
        self.dropped_requests.store(0, Ordering::Relaxed);
    }
}

impl Default for AlignedMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time snapshot of metrics
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub records_processed: u64,
    pub batches_processed: u64,
    pub clusters_created: u64,
    pub merges_performed: u64,
    pub conflicts_detected: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub hot_key_exits: u64,
    pub queue_depth: u64,
    pub avg_latency_us: u64,
    pub max_latency_us: u64,
    pub dropped_requests: u64,
}

impl MetricsSnapshot {
    /// Calculate throughput in records per second
    pub fn throughput(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs > 0.0 {
            self.records_processed as f64 / elapsed_secs
        } else {
            0.0
        }
    }

    /// Calculate cache hit ratio
    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total > 0 {
            self.cache_hits as f64 / total as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn test_aligned_counter_size() {
        // Verify that AlignedCounter is exactly one cache line
        assert_eq!(mem::size_of::<AlignedCounter>(), CACHE_LINE_SIZE);
    }

    #[test]
    fn test_aligned_counter_alignment() {
        // Verify alignment
        assert_eq!(mem::align_of::<AlignedCounter>(), CACHE_LINE_SIZE);
    }

    #[test]
    fn test_aligned_metrics_no_false_sharing() {
        let metrics = AlignedMetrics::new();

        // Get pointers to two adjacent counters
        let ptr1 = &metrics.records_processed as *const AlignedCounter as usize;
        let ptr2 = &metrics.batches_processed as *const AlignedCounter as usize;

        // They should be exactly one cache line apart
        assert_eq!(ptr2 - ptr1, CACHE_LINE_SIZE);
    }

    #[test]
    fn test_metrics_recording() {
        let metrics = AlignedMetrics::new();

        metrics.record_batch(100, 500);
        metrics.record_batch(200, 1000);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.records_processed, 300);
        assert_eq!(snapshot.batches_processed, 2);
        assert_eq!(snapshot.avg_latency_us, 750);
        assert_eq!(snapshot.max_latency_us, 1000);
    }

    #[test]
    fn test_concurrent_increments() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(AlignedMetrics::new());
        let mut handles = vec![];

        for _ in 0..4 {
            let m = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                for _ in 0..10000 {
                    m.records_processed.increment();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(metrics.records_processed.load(Ordering::Relaxed), 40000);
    }
}
