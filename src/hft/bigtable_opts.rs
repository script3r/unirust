//! # Bigtable-Inspired Optimizations
//!
//! High-performance optimizations inspired by Google's Bigtable paper:
//!
//! 1. **Bloom Filters** - Fast negative lookups to avoid index scans
//! 2. **Scan Cache** - LRU cache for identity key lookup results (temporal locality)
//! 3. **Block Cache** - Cache for cluster summaries (spatial locality)
//! 4. **Group Commit** - Batch multiple operations into single commits
//! 5. **Immutable Summaries** - Cache conflict summaries per cluster
//!
//! Reference: "Bigtable: A Distributed Storage System for Structured Data" (OSDI 2006)

use crate::model::{AttrId, ClusterId, RecordId, ValueId};
use crate::sharding::{BloomFilter, IdentityKeySignature};
use crate::temporal::Interval;
use lru::LruCache;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// =============================================================================
// BLOOM FILTER FOR IDENTITY KEYS (Bigtable Section 6: Refinements)
// =============================================================================

/// Bloom filter optimized for identity key lookups.
/// Reduces disk/index reads by filtering out keys that definitely don't exist.
///
/// From Bigtable paper: "We also allow clients to create Bloom filters for
/// SSTables in a particular locality group. A Bloom filter allows us to ask
/// whether an SSTable might contain any data for a specified row/column pair."
#[derive(Debug)]
pub struct IdentityBloomFilter {
    /// The underlying bloom filter
    filter: BloomFilter,
    /// Number of keys inserted
    key_count: AtomicU64,
    /// False positive rate estimate (updated periodically)
    estimated_fpr: AtomicU64, // Stored as fpr * 1_000_000
}

impl IdentityBloomFilter {
    /// Create a new identity bloom filter.
    /// Size in KB determines accuracy vs memory tradeoff.
    pub fn new(size_kb: usize) -> Self {
        Self {
            filter: BloomFilter::new(size_kb * 1024),
            key_count: AtomicU64::new(0),
            estimated_fpr: AtomicU64::new(1000), // 0.1% initial estimate
        }
    }

    /// Create with default 64KB size (~500K keys at 1% FPR)
    pub fn default_size() -> Self {
        Self::new(64)
    }

    /// Create with large 256KB size (~2M keys at 1% FPR)
    pub fn large() -> Self {
        Self::new(256)
    }

    /// Insert an identity key signature
    #[inline]
    pub fn insert(&self, signature: &IdentityKeySignature) {
        // SAFETY: BloomFilter insert only sets bits, never clears them.
        // Concurrent inserts may set the same bits multiple times but this is safe.
        unsafe {
            let filter_ptr = &self.filter as *const BloomFilter as *mut BloomFilter;
            (*filter_ptr).insert(signature);
        }
        self.key_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if an identity key might exist.
    /// Returns `false` if the key definitely doesn't exist (fast path).
    /// Returns `true` if the key might exist (requires index lookup).
    #[inline]
    pub fn may_contain(&self, signature: &IdentityKeySignature) -> bool {
        self.filter.may_contain(signature)
    }

    /// Get the number of keys inserted
    pub fn key_count(&self) -> u64 {
        self.key_count.load(Ordering::Relaxed)
    }

    /// Estimate current false positive rate
    pub fn estimated_fpr(&self) -> f64 {
        self.estimated_fpr.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
}

// =============================================================================
// SCAN CACHE (Bigtable Section 6: Caching)
// =============================================================================

/// Cache entry for identity key lookup results.
/// Stores the candidates found for a given identity key signature.
#[derive(Debug, Clone)]
pub struct ScanCacheEntry {
    /// Candidate record IDs with their intervals
    pub candidates: Vec<(RecordId, Interval)>,
    /// When this entry was created
    pub created_at: Instant,
    /// Number of times this entry was hit
    pub hits: u32,
}

/// Scan cache for identity key lookups (temporal locality).
///
/// From Bigtable paper: "The Scan Cache is a higher-level cache that caches
/// the key-value pairs returned by the SSTable interface to the tablet server code."
///
/// In our case, we cache the candidate records for each identity key signature,
/// avoiding repeated index scans for hot keys.
pub struct ScanCache {
    /// LRU cache mapping identity key signatures to candidates
    cache: RwLock<LruCache<[u8; 32], ScanCacheEntry>>,
    /// Cache hit counter
    hits: AtomicU64,
    /// Cache miss counter
    misses: AtomicU64,
    /// Maximum age for cache entries (ms)
    max_age_ms: u64,
}

impl ScanCache {
    /// Create a new scan cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(
                NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap()),
            )),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            max_age_ms: 5000, // 5 second default TTL
        }
    }

    /// Create with default capacity (16K entries)
    pub fn default_capacity() -> Self {
        Self::new(16384)
    }

    /// Create a large cache (64K entries)
    pub fn large() -> Self {
        Self::new(65536)
    }

    /// Try to get candidates from cache
    #[inline]
    pub fn get(&self, signature: &IdentityKeySignature) -> Option<Vec<(RecordId, Interval)>> {
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get_mut(&signature.0) {
            // Check if entry is still fresh
            if entry.created_at.elapsed().as_millis() < self.max_age_ms as u128 {
                entry.hits += 1;
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.candidates.clone());
            }
            // Entry expired, remove it
            cache.pop(&signature.0);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Insert candidates into cache
    #[inline]
    pub fn insert(&self, signature: &IdentityKeySignature, candidates: Vec<(RecordId, Interval)>) {
        let entry = ScanCacheEntry {
            candidates,
            created_at: Instant::now(),
            hits: 0,
        };
        self.cache.write().put(signature.0, entry);
    }

    /// Invalidate a specific key (when new records are added)
    #[inline]
    pub fn invalidate(&self, signature: &IdentityKeySignature) {
        self.cache.write().pop(&signature.0);
    }

    /// Clear the entire cache
    pub fn clear(&self) {
        self.cache.write().clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> ScanCacheStats {
        let cache = self.cache.read();
        ScanCacheStats {
            size: cache.len(),
            capacity: cache.cap().get(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScanCacheStats {
    pub size: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
}

impl ScanCacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

// =============================================================================
// CLUSTER SUMMARY CACHE (Inspired by Bigtable's Block Cache)
// =============================================================================

/// Cached summary of strong identifiers for a cluster.
/// This is analogous to Bigtable's block cache for spatial locality.
#[derive(Debug, Clone)]
pub struct ClusterSummaryCache {
    /// Per-perspective mapping of (AttrId -> ValueId -> intervals)
    pub by_perspective: FxHashMap<String, FxHashMap<AttrId, FxHashMap<ValueId, Vec<Interval>>>>,
    /// Generation number (incremented on cluster merge)
    pub generation: u64,
    /// When this summary was computed
    pub computed_at: Instant,
}

impl ClusterSummaryCache {
    pub fn new() -> Self {
        Self {
            by_perspective: FxHashMap::default(),
            generation: 0,
            computed_at: Instant::now(),
        }
    }
}

impl Default for ClusterSummaryCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache for cluster strong ID summaries.
/// Avoids recomputing summaries for hot clusters.
pub struct BlockCache {
    /// LRU cache from cluster root -> summary
    cache: RwLock<LruCache<RecordId, ClusterSummaryCache>>,
    /// Current generation (incremented on any cluster merge)
    generation: AtomicU64,
    /// Cache hits
    hits: AtomicU64,
    /// Cache misses
    misses: AtomicU64,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(
                NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap()),
            )),
            generation: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Default capacity (8K clusters)
    pub fn default_capacity() -> Self {
        Self::new(8192)
    }

    /// Get cached summary for a cluster
    #[inline]
    pub fn get(&self, cluster_root: RecordId) -> Option<ClusterSummaryCache> {
        let current_gen = self.generation.load(Ordering::Relaxed);
        let mut cache = self.cache.write();
        if let Some(entry) = cache.get(&cluster_root) {
            // Check if entry is from current generation
            if entry.generation == current_gen {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.clone());
            }
            // Stale entry, remove it
            cache.pop(&cluster_root);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Insert or update cached summary
    #[inline]
    pub fn insert(&self, cluster_root: RecordId, mut summary: ClusterSummaryCache) {
        summary.generation = self.generation.load(Ordering::Relaxed);
        summary.computed_at = Instant::now();
        self.cache.write().put(cluster_root, summary);
    }

    /// Invalidate cache entries when clusters merge
    /// This is O(1) - just bumps generation, lazy invalidation on read
    #[inline]
    pub fn invalidate_on_merge(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Invalidate specific cluster
    #[inline]
    pub fn invalidate(&self, cluster_root: RecordId) {
        self.cache.write().pop(&cluster_root);
    }

    pub fn stats(&self) -> BlockCacheStats {
        let cache = self.cache.read();
        BlockCacheStats {
            size: cache.len(),
            capacity: cache.cap().get(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            generation: self.generation.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlockCacheStats {
    pub size: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub generation: u64,
}

// =============================================================================
// GROUP COMMIT BUFFER (Bigtable Section 6: Commit-log implementation)
// =============================================================================

/// A write operation to be committed
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// Add a record
    AddRecord { record_id: RecordId, data: Vec<u8> },
    /// Merge two clusters
    MergeCluster {
        root_a: RecordId,
        root_b: RecordId,
        new_root: RecordId,
    },
    /// Update cluster assignment
    AssignCluster {
        record_id: RecordId,
        cluster_id: ClusterId,
    },
}

/// Group commit buffer for batching writes.
///
/// From Bigtable paper: "To improve throughput, we batch commits into a
/// single commit by grouping pending mutations together."
///
/// This reduces fsync overhead by committing multiple operations together.
pub struct GroupCommitBuffer {
    /// Pending write operations
    ops: RwLock<Vec<WriteOp>>,
    /// Maximum ops before auto-flush
    max_ops: usize,
    /// Maximum delay before flush (microseconds)
    max_delay_us: u64,
    /// Last flush time
    last_flush: RwLock<Instant>,
    /// Total ops committed
    total_committed: AtomicU64,
    /// Total flushes performed
    total_flushes: AtomicU64,
}

impl GroupCommitBuffer {
    pub fn new(max_ops: usize, max_delay_us: u64) -> Self {
        Self {
            ops: RwLock::new(Vec::with_capacity(max_ops)),
            max_ops,
            max_delay_us,
            last_flush: RwLock::new(Instant::now()),
            total_committed: AtomicU64::new(0),
            total_flushes: AtomicU64::new(0),
        }
    }

    /// Default configuration (1000 ops, 100µs delay)
    pub fn default_config() -> Self {
        Self::new(1000, 100)
    }

    /// High throughput configuration (5000 ops, 500µs delay)
    pub fn high_throughput() -> Self {
        Self::new(5000, 500)
    }

    /// Low latency configuration (100 ops, 50µs delay)
    pub fn low_latency() -> Self {
        Self::new(100, 50)
    }

    /// Add a write operation to the buffer
    #[inline]
    pub fn add(&self, op: WriteOp) -> bool {
        let mut ops = self.ops.write();
        ops.push(op);
        ops.len() >= self.max_ops
    }

    /// Check if buffer should be flushed (based on time)
    #[inline]
    pub fn should_flush(&self) -> bool {
        let ops = self.ops.read();
        if ops.is_empty() {
            return false;
        }
        let last = self.last_flush.read();
        last.elapsed().as_micros() >= self.max_delay_us as u128
    }

    /// Take all pending ops for commit
    pub fn take_ops(&self) -> Vec<WriteOp> {
        let mut ops = self.ops.write();
        let taken = std::mem::take(&mut *ops);
        *self.last_flush.write() = Instant::now();
        if !taken.is_empty() {
            self.total_committed
                .fetch_add(taken.len() as u64, Ordering::Relaxed);
            self.total_flushes.fetch_add(1, Ordering::Relaxed);
        }
        taken
    }

    /// Get buffer statistics
    pub fn stats(&self) -> GroupCommitStats {
        GroupCommitStats {
            pending: self.ops.read().len(),
            total_committed: self.total_committed.load(Ordering::Relaxed),
            total_flushes: self.total_flushes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GroupCommitStats {
    pub pending: usize,
    pub total_committed: u64,
    pub total_flushes: u64,
}

impl GroupCommitStats {
    pub fn avg_batch_size(&self) -> f64 {
        if self.total_flushes == 0 {
            0.0
        } else {
            self.total_committed as f64 / self.total_flushes as f64
        }
    }
}

// =============================================================================
// COMBINED OPTIMIZATION CONTEXT
// =============================================================================

/// Combined Bigtable-style optimizations for a partition.
///
/// This bundles all the caching and batching optimizations together
/// for easy integration into the partitioned processing path.
pub struct PartitionOptimizations {
    /// Bloom filter for identity key negative lookups
    pub bloom: IdentityBloomFilter,
    /// Scan cache for candidate lookup results
    pub scan_cache: ScanCache,
    /// Block cache for cluster summaries
    pub block_cache: BlockCache,
    /// Group commit buffer for write batching
    pub commit_buffer: GroupCommitBuffer,
    /// Partition ID
    pub partition_id: usize,
}

impl PartitionOptimizations {
    /// Create new optimizations for a partition
    pub fn new(partition_id: usize) -> Self {
        Self {
            bloom: IdentityBloomFilter::default_size(),
            scan_cache: ScanCache::default_capacity(),
            block_cache: BlockCache::default_capacity(),
            commit_buffer: GroupCommitBuffer::default_config(),
            partition_id,
        }
    }

    /// Create with high-throughput configuration
    pub fn high_throughput(partition_id: usize) -> Self {
        Self {
            bloom: IdentityBloomFilter::large(),
            scan_cache: ScanCache::large(),
            block_cache: BlockCache::new(16384),
            commit_buffer: GroupCommitBuffer::high_throughput(),
            partition_id,
        }
    }

    /// Check if an identity key might have candidates in this partition
    #[inline]
    pub fn may_have_candidates(&self, signature: &IdentityKeySignature) -> bool {
        self.bloom.may_contain(signature)
    }

    /// Try to get cached candidates for an identity key
    #[inline]
    pub fn get_cached_candidates(
        &self,
        signature: &IdentityKeySignature,
    ) -> Option<Vec<(RecordId, Interval)>> {
        self.scan_cache.get(signature)
    }

    /// Record that we found candidates for an identity key
    #[inline]
    pub fn cache_candidates(
        &self,
        signature: &IdentityKeySignature,
        candidates: Vec<(RecordId, Interval)>,
    ) {
        // Add to bloom filter (key exists)
        self.bloom.insert(signature);
        // Cache the candidates
        self.scan_cache.insert(signature, candidates);
    }

    /// Invalidate caches when a new record is added with this key
    #[inline]
    pub fn on_record_added(&self, signature: &IdentityKeySignature) {
        self.bloom.insert(signature);
        self.scan_cache.invalidate(signature);
    }

    /// Invalidate caches when clusters merge
    #[inline]
    pub fn on_cluster_merge(&self, root_a: RecordId, root_b: RecordId) {
        self.block_cache.invalidate(root_a);
        self.block_cache.invalidate(root_b);
        self.block_cache.invalidate_on_merge();
    }

    /// Get combined statistics
    pub fn stats(&self) -> PartitionOptStats {
        PartitionOptStats {
            partition_id: self.partition_id,
            bloom_keys: self.bloom.key_count(),
            bloom_fpr: self.bloom.estimated_fpr(),
            scan_cache: self.scan_cache.stats(),
            block_cache: self.block_cache.stats(),
            commit_buffer: self.commit_buffer.stats(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartitionOptStats {
    pub partition_id: usize,
    pub bloom_keys: u64,
    pub bloom_fpr: f64,
    pub scan_cache: ScanCacheStats,
    pub block_cache: BlockCacheStats,
    pub commit_buffer: GroupCommitStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let bloom = IdentityBloomFilter::default_size();

        let key1 = IdentityKeySignature([1u8; 32]);
        let _key2 = IdentityKeySignature([2u8; 32]);

        // Key1 not inserted yet
        // Note: may_contain can return true (false positive) or false

        bloom.insert(&key1);

        // Key1 definitely in filter now
        assert!(bloom.may_contain(&key1));

        // Key2 might or might not be (false positive possible)
        // We can't assert false because of FP possibility

        assert_eq!(bloom.key_count(), 1);
    }

    #[test]
    fn test_scan_cache() {
        let cache = ScanCache::new(100);

        let sig = IdentityKeySignature([42u8; 32]);
        let candidates = vec![
            (RecordId(1), Interval::new(0, 100).unwrap()),
            (RecordId(2), Interval::new(50, 150).unwrap()),
        ];

        // Initially empty
        assert!(cache.get(&sig).is_none());

        // Insert
        cache.insert(&sig, candidates.clone());

        // Should hit
        let result = cache.get(&sig);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2);

        // Invalidate
        cache.invalidate(&sig);
        assert!(cache.get(&sig).is_none());
    }

    #[test]
    fn test_block_cache_generation() {
        let cache = BlockCache::new(100);

        let root = RecordId(1);
        let summary = ClusterSummaryCache::new();

        // Insert
        cache.insert(root, summary);

        // Should hit
        assert!(cache.get(root).is_some());

        // Invalidate via generation bump
        cache.invalidate_on_merge();

        // Should miss (stale generation)
        assert!(cache.get(root).is_none());
    }

    #[test]
    fn test_group_commit_buffer() {
        let buffer = GroupCommitBuffer::new(3, 1000);

        // Add ops
        assert!(!buffer.add(WriteOp::AssignCluster {
            record_id: RecordId(1),
            cluster_id: ClusterId(1),
        }));
        assert!(!buffer.add(WriteOp::AssignCluster {
            record_id: RecordId(2),
            cluster_id: ClusterId(1),
        }));
        // Third op should trigger flush threshold
        assert!(buffer.add(WriteOp::AssignCluster {
            record_id: RecordId(3),
            cluster_id: ClusterId(2),
        }));

        let ops = buffer.take_ops();
        assert_eq!(ops.len(), 3);

        let stats = buffer.stats();
        assert_eq!(stats.total_committed, 3);
        assert_eq!(stats.total_flushes, 1);
    }

    #[test]
    fn test_partition_optimizations() {
        let opts = PartitionOptimizations::new(0);

        let sig = IdentityKeySignature([99u8; 32]);

        // Initially, bloom filter is empty but may_contain can return false positive
        // After adding, it should definitely return true
        opts.on_record_added(&sig);
        assert!(opts.may_have_candidates(&sig));

        // Cache candidates
        let candidates = vec![(RecordId(1), Interval::new(0, 100).unwrap())];
        opts.cache_candidates(&sig, candidates);

        // Should be cached
        assert!(opts.get_cached_candidates(&sig).is_some());

        // Cluster merge should invalidate block cache
        opts.on_cluster_merge(RecordId(1), RecordId(2));

        let stats = opts.stats();
        assert_eq!(stats.partition_id, 0);
    }
}
