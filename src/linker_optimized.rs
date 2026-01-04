//! Optimized Streaming Linker Utilities
//!
//! Key optimizations:
//! - Zero-copy key signatures using borrowed references
//! - Pre-allocated candidate buffers (no SmallVec overflow)
//! - Inline metrics without atomic operations in hot path
//! - Batched DSU operations to amortize merge overhead

use crate::model::{KeyValue, RecordId};
use crate::temporal::Interval;
use rustc_hash::FxHashSet;
use std::sync::atomic::{AtomicU64, Ordering};

// ============================================================================
// Zero-Copy Key Signature
// ============================================================================

/// Borrowed key signature that avoids allocation during lookup.
/// Uses precomputed hash for O(1) comparison.
#[derive(Clone, Copy, Debug)]
pub struct KeySignatureRef<'a> {
    entity_type: &'a str,
    key_values: &'a [KeyValue],
    hash: u64,
}

impl<'a> KeySignatureRef<'a> {
    #[inline]
    pub fn new(entity_type: &'a str, key_values: &'a [KeyValue]) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = rustc_hash::FxHasher::default();
        entity_type.hash(&mut hasher);
        key_values.hash(&mut hasher);
        let hash = hasher.finish();

        Self {
            entity_type,
            key_values,
            hash,
        }
    }

    #[inline]
    pub fn entity_type(&self) -> &str {
        self.entity_type
    }

    #[inline]
    pub fn key_values(&self) -> &[KeyValue] {
        self.key_values
    }

    #[inline]
    pub fn precomputed_hash(&self) -> u64 {
        self.hash
    }

    /// Convert to owned signature (only when needed for storage)
    pub fn to_owned(&self) -> OwnedKeySignature {
        OwnedKeySignature {
            entity_type: self.entity_type.to_string(),
            key_values: self.key_values.to_vec(),
            hash: self.hash,
        }
    }
}

impl<'a> PartialEq for KeySignatureRef<'a> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
            && self.entity_type == other.entity_type
            && self.key_values == other.key_values
    }
}

impl<'a> Eq for KeySignatureRef<'a> {}

impl<'a> std::hash::Hash for KeySignatureRef<'a> {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

/// Owned key signature for storage in hash sets
#[derive(Clone, Debug)]
pub struct OwnedKeySignature {
    entity_type: String,
    key_values: Vec<KeyValue>,
    hash: u64,
}

impl PartialEq for OwnedKeySignature {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
            && self.entity_type == other.entity_type
            && self.key_values == other.key_values
    }
}

impl Eq for OwnedKeySignature {}

impl std::hash::Hash for OwnedKeySignature {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl OwnedKeySignature {
    /// Create a borrowed reference (zero-cost)
    #[inline]
    pub fn as_ref(&self) -> KeySignatureRef<'_> {
        KeySignatureRef {
            entity_type: &self.entity_type,
            key_values: &self.key_values,
            hash: self.hash,
        }
    }

    /// Check equality with a borrowed signature (avoids allocation)
    #[inline]
    pub fn eq_ref(&self, other: &KeySignatureRef<'_>) -> bool {
        self.hash == other.hash
            && self.entity_type == other.entity_type
            && self.key_values == other.key_values
    }
}

// ============================================================================
// Pre-allocated Candidate Buffer
// ============================================================================

/// Fixed-capacity candidate buffer that never allocates.
/// Sized for 99th percentile workload (128 candidates covers most cases).
pub struct CandidateBuffer {
    data: [(RecordId, Interval); 128],
    len: usize,
}

impl CandidateBuffer {
    pub const CAPACITY: usize = 128;

    /// Create a new candidate buffer
    pub fn new() -> Self {
        // Use a default interval - will be overwritten before use
        let default_interval = Interval::new(0, 1).expect("valid default interval");
        Self {
            data: [(RecordId(0), default_interval); 128],
            len: 0,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    #[inline]
    pub fn push(&mut self, record_id: RecordId, interval: Interval) -> bool {
        if self.len >= Self::CAPACITY {
            return false; // Buffer full, signal hot key
        }
        self.data[self.len] = (record_id, interval);
        self.len += 1;
        true
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn as_slice(&self) -> &[(RecordId, Interval)] {
        &self.data[..self.len]
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &(RecordId, Interval)> {
        self.data[..self.len].iter()
    }
}

impl Default for CandidateBuffer {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Thread-Local Metrics (No Atomic Overhead in Hot Path)
// ============================================================================

/// Per-thread metrics that are periodically flushed to global counters.
/// Eliminates atomic operations from the hot path.
pub struct ThreadLocalMetrics {
    records_linked: u64,
    clusters_created: u64,
    merges_performed: u64,
    conflicts_detected: u64,
    hot_key_exits: u64,
    /// Flush every N operations to reduce global atomic contention
    flush_threshold: u64,
    ops_since_flush: u64,
}

impl ThreadLocalMetrics {
    pub fn new() -> Self {
        Self {
            records_linked: 0,
            clusters_created: 0,
            merges_performed: 0,
            conflicts_detected: 0,
            hot_key_exits: 0,
            flush_threshold: 1000,
            ops_since_flush: 0,
        }
    }

    #[inline]
    pub fn record_linked(&mut self) {
        self.records_linked += 1;
        self.ops_since_flush += 1;
    }

    #[inline]
    pub fn cluster_created(&mut self) {
        self.clusters_created += 1;
    }

    #[inline]
    pub fn merge_performed(&mut self) {
        self.merges_performed += 1;
    }

    #[inline]
    pub fn conflict_detected(&mut self) {
        self.conflicts_detected += 1;
    }

    #[inline]
    pub fn hot_key_exit(&mut self) {
        self.hot_key_exits += 1;
    }

    #[inline]
    pub fn should_flush(&self) -> bool {
        self.ops_since_flush >= self.flush_threshold
    }

    /// Flush to global metrics and reset local counters
    pub fn flush_to(&mut self, global: &GlobalMetrics) {
        global
            .records_linked
            .fetch_add(self.records_linked, Ordering::Relaxed);
        global
            .clusters_created
            .fetch_add(self.clusters_created, Ordering::Relaxed);
        global
            .merges_performed
            .fetch_add(self.merges_performed, Ordering::Relaxed);
        global
            .conflicts_detected
            .fetch_add(self.conflicts_detected, Ordering::Relaxed);
        global
            .hot_key_exits
            .fetch_add(self.hot_key_exits, Ordering::Relaxed);

        self.records_linked = 0;
        self.clusters_created = 0;
        self.merges_performed = 0;
        self.conflicts_detected = 0;
        self.hot_key_exits = 0;
        self.ops_since_flush = 0;
    }
}

impl Default for ThreadLocalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Global metrics aggregated from thread-local counters
#[derive(Debug, Default)]
pub struct GlobalMetrics {
    pub records_linked: AtomicU64,
    pub clusters_created: AtomicU64,
    pub merges_performed: AtomicU64,
    pub conflicts_detected: AtomicU64,
    pub hot_key_exits: AtomicU64,
}

impl GlobalMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            records_linked: self.records_linked.load(Ordering::Relaxed),
            clusters_created: self.clusters_created.load(Ordering::Relaxed),
            merges_performed: self.merges_performed.load(Ordering::Relaxed),
            conflicts_detected: self.conflicts_detected.load(Ordering::Relaxed),
            hot_key_exits: self.hot_key_exits.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub records_linked: u64,
    pub clusters_created: u64,
    pub merges_performed: u64,
    pub conflicts_detected: u64,
    pub hot_key_exits: u64,
}

// ============================================================================
// Tuning Parameters
// ============================================================================

/// Tuning parameters for the optimized linker
#[derive(Clone, Debug)]
pub struct OptimizedLinkerTuning {
    /// Maximum candidates before declaring hot key
    pub hot_key_threshold: usize,
    /// Candidate cap for normal processing
    pub candidate_cap: usize,
    /// Enable deferred reconciliation for hot keys
    pub deferred_reconciliation: bool,
    /// Shard ID for global cluster ID generation
    pub shard_id: u16,
}

impl Default for OptimizedLinkerTuning {
    fn default() -> Self {
        Self {
            hot_key_threshold: 1000,
            candidate_cap: 500,
            deferred_reconciliation: true,
            shard_id: 0,
        }
    }
}

// ============================================================================
// Hot Key Tracker
// ============================================================================

/// Tracks hot (tainted) keys that should be skipped during linking.
/// Uses fast hash-based lookup.
pub struct HotKeyTracker {
    tainted_keys: FxHashSet<OwnedKeySignature>,
    pending_keys: FxHashSet<OwnedKeySignature>,
    deferred_reconciliation: bool,
}

impl HotKeyTracker {
    pub fn new(deferred_reconciliation: bool) -> Self {
        Self {
            tainted_keys: FxHashSet::default(),
            pending_keys: FxHashSet::default(),
            deferred_reconciliation,
        }
    }

    /// Check if key signature is tainted (hot)
    #[inline]
    pub fn is_tainted(&self, key_sig: &KeySignatureRef<'_>) -> bool {
        self.tainted_keys.iter().any(|owned| owned.eq_ref(key_sig))
    }

    /// Mark a key as tainted
    #[inline]
    pub fn mark_tainted(&mut self, key_sig: KeySignatureRef<'_>) {
        let owned = key_sig.to_owned();
        self.tainted_keys.insert(owned.clone());
        if self.deferred_reconciliation {
            self.pending_keys.insert(owned);
        }
    }

    /// Get count of tainted keys
    pub fn tainted_count(&self) -> usize {
        self.tainted_keys.len()
    }

    /// Get count of pending keys
    pub fn pending_count(&self) -> usize {
        self.pending_keys.len()
    }

    /// Clear pending keys after reconciliation
    pub fn clear_pending(&mut self) {
        self.pending_keys.clear();
    }
}

impl Default for HotKeyTracker {
    fn default() -> Self {
        Self::new(true)
    }
}

// ============================================================================
// Batched Merge Operations
// ============================================================================

/// Batch of pending merge operations for bulk application
pub struct MergeBatch {
    merges: Vec<(RecordId, RecordId, Interval)>,
    capacity: usize,
}

impl MergeBatch {
    pub fn new(capacity: usize) -> Self {
        Self {
            merges: Vec::with_capacity(capacity),
            capacity,
        }
    }

    #[inline]
    pub fn push(&mut self, a: RecordId, b: RecordId, interval: Interval) -> bool {
        if self.merges.len() >= self.capacity {
            return false;
        }
        self.merges.push((a, b, interval));
        true
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.merges.len() >= self.capacity
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.merges.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.merges.is_empty()
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (RecordId, RecordId, Interval)> + '_ {
        self.merges.drain(..)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_signature_ref() {
        let entity = "test_entity";
        let key_values = vec![KeyValue {
            attr: crate::model::AttrId(1),
            value: crate::model::ValueId(1),
        }];

        let sig1 = KeySignatureRef::new(entity, &key_values);
        let sig2 = KeySignatureRef::new(entity, &key_values);

        assert_eq!(sig1, sig2);
        assert_eq!(sig1.precomputed_hash(), sig2.precomputed_hash());
    }

    #[test]
    fn test_candidate_buffer() {
        let mut buffer = CandidateBuffer::new();
        let interval = Interval::new(0, 100).unwrap();

        assert!(buffer.is_empty());

        for i in 0..CandidateBuffer::CAPACITY {
            assert!(buffer.push(RecordId(i as u32), interval));
        }

        assert_eq!(buffer.len(), CandidateBuffer::CAPACITY);

        // Buffer full
        assert!(!buffer.push(RecordId(999), interval));

        buffer.clear();
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_thread_local_metrics() {
        let mut local = ThreadLocalMetrics::new();
        let global = GlobalMetrics::new();

        for _ in 0..500 {
            local.record_linked();
            local.merge_performed();
        }

        assert!(!local.should_flush());

        for _ in 0..500 {
            local.record_linked();
        }

        assert!(local.should_flush());

        local.flush_to(&global);

        assert_eq!(global.records_linked.load(Ordering::Relaxed), 1000);
        assert_eq!(global.merges_performed.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn test_merge_batch() {
        let mut batch = MergeBatch::new(10);
        let interval = Interval::new(0, 100).unwrap();

        for i in 0..10 {
            assert!(batch.push(RecordId(i), RecordId(i + 100), interval));
        }

        assert!(batch.is_full());
        assert!(!batch.push(RecordId(99), RecordId(999), interval));

        let drained: Vec<_> = batch.drain().collect();
        assert_eq!(drained.len(), 10);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_hot_key_tracker() {
        let mut tracker = HotKeyTracker::new(true);
        let entity = "test_entity";
        let key_values = vec![KeyValue {
            attr: crate::model::AttrId(1),
            value: crate::model::ValueId(1),
        }];

        let sig = KeySignatureRef::new(entity, &key_values);

        assert!(!tracker.is_tainted(&sig));

        tracker.mark_tainted(sig);

        assert!(tracker.is_tainted(&sig));
        assert_eq!(tracker.tainted_count(), 1);
        assert_eq!(tracker.pending_count(), 1);
    }
}
