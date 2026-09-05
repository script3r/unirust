//! # Streaming Linker Module
//!
//! Implements interval-aware streaming entity resolution and cluster assignment.
//!
//! ## Parallelism Architecture
//!
//! Batch linking extracts key values and strong-ID summaries in parallel. It then
//! processes records sequentially: find candidates, apply temporal guards and DSU
//! merges, and index each record before linking the next. Cluster mappings, member
//! lists, and guard summaries remain in memory even with persistent DSU/index backends.

use crate::dsu::TemporalGuard;
use crate::dsu::{Clusters, DsuBackend, MergeResult, TemporalDSU};
use crate::index::IndexBackend;
use crate::model::{ClusterId, GlobalClusterId, InternerLookup, KeyValue, Record, RecordId};
use crate::ontology::Ontology;
use crate::perf::bigtable_opts::PartitionOptimizations;
use crate::sharding::{
    BoundaryStrongId, IdentityKeySignature, IdentityKeySignature as ShardingKeySignature,
};
use crate::store::RecordStore;
use crate::temporal::Interval;
use anyhow::Result;
use lru::LruCache;
use rayon::prelude::*;
use rocksdb::DB;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, instrument, warn};

/// Store up to 32 candidates inline; larger candidate sets allocate on the heap.
type CandidateVec = SmallVec<[(RecordId, Interval); 32]>;

struct StoreLookup<'a>(&'a dyn RecordStore);

impl InternerLookup for StoreLookup<'_> {
    fn get_attr_string(&self, id: crate::model::AttrId) -> Option<String> {
        self.0.resolve_attr(id)
    }

    fn get_value_string(&self, id: crate::model::ValueId) -> Option<String> {
        self.0.resolve_value(id)
    }
}

fn candidate_scan_limit(tuning: &crate::StreamingTuning) -> usize {
    if tuning.adaptive_candidate_cap && tuning.adaptive_high_cap > 0 {
        tuning.adaptive_high_cap
    } else {
        tuning.candidate_cap.max(1)
    }
}

/// Data stored for boundary signatures to support cross-shard conflict detection.
#[derive(Debug, Clone)]
struct BoundaryData {
    /// Coalesced identity-key intervals for this (signature, cluster) combination.
    intervals: Vec<Interval>,
    /// Strong ID hashes per perspective for conflict detection.
    /// Key: hash of perspective name, Value: hash of (attr_id, value_id) tuples.
    perspective_strong_ids: HashMap<u64, u64>,
    /// Exact temporal observations used by the distributed merge guard.
    strong_ids: Vec<BoundaryStrongId>,
}

fn coalesce_boundary_intervals(intervals: &mut Vec<Interval>) {
    intervals.sort_by_key(|interval| (interval.start, interval.end));
    let mut coalesced: Vec<Interval> = Vec::with_capacity(intervals.len());
    for interval in intervals.drain(..) {
        if let Some(previous) = coalesced.last_mut() {
            if interval.start <= previous.end {
                previous.end = previous.end.max(interval.end);
                continue;
            }
        }
        coalesced.push(interval);
    }
    *intervals = coalesced;
}

fn sort_dedup_boundary_strong_ids(strong_ids: &mut Vec<BoundaryStrongId>) {
    strong_ids.sort_by(|left, right| {
        (
            &left.perspective,
            &left.attribute,
            &left.value,
            left.interval.start,
            left.interval.end,
        )
            .cmp(&(
                &right.perspective,
                &right.attribute,
                &right.value,
                right.interval.start,
                right.interval.end,
            ))
    });
    strong_ids.dedup();
}

impl BoundaryData {
    fn new(
        interval: Interval,
        perspective_strong_ids: HashMap<u64, u64>,
        strong_ids: Vec<BoundaryStrongId>,
    ) -> Self {
        Self {
            intervals: vec![interval],
            perspective_strong_ids,
            strong_ids,
        }
    }

    fn merge_from(&mut self, other: &Self) {
        self.intervals.extend(other.intervals.iter().copied());
        coalesce_boundary_intervals(&mut self.intervals);
        self.perspective_strong_ids
            .extend(other.perspective_strong_ids.clone());
        self.strong_ids.extend(other.strong_ids.iter().cloned());
        sort_dedup_boundary_strong_ids(&mut self.strong_ids);
    }
}

fn register_boundary_data(
    index: &mut crate::sharding::ClusterBoundaryIndex,
    signature: ShardingKeySignature,
    global_id: GlobalClusterId,
    data: &BoundaryData,
) {
    for interval in &data.intervals {
        index.register_boundary_key_with_conflict_data(
            signature,
            global_id,
            *interval,
            data.perspective_strong_ids.clone(),
            data.strong_ids.clone(),
        );
    }
}

/// Metrics for observability of the streaming linker.
/// All counters are atomic for thread-safe access without locking.
#[derive(Debug, Default)]
pub struct LinkerMetrics {
    /// Total records processed through the linker.
    pub records_linked: AtomicU64,
    /// Number of new clusters created.
    pub clusters_created: AtomicU64,
    /// Number of cluster merges performed.
    pub merges_performed: AtomicU64,
    /// Number of conflicts detected (prevented merges).
    pub conflicts_detected: AtomicU64,
    /// Cache hits in identity index lookups.
    pub cache_hits: AtomicU64,
    /// Cache misses in identity index lookups.
    pub cache_misses: AtomicU64,
    /// Hot key optimizations triggered (early exits).
    pub hot_key_exits: AtomicU64,
    /// Deferred reconciliations performed.
    pub reconciliations: AtomicU64,
    /// Stochastic sampling applied (candidates reduced).
    pub stochastic_samples: AtomicU64,
}

impl LinkerMetrics {
    /// Create new metrics with all counters at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a snapshot of current metrics values.
    pub fn snapshot(&self) -> LinkerMetricsSnapshot {
        LinkerMetricsSnapshot {
            records_linked: self.records_linked.load(Ordering::Relaxed),
            clusters_created: self.clusters_created.load(Ordering::Relaxed),
            merges_performed: self.merges_performed.load(Ordering::Relaxed),
            conflicts_detected: self.conflicts_detected.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            hot_key_exits: self.hot_key_exits.load(Ordering::Relaxed),
            reconciliations: self.reconciliations.load(Ordering::Relaxed),
            stochastic_samples: self.stochastic_samples.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.records_linked.store(0, Ordering::Relaxed);
        self.clusters_created.store(0, Ordering::Relaxed);
        self.merges_performed.store(0, Ordering::Relaxed);
        self.conflicts_detected.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.hot_key_exits.store(0, Ordering::Relaxed);
        self.reconciliations.store(0, Ordering::Relaxed);
        self.stochastic_samples.store(0, Ordering::Relaxed);
    }
}

/// A point-in-time snapshot of linker metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct LinkerMetricsSnapshot {
    pub records_linked: u64,
    pub clusters_created: u64,
    pub merges_performed: u64,
    pub conflicts_detected: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub hot_key_exits: u64,
    pub reconciliations: u64,
    pub stochastic_samples: u64,
}

impl LinkerMetricsSnapshot {
    /// Calculate cache hit rate as a percentage (0.0 to 1.0).
    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// Calculate average merges per record.
    pub fn merges_per_record(&self) -> f64 {
        if self.records_linked == 0 {
            0.0
        } else {
            self.merges_performed as f64 / self.records_linked as f64
        }
    }
}

// ============================================================================
// Parallel Extraction Structures
// ============================================================================

/// Result of parallel key extraction for a single record
struct ParallelExtractionResult {
    record_id: RecordId,
    entity_type: String,
    perspective: String,
    /// Key signature -> (key_values, interval, guard_reason)
    keys: Vec<(LinkerKeySignature, Vec<KeyValue>, Interval, String)>,
    /// Pre-computed strong ID summary
    strong_id_summary: Option<StrongIdSummary>,
}

/// A map wrapper supporting either retained HashMap entries or evicting LRU entries.
///
/// The LRU variant has no durable spill path. [`StreamingLinker`] retains all
/// correctness-critical mappings and summaries in HashMaps, regardless of requested
/// linker-state cache limits.
pub struct LinkerState<K: std::hash::Hash + Eq + Clone, V: Clone> {
    inner: LinkerStateInner<K, V>,
}

enum LinkerStateInner<K: std::hash::Hash + Eq + Clone, V: Clone> {
    HashMap(HashMap<K, V>),
    Lru(LruCache<K, V>),
}

impl<K: std::hash::Hash + Eq + Clone, V: Clone> LinkerState<K, V> {
    /// Create an unbounded state using HashMap.
    pub fn unbounded() -> Self {
        Self {
            inner: LinkerStateInner::HashMap(HashMap::new()),
        }
    }

    /// Create a bounded state using LruCache with the given capacity.
    pub fn bounded(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            inner: LinkerStateInner::Lru(LruCache::new(cap)),
        }
    }

    /// Get a value by key, promoting it in LRU if applicable.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        match &mut self.inner {
            LinkerStateInner::HashMap(map) => map.get(key),
            LinkerStateInner::Lru(lru) => lru.get(key),
        }
    }

    /// Get a value by key without promoting in LRU.
    pub fn peek(&self, key: &K) -> Option<&V> {
        match &self.inner {
            LinkerStateInner::HashMap(map) => map.get(key),
            LinkerStateInner::Lru(lru) => lru.peek(key),
        }
    }

    /// Insert a key-value pair, returning the old value if present.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        match &mut self.inner {
            LinkerStateInner::HashMap(map) => map.insert(key, value),
            LinkerStateInner::Lru(lru) => lru.put(key, value),
        }
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &K) -> bool {
        match &self.inner {
            LinkerStateInner::HashMap(map) => map.contains_key(key),
            LinkerStateInner::Lru(lru) => lru.contains(key),
        }
    }

    /// Get mutable access to a value, or insert default.
    pub fn entry_or_default(&mut self, key: K) -> &mut V
    where
        V: Default,
    {
        match &mut self.inner {
            LinkerStateInner::HashMap(map) => map.entry(key).or_default(),
            LinkerStateInner::Lru(lru) => {
                if !lru.contains(&key) {
                    lru.put(key.clone(), V::default());
                }
                lru.get_mut(&key).unwrap()
            }
        }
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        match &self.inner {
            LinkerStateInner::HashMap(map) => map.len(),
            LinkerStateInner::Lru(lru) => lru.len(),
        }
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove a key and return its value if present.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        match &mut self.inner {
            LinkerStateInner::HashMap(map) => map.remove(key),
            LinkerStateInner::Lru(lru) => lru.pop(key),
        }
    }

    /// Get a copy of the value by key (for Copy types).
    pub fn get_copy(&mut self, key: &K) -> Option<V>
    where
        V: Copy,
    {
        self.get(key).copied()
    }

    /// Iterate over all key-value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        match &self.inner {
            LinkerStateInner::HashMap(map) => LinkerStateIter::HashMap(map.iter()),
            LinkerStateInner::Lru(lru) => LinkerStateIter::Lru(lru.iter()),
        }
    }

    /// Iterate over all key-value pairs with mutable values.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        match &mut self.inner {
            LinkerStateInner::HashMap(map) => LinkerStateIterMut::HashMap(map.iter_mut()),
            LinkerStateInner::Lru(lru) => LinkerStateIterMut::Lru(lru.iter_mut()),
        }
    }
}

enum LinkerStateIter<'a, K, V> {
    HashMap(std::collections::hash_map::Iter<'a, K, V>),
    Lru(lru::Iter<'a, K, V>),
}

impl<'a, K, V> Iterator for LinkerStateIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            LinkerStateIter::HashMap(iter) => iter.next(),
            LinkerStateIter::Lru(iter) => iter.next(),
        }
    }
}

enum LinkerStateIterMut<'a, K, V> {
    HashMap(std::collections::hash_map::IterMut<'a, K, V>),
    Lru(lru::IterMut<'a, K, V>),
}

impl<'a, K, V> Iterator for LinkerStateIterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            LinkerStateIterMut::HashMap(iter) => iter.next(),
            LinkerStateIterMut::Lru(iter) => iter.next(),
        }
    }
}

/// Public function to build clusters using streaming semantics.
pub fn build_clusters(store: &dyn RecordStore, ontology: &Ontology) -> Result<Clusters> {
    build_clusters_streaming(store, ontology)
}

fn build_clusters_streaming(store: &dyn RecordStore, ontology: &Ontology) -> Result<Clusters> {
    let mut streamer = StreamingLinker::new(store, ontology, &crate::StreamingTuning::default())?;
    streamer.clusters_with_conflict_splitting(store, ontology)
}

/// Streaming linker for continuous clustering.
pub struct StreamingLinker {
    /// DSU backend - can be in-memory or persistent
    dsu: DsuBackend,
    /// Index backend - can be in-memory or tiered
    identity_index: IndexBackend,
    /// Retained cluster ID mappings; size grows with resolution state.
    cluster_ids: LinkerState<RecordId, ClusterId>,
    next_cluster_id: u32,
    /// Retained global cluster IDs for cross-shard tracking.
    global_cluster_ids: LinkerState<RecordId, GlobalClusterId>,
    /// Minimum durable record ID in each local cluster, independent of replay order.
    global_cluster_anchors: LinkerState<RecordId, u32>,
    /// Shard ID for this linker (used in GlobalClusterId generation).
    shard_id: u16,
    /// Retained strong ID summaries for conflict detection.
    strong_id_summaries: LinkerState<RecordId, StrongIdSummary>,
    /// Root aliases and member lists let queries hydrate only candidate clusters.
    member_roots: FxHashMap<RecordId, RecordId>,
    cluster_members: FxHashMap<RecordId, Vec<RecordId>>,
    /// Use FxHashSet for faster hashing (non-cryptographic, perfect for internal keys)
    tainted_identity_keys: FxHashSet<LinkerKeySignature>,
    /// Retained record perspectives for same-perspective conflict detection.
    record_perspectives: LinkerState<RecordId, String>,
    /// Use FxHashSet for faster hashing
    pending_keys: FxHashSet<LinkerKeySignature>,
    tuning: crate::StreamingTuning,
    /// Use FxHashMap for faster key stats lookup
    key_stats: FxHashMap<LinkerKeySignature, KeyStats>,
    /// Boundary keys for cross-shard reconciliation (deduplicated by key+cluster).
    /// Maps (signature, cluster) -> (interval, perspective_strong_ids) for that combination.
    boundary_signatures: BTreeMap<(ShardingKeySignature, GlobalClusterId), BoundaryData>,
    /// Dirty boundary keys - signatures modified since last reconciliation.
    dirty_boundary_keys: BTreeSet<ShardingKeySignature>,
    /// Cross-shard merge mappings: secondary -> primary.
    /// Used to redirect cluster ID lookups after cross-shard reconciliation.
    cross_shard_merges: HashMap<GlobalClusterId, GlobalClusterId>,
    /// Metrics for observability.
    metrics: LinkerMetrics,
    /// Bigtable-style optimizations (bloom filter, scan cache, block cache)
    partition_opts: Option<Arc<PartitionOptimizations>>,
}

impl StreamingLinker {
    /// Initialize a streaming linker from the current store snapshot.
    /// Uses shard_id from tuning configuration.
    pub fn new(
        store: &dyn RecordStore,
        ontology: &Ontology,
        tuning: &crate::StreamingTuning,
    ) -> Result<Self> {
        Self::new_with_shard_id(store, ontology, tuning, tuning.shard_id)
    }

    /// Initialize a streaming linker with a specific shard ID.
    /// Use this for distributed deployments where each shard has a unique ID.
    pub fn new_with_shard_id(
        store: &dyn RecordStore,
        ontology: &Ontology,
        tuning: &crate::StreamingTuning,
        shard_id: u16,
    ) -> Result<Self> {
        Self::new_with_backend(
            store,
            ontology,
            tuning,
            shard_id,
            DsuBackend::InMemory(TemporalDSU::new()),
        )
    }

    /// Initialize a streaming linker with a persistent DSU backend.
    /// DSU entries use RocksDB and caches; other linker state still grows in memory.
    pub fn new_with_persistent_dsu(
        store: &dyn RecordStore,
        ontology: &Ontology,
        tuning: &crate::StreamingTuning,
        shard_id: u16,
        db: Arc<DB>,
    ) -> Result<Self> {
        let dsu_config = tuning.dsu_config.clone().unwrap_or_default();
        let backend = DsuBackend::persistent(db, dsu_config)?;
        Self::new_with_backend(store, ontology, tuning, shard_id, backend)
    }

    /// Initialize a streaming linker with a specific DSU backend.
    pub fn new_with_backend(
        store: &dyn RecordStore,
        ontology: &Ontology,
        tuning: &crate::StreamingTuning,
        shard_id: u16,
        dsu: DsuBackend,
    ) -> Result<Self> {
        Self::new_with_backends(
            store,
            ontology,
            tuning,
            shard_id,
            dsu,
            IndexBackend::in_memory(),
        )
    }

    /// Initialize a streaming linker with a tiered index backend.
    /// Index buckets can spill to RocksDB; other linker state still grows in memory.
    pub fn new_with_tiered_index(
        store: &dyn RecordStore,
        ontology: &Ontology,
        tuning: &crate::StreamingTuning,
        shard_id: u16,
        dsu: DsuBackend,
        db: Option<Arc<DB>>,
    ) -> Result<Self> {
        let tier_config = tuning.tier_config.clone().unwrap_or_default();
        let index = IndexBackend::tiered(tier_config, db);
        Self::new_with_backends(store, ontology, tuning, shard_id, dsu, index)
    }

    /// Initialize a streaming linker with specific DSU and Index backends.
    pub fn new_with_backends(
        store: &dyn RecordStore,
        ontology: &Ontology,
        tuning: &crate::StreamingTuning,
        shard_id: u16,
        dsu: DsuBackend,
        mut identity_index: IndexBackend,
    ) -> Result<Self> {
        identity_index.clear()?;
        // These mappings and summaries affect resolution correctness. The bounded
        // backend cannot be enabled until evictions have a durable spill/read-through
        // path; silently dropping an old entry can split clusters after enough ingest.
        if tuning.linker_state_config.is_some() {
            tracing::warn!(
                "bounded linker state requested but durable spill is unavailable; \
                 using correctness-preserving unbounded state"
            );
        }
        let (cluster_ids, global_cluster_ids, strong_id_summaries, record_perspectives) = (
            LinkerState::unbounded(),
            LinkerState::unbounded(),
            LinkerState::unbounded(),
            LinkerState::unbounded(),
        );

        let mut streamer = Self {
            dsu,
            identity_index,
            cluster_ids,
            next_cluster_id: 0,
            global_cluster_ids,
            global_cluster_anchors: LinkerState::unbounded(),
            shard_id,
            strong_id_summaries,
            member_roots: FxHashMap::default(),
            cluster_members: FxHashMap::default(),
            tainted_identity_keys: FxHashSet::default(),
            record_perspectives,
            pending_keys: FxHashSet::default(),
            tuning: tuning.clone(),
            key_stats: FxHashMap::default(),
            boundary_signatures: BTreeMap::new(),
            dirty_boundary_keys: BTreeSet::new(),
            cross_shard_merges: HashMap::new(),
            metrics: LinkerMetrics::new(),
            partition_opts: None,
        };

        if !store.is_empty() {
            const RECOVERY_BATCH_SIZE: usize = 4_096;
            let mut batch = Vec::with_capacity(RECOVERY_BATCH_SIZE);
            let mut link_batch = |batch: &mut Vec<Record>| -> Result<()> {
                let records = batch.iter().collect::<Vec<_>>();
                streamer.link_records_batch_parallel_with_interner(
                    &records,
                    ontology,
                    &StoreLookup(store),
                )?;
                batch.clear();
                Ok(())
            };
            store.try_for_each_record_ordered(&mut |record| {
                batch.push(record);
                if batch.len() == RECOVERY_BATCH_SIZE {
                    link_batch(&mut batch)?;
                }
                Ok(())
            })?;
            if !batch.is_empty() {
                link_batch(&mut batch)?;
            }
        }

        store.ensure_healthy()?;
        Ok(streamer)
    }

    /// Get the shard ID for this linker.
    pub fn shard_id(&self) -> u16 {
        self.shard_id
    }

    /// Check if using persistent DSU backend.
    pub fn is_persistent(&self) -> bool {
        self.dsu.is_persistent()
    }

    /// Check if using tiered index backend.
    pub fn is_tiered_index(&self) -> bool {
        self.identity_index.is_tiered()
    }

    /// Flush DSU to disk (no-op for in-memory).
    pub fn flush_dsu(&mut self) -> Result<()> {
        self.dsu.flush()
    }

    /// Set Bigtable-style optimizations (bloom filter, scan cache).
    /// This enables fast negative lookups and candidate caching.
    pub fn set_partition_opts(&mut self, opts: Arc<PartitionOptimizations>) {
        self.partition_opts = Some(opts);
    }

    /// Get partition optimizations reference if set.
    pub fn partition_opts(&self) -> Option<&Arc<PartitionOptimizations>> {
        self.partition_opts.as_ref()
    }

    /// Link a newly added record to existing clusters and return its cluster ID.
    #[instrument(skip(self, store, ontology), level = "debug", fields(record = ?record_id))]
    pub fn link_record(
        &mut self,
        store: &dyn RecordStore,
        ontology: &Ontology,
        record_id: RecordId,
    ) -> Result<ClusterId> {
        self.link_record_with_interner(store, &StoreLookup(store), ontology, record_id)
    }

    /// Link a newly added record with an explicit interner for boundary tracking.
    #[instrument(skip(self, store, interner, ontology), level = "debug", fields(record = ?record_id))]
    pub fn link_record_with_interner(
        &mut self,
        store: &dyn RecordStore,
        interner: &dyn InternerLookup,
        ontology: &Ontology,
        record_id: RecordId,
    ) -> Result<ClusterId> {
        let _guard = crate::profile::profile_scope("link_record");
        if !self.dsu.has_record(record_id)? {
            self.dsu.add_record(record_id)?;
            self.global_cluster_anchors.insert(record_id, record_id.0);
        }
        self.register_cluster_member(record_id)?;

        // Try to get a reference first (avoids cloning), fall back to cloning if not available.
        let record_owned;
        let record: &Record = if let Some(r) = store.get_record_ref(record_id) {
            r
        } else {
            record_owned = store
                .get_record(record_id)
                .ok_or_else(|| anyhow::anyhow!("Record not found in store: {:?}", record_id))?;
            &record_owned
        };
        let record_perspective = record.identity.perspective.clone();
        self.record_perspectives
            .insert(record_id, record_perspective.clone());
        self.strong_id_summaries
            .entry_or_default(record_id)
            .merge(build_record_summary(record, ontology));

        let entity_type = &record.identity.entity_type;
        let identity_keys = ontology.identity_keys_for_type(entity_type);

        // Cache extracted key values to avoid duplicate extraction in add_record
        #[allow(clippy::type_complexity)]
        let mut cached_keys: Vec<(
            &crate::ontology::IdentityKey,
            Vec<(Vec<crate::model::KeyValue>, crate::temporal::Interval)>,
        )> = Vec::new();

        for identity_key in identity_keys {
            let _key_guard = crate::profile::profile_scope("identity_key_loop");
            let key_values_with_intervals = self
                .identity_index
                .extract_key_values_with_intervals(record, identity_key)?;

            // Cache for later insertion (we iterate by reference to avoid cloning)
            cached_keys.push((identity_key, key_values_with_intervals));
            let cached_entry = &cached_keys.last().unwrap().1;

            // Pre-compute guard reason string once per identity key (not per candidate)
            let guard_reason = format!("identity_key_{}", identity_key.name);

            for (key_values, interval) in cached_entry.iter() {
                let interval = *interval; // deref for use below
                let key_signature = LinkerKeySignature::new(entity_type, key_values);

                // Create identity key signature for bloom filter and scan cache lookups
                let identity_sig = IdentityKeySignature::from_key_values(entity_type, key_values);

                // === BIGTABLE OPTIMIZATION: Bloom filter fast negative lookup ===
                // If bloom filter says key definitely doesn't exist, skip index lookup entirely
                let bloom_negative = self
                    .partition_opts
                    .as_ref()
                    .map(|opts| !opts.may_have_candidates(&identity_sig))
                    .unwrap_or(false);
                if bloom_negative {
                    self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // === BIGTABLE OPTIMIZATION: Scan cache for cached candidates ===
                let cached_candidates = self
                    .partition_opts
                    .as_ref()
                    .and_then(|opts| opts.get_cached_candidates(&identity_sig));

                let (candidates, is_hot): (CandidateVec, bool) =
                    if let Some(cached) = cached_candidates {
                        // Cache hit - use cached candidates
                        self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                        (cached.into_iter().collect(), false)
                    } else {
                        // Cache miss or no optimizations - do index lookup
                        // Need to use destructuring for separate mutable borrows
                        let StreamingLinker {
                            dsu,
                            identity_index,
                            partition_opts,
                            metrics,
                            tuning,
                            ..
                        } = self;

                        metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
                        let max_tree_nodes = candidate_scan_limit(tuning);
                        let (candidates_slice, is_hot) = identity_index
                            .find_matching_clusters_overlapping_limited(
                                dsu,
                                entity_type,
                                key_values,
                                interval,
                                max_tree_nodes,
                            )?;
                        let candidates: CandidateVec = if is_hot {
                            identity_index
                                .find_matching_clusters_overlapping_limited(
                                    dsu,
                                    entity_type,
                                    key_values,
                                    interval,
                                    usize::MAX,
                                )?
                                .0
                                .iter()
                                .copied()
                                .collect()
                        } else {
                            candidates_slice.iter().copied().collect()
                        };
                        // Cache the result for future lookups
                        if let Some(opts) = partition_opts {
                            opts.cache_candidates(&identity_sig, candidates.to_vec());
                        }
                        (candidates, is_hot)
                    };

                let candidate_len = candidates.len();

                if candidate_len > 0 {
                    debug!(
                        "Found {} candidates for record {:?} with key_values={:?}",
                        candidate_len, record_id, key_values
                    );
                }

                // Candidate volume is independent of strong-identifier conflicts.
                if is_hot || candidate_len > self.tuning.hot_key_threshold {
                    self.metrics.hot_key_exits.fetch_add(1, Ordering::Relaxed);
                }

                // === SPER OPTIMIZATION: Stochastic candidate sampling ===
                // When candidate count exceeds threshold, use Bernoulli trials with
                // probability proportional to temporal overlap weight.
                // This achieves O(n) vs O(n log n) for sorting-based approaches.
                let candidates = if self.tuning.stochastic_sampling
                    && candidate_len > self.tuning.sampling_threshold
                {
                    self.metrics
                        .stochastic_samples
                        .fetch_add(1, Ordering::Relaxed);
                    let base_prob = self.tuning.sampling_target as f64 / candidate_len as f64;
                    let interval_len = interval.duration_or_zero();
                    let record_hash = record_id.0 as u64;

                    candidates
                        .into_iter()
                        .filter(|(cand_id, cand_interval)| {
                            // Compute temporal overlap weight (0.0 to 1.0)
                            let overlap = interval.overlap_duration(cand_interval);
                            let cand_len = cand_interval.duration_or_zero();
                            let min_len = interval_len.min(cand_len).max(1);
                            let weight = (overlap as f64 / min_len as f64).min(1.0);

                            // Fast deterministic hash-based selection
                            // Hash combines record and candidate for determinism
                            let hash =
                                record_hash.wrapping_mul(0x517cc1b727220a95) ^ (cand_id.0 as u64);
                            let hash_frac = (hash as f64) / (u64::MAX as f64);

                            // Accept with probability: base_prob * (0.5 + 0.5 * weight)
                            // This weights higher-overlap candidates more likely
                            hash_frac < base_prob * (0.5 + 0.5 * weight)
                        })
                        .collect::<CandidateVec>()
                } else {
                    candidates
                };
                let candidate_len = candidates.len();

                let stats = self.key_stats.get(&key_signature);
                let avg = stats.map(|stats| stats.average_candidates()).unwrap_or(0.0);
                let actual_cap = if self.tuning.adaptive_candidate_cap {
                    if candidate_len >= self.tuning.adaptive_high_threshold
                        || avg >= self.tuning.adaptive_high_threshold as f64
                    {
                        self.tuning.candidate_cap.min(self.tuning.adaptive_high_cap)
                    } else if candidate_len >= self.tuning.adaptive_mid_threshold
                        || avg >= self.tuning.adaptive_mid_threshold as f64
                    {
                        self.tuning.candidate_cap.min(self.tuning.adaptive_mid_cap)
                    } else {
                        self.tuning.candidate_cap
                    }
                } else {
                    self.tuning.candidate_cap
                };
                if candidate_len > actual_cap && self.tuning.deferred_reconciliation {
                    // Record stats before moving key_signature
                    self.key_stats
                        .entry(key_signature.clone())
                        .or_default()
                        .record(candidate_len, actual_cap);
                    self.pending_keys.insert(key_signature);
                    continue;
                }
                let key_is_tainted = self.tainted_identity_keys.contains(&key_signature);
                // Only clone key for stats if we have candidates (skip unique keys)
                let stats_key = if candidate_len > 0 {
                    Some(key_signature.clone())
                } else {
                    None
                };

                // Use destructuring for separate mutable borrows in candidate processing
                let StreamingLinker {
                    dsu,
                    identity_index,
                    cluster_ids,
                    next_cluster_id,
                    global_cluster_ids,
                    global_cluster_anchors,
                    strong_id_summaries,
                    member_roots,
                    cluster_members,
                    tainted_identity_keys,
                    record_perspectives,
                    boundary_signatures,
                    dirty_boundary_keys,
                    cross_shard_merges,
                    metrics,
                    partition_opts,
                    shard_id,
                    ..
                } = self;

                let mut root_a = dsu.find(record_id).unwrap_or(record_id);

                for (candidate_id, candidate_interval) in candidates {
                    let _candidate_guard = crate::profile::profile_scope("candidate_scan");
                    if candidate_id == record_id {
                        continue;
                    }
                    if !crate::temporal::is_overlapping(&interval, &candidate_interval) {
                        continue;
                    }

                    let root_b = dsu.find(candidate_id).unwrap_or(candidate_id);
                    if root_a == root_b {
                        continue;
                    }

                    let candidate_perspective = record_perspectives.get(&candidate_id);
                    let same_perspective_conflict = candidate_perspective
                        .map(|perspective| {
                            let is_same_perspective = perspective == &record_perspective;
                            let has_conflict = same_perspective_conflict_for_clusters(
                                strong_id_summaries,
                                root_a,
                                root_b,
                                perspective,
                            );
                            debug!(
                                "Conflict check: record={:?} candidate={:?} same_persp={} has_conflict={}",
                                record_id, candidate_id, is_same_perspective, has_conflict
                            );
                            is_same_perspective && has_conflict
                        })
                        .unwrap_or(false);

                    if same_perspective_conflict {
                        debug!(
                            "CONFLICT DETECTED: record={:?} candidate={:?}",
                            record_id, candidate_id
                        );
                        metrics.conflicts_detected.fetch_add(1, Ordering::Relaxed);
                        tainted_identity_keys.insert(key_signature.clone());
                        continue;
                    }

                    if key_is_tainted
                        && candidate_perspective
                            .map(|perspective| perspective != &record_perspective)
                            .unwrap_or(false)
                    {
                        continue;
                    }

                    if would_create_conflict_in_clusters(
                        strong_id_summaries,
                        dsu,
                        record_id,
                        candidate_id,
                    ) {
                        metrics.conflicts_detected.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    let overlap = crate::temporal::intersect(&interval, &candidate_interval)
                        .unwrap_or(interval);
                    let guard = TemporalGuard::new(overlap, guard_reason.clone());

                    if let Ok(MergeResult::Success { .. }) =
                        dsu.try_merge(record_id, candidate_id, guard)
                    {
                        metrics.merges_performed.fetch_add(1, Ordering::Relaxed);
                        let new_root = dsu.find(record_id).unwrap_or(record_id);
                        reconcile_cluster_members(
                            member_roots,
                            cluster_members,
                            root_a,
                            root_b,
                            new_root,
                        );
                        reconcile_cluster_ids(
                            cluster_ids,
                            next_cluster_id,
                            root_a,
                            root_b,
                            new_root,
                        );
                        reconcile_global_cluster_ids(
                            global_cluster_ids,
                            global_cluster_anchors,
                            cross_shard_merges,
                            boundary_signatures,
                            dirty_boundary_keys,
                            *shard_id,
                            root_a,
                            root_b,
                            new_root,
                        );
                        reconcile_cluster_summaries(strong_id_summaries, root_a, root_b, new_root);
                        identity_index.merge_key_clusters(
                            entity_type,
                            key_values,
                            root_a,
                            root_b,
                            new_root,
                        )?;
                        // Invalidate cache on merge - candidates changed
                        if let Some(opts) = partition_opts {
                            opts.on_cluster_merge(root_a, root_b);
                        }
                        root_a = new_root;
                    }
                }
                // Record stats after candidates loop (borrows dropped)
                // Only track stats for keys with candidates (skip unique keys)
                if let Some(stats_key) = stats_key {
                    self.key_stats
                        .entry(stats_key)
                        .or_default()
                        .record(candidate_len, actual_cap);
                }
            }
        }

        // Add the record to the index after matching to avoid self-matches.
        let _add_guard = crate::profile::profile_scope("add_to_index");
        let root = self.dsu.find(record_id).unwrap_or(record_id);
        for (_, key_values_with_intervals) in &cached_keys {
            for (key_values, interval) in key_values_with_intervals {
                self.track_merge_boundary(interner, entity_type, key_values, root, *interval);
            }
        }

        self.identity_index.add_record_with_cached_keys(
            record_id,
            root,
            entity_type,
            cached_keys,
        )?;

        self.metrics.records_linked.fetch_add(1, Ordering::Relaxed);
        Ok(self.get_or_assign_cluster_id(root))
    }

    pub fn cluster_count(&self) -> usize {
        self.dsu.cluster_count()
    }

    /// Get a reference to the linker metrics for observability.
    pub fn metrics(&self) -> &LinkerMetrics {
        &self.metrics
    }

    /// Get a snapshot of current metrics values.
    pub fn metrics_snapshot(&self) -> LinkerMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Link a batch of records using parallel extraction.
    ///
    /// This method uses a phased approach:
    /// - **Phase 1 (Parallel)**: Extract key values and build summaries for all records
    /// - **Phase 2 (Sequential)**: For each record, find candidates, apply guarded
    ///   merges, and index the record before linking the next one
    ///
    /// Extraction runs in parallel; cluster mutations remain ordered. The benefit
    /// depends on batch size, key complexity, and candidate overlap.
    #[instrument(skip(self, records, ontology), level = "debug")]
    pub fn link_records_batch_parallel(
        &mut self,
        records: &[&Record],
        ontology: &Ontology,
    ) -> Result<Vec<ClusterId>> {
        self.link_records_batch_parallel_internal(records, ontology, None)
    }

    /// Link a batch of records using parallel extraction, with access to the store.
    /// This enables boundary tracking for cross-shard reconciliation.
    pub fn link_records_batch_parallel_with_interner(
        &mut self,
        records: &[&Record],
        ontology: &Ontology,
        interner: &dyn InternerLookup,
    ) -> Result<Vec<ClusterId>> {
        self.link_records_batch_parallel_internal(records, ontology, Some(interner))
    }

    fn link_records_batch_parallel_internal(
        &mut self,
        records: &[&Record],
        ontology: &Ontology,
        interner: Option<&dyn InternerLookup>,
    ) -> Result<Vec<ClusterId>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 1: Parallel extraction of key values and strong ID summaries
        // This is the expensive CPU work that benefits from parallelism
        let extractions: Vec<ParallelExtractionResult> = records
            .par_iter()
            .map(|record| self.extract_record_data(record, ontology))
            .collect();

        // Phase 2: Sequential linking (DSU mutations require exclusive access)
        let mut cluster_ids = Vec::with_capacity(records.len());
        for (record, extraction) in records.iter().zip(extractions) {
            let cluster_id =
                self.link_extracted_record_with_interner(ontology, extraction, interner)?;
            cluster_ids.push(cluster_id);

            // Phase 3: Add to index (requires record reference)
            self.add_to_index_after_parallel_link(record, ontology, interner)?;
        }

        Ok(cluster_ids)
    }

    /// Extract all data needed for linking from a record (parallelizable).
    /// This is a pure function that doesn't mutate linker state.
    fn extract_record_data(
        &self,
        record: &Record,
        ontology: &Ontology,
    ) -> ParallelExtractionResult {
        let record_id = record.id;
        let entity_type = record.identity.entity_type.clone();
        let perspective = record.identity.perspective.clone();

        // Extract all key values for this record
        let identity_keys = ontology.identity_keys_for_type(&entity_type);
        let mut keys = Vec::new();

        for identity_key in identity_keys {
            let guard_reason = format!("identity_key_{}", identity_key.name);

            // Extract key values - this is the expensive part we're parallelizing
            if let Ok(key_values_with_intervals) =
                crate::index::extract_key_values_from_record(record, identity_key)
            {
                for (key_values, interval) in key_values_with_intervals {
                    let key_signature = LinkerKeySignature::new(&entity_type, &key_values);
                    keys.push((key_signature, key_values, interval, guard_reason.clone()));
                }
            }
        }

        // Build strong ID summary
        let strong_id_summary = Some(build_record_summary(record, ontology));

        ParallelExtractionResult {
            record_id,
            entity_type,
            perspective,
            keys,
            strong_id_summary,
        }
    }

    /// Link an extracted record (sequential, mutates DSU).
    fn link_extracted_record_with_interner(
        &mut self,
        _ontology: &Ontology,
        extraction: ParallelExtractionResult,
        _interner: Option<&dyn InternerLookup>,
    ) -> Result<ClusterId> {
        let record_id = extraction.record_id;
        let entity_type = &extraction.entity_type;

        // Initialize record in DSU if needed
        if !self.dsu.has_record(record_id)? {
            self.dsu.add_record(record_id)?;
            self.global_cluster_anchors.insert(record_id, record_id.0);
        }
        self.register_cluster_member(record_id)?;

        // Store perspective and summary
        self.record_perspectives
            .insert(record_id, extraction.perspective.clone());
        if let Some(summary) = extraction.strong_id_summary {
            self.strong_id_summaries
                .entry_or_default(record_id)
                .merge(summary);
        }

        // Process each key
        for (key_signature, key_values, interval, guard_reason) in extraction.keys {
            // Find candidates and merge
            let max_tree_nodes = candidate_scan_limit(&self.tuning);
            let (candidates_slice, is_hot) = self
                .identity_index
                .find_matching_clusters_overlapping_limited(
                    &mut self.dsu,
                    entity_type,
                    &key_values,
                    interval,
                    max_tree_nodes,
                )?;
            let candidates: CandidateVec = if is_hot {
                self.identity_index
                    .find_matching_clusters_overlapping_limited(
                        &mut self.dsu,
                        entity_type,
                        &key_values,
                        interval,
                        usize::MAX,
                    )?
                    .0
                    .iter()
                    .copied()
                    .collect()
            } else {
                candidates_slice.iter().copied().collect()
            };
            let candidate_len = candidates.len();

            if is_hot || candidate_len > self.tuning.hot_key_threshold {
                self.metrics.hot_key_exits.fetch_add(1, Ordering::Relaxed);
            }

            // Merge with candidates
            let mut root_a = self.dsu.find(record_id).unwrap_or(record_id);
            let key_is_tainted = self.tainted_identity_keys.contains(&key_signature);
            for (candidate_id, candidate_interval) in candidates {
                if candidate_id == record_id {
                    continue;
                }
                if !crate::temporal::is_overlapping(&interval, &candidate_interval) {
                    continue;
                }

                let root_b = self.dsu.find(candidate_id).unwrap_or(candidate_id);
                if root_a == root_b {
                    continue;
                }

                let candidate_perspective = self.record_perspectives.get(&candidate_id);
                let same_perspective_conflict = candidate_perspective
                    .map(|perspective| {
                        perspective == &extraction.perspective
                            && same_perspective_conflict_for_clusters(
                                &self.strong_id_summaries,
                                root_a,
                                root_b,
                                perspective,
                            )
                    })
                    .unwrap_or(false);
                if same_perspective_conflict {
                    self.metrics
                        .conflicts_detected
                        .fetch_add(1, Ordering::Relaxed);
                    self.tainted_identity_keys.insert(key_signature.clone());
                    continue;
                }
                if key_is_tainted
                    && candidate_perspective
                        .map(|perspective| perspective != &extraction.perspective)
                        .unwrap_or(false)
                {
                    continue;
                }

                // Check for conflicts
                if would_create_conflict_in_clusters(
                    &self.strong_id_summaries,
                    &mut self.dsu,
                    record_id,
                    candidate_id,
                ) {
                    self.metrics
                        .conflicts_detected
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                let overlap =
                    crate::temporal::intersect(&interval, &candidate_interval).unwrap_or(interval);
                let guard = TemporalGuard::new(overlap, guard_reason.clone());

                if let Ok(MergeResult::Success { .. }) =
                    self.dsu.try_merge(record_id, candidate_id, guard)
                {
                    self.metrics
                        .merges_performed
                        .fetch_add(1, Ordering::Relaxed);
                    let new_root = self.dsu.find(record_id).unwrap_or(record_id);
                    reconcile_cluster_members(
                        &mut self.member_roots,
                        &mut self.cluster_members,
                        root_a,
                        root_b,
                        new_root,
                    );
                    reconcile_cluster_ids(
                        &mut self.cluster_ids,
                        &mut self.next_cluster_id,
                        root_a,
                        root_b,
                        new_root,
                    );
                    reconcile_global_cluster_ids(
                        &mut self.global_cluster_ids,
                        &mut self.global_cluster_anchors,
                        &mut self.cross_shard_merges,
                        &mut self.boundary_signatures,
                        &mut self.dirty_boundary_keys,
                        self.shard_id,
                        root_a,
                        root_b,
                        new_root,
                    );
                    reconcile_cluster_summaries(
                        &mut self.strong_id_summaries,
                        root_a,
                        root_b,
                        new_root,
                    );
                    self.identity_index.merge_key_clusters(
                        entity_type,
                        &key_values,
                        root_a,
                        root_b,
                        new_root,
                    )?;
                    root_a = new_root;
                }
            }
        }

        // Note: Index addition is handled by the caller who has access to the full record
        // This parallel version only handles the linking/merging phase

        self.metrics.records_linked.fetch_add(1, Ordering::Relaxed);
        let root = self.dsu.find(record_id).unwrap_or(record_id);
        Ok(self.get_or_assign_cluster_id(root))
    }

    /// Add a record to the index after parallel linking.
    /// Call this after link_extracted_record to complete the linking process.
    pub fn add_to_index_after_parallel_link(
        &mut self,
        record: &Record,
        ontology: &Ontology,
        interner: Option<&dyn InternerLookup>,
    ) -> Result<()> {
        let root = self.dsu.find(record.id).unwrap_or(record.id);
        let entity_type = &record.identity.entity_type;

        // Extract keys and add to index
        let identity_keys = ontology.identity_keys_for_type(entity_type);
        let mut cached_keys = Vec::new();

        for identity_key in identity_keys {
            if let Ok(key_values_with_intervals) =
                crate::index::extract_key_values_from_record(record, identity_key)
            {
                cached_keys.push((identity_key, key_values_with_intervals));
            }
        }

        if let Some(interner) = interner {
            for (_, key_values_with_intervals) in &cached_keys {
                for (key_values, interval) in key_values_with_intervals {
                    self.track_merge_boundary(interner, entity_type, key_values, root, *interval);
                }
            }
        }

        self.identity_index.add_record_with_cached_keys(
            record.id,
            root,
            entity_type,
            cached_keys,
        )?;

        Ok(())
    }

    /// Enumerate authoritative local membership, including persistent DSU backends.
    pub fn clusters(&mut self) -> Clusters {
        let members = self
            .cluster_members
            .iter()
            .map(|(root, records)| (*root, records.clone()))
            .collect::<Vec<_>>();
        Clusters {
            clusters: members
                .into_iter()
                .map(|(root, records)| {
                    let id = self.get_or_assign_cluster_id(root);
                    crate::dsu::Cluster::new(id, root, records)
                })
                .collect(),
        }
    }

    /// Finish deferred linking and enumerate clusters guarded at merge time.
    pub fn clusters_with_conflict_splitting(
        &mut self,
        store: &dyn RecordStore,
        ontology: &Ontology,
    ) -> Result<Clusters> {
        self.reconcile_pending(store, ontology)?;
        Ok(self.clusters())
    }

    fn get_or_assign_cluster_id(&mut self, root: RecordId) -> ClusterId {
        if let Some(cluster_id) = self.cluster_ids.get(&root) {
            return *cluster_id;
        }
        let cluster_id = ClusterId(self.next_cluster_id);
        self.next_cluster_id += 1;
        self.cluster_ids.insert(root, cluster_id);
        self.metrics
            .clusters_created
            .fetch_add(1, Ordering::Relaxed);
        cluster_id
    }

    /// Get or assign a global cluster ID for a root record.
    fn get_or_assign_global_cluster_id(&mut self, root: RecordId) -> GlobalClusterId {
        if let Some(global_id) = self.global_cluster_ids.get(&root) {
            return *global_id;
        }
        let local_id = self
            .global_cluster_anchors
            .get_copy(&root)
            .unwrap_or(root.0);
        let global_id = GlobalClusterId::new(self.shard_id, local_id, 0);
        self.global_cluster_ids.insert(root, global_id);
        global_id
    }

    pub fn cluster_id_for(&mut self, record_id: RecordId) -> ClusterId {
        let root = self.dsu.find(record_id).unwrap_or(record_id);
        self.get_or_assign_cluster_id(root)
    }

    fn register_cluster_member(&mut self, record_id: RecordId) -> Result<()> {
        if !self.member_roots.contains_key(&record_id) {
            let root = self.dsu.find(record_id)?;
            self.member_roots.insert(record_id, root);
            self.cluster_members
                .entry(root)
                .or_default()
                .push(record_id);
        }
        Ok(())
    }

    /// Resolve a tracked record without mutating DSU caches or enumerating records.
    pub fn root_for_record(&self, record_id: RecordId) -> Option<RecordId> {
        let mut root = *self.member_roots.get(&record_id)?;
        while let Some(&parent) = self.member_roots.get(&root) {
            if parent == root {
                break;
            }
            root = parent;
        }
        Some(root)
    }

    /// Read authoritative membership for one local cluster.
    pub fn cluster_for_record(&self, record_id: RecordId) -> Option<crate::dsu::Cluster> {
        let root = self.root_for_record(record_id)?;
        let id = *self.cluster_ids.peek(&root)?;
        let members = self.cluster_members.get(&root)?.clone();
        Some(crate::dsu::Cluster::new(id, root, members))
    }

    /// Read all authoritative memberships without replaying records or assigning
    /// new IDs. Missing IDs indicate inconsistent state and must not hide clusters.
    pub fn clusters_readonly(&self) -> Result<Clusters> {
        let clusters = self
            .cluster_members
            .iter()
            .map(|(root, records)| {
                let id = self.cluster_ids.peek(root).copied().ok_or_else(|| {
                    anyhow::anyhow!("cluster ID is unavailable for tracked root {}", root.0)
                })?;
                Ok(crate::dsu::Cluster::new(id, *root, records.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Clusters { clusters })
    }

    /// Read a local cluster's canonical global ID without allocating an assignment.
    pub fn global_cluster_id_for_readonly(&self, record_id: RecordId) -> Option<GlobalClusterId> {
        let root = self.root_for_record(record_id)?;
        let id = self
            .global_cluster_ids
            .peek(&root)
            .copied()
            .unwrap_or_else(|| {
                let anchor = self
                    .global_cluster_anchors
                    .peek(&root)
                    .copied()
                    .unwrap_or(root.0);
                GlobalClusterId::new(self.shard_id, anchor, 0)
            });
        Some(self.resolve_global_cluster_id(id))
    }

    /// Get the global cluster ID for a record.
    pub fn global_cluster_id_for(&mut self, record_id: RecordId) -> GlobalClusterId {
        let root = self.dsu.find(record_id).unwrap_or(record_id);
        self.get_or_assign_global_cluster_id(root)
    }

    /// Get the number of boundary signatures collected during linking.
    pub fn boundary_count(&self) -> usize {
        self.boundary_signatures.len()
    }

    /// Clear boundary signatures after they've been exported.
    pub fn clear_boundary_signatures(&mut self) {
        self.boundary_signatures.clear();
    }

    /// Get the set of dirty boundary keys (modified since last reconciliation).
    pub fn get_dirty_boundary_keys(&self) -> BTreeSet<ShardingKeySignature> {
        self.dirty_boundary_keys.clone()
    }

    /// Return the first ordered dirty signatures after an exclusive cursor.
    pub fn dirty_boundary_key_candidates(
        &self,
        after: Option<ShardingKeySignature>,
        limit: usize,
    ) -> Vec<ShardingKeySignature> {
        match after {
            Some(cursor) => self
                .dirty_boundary_keys
                .range((
                    std::ops::Bound::Excluded(cursor),
                    std::ops::Bound::Unbounded,
                ))
                .take(limit)
                .copied()
                .collect(),
            None => self
                .dirty_boundary_keys
                .iter()
                .take(limit)
                .copied()
                .collect(),
        }
    }

    /// Get the count of dirty boundary keys.
    pub fn dirty_boundary_key_count(&self) -> usize {
        self.dirty_boundary_keys.len()
    }

    /// Clear specific dirty boundary keys after reconciliation.
    pub fn clear_dirty_boundary_keys(&mut self, keys: &[ShardingKeySignature]) {
        for key in keys {
            self.dirty_boundary_keys.remove(key);
        }
    }

    /// Export boundary signatures to a ClusterBoundaryIndex.
    /// This can be used for cross-shard reconciliation.
    pub fn export_boundary_index(&self) -> crate::sharding::ClusterBoundaryIndex {
        self.export_boundary_index_for_keys(None)
    }

    /// Export boundary metadata only for the requested signatures.
    pub fn export_boundary_index_for_signatures(
        &self,
        signatures: &HashSet<ShardingKeySignature>,
    ) -> crate::sharding::ClusterBoundaryIndex {
        self.export_boundary_index_for_keys(Some(signatures))
    }

    fn export_boundary_index_for_keys(
        &self,
        signatures: Option<&HashSet<ShardingKeySignature>>,
    ) -> crate::sharding::ClusterBoundaryIndex {
        let mut index = crate::sharding::ClusterBoundaryIndex::new_small(self.shard_id);
        if let Some(signatures) = signatures {
            let first_cluster = GlobalClusterId::new(0, 0, 0);
            let last_cluster = GlobalClusterId::new(u16::MAX, u32::MAX, u16::MAX);
            for signature in signatures {
                for ((signature, global_id), data) in self.boundary_signatures.range((
                    std::ops::Bound::Included((*signature, first_cluster)),
                    std::ops::Bound::Included((*signature, last_cluster)),
                )) {
                    register_boundary_data(&mut index, *signature, *global_id, data);
                }
            }
        } else {
            for ((signature, global_id), data) in &self.boundary_signatures {
                register_boundary_data(&mut index, *signature, *global_id, data);
            }
        }
        index
    }

    /// Drain boundary signatures and return them.
    /// Clears the internal buffer after draining.
    pub fn drain_boundaries(&mut self) -> Vec<(ShardingKeySignature, GlobalClusterId, Interval)> {
        std::mem::take(&mut self.boundary_signatures)
            .into_iter()
            .flat_map(|((signature, global_id), data)| {
                data.intervals
                    .into_iter()
                    .map(move |interval| (signature, global_id, interval))
            })
            .collect()
    }

    /// Track an identity-key observation for distributed reconciliation.
    #[inline]
    fn track_merge_boundary(
        &mut self,
        interner: &dyn InternerLookup,
        entity_type: &str,
        key_values: &[KeyValue],
        new_root: RecordId,
        interval: Interval,
    ) {
        if !self.tuning.enable_boundary_tracking {
            return;
        }
        let global_id = self.get_or_assign_global_cluster_id(new_root);
        let sharding_sig = match ShardingKeySignature::from_key_values_with_interner(
            entity_type,
            key_values,
            interner,
        ) {
            Some(sig) => sig,
            None => return,
        };
        // Compute perspective_strong_ids from the cluster's summary
        // Uses actual string values (via interner) for cross-shard consistency
        let perspective_strong_ids = self
            .strong_id_summaries
            .peek(&new_root)
            .map(|summary| summary.compute_perspective_strong_ids(interner))
            .unwrap_or_default();
        let strong_ids = self
            .strong_id_summaries
            .peek(&new_root)
            .map(|summary| summary.compute_boundary_strong_ids(interner))
            .unwrap_or_default();

        // Merge intervals for the same (signature, cluster) combination
        self.boundary_signatures
            .entry((sharding_sig, global_id))
            .and_modify(|existing| {
                existing.intervals.push(interval);
                coalesce_boundary_intervals(&mut existing.intervals);
                // Also merge perspective_strong_ids from the updated cluster
                for (k, v) in &perspective_strong_ids {
                    existing.perspective_strong_ids.entry(*k).or_insert(*v);
                }
                existing.strong_ids.extend(strong_ids.iter().cloned());
                sort_dedup_boundary_strong_ids(&mut existing.strong_ids);
            })
            .or_insert_with(|| BoundaryData::new(interval, perspective_strong_ids, strong_ids));

        // Mark this key as dirty for adaptive reconciliation
        self.dirty_boundary_keys.insert(sharding_sig);
    }

    /// Apply a cross-shard cluster merge.
    /// Records that `secondary` should be redirected to `primary`.
    /// Returns the number of affected records on this shard.
    pub fn apply_cross_shard_merge(
        &mut self,
        primary: GlobalClusterId,
        secondary: GlobalClusterId,
    ) -> usize {
        self.apply_cross_shard_merges(&[(primary, secondary)])
    }

    /// Apply canonical cross-shard redirects with one pass over local linker state.
    pub fn apply_cross_shard_merges(
        &mut self,
        merges: &[(GlobalClusterId, GlobalClusterId)],
    ) -> usize {
        let redirects = merges
            .iter()
            .map(|(primary, secondary)| (*secondary, *primary))
            .collect::<HashMap<_, _>>();
        self.cross_shard_merges.extend(
            redirects
                .iter()
                .map(|(secondary, primary)| (*secondary, *primary)),
        );

        let keys_to_update = self
            .boundary_signatures
            .keys()
            .filter_map(|(signature, cluster_id)| {
                redirects
                    .get(cluster_id)
                    .map(|primary| (*signature, *cluster_id, *primary))
            })
            .collect::<Vec<_>>();
        for (signature, secondary, primary) in keys_to_update {
            if let Some(data) = self.boundary_signatures.remove(&(signature, secondary)) {
                self.boundary_signatures
                    .entry((signature, primary))
                    .and_modify(|existing| existing.merge_from(&data))
                    .or_insert(data);
                self.dirty_boundary_keys.insert(signature);
            }
        }

        // Update global_cluster_ids map: any root pointing to secondary should now point to primary
        let mut updated_count = 0;
        for (_root, global_id) in self.global_cluster_ids.iter_mut() {
            if let Some(primary) = redirects.get(global_id) {
                *global_id = *primary;
                updated_count += 1;
            }
        }

        updated_count
    }

    /// Resolve a global cluster ID through any cross-shard merges.
    /// Returns the ultimate primary cluster ID after following merge chains.
    pub fn resolve_global_cluster_id(&self, id: GlobalClusterId) -> GlobalClusterId {
        let mut current = id;
        let mut seen = std::collections::HashSet::new();
        while let Some(&primary) = self.cross_shard_merges.get(&current) {
            if !seen.insert(current) {
                // Cycle detected - shouldn't happen but protect against infinite loop
                break;
            }
            current = primary;
        }
        current
    }

    /// Get the number of cross-shard merge mappings.
    pub fn cross_shard_merge_count(&self) -> usize {
        self.cross_shard_merges.len()
    }

    // Static string for deferred reconciliation guards - avoid repeated allocation
    const GUARD_REASON_DEFERRED: &str = "identity_key_deferred";

    #[instrument(skip(self, store, _ontology), level = "debug")]
    pub fn reconcile_pending(
        &mut self,
        store: &dyn RecordStore,
        _ontology: &Ontology,
    ) -> Result<()> {
        if self.pending_keys.is_empty() {
            return Ok(());
        }

        let pending = std::mem::take(&mut self.pending_keys);
        debug!(
            pending_keys = pending.len(),
            "Starting deferred reconciliation"
        );
        for key_signature in pending {
            let candidates = self
                .identity_index
                .find_matching_records(key_signature.entity_type(), key_signature.key_values())?
                .to_vec();
            if candidates.len() < 2 {
                continue;
            }

            let key_is_tainted = self.tainted_identity_keys.contains(&key_signature);
            let mut records = Vec::with_capacity(candidates.len());
            for (record_id, interval) in &candidates {
                if !self.dsu.has_record(*record_id).unwrap_or(false) {
                    continue;
                }
                let perspective = self
                    .record_perspectives
                    .get(record_id)
                    .cloned()
                    .or_else(|| {
                        store
                            .get_record(*record_id)
                            .map(|record| record.identity.perspective)
                    });
                let Some(perspective) = perspective else {
                    continue;
                };
                records.push((*record_id, *interval, perspective));
            }
            if records.len() < 2 {
                continue;
            }

            records.sort_by_key(|(_, interval, _)| interval.start);
            let cap = if records.len() > self.tuning.candidate_cap {
                self.tuning.candidate_cap
            } else {
                usize::MAX
            };

            // Use sweep-line with min-heap for O(n log n) instead of O(n²) retain()
            // Heap stores (end_time, idx) to efficiently remove expired elements
            use std::cmp::Reverse;
            use std::collections::BinaryHeap;

            // Min-heap by end time: (Reverse(end), idx) - smallest end first
            let mut active_heap: BinaryHeap<Reverse<(crate::temporal::Instant, usize)>> =
                BinaryHeap::with_capacity(records.len().min(1024));
            // Use FxHashSet for faster iteration (better cache locality)
            let mut active_set: FxHashSet<usize> = FxHashSet::default();

            // Pre-allocate guard reason once per key (not per merge)
            let guard_reason = Self::GUARD_REASON_DEFERRED.to_string();

            for idx in 0..records.len() {
                let (record_id, interval, record_perspective) = &records[idx];

                // Remove expired elements from heap - O(log n) per removal
                while let Some(&Reverse((end, expired_idx))) = active_heap.peek() {
                    if end <= interval.start {
                        active_heap.pop();
                        active_set.remove(&expired_idx);
                    } else {
                        break;
                    }
                }

                let mut compared = 0usize;
                for &active_idx in active_set.iter() {
                    if compared >= cap {
                        break;
                    }
                    let (candidate_id, candidate_interval, candidate_perspective) =
                        &records[active_idx];
                    if record_id == candidate_id {
                        continue;
                    }
                    if !crate::temporal::is_overlapping(interval, candidate_interval) {
                        continue;
                    }

                    let root_a = self.dsu.find(*record_id).unwrap_or(*record_id);
                    let root_b = self.dsu.find(*candidate_id).unwrap_or(*candidate_id);
                    if root_a == root_b {
                        continue;
                    }

                    let same_perspective_conflict = candidate_perspective == record_perspective
                        && same_perspective_conflict_for_clusters(
                            &self.strong_id_summaries,
                            root_a,
                            root_b,
                            record_perspective,
                        );

                    if same_perspective_conflict {
                        self.tainted_identity_keys.insert(key_signature.clone());
                        continue;
                    }

                    if key_is_tainted && candidate_perspective != record_perspective {
                        continue;
                    }

                    if would_create_conflict_in_clusters(
                        &self.strong_id_summaries,
                        &mut self.dsu,
                        *record_id,
                        *candidate_id,
                    ) {
                        continue;
                    }

                    let overlap = crate::temporal::intersect(interval, candidate_interval)
                        .unwrap_or(*interval);
                    let guard = TemporalGuard::new(overlap, guard_reason.clone());

                    if let Ok(MergeResult::Success { .. }) =
                        self.dsu.try_merge(*record_id, *candidate_id, guard)
                    {
                        self.metrics
                            .merges_performed
                            .fetch_add(1, Ordering::Relaxed);
                        let new_root = self.dsu.find(*record_id).unwrap_or(*record_id);
                        reconcile_cluster_members(
                            &mut self.member_roots,
                            &mut self.cluster_members,
                            root_a,
                            root_b,
                            new_root,
                        );
                        reconcile_global_cluster_ids(
                            &mut self.global_cluster_ids,
                            &mut self.global_cluster_anchors,
                            &mut self.cross_shard_merges,
                            &mut self.boundary_signatures,
                            &mut self.dirty_boundary_keys,
                            self.shard_id,
                            root_a,
                            root_b,
                            new_root,
                        );
                        reconcile_cluster_ids(
                            &mut self.cluster_ids,
                            &mut self.next_cluster_id,
                            root_a,
                            root_b,
                            new_root,
                        );
                        reconcile_cluster_summaries(
                            &mut self.strong_id_summaries,
                            root_a,
                            root_b,
                            new_root,
                        );
                        self.identity_index.merge_key_clusters(
                            key_signature.entity_type(),
                            key_signature.key_values(),
                            root_a,
                            root_b,
                            new_root,
                        )?;
                    }
                    compared = compared.saturating_add(1);
                }

                // Add current record to active set and heap
                active_set.insert(idx);
                active_heap.push(Reverse((interval.end, idx)));
            }
        }

        self.metrics.reconciliations.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Flush linker state to persistent storage.
    /// This saves cluster_ids, global_cluster_ids, and next_cluster_id.
    /// Call this periodically or before shutdown for restart recovery.
    pub fn flush_state(
        &self,
        persistence: &crate::persistence::LinkerStatePersistence,
    ) -> Result<()> {
        // Save next_cluster_id first
        persistence.save_next_cluster_id(self.next_cluster_id)?;

        // Flush cluster_ids
        persistence.flush_cluster_ids(self.cluster_ids.iter().map(|(k, v)| (*k, *v)))?;

        // Flush global_cluster_ids
        persistence
            .flush_global_cluster_ids(self.global_cluster_ids.iter().map(|(k, v)| (*k, *v)))?;

        for (secondary, primary) in &self.cross_shard_merges {
            persistence.save_cross_shard_merge(*secondary, *primary)?;
        }

        Ok(())
    }

    /// Restore durable cross-shard redirects and apply them to reconstructed state.
    pub fn restore_cross_shard_merges(
        &mut self,
        persistence: &crate::persistence::LinkerStatePersistence,
    ) -> Result<usize> {
        let mappings = persistence.load_cross_shard_merges()?;
        let count = mappings.len();
        if !mappings.is_empty() {
            let merges = mappings
                .into_iter()
                .map(|(secondary, primary)| (primary, secondary))
                .collect::<Vec<_>>();
            self.apply_cross_shard_merges(&merges);
        }
        Ok(count)
    }

    /// Restore linker state from persistent storage.
    /// Call this during initialization to recover cluster ID mappings.
    /// Returns the number of cluster_ids restored.
    pub fn restore_state(
        &mut self,
        persistence: &crate::persistence::LinkerStatePersistence,
    ) -> Result<usize> {
        // Load next_cluster_id
        if let Some(next_id) = persistence.load_next_cluster_id()? {
            self.next_cluster_id = next_id;
        }

        // Load cluster_ids
        let cluster_ids = persistence.load_cluster_ids()?;
        let count = cluster_ids.len();
        for (record_id, cluster_id) in cluster_ids {
            self.cluster_ids.insert(record_id, cluster_id);
        }

        // Load global_cluster_ids
        let global_ids = persistence.load_global_cluster_ids()?;
        for (record_id, global_id) in global_ids {
            self.global_cluster_ids.insert(record_id, global_id);
        }

        self.restore_cross_shard_merges(persistence)?;

        Ok(count)
    }

    /// Get the current next_cluster_id value.
    /// Useful for persistence/recovery scenarios.
    pub fn next_cluster_id(&self) -> u32 {
        self.next_cluster_id
    }

    /// Set the next_cluster_id value.
    /// Use with caution - only for recovery from persistence.
    pub fn set_next_cluster_id(&mut self, value: u32) {
        self.next_cluster_id = value;
    }
}

/// Strong ID summary for conflict detection between clusters.
/// Tracks (perspective -> attr -> value -> intervals) for conflict checking.
#[derive(Debug, Clone, Default)]
pub struct StrongIdSummary {
    /// Per-perspective strong ID mappings: perspective -> attr -> value -> intervals
    pub by_perspective: HashMap<
        String,
        HashMap<crate::model::AttrId, HashMap<crate::model::ValueId, Vec<Interval>>>,
    >,
}

impl StrongIdSummary {
    /// Merge another summary into this one, combining intervals by perspective/attr/value.
    pub fn merge(&mut self, other: StrongIdSummary) {
        for (perspective, attrs) in other.by_perspective {
            let entry = self.by_perspective.entry(perspective).or_default();
            for (attr, values) in attrs {
                let value_entry = entry.entry(attr).or_default();
                for (value, intervals) in values {
                    value_entry.entry(value).or_default().extend(intervals);
                }
                coalesce_value_intervals(value_entry);
            }
        }
    }

    /// Export exact string-valued observations for distributed temporal guards.
    fn compute_boundary_strong_ids(&self, interner: &dyn InternerLookup) -> Vec<BoundaryStrongId> {
        let mut observations = Vec::new();
        for (perspective, attrs) in &self.by_perspective {
            for (attr_id, values) in attrs {
                let Some(attribute) = interner.get_attr_string(*attr_id) else {
                    continue;
                };
                for (value_id, intervals) in values {
                    let Some(value) = interner.get_value_string(*value_id) else {
                        continue;
                    };
                    observations.extend(intervals.iter().map(|interval| BoundaryStrongId {
                        perspective: perspective.clone(),
                        attribute: attribute.clone(),
                        value: value.clone(),
                        interval: *interval,
                    }));
                }
            }
        }
        sort_dedup_boundary_strong_ids(&mut observations);
        observations
    }

    /// Compute perspective -> strong_id_hash for cross-shard conflict detection.
    /// Returns a map from hashed perspective name to hashed strong ID values.
    ///
    /// IMPORTANT: This hashes the actual string values, not internal IDs.
    /// Different shards may assign different internal IDs to the same strings,
    /// so we must hash the actual strings to get consistent results across shards.
    fn compute_perspective_strong_ids(&self, interner: &dyn InternerLookup) -> HashMap<u64, u64> {
        use rustc_hash::FxHasher;
        use std::hash::{Hash, Hasher};

        let mut result = HashMap::new();
        for (perspective, attrs) in &self.by_perspective {
            // Hash the perspective name
            let mut p_hasher = FxHasher::default();
            perspective.hash(&mut p_hasher);
            let perspective_hash = p_hasher.finish();

            // Collect (attr_string, value_string) pairs and sort for deterministic hashing
            // We must use the actual strings, not IDs, to be consistent across shards
            let mut values: Vec<(String, String)> = attrs
                .iter()
                .flat_map(|(attr_id, values)| {
                    let attr_str = interner.get_attr_string(*attr_id);
                    values.keys().filter_map(move |value_id| {
                        let value_str = interner.get_value_string(*value_id);
                        match (attr_str.clone(), value_str) {
                            (Some(a), Some(v)) => Some((a, v)),
                            _ => None,
                        }
                    })
                })
                .collect();
            values.sort();

            let mut v_hasher = FxHasher::default();
            for (attr, value) in &values {
                attr.hash(&mut v_hasher);
                value.hash(&mut v_hasher);
            }
            let values_hash = v_hasher.finish();

            result.insert(perspective_hash, values_hash);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Descriptor, RecordIdentity};
    use crate::ontology::IdentityKey;
    use crate::store::Store;

    #[test]
    fn parallel_link_tracks_boundary_keys() {
        let mut store = Store::new();
        let mut ontology = Ontology::new();

        let email_attr = store.intern_attr("email");
        ontology.add_identity_key(IdentityKey::new(vec![email_attr], "email_key".to_string()));

        let tuning = crate::StreamingTuning {
            enable_boundary_tracking: true,
            ..Default::default()
        };

        let mut linker = StreamingLinker::new(&store, &ontology, &tuning).expect("linker");

        let interval = Interval::new(0, 10).expect("interval");
        let email_value = store.intern_value("alice@example.com");

        let record1 = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "hr".to_string(), "u1".to_string()),
            vec![Descriptor::new(email_attr, email_value, interval)],
        );
        let record2 = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "hr".to_string(), "u2".to_string()),
            vec![Descriptor::new(email_attr, email_value, interval)],
        );

        let id1 = store.add_record(record1).expect("record1");
        let id2 = store.add_record(record2).expect("record2");

        let records_to_link: Vec<&Record> = [id1, id2]
            .iter()
            .filter_map(|id| store.get_record_ref(*id))
            .collect();

        let cluster_ids = linker
            .link_records_batch_parallel_with_interner(
                &records_to_link,
                &ontology,
                store.interner(),
            )
            .expect("link");

        assert_eq!(cluster_ids.len(), 2);
        assert!(linker.boundary_count() > 0);
        assert!(linker.dirty_boundary_key_count() > 0);
    }

    #[test]
    fn local_merge_canonicalizes_previously_exposed_global_ids() {
        let mut store = Store::new();
        let mut ontology = Ontology::new();
        let email_attr = store.intern_attr("email");
        let phone_attr = store.intern_attr("phone");
        ontology.add_identity_key(IdentityKey::new(vec![email_attr], "email_key".to_string()));
        ontology.add_identity_key(IdentityKey::new(vec![phone_attr], "phone_key".to_string()));
        let tuning = crate::StreamingTuning {
            shard_id: 1,
            enable_boundary_tracking: true,
            ..Default::default()
        };
        let mut linker = StreamingLinker::new(&store, &ontology, &tuning).expect("linker");
        let interval = Interval::new(0, 10).expect("interval");
        let email_value = store.intern_value("alice@example.com");
        let phone_value = store.intern_value("+15550001");

        let records = [
            Record::new(
                RecordId(0),
                RecordIdentity::new("person".to_string(), "crm".to_string(), "email".to_string()),
                vec![Descriptor::new(email_attr, email_value, interval)],
            ),
            Record::new(
                RecordId(0),
                RecordIdentity::new("person".to_string(), "crm".to_string(), "phone".to_string()),
                vec![Descriptor::new(phone_attr, phone_value, interval)],
            ),
            Record::new(
                RecordId(0),
                RecordIdentity::new(
                    "person".to_string(),
                    "crm".to_string(),
                    "bridge".to_string(),
                ),
                vec![
                    Descriptor::new(email_attr, email_value, interval),
                    Descriptor::new(phone_attr, phone_value, interval),
                ],
            ),
        ];

        for record in records {
            let record_id = store.add_record(record).expect("record");
            linker
                .link_record(&store, &ontology, record_id)
                .expect("link");
        }

        let boundary = linker.export_boundary_index();
        let mut global_ids = [email_attr, phone_attr]
            .into_iter()
            .zip([email_value, phone_value])
            .flat_map(|(attr, value)| {
                let signature = ShardingKeySignature::from_key_values_with_interner(
                    "person",
                    &[KeyValue::new(attr, value)],
                    store.interner(),
                )
                .expect("signature");
                boundary
                    .get_boundaries(&signature)
                    .expect("boundary")
                    .iter()
                    .map(|entry| entry.cluster_id)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        global_ids.sort_by_key(GlobalClusterId::to_u64);
        global_ids.dedup();
        assert_eq!(global_ids.len(), 1);
        assert_eq!(linker.cross_shard_merge_count(), 1);
    }
}

/// Identity key signature for linker deduplication.
/// Distinct from sharding::IdentityKeySignature which uses a 32-byte hash.
///
/// Caches its hash to avoid re-hashing key values on every map access. Equality
/// still compares the entity type and key values when hashes collide.
#[derive(Debug, Clone)]
pub struct LinkerKeySignature {
    entity_type: String,
    key_values: Vec<KeyValue>,
    /// Precomputed hash for fast lookups
    cached_hash: u64,
}

impl LinkerKeySignature {
    /// Create a new key signature with precomputed hash.
    pub fn new(entity_type: &str, key_values: &[KeyValue]) -> Self {
        use rustc_hash::FxHasher;
        use std::hash::{Hash, Hasher};

        // Compute hash once during construction
        let mut hasher = FxHasher::default();
        entity_type.hash(&mut hasher);
        key_values.hash(&mut hasher);
        let cached_hash = hasher.finish();

        Self {
            entity_type: entity_type.to_string(),
            key_values: key_values.to_vec(),
            cached_hash,
        }
    }

    fn entity_type(&self) -> &str {
        &self.entity_type
    }

    fn key_values(&self) -> &[KeyValue] {
        &self.key_values
    }
}

impl PartialEq for LinkerKeySignature {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // Fast path: different hashes means definitely not equal
        if self.cached_hash != other.cached_hash {
            return false;
        }
        // Slow path: hashes match, verify actual equality
        self.entity_type == other.entity_type && self.key_values == other.key_values
    }
}

impl Eq for LinkerKeySignature {}

impl std::hash::Hash for LinkerKeySignature {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Just write the precomputed hash - O(1) instead of O(n)
        self.cached_hash.hash(state);
    }
}

#[derive(Debug, Clone, Default)]
struct KeyStats {
    total_candidates: u64,
    total_samples: u64,
    max_candidates: usize,
    last_cap: usize,
}

impl KeyStats {
    fn record(&mut self, candidate_len: usize, cap: usize) {
        self.total_candidates = self.total_candidates.saturating_add(candidate_len as u64);
        self.total_samples = self.total_samples.saturating_add(1);
        self.max_candidates = self.max_candidates.max(candidate_len);
        self.last_cap = cap;
    }

    fn average_candidates(&self) -> f64 {
        if self.total_samples == 0 {
            return 0.0;
        }
        self.total_candidates as f64 / self.total_samples as f64
    }
}

fn reconcile_cluster_ids(
    cluster_ids: &mut LinkerState<RecordId, ClusterId>,
    next_cluster_id: &mut u32,
    root_a: RecordId,
    root_b: RecordId,
    new_root: RecordId,
) {
    let id_a = cluster_ids.get_copy(&root_a);
    let id_b = cluster_ids.get_copy(&root_b);

    let chosen = match (id_a, id_b) {
        (Some(a), Some(b)) => {
            if a.0 <= b.0 {
                a
            } else {
                b
            }
        }
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => {
            let id = ClusterId(*next_cluster_id);
            *next_cluster_id += 1;
            id
        }
    };

    if root_a != new_root {
        cluster_ids.remove(&root_a);
    }
    if root_b != new_root {
        cluster_ids.remove(&root_b);
    }
    cluster_ids.insert(new_root, chosen);
}

#[allow(clippy::too_many_arguments)]
fn reconcile_global_cluster_ids(
    global_cluster_ids: &mut LinkerState<RecordId, GlobalClusterId>,
    global_cluster_anchors: &mut LinkerState<RecordId, u32>,
    cross_shard_merges: &mut HashMap<GlobalClusterId, GlobalClusterId>,
    boundary_signatures: &mut BTreeMap<(ShardingKeySignature, GlobalClusterId), BoundaryData>,
    dirty_boundary_keys: &mut BTreeSet<ShardingKeySignature>,
    shard_id: u16,
    root_a: RecordId,
    root_b: RecordId,
    new_root: RecordId,
) {
    fn resolve(
        redirects: &HashMap<GlobalClusterId, GlobalClusterId>,
        id: GlobalClusterId,
    ) -> GlobalClusterId {
        let mut current = id;
        let mut seen = HashSet::new();
        while let Some(next) = redirects.get(&current).copied() {
            if !seen.insert(current) {
                break;
            }
            current = next;
        }
        current
    }

    let anchor_a = global_cluster_anchors.get_copy(&root_a).unwrap_or(root_a.0);
    let anchor_b = global_cluster_anchors.get_copy(&root_b).unwrap_or(root_b.0);
    if root_a != new_root {
        global_cluster_anchors.remove(&root_a);
    }
    if root_b != new_root {
        global_cluster_anchors.remove(&root_b);
    }
    let stable_local_id = anchor_a.min(anchor_b);
    global_cluster_anchors.insert(new_root, stable_local_id);

    let id_a = global_cluster_ids.get_copy(&root_a);
    let id_b = global_cluster_ids.get_copy(&root_b);
    if id_a.is_none() && id_b.is_none() {
        return;
    }
    if root_a != new_root {
        global_cluster_ids.remove(&root_a);
    }
    if root_b != new_root {
        global_cluster_ids.remove(&root_b);
    }

    let mut ids = [id_a, id_b]
        .into_iter()
        .flatten()
        .flat_map(|id| [id, resolve(cross_shard_merges, id)])
        .collect::<Vec<_>>();
    ids.push(GlobalClusterId::new(shard_id, stable_local_id, 0));
    ids.sort_by_key(GlobalClusterId::to_u64);
    ids.dedup();
    let canonical = ids
        .first()
        .copied()
        .unwrap_or_else(|| GlobalClusterId::from_local(shard_id, ClusterId(new_root.0)));

    for id in &ids {
        if *id == canonical {
            cross_shard_merges.remove(id);
        } else {
            cross_shard_merges.insert(*id, canonical);
        }
    }
    for (_, global_id) in global_cluster_ids.iter_mut() {
        if ids.contains(global_id) {
            *global_id = canonical;
        }
    }
    global_cluster_ids.insert(new_root, canonical);

    let keys_to_update = boundary_signatures
        .keys()
        .filter(|(_, global_id)| ids.contains(global_id) && *global_id != canonical)
        .copied()
        .collect::<Vec<_>>();
    for (signature, old_id) in keys_to_update {
        if let Some(data) = boundary_signatures.remove(&(signature, old_id)) {
            boundary_signatures
                .entry((signature, canonical))
                .and_modify(|existing| existing.merge_from(&data))
                .or_insert(data);
            dirty_boundary_keys.insert(signature);
        }
    }
}

fn reconcile_cluster_summaries(
    summaries: &mut LinkerState<RecordId, StrongIdSummary>,
    root_a: RecordId,
    root_b: RecordId,
    new_root: RecordId,
) {
    let mut merged = summaries.remove(&new_root).unwrap_or_default();

    if let Some(summary) = summaries.remove(&root_a) {
        merged.merge(summary);
    }
    if let Some(summary) = summaries.remove(&root_b) {
        merged.merge(summary);
    }

    summaries.insert(new_root, merged);
}

/// Build a strong ID summary for a record based on ontology strong identifiers.
pub fn build_record_summary(record: &Record, ontology: &Ontology) -> StrongIdSummary {
    let _guard = crate::profile::profile_scope("build_record_summary");
    let strong_ids = ontology.strong_identifiers_for_type(&record.identity.entity_type);
    if strong_ids.is_empty() {
        debug!(
            "build_record_summary: no strong_ids for entity_type={}",
            record.identity.entity_type
        );
        return StrongIdSummary::default();
    }

    let mut strong_attrs = HashSet::new();
    for strong_id in &strong_ids {
        strong_attrs.insert(strong_id.attribute);
    }

    debug!(
        "build_record_summary: entity_type={} strong_attrs={:?} descriptor_attrs={:?}",
        record.identity.entity_type,
        strong_attrs,
        record
            .descriptors
            .iter()
            .map(|d| d.attr)
            .collect::<Vec<_>>()
    );

    let mut summary = StrongIdSummary::default();
    let perspective = record.identity.perspective.clone();
    let entry = summary
        .by_perspective
        .entry(perspective.clone())
        .or_default();

    let mut matched = 0;
    for descriptor in &record.descriptors {
        if strong_attrs.contains(&descriptor.attr) {
            matched += 1;
            entry
                .entry(descriptor.attr)
                .or_default()
                .entry(descriptor.value)
                .or_default()
                .push(descriptor.interval);
        }
    }

    debug!(
        "build_record_summary: matched {} descriptors as strong identifiers",
        matched
    );

    if let Some(attrs) = summary.by_perspective.get_mut(&perspective) {
        for value_map in attrs.values_mut() {
            coalesce_value_intervals(value_map);
        }
    }

    summary
}

/// Check if merging would create conflicts in existing clusters.
fn would_create_conflict_in_clusters(
    summaries: &LinkerState<RecordId, StrongIdSummary>,
    dsu: &mut DsuBackend,
    record_a: RecordId,
    record_b: RecordId,
) -> bool {
    let _guard = crate::profile::profile_scope("conflict_check");
    // Get the clusters that these records belong to
    let cluster_a = dsu.find(record_a).unwrap_or(record_a);
    let cluster_b = dsu.find(record_b).unwrap_or(record_b);

    // If they're already in the same cluster, no conflict
    if cluster_a == cluster_b {
        return false;
    }

    let summary_a = get_cluster_summary(summaries, cluster_a);
    let summary_b = get_cluster_summary(summaries, cluster_b);
    if cluster_summaries_conflict(summary_a, summary_b) {
        return true;
    }

    false
}

fn reconcile_cluster_members(
    roots: &mut FxHashMap<RecordId, RecordId>,
    members: &mut FxHashMap<RecordId, Vec<RecordId>>,
    root_a: RecordId,
    root_b: RecordId,
    new_root: RecordId,
) {
    roots.insert(root_a, new_root);
    roots.insert(root_b, new_root);
    roots.insert(new_root, new_root);
    let mut left = members.remove(&root_a).unwrap_or_default();
    let mut right = members.remove(&root_b).unwrap_or_default();
    if left.len() < right.len() {
        std::mem::swap(&mut left, &mut right);
    }
    left.extend(right);
    members.insert(new_root, left);
}

fn cluster_summaries_conflict(a: &StrongIdSummary, b: &StrongIdSummary) -> bool {
    for (perspective, attrs_a) in &a.by_perspective {
        let Some(attrs_b) = b.by_perspective.get(perspective) else {
            continue;
        };

        for (attr, values_a) in attrs_a {
            let Some(values_b) = attrs_b.get(attr) else {
                continue;
            };

            for (value_a, intervals_a) in values_a {
                for (value_b, intervals_b) in values_b {
                    if value_a == value_b {
                        continue;
                    }
                    if has_overlapping_interval(intervals_a, intervals_b) {
                        return true;
                    }
                }
            }
        }
    }

    false
}

fn same_perspective_conflict_for_clusters(
    summaries: &LinkerState<RecordId, StrongIdSummary>,
    root_a: RecordId,
    root_b: RecordId,
    perspective: &str,
) -> bool {
    let summary_a = get_cluster_summary(summaries, root_a);
    let summary_b = get_cluster_summary(summaries, root_b);

    let Some(attrs_a) = summary_a.by_perspective.get(perspective) else {
        debug!(
            "  No attrs for perspective '{}' in summary_a (root {:?})",
            perspective, root_a
        );
        return false;
    };
    let Some(attrs_b) = summary_b.by_perspective.get(perspective) else {
        debug!(
            "  No attrs for perspective '{}' in summary_b (root {:?})",
            perspective, root_b
        );
        return false;
    };

    debug!(
        "  Summary A has {} attrs, Summary B has {} attrs for perspective '{}'",
        attrs_a.len(),
        attrs_b.len(),
        perspective
    );

    for (attr, values_a) in attrs_a {
        let Some(values_b) = attrs_b.get(attr) else {
            continue;
        };

        debug!(
            "  Comparing attr {:?}: {} values in A, {} values in B",
            attr,
            values_a.len(),
            values_b.len()
        );

        for (value_a, intervals_a) in values_a {
            for (value_b, intervals_b) in values_b {
                if value_a == value_b {
                    continue;
                }
                let overlaps = has_overlapping_interval(intervals_a, intervals_b);
                debug!(
                    "    value_a={:?} vs value_b={:?} overlaps={}",
                    value_a, value_b, overlaps
                );
                if overlaps {
                    return true;
                }
            }
        }
    }

    false
}

fn get_cluster_summary(
    summaries: &LinkerState<RecordId, StrongIdSummary>,
    root: RecordId,
) -> &StrongIdSummary {
    static EMPTY: std::sync::OnceLock<StrongIdSummary> = std::sync::OnceLock::new();
    summaries
        .peek(&root)
        .unwrap_or_else(|| EMPTY.get_or_init(StrongIdSummary::default))
}

fn coalesce_value_intervals(values: &mut HashMap<crate::model::ValueId, Vec<Interval>>) {
    for intervals in values.values_mut() {
        if intervals.len() <= 1 {
            continue;
        }
        let coalesced = crate::temporal::coalesce_same_value(
            &intervals
                .iter()
                .map(|interval| (*interval, ()))
                .collect::<Vec<_>>(),
        );
        *intervals = coalesced
            .into_iter()
            .map(|(interval, _)| interval)
            .collect();
    }
}

fn has_overlapping_interval(a: &[Interval], b: &[Interval]) -> bool {
    let mut i = 0;
    let mut j = 0;

    while i < a.len() && j < b.len() {
        let left = &a[i];
        let right = &b[j];
        if crate::temporal::is_overlapping(left, right) {
            return true;
        }
        if left.end <= right.start {
            i += 1;
        } else {
            j += 1;
        }
    }

    false
}

impl StreamingLinker {
    /// Hydrate only local components of the requested canonical entities.
    /// Global IDs are anchored to durable record IDs. Remote canonical entities
    /// additionally reach local anchors through the sparse redirect map.
    pub fn clusters_for_global_ids(&self, ids: &[GlobalClusterId]) -> Vec<crate::dsu::Cluster> {
        let targets: HashSet<_> = ids
            .iter()
            .map(|id| self.resolve_global_cluster_id(*id))
            .collect();
        let mut anchors: HashSet<RecordId> = targets
            .iter()
            .filter(|id| id.shard_id == self.shard_id)
            .map(|id| RecordId(id.local_id))
            .collect();
        for (alias, target) in &self.cross_shard_merges {
            if alias.shard_id == self.shard_id
                && targets.contains(&self.resolve_global_cluster_id(*target))
            {
                anchors.insert(RecordId(alias.local_id));
            }
        }
        let mut roots = HashSet::new();
        anchors
            .into_iter()
            .filter_map(|anchor| {
                let root = self.root_for_record(anchor)?;
                if !roots.insert(root)
                    || !targets.contains(&self.global_cluster_id_for_readonly(anchor)?)
                {
                    return None;
                }
                self.cluster_for_record(anchor)
            })
            .collect()
    }
}
