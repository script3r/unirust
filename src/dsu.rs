//! # Disjoint Set Union (DSU) with Temporal Guards
//!
//! Implements Union-Find data structure with temporal validation to ensure
//! that merges only occur when temporal constraints are satisfied.
//!
//! This module provides two implementations:
//! - `TemporalDSU`: In-memory implementation for smaller datasets
//! - `PersistentTemporalDSU`: RocksDB-backed implementation for billion-scale datasets

use crate::model::{ClusterId, RecordId};
use crate::temporal::Interval;
use anyhow::{anyhow, Result};
use hashbrown::HashMap;
use lru::LruCache;
use rocksdb::{WriteBatch, DB};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

/// A temporal guard that validates whether two records can be merged
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalGuard {
    /// The time interval when this guard is valid
    pub interval: Interval,
    /// The reason for the guard (e.g., "identity_key_match", "crosswalk_match")
    pub reason: String,
    /// Additional metadata about the guard
    pub metadata: HashMap<String, String>,
}

impl TemporalGuard {
    /// Create a new temporal guard
    pub fn new(interval: Interval, reason: String) -> Self {
        Self {
            interval,
            reason,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the guard
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// A temporal conflict that prevents merging
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalConflict {
    /// The time interval when the conflict occurs
    pub interval: Interval,
    /// The reason for the conflict (e.g., "strong_identifier_mismatch")
    pub reason: String,
    /// The conflicting values
    pub conflicting_values: Vec<String>,
    /// Additional metadata about the conflict
    pub metadata: HashMap<String, String>,
}

impl TemporalConflict {
    /// Create a new temporal conflict
    pub fn new(interval: Interval, reason: String, conflicting_values: Vec<String>) -> Self {
        Self {
            interval,
            reason,
            conflicting_values,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the conflict
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// Result of attempting to merge two records
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeResult {
    /// The merge was successful
    Success {
        /// The guard that allowed the merge
        guard: TemporalGuard,
    },
    /// The merge was blocked by a temporal conflict
    Blocked {
        /// The conflict that blocked the merge
        conflict: TemporalConflict,
    },
}

/// Disjoint Set Union with temporal guards
#[derive(Debug, Clone)]
pub struct TemporalDSU {
    /// Parent array for Union-Find - uses FxHashMap for faster hashing
    parent: FxHashMap<RecordId, RecordId>,
    /// Rank array for path compression - uses FxHashMap for faster hashing
    rank: FxHashMap<RecordId, u32>,
    /// Guards for each record (less frequent access, use standard HashMap)
    guards: HashMap<RecordId, Vec<TemporalGuard>>,
    /// Conflicts for each record (less frequent access, use standard HashMap)
    conflicts: HashMap<RecordId, Vec<TemporalConflict>>,
    /// Next available cluster ID
    next_cluster_id: u32,
    /// Current number of clusters
    cluster_count: usize,
    /// Root cache for recently found roots (record_id -> root_id)
    /// Invalidated on union operations
    root_cache: FxHashMap<RecordId, RecordId>,
    /// Maximum root cache size before eviction
    root_cache_max: usize,
}

/// Default root cache size - tuned for typical streaming workloads
const DEFAULT_ROOT_CACHE_SIZE: usize = 16384;

impl TemporalDSU {
    /// Create a new temporal DSU
    pub fn new() -> Self {
        Self {
            parent: FxHashMap::default(),
            rank: FxHashMap::default(),
            guards: HashMap::new(),
            conflicts: HashMap::new(),
            next_cluster_id: 0,
            cluster_count: 0,
            root_cache: FxHashMap::default(),
            root_cache_max: DEFAULT_ROOT_CACHE_SIZE,
        }
    }

    /// Create a new temporal DSU with custom root cache size
    pub fn with_cache_size(root_cache_max: usize) -> Self {
        Self {
            parent: FxHashMap::default(),
            rank: FxHashMap::default(),
            guards: HashMap::new(),
            conflicts: HashMap::new(),
            next_cluster_id: 0,
            cluster_count: 0,
            root_cache: FxHashMap::default(),
            root_cache_max,
        }
    }

    /// Add a record to the DSU
    pub fn add_record(&mut self, record_id: RecordId) {
        self.parent.insert(record_id, record_id);
        self.rank.insert(record_id, 0);
        self.guards.insert(record_id, Vec::new());
        self.conflicts.insert(record_id, Vec::new());
        self.cluster_count += 1;
    }

    /// Check if a record exists in the DSU.
    pub fn has_record(&self, record_id: RecordId) -> bool {
        self.parent.contains_key(&record_id)
    }

    /// Find the root of a record (with path compression via path halving)
    /// Returns the record itself if not in DSU (treats untracked records as self-roots)
    ///
    /// Uses a root cache for O(1) lookup of recently found roots, dramatically
    /// improving performance when the same records are queried repeatedly.
    #[inline]
    pub fn find(&mut self, record_id: RecordId) -> RecordId {
        // Fast path: check root cache first (common in streaming)
        if let Some(&cached_root) = self.root_cache.get(&record_id) {
            // Verify cache is still valid (root's parent should be itself)
            if self.parent.get(&cached_root).copied() == Some(cached_root) {
                return cached_root;
            }
            // Cache is stale, remove it
            self.root_cache.remove(&record_id);
        }

        // Quick check - if not in DSU or already root, return immediately
        let Some(&initial_parent) = self.parent.get(&record_id) else {
            return record_id; // Not in DSU = self-root
        };
        if initial_parent == record_id {
            return record_id;
        }

        // Path halving with deferred writes - collect path first, then compress
        let root = self.find_root_with_path_halving(record_id, initial_parent);

        // Cache the result (with simple eviction when full)
        if self.root_cache.len() >= self.root_cache_max {
            // Clear half the cache (simple but effective eviction)
            let keys_to_remove: Vec<_> = self
                .root_cache
                .keys()
                .take(self.root_cache_max / 2)
                .copied()
                .collect();
            for key in keys_to_remove {
                self.root_cache.remove(&key);
            }
        }
        self.root_cache.insert(record_id, root);

        root
    }

    /// Internal path-halving implementation - called when we know there's work to do
    #[inline]
    fn find_root_with_path_halving(
        &mut self,
        start: RecordId,
        initial_parent: RecordId,
    ) -> RecordId {
        let mut current = start;
        let mut parent = initial_parent;

        // Path halving: point every other node to its grandparent
        // This is a single-pass algorithm that compresses while finding
        loop {
            // Use get() once, avoid double lookup
            let grandparent = self.parent.get(&parent).copied().unwrap_or(parent);

            if grandparent == parent {
                // Parent is root
                break;
            }

            // Point current to grandparent (skip parent)
            self.parent.insert(current, grandparent);
            current = grandparent;

            // Get next parent
            parent = self.parent.get(&current).copied().unwrap_or(current);
            if parent == current {
                break;
            }
        }

        parent
    }

    /// Check if two records are in the same cluster
    pub fn same_cluster(&mut self, a: RecordId, b: RecordId) -> bool {
        self.find(a) == self.find(b)
    }

    /// Attempt to merge two records with temporal validation
    pub fn try_merge(&mut self, a: RecordId, b: RecordId, guard: TemporalGuard) -> MergeResult {
        // Ensure both records exist in the DSU
        if !self.parent.contains_key(&a) {
            self.add_record(a);
        }
        if !self.parent.contains_key(&b) {
            self.add_record(b);
        }

        let root_a = self.find(a);
        let root_b = self.find(b);

        if root_a == root_b {
            return MergeResult::Success { guard };
        }

        // Check for temporal conflicts
        if let Some(conflict) = self.check_temporal_conflict(a, b, &guard) {
            return MergeResult::Blocked { conflict };
        }

        // Perform the union
        self.union(root_a, root_b);

        // Add the guard to both records (use entry API to avoid panic)
        self.guards.entry(a).or_default().push(guard.clone());
        self.guards.entry(b).or_default().push(guard.clone());

        MergeResult::Success { guard }
    }

    /// Check for temporal conflicts between two records
    fn check_temporal_conflict(
        &self,
        a: RecordId,
        b: RecordId,
        _guard: &TemporalGuard,
    ) -> Option<TemporalConflict> {
        // Get all guards for both records
        let empty_vec = Vec::new();
        let guards_a = self.guards.get(&a).unwrap_or(&empty_vec);
        let guards_b = self.guards.get(&b).unwrap_or(&empty_vec);

        // Check for overlapping guards with conflicting reasons
        for guard_a in guards_a {
            for guard_b in guards_b {
                if crate::temporal::is_overlapping(&guard_a.interval, &guard_b.interval)
                    && guard_a.reason != guard_b.reason
                {
                    return Some(TemporalConflict::new(
                        crate::temporal::intersect(&guard_a.interval, &guard_b.interval).unwrap(),
                        "conflicting_guards".to_string(),
                        vec![guard_a.reason.clone(), guard_b.reason.clone()],
                    ));
                }
            }
        }

        None
    }

    /// Union two clusters (internal method)
    /// Invalidates the root cache since cluster structure has changed.
    fn union(&mut self, a: RecordId, b: RecordId) {
        // Use get() with default to avoid panic on missing key
        let rank_a = self.rank.get(&a).copied().unwrap_or(0);
        let rank_b = self.rank.get(&b).copied().unwrap_or(0);

        if rank_a < rank_b {
            self.parent.insert(a, b);
        } else if rank_a > rank_b {
            self.parent.insert(b, a);
        } else {
            self.parent.insert(a, b);
            self.rank.insert(b, rank_b + 1);
        }
        self.cluster_count = self.cluster_count.saturating_sub(1);

        // Invalidate root cache entries for merged roots
        // This is O(1) and necessary for correctness
        self.root_cache.remove(&a);
        self.root_cache.remove(&b);
    }

    /// Get all clusters.
    /// Cluster IDs are assigned on demand and are not stable across calls.
    pub fn get_clusters(&mut self) -> Clusters {
        let num_records = self.parent.len();
        if num_records == 0 {
            return Clusters {
                clusters: Vec::new(),
            };
        }

        // Single pass: compute roots and group in one iteration
        // We need to collect keys first since find() mutates parent
        let record_ids: Vec<RecordId> = self.parent.keys().copied().collect();

        // Group records by root - use estimated cluster count for capacity
        let estimated_clusters = self.cluster_count.max(1);
        let avg_cluster_size = (num_records / estimated_clusters).max(4);
        let mut cluster_map: HashMap<RecordId, Vec<RecordId>> =
            HashMap::with_capacity(estimated_clusters);

        for record_id in record_ids {
            let root = self.find(record_id);
            // Use entry API with pre-allocated Vec capacity
            cluster_map
                .entry(root)
                .or_insert_with(|| Vec::with_capacity(avg_cluster_size))
                .push(record_id);
        }

        // Build clusters - consume the map to avoid cloning
        let mut clusters = Vec::with_capacity(cluster_map.len());
        for (root, records) in cluster_map {
            let cluster_id = ClusterId(self.next_cluster_id);
            self.next_cluster_id += 1;
            clusters.push(Cluster {
                id: cluster_id,
                root,
                records,
            });
        }

        Clusters { clusters }
    }

    /// Get the cluster ID for a record.
    /// This recomputes clusters and assigns new IDs each call.
    pub fn get_cluster_id(&mut self, record_id: RecordId) -> Option<ClusterId> {
        let root = self.find(record_id);
        // Find the cluster that contains this root
        for cluster in self.get_clusters().clusters {
            if cluster.root == root {
                return Some(cluster.id);
            }
        }
        None
    }

    /// Get guards for a record
    pub fn get_guards(&self, record_id: RecordId) -> &[TemporalGuard] {
        self.guards
            .get(&record_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get conflicts for a record
    pub fn get_conflicts(&self, record_id: RecordId) -> &[TemporalConflict] {
        self.conflicts
            .get(&record_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Add a conflict to a record
    pub fn add_conflict(&mut self, record_id: RecordId, conflict: TemporalConflict) {
        self.conflicts.entry(record_id).or_default().push(conflict);
    }

    /// Get the number of clusters
    pub fn num_clusters(&mut self) -> usize {
        self.cluster_count
    }

    /// Get the number of clusters without mutating.
    pub fn cluster_count(&self) -> usize {
        self.cluster_count
    }
}

impl Default for TemporalDSU {
    fn default() -> Self {
        Self::new()
    }
}

/// A cluster of records
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cluster {
    /// Unique identifier for this cluster
    pub id: ClusterId,
    /// The root record of this cluster
    pub root: RecordId,
    /// All records in this cluster
    pub records: Vec<RecordId>,
}

impl Cluster {
    /// Create a new cluster
    pub fn new(id: ClusterId, root: RecordId, records: Vec<RecordId>) -> Self {
        Self { id, root, records }
    }

    /// Get the number of records in this cluster
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if this cluster is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Check if this cluster contains a specific record
    pub fn contains(&self, record_id: RecordId) -> bool {
        self.records.contains(&record_id)
    }
}

/// Collection of all clusters
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Clusters {
    /// All clusters
    pub clusters: Vec<Cluster>,
}

impl Clusters {
    /// Create a new clusters collection
    pub fn new() -> Self {
        Self {
            clusters: Vec::new(),
        }
    }

    /// Add a cluster
    pub fn add_cluster(&mut self, cluster: Cluster) {
        self.clusters.push(cluster);
    }

    /// Get a cluster by ID
    pub fn get_cluster(&self, id: ClusterId) -> Option<&Cluster> {
        self.clusters.iter().find(|c| c.id == id)
    }

    /// Get all clusters
    pub fn get_all_clusters(&self) -> &[Cluster] {
        &self.clusters
    }

    /// Get the number of clusters
    pub fn len(&self) -> usize {
        self.clusters.len()
    }

    /// Check if there are no clusters
    pub fn is_empty(&self) -> bool {
        self.clusters.is_empty()
    }

    /// Get clusters containing a specific record
    pub fn get_clusters_for_record(&self, record_id: RecordId) -> Vec<&Cluster> {
        self.clusters
            .iter()
            .filter(|cluster| cluster.contains(record_id))
            .collect()
    }
}

impl Default for Clusters {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Persistent DSU Implementation
// ============================================================================

/// Configuration for persistent DSU cache sizes
#[derive(Debug, Clone)]
pub struct PersistentDSUConfig {
    /// Maximum entries in parent cache (default: 5M, ~80MB)
    pub parent_cache_size: usize,
    /// Maximum entries in rank cache (default: 1M, ~12MB)
    pub rank_cache_size: usize,
    /// Maximum entries in guards cache (default: 100K)
    pub guards_cache_size: usize,
    /// Size of dirty write buffer before flush (default: 100K)
    pub dirty_buffer_size: usize,
    /// Enable path compression writes to disk (default: false for cold paths)
    pub persist_path_compression: bool,
}

impl Default for PersistentDSUConfig {
    fn default() -> Self {
        Self {
            parent_cache_size: 5_000_000,
            rank_cache_size: 1_000_000,
            guards_cache_size: 500_000, // Increased: ~50MB for 500K entries
            dirty_buffer_size: 200_000, // Increased: fewer flushes
            persist_path_compression: false,
        }
    }
}

impl PersistentDSUConfig {
    /// Memory-optimized configuration (~40MB total cache)
    pub fn memory_saver() -> Self {
        Self {
            parent_cache_size: 500_000,
            rank_cache_size: 100_000,
            guards_cache_size: 10_000,
            dirty_buffer_size: 50_000,
            persist_path_compression: false,
        }
    }

    /// High-performance configuration (~400MB total cache)
    pub fn high_performance() -> Self {
        Self {
            parent_cache_size: 20_000_000,
            rank_cache_size: 5_000_000,
            guards_cache_size: 500_000,
            dirty_buffer_size: 500_000,
            persist_path_compression: true,
        }
    }
}

/// RocksDB-backed Disjoint Set Union for billion-scale entity resolution.
///
/// Uses LRU caches for hot paths with RocksDB for persistent storage.
/// Memory usage: ~2GB instead of 60-80GB for 1B entities.
pub struct PersistentTemporalDSU {
    /// Reference to the RocksDB database
    db: Arc<DB>,
    /// LRU cache for parent lookups (5M entries, ~80MB)
    parent_cache: LruCache<RecordId, RecordId>,
    /// LRU cache for rank lookups (1M entries, ~12MB)
    rank_cache: LruCache<RecordId, u32>,
    /// LRU cache for guards
    guards_cache: LruCache<RecordId, Vec<TemporalGuard>>,
    /// Hot roots that should always stay in memory
    hot_roots: HashMap<RecordId, usize>, // root -> cluster_size
    /// Dirty parent entries pending write
    dirty_parents: HashMap<RecordId, RecordId>,
    /// Dirty rank entries pending write
    dirty_ranks: HashMap<RecordId, u32>,
    /// Dirty guard entries pending write
    dirty_guards: HashMap<RecordId, Vec<TemporalGuard>>,
    /// Next available cluster ID
    next_cluster_id: AtomicU32,
    /// Current number of clusters
    cluster_count: AtomicUsize,
    /// Configuration
    config: PersistentDSUConfig,
    /// Column family name constants
    cf_parent: &'static str,
    cf_rank: &'static str,
    cf_guards: &'static str,
    cf_metadata: &'static str,
}

impl PersistentTemporalDSU {
    /// Create a new persistent DSU with the given database and configuration.
    pub fn new(db: Arc<DB>, config: PersistentDSUConfig) -> Result<Self> {
        let parent_cache = LruCache::new(
            NonZeroUsize::new(config.parent_cache_size).unwrap_or(NonZeroUsize::new(1).unwrap()),
        );
        let rank_cache = LruCache::new(
            NonZeroUsize::new(config.rank_cache_size).unwrap_or(NonZeroUsize::new(1).unwrap()),
        );
        let guards_cache = LruCache::new(
            NonZeroUsize::new(config.guards_cache_size).unwrap_or(NonZeroUsize::new(1).unwrap()),
        );

        let mut dsu = Self {
            db,
            parent_cache,
            rank_cache,
            guards_cache,
            hot_roots: HashMap::new(),
            dirty_parents: HashMap::new(),
            dirty_ranks: HashMap::new(),
            dirty_guards: HashMap::new(),
            next_cluster_id: AtomicU32::new(0),
            cluster_count: AtomicUsize::new(0),
            config,
            cf_parent: crate::persistence::dsu_cf::PARENT,
            cf_rank: crate::persistence::dsu_cf::RANK,
            cf_guards: crate::persistence::dsu_cf::GUARDS,
            cf_metadata: crate::persistence::dsu_cf::METADATA,
        };

        // Load metadata from disk
        dsu.load_metadata()?;

        Ok(dsu)
    }

    /// Create with default configuration
    pub fn with_defaults(db: Arc<DB>) -> Result<Self> {
        Self::new(db, PersistentDSUConfig::default())
    }

    /// Load metadata (next_cluster_id, cluster_count) from RocksDB
    fn load_metadata(&mut self) -> Result<()> {
        let cf = self
            .db
            .cf_handle(self.cf_metadata)
            .ok_or_else(|| anyhow!("missing DSU metadata column family"))?;

        // Load next_cluster_id
        if let Some(bytes) = self
            .db
            .get_cf(cf, crate::persistence::dsu_keys::NEXT_CLUSTER_ID)?
        {
            if bytes.len() >= 4 {
                let id = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                self.next_cluster_id.store(id, Ordering::SeqCst);
            }
        }

        // Load cluster_count
        if let Some(bytes) = self
            .db
            .get_cf(cf, crate::persistence::dsu_keys::CLUSTER_COUNT)?
        {
            if bytes.len() >= 8 {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes[..8]);
                let count = usize::from_be_bytes(buf);
                self.cluster_count.store(count, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    /// Save metadata to RocksDB
    fn save_metadata(&self) -> Result<()> {
        let cf = self
            .db
            .cf_handle(self.cf_metadata)
            .ok_or_else(|| anyhow!("missing DSU metadata column family"))?;

        let next_id = self.next_cluster_id.load(Ordering::SeqCst);
        self.db.put_cf(
            cf,
            crate::persistence::dsu_keys::NEXT_CLUSTER_ID,
            next_id.to_be_bytes(),
        )?;

        let count = self.cluster_count.load(Ordering::SeqCst);
        self.db.put_cf(
            cf,
            crate::persistence::dsu_keys::CLUSTER_COUNT,
            count.to_be_bytes(),
        )?;

        Ok(())
    }

    /// Add a record to the DSU (record becomes its own parent with rank 0)
    pub fn add_record(&mut self, record_id: RecordId) -> Result<()> {
        // Check if already exists in cache or disk
        if self.has_record(record_id)? {
            return Ok(());
        }

        // Initialize: parent = self, rank = 0
        self.dirty_parents.insert(record_id, record_id);
        self.dirty_ranks.insert(record_id, 0);
        self.parent_cache.put(record_id, record_id);
        self.rank_cache.put(record_id, 0);
        self.cluster_count.fetch_add(1, Ordering::SeqCst);

        // Flush if dirty buffer is full
        if self.dirty_parents.len() >= self.config.dirty_buffer_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Check if a record exists in the DSU
    pub fn has_record(&mut self, record_id: RecordId) -> Result<bool> {
        // Check cache first
        if self.parent_cache.contains(&record_id) {
            return Ok(true);
        }

        // Check dirty buffer
        if self.dirty_parents.contains_key(&record_id) {
            return Ok(true);
        }

        // Check disk
        let cf = self
            .db
            .cf_handle(self.cf_parent)
            .ok_or_else(|| anyhow!("missing DSU parent column family"))?;

        let key = crate::persistence::dsu_encoding::encode_record_key(record_id);
        Ok(self.db.get_cf(cf, key)?.is_some())
    }

    /// Get parent from cache, dirty buffer, or disk
    fn get_parent(&mut self, record_id: RecordId) -> Result<Option<RecordId>> {
        // Check cache first (promotes to front)
        if let Some(&parent) = self.parent_cache.get(&record_id) {
            return Ok(Some(parent));
        }

        // Check dirty buffer
        if let Some(&parent) = self.dirty_parents.get(&record_id) {
            self.parent_cache.put(record_id, parent);
            return Ok(Some(parent));
        }

        // Load from disk
        let cf = self
            .db
            .cf_handle(self.cf_parent)
            .ok_or_else(|| anyhow!("missing DSU parent column family"))?;

        let key = crate::persistence::dsu_encoding::encode_record_key(record_id);
        if let Some(bytes) = self.db.get_cf(cf, key)? {
            if let Some(parent) = crate::persistence::dsu_encoding::decode_parent_value(&bytes) {
                self.parent_cache.put(record_id, parent);
                return Ok(Some(parent));
            }
        }

        Ok(None)
    }

    /// Set parent (in cache and dirty buffer)
    fn set_parent(&mut self, record_id: RecordId, parent_id: RecordId) {
        self.parent_cache.put(record_id, parent_id);
        self.dirty_parents.insert(record_id, parent_id);
    }

    /// Get rank from cache, dirty buffer, or disk
    fn get_rank(&mut self, record_id: RecordId) -> Result<u32> {
        // Check cache first
        if let Some(&rank) = self.rank_cache.get(&record_id) {
            return Ok(rank);
        }

        // Check dirty buffer
        if let Some(&rank) = self.dirty_ranks.get(&record_id) {
            self.rank_cache.put(record_id, rank);
            return Ok(rank);
        }

        // Load from disk
        let cf = self
            .db
            .cf_handle(self.cf_rank)
            .ok_or_else(|| anyhow!("missing DSU rank column family"))?;

        let key = crate::persistence::dsu_encoding::encode_record_key(record_id);
        if let Some(bytes) = self.db.get_cf(cf, key)? {
            if let Some(rank) = crate::persistence::dsu_encoding::decode_rank_value(&bytes) {
                self.rank_cache.put(record_id, rank);
                return Ok(rank);
            }
        }

        Ok(0)
    }

    /// Set rank (in cache and dirty buffer)
    fn set_rank(&mut self, record_id: RecordId, rank: u32) {
        self.rank_cache.put(record_id, rank);
        self.dirty_ranks.insert(record_id, rank);
    }

    /// Fast inline cache lookup - avoids function call overhead for hot path.
    #[inline(always)]
    fn get_parent_cached(&self, record_id: RecordId) -> Option<RecordId> {
        // Check cache first (peek doesn't promote, faster)
        if let Some(&parent) = self.parent_cache.peek(&record_id) {
            return Some(parent);
        }
        // Check dirty buffer
        self.dirty_parents.get(&record_id).copied()
    }

    /// Find the root of a record with path halving.
    /// Uses deferred compression for cold paths - only persists hot path compression.
    pub fn find(&mut self, record_id: RecordId) -> Result<RecordId> {
        // Fast path: check cache first (most common case in streaming)
        if let Some(parent) = self.get_parent_cached(record_id) {
            if parent == record_id {
                return Ok(record_id);
            }
            // Continue with fast cache-based path halving
            return self.find_with_path_halving(record_id, parent);
        }

        // Slow path: need to check disk
        let parent = match self.get_parent(record_id)? {
            Some(p) => p,
            None => return Ok(record_id), // Not in DSU, treat as self-root
        };

        if parent == record_id {
            return Ok(record_id);
        }

        self.find_with_path_halving(record_id, parent)
    }

    /// Path halving implementation - called when we know there's a non-trivial path.
    fn find_with_path_halving(
        &mut self,
        start: RecordId,
        initial_parent: RecordId,
    ) -> Result<RecordId> {
        let mut current = start;
        let mut parent = initial_parent;
        let mut path_length = 0;

        loop {
            // Try fast cache path first
            let grandparent = if let Some(gp) = self.get_parent_cached(parent) {
                gp
            } else {
                // Fallback to full lookup
                match self.get_parent(parent)? {
                    Some(gp) => gp,
                    None => parent,
                }
            };

            if grandparent == parent {
                // Parent is root
                break;
            }

            // Point current to grandparent (skip parent)
            self.parent_cache.put(current, grandparent);

            // Only persist compression for hot paths
            if self.config.persist_path_compression || path_length < 3 {
                self.dirty_parents.insert(current, grandparent);
            }

            current = grandparent;

            // Try fast path for next parent
            parent = if let Some(p) = self.get_parent_cached(current) {
                p
            } else {
                match self.get_parent(current)? {
                    Some(p) => p,
                    None => current,
                }
            };

            if parent == current {
                break;
            }

            path_length += 1;
        }

        Ok(parent)
    }

    /// Check if two records are in the same cluster
    pub fn same_cluster(&mut self, a: RecordId, b: RecordId) -> Result<bool> {
        Ok(self.find(a)? == self.find(b)?)
    }

    /// Get guards for a record
    pub fn get_guards(&mut self, record_id: RecordId) -> Result<Vec<TemporalGuard>> {
        // Check cache first
        if let Some(guards) = self.guards_cache.get(&record_id) {
            return Ok(guards.clone());
        }

        // Check dirty buffer
        if let Some(guards) = self.dirty_guards.get(&record_id) {
            self.guards_cache.put(record_id, guards.clone());
            return Ok(guards.clone());
        }

        // Load from disk
        let cf = self
            .db
            .cf_handle(self.cf_guards)
            .ok_or_else(|| anyhow!("missing DSU guards column family"))?;

        let key = crate::persistence::dsu_encoding::encode_record_key(record_id);
        if let Some(bytes) = self.db.get_cf(cf, key)? {
            let guards = crate::persistence::dsu_encoding::decode_guards(&bytes)?;
            self.guards_cache.put(record_id, guards.clone());
            return Ok(guards);
        }

        Ok(Vec::new())
    }

    /// Add a guard to a record, with compaction to prevent unbounded growth.
    /// Guards with the same reason are merged by extending intervals.
    fn add_guard(&mut self, record_id: RecordId, guard: TemporalGuard) -> Result<()> {
        let mut guards = self.get_guards(record_id)?;

        // Try to merge with existing guard of same reason
        let mut merged = false;
        for existing in &mut guards {
            if existing.reason == guard.reason {
                // Extend the interval to encompass both
                let new_start = existing.interval.start.min(guard.interval.start);
                let new_end = existing.interval.end.max(guard.interval.end);
                if let Ok(new_interval) = crate::temporal::Interval::new(new_start, new_end) {
                    existing.interval = new_interval;
                    merged = true;
                    break;
                }
            }
        }

        if !merged {
            // Limit total guards per record to prevent unbounded growth
            const MAX_GUARDS_PER_RECORD: usize = 100;
            if guards.len() >= MAX_GUARDS_PER_RECORD {
                // Remove oldest guard (first in list) to make room
                guards.remove(0);
            }
            guards.push(guard);
        }

        self.guards_cache.put(record_id, guards.clone());
        self.dirty_guards.insert(record_id, guards);
        Ok(())
    }

    /// Check for temporal conflicts between two records.
    /// Fast path: returns early if either record has no guards (common in streaming).
    fn check_temporal_conflict(
        &mut self,
        a: RecordId,
        b: RecordId,
        _guard: &TemporalGuard,
    ) -> Result<Option<TemporalConflict>> {
        // Fast path: check if guards exist before loading them
        // Most records in streaming mode have few or no guards
        let guards_a = self.get_guards(a)?;
        if guards_a.is_empty() {
            return Ok(None);
        }

        let guards_b = self.get_guards(b)?;
        if guards_b.is_empty() {
            return Ok(None);
        }

        // Check for overlapping guards with conflicting reasons
        for guard_a in &guards_a {
            for guard_b in &guards_b {
                if crate::temporal::is_overlapping(&guard_a.interval, &guard_b.interval)
                    && guard_a.reason != guard_b.reason
                {
                    return Ok(Some(TemporalConflict::new(
                        crate::temporal::intersect(&guard_a.interval, &guard_b.interval).unwrap(),
                        "conflicting_guards".to_string(),
                        vec![guard_a.reason.clone(), guard_b.reason.clone()],
                    )));
                }
            }
        }

        Ok(None)
    }

    /// Attempt to merge two records with temporal validation
    pub fn try_merge(
        &mut self,
        a: RecordId,
        b: RecordId,
        guard: TemporalGuard,
    ) -> Result<MergeResult> {
        // Ensure both records exist in the DSU
        if !self.has_record(a)? {
            self.add_record(a)?;
        }
        if !self.has_record(b)? {
            self.add_record(b)?;
        }

        let root_a = self.find(a)?;
        let root_b = self.find(b)?;

        if root_a == root_b {
            return Ok(MergeResult::Success { guard });
        }

        // Check for temporal conflicts
        if let Some(conflict) = self.check_temporal_conflict(a, b, &guard)? {
            return Ok(MergeResult::Blocked { conflict });
        }

        // Perform the union
        self.union(root_a, root_b)?;

        // Add the guard to both records
        self.add_guard(a, guard.clone())?;
        self.add_guard(b, guard.clone())?;

        // Flush if any dirty buffer is full
        let total_dirty = self.dirty_parents.len() + self.dirty_guards.len();
        if total_dirty >= self.config.dirty_buffer_size {
            self.flush()?;
        }

        Ok(MergeResult::Success { guard })
    }

    /// Union two clusters (internal method)
    fn union(&mut self, a: RecordId, b: RecordId) -> Result<()> {
        let rank_a = self.get_rank(a)?;
        let rank_b = self.get_rank(b)?;

        if rank_a < rank_b {
            self.set_parent(a, b);
        } else if rank_a > rank_b {
            self.set_parent(b, a);
        } else {
            self.set_parent(a, b);
            self.set_rank(b, rank_b + 1);
        }

        // Update cluster count
        let count = self.cluster_count.load(Ordering::SeqCst);
        if count > 0 {
            self.cluster_count.store(count - 1, Ordering::SeqCst);
        }

        Ok(())
    }

    /// Flush all dirty entries to RocksDB
    pub fn flush(&mut self) -> Result<()> {
        if self.dirty_parents.is_empty()
            && self.dirty_ranks.is_empty()
            && self.dirty_guards.is_empty()
        {
            return Ok(());
        }

        let mut batch = WriteBatch::default();

        // Flush parents
        let parent_cf = self
            .db
            .cf_handle(self.cf_parent)
            .ok_or_else(|| anyhow!("missing DSU parent column family"))?;

        for (record_id, parent_id) in self.dirty_parents.drain() {
            let key = crate::persistence::dsu_encoding::encode_record_key(record_id);
            let value = crate::persistence::dsu_encoding::encode_parent_value(parent_id);
            batch.put_cf(parent_cf, key, value);
        }

        // Flush ranks
        let rank_cf = self
            .db
            .cf_handle(self.cf_rank)
            .ok_or_else(|| anyhow!("missing DSU rank column family"))?;

        for (record_id, rank) in self.dirty_ranks.drain() {
            let key = crate::persistence::dsu_encoding::encode_record_key(record_id);
            let value = crate::persistence::dsu_encoding::encode_rank_value(rank);
            batch.put_cf(rank_cf, key, value);
        }

        // Flush guards
        let guards_cf = self
            .db
            .cf_handle(self.cf_guards)
            .ok_or_else(|| anyhow!("missing DSU guards column family"))?;

        for (record_id, guards) in self.dirty_guards.drain() {
            let key = crate::persistence::dsu_encoding::encode_record_key(record_id);
            let value = crate::persistence::dsu_encoding::encode_guards(&guards)?;
            batch.put_cf(guards_cf, key, value);
        }

        // Write batch
        self.db.write(batch)?;

        // Save metadata
        self.save_metadata()?;

        Ok(())
    }

    /// Restore DSU from RocksDB, warming the cache with largest clusters
    pub fn restore(&mut self, warm_cache_roots: usize) -> Result<()> {
        // Load metadata
        self.load_metadata()?;

        // Optionally warm the cache with largest clusters
        if warm_cache_roots > 0 {
            self.warm_cache(warm_cache_roots)?;
        }

        Ok(())
    }

    /// Warm the cache by loading the most frequently accessed roots
    fn warm_cache(&mut self, max_roots: usize) -> Result<()> {
        // For now, just iterate and cache the first N entries
        // In production, we'd track access patterns or cluster sizes
        let parent_cf = self
            .db
            .cf_handle(self.cf_parent)
            .ok_or_else(|| anyhow!("missing DSU parent column family"))?;

        let mut loaded = 0;
        let iter = self.db.iterator_cf(parent_cf, rocksdb::IteratorMode::Start);

        for entry in iter {
            if loaded >= max_roots {
                break;
            }

            let (key, value) = entry?;
            if let (Some(record_id), Some(parent_id)) = (
                crate::persistence::dsu_encoding::decode_record_key(&key),
                crate::persistence::dsu_encoding::decode_parent_value(&value),
            ) {
                self.parent_cache.put(record_id, parent_id);
                loaded += 1;
            }
        }

        Ok(())
    }

    /// Get the number of clusters
    pub fn num_clusters(&self) -> usize {
        self.cluster_count.load(Ordering::SeqCst)
    }

    /// Get the current cluster count (non-mutating)
    pub fn cluster_count(&self) -> usize {
        self.cluster_count.load(Ordering::SeqCst)
    }

    /// Get cache statistics for monitoring
    pub fn cache_stats(&self) -> PersistentDSUStats {
        PersistentDSUStats {
            parent_cache_len: self.parent_cache.len(),
            parent_cache_cap: self.parent_cache.cap().get(),
            rank_cache_len: self.rank_cache.len(),
            rank_cache_cap: self.rank_cache.cap().get(),
            guards_cache_len: self.guards_cache.len(),
            guards_cache_cap: self.guards_cache.cap().get(),
            dirty_parents: self.dirty_parents.len(),
            dirty_ranks: self.dirty_ranks.len(),
            dirty_guards: self.dirty_guards.len(),
            hot_roots: self.hot_roots.len(),
            cluster_count: self.cluster_count.load(Ordering::SeqCst),
            next_cluster_id: self.next_cluster_id.load(Ordering::SeqCst),
        }
    }

    /// Mark a root as hot (should stay in cache)
    pub fn mark_hot_root(&mut self, root: RecordId, cluster_size: usize) {
        self.hot_roots.insert(root, cluster_size);
    }
}

/// Statistics for monitoring persistent DSU performance
#[derive(Debug, Clone)]
pub struct PersistentDSUStats {
    pub parent_cache_len: usize,
    pub parent_cache_cap: usize,
    pub rank_cache_len: usize,
    pub rank_cache_cap: usize,
    pub guards_cache_len: usize,
    pub guards_cache_cap: usize,
    pub dirty_parents: usize,
    pub dirty_ranks: usize,
    pub dirty_guards: usize,
    pub hot_roots: usize,
    pub cluster_count: usize,
    pub next_cluster_id: u32,
}

// ============================================================================
// DSU Backend Abstraction
// ============================================================================

/// Backend enum for DSU operations, allowing transparent use of either
/// in-memory or persistent implementations.
pub enum DsuBackend {
    /// In-memory DSU for smaller datasets (< 100M entities)
    InMemory(TemporalDSU),
    /// Persistent DSU for billion-scale datasets (boxed to reduce enum size)
    Persistent(Box<PersistentTemporalDSU>),
}

impl DsuBackend {
    /// Create an in-memory backend
    pub fn in_memory() -> Self {
        DsuBackend::InMemory(TemporalDSU::new())
    }

    /// Create a persistent backend
    pub fn persistent(db: Arc<DB>, config: PersistentDSUConfig) -> Result<Self> {
        Ok(DsuBackend::Persistent(Box::new(
            PersistentTemporalDSU::new(db, config)?,
        )))
    }

    /// Check if a record exists
    pub fn has_record(&mut self, record_id: RecordId) -> Result<bool> {
        match self {
            DsuBackend::InMemory(dsu) => Ok(dsu.has_record(record_id)),
            DsuBackend::Persistent(dsu) => dsu.has_record(record_id),
        }
    }

    /// Add a record to the DSU
    pub fn add_record(&mut self, record_id: RecordId) -> Result<()> {
        match self {
            DsuBackend::InMemory(dsu) => {
                dsu.add_record(record_id);
                Ok(())
            }
            DsuBackend::Persistent(dsu) => dsu.add_record(record_id),
        }
    }

    /// Find the root of a record
    pub fn find(&mut self, record_id: RecordId) -> Result<RecordId> {
        match self {
            DsuBackend::InMemory(dsu) => Ok(dsu.find(record_id)),
            DsuBackend::Persistent(dsu) => dsu.find(record_id),
        }
    }

    /// Check if two records are in the same cluster
    pub fn same_cluster(&mut self, a: RecordId, b: RecordId) -> Result<bool> {
        match self {
            DsuBackend::InMemory(dsu) => Ok(dsu.same_cluster(a, b)),
            DsuBackend::Persistent(dsu) => dsu.same_cluster(a, b),
        }
    }

    /// Attempt to merge two records with temporal validation
    pub fn try_merge(
        &mut self,
        a: RecordId,
        b: RecordId,
        guard: TemporalGuard,
    ) -> Result<MergeResult> {
        match self {
            DsuBackend::InMemory(dsu) => Ok(dsu.try_merge(a, b, guard)),
            DsuBackend::Persistent(dsu) => dsu.try_merge(a, b, guard),
        }
    }

    /// Get the current cluster count
    pub fn cluster_count(&self) -> usize {
        match self {
            DsuBackend::InMemory(dsu) => dsu.cluster_count(),
            DsuBackend::Persistent(dsu) => dsu.cluster_count(),
        }
    }

    /// Get all clusters (only available for in-memory DSU)
    /// For persistent DSU, this returns an error - use iterative methods instead
    pub fn get_clusters(&mut self) -> Result<Clusters> {
        match self {
            DsuBackend::InMemory(dsu) => Ok(dsu.get_clusters()),
            DsuBackend::Persistent(_) => Err(anyhow!(
                "get_clusters not supported for persistent DSU - use iterative methods"
            )),
        }
    }

    /// Flush dirty entries to disk (no-op for in-memory)
    pub fn flush(&mut self) -> Result<()> {
        match self {
            DsuBackend::InMemory(_) => Ok(()),
            DsuBackend::Persistent(dsu) => dsu.flush(),
        }
    }

    /// Check if using persistent backend
    pub fn is_persistent(&self) -> bool {
        matches!(self, DsuBackend::Persistent(_))
    }

    /// Get reference to in-memory DSU if available
    pub fn as_in_memory(&self) -> Option<&TemporalDSU> {
        match self {
            DsuBackend::InMemory(dsu) => Some(dsu),
            DsuBackend::Persistent(_) => None,
        }
    }

    /// Get mutable reference to in-memory DSU if available
    pub fn as_in_memory_mut(&mut self) -> Option<&mut TemporalDSU> {
        match self {
            DsuBackend::InMemory(dsu) => Some(dsu),
            DsuBackend::Persistent(_) => None,
        }
    }

    /// Get reference to persistent DSU if available
    pub fn as_persistent(&self) -> Option<&PersistentTemporalDSU> {
        match self {
            DsuBackend::InMemory(_) => None,
            DsuBackend::Persistent(dsu) => Some(dsu),
        }
    }

    /// Get mutable reference to persistent DSU if available
    pub fn as_persistent_mut(&mut self) -> Option<&mut PersistentTemporalDSU> {
        match self {
            DsuBackend::InMemory(_) => None,
            DsuBackend::Persistent(dsu) => Some(dsu),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporal::Interval;

    #[test]
    fn test_temporal_dsu_creation() {
        let mut dsu = TemporalDSU::new();
        assert_eq!(dsu.num_clusters(), 0);
    }

    #[test]
    fn test_add_record() {
        let mut dsu = TemporalDSU::new();
        let record_id = RecordId(1);

        dsu.add_record(record_id);
        assert_eq!(dsu.find(record_id), record_id);
    }

    #[test]
    fn test_merge_records() {
        let mut dsu = TemporalDSU::new();
        let record_a = RecordId(1);
        let record_b = RecordId(2);

        dsu.add_record(record_a);
        dsu.add_record(record_b);

        let guard = TemporalGuard::new(
            Interval::new(100, 200).unwrap(),
            "identity_key_match".to_string(),
        );

        let result = dsu.try_merge(record_a, record_b, guard);
        assert!(matches!(result, MergeResult::Success { .. }));
        assert!(dsu.same_cluster(record_a, record_b));
    }

    #[test]
    fn test_cluster_creation() {
        let mut dsu = TemporalDSU::new();
        let record_a = RecordId(1);
        let record_b = RecordId(2);

        dsu.add_record(record_a);
        dsu.add_record(record_b);

        let guard = TemporalGuard::new(
            Interval::new(100, 200).unwrap(),
            "identity_key_match".to_string(),
        );

        dsu.try_merge(record_a, record_b, guard);

        let clusters = dsu.get_clusters();
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters.clusters[0].records.len(), 2);
    }

    #[test]
    fn test_temporal_conflict() {
        let mut dsu = TemporalDSU::new();
        let record_a = RecordId(1);
        let record_b = RecordId(2);

        dsu.add_record(record_a);
        dsu.add_record(record_b);

        // Add conflicting guards
        let guard1 = TemporalGuard::new(
            Interval::new(100, 200).unwrap(),
            "identity_key_match".to_string(),
        );
        let guard2 = TemporalGuard::new(
            Interval::new(150, 250).unwrap(),
            "strong_identifier_mismatch".to_string(),
        );

        dsu.guards.get_mut(&record_a).unwrap().push(guard1);
        dsu.guards.get_mut(&record_b).unwrap().push(guard2);

        let guard = TemporalGuard::new(
            Interval::new(100, 200).unwrap(),
            "identity_key_match".to_string(),
        );

        let result = dsu.try_merge(record_a, record_b, guard);
        assert!(matches!(result, MergeResult::Blocked { .. }));
    }

    #[test]
    fn test_cluster_operations() {
        let mut clusters = Clusters::new();

        let cluster = Cluster::new(ClusterId(1), RecordId(1), vec![RecordId(1), RecordId(2)]);

        clusters.add_cluster(cluster);
        assert_eq!(clusters.len(), 1);

        let found_cluster = clusters.get_cluster(ClusterId(1));
        assert!(found_cluster.is_some());
        assert_eq!(found_cluster.unwrap().len(), 2);
    }

    #[test]
    fn test_union_find_functionality() {
        let mut dsu = TemporalDSU::new();

        // Add entity 1
        dsu.add_record(RecordId(1));
        assert_eq!(dsu.find(RecordId(1)), RecordId(1));
        assert_eq!(dsu.num_clusters(), 1);
        assert_eq!(dsu.rank[&RecordId(1)], 0);

        // Add entity 101 and union 1 with 101
        dsu.add_record(RecordId(101));
        let guard1 = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
        let result = dsu.try_merge(RecordId(1), RecordId(101), guard1);
        assert!(matches!(result, MergeResult::Success { .. }));

        let root1 = dsu.find(RecordId(1));
        let root101 = dsu.find(RecordId(101));
        assert_eq!(root1, root101);
        assert_eq!(dsu.num_clusters(), 1);
        assert_eq!(dsu.rank[&root1], 1);

        // Add entity 2
        dsu.add_record(RecordId(2));
        assert_eq!(dsu.find(RecordId(2)), RecordId(2));
        assert_eq!(dsu.num_clusters(), 2);
        assert_eq!(dsu.rank[&RecordId(2)], 0);

        // Union 2 with 101 (which is now in cluster 1)
        let guard2 = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
        let result = dsu.try_merge(RecordId(2), RecordId(101), guard2);
        assert!(matches!(result, MergeResult::Success { .. }));

        let root = dsu.find(RecordId(1));
        assert_eq!(dsu.find(RecordId(101)), root);
        assert_eq!(dsu.find(RecordId(2)), root);
        assert_eq!(dsu.num_clusters(), 1);
        assert_eq!(dsu.rank[&root], 1);

        // Add entity 102 and union 2 with 102
        dsu.add_record(RecordId(102));
        let guard3 = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
        let result = dsu.try_merge(RecordId(2), RecordId(102), guard3);
        assert!(matches!(result, MergeResult::Success { .. }));

        let root = dsu.find(RecordId(1));
        assert_eq!(dsu.find(RecordId(102)), root);
        assert_eq!(dsu.find(RecordId(2)), root);
        assert_eq!(dsu.num_clusters(), 1);
        assert_eq!(dsu.rank[&root], 1);

        // Add entity 3 and create a chain
        dsu.add_record(RecordId(3));
        assert_eq!(dsu.num_clusters(), 2);

        // Add entities and union 3 with multiple entities
        let entities_to_union = vec![103, 113, 123, 133, 143];
        for entity_id in entities_to_union {
            dsu.add_record(RecordId(entity_id));
            let guard =
                TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
            let result = dsu.try_merge(RecordId(3), RecordId(entity_id), guard);
            assert!(matches!(result, MergeResult::Success { .. }));
        }

        // Verify all entities in cluster 3
        let root3 = dsu.find(RecordId(3));
        assert_eq!(dsu.find(RecordId(103)), root3);
        assert_eq!(dsu.find(RecordId(113)), root3);
        assert_eq!(dsu.find(RecordId(123)), root3);
        assert_eq!(dsu.find(RecordId(133)), root3);
        assert_eq!(dsu.find(RecordId(143)), root3);
        // The rank should be 1 because we did multiple unions with equal ranks
        assert_eq!(dsu.rank[&root3], 1);

        // Union cluster 1 with cluster 3
        let guard4 = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
        let result = dsu.try_merge(RecordId(1), RecordId(103), guard4);
        assert!(matches!(result, MergeResult::Success { .. }));

        let final_root = dsu.find(RecordId(1));
        assert_eq!(dsu.num_clusters(), 1);
        // The rank should be 2 because we're merging two rank-1 clusters
        assert_eq!(dsu.rank[&final_root], 2);

        // Add entity 4 and create another cluster
        dsu.add_record(RecordId(4));
        assert_eq!(dsu.num_clusters(), 2);

        // Add entities and union 4 with multiple entities
        let entities_to_union = vec![104, 114, 124];
        for entity_id in entities_to_union {
            dsu.add_record(RecordId(entity_id));
            let guard =
                TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
            let result = dsu.try_merge(RecordId(4), RecordId(entity_id), guard);
            assert!(matches!(result, MergeResult::Success { .. }));
        }

        // Final verification
        assert_eq!(dsu.num_clusters(), 2);
    }

    #[test]
    fn test_union_find_path_compression() {
        let mut dsu = TemporalDSU::new();

        // Create a chain: 1 -> 2 -> 3 -> 4
        dsu.add_record(RecordId(1));
        dsu.add_record(RecordId(2));
        dsu.add_record(RecordId(3));
        dsu.add_record(RecordId(4));

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Union in sequence
        dsu.try_merge(RecordId(1), RecordId(2), guard.clone());
        dsu.try_merge(RecordId(2), RecordId(3), guard.clone());
        dsu.try_merge(RecordId(3), RecordId(4), guard.clone());

        // All should point to the same root
        let root = dsu.find(RecordId(1));
        assert_eq!(dsu.find(RecordId(2)), root);
        assert_eq!(dsu.find(RecordId(3)), root);
        assert_eq!(dsu.find(RecordId(4)), root);

        // After path compression, all should directly point to root
        assert_eq!(dsu.parent[&RecordId(1)], root);
        assert_eq!(dsu.parent[&RecordId(2)], root);
        assert_eq!(dsu.parent[&RecordId(3)], root);
        assert_eq!(dsu.parent[&RecordId(4)], root);
    }

    #[test]
    fn test_union_find_rank_optimization() {
        let mut dsu = TemporalDSU::new();

        // Create two trees with different ranks
        dsu.add_record(RecordId(1));
        dsu.add_record(RecordId(2));
        dsu.add_record(RecordId(3));
        dsu.add_record(RecordId(4));

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Create tree 1: 1 -> 2
        dsu.try_merge(RecordId(1), RecordId(2), guard.clone());
        let root1 = dsu.find(RecordId(1));
        assert_eq!(dsu.rank[&root1], 1);

        // Create tree 2: 3 -> 4
        dsu.try_merge(RecordId(3), RecordId(4), guard.clone());
        let root3 = dsu.find(RecordId(3));
        assert_eq!(dsu.rank[&root3], 1);

        // Union the two trees - should attach smaller rank to larger rank
        dsu.try_merge(RecordId(1), RecordId(3), guard.clone());

        // Both should point to the same root
        let root1 = dsu.find(RecordId(1));
        let root3 = dsu.find(RecordId(3));
        assert_eq!(root1, root3);

        // The root should have rank 2 (incremented since ranks were equal)
        let root_rank = dsu.rank[&root1];
        assert_eq!(root_rank, 2);
    }

    #[test]
    fn test_union_find_same_cluster() {
        let mut dsu = TemporalDSU::new();

        dsu.add_record(RecordId(1));
        dsu.add_record(RecordId(2));
        dsu.add_record(RecordId(3));

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Initially, all records are in different clusters
        assert!(!dsu.same_cluster(RecordId(1), RecordId(2)));
        assert!(!dsu.same_cluster(RecordId(1), RecordId(3)));
        assert!(!dsu.same_cluster(RecordId(2), RecordId(3)));

        // Union 1 and 2
        dsu.try_merge(RecordId(1), RecordId(2), guard.clone());
        assert!(dsu.same_cluster(RecordId(1), RecordId(2)));
        assert!(!dsu.same_cluster(RecordId(1), RecordId(3)));
        assert!(!dsu.same_cluster(RecordId(2), RecordId(3)));

        // Union 2 and 3
        dsu.try_merge(RecordId(2), RecordId(3), guard.clone());
        assert!(dsu.same_cluster(RecordId(1), RecordId(2)));
        assert!(dsu.same_cluster(RecordId(1), RecordId(3)));
        assert!(dsu.same_cluster(RecordId(2), RecordId(3)));
    }

    #[test]
    fn test_union_find_cluster_count() {
        let mut dsu = TemporalDSU::new();

        // Start with 5 separate records
        for i in 1..=5 {
            dsu.add_record(RecordId(i));
        }
        assert_eq!(dsu.num_clusters(), 5);

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Union 1 and 2
        dsu.try_merge(RecordId(1), RecordId(2), guard.clone());
        assert_eq!(dsu.num_clusters(), 4);

        // Union 3 and 4
        dsu.try_merge(RecordId(3), RecordId(4), guard.clone());
        assert_eq!(dsu.num_clusters(), 3);

        // Union 1 and 3 (merges two clusters)
        dsu.try_merge(RecordId(1), RecordId(3), guard.clone());
        assert_eq!(dsu.num_clusters(), 2);

        // Union 5 with the big cluster
        dsu.try_merge(RecordId(1), RecordId(5), guard.clone());
        assert_eq!(dsu.num_clusters(), 1);
    }

    #[test]
    fn test_union_find_get_clusters() {
        let mut dsu = TemporalDSU::new();

        dsu.add_record(RecordId(1));
        dsu.add_record(RecordId(2));
        dsu.add_record(RecordId(3));
        dsu.add_record(RecordId(4));

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Create two clusters: {1, 2} and {3, 4}
        dsu.try_merge(RecordId(1), RecordId(2), guard.clone());
        dsu.try_merge(RecordId(3), RecordId(4), guard.clone());

        let clusters = dsu.get_clusters();
        assert_eq!(clusters.len(), 2);

        // Each cluster should have 2 records
        for cluster in &clusters.clusters {
            assert_eq!(cluster.records.len(), 2);
        }

        // Verify all records are accounted for
        let mut all_records: Vec<RecordId> = clusters
            .clusters
            .iter()
            .flat_map(|c| c.records.iter())
            .cloned()
            .collect();
        all_records.sort();

        let expected = vec![RecordId(1), RecordId(2), RecordId(3), RecordId(4)];
        assert_eq!(all_records, expected);
    }

    #[test]
    fn test_union_find_self_union() {
        let mut dsu = TemporalDSU::new();

        dsu.add_record(RecordId(1));

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Union a record with itself should be a no-op
        let result = dsu.try_merge(RecordId(1), RecordId(1), guard);
        assert!(matches!(result, MergeResult::Success { .. }));

        assert_eq!(dsu.find(RecordId(1)), RecordId(1));
        assert_eq!(dsu.num_clusters(), 1);
    }

    #[test]
    fn test_union_find_guards_tracking() {
        let mut dsu = TemporalDSU::new();

        dsu.add_record(RecordId(1));
        dsu.add_record(RecordId(2));

        let guard1 = TemporalGuard::new(Interval::new(0, 100).unwrap(), "guard1".to_string());
        let guard2 = TemporalGuard::new(Interval::new(100, 200).unwrap(), "guard2".to_string());

        // Union with first guard
        dsu.try_merge(RecordId(1), RecordId(2), guard1.clone());

        // Add another guard to record 1
        dsu.guards
            .get_mut(&RecordId(1))
            .unwrap()
            .push(guard2.clone());

        // Check guards are tracked
        let guards_1 = dsu.get_guards(RecordId(1));
        let guards_2 = dsu.get_guards(RecordId(2));

        assert_eq!(guards_1.len(), 2);
        assert_eq!(guards_2.len(), 1);

        assert_eq!(guards_1[0].reason, "guard1");
        assert_eq!(guards_1[1].reason, "guard2");
        assert_eq!(guards_2[0].reason, "guard1");
    }
}

#[cfg(test)]
mod persistent_tests {
    use super::*;
    use crate::persistence::PersistentStore;
    use crate::temporal::Interval;
    use tempfile::tempdir;

    #[allow(dead_code)]
    fn create_test_persistent_dsu() -> (PersistentTemporalDSU, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path();

        // Create a PersistentStore to ensure the DB is properly initialized
        let store = PersistentStore::open(path).unwrap();
        let db = Arc::new(unsafe { std::ptr::read(store.db() as *const _) });
        std::mem::forget(store);

        let config = PersistentDSUConfig {
            parent_cache_size: 1000,
            rank_cache_size: 1000,
            guards_cache_size: 1000,
            dirty_buffer_size: 100,
            persist_path_compression: true,
        };

        let dsu = PersistentTemporalDSU::new(db, config).unwrap();
        (dsu, dir)
    }

    #[test]
    fn test_persistent_dsu_add_and_find() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        // Create a PersistentStore first to set up all column families
        let store = PersistentStore::open(path).unwrap();
        let db = Arc::new(unsafe { std::ptr::read(store.db() as *const _) });
        std::mem::forget(store);

        let config = PersistentDSUConfig {
            parent_cache_size: 1000,
            rank_cache_size: 1000,
            guards_cache_size: 1000,
            dirty_buffer_size: 100,
            persist_path_compression: true,
        };

        let mut dsu = PersistentTemporalDSU::new(db, config).unwrap();

        // Add a record
        dsu.add_record(RecordId(1)).unwrap();
        assert!(dsu.has_record(RecordId(1)).unwrap());

        // Find should return self for a new record
        assert_eq!(dsu.find(RecordId(1)).unwrap(), RecordId(1));
        assert_eq!(dsu.num_clusters(), 1);
    }

    #[test]
    fn test_persistent_dsu_merge() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let store = PersistentStore::open(path).unwrap();
        let db = Arc::new(unsafe { std::ptr::read(store.db() as *const _) });
        std::mem::forget(store);

        let config = PersistentDSUConfig::default();
        let mut dsu = PersistentTemporalDSU::new(db, config).unwrap();

        // Add records
        dsu.add_record(RecordId(1)).unwrap();
        dsu.add_record(RecordId(2)).unwrap();
        assert_eq!(dsu.num_clusters(), 2);

        // Merge them
        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test".to_string());
        let result = dsu.try_merge(RecordId(1), RecordId(2), guard).unwrap();
        assert!(matches!(result, MergeResult::Success { .. }));

        // Should be in same cluster now
        assert!(dsu.same_cluster(RecordId(1), RecordId(2)).unwrap());
        assert_eq!(dsu.num_clusters(), 1);
    }

    #[test]
    fn test_persistent_dsu_flush_and_restore() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        // Create initial DSU and add records
        {
            let store = PersistentStore::open(path).unwrap();
            let db = Arc::new(unsafe { std::ptr::read(store.db() as *const _) });
            std::mem::forget(store);

            let config = PersistentDSUConfig {
                parent_cache_size: 100,
                rank_cache_size: 100,
                guards_cache_size: 100,
                dirty_buffer_size: 10,
                persist_path_compression: true,
            };

            let mut dsu = PersistentTemporalDSU::new(db, config).unwrap();

            // Add and merge records
            dsu.add_record(RecordId(1)).unwrap();
            dsu.add_record(RecordId(2)).unwrap();
            dsu.add_record(RecordId(3)).unwrap();

            let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test".to_string());
            dsu.try_merge(RecordId(1), RecordId(2), guard.clone())
                .unwrap();
            dsu.try_merge(RecordId(2), RecordId(3), guard).unwrap();

            // Flush to disk
            dsu.flush().unwrap();
        }

        // Reopen and verify state was persisted
        {
            let store = PersistentStore::open(path).unwrap();
            let db = Arc::new(unsafe { std::ptr::read(store.db() as *const _) });
            std::mem::forget(store);

            let config = PersistentDSUConfig::default();
            let mut dsu = PersistentTemporalDSU::new(db, config).unwrap();

            // Restore and warm cache
            dsu.restore(100).unwrap();

            // Verify records exist
            assert!(dsu.has_record(RecordId(1)).unwrap());
            assert!(dsu.has_record(RecordId(2)).unwrap());
            assert!(dsu.has_record(RecordId(3)).unwrap());

            // Verify they're still in the same cluster
            assert!(dsu.same_cluster(RecordId(1), RecordId(2)).unwrap());
            assert!(dsu.same_cluster(RecordId(1), RecordId(3)).unwrap());
        }
    }

    #[test]
    fn test_persistent_dsu_cache_stats() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let store = PersistentStore::open(path).unwrap();
        let db = Arc::new(unsafe { std::ptr::read(store.db() as *const _) });
        std::mem::forget(store);

        let config = PersistentDSUConfig {
            parent_cache_size: 1000,
            rank_cache_size: 500,
            guards_cache_size: 200,
            dirty_buffer_size: 50,
            persist_path_compression: false,
        };

        let mut dsu = PersistentTemporalDSU::new(db, config).unwrap();

        // Add some records
        for i in 0..10 {
            dsu.add_record(RecordId(i)).unwrap();
        }

        let stats = dsu.cache_stats();
        assert_eq!(stats.parent_cache_cap, 1000);
        assert_eq!(stats.rank_cache_cap, 500);
        assert_eq!(stats.guards_cache_cap, 200);
        assert!(stats.parent_cache_len > 0);
        assert_eq!(stats.cluster_count, 10);
    }

    #[test]
    fn test_dsu_backend_in_memory() {
        let mut backend = DsuBackend::in_memory();

        // Add records
        backend.add_record(RecordId(1)).unwrap();
        backend.add_record(RecordId(2)).unwrap();
        assert!(backend.has_record(RecordId(1)).unwrap());
        assert!(!backend.is_persistent());

        // Merge
        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test".to_string());
        let result = backend.try_merge(RecordId(1), RecordId(2), guard).unwrap();
        assert!(matches!(result, MergeResult::Success { .. }));

        // Same cluster
        assert!(backend.same_cluster(RecordId(1), RecordId(2)).unwrap());
        assert_eq!(backend.cluster_count(), 1);

        // Get clusters works for in-memory
        let clusters = backend.get_clusters().unwrap();
        assert_eq!(clusters.len(), 1);
    }

    #[test]
    fn test_dsu_backend_persistent() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        let store = PersistentStore::open(path).unwrap();
        let db = Arc::new(unsafe { std::ptr::read(store.db() as *const _) });
        std::mem::forget(store);

        let config = PersistentDSUConfig::default();
        let mut backend = DsuBackend::persistent(db, config).unwrap();

        // Add records
        backend.add_record(RecordId(1)).unwrap();
        backend.add_record(RecordId(2)).unwrap();
        assert!(backend.has_record(RecordId(1)).unwrap());
        assert!(backend.is_persistent());

        // Merge
        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test".to_string());
        let result = backend.try_merge(RecordId(1), RecordId(2), guard).unwrap();
        assert!(matches!(result, MergeResult::Success { .. }));

        // Same cluster
        assert!(backend.same_cluster(RecordId(1), RecordId(2)).unwrap());
        assert_eq!(backend.cluster_count(), 1);

        // get_clusters should error for persistent
        assert!(backend.get_clusters().is_err());

        // Flush
        backend.flush().unwrap();
    }
}
