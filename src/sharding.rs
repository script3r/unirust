//! Sharded ingestion for parallel streaming.

use crate::linker::StreamingLinker;
use crate::model::ClusterId;
use crate::model::StringInterner;
use crate::model::{Descriptor, GlobalClusterId, KeyValue, Record, RecordId};
use crate::ontology::Ontology;
use crate::store::Store;
use crate::temporal::{is_overlapping, Interval};
use crate::ClusterAssignment;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// A signature of identity key values for fast lookup.
/// This is a 32-byte hash of the identity key's attribute-value pairs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdentityKeySignature(pub [u8; 32]);

impl IdentityKeySignature {
    /// Create a new signature from identity key values.
    pub fn from_key_values(entity_type: &str, key_values: &[KeyValue]) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Use two hashers to fill 32 bytes
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();

        entity_type.hash(&mut hasher1);
        entity_type.hash(&mut hasher2);

        for (i, kv) in key_values.iter().enumerate() {
            kv.hash(&mut hasher1);
            // Mix in index for hasher2 to get different bits
            (kv, i).hash(&mut hasher2);
        }

        let h1 = hasher1.finish().to_le_bytes();
        let h2 = hasher2.finish().to_le_bytes();

        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&h1);
        bytes[8..16].copy_from_slice(&h2);
        // Fill remaining with XOR variations
        for i in 0..8 {
            bytes[16 + i] = h1[i] ^ h2[7 - i];
            bytes[24 + i] = h1[7 - i].wrapping_add(h2[i]);
        }

        Self(bytes)
    }

    /// Convert to bytes for storage.
    pub fn to_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Create from stored bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Simple bloom filter for fast negative lookups.
/// Uses 16MB of memory by default (~128M bits).
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: usize,
}

impl BloomFilter {
    /// Create a new bloom filter with the specified size in bytes.
    pub fn new(size_bytes: usize) -> Self {
        let num_u64s = size_bytes / 8;
        Self {
            bits: vec![0u64; num_u64s.max(1)],
            num_hashes: 7, // Optimal for ~1% false positive rate
        }
    }

    /// Create a 16MB bloom filter (default for shard boundaries).
    pub fn new_16mb() -> Self {
        Self::new(16 * 1024 * 1024)
    }

    /// Create a smaller 1MB bloom filter.
    pub fn new_1mb() -> Self {
        Self::new(1024 * 1024)
    }

    /// Insert a key into the bloom filter.
    pub fn insert(&mut self, key: &IdentityKeySignature) {
        let bit_count = self.bits.len() * 64;
        for i in 0..self.num_hashes {
            let hash = self.hash_with_seed(key, i);
            let bit_index = (hash as usize) % bit_count;
            let word_index = bit_index / 64;
            let bit_offset = bit_index % 64;
            self.bits[word_index] |= 1u64 << bit_offset;
        }
    }

    /// Check if a key might be in the bloom filter.
    /// Returns true if the key might be present, false if definitely not.
    pub fn may_contain(&self, key: &IdentityKeySignature) -> bool {
        let bit_count = self.bits.len() * 64;
        for i in 0..self.num_hashes {
            let hash = self.hash_with_seed(key, i);
            let bit_index = (hash as usize) % bit_count;
            let word_index = bit_index / 64;
            let bit_offset = bit_index % 64;
            if (self.bits[word_index] & (1u64 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }

    /// Clear all bits.
    pub fn clear(&mut self) {
        for word in &mut self.bits {
            *word = 0;
        }
    }

    fn hash_with_seed(&self, key: &IdentityKeySignature, seed: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.0.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish()
    }
}

/// Entry in the cluster boundary index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundaryEntry {
    pub cluster_id: GlobalClusterId,
    pub interval: Interval,
    pub shard_id: u16,
    /// Strong ID hashes per perspective for cross-shard conflict detection.
    /// Key: perspective name hash, Value: hash of (attr_id, value_id) tuples.
    /// Two clusters conflict if they share a perspective key but have different values.
    #[serde(default)]
    pub perspective_strong_ids: HashMap<u64, u64>,
}

/// Index tracking identity keys at shard boundaries.
///
/// This enables incremental reconciliation by tracking which clusters
/// share identity keys across shards. When a new record arrives with
/// a matching boundary key, we can directly merge clusters without
/// loading all records.
#[derive(Debug)]
pub struct ClusterBoundaryIndex {
    /// Map from identity key signature to boundary entries.
    boundary_keys: HashMap<IdentityKeySignature, Vec<BoundaryEntry>>,
    /// Bloom filter for fast negative lookups.
    key_bloom: BloomFilter,
    /// Version number for cache invalidation.
    version: u64,
    /// This shard's ID.
    shard_id: u16,
    /// Keys that have been modified since last reconciliation.
    /// Used for incremental/adaptive reconciliation.
    dirty_keys: HashSet<IdentityKeySignature>,
}

impl ClusterBoundaryIndex {
    /// Create a new boundary index for a shard.
    pub fn new(shard_id: u16) -> Self {
        Self {
            boundary_keys: HashMap::new(),
            key_bloom: BloomFilter::new_16mb(),
            version: 0,
            shard_id,
            dirty_keys: HashSet::new(),
        }
    }

    /// Create a boundary index with a smaller bloom filter (for testing).
    pub fn new_small(shard_id: u16) -> Self {
        Self {
            boundary_keys: HashMap::new(),
            key_bloom: BloomFilter::new_1mb(),
            version: 0,
            shard_id,
            dirty_keys: HashSet::new(),
        }
    }

    /// Register a cluster's identity key at a shard boundary.
    pub fn register_boundary_key(
        &mut self,
        signature: IdentityKeySignature,
        cluster_id: GlobalClusterId,
        interval: Interval,
    ) {
        self.register_boundary_key_with_strong_ids(signature, cluster_id, interval, HashMap::new());
    }

    /// Register a cluster's identity key with strong ID hashes for conflict detection.
    pub fn register_boundary_key_with_strong_ids(
        &mut self,
        signature: IdentityKeySignature,
        cluster_id: GlobalClusterId,
        interval: Interval,
        perspective_strong_ids: HashMap<u64, u64>,
    ) {
        self.key_bloom.insert(&signature);

        let entry = BoundaryEntry {
            cluster_id,
            interval,
            shard_id: self.shard_id,
            perspective_strong_ids,
        };

        self.boundary_keys.entry(signature).or_default().push(entry);

        // Track this key as dirty for incremental reconciliation
        self.dirty_keys.insert(signature);

        self.version += 1;
    }

    /// Get the set of dirty keys (modified since last reconciliation).
    pub fn dirty_keys(&self) -> &HashSet<IdentityKeySignature> {
        &self.dirty_keys
    }

    /// Get dirty key count.
    pub fn dirty_key_count(&self) -> usize {
        self.dirty_keys.len()
    }

    /// Take and clear dirty keys, returning ownership.
    pub fn take_dirty_keys(&mut self) -> HashSet<IdentityKeySignature> {
        std::mem::take(&mut self.dirty_keys)
    }

    /// Clear specific keys from the dirty set (after reconciliation).
    pub fn clear_dirty_keys(&mut self, keys: &[IdentityKeySignature]) {
        for key in keys {
            self.dirty_keys.remove(key);
        }
    }

    /// Check if a key might have boundary entries (fast bloom check).
    pub fn may_have_boundary(&self, signature: &IdentityKeySignature) -> bool {
        self.key_bloom.may_contain(signature)
    }

    /// Get boundary entries for a signature.
    pub fn get_boundaries(&self, signature: &IdentityKeySignature) -> Option<&[BoundaryEntry]> {
        self.boundary_keys.get(signature).map(|v| v.as_slice())
    }

    /// Find all boundary entries that overlap with a given interval.
    pub fn find_overlapping_boundaries(
        &self,
        signature: &IdentityKeySignature,
        interval: &Interval,
    ) -> Vec<&BoundaryEntry> {
        if !self.may_have_boundary(signature) {
            return Vec::new();
        }

        self.boundary_keys
            .get(signature)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|e| is_overlapping(&e.interval, interval))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Update a cluster ID after a merge.
    pub fn update_cluster_id(
        &mut self,
        signature: &IdentityKeySignature,
        old_id: GlobalClusterId,
        new_id: GlobalClusterId,
    ) {
        if let Some(entries) = self.boundary_keys.get_mut(signature) {
            for entry in entries {
                if entry.cluster_id == old_id {
                    entry.cluster_id = new_id;
                }
            }
            self.version += 1;
        }
    }

    /// Get the current version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Get the number of registered boundary keys.
    pub fn len(&self) -> usize {
        self.boundary_keys.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.boundary_keys.is_empty()
    }

    /// Clear all boundary data.
    pub fn clear(&mut self) {
        self.boundary_keys.clear();
        self.key_bloom.clear();
        self.version += 1;
    }

    /// Export boundary metadata for sharing with other shards.
    pub fn export_metadata(&self) -> BoundaryMetadata {
        BoundaryMetadata {
            shard_id: self.shard_id,
            version: self.version,
            entries: self
                .boundary_keys
                .iter()
                .map(|(sig, entries)| (*sig, entries.clone()))
                .collect(),
        }
    }

    /// Import boundary metadata from another shard.
    pub fn import_metadata(&mut self, metadata: &BoundaryMetadata) {
        for (sig, entries) in &metadata.entries {
            self.key_bloom.insert(sig);
            self.boundary_keys
                .entry(*sig)
                .or_default()
                .extend(entries.iter().cloned());
        }
        self.version += 1;
    }

    /// Get the shard ID for this boundary index.
    pub fn shard_id(&self) -> u16 {
        self.shard_id
    }

    /// Merge boundary entries from another index.
    pub fn merge_from(&mut self, other: &ClusterBoundaryIndex) {
        for (sig, entries) in &other.boundary_keys {
            self.key_bloom.insert(sig);
            self.boundary_keys
                .entry(*sig)
                .or_default()
                .extend(entries.iter().cloned());
        }
        self.dirty_keys.extend(other.dirty_keys.iter().copied());
        self.version += 1;
    }
}

/// Serializable boundary metadata for cross-shard exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundaryMetadata {
    pub shard_id: u16,
    pub version: u64,
    pub entries: Vec<(IdentityKeySignature, Vec<BoundaryEntry>)>,
}

/// Result of incremental reconciliation.
#[derive(Debug, Default)]
pub struct ReconciliationResult {
    /// Number of cross-shard merges performed.
    pub merges_performed: usize,
    /// Clusters that were merged.
    pub merged_clusters: Vec<(GlobalClusterId, GlobalClusterId)>,
    /// Number of boundary keys checked.
    pub keys_checked: usize,
    /// Number of keys that matched across shards.
    pub keys_matched: usize,
    /// Number of potential merges evaluated (before conflict check).
    pub merge_candidates: usize,
    /// Number of merges blocked due to cross-shard conflicts.
    pub conflicts_blocked: usize,
}

/// Incremental reconciler for cross-shard cluster merges.
///
/// Instead of loading all records from all shards (O(n)),
/// this uses boundary indices to find and merge only clusters
/// that share identity keys (O(k) where k = boundary keys).
#[derive(Debug)]
pub struct IncrementalReconciler {
    /// Boundary indices from all shards.
    shard_boundaries: Vec<ClusterBoundaryIndex>,
    /// Pending merges to apply.
    pending_merges: Vec<(GlobalClusterId, GlobalClusterId)>,
}

impl IncrementalReconciler {
    /// Create a new reconciler.
    pub fn new() -> Self {
        Self {
            shard_boundaries: Vec::new(),
            pending_merges: Vec::new(),
        }
    }

    /// Add a shard's boundary index.
    pub fn add_shard_boundary(&mut self, boundary: ClusterBoundaryIndex) {
        self.shard_boundaries.push(boundary);
    }

    /// Find clusters that need to be merged based on shared identity keys.
    pub fn find_cross_shard_merges(&self) -> Vec<(GlobalClusterId, GlobalClusterId)> {
        self.find_cross_shard_merges_with_stats().0
    }

    /// Find clusters that need to be merged, returning stats about the process.
    /// Returns (merges, merge_candidates, conflicts_blocked).
    pub fn find_cross_shard_merges_with_stats(
        &self,
    ) -> (Vec<(GlobalClusterId, GlobalClusterId)>, usize, usize) {
        let mut merges = Vec::new();
        let mut merge_candidates = 0;
        let mut conflicts_blocked = 0;

        // Build a map of all signatures to their entries across all shards
        let mut all_entries: HashMap<IdentityKeySignature, Vec<&BoundaryEntry>> = HashMap::new();

        for boundary in &self.shard_boundaries {
            for (sig, entries) in &boundary.boundary_keys {
                all_entries.entry(*sig).or_default().extend(entries.iter());
            }
        }

        // Find signatures that appear in multiple shards
        for (_sig, entries) in all_entries {
            if entries.len() < 2 {
                continue;
            }

            // Group by shard and find overlapping intervals
            let mut shard_entries: HashMap<u16, Vec<&BoundaryEntry>> = HashMap::new();
            for entry in &entries {
                shard_entries.entry(entry.shard_id).or_default().push(entry);
            }

            // If entries exist in multiple shards, check for temporal overlap
            if shard_entries.len() > 1 {
                let entries_vec: Vec<_> = entries.iter().collect();
                for i in 0..entries_vec.len() {
                    for j in (i + 1)..entries_vec.len() {
                        let e1 = entries_vec[i];
                        let e2 = entries_vec[j];

                        // Only merge if from different shards and intervals overlap
                        if e1.shard_id != e2.shard_id && is_overlapping(&e1.interval, &e2.interval)
                        {
                            merge_candidates += 1;

                            // Check for same-perspective conflicts before proposing merge
                            if has_cross_shard_conflict(e1, e2) {
                                // Skip this merge - clusters conflict on strong IDs
                                conflicts_blocked += 1;
                                continue;
                            }

                            // Merge into the lower shard_id for consistency
                            let (primary, secondary) = if e1.shard_id < e2.shard_id {
                                (e1.cluster_id, e2.cluster_id)
                            } else {
                                (e2.cluster_id, e1.cluster_id)
                            };
                            merges.push((primary, secondary));
                        }
                    }
                }
            }
        }

        (merges, merge_candidates, conflicts_blocked)
    }

    /// Perform incremental reconciliation.
    ///
    /// Returns the reconciliation result without loading full records.
    pub fn reconcile(&mut self) -> ReconciliationResult {
        let (merges, merge_candidates, conflicts_blocked) =
            self.find_cross_shard_merges_with_stats();

        let mut result = ReconciliationResult {
            merges_performed: merges.len(),
            merged_clusters: merges.clone(),
            keys_checked: self
                .shard_boundaries
                .iter()
                .map(|b| b.boundary_keys.len())
                .sum(),
            keys_matched: 0,
            merge_candidates,
            conflicts_blocked,
        };

        // Count matched keys
        let mut seen_sigs: HashMap<IdentityKeySignature, usize> = HashMap::new();
        for boundary in &self.shard_boundaries {
            for sig in boundary.boundary_keys.keys() {
                *seen_sigs.entry(*sig).or_insert(0) += 1;
            }
        }
        result.keys_matched = seen_sigs.values().filter(|&&count| count > 1).count();

        // Store pending merges
        self.pending_merges = merges;

        result
    }

    /// Get pending merges.
    pub fn pending_merges(&self) -> &[(GlobalClusterId, GlobalClusterId)] {
        &self.pending_merges
    }

    /// Clear pending merges after they've been applied.
    pub fn clear_pending(&mut self) {
        self.pending_merges.clear();
    }

    /// Perform targeted reconciliation for specific dirty keys only.
    ///
    /// This is more efficient than full reconciliation when only a subset
    /// of keys have changed. Returns the reconciliation result.
    pub fn reconcile_keys(&mut self, keys: &HashSet<IdentityKeySignature>) -> ReconciliationResult {
        if keys.is_empty() {
            return ReconciliationResult::default();
        }

        let mut merges = Vec::new();
        let mut merge_candidates = 0;
        let mut conflicts_blocked = 0;
        let mut keys_matched = 0;

        // For each dirty key, collect entries from all shards
        for sig in keys {
            let mut entries: Vec<&BoundaryEntry> = Vec::new();

            for boundary in &self.shard_boundaries {
                if let Some(boundary_entries) = boundary.boundary_keys.get(sig) {
                    entries.extend(boundary_entries.iter());
                }
            }

            if entries.len() < 2 {
                continue;
            }

            // Count as matched if entries from multiple shards
            let shard_count: HashSet<_> = entries.iter().map(|e| e.shard_id).collect();
            if shard_count.len() > 1 {
                keys_matched += 1;
            }

            // Check all pairs for cross-shard merges
            for i in 0..entries.len() {
                for j in (i + 1)..entries.len() {
                    let e1 = entries[i];
                    let e2 = entries[j];

                    // Only merge if from different shards and intervals overlap
                    if e1.shard_id != e2.shard_id && is_overlapping(&e1.interval, &e2.interval) {
                        merge_candidates += 1;

                        // Check for same-perspective conflicts before proposing merge
                        if has_cross_shard_conflict(e1, e2) {
                            conflicts_blocked += 1;
                            continue;
                        }

                        // Merge into the lower shard_id for consistency
                        let (primary, secondary) = if e1.shard_id < e2.shard_id {
                            (e1.cluster_id, e2.cluster_id)
                        } else {
                            (e2.cluster_id, e1.cluster_id)
                        };
                        merges.push((primary, secondary));
                    }
                }
            }
        }

        self.pending_merges = merges.clone();

        ReconciliationResult {
            merges_performed: merges.len(),
            merged_clusters: merges,
            keys_checked: keys.len(),
            keys_matched,
            merge_candidates,
            conflicts_blocked,
        }
    }

    /// Collect all dirty keys from all shard boundaries.
    pub fn collect_all_dirty_keys(&self) -> HashSet<IdentityKeySignature> {
        let mut all_dirty = HashSet::new();
        for boundary in &self.shard_boundaries {
            all_dirty.extend(boundary.dirty_keys.iter().copied());
        }
        all_dirty
    }
}

impl Default for IncrementalReconciler {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if two boundary entries have conflicting strong IDs.
///
/// Two entries conflict if they share a perspective (same key in perspective_strong_ids)
/// but have different strong ID hashes (different values). This indicates the clusters
/// have the same perspective but different strong identifiers, which would create a
/// conflict if merged.
fn has_cross_shard_conflict(e1: &BoundaryEntry, e2: &BoundaryEntry) -> bool {
    // Check each perspective in e1 against e2
    for (perspective_hash, strong_id_hash_1) in &e1.perspective_strong_ids {
        if let Some(strong_id_hash_2) = e2.perspective_strong_ids.get(perspective_hash) {
            // Same perspective exists in both entries
            // If strong ID hashes differ, there's a conflict
            if strong_id_hash_1 != strong_id_hash_2 {
                return true;
            }
        }
    }
    false
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardedClusterAssignment {
    pub shard_id: usize,
    pub record_id: RecordId,
    pub cluster_id: ClusterId,
}

pub struct ShardedStreamEngine {
    shards: Vec<ShardState>,
    next_sequence: usize,
}

struct ShardState {
    store: Store,
    streamer: StreamingLinker,
    ontology: Ontology,
    record_sequence: std::collections::HashMap<RecordId, usize>,
}

impl ShardedStreamEngine {
    pub fn new(ontology: Ontology, shard_count: usize) -> Result<Self> {
        Self::new_with_tuning(ontology, shard_count, crate::StreamingTuning::default())
    }

    pub fn new_with_tuning(
        ontology: Ontology,
        shard_count: usize,
        tuning: crate::StreamingTuning,
    ) -> Result<Self> {
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let store = Store::new();
            let shard_ontology = ontology.clone();
            let streamer = StreamingLinker::new(&store, &shard_ontology, &tuning)?;
            shards.push(ShardState {
                store,
                streamer,
                ontology: shard_ontology,
                record_sequence: std::collections::HashMap::new(),
            });
        }
        Ok(Self {
            shards,
            next_sequence: 0,
        })
    }

    pub fn seed_interners(&mut self, interner: &StringInterner) {
        let mut attrs = interner
            .attr_ids()
            .filter_map(|id| interner.get_attr(id).cloned().map(|name| (id, name)))
            .collect::<Vec<_>>();
        attrs.sort_by_key(|(id, _)| id.0);

        let mut values = interner
            .value_ids()
            .filter_map(|id| interner.get_value(id).cloned().map(|value| (id, value)))
            .collect::<Vec<_>>();
        values.sort_by_key(|(id, _)| id.0);

        for shard in &mut self.shards {
            let shard_interner = shard.store.interner_mut();
            for (_, attr) in &attrs {
                shard_interner.intern_attr(attr);
            }
            for (_, value) in &values {
                shard_interner.intern_value(value);
            }
        }
    }

    pub fn stream_records(
        &mut self,
        records: Vec<Record>,
    ) -> Result<Vec<ShardedClusterAssignment>> {
        if self.shards.is_empty() {
            return Ok(Vec::new());
        }

        let mut buckets: Vec<Vec<(Record, usize)>> = vec![Vec::new(); self.shards.len()];
        for record in records {
            let sequence = self.next_sequence;
            self.next_sequence = self.next_sequence.saturating_add(1);
            let shard_id = shard_for_record(&record, &self.shards[0].ontology, self.shards.len());
            buckets[shard_id].push((record, sequence));
        }

        let shard_count = self.shards.len();
        let shards = std::mem::take(&mut self.shards);
        let mut handles = Vec::with_capacity(shard_count);
        for ((idx, shard), bucket) in shards.into_iter().enumerate().zip(buckets.into_iter()) {
            handles.push(std::thread::spawn(
                move || -> Result<(usize, ShardState, Vec<ShardedClusterAssignment>)> {
                    let mut shard = shard;
                    let assignments = shard.ingest(bucket)?;
                    let mapped = assignments
                        .into_iter()
                        .map(|assignment| ShardedClusterAssignment {
                            shard_id: idx,
                            record_id: assignment.record_id,
                            cluster_id: assignment.cluster_id,
                        })
                        .collect::<Vec<_>>();
                    Ok((idx, shard, mapped))
                },
            ));
        }

        let mut new_shards: Vec<Option<ShardState>> = (0..shard_count).map(|_| None).collect();
        let mut results = Vec::new();
        for handle in handles {
            let (idx, shard, assignments) = handle.join().expect("shard thread panicked")?;
            new_shards[idx] = Some(shard);
            results.extend(assignments);
        }

        self.shards = new_shards
            .into_iter()
            .map(|shard| shard.expect("missing shard state"))
            .collect();

        Ok(results)
    }

    pub fn reconcile_clusters(&self) -> Result<crate::dsu::Clusters> {
        let (_store, clusters) = self.reconcile_store_and_clusters()?;
        Ok(clusters)
    }

    pub fn reconcile_store_and_clusters(&self) -> Result<(Store, crate::dsu::Clusters)> {
        if self.shards.is_empty() {
            return Ok((
                Store::new(),
                crate::dsu::Clusters {
                    clusters: Vec::new(),
                },
            ));
        }

        let mut store = Store::new();
        seed_store_interner_from(&mut store, self.shards[0].store.interner());
        let mut remapped_records = Vec::new();
        for shard in &self.shards {
            let shard_interner = shard.store.interner();
            for record in shard.store.get_all_records() {
                let sequence = shard
                    .record_sequence
                    .get(&record.id)
                    .copied()
                    .unwrap_or(usize::MAX);
                let mut descriptors = Vec::with_capacity(record.descriptors.len());
                for descriptor in &record.descriptors {
                    let attr = shard_interner
                        .get_attr(descriptor.attr)
                        .unwrap_or(&"unknown".to_string())
                        .clone();
                    let value = shard_interner
                        .get_value(descriptor.value)
                        .unwrap_or(&"unknown".to_string())
                        .clone();
                    let attr_id = store.interner_mut().intern_attr(&attr);
                    let value_id = store.interner_mut().intern_value(&value);
                    descriptors.push(Descriptor::new(attr_id, value_id, descriptor.interval));
                }
                let remapped = Record::new(RecordId(0), record.identity.clone(), descriptors);
                remapped_records.push((sequence, remapped));
            }
        }
        remapped_records.sort_by_key(|(sequence, _)| *sequence);
        for (_, record) in remapped_records {
            store.add_record(record)?;
        }

        let clusters = crate::linker::build_clusters(&store, &self.shards[0].ontology)?;
        Ok((store, clusters))
    }
}

impl ShardState {
    fn ingest(&mut self, records: Vec<(Record, usize)>) -> Result<Vec<ClusterAssignment>> {
        let mut assignments = Vec::with_capacity(records.len());
        for (record, sequence) in records {
            let record_id = self.store.add_record(record)?;
            self.record_sequence.insert(record_id, sequence);
            let cluster_id = self
                .streamer
                .link_record(&self.store, &self.ontology, record_id)?;
            assignments.push(ClusterAssignment {
                record_id,
                cluster_id,
            });
        }
        Ok(assignments)
    }
}

fn shard_for_record(record: &Record, ontology: &Ontology, shard_count: usize) -> usize {
    let identity_keys = ontology.identity_keys_for_type(&record.identity.entity_type);
    let key_values = identity_keys
        .first()
        .and_then(|key| extract_key_values(record, key.attributes.as_slice()))
        .unwrap_or_default();

    let mut hasher = DefaultHasher::new();
    record.identity.entity_type.hash(&mut hasher);
    for value in &key_values {
        value.hash(&mut hasher);
    }
    let hash = hasher.finish();
    (hash as usize) % shard_count.max(1)
}

fn extract_key_values(record: &Record, attrs: &[crate::model::AttrId]) -> Option<Vec<KeyValue>> {
    let mut values = Vec::with_capacity(attrs.len());
    for attr in attrs {
        let descriptor = record
            .descriptors
            .iter()
            .filter(|descriptor| descriptor.attr == *attr)
            .max_by_key(|descriptor| descriptor.interval.end - descriptor.interval.start)?;
        values.push(KeyValue::new(*attr, descriptor.value));
    }
    Some(values)
}

fn seed_store_interner_from(store: &mut Store, interner: &StringInterner) {
    let mut attrs = interner
        .attr_ids()
        .filter_map(|id| interner.get_attr(id).cloned().map(|name| (id, name)))
        .collect::<Vec<_>>();
    attrs.sort_by_key(|(id, _)| id.0);

    let mut values = interner
        .value_ids()
        .filter_map(|id| interner.get_value(id).cloned().map(|value| (id, value)))
        .collect::<Vec<_>>();
    values.sort_by_key(|(id, _)| id.0);

    let store_interner = store.interner_mut();
    for (_, attr) in &attrs {
        store_interner.intern_attr(attr);
    }
    for (_, value) in &values {
        store_interner.intern_value(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Descriptor, RecordIdentity};
    use crate::Interval;

    #[test]
    fn sharded_reconcile_matches_single_linker() {
        let mut ontology = Ontology::new();
        let mut store = Store::new();

        let name_attr = store.interner_mut().intern_attr("name");
        let email_attr = store.interner_mut().intern_attr("email");
        ontology.add_identity_key(crate::ontology::IdentityKey::new(
            vec![name_attr, email_attr],
            "name_email".to_string(),
        ));

        let records = vec![
            Record::new(
                RecordId(0),
                RecordIdentity::new("person".to_string(), "crm".to_string(), "1".to_string()),
                vec![Descriptor::new(
                    name_attr,
                    store.interner_mut().intern_value("alice"),
                    Interval::new(0, 10).unwrap(),
                )],
            ),
            Record::new(
                RecordId(0),
                RecordIdentity::new("person".to_string(), "crm".to_string(), "2".to_string()),
                vec![Descriptor::new(
                    name_attr,
                    store.interner_mut().intern_value("alice"),
                    Interval::new(0, 10).unwrap(),
                )],
            ),
        ];

        let mut sharded = ShardedStreamEngine::new(ontology.clone(), 2).unwrap();
        sharded.stream_records(records.clone()).unwrap();
        let sharded_clusters = sharded.reconcile_clusters().unwrap();

        let mut single_store = Store::new();
        for record in records {
            let mut cloned = record.clone();
            cloned.id = RecordId(0);
            single_store.add_record(cloned).unwrap();
        }
        let single_clusters = crate::linker::build_clusters(&single_store, &ontology).unwrap();

        assert_eq!(
            sharded_clusters.clusters.len(),
            single_clusters.clusters.len()
        );
    }

    #[test]
    fn sharded_stream_assigns_all_records() {
        let mut ontology = Ontology::new();
        let mut store = Store::new();

        let name_attr = store.interner_mut().intern_attr("name");
        let email_attr = store.interner_mut().intern_attr("email");
        ontology.add_identity_key(crate::ontology::IdentityKey::new(
            vec![name_attr, email_attr],
            "name_email".to_string(),
        ));

        let records = (0..20)
            .map(|idx| {
                Record::new(
                    RecordId(0),
                    RecordIdentity::new("person".to_string(), "crm".to_string(), format!("{idx}")),
                    vec![
                        Descriptor::new(
                            name_attr,
                            store.interner_mut().intern_value(&format!("user{idx}")),
                            Interval::new(0, 10).unwrap(),
                        ),
                        Descriptor::new(
                            email_attr,
                            store
                                .interner_mut()
                                .intern_value(&format!("user{idx}@example.com")),
                            Interval::new(0, 10).unwrap(),
                        ),
                    ],
                )
            })
            .collect::<Vec<_>>();

        let mut engine = ShardedStreamEngine::new(ontology, 4).unwrap();
        let assignments = engine.stream_records(records).unwrap();
        assert_eq!(assignments.len(), 20);
        assert!(assignments.iter().all(|assignment| assignment.shard_id < 4));
    }

    #[test]
    fn shard_for_record_is_deterministic() {
        let mut ontology = Ontology::new();
        let mut store = Store::new();

        let name_attr = store.interner_mut().intern_attr("name");
        let email_attr = store.interner_mut().intern_attr("email");
        ontology.add_identity_key(crate::ontology::IdentityKey::new(
            vec![name_attr, email_attr],
            "name_email".to_string(),
        ));

        let record = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "1".to_string()),
            vec![
                Descriptor::new(
                    name_attr,
                    store.interner_mut().intern_value("alice"),
                    Interval::new(0, 10).unwrap(),
                ),
                Descriptor::new(
                    email_attr,
                    store.interner_mut().intern_value("alice@example.com"),
                    Interval::new(0, 10).unwrap(),
                ),
            ],
        );

        let first = shard_for_record(&record, &ontology, 8);
        let second = shard_for_record(&record, &ontology, 8);
        assert_eq!(first, second);
        assert!(first < 8);
    }

    #[test]
    fn test_identity_key_signature() {
        use crate::model::{AttrId, ValueId};

        let kv1 = vec![
            KeyValue::new(AttrId(1), ValueId(10)),
            KeyValue::new(AttrId(2), ValueId(20)),
        ];
        let kv2 = vec![
            KeyValue::new(AttrId(1), ValueId(10)),
            KeyValue::new(AttrId(2), ValueId(20)),
        ];
        let kv3 = vec![
            KeyValue::new(AttrId(1), ValueId(10)),
            KeyValue::new(AttrId(2), ValueId(21)), // Different value
        ];

        let sig1 = IdentityKeySignature::from_key_values("person", &kv1);
        let sig2 = IdentityKeySignature::from_key_values("person", &kv2);
        let sig3 = IdentityKeySignature::from_key_values("person", &kv3);
        let sig4 = IdentityKeySignature::from_key_values("company", &kv1); // Different entity type

        // Same key values should produce same signature
        assert_eq!(sig1, sig2);
        // Different values should produce different signatures
        assert_ne!(sig1, sig3);
        // Different entity type should produce different signature
        assert_ne!(sig1, sig4);
    }

    #[test]
    fn test_bloom_filter() {
        use crate::model::{AttrId, ValueId};

        let mut bloom = BloomFilter::new(1024); // 1KB for testing

        let kv = vec![KeyValue::new(AttrId(1), ValueId(10))];
        let sig1 = IdentityKeySignature::from_key_values("person", &kv);
        let sig2 = IdentityKeySignature::from_key_values("company", &kv);

        // Initially empty
        assert!(!bloom.may_contain(&sig1));
        assert!(!bloom.may_contain(&sig2));

        // Insert sig1
        bloom.insert(&sig1);

        // sig1 should now be found
        assert!(bloom.may_contain(&sig1));

        // sig2 might be found (false positive) or not, but definitely not guaranteed
        // We just check that the bloom filter is working

        // Clear and verify empty
        bloom.clear();
        assert!(!bloom.may_contain(&sig1));
    }

    #[test]
    fn test_cluster_boundary_index() {
        use crate::model::{AttrId, ValueId};

        let mut index = ClusterBoundaryIndex::new_small(0);

        let kv = vec![KeyValue::new(AttrId(1), ValueId(10))];
        let sig = IdentityKeySignature::from_key_values("person", &kv);
        let cluster_id = GlobalClusterId::new(0, 100, 0);
        let interval = Interval::new(0, 100).unwrap();

        // Initially empty
        assert!(index.is_empty());
        assert!(!index.may_have_boundary(&sig));

        // Register boundary key
        index.register_boundary_key(sig, cluster_id, interval);

        // Should now find it
        assert!(!index.is_empty());
        assert_eq!(index.len(), 1);
        assert!(index.may_have_boundary(&sig));

        let boundaries = index.get_boundaries(&sig).unwrap();
        assert_eq!(boundaries.len(), 1);
        assert_eq!(boundaries[0].cluster_id, cluster_id);
        assert_eq!(boundaries[0].interval, interval);
    }

    #[test]
    fn test_cluster_boundary_overlapping() {
        use crate::model::{AttrId, ValueId};

        let mut index = ClusterBoundaryIndex::new_small(0);

        let kv = vec![KeyValue::new(AttrId(1), ValueId(10))];
        let sig = IdentityKeySignature::from_key_values("person", &kv);

        // Add two entries with different intervals
        let cluster1 = GlobalClusterId::new(0, 100, 0);
        let interval1 = Interval::new(0, 50).unwrap();
        index.register_boundary_key(sig, cluster1, interval1);

        let cluster2 = GlobalClusterId::new(0, 200, 0);
        let interval2 = Interval::new(100, 150).unwrap();
        index.register_boundary_key(sig, cluster2, interval2);

        // Query with overlapping interval
        let query_interval = Interval::new(25, 75).unwrap();
        let overlapping = index.find_overlapping_boundaries(&sig, &query_interval);
        assert_eq!(overlapping.len(), 1);
        assert_eq!(overlapping[0].cluster_id, cluster1);

        // Query with interval that overlaps second entry
        let query_interval2 = Interval::new(110, 120).unwrap();
        let overlapping2 = index.find_overlapping_boundaries(&sig, &query_interval2);
        assert_eq!(overlapping2.len(), 1);
        assert_eq!(overlapping2[0].cluster_id, cluster2);
    }

    #[test]
    fn test_incremental_reconciler() {
        use crate::model::{AttrId, ValueId};

        let mut reconciler = IncrementalReconciler::new();

        // Create two shard boundaries with shared keys
        let mut shard0 = ClusterBoundaryIndex::new_small(0);
        let mut shard1 = ClusterBoundaryIndex::new_small(1);

        let kv = vec![KeyValue::new(AttrId(1), ValueId(10))];
        let sig = IdentityKeySignature::from_key_values("person", &kv);

        // Shard 0 has cluster 100 with this key
        let cluster0 = GlobalClusterId::new(0, 100, 0);
        let interval0 = Interval::new(0, 100).unwrap();
        shard0.register_boundary_key(sig, cluster0, interval0);

        // Shard 1 has cluster 200 with the same key and overlapping interval
        let cluster1 = GlobalClusterId::new(1, 200, 0);
        let interval1 = Interval::new(50, 150).unwrap();
        shard1.register_boundary_key(sig, cluster1, interval1);

        reconciler.add_shard_boundary(shard0);
        reconciler.add_shard_boundary(shard1);

        // Reconcile should find the cross-shard merge
        let result = reconciler.reconcile();

        assert_eq!(result.merges_performed, 1);
        assert_eq!(result.keys_matched, 1);
        assert_eq!(result.merged_clusters.len(), 1);

        // The merge should be from shard 0 (lower) to shard 1
        let (primary, secondary) = result.merged_clusters[0];
        assert_eq!(primary.shard_id, 0);
        assert_eq!(secondary.shard_id, 1);
    }

    #[test]
    fn test_boundary_metadata_export_import() {
        use crate::model::{AttrId, ValueId};

        let mut index1 = ClusterBoundaryIndex::new_small(0);

        let kv = vec![KeyValue::new(AttrId(1), ValueId(10))];
        let sig = IdentityKeySignature::from_key_values("person", &kv);
        let cluster_id = GlobalClusterId::new(0, 100, 0);
        let interval = Interval::new(0, 100).unwrap();

        index1.register_boundary_key(sig, cluster_id, interval);

        // Export metadata
        let metadata = index1.export_metadata();
        assert_eq!(metadata.shard_id, 0);
        assert_eq!(metadata.entries.len(), 1);

        // Import into another index
        let mut index2 = ClusterBoundaryIndex::new_small(1);
        index2.import_metadata(&metadata);

        // Should now contain the imported entry
        assert!(index2.may_have_boundary(&sig));
        let boundaries = index2.get_boundaries(&sig).unwrap();
        assert_eq!(boundaries.len(), 1);
        assert_eq!(boundaries[0].cluster_id, cluster_id);
    }
}
