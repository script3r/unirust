//! # Indexing Module
//!
//! Provides efficient indexing for identity keys, crosswalks, and temporal data
//! to enable fast lookup during entity resolution and conflict detection.

use crate::dsu::DsuBackend;
use crate::model::{CanonicalId, KeyValue, RecordId, ValueId};
use crate::ontology::{Crosswalk, IdentityKey};
use crate::temporal::Interval;
use anyhow::Result;
use hashbrown::{Equivalent, HashMap};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

type IdentityIndexMap = HashMap<IdentityIndexKey, KeyBucket>;
const MAX_KEY_VALUES_PER_ATTR: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IdentityIndexKey {
    entity_type: String,
    key_values: Vec<KeyValue>,
}

struct IdentityIndexKeyRef<'a> {
    entity_type: &'a str,
    key_values: &'a [KeyValue],
}

#[derive(Debug, Clone, Default)]
struct CandidateList {
    sorted: Vec<(RecordId, Interval)>,
    unsorted: Vec<(RecordId, Interval)>,
    end_order: Vec<usize>,
    sorted_by_end: bool,
    cluster_intervals: HashMap<RecordId, ClusterIntervalList>,
    cluster_tree: IntervalTree,
    active_intervals: usize,
    total_intervals: usize,
}

#[derive(Debug, Clone, Default)]
struct ClusterIntervalList {
    intervals: Vec<Interval>,
}

impl ClusterIntervalList {
    fn add_interval(&mut self, interval: Interval) {
        let mut start = interval.start;
        let mut end = interval.end;
        let mut idx = 0;
        while idx < self.intervals.len() {
            let current = self.intervals[idx];
            if end <= current.start {
                break;
            }
            if start >= current.end {
                idx += 1;
                continue;
            }
            start = start.min(current.start);
            end = end.max(current.end);
            self.intervals.remove(idx);
        }
        self.intervals.insert(idx, Interval { start, end });
    }

    fn extend_from(&mut self, other: &ClusterIntervalList) {
        for interval in &other.intervals {
            self.add_interval(*interval);
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct KeyBucket {
    record_candidates: CandidateList,
}

impl KeyBucket {
    fn insert_record(&mut self, record_id: RecordId, interval: Interval) {
        self.record_candidates.insert((record_id, interval));
    }

    fn insert_cluster_interval(&mut self, root_id: RecordId, interval: Interval) {
        self.record_candidates
            .insert_cluster_interval(root_id, interval);
    }

    fn merge_clusters(&mut self, root_a: RecordId, root_b: RecordId, new_root: RecordId) {
        self.record_candidates
            .merge_cluster_intervals(root_a, root_b, new_root);
    }
}

#[derive(Debug, Clone, Default)]
struct IntervalTree {
    nodes: Vec<IntervalNode>,
    root: Option<usize>,
}

#[derive(Debug, Clone)]
struct IntervalNode {
    interval: Interval,
    record_id: RecordId,
    max_end: i64,
    left: Option<usize>,
    right: Option<usize>,
}

impl IntervalTree {
    fn clear(&mut self) {
        self.nodes.clear();
        self.root = None;
    }

    fn insert(&mut self, record_id: RecordId, interval: Interval) {
        let node_index = self.nodes.len();
        self.nodes.push(IntervalNode {
            interval,
            record_id,
            max_end: interval.end,
            left: None,
            right: None,
        });

        let Some(mut current) = self.root else {
            self.root = Some(node_index);
            return;
        };

        let new_start = interval.start;
        let new_end = interval.end;
        // Use bit-reversed node_index as tiebreaker for better tree balance.
        // This distributes sequential inserts across the tree when start times are equal.
        let tiebreaker = (node_index as u32).reverse_bits();

        loop {
            let current_node = &self.nodes[current];
            let current_start = current_node.interval.start;
            let current_tiebreaker = (current as u32).reverse_bits();
            self.nodes[current].max_end = self.nodes[current].max_end.max(new_end);

            // Use (start, bit-reversed index) as composite key to prevent tree skew
            // when many intervals have the same start time.
            let go_left = if new_start != current_start {
                new_start < current_start
            } else {
                tiebreaker < current_tiebreaker
            };

            let next = if go_left {
                &mut self.nodes[current].left
            } else {
                &mut self.nodes[current].right
            };

            if let Some(next_idx) = *next {
                current = next_idx;
                continue;
            }

            *next = Some(node_index);
            break;
        }
    }

    #[allow(dead_code)]
    fn collect_overlapping(&self, interval: Interval, out: &mut Vec<(RecordId, Interval)>) {
        self.collect_overlapping_limited(interval, out, usize::MAX);
    }

    /// Collect overlapping intervals with an early exit after max_results.
    /// Returns true if the limit was reached (potentially more results exist).
    fn collect_overlapping_limited(
        &self,
        interval: Interval,
        out: &mut Vec<(RecordId, Interval)>,
        max_results: usize,
    ) -> bool {
        let Some(root) = self.root else {
            return false;
        };
        let mut stack = vec![root];

        while let Some(node_idx) = stack.pop() {
            let node = &self.nodes[node_idx];
            if node.interval.start < interval.end && node.interval.end > interval.start {
                out.push((node.record_id, node.interval));
                if out.len() >= max_results {
                    return true;
                }
            }

            if let Some(left) = node.left {
                if self.nodes[left].max_end > interval.start {
                    stack.push(left);
                }
            }

            if let Some(right) = node.right {
                if node.interval.start < interval.end {
                    stack.push(right);
                }
            }
        }
        false
    }
}

impl CandidateList {
    fn insert(&mut self, entry: (RecordId, Interval)) {
        self.unsorted.push(entry);
    }

    fn insert_cluster_interval(&mut self, root_id: RecordId, interval: Interval) {
        let list = self.cluster_intervals.entry(root_id).or_default();
        let prev_len = list.intervals.len();
        list.add_interval(interval);
        let new_len = list.intervals.len();
        self.active_intervals = self.active_intervals.saturating_sub(prev_len) + new_len;
        self.total_intervals = self.total_intervals.saturating_add(1);
        self.cluster_tree.insert(root_id, interval);
    }

    fn as_slice(&mut self) -> &[(RecordId, Interval)] {
        self.force_merge();
        self.sorted.as_slice()
    }

    fn collect_overlapping(&mut self, interval: Interval, out: &mut Vec<(RecordId, Interval)>) {
        self.maybe_merge();
        self.ensure_sorted_by_end();

        if !self.sorted.is_empty() {
            let start_idx = self
                .sorted
                .partition_point(|(_, entry_interval)| entry_interval.start < interval.end);
            let end_idx = self
                .end_order
                .partition_point(|&idx| self.sorted[idx].1.end <= interval.start);

            let start_len = start_idx;
            let end_suffix_len = self.sorted.len().saturating_sub(end_idx);

            if start_len <= end_suffix_len {
                for (record_id, entry_interval) in &self.sorted[..start_idx] {
                    if entry_interval.end > interval.start {
                        out.push((*record_id, *entry_interval));
                    }
                }
            } else {
                for &idx in &self.end_order[end_idx..] {
                    let (record_id, entry_interval) = self.sorted[idx];
                    if entry_interval.start < interval.end {
                        out.push((record_id, entry_interval));
                    }
                }
            }
        }

        if !self.unsorted.is_empty() {
            for (record_id, entry_interval) in &self.unsorted {
                if entry_interval.start < interval.end && entry_interval.end > interval.start {
                    out.push((*record_id, *entry_interval));
                }
            }
        }
    }

    #[allow(dead_code)]
    fn collect_overlapping_clusters(
        &mut self,
        dsu: &mut DsuBackend,
        interval: Interval,
        out: &mut Vec<(RecordId, Interval)>,
        seen: &mut HashSet<RecordId>,
    ) {
        self.collect_overlapping_clusters_limited(dsu, interval, out, seen, usize::MAX);
    }

    /// Collect overlapping clusters with an early exit after visiting max_tree_nodes.
    /// Returns true if the limit was reached (tree is too hot for efficient querying).
    fn collect_overlapping_clusters_limited(
        &mut self,
        dsu: &mut DsuBackend,
        interval: Interval,
        out: &mut Vec<(RecordId, Interval)>,
        seen: &mut HashSet<RecordId>,
        max_tree_nodes: usize,
    ) -> bool {
        self.rebuild_tree_if_needed();
        let mut scratch = Vec::new();
        let limit_reached =
            self.cluster_tree
                .collect_overlapping_limited(interval, &mut scratch, max_tree_nodes);
        for (record_id, candidate_interval) in scratch {
            let root = dsu.find(record_id).unwrap_or(record_id);
            if seen.insert(root) {
                out.push((root, candidate_interval));
            }
        }
        limit_reached
    }

    fn merge_cluster_intervals(&mut self, root_a: RecordId, root_b: RecordId, new_root: RecordId) {
        let mut merged = ClusterIntervalList::default();
        let mut removed = 0usize;

        if let Some(list) = self.cluster_intervals.remove(&root_a) {
            removed += list.intervals.len();
            merged.extend_from(&list);
        }
        if let Some(list) = self.cluster_intervals.remove(&root_b) {
            removed += list.intervals.len();
            merged.extend_from(&list);
        }

        if removed > 0 {
            self.active_intervals = self.active_intervals.saturating_sub(removed);
        }

        let added = merged.intervals.len();
        if added > 0 {
            for interval in &merged.intervals {
                self.cluster_tree.insert(new_root, *interval);
            }
            self.total_intervals = self.total_intervals.saturating_add(added);
            self.active_intervals = self.active_intervals.saturating_add(added);
            self.cluster_intervals.insert(new_root, merged);
        }
    }

    fn rebuild_tree_if_needed(&mut self) {
        if self.total_intervals <= self.active_intervals.saturating_mul(2).max(1024) {
            return;
        }

        self.cluster_tree.clear();
        self.total_intervals = 0;
        for (root_id, intervals) in &self.cluster_intervals {
            for interval in &intervals.intervals {
                self.cluster_tree.insert(*root_id, *interval);
                self.total_intervals = self.total_intervals.saturating_add(1);
            }
        }
    }

    fn ensure_sorted_by_end(&mut self) {
        if self.sorted_by_end {
            return;
        }
        self.end_order = (0..self.sorted.len()).collect();
        self.end_order.sort_by_key(|&idx| self.sorted[idx].1.end);
        self.sorted_by_end = true;
    }

    fn maybe_merge(&mut self) {
        const MERGE_THRESHOLD: usize = 256;
        if self.unsorted.len() >= MERGE_THRESHOLD {
            self.force_merge();
        }
    }

    fn force_merge(&mut self) {
        if self.unsorted.is_empty() {
            return;
        }
        if self.sorted.is_empty() {
            self.sorted = std::mem::take(&mut self.unsorted);
            self.sorted.sort_by_key(|(_, interval)| interval.start);
            self.sorted_by_end = false;
            return;
        }

        self.unsorted.sort_by_key(|(_, interval)| interval.start);
        let mut merged = Vec::with_capacity(self.sorted.len() + self.unsorted.len());
        let mut i = 0;
        let mut j = 0;
        while i < self.sorted.len() && j < self.unsorted.len() {
            if self.sorted[i].1.start <= self.unsorted[j].1.start {
                merged.push(self.sorted[i]);
                i += 1;
            } else {
                merged.push(self.unsorted[j]);
                j += 1;
            }
        }
        if i < self.sorted.len() {
            merged.extend_from_slice(&self.sorted[i..]);
        }
        if j < self.unsorted.len() {
            merged.extend_from_slice(&self.unsorted[j..]);
        }
        self.sorted = merged;
        self.unsorted.clear();
        self.sorted_by_end = false;
    }
}

impl<'a> Hash for IdentityIndexKeyRef<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.entity_type.hash(state);
        self.key_values.hash(state);
    }
}

impl<'a> Equivalent<IdentityIndexKey> for IdentityIndexKeyRef<'a> {
    fn equivalent(&self, key: &IdentityIndexKey) -> bool {
        self.entity_type == key.entity_type && self.key_values == key.key_values
    }
}

/// Extract key values with their intervals from a record for a given identity key.
/// This is a standalone function used by both IdentityKeyIndex and TieredIdentityKeyIndex.
/// Extract key values with their temporal intervals from a record.
/// This is a pure function that can be called in parallel for multiple records.
pub fn extract_key_values_from_record(
    record: &crate::model::Record,
    identity_key: &crate::ontology::IdentityKey,
) -> Result<Vec<(Vec<KeyValue>, Interval)>> {
    let _guard = crate::profile::profile_scope("identity_key_extract");
    let mut partials: Vec<(Vec<KeyValue>, Interval)> = Vec::new();

    for attr in &identity_key.attributes {
        let descriptors: Vec<_> = record
            .descriptors
            .iter()
            .filter(|d| d.attr == *attr)
            .collect();

        if descriptors.is_empty() {
            return Ok(Vec::new());
        }

        let mut value_intervals = coalesce_value_intervals_impl(descriptors);

        if value_intervals.len() > MAX_KEY_VALUES_PER_ATTR {
            value_intervals = prune_value_intervals_impl(value_intervals);
        }

        if partials.is_empty() {
            for (value, interval) in value_intervals {
                partials.push((vec![KeyValue::new(*attr, value)], interval));
            }
            continue;
        }

        let mut next = Vec::new();
        for (key_values, interval) in &partials {
            for (value, value_interval) in &value_intervals {
                if let Some(overlap) = crate::temporal::intersect(interval, value_interval) {
                    let mut next_key_values = key_values.clone();
                    next_key_values.push(KeyValue::new(*attr, *value));
                    next.push((next_key_values, overlap));
                }
            }
        }

        partials = next;
        if partials.is_empty() {
            break;
        }
    }

    Ok(partials)
}

fn coalesce_value_intervals_impl(
    descriptors: Vec<&crate::model::Descriptor>,
) -> Vec<(ValueId, Interval)> {
    use std::collections::HashMap;

    let mut by_value: HashMap<ValueId, Vec<Interval>> = HashMap::new();
    for descriptor in descriptors {
        by_value
            .entry(descriptor.value)
            .or_default()
            .push(descriptor.interval);
    }

    let mut result = Vec::new();
    for (value, intervals) in by_value {
        let coalesced = crate::temporal::coalesce_same_value(
            &intervals
                .iter()
                .map(|interval| (*interval, ()))
                .collect::<Vec<_>>(),
        );
        for (interval, _) in coalesced {
            result.push((value, interval));
        }
    }

    result
}

fn prune_value_intervals_impl(
    mut value_intervals: Vec<(ValueId, Interval)>,
) -> Vec<(ValueId, Interval)> {
    value_intervals.sort_by(|(value_a, interval_a), (value_b, interval_b)| {
        let len_a = interval_a.end - interval_a.start;
        let len_b = interval_b.end - interval_b.start;
        len_b
            .cmp(&len_a)
            .then_with(|| interval_a.start.cmp(&interval_b.start))
            .then_with(|| value_a.0.cmp(&value_b.0))
    });

    value_intervals
        .into_iter()
        .take(MAX_KEY_VALUES_PER_ATTR)
        .collect()
}

/// Index for identity key lookups
#[derive(Debug, Clone)]
pub struct IdentityKeyIndex {
    /// Maps (entity_type, key_values) -> list of (record_id, interval)
    index: IdentityIndexMap,
    /// Maps record_id -> list of identity keys
    record_keys: HashMap<RecordId, Vec<IdentityKey>>,
    /// Scratch buffer for overlap lookups.
    overlap_buffer: Vec<(RecordId, Interval)>,
    /// Scratch buffer for cluster overlap lookups.
    cluster_overlap_buffer: Vec<(RecordId, Interval)>,
    /// Scratch buffer for cluster dedupe.
    cluster_seen: HashSet<RecordId>,
}

impl IdentityKeyIndex {
    /// Create a new identity key index
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            record_keys: HashMap::new(),
            overlap_buffer: Vec::new(),
            cluster_overlap_buffer: Vec::new(),
            cluster_seen: HashSet::new(),
        }
    }

    /// Build the index from records and ontology
    pub fn build(
        &mut self,
        records: &[crate::model::Record],
        ontology: &crate::ontology::Ontology,
    ) -> Result<()> {
        self.index.clear();
        self.record_keys.clear();
        self.overlap_buffer.clear();
        self.cluster_overlap_buffer.clear();
        self.cluster_seen.clear();

        for record in records {
            let entity_type = &record.identity.entity_type;

            // Get identity keys for this entity type
            let identity_keys = ontology.identity_keys_for_type(entity_type);

            for identity_key in identity_keys {
                // Extract key values and their intervals from the record
                let key_values_with_intervals =
                    self.extract_key_values_with_intervals(record, identity_key)?;

                if !key_values_with_intervals.is_empty() {
                    for (key_values, interval) in key_values_with_intervals {
                        let key = IdentityIndexKey {
                            entity_type: entity_type.clone(),
                            key_values,
                        };
                        let candidates = self.index.entry(key).or_default();
                        candidates.insert_record(record.id, interval);
                        candidates.insert_cluster_interval(record.id, interval);
                    }

                    self.record_keys
                        .entry(record.id)
                        .or_default()
                        .push(identity_key.clone());
                }
            }
        }

        Ok(())
    }

    /// Extract key values with their intervals from a record for a given identity key
    pub(crate) fn extract_key_values_with_intervals(
        &self,
        record: &crate::model::Record,
        identity_key: &IdentityKey,
    ) -> Result<Vec<(Vec<KeyValue>, Interval)>> {
        extract_key_values_from_record(record, identity_key)
    }

    /// Add a single record to the index.
    pub fn add_record(
        &mut self,
        record: &crate::model::Record,
        ontology: &crate::ontology::Ontology,
    ) -> Result<()> {
        self.add_record_with_root(record, record.id, ontology)
    }

    pub fn add_record_with_root(
        &mut self,
        record: &crate::model::Record,
        root_id: RecordId,
        ontology: &crate::ontology::Ontology,
    ) -> Result<()> {
        let entity_type = &record.identity.entity_type;
        let identity_keys = ontology.identity_keys_for_type(entity_type);

        for identity_key in identity_keys {
            let key_values_with_intervals =
                self.extract_key_values_with_intervals(record, identity_key)?;

            if !key_values_with_intervals.is_empty() {
                for (key_values, interval) in key_values_with_intervals {
                    let key = IdentityIndexKey {
                        entity_type: entity_type.clone(),
                        key_values,
                    };
                    let candidates = self.index.entry(key).or_default();
                    candidates.insert_record(record.id, interval);
                    candidates.insert_cluster_interval(root_id, interval);
                }

                self.record_keys
                    .entry(record.id)
                    .or_default()
                    .push(identity_key.clone());
            }
        }

        Ok(())
    }

    /// Add a record using pre-extracted key values (avoids duplicate extraction).
    /// Takes ownership of cached_keys to avoid cloning.
    #[allow(clippy::type_complexity)]
    pub fn add_record_with_cached_keys(
        &mut self,
        record_id: RecordId,
        root_id: RecordId,
        entity_type: &str,
        cached_keys: Vec<(
            &crate::ontology::IdentityKey,
            Vec<(Vec<KeyValue>, Interval)>,
        )>,
    ) {
        use hashbrown::hash_map::RawEntryMut;
        use std::hash::{BuildHasher, Hash, Hasher};

        for (identity_key, key_values_with_intervals) in cached_keys {
            if !key_values_with_intervals.is_empty() {
                self.record_keys
                    .entry(record_id)
                    .or_default()
                    .push(identity_key.clone());

                for (key_values, interval) in key_values_with_intervals {
                    // Use raw_entry_mut to avoid allocating entity_type when key exists
                    let ref_key = IdentityIndexKeyRef {
                        entity_type,
                        key_values: &key_values,
                    };

                    // Compute hash using the borrowed key
                    #[allow(clippy::manual_hash_one)]
                    let hash = {
                        let mut hasher = self.index.hasher().build_hasher();
                        ref_key.hash(&mut hasher);
                        hasher.finish()
                    };

                    // Look up using raw entry, only allocate if inserting
                    let candidates = match self
                        .index
                        .raw_entry_mut()
                        .from_hash(hash, |k| ref_key.equivalent(k))
                    {
                        RawEntryMut::Occupied(entry) => entry.into_mut(),
                        RawEntryMut::Vacant(entry) => {
                            let owned_key = IdentityIndexKey {
                                entity_type: entity_type.to_string(),
                                key_values,
                            };
                            entry
                                .insert_hashed_nocheck(hash, owned_key, KeyBucket::default())
                                .1
                        }
                    };
                    candidates.insert_record(record_id, interval);
                    candidates.insert_cluster_interval(root_id, interval);
                }
            }
        }
    }

    /// Find records that match a given identity key
    pub fn find_matching_records(
        &mut self,
        entity_type: &str,
        key_values: &[KeyValue],
    ) -> &[(RecordId, Interval)] {
        let key = IdentityIndexKeyRef {
            entity_type,
            key_values,
        };
        self.index
            .get_mut(&key)
            .map(|values| values.record_candidates.as_slice())
            .unwrap_or(&[])
    }

    /// Find records that could overlap before the interval end.
    pub fn find_matching_records_overlapping(
        &mut self,
        entity_type: &str,
        key_values: &[KeyValue],
        interval: Interval,
    ) -> &[(RecordId, Interval)] {
        let key = IdentityIndexKeyRef {
            entity_type,
            key_values,
        };

        let Self {
            index,
            overlap_buffer,
            ..
        } = self;

        let Some(values) = index.get_mut(&key) else {
            return &[];
        };

        overlap_buffer.clear();
        values
            .record_candidates
            .collect_overlapping(interval, overlap_buffer);
        overlap_buffer.as_slice()
    }

    pub fn find_matching_clusters_overlapping(
        &mut self,
        dsu: &mut DsuBackend,
        entity_type: &str,
        key_values: &[KeyValue],
        interval: Interval,
    ) -> &[(RecordId, Interval)] {
        self.find_matching_clusters_overlapping_limited(
            dsu,
            entity_type,
            key_values,
            interval,
            usize::MAX,
        )
        .0
    }

    /// Find matching clusters with an early exit after visiting max_tree_nodes.
    /// Returns (candidates, limit_reached) where limit_reached indicates the key is "hot".
    pub fn find_matching_clusters_overlapping_limited(
        &mut self,
        dsu: &mut DsuBackend,
        entity_type: &str,
        key_values: &[KeyValue],
        interval: Interval,
        max_tree_nodes: usize,
    ) -> (&[(RecordId, Interval)], bool) {
        let key = IdentityIndexKeyRef {
            entity_type,
            key_values,
        };

        let Self {
            index,
            cluster_overlap_buffer,
            ..
        } = self;

        let Some(values) = index.get_mut(&key) else {
            return (&[], false);
        };

        cluster_overlap_buffer.clear();
        self.cluster_seen.clear();
        let limit_reached = values
            .record_candidates
            .collect_overlapping_clusters_limited(
                dsu,
                interval,
                cluster_overlap_buffer,
                &mut self.cluster_seen,
                max_tree_nodes,
            );
        (cluster_overlap_buffer.as_slice(), limit_reached)
    }

    pub fn merge_key_clusters(
        &mut self,
        entity_type: &str,
        key_values: &[KeyValue],
        root_a: RecordId,
        root_b: RecordId,
        new_root: RecordId,
    ) {
        let key = IdentityIndexKeyRef {
            entity_type,
            key_values,
        };
        if let Some(values) = self.index.get_mut(&key) {
            values.merge_clusters(root_a, root_b, new_root);
        }
    }

    /// Get all identity keys for a record
    pub fn get_record_keys(&self, record_id: RecordId) -> Vec<IdentityKey> {
        self.record_keys
            .get(&record_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get all indexed entity types
    pub fn get_entity_types(&self) -> HashSet<String> {
        self.index
            .keys()
            .map(|key| key.entity_type.clone())
            .collect()
    }

    /// Get all key values for an entity type
    pub fn get_key_values_for_type(&self, entity_type: &str) -> Vec<Vec<KeyValue>> {
        self.index
            .iter()
            .filter(|(key, _)| key.entity_type == entity_type)
            .map(|(key, _)| key.key_values.clone())
            .collect()
    }
}

impl Default for IdentityKeyIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tiered Index Implementation
// ============================================================================

use crate::persistence::index_encoding::{CompactBucketData, KeyAccessStats};
use lru::LruCache;
use rocksdb::DB;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Configuration for tiered index storage
#[derive(Debug, Clone)]
pub struct TierConfig {
    /// Maximum entries in hot tier (default: 100K keys, ~2GB)
    pub hot_tier_capacity: usize,
    /// Maximum entries in warm tier (default: 100K keys, ~2GB)
    pub warm_tier_capacity: usize,
    /// Score threshold for hot tier (default: 0.5)
    pub hot_threshold: f64,
    /// Score threshold for warm tier (default: 0.2)
    pub warm_threshold: f64,
    /// Maximum cardinality before forcing cold storage (default: 10K)
    pub max_hot_cardinality: u32,
    /// Interval between tier management runs (seconds)
    pub tier_management_interval_secs: u64,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            hot_tier_capacity: 100_000,
            warm_tier_capacity: 100_000,
            hot_threshold: 0.5,
            warm_threshold: 0.2,
            max_hot_cardinality: 10_000,
            tier_management_interval_secs: 60,
        }
    }
}

impl TierConfig {
    /// Memory-optimized configuration (~500MB total)
    pub fn memory_saver() -> Self {
        Self {
            hot_tier_capacity: 20_000,
            warm_tier_capacity: 30_000,
            hot_threshold: 0.6,
            warm_threshold: 0.3,
            max_hot_cardinality: 5_000,
            tier_management_interval_secs: 30,
        }
    }

    /// High-performance configuration (~8GB total)
    pub fn high_performance() -> Self {
        Self {
            hot_tier_capacity: 500_000,
            warm_tier_capacity: 500_000,
            hot_threshold: 0.3,
            warm_threshold: 0.1,
            max_hot_cardinality: 50_000,
            tier_management_interval_secs: 120,
        }
    }
}

/// Compact bucket for warm tier - linear scan but lower memory
#[derive(Debug, Clone, Default)]
pub struct CompactBucket {
    /// Record intervals in compact format (record_id, start, end)
    record_intervals: Vec<(u32, i64, i64)>,
    /// Cluster intervals in compact format (root_id, start, end)
    cluster_intervals: Vec<(u32, i64, i64)>,
}

impl CompactBucket {
    /// Create from a KeyBucket (for demotion)
    pub(crate) fn from_key_bucket(bucket: &KeyBucket) -> Self {
        let mut compact = Self::default();

        // Extract record intervals
        for (record_id, interval) in bucket.record_candidates.sorted.iter() {
            compact
                .record_intervals
                .push((record_id.0, interval.start, interval.end));
        }
        for (record_id, interval) in bucket.record_candidates.unsorted.iter() {
            compact
                .record_intervals
                .push((record_id.0, interval.start, interval.end));
        }

        // Extract cluster intervals
        for (root_id, list) in bucket.record_candidates.cluster_intervals.iter() {
            for interval in &list.intervals {
                compact
                    .cluster_intervals
                    .push((root_id.0, interval.start, interval.end));
            }
        }

        compact
    }

    /// Convert to storage format
    pub fn to_data(&self) -> CompactBucketData {
        CompactBucketData {
            record_intervals: self.record_intervals.clone(),
            cluster_intervals: self.cluster_intervals.clone(),
        }
    }

    /// Create from storage format
    pub fn from_data(data: CompactBucketData) -> Self {
        Self {
            record_intervals: data.record_intervals,
            cluster_intervals: data.cluster_intervals,
        }
    }

    /// Find overlapping records with linear scan
    pub fn find_overlapping_records(&self, interval: Interval) -> Vec<(RecordId, Interval)> {
        self.record_intervals
            .iter()
            .filter_map(|(id, start, end)| {
                if *start < interval.end && *end > interval.start {
                    Interval::new(*start, *end).ok().map(|i| (RecordId(*id), i))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Find overlapping clusters with linear scan
    pub fn find_overlapping_clusters(&self, interval: Interval) -> Vec<(RecordId, Interval)> {
        self.cluster_intervals
            .iter()
            .filter_map(|(id, start, end)| {
                if *start < interval.end && *end > interval.start {
                    Interval::new(*start, *end).ok().map(|i| (RecordId(*id), i))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get cardinality (number of unique records)
    pub fn cardinality(&self) -> usize {
        let mut seen: HashSet<u32> = HashSet::new();
        for (id, _, _) in &self.record_intervals {
            seen.insert(*id);
        }
        seen.len()
    }

    /// Insert a record interval
    pub fn insert_record(&mut self, record_id: RecordId, interval: Interval) {
        self.record_intervals
            .push((record_id.0, interval.start, interval.end));
    }

    /// Insert a cluster interval
    pub fn insert_cluster(&mut self, root_id: RecordId, interval: Interval) {
        self.cluster_intervals
            .push((root_id.0, interval.start, interval.end));
    }
}

/// Tiered identity key index with hot/warm/cold storage
pub struct TieredIdentityKeyIndex {
    /// Hot tier: Full KeyBucket with IntervalTree for O(log n) queries
    hot: HashMap<IdentityIndexKey, KeyBucket>,
    /// Access statistics for hot tier keys
    hot_stats: HashMap<IdentityIndexKey, KeyAccessStats>,
    /// Warm tier: CompactBucket with linear scan
    warm: LruCache<IdentityIndexKey, CompactBucket>,
    /// Warm tier statistics
    warm_stats: HashMap<IdentityIndexKey, KeyAccessStats>,
    /// Reference to RocksDB for cold tier
    db: Option<Arc<DB>>,
    /// Tier configuration
    config: TierConfig,
    /// Last tier management time
    last_tier_management: i64,
    /// Column family names
    cf_identity_keys: &'static str,
    cf_key_stats: &'static str,
    /// Scratch buffers
    #[allow(dead_code)]
    overlap_buffer: Vec<(RecordId, Interval)>,
    cluster_overlap_buffer: Vec<(RecordId, Interval)>,
    cluster_seen: HashSet<RecordId>,
    /// Record keys mapping
    record_keys: HashMap<RecordId, Vec<IdentityKey>>,
}

impl TieredIdentityKeyIndex {
    /// Create a new tiered index with default configuration (in-memory only)
    pub fn new() -> Self {
        Self::with_config(TierConfig::default(), None)
    }

    /// Create with custom configuration
    pub fn with_config(config: TierConfig, db: Option<Arc<DB>>) -> Self {
        let warm_cap =
            NonZeroUsize::new(config.warm_tier_capacity).unwrap_or(NonZeroUsize::new(1).unwrap());

        Self {
            hot: HashMap::new(),
            hot_stats: HashMap::new(),
            warm: LruCache::new(warm_cap),
            warm_stats: HashMap::new(),
            db,
            config,
            last_tier_management: 0,
            cf_identity_keys: crate::persistence::index_cf::IDENTITY_KEYS,
            cf_key_stats: crate::persistence::index_cf::KEY_STATS,
            overlap_buffer: Vec::new(),
            cluster_overlap_buffer: Vec::new(),
            cluster_seen: HashSet::new(),
            record_keys: HashMap::new(),
        }
    }

    /// Get current timestamp
    fn current_time() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    /// Record an access to a key (for future use in promotion logic)
    #[allow(dead_code)]
    fn record_access(&mut self, key: &IdentityIndexKey) {
        let now = Self::current_time();

        // Update hot stats
        if let Some(stats) = self.hot_stats.get_mut(key) {
            stats.access_count = stats.access_count.saturating_add(1);
            stats.total_queries = stats.total_queries.saturating_add(1);
            stats.last_access = now;
            return;
        }

        // Update warm stats
        if let Some(stats) = self.warm_stats.get_mut(key) {
            stats.access_count = stats.access_count.saturating_add(1);
            stats.total_queries = stats.total_queries.saturating_add(1);
            stats.last_access = now;
        }
    }

    /// Add a record to the index
    pub fn add_record(
        &mut self,
        record: &crate::model::Record,
        ontology: &crate::ontology::Ontology,
    ) -> anyhow::Result<()> {
        self.add_record_with_root(record, record.id, ontology)
    }

    /// Add a record with a specific root
    pub fn add_record_with_root(
        &mut self,
        record: &crate::model::Record,
        root_id: RecordId,
        ontology: &crate::ontology::Ontology,
    ) -> anyhow::Result<()> {
        let entity_type = &record.identity.entity_type;
        let identity_keys = ontology.identity_keys_for_type(entity_type);

        for identity_key in identity_keys {
            // Use a temporary IdentityKeyIndex to extract key values
            let temp_index = IdentityKeyIndex::new();
            let key_values_with_intervals =
                temp_index.extract_key_values_with_intervals(record, identity_key)?;

            if !key_values_with_intervals.is_empty() {
                for (key_values, interval) in key_values_with_intervals {
                    let key = IdentityIndexKey {
                        entity_type: entity_type.clone(),
                        key_values,
                    };

                    // Always add to hot tier for new records
                    let bucket = self.hot.entry(key.clone()).or_default();
                    bucket.insert_record(record.id, interval);
                    bucket.insert_cluster_interval(root_id, interval);

                    // Update stats
                    let stats = self.hot_stats.entry(key).or_default();
                    stats.cardinality = stats.cardinality.saturating_add(1);
                    stats.last_access = Self::current_time();
                }

                self.record_keys
                    .entry(record.id)
                    .or_default()
                    .push(identity_key.clone());
            }
        }

        // Run tier management periodically
        self.maybe_manage_tiers();

        Ok(())
    }

    /// Find matching clusters overlapping an interval
    pub fn find_matching_clusters_overlapping(
        &mut self,
        dsu: &mut DsuBackend,
        entity_type: &str,
        key_values: &[KeyValue],
        interval: Interval,
    ) -> &[(RecordId, Interval)] {
        self.find_matching_clusters_overlapping_limited(
            dsu,
            entity_type,
            key_values,
            interval,
            usize::MAX,
        )
        .0
    }

    /// Find matching clusters with a limit
    pub fn find_matching_clusters_overlapping_limited(
        &mut self,
        dsu: &mut DsuBackend,
        entity_type: &str,
        key_values: &[KeyValue],
        interval: Interval,
        max_tree_nodes: usize,
    ) -> (&[(RecordId, Interval)], bool) {
        let key = IdentityIndexKeyRef {
            entity_type,
            key_values,
        };

        self.cluster_overlap_buffer.clear();
        self.cluster_seen.clear();

        // Check hot tier first
        if let Some(bucket) = self.hot.get_mut(&key) {
            // Record access
            if let Some(stats) = self.hot_stats.get_mut(&IdentityIndexKey {
                entity_type: entity_type.to_string(),
                key_values: key_values.to_vec(),
            }) {
                stats.access_count = stats.access_count.saturating_add(1);
                stats.total_queries = stats.total_queries.saturating_add(1);
                stats.last_access = Self::current_time();
            }

            let limit_reached = bucket
                .record_candidates
                .collect_overlapping_clusters_limited(
                    dsu,
                    interval,
                    &mut self.cluster_overlap_buffer,
                    &mut self.cluster_seen,
                    max_tree_nodes,
                );
            return (self.cluster_overlap_buffer.as_slice(), limit_reached);
        }

        // Check warm tier
        let owned_key = IdentityIndexKey {
            entity_type: entity_type.to_string(),
            key_values: key_values.to_vec(),
        };

        if let Some(compact) = self.warm.get(&owned_key) {
            // Record access
            if let Some(stats) = self.warm_stats.get_mut(&owned_key) {
                stats.access_count = stats.access_count.saturating_add(1);
                stats.total_queries = stats.total_queries.saturating_add(1);
                stats.last_access = Self::current_time();
            }

            // Linear scan for warm tier
            let results = compact.find_overlapping_clusters(interval);
            for (root_id, cluster_interval) in results {
                let root = dsu.find(root_id).unwrap_or(root_id);
                if self.cluster_seen.insert(root) {
                    self.cluster_overlap_buffer.push((root, cluster_interval));
                }
            }
            return (self.cluster_overlap_buffer.as_slice(), false);
        }

        // Check cold tier (RocksDB)
        if let Some(db) = &self.db {
            if let Some(cf) = db.cf_handle(self.cf_identity_keys) {
                let key_bytes = crate::persistence::index_encoding::encode_identity_key(
                    entity_type,
                    key_values,
                );
                if let Ok(Some(data_bytes)) = db.get_cf(cf, &key_bytes) {
                    if let Ok(data) =
                        crate::persistence::index_encoding::decode_compact_bucket(&data_bytes)
                    {
                        let compact = CompactBucket::from_data(data);
                        let results = compact.find_overlapping_clusters(interval);
                        for (root_id, cluster_interval) in results {
                            let root = dsu.find(root_id).unwrap_or(root_id);
                            if self.cluster_seen.insert(root) {
                                self.cluster_overlap_buffer.push((root, cluster_interval));
                            }
                        }

                        // Promote to warm tier
                        self.warm.put(owned_key.clone(), compact);
                        self.warm_stats.insert(
                            owned_key,
                            KeyAccessStats {
                                access_count: 1,
                                last_access: Self::current_time(),
                                total_queries: 1,
                                cardinality: 0,
                            },
                        );

                        return (self.cluster_overlap_buffer.as_slice(), false);
                    }
                }
            }
        }

        (&[], false)
    }

    /// Merge clusters in the index
    pub fn merge_key_clusters(
        &mut self,
        entity_type: &str,
        key_values: &[KeyValue],
        root_a: RecordId,
        root_b: RecordId,
        new_root: RecordId,
    ) {
        let key = IdentityIndexKeyRef {
            entity_type,
            key_values,
        };

        // Try hot tier
        if let Some(bucket) = self.hot.get_mut(&key) {
            bucket.merge_clusters(root_a, root_b, new_root);
            return;
        }

        // Try warm tier - need to convert to hot for merge
        let owned_key = IdentityIndexKey {
            entity_type: entity_type.to_string(),
            key_values: key_values.to_vec(),
        };

        if self.warm.contains(&owned_key) {
            // Promote to hot for merge operation
            if let Some(compact) = self.warm.pop(&owned_key) {
                let mut bucket = KeyBucket::default();
                // Convert compact back to full bucket
                for (id, start, end) in compact.record_intervals {
                    if let Ok(interval) = Interval::new(start, end) {
                        bucket.insert_record(RecordId(id), interval);
                    }
                }
                for (id, start, end) in compact.cluster_intervals {
                    if let Ok(interval) = Interval::new(start, end) {
                        bucket.insert_cluster_interval(RecordId(id), interval);
                    }
                }
                bucket.merge_clusters(root_a, root_b, new_root);
                self.hot.insert(owned_key, bucket);
            }
        }
    }

    /// Run tier management if enough time has passed
    fn maybe_manage_tiers(&mut self) {
        let now = Self::current_time();
        if now - self.last_tier_management < self.config.tier_management_interval_secs as i64 {
            return;
        }
        self.last_tier_management = now;

        // Only demote if hot tier is over capacity
        if self.hot.len() <= self.config.hot_tier_capacity {
            return;
        }

        self.demote_cold_keys();
    }

    /// Demote cold keys from hot to warm tier
    fn demote_cold_keys(&mut self) {
        let now = Self::current_time();
        let max_card = self.config.max_hot_cardinality;

        // Collect keys to demote
        let mut to_demote: Vec<(IdentityIndexKey, f64)> = self
            .hot_stats
            .iter()
            .map(|(key, stats)| (key.clone(), stats.tier_score(now, max_card)))
            .filter(|(_, score)| *score < self.config.hot_threshold)
            .collect();

        // Sort by score ascending (coldest first)
        to_demote.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Demote until under capacity
        let to_remove = self.hot.len().saturating_sub(self.config.hot_tier_capacity);
        for (key, _) in to_demote.into_iter().take(to_remove) {
            if let Some(bucket) = self.hot.remove(&key) {
                let compact = CompactBucket::from_key_bucket(&bucket);

                if let Some(stats) = self.hot_stats.remove(&key) {
                    // Move to warm tier
                    self.warm.put(key.clone(), compact);
                    self.warm_stats.insert(key, stats);
                }
            }
        }
    }

    /// Flush warm tier to cold tier (RocksDB)
    pub fn flush_warm_to_cold(&mut self) -> anyhow::Result<()> {
        let Some(db) = &self.db else {
            return Ok(());
        };

        let cf = db
            .cf_handle(self.cf_identity_keys)
            .ok_or_else(|| anyhow::anyhow!("missing identity keys column family"))?;

        let stats_cf = db
            .cf_handle(self.cf_key_stats)
            .ok_or_else(|| anyhow::anyhow!("missing key stats column family"))?;

        let mut batch = rocksdb::WriteBatch::default();

        // Only flush entries that are being evicted from warm tier
        // For now, flush all warm entries to cold as backup
        for (key, compact) in self.warm.iter() {
            let key_bytes = crate::persistence::index_encoding::encode_identity_key(
                &key.entity_type,
                &key.key_values,
            );
            let data = compact.to_data();
            let data_bytes = crate::persistence::index_encoding::encode_compact_bucket(&data)?;
            batch.put_cf(cf, &key_bytes, data_bytes);

            // Also save stats
            if let Some(stats) = self.warm_stats.get(key) {
                let stats_bytes = crate::persistence::index_encoding::encode_key_stats(stats)?;
                batch.put_cf(stats_cf, &key_bytes, stats_bytes);
            }
        }

        db.write(batch)?;
        Ok(())
    }

    /// Get record keys for a record
    pub fn get_record_keys(&self, record_id: RecordId) -> Vec<IdentityKey> {
        self.record_keys
            .get(&record_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get tier statistics
    pub fn tier_stats(&self) -> TieredIndexStats {
        TieredIndexStats {
            hot_keys: self.hot.len(),
            hot_capacity: self.config.hot_tier_capacity,
            warm_keys: self.warm.len(),
            warm_capacity: self.config.warm_tier_capacity,
            cold_enabled: self.db.is_some(),
        }
    }

    /// Build index from records (bulk load)
    pub fn build(
        &mut self,
        records: &[crate::model::Record],
        ontology: &crate::ontology::Ontology,
    ) -> anyhow::Result<()> {
        self.hot.clear();
        self.hot_stats.clear();
        self.warm.clear();
        self.warm_stats.clear();
        self.record_keys.clear();

        for record in records {
            self.add_record(record, ontology)?;
        }

        Ok(())
    }

    /// Extract key values with intervals (delegates to IdentityKeyIndex helper)
    pub fn extract_key_values_with_intervals(
        &self,
        record: &crate::model::Record,
        identity_key: &crate::ontology::IdentityKey,
    ) -> anyhow::Result<Vec<(Vec<KeyValue>, Interval)>> {
        // Use the shared extraction logic
        extract_key_values_from_record(record, identity_key)
    }

    /// Add a record using pre-extracted key values (avoids duplicate extraction).
    #[allow(clippy::type_complexity)]
    pub fn add_record_with_cached_keys(
        &mut self,
        record_id: RecordId,
        root_id: RecordId,
        entity_type: &str,
        cached_keys: Vec<(
            &crate::ontology::IdentityKey,
            Vec<(Vec<KeyValue>, Interval)>,
        )>,
    ) {
        for (identity_key, key_values_with_intervals) in cached_keys {
            if !key_values_with_intervals.is_empty() {
                self.record_keys
                    .entry(record_id)
                    .or_default()
                    .push(identity_key.clone());

                for (key_values, interval) in key_values_with_intervals {
                    let key = IdentityIndexKey {
                        entity_type: entity_type.to_string(),
                        key_values,
                    };

                    // Always add to hot tier
                    let bucket = self.hot.entry(key.clone()).or_default();
                    bucket.insert_record(record_id, interval);
                    bucket.insert_cluster_interval(root_id, interval);

                    // Update stats
                    let stats = self.hot_stats.entry(key).or_default();
                    stats.cardinality = stats.cardinality.saturating_add(1);
                    stats.last_access = Self::current_time();
                }
            }
        }
    }

    /// Find records that match a given identity key
    pub fn find_matching_records(
        &mut self,
        entity_type: &str,
        key_values: &[KeyValue],
    ) -> &[(RecordId, Interval)] {
        // First check warm tier and promote if needed (must happen before hot tier borrow)
        let owned_key = IdentityIndexKey {
            entity_type: entity_type.to_string(),
            key_values: key_values.to_vec(),
        };

        // Promotion from warm to hot must happen before we borrow hot tier
        if !self.hot.contains_key(&owned_key) && self.warm.contains(&owned_key) {
            if let Some(compact) = self.warm.pop(&owned_key) {
                let mut bucket = KeyBucket::default();
                for (id, start, end) in compact.record_intervals {
                    if let Ok(interval) = Interval::new(start, end) {
                        bucket.insert_record(RecordId(id), interval);
                    }
                }
                for (id, start, end) in compact.cluster_intervals {
                    if let Ok(interval) = Interval::new(start, end) {
                        bucket.insert_cluster_interval(RecordId(id), interval);
                    }
                }
                self.hot.insert(owned_key.clone(), bucket);
                if let Some(stats) = self.warm_stats.remove(&owned_key) {
                    self.hot_stats.insert(owned_key, stats);
                }
            }
        }

        // Now check hot tier - use get_mut since as_slice() requires &mut self
        let key = IdentityIndexKeyRef {
            entity_type,
            key_values,
        };

        if let Some(bucket) = self.hot.get_mut(&key) {
            return bucket.record_candidates.as_slice();
        }

        &[]
    }
}

impl Default for TieredIdentityKeyIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for tiered index monitoring
#[derive(Debug, Clone)]
pub struct TieredIndexStats {
    pub hot_keys: usize,
    pub hot_capacity: usize,
    pub warm_keys: usize,
    pub warm_capacity: usize,
    pub cold_enabled: bool,
}

/// Backend abstraction for identity key index.
/// Allows StreamingLinker to use either in-memory or tiered index.
pub enum IndexBackend {
    /// In-memory index for smaller datasets
    InMemory(IdentityKeyIndex),
    /// Tiered index with hot/warm/cold tiers for billion-scale
    Tiered(Box<TieredIdentityKeyIndex>),
}

impl IndexBackend {
    /// Create an in-memory backend
    pub fn in_memory() -> Self {
        IndexBackend::InMemory(IdentityKeyIndex::new())
    }

    /// Create a tiered backend
    pub fn tiered(config: TierConfig, db: Option<Arc<DB>>) -> Self {
        IndexBackend::Tiered(Box::new(TieredIdentityKeyIndex::with_config(config, db)))
    }

    /// Extract key values with intervals from a record
    pub fn extract_key_values_with_intervals(
        &self,
        record: &crate::model::Record,
        identity_key: &crate::ontology::IdentityKey,
    ) -> Result<Vec<(Vec<KeyValue>, Interval)>> {
        match self {
            IndexBackend::InMemory(index) => {
                index.extract_key_values_with_intervals(record, identity_key)
            }
            IndexBackend::Tiered(index) => {
                index.extract_key_values_with_intervals(record, identity_key)
            }
        }
    }

    /// Add a record using pre-extracted key values
    #[allow(clippy::type_complexity)]
    pub fn add_record_with_cached_keys(
        &mut self,
        record_id: RecordId,
        root_id: RecordId,
        entity_type: &str,
        cached_keys: Vec<(
            &crate::ontology::IdentityKey,
            Vec<(Vec<KeyValue>, Interval)>,
        )>,
    ) {
        match self {
            IndexBackend::InMemory(index) => {
                index.add_record_with_cached_keys(record_id, root_id, entity_type, cached_keys)
            }
            IndexBackend::Tiered(index) => {
                index.add_record_with_cached_keys(record_id, root_id, entity_type, cached_keys)
            }
        }
    }

    /// Find matching clusters with overlap filtering (limited query)
    pub fn find_matching_clusters_overlapping_limited(
        &mut self,
        dsu: &mut DsuBackend,
        entity_type: &str,
        key_values: &[KeyValue],
        interval: Interval,
        limit: usize,
    ) -> (&[(RecordId, Interval)], bool) {
        match self {
            IndexBackend::InMemory(index) => index.find_matching_clusters_overlapping_limited(
                dsu,
                entity_type,
                key_values,
                interval,
                limit,
            ),
            IndexBackend::Tiered(index) => index.find_matching_clusters_overlapping_limited(
                dsu,
                entity_type,
                key_values,
                interval,
                limit,
            ),
        }
    }

    /// Find records that match a given identity key
    pub fn find_matching_records(
        &mut self,
        entity_type: &str,
        key_values: &[KeyValue],
    ) -> &[(RecordId, Interval)] {
        match self {
            IndexBackend::InMemory(index) => index.find_matching_records(entity_type, key_values),
            IndexBackend::Tiered(index) => index.find_matching_records(entity_type, key_values),
        }
    }

    /// Merge clusters in the index
    pub fn merge_key_clusters(
        &mut self,
        entity_type: &str,
        key_values: &[KeyValue],
        root_a: RecordId,
        root_b: RecordId,
        new_root: RecordId,
    ) {
        match self {
            IndexBackend::InMemory(index) => {
                index.merge_key_clusters(entity_type, key_values, root_a, root_b, new_root)
            }
            IndexBackend::Tiered(index) => {
                index.merge_key_clusters(entity_type, key_values, root_a, root_b, new_root)
            }
        }
    }

    /// Check if using tiered backend
    pub fn is_tiered(&self) -> bool {
        matches!(self, IndexBackend::Tiered(_))
    }
}

/// Index for crosswalk lookups
#[derive(Debug, Clone)]
pub struct CrosswalkIndex {
    /// Maps perspective -> list of crosswalks
    perspective_index: HashMap<String, Vec<Crosswalk>>,
    /// Maps canonical_id -> list of crosswalks
    canonical_index: HashMap<CanonicalId, Vec<Crosswalk>>,
    /// Maps (perspective, uid) -> crosswalk
    perspective_uid_index: HashMap<(String, String), Crosswalk>,
}

impl CrosswalkIndex {
    /// Create a new crosswalk index
    pub fn new() -> Self {
        Self {
            perspective_index: HashMap::new(),
            canonical_index: HashMap::new(),
            perspective_uid_index: HashMap::new(),
        }
    }

    /// Build the index from crosswalks
    pub fn build(&mut self, crosswalks: &[Crosswalk]) {
        self.perspective_index.clear();
        self.canonical_index.clear();
        self.perspective_uid_index.clear();

        for crosswalk in crosswalks {
            // Index by perspective
            self.perspective_index
                .entry(crosswalk.perspective_id.perspective.clone())
                .or_default()
                .push(crosswalk.clone());

            // Index by canonical ID
            self.canonical_index
                .entry(crosswalk.canonical_id.clone())
                .or_default()
                .push(crosswalk.clone());

            // Index by perspective and UID
            let key = (
                crosswalk.perspective_id.perspective.clone(),
                crosswalk.perspective_id.uid.clone(),
            );
            self.perspective_uid_index.insert(key, crosswalk.clone());
        }
    }

    /// Find crosswalks for a perspective
    pub fn find_by_perspective(&self, perspective: &str) -> Vec<&Crosswalk> {
        self.perspective_index
            .get(perspective)
            .map(|crosswalks| crosswalks.iter().collect())
            .unwrap_or_default()
    }

    /// Find crosswalks for a canonical ID
    pub fn find_by_canonical_id(&self, canonical_id: &CanonicalId) -> Vec<&Crosswalk> {
        self.canonical_index
            .get(canonical_id)
            .map(|crosswalks| crosswalks.iter().collect())
            .unwrap_or_default()
    }

    /// Find crosswalk by perspective and UID
    pub fn find_by_perspective_uid(&self, perspective: &str, uid: &str) -> Option<&Crosswalk> {
        let key = (perspective.to_string(), uid.to_string());
        self.perspective_uid_index.get(&key)
    }

    /// Find crosswalks that overlap with a given interval
    pub fn find_overlapping(&self, interval: Interval) -> Vec<&Crosswalk> {
        let mut result = Vec::new();

        for crosswalks in self.perspective_index.values() {
            for crosswalk in crosswalks {
                if crate::temporal::is_overlapping(&crosswalk.interval, &interval) {
                    result.push(crosswalk);
                }
            }
        }

        result
    }
}

impl Default for CrosswalkIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Index for temporal data
#[derive(Debug, Clone)]
pub struct TemporalIndex {
    /// Maps interval -> list of record IDs
    interval_index: HashMap<Interval, Vec<RecordId>>,
    /// Maps record_id -> list of intervals
    record_intervals: HashMap<RecordId, Vec<Interval>>,
}

impl TemporalIndex {
    /// Create a new temporal index
    pub fn new() -> Self {
        Self {
            interval_index: HashMap::new(),
            record_intervals: HashMap::new(),
        }
    }

    /// Build the index from records
    pub fn build(&mut self, records: &[crate::model::Record]) {
        self.interval_index.clear();
        self.record_intervals.clear();

        for record in records {
            let mut record_intervals = Vec::new();

            for descriptor in &record.descriptors {
                let interval = descriptor.interval;

                // Index by interval
                self.interval_index
                    .entry(interval)
                    .or_default()
                    .push(record.id);

                record_intervals.push(interval);
            }

            self.record_intervals.insert(record.id, record_intervals);
        }
    }

    /// Find records that overlap with a given interval
    pub fn find_overlapping_records(&self, interval: Interval) -> Vec<RecordId> {
        let mut result = Vec::new();

        for (index_interval, record_ids) in &self.interval_index {
            if crate::temporal::is_overlapping(index_interval, &interval) {
                result.extend(record_ids);
            }
        }

        result.sort();
        result.dedup();
        result
    }

    /// Get intervals for a record
    pub fn get_record_intervals(&self, record_id: RecordId) -> Vec<Interval> {
        self.record_intervals
            .get(&record_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Find records that have descriptors in a specific time range
    pub fn find_records_in_time_range(&self, start: i64, end: i64) -> Vec<RecordId> {
        let interval = Interval::new(start, end).unwrap();
        self.find_overlapping_records(interval)
    }
}

impl Default for TemporalIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Main index manager that coordinates all indices
#[derive(Debug, Clone)]
pub struct IndexManager {
    /// Identity key index
    pub identity_key_index: IdentityKeyIndex,
    /// Crosswalk index
    pub crosswalk_index: CrosswalkIndex,
    /// Temporal index
    pub temporal_index: TemporalIndex,
}

impl IndexManager {
    /// Create a new index manager
    pub fn new() -> Self {
        Self {
            identity_key_index: IdentityKeyIndex::new(),
            crosswalk_index: CrosswalkIndex::new(),
            temporal_index: TemporalIndex::new(),
        }
    }

    /// Build all indices
    pub fn build_all(
        &mut self,
        records: &[crate::model::Record],
        ontology: &crate::ontology::Ontology,
    ) -> Result<()> {
        // Build identity key index
        self.identity_key_index.build(records, ontology)?;

        // Build crosswalk index
        self.crosswalk_index.build(&ontology.crosswalks);

        // Build temporal index
        self.temporal_index.build(records);

        Ok(())
    }

    /// Find records that match an identity key in a time interval
    pub fn find_matching_records_in_interval(
        &mut self,
        entity_type: &str,
        key_values: &[KeyValue],
        interval: Interval,
    ) -> Vec<RecordId> {
        let matching_records = self.identity_key_index.find_matching_records_overlapping(
            entity_type,
            key_values,
            interval,
        );

        matching_records
            .iter()
            .filter(|(_, record_interval)| {
                crate::temporal::is_overlapping(record_interval, &interval)
            })
            .map(|(record_id, _)| *record_id)
            .collect()
    }

    /// Find crosswalks that can link records in a time interval
    pub fn find_linking_crosswalks(&self, interval: Interval) -> Vec<&Crosswalk> {
        self.crosswalk_index.find_overlapping(interval)
    }

    /// Get all records that have activity in a time interval
    pub fn get_active_records(&self, interval: Interval) -> Vec<RecordId> {
        self.temporal_index.find_overlapping_records(interval)
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AttrId, Descriptor, PerspectiveScopedId, Record, RecordIdentity, ValueId};
    use crate::ontology::{IdentityKey, Ontology};
    use crate::temporal::Interval;

    #[test]
    fn test_identity_key_index() {
        let mut index = IdentityKeyIndex::new();

        let mut ontology = Ontology::new();
        let name_attr = AttrId(1);
        let email_attr = AttrId(2);

        let identity_key = IdentityKey::new(vec![name_attr, email_attr], "name_email".to_string());
        ontology.add_identity_key(identity_key);

        let record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![
                Descriptor::new(name_attr, ValueId(1), Interval::new(100, 200).unwrap()),
                Descriptor::new(email_attr, ValueId(2), Interval::new(100, 200).unwrap()),
            ],
        );

        index.build(&[record], &ontology).unwrap();

        let key_values = vec![
            KeyValue::new(name_attr, ValueId(1)),
            KeyValue::new(email_attr, ValueId(2)),
        ];

        let matches = index.find_matching_records("person", &key_values);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, RecordId(1));
    }

    #[test]
    fn test_crosswalk_index() {
        let mut index = CrosswalkIndex::new();

        let crosswalk = Crosswalk::new(
            PerspectiveScopedId::new("crm".to_string(), "123".to_string()),
            CanonicalId::new("canonical_123".to_string()),
            Interval::new(100, 200).unwrap(),
        );

        index.build(&[crosswalk]);

        let found = index.find_by_perspective("crm");
        assert_eq!(found.len(), 1);

        let found_by_uid = index.find_by_perspective_uid("crm", "123");
        assert!(found_by_uid.is_some());
    }

    #[test]
    fn test_temporal_index() {
        let mut index = TemporalIndex::new();

        let record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![
                Descriptor::new(AttrId(1), ValueId(1), Interval::new(100, 200).unwrap()),
                Descriptor::new(AttrId(2), ValueId(2), Interval::new(150, 250).unwrap()),
            ],
        );

        index.build(&[record]);

        let overlapping = index.find_overlapping_records(Interval::new(120, 180).unwrap());
        assert_eq!(overlapping.len(), 1);
        assert_eq!(overlapping[0], RecordId(1));
    }

    #[test]
    fn test_index_manager() {
        let mut manager = IndexManager::new();

        let mut ontology = Ontology::new();
        let name_attr = AttrId(1);
        let identity_key = IdentityKey::new(vec![name_attr], "name".to_string());
        ontology.add_identity_key(identity_key);

        let record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![Descriptor::new(
                name_attr,
                ValueId(1),
                Interval::new(100, 200).unwrap(),
            )],
        );

        manager.build_all(&[record], &ontology).unwrap();

        let key_values = vec![KeyValue::new(name_attr, ValueId(1))];
        let matches = manager.find_matching_records_in_interval(
            "person",
            &key_values,
            Interval::new(120, 180).unwrap(),
        );

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], RecordId(1));
    }

    #[test]
    fn test_tiered_index_basic() {
        let mut index = TieredIdentityKeyIndex::new();

        let mut ontology = Ontology::new();
        let name_attr = AttrId(1);
        let identity_key = IdentityKey::new(vec![name_attr], "name".to_string());
        ontology.add_identity_key(identity_key);

        let record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![Descriptor::new(
                name_attr,
                ValueId(1),
                Interval::new(100, 200).unwrap(),
            )],
        );

        index.add_record(&record, &ontology).unwrap();

        let stats = index.tier_stats();
        assert_eq!(stats.hot_keys, 1);
        assert_eq!(stats.warm_keys, 0);
    }

    #[test]
    fn test_tiered_index_bulk_build() {
        let mut index = TieredIdentityKeyIndex::new();

        let mut ontology = Ontology::new();
        let name_attr = AttrId(1);
        let identity_key = IdentityKey::new(vec![name_attr], "name".to_string());
        ontology.add_identity_key(identity_key);

        let records: Vec<Record> = (0..10)
            .map(|i| {
                Record::new(
                    RecordId(i),
                    RecordIdentity::new("person".to_string(), "crm".to_string(), format!("{}", i)),
                    vec![Descriptor::new(
                        name_attr,
                        ValueId(i),
                        Interval::new(100, 200).unwrap(),
                    )],
                )
            })
            .collect();

        index.build(&records, &ontology).unwrap();

        let stats = index.tier_stats();
        assert_eq!(stats.hot_keys, 10);
    }

    #[test]
    fn test_tiered_index_query() {
        let mut index = TieredIdentityKeyIndex::new();
        let mut dsu = DsuBackend::in_memory();

        let mut ontology = Ontology::new();
        let name_attr = AttrId(1);
        let identity_key = IdentityKey::new(vec![name_attr], "name".to_string());
        ontology.add_identity_key(identity_key);

        // Add two records with same key value
        for i in 1..=2 {
            let record = Record::new(
                RecordId(i),
                RecordIdentity::new("person".to_string(), "crm".to_string(), format!("{}", i)),
                vec![Descriptor::new(
                    name_attr,
                    ValueId(1), // Same value
                    Interval::new(100, 200).unwrap(),
                )],
            );
            dsu.add_record(RecordId(i)).unwrap();
            index.add_record(&record, &ontology).unwrap();
        }

        let key_values = vec![KeyValue::new(name_attr, ValueId(1))];
        let results = index.find_matching_clusters_overlapping(
            &mut dsu,
            "person",
            &key_values,
            Interval::new(150, 175).unwrap(),
        );

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_compact_bucket() {
        let mut bucket = CompactBucket::default();

        bucket.insert_record(RecordId(1), Interval::new(100, 200).unwrap());
        bucket.insert_record(RecordId(2), Interval::new(150, 250).unwrap());
        bucket.insert_cluster(RecordId(1), Interval::new(100, 250).unwrap());

        // Test overlapping query
        let results = bucket.find_overlapping_records(Interval::new(175, 225).unwrap());
        assert_eq!(results.len(), 2); // Both records overlap

        let results = bucket.find_overlapping_records(Interval::new(50, 125).unwrap());
        assert_eq!(results.len(), 1); // Only record 1 overlaps

        // Test cluster query
        let cluster_results = bucket.find_overlapping_clusters(Interval::new(175, 225).unwrap());
        assert_eq!(cluster_results.len(), 1);

        // Test cardinality
        assert_eq!(bucket.cardinality(), 2);
    }

    #[test]
    fn test_compact_bucket_from_key_bucket() {
        let mut key_bucket = KeyBucket::default();
        key_bucket.insert_record(RecordId(1), Interval::new(100, 200).unwrap());
        key_bucket.insert_record(RecordId(2), Interval::new(150, 250).unwrap());
        key_bucket.insert_cluster_interval(RecordId(1), Interval::new(100, 200).unwrap());

        let compact = CompactBucket::from_key_bucket(&key_bucket);
        assert_eq!(compact.record_intervals.len(), 2);
        assert!(!compact.cluster_intervals.is_empty());
    }

    #[test]
    fn test_tier_config_profiles() {
        let default = TierConfig::default();
        assert_eq!(default.hot_tier_capacity, 100_000);
        assert_eq!(default.warm_tier_capacity, 100_000);

        let memory_saver = TierConfig::memory_saver();
        assert!(memory_saver.hot_tier_capacity < default.hot_tier_capacity);

        let high_perf = TierConfig::high_performance();
        assert!(high_perf.hot_tier_capacity > default.hot_tier_capacity);
    }

    #[test]
    fn test_compact_bucket_serialization() {
        let mut bucket = CompactBucket::default();
        bucket.insert_record(RecordId(1), Interval::new(100, 200).unwrap());
        bucket.insert_cluster(RecordId(1), Interval::new(100, 200).unwrap());

        let data = bucket.to_data();
        assert_eq!(data.record_intervals.len(), 1);
        assert_eq!(data.cluster_intervals.len(), 1);

        let restored = CompactBucket::from_data(data);
        assert_eq!(restored.record_intervals.len(), 1);
        assert_eq!(restored.cluster_intervals.len(), 1);
    }
}
