//! # Indexing Module
//!
//! Provides efficient indexing for identity keys, crosswalks, and temporal data
//! to enable fast lookup during entity resolution and conflict detection.

use crate::dsu::TemporalDSU;
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
struct KeyBucket {
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

        loop {
            let current_start = self.nodes[current].interval.start;
            self.nodes[current].max_end = self.nodes[current].max_end.max(new_end);

            let next = if new_start < current_start {
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

    fn collect_overlapping(&self, interval: Interval, out: &mut Vec<(RecordId, Interval)>) {
        let Some(root) = self.root else {
            return;
        };
        let mut stack = vec![root];

        while let Some(node_idx) = stack.pop() {
            let node = &self.nodes[node_idx];
            if node.interval.start < interval.end && node.interval.end > interval.start {
                out.push((node.record_id, node.interval));
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

    fn collect_overlapping_clusters(
        &mut self,
        dsu: &mut TemporalDSU,
        interval: Interval,
        out: &mut Vec<(RecordId, Interval)>,
        seen: &mut HashSet<RecordId>,
    ) {
        self.rebuild_tree_if_needed();
        let mut scratch = Vec::new();
        self.cluster_tree
            .collect_overlapping(interval, &mut scratch);
        for (record_id, candidate_interval) in scratch {
            let root = dsu.find(record_id);
            if seen.insert(root) {
                out.push((root, candidate_interval));
            }
        }
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

            let mut value_intervals = Self::coalesce_value_intervals(descriptors);

            if value_intervals.len() > MAX_KEY_VALUES_PER_ATTR {
                value_intervals = Self::prune_value_intervals(value_intervals);
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

    fn coalesce_value_intervals(
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

    fn prune_value_intervals(
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
        dsu: &mut TemporalDSU,
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
            cluster_overlap_buffer,
            ..
        } = self;

        let Some(values) = index.get_mut(&key) else {
            return &[];
        };

        cluster_overlap_buffer.clear();
        self.cluster_seen.clear();
        values.record_candidates.collect_overlapping_clusters(
            dsu,
            interval,
            cluster_overlap_buffer,
            &mut self.cluster_seen,
        );
        cluster_overlap_buffer.as_slice()
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
}
