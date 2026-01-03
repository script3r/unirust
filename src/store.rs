//! # Store Module
//!
//! Provides storage and management for records, with efficient indexing and retrieval.

use crate::model::{AttrId, Record, RecordId, RecordIdentity, StringInterner, ValueId};
use crate::temporal::Interval;
use anyhow::Result;
use hashbrown::HashMap;
use std::collections::BTreeMap;
use std::path::Path;

type AttributeValuePairs = Vec<((AttrId, ValueId), Vec<(RecordId, Interval)>)>;

#[derive(Debug, Clone, Copy)]
pub struct StoreMetrics {
    pub persistent: bool,
    pub running_compactions: u64,
    pub running_flushes: u64,
    pub block_cache_capacity_bytes: u64,
    pub block_cache_usage_bytes: u64,
}

/// Persistence abstraction for records and metadata.
pub trait RecordStore: Send + Sync {
    /// Add a single record and return its assigned ID.
    fn add_record(&mut self, record: Record) -> Result<RecordId>;

    /// Add records to the store.
    fn add_records(&mut self, records: Vec<Record>) -> Result<()> {
        for record in records {
            self.add_record(record)?;
        }
        Ok(())
    }

    /// Get a record by ID.
    fn get_record(&self, id: RecordId) -> Option<Record>;

    /// Get a record ID by identity if present.
    fn get_record_id_by_identity(&self, _identity: &RecordIdentity) -> Option<RecordId> {
        None
    }

    /// Get all records.
    fn get_all_records(&self) -> Vec<Record>;

    /// Apply a function to each record.
    fn for_each_record(&self, f: &mut dyn FnMut(Record)) {
        for record in self.get_all_records() {
            f(record);
        }
    }

    /// Get records for a specific entity type.
    fn get_records_by_entity_type(&self, entity_type: &str) -> Vec<Record>;

    /// Get records for a specific perspective.
    fn get_records_by_perspective(&self, perspective: &str) -> Vec<Record>;

    /// Get records that have descriptors for a specific attribute.
    fn get_records_with_attribute(&self, attr: AttrId) -> Vec<Record>;

    /// Get records that have descriptors overlapping with a time interval.
    fn get_records_in_interval(&self, interval: Interval) -> Vec<Record>;

    /// Get records that have a specific attribute-value pair within a time interval.
    fn get_records_with_value_in_interval(
        &self,
        attr: AttrId,
        value: ValueId,
        interval: Interval,
    ) -> Vec<(RecordId, Interval)> {
        let mut matches = Vec::new();

        self.for_each_record(&mut |record| {
            for descriptor in &record.descriptors {
                if descriptor.attr == attr && descriptor.value == value {
                    if let Some(overlap) =
                        crate::temporal::intersect(&descriptor.interval, &interval)
                    {
                        matches.push((record.id, overlap));
                    }
                }
            }
        });

        matches
    }

    /// Get the string interner.
    fn interner(&self) -> &StringInterner;

    /// Get a mutable reference to the string interner.
    fn interner_mut(&mut self) -> &mut StringInterner;

    /// Intern an attribute string.
    fn intern_attr(&mut self, attr: &str) -> AttrId {
        self.interner_mut().intern_attr(attr)
    }

    /// Intern a value string.
    fn intern_value(&mut self, value: &str) -> ValueId {
        self.interner_mut().intern_value(value)
    }

    /// Resolve an attribute ID to its string.
    fn resolve_attr(&self, id: AttrId) -> Option<String> {
        self.interner().get_attr(id).cloned()
    }

    /// Resolve a value ID to its string.
    fn resolve_value(&self, id: ValueId) -> Option<String> {
        self.interner().get_value(id).cloned()
    }

    /// Get the number of records.
    fn len(&self) -> usize;

    /// Persist the current cluster count if supported.
    fn set_cluster_count(&mut self, _count: usize) -> Result<()> {
        Ok(())
    }

    /// Load the persisted cluster count if supported.
    fn cluster_count(&self) -> Option<usize> {
        None
    }

    /// Persist conflict summaries if supported.
    fn set_conflict_summaries(
        &mut self,
        _summaries: &[crate::conflicts::ConflictSummary],
    ) -> Result<()> {
        Ok(())
    }

    /// Persist conflict summaries for a specific cluster if supported.
    fn set_cluster_conflict_summaries(
        &mut self,
        _cluster_id: crate::model::ClusterId,
        _summaries: &[crate::conflicts::ConflictSummary],
    ) -> Result<()> {
        Ok(())
    }

    /// Load all persisted conflict summaries if supported.
    fn load_conflict_summaries(&self) -> Option<Vec<crate::conflicts::ConflictSummary>> {
        None
    }

    /// Load conflict summary count if supported.
    fn conflict_summary_count(&self) -> Option<usize> {
        None
    }

    /// Persist a record -> cluster assignment if supported.
    fn set_cluster_assignment(
        &mut self,
        _record_id: RecordId,
        _cluster_id: crate::model::ClusterId,
    ) -> Result<()> {
        Ok(())
    }

    /// Check if the store is empty.
    fn is_empty(&self) -> bool;

    /// Get records in an ID range [start, end), limited to max_results.
    fn records_in_id_range(
        &self,
        start: RecordId,
        end: RecordId,
        max_results: usize,
    ) -> Vec<Record>;

    /// Get min/max record IDs if any records exist.
    fn record_id_bounds(&self) -> Option<(RecordId, RecordId)>;

    /// Create a checkpoint at the provided path, if supported.
    fn checkpoint(&self, _path: &Path) -> Result<()> {
        Err(anyhow::anyhow!("checkpoint not supported for this store"))
    }

    /// Add a record if its identity has not been seen; returns (id, inserted).
    fn add_record_if_absent(&mut self, record: Record) -> Result<(RecordId, bool)> {
        let id = self.add_record(record)?;
        Ok((id, true))
    }

    /// Optional store-level metrics.
    fn metrics(&self) -> Option<StoreMetrics> {
        None
    }
}

/// Main in-memory storage for records and metadata
#[derive(Debug, Clone)]
pub struct Store {
    /// All records indexed by ID
    records: HashMap<RecordId, Record>,
    /// Identity to record ID mapping (idempotent ingest)
    identity_index: HashMap<RecordIdentity, RecordId>,
    /// String interner for attributes and values
    interner: StringInterner,
    /// Attribute-value index for fast lookups
    attribute_value_index: AttributeValueIndex,
    /// Temporal index for interval queries
    temporal_index: TemporalIndex,
    /// Next available record ID
    next_record_id: u32,
}

impl Store {
    /// Create a new store
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
            identity_index: HashMap::new(),
            interner: StringInterner::new(),
            attribute_value_index: AttributeValueIndex::new(),
            temporal_index: TemporalIndex::new(),
            next_record_id: 0,
        }
    }

    /// Create a new store with a preloaded interner and record ID counter.
    pub fn with_interner(interner: StringInterner, next_record_id: u32) -> Self {
        Self {
            records: HashMap::new(),
            identity_index: HashMap::new(),
            interner,
            attribute_value_index: AttributeValueIndex::new(),
            temporal_index: TemporalIndex::new(),
            next_record_id,
        }
    }

    fn intern_record(&mut self, record: &mut Record) {
        for descriptor in &mut record.descriptors {
            if self.interner.get_attr(descriptor.attr).is_none() {
                descriptor.attr = self.interner.intern_attr("unknown");
            }
            if self.interner.get_value(descriptor.value).is_none() {
                descriptor.value = self.interner.intern_value("unknown");
            }
        }
    }

    /// Prepare a record for persistence without storing it in memory.
    pub fn prepare_record(&mut self, record: &mut Record) -> Result<RecordId> {
        self.intern_record(record);

        if record.id.0 == 0 {
            record.id = RecordId(self.next_record_id);
            self.next_record_id += 1;
        } else {
            self.next_record_id = self.next_record_id.max(record.id.0 + 1);
        }

        Ok(record.id)
    }

    /// Add a single record to the store and return its assigned ID.
    pub fn add_record(&mut self, mut record: Record) -> Result<RecordId> {
        let record_id = self.prepare_record(&mut record)?;

        let identity = record.identity.clone();
        self.records.insert(record.id, record);
        self.identity_index.insert(identity, record_id);
        if let Some(stored) = self.records.get(&record_id) {
            self.attribute_value_index.add_record(stored);
            self.temporal_index.add_record(stored);
        }
        Ok(record_id)
    }

    /// Insert a record with an explicit ID without assigning a new one.
    pub fn insert_record(&mut self, mut record: Record) -> Result<RecordId> {
        self.intern_record(&mut record);

        self.next_record_id = self.next_record_id.max(record.id.0 + 1);

        let record_id = record.id;
        let identity = record.identity.clone();
        self.records.insert(record.id, record);
        self.identity_index.insert(identity, record_id);
        if let Some(stored) = self.records.get(&record_id) {
            self.attribute_value_index.add_record(stored);
            self.temporal_index.add_record(stored);
        }
        Ok(record_id)
    }

    /// Add records to the store
    pub fn add_records(&mut self, records: Vec<Record>) -> Result<()> {
        for record in records {
            self.add_record(record)?;
        }
        Ok(())
    }

    /// Add a record if its identity is new; returns (id, inserted).
    pub fn add_record_if_absent(&mut self, record: Record) -> Result<(RecordId, bool)> {
        if let Some(existing) = self.get_record_id_by_identity(&record.identity) {
            return Ok((existing, false));
        }
        let id = self.add_record(record)?;
        Ok((id, true))
    }

    /// Get a record by ID
    pub fn get_record(&self, id: RecordId) -> Option<Record> {
        self.records.get(&id).cloned()
    }

    pub fn get_record_id_by_identity(&self, identity: &RecordIdentity) -> Option<RecordId> {
        self.identity_index.get(identity).copied()
    }

    /// Get all records
    pub fn get_all_records(&self) -> Vec<Record> {
        self.records.values().cloned().collect()
    }

    /// Get records for a specific entity type
    pub fn get_records_by_entity_type(&self, entity_type: &str) -> Vec<Record> {
        self.records
            .values()
            .filter(|record| record.identity.entity_type == entity_type)
            .cloned()
            .collect()
    }

    /// Get records for a specific perspective
    pub fn get_records_by_perspective(&self, perspective: &str) -> Vec<Record> {
        self.records
            .values()
            .filter(|record| record.identity.perspective == perspective)
            .cloned()
            .collect()
    }

    /// Get records that have descriptors for a specific attribute
    pub fn get_records_with_attribute(&self, attr: AttrId) -> Vec<Record> {
        self.records
            .values()
            .filter(|record| record.descriptors.iter().any(|d| d.attr == attr))
            .cloned()
            .collect()
    }

    /// Get records that have descriptors overlapping with a time interval
    pub fn get_records_in_interval(&self, interval: Interval) -> Vec<Record> {
        self.records
            .values()
            .filter(|record| {
                record
                    .descriptors
                    .iter()
                    .any(|d| crate::temporal::is_overlapping(&d.interval, &interval))
            })
            .cloned()
            .collect()
    }

    /// Get the string interner
    pub fn interner(&self) -> &StringInterner {
        &self.interner
    }

    /// Get a mutable reference to the string interner
    pub fn interner_mut(&mut self) -> &mut StringInterner {
        &mut self.interner
    }

    /// Get the next record ID.
    pub fn next_record_id(&self) -> u32 {
        self.next_record_id
    }

    /// Set the next record ID.
    pub fn set_next_record_id(&mut self, next_record_id: u32) {
        self.next_record_id = next_record_id;
    }

    /// Get the number of records
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get records in an ID range [start, end), limited to max_results.
    pub fn records_in_id_range(
        &self,
        start: RecordId,
        end: RecordId,
        max_results: usize,
    ) -> Vec<Record> {
        let mut records: Vec<Record> = self
            .records
            .values()
            .filter(|record| record.id >= start && record.id < end)
            .cloned()
            .collect();
        records.sort_by_key(|record| record.id);
        if max_results > 0 && records.len() > max_results {
            records.truncate(max_results);
        }
        records
    }

    /// Get min/max record IDs if any records exist.
    pub fn record_id_bounds(&self) -> Option<(RecordId, RecordId)> {
        let mut iter = self.records.keys();
        let first = iter.next().copied()?;
        let (min_id, max_id) = self
            .records
            .keys()
            .fold((first, first), |(min_id, max_id), id| {
                (std::cmp::min(min_id, *id), std::cmp::max(max_id, *id))
            });
        Some((min_id, max_id))
    }

    pub fn store_metrics(&self) -> StoreMetrics {
        StoreMetrics {
            persistent: false,
            running_compactions: 0,
            running_flushes: 0,
            block_cache_capacity_bytes: 0,
            block_cache_usage_bytes: 0,
        }
    }

    /// Get records that have a specific attribute-value pair in a time interval.
    pub fn get_records_with_value_in_interval(
        &self,
        attr: AttrId,
        value: ValueId,
        interval: Interval,
    ) -> Vec<(RecordId, Interval)> {
        self.attribute_value_index
            .get_records_with_value_in_interval(attr, value, interval)
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordStore for Store {
    fn add_record(&mut self, record: Record) -> Result<RecordId> {
        Store::add_record(self, record)
    }

    fn add_records(&mut self, records: Vec<Record>) -> Result<()> {
        Store::add_records(self, records)
    }

    fn get_record(&self, id: RecordId) -> Option<Record> {
        Store::get_record(self, id)
    }

    fn get_record_id_by_identity(&self, identity: &RecordIdentity) -> Option<RecordId> {
        Store::get_record_id_by_identity(self, identity)
    }

    fn get_all_records(&self) -> Vec<Record> {
        Store::get_all_records(self)
    }

    fn for_each_record(&self, f: &mut dyn FnMut(Record)) {
        for record in self.records.values() {
            f(record.clone());
        }
    }

    fn get_records_by_entity_type(&self, entity_type: &str) -> Vec<Record> {
        Store::get_records_by_entity_type(self, entity_type)
    }

    fn get_records_by_perspective(&self, perspective: &str) -> Vec<Record> {
        Store::get_records_by_perspective(self, perspective)
    }

    fn get_records_with_attribute(&self, attr: AttrId) -> Vec<Record> {
        Store::get_records_with_attribute(self, attr)
    }

    fn get_records_in_interval(&self, interval: Interval) -> Vec<Record> {
        Store::get_records_in_interval(self, interval)
    }

    fn get_records_with_value_in_interval(
        &self,
        attr: AttrId,
        value: ValueId,
        interval: Interval,
    ) -> Vec<(RecordId, Interval)> {
        Store::get_records_with_value_in_interval(self, attr, value, interval)
    }

    fn interner(&self) -> &StringInterner {
        Store::interner(self)
    }

    fn interner_mut(&mut self) -> &mut StringInterner {
        Store::interner_mut(self)
    }

    fn len(&self) -> usize {
        Store::len(self)
    }

    fn is_empty(&self) -> bool {
        Store::is_empty(self)
    }

    fn records_in_id_range(
        &self,
        start: RecordId,
        end: RecordId,
        max_results: usize,
    ) -> Vec<Record> {
        Store::records_in_id_range(self, start, end, max_results)
    }

    fn record_id_bounds(&self) -> Option<(RecordId, RecordId)> {
        Store::record_id_bounds(self)
    }

    fn metrics(&self) -> Option<StoreMetrics> {
        Some(self.store_metrics())
    }

    fn add_record_if_absent(&mut self, record: Record) -> Result<(RecordId, bool)> {
        Store::add_record_if_absent(self, record)
    }
}

/// Index for efficient lookup of records by attribute-value pairs
#[derive(Debug, Clone)]
pub struct AttributeValueIndex {
    /// Maps (attribute, value) -> list of (record_id, interval)
    index: HashMap<(AttrId, ValueId), Vec<(RecordId, Interval)>>,
}

impl AttributeValueIndex {
    /// Create a new index
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    /// Build the index from a store
    pub fn from_store(store: &dyn RecordStore) -> Self {
        let mut index = Self::new();
        index.build(store);
        index
    }

    /// Build the index from a store
    pub fn build(&mut self, store: &dyn RecordStore) {
        self.index.clear();

        store.for_each_record(&mut |record| {
            self.add_record(&record);
        });
    }

    pub fn add_record(&mut self, record: &Record) {
        for descriptor in &record.descriptors {
            let key = (descriptor.attr, descriptor.value);
            self.index
                .entry(key)
                .or_default()
                .push((record.id, descriptor.interval));
        }
    }

    /// Get records that have a specific attribute-value pair
    pub fn get_records_with_value(
        &self,
        attr: AttrId,
        value: ValueId,
    ) -> Vec<(RecordId, Interval)> {
        self.index.get(&(attr, value)).cloned().unwrap_or_default()
    }

    /// Get records that have a specific attribute-value pair in a time interval
    pub fn get_records_with_value_in_interval(
        &self,
        attr: AttrId,
        value: ValueId,
        interval: Interval,
    ) -> Vec<(RecordId, Interval)> {
        self.index
            .get(&(attr, value))
            .map(|records| {
                records
                    .iter()
                    .filter(|(_, record_interval)| {
                        crate::temporal::is_overlapping(record_interval, &interval)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all attribute-value pairs
    pub fn get_all_pairs(&self) -> AttributeValuePairs {
        self.index.iter().map(|(k, v)| (*k, v.clone())).collect()
    }
}

impl Default for AttributeValueIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Index for efficient lookup of records by time intervals
#[derive(Debug, Clone)]
pub struct TemporalIndex {
    /// Maps time intervals to record IDs
    /// Using BTreeMap for ordered iteration
    index: BTreeMap<Interval, Vec<RecordId>>,
}

impl TemporalIndex {
    /// Create a new temporal index
    pub fn new() -> Self {
        Self {
            index: BTreeMap::new(),
        }
    }

    /// Build the index from a store
    pub fn from_store(store: &dyn RecordStore) -> Self {
        let mut index = Self::new();
        index.build(store);
        index
    }

    /// Build the index from a store
    pub fn build(&mut self, store: &dyn RecordStore) {
        self.index.clear();

        store.for_each_record(&mut |record| {
            self.add_record(&record);
        });
    }

    pub fn add_record(&mut self, record: &Record) {
        for descriptor in &record.descriptors {
            self.index
                .entry(descriptor.interval)
                .or_default()
                .push(record.id);
        }
    }

    /// Get records that have descriptors in a time interval
    pub fn get_records_in_interval(&self, interval: Interval) -> Vec<RecordId> {
        let mut result = Vec::new();

        for (index_interval, record_ids) in &self.index {
            if crate::temporal::is_overlapping(index_interval, &interval) {
                result.extend(record_ids);
            }
        }

        result.sort();
        result.dedup();
        result
    }

    /// Get all time intervals
    pub fn get_all_intervals(&self) -> Vec<Interval> {
        self.index.keys().cloned().collect()
    }
}

impl Default for TemporalIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Descriptor, RecordIdentity};
    use crate::temporal::Interval;

    #[test]
    fn test_store_creation() {
        let store = Store::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_add_records() {
        let mut store = Store::new();

        let record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![],
        );

        store.add_records(vec![record]).unwrap();
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_add_record_if_absent_dedupes_identity() {
        let mut store = Store::new();

        let record_a = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![],
        );
        let record_b = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![],
        );

        let (first_id, inserted) = store.add_record_if_absent(record_a).unwrap();
        assert!(inserted);
        let (second_id, inserted) = store.add_record_if_absent(record_b).unwrap();
        assert!(!inserted);
        assert_eq!(first_id, second_id);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_get_records_by_entity_type() {
        let mut store = Store::new();

        let person_record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![],
        );

        let org_record = Record::new(
            RecordId(2),
            RecordIdentity::new(
                "organization".to_string(),
                "crm".to_string(),
                "456".to_string(),
            ),
            vec![],
        );

        store.add_records(vec![person_record, org_record]).unwrap();

        let person_records = store.get_records_by_entity_type("person");
        assert_eq!(person_records.len(), 1);

        let org_records = store.get_records_by_entity_type("organization");
        assert_eq!(org_records.len(), 1);
    }

    #[test]
    fn test_attribute_value_index() {
        let mut store = Store::new();
        let mut interner = StringInterner::new();

        let name_attr = interner.intern_attr("name");
        let name_value = interner.intern_value("John Doe");

        let descriptor = Descriptor::new(name_attr, name_value, Interval::new(100, 200).unwrap());

        let record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![descriptor],
        );

        store.add_records(vec![record]).unwrap();

        let index = AttributeValueIndex::from_store(&store);
        let records = index.get_records_with_value(name_attr, name_value);
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_store_value_interval_lookup() {
        let mut store = Store::new();
        let email_attr = store.interner_mut().intern_attr("email");
        let email_value = store.interner_mut().intern_value("alice@example.com");

        let descriptor = Descriptor::new(email_attr, email_value, Interval::new(10, 20).unwrap());

        let record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![descriptor],
        );

        store.add_records(vec![record]).unwrap();

        let matches = store.get_records_with_value_in_interval(
            email_attr,
            email_value,
            Interval::new(0, 15).unwrap(),
        );
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, RecordId(1));
    }

    #[test]
    fn test_temporal_index() {
        let mut store = Store::new();

        let descriptor = Descriptor::new(AttrId(1), ValueId(1), Interval::new(100, 200).unwrap());

        let record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            vec![descriptor],
        );

        store.add_records(vec![record]).unwrap();

        let index = TemporalIndex::from_store(&store);
        let records = index.get_records_in_interval(Interval::new(150, 180).unwrap());
        assert_eq!(records.len(), 1);
    }
}
