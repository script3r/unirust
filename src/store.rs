//! # Store Module
//!
//! Provides storage and management for records, with efficient indexing and retrieval.

use crate::model::{AttrId, Record, RecordId, StringInterner, ValueId};
use crate::temporal::Interval;
use anyhow::Result;
use hashbrown::HashMap;
use std::collections::BTreeMap;

type AttributeValuePairs = Vec<((AttrId, ValueId), Vec<(RecordId, Interval)>)>;

/// Main storage for records and metadata
#[derive(Debug, Clone)]
pub struct Store {
    /// All records indexed by ID
    records: HashMap<RecordId, Record>,
    /// String interner for attributes and values
    interner: StringInterner,
    /// Next available record ID
    next_record_id: u32,
}

impl Store {
    /// Create a new store
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
            interner: StringInterner::new(),
            next_record_id: 0,
        }
    }

    /// Add records to the store
    pub fn add_records(&mut self, records: Vec<Record>) -> Result<()> {
        for mut record in records {
            // Intern all attribute and value strings
            for descriptor in &mut record.descriptors {
                let attr_str = self
                    .interner
                    .get_attr(descriptor.attr)
                    .unwrap_or(&"unknown".to_string())
                    .clone();
                descriptor.attr = self.interner.intern_attr(&attr_str);

                let value_str = self
                    .interner
                    .get_value(descriptor.value)
                    .unwrap_or(&"unknown".to_string())
                    .clone();
                descriptor.value = self.interner.intern_value(&value_str);
            }

            // Assign a new record ID if not already set
            if record.id.0 == 0 {
                record.id = RecordId(self.next_record_id);
                self.next_record_id += 1;
            }

            self.records.insert(record.id, record);
        }
        Ok(())
    }

    /// Get a record by ID
    pub fn get_record(&self, id: RecordId) -> Option<&Record> {
        self.records.get(&id)
    }

    /// Get all records
    pub fn get_all_records(&self) -> Vec<&Record> {
        self.records.values().collect()
    }

    /// Get records for a specific entity type
    pub fn get_records_by_entity_type(&self, entity_type: &str) -> Vec<&Record> {
        self.records
            .values()
            .filter(|record| record.identity.entity_type == entity_type)
            .collect()
    }

    /// Get records for a specific perspective
    pub fn get_records_by_perspective(&self, perspective: &str) -> Vec<&Record> {
        self.records
            .values()
            .filter(|record| record.identity.perspective == perspective)
            .collect()
    }

    /// Get records that have descriptors for a specific attribute
    pub fn get_records_with_attribute(&self, attr: AttrId) -> Vec<&Record> {
        self.records
            .values()
            .filter(|record| record.descriptors.iter().any(|d| d.attr == attr))
            .collect()
    }

    /// Get records that have descriptors overlapping with a time interval
    pub fn get_records_in_interval(&self, interval: Interval) -> Vec<&Record> {
        self.records
            .values()
            .filter(|record| {
                record
                    .descriptors
                    .iter()
                    .any(|d| crate::temporal::is_overlapping(&d.interval, &interval))
            })
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

    /// Get the number of records
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
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
    pub fn from_store(store: &Store) -> Self {
        let mut index = Self::new();
        index.build(store);
        index
    }

    /// Build the index from a store
    pub fn build(&mut self, store: &Store) {
        self.index.clear();

        for record in store.get_all_records() {
            for descriptor in &record.descriptors {
                let key = (descriptor.attr, descriptor.value);
                self.index
                    .entry(key)
                    .or_default()
                    .push((record.id, descriptor.interval));
            }
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
    pub fn from_store(store: &Store) -> Self {
        let mut index = Self::new();
        index.build(store);
        index
    }

    /// Build the index from a store
    pub fn build(&mut self, store: &Store) {
        self.index.clear();

        for record in store.get_all_records() {
            for descriptor in &record.descriptors {
                self.index
                    .entry(descriptor.interval)
                    .or_default()
                    .push(record.id);
            }
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
