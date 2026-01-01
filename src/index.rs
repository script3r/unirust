//! # Indexing Module
//!
//! Provides efficient indexing for identity keys, crosswalks, and temporal data
//! to enable fast lookup during entity resolution and conflict detection.

use crate::model::{CanonicalId, KeyValue, RecordId};
use crate::ontology::{Crosswalk, IdentityKey};
use crate::temporal::Interval;
use anyhow::Result;
use hashbrown::{Equivalent, HashMap};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

type IdentityIndexMap = HashMap<IdentityIndexKey, CandidateList>;

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
    entries: Vec<(RecordId, Interval)>,
}

impl CandidateList {
    fn insert(&mut self, entry: (RecordId, Interval)) {
        self.entries.push(entry);
    }

    fn as_slice(&self) -> &[(RecordId, Interval)] {
        self.entries.as_slice()
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
}

impl IdentityKeyIndex {
    /// Create a new identity key index
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
            record_keys: HashMap::new(),
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
                        self.index
                            .entry(key)
                            .or_default()
                            .insert((record.id, interval));
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

            if partials.is_empty() {
                for descriptor in descriptors {
                    partials.push((
                        vec![KeyValue::new(*attr, descriptor.value)],
                        descriptor.interval,
                    ));
                }
                continue;
            }

            let mut next = Vec::new();
            for (key_values, interval) in &partials {
                for descriptor in &descriptors {
                    if let Some(overlap) =
                        crate::temporal::intersect(interval, &descriptor.interval)
                    {
                        let mut next_key_values = key_values.clone();
                        next_key_values.push(KeyValue::new(*attr, descriptor.value));
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

    /// Add a single record to the index.
    pub fn add_record(
        &mut self,
        record: &crate::model::Record,
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
                    self.index
                        .entry(key)
                        .or_default()
                        .insert((record.id, interval));
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
        &self,
        entity_type: &str,
        key_values: &[KeyValue],
    ) -> &[(RecordId, Interval)] {
        let key = IdentityIndexKeyRef {
            entity_type,
            key_values,
        };
        self.index
            .get(&key)
            .map(|values| values.as_slice())
            .unwrap_or(&[])
    }

    /// Find records that could overlap before the interval end.

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
        &self,
        entity_type: &str,
        key_values: &[KeyValue],
        interval: Interval,
    ) -> Vec<RecordId> {
        let matching_records = self
            .identity_key_index
            .find_matching_records(entity_type, key_values);

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
