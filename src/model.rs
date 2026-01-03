//! # Data Model
//!
//! Core data structures for entity mastering and conflict resolution.
//! Includes record identification, descriptors, and string interning for efficiency.

use crate::temporal::Interval;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
// use string_interner::{DefaultBackend, StringInterner as ExternalStringInterner};

/// Compact identifier for records
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RecordId(pub u32);

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "R{}", self.0)
    }
}

/// Compact identifier for clusters (local to a shard)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub u32);

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "C{}", self.0)
    }
}

/// Global cluster identifier for distributed entity resolution.
///
/// Encodes shard ownership, local cluster ID, and merge version in a single 64-bit value.
/// Format: `(shard_id << 48) | (version << 32) | local_id`
///
/// This enables:
/// - Tracking which shard owns a cluster
/// - Detecting stale references after cross-shard merges
/// - Efficient comparison and hashing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlobalClusterId {
    /// The shard that owns this cluster
    pub shard_id: u16,
    /// Local cluster ID within the shard
    pub local_id: u32,
    /// Merge version (incremented on cross-shard merges)
    pub version: u16,
}

impl GlobalClusterId {
    /// Create a new global cluster ID
    pub fn new(shard_id: u16, local_id: u32, version: u16) -> Self {
        Self {
            shard_id,
            local_id,
            version,
        }
    }

    /// Create from a local cluster ID on a specific shard
    pub fn from_local(shard_id: u16, local_id: ClusterId) -> Self {
        Self {
            shard_id,
            local_id: local_id.0,
            version: 0,
        }
    }

    /// Encode as a 64-bit integer for efficient storage/comparison
    /// Format: `(shard_id << 48) | (version << 32) | local_id`
    pub fn to_u64(&self) -> u64 {
        ((self.shard_id as u64) << 48) | ((self.version as u64) << 32) | (self.local_id as u64)
    }

    /// Decode from a 64-bit integer
    pub fn from_u64(value: u64) -> Self {
        Self {
            shard_id: ((value >> 48) & 0xFFFF) as u16,
            version: ((value >> 32) & 0xFFFF) as u16,
            local_id: (value & 0xFFFFFFFF) as u32,
        }
    }

    /// Encode as bytes for storage
    pub fn to_bytes(&self) -> [u8; 8] {
        self.to_u64().to_be_bytes()
    }

    /// Decode from bytes
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self::from_u64(u64::from_be_bytes(bytes))
    }

    /// Get the local cluster ID
    pub fn local_cluster_id(&self) -> ClusterId {
        ClusterId(self.local_id)
    }

    /// Create a new version of this cluster (after a merge)
    pub fn with_new_version(&self, new_version: u16) -> Self {
        Self {
            shard_id: self.shard_id,
            local_id: self.local_id,
            version: new_version,
        }
    }

    /// Create with a new owner shard (for cross-shard merge)
    pub fn with_new_owner(&self, new_shard_id: u16, new_version: u16) -> Self {
        Self {
            shard_id: new_shard_id,
            local_id: self.local_id,
            version: new_version,
        }
    }

    /// Check if this cluster belongs to a specific shard
    pub fn is_owned_by(&self, shard_id: u16) -> bool {
        self.shard_id == shard_id
    }
}

impl fmt::Display for GlobalClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GC{}.{}.v{}", self.shard_id, self.local_id, self.version)
    }
}

impl From<GlobalClusterId> for u64 {
    fn from(id: GlobalClusterId) -> u64 {
        id.to_u64()
    }
}

impl From<u64> for GlobalClusterId {
    fn from(value: u64) -> GlobalClusterId {
        GlobalClusterId::from_u64(value)
    }
}

/// Compact identifier for attributes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AttrId(pub u32);

impl fmt::Display for AttrId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "A{}", self.0)
    }
}

/// Compact identifier for values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ValueId(pub u32);

impl fmt::Display for ValueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "V{}", self.0)
    }
}

/// Represents the identity of a record from a specific perspective
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecordIdentity {
    /// The type of entity (e.g., "person", "organization", "product")
    pub entity_type: String,
    /// The perspective or source system (e.g., "crm", "erp", "web")
    pub perspective: String,
    /// Unique identifier within the perspective
    pub uid: String,
}

impl RecordIdentity {
    /// Create a new record identity
    pub fn new(entity_type: String, perspective: String, uid: String) -> Self {
        Self {
            entity_type,
            perspective,
            uid,
        }
    }
}

impl fmt::Display for RecordIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.entity_type, self.perspective, self.uid)
    }
}

/// A temporal descriptor with attribute, value, and time interval
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Descriptor {
    /// The attribute being described
    pub attr: AttrId,
    /// The value of the attribute
    pub value: ValueId,
    /// The time interval when this descriptor is valid
    pub interval: Interval,
}

impl Descriptor {
    /// Create a new descriptor
    pub fn new(attr: AttrId, value: ValueId, interval: Interval) -> Self {
        Self {
            attr,
            value,
            interval,
        }
    }
}

/// A record containing an identity and temporal descriptors
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// Unique identifier for this record
    pub id: RecordId,
    /// The identity of the record
    pub identity: RecordIdentity,
    /// Temporal descriptors for this record
    pub descriptors: Vec<Descriptor>,
}

impl Record {
    /// Create a new record
    pub fn new(id: RecordId, identity: RecordIdentity, descriptors: Vec<Descriptor>) -> Self {
        Self {
            id,
            identity,
            descriptors,
        }
    }

    /// Get descriptors for a specific attribute
    pub fn descriptors_for_attr(&self, attr: AttrId) -> Vec<&Descriptor> {
        self.descriptors.iter().filter(|d| d.attr == attr).collect()
    }

    /// Get descriptors that overlap with a given interval
    pub fn descriptors_in_interval(&self, interval: Interval) -> Vec<&Descriptor> {
        self.descriptors
            .iter()
            .filter(|d| crate::temporal::is_overlapping(&d.interval, &interval))
            .collect()
    }
}

/// String interner for efficient storage of attributes and values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringInterner {
    attr_to_id: HashMap<String, AttrId>,
    value_to_id: HashMap<String, ValueId>,
    id_to_attr: HashMap<AttrId, String>,
    id_to_value: HashMap<ValueId, String>,
    next_attr_id: u32,
    next_value_id: u32,
}

impl StringInterner {
    /// Create a new string interner
    pub fn new() -> Self {
        Self {
            attr_to_id: HashMap::new(),
            value_to_id: HashMap::new(),
            id_to_attr: HashMap::new(),
            id_to_value: HashMap::new(),
            next_attr_id: 0,
            next_value_id: 0,
        }
    }

    /// Intern an attribute string and return its ID
    pub fn intern_attr(&mut self, attr: &str) -> AttrId {
        if let Some(&id) = self.attr_to_id.get(attr) {
            return id;
        }

        let id = AttrId(self.next_attr_id);
        self.next_attr_id += 1;

        self.attr_to_id.insert(attr.to_string(), id);
        self.id_to_attr.insert(id, attr.to_string());

        id
    }

    /// Intern a value string and return its ID
    pub fn intern_value(&mut self, value: &str) -> ValueId {
        if let Some(&id) = self.value_to_id.get(value) {
            return id;
        }

        let id = ValueId(self.next_value_id);
        self.next_value_id += 1;

        self.value_to_id.insert(value.to_string(), id);
        self.id_to_value.insert(id, value.to_string());

        id
    }

    /// Get the string for an attribute ID
    pub fn get_attr(&self, id: AttrId) -> Option<&String> {
        self.id_to_attr.get(&id)
    }

    /// Get the string for a value ID
    pub fn get_value(&self, id: ValueId) -> Option<&String> {
        self.id_to_value.get(&id)
    }

    pub fn get_attr_id(&self, attr: &str) -> Option<AttrId> {
        self.attr_to_id.get(attr).copied()
    }

    pub fn get_value_id(&self, value: &str) -> Option<ValueId> {
        self.value_to_id.get(value).copied()
    }

    /// Get the next attribute ID.
    pub fn next_attr_id(&self) -> u32 {
        self.next_attr_id
    }

    /// Get the next value ID.
    pub fn next_value_id(&self) -> u32 {
        self.next_value_id
    }

    pub fn set_next_attr_id(&mut self, next: u32) {
        self.next_attr_id = self.next_attr_id.max(next);
    }

    pub fn set_next_value_id(&mut self, next: u32) {
        self.next_value_id = self.next_value_id.max(next);
    }

    /// Insert an attribute with a specific ID (used for persistence restores).
    pub fn insert_attr_with_id(&mut self, id: AttrId, attr: String) {
        self.attr_to_id.insert(attr.clone(), id);
        self.id_to_attr.insert(id, attr);
        self.next_attr_id = self.next_attr_id.max(id.0 + 1);
    }

    /// Insert a value with a specific ID (used for persistence restores).
    pub fn insert_value_with_id(&mut self, id: ValueId, value: String) {
        self.value_to_id.insert(value.clone(), id);
        self.id_to_value.insert(id, value);
        self.next_value_id = self.next_value_id.max(id.0 + 1);
    }

    /// Get all attribute IDs
    pub fn attr_ids(&self) -> impl Iterator<Item = AttrId> + '_ {
        self.id_to_attr.keys().copied()
    }

    /// Get all value IDs
    pub fn value_ids(&self) -> impl Iterator<Item = ValueId> + '_ {
        self.id_to_value.keys().copied()
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

/// A key-value pair for identity matching
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyValue {
    pub attr: AttrId,
    pub value: ValueId,
}

impl KeyValue {
    /// Create a new key-value pair
    pub fn new(attr: AttrId, value: ValueId) -> Self {
        Self { attr, value }
    }
}

/// A set of key-value pairs that form an identity key
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdentityKey {
    pub key_values: Vec<KeyValue>,
}

impl IdentityKey {
    /// Create a new identity key
    pub fn new(key_values: Vec<KeyValue>) -> Self {
        Self { key_values }
    }

    /// Check if this identity key matches another over an overlapping interval
    pub fn matches(&self, other: &IdentityKey, _interval: Interval) -> bool {
        if self.key_values.len() != other.key_values.len() {
            return false;
        }

        // Check that all key-value pairs match
        for kv in &self.key_values {
            if !other.key_values.contains(kv) {
                return false;
            }
        }

        true
    }

    /// Get the attributes in this identity key
    pub fn attributes(&self) -> Vec<AttrId> {
        self.key_values.iter().map(|kv| kv.attr).collect()
    }
}

/// A perspective-scoped identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PerspectiveScopedId {
    pub perspective: String,
    pub uid: String,
}

impl PerspectiveScopedId {
    /// Create a new perspective-scoped ID
    pub fn new(perspective: String, uid: String) -> Self {
        Self { perspective, uid }
    }
}

/// A canonical identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CanonicalId {
    pub value: String,
}

impl CanonicalId {
    /// Create a new canonical ID
    pub fn new(value: String) -> Self {
        Self { value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporal::Interval;

    #[test]
    fn test_record_creation() {
        let id = RecordId(1);
        let identity =
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string());
        let descriptors = vec![];
        let record = Record::new(id, identity, descriptors);

        assert_eq!(record.id, id);
        assert_eq!(record.identity.entity_type, "person");
        assert_eq!(record.identity.perspective, "crm");
        assert_eq!(record.identity.uid, "123");
    }

    #[test]
    fn test_string_interner() {
        let mut interner = StringInterner::new();

        let attr1 = interner.intern_attr("name");
        let attr2 = interner.intern_attr("email");
        let attr1_again = interner.intern_attr("name");

        assert_eq!(attr1, attr1_again);
        assert_ne!(attr1, attr2);

        assert_eq!(interner.get_attr(attr1), Some(&"name".to_string()));
        assert_eq!(interner.get_attr(attr2), Some(&"email".to_string()));
    }

    #[test]
    fn test_identity_key_matching() {
        let mut interner = StringInterner::new();
        let name_attr = interner.intern_attr("name");
        let email_attr = interner.intern_attr("email");
        let name_value = interner.intern_value("John Doe");
        let email_value = interner.intern_value("john@example.com");

        let key1 = IdentityKey::new(vec![
            KeyValue::new(name_attr, name_value),
            KeyValue::new(email_attr, email_value),
        ]);

        let key2 = IdentityKey::new(vec![
            KeyValue::new(email_attr, email_value),
            KeyValue::new(name_attr, name_value),
        ]);

        let interval = Interval::new(100, 200).unwrap();
        assert!(key1.matches(&key2, interval));
    }

    #[test]
    fn test_descriptor_filtering() {
        let mut interner = StringInterner::new();
        let name_attr = interner.intern_attr("name");
        let email_attr = interner.intern_attr("email");
        let name_value = interner.intern_value("John Doe");
        let email_value = interner.intern_value("john@example.com");

        let descriptors = vec![
            Descriptor::new(name_attr, name_value, Interval::new(100, 200).unwrap()),
            Descriptor::new(email_attr, email_value, Interval::new(150, 250).unwrap()),
        ];

        let record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "123".to_string()),
            descriptors,
        );

        let name_descriptors = record.descriptors_for_attr(name_attr);
        assert_eq!(name_descriptors.len(), 1);
        assert_eq!(name_descriptors[0].attr, name_attr);

        let interval_descriptors = record.descriptors_in_interval(Interval::new(120, 180).unwrap());
        assert_eq!(interval_descriptors.len(), 2);
    }

    #[test]
    fn test_global_cluster_id() {
        let id = GlobalClusterId::new(1, 12345, 0);
        assert_eq!(id.shard_id, 1);
        assert_eq!(id.local_id, 12345);
        assert_eq!(id.version, 0);

        // Test encoding/decoding
        let encoded = id.to_u64();
        let decoded = GlobalClusterId::from_u64(encoded);
        assert_eq!(id, decoded);

        // Test bytes encoding
        let bytes = id.to_bytes();
        let from_bytes = GlobalClusterId::from_bytes(bytes);
        assert_eq!(id, from_bytes);
    }

    #[test]
    fn test_global_cluster_id_encoding() {
        // Test that encoding preserves all bits correctly
        let id = GlobalClusterId::new(0xABCD, 0x12345678, 0x9876);
        let encoded = id.to_u64();

        // Check bit positions
        assert_eq!((encoded >> 48) & 0xFFFF, 0xABCD);
        assert_eq!((encoded >> 32) & 0xFFFF, 0x9876);
        assert_eq!(encoded & 0xFFFFFFFF, 0x12345678);

        let decoded = GlobalClusterId::from_u64(encoded);
        assert_eq!(decoded.shard_id, 0xABCD);
        assert_eq!(decoded.version, 0x9876);
        assert_eq!(decoded.local_id, 0x12345678);
    }

    #[test]
    fn test_global_cluster_id_from_local() {
        let local = ClusterId(42);
        let global = GlobalClusterId::from_local(5, local);

        assert_eq!(global.shard_id, 5);
        assert_eq!(global.local_id, 42);
        assert_eq!(global.version, 0);
        assert_eq!(global.local_cluster_id(), local);
    }

    #[test]
    fn test_global_cluster_id_versioning() {
        let id = GlobalClusterId::new(1, 100, 0);

        let v1 = id.with_new_version(1);
        assert_eq!(v1.shard_id, 1);
        assert_eq!(v1.local_id, 100);
        assert_eq!(v1.version, 1);

        let transferred = id.with_new_owner(2, 1);
        assert_eq!(transferred.shard_id, 2);
        assert_eq!(transferred.local_id, 100);
        assert_eq!(transferred.version, 1);
    }

    #[test]
    fn test_global_cluster_id_ownership() {
        let id = GlobalClusterId::new(3, 100, 0);
        assert!(id.is_owned_by(3));
        assert!(!id.is_owned_by(1));
        assert!(!id.is_owned_by(5));
    }
}
