//! # Ontology Module
//!
//! Defines the ontology rules for entity mastering, including identity keys,
//! strong identifiers, crosswalks, and constraints.

use crate::model::{AttrId, CanonicalId, PerspectiveScopedId};
use crate::temporal::Interval;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// An identity key defines which attributes must match for records to be considered the same entity
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdentityKey {
    /// The attributes that form the identity key
    pub attributes: Vec<AttrId>,
    /// Human-readable name for this identity key
    pub name: String,
}

impl IdentityKey {
    /// Create a new identity key
    pub fn new(attributes: Vec<AttrId>, name: String) -> Self {
        Self { attributes, name }
    }

    /// Check if this identity key matches another over an overlapping interval
    pub fn matches(&self, other: &IdentityKey, _interval: Interval) -> bool {
        if self.attributes.len() != other.attributes.len() {
            return false;
        }

        // Check that all attributes match
        for attr in &self.attributes {
            if !other.attributes.contains(attr) {
                return false;
            }
        }

        true
    }
}

/// A strong identifier defines attributes that must not conflict within the same cluster
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StrongIdentifier {
    /// The attribute that serves as a strong identifier
    pub attribute: AttrId,
    /// Human-readable name for this strong identifier
    pub name: String,
}

impl StrongIdentifier {
    /// Create a new strong identifier
    pub fn new(attribute: AttrId, name: String) -> Self {
        Self { attribute, name }
    }
}

/// A crosswalk maps between perspective-scoped IDs and canonical IDs
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Crosswalk {
    /// The perspective-scoped ID
    pub perspective_id: PerspectiveScopedId,
    /// The canonical ID
    pub canonical_id: CanonicalId,
    /// The time interval when this crosswalk is valid
    pub interval: Interval,
}

impl Crosswalk {
    /// Create a new crosswalk
    pub fn new(
        perspective_id: PerspectiveScopedId,
        canonical_id: CanonicalId,
        interval: Interval,
    ) -> Self {
        Self {
            perspective_id,
            canonical_id,
            interval,
        }
    }
}

/// A constraint defines rules that must be satisfied within clusters
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Constraint {
    /// Unique constraint: no two records can have different values for this attribute
    /// within the same cluster over overlapping time intervals
    Unique { attribute: AttrId, name: String },
    /// Unique within perspective: no two records from the same perspective can have
    /// different values for this attribute over overlapping time intervals
    UniqueWithinPerspective { attribute: AttrId, name: String },
}

impl Constraint {
    /// Create a unique constraint
    pub fn unique(attribute: AttrId, name: String) -> Self {
        Self::Unique { attribute, name }
    }

    /// Create a unique within perspective constraint
    pub fn unique_within_perspective(attribute: AttrId, name: String) -> Self {
        Self::UniqueWithinPerspective { attribute, name }
    }

    /// Get the attribute this constraint applies to
    pub fn attribute(&self) -> AttrId {
        match self {
            Self::Unique { attribute, .. } => *attribute,
            Self::UniqueWithinPerspective { attribute, .. } => *attribute,
        }
    }

    /// Get the name of this constraint
    pub fn name(&self) -> &str {
        match self {
            Self::Unique { name, .. } => name,
            Self::UniqueWithinPerspective { name, .. } => name,
        }
    }
}

/// The complete ontology defining entity mastering rules
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ontology {
    /// Identity keys for entity matching
    pub identity_keys: Vec<IdentityKey>,
    /// Strong identifiers that prevent merging when they conflict
    pub strong_identifiers: Vec<StrongIdentifier>,
    /// Crosswalks between different ID systems
    pub crosswalks: Vec<Crosswalk>,
    /// Constraints that must be satisfied
    pub constraints: Vec<Constraint>,
    /// Entity type definitions
    pub entity_types: HashMap<String, EntityType>,
    /// Perspective weights for conflict resolution (higher weight = higher precedence)
    pub perspective_weights: HashMap<String, u32>,
    /// Perspective permanent attributes (authoritative source for each perspective)
    pub perspective_permanent_attributes: HashMap<String, Vec<AttrId>>,
}

impl Ontology {
    /// Create a new ontology
    pub fn new() -> Self {
        Self {
            identity_keys: Vec::new(),
            strong_identifiers: Vec::new(),
            crosswalks: Vec::new(),
            constraints: Vec::new(),
            entity_types: HashMap::new(),
            perspective_weights: HashMap::new(),
            perspective_permanent_attributes: HashMap::new(),
        }
    }

    /// Add an identity key
    pub fn add_identity_key(&mut self, identity_key: IdentityKey) {
        self.identity_keys.push(identity_key);
    }

    /// Add a strong identifier
    pub fn add_strong_identifier(&mut self, strong_identifier: StrongIdentifier) {
        self.strong_identifiers.push(strong_identifier);
    }

    /// Add a crosswalk
    pub fn add_crosswalk(&mut self, crosswalk: Crosswalk) {
        self.crosswalks.push(crosswalk);
    }

    /// Add a constraint
    pub fn add_constraint(&mut self, constraint: Constraint) {
        self.constraints.push(constraint);
    }

    /// Add an entity type
    pub fn add_entity_type(&mut self, entity_type: EntityType) {
        self.entity_types
            .insert(entity_type.name.clone(), entity_type);
    }

    /// Get identity keys for a specific entity type
    pub fn identity_keys_for_type(&self, _entity_type: &str) -> Vec<&IdentityKey> {
        self.identity_keys
            .iter()
            .filter(|_key| {
                // For now, all identity keys apply to all entity types
                // In a more sophisticated system, we might have type-specific keys
                true
            })
            .collect()
    }

    /// Get strong identifiers for a specific entity type
    pub fn strong_identifiers_for_type(&self, _entity_type: &str) -> Vec<&StrongIdentifier> {
        self.strong_identifiers
            .iter()
            .filter(|_| {
                // For now, all strong identifiers apply to all entity types
                true
            })
            .collect()
    }

    /// Check if an attribute is a strong identifier for a given entity type
    pub fn is_strong_identifier(&self, entity_type: &str, attribute: AttrId) -> bool {
        self.strong_identifiers_for_type(entity_type)
            .iter()
            .any(|strong_id| strong_id.attribute == attribute)
    }

    /// Get crosswalks for a specific perspective
    pub fn crosswalks_for_perspective(&self, perspective: &str) -> Vec<&Crosswalk> {
        self.crosswalks
            .iter()
            .filter(|crosswalk| crosswalk.perspective_id.perspective == perspective)
            .collect()
    }

    /// Get constraints for a specific attribute
    pub fn constraints_for_attribute(&self, attribute: AttrId) -> Vec<&Constraint> {
        self.constraints
            .iter()
            .filter(|constraint| constraint.attribute() == attribute)
            .collect()
    }

    /// Set the weight for a perspective
    pub fn set_perspective_weight(&mut self, perspective: String, weight: u32) {
        self.perspective_weights.insert(perspective, weight);
    }

    /// Get the weight for a perspective
    pub fn get_perspective_weight(&self, perspective: &str) -> u32 {
        self.perspective_weights
            .get(perspective)
            .copied()
            .unwrap_or(0)
    }

    /// Compare two perspectives by weight (higher weight = higher precedence)
    pub fn compare_perspectives(
        &self,
        perspective_a: &str,
        perspective_b: &str,
    ) -> std::cmp::Ordering {
        let weight_a = self.get_perspective_weight(perspective_a);
        let weight_b = self.get_perspective_weight(perspective_b);
        weight_b.cmp(&weight_a) // Higher weight comes first
    }

    /// Check if one perspective has higher precedence than another
    pub fn has_higher_precedence(&self, perspective_a: &str, perspective_b: &str) -> bool {
        self.compare_perspectives(perspective_a, perspective_b) == std::cmp::Ordering::Greater
    }

    /// Set permanent attributes for a perspective
    pub fn set_perspective_permanent_attributes(
        &mut self,
        perspective: String,
        attributes: Vec<AttrId>,
    ) {
        self.perspective_permanent_attributes
            .insert(perspective, attributes);
    }

    /// Get permanent attributes for a perspective
    pub fn get_perspective_permanent_attributes(&self, perspective: &str) -> Vec<AttrId> {
        self.perspective_permanent_attributes
            .get(perspective)
            .cloned()
            .unwrap_or_default()
    }

    /// Check if an attribute is permanent for a perspective
    pub fn is_permanent_attribute(&self, perspective: &str, attribute: AttrId) -> bool {
        self.get_perspective_permanent_attributes(perspective)
            .contains(&attribute)
    }

    /// Get the authoritative perspective for an attribute (highest weight with permanent attribute)
    pub fn get_authoritative_perspective(&self, attribute: AttrId) -> Option<String> {
        let mut best_perspective = None;
        let mut best_weight = 0;

        for (perspective, permanent_attrs) in &self.perspective_permanent_attributes {
            if permanent_attrs.contains(&attribute) {
                let weight = self.get_perspective_weight(perspective);
                if weight > best_weight {
                    best_weight = weight;
                    best_perspective = Some(perspective.clone());
                }
            }
        }

        best_perspective
    }
}

impl Default for Ontology {
    fn default() -> Self {
        Self::new()
    }
}

/// Defines an entity type with its specific rules
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityType {
    /// The name of the entity type
    pub name: String,
    /// The attributes that can be used for this entity type
    pub attributes: Vec<AttrId>,
    /// Whether this entity type requires a strong identifier
    pub requires_strong_identifier: bool,
}

impl EntityType {
    /// Create a new entity type
    pub fn new(name: String, attributes: Vec<AttrId>, requires_strong_identifier: bool) -> Self {
        Self {
            name,
            attributes,
            requires_strong_identifier,
        }
    }
}

/// A violation of an ontology constraint
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConstraintViolation {
    /// The constraint that was violated
    pub constraint: Constraint,
    /// The time interval when the violation occurred
    pub interval: Interval,
    /// The records involved in the violation
    pub participants: Vec<crate::model::RecordId>,
    /// Additional details about the violation
    pub details: String,
}

impl ConstraintViolation {
    /// Create a new constraint violation
    pub fn new(
        constraint: Constraint,
        interval: Interval,
        participants: Vec<crate::model::RecordId>,
        details: String,
    ) -> Self {
        Self {
            constraint,
            interval,
            participants,
            details,
        }
    }
}

impl fmt::Display for ConstraintViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Constraint violation: {} at {} involving records {:?} - {}",
            self.constraint.name(),
            self.interval,
            self.participants,
            self.details
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporal::Interval;

    #[test]
    fn test_ontology_creation() {
        let mut ontology = Ontology::new();

        let name_attr = AttrId(1);
        let email_attr = AttrId(2);

        let identity_key = IdentityKey::new(vec![name_attr, email_attr], "name_email".to_string());
        ontology.add_identity_key(identity_key);

        let strong_id = StrongIdentifier::new(email_attr, "email_unique".to_string());
        ontology.add_strong_identifier(strong_id);

        assert_eq!(ontology.identity_keys.len(), 1);
        assert_eq!(ontology.strong_identifiers.len(), 1);
    }

    #[test]
    fn test_identity_key_matching() {
        let name_attr = AttrId(1);
        let email_attr = AttrId(2);

        let key1 = IdentityKey::new(vec![name_attr, email_attr], "key1".to_string());
        let key2 = IdentityKey::new(vec![email_attr, name_attr], "key2".to_string());

        let interval = Interval::new(100, 200).unwrap();
        assert!(key1.matches(&key2, interval));
    }

    #[test]
    fn test_crosswalk_creation() {
        let perspective_id = PerspectiveScopedId::new("crm".to_string(), "123".to_string());
        let canonical_id = CanonicalId::new("canonical_123".to_string());
        let interval = Interval::new(100, 200).unwrap();

        let crosswalk = Crosswalk::new(perspective_id, canonical_id, interval);

        assert_eq!(crosswalk.perspective_id.perspective, "crm");
        assert_eq!(crosswalk.perspective_id.uid, "123");
        assert_eq!(crosswalk.canonical_id.value, "canonical_123");
    }

    #[test]
    fn test_constraint_creation() {
        let attr = AttrId(1);

        let unique_constraint = Constraint::unique(attr, "unique_name".to_string());
        assert_eq!(unique_constraint.attribute(), attr);
        assert_eq!(unique_constraint.name(), "unique_name");

        let perspective_constraint =
            Constraint::unique_within_perspective(attr, "unique_within_perspective".to_string());
        assert_eq!(perspective_constraint.attribute(), attr);
        assert_eq!(perspective_constraint.name(), "unique_within_perspective");
    }

    #[test]
    fn test_constraint_violation() {
        let attr = AttrId(1);
        let constraint = Constraint::unique(attr, "unique_name".to_string());
        let interval = Interval::new(100, 200).unwrap();
        let participants = vec![crate::model::RecordId(1), crate::model::RecordId(2)];
        let details = "Two records have different names".to_string();

        let violation = ConstraintViolation::new(constraint, interval, participants, details);

        assert_eq!(violation.constraint.attribute(), attr);
        assert_eq!(violation.interval, interval);
        assert_eq!(violation.participants.len(), 2);
    }
}
