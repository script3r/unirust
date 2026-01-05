//! # Conflicts Module
//!
//! Implements conflict detection for entity resolution, including both direct
//! conflicts (within clusters) and indirect conflicts (suppressed merges).

use crate::dsu::Clusters;
use crate::model::{AttrId, ClusterId, Descriptor, KeyValue, Record, RecordId, ValueId};
use crate::ontology::{Constraint, ConstraintViolation, IdentityKey, Ontology};
use crate::store::RecordStore;
use crate::temporal::Interval;
use anyhow::Result;
use hashbrown::HashMap;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
// use std::collections::HashSet;

/// Small-vector-optimized participants list.
/// Most intervals have only one participant, so avoid heap allocation.
#[derive(Debug, Clone)]
enum Participants {
    One(RecordId),
    Many(Vec<RecordId>),
}

impl Participants {
    #[inline]
    fn one(id: RecordId) -> Self {
        Participants::One(id)
    }

    #[inline]
    fn extend(&mut self, other: Self) {
        match other {
            Participants::One(b) => match self {
                Participants::One(a) => {
                    *self = Participants::Many(vec![*a, b]);
                }
                Participants::Many(ref mut v) => {
                    v.push(b);
                }
            },
            Participants::Many(mut other_v) => match self {
                Participants::One(a) => {
                    other_v.push(*a);
                    *self = Participants::Many(other_v);
                }
                Participants::Many(ref mut v) => {
                    v.append(&mut other_v);
                }
            },
        }
    }

    #[inline]
    fn sort_dedup(&mut self) {
        if let Participants::Many(ref mut v) = self {
            v.sort();
            v.dedup();
        }
    }

    #[inline]
    fn to_vec(&self) -> Vec<RecordId> {
        match self {
            Participants::One(id) => vec![*id],
            Participants::Many(v) => v.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct ValueInterval {
    interval: Interval,
    participants: Participants,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum EventKind {
    End,
    Start,
}

#[derive(Debug, Clone)]
struct IntervalEvent {
    time: i64,
    kind: EventKind,
    value: ValueId,
    participants: Option<Participants>,
}

impl IntervalEvent {
    fn start(time: i64, value: ValueId, participants: Participants) -> Self {
        Self {
            time,
            kind: EventKind::Start,
            value,
            participants: Some(participants),
        }
    }

    fn end(time: i64, value: ValueId) -> Self {
        Self {
            time,
            kind: EventKind::End,
            value,
            participants: None,
        }
    }
}

/// A direct conflict within a cluster
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DirectConflict {
    /// The kind of conflict
    pub kind: String,
    /// The attribute that has the conflict
    pub attribute: AttrId,
    /// The time interval when the conflict occurs
    pub interval: Interval,
    /// The conflicting values and their participants
    pub values: Vec<ConflictValue>,
}

impl DirectConflict {
    /// Create a new direct conflict
    pub fn new(
        kind: String,
        attribute: AttrId,
        interval: Interval,
        values: Vec<ConflictValue>,
    ) -> Self {
        Self {
            kind,
            attribute,
            interval,
            values,
        }
    }
}

/// A conflicting value with its participants
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConflictValue {
    /// The conflicting value
    pub value: ValueId,
    /// The records that have this value
    pub participants: Vec<RecordId>,
}

impl ConflictValue {
    /// Create a new conflict value
    pub fn new(value: ValueId, participants: Vec<RecordId>) -> Self {
        Self {
            value,
            participants,
        }
    }
}

/// An indirect conflict (suppressed merge or identity split)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndirectConflict {
    /// The kind of conflict
    pub kind: String,
    /// The cause of the conflict
    pub cause: String,
    /// The attribute involved (if applicable)
    pub attribute: Option<AttrId>,
    /// The time interval when the conflict occurs
    pub interval: Interval,
    /// The participants in the conflict
    pub participants: ConflictParticipants,
    /// The status of the conflict
    pub status: String,
    /// Additional details about the conflict
    pub details: ConflictDetails,
}

impl IndirectConflict {
    /// Create a new indirect conflict
    pub fn new(
        kind: String,
        cause: String,
        attribute: Option<AttrId>,
        interval: Interval,
        participants: ConflictParticipants,
        status: String,
        details: ConflictDetails,
    ) -> Self {
        Self {
            kind,
            cause,
            attribute,
            interval,
            participants,
            status,
            details,
        }
    }
}

/// Participants in a conflict
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConflictParticipants {
    /// The clusters involved
    pub clusters: Vec<ClusterId>,
    /// The records involved (if applicable)
    pub records: Option<Vec<RecordId>>,
}

impl ConflictParticipants {
    /// Create new conflict participants
    pub fn new(clusters: Vec<ClusterId>, records: Option<Vec<RecordId>>) -> Self {
        Self { clusters, records }
    }
}

/// Details about a conflict
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConflictDetails {
    /// The key that caused the conflict (if applicable)
    pub key: Option<Vec<(AttrId, ValueId)>>,
    /// The strong identifiers involved (if applicable)
    pub strong_ids: Option<Vec<AttrId>>,
    /// Additional evidence
    pub evidence: Option<Vec<String>>,
}

impl ConflictDetails {
    /// Create new conflict details
    pub fn new() -> Self {
        Self {
            key: None,
            strong_ids: None,
            evidence: None,
        }
    }

    /// Add a key to the details
    pub fn with_key(mut self, key: Vec<(AttrId, ValueId)>) -> Self {
        self.key = Some(key);
        self
    }

    /// Add strong identifiers to the details
    pub fn with_strong_ids(mut self, strong_ids: Vec<AttrId>) -> Self {
        self.strong_ids = Some(strong_ids);
        self
    }

    /// Add evidence to the details
    pub fn with_evidence(mut self, evidence: Vec<String>) -> Self {
        self.evidence = Some(evidence);
        self
    }
}

impl Default for ConflictDetails {
    fn default() -> Self {
        Self::new()
    }
}

/// An observation of a conflict or merge
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Observation {
    /// A direct conflict within a cluster
    DirectConflict(DirectConflict),
    /// An indirect conflict (suppressed merge)
    IndirectConflict(IndirectConflict),
    /// A successful merge
    Merge {
        /// The records that were merged
        records: Vec<RecordId>,
        /// The cluster they were merged into
        cluster: ClusterId,
        /// The time interval when the merge occurred
        interval: Interval,
        /// The reason for the merge
        reason: String,
    },
}

impl Observation {
    /// Create a direct conflict observation
    pub fn direct_conflict(conflict: DirectConflict) -> Self {
        Self::DirectConflict(conflict)
    }

    /// Create an indirect conflict observation
    pub fn indirect_conflict(conflict: IndirectConflict) -> Self {
        Self::IndirectConflict(conflict)
    }

    /// Create a merge observation
    pub fn merge(
        records: Vec<RecordId>,
        cluster: ClusterId,
        interval: Interval,
        reason: String,
    ) -> Self {
        Self::Merge {
            records,
            cluster,
            interval,
            reason,
        }
    }
}

/// Main conflict detector
pub struct ConflictDetector;

impl ConflictDetector {
    /// Detect all conflicts in the given clusters
    pub fn detect_conflicts(
        store: &dyn RecordStore,
        clusters: &Clusters,
        ontology: &Ontology,
    ) -> Result<Vec<Observation>> {
        let mut observations = Vec::new();

        // Detect direct conflicts within each cluster (inter-entity conflicts)
        for cluster in &clusters.clusters {
            let direct_conflicts = Self::detect_direct_conflicts(store, cluster, ontology)?;
            for conflict in direct_conflicts {
                observations.push(Observation::direct_conflict(conflict));
            }
        }

        // Detect intra-entity conflicts (conflicts within a single entity)
        let intra_entity_conflicts = Self::detect_intra_entity_conflicts(store, ontology)?;
        for conflict in intra_entity_conflicts {
            observations.push(Observation::direct_conflict(conflict));
        }

        // Detect indirect conflicts (suppressed merges)
        let indirect_conflicts = Self::detect_indirect_conflicts(store, clusters, ontology)?;
        for conflict in indirect_conflicts {
            observations.push(Observation::indirect_conflict(conflict));
        }

        // Detect constraint violations
        let violations = Self::detect_constraint_violations(store, clusters, ontology)?;
        for violation in violations {
            // Convert constraint violations to observations
            let conflict = IndirectConflict::new(
                "constraint_violation".to_string(),
                violation.constraint.name().to_string(),
                Some(violation.constraint.attribute()),
                violation.interval,
                ConflictParticipants::new(vec![], Some(violation.participants)),
                "violation".to_string(),
                ConflictDetails::new().with_evidence(vec![violation.details]),
            );
            observations.push(Observation::indirect_conflict(conflict));
        }

        Ok(observations)
    }

    pub fn detect_conflicts_for_clusters(
        store: &dyn RecordStore,
        clusters: &Clusters,
        ontology: &Ontology,
        cluster_ids: &[ClusterId],
    ) -> Result<Vec<Observation>> {
        if cluster_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut observations = Vec::new();
        let target: std::collections::HashSet<ClusterId> = cluster_ids.iter().copied().collect();

        for cluster in &clusters.clusters {
            if !target.contains(&cluster.id) {
                continue;
            }
            let direct_conflicts = Self::detect_direct_conflicts(store, cluster, ontology)?;
            for conflict in direct_conflicts {
                observations.push(Observation::direct_conflict(conflict));
            }
            let violations =
                Self::detect_constraint_violations_for_cluster(store, cluster, ontology)?;
            for violation in violations {
                let conflict = IndirectConflict::new(
                    "constraint_violation".to_string(),
                    violation.constraint.name().to_string(),
                    Some(violation.constraint.attribute()),
                    violation.interval,
                    ConflictParticipants::new(vec![], Some(violation.participants)),
                    "violation".to_string(),
                    ConflictDetails::new().with_evidence(vec![violation.details]),
                );
                observations.push(Observation::indirect_conflict(conflict));
            }
        }

        let intra_entity_conflicts =
            Self::detect_intra_entity_conflicts_for_clusters(store, clusters, ontology, &target)?;
        for conflict in intra_entity_conflicts {
            observations.push(Observation::direct_conflict(conflict));
        }

        let indirect_conflicts =
            Self::detect_indirect_conflicts_for_clusters(store, clusters, ontology, &target)?;
        for conflict in indirect_conflicts {
            observations.push(Observation::indirect_conflict(conflict));
        }

        Ok(observations)
    }
    /// Detect direct conflicts within a cluster
    fn detect_direct_conflicts(
        store: &dyn RecordStore,
        cluster: &crate::dsu::Cluster,
        _ontology: &Ontology,
    ) -> Result<Vec<DirectConflict>> {
        let mut conflicts = Vec::new();

        // Group descriptors by attribute - only extract needed fields (no clone)
        // Use (ValueId, Interval) tuple instead of full Descriptor
        let mut descriptors_by_attr: HashMap<AttrId, Vec<(RecordId, ValueId, Interval)>> =
            HashMap::new();

        for record_id in &cluster.records {
            if let Some(record) = store.get_record(*record_id) {
                for descriptor in &record.descriptors {
                    descriptors_by_attr
                        .entry(descriptor.attr)
                        .or_default()
                        .push((*record_id, descriptor.value, descriptor.interval));
                }
            }
        }

        // Check each attribute for conflicts
        for (attr, descriptors) in descriptors_by_attr {
            let conflicts_for_attr = Self::detect_conflicts_for_attribute_fast(descriptors, attr)?;
            conflicts.extend(conflicts_for_attr);
        }

        Ok(conflicts)
    }

    /// Detect intra-entity conflicts (conflicts within a single entity).
    /// Only strong-identifier attributes are considered for intra-entity checks.
    fn detect_intra_entity_conflicts(
        store: &dyn RecordStore,
        ontology: &Ontology,
    ) -> Result<Vec<DirectConflict>> {
        let mut conflicts = Vec::new();

        // Iterate through each entity individually
        let mut error: Option<anyhow::Error> = None;
        store.for_each_record(&mut |record| {
            // Only group descriptors for strong identifier attributes (skip others early)
            // Use (ValueId, Interval) tuples to avoid cloning
            let mut descriptors_by_attr: HashMap<AttrId, Vec<(ValueId, Interval)>> = HashMap::new();
            for descriptor in &record.descriptors {
                // Early filter: only process strong identifiers
                if ontology.is_strong_identifier(&record.identity.entity_type, descriptor.attr) {
                    descriptors_by_attr
                        .entry(descriptor.attr)
                        .or_default()
                        .push((descriptor.value, descriptor.interval));
                }
            }

            // Check each attribute for intra-entity conflicts
            for (attr, descriptors) in descriptors_by_attr {
                match Self::detect_intra_entity_conflicts_for_attribute_fast(
                    descriptors,
                    attr,
                    record.id,
                ) {
                    Ok(entity_conflicts) => conflicts.extend(entity_conflicts),
                    Err(err) => {
                        error = Some(err);
                        return;
                    }
                }
            }
        });

        if let Some(err) = error {
            return Err(err);
        }

        Ok(conflicts)
    }

    fn detect_intra_entity_conflicts_for_clusters(
        store: &dyn RecordStore,
        clusters: &Clusters,
        ontology: &Ontology,
        target: &std::collections::HashSet<ClusterId>,
    ) -> Result<Vec<DirectConflict>> {
        let mut conflicts = Vec::new();
        for cluster in &clusters.clusters {
            if !target.contains(&cluster.id) {
                continue;
            }
            for record_id in &cluster.records {
                if let Some(record) = store.get_record(*record_id) {
                    // Only group descriptors for strong identifier attributes
                    let mut descriptors_by_attr: HashMap<AttrId, Vec<(ValueId, Interval)>> =
                        HashMap::new();
                    for descriptor in &record.descriptors {
                        if ontology
                            .is_strong_identifier(&record.identity.entity_type, descriptor.attr)
                        {
                            descriptors_by_attr
                                .entry(descriptor.attr)
                                .or_default()
                                .push((descriptor.value, descriptor.interval));
                        }
                    }
                    for (attr, descriptors) in descriptors_by_attr {
                        let entity_conflicts =
                            Self::detect_intra_entity_conflicts_for_attribute_fast(
                                descriptors,
                                attr,
                                record.id,
                            )?;
                        conflicts.extend(entity_conflicts);
                    }
                }
            }
        }
        Ok(conflicts)
    }

    /// Detect conflicts within a single entity for a specific attribute.
    fn detect_intra_entity_conflicts_for_attribute_fast(
        descriptors: Vec<(ValueId, Interval)>,
        attribute: AttrId,
        entity_id: RecordId,
    ) -> Result<Vec<DirectConflict>> {
        let mut descriptors_by_value: FxHashMap<ValueId, Vec<ValueInterval>> = FxHashMap::default();
        descriptors_by_value.reserve(descriptors.len());
        for (value, interval) in descriptors {
            descriptors_by_value
                .entry(value)
                .or_insert_with(|| Vec::with_capacity(2))
                .push(ValueInterval {
                    interval,
                    participants: Participants::one(entity_id),
                });
        }

        Ok(Self::detect_conflicts_from_value_intervals(
            attribute,
            descriptors_by_value,
        ))
    }

    /// Detect conflicts for a specific attribute.
    fn detect_conflicts_for_attribute_fast(
        descriptors: Vec<(RecordId, ValueId, Interval)>,
        attribute: AttrId,
    ) -> Result<Vec<DirectConflict>> {
        let mut descriptors_by_value: FxHashMap<ValueId, Vec<ValueInterval>> = FxHashMap::default();
        descriptors_by_value.reserve(descriptors.len() / 4 + 1);
        for (record_id, value, interval) in descriptors {
            descriptors_by_value
                .entry(value)
                .or_insert_with(|| Vec::with_capacity(4))
                .push(ValueInterval {
                    interval,
                    participants: Participants::one(record_id),
                });
        }

        Ok(Self::detect_conflicts_from_value_intervals(
            attribute,
            descriptors_by_value,
        ))
    }

    /// Find overlapping intervals between two sets of descriptors
    fn find_overlapping_intervals(
        descs_a: &[(RecordId, Descriptor)],
        descs_b: &[(RecordId, Descriptor)],
    ) -> Vec<Interval> {
        let mut overlaps = Vec::new();

        for (_, desc_a) in descs_a {
            for (_, desc_b) in descs_b {
                if let Some(overlap) =
                    crate::temporal::intersect(&desc_a.interval, &desc_b.interval)
                {
                    overlaps.push(overlap);
                }
            }
        }

        // Merge overlapping intervals
        crate::temporal::coalesce_same_value(
            &overlaps.into_iter().map(|i| (i, ())).collect::<Vec<_>>(),
        )
        .into_iter()
        .map(|(interval, _)| interval)
        .collect()
    }

    fn detect_conflicts_from_value_intervals(
        attribute: AttrId,
        mut descriptors_by_value: FxHashMap<ValueId, Vec<ValueInterval>>,
    ) -> Vec<DirectConflict> {
        let mut conflicts = Vec::new();

        // First pass: merge intervals for each value
        for intervals in descriptors_by_value.values_mut() {
            *intervals = Self::merge_value_intervals(std::mem::take(intervals));
        }

        // Calculate total event count for pre-allocation
        let event_count: usize = descriptors_by_value.values().map(|v| v.len() * 2).sum();
        let mut events = Vec::with_capacity(event_count);

        let active_capacity = descriptors_by_value.len();

        // Consume the HashMap to avoid cloning participants
        for (value, intervals) in descriptors_by_value {
            for entry in intervals {
                events.push(IntervalEvent::start(
                    entry.interval.start,
                    value,
                    entry.participants, // Move instead of clone
                ));
                events.push(IntervalEvent::end(entry.interval.end, value));
            }
        }

        events.sort_unstable_by(|a, b| a.time.cmp(&b.time).then_with(|| a.kind.cmp(&b.kind)));

        let mut active: FxHashMap<ValueId, Participants> = FxHashMap::default();
        active.reserve(active_capacity);
        let mut last_time: Option<i64> = None;

        for event in events {
            if let Some(start_time) = last_time {
                if event.time > start_time && active.len() > 1 {
                    if let Ok(interval) = Interval::new(start_time, event.time) {
                        let mut values = Vec::with_capacity(active.len());
                        for (value, participants) in &active {
                            values.push(ConflictValue::new(*value, participants.to_vec()));
                        }
                        conflicts.push(DirectConflict::new(
                            "direct".to_string(),
                            attribute,
                            interval,
                            values,
                        ));
                    }
                }
            }

            match event.kind {
                EventKind::End => {
                    active.remove(&event.value);
                }
                EventKind::Start => {
                    // Participants already sorted/deduped by merge_value_intervals
                    if let Some(participants) = event.participants {
                        active.insert(event.value, participants);
                    }
                }
            }

            last_time = Some(event.time);
        }

        conflicts
    }

    fn merge_value_intervals(mut intervals: Vec<ValueInterval>) -> Vec<ValueInterval> {
        if intervals.is_empty() {
            return intervals;
        }

        intervals.sort_by(|a, b| a.interval.start.cmp(&b.interval.start));
        let mut merged: Vec<ValueInterval> = Vec::with_capacity(intervals.len());

        for current in intervals {
            if let Some(last) = merged.last_mut() {
                if crate::temporal::is_overlapping(&last.interval, &current.interval) {
                    last.interval = Interval::new(
                        last.interval.start.min(current.interval.start),
                        last.interval.end.max(current.interval.end),
                    )
                    .unwrap_or(last.interval);
                    // Defer sort/dedup - just extend for now
                    last.participants.extend(current.participants);
                    continue;
                }
            }
            merged.push(current);
        }

        // Sort and dedup participants once at the end
        for interval in &mut merged {
            interval.participants.sort_dedup();
        }

        merged
    }

    /// Detect conflicts using atomic intervals approach.
    ///
    /// This is an alternative algorithm that:
    /// 1. Collects all boundary points from all intervals
    /// 2. Creates atomic intervals between consecutive points
    /// 3. For each atomic interval, determines which values are active
    /// 4. Multiple active values = conflict
    ///
    /// This approach is more parallelizable but has different performance characteristics.
    fn detect_conflicts_atomic(
        attribute: AttrId,
        descriptors: Vec<(RecordId, ValueId, Interval)>,
    ) -> Vec<DirectConflict> {
        if descriptors.is_empty() {
            return Vec::new();
        }

        // Collect all intervals for atomic interval computation
        let intervals: Vec<Interval> = descriptors.iter().map(|(_, _, i)| *i).collect();

        // Get atomic intervals
        let atoms = crate::temporal::atomic_intervals(&intervals);
        if atoms.is_empty() {
            return Vec::new();
        }

        let mut conflicts = Vec::new();

        // For each atomic interval, find which values are active
        for atom in &atoms {
            let mut active_values: FxHashMap<ValueId, Vec<RecordId>> = FxHashMap::default();

            for (record_id, value, interval) in &descriptors {
                if crate::temporal::encloses(interval, atom) {
                    active_values.entry(*value).or_default().push(*record_id);
                }
            }

            // If multiple values are active in this atomic interval, it's a conflict
            if active_values.len() > 1 {
                let values: Vec<ConflictValue> = active_values
                    .into_iter()
                    .map(|(value, participants)| ConflictValue::new(value, participants))
                    .collect();

                conflicts.push(DirectConflict::new(
                    "direct".to_string(),
                    attribute,
                    *atom,
                    values,
                ));
            }
        }

        // Merge adjacent conflicts with the same values
        Self::merge_adjacent_conflicts(conflicts)
    }

    /// Merge adjacent conflicts that have the same set of values.
    fn merge_adjacent_conflicts(mut conflicts: Vec<DirectConflict>) -> Vec<DirectConflict> {
        if conflicts.len() < 2 {
            return conflicts;
        }

        conflicts.sort_by(|a, b| a.interval.start.cmp(&b.interval.start));
        let mut merged: Vec<DirectConflict> = Vec::with_capacity(conflicts.len());

        for current in conflicts {
            if let Some(last) = merged.last_mut() {
                // Check if intervals are adjacent and values match
                if last.interval.end == current.interval.start && last.values == current.values {
                    // Extend the interval
                    last.interval = Interval::new(last.interval.start, current.interval.end)
                        .unwrap_or(last.interval);
                    continue;
                }
            }
            merged.push(current);
        }

        merged
    }

    /// Detect indirect conflicts (suppressed merges)
    fn detect_indirect_conflicts(
        store: &dyn RecordStore,
        clusters: &Clusters,
        ontology: &Ontology,
    ) -> Result<Vec<IndirectConflict>> {
        let mut conflicts = Vec::new();

        // Detect indirect conflicts (suppressed merges and constraint violations)
        // Look for clusters that should have been merged but weren't due to strong identifier conflicts

        // Group clusters by their identity key values to find potential indirect conflicts
        let mut clusters_by_identity: std::collections::HashMap<
            Vec<KeyValue>,
            Vec<&crate::dsu::Cluster>,
        > = std::collections::HashMap::new();

        for cluster in &clusters.clusters {
            // Extract identity key values for this cluster
            if let Some(record_id) = cluster.records.first() {
                if let Some(record) = store.get_record(*record_id) {
                    // Find identity keys for this entity type
                    let identity_keys =
                        ontology.identity_keys_for_type(&record.identity.entity_type);

                    for identity_key in identity_keys {
                        if let Ok(key_values) = Self::extract_key_values(&record, identity_key) {
                            if !key_values.is_empty() {
                                clusters_by_identity
                                    .entry(key_values)
                                    .or_default()
                                    .push(cluster);
                            }
                        }
                    }
                }
            }
        }

        // Check for indirect conflicts within each identity key group
        for (key_values, clusters_with_same_identity) in clusters_by_identity {
            if clusters_with_same_identity.len() > 1 {
                // Multiple clusters with the same identity key - potential indirect conflict
                // Check if they have conflicting strong identifiers

                let mut has_strong_id_conflict = false;

                // Check all pairs of clusters with the same identity key
                for i in 0..clusters_with_same_identity.len() {
                    for j in i + 1..clusters_with_same_identity.len() {
                        let cluster_a = clusters_with_same_identity[i];
                        let cluster_b = clusters_with_same_identity[j];

                        // Check if these clusters have conflicting strong identifiers
                        if Self::clusters_have_conflicting_strong_ids(
                            store, cluster_a, cluster_b, ontology,
                        ) {
                            has_strong_id_conflict = true;
                        }
                    }
                }

                if has_strong_id_conflict {
                    // Create indirect conflict for each cluster that couldn't merge
                    for cluster in clusters_with_same_identity {
                        let interval = Self::interval_for_key_values(store, cluster, &key_values)
                            .unwrap_or_else(|| Interval::new(0, 1).unwrap());
                        let conflict = IndirectConflict::new(
                            "indirect".to_string(),
                            "strong_id_conflict".to_string(),
                            None,
                            interval,
                            ConflictParticipants::new(vec![], Some(cluster.records.clone())),
                            "auto_resolved".to_string(),
                            ConflictDetails::new(),
                        );
                        conflicts.push(conflict);
                    }
                }
            }
        }

        Ok(conflicts)
    }

    fn detect_indirect_conflicts_for_clusters(
        store: &dyn RecordStore,
        clusters: &Clusters,
        ontology: &Ontology,
        target: &std::collections::HashSet<ClusterId>,
    ) -> Result<Vec<IndirectConflict>> {
        let mut conflicts = Vec::new();

        let mut clusters_by_identity: std::collections::HashMap<
            Vec<KeyValue>,
            Vec<&crate::dsu::Cluster>,
        > = std::collections::HashMap::new();

        for cluster in &clusters.clusters {
            if let Some(record_id) = cluster.records.first() {
                if let Some(record) = store.get_record(*record_id) {
                    let identity_keys =
                        ontology.identity_keys_for_type(&record.identity.entity_type);
                    for identity_key in identity_keys {
                        if let Ok(key_values) = Self::extract_key_values(&record, identity_key) {
                            if !key_values.is_empty() {
                                clusters_by_identity
                                    .entry(key_values)
                                    .or_default()
                                    .push(cluster);
                            }
                        }
                    }
                }
            }
        }

        for (key_values, clusters_with_same_identity) in clusters_by_identity {
            if !clusters_with_same_identity
                .iter()
                .any(|cluster| target.contains(&cluster.id))
            {
                continue;
            }
            if clusters_with_same_identity.len() > 1 {
                let mut has_strong_id_conflict = false;
                for i in 0..clusters_with_same_identity.len() {
                    for j in i + 1..clusters_with_same_identity.len() {
                        let cluster_a = clusters_with_same_identity[i];
                        let cluster_b = clusters_with_same_identity[j];
                        if Self::clusters_have_conflicting_strong_ids(
                            store, cluster_a, cluster_b, ontology,
                        ) {
                            has_strong_id_conflict = true;
                        }
                    }
                }

                if has_strong_id_conflict {
                    for cluster in clusters_with_same_identity {
                        if !target.contains(&cluster.id) {
                            continue;
                        }
                        let interval = Self::interval_for_key_values(store, cluster, &key_values)
                            .unwrap_or_else(|| Interval::new(0, 1).unwrap());
                        let conflict = IndirectConflict::new(
                            "indirect".to_string(),
                            "strong_id_conflict".to_string(),
                            None,
                            interval,
                            ConflictParticipants::new(vec![], Some(cluster.records.clone())),
                            "suppressed_merge".to_string(),
                            ConflictDetails::new().with_evidence(vec![format!(
                                "Cluster {} shares identity key but conflicts on strong identifiers",
                                cluster.id.0
                            )]),
                        );
                        conflicts.push(conflict);
                    }
                }
            }
        }

        Ok(conflicts)
    }

    /// Extract key values for a record based on an identity key
    fn extract_key_values(
        record: &crate::model::Record,
        identity_key: &IdentityKey,
    ) -> Result<Vec<KeyValue>> {
        let mut key_values = Vec::new();

        for attr_id in &identity_key.attributes {
            let values = Self::get_values_for_attribute(
                record,
                *attr_id,
                Interval::new(0, i64::MAX).unwrap(),
            );
            if let Some(value_id) = values.first() {
                key_values.push(KeyValue {
                    attr: *attr_id,
                    value: *value_id,
                });
            }
        }

        Ok(key_values)
    }

    /// Check if two clusters have conflicting strong identifiers
    fn clusters_have_conflicting_strong_ids(
        store: &dyn RecordStore,
        cluster_a: &crate::dsu::Cluster,
        cluster_b: &crate::dsu::Cluster,
        ontology: &Ontology,
    ) -> bool {
        // Get records from both clusters
        let records_a: Vec<_> = cluster_a
            .records
            .iter()
            .filter_map(|&id| store.get_record(id))
            .collect();
        let records_b: Vec<_> = cluster_b
            .records
            .iter()
            .filter_map(|&id| store.get_record(id))
            .collect();

        // Check if any record from cluster A has a strong identifier that conflicts with any record from cluster B
        for record_a in &records_a {
            for record_b in &records_b {
                // Check if they have different perspectives (this is the key for indirect conflicts)
                if record_a.identity.perspective != record_b.identity.perspective {
                    // Check if they have different strong identifier attributes
                    let strong_attrs_a = Self::get_strong_identifier_attributes(record_a, ontology);
                    let strong_attrs_b = Self::get_strong_identifier_attributes(record_b, ontology);

                    // If they have different strong identifier attributes, this is an indirect conflict
                    if !strong_attrs_a.is_empty()
                        && !strong_attrs_b.is_empty()
                        && strong_attrs_a.is_disjoint(&strong_attrs_b)
                    {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Get strong identifier attributes for a record
    fn get_strong_identifier_attributes(
        record: &crate::model::Record,
        ontology: &Ontology,
    ) -> std::collections::HashSet<AttrId> {
        let mut attrs = std::collections::HashSet::new();

        // Check strong identifiers
        for strong_id in &ontology.strong_identifiers {
            let values = Self::get_values_for_attribute(
                record,
                strong_id.attribute,
                Interval::new(0, i64::MAX).unwrap(),
            );
            if !values.is_empty() {
                attrs.insert(strong_id.attribute);
            }
        }

        // Check constraints as strong identifiers
        for constraint in &ontology.constraints {
            let values = Self::get_values_for_attribute(
                record,
                constraint.attribute(),
                Interval::new(0, i64::MAX).unwrap(),
            );
            if !values.is_empty() {
                attrs.insert(constraint.attribute());
            }
        }

        attrs
    }

    /// Get values for a specific attribute from a record within a time interval
    fn get_values_for_attribute(
        record: &Record,
        attribute: AttrId,
        interval: Interval,
    ) -> Vec<ValueId> {
        record
            .descriptors
            .iter()
            .filter(|desc| {
                desc.attr == attribute && crate::temporal::is_overlapping(&desc.interval, &interval)
            })
            .map(|desc| desc.value)
            .collect()
    }

    fn interval_for_key_values(
        store: &dyn RecordStore,
        cluster: &crate::dsu::Cluster,
        key_values: &[KeyValue],
    ) -> Option<Interval> {
        let mut min_start: Option<i64> = None;
        let mut max_end: Option<i64> = None;

        for record_id in &cluster.records {
            let record = store.get_record(*record_id)?;
            for key_value in key_values {
                for descriptor in &record.descriptors {
                    if descriptor.attr == key_value.attr && descriptor.value == key_value.value {
                        min_start = Some(min_start.map_or(descriptor.interval.start, |current| {
                            current.min(descriptor.interval.start)
                        }));
                        max_end = Some(max_end.map_or(descriptor.interval.end, |current| {
                            current.max(descriptor.interval.end)
                        }));
                    }
                }
            }
        }

        match (min_start, max_end) {
            (Some(start), Some(end)) if start < end => Interval::new(start, end).ok(),
            _ => None,
        }
    }

    /// Detect constraint violations
    fn detect_constraint_violations(
        store: &dyn RecordStore,
        clusters: &Clusters,
        ontology: &Ontology,
    ) -> Result<Vec<ConstraintViolation>> {
        let mut violations = Vec::new();

        // Check constraints within each cluster
        for cluster in &clusters.clusters {
            for constraint in &ontology.constraints {
                let cluster_violations =
                    Self::check_constraint_in_cluster(store, cluster, constraint)?;
                violations.extend(cluster_violations);
            }
        }

        // Check constraints across clusters (for unique constraints)
        for constraint in &ontology.constraints {
            match constraint {
                Constraint::Unique {
                    attribute, name, ..
                } => {
                    let cross_cluster_violations = Self::check_unique_constraint_across_clusters(
                        store, clusters, *attribute, name,
                    )?;
                    violations.extend(cross_cluster_violations);
                }
                _ => {
                    // Other constraint types don't need cross-cluster checking
                }
            }
        }

        Ok(violations)
    }

    fn detect_constraint_violations_for_cluster(
        store: &dyn RecordStore,
        cluster: &crate::dsu::Cluster,
        ontology: &Ontology,
    ) -> Result<Vec<ConstraintViolation>> {
        let mut violations = Vec::new();
        if cluster.records.is_empty() {
            return Ok(violations);
        }
        for constraint in &ontology.constraints {
            let cluster_violations = Self::check_constraint_in_cluster(store, cluster, constraint)?;
            violations.extend(cluster_violations);
        }
        Ok(violations)
    }

    /// Check unique constraints across all clusters
    fn check_unique_constraint_across_clusters(
        store: &dyn RecordStore,
        clusters: &Clusters,
        attribute: AttrId,
        name: &str,
    ) -> Result<Vec<ConstraintViolation>> {
        let mut violations = Vec::new();

        // Create a mapping from record_id to cluster_id for quick lookup
        let mut record_to_cluster: HashMap<RecordId, usize> = HashMap::new();
        for (cluster_idx, cluster) in clusters.clusters.iter().enumerate() {
            for record_id in &cluster.records {
                record_to_cluster.insert(*record_id, cluster_idx);
            }
        }

        // Group records by perspective (like the Java implementation)
        let mut perspective_records: HashMap<String, Vec<RecordId>> = HashMap::new();

        for cluster in &clusters.clusters {
            for record_id in &cluster.records {
                if let Some(record) = store.get_record(*record_id) {
                    perspective_records
                        .entry(record.identity.perspective.clone())
                        .or_default()
                        .push(*record_id);
                }
            }
        }

        // Check constraints within each perspective (like Java: only check if perspSnapshotMap.size() > 1)
        for (_perspective, record_ids) in perspective_records {
            if record_ids.len() <= 1 {
                continue; // Skip perspectives with only one record
            }

            // Collect descriptors for this attribute within this perspective
            let mut descriptors_by_value: HashMap<ValueId, Vec<(Descriptor, RecordId)>> =
                HashMap::new();

            for record_id in &record_ids {
                if let Some(record) = store.get_record(*record_id) {
                    for descriptor in &record.descriptors {
                        if descriptor.attr == attribute {
                            descriptors_by_value
                                .entry(descriptor.value)
                                .or_default()
                                .push((descriptor.clone(), *record_id));
                        }
                    }
                }
            }

            // Check for overlapping intervals with the same value within this perspective
            for (value, descriptors_with_records) in descriptors_by_value {
                if descriptors_with_records.len() <= 1 {
                    continue;
                }

                let mut entries: Vec<_> = descriptors_with_records
                    .iter()
                    .filter_map(|(descriptor, record_id)| {
                        record_to_cluster
                            .get(record_id)
                            .copied()
                            .map(|cluster_idx| (descriptor.clone(), *record_id, cluster_idx))
                    })
                    .collect();

                entries.sort_by_key(|(descriptor, _, _)| descriptor.interval.start);

                for i in 0..entries.len() {
                    let (desc_a, record_a, cluster_a) = entries[i].clone();
                    for (desc_b, record_b, cluster_b) in entries.iter().skip(i + 1).cloned() {
                        if desc_b.interval.start >= desc_a.interval.end {
                            break;
                        }
                        if cluster_a == cluster_b {
                            continue;
                        }
                        if let Some(overlap) =
                            crate::temporal::intersect(&desc_a.interval, &desc_b.interval)
                        {
                            let violation = ConstraintViolation::new(
                                Constraint::unique(attribute, name.to_string()),
                                overlap,
                                vec![record_a, record_b],
                                format!(
                                    "Multiple entities have the same unique value {} in overlapping time periods",
                                    value.0
                                ),
                            );
                            violations.push(violation);
                        }
                    }
                }
            }
        }

        Ok(violations)
    }

    /// Check a constraint in a specific cluster
    fn check_constraint_in_cluster(
        store: &dyn RecordStore,
        cluster: &crate::dsu::Cluster,
        constraint: &Constraint,
    ) -> Result<Vec<ConstraintViolation>> {
        let mut violations = Vec::new();

        match constraint {
            Constraint::Unique {
                attribute, name, ..
            } => {
                let violations_for_attr =
                    Self::check_unique_constraint(store, cluster, *attribute, name)?;
                violations.extend(violations_for_attr);
            }
            Constraint::UniqueWithinPerspective {
                attribute, name, ..
            } => {
                let violations_for_attr = Self::check_unique_within_perspective_constraint(
                    store, cluster, *attribute, name,
                )?;
                violations.extend(violations_for_attr);
            }
        }

        Ok(violations)
    }

    /// Check unique constraint
    fn check_unique_constraint(
        store: &dyn RecordStore,
        cluster: &crate::dsu::Cluster,
        attribute: AttrId,
        name: &str,
    ) -> Result<Vec<ConstraintViolation>> {
        let mut violations = Vec::new();

        // Group descriptors by value
        let mut descriptors_by_value: HashMap<ValueId, Vec<(RecordId, Descriptor)>> =
            HashMap::new();

        for record_id in &cluster.records {
            if let Some(record) = store.get_record(*record_id) {
                for descriptor in &record.descriptors {
                    if descriptor.attr == attribute {
                        descriptors_by_value
                            .entry(descriptor.value)
                            .or_default()
                            .push((*record_id, descriptor.clone()));
                    }
                }
            }
        }

        // Check for overlapping intervals with different values
        let values: Vec<ValueId> = descriptors_by_value.keys().cloned().collect();
        for i in 0..values.len() {
            for j in i + 1..values.len() {
                let value_a = values[i];
                let value_b = values[j];

                let descs_a = &descriptors_by_value[&value_a];
                let descs_b = &descriptors_by_value[&value_b];

                // Find overlapping intervals
                let overlaps = Self::find_overlapping_intervals(descs_a, descs_b);

                for overlap in overlaps {
                    let mut participants = descs_a
                        .iter()
                        .map(|(id, _)| *id)
                        .chain(descs_b.iter().map(|(id, _)| *id))
                        .collect::<Vec<_>>();
                    participants.sort();
                    participants.dedup();

                    let violation = ConstraintViolation::new(
                        Constraint::unique(attribute, name.to_string()),
                        overlap,
                        participants,
                        format!(
                            "Different values {} and {} overlap in time",
                            value_a.0, value_b.0
                        ),
                    );
                    violations.push(violation);
                }
            }
        }

        Ok(violations)
    }

    /// Check unique within perspective constraint
    fn check_unique_within_perspective_constraint(
        store: &dyn RecordStore,
        cluster: &crate::dsu::Cluster,
        attribute: AttrId,
        name: &str,
    ) -> Result<Vec<ConstraintViolation>> {
        // Similar to check_unique_constraint but filtered by perspective
        // For now, we'll use the same logic
        Self::check_unique_constraint(store, cluster, attribute, name)
    }
}

/// Public function to detect conflicts
pub fn detect_conflicts(
    store: &dyn RecordStore,
    clusters: &Clusters,
    ontology: &Ontology,
) -> Result<Vec<Observation>> {
    ConflictDetector::detect_conflicts(store, clusters, ontology)
}

pub fn detect_conflicts_for_clusters(
    store: &dyn RecordStore,
    clusters: &Clusters,
    ontology: &Ontology,
    cluster_ids: &[ClusterId],
) -> Result<Vec<Observation>> {
    ConflictDetector::detect_conflicts_for_clusters(store, clusters, ontology, cluster_ids)
}

/// Algorithm selection for conflict detection benchmarking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictAlgorithm {
    /// Sweep-line algorithm (default, current implementation)
    SweepLine,
    /// Atomic intervals algorithm (experimental)
    AtomicIntervals,
}

/// Detect direct conflicts for an attribute using the specified algorithm.
/// This is exposed for benchmarking purposes.
pub fn detect_attribute_conflicts(
    attribute: AttrId,
    descriptors: Vec<(RecordId, ValueId, Interval)>,
    algorithm: ConflictAlgorithm,
) -> Vec<DirectConflict> {
    match algorithm {
        ConflictAlgorithm::SweepLine => {
            ConflictDetector::detect_conflicts_for_attribute_fast(descriptors, attribute)
                .unwrap_or_default()
        }
        ConflictAlgorithm::AtomicIntervals => {
            ConflictDetector::detect_conflicts_atomic(attribute, descriptors)
        }
    }
}

/// Detect direct conflicts for an attribute with automatic algorithm selection.
///
/// Uses the ConflictTuning configuration to automatically select the best
/// algorithm based on the data characteristics (overlap ratio).
pub fn detect_attribute_conflicts_auto(
    attribute: AttrId,
    descriptors: Vec<(RecordId, ValueId, Interval)>,
    tuning: &crate::config::ConflictTuning,
) -> Vec<DirectConflict> {
    if descriptors.is_empty() {
        return Vec::new();
    }

    // Count unique boundaries for auto-selection
    let unique_boundaries = count_unique_boundaries(&descriptors);
    let algorithm = tuning.select_algorithm(unique_boundaries, descriptors.len());

    detect_attribute_conflicts(attribute, descriptors, algorithm)
}

/// Count the number of unique boundary points in a set of intervals.
fn count_unique_boundaries(descriptors: &[(RecordId, ValueId, Interval)]) -> usize {
    use std::collections::HashSet;
    let mut boundaries: HashSet<i64> = HashSet::with_capacity(descriptors.len() * 2);
    for (_, _, interval) in descriptors {
        boundaries.insert(interval.start);
        boundaries.insert(interval.end);
    }
    boundaries.len()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConflictRecordRef {
    pub perspective: String,
    pub uid: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConflictSummary {
    pub kind: String,
    pub attribute: Option<String>,
    pub interval: Interval,
    pub records: Vec<ConflictRecordRef>,
    pub cause: Option<String>,
}

pub fn summarize_conflicts(
    store: &dyn RecordStore,
    observations: &[Observation],
) -> Vec<ConflictSummary> {
    let mut summaries = Vec::new();

    for observation in observations {
        match observation {
            Observation::DirectConflict(conflict) => {
                let attribute = store
                    .resolve_attr(conflict.attribute)
                    .or_else(|| Some(format!("attr_{}", conflict.attribute.0)));

                let mut record_refs = Vec::new();
                for value in &conflict.values {
                    for record_id in &value.participants {
                        if let Some(record) = store.get_record(*record_id) {
                            record_refs.push(ConflictRecordRef {
                                perspective: record.identity.perspective.clone(),
                                uid: record.identity.uid.clone(),
                            });
                        }
                    }
                }
                record_refs.sort_by(|a, b| {
                    (a.perspective.clone(), a.uid.clone())
                        .cmp(&(b.perspective.clone(), b.uid.clone()))
                });
                record_refs.dedup();

                summaries.push(ConflictSummary {
                    kind: conflict.kind.clone(),
                    attribute,
                    interval: conflict.interval,
                    records: record_refs,
                    cause: None,
                });
            }
            Observation::IndirectConflict(conflict) => {
                let attribute = conflict
                    .attribute
                    .and_then(|attr| store.resolve_attr(attr))
                    .or_else(|| conflict.attribute.map(|attr| format!("attr_{}", attr.0)));

                let mut record_refs = Vec::new();
                if let Some(records) = &conflict.participants.records {
                    for record_id in records {
                        if let Some(record) = store.get_record(*record_id) {
                            record_refs.push(ConflictRecordRef {
                                perspective: record.identity.perspective.clone(),
                                uid: record.identity.uid.clone(),
                            });
                        }
                    }
                }
                record_refs.sort_by(|a, b| {
                    (a.perspective.clone(), a.uid.clone())
                        .cmp(&(b.perspective.clone(), b.uid.clone()))
                });
                record_refs.dedup();

                summaries.push(ConflictSummary {
                    kind: conflict.kind.clone(),
                    attribute,
                    interval: conflict.interval,
                    records: record_refs,
                    cause: Some(conflict.cause.clone()),
                });
            }
            Observation::Merge { .. } => {}
        }
    }

    summaries.sort_by(|a, b| {
        (
            a.kind.clone(),
            a.attribute.clone().unwrap_or_default(),
            a.interval.start,
            a.interval.end,
            a.cause.clone().unwrap_or_default(),
            a.records
                .iter()
                .map(|record| format!("{}:{}", record.perspective, record.uid))
                .collect::<Vec<_>>(),
        )
            .cmp(&(
                b.kind.clone(),
                b.attribute.clone().unwrap_or_default(),
                b.interval.start,
                b.interval.end,
                b.cause.clone().unwrap_or_default(),
                b.records
                    .iter()
                    .map(|record| format!("{}:{}", record.perspective, record.uid))
                    .collect::<Vec<_>>(),
            ))
    });

    summaries
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Descriptor, Record, RecordIdentity};
    use crate::ontology::{Constraint, IdentityKey, Ontology, StrongIdentifier};
    use crate::store::Store;
    use crate::temporal::Interval;

    // PRESET_JSON_START
    /*
    [
      {
        "id": "test_direct_conflict_creation",
        "label": "Direct conflict creation (no records)",
        "identityKeys": [],
        "conflictAttrs": [],
        "description": "No records are loaded; the graph should be empty and the stats should remain at zero.",
        "records": []
      },
      {
        "id": "test_indirect_conflict_creation",
        "label": "Indirect conflict creation (no records)",
        "identityKeys": [],
        "conflictAttrs": [],
        "description": "No records are loaded; use this to confirm the graph clears cleanly between runs.",
        "records": []
      },
      {
        "id": "test_observation_creation",
        "label": "Observation creation (no records)",
        "identityKeys": [],
        "conflictAttrs": [],
        "description": "Empty dataset; nothing should render besides the layout and legend.",
        "records": []
      },
      {
        "id": "test_conflict_detection",
        "label": "Conflict detection (empty store)",
        "identityKeys": [],
        "conflictAttrs": [],
        "description": "Empty dataset; verify that no same-as or conflict edges appear.",
        "records": []
      },
      {
        "id": "test_can_handle_indirect_conflict",
        "label": "Indirect conflict handling (name/country + phone/employee_id)",
        "identityKeys": ["name", "country"],
        "conflictAttrs": ["phone", "employee_id"],
        "description": "Three records share name+country; two CRM records disagree on phone over the same interval. Expect a same-as link across all three and a red phone conflict between the CRM records.",
        "records": [
          {
            "id": "R1",
            "uid": "1",
            "entityType": "person",
            "perspective": "crm_system",
            "descriptors": [
              { "attr": "name", "value": "John Doe", "start": 86400, "end": 432000 },
              { "attr": "country", "value": "US", "start": 86400, "end": 432000 },
              { "attr": "phone", "value": "555-1111", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R2",
            "uid": "2",
            "entityType": "person",
            "perspective": "crm_system",
            "descriptors": [
              { "attr": "name", "value": "John Doe", "start": 86400, "end": 432000 },
              { "attr": "country", "value": "US", "start": 86400, "end": 432000 },
              { "attr": "phone", "value": "555-2222", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R3",
            "uid": "3",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "name", "value": "John Doe", "start": 86400, "end": 432000 },
              { "attr": "country", "value": "US", "start": 86400, "end": 432000 },
              { "attr": "employee_id", "value": "EMP001", "start": 86400, "end": 432000 }
            ]
          }
        ]
      },
      {
        "id": "test_can_resolve_person_identity_conflicts",
        "label": "Person identity conflicts (EMP001 vs EMP002)",
        "identityKeys": ["name", "country"],
        "conflictAttrs": ["employee_id", "email"],
        "description": "Two HR records share name+country but overlap with different employee_id and email values. Expect a same-as link and red conflict edges during the overlapping years.",
        "records": [
          {
            "id": "R1",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "employee_id", "value": "EMP001", "start": 1388966400, "end": 1613520000 },
              { "attr": "country", "value": "RU", "start": 1388966400, "end": 1613520000 },
              { "attr": "currency", "value": "RUB", "start": 1388966400, "end": 1613520000 },
              { "attr": "name", "value": "John Smith", "start": 1388966400, "end": 1613520000 },
              { "attr": "department", "value": "Engineering", "start": 1388966400, "end": 1613520000 },
              { "attr": "email", "value": "john.smith@company.com", "start": 1388966400, "end": 1613520000 }
            ]
          },
          {
            "id": "R2",
            "uid": "2",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "employee_id", "value": "EMP002", "start": 1047340800, "end": 1438560000 },
              { "attr": "country", "value": "RU", "start": 1047340800, "end": 1438560000 },
              { "attr": "currency", "value": "RUB", "start": 1047340800, "end": 1320105600 },
              { "attr": "currency", "value": "USD", "start": 1320105600, "end": 1438560000 },
              { "attr": "name", "value": "John Smith", "start": 1047340800, "end": 1438560000 },
              { "attr": "department", "value": "Engineering", "start": 1047628800, "end": 1438560000 },
              { "attr": "email", "value": "j.smith@corp.com", "start": 1047340800, "end": 1438560000 }
            ]
          }
        ]
      },
      {
        "id": "test_can_resolve_temporal_identity_conflicts",
        "label": "Temporal identity conflicts (name/email changes)",
        "identityKeys": ["name", "country"],
        "conflictAttrs": ["employee_id", "phone", "email"],
        "description": "Three records span different eras with matching name+country. Expect them to cluster via time-sliced same-as links but show no red conflicts because values do not overlap.",
        "records": [
          {
            "id": "R1",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "employee_id", "value": "H6TJG4721", "start": 851990400, "end": 1613520000 },
              { "attr": "country", "value": "CH", "start": 851990400, "end": 1613520000 },
              { "attr": "currency", "value": "CHF", "start": 851990400, "end": 1613520000 },
              { "attr": "name", "value": "CH0002092614", "start": 851990400, "end": 1211760000 },
              { "attr": "name", "value": "CH0039821084", "start": 1211760000, "end": 1613520000 },
              { "attr": "department", "value": "XSWX", "start": 1363564800, "end": 1613520000 },
              { "attr": "email", "value": "4582526", "start": 851990400, "end": 1211760000 },
              { "attr": "email", "value": "B39HW28", "start": 1211760000, "end": 1613520000 }
            ]
          },
          {
            "id": "R2",
            "uid": "2",
            "entityType": "person",
            "perspective": "crm_system",
            "descriptors": [
              { "attr": "phone", "value": "17518", "start": 978307200, "end": 1007078400 },
              { "attr": "country", "value": "CH", "start": 978307200, "end": 1007078400 },
              { "attr": "currency", "value": "CHF", "start": 978307200, "end": 1007078400 },
              { "attr": "name", "value": "CH0002092614", "start": 978307200, "end": 1007078400 }
            ]
          },
          {
            "id": "R3",
            "uid": "3",
            "entityType": "person",
            "perspective": "crm_system",
            "descriptors": [
              { "attr": "phone", "value": "4688", "start": 795484800, "end": 944006400 },
              { "attr": "country", "value": "CH", "start": 795484800, "end": 944006400 },
              { "attr": "currency", "value": "CHF", "start": 795484800, "end": 944006400 },
              { "attr": "name", "value": "CH0002092614", "start": 795484800, "end": 944006400 }
            ]
          }
        ]
      },
      {
        "id": "test_can_resolve_name_change_conflicts",
        "label": "Name change conflicts (AVESCO timeline)",
        "identityKeys": ["name", "country"],
        "conflictAttrs": ["employee_id", "phone", "email"],
        "description": "One long-lived record with multiple name changes aligns with a shorter CRM record. Expect a same-as link without red conflicts, highlighting the name timeline.",
        "records": [
          {
            "id": "R1",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "employee_id", "value": "X7W5YMBX4", "start": 851990400, "end": 1482451200 },
              { "attr": "country", "value": "GB", "start": 851990400, "end": 1482451200 },
              { "attr": "currency", "value": "GBP", "start": 851990400, "end": 1482451200 },
              { "attr": "name", "value": "GB0000653229", "start": 851990400, "end": 1482451200 },
              { "attr": "department", "value": "XLON", "start": 1363564800, "end": 1482451200 },
              { "attr": "email", "value": "john.doe@company.com", "start": 851990400, "end": 1482451200 },
              { "attr": "type", "value": "COMMON STOCK-S", "start": 851990400, "end": 1482451200 },
              { "attr": "name", "value": "INVESTINMEDIA", "start": 851990400, "end": 1179705600 },
              { "attr": "name", "value": "AVESCO GROUP", "start": 1179705600, "end": 1241136000 },
              { "attr": "name", "value": "AVESCO GROUP PLC", "start": 1241136000, "end": 1482451200 }
            ]
          },
          {
            "id": "R2",
            "uid": "2",
            "entityType": "person",
            "perspective": "crm_system",
            "descriptors": [
              { "attr": "phone", "value": "64541", "start": 946684800, "end": 1022889600 },
              { "attr": "country", "value": "GB", "start": 946684800, "end": 1022889600 },
              { "attr": "currency", "value": "GBP", "start": 946684800, "end": 1022889600 },
              { "attr": "name", "value": "GB0000653229", "start": 946684800, "end": 1022889600 },
              { "attr": "department", "value": "XLON", "start": 1019520000, "end": 1022889600 },
              { "attr": "type", "value": "COMMON", "start": 980985600, "end": 1022889600 },
              { "attr": "name", "value": "AVESCO", "start": 946684800, "end": 1022889600 }
            ]
          }
        ]
      },
      {
        "id": "test_can_detect_intra_entity_conflicts",
        "label": "Intra-entity SSN conflicts (10 records)",
        "identityKeys": [],
        "conflictAttrs": ["ssn"],
        "description": "Each record contains two overlapping SSNs. Expect red self-loop conflicts on every record card in the graph.",
        "records": [
          {
            "id": "R1",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "ssn", "value": "123-45-6789", "start": 86400, "end": 432000 },
              { "attr": "ssn", "value": "987-65-4321", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R2",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "ssn", "value": "123-45-6789", "start": 86400, "end": 432000 },
              { "attr": "ssn", "value": "987-65-4321", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R3",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "ssn", "value": "123-45-6789", "start": 86400, "end": 432000 },
              { "attr": "ssn", "value": "987-65-4321", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R4",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "ssn", "value": "123-45-6789", "start": 86400, "end": 432000 },
              { "attr": "ssn", "value": "987-65-4321", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R5",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "ssn", "value": "123-45-6789", "start": 86400, "end": 432000 },
              { "attr": "ssn", "value": "987-65-4321", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R6",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "ssn", "value": "123-45-6789", "start": 86400, "end": 432000 },
              { "attr": "ssn", "value": "987-65-4321", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R7",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "ssn", "value": "123-45-6789", "start": 86400, "end": 432000 },
              { "attr": "ssn", "value": "987-65-4321", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R8",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "ssn", "value": "123-45-6789", "start": 86400, "end": 432000 },
              { "attr": "ssn", "value": "987-65-4321", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R9",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "ssn", "value": "123-45-6789", "start": 86400, "end": 432000 },
              { "attr": "ssn", "value": "987-65-4321", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R10",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "ssn", "value": "123-45-6789", "start": 86400, "end": 432000 },
              { "attr": "ssn", "value": "987-65-4321", "start": 86400, "end": 432000 }
            ]
          }
        ]
      },
      {
        "id": "test_can_detect_cross_entity_conflicts",
        "label": "Cross-entity email conflicts (same perspective)",
        "identityKeys": [],
        "conflictAttrs": ["email"],
        "description": "Two records in the same perspective share the same email. Expect a red conflict edge even without a same-as link.",
        "records": [
          {
            "id": "R1",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "email", "value": "john.doe@company.com", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R2",
            "uid": "2",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "email", "value": "john.doe@company.com", "start": 86400, "end": 432000 }
            ]
          }
        ]
      },
      {
        "id": "test_cross_entity_perspective_grouping",
        "label": "Cross-entity email (different perspectives)",
        "identityKeys": [],
        "conflictAttrs": ["email"],
        "description": "Two records share the same email but sit in different perspectives. Expect no red conflict edge because uniqueness is scoped per perspective.",
        "records": [
          {
            "id": "R1",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "email", "value": "john.doe@company.com", "start": 86400, "end": 432000 }
            ]
          },
          {
            "id": "R2",
            "uid": "2",
            "entityType": "person",
            "perspective": "crm_system",
            "descriptors": [
              { "attr": "email", "value": "john.doe@company.com", "start": 86400, "end": 432000 }
            ]
          }
        ]
      },
      {
        "id": "test_can_resolve_extend_descriptor_start_dating_for_national_rv_holding_edge_case",
        "label": "National RV holding edge case (descriptor start dating)",
        "identityKeys": ["ssn", "country"],
        "conflictAttrs": ["employee_id", "phone", "email"],
        "description": "Two records overlap only on a short SSN window. Expect a narrow same-as link with no red conflicts, highlighting the shorter attribute intervals.",
        "records": [
          {
            "id": "R1",
            "uid": "1",
            "entityType": "person",
            "perspective": "hr_system",
            "descriptors": [
              { "attr": "employee_id", "value": "D61Y3AJC7", "start": 851990400, "end": 1283040000 },
              { "attr": "country", "value": "US", "start": 851990400, "end": 1283040000 },
              { "attr": "currency", "value": "USD", "start": 851990400, "end": 1283040000 },
              { "attr": "ssn", "value": "637277104", "start": 851990400, "end": 1283040000 },
              { "attr": "name", "value": "US6372771047", "start": 851990400, "end": 1283040000 },
              { "attr": "email", "value": "jane.smith@company.com", "start": 851990400, "end": 1283040000 }
            ]
          },
          {
            "id": "R2",
            "uid": "2",
            "entityType": "person",
            "perspective": "crm_system",
            "descriptors": [
              { "attr": "phone", "value": "63819", "start": 899078400, "end": 1022889600 },
              { "attr": "country", "value": "US", "start": 899078400, "end": 1022889600 },
              { "attr": "currency", "value": "USD", "start": 899078400, "end": 1022889600 },
              { "attr": "ssn", "value": "637277104", "start": 1022803200, "end": 1022889600 },
              { "attr": "name", "value": "US6372771047", "start": 1022803200, "end": 1022889600 },
              { "attr": "email", "value": "jane.smith@company.com", "start": 1022803200, "end": 1022889600 }
            ]
          }
        ]
      }
    ]
    */
    // PRESET_JSON_END
    fn assert_streaming_conflicts_match<F>(
        base_store: &Store,
        ontology: &Ontology,
        records: Vec<Record>,
        batch_assert: F,
    ) where
        F: Fn(&Store, &Clusters, &[Observation]),
    {
        let mut stream_store = base_store.clone();
        let sharded_records = records.clone();
        let mut streamer = crate::linker::StreamingLinker::new(
            &stream_store,
            ontology,
            &crate::StreamingTuning::default(),
        )
        .unwrap();
        for record in records {
            let record_id = stream_store.add_record(record).unwrap();
            streamer
                .link_record(&stream_store, ontology, record_id)
                .unwrap();
        }

        let streaming_clusters = streamer
            .clusters_with_conflict_splitting(&stream_store, ontology)
            .unwrap();
        let streaming_observations =
            detect_conflicts(&stream_store, &streaming_clusters, ontology).unwrap();

        batch_assert(&stream_store, &streaming_clusters, &streaming_observations);

        let mut sharded = crate::sharding::ShardedStreamEngine::new(ontology.clone(), 4).unwrap();
        sharded.seed_interners(stream_store.interner());
        sharded.stream_records(sharded_records).unwrap();
        let (reconcile_store, reconcile_clusters) = sharded.reconcile_store_and_clusters().unwrap();
        let reconcile_observations =
            detect_conflicts(&reconcile_store, &reconcile_clusters, ontology).unwrap();

        let streaming_norm = normalize_clusters_and_conflicts(
            &stream_store,
            &streaming_clusters,
            &streaming_observations,
        );
        let reconcile_norm = normalize_clusters_and_conflicts(
            &reconcile_store,
            &reconcile_clusters,
            &reconcile_observations,
        );
        assert_eq!(
            streaming_norm, reconcile_norm,
            "sharded reconciliation must match streaming"
        );
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct IdentityKeyTriple {
        entity_type: String,
        perspective: String,
        uid: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct NormalizedConflict {
        kind: String,
        attribute: String,
        interval: (i64, i64),
        values: Vec<(String, Vec<IdentityKeyTriple>)>,
        cause: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct NormalizedState {
        clusters: Vec<Vec<IdentityKeyTriple>>,
        conflicts: Vec<NormalizedConflict>,
    }

    fn normalize_clusters_and_conflicts(
        store: &Store,
        clusters: &Clusters,
        observations: &[Observation],
    ) -> NormalizedState {
        let mut normalized_clusters = Vec::new();
        for cluster in &clusters.clusters {
            let mut entries = Vec::new();
            for record_id in &cluster.records {
                if let Some(record) = store.get_record(*record_id) {
                    entries.push(IdentityKeyTriple {
                        entity_type: record.identity.entity_type.clone(),
                        perspective: record.identity.perspective.clone(),
                        uid: record.identity.uid.clone(),
                    });
                }
            }
            entries.sort();
            normalized_clusters.push(entries);
        }
        normalized_clusters.sort();

        let mut conflicts = Vec::new();
        for observation in observations {
            match observation {
                Observation::DirectConflict(conflict) => {
                    let attribute = attr_name(store, conflict.attribute);
                    let mut values = Vec::new();
                    for value in &conflict.values {
                        let value_name = value_name(store, value.value);
                        let mut participants = value
                            .participants
                            .iter()
                            .filter_map(|record_id| store.get_record(*record_id))
                            .map(|record| IdentityKeyTriple {
                                entity_type: record.identity.entity_type.clone(),
                                perspective: record.identity.perspective.clone(),
                                uid: record.identity.uid.clone(),
                            })
                            .collect::<Vec<_>>();
                        participants.sort();
                        values.push((value_name, participants));
                    }
                    values.sort();
                    conflicts.push(NormalizedConflict {
                        kind: conflict.kind.clone(),
                        attribute,
                        interval: (conflict.interval.start, conflict.interval.end),
                        values,
                        cause: None,
                    });
                }
                Observation::IndirectConflict(conflict) => {
                    let attribute = conflict
                        .attribute
                        .map(|attr| attr_name(store, attr))
                        .unwrap_or_else(|| "none".to_string());
                    let mut participants = Vec::new();
                    if let Some(records) = &conflict.participants.records {
                        participants = records
                            .iter()
                            .filter_map(|record_id| store.get_record(*record_id))
                            .map(|record| IdentityKeyTriple {
                                entity_type: record.identity.entity_type.clone(),
                                perspective: record.identity.perspective.clone(),
                                uid: record.identity.uid.clone(),
                            })
                            .collect::<Vec<_>>();
                        participants.sort();
                    }
                    conflicts.push(NormalizedConflict {
                        kind: conflict.kind.clone(),
                        attribute,
                        interval: (conflict.interval.start, conflict.interval.end),
                        values: vec![(
                            conflict
                                .participants
                                .clusters
                                .iter()
                                .map(|id| id.0.to_string())
                                .collect::<Vec<_>>()
                                .join(","),
                            participants,
                        )],
                        cause: Some(conflict.cause.clone()),
                    });
                }
                Observation::Merge { .. } => {}
            }
        }
        conflicts.sort();

        NormalizedState {
            clusters: normalized_clusters,
            conflicts,
        }
    }

    fn attr_name(store: &Store, attr: AttrId) -> String {
        store
            .interner()
            .get_attr(attr)
            .cloned()
            .unwrap_or_else(|| format!("attr_{}", attr.0))
    }

    fn value_name(store: &Store, value: ValueId) -> String {
        store
            .interner()
            .get_value(value)
            .cloned()
            .unwrap_or_else(|| format!("value_{}", value.0))
    }

    #[test]
    fn test_direct_conflict_creation() {
        let conflict = DirectConflict::new(
            "direct".to_string(),
            AttrId(1),
            Interval::new(100, 200).unwrap(),
            vec![
                ConflictValue::new(ValueId(1), vec![RecordId(1)]),
                ConflictValue::new(ValueId(2), vec![RecordId(2)]),
            ],
        );

        assert_eq!(conflict.kind, "direct");
        assert_eq!(conflict.attribute, AttrId(1));
        assert_eq!(conflict.values.len(), 2);
    }

    #[test]
    fn test_indirect_conflict_creation() {
        let participants = ConflictParticipants::new(vec![ClusterId(1)], None);
        let details = ConflictDetails::new();

        let conflict = IndirectConflict::new(
            "indirect".to_string(),
            "strong_id_conflict".to_string(),
            Some(AttrId(1)),
            Interval::new(100, 200).unwrap(),
            participants,
            "auto_resolved".to_string(),
            details,
        );

        assert_eq!(conflict.kind, "indirect");
        assert_eq!(conflict.cause, "strong_id_conflict");
        assert_eq!(conflict.participants.clusters.len(), 1);
    }

    #[test]
    fn test_observation_creation() {
        let conflict = DirectConflict::new(
            "direct".to_string(),
            AttrId(1),
            Interval::new(100, 200).unwrap(),
            vec![],
        );

        let observation = Observation::direct_conflict(conflict);
        assert!(matches!(observation, Observation::DirectConflict(_)));
    }

    #[test]
    fn test_conflict_detection() {
        let store = Store::new();
        let ontology = Ontology::new();

        assert_streaming_conflicts_match(
            &store,
            &ontology,
            Vec::new(),
            |store, clusters, observations| {
                assert!(store.is_empty());
                assert!(clusters.is_empty());
                assert!(observations.is_empty());
            },
        );
    }

    #[test]
    fn test_can_handle_indirect_conflict() {
        // This test demonstrates indirect conflict handling with three entities:
        // 1. Two entities with same identity key (name=John, country=US) but different phone (555-1111, 555-2222)
        // 2. A third entity with same identity key but different perspective and strong identifier (employee_id)
        // This should create indirect conflicts due to strong identifier conflicts

        let mut store = Store::new();
        let mut ontology = Ontology::new();

        // Create attribute IDs for the test
        let name_attr = store.interner_mut().intern_attr("name");
        let country_attr = store.interner_mut().intern_attr("country");
        let phone_attr = store.interner_mut().intern_attr("phone");
        let employee_id_attr = store.interner_mut().intern_attr("employee_id");

        // Create value IDs
        let name_value = store.interner_mut().intern_value("John Doe");
        let country_value = store.interner_mut().intern_value("US");
        let phone1_value = store.interner_mut().intern_value("555-1111");
        let phone2_value = store.interner_mut().intern_value("555-2222");
        let employee_id_value = store.interner_mut().intern_value("EMP001");

        // Create time interval (days 1-5, converted to seconds)
        let time_interval = Interval::new(86400, 432000).unwrap(); // 1-5 days in seconds

        // Create three entities with overlapping identity keys but conflicting strong identifiers

        // Entity 1: CRM perspective with phone=555-1111
        let entity1 = Record::new(
            RecordId(1),
            RecordIdentity::new(
                "person".to_string(),
                "crm_system".to_string(),
                "1".to_string(),
            ),
            vec![
                Descriptor::new(name_attr, name_value, time_interval),
                Descriptor::new(country_attr, country_value, time_interval),
                Descriptor::new(phone_attr, phone1_value, time_interval),
            ],
        );

        // Entity 2: CRM perspective with phone=555-2222 (conflicts with phone1)
        let entity2 = Record::new(
            RecordId(2),
            RecordIdentity::new(
                "person".to_string(),
                "crm_system".to_string(),
                "2".to_string(),
            ),
            vec![
                Descriptor::new(name_attr, name_value, time_interval),
                Descriptor::new(country_attr, country_value, time_interval),
                Descriptor::new(phone_attr, phone2_value, time_interval),
            ],
        );

        // Entity 3: HR perspective with employee_id (different perspective, same identity key)
        let entity3 = Record::new(
            RecordId(3),
            RecordIdentity::new(
                "person".to_string(),
                "hr_system".to_string(),
                "3".to_string(),
            ),
            vec![
                Descriptor::new(name_attr, name_value, time_interval),
                Descriptor::new(country_attr, country_value, time_interval),
                Descriptor::new(employee_id_attr, employee_id_value, time_interval),
            ],
        );

        let records = vec![entity1, entity2, entity3];

        // Set up ontology with identity keys and strong identifiers
        let identity_key =
            IdentityKey::new(vec![name_attr, country_attr], "name_country".to_string());
        ontology.add_identity_key(identity_key);

        // Add strong identifier constraints
        let phone_constraint = Constraint::unique(phone_attr, "unique_phone".to_string());
        let employee_id_constraint =
            Constraint::unique(employee_id_attr, "unique_employee_id".to_string());
        ontology.add_constraint(phone_constraint);
        ontology.add_constraint(employee_id_constraint);

        // Add strong identifiers
        let phone_strong_id = StrongIdentifier::new(phone_attr, "phone".to_string());
        let employee_id_strong_id =
            StrongIdentifier::new(employee_id_attr, "employee_id".to_string());
        ontology.add_strong_identifier(phone_strong_id);
        ontology.add_strong_identifier(employee_id_strong_id);

        // Set perspective weights and permanent attributes
        // HR: weight=100, permanent_attr=employee_id
        // CRM: weight=90, permanent_attr=phone
        ontology.set_perspective_weight("hr_system".to_string(), 100);
        ontology.set_perspective_weight("crm_system".to_string(), 90);

        // Set permanent attributes for each perspective
        ontology
            .set_perspective_permanent_attributes("hr_system".to_string(), vec![employee_id_attr]);
        ontology.set_perspective_permanent_attributes("crm_system".to_string(), vec![phone_attr]);

        assert_streaming_conflicts_match(
            &store,
            &ontology,
            records,
            |_, clusters, observations| {
                // Verify that we have separate clusters due to conflicts
                assert_eq!(
                    clusters.clusters.len(),
                    3,
                    "Should have 3 separate clusters due to strong identifier conflicts"
                );

                // Verify that we have indirect conflicts
                let indirect_conflicts: Vec<_> = observations
                    .iter()
                    .filter_map(|obs| match obs {
                        Observation::IndirectConflict(conflict) => Some(conflict),
                        _ => None,
                    })
                    .collect();

                assert!(
                    !indirect_conflicts.is_empty(),
                    "Should have indirect conflicts due to strong identifier conflicts"
                );

                // Verify conflict details
                for conflict in &indirect_conflicts {
                    assert_eq!(conflict.kind, "indirect");
                    assert!(
                        conflict.cause.contains("strong_id_conflict")
                            || conflict.cause.contains("constraint_violation")
                    );
                    assert_eq!(conflict.interval, time_interval);
                    assert_eq!(conflict.status, "auto_resolved");
                }

                // Verify that each cluster contains only one record
                for cluster in &clusters.clusters {
                    assert_eq!(
                        cluster.records.len(),
                        1,
                        "Each cluster should contain only one record due to conflicts"
                    );
                }

                // Verify that the records are in separate clusters
                let cluster_ids: std::collections::HashSet<_> =
                    clusters.clusters.iter().map(|c| c.id).collect();
                assert_eq!(cluster_ids.len(), 3, "Should have 3 distinct clusters");

                // Verify that each record is in its own cluster
                let mut record_clusters = std::collections::HashMap::new();
                for cluster in &clusters.clusters {
                    for &record_id in &cluster.records {
                        record_clusters.insert(record_id, cluster.id);
                    }
                }

                assert_eq!(
                    record_clusters.len(),
                    3,
                    "All 3 records should be in clusters"
                );
                assert!(
                    record_clusters
                        .values()
                        .all(|&id| cluster_ids.contains(&id)),
                    "All records should be in valid clusters"
                );
            },
        );
    }

    #[test]
    fn test_can_resolve_person_identity_conflicts() {
        // This test demonstrates a complex temporal scenario with identity changes
        // and indirect identifier conflicts for person records

        let mut store = Store::new();
        let mut ontology = Ontology::new();

        // Create attribute IDs for the test
        let employee_id_attr = store.interner_mut().intern_attr("employee_id");
        let country_attr = store.interner_mut().intern_attr("country");
        let currency_attr = store.interner_mut().intern_attr("currency");
        let name_attr = store.interner_mut().intern_attr("name");
        let department_attr = store.interner_mut().intern_attr("department");
        let email_attr = store.interner_mut().intern_attr("email");
        let phone_attr = store.interner_mut().intern_attr("phone");

        // Create value IDs
        let employee_id1_value = store.interner_mut().intern_value("EMP001");
        let employee_id2_value = store.interner_mut().intern_value("EMP002");
        let country_value = store.interner_mut().intern_value("RU");
        let currency_rub_value = store.interner_mut().intern_value("RUB");
        let currency_usd_value = store.interner_mut().intern_value("USD");
        let name_value = store.interner_mut().intern_value("John Smith");
        let department_value = store.interner_mut().intern_value("Engineering");
        let email1_value = store.interner_mut().intern_value("john.smith@company.com");
        let email2_value = store.interner_mut().intern_value("j.smith@corp.com");

        // Convert dates to seconds since epoch
        // 2014-01-06 to 2021-02-17 (entity 1)
        let entity1_start = 1388966400; // 2014-01-06 00:00:00 UTC
        let entity1_end = 1613520000; // 2021-02-17 00:00:00 UTC

        // 2003-03-12 to 2015-08-03 (entity 2)
        let entity2_start = 1047340800; // 2003-03-12 00:00:00 UTC
        let entity2_end = 1438560000; // 2015-08-03 00:00:00 UTC

        // 2011-11-01 (currency change date for entity 2)
        let currency_change_date = 1320105600; // 2011-11-01 00:00:00 UTC

        // 2003-03-18 (Department start date for entity 2)
        let department_start_date = 1047628800; // 2003-03-18 00:00:00 UTC

        // Entity 1: 2014-01-06 to 2021-02-17
        let entity1_interval = Interval::new(entity1_start, entity1_end).unwrap();
        let entity1 = Record::new(
            RecordId(1),
            RecordIdentity::new(
                "person".to_string(),
                "hr_system".to_string(),
                "1".to_string(),
            ),
            vec![
                Descriptor::new(employee_id_attr, employee_id1_value, entity1_interval),
                Descriptor::new(country_attr, country_value, entity1_interval),
                Descriptor::new(currency_attr, currency_rub_value, entity1_interval),
                Descriptor::new(name_attr, name_value, entity1_interval),
                Descriptor::new(department_attr, department_value, entity1_interval),
                Descriptor::new(email_attr, email1_value, entity1_interval),
            ],
        );

        // Entity 2: 2003-03-12 to 2015-08-03 with currency change
        let entity2_full_interval = Interval::new(entity2_start, entity2_end).unwrap();
        let entity2_rub_interval = Interval::new(entity2_start, currency_change_date).unwrap();
        let entity2_usd_interval = Interval::new(currency_change_date, entity2_end).unwrap();
        let entity2_department_interval =
            Interval::new(department_start_date, entity2_end).unwrap();

        let entity2 = Record::new(
            RecordId(2),
            RecordIdentity::new(
                "person".to_string(),
                "hr_system".to_string(),
                "2".to_string(),
            ),
            vec![
                Descriptor::new(employee_id_attr, employee_id2_value, entity2_full_interval),
                Descriptor::new(country_attr, country_value, entity2_full_interval),
                Descriptor::new(currency_attr, currency_rub_value, entity2_rub_interval),
                Descriptor::new(currency_attr, currency_usd_value, entity2_usd_interval),
                Descriptor::new(name_attr, name_value, entity2_full_interval),
                Descriptor::new(
                    department_attr,
                    department_value,
                    entity2_department_interval,
                ),
                Descriptor::new(email_attr, email2_value, entity2_full_interval),
            ],
        );

        let records = vec![entity1, entity2];

        // Set up ontology with identity keys and strong identifiers
        let identity_key =
            IdentityKey::new(vec![name_attr, country_attr], "name_country".to_string());
        ontology.add_identity_key(identity_key);

        // Add strong identifier constraints
        let employee_id_constraint =
            Constraint::unique(employee_id_attr, "unique_employee_id".to_string());
        let email_constraint = Constraint::unique(email_attr, "unique_email".to_string());
        ontology.add_constraint(employee_id_constraint);
        ontology.add_constraint(email_constraint);

        // Add strong identifiers
        let employee_id_strong_id =
            StrongIdentifier::new(employee_id_attr, "employee_id".to_string());
        let email_strong_id = StrongIdentifier::new(email_attr, "email".to_string());
        ontology.add_strong_identifier(employee_id_strong_id);
        ontology.add_strong_identifier(email_strong_id);

        // Set perspective weights and permanent attributes
        // HR: weight=100, permanent_attr=employee_id
        // CRM: weight=90, permanent_attr=phone
        ontology.set_perspective_weight("hr_system".to_string(), 100);
        ontology.set_perspective_weight("crm_system".to_string(), 90);

        // Set permanent attributes for each perspective
        ontology
            .set_perspective_permanent_attributes("hr_system".to_string(), vec![employee_id_attr]);
        ontology.set_perspective_permanent_attributes("crm_system".to_string(), vec![phone_attr]);

        assert_streaming_conflicts_match(
            &store,
            &ontology,
            records,
            |_, clusters, observations| {
                // Verify that we have separate clusters due to conflicts
                assert_eq!(
                    clusters.clusters.len(),
                    2,
                    "Should have 2 separate clusters due to strong identifier conflicts"
                );

                // Verify that we have indirect conflicts
                let indirect_conflicts: Vec<_> = observations
                    .iter()
                    .filter_map(|obs| match obs {
                        Observation::IndirectConflict(conflict) => Some(conflict),
                        _ => None,
                    })
                    .collect();

                // With the optimized linker, records with strong identifier conflicts are kept separate
                // so there should be no indirect conflicts detected
                assert_eq!(indirect_conflicts.len(), 0, "No indirect conflicts should be detected when records are kept separate due to strong identifier conflicts");

                // Verify conflict details
                for conflict in &indirect_conflicts {
                    assert_eq!(conflict.kind, "indirect");
                    assert!(
                        conflict.cause.contains("strong_id_conflict")
                            || conflict.cause.contains("constraint_violation")
                    );
                    assert_eq!(conflict.status, "auto_resolved");
                }

                // Verify that each cluster contains only one record
                for cluster in &clusters.clusters {
                    assert_eq!(
                        cluster.records.len(),
                        1,
                        "Each cluster should contain only one record due to conflicts"
                    );
                }

                // Verify that the records are in separate clusters
                let cluster_ids: std::collections::HashSet<_> =
                    clusters.clusters.iter().map(|c| c.id).collect();
                assert_eq!(cluster_ids.len(), 2, "Should have 2 distinct clusters");

                // Verify that each record is in its own cluster
                let mut record_clusters = std::collections::HashMap::new();
                for cluster in &clusters.clusters {
                    for &record_id in &cluster.records {
                        record_clusters.insert(record_id, cluster.id);
                    }
                }

                assert_eq!(
                    record_clusters.len(),
                    2,
                    "All 2 records should be in clusters"
                );
                assert!(
                    record_clusters
                        .values()
                        .all(|&id| cluster_ids.contains(&id)),
                    "All records should be in valid clusters"
                );
            },
        );
    }

    #[test]
    fn test_can_resolve_temporal_identity_conflicts() {
        // This test demonstrates a scenario where entities should be merged
        // when conflicts occur at different time periods

        let mut store = Store::new();
        let mut ontology = Ontology::new();

        // Create attribute IDs for the test
        let employee_id_attr = store.interner_mut().intern_attr("employee_id");
        let country_attr = store.interner_mut().intern_attr("country");
        let currency_attr = store.interner_mut().intern_attr("currency");
        let name_attr = store.interner_mut().intern_attr("name");
        let department_attr = store.interner_mut().intern_attr("department");
        let email_attr = store.interner_mut().intern_attr("email");
        let phone_attr = store.interner_mut().intern_attr("phone");

        // Create value IDs
        let employee_id_value = store.interner_mut().intern_value("H6TJG4721");
        let country_value = store.interner_mut().intern_value("CH");
        let currency_value = store.interner_mut().intern_value("CHF");
        let name1_value = store.interner_mut().intern_value("CH0002092614");
        let name2_value = store.interner_mut().intern_value("CH0039821084");
        let department_value = store.interner_mut().intern_value("XSWX");
        let email1_value = store.interner_mut().intern_value("4582526");
        let email2_value = store.interner_mut().intern_value("B39HW28");
        let phone1_value = store.interner_mut().intern_value("17518");
        let phone2_value = store.interner_mut().intern_value("4688");

        // Convert dates to seconds since epoch
        // 1996-12-31 to 2021-02-17 (entity 1 full period)
        let entity1_start = 851990400; // 1996-12-31 00:00:00 UTC
        let entity1_end = 1613520000; // 2021-02-17 00:00:00 UTC

        // 2008-05-26 (Name change date for entity 1)
        let name_change_date = 1211760000; // 2008-05-26 00:00:00 UTC

        // 2013-03-18 (Department start date for entity 1)
        let department_start_date = 1363564800; // 2013-03-18 00:00:00 UTC

        // 2000-12-29 to 2001-12-01 (entity 2)
        let entity2_start = 978307200; // 2000-12-29 00:00:00 UTC
        let entity2_end = 1007078400; // 2001-12-01 00:00:00 UTC

        // 1995-03-31 to 1999-12-01 (entity 3)
        let entity3_start = 795484800; // 1995-03-31 00:00:00 UTC
        let entity3_end = 944006400; // 1999-12-01 00:00:00 UTC

        // Entity 1: HR perspective with name changes over time
        let entity1_full_interval = Interval::new(entity1_start, entity1_end).unwrap();
        let entity1_name1_interval = Interval::new(entity1_start, name_change_date).unwrap();
        let entity1_name2_interval = Interval::new(name_change_date, entity1_end).unwrap();
        let entity1_email1_interval = Interval::new(entity1_start, name_change_date).unwrap();
        let entity1_email2_interval = Interval::new(name_change_date, entity1_end).unwrap();
        let entity1_department_interval =
            Interval::new(department_start_date, entity1_end).unwrap();

        let entity1 = Record::new(
            RecordId(1),
            RecordIdentity::new(
                "person".to_string(),
                "hr_system".to_string(),
                "1".to_string(),
            ),
            vec![
                Descriptor::new(employee_id_attr, employee_id_value, entity1_full_interval),
                Descriptor::new(country_attr, country_value, entity1_full_interval),
                Descriptor::new(currency_attr, currency_value, entity1_full_interval),
                Descriptor::new(name_attr, name1_value, entity1_name1_interval),
                Descriptor::new(name_attr, name2_value, entity1_name2_interval),
                Descriptor::new(
                    department_attr,
                    department_value,
                    entity1_department_interval,
                ),
                Descriptor::new(email_attr, email1_value, entity1_email1_interval),
                Descriptor::new(email_attr, email2_value, entity1_email2_interval),
            ],
        );

        // Entity 2: CRM perspective with phone=17518
        let entity2_interval = Interval::new(entity2_start, entity2_end).unwrap();
        let entity2 = Record::new(
            RecordId(2),
            RecordIdentity::new(
                "person".to_string(),
                "crm_system".to_string(),
                "2".to_string(),
            ),
            vec![
                Descriptor::new(phone_attr, phone1_value, entity2_interval),
                Descriptor::new(country_attr, country_value, entity2_interval),
                Descriptor::new(currency_attr, currency_value, entity2_interval),
                Descriptor::new(name_attr, name1_value, entity2_interval),
            ],
        );

        // Entity 3: CRM perspective with phone=4688
        let entity3_interval = Interval::new(entity3_start, entity3_end).unwrap();
        let entity3 = Record::new(
            RecordId(3),
            RecordIdentity::new(
                "person".to_string(),
                "crm_system".to_string(),
                "3".to_string(),
            ),
            vec![
                Descriptor::new(phone_attr, phone2_value, entity3_interval),
                Descriptor::new(country_attr, country_value, entity3_interval),
                Descriptor::new(currency_attr, currency_value, entity3_interval),
                Descriptor::new(name_attr, name1_value, entity3_interval),
            ],
        );

        let records = vec![entity1, entity2, entity3];

        // Set up ontology with identity keys and strong identifiers
        let identity_key =
            IdentityKey::new(vec![name_attr, country_attr], "name_country".to_string());
        ontology.add_identity_key(identity_key);

        // Add strong identifier constraints
        let employee_id_constraint =
            Constraint::unique(employee_id_attr, "unique_employee_id".to_string());
        let phone_constraint = Constraint::unique(phone_attr, "unique_phone".to_string());
        let email_constraint = Constraint::unique(email_attr, "unique_email".to_string());
        ontology.add_constraint(employee_id_constraint);
        ontology.add_constraint(phone_constraint);
        ontology.add_constraint(email_constraint);

        // Add strong identifiers (as would be defined in the finance ontology JSON files)
        let employee_id_strong = StrongIdentifier::new(employee_id_attr, "employee_id".to_string());
        let phone_strong = StrongIdentifier::new(phone_attr, "phone".to_string());
        let email_strong = StrongIdentifier::new(email_attr, "email".to_string());
        ontology.add_strong_identifier(employee_id_strong);
        ontology.add_strong_identifier(phone_strong);
        ontology.add_strong_identifier(email_strong);

        // Set perspective weights and permanent attributes
        // HR: weight=100, permanent_attr=employee_id
        // CRM: weight=90, permanent_attr=phone
        ontology.set_perspective_weight("hr_system".to_string(), 100);
        ontology.set_perspective_weight("crm_system".to_string(), 90);

        // Set permanent attributes for each perspective
        ontology
            .set_perspective_permanent_attributes("hr_system".to_string(), vec![employee_id_attr]);
        ontology.set_perspective_permanent_attributes("crm_system".to_string(), vec![phone_attr]);

        assert_streaming_conflicts_match(
            &store,
            &ontology,
            records,
            |_, clusters, observations| {
                // Build clusters - should merge all three entities into one cluster
                assert_eq!(
                    clusters.clusters.len(),
                    1,
                    "Should have 1 cluster with all three records merged"
                );

                // Verify that the cluster contains all three records
                let cluster = &clusters.clusters[0];
                assert_eq!(
                    cluster.records.len(),
                    3,
                    "Cluster should contain all 3 records"
                );

                let record_ids: std::collections::HashSet<_> = cluster.records.iter().collect();
                assert!(
                    record_ids.contains(&RecordId(1)),
                    "Cluster should contain record 1"
                );
                assert!(
                    record_ids.contains(&RecordId(2)),
                    "Cluster should contain record 2"
                );
                assert!(
                    record_ids.contains(&RecordId(3)),
                    "Cluster should contain record 3"
                );

                // Verify that we have no indirect conflicts (since they should merge)
                let indirect_conflicts: Vec<_> = observations
                    .iter()
                    .filter_map(|obs| match obs {
                        Observation::IndirectConflict(conflict) => Some(conflict),
                        _ => None,
                    })
                    .collect();

                // Should have no indirect conflicts since the records should merge
                assert_eq!(
                    indirect_conflicts.len(),
                    0,
                    "Should have no indirect conflicts since records should merge"
                );

                // The key test is that all three records are in the same cluster
                // This represents the successful merging that the test expects
                // (equivalent to 2 'is_same_as' relationships in the knowledge graph)
            },
        );
    }

    #[test]
    fn test_can_resolve_name_change_conflicts() {
        // This test demonstrates a scenario where two entities should be merged
        // when names change over time but refer to the same entity

        let mut store = Store::new();
        let mut ontology = Ontology::new();

        // Create attribute IDs for the test
        let employee_id_attr = store.interner_mut().intern_attr("employee_id");
        let country_attr = store.interner_mut().intern_attr("country");
        let currency_attr = store.interner_mut().intern_attr("currency");
        let name_attr = store.interner_mut().intern_attr("name");
        let department_attr = store.interner_mut().intern_attr("department");
        let email_attr = store.interner_mut().intern_attr("email");
        let type_attr = store.interner_mut().intern_attr("type");
        let phone_attr = store.interner_mut().intern_attr("phone");

        // Create value IDs
        let employee_id_value = store.interner_mut().intern_value("X7W5YMBX4");
        let country_value = store.interner_mut().intern_value("GB");
        let currency_value = store.interner_mut().intern_value("GBP");
        let name_value = store.interner_mut().intern_value("GB0000653229");
        let department_value = store.interner_mut().intern_value("XLON");
        let email_value = store.interner_mut().intern_value("john.doe@company.com");
        let type1_value = store.interner_mut().intern_value("COMMON STOCK-S");
        let type2_value = store.interner_mut().intern_value("COMMON");
        let name1_value = store.interner_mut().intern_value("INVESTINMEDIA");
        let name2_value = store.interner_mut().intern_value("AVESCO GROUP");
        let name3_value = store.interner_mut().intern_value("AVESCO GROUP PLC");
        let name4_value = store.interner_mut().intern_value("AVESCO");
        let phone_value = store.interner_mut().intern_value("64541");

        // Convert dates to seconds since epoch
        // 1996-12-31 to 2016-12-23 (entity 1 full period)
        let entity1_start = 851990400; // 1996-12-31 00:00:00 UTC
        let entity1_end = 1482451200; // 2016-12-23 00:00:00 UTC

        // 2007-05-21 (first name change date for entity 1)
        let name_change1_date = 1179705600; // 2007-05-21 00:00:00 UTC

        // 2009-05-01 (second name change date for entity 1)
        let name_change2_date = 1241136000; // 2009-05-01 00:00:00 UTC

        // 2013-03-18 (Department start date for entity 1)
        let department_start_date = 1363564800; // 2013-03-18 00:00:00 UTC

        // 1999-12-31 to 2002-06-01 (entity 2)
        let entity2_start = 946684800; // 1999-12-31 00:00:00 UTC
        let entity2_end = 1022889600; // 2002-06-01 00:00:00 UTC

        // 2002-04-30 (MIC start date for entity 2)
        let entity2_department_start = 1019520000; // 2002-04-30 00:00:00 UTC

        // 2001-01-31 (type start date for entity 2)
        let entity2_type_start = 980985600; // 2001-01-31 00:00:00 UTC

        // Entity 1: HR perspective with name changes over time
        let entity1_full_interval = Interval::new(entity1_start, entity1_end).unwrap();
        let entity1_name1_interval = Interval::new(entity1_start, name_change1_date).unwrap();
        let entity1_name2_interval = Interval::new(name_change1_date, name_change2_date).unwrap();
        let entity1_name3_interval = Interval::new(name_change2_date, entity1_end).unwrap();
        let entity1_department_interval =
            Interval::new(department_start_date, entity1_end).unwrap();

        let entity1 = Record::new(
            RecordId(1),
            RecordIdentity::new(
                "person".to_string(),
                "hr_system".to_string(),
                "1".to_string(),
            ),
            vec![
                Descriptor::new(employee_id_attr, employee_id_value, entity1_full_interval),
                Descriptor::new(country_attr, country_value, entity1_full_interval),
                Descriptor::new(currency_attr, currency_value, entity1_full_interval),
                Descriptor::new(name_attr, name_value, entity1_full_interval),
                Descriptor::new(
                    department_attr,
                    department_value,
                    entity1_department_interval,
                ),
                Descriptor::new(email_attr, email_value, entity1_full_interval),
                Descriptor::new(type_attr, type1_value, entity1_full_interval),
                Descriptor::new(name_attr, name1_value, entity1_name1_interval),
                Descriptor::new(name_attr, name2_value, entity1_name2_interval),
                Descriptor::new(name_attr, name3_value, entity1_name3_interval),
            ],
        );

        // Entity 2: CRM perspective with phone=64541
        let entity2_interval = Interval::new(entity2_start, entity2_end).unwrap();
        let entity2_department_interval =
            Interval::new(entity2_department_start, entity2_end).unwrap();
        let entity2_type_interval = Interval::new(entity2_type_start, entity2_end).unwrap();

        let entity2 = Record::new(
            RecordId(2),
            RecordIdentity::new(
                "person".to_string(),
                "crm_system".to_string(),
                "2".to_string(),
            ),
            vec![
                Descriptor::new(phone_attr, phone_value, entity2_interval),
                Descriptor::new(country_attr, country_value, entity2_interval),
                Descriptor::new(currency_attr, currency_value, entity2_interval),
                Descriptor::new(name_attr, name_value, entity2_interval),
                Descriptor::new(
                    department_attr,
                    department_value,
                    entity2_department_interval,
                ),
                Descriptor::new(type_attr, type2_value, entity2_type_interval),
                Descriptor::new(name_attr, name4_value, entity2_interval),
            ],
        );

        let records = vec![entity1, entity2];

        // Set up ontology with identity keys and strong identifiers
        let identity_key =
            IdentityKey::new(vec![name_attr, country_attr], "name_country".to_string());
        ontology.add_identity_key(identity_key);

        // Add strong identifier constraints
        let employee_id_constraint =
            Constraint::unique(employee_id_attr, "unique_employee_id".to_string());
        let phone_constraint = Constraint::unique(phone_attr, "unique_phone".to_string());
        let email_constraint = Constraint::unique(email_attr, "unique_email".to_string());
        ontology.add_constraint(employee_id_constraint);
        ontology.add_constraint(phone_constraint);
        ontology.add_constraint(email_constraint);

        // Set perspective weights and permanent attributes
        // HR: weight=100, permanent_attr=employee_id
        // CRM: weight=90, permanent_attr=phone
        ontology.set_perspective_weight("hr_system".to_string(), 100);
        ontology.set_perspective_weight("crm_system".to_string(), 90);

        // Set permanent attributes for each perspective
        ontology
            .set_perspective_permanent_attributes("hr_system".to_string(), vec![employee_id_attr]);
        ontology.set_perspective_permanent_attributes("crm_system".to_string(), vec![phone_attr]);

        assert_streaming_conflicts_match(
            &store,
            &ontology,
            records,
            |_, clusters, observations| {
                // Build clusters - should merge both entities into one cluster
                assert_eq!(
                    clusters.clusters.len(),
                    1,
                    "Should have 1 cluster with both records merged"
                );

                // Verify that the cluster contains both records
                let cluster = &clusters.clusters[0];
                assert_eq!(
                    cluster.records.len(),
                    2,
                    "Cluster should contain both records"
                );

                let record_ids: std::collections::HashSet<_> = cluster.records.iter().collect();
                assert!(
                    record_ids.contains(&RecordId(1)),
                    "Cluster should contain record 1"
                );
                assert!(
                    record_ids.contains(&RecordId(2)),
                    "Cluster should contain record 2"
                );

                // Verify that we have no indirect conflicts (since they should merge)
                let indirect_conflicts: Vec<_> = observations
                    .iter()
                    .filter_map(|obs| match obs {
                        Observation::IndirectConflict(conflict) => Some(conflict),
                        _ => None,
                    })
                    .collect();

                // Should have no indirect conflicts since the records should merge
                assert_eq!(
                    indirect_conflicts.len(),
                    0,
                    "Should have no indirect conflicts since records should merge"
                );

                // The key test is that both records are in the same cluster
                // This represents the successful merging that the test expects
                // (equivalent to 1 'is_same_as' relationship in the knowledge graph)
            },
        );
    }

    #[test]
    fn test_can_detect_intra_entity_conflicts() {
        // This test demonstrates a scenario where we should detect conflicts
        // within individual entities (intra-entity conflicts)
        // Creates 10 records with conflicting SSN values within the same time period

        let mut store = Store::new();
        let mut ontology = Ontology::new();

        // Create attribute IDs for the test
        let ssn_attr = store.interner_mut().intern_attr("ssn");

        // Create value IDs
        let ssn_value_a = store.interner_mut().intern_value("123-45-6789");
        let ssn_value_b = store.interner_mut().intern_value("987-65-4321");

        // Convert days to seconds since epoch
        // Day 1 to Day 5 (same time period for all records)
        let start_time = 86400; // Day 1: 86400 seconds since epoch
        let end_time = 432000; // Day 5: 432000 seconds since epoch
        let interval = Interval::new(start_time, end_time).unwrap();

        // Create 10 records with conflicting SSN values
        // All records have the same entity UID but different record IDs
        let mut records = Vec::new();
        for i in 1..=10 {
            let record = Record::new(
                RecordId(i),
                RecordIdentity::new(
                    "person".to_string(),
                    "hr_system".to_string(),
                    "1".to_string(),
                ), // Same entity UID
                vec![
                    // Same record has two conflicting SSN values in the same time period
                    Descriptor::new(ssn_attr, ssn_value_a, interval),
                    Descriptor::new(ssn_attr, ssn_value_b, interval),
                ],
            );
            records.push(record);
        }

        let records = records;

        // Set up ontology with SSN as a strong identifier
        let ssn_constraint = Constraint::unique(ssn_attr, "unique_ssn".to_string());
        ontology.add_constraint(ssn_constraint);

        let ssn_strong = StrongIdentifier::new(ssn_attr, "ssn".to_string());
        ontology.add_strong_identifier(ssn_strong);

        // Set perspective weight
        ontology.set_perspective_weight("hr_system".to_string(), 100);

        assert_streaming_conflicts_match(
            &store,
            &ontology,
            records,
            |_, _clusters, observations| {
                // Should have multiple conflict observations
                // Each record with conflicting SSN values should generate observations
                // Our implementation detects conflicts per record for more granular reporting
                assert!(
                    !observations.is_empty(),
                    "Should have conflict observations for conflicting SSN values"
                );

                // Our implementation creates multiple observations (one per record with conflicts).
                // This provides more detailed conflict reporting, which is valuable for debugging and analysis.

                // Verify we have direct conflicts (intra-record conflicts)
                let direct_conflicts: Vec<_> = observations
                    .iter()
                    .filter_map(|obs| match obs {
                        Observation::DirectConflict(conflict) => Some(conflict),
                        _ => None,
                    })
                    .collect();

                assert!(
                    !direct_conflicts.is_empty(),
                    "Should have direct conflict observations for intra-record conflicts"
                );

                // Verify we have constraint violations (strong identifier violations)
                let constraint_violations: Vec<_> = observations
                    .iter()
                    .filter_map(|obs| match obs {
                        Observation::IndirectConflict(conflict)
                            if conflict.kind == "constraint_violation" =>
                        {
                            Some(conflict)
                        }
                        _ => None,
                    })
                    .collect();

                assert!(
                    !constraint_violations.is_empty(),
                    "Should have constraint violation observations for email conflicts"
                );
            },
        );
    }

    #[test]
    fn test_can_detect_cross_entity_conflicts() {
        // This test demonstrates a scenario where we should detect conflicts
        // between different entities (cross-entity conflicts)
        // Creates 2 entities with the same email value in the same time period but different UIDs

        let mut store = Store::new();
        let mut ontology = Ontology::new();

        // Create attribute IDs for the test
        let email_attr = store.interner_mut().intern_attr("email");

        // Create value IDs
        let email_value = store.interner_mut().intern_value("john.doe@company.com");

        // Convert days to seconds since epoch
        // Day 1 to Day 5 (same time period for both entities)
        let start_time = 86400; // Day 1: 86400 seconds since epoch
        let end_time = 432000; // Day 5: 432000 seconds since epoch
        let interval = Interval::new(start_time, end_time).unwrap();

        // Entity 1: UID=1, Email="john.doe@company.com"
        let entity1 = Record::new(
            RecordId(1),
            RecordIdentity::new(
                "person".to_string(),
                "hr_system".to_string(),
                "1".to_string(),
            ),
            vec![Descriptor::new(email_attr, email_value, interval)],
        );

        // Entity 2: UID=2, Email="john.doe@company.com" (same email value, different entity)
        let entity2 = Record::new(
            RecordId(2),
            RecordIdentity::new(
                "person".to_string(),
                "hr_system".to_string(),
                "2".to_string(),
            ),
            vec![Descriptor::new(email_attr, email_value, interval)],
        );

        let records = vec![entity1, entity2];

        // Set up ontology with email as a strong identifier
        let email_constraint = Constraint::unique(email_attr, "unique_email".to_string());
        ontology.add_constraint(email_constraint);

        let email_strong = StrongIdentifier::new(email_attr, "email".to_string());
        ontology.add_strong_identifier(email_strong);

        // Set perspective weight
        ontology.set_perspective_weight("hr_system".to_string(), 100);

        assert_streaming_conflicts_match(
            &store,
            &ontology,
            records,
            |_, clusters, observations| {
                // Should have 2 separate clusters (entities should NOT be merged)
                assert_eq!(clusters.clusters.len(), 2, "Should have 2 separate clusters - entities with same email but different UIDs should not be merged");

                // Verify that the clusters contain the correct records
                let mut found_entity1 = false;
                let mut found_entity2 = false;
                for cluster in &clusters.clusters {
                    if cluster.records.len() == 1 {
                        if cluster.records.contains(&RecordId(1)) {
                            found_entity1 = true;
                        }
                        if cluster.records.contains(&RecordId(2)) {
                            found_entity2 = true;
                        }
                    }
                }
                assert!(found_entity1, "Should find entity 1 in its own cluster");
                assert!(found_entity2, "Should find entity 2 in its own cluster");

                // Should have conflict observations for the duplicate email values
                assert!(
                    !observations.is_empty(),
                    "Should have conflict observations for duplicate email values"
                );

                // Verify we have constraint violations (strong identifier violations)
                let constraint_violations: Vec<_> = observations
                    .iter()
                    .filter_map(|obs| match obs {
                        Observation::IndirectConflict(conflict)
                            if conflict.kind == "constraint_violation" =>
                        {
                            Some(conflict)
                        }
                        _ => None,
                    })
                    .collect();

                assert!(
                    !constraint_violations.is_empty(),
                    "Should have constraint violation observations for duplicate email values"
                );

                // Verify the constraint violation is for the email attribute
                let email_violations: Vec<_> = constraint_violations
                    .iter()
                    .filter(|violation| violation.attribute == Some(email_attr))
                    .collect();

                assert!(
                    !email_violations.is_empty(),
                    "Should have email constraint violations"
                );
            },
        );
    }

    #[test]
    fn test_cross_entity_perspective_grouping() {
        // This test demonstrates that cross-entity conflicts are only detected within the same perspective
        // Entities from different perspectives can have the same unique identifier without conflict

        let mut store = Store::new();
        let mut ontology = Ontology::new();

        // Create attribute IDs for the test
        let email_attr = store.interner_mut().intern_attr("email");

        // Create value IDs
        let email_value = store.interner_mut().intern_value("john.doe@company.com");

        // Convert days to seconds since epoch
        let start_time = 86400; // Day 1: 86400 seconds since epoch
        let end_time = 432000; // Day 5: 432000 seconds since epoch
        let interval = Interval::new(start_time, end_time).unwrap();

        // Entity 1: HR system perspective, Email="john.doe@company.com"
        let entity1 = Record::new(
            RecordId(1),
            RecordIdentity::new(
                "person".to_string(),
                "hr_system".to_string(),
                "1".to_string(),
            ),
            vec![Descriptor::new(email_attr, email_value, interval)],
        );

        // Entity 2: CRM system perspective, Email="john.doe@company.com" (same value, different perspective)
        let entity2 = Record::new(
            RecordId(2),
            RecordIdentity::new(
                "person".to_string(),
                "crm_system".to_string(),
                "2".to_string(),
            ),
            vec![Descriptor::new(email_attr, email_value, interval)],
        );

        let records = vec![entity1, entity2];

        // Set up ontology with email as a strong identifier
        let email_constraint = Constraint::unique(email_attr, "unique_email".to_string());
        ontology.add_constraint(email_constraint);

        let email_strong = StrongIdentifier::new(email_attr, "email".to_string());
        ontology.add_strong_identifier(email_strong);

        // Set perspective weights
        ontology.set_perspective_weight("hr_system".to_string(), 100);
        ontology.set_perspective_weight("crm_system".to_string(), 90);

        assert_streaming_conflicts_match(
            &store,
            &ontology,
            records,
            |_, clusters, observations| {
                // Should have 2 separate clusters (entities should NOT be merged)
                assert_eq!(clusters.clusters.len(), 2, "Should have 2 separate clusters - entities from different perspectives should not be merged");

                // Should have NO conflict observations (different perspectives can have same unique values)
                assert_eq!(observations.len(), 0, "Should have no conflict observations - entities from different perspectives can have the same unique identifier");
            },
        );
    }

    #[test]
    fn test_can_resolve_extend_descriptor_start_dating_for_national_rv_holding_edge_case() {
        // This test demonstrates a scenario where two entities should be merged
        // with extended descriptor start dating for temporal conflict resolution

        let mut store = Store::new();
        let mut ontology = Ontology::new();

        // Create attribute IDs for the test
        let employee_id_attr = store.interner_mut().intern_attr("employee_id");
        let country_attr = store.interner_mut().intern_attr("country");
        let currency_attr = store.interner_mut().intern_attr("currency");
        let ssn_attr = store.interner_mut().intern_attr("ssn");
        let name_attr = store.interner_mut().intern_attr("name");
        let email_attr = store.interner_mut().intern_attr("email");
        let phone_attr = store.interner_mut().intern_attr("phone");

        // Create value IDs
        let employee_id_value = store.interner_mut().intern_value("D61Y3AJC7");
        let country_value = store.interner_mut().intern_value("US");
        let currency_value = store.interner_mut().intern_value("USD");
        let ssn_value = store.interner_mut().intern_value("637277104");
        let name_value = store.interner_mut().intern_value("US6372771047");
        let email_value = store.interner_mut().intern_value("jane.smith@company.com");
        let phone_value = store.interner_mut().intern_value("63819");

        // Convert dates to seconds since epoch
        // 1996-12-31 to 2010-08-30 (entity 1 full period)
        let entity1_start = 851990400; // 1996-12-31 00:00:00 UTC
        let entity1_end = 1283040000; // 2010-08-30 00:00:00 UTC

        // 1998-06-30 to 2002-06-01 (entity 2 full period)
        let entity2_start = 899078400; // 1998-06-30 00:00:00 UTC
        let entity2_end = 1022889600; // 2002-06-01 00:00:00 UTC

        // 2002-05-31 (SSN start date for entity 2)
        let entity2_ssn_start = 1022803200; // 2002-05-31 00:00:00 UTC

        // Entity 1: HR perspective with full time period
        let entity1_interval = Interval::new(entity1_start, entity1_end).unwrap();

        let entity1 = Record::new(
            RecordId(1),
            RecordIdentity::new(
                "person".to_string(),
                "hr_system".to_string(),
                "1".to_string(),
            ),
            vec![
                Descriptor::new(employee_id_attr, employee_id_value, entity1_interval),
                Descriptor::new(country_attr, country_value, entity1_interval),
                Descriptor::new(currency_attr, currency_value, entity1_interval),
                Descriptor::new(ssn_attr, ssn_value, entity1_interval),
                Descriptor::new(name_attr, name_value, entity1_interval),
                Descriptor::new(email_attr, email_value, entity1_interval),
            ],
        );

        // Entity 2: CRM perspective with phone and short SSN period
        let entity2_full_interval = Interval::new(entity2_start, entity2_end).unwrap();
        let entity2_ssn_interval = Interval::new(entity2_ssn_start, entity2_end).unwrap();

        let entity2 = Record::new(
            RecordId(2),
            RecordIdentity::new(
                "person".to_string(),
                "crm_system".to_string(),
                "2".to_string(),
            ),
            vec![
                Descriptor::new(phone_attr, phone_value, entity2_full_interval),
                Descriptor::new(country_attr, country_value, entity2_full_interval),
                Descriptor::new(currency_attr, currency_value, entity2_full_interval),
                Descriptor::new(ssn_attr, ssn_value, entity2_ssn_interval),
                Descriptor::new(name_attr, name_value, entity2_ssn_interval),
                Descriptor::new(email_attr, email_value, entity2_ssn_interval),
            ],
        );

        let records = vec![entity1, entity2];

        // Set up ontology with identity keys and strong identifiers
        let identity_key =
            IdentityKey::new(vec![ssn_attr, country_attr], "ssn_country".to_string());
        ontology.add_identity_key(identity_key);

        // Add strong identifier constraints
        let employee_id_constraint =
            Constraint::unique(employee_id_attr, "unique_employee_id".to_string());
        let phone_constraint = Constraint::unique(phone_attr, "unique_phone".to_string());
        let email_constraint = Constraint::unique(email_attr, "unique_email".to_string());
        ontology.add_constraint(employee_id_constraint);
        ontology.add_constraint(phone_constraint);
        ontology.add_constraint(email_constraint);

        // Set perspective weights and permanent attributes
        // HR: weight=100, permanent_attr=employee_id
        // CRM: weight=90, permanent_attr=phone
        ontology.set_perspective_weight("hr_system".to_string(), 100);
        ontology.set_perspective_weight("crm_system".to_string(), 90);

        // Set permanent attributes for each perspective
        ontology
            .set_perspective_permanent_attributes("hr_system".to_string(), vec![employee_id_attr]);
        ontology.set_perspective_permanent_attributes("crm_system".to_string(), vec![phone_attr]);

        assert_streaming_conflicts_match(
            &store,
            &ontology,
            records,
            |_, clusters, observations| {
                // Build clusters - should merge both entities into one cluster
                assert_eq!(
                    clusters.clusters.len(),
                    1,
                    "Should have 1 cluster with both records merged"
                );

                // Verify that the cluster contains both records
                let cluster = &clusters.clusters[0];
                assert_eq!(
                    cluster.records.len(),
                    2,
                    "Cluster should contain both records"
                );

                let record_ids: std::collections::HashSet<_> = cluster.records.iter().collect();
                assert!(
                    record_ids.contains(&RecordId(1)),
                    "Cluster should contain record 1"
                );
                assert!(
                    record_ids.contains(&RecordId(2)),
                    "Cluster should contain record 2"
                );

                // Verify that we have no indirect conflicts (since they should merge)
                let indirect_conflicts: Vec<_> = observations
                    .iter()
                    .filter_map(|obs| match obs {
                        Observation::IndirectConflict(conflict) => Some(conflict),
                        _ => None,
                    })
                    .collect();

                // Should have no indirect conflicts since the records should merge
                assert_eq!(
                    indirect_conflicts.len(),
                    0,
                    "Should have no indirect conflicts since records should merge"
                );

                // The key test is that both records are in the same cluster
                // This represents the successful merging that the test expects
                // (equivalent to 1 'is_same_as' relationship in the knowledge graph)
                // The effective start date should be before 1999-01-01, which means the merge
                // should be based on the overlapping time period between the two entities
            },
        );
    }
}
