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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRecordReservation {
    pub identity: RecordIdentity,
    pub payload_digest: [u8; 32],
    pub target_shard_id: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceReservationError {
    #[error("source record identity is already reserved for a different payload")]
    PayloadConflict,
    #[error(
        "source record identity is already assigned to shard {existing_shard}, not shard {requested_shard}"
    )]
    TargetConflict {
        existing_shard: u32,
        requested_shard: u32,
    },
}

/// Persistence abstraction for records and metadata.
pub trait RecordStore: Send + Sync {
    /// Fail if the store observed an I/O or decode error that made a prior read incomplete.
    fn ensure_healthy(&self) -> Result<()> {
        Ok(())
    }

    /// Remove all records and derived state while keeping the store usable.
    fn reset_data(&mut self) -> Result<()> {
        anyhow::bail!("reset is not supported by this record store")
    }

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

    /// Borrow a record when the backend can lend one without cloning.
    /// The default returns `None`; callers can fall back to [`Self::get_record`].
    fn get_record_ref(&self, _id: RecordId) -> Option<&Record> {
        None // Default implementation returns None, callers should use get_record
    }

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

    /// Apply a fallible function to every record in ascending record-ID order.
    ///
    /// Persistent implementations should stream their ordered keyspace instead
    /// of materializing all records so linker recovery stays sequential.
    fn try_for_each_record_ordered(&self, f: &mut dyn FnMut(Record) -> Result<()>) -> Result<()> {
        let mut records = self.get_all_records();
        records.sort_by_key(|record| record.id.0);
        for record in records {
            f(record)?;
        }
        Ok(())
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

    /// Look up a string without allocating an ID or mutating the store.
    fn lookup_attr(&self, attr: &str) -> Option<AttrId> {
        self.interner().get_attr_id(attr)
    }

    /// Look up a string without allocating an ID or mutating the store.
    fn lookup_value(&self, value: &str) -> Option<ValueId> {
        self.interner().get_value_id(value)
    }

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

    /// Persist cross-shard conflicts detected by distributed reconciliation.
    fn set_cross_shard_conflicts(
        &mut self,
        _conflicts: &[crate::sharding::CrossShardConflict],
    ) -> Result<()> {
        Ok(())
    }

    /// Load persisted cross-shard conflicts.
    fn load_cross_shard_conflicts(&self) -> Result<Vec<crate::sharding::CrossShardConflict>> {
        Ok(Vec::new())
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

    /// Persist multiple record -> cluster assignments in a single batch.
    fn set_cluster_assignments_batch(
        &mut self,
        assignments: &[(RecordId, crate::model::ClusterId)],
    ) -> Result<()> {
        for (record_id, cluster_id) in assignments {
            self.set_cluster_assignment(*record_id, *cluster_id)?;
        }
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

    /// Batch add records if their identities have not been seen.
    /// Returns Vec of (record_id, inserted) in the same order as input.
    fn add_records_if_absent(&mut self, records: Vec<Record>) -> Result<Vec<(RecordId, bool)>> {
        let mut results = Vec::with_capacity(records.len());
        for record in records {
            results.push(self.add_record_if_absent(record)?);
        }
        Ok(results)
    }

    /// Stage a record for later batch write. Returns (record_id, inserted).
    /// Default implementation just calls add_record_if_absent.
    fn stage_record_if_absent(&mut self, record: Record) -> Result<(RecordId, bool)> {
        self.add_record_if_absent(record)
    }

    /// Stage a record while preserving its explicit ID.
    fn stage_record_with_explicit_id_if_absent(
        &mut self,
        _record: Record,
    ) -> Result<(RecordId, bool)> {
        Err(anyhow::anyhow!(
            "explicit record ID staging is not supported by this store"
        ))
    }

    /// Flush all staged records to the database. Returns count of flushed records.
    /// Default implementation returns 0 (no staging support).
    fn flush_staged_records(&mut self) -> Result<usize> {
        Ok(0)
    }

    /// Abandon records staged by a failed ingest before they can be committed by
    /// a later request. Implementations without staging have nothing to discard.
    fn discard_staged_records(&mut self) -> Result<()> {
        Ok(())
    }

    /// Make all completed writes durable on stable storage.
    ///
    /// In-memory stores have nothing to synchronize. Persistent implementations
    /// must not return until their recovery log is synced.
    fn sync(&self) -> Result<()> {
        Ok(())
    }

    /// Atomically reserve immutable source identities before distributed ingest.
    ///
    /// The returned target shard IDs correspond to the input order. Persistent
    /// implementations must make newly created reservations durable before returning.
    fn reserve_source_records(
        &mut self,
        _reservations: &[SourceRecordReservation],
    ) -> Result<Vec<u32>> {
        anyhow::bail!("source record reservations are not supported by this record store")
    }

    /// Return the completed source-reservation migration protocol and shard count.
    fn source_reservation_backfill(&self) -> Result<Option<(u32, u32)>> {
        Ok(None)
    }

    /// Durably mark all records currently stored on this shard as reserved.
    fn mark_source_reservation_backfill(
        &mut self,
        _protocol_version: u32,
        _shard_count: u32,
    ) -> Result<()> {
        anyhow::bail!("source reservation migration markers are not supported by this record store")
    }

    /// Optional store-level metrics.
    fn metrics(&self) -> Option<StoreMetrics> {
        None
    }

    /// Get a shared reference to the underlying database, if available.
    /// Used by persistent DSU and tiered index backends.
    fn shared_db(&self) -> Option<std::sync::Arc<rocksdb::DB>> {
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
    /// Cluster-wide immutable source identities owned by this shard.
    source_reservations: HashMap<RecordIdentity, ([u8; 32], u32)>,
    /// Completed distributed reservation migration version and topology size.
    source_reservation_backfill: Option<(u32, u32)>,
    /// String interner for attributes and values
    interner: StringInterner,
    /// Attribute-value index for fast lookups
    attribute_value_index: AttributeValueIndex,
    /// Temporal index for interval queries
    temporal_index: TemporalIndex,
    /// Next available record ID
    next_record_id: u32,
    /// Inserts made through staging remain removable until the batch commits.
    staged_record_ids: Vec<RecordId>,
}

pub(crate) fn records_have_same_payload(left: &Record, right: &Record) -> bool {
    if left.identity != right.identity || left.descriptors.len() != right.descriptors.len() {
        return false;
    }

    let canonical_descriptors = |record: &Record| {
        let mut descriptors = record
            .descriptors
            .iter()
            .map(|descriptor| {
                (
                    descriptor.attr.0,
                    descriptor.value.0,
                    descriptor.interval.start,
                    descriptor.interval.end,
                )
            })
            .collect::<Vec<_>>();
        descriptors.sort_unstable();
        descriptors
    };

    canonical_descriptors(left) == canonical_descriptors(right)
}

fn ensure_idempotent_record(existing: &Record, incoming: &Record) -> Result<()> {
    if records_have_same_payload(existing, incoming) {
        Ok(())
    } else {
        anyhow::bail!("source record identity already exists with a different payload")
    }
}

impl Store {
    const EXHAUSTED_RECORD_ID: u32 = u32::MAX;

    /// Create a new store
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
            identity_index: HashMap::new(),
            source_reservations: HashMap::new(),
            source_reservation_backfill: None,
            interner: StringInterner::new(),
            attribute_value_index: AttributeValueIndex::new(),
            temporal_index: TemporalIndex::new(),
            next_record_id: 0,
            staged_record_ids: Vec::new(),
        }
    }

    /// Create a new store with a preloaded interner and record ID counter.
    pub fn with_interner(interner: StringInterner, next_record_id: u32) -> Self {
        Self {
            records: HashMap::new(),
            identity_index: HashMap::new(),
            source_reservations: HashMap::new(),
            source_reservation_backfill: None,
            interner,
            attribute_value_index: AttributeValueIndex::new(),
            temporal_index: TemporalIndex::new(),
            next_record_id,
            staged_record_ids: Vec::new(),
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

    fn allocate_record_id(&mut self) -> Result<RecordId> {
        if self.next_record_id == Self::EXHAUSTED_RECORD_ID {
            anyhow::bail!("record ID space exhausted");
        }
        let record_id = RecordId(self.next_record_id);
        self.next_record_id += 1;
        Ok(record_id)
    }

    fn advance_past_explicit_record_id(&mut self, record_id: RecordId) {
        self.next_record_id = self.next_record_id.max(record_id.0.saturating_add(1));
    }

    /// Prepare a record for persistence without storing it in memory.
    pub fn prepare_record(&mut self, record: &mut Record) -> Result<RecordId> {
        self.intern_record(record);

        if record.id.0 == 0 {
            record.id = self.allocate_record_id()?;
        } else {
            if record.id.0 == Self::EXHAUSTED_RECORD_ID {
                anyhow::bail!("record ID {} is reserved", Self::EXHAUSTED_RECORD_ID);
            }
            self.advance_past_explicit_record_id(record.id);
        }

        Ok(record.id)
    }

    /// Prepare a record without treating ID zero as an allocation sentinel.
    pub fn prepare_record_with_explicit_id(&mut self, record: &mut Record) -> Result<RecordId> {
        self.intern_record(record);
        if record.id.0 == Self::EXHAUSTED_RECORD_ID {
            anyhow::bail!("record ID {} is reserved", Self::EXHAUSTED_RECORD_ID);
        }
        self.advance_past_explicit_record_id(record.id);
        Ok(record.id)
    }

    /// Add a single record to the store and return its assigned ID.
    pub fn add_record(&mut self, mut record: Record) -> Result<RecordId> {
        if record.id.0 != 0 && self.records.contains_key(&record.id) {
            anyhow::bail!("record ID {} already exists", record.id.0);
        }
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
        if self.records.contains_key(&record.id) {
            anyhow::bail!("record ID {} already exists", record.id.0);
        }
        self.intern_record(&mut record);
        if record.id.0 == Self::EXHAUSTED_RECORD_ID {
            anyhow::bail!("record ID {} is reserved", Self::EXHAUSTED_RECORD_ID);
        }

        self.advance_past_explicit_record_id(record.id);

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
            let stored = self
                .records
                .get(&existing)
                .ok_or_else(|| anyhow::anyhow!("identity index references a missing record"))?;
            ensure_idempotent_record(stored, &record)?;
            return Ok((existing, false));
        }
        let id = self.add_record(record)?;
        Ok((id, true))
    }

    fn reserve_source_records(
        &mut self,
        reservations: &[SourceRecordReservation],
    ) -> Result<Vec<u32>> {
        let mut pending = HashMap::with_capacity(reservations.len());
        let mut targets = Vec::with_capacity(reservations.len());

        for reservation in reservations {
            let existing = self
                .source_reservations
                .get(&reservation.identity)
                .copied()
                .or_else(|| pending.get(&reservation.identity).copied());
            if let Some((payload_digest, target_shard_id)) = existing {
                if payload_digest != reservation.payload_digest {
                    return Err(SourceReservationError::PayloadConflict.into());
                }
                if target_shard_id != reservation.target_shard_id {
                    return Err(SourceReservationError::TargetConflict {
                        existing_shard: target_shard_id,
                        requested_shard: reservation.target_shard_id,
                    }
                    .into());
                }
                targets.push(target_shard_id);
            } else {
                pending.insert(
                    reservation.identity.clone(),
                    (reservation.payload_digest, reservation.target_shard_id),
                );
                targets.push(reservation.target_shard_id);
            }
        }

        self.source_reservations.extend(pending);
        Ok(targets)
    }

    fn source_reservation_backfill(&self) -> Option<(u32, u32)> {
        self.source_reservation_backfill
    }

    fn mark_source_reservation_backfill(&mut self, protocol_version: u32, shard_count: u32) {
        self.source_reservation_backfill = Some((protocol_version, shard_count));
    }

    /// Add a record without re-interning. Used when records are pre-interned
    /// with an external interner (e.g., ConcurrentInterner in partitioned mode).
    ///
    /// IMPORTANT: Only use this when you know the AttrIds and ValueIds in the
    /// record are valid and don't need to be re-mapped to this Store's interner.
    pub fn add_record_raw(&mut self, mut record: Record) -> Result<RecordId> {
        // Assign record ID without interning
        if record.id.0 == 0 {
            record.id = self.allocate_record_id()?;
        } else {
            if record.id.0 == Self::EXHAUSTED_RECORD_ID {
                anyhow::bail!("record ID {} is reserved", Self::EXHAUSTED_RECORD_ID);
            }
            self.advance_past_explicit_record_id(record.id);
        }

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

    /// Add a record if its identity is new, without re-interning.
    /// Used when records are pre-interned with an external interner.
    pub fn add_record_if_absent_raw(&mut self, record: Record) -> Result<(RecordId, bool)> {
        if let Some(existing) = self.get_record_id_by_identity(&record.identity) {
            let stored = self
                .records
                .get(&existing)
                .ok_or_else(|| anyhow::anyhow!("identity index references a missing record"))?;
            ensure_idempotent_record(stored, &record)?;
            return Ok((existing, false));
        }
        let id = self.add_record_raw(record)?;
        Ok((id, true))
    }

    /// Get a record by ID
    pub fn get_record(&self, id: RecordId) -> Option<Record> {
        self.records.get(&id).cloned()
    }

    /// Get a reference to a record by ID (avoids cloning).
    pub fn get_record_ref(&self, id: RecordId) -> Option<&Record> {
        self.records.get(&id)
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
    fn reset_data(&mut self) -> Result<()> {
        *self = Store::new();
        Ok(())
    }

    fn add_record(&mut self, record: Record) -> Result<RecordId> {
        Store::add_record(self, record)
    }

    fn add_records(&mut self, records: Vec<Record>) -> Result<()> {
        Store::add_records(self, records)
    }

    fn get_record(&self, id: RecordId) -> Option<Record> {
        Store::get_record(self, id)
    }

    fn get_record_ref(&self, id: RecordId) -> Option<&Record> {
        Store::get_record_ref(self, id)
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

    fn reserve_source_records(
        &mut self,
        reservations: &[SourceRecordReservation],
    ) -> Result<Vec<u32>> {
        Store::reserve_source_records(self, reservations)
    }

    fn source_reservation_backfill(&self) -> Result<Option<(u32, u32)>> {
        Ok(Store::source_reservation_backfill(self))
    }

    fn mark_source_reservation_backfill(
        &mut self,
        protocol_version: u32,
        shard_count: u32,
    ) -> Result<()> {
        Store::mark_source_reservation_backfill(self, protocol_version, shard_count);
        Ok(())
    }

    fn add_record_if_absent(&mut self, record: Record) -> Result<(RecordId, bool)> {
        Store::add_record_if_absent(self, record)
    }

    fn stage_record_if_absent(&mut self, record: Record) -> Result<(RecordId, bool)> {
        let (id, inserted) = self.add_record_if_absent(record)?;
        if inserted {
            self.staged_record_ids.push(id);
        }
        Ok((id, inserted))
    }

    fn flush_staged_records(&mut self) -> Result<usize> {
        let count = self.staged_record_ids.len();
        self.staged_record_ids.clear();
        Ok(count)
    }

    fn discard_staged_records(&mut self) -> Result<()> {
        if self.staged_record_ids.is_empty() {
            return Ok(());
        }
        for id in self.staged_record_ids.drain(..) {
            if let Some(record) = self.records.remove(&id) {
                self.identity_index.remove(&record.identity);
            }
        }
        // Abort is an exceptional path; rebuilding avoids retaining index entries
        // for records whose staging never committed.
        self.attribute_value_index = AttributeValueIndex::new();
        self.temporal_index = TemporalIndex::new();
        for record in self.records.values() {
            self.attribute_value_index.add_record(record);
            self.temporal_index.add_record(record);
        }
        Ok(())
    }

    fn stage_record_with_explicit_id_if_absent(
        &mut self,
        record: Record,
    ) -> Result<(RecordId, bool)> {
        if let Some(existing) = self.get_record_id_by_identity(&record.identity) {
            let stored = self
                .records
                .get(&existing)
                .ok_or_else(|| anyhow::anyhow!("identity index references a missing record"))?;
            ensure_idempotent_record(stored, &record)?;
            return Ok((existing, false));
        }
        if self.records.contains_key(&record.id) {
            anyhow::bail!("record ID {} already exists", record.id.0);
        }
        let id = self.insert_record(record)?;
        self.staged_record_ids.push(id);
        Ok((id, true))
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
    fn aborted_staging_removes_records_and_secondary_indexes() {
        let mut store = Store::new();
        let attr = store.interner_mut().intern_attr("email");
        let value = store.interner_mut().intern_value("shared@example.com");
        let interval = Interval::new(0, 10).unwrap();
        let make_record = |source: &str| {
            Record::new(
                RecordId(0),
                RecordIdentity::new("person".into(), "crm".into(), source.into()),
                vec![Descriptor::new(attr, value, interval)],
            )
        };
        let committed = store.add_record(make_record("committed")).unwrap();
        let (pending, _) = store
            .stage_record_if_absent(make_record("pending"))
            .unwrap();
        assert_eq!(store.get_records_in_interval(interval).len(), 2);
        store.discard_staged_records().unwrap();
        assert!(store.get_record(pending).is_none());
        assert_eq!(store.get_records_in_interval(interval).len(), 1);
        assert_eq!(store.get_records_with_attribute(attr)[0].id, committed);
        let (retried, inserted) = store
            .stage_record_if_absent(make_record("pending"))
            .unwrap();
        assert!(inserted);
        store.flush_staged_records().unwrap();
        store.discard_staged_records().unwrap();
        assert!(store.get_record(retried).is_some());
    }

    #[test]
    fn record_id_allocation_fails_closed_at_u32_boundary() {
        let mut store = Store::new();
        store.set_next_record_id(u32::MAX - 1);

        let last = Record::new(
            RecordId(0),
            RecordIdentity::new(
                "person".to_string(),
                "crm".to_string(),
                "last-id".to_string(),
            ),
            vec![],
        );
        assert_eq!(store.add_record(last).unwrap(), RecordId(u32::MAX - 1));

        let overflow = Record::new(
            RecordId(0),
            RecordIdentity::new(
                "person".to_string(),
                "crm".to_string(),
                "overflow".to_string(),
            ),
            vec![],
        );
        let error = store
            .add_record(overflow)
            .expect_err("record IDs must never wrap");
        assert!(error.to_string().contains("record ID space exhausted"));
        assert_eq!(store.len(), 1);
        assert!(store.get_record(RecordId(u32::MAX - 1)).is_some());
        assert!(store.get_record(RecordId(0)).is_none());
    }

    #[test]
    fn explicit_last_record_id_exhausts_future_allocation() {
        let mut store = Store::new();
        let explicit = Record::new(
            RecordId(u32::MAX - 1),
            RecordIdentity::new(
                "person".to_string(),
                "import".to_string(),
                "max-id".to_string(),
            ),
            vec![],
        );
        store
            .stage_record_with_explicit_id_if_absent(explicit)
            .unwrap();

        let next = Record::new(
            RecordId(0),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "next".to_string()),
            vec![],
        );
        let error = store
            .add_record(next)
            .expect_err("allocation after the maximum explicit ID must fail");
        assert!(error.to_string().contains("record ID space exhausted"));

        let reserved = Record::new(
            RecordId(u32::MAX),
            RecordIdentity::new(
                "person".to_string(),
                "import".to_string(),
                "reserved".to_string(),
            ),
            vec![],
        );
        let error = store
            .stage_record_with_explicit_id_if_absent(reserved)
            .expect_err("the exhaustion sentinel must not be assignable");
        assert!(error.to_string().contains("is reserved"));
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
    fn source_reservations_are_atomic_and_immutable() {
        let mut store = Store::new();
        let identity_a =
            RecordIdentity::new("person".to_string(), "crm".to_string(), "a".to_string());
        let identity_b =
            RecordIdentity::new("person".to_string(), "crm".to_string(), "b".to_string());
        let reservation_a = SourceRecordReservation {
            identity: identity_a.clone(),
            payload_digest: [1; 32],
            target_shard_id: 2,
        };

        assert_eq!(
            store
                .reserve_source_records(std::slice::from_ref(&reservation_a))
                .unwrap(),
            vec![2]
        );
        assert_eq!(
            store
                .reserve_source_records(std::slice::from_ref(&reservation_a))
                .unwrap(),
            vec![2]
        );

        let error = store
            .reserve_source_records(&[
                SourceRecordReservation {
                    identity: identity_b.clone(),
                    payload_digest: [2; 32],
                    target_shard_id: 1,
                },
                SourceRecordReservation {
                    identity: identity_a,
                    payload_digest: [3; 32],
                    target_shard_id: 2,
                },
            ])
            .expect_err("a conflicting batch must fail atomically");
        assert!(error.downcast_ref::<SourceReservationError>().is_some());

        assert_eq!(
            store
                .reserve_source_records(&[SourceRecordReservation {
                    identity: identity_b,
                    payload_digest: [4; 32],
                    target_shard_id: 0,
                }])
                .unwrap(),
            vec![0],
            "the first item from the rejected batch must not be retained"
        );
    }

    #[test]
    fn repeated_identity_requires_the_same_semantic_payload() {
        let mut store = Store::new();
        let email = store.intern_attr("email");
        let phone = store.intern_attr("phone");
        let email_value = store.intern_value("same@example.com");
        let phone_value = store.intern_value("555-0100");
        let interval = Interval::new(0, 10).unwrap();
        let identity = RecordIdentity::new(
            "person".to_string(),
            "crm".to_string(),
            "immutable-1".to_string(),
        );
        let first = Record::new(
            RecordId(0),
            identity.clone(),
            vec![
                Descriptor::new(email, email_value, interval),
                Descriptor::new(phone, phone_value, interval),
            ],
        );
        let reordered_retry = Record::new(
            RecordId(0),
            identity.clone(),
            vec![
                Descriptor::new(phone, phone_value, interval),
                Descriptor::new(email, email_value, interval),
            ],
        );
        let changed = Record::new(
            RecordId(0),
            identity,
            vec![Descriptor::new(email, phone_value, interval)],
        );

        let (record_id, inserted) = store.add_record_if_absent(first).unwrap();
        assert!(inserted);
        let (retry_id, inserted) = store
            .add_record_if_absent(reordered_retry)
            .expect("descriptor ordering must not change payload identity");
        assert_eq!(retry_id, record_id);
        assert!(!inserted);

        let error = store
            .add_record_if_absent(changed)
            .expect_err("changed payload must not be silently discarded");
        assert!(error.to_string().contains("different payload"));
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
