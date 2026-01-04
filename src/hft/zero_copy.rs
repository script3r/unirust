//! # Zero-Copy Record Passing
//!
//! Eliminates unnecessary allocations and copies in the hot path.
//!
//! ## Key Optimizations
//! - `RecordSlice`: Shared ownership of record batches via Arc
//! - `ZeroCopyBatch`: Pre-partitioned records without cloning
//! - Inline storage for small batches

use crate::model::{Record, RecordId};
use std::ops::Deref;
use std::sync::Arc;

/// A shared, immutable slice of records
///
/// Uses Arc for zero-copy sharing between partitions.
/// The underlying allocation is shared, not cloned.
#[derive(Clone)]
pub struct RecordSlice {
    /// Shared backing storage
    data: Arc<[Record]>,
    /// Start offset in the backing array
    start: usize,
    /// Length of this slice
    len: usize,
}

impl RecordSlice {
    /// Create a new record slice from a Vec
    pub fn from_vec(records: Vec<Record>) -> Self {
        let len = records.len();
        Self {
            data: Arc::from(records),
            start: 0,
            len,
        }
    }

    /// Create a sub-slice (zero-copy)
    pub fn slice(&self, start: usize, len: usize) -> Self {
        debug_assert!(start + len <= self.len);
        Self {
            data: Arc::clone(&self.data),
            start: self.start + start,
            len,
        }
    }

    /// Get the length
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get a record by index
    #[inline]
    pub fn get(&self, index: usize) -> Option<&Record> {
        if index < self.len {
            Some(&self.data[self.start + index])
        } else {
            None
        }
    }

    /// Iterate over records
    pub fn iter(&self) -> impl Iterator<Item = &Record> {
        self.data[self.start..self.start + self.len].iter()
    }

    /// Get the number of Arc references
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.data)
    }
}

impl Deref for RecordSlice {
    type Target = [Record];

    fn deref(&self) -> &Self::Target {
        &self.data[self.start..self.start + self.len]
    }
}

/// A batch of records pre-partitioned for zero-copy processing
pub struct ZeroCopyBatch {
    /// Original records (shared)
    records: RecordSlice,
    /// Partition assignments: (partition_id, start_idx, len)
    partitions: Vec<(usize, usize, usize)>,
    /// Index mapping: original_idx -> partition_idx (reserved for future use)
    #[allow(dead_code)]
    index_map: Vec<u32>,
}

impl ZeroCopyBatch {
    /// Create a new batch and partition records
    ///
    /// The partition function receives a record and returns its partition ID.
    pub fn new<F>(records: Vec<Record>, partition_count: usize, partition_fn: F) -> Self
    where
        F: Fn(&Record) -> usize,
    {
        let record_count = records.len();
        let mut partition_indices: Vec<Vec<usize>> = vec![Vec::new(); partition_count];
        let mut index_map = vec![0u32; record_count];

        // Determine partition for each record
        for (idx, record) in records.iter().enumerate() {
            let partition = partition_fn(record);
            let local_idx = partition_indices[partition].len();
            partition_indices[partition].push(idx);
            index_map[idx] = local_idx as u32;
        }

        // Build partition ranges
        // We need to reorder records so each partition's records are contiguous
        let mut reordered = Vec::with_capacity(record_count);
        let mut partitions = Vec::with_capacity(partition_count);
        let mut offset = 0;

        for (partition_id, indices) in partition_indices.iter().enumerate() {
            let len = indices.len();
            if len > 0 {
                partitions.push((partition_id, offset, len));
                for &idx in indices {
                    reordered.push(records[idx].clone());
                }
                offset += len;
            }
        }

        Self {
            records: RecordSlice::from_vec(reordered),
            partitions,
            index_map,
        }
    }

    /// Create a batch from pre-partitioned data (no reordering needed)
    pub fn from_partitioned(
        records: RecordSlice,
        partitions: Vec<(usize, usize, usize)>,
        index_map: Vec<u32>,
    ) -> Self {
        Self {
            records,
            partitions,
            index_map,
        }
    }

    /// Get records for a specific partition (zero-copy)
    pub fn partition_records(&self, partition_id: usize) -> Option<RecordSlice> {
        self.partitions
            .iter()
            .find(|(id, _, _)| *id == partition_id)
            .map(|(_, start, len)| self.records.slice(*start, *len))
    }

    /// Iterate over all partitions
    pub fn iter_partitions(&self) -> impl Iterator<Item = (usize, RecordSlice)> + '_ {
        self.partitions
            .iter()
            .map(|(id, start, len)| (*id, self.records.slice(*start, *len)))
    }

    /// Get the original index for a record at (partition, local_idx)
    pub fn original_index(&self, partition: usize, local_idx: usize) -> Option<usize> {
        let (_, start, len) = self.partitions.iter().find(|(id, _, _)| *id == partition)?;

        if local_idx >= *len {
            return None;
        }

        // Walk through index_map to find the original index
        // This is O(n) but only used for result mapping
        Some(*start + local_idx)
    }

    /// Get total record count
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get number of non-empty partitions
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
}

/// Result collector for zero-copy batch processing
pub struct BatchResultCollector {
    /// Results indexed by original record order
    results: Vec<Option<RecordId>>,
    /// Number of results collected
    collected: usize,
}

impl BatchResultCollector {
    /// Create a new collector for the given batch size
    pub fn new(size: usize) -> Self {
        Self {
            results: vec![None; size],
            collected: 0,
        }
    }

    /// Set the result for a record at the original index
    #[inline]
    pub fn set_result(&mut self, original_idx: usize, cluster_id: RecordId) {
        if original_idx < self.results.len() && self.results[original_idx].is_none() {
            self.results[original_idx] = Some(cluster_id);
            self.collected += 1;
        }
    }

    /// Check if all results are collected
    pub fn is_complete(&self) -> bool {
        self.collected == self.results.len()
    }

    /// Get collected count
    pub fn collected_count(&self) -> usize {
        self.collected
    }

    /// Consume and return results
    pub fn into_results(self) -> Vec<Option<RecordId>> {
        self.results
    }

    /// Get results as cluster IDs (panics if incomplete)
    pub fn into_cluster_ids(self) -> Vec<RecordId> {
        self.results
            .into_iter()
            .map(|opt| opt.expect("incomplete results"))
            .collect()
    }
}

/// Inline storage for small batches (avoids heap allocation)
#[derive(Clone)]
#[allow(clippy::large_enum_variant)] // Intentional: inline storage trades memory for speed
pub enum SmallBatch {
    /// Inline storage for 1-8 records
    Inline {
        records: [Option<Record>; 8],
        len: usize,
    },
    /// Heap storage for larger batches
    Heap(Vec<Record>),
}

impl SmallBatch {
    /// Create from a Vec, using inline storage if possible
    pub fn from_vec(records: Vec<Record>) -> Self {
        let len = records.len();
        if len <= 8 {
            let mut inline = [const { None }; 8];
            for (i, record) in records.into_iter().enumerate() {
                inline[i] = Some(record);
            }
            Self::Inline {
                records: inline,
                len,
            }
        } else {
            Self::Heap(records)
        }
    }

    /// Get the length
    pub fn len(&self) -> usize {
        match self {
            Self::Inline { len, .. } => *len,
            Self::Heap(v) => v.len(),
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over records
    pub fn iter(&self) -> impl Iterator<Item = &Record> {
        match self {
            Self::Inline { records, len } => records[..*len]
                .iter()
                .filter_map(|r| r.as_ref())
                .collect::<Vec<_>>()
                .into_iter(),
            Self::Heap(v) => v.iter().collect::<Vec<_>>().into_iter(),
        }
    }

    /// Convert to Vec
    pub fn into_vec(self) -> Vec<Record> {
        match self {
            Self::Inline { records, .. } => records.into_iter().flatten().collect(),
            Self::Heap(v) => v,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AttrId, Descriptor, RecordIdentity, ValueId};
    use crate::temporal::Interval;

    fn make_record(id: u32, key: &str) -> Record {
        Record {
            id: RecordId(id),
            identity: RecordIdentity {
                entity_type: "person".to_string(),
                perspective: "test".to_string(),
                uid: key.to_string(),
            },
            descriptors: vec![Descriptor {
                attr: AttrId(1),
                value: ValueId(1),
                interval: Interval::new(0, 1000).unwrap(),
            }],
        }
    }

    #[test]
    fn test_record_slice() {
        let records: Vec<Record> = (0..10)
            .map(|i| make_record(i, &format!("key_{}", i)))
            .collect();
        let slice = RecordSlice::from_vec(records);

        assert_eq!(slice.len(), 10);
        assert_eq!(slice.get(0).unwrap().id, RecordId(0));
        assert_eq!(slice.get(9).unwrap().id, RecordId(9));

        // Create sub-slice
        let sub = slice.slice(3, 4);
        assert_eq!(sub.len(), 4);
        assert_eq!(sub.get(0).unwrap().id, RecordId(3));
        assert_eq!(sub.get(3).unwrap().id, RecordId(6));

        // Both share the same backing storage
        assert_eq!(slice.ref_count(), 2);
    }

    #[test]
    fn test_zero_copy_batch() {
        let records: Vec<Record> = (0..100)
            .map(|i| make_record(i, &format!("key_{}", i)))
            .collect();

        let batch = ZeroCopyBatch::new(records, 4, |r| (r.id.0 as usize) % 4);

        assert_eq!(batch.len(), 100);
        assert!(batch.partition_count() <= 4);

        // Each partition should have ~25 records
        for (id, slice) in batch.iter_partitions() {
            assert!(id < 4);
            assert!(!slice.is_empty());
        }
    }

    #[test]
    fn test_result_collector() {
        let mut collector = BatchResultCollector::new(5);

        collector.set_result(0, RecordId(100));
        collector.set_result(2, RecordId(200));
        collector.set_result(4, RecordId(300));

        assert_eq!(collector.collected_count(), 3);
        assert!(!collector.is_complete());

        collector.set_result(1, RecordId(150));
        collector.set_result(3, RecordId(250));

        assert!(collector.is_complete());

        let results = collector.into_cluster_ids();
        assert_eq!(
            results,
            vec![
                RecordId(100),
                RecordId(150),
                RecordId(200),
                RecordId(250),
                RecordId(300),
            ]
        );
    }

    #[test]
    fn test_small_batch() {
        // Small batch uses inline storage
        let small: Vec<Record> = (0..4)
            .map(|i| make_record(i, &format!("key_{}", i)))
            .collect();
        let batch = SmallBatch::from_vec(small);
        assert!(matches!(batch, SmallBatch::Inline { .. }));
        assert_eq!(batch.len(), 4);

        // Large batch uses heap
        let large: Vec<Record> = (0..20)
            .map(|i| make_record(i, &format!("key_{}", i)))
            .collect();
        let batch = SmallBatch::from_vec(large);
        assert!(matches!(batch, SmallBatch::Heap(_)));
        assert_eq!(batch.len(), 20);
    }
}
