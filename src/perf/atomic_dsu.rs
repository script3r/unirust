//! # Lock-Free Atomic DSU (Disjoint Set Union)
//!
//! High-performance union-find implementation using only atomic operations.
//! No locks required - all operations use CAS (Compare-And-Swap).
//!
//! ## Performance Characteristics
//! - find(): O(α(n)) amortized with path compression
//! - union(): O(α(n)) amortized with union-by-rank
//! - No lock contention under high concurrency
//!
//! ## Safety
//! Uses atomic operations with appropriate memory ordering:
//! - Relaxed for reads during find (path compression is best-effort)
//! - AcqRel for union operations (ensures visibility)

use crate::model::RecordId;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};

/// Maximum number of records supported (configurable at compile time)
const MAX_RECORDS: usize = 16_777_216; // 16M records per partition

/// Lock-free DSU with atomic parent pointers
pub struct AtomicDSU {
    /// Parent pointers (record_id -> parent_id, or self if root)
    parent: Box<[AtomicU32]>,
    /// Rank for union-by-rank heuristic
    rank: Box<[AtomicU8]>,
    /// Number of active records
    count: AtomicU32,
    /// Number of distinct sets (clusters)
    set_count: AtomicU32,
}

/// Result of an atomic merge operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicMergeResult {
    /// Successfully merged two different sets
    Merged { new_root: RecordId },
    /// Records were already in the same set
    AlreadySame { root: RecordId },
    /// One or both records don't exist
    NotFound,
}

impl AtomicDSU {
    /// Create a new atomic DSU with pre-allocated capacity
    pub fn new() -> Self {
        Self::with_capacity(MAX_RECORDS)
    }

    /// Create with specific capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let mut parent = Vec::with_capacity(capacity);
        let mut rank = Vec::with_capacity(capacity);

        for i in 0..capacity {
            parent.push(AtomicU32::new(i as u32));
            rank.push(AtomicU8::new(0));
        }

        Self {
            parent: parent.into_boxed_slice(),
            rank: rank.into_boxed_slice(),
            count: AtomicU32::new(0),
            set_count: AtomicU32::new(0),
        }
    }

    /// Add a new record (must be called before find/union)
    #[inline]
    pub fn add_record(&self, id: RecordId) -> bool {
        let idx = id.0 as usize;
        if idx >= self.parent.len() {
            return false;
        }

        // Initialize as its own parent (singleton set)
        self.parent[idx].store(id.0, Ordering::Release);
        self.rank[idx].store(0, Ordering::Release);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.set_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Check if a record exists in the DSU
    #[inline]
    pub fn has_record(&self, id: RecordId) -> bool {
        let idx = id.0 as usize;
        idx < self.parent.len()
    }

    /// Find the root of a record with path compression
    ///
    /// Uses two-pass path compression:
    /// 1. First pass: find the root
    /// 2. Second pass: update all nodes on path to point to root
    #[inline]
    pub fn find(&self, id: RecordId) -> RecordId {
        let mut current = id.0;

        // First pass: find root
        loop {
            let parent = self.parent[current as usize].load(Ordering::Relaxed);
            if parent == current {
                break;
            }
            current = parent;
        }
        let root = current;

        // Second pass: path compression (best-effort, no CAS needed)
        current = id.0;
        while current != root {
            let parent = self.parent[current as usize].load(Ordering::Relaxed);
            if parent == root {
                break;
            }
            // Best-effort update - if another thread changed it, that's fine
            let _ = self.parent[current as usize].compare_exchange(
                parent,
                root,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            current = parent;
        }

        RecordId(root)
    }

    /// Find root without path compression (read-only)
    #[inline]
    pub fn find_readonly(&self, id: RecordId) -> RecordId {
        let mut current = id.0;
        loop {
            let parent = self.parent[current as usize].load(Ordering::Relaxed);
            if parent == current {
                return RecordId(current);
            }
            current = parent;
        }
    }

    /// Attempt to merge two sets atomically
    ///
    /// Uses CAS to ensure exactly one thread succeeds in merging.
    /// Union-by-rank ensures tree depth stays O(log n).
    #[inline]
    pub fn try_union(&self, a: RecordId, b: RecordId) -> AtomicMergeResult {
        loop {
            let root_a = self.find(a);
            let root_b = self.find(b);

            if root_a == root_b {
                return AtomicMergeResult::AlreadySame { root: root_a };
            }

            let rank_a = self.rank[root_a.0 as usize].load(Ordering::Relaxed);
            let rank_b = self.rank[root_b.0 as usize].load(Ordering::Relaxed);

            // Determine which root becomes the new parent (union-by-rank)
            let (child, parent, child_rank, parent_rank) = if rank_a < rank_b {
                (root_a, root_b, rank_a, rank_b)
            } else {
                (root_b, root_a, rank_b, rank_a)
            };

            // Try to update child's parent to point to new parent
            match self.parent[child.0 as usize].compare_exchange(
                child.0,
                parent.0,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Successfully merged - update rank if needed
                    if child_rank == parent_rank {
                        // Increment parent's rank
                        let _ = self.rank[parent.0 as usize].compare_exchange(
                            parent_rank,
                            parent_rank.saturating_add(1),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        );
                    }
                    self.set_count.fetch_sub(1, Ordering::Relaxed);
                    return AtomicMergeResult::Merged { new_root: parent };
                }
                Err(_) => {
                    // Another thread modified the tree - retry
                    continue;
                }
            }
        }
    }

    /// Get the number of distinct sets (clusters)
    #[inline]
    pub fn cluster_count(&self) -> usize {
        self.set_count.load(Ordering::Relaxed) as usize
    }

    /// Get the total number of records
    #[inline]
    pub fn record_count(&self) -> usize {
        self.count.load(Ordering::Relaxed) as usize
    }

    /// Collect all clusters with their member records
    pub fn get_clusters(&self) -> Vec<(RecordId, Vec<RecordId>)> {
        use rustc_hash::FxHashMap;

        let count = self.count.load(Ordering::Relaxed) as usize;
        let mut clusters: FxHashMap<u32, Vec<RecordId>> = FxHashMap::default();

        for i in 0..count {
            let id = RecordId(i as u32);
            let root = self.find_readonly(id);
            clusters.entry(root.0).or_default().push(id);
        }

        clusters
            .into_iter()
            .map(|(root, members)| (RecordId(root), members))
            .collect()
    }
}

impl Default for AtomicDSU {
    fn default() -> Self {
        Self::new()
    }
}

// Safety: AtomicDSU uses only atomic operations
unsafe impl Send for AtomicDSU {}
unsafe impl Sync for AtomicDSU {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_dsu_basic() {
        let dsu = AtomicDSU::with_capacity(100);

        // Add records
        assert!(dsu.add_record(RecordId(0)));
        assert!(dsu.add_record(RecordId(1)));
        assert!(dsu.add_record(RecordId(2)));

        // Initially all in separate sets
        assert_eq!(dsu.cluster_count(), 3);
        assert_eq!(dsu.find(RecordId(0)), RecordId(0));
        assert_eq!(dsu.find(RecordId(1)), RecordId(1));

        // Merge 0 and 1
        let result = dsu.try_union(RecordId(0), RecordId(1));
        assert!(matches!(result, AtomicMergeResult::Merged { .. }));
        assert_eq!(dsu.cluster_count(), 2);

        // Find should return same root
        let root0 = dsu.find(RecordId(0));
        let root1 = dsu.find(RecordId(1));
        assert_eq!(root0, root1);

        // Merge again should return AlreadySame
        let result = dsu.try_union(RecordId(0), RecordId(1));
        assert!(matches!(result, AtomicMergeResult::AlreadySame { .. }));
    }

    #[test]
    fn test_atomic_dsu_path_compression() {
        let dsu = AtomicDSU::with_capacity(100);

        // Create a chain: 0 -> 1 -> 2 -> 3
        for i in 0..4 {
            dsu.add_record(RecordId(i));
        }
        dsu.try_union(RecordId(0), RecordId(1));
        dsu.try_union(RecordId(1), RecordId(2));
        dsu.try_union(RecordId(2), RecordId(3));

        // After finds, path compression should flatten the tree
        let root = dsu.find(RecordId(0));
        assert_eq!(dsu.find(RecordId(1)), root);
        assert_eq!(dsu.find(RecordId(2)), root);
        assert_eq!(dsu.find(RecordId(3)), root);
    }

    #[test]
    fn test_concurrent_unions() {
        use std::sync::Arc;
        use std::thread;

        let dsu = Arc::new(AtomicDSU::with_capacity(1000));

        // Add all records
        for i in 0..100 {
            dsu.add_record(RecordId(i));
        }

        // Spawn threads to do concurrent unions
        let mut handles = vec![];
        for t in 0..4 {
            let dsu = Arc::clone(&dsu);
            handles.push(thread::spawn(move || {
                for i in 0..25 {
                    let a = RecordId((t * 25 + i) as u32);
                    let b = RecordId(((t * 25 + i + 1) % 100) as u32);
                    dsu.try_union(a, b);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All should be connected (single cluster)
        let root = dsu.find(RecordId(0));
        for i in 1..100 {
            assert_eq!(dsu.find(RecordId(i)), root);
        }
    }
}
