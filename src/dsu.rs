//! # Disjoint Set Union (DSU) with Temporal Guards
//!
//! Implements Union-Find data structure with temporal validation to ensure
//! that merges only occur when temporal constraints are satisfied.

use crate::model::{ClusterId, RecordId};
use crate::temporal::Interval;
// use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;

/// A temporal guard that validates whether two records can be merged
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalGuard {
    /// The time interval when this guard is valid
    pub interval: Interval,
    /// The reason for the guard (e.g., "identity_key_match", "crosswalk_match")
    pub reason: String,
    /// Additional metadata about the guard
    pub metadata: HashMap<String, String>,
}

impl TemporalGuard {
    /// Create a new temporal guard
    pub fn new(interval: Interval, reason: String) -> Self {
        Self {
            interval,
            reason,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the guard
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// A temporal conflict that prevents merging
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalConflict {
    /// The time interval when the conflict occurs
    pub interval: Interval,
    /// The reason for the conflict (e.g., "strong_identifier_mismatch")
    pub reason: String,
    /// The conflicting values
    pub conflicting_values: Vec<String>,
    /// Additional metadata about the conflict
    pub metadata: HashMap<String, String>,
}

impl TemporalConflict {
    /// Create a new temporal conflict
    pub fn new(interval: Interval, reason: String, conflicting_values: Vec<String>) -> Self {
        Self {
            interval,
            reason,
            conflicting_values,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the conflict
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// Result of attempting to merge two records
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeResult {
    /// The merge was successful
    Success {
        /// The guard that allowed the merge
        guard: TemporalGuard,
    },
    /// The merge was blocked by a temporal conflict
    Blocked {
        /// The conflict that blocked the merge
        conflict: TemporalConflict,
    },
}

/// Disjoint Set Union with temporal guards
#[derive(Debug, Clone)]
pub struct TemporalDSU {
    /// Parent array for Union-Find
    parent: HashMap<RecordId, RecordId>,
    /// Rank array for path compression
    rank: HashMap<RecordId, u32>,
    /// Guards for each record
    guards: HashMap<RecordId, Vec<TemporalGuard>>,
    /// Conflicts for each record
    conflicts: HashMap<RecordId, Vec<TemporalConflict>>,
    /// Next available cluster ID
    next_cluster_id: u32,
}

impl TemporalDSU {
    /// Create a new temporal DSU
    pub fn new() -> Self {
        Self {
            parent: HashMap::new(),
            rank: HashMap::new(),
            guards: HashMap::new(),
            conflicts: HashMap::new(),
            next_cluster_id: 0,
        }
    }

    /// Add a record to the DSU
    pub fn add_record(&mut self, record_id: RecordId) {
        self.parent.insert(record_id, record_id);
        self.rank.insert(record_id, 0);
        self.guards.insert(record_id, Vec::new());
        self.conflicts.insert(record_id, Vec::new());
    }

    /// Find the root of a record (with path compression)
    pub fn find(&mut self, record_id: RecordId) -> RecordId {
        if self.parent[&record_id] != record_id {
            let root = self.find(self.parent[&record_id]);
            self.parent.insert(record_id, root);
        }
        self.parent[&record_id]
    }

    /// Check if two records are in the same cluster
    pub fn same_cluster(&mut self, a: RecordId, b: RecordId) -> bool {
        self.find(a) == self.find(b)
    }

    /// Attempt to merge two records with temporal validation
    pub fn try_merge(&mut self, a: RecordId, b: RecordId, guard: TemporalGuard) -> MergeResult {
        // Ensure both records exist in the DSU
        if !self.parent.contains_key(&a) {
            self.add_record(a);
        }
        if !self.parent.contains_key(&b) {
            self.add_record(b);
        }

        let root_a = self.find(a);
        let root_b = self.find(b);

        if root_a == root_b {
            return MergeResult::Success { guard };
        }

        // Check for temporal conflicts
        if let Some(conflict) = self.check_temporal_conflict(a, b, &guard) {
            return MergeResult::Blocked { conflict };
        }

        // Perform the union
        self.union(root_a, root_b);

        // Add the guard to both records
        self.guards.get_mut(&a).unwrap().push(guard.clone());
        self.guards.get_mut(&b).unwrap().push(guard);

        MergeResult::Success {
            guard: self.guards[&a].last().unwrap().clone(),
        }
    }

    /// Check for temporal conflicts between two records
    fn check_temporal_conflict(
        &self,
        a: RecordId,
        b: RecordId,
        _guard: &TemporalGuard,
    ) -> Option<TemporalConflict> {
        // Get all guards for both records
        let empty_vec = Vec::new();
        let guards_a = self.guards.get(&a).unwrap_or(&empty_vec);
        let guards_b = self.guards.get(&b).unwrap_or(&empty_vec);

        // Check for overlapping guards with conflicting reasons
        for guard_a in guards_a {
            for guard_b in guards_b {
                if crate::temporal::is_overlapping(&guard_a.interval, &guard_b.interval) {
                    if guard_a.reason != guard_b.reason {
                        return Some(TemporalConflict::new(
                            crate::temporal::intersect(&guard_a.interval, &guard_b.interval)
                                .unwrap(),
                            "conflicting_guards".to_string(),
                            vec![guard_a.reason.clone(), guard_b.reason.clone()],
                        ));
                    }
                }
            }
        }

        None
    }

    /// Union two clusters (internal method)
    fn union(&mut self, a: RecordId, b: RecordId) {
        let rank_a = self.rank[&a];
        let rank_b = self.rank[&b];

        if rank_a < rank_b {
            self.parent.insert(a, b);
        } else if rank_a > rank_b {
            self.parent.insert(b, a);
        } else {
            self.parent.insert(a, b);
            self.rank.insert(b, rank_b + 1);
        }
    }

    /// Get all clusters
    pub fn get_clusters(&mut self) -> Clusters {
        let mut cluster_map: HashMap<RecordId, Vec<RecordId>> = HashMap::new();

        let record_ids: Vec<RecordId> = self.parent.keys().cloned().collect();
        for record_id in record_ids {
            let root = self.find(record_id);
            cluster_map
                .entry(root)
                .or_insert_with(Vec::new)
                .push(record_id);
        }

        let mut clusters = Vec::new();
        for (root, records) in cluster_map {
            let cluster_id = ClusterId(self.next_cluster_id);
            self.next_cluster_id += 1;

            clusters.push(Cluster {
                id: cluster_id,
                root,
                records,
            });
        }

        Clusters { clusters }
    }

    /// Get the cluster ID for a record
    pub fn get_cluster_id(&mut self, record_id: RecordId) -> Option<ClusterId> {
        let root = self.find(record_id);
        // Find the cluster that contains this root
        for cluster in self.get_clusters().clusters {
            if cluster.root == root {
                return Some(cluster.id);
            }
        }
        None
    }

    /// Get guards for a record
    pub fn get_guards(&self, record_id: RecordId) -> &[TemporalGuard] {
        self.guards
            .get(&record_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get conflicts for a record
    pub fn get_conflicts(&self, record_id: RecordId) -> &[TemporalConflict] {
        self.conflicts
            .get(&record_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Add a conflict to a record
    pub fn add_conflict(&mut self, record_id: RecordId, conflict: TemporalConflict) {
        self.conflicts
            .entry(record_id)
            .or_insert_with(Vec::new)
            .push(conflict);
    }

    /// Get the number of clusters
    pub fn num_clusters(&mut self) -> usize {
        let mut roots: HashSet<RecordId> = HashSet::new();
        let record_ids: Vec<RecordId> = self.parent.keys().cloned().collect();
        for record_id in record_ids {
            roots.insert(self.find(record_id));
        }
        roots.len()
    }
}

impl Default for TemporalDSU {
    fn default() -> Self {
        Self::new()
    }
}

/// A cluster of records
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cluster {
    /// Unique identifier for this cluster
    pub id: ClusterId,
    /// The root record of this cluster
    pub root: RecordId,
    /// All records in this cluster
    pub records: Vec<RecordId>,
}

impl Cluster {
    /// Create a new cluster
    pub fn new(id: ClusterId, root: RecordId, records: Vec<RecordId>) -> Self {
        Self { id, root, records }
    }

    /// Get the number of records in this cluster
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if this cluster is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Check if this cluster contains a specific record
    pub fn contains(&self, record_id: RecordId) -> bool {
        self.records.contains(&record_id)
    }
}

/// Collection of all clusters
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Clusters {
    /// All clusters
    pub clusters: Vec<Cluster>,
}

impl Clusters {
    /// Create a new clusters collection
    pub fn new() -> Self {
        Self {
            clusters: Vec::new(),
        }
    }

    /// Add a cluster
    pub fn add_cluster(&mut self, cluster: Cluster) {
        self.clusters.push(cluster);
    }

    /// Get a cluster by ID
    pub fn get_cluster(&self, id: ClusterId) -> Option<&Cluster> {
        self.clusters.iter().find(|c| c.id == id)
    }

    /// Get all clusters
    pub fn get_all_clusters(&self) -> &[Cluster] {
        &self.clusters
    }

    /// Get the number of clusters
    pub fn len(&self) -> usize {
        self.clusters.len()
    }

    /// Check if there are no clusters
    pub fn is_empty(&self) -> bool {
        self.clusters.is_empty()
    }

    /// Get clusters containing a specific record
    pub fn get_clusters_for_record(&self, record_id: RecordId) -> Vec<&Cluster> {
        self.clusters
            .iter()
            .filter(|cluster| cluster.contains(record_id))
            .collect()
    }
}

impl Default for Clusters {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporal::Interval;

    #[test]
    fn test_temporal_dsu_creation() {
        let mut dsu = TemporalDSU::new();
        assert_eq!(dsu.num_clusters(), 0);
    }

    #[test]
    fn test_add_record() {
        let mut dsu = TemporalDSU::new();
        let record_id = RecordId(1);

        dsu.add_record(record_id);
        assert_eq!(dsu.find(record_id), record_id);
    }

    #[test]
    fn test_merge_records() {
        let mut dsu = TemporalDSU::new();
        let record_a = RecordId(1);
        let record_b = RecordId(2);

        dsu.add_record(record_a);
        dsu.add_record(record_b);

        let guard = TemporalGuard::new(
            Interval::new(100, 200).unwrap(),
            "identity_key_match".to_string(),
        );

        let result = dsu.try_merge(record_a, record_b, guard);
        assert!(matches!(result, MergeResult::Success { .. }));
        assert!(dsu.same_cluster(record_a, record_b));
    }

    #[test]
    fn test_cluster_creation() {
        let mut dsu = TemporalDSU::new();
        let record_a = RecordId(1);
        let record_b = RecordId(2);

        dsu.add_record(record_a);
        dsu.add_record(record_b);

        let guard = TemporalGuard::new(
            Interval::new(100, 200).unwrap(),
            "identity_key_match".to_string(),
        );

        dsu.try_merge(record_a, record_b, guard);

        let clusters = dsu.get_clusters();
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters.clusters[0].records.len(), 2);
    }

    #[test]
    fn test_temporal_conflict() {
        let mut dsu = TemporalDSU::new();
        let record_a = RecordId(1);
        let record_b = RecordId(2);

        dsu.add_record(record_a);
        dsu.add_record(record_b);

        // Add conflicting guards
        let guard1 = TemporalGuard::new(
            Interval::new(100, 200).unwrap(),
            "identity_key_match".to_string(),
        );
        let guard2 = TemporalGuard::new(
            Interval::new(150, 250).unwrap(),
            "strong_identifier_mismatch".to_string(),
        );

        dsu.guards.get_mut(&record_a).unwrap().push(guard1);
        dsu.guards.get_mut(&record_b).unwrap().push(guard2);

        let guard = TemporalGuard::new(
            Interval::new(100, 200).unwrap(),
            "identity_key_match".to_string(),
        );

        let result = dsu.try_merge(record_a, record_b, guard);
        assert!(matches!(result, MergeResult::Blocked { .. }));
    }

    #[test]
    fn test_cluster_operations() {
        let mut clusters = Clusters::new();

        let cluster = Cluster::new(ClusterId(1), RecordId(1), vec![RecordId(1), RecordId(2)]);

        clusters.add_cluster(cluster);
        assert_eq!(clusters.len(), 1);

        let found_cluster = clusters.get_cluster(ClusterId(1));
        assert!(found_cluster.is_some());
        assert_eq!(found_cluster.unwrap().len(), 2);
    }

    #[test]
    fn test_union_find_functionality() {
        let mut dsu = TemporalDSU::new();

        // Add entity 1
        dsu.add_record(RecordId(1));
        assert_eq!(dsu.find(RecordId(1)), RecordId(1));
        assert_eq!(dsu.num_clusters(), 1);
        assert_eq!(dsu.rank[&RecordId(1)], 0);

        // Add entity 101 and union 1 with 101
        dsu.add_record(RecordId(101));
        let guard1 = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
        let result = dsu.try_merge(RecordId(1), RecordId(101), guard1);
        assert!(matches!(result, MergeResult::Success { .. }));

        let root1 = dsu.find(RecordId(1));
        let root101 = dsu.find(RecordId(101));
        assert_eq!(root1, root101);
        assert_eq!(dsu.num_clusters(), 1);
        assert_eq!(dsu.rank[&root1], 1);

        // Add entity 2
        dsu.add_record(RecordId(2));
        assert_eq!(dsu.find(RecordId(2)), RecordId(2));
        assert_eq!(dsu.num_clusters(), 2);
        assert_eq!(dsu.rank[&RecordId(2)], 0);

        // Union 2 with 101 (which is now in cluster 1)
        let guard2 = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
        let result = dsu.try_merge(RecordId(2), RecordId(101), guard2);
        assert!(matches!(result, MergeResult::Success { .. }));

        let root = dsu.find(RecordId(1));
        assert_eq!(dsu.find(RecordId(101)), root);
        assert_eq!(dsu.find(RecordId(2)), root);
        assert_eq!(dsu.num_clusters(), 1);
        assert_eq!(dsu.rank[&root], 1);

        // Add entity 102 and union 2 with 102
        dsu.add_record(RecordId(102));
        let guard3 = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
        let result = dsu.try_merge(RecordId(2), RecordId(102), guard3);
        assert!(matches!(result, MergeResult::Success { .. }));

        let root = dsu.find(RecordId(1));
        assert_eq!(dsu.find(RecordId(102)), root);
        assert_eq!(dsu.find(RecordId(2)), root);
        assert_eq!(dsu.num_clusters(), 1);
        assert_eq!(dsu.rank[&root], 1);

        // Add entity 3 and create a chain
        dsu.add_record(RecordId(3));
        assert_eq!(dsu.num_clusters(), 2);

        // Add entities and union 3 with multiple entities
        let entities_to_union = vec![103, 113, 123, 133, 143];
        for entity_id in entities_to_union {
            dsu.add_record(RecordId(entity_id));
            let guard =
                TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
            let result = dsu.try_merge(RecordId(3), RecordId(entity_id), guard);
            assert!(matches!(result, MergeResult::Success { .. }));
        }

        // Verify all entities in cluster 3
        let root3 = dsu.find(RecordId(3));
        assert_eq!(dsu.find(RecordId(103)), root3);
        assert_eq!(dsu.find(RecordId(113)), root3);
        assert_eq!(dsu.find(RecordId(123)), root3);
        assert_eq!(dsu.find(RecordId(133)), root3);
        assert_eq!(dsu.find(RecordId(143)), root3);
        // The rank should be 1 because we did multiple unions with equal ranks
        assert_eq!(dsu.rank[&root3], 1);

        // Union cluster 1 with cluster 3
        let guard4 = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
        let result = dsu.try_merge(RecordId(1), RecordId(103), guard4);
        assert!(matches!(result, MergeResult::Success { .. }));

        let final_root = dsu.find(RecordId(1));
        assert_eq!(dsu.num_clusters(), 1);
        // The rank should be 2 because we're merging two rank-1 clusters
        assert_eq!(dsu.rank[&final_root], 2);

        // Add entity 4 and create another cluster
        dsu.add_record(RecordId(4));
        assert_eq!(dsu.num_clusters(), 2);

        // Add entities and union 4 with multiple entities
        let entities_to_union = vec![104, 114, 124];
        for entity_id in entities_to_union {
            dsu.add_record(RecordId(entity_id));
            let guard =
                TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());
            let result = dsu.try_merge(RecordId(4), RecordId(entity_id), guard);
            assert!(matches!(result, MergeResult::Success { .. }));
        }

        // Final verification
        assert_eq!(dsu.num_clusters(), 2);
    }

    #[test]
    fn test_union_find_path_compression() {
        let mut dsu = TemporalDSU::new();

        // Create a chain: 1 -> 2 -> 3 -> 4
        dsu.add_record(RecordId(1));
        dsu.add_record(RecordId(2));
        dsu.add_record(RecordId(3));
        dsu.add_record(RecordId(4));

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Union in sequence
        dsu.try_merge(RecordId(1), RecordId(2), guard.clone());
        dsu.try_merge(RecordId(2), RecordId(3), guard.clone());
        dsu.try_merge(RecordId(3), RecordId(4), guard.clone());

        // All should point to the same root
        let root = dsu.find(RecordId(1));
        assert_eq!(dsu.find(RecordId(2)), root);
        assert_eq!(dsu.find(RecordId(3)), root);
        assert_eq!(dsu.find(RecordId(4)), root);

        // After path compression, all should directly point to root
        assert_eq!(dsu.parent[&RecordId(1)], root);
        assert_eq!(dsu.parent[&RecordId(2)], root);
        assert_eq!(dsu.parent[&RecordId(3)], root);
        assert_eq!(dsu.parent[&RecordId(4)], root);
    }

    #[test]
    fn test_union_find_rank_optimization() {
        let mut dsu = TemporalDSU::new();

        // Create two trees with different ranks
        dsu.add_record(RecordId(1));
        dsu.add_record(RecordId(2));
        dsu.add_record(RecordId(3));
        dsu.add_record(RecordId(4));

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Create tree 1: 1 -> 2
        dsu.try_merge(RecordId(1), RecordId(2), guard.clone());
        let root1 = dsu.find(RecordId(1));
        assert_eq!(dsu.rank[&root1], 1);

        // Create tree 2: 3 -> 4
        dsu.try_merge(RecordId(3), RecordId(4), guard.clone());
        let root3 = dsu.find(RecordId(3));
        assert_eq!(dsu.rank[&root3], 1);

        // Union the two trees - should attach smaller rank to larger rank
        dsu.try_merge(RecordId(1), RecordId(3), guard.clone());

        // Both should point to the same root
        let root1 = dsu.find(RecordId(1));
        let root3 = dsu.find(RecordId(3));
        assert_eq!(root1, root3);

        // The root should have rank 2 (incremented since ranks were equal)
        let root_rank = dsu.rank[&root1];
        assert_eq!(root_rank, 2);
    }

    #[test]
    fn test_union_find_same_cluster() {
        let mut dsu = TemporalDSU::new();

        dsu.add_record(RecordId(1));
        dsu.add_record(RecordId(2));
        dsu.add_record(RecordId(3));

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Initially, all records are in different clusters
        assert!(!dsu.same_cluster(RecordId(1), RecordId(2)));
        assert!(!dsu.same_cluster(RecordId(1), RecordId(3)));
        assert!(!dsu.same_cluster(RecordId(2), RecordId(3)));

        // Union 1 and 2
        dsu.try_merge(RecordId(1), RecordId(2), guard.clone());
        assert!(dsu.same_cluster(RecordId(1), RecordId(2)));
        assert!(!dsu.same_cluster(RecordId(1), RecordId(3)));
        assert!(!dsu.same_cluster(RecordId(2), RecordId(3)));

        // Union 2 and 3
        dsu.try_merge(RecordId(2), RecordId(3), guard.clone());
        assert!(dsu.same_cluster(RecordId(1), RecordId(2)));
        assert!(dsu.same_cluster(RecordId(1), RecordId(3)));
        assert!(dsu.same_cluster(RecordId(2), RecordId(3)));
    }

    #[test]
    fn test_union_find_cluster_count() {
        let mut dsu = TemporalDSU::new();

        // Start with 5 separate records
        for i in 1..=5 {
            dsu.add_record(RecordId(i));
        }
        assert_eq!(dsu.num_clusters(), 5);

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Union 1 and 2
        dsu.try_merge(RecordId(1), RecordId(2), guard.clone());
        assert_eq!(dsu.num_clusters(), 4);

        // Union 3 and 4
        dsu.try_merge(RecordId(3), RecordId(4), guard.clone());
        assert_eq!(dsu.num_clusters(), 3);

        // Union 1 and 3 (merges two clusters)
        dsu.try_merge(RecordId(1), RecordId(3), guard.clone());
        assert_eq!(dsu.num_clusters(), 2);

        // Union 5 with the big cluster
        dsu.try_merge(RecordId(1), RecordId(5), guard.clone());
        assert_eq!(dsu.num_clusters(), 1);
    }

    #[test]
    fn test_union_find_get_clusters() {
        let mut dsu = TemporalDSU::new();

        dsu.add_record(RecordId(1));
        dsu.add_record(RecordId(2));
        dsu.add_record(RecordId(3));
        dsu.add_record(RecordId(4));

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Create two clusters: {1, 2} and {3, 4}
        dsu.try_merge(RecordId(1), RecordId(2), guard.clone());
        dsu.try_merge(RecordId(3), RecordId(4), guard.clone());

        let clusters = dsu.get_clusters();
        assert_eq!(clusters.len(), 2);

        // Each cluster should have 2 records
        for cluster in &clusters.clusters {
            assert_eq!(cluster.records.len(), 2);
        }

        // Verify all records are accounted for
        let mut all_records: Vec<RecordId> = clusters
            .clusters
            .iter()
            .flat_map(|c| c.records.iter())
            .cloned()
            .collect();
        all_records.sort();

        let expected = vec![RecordId(1), RecordId(2), RecordId(3), RecordId(4)];
        assert_eq!(all_records, expected);
    }

    #[test]
    fn test_union_find_self_union() {
        let mut dsu = TemporalDSU::new();

        dsu.add_record(RecordId(1));

        let guard = TemporalGuard::new(Interval::new(0, 1000).unwrap(), "test_union".to_string());

        // Union a record with itself should be a no-op
        let result = dsu.try_merge(RecordId(1), RecordId(1), guard);
        assert!(matches!(result, MergeResult::Success { .. }));

        assert_eq!(dsu.find(RecordId(1)), RecordId(1));
        assert_eq!(dsu.num_clusters(), 1);
    }

    #[test]
    fn test_union_find_guards_tracking() {
        let mut dsu = TemporalDSU::new();

        dsu.add_record(RecordId(1));
        dsu.add_record(RecordId(2));

        let guard1 = TemporalGuard::new(Interval::new(0, 100).unwrap(), "guard1".to_string());
        let guard2 = TemporalGuard::new(Interval::new(100, 200).unwrap(), "guard2".to_string());

        // Union with first guard
        dsu.try_merge(RecordId(1), RecordId(2), guard1.clone());

        // Add another guard to record 1
        dsu.guards
            .get_mut(&RecordId(1))
            .unwrap()
            .push(guard2.clone());

        // Check guards are tracked
        let guards_1 = dsu.get_guards(RecordId(1));
        let guards_2 = dsu.get_guards(RecordId(2));

        assert_eq!(guards_1.len(), 2);
        assert_eq!(guards_2.len(), 1);

        assert_eq!(guards_1[0].reason, "guard1");
        assert_eq!(guards_1[1].reason, "guard2");
        assert_eq!(guards_2[0].reason, "guard1");
    }
}
