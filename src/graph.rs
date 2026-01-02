//! # Knowledge Graph Module
//!
//! Provides export functionality for the knowledge graph, including SAME_AS edges
//! and CONFLICTS_WITH edges with temporal provenance.

use crate::conflicts::Observation;
use crate::dsu::Clusters;
use crate::model::{AttrId, ClusterId, RecordId, ValueId};
use crate::ontology::Ontology;
use crate::store::RecordStore;
use crate::temporal::{coalesce_same_value, difference, Interval};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A SAME_AS edge in the knowledge graph
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SameAsEdge {
    /// The source record
    pub source: RecordId,
    /// The target record
    pub target: RecordId,
    /// The time interval when this relationship is valid
    pub interval: Interval,
    /// The reason for the relationship (e.g., "identity_key_match", "crosswalk_match")
    pub reason: String,
    /// Additional metadata about the relationship
    pub metadata: HashMap<String, String>,
}

impl SameAsEdge {
    /// Create a new SAME_AS edge
    pub fn new(source: RecordId, target: RecordId, interval: Interval, reason: String) -> Self {
        Self {
            source,
            target,
            interval,
            reason,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the edge
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// A CONFLICTS_WITH edge in the knowledge graph
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictsWithEdge {
    /// The source record
    pub source: RecordId,
    /// The target record
    pub target: RecordId,
    /// The attribute that has the conflict
    pub attribute: AttrId,
    /// The time interval when the conflict occurs
    pub interval: Interval,
    /// The kind of conflict (e.g., "direct", "indirect")
    pub kind: String,
    /// The cause of the conflict
    pub cause: String,
    /// The conflicting values
    pub conflicting_values: Vec<ValueId>,
    /// Additional metadata about the conflict
    pub metadata: HashMap<String, String>,
}

impl ConflictsWithEdge {
    /// Create a new CONFLICTS_WITH edge
    pub fn new(
        source: RecordId,
        target: RecordId,
        attribute: AttrId,
        interval: Interval,
        kind: String,
        cause: String,
        conflicting_values: Vec<ValueId>,
    ) -> Self {
        Self {
            source,
            target,
            attribute,
            interval,
            kind,
            cause,
            conflicting_values,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the edge
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// A node in the knowledge graph
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNode {
    /// The ID of the node
    pub id: String,
    /// The type of the node (e.g., "record", "cluster", "conflict")
    pub node_type: String,
    /// The original record ID (if this is a record node)
    pub record_id: Option<RecordId>,
    /// The cluster ID (if this is a cluster node)
    pub cluster_id: Option<ClusterId>,
    /// Additional properties of the node
    pub properties: HashMap<String, String>,
}

impl GraphNode {
    /// Create a new graph node
    pub fn new(id: String, node_type: String) -> Self {
        Self {
            id,
            node_type,
            record_id: None,
            cluster_id: None,
            properties: HashMap::new(),
        }
    }

    /// Create a record node
    pub fn record(record_id: RecordId) -> Self {
        Self {
            id: format!("record_{}", record_id.0),
            node_type: "record".to_string(),
            record_id: Some(record_id),
            cluster_id: None,
            properties: HashMap::new(),
        }
    }

    /// Create a cluster node
    pub fn cluster(cluster_id: ClusterId) -> Self {
        Self {
            id: format!("cluster_{}", cluster_id.0),
            node_type: "cluster".to_string(),
            record_id: None,
            cluster_id: Some(cluster_id),
            properties: HashMap::new(),
        }
    }

    /// Add a property to the node
    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.properties.insert(key, value);
        self
    }
}

/// The complete knowledge graph
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeGraph {
    /// All nodes in the graph
    pub nodes: Vec<GraphNode>,
    /// All SAME_AS edges
    pub same_as_edges: Vec<SameAsEdge>,
    /// All CONFLICTS_WITH edges
    pub conflicts_with_edges: Vec<ConflictsWithEdge>,
    /// Metadata about the graph
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoldenDescriptor {
    pub attr: String,
    pub value: String,
    pub interval: Interval,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterKey {
    pub value: String,
    pub identity_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SameAsKey {
    source: RecordId,
    target: RecordId,
    interval: Interval,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConflictsWithKey {
    source: RecordId,
    target: RecordId,
    attribute: AttrId,
    interval: Interval,
    kind: String,
    cause: String,
    conflicting_values: Vec<ValueId>,
}

#[derive(Debug, Clone)]
struct GraphSnapshot {
    nodes: HashMap<String, GraphNode>,
    same_as_edges: HashMap<SameAsKey, SameAsEdge>,
    conflicts_with_edges: HashMap<ConflictsWithKey, ConflictsWithEdge>,
    metadata: HashMap<String, String>,
}

/// Incremental knowledge graph that supports updates from streaming snapshots.
#[derive(Debug, Clone)]
pub struct IncrementalKnowledgeGraph {
    nodes: HashMap<String, GraphNode>,
    same_as_edges: HashMap<SameAsKey, SameAsEdge>,
    conflicts_with_edges: HashMap<ConflictsWithKey, ConflictsWithEdge>,
    metadata: HashMap<String, String>,
}

impl IncrementalKnowledgeGraph {
    /// Create an empty incremental graph.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            same_as_edges: HashMap::new(),
            conflicts_with_edges: HashMap::new(),
            metadata: HashMap::new(),
        }
    }

    /// Update the incremental graph from the current snapshot.
    pub fn update(
        &mut self,
        store: &dyn RecordStore,
        clusters: &Clusters,
        observations: &[Observation],
        ontology: &Ontology,
    ) -> Result<()> {
        let snapshot = build_graph_snapshot(store, clusters, observations, ontology)?;
        self.apply_snapshot(snapshot);
        Ok(())
    }

    /// Convert the incremental graph to a KnowledgeGraph.
    pub fn to_knowledge_graph(&self) -> KnowledgeGraph {
        let mut graph = KnowledgeGraph::new();
        graph.metadata = self.metadata.clone();
        graph.nodes = self.nodes.values().cloned().collect();
        graph.same_as_edges = self.same_as_edges.values().cloned().collect();
        graph.conflicts_with_edges = self.conflicts_with_edges.values().cloned().collect();
        graph
    }

    fn apply_snapshot(&mut self, snapshot: GraphSnapshot) {
        self.nodes.retain(|key, _| snapshot.nodes.contains_key(key));
        for (key, node) in snapshot.nodes {
            self.nodes.insert(key, node);
        }

        self.same_as_edges
            .retain(|key, _| snapshot.same_as_edges.contains_key(key));
        for (key, edge) in snapshot.same_as_edges {
            self.same_as_edges.insert(key, edge);
        }

        self.conflicts_with_edges
            .retain(|key, _| snapshot.conflicts_with_edges.contains_key(key));
        for (key, edge) in snapshot.conflicts_with_edges {
            self.conflicts_with_edges.insert(key, edge);
        }

        self.metadata = snapshot.metadata;
    }
}

impl Default for IncrementalKnowledgeGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl KnowledgeGraph {
    /// Create a new knowledge graph
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            same_as_edges: Vec::new(),
            conflicts_with_edges: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Add a node to the graph
    pub fn add_node(&mut self, node: GraphNode) {
        self.nodes.push(node);
    }

    /// Add a SAME_AS edge to the graph
    pub fn add_same_as_edge(&mut self, edge: SameAsEdge) {
        self.same_as_edges.push(edge);
    }

    /// Add a CONFLICTS_WITH edge to the graph
    pub fn add_conflicts_with_edge(&mut self, edge: ConflictsWithEdge) {
        self.conflicts_with_edges.push(edge);
    }

    /// Add metadata to the graph
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }

    /// Get the number of nodes
    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    /// Get the number of SAME_AS edges
    pub fn num_same_as_edges(&self) -> usize {
        self.same_as_edges.len()
    }

    /// Get the number of CONFLICTS_WITH edges
    pub fn num_conflicts_with_edges(&self) -> usize {
        self.conflicts_with_edges.len()
    }

    /// Export the graph as JSONL
    pub fn to_jsonl(&self) -> Result<String> {
        let mut lines = Vec::new();

        // Add metadata line
        let metadata_line = serde_json::to_string(&self.metadata)?;
        lines.push(format!(
            "{{\"type\": \"metadata\", \"data\": {}}}",
            metadata_line
        ));

        // Add nodes
        for node in &self.nodes {
            let node_line = serde_json::to_string(&node)?;
            lines.push(format!("{{\"type\": \"node\", \"data\": {}}}", node_line));
        }

        // Add SAME_AS edges
        for edge in &self.same_as_edges {
            let edge_line = serde_json::to_string(&edge)?;
            lines.push(format!(
                "{{\"type\": \"same_as\", \"data\": {}}}",
                edge_line
            ));
        }

        // Add CONFLICTS_WITH edges
        for edge in &self.conflicts_with_edges {
            let edge_line = serde_json::to_string(&edge)?;
            lines.push(format!(
                "{{\"type\": \"conflicts_with\", \"data\": {}}}",
                edge_line
            ));
        }

        Ok(lines.join("\n"))
    }

    /// Export the graph as JSON
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

impl Default for KnowledgeGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Main graph exporter
pub struct GraphExporter;

impl GraphExporter {
    /// Export the knowledge graph from the given data
    pub fn export_graph(
        store: &dyn RecordStore,
        clusters: &Clusters,
        observations: &[Observation],
        ontology: &Ontology,
    ) -> Result<KnowledgeGraph> {
        let mut graph = KnowledgeGraph::new();

        // Add metadata
        graph.add_metadata("version".to_string(), "1.0".to_string());
        graph.add_metadata("timestamp".to_string(), chrono::Utc::now().to_rfc3339());
        graph.add_metadata("num_records".to_string(), store.len().to_string());
        graph.add_metadata("num_clusters".to_string(), clusters.len().to_string());

        // Add record nodes
        store.for_each_record(&mut |record| {
            let mut node = GraphNode::record(record.id);
            node = node.with_property(
                "entity_type".to_string(),
                record.identity.entity_type.clone(),
            );
            node = node.with_property(
                "perspective".to_string(),
                record.identity.perspective.clone(),
            );
            node = node.with_property("uid".to_string(), record.identity.uid.clone());
            graph.add_node(node);
        });

        let cluster_keys = cluster_keys_for_clusters(store, clusters, ontology);

        // Add cluster nodes
        for cluster in &clusters.clusters {
            let mut node = GraphNode::cluster(cluster.id);
            node = node.with_property("size".to_string(), cluster.len().to_string());
            node = node.with_property("root_record".to_string(), cluster.root.to_string());
            let golden = golden_for_cluster(store, cluster);
            let golden_json = serde_json::to_string(&golden)?;
            node = node.with_property("golden".to_string(), golden_json);
            if let Some(cluster_key) = cluster_keys.get(&cluster.id) {
                node = node.with_property("cluster_key".to_string(), cluster_key.value.clone());
                node = node.with_property(
                    "cluster_key_identity".to_string(),
                    cluster_key.identity_key.clone(),
                );
            }
            graph.add_node(node);
        }

        // Add SAME_AS edges from clusters
        for cluster in &clusters.clusters {
            if cluster.len() > 1 {
                // Create edges between all pairs in the cluster
                for i in 0..cluster.records.len() {
                    for j in i + 1..cluster.records.len() {
                        let edge = SameAsEdge::new(
                            cluster.records[i],
                            cluster.records[j],
                            Interval::all_time(), // Placeholder - would need actual temporal data
                            "cluster_member".to_string(),
                        );
                        graph.add_same_as_edge(edge);
                    }
                }
            }
        }

        // Add CONFLICTS_WITH edges from observations
        for observation in observations {
            match observation {
                Observation::DirectConflict(conflict) => {
                    // Create edges between conflicting records
                    for i in 0..conflict.values.len() {
                        for j in i + 1..conflict.values.len() {
                            let value_a = &conflict.values[i];
                            let value_b = &conflict.values[j];

                            for record_a in &value_a.participants {
                                for record_b in &value_b.participants {
                                    let edge = ConflictsWithEdge::new(
                                        *record_a,
                                        *record_b,
                                        conflict.attribute,
                                        conflict.interval,
                                        conflict.kind.clone(),
                                        "direct_conflict".to_string(),
                                        vec![value_a.value, value_b.value],
                                    );
                                    graph.add_conflicts_with_edge(edge);
                                }
                            }
                        }
                    }
                }
                Observation::IndirectConflict(conflict) => {
                    // Create edges for indirect conflicts
                    if let Some(records) = &conflict.participants.records {
                        if records.len() >= 2 {
                            let edge = ConflictsWithEdge::new(
                                records[0],
                                records[1],
                                conflict.attribute.unwrap_or(AttrId(0)),
                                conflict.interval,
                                conflict.kind.clone(),
                                conflict.cause.clone(),
                                vec![], // Placeholder - would need actual conflicting values
                            );
                            graph.add_conflicts_with_edge(edge);
                        }
                    }
                }
                Observation::Merge {
                    records,
                    cluster: _cluster,
                    interval,
                    reason,
                } => {
                    // Create SAME_AS edges for successful merges
                    if records.len() >= 2 {
                        for i in 0..records.len() {
                            for j in i + 1..records.len() {
                                let edge = SameAsEdge::new(
                                    records[i],
                                    records[j],
                                    *interval,
                                    reason.clone(),
                                );
                                graph.add_same_as_edge(edge);
                            }
                        }
                    }
                }
            }
        }

        Ok(graph)
    }
}

/// Public function to export graph
pub fn export_graph(
    store: &dyn RecordStore,
    clusters: &Clusters,
    observations: &[Observation],
    ontology: &Ontology,
) -> Result<KnowledgeGraph> {
    GraphExporter::export_graph(store, clusters, observations, ontology)
}

fn build_graph_snapshot(
    store: &dyn RecordStore,
    clusters: &Clusters,
    observations: &[Observation],
    ontology: &Ontology,
) -> Result<GraphSnapshot> {
    let mut nodes = HashMap::new();
    let mut same_as_edges = HashMap::new();
    let mut conflicts_with_edges = HashMap::new();
    let mut metadata = HashMap::new();

    metadata.insert("version".to_string(), "1.0".to_string());
    metadata.insert("timestamp".to_string(), chrono::Utc::now().to_rfc3339());
    metadata.insert("num_records".to_string(), store.len().to_string());
    metadata.insert("num_clusters".to_string(), clusters.len().to_string());

    store.for_each_record(&mut |record| {
        let mut node = GraphNode::record(record.id);
        node = node.with_property(
            "entity_type".to_string(),
            record.identity.entity_type.clone(),
        );
        node = node.with_property(
            "perspective".to_string(),
            record.identity.perspective.clone(),
        );
        node = node.with_property("uid".to_string(), record.identity.uid.clone());
        nodes.insert(node.id.clone(), node);
    });

    let cluster_keys = cluster_keys_for_clusters(store, clusters, ontology);

    for cluster in &clusters.clusters {
        let mut node = GraphNode::cluster(cluster.id);
        node = node.with_property("size".to_string(), cluster.len().to_string());
        node = node.with_property("root_record".to_string(), cluster.root.to_string());
        let golden = golden_for_cluster(store, cluster);
        let golden_json = serde_json::to_string(&golden)?;
        node = node.with_property("golden".to_string(), golden_json);
        if let Some(cluster_key) = cluster_keys.get(&cluster.id) {
            node = node.with_property("cluster_key".to_string(), cluster_key.value.clone());
            node = node.with_property(
                "cluster_key_identity".to_string(),
                cluster_key.identity_key.clone(),
            );
        }
        nodes.insert(node.id.clone(), node);
    }

    for cluster in &clusters.clusters {
        if cluster.len() > 1 {
            for i in 0..cluster.records.len() {
                for j in i + 1..cluster.records.len() {
                    let (source, target) = ordered_pair(cluster.records[i], cluster.records[j]);
                    let edge = SameAsEdge::new(
                        source,
                        target,
                        Interval::all_time(),
                        "cluster_member".to_string(),
                    );
                    let key = SameAsKey {
                        source,
                        target,
                        interval: edge.interval,
                        reason: edge.reason.clone(),
                    };
                    same_as_edges.insert(key, edge);
                }
            }
        }
    }

    for observation in observations {
        match observation {
            Observation::DirectConflict(conflict) => {
                for i in 0..conflict.values.len() {
                    for j in i + 1..conflict.values.len() {
                        let value_a = &conflict.values[i];
                        let value_b = &conflict.values[j];

                        for record_a in &value_a.participants {
                            for record_b in &value_b.participants {
                                let (source, target) = ordered_pair(*record_a, *record_b);
                                let mut conflicting_values = vec![value_a.value, value_b.value];
                                conflicting_values.sort_by_key(|value| value.0);
                                let edge = ConflictsWithEdge::new(
                                    source,
                                    target,
                                    conflict.attribute,
                                    conflict.interval,
                                    conflict.kind.clone(),
                                    "direct_conflict".to_string(),
                                    conflicting_values.clone(),
                                );
                                let key = ConflictsWithKey {
                                    source,
                                    target,
                                    attribute: conflict.attribute,
                                    interval: conflict.interval,
                                    kind: conflict.kind.clone(),
                                    cause: "direct_conflict".to_string(),
                                    conflicting_values,
                                };
                                conflicts_with_edges.insert(key, edge);
                            }
                        }
                    }
                }
            }
            Observation::IndirectConflict(conflict) => {
                if let Some(records) = &conflict.participants.records {
                    if records.len() >= 2 {
                        let (source, target) = ordered_pair(records[0], records[1]);
                        let edge = ConflictsWithEdge::new(
                            source,
                            target,
                            conflict.attribute.unwrap_or(AttrId(0)),
                            conflict.interval,
                            conflict.kind.clone(),
                            conflict.cause.clone(),
                            Vec::new(),
                        );
                        let key = ConflictsWithKey {
                            source,
                            target,
                            attribute: conflict.attribute.unwrap_or(AttrId(0)),
                            interval: conflict.interval,
                            kind: conflict.kind.clone(),
                            cause: conflict.cause.clone(),
                            conflicting_values: Vec::new(),
                        };
                        conflicts_with_edges.insert(key, edge);
                    }
                }
            }
            Observation::Merge {
                records,
                cluster: _cluster,
                interval,
                reason,
            } => {
                if records.len() >= 2 {
                    for i in 0..records.len() {
                        for j in i + 1..records.len() {
                            let (source, target) = ordered_pair(records[i], records[j]);
                            let edge = SameAsEdge::new(source, target, *interval, reason.clone());
                            let key = SameAsKey {
                                source,
                                target,
                                interval: edge.interval,
                                reason: edge.reason.clone(),
                            };
                            same_as_edges.insert(key, edge);
                        }
                    }
                }
            }
        }
    }

    Ok(GraphSnapshot {
        nodes,
        same_as_edges,
        conflicts_with_edges,
        metadata,
    })
}

fn ordered_pair(a: RecordId, b: RecordId) -> (RecordId, RecordId) {
    if a.0 <= b.0 {
        (a, b)
    } else {
        (b, a)
    }
}

fn coalesce_intervals(intervals: &[Interval]) -> Vec<Interval> {
    if intervals.is_empty() {
        return Vec::new();
    }
    coalesce_same_value(
        &intervals
            .iter()
            .map(|interval| (*interval, ()))
            .collect::<Vec<_>>(),
    )
    .into_iter()
    .map(|(interval, _)| interval)
    .collect()
}

fn subtract_intervals(base: &[Interval], others: &[Interval]) -> Vec<Interval> {
    if base.is_empty() {
        return Vec::new();
    }
    let mut remaining = base.to_vec();
    for other in others {
        let mut next = Vec::new();
        for segment in remaining {
            next.extend(difference(&segment, other));
        }
        remaining = next;
    }
    coalesce_intervals(&remaining)
}

pub fn golden_for_cluster(
    store: &dyn RecordStore,
    cluster: &crate::dsu::Cluster,
) -> Vec<GoldenDescriptor> {
    let mut attr_map: HashMap<AttrId, HashMap<ValueId, Vec<Interval>>> = HashMap::new();

    for record_id in &cluster.records {
        if let Some(record) = store.get_record(*record_id) {
            for descriptor in &record.descriptors {
                attr_map
                    .entry(descriptor.attr)
                    .or_default()
                    .entry(descriptor.value)
                    .or_default()
                    .push(descriptor.interval);
            }
        }
    }

    let mut golden = Vec::new();

    for (attr, values) in attr_map {
        let mut candidates: Vec<(ValueId, Vec<Interval>)> = values
            .into_iter()
            .map(|(value, intervals)| (value, coalesce_intervals(&intervals)))
            .collect();

        if candidates.is_empty() {
            continue;
        }

        if candidates.len() == 1 {
            let (value, intervals) = candidates.remove(0);
            let attr_name = store
                .interner()
                .get_attr(attr)
                .cloned()
                .unwrap_or_else(|| format!("attr_{}", attr.0));
            let value_str = store
                .interner()
                .get_value(value)
                .cloned()
                .unwrap_or_else(|| format!("value_{}", value.0));

            for interval in intervals {
                golden.push(GoldenDescriptor {
                    attr: attr_name.clone(),
                    value: value_str.clone(),
                    interval,
                });
            }
            continue;
        }

        for i in 0..candidates.len() {
            let (value, intervals) = &candidates[i];
            let others: Vec<Interval> = candidates
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx != i)
                .flat_map(|(_, (_, other_intervals))| other_intervals.clone())
                .collect();
            let safe = subtract_intervals(intervals, &others);
            if safe.is_empty() {
                continue;
            }
            let attr_name = store
                .interner()
                .get_attr(attr)
                .cloned()
                .unwrap_or_else(|| format!("attr_{}", attr.0));
            let value_str = store
                .interner()
                .get_value(*value)
                .cloned()
                .unwrap_or_else(|| format!("value_{}", value.0));
            for interval in safe {
                golden.push(GoldenDescriptor {
                    attr: attr_name.clone(),
                    value: value_str.clone(),
                    interval,
                });
            }
        }
    }

    golden.sort_by(|a, b| {
        a.attr
            .cmp(&b.attr)
            .then_with(|| a.value.cmp(&b.value))
            .then_with(|| a.interval.start.cmp(&b.interval.start))
    });

    golden
}

pub fn cluster_keys_for_clusters(
    store: &dyn RecordStore,
    clusters: &Clusters,
    ontology: &Ontology,
) -> HashMap<ClusterId, ClusterKey> {
    let mut seeds_by_type: HashMap<String, Vec<ClusterKeySeed>> = HashMap::new();

    for cluster in &clusters.clusters {
        let entity_type = match dominant_entity_type(store, cluster) {
            Some(entity_type) => entity_type,
            None => continue,
        };
        let (key_attrs, identity_key_name) = key_attributes_for_type(ontology, &entity_type);
        let seed = build_cluster_seed(store, cluster, &entity_type, &key_attrs, &identity_key_name);
        if let Some(seed) = seed {
            seeds_by_type.entry(entity_type).or_default().push(seed);
        }
    }

    let mut resolved = HashMap::new();
    for seeds in seeds_by_type.values() {
        resolved.extend(resolve_group_keys(seeds));
    }

    resolved
}

fn dominant_entity_type(store: &dyn RecordStore, cluster: &crate::dsu::Cluster) -> Option<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for record_id in &cluster.records {
        if let Some(record) = store.get_record(*record_id) {
            *counts
                .entry(record.identity.entity_type.clone())
                .or_insert(0) += 1;
        }
    }
    counts
        .into_iter()
        .max_by(|(name_a, count_a), (name_b, count_b)| {
            count_a.cmp(count_b).then_with(|| name_b.cmp(name_a))
        })
        .map(|(name, _)| name)
}

fn pick_cluster_value(
    store: &dyn RecordStore,
    cluster: &crate::dsu::Cluster,
    attr: AttrId,
) -> Option<String> {
    let mut counts: HashMap<ValueId, usize> = HashMap::new();
    for record_id in &cluster.records {
        if let Some(record) = store.get_record(*record_id) {
            for descriptor in &record.descriptors {
                if descriptor.attr == attr {
                    *counts.entry(descriptor.value).or_insert(0) += 1;
                }
            }
        }
    }

    if counts.is_empty() {
        return None;
    }

    let interner = store.interner();
    let mut choices: Vec<(String, usize)> = counts
        .into_iter()
        .map(|(value, count)| {
            let value_str = interner
                .get_value(value)
                .cloned()
                .unwrap_or_else(|| format!("value_{}", value.0));
            (value_str, count)
        })
        .collect();

    choices.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Some(choices[0].0.clone())
}

fn short_hash(input: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    encode_base32(hash)
}

fn short_token_from_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let normalized = trimmed.to_lowercase();
    let core = if let Some((local, _)) = normalized.split_once('@') {
        local
    } else {
        &normalized
    };

    let alnum: String = core.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    if !alnum.is_empty() && alnum.chars().all(|c| c.is_ascii_digit()) {
        if alnum.len() > 3 {
            return alnum[alnum.len() - 3..].to_uppercase();
        }
        return alnum.to_uppercase();
    }

    let parts: Vec<&str> = core
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .collect();

    let mut token = String::new();
    if parts.len() >= 2 {
        for part in parts.iter().take(4) {
            if let Some(ch) = part.chars().next() {
                token.push(ch);
            }
        }
    } else if let Some(part) = parts.first() {
        token = part.chars().take(4).collect();
    }

    if token.is_empty() {
        token = alnum.chars().take(4).collect();
    }

    token.to_uppercase()
}

#[derive(Debug, Clone)]
struct ClusterKeySeed {
    cluster_id: ClusterId,
    identity_key: String,
    tokens: Vec<String>,
    material: String,
}

fn build_cluster_seed(
    store: &dyn RecordStore,
    cluster: &crate::dsu::Cluster,
    entity_type: &str,
    key_attrs: &[AttrId],
    identity_key_name: &str,
) -> Option<ClusterKeySeed> {
    let interner = store.interner();
    let mut tokens = Vec::new();
    let mut parts = Vec::new();
    let signature = cluster_signature(store, cluster);

    if key_attrs.is_empty() {
        let (_attr, value, attr_name) = pick_cluster_attr_value(store, cluster)?;
        let token = token_from_parts(&attr_name, &value, entity_type);
        tokens.push(token);
        parts.push(format!("{}={}", attr_name, value));
        let material = format!("{entity_type}|fallback|{}|{}", parts.join("|"), signature);
        return Some(ClusterKeySeed {
            cluster_id: cluster.id,
            identity_key: format!("fallback_{}", attr_name),
            tokens,
            material,
        });
    }

    for attr in key_attrs {
        let attr_name = interner
            .get_attr(*attr)
            .cloned()
            .unwrap_or_else(|| format!("attr_{}", attr.0));
        let value =
            pick_cluster_value(store, cluster, *attr).unwrap_or_else(|| "unknown".to_string());
        let token = token_from_parts(&attr_name, &value, entity_type);
        tokens.push(token);
        parts.push(format!("{}={}", attr_name, value));
    }

    if tokens.iter().all(|token| token.is_empty()) {
        return None;
    }

    let material = format!(
        "{entity_type}|{identity_key_name}|{}|{}",
        parts.join("|"),
        signature
    );
    Some(ClusterKeySeed {
        cluster_id: cluster.id,
        identity_key: identity_key_name.to_string(),
        tokens,
        material,
    })
}

fn pick_cluster_attr_value(
    store: &dyn RecordStore,
    cluster: &crate::dsu::Cluster,
) -> Option<(AttrId, String, String)> {
    let mut attr_counts: HashMap<AttrId, usize> = HashMap::new();
    for record_id in &cluster.records {
        if let Some(record) = store.get_record(*record_id) {
            for descriptor in &record.descriptors {
                *attr_counts.entry(descriptor.attr).or_insert(0) += 1;
            }
        }
    }

    if attr_counts.is_empty() {
        return None;
    }

    let interner = store.interner();
    let mut attrs: Vec<(AttrId, usize, String)> = attr_counts
        .into_iter()
        .map(|(attr, count)| {
            let name = interner
                .get_attr(attr)
                .cloned()
                .unwrap_or_else(|| format!("attr_{}", attr.0));
            (attr, count, name)
        })
        .collect();

    attrs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.2.cmp(&b.2)));
    let (attr, _count, attr_name) = attrs[0].clone();
    let value = pick_cluster_value(store, cluster, attr)?;
    Some((attr, value, attr_name))
}

fn cluster_signature(store: &dyn RecordStore, cluster: &crate::dsu::Cluster) -> String {
    let interner = store.interner();
    let mut parts = Vec::new();
    for record_id in &cluster.records {
        if let Some(record) = store.get_record(*record_id) {
            parts.push(format!(
                "record:{}:{}:{}",
                record.identity.entity_type, record.identity.perspective, record.identity.uid
            ));
            for descriptor in &record.descriptors {
                let attr = interner
                    .get_attr(descriptor.attr)
                    .cloned()
                    .unwrap_or_else(|| format!("attr_{}", descriptor.attr.0));
                let value = interner
                    .get_value(descriptor.value)
                    .cloned()
                    .unwrap_or_else(|| format!("value_{}", descriptor.value.0));
                parts.push(format!(
                    "{}={}@{}:{}",
                    attr, value, descriptor.interval.start, descriptor.interval.end
                ));
            }
        }
    }
    parts.sort();
    parts.join("|")
}

fn encode_base32(value: u64) -> String {
    const ALPHABET: &[u8; 32] = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZ";
    let mut chars = Vec::new();
    let mut remaining = value;
    for _ in 0..8 {
        let idx = (remaining & 31) as usize;
        chars.push(ALPHABET[idx] as char);
        remaining >>= 5;
    }
    chars.reverse();
    chars.into_iter().collect()
}

fn hash_suffix(input: &str) -> String {
    let encoded = short_hash(input);
    encoded
        .chars()
        .rev()
        .take(2)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

fn resolve_group_keys(seeds: &[ClusterKeySeed]) -> HashMap<ClusterId, ClusterKey> {
    if seeds.is_empty() {
        return HashMap::new();
    }

    let max_len = seeds
        .iter()
        .map(|seed| seed.tokens.len())
        .max()
        .unwrap_or(1);
    let normalized: Vec<ClusterKeySeed> = seeds
        .iter()
        .cloned()
        .map(|mut seed| {
            while seed.tokens.len() < max_len {
                seed.tokens.push("UNK".to_string());
            }
            seed
        })
        .collect();

    let mut assigned: HashMap<ClusterId, usize> = HashMap::new();
    for len in 1..=max_len {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for seed in &normalized {
            let key = seed.tokens[..len].join("-");
            *counts.entry(key).or_insert(0) += 1;
        }
        for seed in &normalized {
            if assigned.contains_key(&seed.cluster_id) {
                continue;
            }
            let key = seed.tokens[..len].join("-");
            if counts.get(&key).copied().unwrap_or(0) == 1 {
                assigned.insert(seed.cluster_id, len);
            }
        }
    }

    let mut resolved = HashMap::new();
    for seed in normalized {
        let len = assigned.get(&seed.cluster_id).copied().unwrap_or(max_len);
        let mut base = seed.tokens[..len].join("-");
        if base.is_empty() {
            base = "UNK".to_string();
        }
        let suffix = hash_suffix(&seed.material);
        resolved.insert(
            seed.cluster_id,
            ClusterKey {
                value: format!("{base}-{suffix}"),
                identity_key: seed.identity_key.clone(),
            },
        );
    }

    resolved
}

fn token_from_parts(attr_name: &str, value: &str, fallback: &str) -> String {
    let value_token = short_token_from_value(value);
    let attr_token = short_token_from_value(attr_name);
    let fallback_token = short_token_from_value(fallback);

    let mut token = if value_token.is_empty() {
        attr_token
    } else {
        value_token
    };

    if token.is_empty() {
        token = fallback_token;
    }

    if token.len() > 12 {
        token = token.chars().take(12).collect();
    }

    token
}

fn key_attributes_for_type(ontology: &Ontology, entity_type: &str) -> (Vec<AttrId>, String) {
    if let Some(entity) = ontology.entity_types.get(entity_type) {
        if !entity.key_attributes.is_empty() {
            return (entity.key_attributes.clone(), "entity_key".to_string());
        }
    }

    let mut identity_keys = ontology.identity_keys_for_type(entity_type);
    identity_keys.sort_by(|a, b| b.attributes.len().cmp(&a.attributes.len()));
    if let Some(identity_key) = identity_keys.first() {
        return (identity_key.attributes.clone(), identity_key.name.clone());
    }

    (Vec::new(), "fallback".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::linker::build_clusters;
    use crate::model::{Descriptor, Record, RecordIdentity};
    use crate::ontology::{IdentityKey, Ontology};
    use crate::store::Store;
    use crate::temporal::Interval;

    #[test]
    fn test_same_as_edge_creation() {
        let edge = SameAsEdge::new(
            RecordId(1),
            RecordId(2),
            Interval::new(100, 200).unwrap(),
            "identity_key_match".to_string(),
        );

        assert_eq!(edge.source, RecordId(1));
        assert_eq!(edge.target, RecordId(2));
        assert_eq!(edge.reason, "identity_key_match");
    }

    #[test]
    fn test_conflicts_with_edge_creation() {
        let edge = ConflictsWithEdge::new(
            RecordId(1),
            RecordId(2),
            AttrId(1),
            Interval::new(100, 200).unwrap(),
            "direct".to_string(),
            "value_mismatch".to_string(),
            vec![ValueId(1), ValueId(2)],
        );

        assert_eq!(edge.source, RecordId(1));
        assert_eq!(edge.target, RecordId(2));
        assert_eq!(edge.attribute, AttrId(1));
        assert_eq!(edge.kind, "direct");
    }

    #[test]
    fn test_graph_node_creation() {
        let node = GraphNode::record(RecordId(1));
        assert_eq!(node.id, "record_1");
        assert_eq!(node.node_type, "record");
        assert_eq!(node.record_id, Some(RecordId(1)));
    }

    #[test]
    fn test_knowledge_graph_creation() {
        let mut graph = KnowledgeGraph::new();

        let node = GraphNode::record(RecordId(1));
        graph.add_node(node);

        let edge = SameAsEdge::new(
            RecordId(1),
            RecordId(2),
            Interval::new(100, 200).unwrap(),
            "identity_key_match".to_string(),
        );
        graph.add_same_as_edge(edge);

        assert_eq!(graph.num_nodes(), 1);
        assert_eq!(graph.num_same_as_edges(), 1);
    }

    #[test]
    fn test_jsonl_export() {
        let graph = KnowledgeGraph::new();
        let jsonl = graph.to_jsonl().unwrap();
        assert!(jsonl.contains("metadata"));
    }

    #[test]
    fn test_golden_copy_trims_conflicts() {
        let mut store = Store::new();
        let mut ontology = Ontology::new();

        let name_attr = store.interner_mut().intern_attr("name");
        let email_attr = store.interner_mut().intern_attr("email");
        let name_value = store.interner_mut().intern_value("Alice");
        let email_a = store.interner_mut().intern_value("alice@corp.example");
        let email_b = store.interner_mut().intern_value("alice@next.example");

        ontology.add_identity_key(IdentityKey::new(vec![name_attr], "name".to_string()));

        let record1 = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "hr".to_string(), "1".to_string()),
            vec![
                Descriptor::new(name_attr, name_value, Interval::new(0, 30).unwrap()),
                Descriptor::new(email_attr, email_a, Interval::new(0, 20).unwrap()),
            ],
        );
        let record2 = Record::new(
            RecordId(2),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "2".to_string()),
            vec![
                Descriptor::new(name_attr, name_value, Interval::new(10, 40).unwrap()),
                Descriptor::new(email_attr, email_b, Interval::new(10, 40).unwrap()),
            ],
        );

        store.add_records(vec![record1, record2]).unwrap();
        let clusters = build_clusters(&store, &ontology).unwrap();
        let cluster = clusters.clusters.first().expect("cluster missing");

        let golden = golden_for_cluster(&store, cluster);
        let email_entries: Vec<_> = golden.iter().filter(|desc| desc.attr == "email").collect();

        assert!(
            email_entries
                .iter()
                .any(|desc| desc.value == "alice@corp.example"
                    && desc.interval == Interval::new(0, 10).unwrap()),
            "golden should keep non-overlapping slice of first email"
        );
        assert!(
            email_entries
                .iter()
                .any(|desc| desc.value == "alice@next.example"
                    && desc.interval == Interval::new(20, 40).unwrap()),
            "golden should keep non-overlapping slice of second email"
        );
    }

    #[test]
    fn test_cluster_key_uses_identity_key_values() {
        let mut store = Store::new();
        let mut ontology = Ontology::new();

        let email_attr = store.interner_mut().intern_attr("email");
        let email_value = store.interner_mut().intern_value("alice@example.com");

        ontology.add_identity_key(IdentityKey::new(vec![email_attr], "email".to_string()));

        let record = Record::new(
            RecordId(1),
            RecordIdentity::new("person".to_string(), "crm".to_string(), "1".to_string()),
            vec![Descriptor::new(
                email_attr,
                email_value,
                Interval::new(0, 10).unwrap(),
            )],
        );

        store.add_record(record).unwrap();
        let clusters = build_clusters(&store, &ontology).unwrap();
        let cluster = clusters.clusters.first().expect("cluster missing");

        let keys = cluster_keys_for_clusters(&store, &clusters, &ontology);
        let key = keys.get(&cluster.id).expect("cluster key missing");

        assert_eq!(key.identity_key, "email");
        assert!(
            key.value.starts_with("ALIC-") && key.value.len() == 7,
            "cluster key should use minimal token plus 2-char hash"
        );
    }
}
