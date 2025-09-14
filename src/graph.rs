//! # Knowledge Graph Module
//! 
//! Provides export functionality for the knowledge graph, including SAME_AS edges
//! and CONFLICTS_WITH edges with temporal provenance.

use crate::model::{RecordId, ClusterId, AttrId, ValueId};
use crate::temporal::Interval;
use crate::store::Store;
use crate::dsu::Clusters;
use crate::conflicts::Observation;
use crate::ontology::Ontology;
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
        lines.push(format!("{{\"type\": \"metadata\", \"data\": {}}}", metadata_line));
        
        // Add nodes
        for node in &self.nodes {
            let node_line = serde_json::to_string(&node)?;
            lines.push(format!("{{\"type\": \"node\", \"data\": {}}}", node_line));
        }
        
        // Add SAME_AS edges
        for edge in &self.same_as_edges {
            let edge_line = serde_json::to_string(&edge)?;
            lines.push(format!("{{\"type\": \"same_as\", \"data\": {}}}", edge_line));
        }
        
        // Add CONFLICTS_WITH edges
        for edge in &self.conflicts_with_edges {
            let edge_line = serde_json::to_string(&edge)?;
            lines.push(format!("{{\"type\": \"conflicts_with\", \"data\": {}}}", edge_line));
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
        store: &Store,
        clusters: &Clusters,
        observations: &[Observation],
        _ontology: &Ontology,
    ) -> Result<KnowledgeGraph> {
        let mut graph = KnowledgeGraph::new();
        
        // Add metadata
        graph.add_metadata("version".to_string(), "1.0".to_string());
        graph.add_metadata("timestamp".to_string(), chrono::Utc::now().to_rfc3339());
        graph.add_metadata("num_records".to_string(), store.len().to_string());
        graph.add_metadata("num_clusters".to_string(), clusters.len().to_string());
        
        // Add record nodes
        for record in store.get_all_records() {
            let mut node = GraphNode::record(record.id);
            node = node.with_property("entity_type".to_string(), record.identity.entity_type.clone());
            node = node.with_property("perspective".to_string(), record.identity.perspective.clone());
            node = node.with_property("uid".to_string(), record.identity.uid.clone());
            graph.add_node(node);
        }
        
        // Add cluster nodes
        for cluster in &clusters.clusters {
            let mut node = GraphNode::cluster(cluster.id);
            node = node.with_property("size".to_string(), cluster.len().to_string());
            node = node.with_property("root_record".to_string(), cluster.root.to_string());
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
                Observation::Merge { records, cluster: _cluster, interval, reason } => {
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
    store: &Store,
    clusters: &Clusters,
    observations: &[Observation],
    ontology: &Ontology,
) -> Result<KnowledgeGraph> {
    GraphExporter::export_graph(store, clusters, observations, ontology)
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
