use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use minitao::model::{AssocType, MinitaoAssociation, MinitaoObject, ObjectId, ObjectType};
use minitao::storage::StorageEngine;
use serde::Serialize;

use crate::graph::{ConflictsWithEdge, GraphNode, KnowledgeGraph, SameAsEdge};
use crate::model::{ClusterId, RecordId};

pub const OBJECT_TYPE_RECORD: ObjectType = 1;
pub const OBJECT_TYPE_CLUSTER: ObjectType = 2;
pub const OBJECT_TYPE_CONFLICT_EDGE: ObjectType = 3;
pub const ASSOC_TYPE_SAME_AS: AssocType = 1;
pub const ASSOC_TYPE_CONFLICT_EDGE: AssocType = 2;

const RECORD_PREFIX: u64 = 1 << 60;
const CLUSTER_PREFIX: u64 = 2 << 60;
const CONFLICT_PREFIX: u64 = 3 << 60;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AssocKey {
    id1: ObjectId,
    atype: AssocType,
    id2: ObjectId,
}

#[derive(Default)]
struct WriterState {
    objects: HashSet<ObjectId>,
    assocs: HashSet<AssocKey>,
}

pub struct MinitaoGraphWriter {
    storage: Arc<dyn StorageEngine>,
    state: Mutex<WriterState>,
}

impl MinitaoGraphWriter {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            state: Mutex::new(WriterState::default()),
        }
    }

    pub async fn apply_graph(&self, graph: &KnowledgeGraph) -> anyhow::Result<()> {
        let payloads = build_minitao_payloads(graph)?;
        let objects = payloads.objects;
        let assocs = payloads.assocs;

        let new_object_ids: HashSet<ObjectId> = objects.keys().copied().collect();
        let new_assoc_keys: HashSet<AssocKey> = assocs
            .keys()
            .map(|(id1, atype, id2)| AssocKey {
                id1: *id1,
                atype: *atype,
                id2: *id2,
            })
            .collect();

        let (to_delete_objects, to_delete_assocs) = {
            let state = self.state.lock().expect("minitao writer mutex poisoned");
            let to_delete_objects = state
                .objects
                .difference(&new_object_ids)
                .copied()
                .collect::<Vec<_>>();
            let to_delete_assocs = state
                .assocs
                .difference(&new_assoc_keys)
                .cloned()
                .collect::<Vec<_>>();
            (to_delete_objects, to_delete_assocs)
        };

        for key in to_delete_assocs {
            self.storage
                .delete_assoc(key.id1, key.atype, key.id2)
                .await
                .context("delete assoc")?;
        }

        for id in to_delete_objects {
            self.storage
                .delete_object(id)
                .await
                .context("delete object")?;
        }

        for obj in objects.values() {
            self.storage
                .put_object(obj.clone())
                .await
                .context("put object")?;
        }

        for assoc in assocs.values() {
            self.storage
                .put_assoc(assoc.clone())
                .await
                .context("put assoc")?;
        }

        let mut state = self.state.lock().expect("minitao writer mutex poisoned");
        state.objects = new_object_ids;
        state.assocs = new_assoc_keys;

        Ok(())
    }
}

pub fn record_object_id(record_id: RecordId) -> ObjectId {
    RECORD_PREFIX | record_id.0 as u64
}

pub fn cluster_object_id(cluster_id: ClusterId) -> ObjectId {
    CLUSTER_PREFIX | cluster_id.0 as u64
}

pub struct MinitaoPayloads {
    pub objects: HashMap<ObjectId, MinitaoObject>,
    pub assocs: HashMap<(ObjectId, AssocType, ObjectId), MinitaoAssociation>,
}

pub fn build_minitao_payloads(graph: &KnowledgeGraph) -> anyhow::Result<MinitaoPayloads> {
    let objects = build_objects(graph)?;
    let assocs = build_assocs(graph)?
        .into_iter()
        .map(|(key, assoc)| ((key.id1, key.atype, key.id2), assoc))
        .collect();
    Ok(MinitaoPayloads { objects, assocs })
}

fn build_objects(graph: &KnowledgeGraph) -> anyhow::Result<HashMap<ObjectId, MinitaoObject>> {
    let mut objects = HashMap::new();
    for node in &graph.nodes {
        let obj = node_to_object(node)?;
        objects.insert(obj.id, obj);
    }
    for obj in build_conflict_objects(&graph.conflicts_with_edges)? {
        objects.insert(obj.id, obj);
    }
    Ok(objects)
}

fn node_to_object(node: &GraphNode) -> anyhow::Result<MinitaoObject> {
    let mut data = HashMap::new();
    data.insert("node_id".to_string(), node.id.clone());
    data.insert("node_type".to_string(), node.node_type.clone());

    for (key, value) in &node.properties {
        data.insert(format!("prop.{key}"), value.clone());
    }

    match node.node_type.as_str() {
        "record" => {
            let record_id = node
                .record_id
                .context("record node missing record_id")?;
            data.insert("record_id".to_string(), record_id.0.to_string());
            Ok(MinitaoObject {
                id: record_object_id(record_id),
                otype: OBJECT_TYPE_RECORD,
                data,
            })
        }
        "cluster" => {
            let cluster_id = node
                .cluster_id
                .context("cluster node missing cluster_id")?;
            data.insert("cluster_id".to_string(), cluster_id.0.to_string());
            Ok(MinitaoObject {
                id: cluster_object_id(cluster_id),
                otype: OBJECT_TYPE_CLUSTER,
                data,
            })
        }
        other => anyhow::bail!("unsupported node type: {other}"),
    }
}

fn build_assocs(graph: &KnowledgeGraph) -> anyhow::Result<HashMap<AssocKey, MinitaoAssociation>> {
    let mut assocs = HashMap::new();

    let grouped_same_as = group_same_as_edges(&graph.same_as_edges)?;
    for (key, assoc) in grouped_same_as {
        assocs.insert(key, assoc);
    }

    let conflict_assocs = build_conflict_assocs(&graph.conflicts_with_edges)?;
    for (key, assoc) in conflict_assocs {
        assocs.insert(key, assoc);
    }

    Ok(assocs)
}

fn group_same_as_edges(
    edges: &[SameAsEdge],
) -> anyhow::Result<HashMap<AssocKey, MinitaoAssociation>> {
    // Minitao associations are keyed by (id1, atype, id2), so we aggregate
    // multiple SAME_AS edges between a record pair into a single payload.
    let mut grouped: HashMap<(RecordId, RecordId), Vec<SameAsPayload>> = HashMap::new();

    for edge in edges {
        let (left, right) = ordered_record_pair(edge.source, edge.target);
        grouped
            .entry((left, right))
            .or_default()
            .push(SameAsPayload {
                source: edge.source.0,
                target: edge.target.0,
                interval_start: edge.interval.start,
                interval_end: edge.interval.end,
                reason: edge.reason.clone(),
                metadata: edge.metadata.clone(),
            });
    }

    let mut assocs = HashMap::new();
    for ((left, right), payloads) in grouped {
        let id1 = record_object_id(left);
        let id2 = record_object_id(right);
        let time = earliest_interval_start(&payloads.iter().map(|p| p.interval_start).collect::<Vec<_>>());
        let edges_json = serde_json::to_string(&payloads)?;
        let mut data = HashMap::new();
        data.insert("edge_type".to_string(), "same_as".to_string());
        data.insert("edge_count".to_string(), payloads.len().to_string());
        data.insert("edges_json".to_string(), edges_json);

        let key = AssocKey {
            id1,
            atype: ASSOC_TYPE_SAME_AS,
            id2,
        };
        assocs.insert(
            key,
            MinitaoAssociation {
                id1,
                atype: ASSOC_TYPE_SAME_AS,
                id2,
                time,
                data,
            },
        );
    }

    Ok(assocs)
}

fn build_conflict_objects(edges: &[ConflictsWithEdge]) -> anyhow::Result<Vec<MinitaoObject>> {
    let mut objects = Vec::with_capacity(edges.len());
    for edge in edges {
        let payload = ConflictEdgeObjectPayload::from(edge);
        let payload_json = serde_json::to_string(&payload)?;
        let metadata_json = serde_json::to_string(&edge.metadata)?;
        let mut data = HashMap::new();
        data.insert("edge_type".to_string(), "conflict".to_string());
        data.insert("source_record_id".to_string(), edge.source.0.to_string());
        data.insert("target_record_id".to_string(), edge.target.0.to_string());
        data.insert("attribute_id".to_string(), edge.attribute.0.to_string());
        data.insert("interval_start".to_string(), edge.interval.start.to_string());
        data.insert("interval_end".to_string(), edge.interval.end.to_string());
        data.insert("kind".to_string(), edge.kind.clone());
        data.insert("cause".to_string(), edge.cause.clone());
        data.insert(
            "conflicting_values_json".to_string(),
            serde_json::to_string(
                &edge
                    .conflicting_values
                    .iter()
                    .map(|value| value.0)
                    .collect::<Vec<_>>(),
            )?,
        );
        data.insert("metadata_json".to_string(), metadata_json);
        data.insert("payload_json".to_string(), payload_json);

        objects.push(MinitaoObject {
            id: conflict_object_id(edge),
            otype: OBJECT_TYPE_CONFLICT_EDGE,
            data,
        });
    }
    Ok(objects)
}

fn build_conflict_assocs(
    edges: &[ConflictsWithEdge],
) -> anyhow::Result<HashMap<AssocKey, MinitaoAssociation>> {
    let mut assocs = HashMap::new();
    for edge in edges {
        let conflict_id = conflict_object_id(edge);
        let time = interval_start_or_zero(&edge.interval);
        let source_id = record_object_id(edge.source);
        let target_id = record_object_id(edge.target);

        let source_assoc = MinitaoAssociation {
            id1: source_id,
            atype: ASSOC_TYPE_CONFLICT_EDGE,
            id2: conflict_id,
            time,
            data: conflict_assoc_data(edge, "source")?,
        };
        let target_assoc = MinitaoAssociation {
            id1: target_id,
            atype: ASSOC_TYPE_CONFLICT_EDGE,
            id2: conflict_id,
            time,
            data: conflict_assoc_data(edge, "target")?,
        };

        assocs.insert(
            AssocKey {
                id1: source_id,
                atype: ASSOC_TYPE_CONFLICT_EDGE,
                id2: conflict_id,
            },
            source_assoc,
        );
        assocs.insert(
            AssocKey {
                id1: target_id,
                atype: ASSOC_TYPE_CONFLICT_EDGE,
                id2: conflict_id,
            },
            target_assoc,
        );
    }
    Ok(assocs)
}

fn ordered_record_pair(a: RecordId, b: RecordId) -> (RecordId, RecordId) {
    if a.0 <= b.0 {
        (a, b)
    } else {
        (b, a)
    }
}

fn interval_start_or_zero(interval: &crate::temporal::Interval) -> u64 {
    if interval.start <= 0 {
        0
    } else {
        interval.start as u64
    }
}

fn earliest_interval_start(starts: &[i64]) -> u64 {
    let mut earliest = i64::MAX;
    for start in starts {
        if *start < earliest {
            earliest = *start;
        }
    }
    if earliest <= 0 {
        0
    } else {
        earliest as u64
    }
}

#[derive(Debug, Clone, Serialize)]
struct SameAsPayload {
    source: u32,
    target: u32,
    interval_start: i64,
    interval_end: i64,
    reason: String,
    metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
struct ConflictEdgeObjectPayload {
    source: u32,
    target: u32,
    attribute: u32,
    interval_start: i64,
    interval_end: i64,
    kind: String,
    cause: String,
    conflicting_values: Vec<u32>,
    metadata: HashMap<String, String>,
}

impl From<&ConflictsWithEdge> for ConflictEdgeObjectPayload {
    fn from(edge: &ConflictsWithEdge) -> Self {
        Self {
            source: edge.source.0,
            target: edge.target.0,
            attribute: edge.attribute.0,
            interval_start: edge.interval.start,
            interval_end: edge.interval.end,
            kind: edge.kind.clone(),
            cause: edge.cause.clone(),
            conflicting_values: edge
                .conflicting_values
                .iter()
                .map(|value| value.0)
                .collect(),
            metadata: edge.metadata.clone(),
        }
    }
}

fn conflict_object_id(edge: &ConflictsWithEdge) -> ObjectId {
    let mut parts = Vec::new();
    parts.extend_from_slice(&edge.source.0.to_le_bytes());
    parts.extend_from_slice(&edge.target.0.to_le_bytes());
    parts.extend_from_slice(&edge.attribute.0.to_le_bytes());
    parts.extend_from_slice(&edge.interval.start.to_le_bytes());
    parts.extend_from_slice(&edge.interval.end.to_le_bytes());
    parts.extend_from_slice(edge.kind.as_bytes());
    parts.push(0);
    parts.extend_from_slice(edge.cause.as_bytes());
    parts.push(0);
    for value in &edge.conflicting_values {
        parts.extend_from_slice(&value.0.to_le_bytes());
    }
    let mut metadata_keys: Vec<_> = edge.metadata.keys().collect();
    metadata_keys.sort();
    for key in metadata_keys {
        parts.extend_from_slice(key.as_bytes());
        parts.push(0);
        if let Some(value) = edge.metadata.get(key) {
            parts.extend_from_slice(value.as_bytes());
        }
        parts.push(0);
    }
    let hash = fnv1a_hash64(&parts);
    CONFLICT_PREFIX | (hash & ((1_u64 << 60) - 1))
}

fn conflict_assoc_data(
    edge: &ConflictsWithEdge,
    role: &str,
) -> anyhow::Result<HashMap<String, String>> {
    let mut data = HashMap::new();
    data.insert("edge_type".to_string(), "conflict".to_string());
    data.insert("role".to_string(), role.to_string());
    data.insert("source_record_id".to_string(), edge.source.0.to_string());
    data.insert("target_record_id".to_string(), edge.target.0.to_string());
    data.insert("attribute_id".to_string(), edge.attribute.0.to_string());
    data.insert("interval_start".to_string(), edge.interval.start.to_string());
    data.insert("interval_end".to_string(), edge.interval.end.to_string());
    data.insert("kind".to_string(), edge.kind.clone());
    data.insert("cause".to_string(), edge.cause.clone());
    data.insert(
        "conflicting_values_json".to_string(),
        serde_json::to_string(
            &edge
                .conflicting_values
                .iter()
                .map(|value| value.0)
                .collect::<Vec<_>>(),
        )?,
    );
    Ok(data)
}

fn fnv1a_hash64(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let mut hash = FNV_OFFSET;
    for byte in data {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}
