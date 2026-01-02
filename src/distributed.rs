use crate::conflicts::ConflictSummary;
use crate::graph::GoldenDescriptor;
use crate::model::{AttrId, Record, RecordId, RecordIdentity};
use crate::ontology::{Constraint, IdentityKey, Ontology, StrongIdentifier};
use crate::query::{QueryDescriptor, QueryOutcome};
use crate::temporal::Interval;
use crate::{PersistentStore, StreamingTuning, Unirust};
use anyhow::Result as AnyResult;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

#[derive(Debug, Deserialize)]
struct JsonRecordIdentity {
    entity_type: String,
    perspective: String,
    uid: String,
}

#[derive(Debug, Deserialize)]
struct JsonRecordDescriptor {
    attr: String,
    value: String,
    start: i64,
    end: i64,
}

#[derive(Debug, Deserialize)]
struct JsonRecordInput {
    index: u32,
    identity: JsonRecordIdentity,
    descriptors: Vec<JsonRecordDescriptor>,
}

pub mod proto {
    tonic::include_proto!("unirust");
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityKeyConfig {
    pub name: String,
    pub attributes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintKind {
    Unique,
    UniqueWithinPerspective,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintConfig {
    pub name: String,
    pub attribute: String,
    pub kind: ConstraintKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedOntologyConfig {
    pub identity_keys: Vec<IdentityKeyConfig>,
    pub strong_identifiers: Vec<String>,
    pub constraints: Vec<ConstraintConfig>,
}

impl DistributedOntologyConfig {
    pub fn empty() -> Self {
        Self {
            identity_keys: Vec::new(),
            strong_identifiers: Vec::new(),
            constraints: Vec::new(),
        }
    }

    pub fn build_ontology(&self, store: &mut crate::Store) -> Ontology {
        let mut ontology = Ontology::new();

        for key in &self.identity_keys {
            let attrs: Vec<AttrId> = key
                .attributes
                .iter()
                .map(|attr| store.interner_mut().intern_attr(attr))
                .collect();
            ontology.add_identity_key(IdentityKey::new(attrs, key.name.clone()));
        }

        for attr in &self.strong_identifiers {
            let attr_id = store.interner_mut().intern_attr(attr);
            ontology
                .add_strong_identifier(StrongIdentifier::new(attr_id, format!("{attr}_strong")));
        }

        for constraint in &self.constraints {
            let attr_id = store.interner_mut().intern_attr(&constraint.attribute);
            let constraint = match constraint.kind {
                ConstraintKind::Unique => Constraint::unique(attr_id, constraint.name.clone()),
                ConstraintKind::UniqueWithinPerspective => {
                    Constraint::unique_within_perspective(attr_id, constraint.name.clone())
                }
            };
            ontology.add_constraint(constraint);
        }

        ontology
    }
}

fn map_proto_config(config: &proto::OntologyConfig) -> DistributedOntologyConfig {
    DistributedOntologyConfig {
        identity_keys: config
            .identity_keys
            .iter()
            .map(|entry| IdentityKeyConfig {
                name: entry.name.clone(),
                attributes: entry.attributes.clone(),
            })
            .collect(),
        strong_identifiers: config.strong_identifiers.clone(),
        constraints: config
            .constraints
            .iter()
            .map(|entry| ConstraintConfig {
                name: entry.name.clone(),
                attribute: entry.attribute.clone(),
                kind: match proto::ConstraintKind::try_from(entry.kind)
                    .unwrap_or(proto::ConstraintKind::Unique)
                {
                    proto::ConstraintKind::Unique => ConstraintKind::Unique,
                    proto::ConstraintKind::UniqueWithinPerspective => {
                        ConstraintKind::UniqueWithinPerspective
                    }
                    proto::ConstraintKind::Unspecified => ConstraintKind::Unique,
                },
            })
            .collect(),
    }
}

async fn fetch_record_batch_from_url(url: &str) -> Result<proto::IngestRecordsRequest, Status> {
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(Status::invalid_argument("url must be http or https"));
    }

    let response = reqwest::get(url)
        .await
        .map_err(|err| Status::unavailable(err.to_string()))?;
    if !response.status().is_success() {
        return Err(Status::unavailable(format!(
            "failed to fetch batch: {}",
            response.status()
        )));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|err| Status::unavailable(err.to_string()))?;
    let inputs: Vec<JsonRecordInput> = serde_json::from_slice(&bytes)
        .map_err(|err| Status::invalid_argument(err.to_string()))?;

    let records = inputs
        .into_iter()
        .map(|input| proto::RecordInput {
            index: input.index,
            identity: Some(proto::RecordIdentity {
                entity_type: input.identity.entity_type,
                perspective: input.identity.perspective,
                uid: input.identity.uid,
            }),
            descriptors: input
                .descriptors
                .into_iter()
                .map(|descriptor| proto::RecordDescriptor {
                    attr: descriptor.attr,
                    value: descriptor.value,
                    start: descriptor.start,
                    end: descriptor.end,
                })
                .collect(),
        })
        .collect();

    Ok(proto::IngestRecordsRequest { records })
}

pub fn hash_record_to_shard(
    config: &DistributedOntologyConfig,
    record: &proto::RecordInput,
    shard_count: usize,
) -> usize {
    let identity = record.identity.as_ref();
    let mut descriptors_by_attr: HashMap<&str, &str> = HashMap::new();
    for descriptor in &record.descriptors {
        descriptors_by_attr.insert(descriptor.attr.as_str(), descriptor.value.as_str());
    }

    for key in &config.identity_keys {
        let mut values = Vec::new();
        let mut has_all = true;
        for attr in &key.attributes {
            if let Some(value) = descriptors_by_attr.get(attr.as_str()) {
                values.push(*value);
            } else {
                has_all = false;
                break;
            }
        }
        if has_all {
            let mut state = std::collections::hash_map::DefaultHasher::new();
            if let Some(identity) = identity {
                identity.entity_type.hash(&mut state);
            }
            key.name.hash(&mut state);
            for value in values {
                value.hash(&mut state);
            }
            return (state.finish() as usize) % shard_count;
        }
    }

    for constraint in &config.constraints {
        if let Some(value) = descriptors_by_attr.get(constraint.attribute.as_str()) {
            let mut state = std::collections::hash_map::DefaultHasher::new();
            if let Some(identity) = identity {
                identity.entity_type.hash(&mut state);
                if matches!(constraint.kind, ConstraintKind::UniqueWithinPerspective) {
                    identity.perspective.hash(&mut state);
                }
            }
            constraint.name.hash(&mut state);
            constraint.attribute.hash(&mut state);
            value.hash(&mut state);
            return (state.finish() as usize) % shard_count;
        }
    }

    let mut state = std::collections::hash_map::DefaultHasher::new();
    if let Some(identity) = identity {
        identity.entity_type.hash(&mut state);
        identity.perspective.hash(&mut state);
        identity.uid.hash(&mut state);
    }
    (state.finish() as usize) % shard_count
}

#[derive(Clone)]
pub struct ShardNode {
    shard_id: u32,
    unirust: Arc<Mutex<Unirust>>,
    tuning: StreamingTuning,
    ontology_config: Arc<Mutex<DistributedOntologyConfig>>,
    data_dir: Option<PathBuf>,
}

impl ShardNode {
    pub fn new(
        shard_id: u32,
        ontology_config: DistributedOntologyConfig,
        tuning: StreamingTuning,
    ) -> AnyResult<Self> {
        Self::new_with_data_dir(shard_id, ontology_config, tuning, None)
    }

    pub fn new_with_data_dir(
        shard_id: u32,
        ontology_config: DistributedOntologyConfig,
        tuning: StreamingTuning,
        data_dir: Option<PathBuf>,
    ) -> AnyResult<Self> {
        if let Some(path) = data_dir.clone() {
            let (store, config, ontology) = load_persistent_state(&path, ontology_config)?;
            let unirust = Unirust::with_store_and_tuning(ontology, store, tuning.clone());
            return Ok(Self {
                shard_id,
                unirust: Arc::new(Mutex::new(unirust)),
                tuning,
                ontology_config: Arc::new(Mutex::new(config)),
                data_dir: Some(path),
            });
        }

        let mut store = crate::Store::new();
        let ontology = ontology_config.clone().build_ontology(&mut store);
        let unirust = Unirust::with_store_and_tuning(ontology, store, tuning.clone());
        Ok(Self {
            shard_id,
            unirust: Arc::new(Mutex::new(unirust)),
            tuning,
            ontology_config: Arc::new(Mutex::new(ontology_config)),
            data_dir: None,
        })
    }

    #[allow(clippy::result_large_err)]
    fn build_record(unirust: &mut Unirust, input: &proto::RecordInput) -> Result<Record, Status> {
        let identity = input
            .identity
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("record identity is required"))?;

        let descriptors = input
            .descriptors
            .iter()
            .map(|desc| {
                let attr = unirust.intern_attr(&desc.attr);
                let value = unirust.intern_value(&desc.value);
                let interval = Interval::new(desc.start, desc.end)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;
                Ok(crate::Descriptor::new(attr, value, interval))
            })
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Record::new(
            RecordId(0),
            RecordIdentity::new(
                identity.entity_type.clone(),
                identity.perspective.clone(),
                identity.uid.clone(),
            ),
            descriptors,
        ))
    }

    fn cluster_key_from_graph(
        cluster_id: crate::ClusterId,
        graph: &crate::graph::KnowledgeGraph,
    ) -> String {
        graph
            .nodes
            .iter()
            .find(|node| node.cluster_id == Some(cluster_id))
            .and_then(|node| node.properties.get("cluster_key"))
            .cloned()
            .unwrap_or_default()
    }

    fn to_proto_match(
        shard_id: u32,
        cluster_id: crate::ClusterId,
        interval: Interval,
        golden: &[GoldenDescriptor],
        cluster_key: Option<String>,
        cluster_key_identity: Option<String>,
    ) -> proto::QueryMatch {
        proto::QueryMatch {
            shard_id,
            cluster_id: cluster_id.0,
            start: interval.start,
            end: interval.end,
            cluster_key: cluster_key.unwrap_or_default(),
            cluster_key_identity: cluster_key_identity.unwrap_or_default(),
            golden: golden
                .iter()
                .map(|descriptor| proto::GoldenDescriptor {
                    attr: descriptor.attr.clone(),
                    value: descriptor.value.clone(),
                    start: descriptor.interval.start,
                    end: descriptor.interval.end,
                })
                .collect(),
        }
    }
}

fn load_persistent_state(
    path: &Path,
    fallback_config: DistributedOntologyConfig,
) -> AnyResult<(PersistentStore, DistributedOntologyConfig, Ontology)> {
    let mut store = PersistentStore::open(path)?;
    let stored_config = store
        .load_ontology_config()?
        .map(|payload| serde_json::from_slice(&payload))
        .transpose()?;
    let config = if let Some(config) = stored_config {
        config
    } else {
        store.save_ontology_config(&serde_json::to_vec(&fallback_config)?)?;
        fallback_config
    };
    let ontology = config.build_ontology(store.inner_mut());
    store.persist_interner()?;
    Ok((store, config, ontology))
}

#[tonic::async_trait]
impl proto::shard_service_server::ShardService for ShardNode {
    async fn set_ontology(
        &self,
        request: Request<proto::ApplyOntologyRequest>,
    ) -> Result<Response<proto::ApplyOntologyResponse>, Status> {
        let config = request
            .into_inner()
            .config
            .ok_or_else(|| Status::invalid_argument("ontology config is required"))?;

        let config = map_proto_config(&config);
        let mut config_guard = self.ontology_config.lock().await;
        *config_guard = config.clone();

        if let Some(path) = &self.data_dir {
            let mut store =
                PersistentStore::open(path).map_err(|err| Status::internal(err.to_string()))?;
            store
                .reset_data()
                .map_err(|err| Status::internal(err.to_string()))?;
            store
                .save_ontology_config(&serde_json::to_vec(&config).map_err(|err| {
                    Status::internal(format!("failed to encode ontology config: {err}"))
                })?)
                .map_err(|err| Status::internal(err.to_string()))?;
            let ontology = config.build_ontology(store.inner_mut());
            store
                .persist_interner()
                .map_err(|err| Status::internal(err.to_string()))?;
            let mut guard = self.unirust.lock().await;
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        } else {
            let mut store = crate::Store::new();
            let ontology = config.build_ontology(&mut store);
            let mut guard = self.unirust.lock().await;
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        }

        Ok(Response::new(proto::ApplyOntologyResponse {}))
    }

    async fn ingest_records(
        &self,
        request: Request<proto::IngestRecordsRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        let mut unirust = self.unirust.lock().await;
        let mut assignments = Vec::new();

        for record in &request.into_inner().records {
            let record_input = Self::build_record(&mut unirust, record)?;
            let update = unirust
                .stream_record_update_graph(record_input)
                .map_err(|err| Status::internal(err.to_string()))?;
            let cluster_key =
                Self::cluster_key_from_graph(update.assignment.cluster_id, &update.graph);

            assignments.push(proto::IngestAssignment {
                index: record.index,
                shard_id: self.shard_id,
                record_id: update.assignment.record_id.0,
                cluster_id: update.assignment.cluster_id.0,
                cluster_key,
            });
        }

        Ok(Response::new(proto::IngestRecordsResponse { assignments }))
    }

    async fn ingest_records_from_url(
        &self,
        request: Request<proto::IngestRecordsFromUrlRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        let batch = fetch_record_batch_from_url(&request.into_inner().url).await?;
        self.ingest_records(Request::new(batch)).await
    }

    async fn query_entities(
        &self,
        request: Request<proto::QueryEntitiesRequest>,
    ) -> Result<Response<proto::QueryEntitiesResponse>, Status> {
        let mut unirust = self.unirust.lock().await;
        let request = request.into_inner();
        let interval = Interval::new(request.start, request.end)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;

        let descriptors = request
            .descriptors
            .iter()
            .map(|descriptor| QueryDescriptor {
                attr: unirust.intern_attr(&descriptor.attr),
                value: unirust.intern_value(&descriptor.value),
            })
            .collect::<Vec<_>>();

        let outcome = unirust
            .query_master_entities(&descriptors, interval)
            .map_err(|err| Status::internal(err.to_string()))?;

        let response = match outcome {
            QueryOutcome::Matches(matches) => proto::QueryEntitiesResponse {
                outcome: Some(proto::query_entities_response::Outcome::Matches(
                    proto::QueryMatches {
                        matches: matches
                            .into_iter()
                            .map(|entry| {
                                Self::to_proto_match(
                                    self.shard_id,
                                    entry.cluster_id,
                                    entry.interval,
                                    &entry.golden,
                                    entry.cluster_key,
                                    entry.cluster_key_identity,
                                )
                            })
                            .collect(),
                    },
                )),
            },
            QueryOutcome::Conflict(conflict) => {
                let descriptors = conflict
                    .descriptors
                    .into_iter()
                    .map(|descriptor| proto::QueryDescriptorOverlap {
                        descriptor: Some(proto::QueryDescriptor {
                            attr: unirust
                                .resolve_attr(descriptor.descriptor.attr)
                                .unwrap_or_default(),
                            value: unirust
                                .resolve_value(descriptor.descriptor.value)
                                .unwrap_or_default(),
                        }),
                        start: descriptor.interval.start,
                        end: descriptor.interval.end,
                    })
                    .collect();

                let clusters = conflict
                    .clusters
                    .into_iter()
                    .map(|cluster_id| proto::QueryMatch {
                        shard_id: self.shard_id,
                        cluster_id: cluster_id.0,
                        start: conflict.interval.start,
                        end: conflict.interval.end,
                        cluster_key: String::new(),
                        cluster_key_identity: String::new(),
                        golden: Vec::new(),
                    })
                    .collect();

                proto::QueryEntitiesResponse {
                    outcome: Some(proto::query_entities_response::Outcome::Conflict(
                        proto::QueryConflict {
                            start: conflict.interval.start,
                            end: conflict.interval.end,
                            clusters,
                            descriptors,
                        },
                    )),
                }
            }
        };

        Ok(Response::new(response))
    }

    async fn get_stats(
        &self,
        _request: Request<proto::StatsRequest>,
    ) -> Result<Response<proto::StatsResponse>, Status> {
        let unirust = self.unirust.lock().await;
        let clusters = unirust
            .build_clusters()
            .map_err(|err| Status::internal(err.to_string()))?;
        let observations = unirust
            .detect_conflicts(&clusters)
            .map_err(|err| Status::internal(err.to_string()))?;
        let graph = unirust
            .export_graph(&clusters, &observations)
            .map_err(|err| Status::internal(err.to_string()))?;
        let record_count = graph
            .metadata
            .get("num_records")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);

        Ok(Response::new(proto::StatsResponse {
            record_count,
            cluster_count: clusters.len() as u64,
            conflict_count: observations.len() as u64,
            graph_node_count: graph.num_nodes() as u64,
            graph_edge_count: (graph.num_same_as_edges() + graph.num_conflicts_with_edges())
                as u64,
        }))
    }

    async fn health_check(
        &self,
        _request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        Ok(Response::new(proto::HealthCheckResponse {
            status: "ok".to_string(),
        }))
    }

    async fn list_conflicts(
        &self,
        request: Request<proto::ListConflictsRequest>,
    ) -> Result<Response<proto::ListConflictsResponse>, Status> {
        let unirust = self.unirust.lock().await;
        let request = request.into_inner();
        let clusters = unirust
            .build_clusters()
            .map_err(|err| Status::internal(err.to_string()))?;
        let observations = unirust
            .detect_conflicts(&clusters)
            .map_err(|err| Status::internal(err.to_string()))?;
        let mut summaries = unirust.summarize_conflicts(&observations);

        if !request.attribute.is_empty() {
            summaries.retain(|summary| summary.attribute.as_deref() == Some(&request.attribute));
        }

        if request.end > request.start {
            let filter = Interval::new(request.start, request.end)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
            summaries.retain(|summary| crate::temporal::is_overlapping(&summary.interval, &filter));
        }
        let response = proto::ListConflictsResponse {
            conflicts: summaries
                .into_iter()
                .map(|summary| to_proto_conflict_summary(summary))
                .collect(),
        };
        Ok(Response::new(response))
    }

    async fn reset(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        let config = self.ontology_config.lock().await.clone();
        if let Some(path) = &self.data_dir {
            let mut store =
                PersistentStore::open(path).map_err(|err| Status::internal(err.to_string()))?;
            store
                .reset_data()
                .map_err(|err| Status::internal(err.to_string()))?;
            store
                .save_ontology_config(&serde_json::to_vec(&config).map_err(|err| {
                    Status::internal(format!("failed to encode ontology config: {err}"))
                })?)
                .map_err(|err| Status::internal(err.to_string()))?;
            let ontology = config.build_ontology(store.inner_mut());
            store
                .persist_interner()
                .map_err(|err| Status::internal(err.to_string()))?;
            let mut guard = self.unirust.lock().await;
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        } else {
            let mut store = crate::Store::new();
            let ontology = config.build_ontology(&mut store);
            let mut guard = self.unirust.lock().await;
            *guard = Unirust::with_store_and_tuning(ontology, store, self.tuning.clone());
        }
        Ok(Response::new(proto::Empty {}))
    }
}

#[derive(Clone)]
pub struct RouterNode {
    shard_clients: Vec<proto::shard_service_client::ShardServiceClient<tonic::transport::Channel>>,
    ontology_config: Arc<Mutex<DistributedOntologyConfig>>,
}

impl RouterNode {
    pub async fn connect(
        shard_addrs: Vec<String>,
        ontology_config: DistributedOntologyConfig,
    ) -> Result<Self, Status> {
        let mut shard_clients = Vec::with_capacity(shard_addrs.len());
        for addr in shard_addrs {
            let client = proto::shard_service_client::ShardServiceClient::connect(addr)
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
            shard_clients.push(client);
        }
        Ok(Self {
            shard_clients,
            ontology_config: Arc::new(Mutex::new(ontology_config)),
        })
    }

    fn merge_query_responses(
        &self,
        descriptors: &[proto::QueryDescriptor],
        responses: Vec<proto::QueryEntitiesResponse>,
    ) -> proto::QueryEntitiesResponse {
        let mut matches = Vec::new();
        for response in responses {
            match response.outcome {
                Some(proto::query_entities_response::Outcome::Conflict(conflict)) => {
                    return proto::QueryEntitiesResponse {
                        outcome: Some(proto::query_entities_response::Outcome::Conflict(conflict)),
                    };
                }
                Some(proto::query_entities_response::Outcome::Matches(found)) => {
                    matches.extend(found.matches);
                }
                None => {}
            }
        }

        if matches.len() <= 1 {
            return proto::QueryEntitiesResponse {
                outcome: Some(proto::query_entities_response::Outcome::Matches(
                    proto::QueryMatches { matches },
                )),
            };
        }

        matches.sort_by(|a, b| a.start.cmp(&b.start));

        for window in matches.windows(2) {
            let current = &window[0];
            let next = &window[1];
            if current.shard_id == next.shard_id && current.cluster_id == next.cluster_id {
                continue;
            }
            if current.start < next.end && next.start < current.end {
                let overlap_start = current.start.max(next.start);
                let overlap_end = current.end.min(next.end);
                let descriptors = descriptors
                    .iter()
                    .map(|descriptor| proto::QueryDescriptorOverlap {
                        descriptor: Some(descriptor.clone()),
                        start: overlap_start,
                        end: overlap_end,
                    })
                    .collect();

                return proto::QueryEntitiesResponse {
                    outcome: Some(proto::query_entities_response::Outcome::Conflict(
                        proto::QueryConflict {
                            start: overlap_start,
                            end: overlap_end,
                            clusters: vec![current.clone(), next.clone()],
                            descriptors,
                        },
                    )),
                };
            }
        }

        proto::QueryEntitiesResponse {
            outcome: Some(proto::query_entities_response::Outcome::Matches(
                proto::QueryMatches { matches },
            )),
        }
    }
}

#[tonic::async_trait]
impl proto::router_service_server::RouterService for RouterNode {
    async fn set_ontology(
        &self,
        request: Request<proto::ApplyOntologyRequest>,
    ) -> Result<Response<proto::ApplyOntologyResponse>, Status> {
        let payload = request.into_inner();
        let config = payload
            .config
            .clone()
            .ok_or_else(|| Status::invalid_argument("ontology config is required"))?;
        let mapped = map_proto_config(&config);
        *self.ontology_config.lock().await = mapped;

        for client in &self.shard_clients {
            let mut client = client.clone();
            client
                .set_ontology(Request::new(payload.clone()))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
        }

        Ok(Response::new(proto::ApplyOntologyResponse {}))
    }

    async fn ingest_records(
        &self,
        request: Request<proto::IngestRecordsRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        let batch = request.into_inner();
        let shard_count = self.shard_clients.len();
        let config = self.ontology_config.lock().await.clone();
        let mut shard_batches: Vec<Vec<proto::RecordInput>> = vec![Vec::new(); shard_count];

        for record in batch.records {
            let shard_idx = hash_record_to_shard(&config, &record, shard_count);
            shard_batches[shard_idx].push(record);
        }

        let mut results: Vec<proto::IngestAssignment> = Vec::new();
        for (idx, records) in shard_batches.into_iter().enumerate() {
            if records.is_empty() {
                continue;
            }
            let mut client = self.shard_clients[idx].clone();
            let response = client
                .ingest_records(Request::new(proto::IngestRecordsRequest { records }))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
            results.extend(response.into_inner().assignments);
        }

        results.sort_by_key(|assignment| assignment.index);
        Ok(Response::new(proto::IngestRecordsResponse {
            assignments: results,
        }))
    }

    async fn ingest_records_from_url(
        &self,
        request: Request<proto::IngestRecordsFromUrlRequest>,
    ) -> Result<Response<proto::IngestRecordsResponse>, Status> {
        let batch = fetch_record_batch_from_url(&request.into_inner().url).await?;
        self.ingest_records(Request::new(batch)).await
    }

    async fn query_entities(
        &self,
        request: Request<proto::QueryEntitiesRequest>,
    ) -> Result<Response<proto::QueryEntitiesResponse>, Status> {
        let request = request.into_inner();
        let mut responses = Vec::with_capacity(self.shard_clients.len());
        for client in &self.shard_clients {
            let mut client = client.clone();
            let response = client
                .query_entities(Request::new(request.clone()))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
            responses.push(response.into_inner());
        }

        let merged = self.merge_query_responses(&request.descriptors, responses);
        Ok(Response::new(merged))
    }

    async fn get_stats(
        &self,
        _request: Request<proto::StatsRequest>,
    ) -> Result<Response<proto::StatsResponse>, Status> {
        let mut totals = proto::StatsResponse {
            record_count: 0,
            cluster_count: 0,
            conflict_count: 0,
            graph_node_count: 0,
            graph_edge_count: 0,
        };

        for client in &self.shard_clients {
            let mut client = client.clone();
            let response = client
                .get_stats(Request::new(proto::StatsRequest {}))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner();
            totals.record_count += response.record_count;
            totals.cluster_count += response.cluster_count;
            totals.conflict_count += response.conflict_count;
            totals.graph_node_count += response.graph_node_count;
            totals.graph_edge_count += response.graph_edge_count;
        }

        Ok(Response::new(totals))
    }

    async fn health_check(
        &self,
        _request: Request<proto::HealthCheckRequest>,
    ) -> Result<Response<proto::HealthCheckResponse>, Status> {
        for client in &self.shard_clients {
            let mut client = client.clone();
            client
                .health_check(Request::new(proto::HealthCheckRequest {}))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
        }

        Ok(Response::new(proto::HealthCheckResponse {
            status: "ok".to_string(),
        }))
    }

    async fn list_conflicts(
        &self,
        request: Request<proto::ListConflictsRequest>,
    ) -> Result<Response<proto::ListConflictsResponse>, Status> {
        let payload = request.into_inner();
        let mut summaries = Vec::new();
        for client in &self.shard_clients {
            let mut client = client.clone();
            let response = client
                .list_conflicts(Request::new(payload.clone()))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?
                .into_inner();
            summaries.extend(response.conflicts);
        }

        summaries.sort_by(|a, b| {
            (
                a.kind.clone(),
                a.attribute.clone(),
                a.start,
                a.end,
                a.cause.clone(),
                a.records
                    .iter()
                    .map(|record| format!("{}:{}", record.perspective, record.uid))
                    .collect::<Vec<_>>(),
            )
                .cmp(&(
                    b.kind.clone(),
                    b.attribute.clone(),
                    b.start,
                    b.end,
                    b.cause.clone(),
                    b.records
                        .iter()
                        .map(|record| format!("{}:{}", record.perspective, record.uid))
                        .collect::<Vec<_>>(),
                ))
        });

        Ok(Response::new(proto::ListConflictsResponse {
            conflicts: summaries,
        }))
    }

    async fn reset(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::Empty>, Status> {
        for client in &self.shard_clients {
            let mut client = client.clone();
            client
                .reset(Request::new(proto::Empty {}))
                .await
                .map_err(|err| Status::unavailable(err.to_string()))?;
        }
        Ok(Response::new(proto::Empty {}))
    }
}

fn to_proto_conflict_summary(summary: ConflictSummary) -> proto::ConflictSummary {
    proto::ConflictSummary {
        kind: summary.kind,
        attribute: summary.attribute.unwrap_or_default(),
        start: summary.interval.start,
        end: summary.interval.end,
        records: summary
            .records
            .into_iter()
            .map(|record| proto::RecordRef {
                perspective: record.perspective,
                uid: record.uid,
            })
            .collect(),
        cause: summary.cause.unwrap_or_default(),
    }
}
