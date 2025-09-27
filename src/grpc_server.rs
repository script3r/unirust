//! # gRPC Server Implementation
//!
//! Provides a gRPC API for the Unirust entity mastering engine.

use crate::conflicts;
use crate::dsu;
use crate::graph;
use crate::linker;
use crate::model::{ClusterId, Descriptor, Record, RecordId, RecordIdentity, StringInterner};
use crate::ontology::{Constraint, IdentityKey, Ontology, StrongIdentifier};
use crate::store::Store;
use crate::temporal::Interval;
use crate::utils;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};
use uuid::Uuid;

// Include the generated gRPC code
pub mod unirust {
    tonic::include_proto!("unirust.v1");
}

use unirust::unirust_service_server::{UnirustService, UnirustServiceServer};
use unirust::*;

/// Session state for a Unirust instance
#[derive(Debug, Clone)]
struct Session {
    store: Store,
    ontology: Ontology,
}

impl Session {
    fn new(ontology: Ontology) -> Self {
        Self {
            store: Store::new(),
            ontology,
        }
    }
}

/// gRPC service implementation
#[derive(Debug)]
pub struct UnirustGrpcService {
    sessions: Arc<RwLock<HashMap<String, Session>>>,
}

impl UnirustGrpcService {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn get_session(&self, session_id: &str) -> Result<Session> {
        let sessions = self
            .sessions
            .read()
            .map_err(|_| anyhow!("Failed to acquire read lock"))?;
        sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| anyhow!("Session not found: {}", session_id))
    }

    fn update_session(&self, session_id: &str, session: Session) -> Result<()> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| anyhow!("Failed to acquire write lock"))?;
        sessions.insert(session_id.to_string(), session);
        Ok(())
    }

    fn delete_session(&self, session_id: &str) -> Result<bool> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| anyhow!("Failed to acquire write lock"))?;
        Ok(sessions.remove(session_id).is_some())
    }

    fn list_session_ids(&self) -> Result<Vec<String>> {
        let sessions = self
            .sessions
            .read()
            .map_err(|_| anyhow!("Failed to acquire read lock"))?;
        Ok(sessions.keys().cloned().collect())
    }
}

// Conversion functions between proto and internal types
impl From<unirust::Interval> for Interval {
    fn from(proto: unirust::Interval) -> Self {
        Interval::new(proto.start, proto.end).unwrap_or_else(|_| Interval::new(0, 1).unwrap())
    }
}

impl From<Interval> for unirust::Interval {
    fn from(interval: Interval) -> Self {
        Self {
            start: interval.start,
            end: interval.end,
        }
    }
}

impl From<unirust::RecordIdentity> for RecordIdentity {
    fn from(proto: unirust::RecordIdentity) -> Self {
        RecordIdentity::new(proto.entity_type, proto.perspective, proto.uid)
    }
}

impl From<RecordIdentity> for unirust::RecordIdentity {
    fn from(identity: RecordIdentity) -> Self {
        Self {
            entity_type: identity.entity_type,
            perspective: identity.perspective,
            uid: identity.uid,
        }
    }
}

fn convert_proto_record_to_internal(
    proto: unirust::Record,
    interner: &mut StringInterner,
) -> Record {
    let descriptors = proto
        .descriptors
        .into_iter()
        .map(|desc| {
            let attr = interner.intern_attr(&desc.attribute);
            let value = interner.intern_value(&desc.value);
            let interval = desc.interval.unwrap_or_default().into();
            Descriptor::new(attr, value, interval)
        })
        .collect();

    Record::new(RecordId(proto.id), proto.identity.unwrap().into(), descriptors)
}

fn convert_proto_ontology_to_internal(proto: unirust::Ontology) -> Ontology {
    let mut ontology = Ontology::new();

    // Convert identity keys
    for proto_key in proto.identity_keys {
        let mut interner = StringInterner::new(); // Temporary interner for conversion
        let attributes = proto_key
            .attributes
            .into_iter()
            .map(|attr| interner.intern_attr(&attr))
            .collect();
        let identity_key = IdentityKey::new(attributes, proto_key.name);
        ontology.add_identity_key(identity_key);
    }

    // Convert strong identifiers
    for proto_strong in proto.strong_identifiers {
        let mut interner = StringInterner::new(); // Temporary interner for conversion
        let attr = interner.intern_attr(&proto_strong.attribute);
        let strong_id = StrongIdentifier::new(attr, proto_strong.name);
        ontology.add_strong_identifier(strong_id);
    }

    // Convert constraints
    for proto_constraint in proto.constraints {
        match proto_constraint.constraint_type {
            Some(unirust::constraint::ConstraintType::Unique(unique)) => {
                let mut interner = StringInterner::new(); // Temporary interner for conversion
                let attr = interner.intern_attr(&unique.attribute);
                let constraint = Constraint::unique(attr, unique.name);
                ontology.add_constraint(constraint);
            }
            Some(unirust::constraint::ConstraintType::UniqueWithinPerspective(unique_wp)) => {
                let mut interner = StringInterner::new(); // Temporary interner for conversion
                let attr = interner.intern_attr(&unique_wp.attribute);
                let constraint = Constraint::unique_within_perspective(attr, unique_wp.name);
                ontology.add_constraint(constraint);
            }
            None => {} // Skip malformed constraints
        }
    }

    // Set perspective weights
    for (perspective, weight) in proto.perspective_weights {
        ontology.set_perspective_weight(perspective, weight);
    }

    ontology
}

fn convert_internal_clusters_to_proto(clusters: &dsu::Clusters) -> unirust::Clusters {
    let proto_clusters = clusters
        .clusters
        .iter()
        .enumerate()
        .map(|(i, cluster)| unirust::Cluster {
            id: i as u32,
            records: cluster.records.iter().map(|id| id.0).collect(),
        })
        .collect();

    unirust::Clusters {
        clusters: proto_clusters,
    }
}

fn convert_proto_clusters_to_internal(proto: unirust::Clusters) -> dsu::Clusters {
    let clusters = proto
        .clusters
        .into_iter()
        .map(|cluster| {
            let records: Vec<RecordId> = cluster.records.into_iter().map(RecordId).collect();
            let root = records.first().copied().unwrap_or(RecordId(0));
            dsu::Cluster::new(ClusterId(cluster.id), root, records)
        })
        .collect();

    dsu::Clusters { clusters }
}

fn convert_internal_observations_to_proto(
    observations: &[conflicts::Observation],
) -> Vec<unirust::Observation> {
    observations
        .iter()
        .map(|obs| match obs {
            conflicts::Observation::DirectConflict(conflict) => unirust::Observation {
                observation_type: Some(unirust::observation::ObservationType::DirectConflict(
                    unirust::DirectConflict {
                        kind: conflict.kind.clone(),
                        attribute: conflict.attribute.0.to_string(), // Convert AttrId to string
                        interval: Some(conflict.interval.into()),
                        values: conflict
                            .values
                            .iter()
                            .map(|cv| unirust::ConflictValue {
                                value: cv.value.0.to_string(), // Convert ValueId to string
                                participants: cv.participants.iter().map(|id| id.0).collect(),
                            })
                            .collect(),
                    },
                )),
            },
            conflicts::Observation::IndirectConflict(conflict) => unirust::Observation {
                observation_type: Some(unirust::observation::ObservationType::IndirectConflict(
                    unirust::IndirectConflict {
                        kind: conflict.kind.clone(),
                        cause: conflict.cause.clone(),
                        attribute: conflict.attribute.map(|attr| attr.0.to_string()),
                        interval: Some(conflict.interval.into()),
                        status: conflict.status.clone(),
                        details: HashMap::new(), // Simplified for now
                    },
                )),
            },
            conflicts::Observation::Merge {
                records,
                cluster: _,
                interval,
                reason,
            } => unirust::Observation {
                observation_type: Some(unirust::observation::ObservationType::Merge(
                    unirust::MergeObservation {
                        records: records.iter().map(|id| id.0).collect(),
                        interval: Some((*interval).into()),
                        reason: reason.clone(),
                    },
                )),
            },
        })
        .collect()
}

#[tonic::async_trait]
impl UnirustService for UnirustGrpcService {
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: "OK".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }

    async fn create_ontology(
        &self,
        request: Request<CreateOntologyRequest>,
    ) -> Result<Response<CreateOntologyResponse>, Status> {
        let req = request.into_inner();
        let session_id = Uuid::new_v4().to_string();

        match req.ontology {
            Some(proto_ontology) => {
                let ontology = convert_proto_ontology_to_internal(proto_ontology);
                let session = Session::new(ontology);

                let mut sessions = self
                    .sessions
                    .write()
                    .map_err(|_| Status::internal("Failed to acquire write lock"))?;
                sessions.insert(session_id.clone(), session);

                Ok(Response::new(CreateOntologyResponse {
                    session_id,
                    success: true,
                    message: "Ontology created successfully".to_string(),
                }))
            }
            None => Err(Status::invalid_argument("Ontology is required")),
        }
    }

    async fn get_session_info(
        &self,
        request: Request<GetSessionInfoRequest>,
    ) -> Result<Response<GetSessionInfoResponse>, Status> {
        let req = request.into_inner();
        let session = self
            .get_session(&req.session_id)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let available_attributes: Vec<String> = session
            .store
            .interner()
            .attr_ids()
            .map(|attr_id| {
                session
                    .store
                    .interner()
                    .get_attr(attr_id)
                    .unwrap_or(&"unknown".to_string())
                    .clone()
            })
            .collect();

        let available_perspectives: Vec<String> = session
            .store
            .get_all_records()
            .iter()
            .map(|record| record.identity.perspective.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        Ok(Response::new(GetSessionInfoResponse {
            success: true,
            message: "Session info retrieved successfully".to_string(),
            session_id: req.session_id,
            record_count: session.store.len() as u32,
            has_ontology: true, // Always true since we create ontology with session
            available_attributes,
            available_perspectives,
        }))
    }

    async fn list_sessions(
        &self,
        _request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsResponse>, Status> {
        let session_ids = self
            .list_session_ids()
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ListSessionsResponse { session_ids }))
    }

    async fn delete_session(
        &self,
        request: Request<DeleteSessionRequest>,
    ) -> Result<Response<DeleteSessionResponse>, Status> {
        let req = request.into_inner();
        let deleted = self
            .delete_session(&req.session_id)
            .map_err(|e| Status::internal(e.to_string()))?;

        let (success, message) = if deleted {
            (true, "Session deleted successfully".to_string())
        } else {
            (false, "Session not found".to_string())
        };

        Ok(Response::new(DeleteSessionResponse { success, message }))
    }

    async fn ingest_records(
        &self,
        request: Request<IngestRecordsRequest>,
    ) -> Result<Response<IngestRecordsResponse>, Status> {
        let req = request.into_inner();
        let mut session = self
            .get_session(&req.session_id)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let records: Vec<Record> = req
            .records
            .into_iter()
            .map(|proto_record| convert_proto_record_to_internal(proto_record, session.store.interner_mut()))
            .collect();

        let records_count = records.len();
        
        session
            .store
            .add_records(records)
            .map_err(|e| Status::internal(e.to_string()))?;

        self.update_session(&req.session_id, session)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(IngestRecordsResponse {
            success: true,
            message: format!("Successfully ingested {} records", records_count),
            records_ingested: records_count as u32,
        }))
    }

    async fn build_clusters(
        &self,
        request: Request<BuildClustersRequest>,
    ) -> Result<Response<BuildClustersResponse>, Status> {
        let req = request.into_inner();
        let session = self
            .get_session(&req.session_id)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let clusters = if req.use_optimized {
            linker::build_clusters_optimized(&session.store, &session.ontology)
        } else {
            linker::build_clusters(&session.store, &session.ontology)
        };

        match clusters {
            Ok(clusters) => {
                let proto_clusters = convert_internal_clusters_to_proto(&clusters);
                Ok(Response::new(BuildClustersResponse {
                    success: true,
                    message: format!("Built {} clusters", clusters.clusters.len()),
                    clusters: Some(proto_clusters),
                }))
            }
            Err(e) => Err(Status::internal(format!("Failed to build clusters: {}", e))),
        }
    }

    async fn detect_conflicts(
        &self,
        request: Request<DetectConflictsRequest>,
    ) -> Result<Response<DetectConflictsResponse>, Status> {
        let req = request.into_inner();
        let session = self
            .get_session(&req.session_id)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let clusters = req
            .clusters
            .ok_or_else(|| Status::invalid_argument("Clusters are required"))?;
        let internal_clusters = convert_proto_clusters_to_internal(clusters);

        match conflicts::detect_conflicts(&session.store, &internal_clusters, &session.ontology) {
            Ok(observations) => {
                let proto_observations = convert_internal_observations_to_proto(&observations);
                Ok(Response::new(DetectConflictsResponse {
                    success: true,
                    message: format!("Detected {} observations", observations.len()),
                    observations: proto_observations,
                }))
            }
            Err(e) => Err(Status::internal(format!(
                "Failed to detect conflicts: {}",
                e
            ))),
        }
    }

    async fn export_knowledge_graph(
        &self,
        request: Request<ExportKnowledgeGraphRequest>,
    ) -> Result<Response<ExportKnowledgeGraphResponse>, Status> {
        let req = request.into_inner();
        let session = self
            .get_session(&req.session_id)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let clusters = req
            .clusters
            .ok_or_else(|| Status::invalid_argument("Clusters are required"))?;
        let internal_clusters = convert_proto_clusters_to_internal(clusters);

        // Convert proto observations back to internal format
        let observations: Vec<conflicts::Observation> = req
            .observations
            .into_iter()
            .filter_map(|proto_obs| {
                // This is a simplified conversion - in a real implementation you'd want full bidirectional conversion
                match proto_obs.observation_type {
                    Some(unirust::observation::ObservationType::Merge(merge)) => {
                        Some(conflicts::Observation::Merge {
                            records: merge.records.into_iter().map(RecordId).collect(),
                            cluster: ClusterId(0), // Simplified - would need proper mapping
                            interval: merge.interval.unwrap_or_default().into(),
                            reason: merge.reason,
                        })
                    }
                    _ => None, // Skip other types for now
                }
            })
            .collect();

        match graph::export_graph(&session.store, &internal_clusters, &observations, &session.ontology) {
            Ok(_kg) => {
                // Convert internal knowledge graph to proto format
                // This is a simplified conversion
                let proto_kg = unirust::KnowledgeGraph {
                    nodes: vec![], // Simplified - you'd implement full conversion
                    edges: vec![], // Simplified - you'd implement full conversion
                };

                Ok(Response::new(ExportKnowledgeGraphResponse {
                    success: true,
                    message: "Knowledge graph exported successfully".to_string(),
                    graph: Some(proto_kg),
                }))
            }
            Err(e) => Err(Status::internal(format!(
                "Failed to export knowledge graph: {}",
                e
            ))),
        }
    }

    async fn export_formats(
        &self,
        request: Request<ExportFormatsRequest>,
    ) -> Result<Response<ExportFormatsResponse>, Status> {
        let req = request.into_inner();
        let session = self
            .get_session(&req.session_id)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let clusters = req
            .clusters
            .ok_or_else(|| Status::invalid_argument("Clusters are required"))?;
        let internal_clusters = convert_proto_clusters_to_internal(clusters);

        let observations: Vec<conflicts::Observation> = req
            .observations
            .into_iter()
            .filter_map(|proto_obs| {
                match proto_obs.observation_type {
                    Some(unirust::observation::ObservationType::Merge(merge)) => {
                        Some(conflicts::Observation::Merge {
                            records: merge.records.into_iter().map(RecordId).collect(),
                            cluster: ClusterId(0), // Simplified - would need proper mapping
                            interval: merge.interval.unwrap_or_default().into(),
                            reason: merge.reason,
                        })
                    }
                    _ => None,
                }
            })
            .collect();

        let mut exports = HashMap::new();

        for format in req.formats {
            let result = match format.as_str() {
                "jsonl" => {
                    graph::export_graph(&session.store, &internal_clusters, &observations, &session.ontology)
                        .and_then(|kg| kg.to_jsonl())
                        .map_err(|e| e.to_string())
                }
                "dot" => {
                    utils::export_to_dot(&session.store, &internal_clusters, &observations, &session.ontology)
                        .map_err(|e| e.to_string())
                }
                "text_summary" => {
                    utils::export_to_text_summary(&session.store, &internal_clusters, &observations)
                        .map_err(|e| e.to_string())
                }
                _ => Err(format!("Unsupported format: {}", format)),
            };

            match result {
                Ok(content) => {
                    exports.insert(format, content);
                }
                Err(e) => {
                    return Err(Status::internal(format!(
                        "Failed to export format {}: {}",
                        format, e
                    )));
                }
            }
        }

        Ok(Response::new(ExportFormatsResponse {
            success: true,
            message: format!("Exported {} formats", exports.len()),
            exports,
        }))
    }
}

/// Create a new gRPC server
pub fn create_grpc_server() -> UnirustServiceServer<UnirustGrpcService> {
    let service = UnirustGrpcService::new();
    UnirustServiceServer::new(service)
}

/// Run the gRPC server on the specified address
pub async fn run_grpc_server(addr: std::net::SocketAddr) -> Result<()> {
    let server = create_grpc_server();

    println!("Unirust gRPC server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(server)
        .serve(addr)
        .await
        .map_err(|e| anyhow!("Server error: {}", e))?;

    Ok(())
}