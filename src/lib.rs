//! # Unirust
//!
//! A general-purpose, temporal-first entity mastering and conflict-resolution engine.
//!
//! This library provides precise temporal modeling, entity resolution, and conflict detection
//! with strong guarantees about temporal correctness and auditability.

pub mod config;
pub mod conflicts;
pub mod distributed;
pub mod dsu;
pub mod graph;
pub mod index;
pub mod linker;
pub mod model;
pub mod ontology;
pub mod persistence;
pub mod profile;
pub mod query;
pub mod sharding;
pub mod store;
pub mod temporal;
pub mod utils;

// Re-export main types for convenience
pub use config::{StreamingTuning, TuningProfile};
pub use model::{ClusterId, Descriptor, Record, RecordId, RecordIdentity};
pub use ontology::Ontology;
pub use persistence::PersistentStore;
pub use query::{QueryConflict, QueryDescriptor, QueryDescriptorOverlap, QueryMatch, QueryOutcome};
pub use store::{RecordStore, Store, StoreMetrics};
pub use temporal::Interval;

/// Assignment result for streaming clustering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamedClusterAssignment {
    pub record_id: RecordId,
    pub cluster_id: ClusterId,
}

/// Streaming update with conflict observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamedConflictUpdate {
    pub assignment: StreamedClusterAssignment,
    pub observations: Vec<conflicts::Observation>,
}

/// Streaming update with graph output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamedGraphUpdate {
    pub assignment: StreamedClusterAssignment,
    pub observations: Vec<conflicts::Observation>,
    pub graph: graph::KnowledgeGraph,
}

/// Main API for entity mastering
pub struct Unirust {
    store: Box<dyn RecordStore>,
    ontology: Ontology,
    streaming: Option<linker::StreamingLinker>,
    graph_state: Option<graph::IncrementalKnowledgeGraph>,
    query_cache: std::sync::Mutex<Option<QueryCache>>,
    conflict_cache: std::sync::Mutex<Option<Vec<conflicts::ConflictSummary>>>,
    query_stats: std::sync::Mutex<query::QuerySelectivityStats>,
    tuning: StreamingTuning,
}

#[derive(Clone)]
struct QueryCache {
    clusters: dsu::Clusters,
    golden: std::collections::HashMap<ClusterId, Vec<graph::GoldenDescriptor>>,
    cluster_keys: std::collections::HashMap<ClusterId, graph::ClusterKey>,
    record_to_cluster: std::collections::HashMap<RecordId, ClusterId>,
}

impl Unirust {
    /// Create a new Unirust instance
    pub fn new(ontology: Ontology) -> Self {
        Self::with_store(ontology, Store::new())
    }

    /// Create a new Unirust instance with a custom store implementation.
    pub fn with_store<S>(ontology: Ontology, store: S) -> Self
    where
        S: RecordStore + 'static,
    {
        Self {
            store: Box::new(store),
            ontology,
            streaming: None,
            graph_state: None,
            query_cache: std::sync::Mutex::new(None),
            conflict_cache: std::sync::Mutex::new(None),
            query_stats: std::sync::Mutex::new(query::QuerySelectivityStats::default()),
            tuning: StreamingTuning::default(),
        }
    }

    pub fn store_mut(&mut self) -> &mut dyn RecordStore {
        self.store.as_mut()
    }

    pub fn with_store_and_tuning<S>(ontology: Ontology, store: S, tuning: StreamingTuning) -> Self
    where
        S: RecordStore + 'static,
    {
        Self {
            store: Box::new(store),
            ontology,
            streaming: None,
            graph_state: None,
            query_cache: std::sync::Mutex::new(None),
            conflict_cache: std::sync::Mutex::new(None),
            query_stats: std::sync::Mutex::new(query::QuerySelectivityStats::default()),
            tuning,
        }
    }

    /// Ingest records into the store
    pub fn ingest(&mut self, records: Vec<Record>) -> anyhow::Result<()> {
        self.store.add_records(records)?;
        self.streaming = None;
        self.graph_state = None;
        self.invalidate_query_cache();
        self.clear_conflict_cache();
        self.clear_query_stats();
        Ok(())
    }

    /// Build clusters from the current store
    pub fn build_clusters(&self) -> anyhow::Result<dsu::Clusters> {
        linker::build_clusters(self.store.as_ref(), &self.ontology)
    }

    /// Detect conflicts in the given clusters
    pub fn detect_conflicts(
        &self,
        clusters: &dsu::Clusters,
    ) -> anyhow::Result<Vec<conflicts::Observation>> {
        conflicts::detect_conflicts(self.store.as_ref(), clusters, &self.ontology)
    }

    /// Export the knowledge graph
    pub fn export_graph(
        &self,
        clusters: &dsu::Clusters,
        observations: &[conflicts::Observation],
    ) -> anyhow::Result<graph::KnowledgeGraph> {
        graph::export_graph(self.store.as_ref(), clusters, observations, &self.ontology)
    }

    /// Export the knowledge graph to DOT format.
    pub fn export_dot(
        &self,
        clusters: &dsu::Clusters,
        observations: &[conflicts::Observation],
    ) -> anyhow::Result<String> {
        utils::export_to_dot(self.store.as_ref(), clusters, observations, &self.ontology)
    }

    /// Get a record by ID from the underlying store.
    pub fn get_record(&self, id: RecordId) -> Option<Record> {
        self.store.get_record(id)
    }

    /// Get records in an ID range [start, end), limited to max_results.
    pub fn records_in_id_range(
        &self,
        start: RecordId,
        end: RecordId,
        max_results: usize,
    ) -> Vec<Record> {
        self.store.records_in_id_range(start, end, max_results)
    }

    /// Get min/max record IDs if any records exist.
    pub fn record_id_bounds(&self) -> Option<(RecordId, RecordId)> {
        self.store.record_id_bounds()
    }

    /// Get the number of records in the store.
    pub fn record_count(&self) -> usize {
        self.store.len()
    }

    pub fn streaming_cluster_count(&self) -> Option<usize> {
        self.streaming
            .as_ref()
            .map(|streaming| streaming.cluster_count())
            .or_else(|| self.store.cluster_count())
    }

    pub fn graph_counts(&self) -> Option<(u64, u64)> {
        self.graph_state.as_ref().map(|graph| {
            let nodes = graph.num_nodes() as u64;
            let edges = (graph.num_same_as_edges() + graph.num_conflicts_with_edges()) as u64;
            (nodes, edges)
        })
    }

    pub fn store_metrics(&self) -> Option<StoreMetrics> {
        self.store.metrics()
    }

    /// Intern an attribute name into the store interner.
    pub fn intern_attr(&mut self, attr: &str) -> crate::model::AttrId {
        self.store.intern_attr(attr)
    }

    /// Intern a value string into the store interner.
    pub fn intern_value(&mut self, value: &str) -> crate::model::ValueId {
        self.store.intern_value(value)
    }

    /// Resolve an attribute ID back to its string label.
    pub fn resolve_attr(&self, attr: crate::model::AttrId) -> Option<String> {
        self.store.resolve_attr(attr)
    }

    /// Resolve a value ID back to its string label.
    pub fn resolve_value(&self, value: crate::model::ValueId) -> Option<String> {
        self.store.resolve_value(value)
    }

    /// Export the knowledge graph as a text summary.
    pub fn export_text_summary(
        &self,
        clusters: &dsu::Clusters,
        observations: &[conflicts::Observation],
    ) -> anyhow::Result<String> {
        utils::export_to_text_summary(self.store.as_ref(), clusters, observations)
    }

    /// Create a durable checkpoint of the underlying store, if supported.
    pub fn checkpoint(&self, path: &std::path::Path) -> anyhow::Result<()> {
        self.store.checkpoint(path)
    }

    /// Generate graph visualizations (DOT/PNG/SVG).
    pub fn generate_graph_visualizations(
        &self,
        clusters: &dsu::Clusters,
        observations: &[conflicts::Observation],
        base_filename: &str,
    ) -> anyhow::Result<()> {
        utils::generate_graph_visualizations(
            self.store.as_ref(),
            clusters,
            observations,
            &self.ontology,
            base_filename,
        )
    }

    /// Stream records and return the cluster assignment for each record.
    pub fn stream_records(
        &mut self,
        records: Vec<Record>,
    ) -> anyhow::Result<Vec<StreamedClusterAssignment>> {
        self.clear_conflict_cache();
        self.clear_query_stats();
        if self.streaming.is_none() {
            self.streaming = Some(linker::StreamingLinker::new(
                self.store.as_ref(),
                &self.ontology,
                &self.tuning,
            )?);
        }

        let mut assignments = Vec::with_capacity(records.len());
        let mut record_ids = Vec::with_capacity(records.len());
        let cluster_count = {
            let streaming = self.streaming.as_mut().unwrap();

            // Stage all records first (adds to cache for reading, defers DB write)
            for record in records {
                let (record_id, inserted) = self.store.stage_record_if_absent(record)?;
                let cluster_id = if inserted {
                    streaming.link_record(self.store.as_ref(), &self.ontology, record_id)?
                } else {
                    streaming.cluster_id_for(record_id)
                };
                assignments.push(StreamedClusterAssignment {
                    record_id,
                    cluster_id,
                });
                record_ids.push(record_id);
            }

            if self.tuning.deferred_reconciliation {
                streaming.reconcile_pending(self.store.as_ref(), &self.ontology)?;
                for assignment in &mut assignments {
                    assignment.cluster_id = streaming.cluster_id_for(assignment.record_id);
                }
            }

            streaming.cluster_count()
        };

        // Flush all staged records to DB in a single batch write
        let _ = self.store.flush_staged_records();

        // Batch write all cluster assignments at once
        let batch_assignments: Vec<_> = assignments
            .iter()
            .map(|a| (a.record_id, a.cluster_id))
            .collect();
        let _ = self.store.set_cluster_assignments_batch(&batch_assignments);
        let _ = self.store.set_cluster_count(cluster_count);
        self.invalidate_query_cache();
        Ok(assignments)
    }

    /// Stream records and return cluster assignments plus conflict observations.
    pub fn stream_records_with_conflicts(
        &mut self,
        records: Vec<Record>,
    ) -> anyhow::Result<Vec<StreamedConflictUpdate>> {
        self.clear_conflict_cache();
        self.clear_query_stats();
        if self.streaming.is_none() {
            self.streaming = Some(linker::StreamingLinker::new(
                self.store.as_ref(),
                &self.ontology,
                &self.tuning,
            )?);
        }

        let mut updates = Vec::with_capacity(records.len());
        let cluster_count = {
            let streaming = self.streaming.as_mut().unwrap();

            for record in records {
                let (record_id, inserted) = self.store.add_record_if_absent(record)?;
                let cluster_id = if inserted {
                    streaming.link_record(self.store.as_ref(), &self.ontology, record_id)?
                } else {
                    streaming.cluster_id_for(record_id)
                };
                let _ = self.store.set_cluster_assignment(record_id, cluster_id);
                let assignment = StreamedClusterAssignment {
                    record_id,
                    cluster_id,
                };
                let observations = if inserted {
                    let clusters = streaming
                        .clusters_with_conflict_splitting(self.store.as_ref(), &self.ontology)?;
                    conflicts::detect_conflicts_for_clusters(
                        self.store.as_ref(),
                        &clusters,
                        &self.ontology,
                        &[cluster_id],
                    )?
                } else {
                    Vec::new()
                };
                if inserted && !observations.is_empty() {
                    let summaries =
                        conflicts::summarize_conflicts(self.store.as_ref(), &observations);
                    let _ = self
                        .store
                        .set_cluster_conflict_summaries(cluster_id, &summaries);
                }
                updates.push(StreamedConflictUpdate {
                    assignment,
                    observations,
                });
            }

            streaming.cluster_count()
        };

        self.invalidate_query_cache();
        let _ = self.store.set_cluster_count(cluster_count);
        Ok(updates)
    }

    /// Stream a single record and return its cluster assignment plus conflicts.
    pub fn stream_record_with_conflicts(
        &mut self,
        record: Record,
    ) -> anyhow::Result<StreamedConflictUpdate> {
        let mut updates = self.stream_records_with_conflicts(vec![record])?;
        Ok(updates.remove(0))
    }

    /// Stream records and incrementally update the knowledge graph.
    pub fn stream_records_update_graph(
        &mut self,
        records: Vec<Record>,
    ) -> anyhow::Result<Vec<StreamedGraphUpdate>> {
        self.clear_conflict_cache();
        self.clear_query_stats();
        if self.streaming.is_none() {
            self.streaming = Some(linker::StreamingLinker::new(
                self.store.as_ref(),
                &self.ontology,
                &self.tuning,
            )?);
        }

        let graph_state = self
            .graph_state
            .get_or_insert_with(graph::IncrementalKnowledgeGraph::new);
        let mut updates = Vec::with_capacity(records.len());
        let cluster_count = {
            let streaming = self.streaming.as_mut().unwrap();

            for record in records {
                let (record_id, inserted) = self.store.add_record_if_absent(record)?;
                let cluster_id = if inserted {
                    streaming.link_record(self.store.as_ref(), &self.ontology, record_id)?
                } else {
                    streaming.cluster_id_for(record_id)
                };
                let _ = self.store.set_cluster_assignment(record_id, cluster_id);
                let assignment = StreamedClusterAssignment {
                    record_id,
                    cluster_id,
                };

                let observations = if inserted {
                    let clusters = streaming
                        .clusters_with_conflict_splitting(self.store.as_ref(), &self.ontology)?;
                    let observations = conflicts::detect_conflicts(
                        self.store.as_ref(),
                        &clusters,
                        &self.ontology,
                    )?;
                    let summaries =
                        conflicts::summarize_conflicts(self.store.as_ref(), &observations);
                    if let Err(err) = self.store.set_conflict_summaries(&summaries) {
                        eprintln!("Failed to persist conflict summaries: {err}");
                    }
                    *self.conflict_cache.lock().unwrap() = Some(summaries);

                    graph_state.update(
                        self.store.as_ref(),
                        &clusters,
                        &observations,
                        &self.ontology,
                    )?;
                    observations
                } else {
                    Vec::new()
                };
                let graph = graph_state.to_knowledge_graph();

                updates.push(StreamedGraphUpdate {
                    assignment,
                    observations,
                    graph,
                });
            }

            streaming.cluster_count()
        };

        self.invalidate_query_cache();
        let _ = self.store.set_cluster_count(cluster_count);
        Ok(updates)
    }

    /// Stream a single record and return its graph update.
    pub fn stream_record_update_graph(
        &mut self,
        record: Record,
    ) -> anyhow::Result<StreamedGraphUpdate> {
        let mut updates = self.stream_records_update_graph(vec![record])?;
        Ok(updates.remove(0))
    }

    /// Query master entities for the given descriptors and time interval.
    pub fn query_master_entities(
        &self,
        descriptors: &[query::QueryDescriptor],
        interval: Interval,
    ) -> anyhow::Result<query::QueryOutcome> {
        let mut cache_guard = self.query_cache.lock().expect("query cache lock");
        if cache_guard.is_none() {
            let clusters = self.build_clusters()?;
            let golden = query::build_golden_cache(self.store.as_ref(), &clusters);
            let cluster_keys =
                query::build_cluster_key_cache(self.store.as_ref(), &clusters, &self.ontology);
            let record_to_cluster = query::build_record_to_cluster_map(&clusters);
            *cache_guard = Some(QueryCache {
                clusters,
                golden,
                cluster_keys,
                record_to_cluster,
            });
        }
        let cache = cache_guard.as_ref().expect("cache");
        let mut stats_guard = self.query_stats.lock().expect("query stats lock");
        query::query_master_entities_with_cache_selective(
            self.store.as_ref(),
            &cache.clusters,
            descriptors,
            interval,
            &cache.golden,
            &cache.cluster_keys,
            &cache.record_to_cluster,
            &mut stats_guard,
        )
    }

    fn invalidate_query_cache(&self) {
        if let Ok(mut guard) = self.query_cache.lock() {
            *guard = None;
        }
    }

    /// Summarize conflicts into stable, record-identity based descriptors.
    pub fn summarize_conflicts(
        &self,
        observations: &[conflicts::Observation],
    ) -> Vec<conflicts::ConflictSummary> {
        conflicts::summarize_conflicts(self.store.as_ref(), observations)
    }

    pub fn cached_conflict_summaries(&self) -> Option<Vec<conflicts::ConflictSummary>> {
        self.conflict_cache.lock().unwrap().clone()
    }

    pub fn load_conflict_summaries(&self) -> Option<Vec<conflicts::ConflictSummary>> {
        self.store.load_conflict_summaries()
    }

    pub fn conflict_summary_count(&self) -> Option<usize> {
        self.conflict_cache
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|summaries| summaries.len()))
            .or_else(|| self.store.conflict_summary_count())
    }

    pub fn set_conflict_cache(&mut self, summaries: Vec<conflicts::ConflictSummary>) {
        if let Err(err) = self.store.set_conflict_summaries(&summaries) {
            eprintln!("Failed to persist conflict summaries: {err}");
        }
        *self.conflict_cache.lock().unwrap() = Some(summaries);
    }

    fn clear_conflict_cache(&self) {
        *self.conflict_cache.lock().unwrap() = None;
    }

    fn clear_query_stats(&self) {
        *self.query_stats.lock().unwrap() = query::QuerySelectivityStats::default();
    }
}
