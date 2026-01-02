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
pub use query::{QueryConflict, QueryDescriptor, QueryDescriptorOverlap, QueryMatch, QueryOutcome};
pub use persistence::PersistentStore;
pub use store::{RecordStore, Store};
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
    tuning: StreamingTuning,
}

#[derive(Clone)]
struct QueryCache {
    clusters: dsu::Clusters,
    golden: std::collections::HashMap<ClusterId, Vec<graph::GoldenDescriptor>>,
    cluster_keys: std::collections::HashMap<ClusterId, graph::ClusterKey>,
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
            tuning: StreamingTuning::default(),
        }
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
            tuning,
        }
    }

    /// Ingest records into the store
    pub fn ingest(&mut self, records: Vec<Record>) -> anyhow::Result<()> {
        self.store.add_records(records)?;
        self.streaming = None;
        self.graph_state = None;
        self.invalidate_query_cache();
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

    /// Intern an attribute name into the store interner.
    pub fn intern_attr(&mut self, attr: &str) -> crate::model::AttrId {
        self.store.interner_mut().intern_attr(attr)
    }

    /// Intern a value string into the store interner.
    pub fn intern_value(&mut self, value: &str) -> crate::model::ValueId {
        self.store.interner_mut().intern_value(value)
    }

    /// Resolve an attribute ID back to its string label.
    pub fn resolve_attr(&self, attr: crate::model::AttrId) -> Option<String> {
        self.store.interner().get_attr(attr).cloned()
    }

    /// Resolve a value ID back to its string label.
    pub fn resolve_value(&self, value: crate::model::ValueId) -> Option<String> {
        self.store.interner().get_value(value).cloned()
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
        if self.streaming.is_none() {
            self.streaming = Some(linker::StreamingLinker::new(
                self.store.as_ref(),
                &self.ontology,
                &self.tuning,
            )?);
        }

        let streaming = self.streaming.as_mut().unwrap();
        let mut assignments = Vec::with_capacity(records.len());
        let mut record_ids = Vec::with_capacity(records.len());

        for record in records {
            let record_id = self.store.add_record(record)?;
            let cluster_id =
                streaming.link_record(self.store.as_ref(), &self.ontology, record_id)?;
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

        self.invalidate_query_cache();
        Ok(assignments)
    }

    /// Stream records and return cluster assignments plus conflict observations.
    pub fn stream_records_with_conflicts(
        &mut self,
        records: Vec<Record>,
    ) -> anyhow::Result<Vec<StreamedConflictUpdate>> {
        if self.streaming.is_none() {
            self.streaming = Some(linker::StreamingLinker::new(
                self.store.as_ref(),
                &self.ontology,
                &self.tuning,
            )?);
        }

        let streaming = self.streaming.as_mut().unwrap();
        let mut updates = Vec::with_capacity(records.len());

        for record in records {
            let record_id = self.store.add_record(record)?;
            let cluster_id =
                streaming.link_record(self.store.as_ref(), &self.ontology, record_id)?;
            let assignment = StreamedClusterAssignment {
                record_id,
                cluster_id,
            };
            let clusters =
                streaming.clusters_with_conflict_splitting(self.store.as_ref(), &self.ontology)?;
            let observations = conflicts::detect_conflicts_for_clusters(
                self.store.as_ref(),
                &clusters,
                &self.ontology,
                &[cluster_id],
            )?;
            updates.push(StreamedConflictUpdate {
                assignment,
                observations,
            });
        }

        self.invalidate_query_cache();
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
        if self.streaming.is_none() {
            self.streaming = Some(linker::StreamingLinker::new(
                self.store.as_ref(),
                &self.ontology,
                &self.tuning,
            )?);
        }

        let streaming = self.streaming.as_mut().unwrap();
        let graph_state = self
            .graph_state
            .get_or_insert_with(graph::IncrementalKnowledgeGraph::new);
        let mut updates = Vec::with_capacity(records.len());

        for record in records {
            let record_id = self.store.add_record(record)?;
            let cluster_id =
                streaming.link_record(self.store.as_ref(), &self.ontology, record_id)?;
            let assignment = StreamedClusterAssignment {
                record_id,
                cluster_id,
            };

            let clusters =
                streaming.clusters_with_conflict_splitting(self.store.as_ref(), &self.ontology)?;
            let observations =
                conflicts::detect_conflicts(self.store.as_ref(), &clusters, &self.ontology)?;

            graph_state.update(
                self.store.as_ref(),
                &clusters,
                &observations,
                &self.ontology,
            )?;
            let graph = graph_state.to_knowledge_graph();

            updates.push(StreamedGraphUpdate {
                assignment,
                observations,
                graph,
            });
        }

        self.invalidate_query_cache();
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
            let cluster_keys = query::build_cluster_key_cache(
                self.store.as_ref(),
                &clusters,
                &self.ontology,
            );
            *cache_guard = Some(QueryCache {
                clusters,
                golden,
                cluster_keys,
            });
        }
        let cache = cache_guard.as_ref().expect("cache");
        query::query_master_entities_with_cache(
            self.store.as_ref(),
            &cache.clusters,
            descriptors,
            interval,
            &cache.golden,
            &cache.cluster_keys,
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
}
