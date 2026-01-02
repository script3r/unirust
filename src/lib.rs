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
    tuning: StreamingTuning,
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
            tuning,
        }
    }

    /// Ingest records into the store
    pub fn ingest(&mut self, records: Vec<Record>) -> anyhow::Result<()> {
        self.store.add_records(records)?;
        self.streaming = None;
        self.graph_state = None;
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
    pub fn get_record(&self, id: RecordId) -> Option<&Record> {
        self.store.get_record(id)
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
            let observations =
                conflicts::detect_conflicts(self.store.as_ref(), &clusters, &self.ontology)?;
            updates.push(StreamedConflictUpdate {
                assignment,
                observations,
            });
        }

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
        let clusters = self.build_clusters()?;
        query::query_master_entities(
            self.store.as_ref(),
            &clusters,
            &self.ontology,
            descriptors,
            interval,
        )
    }

    /// Summarize conflicts into stable, record-identity based descriptors.
    pub fn summarize_conflicts(
        &self,
        observations: &[conflicts::Observation],
    ) -> Vec<conflicts::ConflictSummary> {
        conflicts::summarize_conflicts(self.store.as_ref(), observations)
    }
}
