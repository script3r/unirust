//! # Unirust
//! 
//! A general-purpose, temporal-first entity mastering and conflict-resolution engine.
//! 
//! This library provides precise temporal modeling, entity resolution, and conflict detection
//! with strong guarantees about temporal correctness and auditability.

pub mod temporal;
pub mod model;
pub mod index;
pub mod dsu;
pub mod linker;
pub mod conflicts;
pub mod graph;
pub mod store;
pub mod ontology;
pub mod utils;

// Re-export main types for convenience
pub use model::{Record, RecordId, ClusterId, RecordIdentity, Descriptor};
pub use temporal::Interval;
pub use store::Store;
pub use ontology::Ontology;

/// Main API for entity mastering
pub struct Unirust {
    store: Store,
    ontology: Ontology,
}

impl Unirust {
    /// Create a new Unirust instance
    pub fn new(ontology: Ontology) -> Self {
        Self {
            store: Store::new(),
            ontology,
        }
    }

    /// Ingest records into the store
    pub fn ingest(&mut self, records: Vec<Record>) -> anyhow::Result<()> {
        self.store.add_records(records)?;
        Ok(())
    }

    /// Build clusters from the current store
    pub fn build_clusters(&self) -> anyhow::Result<dsu::Clusters> {
        linker::build_clusters(&self.store, &self.ontology)
    }

    /// Detect conflicts in the given clusters
    pub fn detect_conflicts(&self, clusters: &dsu::Clusters) -> anyhow::Result<Vec<conflicts::Observation>> {
        conflicts::detect_conflicts(&self.store, clusters, &self.ontology)
    }

    /// Export the knowledge graph
    pub fn export_graph(&self, clusters: &dsu::Clusters, observations: &[conflicts::Observation]) -> anyhow::Result<graph::KnowledgeGraph> {
        graph::export_graph(&self.store, clusters, observations, &self.ontology)
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_basic_functionality() {
        // This will be expanded with comprehensive tests
        assert!(true);
    }
}