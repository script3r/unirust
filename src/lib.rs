//! # Unirust
//!
//! A simple, fast entity resolution engine with temporal awareness.
//!
//! ## Quick Start
//!
//! ```ignore
//! use unirust::{Unirust, Ontology, Record};
//!
//! // Create engine with ontology (matching rules)
//! let ontology = Ontology::new();
//! let mut engine = Unirust::new(ontology);
//!
//! // Ingest records - returns assignments and detected conflicts
//! let result = engine.ingest(records)?;
//! println!("Assigned {} records to {} clusters", result.assignments.len(), result.cluster_count);
//!
//! // Query master entities
//! let matches = engine.query(&[QueryDescriptor { attr, value }], interval)?;
//!
//! // Export knowledge graph
//! let graph = engine.graph()?;
//! ```
//!
//! ## API Design
//!
//! The API is intentionally minimal:
//! - `ingest()` - Add records, get cluster assignments + conflicts
//! - `query()` - Find master entities by attributes
//! - `clusters()` - Get all cluster assignments
//! - `graph()` - Export knowledge graph
//! - `checkpoint()` - Persist state to disk
//! - `stats()` - Get metrics and statistics

pub mod coalescer;
pub mod config;
pub mod conflicts;
pub mod distributed;
pub mod dsu;
pub mod graph;
pub mod hft;
pub mod index;
pub mod linker;
pub mod model;
pub mod ontology;
pub mod partitioned;
pub mod persistence;
pub mod profile;
pub mod query;
pub mod sharding;
pub mod store;
pub mod temporal;
pub mod utils;

/// Test support utilities (also used by benchmarks and integration tests)
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod test_support;

use tracing::{error, warn};

// ============================================================================
// Public API - Core Types
// ============================================================================

// Data model
pub use model::{Descriptor, Record, RecordId, RecordIdentity};
pub use temporal::Interval;

// Configuration
pub use config::{StreamingTuning, TuningProfile};
pub use ontology::Ontology;

// Results
pub use conflicts::{ConflictSummary, Observation as ConflictObservation};
pub use dsu::Clusters;
pub use graph::KnowledgeGraph;
pub use query::{QueryDescriptor, QueryMatch, QueryOutcome};

// Storage (for custom backends)
pub use persistence::PersistentStore;
pub use store::{RecordStore, Store};

// ============================================================================
// Public API - Result Types
// ============================================================================

/// Result of ingesting records into the engine.
#[derive(Debug, Clone)]
pub struct IngestResult {
    /// Cluster assignment for each ingested record
    pub assignments: Vec<ClusterAssignment>,
    /// Conflicts detected during ingestion
    pub conflicts: Vec<ConflictSummary>,
    /// Total number of clusters after ingestion
    pub cluster_count: usize,
}

/// A single record's cluster assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterAssignment {
    pub record_id: RecordId,
    pub cluster_id: model::ClusterId,
}

/// Engine statistics and metrics.
#[derive(Debug, Clone, Default)]
pub struct Stats {
    /// Number of records in the store
    pub record_count: usize,
    /// Number of clusters
    pub cluster_count: usize,
    /// Records linked per second (recent average)
    pub records_per_second: f64,
    /// Number of merges performed
    pub merges_performed: u64,
    /// Number of conflicts detected
    pub conflicts_detected: u64,
    /// Number of hot keys encountered
    pub hot_key_exits: u64,
}

// ============================================================================
// Legacy types (deprecated, use new API)
// ============================================================================

/// Assignment result for streaming clustering.
#[deprecated(since = "0.2.0", note = "Use ClusterAssignment instead")]
pub type StreamedClusterAssignment = ClusterAssignment;

/// Streaming update with conflict observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamedConflictUpdate {
    pub assignment: ClusterAssignment,
    pub observations: Vec<conflicts::Observation>,
}

/// Streaming update with graph output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamedGraphUpdate {
    pub assignment: ClusterAssignment,
    pub observations: Vec<conflicts::Observation>,
    pub graph: graph::KnowledgeGraph,
}

// Internal imports for implementation
use linker::{LinkerMetrics, LinkerMetricsSnapshot};
use model::GlobalClusterId;
use persistence::LinkerStatePersistence;
use store::StoreMetrics;

// ============================================================================
// Advanced/Internal Re-exports (for power users)
// ============================================================================

/// Advanced types for custom backends and distributed deployments.
pub mod advanced {
    pub use crate::coalescer::{
        AsyncBatchCoalescer, BatchCoalescer, CoalescerConfig, CoalescerStats,
    };
    pub use crate::config::{ConflictAlgorithmChoice, ConflictTuning, LinkerStateConfig};
    pub use crate::distributed::{ClusterLocality, ClusterLocalityIndex};
    pub use crate::dsu::{
        Cluster, DsuBackend, MergeResult, PersistentDSUConfig, PersistentDSUStats,
        PersistentTemporalDSU, TemporalConflict, TemporalDSU, TemporalGuard,
    };
    pub use crate::hft::{
        AlignedCounter, AlignedMetrics, AsyncWal, AsyncWalConfig, AtomicDSU, AtomicMergeResult,
        BatchAggregator, IngestJob, IngestQueue, MetricsSnapshot, QueueStats, RecordSlice,
        SimdHasher, WalError, WalTicket, ZeroCopyBatch,
    };
    pub use crate::index::{IndexBackend, TierConfig, TieredIdentityKeyIndex, TieredIndexStats};
    pub use crate::linker::{LinkerMetrics, LinkerMetricsSnapshot};
    pub use crate::model::{ClusterId, GlobalClusterId};
    pub use crate::partitioned::{
        CrossPartitionMerge, Partition, PartitionConfig, PartitionIngestResult, PartitionStats,
        PartitionedUnirust, PartitionedUnirustHandle,
    };
    pub use crate::persistence::LinkerStatePersistence;
    pub use crate::query::{QueryConflict, QueryDescriptorOverlap};
    pub use crate::sharding::{
        BloomFilter, BoundaryEntry, BoundaryMetadata, ClusterBoundaryIndex, IdentityKeySignature,
        IncrementalReconciler, ReconciliationResult,
    };
    pub use crate::store::StoreMetrics;
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
    golden: std::collections::HashMap<model::ClusterId, Vec<graph::GoldenDescriptor>>,
    cluster_keys: std::collections::HashMap<model::ClusterId, graph::ClusterKey>,
    record_to_cluster: std::collections::HashMap<RecordId, model::ClusterId>,
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

    /// Create a streaming linker with appropriate backends based on tuning.
    /// Uses persistent DSU and/or tiered index when tuning requires it and store supports it.
    fn create_streaming_linker(&self) -> anyhow::Result<linker::StreamingLinker> {
        let db = self.store.shared_db();

        // Determine which backends to use based on tuning and store capabilities
        let use_persistent_dsu = self.tuning.use_persistent_dsu && db.is_some();
        let use_tiered_index = self.tuning.use_tiered_index && db.is_some();

        if use_persistent_dsu || use_tiered_index {
            let db = db.unwrap();
            let dsu = if use_persistent_dsu {
                let dsu_config = self.tuning.dsu_config.clone().unwrap_or_default();
                dsu::DsuBackend::persistent(db.clone(), dsu_config)?
            } else {
                dsu::DsuBackend::in_memory()
            };

            let index = if use_tiered_index {
                let tier_config = self.tuning.tier_config.clone().unwrap_or_default();
                index::IndexBackend::tiered(tier_config, Some(db))
            } else {
                index::IndexBackend::in_memory()
            };

            linker::StreamingLinker::new_with_backends(
                self.store.as_ref(),
                &self.ontology,
                &self.tuning,
                self.tuning.shard_id,
                dsu,
                index,
            )
        } else {
            linker::StreamingLinker::new(self.store.as_ref(), &self.ontology, &self.tuning)
        }
    }

    // ========================================================================
    // CORE API - The main methods most users need
    // ========================================================================

    /// Ingest records and return cluster assignments with detected conflicts.
    ///
    /// This is the primary method for adding data to the engine. It:
    /// 1. Adds records to the store
    /// 2. Links them into clusters based on identity keys
    /// 3. Detects conflicts within clusters
    ///
    /// # Example
    /// ```ignore
    /// let result = engine.ingest(records)?;
    /// for assignment in result.assignments {
    ///     println!("Record {} -> Cluster {}", assignment.record_id.0, assignment.cluster_id.0);
    /// }
    /// ```
    pub fn ingest(&mut self, records: Vec<Record>) -> anyhow::Result<IngestResult> {
        self.clear_conflict_cache();
        self.clear_query_stats();
        if self.streaming.is_none() {
            self.streaming = Some(self.create_streaming_linker()?);
        }

        // Stage all records and collect their IDs
        let mut staged_info: Vec<(RecordId, bool)> = Vec::with_capacity(records.len());
        for record in records {
            let (record_id, inserted) = self.store.stage_record_if_absent(record)?;
            staged_info.push((record_id, inserted));
        }

        // Link records (parallel for large batches, sequential for small)
        let mut assignments = Vec::with_capacity(staged_info.len());
        let cluster_count;
        let observations;
        {
            let streaming = self
                .streaming
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("Streaming not initialized"))?;

            // Get record references for parallel processing
            let records_to_link: Vec<&Record> = staged_info
                .iter()
                .filter(|(_, inserted)| *inserted)
                .filter_map(|(id, _)| self.store.get_record_ref(*id))
                .collect();

            // Use parallel batch linking for large batches (>= 100 records)
            if records_to_link.len() >= 100 {
                let cluster_ids =
                    streaming.link_records_batch_parallel(&records_to_link, &self.ontology)?;
                let mut cluster_id_iter = cluster_ids.into_iter();

                for (record_id, inserted) in &staged_info {
                    let cluster_id = if *inserted {
                        cluster_id_iter
                            .next()
                            .unwrap_or_else(|| streaming.cluster_id_for(*record_id))
                    } else {
                        streaming.cluster_id_for(*record_id)
                    };
                    assignments.push(ClusterAssignment {
                        record_id: *record_id,
                        cluster_id,
                    });
                }
            } else {
                // Sequential for small batches
                for (record_id, inserted) in &staged_info {
                    let cluster_id = if *inserted {
                        streaming.link_record(self.store.as_ref(), &self.ontology, *record_id)?
                    } else {
                        streaming.cluster_id_for(*record_id)
                    };
                    assignments.push(ClusterAssignment {
                        record_id: *record_id,
                        cluster_id,
                    });
                }
            }

            if self.tuning.deferred_reconciliation {
                streaming.reconcile_pending(self.store.as_ref(), &self.ontology)?;
                for assignment in &mut assignments {
                    assignment.cluster_id = streaming.cluster_id_for(assignment.record_id);
                }
            }

            cluster_count = streaming.cluster_count();

            // Detect conflicts
            let clusters =
                streaming.clusters_with_conflict_splitting(self.store.as_ref(), &self.ontology)?;
            let affected_cluster_ids: Vec<_> = assignments.iter().map(|a| a.cluster_id).collect();
            observations = conflicts::detect_conflicts_for_clusters(
                self.store.as_ref(),
                &clusters,
                &self.ontology,
                &affected_cluster_ids,
            )?;
        }

        // Flush all staged records to DB
        if let Err(e) = self.store.flush_staged_records() {
            error!(error = %e, "Failed to flush staged records");
        }

        // Batch write all cluster assignments
        let batch_assignments: Vec<_> = assignments
            .iter()
            .map(|a| (a.record_id, a.cluster_id))
            .collect();
        if let Err(e) = self.store.set_cluster_assignments_batch(&batch_assignments) {
            error!(error = %e, "Failed to persist cluster assignments");
        }
        if let Err(e) = self.store.set_cluster_count(cluster_count) {
            warn!(error = %e, "Failed to persist cluster count");
        }

        // Summarize conflicts
        let conflicts = conflicts::summarize_conflicts(self.store.as_ref(), &observations);

        self.invalidate_query_cache();
        Ok(IngestResult {
            assignments,
            conflicts,
            cluster_count,
        })
    }

    /// Query for master entities matching the given descriptors in a time interval.
    ///
    /// Returns matching clusters or indicates if there's a conflict (multiple
    /// clusters claim the same identity).
    pub fn query(
        &self,
        descriptors: &[query::QueryDescriptor],
        interval: Interval,
    ) -> anyhow::Result<query::QueryOutcome> {
        self.query_master_entities(descriptors, interval)
    }

    /// Get all cluster assignments.
    pub fn clusters(&mut self) -> anyhow::Result<Clusters> {
        if let Some(streaming) = &mut self.streaming {
            Ok(streaming.clusters_with_conflict_splitting(self.store.as_ref(), &self.ontology)?)
        } else {
            linker::build_clusters(self.store.as_ref(), &self.ontology)
        }
    }

    /// Export the knowledge graph.
    pub fn graph(&mut self) -> anyhow::Result<KnowledgeGraph> {
        let clusters = self.clusters()?;
        let observations =
            conflicts::detect_conflicts(self.store.as_ref(), &clusters, &self.ontology)?;
        graph::export_graph(
            self.store.as_ref(),
            &clusters,
            &observations,
            &self.ontology,
        )
    }

    /// Persist all state to disk (for persistent stores).
    ///
    /// This flushes the linker state and any staged records.
    pub fn checkpoint(&mut self) -> anyhow::Result<()> {
        // Flush linker state if available
        if let Some(streaming) = &self.streaming {
            if let Some(db) = self.store.shared_db() {
                let persistence = LinkerStatePersistence::new(&db);
                streaming.flush_state(&persistence)?;
            }
        }
        // Flush any staged records
        self.store.flush_staged_records()?;
        Ok(())
    }

    /// Create a durable checkpoint at a specific path (advanced).
    ///
    /// For most cases, use `checkpoint()` instead.
    #[doc(hidden)]
    pub fn checkpoint_to_path(&self, path: &std::path::Path) -> anyhow::Result<()> {
        self.store.checkpoint(path)
    }

    /// Get engine statistics and metrics.
    pub fn stats(&self) -> Stats {
        let mut stats = Stats {
            record_count: self.store.len(),
            cluster_count: self
                .streaming
                .as_ref()
                .map(|s| s.cluster_count())
                .unwrap_or(0),
            ..Default::default()
        };

        if let Some(streaming) = &self.streaming {
            let snapshot = streaming.metrics().snapshot();
            stats.merges_performed = snapshot.merges_performed;
            stats.conflicts_detected = snapshot.conflicts_detected;
            stats.hot_key_exits = snapshot.hot_key_exits;
            stats.records_per_second = if snapshot.records_linked > 0 {
                snapshot.records_linked as f64 // Approximation
            } else {
                0.0
            };
        }

        stats
    }

    // ========================================================================
    // ADDITIONAL PUBLIC METHODS
    // ========================================================================

    /// Get a record by ID.
    pub fn get_record(&self, id: RecordId) -> Option<Record> {
        self.store.get_record(id)
    }

    /// Access the underlying store (for advanced use cases).
    pub fn store(&self) -> &dyn RecordStore {
        self.store.as_ref()
    }

    /// Mutable access to the underlying store.
    pub fn store_mut(&mut self) -> &mut dyn RecordStore {
        self.store.as_mut()
    }

    // ========================================================================
    // INTERNAL/LEGACY METHODS (kept for compatibility)
    // ========================================================================

    /// Build clusters from the current store (legacy - use `clusters()` instead)
    #[doc(hidden)]
    pub fn build_clusters(&self) -> anyhow::Result<dsu::Clusters> {
        linker::build_clusters(self.store.as_ref(), &self.ontology)
    }

    /// Detect conflicts in the given clusters (legacy - use `ingest()` which does this automatically)
    #[doc(hidden)]
    pub fn detect_conflicts(
        &self,
        clusters: &dsu::Clusters,
    ) -> anyhow::Result<Vec<conflicts::Observation>> {
        conflicts::detect_conflicts(self.store.as_ref(), clusters, &self.ontology)
    }

    /// Export the knowledge graph (legacy - use `graph()` instead)
    #[doc(hidden)]
    pub fn export_graph(
        &self,
        clusters: &dsu::Clusters,
        observations: &[conflicts::Observation],
    ) -> anyhow::Result<graph::KnowledgeGraph> {
        graph::export_graph(self.store.as_ref(), clusters, observations, &self.ontology)
    }

    /// Export the knowledge graph to DOT format.
    #[doc(hidden)]
    pub fn export_dot(
        &self,
        clusters: &dsu::Clusters,
        observations: &[conflicts::Observation],
    ) -> anyhow::Result<String> {
        utils::export_to_dot(self.store.as_ref(), clusters, observations, &self.ontology)
    }

    /// Get records in an ID range [start, end), limited to max_results.
    #[doc(hidden)]
    pub fn records_in_id_range(
        &self,
        start: RecordId,
        end: RecordId,
        max_results: usize,
    ) -> Vec<Record> {
        self.store.records_in_id_range(start, end, max_results)
    }

    /// Get min/max record IDs if any records exist.
    #[doc(hidden)]
    pub fn record_id_bounds(&self) -> Option<(RecordId, RecordId)> {
        self.store.record_id_bounds()
    }

    /// Get the number of records in the store.
    #[doc(hidden)]
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
    ///
    /// Uses parallel extraction for batches >= 100 records, providing 20-40% better
    /// scaling behavior at large dataset sizes. Falls back to sequential processing
    /// for smaller batches where parallelism overhead isn't worth it.
    pub fn stream_records(
        &mut self,
        records: Vec<Record>,
    ) -> anyhow::Result<Vec<ClusterAssignment>> {
        self.clear_conflict_cache();
        self.clear_query_stats();
        if self.streaming.is_none() {
            self.streaming = Some(self.create_streaming_linker()?);
        }

        // Stage all records and collect their IDs
        let mut staged_info: Vec<(RecordId, bool)> = Vec::with_capacity(records.len());
        for record in records {
            let (record_id, inserted) = self.store.stage_record_if_absent(record)?;
            staged_info.push((record_id, inserted));
        }

        // Link records (parallel for large batches, sequential for small)
        let mut assignments = Vec::with_capacity(staged_info.len());
        let cluster_count = {
            let streaming = self
                .streaming
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("Streaming not initialized"))?;

            // Get record references for parallel processing
            let records_to_link: Vec<&Record> = staged_info
                .iter()
                .filter(|(_, inserted)| *inserted)
                .filter_map(|(id, _)| self.store.get_record_ref(*id))
                .collect();

            // Use parallel batch linking for large batches (>= 100 records)
            if records_to_link.len() >= 100 {
                let cluster_ids =
                    streaming.link_records_batch_parallel(&records_to_link, &self.ontology)?;
                let mut cluster_id_iter = cluster_ids.into_iter();

                for (record_id, inserted) in &staged_info {
                    let cluster_id = if *inserted {
                        cluster_id_iter
                            .next()
                            .unwrap_or_else(|| streaming.cluster_id_for(*record_id))
                    } else {
                        streaming.cluster_id_for(*record_id)
                    };
                    assignments.push(ClusterAssignment {
                        record_id: *record_id,
                        cluster_id,
                    });
                }
            } else {
                // Sequential for small batches
                for (record_id, inserted) in &staged_info {
                    let cluster_id = if *inserted {
                        streaming.link_record(self.store.as_ref(), &self.ontology, *record_id)?
                    } else {
                        streaming.cluster_id_for(*record_id)
                    };
                    assignments.push(ClusterAssignment {
                        record_id: *record_id,
                        cluster_id,
                    });
                }
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
        if let Err(e) = self.store.flush_staged_records() {
            error!(error = %e, "Failed to flush staged records");
        }

        // Batch write all cluster assignments
        let batch_assignments: Vec<_> = assignments
            .iter()
            .map(|a| (a.record_id, a.cluster_id))
            .collect();
        if let Err(e) = self.store.set_cluster_assignments_batch(&batch_assignments) {
            error!(error = %e, "Failed to persist cluster assignments");
        }
        if let Err(e) = self.store.set_cluster_count(cluster_count) {
            warn!(error = %e, "Failed to persist cluster count");
        }
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
            self.streaming = Some(self.create_streaming_linker()?);
        }

        let mut updates = Vec::with_capacity(records.len());
        let cluster_count = {
            let streaming = self.streaming.as_mut().ok_or_else(|| {
                anyhow::anyhow!("Streaming not initialized - call enable_streaming() first")
            })?;

            for record in records {
                let (record_id, inserted) = self.store.add_record_if_absent(record)?;
                let cluster_id = if inserted {
                    streaming.link_record(self.store.as_ref(), &self.ontology, record_id)?
                } else {
                    streaming.cluster_id_for(record_id)
                };
                if let Err(e) = self.store.set_cluster_assignment(record_id, cluster_id) {
                    error!(record_id = ?record_id, cluster_id = ?cluster_id, error = %e, "Failed to persist cluster assignment");
                }
                let assignment = ClusterAssignment {
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
                    if let Err(e) = self
                        .store
                        .set_cluster_conflict_summaries(cluster_id, &summaries)
                    {
                        warn!(cluster_id = ?cluster_id, error = %e, "Failed to persist cluster conflict summaries");
                    }
                }
                updates.push(StreamedConflictUpdate {
                    assignment,
                    observations,
                });
            }

            streaming.cluster_count()
        };

        self.invalidate_query_cache();
        if let Err(e) = self.store.set_cluster_count(cluster_count) {
            warn!(cluster_count = cluster_count, error = %e, "Failed to persist cluster count");
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
        self.clear_conflict_cache();
        self.clear_query_stats();
        if self.streaming.is_none() {
            self.streaming = Some(self.create_streaming_linker()?);
        }

        let graph_state = self
            .graph_state
            .get_or_insert_with(graph::IncrementalKnowledgeGraph::new);
        // Cache the initial graph to avoid rebuilding it 200 times
        let initial_graph = graph_state.to_knowledge_graph();
        let mut updates = Vec::with_capacity(records.len());
        let mut assignments_batch = Vec::with_capacity(records.len());
        let mut pending_graph_update = false;
        let cluster_count = {
            let streaming = self.streaming.as_mut().ok_or_else(|| {
                anyhow::anyhow!("Streaming not initialized - call enable_streaming() first")
            })?;

            for record in records {
                // Use staging to defer DB writes
                let (record_id, inserted) = self.store.stage_record_if_absent(record)?;
                let cluster_id = if inserted {
                    streaming.link_record(self.store.as_ref(), &self.ontology, record_id)?
                } else {
                    streaming.cluster_id_for(record_id)
                };
                assignments_batch.push((record_id, cluster_id));
                let assignment = ClusterAssignment {
                    record_id,
                    cluster_id,
                };

                // Mark that we need a graph update, but defer the expensive computation
                if inserted {
                    pending_graph_update = true;
                }

                // Use cached graph for intermediate updates
                updates.push(StreamedGraphUpdate {
                    assignment,
                    observations: Vec::new(),
                    graph: initial_graph.clone(),
                });
            }

            streaming.cluster_count()
        };

        // Perform graph update once at the end if any records were inserted
        if pending_graph_update {
            if let Some(streaming) = self.streaming.as_mut() {
                let clusters = streaming.clusters();
                let observations =
                    conflicts::detect_conflicts(self.store.as_ref(), &clusters, &self.ontology)?;
                graph_state.update(
                    self.store.as_ref(),
                    &clusters,
                    &observations,
                    &self.ontology,
                )?;
                let final_graph = graph_state.to_knowledge_graph();
                // Update all entries with final graph state
                for update in &mut updates {
                    update.graph = final_graph.clone();
                }
                // Update last entry with observations
                if let Some(last) = updates.last_mut() {
                    last.observations = observations;
                }
            }
        }

        // Flush all staged records in a single batch write
        if let Err(e) = self.store.flush_staged_records() {
            error!(error = %e, "Failed to flush staged records - data may be stale");
        }

        // Batch write all cluster assignments
        if let Err(e) = self.store.set_cluster_assignments_batch(&assignments_batch) {
            error!(error = %e, "Failed to persist cluster assignments batch");
        }

        // Final conflict detection and summary (once at end)
        if let Some(streaming) = self.streaming.as_mut() {
            let clusters = streaming.clusters();
            let observations =
                conflicts::detect_conflicts(self.store.as_ref(), &clusters, &self.ontology)?;
            let summaries = conflicts::summarize_conflicts(self.store.as_ref(), &observations);
            if let Err(e) = self.store.set_conflict_summaries(&summaries) {
                warn!(error = %e, "Failed to persist conflict summaries");
            }
            *self.conflict_cache.lock().unwrap() = Some(summaries);
        }

        self.invalidate_query_cache();
        if let Err(e) = self.store.set_cluster_count(cluster_count) {
            warn!(cluster_count = cluster_count, error = %e, "Failed to persist cluster count");
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

    /// Export the current boundary index from the streaming linker.
    /// Returns None if streaming has not been initialized.
    pub fn export_boundary_index(&self) -> Option<sharding::ClusterBoundaryIndex> {
        self.streaming.as_ref().map(|s| s.export_boundary_index())
    }

    /// Drain boundary signatures from the streaming linker.
    /// Returns an empty Vec if streaming has not been initialized.
    pub fn drain_boundaries(
        &mut self,
    ) -> Vec<(sharding::IdentityKeySignature, GlobalClusterId, Interval)> {
        self.streaming
            .as_mut()
            .map(|s| s.drain_boundaries())
            .unwrap_or_default()
    }

    /// Get the count of accumulated boundary signatures.
    pub fn boundary_count(&self) -> usize {
        self.streaming
            .as_ref()
            .map(|s| s.boundary_count())
            .unwrap_or(0)
    }

    /// Apply a cross-shard cluster merge.
    /// Records that records in `secondary` cluster should now be considered part of `primary`.
    /// Returns the number of affected records.
    pub fn apply_cross_shard_merge(
        &mut self,
        primary: GlobalClusterId,
        secondary: GlobalClusterId,
    ) -> anyhow::Result<usize> {
        // Initialize streaming linker if not already done
        if self.streaming.is_none() {
            self.streaming = Some(self.create_streaming_linker()?);
        }

        let streaming = self.streaming.as_mut().ok_or_else(|| {
            anyhow::anyhow!("Streaming not initialized - call enable_streaming() first")
        })?;
        Ok(streaming.apply_cross_shard_merge(primary, secondary))
    }

    /// Get the number of cross-shard merge mappings tracked.
    pub fn cross_shard_merge_count(&self) -> usize {
        self.streaming
            .as_ref()
            .map(|s| s.cross_shard_merge_count())
            .unwrap_or(0)
    }

    /// Get a reference to the linker metrics for observability.
    /// Returns None if streaming is not initialized.
    pub fn linker_metrics(&self) -> Option<&LinkerMetrics> {
        self.streaming.as_ref().map(|s| s.metrics())
    }

    /// Get a snapshot of current linker metrics values.
    /// Returns default (zero) values if streaming is not initialized.
    pub fn linker_metrics_snapshot(&self) -> LinkerMetricsSnapshot {
        self.streaming
            .as_ref()
            .map(|s| s.metrics_snapshot())
            .unwrap_or_default()
    }

    /// Get the current next_cluster_id value from the streaming linker.
    /// Returns None if streaming is not initialized.
    /// This value represents the next cluster ID that will be assigned.
    pub fn next_cluster_id(&self) -> Option<u32> {
        self.streaming.as_ref().map(|s| s.next_cluster_id())
    }

    /// Flush linker state to persistent storage.
    /// This saves cluster_ids, global_cluster_ids, and next_cluster_id.
    /// Call this periodically or before shutdown for restart recovery.
    /// Returns an error if streaming is not initialized or persistence is not available.
    pub fn flush_linker_state(&self) -> anyhow::Result<()> {
        let streaming = self.streaming.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Streaming not initialized - call enable_streaming() first")
        })?;

        let db = self
            .store
            .shared_db()
            .ok_or_else(|| anyhow::anyhow!("Persistence not available - use PersistentStore"))?;

        let persistence = LinkerStatePersistence::new(&db);
        streaming.flush_state(&persistence)
    }

    /// Restore linker state from persistent storage.
    /// Call this after enable_streaming() to recover cluster ID mappings.
    /// Returns the number of cluster_ids restored, or an error if persistence is not available.
    pub fn restore_linker_state(&mut self) -> anyhow::Result<usize> {
        let streaming = self.streaming.as_mut().ok_or_else(|| {
            anyhow::anyhow!("Streaming not initialized - call enable_streaming() first")
        })?;

        let db = self
            .store
            .shared_db()
            .ok_or_else(|| anyhow::anyhow!("Persistence not available - use PersistentStore"))?;

        let persistence = LinkerStatePersistence::new(&db);
        streaming.restore_state(&persistence)
    }

    /// Flush all linker state and sync to disk.
    /// This is a convenience method that flushes linker state and ensures
    /// all data is synced to disk. Call before shutdown for complete recovery.
    pub fn checkpoint_linker_state(&mut self) -> anyhow::Result<()> {
        self.flush_linker_state()?;

        // Also flush the underlying store to ensure staged records are persisted
        if let Err(e) = self.store.flush_staged_records() {
            tracing::error!(error = %e, "Failed to flush staged records during checkpoint");
            return Err(anyhow::anyhow!("Failed to flush staged records: {}", e));
        }

        Ok(())
    }
}
