//! # HFT (High-Frequency Trading) Optimizations Module
//!
//! Ultra-low-latency optimizations for maximum throughput:
//! - Lock-free DSU with atomic parent links
//! - SIMD-accelerated key hashing
//! - Zero-copy record passing
//! - Cache-line aligned metrics
//! - Lock-free ingest queue
//! - Async WAL with write coalescing

pub mod aligned;
pub mod async_wal;
pub mod atomic_dsu;
pub mod queue;
pub mod simd_hash;
pub mod zero_copy;

// Core types
pub use atomic_dsu::{AtomicDSU, AtomicMergeResult};
pub use simd_hash::SimdHasher;
pub use zero_copy::{RecordSlice, ZeroCopyBatch};

// HFT infrastructure
pub use aligned::{AlignedCounter, AlignedMetrics, MetricsSnapshot};
pub use async_wal::{AsyncWal, AsyncWalConfig, WalError, WalTicket};
pub use queue::{BatchAggregator, IngestJob, IngestQueue, QueueStats};
