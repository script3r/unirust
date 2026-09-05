//! # Performance Optimizations Module
//!
//! Performance utilities with different integration and durability requirements:
//! - Atomic parent links for a standalone DSU without temporal guards
//! - Scalar and platform-specific batch hashing
//! - Shared record slices
//! - Cache-line aligned metrics
//! - Lock-free ingest queue
//! - Experimental background batch-file writing
//! - String interning with sharded locks
//! - Bigtable-inspired caching (bloom filters, scan cache, block cache)
//!
//! These helpers are not all used by the production ingest path. In particular,
//! [`AsyncWal`] is not the durable distributed ingest WAL, and [`AtomicDSU`] does
//! not replace the temporally guarded linker DSU.

pub mod aligned;
pub mod async_wal;
pub mod atomic_dsu;
pub mod bigtable_opts;
pub mod compression;
pub mod interner;
pub mod queue;
pub mod sharded_cache;
pub mod simd_hash;
pub mod write_buffer;
pub mod zero_copy;

// Core types
pub use atomic_dsu::{AtomicDSU, AtomicMergeResult};
pub use interner::ConcurrentInterner;
pub use simd_hash::SimdHasher;
pub use zero_copy::{RecordSlice, ZeroCopyBatch};

// Performance infrastructure
pub use aligned::{AlignedCounter, AlignedMetrics, MetricsSnapshot};
pub use async_wal::{AsyncWal, AsyncWalConfig, WalError, WalTicket};
pub use bigtable_opts::{
    BlockCache, GroupCommitBuffer, IdentityBloomFilter, PartitionOptimizations, ScanCache,
};
pub use compression::{
    compress, decompress, decompress_batch, BatchCompressor, CompressionStats, StreamingCompressor,
};
pub use queue::{BatchAggregator, IngestJob, IngestQueue, QueueStats};
pub use sharded_cache::{ShardedCacheConfig, ShardedCacheStats, ShardedLruCache};
pub use write_buffer::{
    ReservationResult, WriteBufferConfig, WriteBufferManager, WriteBufferStats,
};
