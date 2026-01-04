//! # HFT (High-Frequency Trading) Optimizations Module
//!
//! Ultra-low-latency optimizations for maximum throughput:
//! - Lock-free DSU with atomic parent links
//! - SIMD-accelerated key hashing
//! - Zero-copy record passing
//! - Batch-optimized operations

pub mod atomic_dsu;
pub mod simd_hash;
pub mod zero_copy;

pub use atomic_dsu::{AtomicDSU, AtomicMergeResult};
pub use simd_hash::SimdHasher;
pub use zero_copy::{RecordSlice, ZeroCopyBatch};
