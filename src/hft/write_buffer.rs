//! # Write Buffer Manager
//!
//! RocksDB-inspired write buffer management with backpressure.
//!
//! From RocksDB Write Buffer Manager:
//! "Helps manage total memory consumed by memtables across multiple
//! column families and DB instances, preventing memory bloat during
//! heavy write loads."
//!
//! Key features:
//! - Memory limit enforcement across multiple buffers
//! - Automatic flush triggers at configurable thresholds
//! - Backpressure/stalling when memory exceeds limits
//! - Statistics for monitoring memory pressure

use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for write buffer manager
#[derive(Debug, Clone)]
pub struct WriteBufferConfig {
    /// Total memory limit for all buffers (bytes)
    pub memory_limit: usize,
    /// Trigger flush when mutable buffer reaches this ratio of limit
    /// Default: 0.9 (90%)
    pub flush_trigger_ratio: f64,
    /// Stall writes when total memory exceeds limit AND mutable > this ratio
    /// Default: 0.5 (50%)
    pub stall_trigger_ratio: f64,
    /// Allow stalling writers when memory exceeded
    /// Default: true
    pub allow_stall: bool,
    /// Maximum stall duration before forcing through
    /// Default: 1 second
    pub max_stall_duration: Duration,
    /// Check interval for memory pressure
    /// Default: 1ms
    pub check_interval: Duration,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        Self {
            memory_limit: 256 * 1024 * 1024, // 256MB
            flush_trigger_ratio: 0.9,
            stall_trigger_ratio: 0.5,
            allow_stall: true,
            max_stall_duration: Duration::from_secs(1),
            check_interval: Duration::from_millis(1),
        }
    }
}

impl WriteBufferConfig {
    /// High-throughput configuration with larger buffers
    pub fn high_throughput() -> Self {
        Self {
            memory_limit: 512 * 1024 * 1024, // 512MB
            flush_trigger_ratio: 0.95,
            stall_trigger_ratio: 0.6,
            allow_stall: true,
            max_stall_duration: Duration::from_millis(500),
            check_interval: Duration::from_micros(500),
        }
    }

    /// Low-latency configuration with aggressive flushing
    pub fn low_latency() -> Self {
        Self {
            memory_limit: 128 * 1024 * 1024, // 128MB
            flush_trigger_ratio: 0.7,
            stall_trigger_ratio: 0.4,
            allow_stall: false, // Don't stall, just reject
            max_stall_duration: Duration::from_millis(100),
            check_interval: Duration::from_millis(1),
        }
    }
}

/// Statistics for write buffer manager
#[derive(Debug, Default)]
pub struct WriteBufferStats {
    /// Current memory usage (bytes)
    pub current_memory: AtomicUsize,
    /// Peak memory usage (bytes)
    pub peak_memory: AtomicUsize,
    /// Number of flush triggers
    pub flush_triggers: AtomicU64,
    /// Number of stalls
    pub stalls: AtomicU64,
    /// Total stall duration (microseconds)
    pub stall_duration_us: AtomicU64,
    /// Number of rejected writes due to memory pressure
    pub rejected_writes: AtomicU64,
    /// Number of successful reservations
    pub reservations: AtomicU64,
    /// Number of releases
    pub releases: AtomicU64,
}

impl WriteBufferStats {
    /// Get memory utilization ratio
    pub fn utilization(&self, limit: usize) -> f64 {
        if limit == 0 {
            0.0
        } else {
            self.current_memory.load(Ordering::Relaxed) as f64 / limit as f64
        }
    }

    /// Get average stall duration
    pub fn avg_stall_duration(&self) -> Duration {
        let stalls = self.stalls.load(Ordering::Relaxed);
        if stalls == 0 {
            Duration::ZERO
        } else {
            let total_us = self.stall_duration_us.load(Ordering::Relaxed);
            Duration::from_micros(total_us / stalls)
        }
    }
}

/// Result of a reservation attempt
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationResult {
    /// Memory reserved successfully
    Success,
    /// Reservation succeeded after stalling
    SuccessAfterStall,
    /// Reservation rejected due to memory pressure
    Rejected,
    /// Flush should be triggered
    NeedsFlush,
}

/// A reservation handle that releases memory when dropped
pub struct Reservation {
    manager: Arc<WriteBufferManager>,
    size: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.manager.release(self.size);
    }
}

/// Write buffer manager for coordinating memory across buffers
pub struct WriteBufferManager {
    config: WriteBufferConfig,
    stats: WriteBufferStats,
    /// Current mutable buffer memory
    mutable_memory: AtomicUsize,
    /// Memory being flushed (immutable)
    flushing_memory: AtomicUsize,
    /// Flag indicating flush is in progress
    flush_in_progress: AtomicBool,
    /// Waiters for memory to become available
    waiters: Mutex<Vec<std::thread::Thread>>,
    /// Flush callback
    flush_callback: RwLock<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl WriteBufferManager {
    /// Create a new write buffer manager
    pub fn new(config: WriteBufferConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            stats: WriteBufferStats::default(),
            mutable_memory: AtomicUsize::new(0),
            flushing_memory: AtomicUsize::new(0),
            flush_in_progress: AtomicBool::new(false),
            waiters: Mutex::new(Vec::new()),
            flush_callback: RwLock::new(None),
        })
    }

    /// Set the flush callback
    pub fn set_flush_callback<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut cb = self.flush_callback.write();
        *cb = Some(Box::new(callback));
    }

    /// Try to reserve memory for a write
    pub fn try_reserve(self: &Arc<Self>, size: usize) -> (ReservationResult, Option<Reservation>) {
        let current = self.mutable_memory.load(Ordering::Relaxed);
        let flushing = self.flushing_memory.load(Ordering::Relaxed);
        let total = current + flushing + size;

        // Check if we need to trigger a flush
        let mutable_ratio = (current + size) as f64 / self.config.memory_limit as f64;
        if mutable_ratio >= self.config.flush_trigger_ratio {
            self.trigger_flush();
        }

        // Check if we're over limit
        if total > self.config.memory_limit {
            let mutable_over_stall = (current + size) as f64 / self.config.memory_limit as f64
                > self.config.stall_trigger_ratio;

            if mutable_over_stall {
                if self.config.allow_stall {
                    // Stall and wait for memory
                    let stall_result = self.stall_until_available(size);
                    return stall_result;
                } else {
                    // Reject the write
                    self.stats.rejected_writes.fetch_add(1, Ordering::Relaxed);
                    return (ReservationResult::Rejected, None);
                }
            }
        }

        // Reserve the memory
        self.mutable_memory.fetch_add(size, Ordering::Relaxed);
        self.stats.reservations.fetch_add(1, Ordering::Relaxed);

        // Update peak
        let new_total = self.mutable_memory.load(Ordering::Relaxed)
            + self.flushing_memory.load(Ordering::Relaxed);
        self.stats
            .peak_memory
            .fetch_max(new_total, Ordering::Relaxed);
        self.stats
            .current_memory
            .store(new_total, Ordering::Relaxed);

        let needs_flush = mutable_ratio >= self.config.flush_trigger_ratio;
        let result = if needs_flush {
            ReservationResult::NeedsFlush
        } else {
            ReservationResult::Success
        };

        (
            result,
            Some(Reservation {
                manager: Arc::clone(self),
                size,
            }),
        )
    }

    /// Reserve memory, blocking if necessary
    pub fn reserve(self: &Arc<Self>, size: usize) -> Option<Reservation> {
        let (result, reservation) = self.try_reserve(size);
        match result {
            ReservationResult::Success
            | ReservationResult::SuccessAfterStall
            | ReservationResult::NeedsFlush => reservation,
            ReservationResult::Rejected => None,
        }
    }

    /// Release memory back to the pool
    fn release(&self, size: usize) {
        self.mutable_memory.fetch_sub(size, Ordering::Relaxed);
        self.stats.releases.fetch_add(1, Ordering::Relaxed);

        let new_total = self.mutable_memory.load(Ordering::Relaxed)
            + self.flushing_memory.load(Ordering::Relaxed);
        self.stats
            .current_memory
            .store(new_total, Ordering::Relaxed);

        // Wake up any waiters
        self.wake_waiters();
    }

    /// Mark memory as being flushed (moves from mutable to flushing)
    pub fn begin_flush(&self, size: usize) {
        self.mutable_memory.fetch_sub(size, Ordering::Relaxed);
        self.flushing_memory.fetch_add(size, Ordering::Relaxed);
        self.flush_in_progress.store(true, Ordering::Relaxed);
    }

    /// Complete a flush (releases flushing memory)
    pub fn complete_flush(&self, size: usize) {
        self.flushing_memory.fetch_sub(size, Ordering::Relaxed);
        self.flush_in_progress.store(false, Ordering::Relaxed);

        let new_total = self.mutable_memory.load(Ordering::Relaxed)
            + self.flushing_memory.load(Ordering::Relaxed);
        self.stats
            .current_memory
            .store(new_total, Ordering::Relaxed);

        // Wake up any waiters
        self.wake_waiters();
    }

    /// Trigger a flush
    fn trigger_flush(&self) {
        self.stats.flush_triggers.fetch_add(1, Ordering::Relaxed);

        // Call flush callback if set
        if let Some(ref callback) = *self.flush_callback.read() {
            callback();
        }
    }

    /// Stall until memory becomes available
    fn stall_until_available(
        self: &Arc<Self>,
        size: usize,
    ) -> (ReservationResult, Option<Reservation>) {
        let start = Instant::now();
        self.stats.stalls.fetch_add(1, Ordering::Relaxed);

        // Trigger flush to free up memory
        self.trigger_flush();

        loop {
            // Check if we can proceed
            let current = self.mutable_memory.load(Ordering::Relaxed);
            let flushing = self.flushing_memory.load(Ordering::Relaxed);
            let total = current + flushing + size;

            if total <= self.config.memory_limit {
                // Can proceed
                self.mutable_memory.fetch_add(size, Ordering::Relaxed);
                self.stats.reservations.fetch_add(1, Ordering::Relaxed);

                let stall_duration = start.elapsed();
                self.stats
                    .stall_duration_us
                    .fetch_add(stall_duration.as_micros() as u64, Ordering::Relaxed);

                return (
                    ReservationResult::SuccessAfterStall,
                    Some(Reservation {
                        manager: Arc::clone(self),
                        size,
                    }),
                );
            }

            // Check timeout
            if start.elapsed() >= self.config.max_stall_duration {
                let stall_duration = start.elapsed();
                self.stats
                    .stall_duration_us
                    .fetch_add(stall_duration.as_micros() as u64, Ordering::Relaxed);
                self.stats.rejected_writes.fetch_add(1, Ordering::Relaxed);
                return (ReservationResult::Rejected, None);
            }

            // Register as waiter and park
            {
                let mut waiters = self.waiters.lock();
                waiters.push(std::thread::current());
            }

            std::thread::park_timeout(self.config.check_interval);
        }
    }

    /// Wake up waiting threads
    fn wake_waiters(&self) {
        let waiters: Vec<_> = {
            let mut w = self.waiters.lock();
            std::mem::take(&mut *w)
        };

        for thread in waiters {
            thread.unpark();
        }
    }

    /// Get current memory usage
    pub fn memory_usage(&self) -> usize {
        self.mutable_memory.load(Ordering::Relaxed) + self.flushing_memory.load(Ordering::Relaxed)
    }

    /// Get mutable memory usage
    pub fn mutable_memory(&self) -> usize {
        self.mutable_memory.load(Ordering::Relaxed)
    }

    /// Get flushing memory usage
    pub fn flushing_memory(&self) -> usize {
        self.flushing_memory.load(Ordering::Relaxed)
    }

    /// Check if memory is under pressure
    pub fn is_under_pressure(&self) -> bool {
        self.memory_usage() as f64 / self.config.memory_limit as f64
            >= self.config.stall_trigger_ratio
    }

    /// Check if flush is needed
    pub fn needs_flush(&self) -> bool {
        self.mutable_memory.load(Ordering::Relaxed) as f64 / self.config.memory_limit as f64
            >= self.config.flush_trigger_ratio
    }

    /// Get statistics
    pub fn stats(&self) -> &WriteBufferStats {
        &self.stats
    }

    /// Get configuration
    pub fn config(&self) -> &WriteBufferConfig {
        &self.config
    }

    /// Get memory limit
    pub fn memory_limit(&self) -> usize {
        self.config.memory_limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_reservation() {
        let manager = WriteBufferManager::new(WriteBufferConfig {
            memory_limit: 1000,
            flush_trigger_ratio: 0.9,
            stall_trigger_ratio: 0.5,
            allow_stall: false,
            ..Default::default()
        });

        // Reserve some memory
        let (result, reservation) = manager.try_reserve(100);
        assert_eq!(result, ReservationResult::Success);
        assert!(reservation.is_some());
        assert_eq!(manager.mutable_memory(), 100);

        // Drop reservation
        drop(reservation);
        assert_eq!(manager.mutable_memory(), 0);
    }

    #[test]
    fn test_flush_trigger() {
        let manager = WriteBufferManager::new(WriteBufferConfig {
            memory_limit: 1000,
            flush_trigger_ratio: 0.5, // 50%
            stall_trigger_ratio: 0.8,
            allow_stall: false,
            ..Default::default()
        });

        // Reserve below threshold
        let (result, _r1) = manager.try_reserve(400);
        assert_eq!(result, ReservationResult::Success);

        // Reserve above threshold - should trigger flush
        let (result, _r2) = manager.try_reserve(200);
        assert_eq!(result, ReservationResult::NeedsFlush);

        assert!(manager.stats().flush_triggers.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn test_rejection_when_full() {
        let manager = WriteBufferManager::new(WriteBufferConfig {
            memory_limit: 1000,
            flush_trigger_ratio: 0.9,
            stall_trigger_ratio: 0.5,
            allow_stall: false,
            ..Default::default()
        });

        // Fill up the buffer
        let _reservations: Vec<_> = (0..10).map(|_| manager.try_reserve(100).1).collect();

        // Next reservation should be rejected (over limit and over stall ratio)
        let (result, reservation) = manager.try_reserve(100);
        assert_eq!(result, ReservationResult::Rejected);
        assert!(reservation.is_none());
    }

    #[test]
    fn test_flush_lifecycle() {
        let manager = WriteBufferManager::new(WriteBufferConfig {
            memory_limit: 1000,
            ..Default::default()
        });

        // Reserve memory
        manager.mutable_memory.store(500, Ordering::Relaxed);

        // Begin flush
        manager.begin_flush(300);
        assert_eq!(manager.mutable_memory(), 200);
        assert_eq!(manager.flushing_memory(), 300);

        // Complete flush
        manager.complete_flush(300);
        assert_eq!(manager.mutable_memory(), 200);
        assert_eq!(manager.flushing_memory(), 0);
    }

    #[test]
    fn test_stats() {
        let manager = WriteBufferManager::new(WriteBufferConfig {
            memory_limit: 1000,
            ..Default::default()
        });

        // Do some operations
        let (_, r1) = manager.try_reserve(100);
        let (_, r2) = manager.try_reserve(200);
        drop(r1);
        drop(r2);

        let stats = manager.stats();
        assert_eq!(stats.reservations.load(Ordering::Relaxed), 2);
        assert_eq!(stats.releases.load(Ordering::Relaxed), 2);
    }
}
