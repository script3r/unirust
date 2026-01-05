//! # Batch Coalescing Module
//!
//! Implements batch coalescing to reduce per-record overhead by combining
//! multiple incoming records into larger batches before processing.
//!
//! ## Benefits
//!
//! - Amortizes fixed overhead (lock acquisition, batch setup) across more records
//! - Improves cache locality by processing related records together
//! - Reduces gRPC/network overhead by processing batched messages
//!
//! ## Configuration
//!
//! - `capacity`: Maximum records to buffer before forcing a flush
//! - `flush_interval`: Maximum time to wait before flushing

use std::time::{Duration, Instant};

/// Configuration for batch coalescing
#[derive(Debug, Clone)]
pub struct CoalescerConfig {
    /// Maximum number of records to buffer before flush
    pub capacity: usize,
    /// Maximum time to wait before flushing (even if not full)
    pub flush_interval: Duration,
    /// Minimum batch size before time-based flush is allowed
    pub min_batch_size: usize,
}

impl Default for CoalescerConfig {
    fn default() -> Self {
        Self {
            capacity: 8192,
            flush_interval: Duration::from_millis(10),
            min_batch_size: 500,
        }
    }
}

impl CoalescerConfig {
    /// High-throughput configuration: larger batches, longer wait
    pub fn high_throughput() -> Self {
        Self {
            capacity: 8192,
            flush_interval: Duration::from_millis(10),
            min_batch_size: 500,
        }
    }

    /// Low-latency configuration: smaller batches, shorter wait
    pub fn low_latency() -> Self {
        Self {
            capacity: 1024,
            flush_interval: Duration::from_millis(2),
            min_batch_size: 50,
        }
    }

    /// Create config with custom capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            ..Default::default()
        }
    }
}

/// Batch coalescer for combining records before processing
///
/// Generic over T to support any record type
pub struct BatchCoalescer<T> {
    /// Buffered records
    buffer: Vec<T>,
    /// Configuration
    config: CoalescerConfig,
    /// Time of last flush
    last_flush: Instant,
    /// Total records coalesced (for metrics)
    total_coalesced: u64,
    /// Total flushes performed
    total_flushes: u64,
}

impl<T> BatchCoalescer<T> {
    /// Create a new batch coalescer with default configuration
    pub fn new() -> Self {
        Self::with_config(CoalescerConfig::default())
    }

    /// Create a new batch coalescer with custom configuration
    pub fn with_config(config: CoalescerConfig) -> Self {
        Self {
            buffer: Vec::with_capacity(config.capacity),
            config,
            last_flush: Instant::now(),
            total_coalesced: 0,
            total_flushes: 0,
        }
    }

    /// Add a record to the buffer
    ///
    /// Returns `Some(batch)` if the buffer should be flushed
    #[inline]
    pub fn add(&mut self, record: T) -> Option<Vec<T>> {
        self.buffer.push(record);
        self.total_coalesced += 1;

        if self.should_flush() {
            self.flush()
        } else {
            None
        }
    }

    /// Add multiple records at once
    ///
    /// Returns `Some(batch)` if the buffer should be flushed
    pub fn add_batch(&mut self, records: impl IntoIterator<Item = T>) -> Option<Vec<T>> {
        for record in records {
            self.buffer.push(record);
            self.total_coalesced += 1;
        }

        if self.should_flush() {
            self.flush()
        } else {
            None
        }
    }

    /// Check if the buffer should be flushed
    #[inline]
    fn should_flush(&self) -> bool {
        // Flush if at capacity
        if self.buffer.len() >= self.config.capacity {
            return true;
        }

        // Flush if time elapsed and we have minimum records
        if self.buffer.len() >= self.config.min_batch_size
            && self.last_flush.elapsed() >= self.config.flush_interval
        {
            return true;
        }

        false
    }

    /// Force flush the buffer, returning the batch if non-empty
    pub fn flush(&mut self) -> Option<Vec<T>> {
        if self.buffer.is_empty() {
            return None;
        }

        self.last_flush = Instant::now();
        self.total_flushes += 1;

        Some(std::mem::replace(
            &mut self.buffer,
            Vec::with_capacity(self.config.capacity),
        ))
    }

    /// Flush only if time-based flush is due (for background timer)
    pub fn tick(&mut self) -> Option<Vec<T>> {
        if !self.buffer.is_empty()
            && self.buffer.len() >= self.config.min_batch_size
            && self.last_flush.elapsed() >= self.config.flush_interval
        {
            self.flush()
        } else {
            None
        }
    }

    /// Get the current buffer length
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Get coalescer statistics
    pub fn stats(&self) -> CoalescerStats {
        CoalescerStats {
            total_coalesced: self.total_coalesced,
            total_flushes: self.total_flushes,
            current_buffer_size: self.buffer.len(),
            average_batch_size: if self.total_flushes > 0 {
                self.total_coalesced as f64 / self.total_flushes as f64
            } else {
                0.0
            },
        }
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.total_coalesced = 0;
        self.total_flushes = 0;
    }
}

impl<T> Default for BatchCoalescer<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for batch coalescer
#[derive(Debug, Clone)]
pub struct CoalescerStats {
    /// Total records that have been coalesced
    pub total_coalesced: u64,
    /// Total number of flushes performed
    pub total_flushes: u64,
    /// Current number of records in buffer
    pub current_buffer_size: usize,
    /// Average batch size across all flushes
    pub average_batch_size: f64,
}

/// Async-compatible batch coalescer with timeout support
///
/// This version is designed for use in async contexts where
/// we need to handle both capacity-based and time-based flushing.
pub struct AsyncBatchCoalescer<T> {
    /// Inner coalescer
    inner: BatchCoalescer<T>,
    /// Deadline for next time-based flush
    next_flush_deadline: Option<Instant>,
}

impl<T> AsyncBatchCoalescer<T> {
    /// Create a new async batch coalescer
    pub fn new() -> Self {
        Self::with_config(CoalescerConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(config: CoalescerConfig) -> Self {
        Self {
            inner: BatchCoalescer::with_config(config),
            next_flush_deadline: None,
        }
    }

    /// Add a record and return batch if ready
    pub fn add(&mut self, record: T) -> Option<Vec<T>> {
        // Set deadline on first record
        if self.inner.is_empty() {
            self.next_flush_deadline = Some(Instant::now() + self.inner.config.flush_interval);
        }

        let result = self.inner.add(record);
        if result.is_some() {
            self.next_flush_deadline = None;
        }
        result
    }

    /// Force flush
    pub fn flush(&mut self) -> Option<Vec<T>> {
        self.next_flush_deadline = None;
        self.inner.flush()
    }

    /// Get time until next deadline (for async timeout)
    pub fn time_until_deadline(&self) -> Option<Duration> {
        self.next_flush_deadline.map(|deadline| {
            let now = Instant::now();
            if deadline > now {
                deadline - now
            } else {
                Duration::ZERO
            }
        })
    }

    /// Check if deadline has passed
    pub fn is_deadline_passed(&self) -> bool {
        self.next_flush_deadline
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(false)
    }

    /// Flush if deadline passed
    pub fn flush_if_deadline(&mut self) -> Option<Vec<T>> {
        if self.is_deadline_passed() {
            self.flush()
        } else {
            None
        }
    }

    /// Get current buffer size
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get statistics
    pub fn stats(&self) -> CoalescerStats {
        self.inner.stats()
    }
}

impl<T> Default for AsyncBatchCoalescer<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coalescer_capacity_flush() {
        let config = CoalescerConfig {
            capacity: 5,
            flush_interval: Duration::from_secs(100), // Long timeout
            min_batch_size: 1,
        };

        let mut coalescer: BatchCoalescer<i32> = BatchCoalescer::with_config(config);

        // Add 4 records - no flush
        for i in 0..4 {
            assert!(coalescer.add(i).is_none());
        }
        assert_eq!(coalescer.len(), 4);

        // Add 5th record - should flush
        let batch = coalescer.add(4);
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.len(), 5);
        assert_eq!(batch, vec![0, 1, 2, 3, 4]);
        assert_eq!(coalescer.len(), 0);
    }

    #[test]
    fn test_coalescer_force_flush() {
        let mut coalescer: BatchCoalescer<i32> = BatchCoalescer::new();

        coalescer.add(1);
        coalescer.add(2);
        coalescer.add(3);

        let batch = coalescer.flush();
        assert!(batch.is_some());
        assert_eq!(batch.unwrap(), vec![1, 2, 3]);
        assert!(coalescer.is_empty());
    }

    #[test]
    fn test_coalescer_stats() {
        let config = CoalescerConfig {
            capacity: 3,
            flush_interval: Duration::from_secs(100),
            min_batch_size: 1,
        };

        let mut coalescer: BatchCoalescer<i32> = BatchCoalescer::with_config(config);

        // Add 6 records (2 flushes of 3)
        for i in 0..6 {
            coalescer.add(i);
        }

        let stats = coalescer.stats();
        assert_eq!(stats.total_coalesced, 6);
        assert_eq!(stats.total_flushes, 2);
        assert_eq!(stats.average_batch_size, 3.0);
    }

    #[test]
    fn test_coalescer_add_batch() {
        let config = CoalescerConfig {
            capacity: 10,
            flush_interval: Duration::from_secs(100),
            min_batch_size: 1,
        };

        let mut coalescer: BatchCoalescer<i32> = BatchCoalescer::with_config(config);

        // Add batch of 8 - no flush
        let result = coalescer.add_batch(0..8);
        assert!(result.is_none());
        assert_eq!(coalescer.len(), 8);

        // Add batch of 5 - should flush (total 13 > capacity 10)
        let result = coalescer.add_batch(8..13);
        assert!(result.is_some());
    }

    #[test]
    fn test_async_coalescer_deadline() {
        let config = CoalescerConfig {
            capacity: 1000,
            flush_interval: Duration::from_millis(10),
            min_batch_size: 1,
        };

        let mut coalescer: AsyncBatchCoalescer<i32> = AsyncBatchCoalescer::with_config(config);

        // No deadline initially
        assert!(coalescer.time_until_deadline().is_none());

        // Add first record - deadline set
        coalescer.add(1);
        assert!(coalescer.time_until_deadline().is_some());

        // Should have some time left
        let time_left = coalescer.time_until_deadline().unwrap();
        assert!(time_left > Duration::ZERO);
        assert!(time_left <= Duration::from_millis(10));
    }

    #[test]
    fn test_config_presets() {
        let high_tp = CoalescerConfig::high_throughput();
        assert_eq!(high_tp.capacity, 8192);

        let low_lat = CoalescerConfig::low_latency();
        assert_eq!(low_lat.capacity, 1024);

        let custom = CoalescerConfig::with_capacity(2048);
        assert_eq!(custom.capacity, 2048);
    }
}
