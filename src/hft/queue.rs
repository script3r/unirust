//! Lock-free ingest queue for HFT throughput
//!
//! Replaces RwLock contention with bounded lock-free MPSC queue.
//! Single aggregator thread processes batches with exclusive access.

use crossbeam_queue::ArrayQueue;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Capacity for the lock-free ingest queue
pub const QUEUE_CAPACITY: usize = 10_000;

/// Maximum records per aggregated batch
pub const MAX_BATCH_SIZE: usize = 1000;

/// Maximum wait time before flushing partial batch
pub const MAX_BATCH_WAIT: Duration = Duration::from_micros(500);

/// A job submitted to the ingest queue
pub struct IngestJob {
    /// Records to ingest (shared ownership, zero-copy)
    pub records: Arc<Vec<crate::distributed::proto::RecordInput>>,
    /// Range within records for this job
    pub start: usize,
    pub end: usize,
    /// Channel to send response
    pub respond_to:
        oneshot::Sender<Result<Vec<crate::distributed::proto::IngestAssignment>, tonic::Status>>,
    /// Submission timestamp for latency tracking
    pub submitted_at: Instant,
}

/// Lock-free bounded queue for ingest jobs
pub struct IngestQueue {
    /// The underlying lock-free queue
    queue: ArrayQueue<IngestJob>,
    /// Number of pending jobs (for backpressure)
    pending: AtomicU64,
    /// Whether the queue is accepting jobs
    accepting: AtomicBool,
    /// Total jobs submitted (metrics)
    total_submitted: AtomicU64,
    /// Total jobs dropped due to backpressure (metrics)
    total_dropped: AtomicU64,
}

impl IngestQueue {
    /// Create a new ingest queue with default capacity
    pub fn new() -> Self {
        Self::with_capacity(QUEUE_CAPACITY)
    }

    /// Create a new ingest queue with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
            pending: AtomicU64::new(0),
            accepting: AtomicBool::new(true),
            total_submitted: AtomicU64::new(0),
            total_dropped: AtomicU64::new(0),
        }
    }

    /// Submit a job to the queue
    ///
    /// Returns Err if queue is full (backpressure)
    #[inline]
    pub fn submit(&self, job: IngestJob) -> Result<(), IngestJob> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(job);
        }

        match self.queue.push(job) {
            Ok(()) => {
                self.pending.fetch_add(1, Ordering::Release);
                self.total_submitted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(job) => {
                self.total_dropped.fetch_add(1, Ordering::Relaxed);
                Err(job)
            }
        }
    }

    /// Try to receive a job from the queue
    #[inline]
    pub fn try_recv(&self) -> Option<IngestJob> {
        self.queue.pop().inspect(|_| {
            self.pending.fetch_sub(1, Ordering::Release);
        })
    }

    /// Get the number of pending jobs
    #[inline]
    pub fn pending_count(&self) -> u64 {
        self.pending.load(Ordering::Acquire)
    }

    /// Check if queue is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Stop accepting new jobs (for shutdown)
    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    /// Get queue statistics
    pub fn stats(&self) -> QueueStats {
        QueueStats {
            pending: self.pending.load(Ordering::Relaxed),
            total_submitted: self.total_submitted.load(Ordering::Relaxed),
            total_dropped: self.total_dropped.load(Ordering::Relaxed),
            capacity: self.queue.capacity(),
        }
    }
}

impl Default for IngestQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Queue statistics for monitoring
#[derive(Debug, Clone, Copy)]
pub struct QueueStats {
    pub pending: u64,
    pub total_submitted: u64,
    pub total_dropped: u64,
    pub capacity: usize,
}

/// Aggregator that batches jobs from the queue
pub struct BatchAggregator {
    /// Jobs collected for current batch
    batch: Vec<IngestJob>,
    /// Total records in current batch
    record_count: usize,
    /// When batch collection started
    batch_start: Option<Instant>,
}

impl BatchAggregator {
    pub fn new() -> Self {
        Self {
            batch: Vec::with_capacity(100),
            record_count: 0,
            batch_start: None,
        }
    }

    /// Add a job to the batch
    pub fn add(&mut self, job: IngestJob) {
        if self.batch_start.is_none() {
            self.batch_start = Some(Instant::now());
        }
        self.record_count += job.end - job.start;
        self.batch.push(job);
    }

    /// Check if batch should be flushed
    pub fn should_flush(&self) -> bool {
        if self.batch.is_empty() {
            return false;
        }

        // Flush if we have enough records
        if self.record_count >= MAX_BATCH_SIZE {
            return true;
        }

        // Flush if we've waited long enough
        if let Some(start) = self.batch_start {
            if start.elapsed() >= MAX_BATCH_WAIT {
                return true;
            }
        }

        false
    }

    /// Take the current batch for processing
    pub fn take_batch(&mut self) -> Vec<IngestJob> {
        self.record_count = 0;
        self.batch_start = None;
        std::mem::take(&mut self.batch)
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Get current record count
    pub fn record_count(&self) -> usize {
        self.record_count
    }
}

impl Default for BatchAggregator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_basic() {
        let queue = IngestQueue::new();
        assert!(queue.is_empty());
        assert_eq!(queue.pending_count(), 0);
    }

    #[test]
    fn test_queue_capacity() {
        let queue = IngestQueue::with_capacity(2);

        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let (tx3, _rx3) = oneshot::channel();

        let job1 = IngestJob {
            records: Arc::new(vec![]),
            start: 0,
            end: 0,
            respond_to: tx1,
            submitted_at: Instant::now(),
        };
        let job2 = IngestJob {
            records: Arc::new(vec![]),
            start: 0,
            end: 0,
            respond_to: tx2,
            submitted_at: Instant::now(),
        };
        let job3 = IngestJob {
            records: Arc::new(vec![]),
            start: 0,
            end: 0,
            respond_to: tx3,
            submitted_at: Instant::now(),
        };

        assert!(queue.submit(job1).is_ok());
        assert!(queue.submit(job2).is_ok());
        assert!(queue.submit(job3).is_err()); // Queue full

        assert_eq!(queue.pending_count(), 2);
    }

    #[test]
    fn test_aggregator_flush_by_size() {
        let mut agg = BatchAggregator::new();

        for i in 0..10 {
            let (tx, _rx) = oneshot::channel();
            agg.add(IngestJob {
                records: Arc::new(vec![]),
                start: 0,
                end: 100, // 100 records each
                respond_to: tx,
                submitted_at: Instant::now(),
            });

            if i < 9 {
                assert!(!agg.should_flush()); // < 1000 records
            }
        }

        assert!(agg.should_flush()); // >= 1000 records
    }
}
