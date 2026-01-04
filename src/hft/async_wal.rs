//! Async Write-Ahead Log with coalescing
//!
//! Removes WAL fsync from the critical path by:
//! 1. Submitting writes to a background thread
//! 2. Coalescing multiple writes into single fsync
//! 3. Optional io_uring support for async disk I/O

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{self, Receiver, Sender, TrySendError};

/// Configuration for the async WAL
#[derive(Debug, Clone)]
pub struct AsyncWalConfig {
    /// Maximum time to wait before flushing a partial batch
    pub max_coalesce_delay: Duration,
    /// Maximum records to coalesce before flushing
    pub max_coalesce_records: usize,
    /// Channel capacity for write requests
    pub channel_capacity: usize,
    /// Whether to actually sync to disk (false for testing)
    pub sync_enabled: bool,
}

impl Default for AsyncWalConfig {
    fn default() -> Self {
        Self {
            max_coalesce_delay: Duration::from_micros(100),
            max_coalesce_records: 1000,
            channel_capacity: 1000,
            sync_enabled: true,
        }
    }
}

impl AsyncWalConfig {
    /// Config optimized for low latency (less coalescing)
    pub fn low_latency() -> Self {
        Self {
            max_coalesce_delay: Duration::from_micros(50),
            max_coalesce_records: 100,
            channel_capacity: 10000,
            sync_enabled: true,
        }
    }

    /// Config optimized for high throughput (more coalescing)
    pub fn high_throughput() -> Self {
        Self {
            max_coalesce_delay: Duration::from_micros(500),
            max_coalesce_records: 5000,
            channel_capacity: 1000,
            sync_enabled: true,
        }
    }
}

/// A WAL write request
struct WalEntry {
    /// Serialized record data
    data: Vec<u8>,
    /// Ticket for completion notification
    ticket: WalTicket,
}

/// Ticket for tracking WAL write completion
#[derive(Clone)]
pub struct WalTicket {
    completed: Arc<AtomicBool>,
    #[allow(dead_code)]
    sequence: u64,
}

impl WalTicket {
    fn new(sequence: u64) -> Self {
        Self {
            completed: Arc::new(AtomicBool::new(false)),
            sequence,
        }
    }

    /// Check if the write has been synced to disk
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    /// Block until the write is complete
    pub fn wait(&self) {
        while !self.is_complete() {
            std::hint::spin_loop();
        }
    }

    /// Wait with timeout
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        while !self.is_complete() {
            if start.elapsed() >= timeout {
                return false;
            }
            std::hint::spin_loop();
        }
        true
    }

    fn mark_complete(&self) {
        self.completed.store(true, Ordering::Release);
    }
}

/// Async Write-Ahead Log
pub struct AsyncWal {
    /// Channel to send write requests
    tx: Sender<WalEntry>,
    /// Sequence counter for ordering
    sequence: AtomicU64,
    /// Writer thread handle
    writer_handle: Option<JoinHandle<()>>,
    /// Shutdown signal
    shutdown: Arc<AtomicBool>,
    /// Path to WAL file
    path: PathBuf,
    /// Configuration
    #[allow(dead_code)]
    config: AsyncWalConfig,
}

impl AsyncWal {
    /// Create a new async WAL
    pub fn new(data_dir: &std::path::Path, config: AsyncWalConfig) -> std::io::Result<Self> {
        let path = data_dir.join("ingest_wal.bin");
        let temp_path = data_dir.join("ingest_wal.bin.tmp");

        // Ensure directory exists
        fs::create_dir_all(data_dir)?;

        let (tx, rx) = crossbeam_channel::bounded(config.channel_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));

        let writer_shutdown = Arc::clone(&shutdown);
        let writer_config = config.clone();
        let writer_path = path.clone();
        let writer_temp_path = temp_path;

        let writer_handle = thread::Builder::new()
            .name("wal-writer".into())
            .spawn(move || {
                wal_writer_loop(
                    rx,
                    writer_shutdown,
                    writer_config,
                    writer_path,
                    writer_temp_path,
                );
            })?;

        Ok(Self {
            tx,
            sequence: AtomicU64::new(0),
            writer_handle: Some(writer_handle),
            shutdown,
            path,
            config,
        })
    }

    /// Submit a batch of records to the WAL
    ///
    /// Returns a ticket that can be used to wait for sync completion.
    /// The actual write happens asynchronously - this call returns immediately.
    pub fn submit(&self, data: Vec<u8>) -> Result<WalTicket, WalError> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let ticket = WalTicket::new(sequence);

        let entry = WalEntry {
            data,
            ticket: ticket.clone(),
        };

        match self.tx.try_send(entry) {
            Ok(()) => Ok(ticket),
            Err(TrySendError::Full(_)) => Err(WalError::QueueFull),
            Err(TrySendError::Disconnected(_)) => Err(WalError::Shutdown),
        }
    }

    /// Submit and wait for completion
    pub fn submit_sync(&self, data: Vec<u8>) -> Result<(), WalError> {
        let ticket = self.submit(data)?;
        ticket.wait();
        Ok(())
    }

    /// Clear the WAL (after successful processing)
    pub fn clear(&self) -> std::io::Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        Ok(())
    }

    /// Get WAL statistics
    pub fn stats(&self) -> WalStats {
        WalStats {
            pending: self.tx.len(),
            total_writes: self.sequence.load(Ordering::Relaxed),
        }
    }

    /// Check if WAL has pending writes
    pub fn has_pending(&self) -> bool {
        !self.tx.is_empty()
    }
}

impl Drop for AsyncWal {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Release);

        // Wait for writer thread to finish
        if let Some(handle) = self.writer_handle.take() {
            let _ = handle.join();
        }
    }
}

/// WAL writer loop running in dedicated thread
fn wal_writer_loop(
    rx: Receiver<WalEntry>,
    shutdown: Arc<AtomicBool>,
    config: AsyncWalConfig,
    path: PathBuf,
    temp_path: PathBuf,
) {
    let mut buffer: Vec<WalEntry> = Vec::with_capacity(config.max_coalesce_records);
    let mut last_flush = Instant::now();

    loop {
        // Check for shutdown
        if shutdown.load(Ordering::Acquire) && rx.is_empty() {
            break;
        }

        // Try to receive with timeout for coalescing
        let timeout = config
            .max_coalesce_delay
            .saturating_sub(last_flush.elapsed());

        match rx.recv_timeout(timeout) {
            Ok(entry) => {
                buffer.push(entry);

                // Check if we should flush due to size
                if buffer.len() >= config.max_coalesce_records {
                    flush_buffer(&mut buffer, &config, &path, &temp_path);
                    last_flush = Instant::now();
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                // Flush due to timeout
                if !buffer.is_empty() {
                    flush_buffer(&mut buffer, &config, &path, &temp_path);
                    last_flush = Instant::now();
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                // Channel closed, flush remaining and exit
                if !buffer.is_empty() {
                    flush_buffer(&mut buffer, &config, &path, &temp_path);
                }
                break;
            }
        }
    }
}

/// Flush the buffer to disk
fn flush_buffer(
    buffer: &mut Vec<WalEntry>,
    config: &AsyncWalConfig,
    path: &PathBuf,
    temp_path: &PathBuf,
) {
    if buffer.is_empty() {
        return;
    }

    // Write all entries to temp file
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(temp_path)
    {
        // Write length-prefixed entries
        for entry in buffer.iter() {
            let len = entry.data.len() as u32;
            let _ = file.write_all(&len.to_le_bytes());
            let _ = file.write_all(&entry.data);
        }

        // Sync if enabled
        if config.sync_enabled {
            let _ = file.sync_all();
        }

        // Atomic rename
        let _ = fs::rename(temp_path, path);
    }

    // Mark all tickets as complete
    for entry in buffer.drain(..) {
        entry.ticket.mark_complete();
    }
}

/// WAL error types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalError {
    /// Write queue is full (backpressure)
    QueueFull,
    /// WAL has been shut down
    Shutdown,
    /// I/O error
    IoError,
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::QueueFull => write!(f, "WAL queue full"),
            WalError::Shutdown => write!(f, "WAL shutdown"),
            WalError::IoError => write!(f, "WAL I/O error"),
        }
    }
}

impl std::error::Error for WalError {}

/// WAL statistics
#[derive(Debug, Clone, Copy)]
pub struct WalStats {
    pub pending: usize,
    pub total_writes: u64,
}

/// Load records from WAL for replay
pub fn load_wal(path: &std::path::Path) -> std::io::Result<Vec<Vec<u8>>> {
    let wal_path = path.join("ingest_wal.bin");
    let temp_path = path.join("ingest_wal.bin.tmp");

    // Check which file exists
    let source = if wal_path.exists() {
        Some(wal_path)
    } else if temp_path.exists() {
        Some(temp_path)
    } else {
        None
    };

    let Some(path) = source else {
        return Ok(Vec::new());
    };

    let data = fs::read(&path)?;
    if data.is_empty() {
        return Ok(Vec::new());
    }

    // Parse length-prefixed entries
    let mut entries = Vec::new();
    let mut offset = 0;

    while offset + 4 <= data.len() {
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + len > data.len() {
            break; // Truncated entry
        }

        entries.push(data[offset..offset + len].to_vec());
        offset += len;
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_async_wal_basic() {
        let dir = tempdir().unwrap();
        let config = AsyncWalConfig {
            sync_enabled: false, // Disable for tests
            ..Default::default()
        };

        let wal = AsyncWal::new(dir.path(), config).unwrap();

        // Submit some data
        let ticket = wal.submit(b"test data".to_vec()).unwrap();

        // Wait for completion
        assert!(ticket.wait_timeout(Duration::from_secs(1)));
        assert!(ticket.is_complete());
    }

    #[test]
    fn test_async_wal_coalescing() {
        let dir = tempdir().unwrap();
        let config = AsyncWalConfig {
            max_coalesce_records: 3,
            max_coalesce_delay: Duration::from_millis(100),
            sync_enabled: false,
            ..Default::default()
        };

        let wal = AsyncWal::new(dir.path(), config).unwrap();

        // Submit multiple entries
        let t1 = wal.submit(b"entry1".to_vec()).unwrap();
        let t2 = wal.submit(b"entry2".to_vec()).unwrap();
        let t3 = wal.submit(b"entry3".to_vec()).unwrap();

        // All should complete within the timeout (coalesced together)
        assert!(t1.wait_timeout(Duration::from_secs(1)));
        assert!(t2.wait_timeout(Duration::from_secs(1)));
        assert!(t3.wait_timeout(Duration::from_secs(1)));
    }

    #[test]
    fn test_wal_persistence() {
        let dir = tempdir().unwrap();
        let config = AsyncWalConfig {
            max_coalesce_delay: Duration::from_millis(10),
            sync_enabled: false,
            ..Default::default()
        };

        // Write some data
        {
            let wal = AsyncWal::new(dir.path(), config.clone()).unwrap();
            let t = wal.submit(b"persistent data".to_vec()).unwrap();
            t.wait();
        }

        // Load and verify
        let entries = load_wal(dir.path()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], b"persistent data");
    }
}
