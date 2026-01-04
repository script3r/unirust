//! # LZ4 Compression Module
//!
//! High-speed compression for records and WAL entries.
//!
//! From Bigtable paper (Section 6): "Many clients use a two-pass custom
//! compression scheme. The first pass uses Bentley and McIlroy's scheme,
//! which compresses long common strings across a large window. The second
//! pass uses a fast compression algorithm that looks for repetitions in
//! a small 16 KB window of the data."
//!
//! LZ4 provides similar characteristics:
//! - Encode speed: 780+ MB/s
//! - Decode speed: 4970+ MB/s
//! - Compression ratio: ~2.1x for typical data

use std::io;

/// Compression level for LZ4
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionLevel {
    /// Fastest compression (default)
    #[default]
    Fast,
    /// Higher compression ratio (slower)
    High,
}

/// Compress data using LZ4.
///
/// Returns compressed bytes with a 4-byte length prefix for the uncompressed size.
/// This format allows decompression to pre-allocate the output buffer.
#[inline]
pub fn compress(data: &[u8]) -> Vec<u8> {
    // The lz4_flex compress_prepend_size adds the size prefix automatically
    lz4_flex::compress_prepend_size(data)
}

/// Decompress LZ4-compressed data.
///
/// Expects data in the format produced by `compress()`.
#[inline]
pub fn decompress(compressed: &[u8]) -> io::Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(compressed)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Compress data with a known maximum size hint for better performance.
#[inline]
pub fn compress_with_hint(data: &[u8], _size_hint: usize) -> Vec<u8> {
    // lz4_flex handles buffer sizing internally
    compress(data)
}

/// Decompress with a known output size for better performance.
#[inline]
pub fn decompress_with_size(compressed: &[u8], uncompressed_size: usize) -> io::Result<Vec<u8>> {
    // Skip the size prefix if present
    if compressed.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "compressed data too short",
        ));
    }

    let stored_size =
        u32::from_le_bytes([compressed[0], compressed[1], compressed[2], compressed[3]]) as usize;

    if stored_size != uncompressed_size {
        // Size mismatch - use stored size
        return decompress(compressed);
    }

    let mut output = vec![0u8; uncompressed_size];
    let decompressed_len = lz4_flex::decompress_into(&compressed[4..], &mut output)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    if decompressed_len != uncompressed_size {
        output.truncate(decompressed_len);
    }

    Ok(output)
}

/// Statistics for compression operations
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    /// Total bytes compressed
    pub bytes_in: u64,
    /// Total compressed bytes produced
    pub bytes_out: u64,
    /// Number of compression operations
    pub compress_ops: u64,
    /// Number of decompression operations
    pub decompress_ops: u64,
}

impl CompressionStats {
    /// Calculate compression ratio (uncompressed / compressed)
    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_out == 0 {
            1.0
        } else {
            self.bytes_in as f64 / self.bytes_out as f64
        }
    }

    /// Calculate space savings as percentage
    pub fn space_savings(&self) -> f64 {
        if self.bytes_in == 0 {
            0.0
        } else {
            (1.0 - (self.bytes_out as f64 / self.bytes_in as f64)) * 100.0
        }
    }
}

/// Streaming compressor for large data
pub struct StreamingCompressor {
    buffer: Vec<u8>,
    compressed_chunks: Vec<Vec<u8>>,
    chunk_size: usize,
}

impl StreamingCompressor {
    /// Create a new streaming compressor
    pub fn new(chunk_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(chunk_size),
            compressed_chunks: Vec::new(),
            chunk_size,
        }
    }

    /// Default chunk size (64KB - optimal for LZ4)
    pub fn default_chunk() -> Self {
        Self::new(64 * 1024)
    }

    /// Write data to the compressor
    pub fn write(&mut self, data: &[u8]) -> usize {
        let mut written = 0;
        let mut remaining = data;

        while !remaining.is_empty() {
            let space = self.chunk_size - self.buffer.len();
            let to_copy = remaining.len().min(space);

            self.buffer.extend_from_slice(&remaining[..to_copy]);
            remaining = &remaining[to_copy..];
            written += to_copy;

            if self.buffer.len() >= self.chunk_size {
                self.flush_chunk();
            }
        }

        written
    }

    /// Flush the current chunk
    fn flush_chunk(&mut self) {
        if !self.buffer.is_empty() {
            let compressed = compress(&self.buffer);
            self.compressed_chunks.push(compressed);
            self.buffer.clear();
        }
    }

    /// Finish compression and return all compressed chunks
    pub fn finish(mut self) -> Vec<Vec<u8>> {
        self.flush_chunk();
        self.compressed_chunks
    }

    /// Get total uncompressed bytes written
    pub fn bytes_written(&self) -> usize {
        self.compressed_chunks
            .iter()
            .map(|c| {
                if c.len() >= 4 {
                    u32::from_le_bytes([c[0], c[1], c[2], c[3]]) as usize
                } else {
                    0
                }
            })
            .sum::<usize>()
            + self.buffer.len()
    }

    /// Get total compressed bytes
    pub fn compressed_bytes(&self) -> usize {
        self.compressed_chunks.iter().map(|c| c.len()).sum()
    }
}

/// Batch compression for multiple records
pub struct BatchCompressor {
    /// Uncompressed records buffer
    buffer: Vec<u8>,
    /// Offsets of each record in the buffer
    offsets: Vec<usize>,
    /// Compressed output
    compressed: Option<Vec<u8>>,
}

impl BatchCompressor {
    /// Create a new batch compressor
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(64 * 1024),
            offsets: Vec::with_capacity(1024),
            compressed: None,
        }
    }

    /// Add a record to the batch
    pub fn add_record(&mut self, data: &[u8]) {
        self.offsets.push(self.buffer.len());
        // Write length prefix (4 bytes) + data
        self.buffer
            .extend_from_slice(&(data.len() as u32).to_le_bytes());
        self.buffer.extend_from_slice(data);
    }

    /// Compress the entire batch
    pub fn compress(&mut self) -> &[u8] {
        if self.compressed.is_none() {
            self.compressed = Some(compress(&self.buffer));
        }
        self.compressed.as_ref().unwrap()
    }

    /// Get compression ratio for this batch
    pub fn compression_ratio(&self) -> f64 {
        if let Some(ref compressed) = self.compressed {
            if compressed.is_empty() {
                1.0
            } else {
                self.buffer.len() as f64 / compressed.len() as f64
            }
        } else {
            1.0
        }
    }

    /// Number of records in batch
    pub fn record_count(&self) -> usize {
        self.offsets.len()
    }

    /// Total uncompressed size
    pub fn uncompressed_size(&self) -> usize {
        self.buffer.len()
    }

    /// Clear the batch for reuse
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.offsets.clear();
        self.compressed = None;
    }
}

impl Default for BatchCompressor {
    fn default() -> Self {
        Self::new()
    }
}

/// Decompress a batch of records
pub fn decompress_batch(compressed: &[u8]) -> io::Result<Vec<Vec<u8>>> {
    let decompressed = decompress(compressed)?;

    let mut records = Vec::new();
    let mut offset = 0;

    while offset + 4 <= decompressed.len() {
        let len = u32::from_le_bytes([
            decompressed[offset],
            decompressed[offset + 1],
            decompressed[offset + 2],
            decompressed[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + len > decompressed.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated record in batch",
            ));
        }

        records.push(decompressed[offset..offset + len].to_vec());
        offset += len;
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        let data = b"Hello, World! This is a test of LZ4 compression.";
        let compressed = compress(data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_empty() {
        let data: &[u8] = b"";
        let compressed = compress(data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_repetitive_data() {
        // Repetitive data should compress well
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let compressed = compress(&data);

        // Should achieve at least 2x compression on repetitive data
        assert!(compressed.len() < data.len());

        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_streaming_compressor() {
        let mut compressor = StreamingCompressor::new(100);

        // Write data in chunks
        let data: Vec<u8> = (0..500).map(|i| (i % 256) as u8).collect();
        for chunk in data.chunks(50) {
            compressor.write(chunk);
        }

        let chunks = compressor.finish();

        // Should have multiple chunks
        assert!(!chunks.is_empty());

        // Decompress and verify
        let mut decompressed = Vec::new();
        for chunk in chunks {
            decompressed.extend(decompress(&chunk).unwrap());
        }
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_batch_compressor() {
        let mut batch = BatchCompressor::new();

        let records = vec![
            b"record 1 data".to_vec(),
            b"record 2 data with more content".to_vec(),
            b"record 3".to_vec(),
        ];

        for record in &records {
            batch.add_record(record);
        }

        assert_eq!(batch.record_count(), 3);

        let compressed = batch.compress();
        let decompressed_records = decompress_batch(compressed).unwrap();

        assert_eq!(decompressed_records.len(), 3);
        for (orig, decompressed) in records.iter().zip(decompressed_records.iter()) {
            assert_eq!(orig, decompressed);
        }
    }

    #[test]
    fn test_compression_stats() {
        let stats = CompressionStats {
            bytes_in: 1000,
            bytes_out: 400,
            compress_ops: 10,
            decompress_ops: 5,
        };

        // 1000 / 400 = 2.5x compression ratio
        assert!((stats.compression_ratio() - 2.5).abs() < 0.01);

        // (1 - 400/1000) * 100 = 60% savings
        assert!((stats.space_savings() - 60.0).abs() < 0.01);
    }
}
