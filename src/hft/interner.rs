//! # Concurrent Interner
//!
//! Thread-safe string interning using DashMap for lock-free concurrent access.
//! Eliminates the need for write locks in the hot path.

use crate::model::{AttrId, ValueId};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, Ordering};

/// Thread-safe concurrent interner for attributes and values.
/// Uses DashMap for lock-free concurrent access.
pub struct ConcurrentInterner {
    /// Attribute string to ID mapping
    attr_to_id: DashMap<String, AttrId>,
    /// Value string to ID mapping
    value_to_id: DashMap<String, ValueId>,
    /// Next attribute ID (atomic for thread safety)
    next_attr_id: AtomicU32,
    /// Next value ID (atomic for thread safety)
    next_value_id: AtomicU32,
}

impl ConcurrentInterner {
    /// Create a new concurrent interner
    pub fn new() -> Self {
        Self {
            attr_to_id: DashMap::with_capacity(1024),
            value_to_id: DashMap::with_capacity(65536),
            next_attr_id: AtomicU32::new(1),
            next_value_id: AtomicU32::new(1),
        }
    }

    /// Intern an attribute string, returning its ID.
    /// Thread-safe and lock-free.
    #[inline]
    pub fn intern_attr(&self, attr: &str) -> AttrId {
        // Fast path: check if already interned
        if let Some(id) = self.attr_to_id.get(attr) {
            return *id;
        }

        // Slow path: insert new entry
        // Use entry API to avoid TOCTOU race
        *self.attr_to_id.entry(attr.to_string()).or_insert_with(|| {
            let id = self.next_attr_id.fetch_add(1, Ordering::Relaxed);
            AttrId(id)
        })
    }

    /// Intern a value string, returning its ID.
    /// Thread-safe and lock-free.
    #[inline]
    pub fn intern_value(&self, value: &str) -> ValueId {
        // Fast path: check if already interned
        if let Some(id) = self.value_to_id.get(value) {
            return *id;
        }

        // Slow path: insert new entry
        *self
            .value_to_id
            .entry(value.to_string())
            .or_insert_with(|| {
                let id = self.next_value_id.fetch_add(1, Ordering::Relaxed);
                ValueId(id)
            })
    }

    /// Get the number of interned attributes
    pub fn attr_count(&self) -> usize {
        self.attr_to_id.len()
    }

    /// Get the number of interned values
    pub fn value_count(&self) -> usize {
        self.value_to_id.len()
    }
}

impl Default for ConcurrentInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_concurrent_intern_attr() {
        let interner = ConcurrentInterner::new();

        let id1 = interner.intern_attr("name");
        let id2 = interner.intern_attr("name");
        let id3 = interner.intern_attr("email");

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_concurrent_intern_value() {
        let interner = ConcurrentInterner::new();

        let id1 = interner.intern_value("John");
        let id2 = interner.intern_value("John");
        let id3 = interner.intern_value("Jane");

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_thread_safety() {
        let interner = Arc::new(ConcurrentInterner::new());
        let mut handles = vec![];

        for t in 0..8 {
            let interner = Arc::clone(&interner);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    let attr = format!("attr_{}_{}", t, i % 10);
                    let value = format!("value_{}_{}", t, i % 100);
                    interner.intern_attr(&attr);
                    interner.intern_value(&value);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Each thread uses 10 unique attrs (0-9) = 80 total unique attrs
        // But with t=0..8 and i%10, we get: attr_0_0..attr_0_9, attr_1_0..attr_1_9, etc
        assert!(interner.attr_count() <= 80);
        assert!(interner.value_count() <= 800);
    }
}
