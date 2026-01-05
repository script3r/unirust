//! # Temporal Module
//!
//! Provides precise temporal modeling with intervals, Allen relations, and temporal operations.
//! All times are normalized to UTC for consistency and correctness.

use serde::{Deserialize, Serialize};
use std::cmp::{max, min, Ordering};
use std::fmt;
use time::{OffsetDateTime, UtcOffset};

/// Represents a temporal instant as UTC epoch seconds
/// Using i64 to support both past and future times, and to avoid floating point issues
pub type Instant = i64;

/// Special sentinel values for open-ended intervals
pub const NEG_INF: Instant = i64::MIN;
pub const POS_INF: Instant = i64::MAX;

/// A temporal interval [start, end) where start < end
///
/// Intervals are half-open: the start time is inclusive, the end time is exclusive.
/// This ensures that adjacent intervals [t0, t1) and [t1, t2) can be merged without gaps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Interval {
    /// Start time (inclusive)
    pub start: Instant,
    /// End time (exclusive)
    pub end: Instant,
}

impl Interval {
    /// Create a new interval with validation
    ///
    /// # Arguments
    /// * `start` - Start time (inclusive)
    /// * `end` - End time (exclusive)
    ///
    /// # Errors
    /// Returns an error if start >= end (zero-length intervals are not allowed)
    pub fn new(start: Instant, end: Instant) -> anyhow::Result<Self> {
        if start >= end {
            anyhow::bail!(
                "Invalid interval: start ({}) must be less than end ({})",
                start,
                end
            );
        }
        Ok(Self { start, end })
    }

    /// Create an interval from UTC OffsetDateTime instances
    pub fn from_utc_datetimes(start: OffsetDateTime, end: OffsetDateTime) -> anyhow::Result<Self> {
        let start_instant = start.unix_timestamp();
        let end_instant = end.unix_timestamp();
        Self::new(start_instant, end_instant)
    }

    /// Create an open-ended interval starting from a specific time
    pub fn from_start(start: Instant) -> Self {
        Self {
            start,
            end: POS_INF,
        }
    }

    /// Create an open-ended interval ending at a specific time
    pub fn until_end(end: Instant) -> Self {
        Self {
            start: NEG_INF,
            end,
        }
    }

    /// Create an interval that covers all time
    pub fn all_time() -> Self {
        Self {
            start: NEG_INF,
            end: POS_INF,
        }
    }

    /// Check if this interval contains a specific instant
    pub fn contains(&self, instant: Instant) -> bool {
        self.start <= instant && instant < self.end
    }

    /// Check if this interval is empty (should never happen with our validation)
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// Get the duration of this interval in seconds
    /// Returns None for intervals with infinite endpoints
    pub fn duration(&self) -> Option<i64> {
        if self.start == NEG_INF || self.end == POS_INF {
            None
        } else {
            Some(self.end - self.start)
        }
    }

    /// Get the duration of this interval, returning 0 for infinite intervals.
    /// Useful for weight calculations where infinite intervals should not dominate.
    #[inline]
    pub fn duration_or_zero(&self) -> i64 {
        self.duration().unwrap_or(0)
    }

    /// Calculate the overlap duration between this interval and another.
    /// Returns 0 if the intervals don't overlap.
    #[inline]
    pub fn overlap_duration(&self, other: &Interval) -> i64 {
        let overlap_start = self.start.max(other.start);
        let overlap_end = self.end.min(other.end);
        if overlap_start < overlap_end {
            overlap_end - overlap_start
        } else {
            0
        }
    }

    /// Check if this interval is finite (has both start and end defined)
    pub fn is_finite(&self) -> bool {
        self.start != NEG_INF && self.end != POS_INF
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let start_str = if self.start == NEG_INF {
            "(-∞".to_string()
        } else {
            format!("[{}", self.start)
        };

        let end_str = if self.end == POS_INF {
            "+∞)".to_string()
        } else {
            format!("{})", self.end)
        };

        write!(f, "{}, {}", start_str, end_str)
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.start.cmp(&other.start) {
            Ordering::Equal => self.end.cmp(&other.end),
            ordering => ordering,
        }
    }
}

/// Allen's interval relations
///
/// These relations describe the temporal relationship between two intervals.
/// All relations are mutually exclusive and collectively exhaustive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AllenRelation {
    /// a precedes b: a.end <= b.start
    Precedes,
    /// a meets b: a.end == b.start (adjacent)
    Meets,
    /// a overlaps b: a.start < b.start < a.end < b.end
    Overlaps,
    /// a starts b: a.start == b.start && a.end < b.end
    Starts,
    /// a during b: b.start < a.start && a.end < b.end
    During,
    /// a finishes b: a.start > b.start && a.end == b.end
    Finishes,
    /// a equals b: a.start == b.start && a.end == b.end
    Equals,
    /// a started by b: b.start == a.start && b.end < a.end
    StartedBy,
    /// a contains b: a.start < b.start && b.end < a.end
    Contains,
    /// a finished by b: b.start < a.start && b.end == a.end
    FinishedBy,
    /// a overlapped by b: b.start < a.start && a.start < b.end < a.end
    OverlappedBy,
    /// a met by b: b.end == a.start
    MetBy,
    /// a preceded by b: b.end <= a.start
    PrecededBy,
}

/// Determine the Allen relation between two intervals
pub fn allen_relation(a: &Interval, b: &Interval) -> AllenRelation {
    use AllenRelation::*;

    match (a.start.cmp(&b.start), a.end.cmp(&b.end)) {
        (Ordering::Equal, Ordering::Equal) => Equals,
        (Ordering::Equal, Ordering::Less) => Starts,
        (Ordering::Equal, Ordering::Greater) => StartedBy,
        (Ordering::Less, Ordering::Equal) => Finishes,
        (Ordering::Less, Ordering::Less) => {
            if a.end < b.start {
                Precedes
            } else if a.end == b.start {
                Meets
            } else if a.end < b.end {
                Overlaps
            } else {
                Contains
            }
        }
        (Ordering::Greater, Ordering::Equal) => FinishedBy,
        (Ordering::Greater, Ordering::Greater) => {
            if b.end < a.start {
                PrecededBy
            } else if b.end == a.start {
                MetBy
            } else if b.end < a.end {
                OverlappedBy
            } else {
                During
            }
        }
        (Ordering::Less, Ordering::Greater) => {
            // a starts before b and ends after b ends
            // This means a contains b, so it's Contains
            Contains
        }
        (Ordering::Greater, Ordering::Less) => {
            // a starts after b and ends before b ends
            // This means a is during b, so it's During
            During
        }
    }
}

/// Check if two intervals are adjacent (meet)
#[inline]
pub fn is_adjacent(a: &Interval, b: &Interval) -> bool {
    a.end == b.start || b.end == a.start
}

/// Check if two intervals overlap
#[inline]
pub fn is_overlapping(a: &Interval, b: &Interval) -> bool {
    // Half-open intervals overlap unless one ends at or before the other's start.
    a.start < b.end && b.start < a.end
}

/// Compute the intersection of two intervals
/// Returns None if the intervals don't overlap
pub fn intersect(a: &Interval, b: &Interval) -> Option<Interval> {
    let start = max(a.start, b.start);
    let end = min(a.end, b.end);

    if start < end {
        Some(Interval { start, end })
    } else {
        None
    }
}

/// Compute the union of two intervals if they overlap or are adjacent
/// Returns None if the intervals are disjoint
pub fn union(a: &Interval, b: &Interval) -> Option<Interval> {
    if is_overlapping(a, b) || is_adjacent(a, b) {
        let start = min(a.start, b.start);
        let end = max(a.end, b.end);
        Some(Interval { start, end })
    } else {
        None
    }
}

/// Compute the difference of interval a minus interval b
/// Returns a vector of intervals representing the parts of a that don't overlap with b
pub fn difference(a: &Interval, b: &Interval) -> Vec<Interval> {
    let intersection = intersect(a, b);
    if intersection.is_none() {
        return vec![*a];
    }

    let mut result = Vec::new();

    // Left part: from a.start to intersection.start
    if a.start < b.start {
        if let Ok(left) = Interval::new(a.start, b.start) {
            result.push(left);
        }
    }

    // Right part: from intersection.end to a.end
    if b.end < a.end {
        if let Ok(right) = Interval::new(b.end, a.end) {
            result.push(right);
        }
    }

    result
}

/// Coalesce a list of intervals with the same value
/// Merges adjacent and overlapping intervals into a minimal set
pub fn coalesce_same_value<T: Clone + PartialEq>(
    intervals: &[(Interval, T)],
) -> Vec<(Interval, T)> {
    if intervals.is_empty() {
        return Vec::new();
    }

    // Sort by start time, then by end time
    let mut sorted: Vec<_> = intervals.to_vec();
    sorted.sort_by_key(|(interval, _)| *interval);

    let mut result = Vec::new();
    let mut current = sorted[0].clone();

    for (interval, value) in sorted.iter().skip(1) {
        // If values are different, start a new interval
        if current.1 != *value {
            result.push(current);
            current = (*interval, value.clone());
            continue;
        }

        // If intervals overlap or are adjacent, merge them
        if let Some(merged) = union(&current.0, interval) {
            current.0 = merged;
        } else {
            // No overlap, start a new interval
            result.push(current);
            current = (*interval, value.clone());
        }
    }

    result.push(current);
    result
}

/// Convert a naive datetime to UTC, handling DST transitions
pub fn naive_to_utc(
    naive: time::PrimitiveDateTime,
    offset: UtcOffset,
) -> anyhow::Result<OffsetDateTime> {
    let dt = naive.assume_offset(offset);
    Ok(dt.to_offset(UtcOffset::UTC))
}

/// Convert a local datetime to UTC using the system timezone
pub fn local_to_utc(local: time::PrimitiveDateTime) -> anyhow::Result<OffsetDateTime> {
    // For simplicity, we'll assume UTC for now
    // In a real implementation, you'd use a timezone library like chrono-tz
    Ok(local.assume_utc())
}

/// Compute atomic intervals from a collection of intervals.
///
/// Atomic intervals are the finest-grained intervals where no boundary points
/// change within them. For example, given intervals [0,10), [5,15), [10,20),
/// the atomic intervals are [0,5), [5,10), [10,15), [15,20).
///
/// This is useful for temporal conflict detection because within each atomic
/// interval, the set of active values is constant, enabling per-slice processing.
pub fn atomic_intervals(intervals: &[Interval]) -> Vec<Interval> {
    if intervals.is_empty() {
        return Vec::new();
    }

    // Collect all boundary points
    let mut points: Vec<Instant> = Vec::with_capacity(intervals.len() * 2);
    for interval in intervals {
        points.push(interval.start);
        points.push(interval.end);
    }

    // Sort and deduplicate
    points.sort_unstable();
    points.dedup();

    if points.len() < 2 {
        return Vec::new();
    }

    // Create atomic intervals between consecutive points
    let mut result = Vec::with_capacity(points.len() - 1);
    for i in 0..points.len() - 1 {
        if let Ok(interval) = Interval::new(points[i], points[i + 1]) {
            result.push(interval);
        }
    }

    result
}

/// Check if an interval encloses another (i.e., completely contains it).
/// An interval A encloses B if A.start <= B.start and A.end >= B.end.
#[inline]
pub fn encloses(outer: &Interval, inner: &Interval) -> bool {
    outer.start <= inner.start && outer.end >= inner.end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_creation() {
        let interval = Interval::new(100, 200).unwrap();
        assert_eq!(interval.start, 100);
        assert_eq!(interval.end, 200);
    }

    #[test]
    fn test_interval_validation() {
        assert!(Interval::new(100, 100).is_err());
        assert!(Interval::new(200, 100).is_err());
    }

    #[test]
    fn test_interval_contains() {
        let interval = Interval::new(100, 200).unwrap();
        assert!(interval.contains(150));
        assert!(interval.contains(100)); // 100 is included in [100, 200)
        assert!(!interval.contains(200)); // 200 is excluded from [100, 200)
        assert!(!interval.contains(50));
        assert!(!interval.contains(250));
    }

    #[test]
    fn test_allen_relations() {
        let a = Interval::new(100, 200).unwrap();
        let b = Interval::new(150, 250).unwrap();
        let c = Interval::new(200, 300).unwrap();
        let d = Interval::new(50, 100).unwrap();

        assert_eq!(allen_relation(&a, &b), AllenRelation::Overlaps);
        assert_eq!(allen_relation(&a, &c), AllenRelation::Meets);
        assert_eq!(allen_relation(&a, &d), AllenRelation::MetBy); // d meets a, so a is met by d
    }

    #[test]
    fn test_intersection() {
        let a = Interval::new(100, 200).unwrap();
        let b = Interval::new(150, 250).unwrap();
        let c = Interval::new(300, 400).unwrap();

        let intersection = intersect(&a, &b).unwrap();
        assert_eq!(intersection.start, 150);
        assert_eq!(intersection.end, 200);

        assert!(intersect(&a, &c).is_none());
    }

    #[test]
    fn test_union() {
        let a = Interval::new(100, 200).unwrap();
        let b = Interval::new(150, 250).unwrap();
        let c = Interval::new(200, 300).unwrap();
        let d = Interval::new(300, 400).unwrap();

        let union_ab = union(&a, &b).unwrap();
        assert_eq!(union_ab.start, 100);
        assert_eq!(union_ab.end, 250);

        let union_ac = union(&a, &c).unwrap();
        assert_eq!(union_ac.start, 100);
        assert_eq!(union_ac.end, 300);

        assert!(union(&a, &d).is_none());
    }

    #[test]
    fn test_difference() {
        let a = Interval::new(100, 300).unwrap();
        let b = Interval::new(150, 250).unwrap();

        let diff = difference(&a, &b);
        assert_eq!(diff.len(), 2);
        assert_eq!(diff[0].start, 100);
        assert_eq!(diff[0].end, 150);
        assert_eq!(diff[1].start, 250);
        assert_eq!(diff[1].end, 300);
    }

    #[test]
    fn test_coalesce_same_value() {
        let intervals = vec![
            (Interval::new(100, 200).unwrap(), "A"),
            (Interval::new(150, 250).unwrap(), "A"),
            (Interval::new(200, 300).unwrap(), "A"),
            (Interval::new(400, 500).unwrap(), "A"),
        ];

        let coalesced = coalesce_same_value(&intervals);
        assert_eq!(coalesced.len(), 2);
        assert_eq!(coalesced[0].0.start, 100);
        assert_eq!(coalesced[0].0.end, 300);
        assert_eq!(coalesced[1].0.start, 400);
        assert_eq!(coalesced[1].0.end, 500);
    }

    #[test]
    fn test_infinite_intervals() {
        let all_time = Interval::all_time();
        let from_start = Interval::from_start(100);
        let until_end = Interval::until_end(200);

        assert_eq!(all_time.start, NEG_INF);
        assert_eq!(all_time.end, POS_INF);
        assert_eq!(from_start.start, 100);
        assert_eq!(from_start.end, POS_INF);
        assert_eq!(until_end.start, NEG_INF);
        assert_eq!(until_end.end, 200);
    }

    #[test]
    fn test_adjacent_intervals() {
        let a = Interval::new(100, 200).unwrap();
        let b = Interval::new(200, 300).unwrap();
        let c = Interval::new(300, 400).unwrap();

        assert!(is_adjacent(&a, &b));
        assert!(is_adjacent(&b, &c));
        assert!(!is_adjacent(&a, &c));
    }

    #[test]
    fn test_overlapping_intervals() {
        let a = Interval::new(100, 200).unwrap();
        let b = Interval::new(150, 250).unwrap();
        let c = Interval::new(200, 300).unwrap();
        let d = Interval::new(300, 400).unwrap();

        assert!(is_overlapping(&a, &b));
        assert!(!is_overlapping(&a, &c));
        assert!(!is_overlapping(&a, &d));
    }

    #[test]
    fn test_atomic_intervals() {
        // Given intervals [0,10), [5,15), [10,20)
        // Atomic intervals should be [0,5), [5,10), [10,15), [15,20)
        let intervals = vec![
            Interval::new(0, 10).unwrap(),
            Interval::new(5, 15).unwrap(),
            Interval::new(10, 20).unwrap(),
        ];

        let atoms = atomic_intervals(&intervals);
        assert_eq!(atoms.len(), 4);
        assert_eq!(atoms[0], Interval::new(0, 5).unwrap());
        assert_eq!(atoms[1], Interval::new(5, 10).unwrap());
        assert_eq!(atoms[2], Interval::new(10, 15).unwrap());
        assert_eq!(atoms[3], Interval::new(15, 20).unwrap());
    }

    #[test]
    fn test_atomic_intervals_empty() {
        let atoms = atomic_intervals(&[]);
        assert!(atoms.is_empty());
    }

    #[test]
    fn test_encloses() {
        let outer = Interval::new(0, 100).unwrap();
        let inner = Interval::new(10, 50).unwrap();
        let partial = Interval::new(50, 150).unwrap();

        assert!(encloses(&outer, &inner));
        assert!(!encloses(&outer, &partial));
        assert!(encloses(&outer, &outer)); // Self-enclosure
    }
}
