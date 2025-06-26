/// A concurrent, space-efficient bit array implementation for tracking active bits.
///
/// # Design
///
/// The `ActiveBits` structure is designed for efficient management of sparse bit arrays,
/// particularly useful for tracking active or in-use items in a large address space.
/// It uses a segmented approach where bits are stored in fixed-size segments to optimize
/// memory usage for sparse bit patterns.
///
/// # Features
///
/// - Thread-safe operations using atomic operations
/// - Automatic segment allocation and deallocation
/// - Space-efficient storage for sparse bit patterns
/// - Support for large bit indices (up to u64::MAX)
/// - Lock-free reads and writes
///
/// # Implementation Details
///
/// The implementation uses a two-level structure:
/// - A concurrent hash map (`DashMap`) storing segments
/// - Each segment containing a fixed number of bits (32,768) using atomic integers
/// - Automatic segment cleanup when all bits in a segment are cleared
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, Ordering};

/// Number of 32-bit integers per segment
const U32_COUNT: u64 = 1024;
/// Total number of bits per segment
const BIT_COUNT: u64 = 32 * U32_COUNT;

/// A fixed-size segment of bits implemented using atomic integers.
///
/// Each segment manages 32,768 bits (1024 * 32) and keeps track of how many
/// bits are set using an atomic counter.
struct Segment {
    /// Number of bits currently set in this segment
    count: AtomicU32,
    /// Array of atomic integers storing the actual bits
    arr: [AtomicU32; U32_COUNT as usize],
}

#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU32 = AtomicU32::new(0);

impl Segment {
    /// Creates a new empty segment with all bits cleared.
    fn new() -> Self {
        Self {
            count: ZERO,
            arr: [ZERO; U32_COUNT as usize],
        }
    }

    /// Checks if a specific bit is set within this segment.
    ///
    /// # Arguments
    ///
    /// * `n` - The bit index within the segment (0 to BIT_COUNT-1)
    ///
    /// # Returns
    ///
    /// * `true` if the bit is set, `false` otherwise
    fn get(&self, n: usize) -> bool {
        let (i, j) = (n / 32, n % 32);
        let mask = 1u32 << j;
        let old = self.arr[i].load(Ordering::SeqCst);
        (old & mask) != 0
    }

    /// Sets a specific bit within this segment.
    ///
    /// If the bit was not previously set, the segment's count is incremented.
    ///
    /// # Arguments
    ///
    /// * `n` - The bit index within the segment (0 to BIT_COUNT-1)
    fn set(&self, n: usize) {
        let (i, j) = (n / 32, n % 32);
        let mask = 1u32 << j;
        let old = self.arr[i].fetch_or(mask, Ordering::SeqCst);
        if (old & mask) == 0 {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Clears a specific bit within this segment.
    ///
    /// If the bit was previously set, the segment's count is decremented.
    ///
    /// # Arguments
    ///
    /// * `n` - The bit index within the segment (0 to BIT_COUNT-1)
    ///
    /// # Returns
    ///
    /// * `true` if this was the last set bit in the segment (count is now 0),
    ///   indicating the segment can be removed
    fn clear(&self, n: usize) -> bool {
        let (i, j) = (n / 32, n % 32);
        let mask = 1u32 << j;
        let old = self.arr[i].fetch_and(!mask, Ordering::SeqCst);
        if (old & mask) != 0 {
            let old_count = self.count.fetch_sub(1, Ordering::SeqCst);
            return old_count == 1; //need to remove myself
        }
        false
    }
}

/// A concurrent, segmented bit array for tracking active bits.
///
/// This structure provides thread-safe operations for setting, clearing, and checking
/// bits across a large address space. It automatically manages memory by allocating
/// segments only when needed and removing them when empty.
pub struct ActiveBits {
    /// Map of segment indices to segments, using DashMap for concurrent access
    m: DashMap<u64, Box<Segment>>,
}

impl ActiveBits {
    /// Creates a new `ActiveBits` instance with the specified initial capacity.
    ///
    /// # Arguments
    ///
    /// * `n` - The initial number of segments to allocate space for
    pub fn with_capacity(n: usize) -> Self {
        Self {
            m: DashMap::with_capacity(n),
        }
    }

    /// Checks if a specific bit is set.
    ///
    /// # Arguments
    ///
    /// * `n` - The global bit index to check
    ///
    /// # Returns
    ///
    /// * `true` if the bit is set, `false` otherwise
    pub fn get(&self, n: u64) -> bool {
        let (i, j) = (n / BIT_COUNT, n % BIT_COUNT);
        if let Some(seg) = self.m.get(&i) {
            return seg.get(j as usize);
        }
        false
    }

    /// Sets a specific bit.
    ///
    /// If the segment containing the bit doesn't exist, it will be created.
    ///
    /// # Arguments
    ///
    /// * `n` - The global bit index to set
    pub fn set(&self, n: u64) {
        let (i, j) = (n / BIT_COUNT, n % BIT_COUNT);
        let need_allocate = {
            if let Some(seg) = self.m.get(&i) {
                seg.set(j as usize);
                false
            } else {
                true
            }
        };
        if need_allocate {
            let seg = Box::new(Segment::new());
            seg.set(j as usize);
            self.m.insert(i, seg);
        }
    }

    /// Clears a specific bit.
    ///
    /// If this was the last set bit in a segment, the segment will be removed
    /// to free memory.
    ///
    /// # Arguments
    ///
    /// * `n` - The global bit index to clear
    pub fn clear(&self, n: u64) {
        let (i, j) = (n / BIT_COUNT, n % BIT_COUNT);
        let need_remove = {
            if let Some(seg) = self.m.get(&i) {
                seg.clear(j as usize)
            } else {
                false
            }
        };
        if need_remove {
            self.m.remove(&i);
        }
    }
}

#[cfg(test)]
mod segments_tests {
    use super::*;

    #[test]
    fn test_segment() {
        let segment = Segment::new();

        // Test set and get
        assert!(!segment.get(0));
        segment.set(0);
        assert!(segment.get(0));
        assert!(!segment.get(1));

        // Test clear
        assert!(!segment.clear(1)); // Clearing unset bit
        assert!(segment.clear(0)); // Clearing last set bit
        assert!(!segment.get(0));

        // Test multiple bits
        segment.set(31);
        segment.set(32);
        assert!(segment.get(31));
        assert!(segment.get(32));
        assert!(!segment.get(33));

        // Test count
        assert_eq!(segment.count.load(Ordering::SeqCst), 2);
        segment.clear(31);
        assert_eq!(segment.count.load(Ordering::SeqCst), 1);
        segment.clear(32);
        assert_eq!(segment.count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_segment_edge_cases() {
        let segment = Segment::new();

        // Test first and last bits
        segment.set(0);
        segment.set(BIT_COUNT as usize - 1);
        assert!(segment.get(0));
        assert!(segment.get(BIT_COUNT as usize - 1));
        assert_eq!(segment.count.load(Ordering::SeqCst), 2);

        // Test setting already set bit
        segment.set(0);
        assert_eq!(segment.count.load(Ordering::SeqCst), 2);

        // Test clearing unset bit
        assert!(!segment.clear(1));
        assert_eq!(segment.count.load(Ordering::SeqCst), 2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_set_clear() {
        let ab = ActiveBits::with_capacity(10);

        // Test set and get
        ab.set(42);
        assert!(ab.get(42));
        assert!(!ab.get(43));

        // Test clear
        ab.clear(42);
        assert!(!ab.get(42));
    }

    #[test]
    fn test_large_numbers() {
        let ab = ActiveBits::with_capacity(10);

        let large_num = u64::MAX - 1;
        ab.set(large_num);
        assert!(ab.get(large_num));
        assert!(!ab.get(large_num - 1));
        assert!(!ab.get(large_num + 1));

        ab.clear(large_num);
        assert!(!ab.get(large_num));
    }

    #[test]
    fn test_multiple_segments() {
        let ab = ActiveBits::with_capacity(10);

        let num1 = BIT_COUNT - 1;
        let num2 = BIT_COUNT;
        let num3 = BIT_COUNT + 1;

        ab.set(num1);
        ab.set(num2);
        ab.set(num3);

        assert!(ab.get(num1));
        assert!(ab.get(num2));
        assert!(ab.get(num3));

        ab.clear(num2);
        assert!(ab.get(num1));
        assert!(!ab.get(num2));
        assert!(ab.get(num3));
    }
}
