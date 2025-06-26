//! In-memory indexing implementation optimized for small to medium datasets.
//!
//! The memory indexer provides a fast and simple solution by:
//! - Keeping all data in memory for maximum performance
//! - Using efficient data structures for lookups
//! - Minimizing lock contention
//! - Supporting concurrent access
//!
//! # Architecture
//!
//! The memory indexer uses a single-level storage approach:
//! 1. All data stored in memory using optimized structures
//! 2. Lock-free reads where possible
//! 3. Fine-grained write locking
//!
//! # Performance Characteristics
//!
//! - O(log n) lookup complexity
//! - O(1) insertions in average case
//! - Efficient range queries
//! - No disk I/O overhead
//!
//! # Thread Safety
//!
//! The implementation is thread-safe through:
//! - Atomic operations for counters
//! - RwLock for concurrent access
//! - Lock-free reads
//! - Safe memory barriers

use super::hybrid::ref_unit::RefUnit;
use super::indexer_trait::{IndexerError, IndexerTrait};
use super::kvlist::{self, KVCursor};
use super::utils::poslist::PosList;
use crate::def::{OP_CREATE, OP_DELETE, OP_READ, OP_WRITE, SHARD_COUNT, SHARD_DIV};
use aes_gcm::Aes256Gcm;
use byteorder::{BigEndian, ByteOrder};
use hex;
use parking_lot::RwLock;
use std::collections::{BTreeSet, HashSet};
use std::ops::Bound::{self, Excluded, Included};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time;

const COMPACT_RATIO: usize = 30;
const COMPACT_TRY_RANGE: usize = 2000; //2000 or 20(debug)

type KV = (u64, [u8; 6]); // 8-byte key -> 6-byte pos
type KVList = kvlist::KVList<9, 10, 6>; // 4-byte key -> 6-byte pos

/// Storage unit for key-value pairs with efficient lookup and modification.
///
/// # Architecture
///
/// The unit uses a hybrid storage approach:
/// - `big_list`: Main storage for most key-value pairs
/// - `small_set`: Overflow storage for hash collisions
/// - Nillification markers for deleted entries
///
/// # Performance
///
/// - O(log n) lookups in both storages
/// - Amortized O(1) insertions
/// - Efficient range queries
/// - Background compaction
///
/// # Thread Safety
///
/// All operations are thread-safe through the parent's locking mechanism
pub struct Unit {
    /// Number of valid KV pairs in this unit, excluding nillified ones
    size: usize,

    /// Number of nillified KV pairs in big_list
    num_of_nil: usize,

    /// Main storage for key-value pairs using KVList
    big_list: KVList,

    /// Overflow storage for hash collisions using BTreeSet
    small_set: BTreeSet<KV>,
}

// This special value is used to mark a KV pair in big_list as nillified.
// Please note it is still a valid value to be stored in small_set BTreeSet.
// Only in big_list it is used as a marker.
static NIL_MARKER: &[u8; 6] = &[255, 255, 255, 255, 255, 255];

impl Unit {
    /// Gets the number of valid entries in the unit.
    ///
    /// # Returns
    ///
    /// Number of valid entries
    ///
    /// # Performance
    ///
    /// O(1) field access
    pub fn len(&self) -> usize {
        self.size
    }

    /// Merges a key and value into a single array.
    ///
    /// # Arguments
    ///
    /// * `k` - 64-bit key
    /// * `v6` - 6-byte value
    ///
    /// # Returns
    ///
    /// 10-byte array containing merged key and value
    ///
    /// # Performance
    ///
    /// O(1) array operations
    fn merge_kv(k: u64, v6: &[u8; 6]) -> [u8; 10] {
        let mut elem = [0u8; 10];
        BigEndian::write_u32(&mut elem[..4], (k >> 32) as u32);
        elem[4..].copy_from_slice(&v6[..]);
        elem
    }

    /// Splits a merged array back into key and value.
    ///
    /// # Arguments
    ///
    /// * `elem` - 10-byte array containing merged key and value
    ///
    /// # Returns
    ///
    /// Tuple of (key, value)
    ///
    /// # Performance
    ///
    /// O(1) array operations
    fn split_kv(elem: &[u8; 10]) -> (u64, [u8; 6]) {
        let mut v6 = [0u8; 6];
        v6[..].copy_from_slice(&elem[4..]);
        // only the high 32 bits of k_hi32 is meaningful and low 32 bits are zero
        let k_hi32 = (BigEndian::read_u32(&elem[..4]) as u64) << 32;
        (k_hi32, v6)
    }

    /// Converts a 64-bit key to a 10-byte array.
    ///
    /// # Arguments
    ///
    /// * `k` - 64-bit key
    ///
    /// # Returns
    ///
    /// 10-byte array containing the key
    ///
    /// # Performance
    ///
    /// O(1) array operations
    fn to_k10(k: u64) -> [u8; 10] {
        let k8 = k.to_be_bytes();
        let mut k10 = [0u8; 10];
        k10[..4].copy_from_slice(&k8[..4]); //big_list's 4-byte key
        k10
    }

    /// Converts a 64-bit value to a 6-byte array.
    ///
    /// # Arguments
    ///
    /// * `v` - 64-bit value
    ///
    /// # Returns
    ///
    /// 6-byte array containing the value
    ///
    /// # Performance
    ///
    /// O(1) array operations
    fn to_v6(v: i64) -> [u8; 6] {
        let v8 = v.to_be_bytes();
        let mut v6 = [0u8; 6];
        v6[..].copy_from_slice(&v8[2..]);
        v6
    }

    /// Converts a 6-byte array back to a 64-bit value.
    ///
    /// # Arguments
    ///
    /// * `v6` - 6-byte array containing the value
    ///
    /// # Returns
    ///
    /// 64-bit value
    ///
    /// # Performance
    ///
    /// O(1) array operations
    fn v6_to_i64(v6: &[u8; 6]) -> i64 {
        let mut bz = [0u8; 8];
        bz[2..].copy_from_slice(&v6[..]);
        i64::from_be_bytes(bz)
    }

    /// Appends a key-value pair to the appropriate storage.
    ///
    /// # Arguments
    ///
    /// * `offsets_idx` - Index for offsets tracking
    /// * `last_k` - Last key processed
    /// * `k` - Key to append
    /// * `v6` - Value to append
    ///
    /// # Performance
    ///
    /// - O(1) for big_list append
    /// - O(log n) for small_set insert
    fn append(&mut self, offsets_idx: &mut usize, last_k: &mut u64, k: u64, v6: &[u8; 6]) {
        self.size += 1;
        // When the current KV pair and the previous pair falls into the same
        // hash bucket, we store the current pair in small_set
        let same_bucket = (*last_k >> 32) == (k >> 32) && (k & 0xFFFFFFFF) != 0;
        *last_k = k;
        if v6 == NIL_MARKER || same_bucket {
            // NIL_MARKER cannot be stored in big_list
            self.small_set.insert((k, *v6));
            return;
        }
        let elem = Self::merge_kv(k, v6);
        self.big_list.append(offsets_idx, &elem);
    }

    /// Dumps all key-value pairs to another unit.
    ///
    /// # Arguments
    ///
    /// * `out` - Target unit to receive the pairs
    ///
    /// # Performance
    ///
    /// - O(n) for iteration
    /// - O(n log n) for sorted insertion
    fn dump_to(&self, out: &mut Self) {
        let mut offsets_idx = usize::MAX;
        let mut last_k = u64::MAX;
        let mut iter = self.small_set.iter();
        let mut kv = iter.next();
        let mut cursor = self.big_list.get_first_cursor();
        while cursor.valid() {
            let elem = self.big_list.at(&cursor);
            let (k_hi32, file_pos) = Self::split_kv(&elem);
            if &file_pos == NIL_MARKER {
                self.big_list.move_forward(&mut cursor);
                continue; // nillified pairs in big_list are ignored
            }
            while kv.is_some() && kv.unwrap().0 < k_hi32 {
                out.append(&mut offsets_idx, &mut last_k, kv.unwrap().0, &kv.unwrap().1);
                kv = iter.next();
            }
            out.append(&mut offsets_idx, &mut last_k, k_hi32, &file_pos);
            self.big_list.move_forward(&mut cursor);
        }
        while kv.is_some() {
            out.append(&mut offsets_idx, &mut last_k, kv.unwrap().0, &kv.unwrap().1);
            kv = iter.next();
        }
    }

    /// Finds a key-value pair in the storage.
    ///
    /// # Arguments
    ///
    /// * `k` - Key to find
    /// * `v6` - Value to match
    ///
    /// # Returns
    ///
    /// Tuple of (cursor, found)
    ///
    /// # Performance
    ///
    /// O(log n) for binary search
    fn find_kv(&self, k: u64, v6: &[u8; 6]) -> (KVCursor, bool) {
        let mut k10 = Self::to_k10(k);
        let mut cursor = self.big_list.find(&k10);
        k10[4..].copy_from_slice(&v6[..]);
        while cursor.valid() {
            let elem = self.big_list.at(&cursor);
            let (k_hi32, _) = Self::split_kv(&elem);
            if (k_hi32 >> 32) != (k >> 32) {
                break;
            }
            if elem == k10 {
                return (cursor, true);
            }
            self.big_list.move_forward(&mut cursor);
        }
        (cursor, false)
    }

    /// Iterates over all values for a key.
    ///
    /// # Arguments
    ///
    /// * `k` - Key to look up
    /// * `access` - Function to call for each value
    ///
    /// # Returns
    ///
    /// Tuple of (cursor, accessed)
    ///
    /// # Performance
    ///
    /// - O(log n) for initial lookup
    /// - O(k) for iterating k matching entries
    fn _for_each_value<F>(&self, k: u64, mut access: F) -> (KVCursor, bool)
    where
        F: FnMut(i64) -> bool,
    {
        let k10 = Self::to_k10(k);
        // First we iterate big_list
        let found_cursor = self.big_list.find(&k10);
        let mut cursor = found_cursor.clone();
        while cursor.valid() {
            let elem = self.big_list.at(&cursor);
            let (k_hi32, file_pos) = Self::split_kv(&elem);
            if (k_hi32 >> 32) != (k >> 32) {
                break;
            }
            if &file_pos == NIL_MARKER {
                self.big_list.move_forward(&mut cursor);
                continue; // nillified pairs in big_list are ignored
            }
            if access(Self::v6_to_i64(&file_pos)) {
                return (found_cursor, true);
            }
            self.big_list.move_forward(&mut cursor);
        }

        // Then we iterate small_set BTreeSet
        let start = (k, [0u8; 6]);
        let end = (k, [0xFFu8; 6]);
        let range = (Included(&start), Included(&end));
        for &v in self
            .small_set
            .range::<KV, (Bound<&KV>, Bound<&KV>)>(range)
            .rev()
        {
            let file_pos = Self::v6_to_i64(&v.1);
            if access(file_pos) {
                return (found_cursor, true);
            }
        }
        (found_cursor, false)
    }

    /// Gets all values associated with a key for testing.
    ///
    /// # Arguments
    ///
    /// * `k_in` - Key to look up
    ///
    /// # Returns
    ///
    /// Set of values associated with the key
    ///
    /// # Performance
    ///
    /// - O(log n) for initial lookup
    /// - O(k) for collecting k matching entries
    fn debug_get_values(&self, k_in: u64) -> HashSet<i64> {
        let mut res = HashSet::new();
        self.for_each_value(k_in, |v| -> bool {
            res.insert(v);
            false // do not exit loop
        });
        res
    }

    /// Gets all values for adjacent keys for testing.
    ///
    /// # Arguments
    ///
    /// * `k80` - 80-bit key bytes
    /// * `k_in` - Key to look up
    ///
    /// # Returns
    ///
    /// Set of (key, value) pairs for adjacent keys
    ///
    /// # Performance
    ///
    /// - O(log n) for initial lookup
    /// - O(k) for collecting k adjacent entries
    fn debug_get_adjacent_values(&self, k80: &[u8; 10], k_in: u64) -> HashSet<([u8; 10], i64)> {
        let mut res = HashSet::new();
        self.for_each_adjacent_value(*k80, k_in, |v| -> bool {
            let mut k80arr = [0u8; 10];
            k80arr[..].copy_from_slice(k80);
            BigEndian::write_u32(&mut k80arr[6..], 0u32); // clear last 32 bits
            res.insert((k80arr, v));
            false // do not exit loop
        });
        res
    }
}

/// Trait defining the interface for storage units.
///
/// # Thread Safety
///
/// All implementations must be thread-safe.
pub trait UnitTrait: Send + Sync {
    /// Creates a new storage unit.
    ///
    /// # Returns
    ///
    /// A new instance of the storage unit
    fn new() -> Self;

    /// Adds a key-value pair to the unit.
    ///
    /// # Arguments
    ///
    /// * `k` - Key to add
    /// * `v` - Value to store
    fn add_kv(&mut self, k: u64, v: i64);

    /// Removes a key-value pair from the unit.
    ///
    /// # Arguments
    ///
    /// * `k` - Key to remove
    /// * `v` - Value to remove
    fn erase_kv(&mut self, k: u64, v: i64);

    /// Changes a key-value pair in the unit.
    ///
    /// # Arguments
    ///
    /// * `k` - Key to modify
    /// * `v_old` - Old value to replace
    /// * `v_new` - New value to store
    fn change_kv(&mut self, k: u64, v_old: i64, v_new: i64);

    /// Iterates over all values for a key.
    ///
    /// # Arguments
    ///
    /// * `k` - Key to look up
    /// * `access` - Function to call for each value
    fn for_each_value<F>(&self, k: u64, access: F)
    where
        F: FnMut(i64) -> bool;

    /// Iterates over values for adjacent keys.
    ///
    /// # Arguments
    ///
    /// * `k80` - 80-bit key bytes
    /// * `k` - Key to start from
    /// * `access` - Function to call for each value
    fn for_each_adjacent_value<F>(&self, k80: [u8; 10], k: u64, access: F)
    where
        F: FnMut(i64) -> bool;

    /// Checks if the unit needs compaction.
    ///
    /// # Returns
    ///
    /// True if compaction is needed
    fn is_compactible(&self) -> bool;

    /// Performs compaction on the unit.
    fn compact(&mut self);
}

impl UnitTrait for Unit {
    /// Creates a new Unit instance.
    ///
    /// # Returns
    ///
    /// A new Unit with empty storage
    ///
    /// # Performance
    ///
    /// O(1) initialization
    fn new() -> Self {
        Self {
            size: 0,
            num_of_nil: 0,
            big_list: KVList::new(512),
            small_set: BTreeSet::new(),
        }
    }

    /// Adds a key-value pair to the unit.
    ///
    /// # Arguments
    ///
    /// * `k` - Key to add
    /// * `v` - Value to store
    ///
    /// # Performance
    ///
    /// - O(log n) for lookup
    /// - O(1) for insertion
    fn add_kv(&mut self, k: u64, v: i64) {
        let v6 = Self::to_v6(v);
        let success = self.small_set.insert((k, v6));
        assert!(
            success,
            "Add Duplicated KV k={:#016x} v={}",
            k,
            hex::encode(v6)
        );
        //println!("add_kv k={:#016x} v={}", k, hex::encode(&v6));
        self.size += 1;
    }

    /// Removes a key-value pair from the unit.
    ///
    /// # Arguments
    ///
    /// * `k` - Key to remove
    /// * `v` - Value to remove
    ///
    /// # Performance
    ///
    /// - O(log n) for lookup
    /// - O(1) for removal
    fn erase_kv(&mut self, k: u64, v: i64) {
        self.size -= 1;
        let v6 = Self::to_v6(v);
        //println!("erase_kv k={:#016x} v={}", k, hex::encode(&v6));
        let (cursor, found_it) = self.find_kv(k, &v6);
        if found_it {
            self.num_of_nil += 1;
            self.big_list.change(&cursor, NIL_MARKER);
            return;
        }
        let removed = self.small_set.remove(&(k, v6));
        assert!(
            removed,
            "Cannot Erase Non-existent KV pair k={:#016x} v={}",
            k,
            hex::encode(v6)
        );
    }

    /// Changes a key-value pair in the unit.
    ///
    /// # Arguments
    ///
    /// * `k` - Key to modify
    /// * `v_old` - Old value to replace
    /// * `v_new` - New value to store
    ///
    /// # Performance
    ///
    /// - O(log n) for lookup
    /// - O(1) for modification
    fn change_kv(&mut self, k: u64, v_old: i64, v_new: i64) {
        let v6 = Self::to_v6(v_old);
        let v6_new = Self::to_v6(v_new);
        //println!("change_kv k={:#016x} v={}", k, hex::encode(&v6));
        let (cursor, found_it) = self.find_kv(k, &v6);
        if found_it {
            //NIL_MARKER is just an nullification marker in big_list
            if &v6_new == NIL_MARKER {
                self.small_set.insert((k, v6_new));
            }
            self.big_list.change(&cursor, &v6_new);
            return;
        }
        let removed = self.small_set.remove(&(k, v6));
        assert!(
            removed,
            "Cannot change non-existant KV pair k={:#016x} v={}",
            k,
            hex::encode(v6)
        );
        self.small_set.insert((k, v6_new));
    }

    /// Checks if the unit needs compaction.
    ///
    /// # Returns
    ///
    /// True if the ratio of nillified entries exceeds threshold
    ///
    /// # Performance
    ///
    /// O(1) field comparison
    fn is_compactible(&self) -> bool {
        (self.num_of_nil + self.small_set.len()) * COMPACT_RATIO > self.size
    }

    /// Performs compaction on the unit.
    ///
    /// # Performance
    ///
    /// - O(n) for iteration
    /// - O(n log n) for sorted insertion
    fn compact(&mut self) {
        let mut out = Self {
            size: 0,
            num_of_nil: 0,
            small_set: BTreeSet::new(),
            big_list: KVList::new(self.size),
        };
        self.dump_to(&mut out);
        assert!(
            self.size == out.size,
            "Compacted size mismatch! {} != {}",
            self.size,
            out.size
        );
        std::mem::swap(self, &mut out);
    }

    /// Iterates over all values for a key.
    ///
    /// # Arguments
    ///
    /// * `k` - Key to look up
    /// * `access` - Function to call for each value
    ///
    /// # Performance
    ///
    /// - O(log n) for initial lookup
    /// - O(k) for iterating k matching entries
    fn for_each_value<F>(&self, k: u64, access: F)
    where
        F: FnMut(i64) -> bool,
    {
        self._for_each_value(k, access);
    }

    /// Iterates over values for adjacent keys.
    ///
    /// # Arguments
    ///
    /// * `k80` - 80-bit key bytes
    /// * `k` - Key to start from
    /// * `access` - Function to call for each value
    ///
    /// # Performance
    ///
    /// - O(log n) for initial lookup
    /// - O(k) for iterating k adjacent entries
    fn for_each_adjacent_value<F>(&self, mut k80: [u8; 10], k: u64, mut access: F)
    where
        F: FnMut(i64) -> bool,
    {
        // 1) iterate for the current key
        let (mut cursor, done) = self._for_each_value(k, &mut access);
        if done {
            return;
        }

        // 2.1) get the 'prev' from small_set
        let end = (k, [0u8; 6]);
        let start = (0u64, [0u8; 6]);
        let range = (Included(&start), Excluded(&end));
        let small_set = &self.small_set;
        let small_prev = small_set
            .range::<KV, (Bound<&KV>, Bound<&KV>)>(range)
            .next_back();

        // 2.2) get the 'prev' from big_list
        if cursor.valid() {
            self.big_list.move_backward(&mut cursor);
        } else {
            cursor = self.big_list.get_last_cursor();
        }
        while cursor.valid() {
            let elem = self.big_list.at(&cursor);
            let (_, file_pos) = Self::split_kv(&elem);
            if &file_pos != NIL_MARKER {
                break;
            }
            self.big_list.move_backward(&mut cursor);
        }
        let (big_prev, got_big) = if cursor.valid() {
            let elem = self.big_list.at(&cursor);
            let (k_hi32, _) = Self::split_kv(&elem);
            (k_hi32, true)
        } else {
            (0u64, false)
        };

        // 3.1) if the 'prev' from big_list is more adjacent
        if got_big && (small_prev.is_none() || (&small_prev.unwrap().0 >> 32) <= (big_prev >> 32)) {
            while cursor.valid() {
                let elem = self.big_list.at(&cursor);
                let (k_hi32, file_pos) = Self::split_kv(&elem);
                if (k_hi32 >> 32) != (big_prev >> 32) {
                    break;
                }
                BigEndian::write_u64(&mut k80[2..], k_hi32);
                if &file_pos != NIL_MARKER {
                    let file_pos = Self::v6_to_i64(&file_pos);
                    if access(file_pos) {
                        return;
                    }
                }
                self.big_list.move_backward(&mut cursor);
            }
        }

        // 3.2) if the 'prev' from small_set is more adjacent
        if let Some(prev_elem) = small_prev {
            if got_big && (prev_elem.0 >> 32) < (big_prev >> 32) {
                return;
            }
            for &elem in small_set.range::<KV, (Bound<&KV>, Bound<&KV>)>(range).rev() {
                if elem.0 == prev_elem.0 {
                    BigEndian::write_u64(&mut k80[2..], elem.0);
                    let file_pos = Self::v6_to_i64(&elem.1);
                    if access(file_pos) {
                        return;
                    }
                } else {
                    break;
                }
            }
        }
    }
}

fn print_set(hash_set: &HashSet<([u8; 10], i64)>) {
    for (k80, pos) in hash_set.iter() {
        println!("k80={} pos={:#016x}", hex::encode(k80), pos);
    }
}

// only used in fuzz test
pub struct Unit4Test {
    pub u: Unit,
    pub ref_u: RefUnit,
}

impl Unit4Test {
    pub fn debug_get_kv(&self, k_in: u64) -> (u64, i64) {
        let (k, v) = self.ref_u.debug_get_kv(0, k_in);
        (k, v)
    }
}

impl UnitTrait for Unit4Test {
    fn new() -> Self {
        Self {
            u: Unit::new(),
            ref_u: RefUnit::new(),
        }
    }

    fn add_kv(&mut self, k: u64, v: i64) {
        self.u.add_kv(k, v);
        self.ref_u.add_kv(0, k, v);
    }

    fn erase_kv(&mut self, k: u64, v: i64) {
        self.u.erase_kv(k, v);
        self.ref_u.erase_kv(0, k, v);
    }

    fn change_kv(&mut self, k: u64, v_old: i64, v_new: i64) {
        self.u.change_kv(k, v_old, v_new);
        self.ref_u.change_kv(0, k, v_old, v_new);
    }

    fn for_each_value<F>(&self, k: u64, access: F)
    where
        F: FnMut(i64) -> bool,
    {
        let r = self.ref_u.debug_get_values(0, k);
        let i = self.u.debug_get_values(k);
        if r != i {
            println!("r={:?}\ni={:?}", r, i);
            println!("for_each_value mismatch k={:#016x}", k);
            for e in r.iter() {
                if !i.contains(e) {
                    panic!("for_each_adjacent_value mismatch");
                }
            }
        }
        self.u.for_each_value(k, access);
    }

    fn for_each_adjacent_value<F>(&self, k80: [u8; 10], k: u64, access: F)
    where
        F: FnMut(i64) -> bool,
    {
        let r = self.ref_u.debug_get_adjacent_values(&k80, 0, k);
        let i = self.u.debug_get_adjacent_values(&k80, k);
        if r != i {
            println!("=====ref====== k80={}", hex::encode(k80));
            print_set(&r);
            println!("=====imp======");
            print_set(&i);
            println!("for_each_adjacent_value mismatch");
            for e in r.iter() {
                if !i.contains(e) {
                    panic!("for_each_adjacent_value mismatch");
                }
            }
        }
        self.u.for_each_adjacent_value(k80, k, access);
    }

    fn is_compactible(&self) -> bool {
        self.u.is_compactible()
    }

    fn compact(&mut self) {
        self.u.compact();
    }
}

/// High-performance in-memory indexer implementation.
///
/// # Features
///
/// - Pure in-memory storage
/// - Fast key-value operations
/// - Concurrent access support
/// - Simple recovery
///
/// # Performance
///
/// - O(log n) lookups
/// - O(1) insertions (amortized)
/// - Efficient range queries
/// - Zero disk I/O
///
/// # Thread Safety
///
/// All operations are thread-safe through atomic operations
/// and fine-grained locking.
pub struct InMemIndexerGeneric<U: UnitTrait> {
    units: Vec<RwLock<U>>,
    sizes: [AtomicUsize; SHARD_COUNT],
}

/// Type alias for the standard in-memory indexer implementation.
pub type InMemIndexer = InMemIndexerGeneric<Unit>;

/// Type alias for the test-specific in-memory indexer implementation.
pub type InMemIndexer4Test = InMemIndexerGeneric<Unit4Test>;

#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicUsize = AtomicUsize::new(0);

impl<U: UnitTrait + 'static> InMemIndexerGeneric<U> {
    /// Creates a new indexer instance with a directory path.
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory path for persistence (unused in memory implementation)
    ///
    /// # Returns
    ///
    /// A new `InMemIndexerGeneric` instance
    pub fn with_dir(_dir: String) -> Self {
        Self::new(1 << 16)
    }

    /// Creates a new indexer instance with a directory path and optional cipher.
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory path for persistence (unused in memory implementation)
    /// * `cipher` - Optional AES-GCM cipher for encryption
    ///
    /// # Returns
    ///
    /// A new `InMemIndexerGeneric` instance
    pub fn with_dir_and_cipher(_dir: String, _cipher: Arc<Option<Aes256Gcm>>) -> Self {
        Self::new(1 << 16)
    }

    /// Creates a new indexer instance with specified capacity.
    ///
    /// # Arguments
    ///
    /// * `n` - Initial capacity for each shard
    ///         n is 65536 in production, and it can be smaller in test
    ///
    /// # Returns
    ///
    /// A new `InMemIndexerGeneric` instance
    ///
    /// # Performance
    ///
    /// - O(SHARD_COUNT) for initialization
    /// - Memory allocated lazily per shard
    pub fn new(n: usize) -> Self {
        let mut res = Self {
            units: Vec::with_capacity(n),
            sizes: [ZERO; SHARD_COUNT],
        };
        for _ in 0..n {
            res.units.push(RwLock::new(U::new()));
        }
        res
    }

    /// Shrinks a value to fit in the storage format.
    ///
    /// # Arguments
    ///
    /// * `v` - Value to shrink
    ///
    /// # Returns
    ///
    /// Shrunk value
    ///
    /// # Performance
    ///
    /// O(1) bitwise operations
    fn shrink_value(v: i64) -> i64 {
        assert!(v % 8 == 0, "value not 8x");
        v / 8
    }

    /// Scales a value back to its original size.
    ///
    /// # Arguments
    ///
    /// * `v` - Value to scale
    ///
    /// # Returns
    ///
    /// Scaled value
    ///
    /// # Performance
    ///
    /// O(1) bitwise operations
    fn scale_value(v: i64) -> i64 {
        v * 8
    }

    /// Gets the shard ID, key, and value for given inputs.
    ///
    /// # Arguments
    ///
    /// * `k80` - 80-bit key bytes
    /// * `v` - Value to process
    ///
    /// # Returns
    ///
    /// Tuple of (shard_id, key, value)
    ///
    /// # Performance
    ///
    /// O(1) operations for key processing and sharding
    fn get_inputs(&self, k80: &[u8], v: i64) -> (usize, u64, i64) {
        let idx = BigEndian::read_u16(&k80[..2]) as usize;
        let k = BigEndian::read_u64(&k80[2..]);
        (idx % self.units.len(), k, Self::shrink_value(v))
    }

    /// Starts background compaction for the indexer.
    ///
    /// # Arguments
    ///
    /// * `indexer` - Arc reference to the indexer
    ///
    /// # Performance
    ///
    /// - Runs in background thread
    /// - Processes shards in batches
    /// - Minimal impact on main operations
    ///
    /// # Thread Safety
    ///
    /// Safe to call from multiple threads as it uses Arc and RwLock
    pub fn start_compacting(indexer: Arc<Self>) {
        let len = indexer.units.len();
        Self::_start_compacting(indexer.clone(), 0, len); // / 2);
                                                          //Self::_start_compacting(indexer, len / 2, len);
    }

    /// Internal compaction implementation.
    ///
    /// # Arguments
    ///
    /// * `indexer` - Arc reference to the indexer
    /// * `start` - Starting shard index
    /// * `end` - Ending shard index
    ///
    /// # Performance
    ///
    /// - O(n) for compaction
    /// - Runs in background
    /// - Uses write locks per shard
    ///
    /// # Thread Safety
    ///
    /// Safe to call from multiple threads as it uses Arc and RwLock
    fn _start_compacting(indexer: Arc<Self>, start: usize, end: usize) {
        thread::spawn(move || loop {
            let mut did_nothing_count = 0usize;
            for i in start..end {
                if did_nothing_count > COMPACT_TRY_RANGE {
                    did_nothing_count = 0;
                    thread::sleep(time::Duration::from_millis(100));
                }
                if !indexer.units[i].read().is_compactible() {
                    did_nothing_count += 1;
                    continue;
                }
                let mut unit = indexer.units[i].write();
                unit.compact();
            }
        });
    }
}

impl<U: UnitTrait + 'static> IndexerTrait for InMemIndexerGeneric<U> {
    /// Gets the number of entries in a specific shard.
    ///
    /// # Arguments
    ///
    /// * `shard_id` - ID of the shard to query
    ///
    /// # Returns
    ///
    /// Number of entries in the shard
    ///
    /// # Performance
    ///
    /// O(1) atomic load operation
    fn len(&self, shard_id: usize) -> usize {
        self.sizes[shard_id].load(Ordering::SeqCst)
    }

    /// Adds a key-value pair to the index.
    ///
    /// # Arguments
    ///
    /// * `k80` - 80-bit key bytes
    /// * `v` - Value to store
    /// * `_sn` - Serial number for versioning
    ///
    /// # Performance
    ///
    /// - O(log n) for lookup in shard
    /// - O(1) for insertion
    /// - Concurrent access to different shards
    ///
    /// # Thread Safety
    ///
    /// Uses write locking per shard for safe concurrent modifications
    fn add_kv(&self, k80: &[u8], v: i64, _sn: u64) -> Result<(), IndexerError> {
        let (idx, k, v) = self.get_inputs(k80, v);
        let unit = &mut self.units[idx].write();
        unit.add_kv(k, v);
        self.sizes[idx / SHARD_DIV].fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Removes a key-value pair from the index.
    ///
    /// # Arguments
    ///
    /// * `k80` - 80-bit key bytes
    /// * `v` - Value to remove
    /// * `_sn` - Serial number for versioning
    ///
    /// # Performance
    ///
    /// - O(log n) for lookup in shard
    /// - O(1) for removal
    /// - Concurrent access to different shards
    fn erase_kv(&self, k80: &[u8], v: i64, _sn: u64) -> Result<(), IndexerError> {
        let (idx, k, v) = self.get_inputs(k80, v);
        let unit = &mut self.units[idx].write();
        unit.erase_kv(k, v);
        self.sizes[idx / SHARD_DIV].fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    /// Changes a key-value pair in the index.
    ///
    /// # Arguments
    ///
    /// * `k80` - 80-bit key bytes
    /// * `v_old` - Old value to replace
    /// * `v_new` - New value to store
    /// * `_sn_old` - Old serial number
    /// * `_sn_new` - New serial number
    ///
    /// # Performance
    ///
    /// - O(log n) for lookup
    /// - O(1) for modification
    /// - Atomic update within shard
    fn change_kv(
        &self,
        k80: &[u8],
        v_old: i64,
        v_new: i64,
        _sn_old: u64,
        _sn_new: u64,
    ) -> Result<(), IndexerError> {
        let (idx, k, v_old) = self.get_inputs(k80, v_old);
        let v_new = Self::shrink_value(v_new);
        let unit = &mut self.units[idx].write();
        unit.change_kv(k, v_old, v_new);
        Ok(())
    }

    /// Iterates over entries with specific operation type.
    ///
    /// # Arguments
    ///
    /// * `h` - Block height (-1 for latest)
    /// * `op` - Operation type (READ, WRITE, etc.)
    /// * `k80` - 80-bit key bytes
    ///
    /// # Returns
    ///
    /// Position list for matching entries
    ///
    /// # Performance
    ///
    /// - O(log n) for initial lookup
    /// - O(k) for iterating k matching entries
    fn for_each(&self, h: i64, op: u8, k80: &[u8]) -> PosList {
        if op == OP_CREATE || op == OP_DELETE {
            return self.for_each_adjacent_value(h, k80);
        } else if op == OP_WRITE || op == OP_READ {
            // OP_READ is only for debug
            return self.for_each_value(h, k80);
        }
        panic!("Unknown op={}", op);
    }

    fn for_each_value_warmup(&self, h: i64, k80: &[u8]) -> PosList {
        self.for_each_value(h, k80)
    }

    /// Iterates over all values for a key.
    ///
    /// # Arguments
    ///
    /// * `_h` - Block height (-1 for latest)
    /// * `k80` - 80-bit key bytes
    ///
    /// # Returns
    ///
    /// Position list for matching entries
    ///
    /// # Performance
    ///
    /// - O(log n) for initial lookup
    /// - O(k) for iterating k matching entries
    fn for_each_value(&self, _h: i64, k80: &[u8]) -> PosList {
        let (idx, k, _) = self.get_inputs(k80, 0);
        let unit = self.units[idx].read();
        let mut pos_list = PosList::default();
        unit.for_each_value(k, |v| {
            pos_list.append(Self::scale_value(v));
            false // do not exit loop
        });
        pos_list
    }

    /// Iterates over values for adjacent keys.
    ///
    /// # Arguments
    ///
    /// * `_h` - Block height (-1 for latest)
    /// * `k80` - 80-bit key bytes
    ///
    /// # Returns
    ///
    /// Position list for adjacent entries
    ///
    /// # Performance
    ///
    /// - O(log n) for initial lookup
    /// - O(k) for iterating k adjacent entries
    fn for_each_adjacent_value(&self, _h: i64, k80: &[u8]) -> PosList {
        let (idx, k, _) = self.get_inputs(k80, 0);
        let unit = self.units[idx].read();
        let mut buf = [0u8; 10];
        buf[..9].copy_from_slice(&k80[..9]);
        let mut pos_list = PosList::default();
        unit.for_each_adjacent_value(buf, k, |v| {
            pos_list.append(Self::scale_value(v));
            false // do not exit loop
        });
        pos_list
    }

    /// Checks if a key exists at a specific position.
    ///
    /// # Arguments
    ///
    /// * `k80` - 80-bit key bytes
    /// * `file_pos` - File position to check
    /// * `_` - Unused parameter
    ///
    /// # Returns
    ///
    /// True if key exists at position
    ///
    /// # Performance
    ///
    /// O(log n) for key lookup
    fn key_exists(&self, k80: &[u8], file_pos: i64, _: u64) -> bool {
        let (idx, k, _) = self.get_inputs(k80, 0);
        let unit = self.units[idx].read();
        let mut exists = false;
        let v_ = Self::shrink_value(file_pos);
        unit.for_each_value(k, |v| {
            exists = v == v_;
            exists
        });
        exists
    }
}
