#![allow(clippy::too_many_arguments)]

/// A module for managing and tracking changes to key-value pairs across multiple shards.
///
/// # Design
///
/// The `ChangeSet` system provides efficient tracking and application of changes to a key-value store,
/// with support for:
/// - Multiple operation types (create, read, write, delete)
/// - Sharded data organization
/// - Efficient storage of keys, values, and their hashes
/// - Operation sorting and batching
/// - Change history tracking
///
/// # Implementation Details
///
/// The implementation uses a compact storage format where:
/// - All keys, values, and hashes are stored in a single contiguous buffer
/// - Operations reference data by offset within this buffer
/// - Operations are organized by shard for efficient processing
/// - Changes can be applied selectively by shard or operation type
use crate::def::{OP_CREATE, OP_DELETE, OP_READ, OP_WRITE, SHARD_COUNT};
use crate::utils::{byte0_to_shard_id, hasher, OpRecord};
use std::cmp::Ordering;

/// A collection of changes to be applied to a key-value store.
///
/// The `ChangeSet` efficiently stores multiple operations and their associated data
/// in a format optimized for both storage and application of changes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangeSet {
    /// Contiguous buffer storing all keys, values, and key hashes
    pub data: Vec<u8>,
    /// List of operations referencing data in the buffer
    pub op_list: Vec<ChangeOp>,
    /// Starting index of operations for each shard
    shard_starts: [u32; SHARD_COUNT],
    /// Number of operations in each shard
    shard_op_count: [u32; SHARD_COUNT],
    /// Whether the changeset contains any create or delete operations
    has_create_del_op: bool,
    sorted: bool,
}

/// A single operation within a changeset.
///
/// Each operation references its data (key, value, hash) by offset within
/// the changeset's data buffer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChangeOp {
    /// Type of operation (create, read, write, delete)
    pub op_type: u8,
    /// Shard ID this operation belongs to
    shard_id: u8,
    /// Length of the old value (for tracking changes)
    old_value_len: u16,
    /// Offset of the key in the data buffer
    key_start: u32,
    /// Offset of the value in the data buffer
    value_start: u32,
    /// Offset of the key hash in the data buffer
    key_hash_start: u32,
    /// Optional operation record for additional metadata
    rec: Option<Box<OpRecord>>,
}

impl Default for ChangeSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeSet {
    /// Creates a new empty changeset with pre-allocated capacity.
    pub fn new() -> Self {
        Self {
            data: Vec::with_capacity(1000),
            op_list: Vec::with_capacity(10),
            shard_starts: [u32::MAX; SHARD_COUNT],
            shard_op_count: [0u32; SHARD_COUNT],
            has_create_del_op: false,
            sorted: false,
        }
    }

    /// Creates a new empty changeset without pre-allocated capacity.
    pub fn new_uninit() -> Self {
        Self {
            data: Vec::with_capacity(0),
            op_list: Vec::with_capacity(0),
            shard_starts: [u32::MAX; SHARD_COUNT],
            shard_op_count: [0u32; SHARD_COUNT],
            has_create_del_op: false,
            sorted: true,
        }
    }

    /// Adds an operation from an existing operation record.
    ///
    /// # Arguments
    ///
    /// * `rec` - The operation record to add
    ///
    /// # Panics
    ///
    /// * If the shard ID derived from the key hash doesn't match the record's shard ID
    pub fn add_op_rec(&mut self, rec: OpRecord) {
        assert!(!self.sorted, "Already sorted");
        let rec_box = Box::new(rec);
        let key_hash = hasher::hash(&rec_box.key[..]);
        let (k, v) = (&rec_box.key[..], &rec_box.value[..]);

        let shard_id = byte0_to_shard_id(key_hash[0]) as u8;
        if shard_id != rec_box.shard_id as u8 {
            panic!(
                "mismatch: shard_id={} key_hash={:?} rec={:?}",
                shard_id, key_hash, *rec_box
            );
        }

        if rec_box.op_type == OP_DELETE || rec_box.op_type == OP_CREATE {
            self.has_create_del_op = true;
        }
        let key_start = self.data.len();
        self.data.extend_from_slice(k);
        let value_start = self.data.len();
        self.data.extend_from_slice(v);
        let key_hash_start = self.data.len();
        self.data.extend_from_slice(&key_hash[..]);
        let op_type = rec_box.op_type;
        self.op_list.push(ChangeOp {
            op_type,
            shard_id,
            old_value_len: 0,
            key_start: key_start as u32,
            value_start: value_start as u32,
            key_hash_start: key_hash_start as u32,
            rec: Some(rec_box),
        });
    }

    /// Adds an operation with both new and old values.
    ///
    /// # Arguments
    ///
    /// * `op_type` - Type of operation (create, read, write, delete)
    /// * `shard_id` - ID of the shard this operation belongs to
    /// * `key_hash` - Hash of the key
    /// * `k` - Key bytes
    /// * `v` - New value bytes
    /// * `old_v` - Old value bytes
    /// * `rec` - Optional operation record for additional metadata
    pub fn add_op_with_old_value(
        &mut self,
        op_type: u8,
        shard_id: u8,
        key_hash: &[u8; 32],
        k: &[u8],
        v: &[u8],
        old_v: &[u8],
        rec: Option<Box<OpRecord>>,
    ) {
        assert!(!self.sorted, "Already sorted");
        if op_type == OP_DELETE || op_type == OP_CREATE {
            self.has_create_del_op = true;
        }
        let key_start = self.data.len();
        self.data.extend_from_slice(k);
        let value_start = self.data.len();
        self.data.extend_from_slice(v);
        self.data.extend_from_slice(old_v);
        let key_hash_start = self.data.len();
        self.data.extend_from_slice(&key_hash[..]);
        self.op_list.push(ChangeOp {
            op_type,
            shard_id,
            old_value_len: old_v.len() as u16,
            key_start: key_start as u32,
            value_start: value_start as u32,
            key_hash_start: key_hash_start as u32,
            rec,
        });
    }

    /// Adds an operation without an old value.
    ///
    /// # Arguments
    ///
    /// * `op_type` - Type of operation (create, read, write, delete)
    /// * `shard_id` - ID of the shard this operation belongs to
    /// * `key_hash` - Hash of the key
    /// * `k` - Key bytes
    /// * `v` - Value bytes
    /// * `rec` - Optional operation record for additional metadata
    pub fn add_op(
        &mut self,
        op_type: u8,
        shard_id: u8,
        key_hash: &[u8; 32],
        k: &[u8],
        v: &[u8],
        rec: Option<Box<OpRecord>>,
    ) {
        assert!(!self.sorted, "Already sorted");
        if op_type == OP_DELETE || op_type == OP_CREATE {
            self.has_create_del_op = true;
        }
        let key_start = self.data.len();
        self.data.extend_from_slice(k);
        let value_start = self.data.len();
        self.data.extend_from_slice(v);
        let key_hash_start = self.data.len();
        self.data.extend_from_slice(&key_hash[..]);
        self.op_list.push(ChangeOp {
            op_type,
            shard_id,
            old_value_len: 0,
            key_start: key_start as u32,
            value_start: value_start as u32,
            key_hash_start: key_hash_start as u32,
            rec,
        });
    }

    /// Sorts operations by shard ID, key hash, and operation type.
    ///
    /// This method also updates the shard start indices and operation counts.
    pub fn sort(&mut self) {
        assert!(!self.sorted, "Already sorted");
        self.sorted = true;
        self.op_list.sort_by(|a, b| {
            let mut res = a.shard_id.cmp(&b.shard_id);
            if res == Ordering::Equal {
                let x = &self.data[a.key_hash_start as usize..a.key_hash_start as usize + 32];
                let y = &self.data[b.key_hash_start as usize..b.key_hash_start as usize + 32];
                res = x.cmp(y);
            }
            if res == Ordering::Equal {
                res = a.op_type.cmp(&b.op_type);
            }
            res
        });
        for idx in 0..self.op_list.len() {
            let shard_id = self.op_list[idx].shard_id as usize;
            self.shard_op_count[shard_id] += 1;
            if self.shard_starts[shard_id] == u32::MAX {
                self.shard_starts[shard_id] = idx as u32;
            }
        }
    }

    /// Applies operations in order, providing access to both old and new values.
    ///
    /// # Arguments
    ///
    /// * `access` - Function to call for each operation with:
    ///   - Operation type
    ///   - Key hash
    ///   - Key
    ///   - New value
    ///   - Old value
    ///   - Optional operation record
    pub fn apply_op_in_range<F>(&self, mut access: F)
    where
        F: FnMut(u8, &[u8; 32], &[u8], &[u8], &[u8], Option<&Box<OpRecord>>),
    {
        assert!(self.sorted, "Not sorted");
        let mut key_hash = [0u8; 32];
        for op in self.op_list.iter() {
            let kh_start = op.key_hash_start as usize;
            key_hash[..].copy_from_slice(&self.data[kh_start..kh_start + 32]);
            let key = &self.data[op.key_start as usize..op.value_start as usize];
            // 'value' contains the old value and the new value
            let old_value_start = kh_start - op.old_value_len as usize;
            let value = &self.data[op.value_start as usize..old_value_start];
            let old_value =
                &self.data[old_value_start..old_value_start + op.old_value_len as usize];
            access(
                op.op_type,
                &key_hash,
                key,
                value,
                old_value,
                op.rec.as_ref(),
            );
        }
    }

    /// Returns the number of operations in a specific shard.
    ///
    /// # Arguments
    ///
    /// * `shard_id` - The shard to count operations for
    pub fn op_count_in_shard(&self, shard_id: usize) -> usize {
        self.shard_op_count[shard_id] as usize
    }

    /// Checks if the changeset contains any create or delete operations.
    pub fn has_create_del(&self) -> bool {
        self.has_create_del_op
    }

    /// Applies all operations in order, providing access to new values only.
    ///
    /// # Arguments
    ///
    /// * `access` - Function to call for each operation with:
    ///   - Operation type
    ///   - Key hash
    ///   - Key
    ///   - New value
    ///   - Optional operation record
    pub fn run_all<F>(&self, mut access: F)
    where
        F: FnMut(u8, &[u8; 32], &[u8], &[u8], Option<&Box<OpRecord>>),
    {
        assert!(self.sorted, "Not sorted");
        let mut key_hash = [0u8; 32];
        for op in self.op_list.iter() {
            let kh_start = op.key_hash_start as usize;
            key_hash[..].copy_from_slice(&self.data[kh_start..kh_start + 32]);
            let key = &self.data[op.key_start as usize..op.value_start as usize];
            let old_start = kh_start - op.old_value_len as usize;
            let value = &self.data[op.value_start as usize..old_start];
            access(op.op_type, &key_hash, key, value, op.rec.as_ref());
        }
    }

    /// Applies operations for a specific shard in order.
    ///
    /// # Arguments
    ///
    /// * `shard_id` - The shard to process operations for
    /// * `access` - Function to call for each operation with:
    ///   - Operation type
    ///   - Key hash
    ///   - Key
    ///   - New value
    ///   - Optional operation record
    pub fn run_in_shard<F>(&self, shard_id: usize, mut access: F)
    where
        F: FnMut(u8, &[u8; 32], &[u8], &[u8], Option<&Box<OpRecord>>),
    {
        assert!(self.sorted, "Not sorted");
        let shard_start = self.shard_starts[shard_id];
        if shard_start == u32::MAX {
            return;
        }
        let mut key_hash = [0u8; 32];
        let mut idx = shard_start as usize;
        while idx < self.op_list.len() {
            let op = &self.op_list[idx];
            if shard_id != op.shard_id as usize {
                break;
            }
            let kh_start = op.key_hash_start as usize;
            key_hash[..].copy_from_slice(&self.data[kh_start..kh_start + 32]);
            let key = &self.data[op.key_start as usize..op.value_start as usize];
            let old_start = kh_start - op.old_value_len as usize;
            let value = &self.data[op.value_start as usize..old_start];
            access(op.op_type, &key_hash, key, value, op.rec.as_ref());
            idx += 1;
        }
    }

    // debug
    pub fn print(&self) {
        for op in &self.op_list {
            let _op = match op.op_type {
                OP_CREATE => "C",
                OP_READ => "R",
                OP_WRITE => "U",
                OP_DELETE => "D",
                _ => "?",
            };

            let old_val_start = op.key_hash_start as usize - op.old_value_len as usize;
            println!(
                "op:{:?}, shard:{:?}, key:{:?}, nv:{:?}, ov:{:?}, kh:{:?}",
                _op,
                op.shard_id,
                hex::encode(&self.data[op.key_start as usize..op.value_start as usize]),
                hex::encode(&self.data[op.value_start as usize..old_val_start]),
                hex::encode(&self.data[old_val_start..op.key_hash_start as usize]),
                hex::encode(
                    &self.data[op.key_hash_start as usize..op.key_hash_start as usize + 32]
                )
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::OpRecord;

    #[test]
    fn test_changeset_new() {
        let changeset = ChangeSet::new();
        assert_eq!(changeset.data.capacity(), 1000);
        assert_eq!(changeset.op_list.capacity(), 10);
        assert_eq!(changeset.shard_starts.len(), SHARD_COUNT);
        assert!(changeset
            .shard_starts
            .iter()
            .all(|&start| start == u32::MAX));
    }

    #[test]
    fn test_add_op() {
        let mut changeset = ChangeSet::new();
        let key_hash = [1u8; 32];
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let rec = Some(Box::new(OpRecord::new(1)));

        changeset.add_op(1, 0, &key_hash, &key, &value, rec.clone());

        assert_eq!(
            changeset.data,
            vec![
                1, 2, 3, 4, 5, 6, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1
            ]
        );
        assert_eq!(changeset.op_list.len(), 1);

        let op = &changeset.op_list[0];
        assert_eq!(op.op_type, 1);
        assert_eq!(op.shard_id, 0);
        assert_eq!(op.key_start, 0);
        assert_eq!(op.value_start, 3);
        assert_eq!(op.key_hash_start, 6);
        assert_eq!(op.rec, rec);
    }

    #[test]
    fn test_apply_op_in_range() {
        let mut changeset = ChangeSet::new();
        let key_hash = [1u8; 32];
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let rec = Some(Box::new(OpRecord::new(1)));

        changeset.add_op(0, 0, &key_hash, &key, &value, rec.clone());
        changeset.add_op(1, 0, &key_hash, &key, &value, rec.clone());
        changeset.sort();

        let mut count = 0;
        changeset.apply_op_in_range(|op_type, kh, k, v, old_v, r| {
            assert_eq!(op_type, count);
            assert_eq!(kh, &key_hash);
            assert_eq!(k, &key[..]);
            assert_eq!(v, &value[..]);
            assert_eq!(old_v, &[]);
            assert_eq!(r, rec.as_ref());
            count += 1;
        });
        assert_eq!(count, 2);
    }

    #[test]
    fn test_sort() {
        let mut changeset = ChangeSet::new();
        let key_hash1 = [1u8; 32];
        let key1 = vec![1, 2, 3];
        let value1 = vec![4, 5, 6];
        let key_hash2 = [2u8; 32];
        let key2 = vec![7, 8, 9];
        let value2 = vec![10, 11, 12];

        changeset.add_op(1, 1, &key_hash2, &key2, &value2, None);
        changeset.add_op(1, 0, &key_hash1, &key1, &value1, None);

        changeset.sort();

        assert_eq!(changeset.op_list.len(), 2);
        assert_eq!(changeset.op_list[0].shard_id, 0);
        assert_eq!(changeset.op_list[1].shard_id, 1);
    }

    #[test]
    fn test_run_in_shard() {
        let mut changeset = ChangeSet::new();

        let key_hash = [1u8; 32];
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let rec = Some(Box::new(OpRecord::new(1)));

        changeset.add_op(1, 1, &key_hash, &key, &value, rec.clone());
        changeset.add_op(2, 0, &key_hash, &key, &value, rec.clone());
        changeset.add_op(3, 0, &[0u8; 32], &key, &value, rec.clone());
        changeset.sort();

        assert_eq!(changeset.op_list[0].shard_id, 0);
        assert_eq!(changeset.op_list[0].op_type, 3);
        assert_eq!(changeset.op_list[1].shard_id, 0);
        assert_eq!(changeset.op_list[1].op_type, 2);
        assert_eq!(changeset.op_list[2].shard_id, 1);
        assert_eq!(changeset.op_list[2].op_type, 1);

        let mut accessed = false;
        changeset.run_in_shard(3, |_, _, _, _, _| {
            accessed = true;
        });
        assert!(!accessed);

        changeset.run_in_shard(1, |op_type, kh, k, v, r| {
            assert_eq!(op_type, 1);
            assert_eq!(kh, &key_hash);
            assert_eq!(k, &key[..]);
            assert_eq!(v, &value[..]);
            assert_eq!(r, rec.as_ref());
            accessed = true;
        });

        assert!(accessed);
    }
}
