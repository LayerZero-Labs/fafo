//! Utility types and functions for the QMDB database.
//!
//! This module provides various utilities including:
//! - Buffer management
//! - Shard mapping
//! - Operation recording
//! - Active bit tracking
//! - Cache management
//! - Channel implementations
//! - Data encoding/decoding
//! - Hashing functions

pub mod activebits;
pub mod bitmap;
pub mod bytescache;
pub mod changeset;
pub mod codec;
pub mod hasher;
pub mod intset;
pub mod lfsr;
pub mod mpsc;
pub mod ringchannel;
pub mod shortlist;
pub mod slice;

use crate::def::{BIG_BUF_SIZE, SHARD_COUNT};

/// Type alias for a large buffer used throughout the database.
/// The size is defined by BIG_BUF_SIZE.
pub type BigBuf = [u8];

/// Creates a new boxed buffer of size BIG_BUF_SIZE.
///
/// # Returns
/// A boxed slice initialized with zeros
pub fn new_big_buf_boxed() -> Box<[u8]> {
    vec![0u8; BIG_BUF_SIZE].into_boxed_slice()
}

/// Maps a byte value to a shard ID.
///
/// This function distributes keys across shards by mapping the first byte
/// of a key to a shard ID. The mapping ensures an even distribution of
/// keys across all shards.
///
/// # Arguments
/// * `byte0` - The first byte of a key
///
/// # Returns
/// The shard ID (0 to SHARD_COUNT-1) for the given byte
pub fn byte0_to_shard_id(byte0: u8) -> usize {
    (byte0 as usize) * SHARD_COUNT / 256
}

/// Record of an operation performed on the database.
///
/// This struct tracks all relevant information about a database operation,
/// including:
/// - Operation type and metadata
/// - Key-value data
/// - Read and write lists
/// - Compaction-related data
#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub struct OpRecord {
    /// Type of operation (create, write, delete, etc.)
    pub op_type: u8,
    /// Number of active entries
    pub num_active: usize,
    /// Oldest active serial number
    pub oldest_active_sn: u64,
    /// ID of the shard this operation affects
    pub shard_id: usize,
    /// Next serial number to use
    pub next_sn: u64,
    /// Key involved in the operation
    pub key: Vec<u8>,
    /// Value involved in the operation
    pub value: Vec<u8>,
    /// List of entries read during the operation
    pub rd_list: Vec<Vec<u8>>,
    /// List of entries written during the operation
    pub wr_list: Vec<Vec<u8>>,
    /// List of old entries during compaction
    pub dig_list: Vec<Vec<u8>>, //old entries in compaction
    /// List of new entries during compaction
    pub put_list: Vec<Vec<u8>>, //new entries in compaction
}

impl OpRecord {
    /// Creates a new OpRecord with the specified operation type.
    ///
    /// # Arguments
    /// * `op_type` - Type of operation to record
    ///
    /// # Returns
    /// A new OpRecord initialized with empty vectors and default values
    pub fn new(op_type: u8) -> OpRecord {
        OpRecord {
            op_type,
            num_active: 0,
            oldest_active_sn: 0,
            shard_id: 0,
            next_sn: 0,
            key: Vec::with_capacity(0),
            value: Vec::with_capacity(0),
            rd_list: Vec::with_capacity(2),
            wr_list: Vec::with_capacity(2),
            dig_list: Vec::with_capacity(2),
            put_list: Vec::with_capacity(2),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::codec::*;
    use super::hasher::*;

    /// Tests the hash2 and hash2x functions.
    #[test]
    fn test_hash2() {
        assert_eq!(
            hex::encode(hash2(8, "hello", "world")),
            "8e6fc50a3f98a3c314021b89688ca83a9b5697ca956e211198625fc460ddf1e9"
        );

        assert_eq!(
            hex::encode(hash2x(8, "world", "hello", true)),
            "8e6fc50a3f98a3c314021b89688ca83a9b5697ca956e211198625fc460ddf1e9"
        );
    }

    /// Tests the node_hash_inplace function.
    #[test]
    fn test_node_hash_inplace() {
        let mut target: [u8; 32] = [0; 32];
        node_hash_inplace(8, &mut target, "hello", "world");
        assert_eq!(
            hex::encode(target),
            "8e6fc50a3f98a3c314021b89688ca83a9b5697ca956e211198625fc460ddf1e9"
        );
    }

    /// Tests encoding and decoding of 64-bit integers.
    #[test]
    fn test_encode_decode_n64() {
        let v = vec![0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        assert_eq!(decode_le_i64(&v), -8613303245920329199);
        assert_eq!(decode_le_u64(&v), 0x8877665544332211);
        assert_eq!(encode_le_i64(-8613303245920329199), v);
        assert_eq!(encode_le_u64(0x8877665544332211), v);
    }
}
