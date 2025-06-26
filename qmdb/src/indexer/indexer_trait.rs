use auto_impl::auto_impl;
use thiserror::Error;

use super::utils::poslist::PosList;

/// Error types that can occur during indexer operations
#[derive(Error, Debug)]
pub enum IndexerError {
    #[error("Value not 8x aligned: {0}")]
    InvalidValueAlignment(i64),

    #[error("Operation not allowed during initialization")]
    InitializationError,
}

/// Trait defining the interface for indexers in QMDB.
///
/// This trait provides the core functionality for managing key-value pairs
/// in the database, including adding, changing, and erasing entries, as well
/// as querying the index.
#[auto_impl(Arc)]
pub trait IndexerTrait: Send + Sync {
    /// Add a new key-value pair to the index.
    ///
    /// # Arguments
    ///
    /// * `key` - The key bytes to add
    /// * `position` - The position in the database where the value is stored
    /// * `serial_number` - A unique identifier for this entry
    fn add_kv(&self, key: &[u8], position: i64, serial_number: u64) -> Result<(), IndexerError>;

    /// Change an existing key-value pair in the index.
    ///
    /// # Arguments
    ///
    /// * `key` - The key bytes to change
    /// * `pos_old` - The old position in the database where the value is stored
    /// * `pos_new` - The new position in the database where the value is stored
    /// * `sn_old` - The old serial number for this entry
    /// * `sn_new` - The new serial number for this entry
    fn change_kv(
        &self,
        k80: &[u8],
        pos_old: i64,
        pos_new: i64,
        sn_old: u64,
        sn_new: u64,
    ) -> Result<(), IndexerError>;

    /// Erase a key-value pair from the index.
    ///
    /// # Arguments
    ///
    /// * `key` - The key bytes to erase
    /// * `position` - The position in the database where the value was stored
    /// * `serial_number` - A unique identifier for this entry
    fn erase_kv(&self, key: &[u8], position: i64, serial_number: u64) -> Result<(), IndexerError>;

    fn for_each(&self, h: i64, op: u8, k80: &[u8]) -> PosList;

    fn for_each_value_warmup(&self, h: i64, k80: &[u8]) -> PosList;

    /// Iterate over all values associated with a key at a specific height.
    ///
    /// # Arguments
    ///
    /// * `height` - Block height to search at (-1 for latest)
    /// * `key` - 80-bit key bytes to search for
    ///
    /// # Returns
    ///
    /// Position list containing all matching file positions
    ///
    /// # Performance
    ///
    /// - O(log n) for initial lookup
    /// - O(k) for iterating k matching entries
    fn for_each_value(&self, _h: i64, k80: &[u8]) -> PosList;

    /// Iterate over all adjacent values for a key at a specific height.
    ///
    /// # Arguments
    ///
    /// * `height` - Block height to search at (-1 for latest)
    /// * `key` - 80-bit key bytes to search for
    ///
    /// # Returns
    ///
    /// Position list containing all matching file positions for adjacent keys
    ///
    /// # Performance
    ///
    /// - O(log n) for initial lookup
    /// - O(k) for iterating k adjacent entries
    fn for_each_adjacent_value(&self, _h: i64, k80: &[u8]) -> PosList;

    /// Check if a key exists in the index.
    ///
    /// # Arguments
    ///
    /// * `key` - The key bytes to check
    ///
    /// # Returns
    ///
    /// `true` if the key exists, `false` otherwise
    fn key_exists(&self, k80: &[u8], file_pos: i64, sn: u64) -> bool;

    /// Get the number of entries in the index.
    ///
    /// # Returns
    ///
    /// The number of entries in the index
    fn len(&self, shard_id: usize) -> usize;
}
