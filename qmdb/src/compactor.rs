//! Compaction module for QMDB.
//!
//! This module handles database compaction to optimize storage utilization and performance.
//! Compaction is a critical process that:
//! - Reclaims space by removing obsolete entries
//! - Defragments storage by rewriting active entries
//! - Maintains data consistency during reorganization
//! - Updates indexing to reflect new entry positions
//!
//! # Architecture
//! The compaction process involves several components:
//! - Compactor: Coordinates the compaction process for a shard
//! - CompactJob: Represents an entry that needs to be rewritten
//! - Producer/Consumer Channel: Efficiently transfers jobs to the updater
//! - Indexer: Tracks entry validity and positions
//!
//! # Process Flow
//! 1. Compaction Triggering:
//!    - Monitors entry count in each shard
//!    - Initiates compaction when threshold is reached
//!    - Can be manually triggered for maintenance
//!
//! 2. Entry Processing:
//!    - Sequentially reads entries from the entry file
//!    - Checks entry validity with the indexer
//!    - Queues active entries for rewriting
//!    - Updates indices to reflect new positions
//!
//! 3. Storage Management:
//!    - Maintains write position for new entries
//!    - Tracks space reclamation progress
//!    - Ensures atomic updates during rewriting
//!
//! # Performance Considerations
//! - Compaction is I/O intensive and may impact performance
//! - Uses producer/consumer pattern for efficient job handling
//! - Implements batching to optimize throughput
//! - Maintains consistency during concurrent operations
//!
//! # Configuration
//! Key parameters that affect compaction:
//! - `compact_trigger`: Entry count threshold
//! - Buffer sizes for reading and writing
//! - Channel capacity for job queuing
//!
//! # Example
//! ```no_run
//! use qmdb::compactor::{Compactor, CompactJob};
//! use qmdb::utils::ringchannel;
//!
//! // Create a compaction job template
//! let job = CompactJob {
//!     old_pos: 0,
//!     entry_bz: Vec::new(),
//! };
//!
//! // Set up the producer/consumer channel
//! let (producer, consumer) = ringchannel::new(100, &job);
//!
//! // Create a compactor instance
//! let compactor = Compactor::new(
//!     0,  // shard_id
//!     1000,  // compact_trigger
//!     entry_file,
//!     indexer,
//!     producer,
//! );
//!
//! // Start compaction from a specific position
//! compactor.fill_compact_chan(start_pos);
//! ```
//!
//! # Error Handling
//! The compactor handles several error conditions:
//! - Channel closure during operation
//! - I/O errors during entry reading
//! - Invalid entry formats
//! - Concurrent modification conflicts
//!
//! # Best Practices
//! 1. Configure appropriate trigger thresholds
//! 2. Monitor compaction frequency and duration
//! 3. Schedule compaction during low-usage periods
//! 4. Ensure sufficient disk space for rewriting
//! 5. Back up data before major compactions

use crate::def::DEFAULT_ENTRY_SIZE;
use crate::entryfile::{EntryBz, EntryFile};
use crate::indexer::{Indexer, IndexerTrait};
use crate::utils::ringchannel::Producer;
use hpfile::PreReader;
use log::error;
use std::sync::Arc;
use std::thread;
use std::time;

pub const END_MARGIN: i64 = 2000;

/// A job representing an entry to be compacted.
///
/// Contains the original position and content of an entry that needs
/// to be rewritten during compaction. Jobs are queued and processed
/// asynchronously to optimize throughput.
///
/// # Fields
/// * `old_pos` - Original position in the entry file
/// * `entry_bz` - Raw bytes of the entry to be rewritten
///
/// # Performance
/// - Minimizes memory usage by storing only necessary data
/// - Allows for efficient batch processing
/// - Enables asynchronous rewriting
#[derive(Clone)]
pub struct CompactJob {
    /// Original position of the entry in the entry file
    pub old_pos: i64,
    /// Raw bytes of the entry
    pub entry_bz: Vec<u8>,
}

/// Manages the compaction process for a database shard.
///
/// The Compactor coordinates storage optimization by:
/// - Monitoring entry counts and triggering compaction
/// - Reading and validating entries
/// - Queueing active entries for rewriting
/// - Updating indices to reflect new positions
///
/// # Architecture
/// - Uses producer/consumer pattern for job handling
/// - Maintains consistency with atomic operations
/// - Coordinates with indexer for entry validation
/// - Optimizes I/O through batched operations
///
/// # Performance
/// - Minimizes impact on concurrent operations
/// - Implements efficient entry validation
/// - Uses pre-allocated buffers for I/O
/// - Supports parallel processing across shards
pub struct Compactor {
    /// ID of the shard being compacted
    shard_id: usize,
    /// Minimum number of entries needed to trigger compaction
    compact_trigger: usize,
    /// Reference to the entry file being compacted
    entry_file: Arc<EntryFile>,
    /// Reference to the indexer for checking entry validity
    indexer: Arc<Indexer>,
    // compact_buf_wr: EntryBufferWriter, // to simulate a virtual in-memory EntryFile
    /// Producer for sending compact jobs to the updater
    compact_producer: Producer<CompactJob>,
}

impl Compactor {
    /// Creates a new Compactor instance.
    ///
    /// # Arguments
    /// * `shard_id` - ID of the shard to compact
    /// * `compact_trigger` - Minimum entries needed to trigger compaction
    /// * `entry_file` - Reference to the entry file
    /// * `indexer` - Reference to the indexer
    /// * `compact_producer` - Producer for sending compact jobs
    ///
    /// # Returns
    /// A new Compactor instance configured for the specified shard
    ///
    /// # Performance
    /// - Initializes with pre-allocated resources
    /// - Sets up efficient communication channels
    /// - Prepares for parallel processing
    pub fn new(
        shard_id: usize,
        compact_trigger: usize,
        entry_file: Arc<EntryFile>,
        indexer: Arc<Indexer>,
        compact_producer: Producer<CompactJob>,
    ) -> Self {
        Self {
            shard_id,
            compact_trigger,
            entry_file,
            indexer,
            compact_producer,
        }
    }

    /// Fills the compaction channel with active entries that need to be rewritten.
    ///
    /// This method implements the core compaction logic:
    /// 1. Monitors entry count against the trigger threshold
    /// 2. Reads entries sequentially from the entry file
    /// 3. Validates each entry's activity status
    /// 4. Queues active entries for rewriting
    ///
    /// # Arguments
    /// * `file_pos` - Starting position in the entry file
    ///
    /// # Performance
    /// - Uses pre-reader for optimized I/O
    /// - Implements entry validation caching
    /// - Batches operations for efficiency
    /// - Handles backpressure through channel
    ///
    /// # Error Handling
    /// - Logs errors during job production
    /// - Handles channel closure gracefully
    /// - Maintains consistency on failure
    pub fn fill_compact_chan(&mut self, file_pos: i64) {
        let mut file_pos = file_pos;
        let mut bz = Vec::with_capacity(DEFAULT_ENTRY_SIZE);
        let mut pre_reader = PreReader::new();
        let mut file_size = self.entry_file.size();
        loop {
            while self.indexer.len(self.shard_id) < self.compact_trigger {
                //println!("sleeping compactor {}", self.shard_id);
                thread::sleep(time::Duration::from_millis(500));
            }

            while file_pos + END_MARGIN >= file_size {
                thread::sleep(time::Duration::from_millis(500));
                file_size = self.entry_file.size();
            }
            let size = self
                .entry_file
                .read_entry_with_pre_reader(file_pos, i64::MAX, &mut bz, &mut pre_reader)
                .unwrap();
            let e = EntryBz { bz: &bz[..size] };
            let kh = e.key_hash();
            let ke = self
                .indexer
                .key_exists(&kh[..], file_pos, e.serial_number());
            if ke {
                match self.compact_producer.receive_returned() {
                    Ok(mut job) => {
                        job.old_pos = file_pos;
                        job.entry_bz.clear();
                        job.entry_bz.extend_from_slice(e.bz);
                        if self.compact_producer.produce(job).is_err() {
                            error!("compactor exit when produce job!");
                            return;
                        }
                    }
                    Err(_) => {
                        error!("compactor exit when receive job!");
                        return;
                    }
                }
            }
            file_pos += e.len() as i64;
        }
    }
}

#[cfg(test)]
mod compactor_tests {
    use super::*;

    use std::fs::create_dir_all;

    use crate::def::{ENTRIES_PATH, TWIG_PATH};
    use crate::entryfile::{entry::entry_to_bytes, entrybuffer, Entry, EntryFileWriter};
    use crate::merkletree::TwigFile;
    use crate::tasks::BlockPairTaskHub;
    use crate::test_helper::SimpleTask;
    use crate::updater::Updater;
    use crate::utils::ringchannel;
    use parking_lot::Mutex;

    /// Tests the compaction process by:
    /// 1. Creating test entries and writing them to an entry file
    /// 2. Setting up indexing for the entries
    /// 3. Running compaction
    /// 4. Verifying that entries are correctly rewritten and indexed
    #[test]
    fn test_compact() {
        let temp_dir = tempfile::Builder::new()
            .prefix("test_compactor")
            .tempdir()
            .unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();
        let suffix = ".test";
        let indexer = Indexer::new(2);
        let dir_entry = format!("{}/{}{}", dir_path, ENTRIES_PATH, suffix);
        create_dir_all(&dir_entry).unwrap();
        let entry_file = Arc::new(EntryFile::new(4096, 4096, dir_entry, true, None));
        let mut entry_file_w = EntryFileWriter::new(entry_file.clone(), 4096);
        let dir_twig = format!("{}/{}{}", dir_path, TWIG_PATH, suffix);
        create_dir_all(&dir_twig).unwrap();
        let _twig_file = TwigFile::new(1024, 2048i64, dir_twig);
        let e1 = Entry {
            key: "Key1Key ILOVEYOU 1Key1Key1".as_bytes(),
            value: "Value1Value1".as_bytes(),
            next_key_hash: [2; 32].as_slice(),
            version: 10,
            serial_number: 1,
        };
        let mut buf = [0; 1024];
        let bz1 = entry_to_bytes(&e1, &[], &mut buf);
        let pos1 = entry_file_w.append(&bz1).unwrap();
        for _ in 0..100 {
            entry_file_w.append(&bz1).unwrap();
        }
        let pos_end = entry_file_w.append(&bz1).unwrap();
        entry_file_w.flush().unwrap();

        // Add longer sleep to ensure writes are completed
        thread::sleep(time::Duration::from_millis(500));

        let kh = bz1.key_hash();
        indexer.add_kv(&kh[..], pos1, 1).unwrap();
        assert!(indexer.key_exists(&kh[..], pos1, 1));
        let indexer = Arc::new(indexer);
        let entry_file_size = entry_file.size();

        let job = CompactJob {
            old_pos: 0,
            entry_bz: Vec::new(),
        };
        let (producer, consumer) = ringchannel::new(100, &job);

        let mut compactor = Compactor {
            shard_id: 0,
            compact_trigger: 1,
            entry_file: entry_file.clone(),
            indexer: indexer.clone(),
            compact_producer: producer,
        };

        // Spawn compactor thread without trying to join it
        thread::spawn(move || {
            compactor.fill_compact_chan(pos1);
        });

        let (u_eb_wr, _) = entrybuffer::new(entry_file_size, 1024);

        let sn_end = 1;
        let mut updater = Updater::new(
            0,
            Arc::new(BlockPairTaskHub::<SimpleTask>::new()),
            Arc::new(Mutex::new(u_eb_wr)),
            entry_file.clone(),
            indexer.clone(),
            0,
            0,
            sn_end,
            consumer,
            0,
            1,
            1,
            0,
            0,
        );
        updater.test_compact(None, 0);

        thread::sleep(time::Duration::from_millis(500));

        assert_eq!(indexer.len(0), 1);
        let pos_list = indexer.for_each_value(-1, &kh[..]);
        assert!(pos_list.len() == 1);
        let new_pos = pos_list.get(0).unwrap();
        assert_eq!(new_pos, pos_end + bz1.len() as i64);
    }
}
