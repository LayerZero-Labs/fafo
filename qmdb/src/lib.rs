//! QMDB (Quick Merkle Database) is a high-performance verifiable key-value store designed to optimize blockchain state storage.
//!
//! # Overview
//! QMDB provides:
//! - High-performance state storage optimized for blockchain use cases
//! - Verifiable state transitions with Merkle proofs
//! - Efficient parallel processing and prefetching
//! - Secure encryption support
//! - Modular architecture for extensibility
//!
//! # Architecture
//! QMDB is built around several key components:
//!
//! ## Core Components
//! - [`AdsCore`]: The core database engine handling state storage and verification
//!   - Manages sharded entry files for efficient data storage
//!   - Coordinates proof generation and verification
//!   - Handles encryption and secure state transitions
//!
//! - [`AdsWrap`]: A high-level wrapper providing block-oriented operations
//!   - Manages block lifecycle and state transitions
//!   - Coordinates task execution and caching
//!   - Handles parallel processing of operations
//!
//! - [`ADS`]: The main trait defining the database interface
//!   - Provides core operations for reading and writing entries
//!   - Manages task scheduling and execution
//!   - Handles root hash management and verification
//!
//! ## Supporting Components
//! - Merkle Tree: Implements a stateless Merkle tree for efficient verification
//! - Entry Management: Handles entry storage, updates, and pruning
//! - Task System: Manages concurrent operations and state transitions
//! - Prefetcher: Optimizes data access patterns for improved performance
//!
//! # Performance Optimizations
//! - Sharded storage architecture for parallel processing
//! - Efficient caching and prefetching mechanisms
//! - Optimized Merkle tree operations
//! - Batched updates and proof generation
//!
//! # Security Features
//! - AES-GCM encryption support for sensitive data
//! - Secure state transitions with Merkle proofs
//! - Protection against replay attacks and invalid state changes
//! - Robust error handling and validation
//!
//! # Example Usage
//! ```no_run
//! use qmdb::{AdsWrap, config::Config};
//!
//! // Create a new database instance
//! let config = Config::default();
//! let mut ads = AdsWrap::new(&config);
//!
//! // Start a new block
//! let height = 1;
//! let tasks_manager = Arc::new(TasksManager::new());
//! let (success, meta_info) = ads.start_block(height, tasks_manager);
//!
//! // Process operations within the block
//! // ...
//!
//! // Flush changes and get updated state
//! let meta_updates = ads.flush();
//! ```
//!
//! # Implementation Details
//! - Uses a sharded architecture with configurable shard count
//! - Implements efficient parallel processing with thread pools
//! - Provides configurable write buffer and segment sizes
//! - Supports optional encryption for sensitive data
//! - Implements efficient pruning and compaction strategies
//!
//! # Best Practices
//! 1. Configure shard count based on available CPU cores
//! 2. Adjust buffer sizes for optimal performance
//! 3. Enable encryption for sensitive deployments
//! 4. Implement proper error handling
//! 5. Monitor and tune cache performance
//!
//! # Note on Thread Safety
//! All public interfaces are thread-safe and can be safely shared across threads.
//! Internal synchronization is handled through Arc and RwLock mechanisms.

#![allow(clippy::too_many_arguments)]
extern crate core;
pub mod compactor;
pub mod config;
pub mod def;
pub mod entryfile;
pub mod flusher;
pub mod indexer;
pub mod merkletree;
pub mod metadb;
pub mod prefetcher;
pub mod tasks;

pub mod updater;
pub mod utils;

pub mod mutator;

// for test
pub mod test_helper;

use aes_gcm::{Aes256Gcm, Key, KeyInit};
use compactor::END_MARGIN;
use crossbeam::channel::{self, bounded, unbounded, Sender};
use def::calc_max_level;
use entryfile::entrybuffer;
use entryfile::readbuf::ReadBuf;
use flusher::flusher::Flusher;
use flusher::shard::{FlusherShard, ProofReqElem};
use merkletree::proof::ProofPath;
use parking_lot::RwLock;
use prefetcher::{JobManager, Prefetcher};
use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use tasks::taskid::join_task_id;
use threadpool::ThreadPool;

use crate::compactor::{CompactJob, Compactor};
use crate::def::{COMPACT_RING_SIZE, DEFAULT_ENTRY_SIZE, SENTRY_COUNT, SHARD_COUNT, TWIG_SHIFT};
use crate::entryfile::{entry::sentry_entry, EntryBz, EntryCache, EntryFile, ExtEntryFile};
use crate::indexer::{Indexer, IndexerTrait};
use crate::merkletree::{
    recover::{bytes_to_edge_nodes, recover_tree},
    Tree,
};
use crate::metadb::{MetaDB, MetaInfo};
use crate::tasks::{BlockPairTaskHub, Task, TaskHub, TasksManager};
use crate::updater::Updater;
use crate::utils::{byte0_to_shard_id, hasher, ringchannel};
use log::{debug, info};

/// Core database engine handling state storage and verification.
/// Manages the underlying storage, indexing, and proof generation.
///
/// # Architecture
/// AdsCore implements a sharded storage system with the following components:
/// - Task Management: Coordinates concurrent operations
/// - Entry Storage: Manages data across multiple shards
/// - Indexing: Maintains efficient data lookup
/// - Proof Generation: Creates and verifies Merkle proofs
/// - Prefetching: Optimizes data access patterns
///
/// # Thread Safety
/// All operations are thread-safe, using Arc and RwLock for synchronization.
/// The struct can be safely shared across multiple threads.
///
/// # Performance
/// - Implements parallel processing across shards
/// - Uses efficient caching and prefetching
/// - Optimizes proof generation and verification
/// - Supports configurable write buffering
///
/// # Security
/// - Optional AES-GCM encryption
/// - Secure state transitions
/// - Protected metadata storage
/// - Safe concurrent access
pub struct AdsCore {
    /// Task hub for managing concurrent operations
    task_hub: Arc<dyn TaskHub>,
    /// Channel for sending task IDs
    task_sender: Sender<i64>,
    /// Channels for intermediate task communication
    mid_senders: Vec<crossbeam::channel::Sender<i64>>,
    /// Core indexing engine
    indexer: Arc<Indexer>,
    /// Storage for entry files across shards
    entry_files: Vec<Arc<EntryFile>>,
    /// Extended entry file storage
    ext_entry_files: Vec<ExtEntryFile>,
    /// Metadata database
    meta: Arc<RwLock<MetaDB>>,
    /// Write buffer size
    wrbuf_size: usize,
    /// Channels for proof request handling
    proof_req_senders: Vec<crossbeam::channel::Sender<ProofReqElem>>,
    /// Flag to bypass prefetcher for direct access
    bypass_prefetcher: bool,
    /// Optional prefetcher for performance optimization
    prefetcher: Option<Arc<Prefetcher>>,
}

/// Generates encryption ciphers for secure storage.
///
/// # Arguments
/// * `aes_keys` - Optional 96-byte key material for AES-GCM encryption
///
/// # Returns
/// A tuple containing:
/// - Cipher queue for shards
/// - Indexer cipher
/// - MetaDB cipher
fn get_ciphers(
    aes_keys: &Option<[u8; 96]>,
) -> (
    VecDeque<Option<Aes256Gcm>>,
    Arc<Option<Aes256Gcm>>,
    Option<Aes256Gcm>,
) {
    let mut vec = VecDeque::with_capacity(SHARD_COUNT);
    for _ in 0..SHARD_COUNT {
        vec.push_back(None);
    }
    if cfg!(not(feature = "tee_cipher")) {
        return (vec, Arc::new(None), None);
    }

    let _aes_keys = aes_keys.as_ref().unwrap();
    if _aes_keys.len() != 96 {
        panic!("Invalid length for aes_keys");
    }
    let mut key = [0u8; 33];
    key[1..].copy_from_slice(&_aes_keys[..32]);
    #[allow(clippy::needless_range_loop)]
    for i in 0..SHARD_COUNT {
        key[0] = i as u8;
        let hash = hasher::hash(key);
        let aes_key: &Key<Aes256Gcm> = (&hash).into();
        vec[i] = Some(Aes256Gcm::new(aes_key));
    }
    let aes_key = Key::<Aes256Gcm>::from_slice(&_aes_keys[32..64]);
    let indexer_cipher = Some(Aes256Gcm::new(aes_key));
    let aes_key = Key::<Aes256Gcm>::from_slice(&_aes_keys[64..96]);
    let meta_db_cipher = Some(Aes256Gcm::new(aes_key));
    (vec, Arc::new(indexer_cipher), meta_db_cipher)
}

impl AdsCore {
    /// Gets the subdirectories used by the database.
    ///
    /// # Arguments
    /// * `dir` - Base directory path
    ///
    /// # Returns
    /// A tuple containing paths for (data, meta, indexer) directories
    pub fn get_sub_dirs(dir: &str) -> (String, String, String) {
        let data_dir = dir.to_owned() + "/data";
        let meta_dir = dir.to_owned() + "/metadb";
        let indexer_dir = dir.to_owned() + "/indexer";
        (data_dir, meta_dir, indexer_dir)
    }

    /// Creates a new AdsCore instance with the given configuration.
    ///
    /// # Arguments
    /// * `task_hub` - Task hub for managing concurrent operations
    /// * `config` - Database configuration
    ///
    /// # Returns
    /// A tuple containing:
    /// - The AdsCore instance
    /// - A channel receiver for metadata updates
    /// - A Flusher instance for managing state persistence
    pub fn new(
        task_hub: Arc<dyn TaskHub>,
        config: &config::Config,
    ) -> (Self, crossbeam::channel::Receiver<Arc<MetaInfo>>, Flusher) {
        #[cfg(feature = "tee_cipher")]
        assert!(config.aes_keys.unwrap().len() == 96);

        Self::_new(
            task_hub,
            &config.dir,
            config.wrbuf_size,
            config.file_segment_size,
            config.with_twig_file,
            config.bypass_prefetcher,
            &config.aes_keys,
        )
    }

    /// Internal constructor with detailed parameters.
    ///
    /// # Arguments
    /// * `task_hub` - Task hub for managing concurrent operations
    /// * `dir` - Base directory path
    /// * `wrbuf_size` - Write buffer size
    /// * `file_segment_size` - Size of file segments
    /// * `with_twig_file` - Whether to use twig files
    /// * `bypass_prefetcher` - Whether to bypass the prefetcher
    /// * `aes_keys` - Optional encryption keys
    ///
    /// # Returns
    /// Same as `new()`
    fn _new(
        task_hub: Arc<dyn TaskHub>,
        dir: &str,
        wrbuf_size: usize,
        file_segment_size: usize,
        with_twig_file: bool,
        bypass_prefetcher: bool,
        aes_keys: &Option<[u8; 96]>,
    ) -> (Self, crossbeam::channel::Receiver<Arc<MetaInfo>>, Flusher) {
        let (ciphers, idx_cipher, meta_db_cipher) = get_ciphers(aes_keys);
        let (data_dir, meta_dir, _indexer_dir) = Self::get_sub_dirs(dir);

        let dir = (dir.to_owned() + "/idx").to_owned();
        let indexer = Arc::new(Indexer::with_dir_and_cipher(dir, idx_cipher));
        let (eb_sender, eb_receiver) = bounded(2);
        let meta = MetaDB::with_dir(&meta_dir, meta_db_cipher);
        let curr_height = meta.get_curr_height();
        let meta = Arc::new(RwLock::new(meta));

        // recover trees in parallel
        let mut trees = Self::recover_trees(
            meta.clone(),
            data_dir,
            indexer.clone(),
            wrbuf_size,
            file_segment_size,
            with_twig_file,
            curr_height,
            ciphers,
        );

        let mut entry_files = Vec::with_capacity(SHARD_COUNT);
        let mut shards: Vec<Box<FlusherShard>> = Vec::with_capacity(SHARD_COUNT);

        for shard_id in 0..SHARD_COUNT {
            let (tree, oldest_active_sn) = trees.remove(0);
            entry_files.push(tree.entry_file_wr.entry_file.clone());
            shards.push(Box::new(FlusherShard::new(
                tree,
                oldest_active_sn,
                shard_id,
            )));
        }

        let max_kept_height = 10; //1000;
        let flusher = Flusher::new(
            shards,
            meta.clone(),
            curr_height,
            max_kept_height,
            eb_sender,
        );

        let (task_sender, _) = channel::bounded(1); // TODO: init none
        let ads_core = Self {
            task_hub,
            task_sender,
            mid_senders: Vec::with_capacity(0),
            indexer,
            entry_files,
            ext_entry_files: Vec::with_capacity(SHARD_COUNT),
            meta: meta.clone(),
            wrbuf_size,
            bypass_prefetcher,
            proof_req_senders: flusher.get_proof_req_senders(),
            prefetcher: None,
        };
        (ads_core, eb_receiver, flusher)
    }

    /// Gets a proof for a specific entry.
    ///
    /// # Arguments
    /// * `shard_id` - ID of the shard containing the entry
    /// * `sn` - Serial number of the entry
    ///
    /// # Returns
    /// The proof path if successful, or an error message
    ///
    /// # Errors
    /// Returns an error if proofs are not supported in slow hashing mode
    pub fn get_proof(&self, shard_id: usize, sn: u64) -> Result<ProofPath, String> {
        if cfg!(feature = "slow_hashing") {
            return Err("do not support proof in slow hashing mode".to_owned());
        }

        let pair = Arc::new((Mutex::new((sn, Option::None)), Condvar::new()));

        if let Err(er) = self.proof_req_senders[shard_id].send(Arc::clone(&pair)) {
            return Err(format!("send proof request failed: {:?}", er));
        }

        // wait for the request to be handled
        let (lock, cvar) = &*pair;
        let mut sn_proof = lock.lock().unwrap();
        while sn_proof.1.is_none() {
            sn_proof = cvar.wait(sn_proof).unwrap();
        }

        if let Err(er) = sn_proof.1.as_ref().unwrap() {
            return Err(format!("get proof failed: {:?}", er));
        }
        sn_proof.1.take().unwrap()
    }

    /// Gets a reference to all entry files.
    ///
    /// # Returns
    /// Vector of references to entry files
    pub fn get_entry_files(&self) -> Vec<Arc<EntryFile>> {
        let mut res = Vec::with_capacity(self.entry_files.len());
        for ef in self.entry_files.iter() {
            res.push(ef.clone());
        }
        res
    }

    /// Recovers trees from disk storage.
    ///
    /// # Arguments
    /// * `meta` - Metadata database
    /// * `data_dir` - Data directory path
    /// * `indexer` - Indexer instance
    /// * `wrbuf_size` - Write buffer size
    /// * `file_segment_size` - Size of file segments
    /// * `with_twig_file` - Whether to use twig files
    /// * `curr_height` - Current block height
    /// * `ciphers` - Queue of encryption ciphers
    ///
    /// # Returns
    /// Vector of recovered trees and their oldest active serial numbers
    #[cfg(not(feature = "use_hybridindexer"))]
    fn recover_trees(
        meta: Arc<RwLock<MetaDB>>,
        data_dir: String,
        indexer: Arc<Indexer>,
        wrbuf_size: usize,
        file_segment_size: usize,
        with_twig_file: bool,
        curr_height: i64,
        mut ciphers: VecDeque<Option<Aes256Gcm>>,
    ) -> Vec<(Tree, u64)> {
        use log::debug;

        let mut recover_handles = Vec::with_capacity(SHARD_COUNT);
        for shard_id in 0..SHARD_COUNT {
            let meta = meta.clone();
            let data_dir = data_dir.clone();
            let indexer = indexer.clone();
            let cipher = ciphers.pop_front().unwrap();
            let handle = thread::spawn(move || {
                let meta = meta.read_arc();
                let (tree, ef_prune_to, oldest_active_sn) = Self::recover_tree(
                    &meta,
                    data_dir,
                    wrbuf_size,
                    file_segment_size,
                    with_twig_file,
                    curr_height,
                    shard_id,
                    cipher,
                );

                Self::index_tree(&tree, oldest_active_sn, ef_prune_to, &indexer);

                (tree, oldest_active_sn)
            });
            recover_handles.push(handle);
        }

        let mut result = Vec::with_capacity(SENTRY_COUNT);
        for _shard_id in 0..SHARD_COUNT {
            let handle = recover_handles.remove(0);
            let (tree, oldest_active_sn) = handle.join().unwrap();
            result.push((tree, oldest_active_sn));
        }
        debug!("finish recover_tree");
        result
    }

    #[cfg(feature = "use_hybridindexer")]
    fn recover_trees(
        meta: Arc<RwLock<MetaDB>>,
        data_dir: String,
        indexer: Arc<Indexer>,
        wrbuf_size: usize,
        file_segment_size: usize,
        with_twig_file: bool,
        curr_height: i64,
        mut ciphers: VecDeque<Option<Aes256Gcm>>,
    ) -> Vec<(Tree, u64)> {
        let mut recover_handles = Vec::with_capacity(SHARD_COUNT);
        for shard_id in 0..SHARD_COUNT {
            let meta = meta.clone();
            let data_dir = data_dir.clone();
            let cipher = ciphers.pop_front().unwrap();
            let handle = thread::spawn(move || {
                let meta = meta.read_arc();
                let (tree, ef_prune_to, oldest_active_sn) = Self::recover_tree(
                    &meta,
                    data_dir,
                    wrbuf_size,
                    file_segment_size,
                    with_twig_file,
                    curr_height,
                    shard_id,
                    cipher,
                );

                (tree, ef_prune_to, oldest_active_sn)
            });
            recover_handles.push(handle);
        }

        let mut result = Vec::with_capacity(SENTRY_COUNT);
        for shard_id in 0..SHARD_COUNT {
            let handle = recover_handles.remove(0);
            let (tree, ef_prune_to, oldest_active_sn) = handle.join().unwrap();

            Self::index_tree(&tree, oldest_active_sn, ef_prune_to, &indexer);
            indexer.dump_mem_to_file(shard_id);

            result.push((tree, oldest_active_sn));
        }
        result
    }

    /// Recovers a single tree from disk.
    ///
    /// # Arguments
    /// * `meta` - Metadata database
    /// * `data_dir` - Data directory path
    /// * `buffer_size` - Buffer size for reading
    /// * `file_segment_size` - Size of file segments
    /// * `with_twig_file` - Whether to use twig files
    /// * `curr_height` - Current block height
    /// * `shard_id` - ID of the shard to recover
    /// * `cipher` - Optional encryption cipher
    ///
    /// # Returns
    /// The recovered tree, current height, and oldest active serial number
    #[allow(clippy::too_many_arguments)]
    pub fn recover_tree(
        meta: &MetaDB,
        data_dir: String,
        buffer_size: usize,
        file_segment_size: usize,
        with_twig_file: bool,
        curr_height: i64,
        shard_id: usize,
        cipher: Option<Aes256Gcm>,
    ) -> (Tree, i64, u64) {
        // let meta = meta.read_arc();
        let oldest_active_sn = meta.get_oldest_active_sn(shard_id);
        let youngest_twig_id = meta.get_youngest_twig_id(shard_id);
        let edge_nodes = bytes_to_edge_nodes(&meta.get_edge_nodes(shard_id));
        let (last_pruned_twig_id, ef_prune_to) = meta.get_last_pruned_twig(shard_id);
        let root = meta.get_root_hash(shard_id);
        let entryfile_size = meta.get_entry_file_size(shard_id);
        let twigfile_size = meta.get_twig_file_size(shard_id);
        let (tree, recovered_root) = recover_tree(
            shard_id,
            buffer_size,
            file_segment_size,
            with_twig_file,
            data_dir,
            format!("{}", shard_id),
            &edge_nodes,
            last_pruned_twig_id,
            ef_prune_to,
            oldest_active_sn >> TWIG_SHIFT,
            youngest_twig_id,
            &[entryfile_size, twigfile_size],
            cipher,
        );
        if shard_id == 0 {
            info!("edge_nodes len:{}, last_pruned_twig_id:{}, oldest_active_twig_id:{}, youngest_twig_id:{}, entryfile_size:{}, twigfile_size:{}, recovered_root:{:?}", edge_nodes.len(), last_pruned_twig_id, oldest_active_sn >> TWIG_SHIFT, youngest_twig_id, entryfile_size, twigfile_size, recovered_root);
        }

        if root != recovered_root && curr_height != 0 {
            panic!(
                "root mismatch, shard_id: {}, root: {:?}, recovered_root: {:?}",
                shard_id, root, recovered_root
            );
        }

        (tree, ef_prune_to, oldest_active_sn)
    }

    /// Indexes a tree's entries.
    ///
    /// # Arguments
    /// * `tree` - Tree to index
    /// * `oldest_active_sn` - Oldest active serial number
    /// * `ef_prune_to` - Height to prune entries up to
    /// * `indexer` - Indexer instance
    pub fn index_tree(tree: &Tree, oldest_active_sn: u64, ef_prune_to: i64, indexer: &Indexer) {
        tree.scan_entries_lite(
            oldest_active_sn >> TWIG_SHIFT,
            ef_prune_to,
            |k80, _nkh, pos, sn| {
                if tree.get_active_bit(sn) {
                    indexer.add_kv(k80, pos, sn).unwrap();
                }
            },
        );
    }

    /// Initializes the database directory structure.
    ///
    /// # Arguments
    /// * `config` - Database configuration
    pub fn init_dir(config: &config::Config) {
        #[cfg(feature = "tee_cipher")]
        assert!(config.aes_keys.unwrap().len() == 96);

        Self::_init_dir(
            &config.dir,
            config.file_segment_size,
            config.with_twig_file,
            &config.aes_keys,
        );
    }

    /// Internal directory initialization.
    ///
    /// # Arguments
    /// * `dir` - Base directory path
    /// * `file_segment_size` - Size of file segments
    /// * `with_twig_file` - Whether to use twig files
    /// * `aes_keys` - Optional encryption keys
    fn _init_dir(
        dir: &str,
        file_segment_size: usize,
        with_twig_file: bool,
        aes_keys: &Option<[u8; 96]>,
    ) {
        let (data_dir, meta_dir, _indexer_dir) = Self::get_sub_dirs(dir);

        if Path::new(dir).exists() {
            fs::remove_dir_all(dir).unwrap();
        }
        fs::create_dir(dir).unwrap();
        let (mut ciphers, _, meta_db_cipher) = get_ciphers(aes_keys);
        let mut meta = MetaDB::with_dir(&meta_dir, meta_db_cipher);
        for shard_id in 0..SHARD_COUNT {
            let mut tree = Tree::new(
                shard_id,
                8192,
                file_segment_size as i64,
                data_dir.clone(),
                format!("{}", shard_id),
                with_twig_file,
                ciphers.pop_front().unwrap(),
            );
            let mut bz = [0u8; DEFAULT_ENTRY_SIZE];
            for sn in 0..SENTRY_COUNT {
                let e = sentry_entry(shard_id, sn as u64, &mut bz[..]);
                tree.append_entry(&e).unwrap();
            }
            tree.flush_files(0, 0);
            let root_hash = {
                let max_level = calc_max_level(tree.youngest_twig_id) as u8;
                tree.get_hashes_by_pos_list(&vec![(max_level, 0)])
                    .pop()
                    .unwrap()
            };
            meta.set_root_hash(shard_id, root_hash);
            let (entry_file_size, twig_file_size) = tree.get_file_sizes();
            meta.set_entry_file_size(shard_id, entry_file_size);
            meta.set_twig_file_size(shard_id, twig_file_size);
            meta.set_next_serial_num(shard_id, SENTRY_COUNT as u64);
        }
        meta.insert_extra_data(0, "".to_owned());
        meta.commit();
    }

    /// Checks if an entry matches its key.
    ///
    /// # Arguments
    /// * `key_hash` - Hash of the key
    /// * `key` - Original key
    /// * `entry_bz` - Entry bytes to check
    ///
    /// # Returns
    /// true if the entry matches the key, false otherwise
    pub fn check_entry(key_hash: &[u8], key: &[u8], entry_bz: &EntryBz) -> bool {
        if key.is_empty() {
            entry_bz.key_hash() == key_hash
        } else {
            entry_bz.key() == key
        }
    }

    /// Warms up the cache for a specific height and key prefix.
    ///
    /// # Arguments
    /// * `height` - Block height to warm up
    /// * `k80` - 80-bit key prefix for selective warming
    /// * `cache` - Cache to warm up
    pub fn warmup_cache(&self, height: i64, k80: &[u8], cache: Arc<EntryCache>) {
        let idx = &self.indexer;
        #[cfg(target_os = "linux")]
        {
            let pos_list = idx.for_each_value_warmup(height, k80);
            for file_pos in pos_list.enumerate() {
                let shard_id = byte0_to_shard_id(k80[0]);
                let p = self.prefetcher.as_ref().unwrap();
                p.run_warmup(file_pos, shard_id, cache.clone());
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let shard_id = byte0_to_shard_id(k80[0]);
            let pos_list = idx.for_each_value_warmup(height, k80);
            let mut buf = ReadBuf::new();
            for file_pos in pos_list.enumerate() {
                if cache.contains(shard_id, file_pos) {
                    continue; //continue to next 'file_pos
                }
                buf.clear();
                self.ext_entry_files[shard_id].get_entry_bz(file_pos, &mut buf, false);
                if !buf.is_empty() {
                    cache.insert(shard_id, file_pos, &buf.as_entry_bz());
                }
            }
        }
    }

    /// Reads an entry from the database.
    ///
    /// # Arguments
    /// * `height` - Block height to read from
    /// * `key_hash` - Hash of the key to read
    /// * `key` - Original key
    /// * `cache` - Optional cache to use
    /// * `read_buf` - Buffer to store the read data
    ///
    /// # Returns
    /// The size of the read data, or -1 if not found
    fn read_entry(
        &self,
        height: i64,
        key_hash: &[u8],
        key: &[u8],
        cache: Option<&EntryCache>,
        read_buf: &mut ReadBuf,
        use_uring: bool,
    ) -> i64 {
        let shard_id = byte0_to_shard_id(key_hash[0]);
        let idx = &self.indexer;
        let pos_list = idx.for_each_value(height, key_hash);
        for file_pos in pos_list.enumerate() {
            read_buf.clear();
            if let Some(cache) = cache {
                cache.lookup(shard_id, file_pos, read_buf);
                if !read_buf.is_empty() {
                    if Self::check_entry(key_hash, key, &read_buf.as_entry_bz()) {
                        return file_pos;
                    }
                    continue;
                }
            }

            self.ext_entry_files[shard_id].get_entry_bz(file_pos, read_buf, use_uring);
            if !read_buf.is_empty() && Self::check_entry(key_hash, key, &read_buf.as_entry_bz()) {
                if let Some(cache) = cache {
                    cache.insert(shard_id, file_pos, &read_buf.as_entry_bz());
                }
                return file_pos;
            }
        }
        0
    }

    /// Adds a task to the updater component.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to add
    pub fn add_task_to_updater(&self, task_id: i64) {
        if !self.bypass_prefetcher {
            panic!("bypass_prefetcher is not enabled");
        }
        for sender in &self.mid_senders {
            sender.send(task_id).unwrap();
        }
    }

    /// Fetches entries from previous blocks for a task.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to fetch entries for
    pub fn fetch_prev_entries(&self, task_id: i64, cache: Arc<EntryCache>) {
        if !self.bypass_prefetcher {
            panic!("bypass_prefetcher is not enabled");
        }
        let prefetcher = self.prefetcher.as_ref().unwrap();
        prefetcher.fetch_prev_entries(task_id, cache);
    }

    /// Adds a task to be processed.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to add
    pub fn add_task(&self, task_id: i64) {
        self.task_sender.send(task_id).unwrap();
    }

    /// Starts the database's background threads.
    ///
    /// # Arguments
    /// * `flusher` - Flusher instance for managing state persistence
    /// * `compact_thres` - Compaction threshold
    /// * `utilization_ratio` - Target utilization ratio
    /// * `utilization_div` - Utilization divisor
    /// * `prefetcher_thread_count` - Number of prefetcher threads
    /// * `uring_count` - Number of io_uring instances (Linux only)
    /// * `uring_size` - Size of io_uring queues (Linux only)
    pub fn start_threads(
        &mut self,
        mut flusher: Flusher,
        compact_thres: i64,
        utilization_ratio: i64,
        utilization_div: i64,
        prefetcher_thread_count: usize,
        uring_count: usize,
        uring_size: u32,
    ) {
        let meta = self.meta.read_arc();
        let curr_height = meta.get_curr_height() + 1;

        Indexer::start_compacting(self.indexer.clone());

        let job = CompactJob {
            old_pos: 0,
            entry_bz: Vec::with_capacity(DEFAULT_ENTRY_SIZE),
        };
        let mut job_man = JobManager::new(uring_count, uring_size);

        let (task_sender, task_receiver) = channel::unbounded();
        self.task_sender = task_sender;
        for shard_id in 0..SHARD_COUNT {
            let (mid_sender, mid_receiver) = unbounded();
            let entryfile_size = meta.get_entry_file_size(shard_id);
            let (u_eb_wr, u_eb_rd) = entrybuffer::new(entryfile_size, self.wrbuf_size);
            let entry_file = flusher.get_entry_file(shard_id);
            self.mid_senders.push(mid_sender.clone());

            job_man.add_shard(u_eb_wr.entry_buffer.clone(), entry_file.clone(), mid_sender);

            let ext_entry_file = u_eb_wr.into_ext_entry_file(&entry_file, shard_id);

            let (cmpt_producer, cmpt_consumer) = ringchannel::new(COMPACT_RING_SIZE, &job);
            let mut compactor = Compactor::new(
                shard_id,
                compact_thres as usize / 2,
                entry_file.clone(),
                self.indexer.clone(),
                cmpt_producer,
            );
            let compact_start = meta.get_oldest_active_file_pos(shard_id);
            thread::spawn(move || {
                compactor.fill_compact_chan(compact_start);
            });

            let sn_start = meta.get_oldest_active_sn(shard_id);
            let sn_end = meta.get_next_serial_num(shard_id);
            let task_id = join_task_id(curr_height, 0, false);
            let updater = Updater::new(
                shard_id,
                self.task_hub.clone(),
                ext_entry_file.ebw.clone(),
                entry_file,
                self.indexer.clone(),
                -1, // curr_version, will be overwritten
                sn_start,
                sn_end,
                cmpt_consumer,
                compact_start,
                utilization_div,
                utilization_ratio,
                compact_thres,
                task_id,
            );
            self.ext_entry_files.push(ext_entry_file);
            updater.start_thread(mid_receiver);
            flusher.set_entry_buf_reader(shard_id, u_eb_rd)
        }

        let prefetcher = Arc::new(Prefetcher::new(
            self.task_hub.clone(),
            Arc::new(EntryCache::new_uninit()),
            self.indexer.clone(),
            Arc::new(ThreadPool::new(prefetcher_thread_count)),
            job_man,
        ));
        self.prefetcher = Some(prefetcher.clone());
        if !self.bypass_prefetcher {
            prefetcher.start_threads(task_receiver);
        }

        drop(meta);
        thread::spawn(move || {
            flusher.flush(SHARD_COUNT);
        });
    }

    /// Gets a reference to the metadata database.
    ///
    /// # Returns
    /// Reference to the metadata database
    pub fn get_metadb(&self) -> Arc<RwLock<MetaDB>> {
        self.meta.clone()
    }
}

/// High-level wrapper providing block-oriented database operations.
/// Manages block lifecycle, caching, and task coordination.
///
/// # Type Parameters
/// * `T`: Task type implementing the Task trait
///
/// # Architecture
/// - Manages block lifecycle and state transitions
/// - Coordinates task execution and caching
/// - Handles parallel processing of operations
/// - Maintains consistency across block boundaries
///
/// # Performance
/// - Efficient cache management
/// - Parallel task processing
/// - Optimized block transitions
/// - Configurable resource utilization
///
/// # Thread Safety
/// All operations are thread-safe and can be shared across threads.
pub struct AdsWrap<T: Task> {
    /// Task hub for block pair processing
    task_hub: Arc<BlockPairTaskHub<T>>,
    /// Core database engine
    ads: Arc<AdsCore>,
    /// Current block's entry cache
    cache: Arc<EntryCache>,
    /// List of caches for parallel processing
    cache_list: Vec<Arc<EntryCache>>,
    // when ads finish the prev block disk job, end_block_chan will receive MetaInfo
    /// Channel for receiving block completion notifications
    end_block_chan: crossbeam::channel::Receiver<Arc<MetaInfo>>,
    /// Height at which to stop processing
    stop_height: i64,
}

/// Shared reference to the database core and cache.
/// Provides thread-safe access to database operations.
#[derive(Clone)]
pub struct SharedAdsWrap {
    /// Core database engine
    ads: Arc<AdsCore>,
    /// Shared entry cache
    pub cache: Arc<EntryCache>, // TODO: remove pub
}

impl<T: Task + 'static> AdsWrap<T> {
    /// Creates a new AdsWrap instance with the given configuration.
    ///
    /// # Arguments
    /// * `config` - Database configuration
    ///
    /// # Returns
    /// A new AdsWrap instance
    pub fn new(config: &config::Config) -> Self {
        #[cfg(feature = "tee_cipher")]
        assert!(config.aes_keys.unwrap().len() == 96);

        Self::_new(
            &config.dir,
            config.wrbuf_size,
            config.file_segment_size,
            config.with_twig_file,
            config.bypass_prefetcher,
            config.compact_thres,
            config.utilization_ratio,
            config.utilization_div,
            &config.aes_keys,
            config.prefetcher_thread_count,
            config.uring_count,
            config.uring_size,
        )
    }

    /// Internal constructor with detailed parameters.
    ///
    /// # Arguments
    /// * `dir` - Base directory path
    /// * `wrbuf_size` - Write buffer size
    /// * `file_segment_size` - Size of file segments
    /// * `with_twig_file` - Whether to use twig files
    /// * `bypass_prefetcher` - Whether to bypass the prefetcher
    /// * `compact_thres` - Compaction threshold
    /// * `utilization_ratio` - Target utilization ratio
    /// * `utilization_div` - Utilization divisor
    /// * `aes_keys` - Optional encryption keys
    /// * `prefetcher_thread_count` - Number of prefetcher threads
    /// * `uring_count` - Number of io_uring instances (Linux only)
    /// * `uring_size` - Size of io_uring queues (Linux only)
    ///
    /// # Returns
    /// A new AdsWrap instance
    fn _new(
        dir: &str,
        wrbuf_size: usize,
        file_segment_size: usize,
        with_twig_file: bool,
        bypass_prefetcher: bool,
        compact_thres: i64,
        utilization_ratio: i64,
        utilization_div: i64,
        aes_keys: &Option<[u8; 96]>,
        prefetcher_thread_count: usize,
        uring_count: usize,
        uring_size: u32,
    ) -> Self {
        // Ensure compaction threshold is sufficient to avoid running compaction
        // when there is not enough data (file_pos + END_MARGIN >= file_size).
        // The minimum entry length is 60 bytes.
        assert!(compact_thres * 60 >= END_MARGIN);
        let task_hub = Arc::new(BlockPairTaskHub::<T>::new());
        let (mut ads, end_block_chan, flusher) = AdsCore::_new(
            task_hub.clone(),
            dir,
            wrbuf_size,
            file_segment_size,
            with_twig_file,
            bypass_prefetcher,
            aes_keys,
        );
        ads.start_threads(
            flusher,
            compact_thres,
            utilization_ratio,
            utilization_div,
            prefetcher_thread_count,
            uring_count,
            uring_size,
        );

        Self {
            task_hub,
            ads: Arc::new(ads),
            cache: Arc::new(EntryCache::new_uninit()),
            cache_list: Vec::new(),
            end_block_chan,
            stop_height: -1,
        }
    }

    /// Gets a reference to the indexer.
    ///
    /// # Returns
    /// Reference to the indexer
    pub fn get_indexer(&self) -> Arc<Indexer> {
        self.ads.indexer.clone()
    }

    /// Gets a reference to all entry files.
    ///
    /// # Returns
    /// Vector of references to entry files
    pub fn get_entry_files(&self) -> Vec<Arc<EntryFile>> {
        self.ads.get_entry_files()
    }

    /// Gets a proof for a specific entry.
    ///
    /// # Arguments
    /// * `shard_id` - ID of the shard containing the entry
    /// * `sn` - Serial number of the entry
    ///
    /// # Returns
    /// The proof path if successful, or an error message
    pub fn get_proof(&self, shard_id: usize, sn: u64) -> Result<ProofPath, String> {
        self.ads.get_proof(shard_id, sn)
    }

    /// Flushes pending changes to disk.
    ///
    /// # Returns
    /// Vector of metadata updates that were flushed
    pub fn flush(&mut self) -> Vec<Arc<MetaInfo>> {
        let mut v = Vec::with_capacity(2);
        while self.task_hub.free_slot_count() < 2 {
            let meta_info = self.end_block_chan.recv().unwrap();
            self.task_hub.end_block(meta_info.curr_height);
            v.push(meta_info);
        }
        v
    }

    /// Allocates a new entry cache.
    ///
    /// # Returns
    /// A new entry cache instance
    fn allocate_cache(&mut self) -> Arc<EntryCache> {
        let mut idx = usize::MAX;
        for (i, arc) in self.cache_list.iter().enumerate() {
            if Arc::strong_count(arc) == 1 && Arc::weak_count(arc) == 0 {
                idx = i;
                break;
            }
        }
        if idx != usize::MAX {
            let cache = self.cache_list[idx].clone();
            cache.clear();
            return cache;
        }
        let cache = Arc::new(EntryCache::new());
        self.cache_list.push(cache.clone());
        cache
    }

    /// Sets the block height at which to stop processing.
    ///
    /// # Arguments
    /// * `height` - Block height to stop at
    // only for test
    pub fn set_stop_block(&mut self, height: i64) {
        self.stop_height = height;
    }

    /// Starts processing a new block.
    ///
    /// # Arguments
    /// * `height` - Block height to process
    /// * `tasks_manager` - Manager for block tasks
    ///
    /// # Returns
    /// A tuple containing:
    /// - Whether the block was started successfully
    /// - Optional metadata update from previous block
    pub fn start_block(
        &mut self,
        height: i64,
        tasks_manager: Arc<TasksManager<T>>,
    ) -> (bool, Option<Arc<MetaInfo>>) {
        let cache = self.allocate_cache();
        self.start_block_with_cache(height, cache, tasks_manager)
    }

    /// Starts processing a new block with a specific cache.
    ///
    /// # Arguments
    /// * `height` - Block height to process
    /// * `cache` - Cache to use for the block
    /// * `tasks_manager` - Manager for block tasks
    ///
    /// # Returns
    /// A tuple containing:
    /// - Whether the block was started successfully
    /// - Optional metadata update from previous block
    pub fn start_block_with_cache(
        &mut self,
        height: i64,
        cache: Arc<EntryCache>,
        tasks_manager: Arc<TasksManager<T>>,
    ) -> (bool, Option<Arc<MetaInfo>>) {
        if height == self.stop_height + 1 {
            return (false, Option::None);
        }
        self.cache = cache;

        let mut meta_info = Option::None;
        if self.task_hub.free_slot_count() == 0 {
            // adscore and task_hub are busy, wait for them to finish an old block
            let _meta_info = self.end_block_chan.recv().unwrap();
            self.task_hub.end_block(_meta_info.curr_height);
            meta_info = Some(_meta_info);
        }

        self.task_hub
            .start_block(height, tasks_manager, self.cache.clone());
        (true, meta_info)
    }

    /// Gets a shared reference to the database.
    ///
    /// # Returns
    /// A SharedAdsWrap instance for concurrent access
    pub fn get_shared(&self) -> SharedAdsWrap {
        SharedAdsWrap {
            ads: self.ads.clone(),
            cache: self.cache.clone(),
        }
    }

    /// Gets a reference to the metadata database.
    ///
    /// # Returns
    /// Reference to the metadata database
    pub fn get_metadb(&self) -> Arc<RwLock<MetaDB>> {
        self.ads.get_metadb()
    }

    pub fn get_entries_list(&self, old_size_list: &Vec<i64>) -> Vec<Vec<u8>> {
        let mut result = Vec::with_capacity(old_size_list.len());
        let entry_files = self.ads.get_entry_files();
        for shard_id in 0..entry_files.len() {
            let size = self.ads.get_metadb().read().get_entry_file_size(shard_id);
            let capacity = size as usize - old_size_list[shard_id] as usize;
            let mut buf = Vec::with_capacity(capacity);
            buf.resize(capacity, 0);
            if capacity != 0 {
                entry_files[shard_id]
                    .read_range(buf.as_mut_slice(), old_size_list[shard_id])
                    .unwrap();
            }
            result.push(buf);
        }
        result
    }
}

/// Core trait defining the database interface.
/// Provides operations for reading, writing, and verifying state.
///
/// # Thread Safety
/// All methods are thread-safe and can be called from multiple threads.
///
/// # Implementation Notes
/// - Implement proper error handling
/// - Ensure consistent state transitions
/// - Handle edge cases and invalid inputs
/// - Optimize for common access patterns
pub trait ADS: Send + Sync + 'static {
    /// Reads an entry from the database at a specific height.
    ///
    /// # Arguments
    /// * `height` - Block height to read from
    /// * `key_hash` - Hash of the key to read
    /// * `key` - Original key
    /// * `buf` - Buffer to store the read data
    ///
    /// # Returns
    /// The size of the read data, or -1 if not found
    fn read_entry(&self, height: i64, key_hash: &[u8], key: &[u8], buf: &mut ReadBuf) -> i64;

    fn read_entry_with_uring(
        &self,
        height: i64,
        key_hash: &[u8],
        key: &[u8],
        buf: &mut ReadBuf,
    ) -> i64;

    /// Warms up the cache for a specific block height and key prefix.
    ///
    /// # Arguments
    /// * `height` - Block height to warm up
    /// * `k80` - 80-bit key prefix for selective warming
    fn warmup(&self, height: i64, k80: &[u8]);

    /// Adds a task to be processed.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to add
    fn add_task(&self, task_id: i64);

    /// Adds a task to the updater component.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to add
    fn add_task_to_updater(&self, task_id: i64);

    /// Fetches entries from previous blocks for a task.
    /// Only available on Linux systems.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to fetch entries for
    fn fetch_prev_entries(&self, task_id: i64);

    /// Inserts extra data at a specific block height.
    ///
    /// # Arguments
    /// * `height` - Block height to insert at
    /// * `data` - Extra data to insert
    fn insert_extra_data(&self, height: i64, data: String);

    /// Gets the root hash for a specific block height.
    ///
    /// # Arguments
    /// * `height` - Block height to get the root hash for
    ///
    /// # Returns
    /// The 32-byte root hash
    fn get_root_hash_of_height(&self, height: i64) -> [u8; 32];
}

impl ADS for SharedAdsWrap {
    /// Reads an entry from the database.
    ///
    /// # Arguments
    /// * `height` - Block height to read from
    /// * `key_hash` - Hash of the key to read
    /// * `key` - Original key
    /// * `buf` - Buffer to store the read data
    ///
    /// # Returns
    /// The size of the read data, or -1 if not found
    fn read_entry(&self, height: i64, key_hash: &[u8], key: &[u8], buf: &mut ReadBuf) -> i64 {
        let cache = if height < 0 {
            None
        } else {
            Some(self.cache.as_ref())
        };
        self.ads
            .read_entry(height, key_hash, key, cache, buf, false)
    }

    fn read_entry_with_uring(
        &self,
        height: i64,
        key_hash: &[u8],
        key: &[u8],
        buf: &mut ReadBuf,
    ) -> i64 {
        let cache = if height < 0 {
            None
        } else {
            Some(self.cache.as_ref())
        };
        self.ads.read_entry(height, key_hash, key, cache, buf, true)
    }

    /// Warms up the cache for a specific block height and key prefix.
    ///
    /// # Arguments
    /// * `height` - Block height to warm up
    /// * `k80` - 80-bit key prefix for selective warming
    fn warmup(&self, height: i64, k80: &[u8]) {
        self.ads.warmup_cache(height, k80, self.cache.clone());
    }

    /// Adds a task to be processed.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to add
    fn add_task(&self, task_id: i64) {
        self.ads.add_task(task_id);
    }

    /// Adds a task to the updater component.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to add
    fn add_task_to_updater(&self, task_id: i64) {
        self.ads.add_task_to_updater(task_id);
    }

    /// Fetches entries from previous blocks for a task.
    /// Only available on Linux systems.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to fetch entries for
    fn fetch_prev_entries(&self, task_id: i64) {
        self.ads.fetch_prev_entries(task_id, self.cache.clone());
    }

    /// Inserts extra data at a specific block height.
    ///
    /// # Arguments
    /// * `height` - Block height to insert at
    /// * `data` - Extra data to insert
    fn insert_extra_data(&self, height: i64, data: String) {
        self.ads
            .get_metadb()
            .write()
            .insert_extra_data(height, data);
    }

    /// Gets the root hash for a specific block height.
    ///
    /// # Arguments
    /// * `height` - Block height to get the root hash for
    ///
    /// # Returns
    /// The 32-byte root hash
    fn get_root_hash_of_height(&self, height: i64) -> [u8; 32] {
        return self.ads.get_metadb().read().get_hash_of_root_hash(height);
    }
}

impl SharedAdsWrap {
    /// Creates a new SharedAdsWrap instance.
    ///
    /// # Arguments
    /// * `ads` - Core database engine
    /// * `cache` - Entry cache
    ///
    /// # Returns
    /// A new SharedAdsWrap instance
    pub fn new(ads: Arc<AdsCore>, cache: Arc<EntryCache>) -> Self {
        SharedAdsWrap { ads, cache }
    }
}

#[cfg(test)]
mod tests {
    use config::Config;
    use std::collections::HashSet;
    use tempfile::Builder; // avoid conflict with the local tempfile in scope

    use crate::{
        tasks::BlockPairTaskHub,
        test_helper::{task_builder::TaskBuilder, SimpleTask},
    };

    use super::*;

    #[test]
    fn test_init_dir() {
        let temp_dir = Builder::new().prefix("test_init_dir").tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        let mut config = config::Config::from_dir(&dir_path);
        config.set_with_twig_file(true);
        #[cfg(feature = "tee_cipher")]
        config.set_aes_keys([1; 96]);

        AdsCore::init_dir(&config);

        assert_eq!(
            [format!("{}/data", dir_path), format!("{}/metadb", dir_path)].join(","),
            [format!("{}/data", dir_path), format!("{}/metadb", dir_path)].join(",")
        );

        let data_dir_path = format!("{}/data", dir_path);
        let expected_paths: HashSet<_> = [
            (0..SHARD_COUNT)
                .map(|i| format!("{}/entries{}", data_dir_path, i))
                .collect::<Vec<String>>(),
            (0..SHARD_COUNT)
                .map(|i| format!("{}/twig{}", data_dir_path, i))
                .collect::<Vec<String>>(),
        ]
        .concat()
        .into_iter()
        .collect();

        let actual_paths: HashSet<_> = std::fs::read_dir(&data_dir_path)
            .unwrap()
            .map(|entry| entry.unwrap().path().to_str().unwrap().to_string())
            .collect();

        assert_eq!(actual_paths, expected_paths);

        let (_data_dir, meta_dir, _indexer_dir) = AdsCore::get_sub_dirs(&dir_path);
        let mut meta = MetaDB::with_dir(&meta_dir, get_ciphers(&config.aes_keys).2);
        meta.reload_from_file();

        #[cfg(not(feature = "tee_cipher"))]
        for i in 0..SHARD_COUNT {
            assert_eq!(
                4096 * 88 * (16 / SHARD_COUNT as i64),
                meta.get_entry_file_size(i)
            );
            assert_eq!(
                147416 * (16 / SHARD_COUNT as i64),
                meta.get_twig_file_size(i)
            );
            assert_eq!(
                4096 * (16 / SHARD_COUNT as u64),
                meta.get_next_serial_num(i)
            );
        }
        #[cfg(feature = "tee_cipher")]
        for i in 0..SHARD_COUNT {
            assert_eq!(
                4096 * (88 + crate::def::TAG_SIZE) * (16 / SHARD_COUNT),
                meta.get_entry_file_size(i) as usize
            );
            assert_eq!(
                147416 * (16 / SHARD_COUNT as i64),
                meta.get_twig_file_size(i)
            );
            assert_eq!(
                4096 * (16 / SHARD_COUNT as u64),
                meta.get_next_serial_num(i)
            );
        }
    }

    #[test]
    fn test_adscore() {
        let temp_dir = Builder::new().prefix("test_adscore").tempdir().unwrap();

        let task_hub = Arc::new(BlockPairTaskHub::<SimpleTask>::new());
        let mut config = config::Config::from_dir(temp_dir.path().to_str().unwrap());
        if cfg!(feature = "tee_cipher") {
            config.set_aes_keys([1; 96]);
        }

        AdsCore::init_dir(&config);
        let _ads_core = AdsCore::new(task_hub, &config);
    }

    #[test]
    fn test_start_block() {
        let temp_dir = Builder::new().prefix("test_start_block").tempdir().unwrap();
        let config = Config::from_dir(temp_dir.path().to_str().unwrap());
        AdsCore::init_dir(&config);

        let mut ads = AdsWrap::new(&config);

        for h in 1..=3 {
            let task_id = join_task_id(h, 0, true);
            let r = ads.start_block(
                h,
                Arc::new(TasksManager::new(
                    vec![RwLock::new(Some(
                        TaskBuilder::new()
                            .create(&(h as u64).to_be_bytes(), b"v1")
                            .build(),
                    ))],
                    3,
                )),
            );
            assert!(r.0);
            if h <= 2 {
                assert!(r.1.is_none());
            } else {
                assert_eq!(r.1.as_ref().unwrap().curr_height, 1);
                assert_eq!(r.1.as_ref().unwrap().extra_data, format!("height:{}", 1));
            }
            let shared_ads = ads.get_shared();
            shared_ads.insert_extra_data(h, format!("height:{}", h));
            shared_ads.add_task(task_id);
        }
        let r = ads.flush();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].curr_height, 2);
        assert_eq!(r[1].curr_height, 3);
    }
}
