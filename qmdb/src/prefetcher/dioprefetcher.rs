//! Direct I/O prefetcher implementation for Linux systems.
//!
//! This module provides a high-performance prefetching implementation that:
//! - Uses io_uring for asynchronous I/O operations
//! - Manages direct I/O buffers for optimal performance
//! - Coordinates prefetch operations across multiple shards
//! - Handles both task-based and warmup-based prefetching

use crate::def::{DEFAULT_ENTRY_SIZE, OP_CREATE, OP_DELETE, SHARD_COUNT, SUB_JOB_RESERVE_COUNT};
use crate::entryfile::readbuf::ReadBuf;
use crate::entryfile::{EntryBuffer, EntryBz, EntryCache, EntryFile};
use crate::indexer::{Indexer, IndexerTrait};
use crate::tasks::taskid::{is_first_task_in_block, split_task_id};
use crate::tasks::TaskHub;
use crate::utils::byte0_to_shard_id;
use atomptr::{AtomPtr, Ref};
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use hpfile::{direct_io, IO_BLK_SIZE};
use io_uring::{cqueue, opcode, squeue, types, IoUring};
use log::debug;
use std::iter::repeat_with;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::usize;
use threadpool::ThreadPool;
use {
    crate::def::{JOB_COUNT, WARMUP_JOB_COUNT},
    std::fs::File,
    std::os::unix::io::AsRawFd,
};

const SC: Ordering = Ordering::SeqCst;
pub const BLK_SIZE: i64 = 4096;

/// Main prefetcher component that manages prefetching operations.
///
/// Coordinates:
/// - Task-based prefetching
/// - Warmup prefetching
/// - Cache management
/// - Job scheduling
pub struct Prefetcher {
    /// Task hub for accessing task information
    task_hub: Arc<dyn TaskHub>,
    /// Entry cache for storing prefetched data
    cache: AtomPtr<Arc<EntryCache>>,
    /// Indexer for looking up entry positions
    indexer: Arc<Indexer>,
    /// Job manager for coordinating prefetch operations
    job_man: Arc<JobManager>,
    /// Thread pool for executing tasks
    tpool: Arc<ThreadPool>,
    /// Channel for receiving task jobs
    bck_receiver: Receiver<Box<TaskJob>>,
    /// Channels for receiving warmup jobs
    warmup_bck_receivers: Vec<Receiver<Box<WarmupJob>>>,
}

/// Manages prefetch jobs and I/O operations across shards.
///
/// Handles:
/// - Buffer management
/// - I/O scheduling
/// - Job coordination
/// - Completion notification
pub struct JobManager {
    /// Entry buffers for each shard
    update_buffers: Vec<Arc<EntryBuffer>>,
    /// Entry files for each shard
    entry_files: Vec<Arc<EntryFile>>,
    /// Channels for signaling job completion
    done_chans: Vec<Sender<i64>>,
    /// Channel for sending task jobs
    bck_sender: Sender<Box<TaskJob>>,
    /// Channels for sending warmup jobs
    warmup_bck_senders: Vec<Sender<Box<WarmupJob>>>,
    /// Channels for sending sub-jobs to io_uring instances
    sub_id_senders: Vec<Sender<(u32, Arc<Job>)>>,
    /// Number of io_uring instances
    uring_count: usize,
    /// Size of each io_uring instance
    uring_size: u32,
}

impl Prefetcher {
    /// Creates a new Prefetcher instance.
    ///
    /// # Arguments
    /// * `task_hub` - Task hub for accessing task information
    /// * `cache` - Entry cache for storing prefetched data
    /// * `indexer` - Indexer for looking up entry positions
    /// * `tpool` - Thread pool for executing tasks
    /// * `job_man` - Job manager for coordinating prefetch operations
    ///
    /// # Returns
    /// A new Prefetcher instance
    pub fn new(
        task_hub: Arc<dyn TaskHub>,
        cache: Arc<EntryCache>,
        indexer: Arc<Indexer>,
        tpool: Arc<ThreadPool>,
        job_man: JobManager,
    ) -> Self {
        let uring_count = job_man.uring_count;
        let (job_man, bck_receiver, warmup_bck_receivers) = job_man.start_threads(uring_count);
        Self {
            task_hub,
            cache: AtomPtr::new(cache),
            indexer,
            job_man,
            tpool,
            bck_receiver,
            warmup_bck_receivers,
        }
    }

    /// Starts the prefetcher threads.
    ///
    /// # Arguments
    /// * `task_receiver` - Channel for receiving task IDs to process
    pub fn start_threads(self: Arc<Self>, task_receiver: Receiver<i64>) {
        std::thread::spawn(move || loop {
            match task_receiver.recv() {
                Ok(task_id) => {
                    self.run_task(task_id);
                }
                Err(_) => {
                    debug!("task receiver in prefetcher exit!");
                    return;
                }
            };
        });
    }

    /// Runs a warmup job for prefetching.
    ///
    /// # Arguments
    /// * `file_pos` - Position in the entry file
    /// * `shard_id` - ID of the shard
    /// * `cache` - Cache to store the fetched data
    pub fn run_warmup(&self, file_pos: i64, shard_id: usize, cache: Arc<EntryCache>) {
        let count = self.warmup_bck_receivers.len();
        let uring_id = SubJob::uring_id(file_pos, shard_id, count);
        let mut job = self.warmup_bck_receivers[uring_id].recv().unwrap();
        job.cache = Some(cache);
        job.add(0, shard_id, file_pos);
        let job_man = self.job_man.clone();
        job_man.add_job(Arc::new(Job::Warmup(job)));
    }

    /// Runs a task job for prefetching.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to process
    pub fn run_task(&self, task_id: i64) {
        //if (task_id >> 24) > 76 {
        //    println!("HH run_task {:#016x}", task_id);
        //}
        let height = split_task_id(task_id).0;
        if is_first_task_in_block(task_id) {
            let cache = self.task_hub.get_entry_cache(height);
            let old = self.cache.swap(cache);
            drop(old);
        }
        let change_sets = self.task_hub.get_change_sets(task_id).clone();
        //println!("AA to_get_job task_id={:#016x}", task_id);
        let mut job = self.bck_receiver.recv().unwrap();
        job.need_send_to_updater = true;
        job.task_id = task_id;
        let arc: Ref<Arc<EntryCache>> = self.cache.get_ref();
        job.cache = Some(Arc::clone(&arc));
        let indexer = self.indexer.clone();
        let change_sets = change_sets.clone();
        let job_man = self.job_man.clone();
        self.tpool.execute(move || {
            let mut sub_id = 0u32;
            for change_set in change_sets.iter() {
                change_set.run_all(|op, kh: &[u8; 32], _k, _v, _r| {
                    let shard_id = byte0_to_shard_id(kh[0]);
                    //if shard_id==0 && task_id==TASK_ID  {
                    //    println!("HH kh={:?}", &kh[..4]);
                    //}

                    let pos_list = if op == OP_CREATE || op == OP_DELETE {
                        indexer.for_each_adjacent_value(height, &kh[..])
                    } else {
                        indexer.for_each(height, op, &kh[..])
                    };

                    pos_list.enumerate().for_each(|pos| {
                        job.add(sub_id, shard_id, pos);
                        sub_id += 1;
                    });
                });
            }
            job_man.add_job(Arc::new(Job::Task(job)));
            for shard_id in 0..SHARD_COUNT {
                job_man.done_chans[shard_id].send(task_id).unwrap();
            }
        });
    }

    /// Fetches entries from previous tasks.
    ///
    /// # Arguments
    /// * `task_id` - ID of the task to process
    pub fn fetch_prev_entries(&self, task_id: i64, cache: Arc<EntryCache>) {
        let height = split_task_id(task_id).0;
        let change_sets = self.task_hub.get_change_sets(task_id).clone();
        let need_job = change_sets.iter().any(|x| x.has_create_del());
        if !need_job {
            // for shard_id in 0..SHARD_COUNT {
            //     self.job_man.done_chans[shard_id].send(task_id).unwrap();
            // }
            return;
        }

        let mut job = self.bck_receiver.recv().unwrap();
        job.task_id = task_id;
        job.cache = Some(cache);
        job.need_send_to_updater = false;
        let mut sub_id = 0u32;
        for change_set in change_sets.iter() {
            change_set.run_all(|op, kh: &[u8; 32], _k, _v, _r| {
                if op == OP_CREATE || op == OP_DELETE {
                    let shard_id = byte0_to_shard_id(kh[0]);
                    let v = self.indexer.for_each_adjacent_value(height, &kh[..]);
                    v.enumerate().for_each(|pos| {
                        job.add(sub_id, shard_id, pos);
                        sub_id += 1;
                    });
                }
            });
        }
        self.job_man.add_job(Arc::new(Job::Task(job)));
    }
}

/// Represents a job to be processed by the prefetcher.
///
/// Can be either:
/// - A task job for processing multiple entries
/// - A warmup job for prefetching specific entries
enum Job {
    /// Task-based job for processing multiple entries
    Task(Box<TaskJob>),
    /// Warmup job for prefetching specific entries
    Warmup(Box<WarmupJob>),
}

/// A job for processing multiple entries as part of a task.
pub struct TaskJob {
    /// Cache for storing prefetched entries
    cache: Option<Arc<EntryCache>>,
    /// ID of the task being processed
    task_id: i64,
    /// List of sub-jobs to process
    sub_jobs: Vec<SubJob>,
    /// Total number of sub-jobs
    sub_job_count: usize,
    need_send_to_updater: bool,
}

/// A job for prefetching specific entries during warmup.
pub struct WarmupJob {
    /// Cache for storing prefetched entries
    cache: Option<Arc<EntryCache>>,
    /// Single sub-job to process
    sub_jobs: [SubJob; 1],
}

impl Default for TaskJob {
    /// Creates a new TaskJob with default settings.
    ///
    /// Initializes:
    /// - Empty read counts for each shard
    /// - No cache
    /// - Minimum task ID
    /// - Empty sub-jobs list
    fn default() -> Self {
        let mut v = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            v.push(AtomicUsize::new(0));
        }
        Self {
            cache: None,
            task_id: i64::MIN,
            sub_jobs: Vec::new(),
            sub_job_count: 0,
            need_send_to_updater: true,
        }
    }
}

impl Job {
    /// Gets a reference to a specific sub-job.
    ///
    /// # Arguments
    /// * `idx` - Index of the sub-job
    ///
    /// # Returns
    /// Reference to the requested sub-job
    fn get_sub_job(&self, idx: usize) -> &SubJob {
        match self {
            Self::Task(job) => &job.sub_jobs[idx],
            Self::Warmup(job) => &job.sub_jobs[idx],
        }
    }

    /// Gets the total number of sub-jobs.
    ///
    /// # Returns
    /// Number of sub-jobs in this job
    fn get_sub_job_count(&self) -> usize {
        match self {
            Self::Task(job) => job.sub_job_count,
            Self::Warmup(_) => 1,
        }
    }

    /// Gets a reference to the entry cache.
    ///
    /// # Returns
    /// Reference to the entry cache
    fn get_entry_cache(&self) -> &Arc<EntryCache> {
        match self {
            Self::Task(job) => job.cache.as_ref().unwrap(),
            Self::Warmup(job) => job.cache.as_ref().unwrap(),
        }
    }
}

impl TaskJob {
    /// Adds a new sub-job to this task.
    ///
    /// # Arguments
    /// * `sub_id` - ID for the new sub-job
    /// * `shard_id` - ID of the shard
    /// * `file_pos` - Position in the entry file
    fn add(&mut self, sub_id: u32, shard_id: usize, file_pos: i64) {
        if self.sub_job_count < self.sub_jobs.len() {
            let i = self.sub_job_count;
            self.sub_jobs[i].reinit(sub_id, shard_id, file_pos);
        } else {
            let sj = SubJob::new(sub_id, shard_id, file_pos);
            self.sub_jobs.push(sj);
        }
        self.sub_job_count += 1;
    }

    /// Clears this task job for reuse.
    ///
    /// Resets:
    /// - Read counts
    /// - Cache
    /// - Task ID
    /// - Sub-job count
    fn clear(&mut self) {
        self.cache = None;
        self.task_id = i64::MIN;
        self.sub_job_count = 0;
        self.sub_jobs.truncate(SUB_JOB_RESERVE_COUNT);
        self.sub_jobs.shrink_to_fit();
    }
}

impl WarmupJob {
    /// Adds a new sub-job to this warmup job.
    ///
    /// # Arguments
    /// * `sub_id` - ID for the new sub-job
    /// * `shard_id` - ID of the shard
    /// * `file_pos` - Position in the entry file
    fn add(&mut self, sub_id: u32, shard_id: usize, file_pos: i64) {
        let sub_job = self.sub_jobs.get_mut(0).unwrap();
        sub_job.reinit(sub_id, shard_id, file_pos);
    }

    /// Clears this warmup job for reuse.
    ///
    /// Resets:
    /// - Cache
    /// - Data sender
    fn clear(&mut self) {
        self.cache = None;
    }
}

impl Default for WarmupJob {
    /// Creates a new WarmupJob with default settings.
    ///
    /// # Returns
    /// A new WarmupJob with:
    /// - No cache
    /// - No data sender
    /// - One empty sub-job
    fn default() -> Self {
        Self {
            cache: None,
            sub_jobs: [SubJob::new(0, 0, 0)],
        }
    }
}

impl JobManager {
    /// Creates a new JobManager instance.
    ///
    /// # Arguments
    /// * `uring_count` - Number of io_uring instances
    /// * `uring_size` - Size of each io_uring instance
    ///
    /// # Returns
    /// A new JobManager instance
    pub fn new(uring_count: usize, uring_size: u32) -> Self {
        let (bck_sender, _) = bounded(1);

        JobManager {
            update_buffers: Vec::with_capacity(SHARD_COUNT),
            entry_files: Vec::with_capacity(SHARD_COUNT),
            done_chans: Vec::with_capacity(SHARD_COUNT),
            bck_sender,
            warmup_bck_senders: Vec::with_capacity(0),
            sub_id_senders: Vec::with_capacity(0),
            uring_count,
            uring_size,
        }
    }

    /// Adds a shard to the JobManager.
    ///
    /// # Arguments
    /// * `update_buffer` - Buffer for updates
    /// * `entry_file` - File containing entries
    /// * `done_chan` - Channel for completion notifications
    pub fn add_shard(
        &mut self,
        update_buffer: Arc<EntryBuffer>,
        entry_file: Arc<EntryFile>,
        done_chan: Sender<i64>,
    ) {
        self.update_buffers.push(update_buffer);
        self.entry_files.push(entry_file);
        self.done_chans.push(done_chan);
    }

    /// Adds a sub-job and checks if it needs to read from SSD.
    ///
    /// # Arguments
    /// * `j` - Reference to the parent job
    /// * `sj` - Reference to the sub-job
    ///
    /// # Returns
    /// `true` if the sub-job needs to read from SSD, `false` otherwise
    fn add_sub_job(&self, j: &Job, sj: &SubJob) -> bool {
        // try to insert a locked entry_pos
        let cache = &j.get_entry_cache();
        let cache_hit = cache.contains(sj.shard_id as usize, sj.file_pos);
        if cache_hit {
            return false; // no need to fetch
        }
        let update_buffer = &self.update_buffers[sj.shard_id as usize];
        // in 'get_entry_bz_at', 'curr_buf' is None because we cannot read it
        let mut buf = ReadBuf::new();
        let in_disk = update_buffer.get_entry_bz(sj.file_pos, &mut buf);
        if !buf.is_empty() {
            let e = buf.as_entry_bz();
            cache.insert(sj.shard_id as usize, sj.file_pos, &e);
            return false;
        }
        in_disk
    }

    /// Builds job channels for task and warmup jobs.
    ///
    /// # Returns
    /// A tuple containing:
    /// - Arc reference to self
    /// - Receiver for task jobs
    /// - Vector of receivers for warmup jobs
    fn build_job_channel(
        mut self,
    ) -> (
        Arc<Self>,
        Receiver<Box<TaskJob>>,
        Vec<Receiver<Box<WarmupJob>>>,
    ) {
        let (bck_sender, bck_receiver) = unbounded();

        for _ in 0..JOB_COUNT {
            bck_sender.send(Box::new(TaskJob::default())).unwrap();
        }

        let mut warmup_bck_senders = Vec::with_capacity(self.uring_count);
        let mut warmup_bck_receivers = Vec::with_capacity(self.uring_count);
        for _ in 0..self.uring_count {
            let (sender, receiver) = unbounded();
            for _ in 0..WARMUP_JOB_COUNT {
                sender.send(Box::new(WarmupJob::default())).unwrap();
            }
            warmup_bck_senders.push(sender);
            warmup_bck_receivers.push(receiver);
        }
        self.bck_sender = bck_sender;
        self.warmup_bck_senders = warmup_bck_senders;
        (Arc::new(self), bck_receiver, warmup_bck_receivers)
    }

    /// Recycles a job object after completion.
    ///
    /// # Arguments
    /// * `job` - Job to recycle
    /// * `bz` - Buffer containing entry data
    fn recycle_job_object(&self, job: Arc<Job>, _bz: &[u8]) {
        if let Some(job) = Arc::<Job>::into_inner(job) {
            match job {
                Job::Task(mut j) => {
                    j.clear();
                    self.bck_sender.send(j).unwrap();
                }
                Job::Warmup(mut j) => {
                    let uring_id = j.sub_jobs[0].get_uring_id(self.uring_count);
                    j.clear();
                    self.warmup_bck_senders[uring_id].send(j).unwrap();
                }
            };
        }
    }

    /// Starts threads to drive io_urings.
    ///
    /// # Arguments
    /// * `uring_count` - Number of io_uring instances
    ///
    /// # Returns
    /// A tuple containing:
    /// - Arc reference to self
    /// - Receiver for task jobs
    /// - Vector of receivers for warmup jobs
    pub fn start_threads(
        mut self,
        uring_count: usize,
    ) -> (
        Arc<Self>,
        Receiver<Box<TaskJob>>,
        Vec<Receiver<Box<WarmupJob>>>,
    ) {
        self.sub_id_senders = Vec::with_capacity(uring_count);
        let mut sub_id_receivers = Vec::with_capacity(uring_count);
        for _ in 0..uring_count {
            let (sub_id_sender, sub_id_receiver) = unbounded();
            self.sub_id_senders.push(sub_id_sender);
            sub_id_receivers.push(sub_id_receiver);
        }
        let (job_man, bck_receiver, warmup_bck_receivers) = self.build_job_channel();
        for _ in 0..uring_count {
            let sub_id_receiver = sub_id_receivers.pop().unwrap();
            let job_man = job_man.clone();
            std::thread::spawn(move || {
                Self::uring_loop(job_man, sub_id_receiver);
            });
        }
        (job_man, bck_receiver, warmup_bck_receivers)
    }

    /// Adds a job to be processed.
    ///
    /// # Arguments
    /// * `job` - Job to add
    /// * `only_disk` - Whether to only read from disk
    fn add_job(&self, job: Arc<Job>) {
        for i in 0..job.get_sub_job_count() {
            let sj = job.get_sub_job(i);
            let need_read_ssd = self.add_sub_job(&job, sj);
            sj.need_read_ssd.store(need_read_ssd, SC);
            //println!("AA SET id={:#016x} need={}", sj.sub_id, need_read_ssd);
        }
        for i in 0..job.get_sub_job_count() {
            let sj = &job.get_sub_job(i);
            let need_read_ssd = sj.need_read_ssd.load(SC);
            //println!("AA GET id={:#016x} need={}", sj.sub_id, need_read_ssd);
            if need_read_ssd {
                let uring_id = sj.get_uring_id(self.sub_id_senders.len());
                //println!("AA send sub_id {:#016x}", sj.sub_id);
                self.sub_id_senders[uring_id]
                    .send((sj.sub_id, job.clone()))
                    .unwrap();
            }
        }
        self.recycle_job_object(job, &[]);
    }

    /// Gets a read entry for io_uring.
    ///
    /// # Arguments
    /// * `sj` - Reference to the sub-job
    /// * `buf` - Buffer to read into
    /// * `id` - Buffer ID
    ///
    /// # Returns
    /// A tuple containing:
    /// - io_uring entry
    /// - File reference
    pub fn get_read_e(file: &File, offset: i64, buf: &mut [u8], id: u32) -> squeue::Entry {
        let fd = file.as_raw_fd();
        let buf_ptr = buf.as_ptr() as *mut u8;
        // we are sure one entry is no larger than BLK_SIZE
        let end = offset % BLK_SIZE + (DEFAULT_ENTRY_SIZE as i64);
        let blk_count = if end > BLK_SIZE { 2 } else { 1 };
        let aligned_offset = (offset / BLK_SIZE) * BLK_SIZE;
        let read_len = (BLK_SIZE * blk_count) as u32;

        opcode::Read::new(types::Fd(fd), buf_ptr, read_len)
            .offset(aligned_offset as u64)
            .build()
            .user_data(id as u64)
    }

    /// Main loop for io_uring processing.
    ///
    /// # Arguments
    /// * `job_man` - Reference to the job manager
    /// * `sub_id_receiver` - Receiver for sub-job IDs
    fn uring_loop(job_man: Arc<Self>, sub_id_receiver: Receiver<(u32, Arc<Job>)>) {
        let mut buf_list = BufList::new(job_man.uring_size);
        let mut sub_id_vec: Vec<(u32, Option<Arc<Job>>, Option<Arc<(File, bool)>>)> =
            repeat_with(|| (0u32, None, None))
                .take(job_man.uring_size as usize)
                .collect();
        let mut ring = IoUring::<squeue::Entry, cqueue::Entry>::builder()
            //.setup_sqpoll(5000)
            .setup_single_issuer()
            .build(job_man.uring_size)
            .unwrap();
        //new(URING_SIZE).unwrap();
        // keep files being read in hashmap to prevent it from being dropped

        let mut reaming_count = 0;
        loop {
            let mut sq = ring.submission();
            let mut can_submit = false;
            while buf_list.has_free_buf() && !sq.is_full() {
                let (sub_id, job) = if reaming_count == 0 {
                    match sub_id_receiver.recv() {
                        Ok((sub_id, job)) => (sub_id, job),
                        Err(_) => {
                            eprintln!("uring worker receiver disconnected");
                            return;
                        }
                    }
                } else {
                    match sub_id_receiver.try_recv() {
                        Ok((sub_id, job)) => (sub_id, job),
                        Err(err) => match err {
                            crossbeam::channel::TryRecvError::Empty => {
                                break;
                            }
                            crossbeam::channel::TryRecvError::Disconnected => {
                                eprintln!("uring worker receiver disconnected");
                                return;
                            }
                        },
                    }
                };
                reaming_count += 1;
                can_submit = true;
                let idx = sub_id as usize;
                let sj = job.get_sub_job(idx);
                let (buf, id) = buf_list.allocate_buf();
                let (file, offset) =
                    job_man.entry_files[sj.shard_id as usize].get_file_and_pos(sj.file_pos);
                let read_e = JobManager::get_read_e(&file.0, offset, buf, id);
                let f = if file.1 {
                    None
                } else {
                    // still a file being appended
                    Some(file)
                };
                sub_id_vec[id as usize] = (sj.sub_id, Some(job), f);
                unsafe { sq.push(&read_e).unwrap() };
            }
            drop(sq);
            if can_submit {
                ring.submit().unwrap();
            }
            for cqe in ring.completion() {
                reaming_count -= 1;
                let buf_id = cqe.user_data() as u32;
                let mut tmp = (0u32, None, None);
                std::mem::swap(&mut sub_id_vec[buf_id as usize], &mut tmp);
                let (sub_id, job) = (tmp.0, tmp.1.unwrap());
                if cqe.result() <= 0 {
                    panic!("AA uring read error");
                }
                //println!("AA done sub_id {:#016x}", sub_id);
                job_man.uring_finish_sub_job(buf_id, job, sub_id, &buf_list);
                buf_list.return_buf(buf_id);
            }
        }
    }

    /// Finishes processing a sub-job.
    ///
    /// # Arguments
    /// * `buf_id` - ID of the buffer
    /// * `job` - Job being processed
    /// * `sub_id` - ID of the sub-job
    /// * `buf_list` - Reference to the buffer list
    fn uring_finish_sub_job(&self, buf_id: u32, job: Arc<Job>, sub_id: u32, buf_list: &BufList) {
        let idx = sub_id as usize;
        let sj = job.get_sub_job(idx);
        let offset = (sj.file_pos % BLK_SIZE) as usize;
        let buf = buf_list.get_buf(buf_id, offset);
        let len = EntryBz::get_entry_len(buf);
        let e = EntryBz { bz: &buf[..len] };
        if len == 0 || len > BLK_SIZE as usize || e.key().len() < 20 {
            panic!(
                "AA file_pos={} offset={} len={}\n buf={:?}",
                sj.file_pos, offset, len, buf,
            );
        }
        job.get_entry_cache()
            .insert(sj.shard_id as usize, sj.file_pos, &e);
        // the last finished sub_job send task_id to done_chan
        // self.handle_sub_job_completed(&job, sj.shard_id as usize);
        self.recycle_job_object(job, e.bz);
    }
}

/// A sub-job representing a single I/O operation.
///
/// Contains:
/// - Sub-job ID
/// - Shard ID
/// - File position
/// - SSD read status
#[derive(Debug)]
struct SubJob {
    /// ID of this sub-job
    sub_id: u32,
    /// ID of the target shard
    shard_id: u8,
    /// Whether this job needs to read from SSD
    need_read_ssd: AtomicBool,
    /// Position in the entry file
    file_pos: i64,
}

impl SubJob {
    /// Creates a new SubJob.
    ///
    /// # Arguments
    /// * `sub_id` - ID for the new sub-job
    /// * `shard_id` - ID of the shard
    /// * `file_pos` - Position in the entry file
    ///
    /// # Returns
    /// A new SubJob instance
    fn new(sub_id: u32, shard_id: usize, file_pos: i64) -> Self {
        //buf.fill(0);
        SubJob {
            sub_id,
            shard_id: shard_id as u8,
            file_pos,
            need_read_ssd: AtomicBool::new(false),
        }
    }

    /// Reinitializes this sub-job with new values.
    ///
    /// # Arguments
    /// * `sub_id` - New sub-job ID
    /// * `shard_id` - New shard ID
    /// * `file_pos` - New file position
    fn reinit(&mut self, sub_id: u32, shard_id: usize, file_pos: i64) {
        self.sub_id = sub_id;
        self.shard_id = shard_id as u8;
        self.file_pos = file_pos;
        self.need_read_ssd.store(false, SC);
        //self.buf.fill(0);
    }

    /// Calculates the io_uring instance ID for a given position.
    ///
    /// # Arguments
    /// * `file_pos` - Position in the entry file
    /// * `shard_id` - ID of the shard
    /// * `uring_count` - Total number of io_uring instances
    ///
    /// # Returns
    /// The calculated io_uring instance ID
    fn uring_id(file_pos: i64, shard_id: usize, uring_count: usize) -> usize {
        let mut r = file_pos as usize;
        r = (r >> 10) ^ (r >> 3) ^ shard_id;
        r % uring_count
    }

    /// select a io_uring in a pseudo-random way
    ///
    /// # Arguments
    /// * `uring_count` - Total number of io_uring instances
    ///
    /// # Returns
    /// The io_uring instance ID for this sub-job
    fn get_uring_id(&self, uring_count: usize) -> usize {
        Self::uring_id(self.file_pos, self.shard_id as usize, uring_count)
    }
}

/// Manages a pool of direct I/O buffers.
///
/// Provides:
/// - Buffer allocation
/// - Buffer recycling
/// - Buffer access
pub struct BufList {
    /// List of allocated buffers
    buf_list: Vec<Box<[u8]>>,
    /// List of free buffer IDs
    free_buf_ids: Vec<u32>,
}

impl BufList {
    /// Creates a new BufList.
    ///
    /// # Arguments
    /// * `n` - Number of buffers to allocate
    ///
    /// # Returns
    /// A new BufList instance
    pub fn new(n: u32) -> Self {
        let mut res = Self {
            buf_list: Vec::with_capacity(n as usize),
            free_buf_ids: Vec::with_capacity(n as usize),
        };
        for i in 0..n {
            let v = direct_io::allocate_aligned_vec((BLK_SIZE * 2) as usize, IO_BLK_SIZE);
            res.buf_list.push(v.into_boxed_slice());
            res.free_buf_ids.push(i);
        }
        res
    }

    /// Checks if there are free buffers available.
    ///
    /// # Returns
    /// `true` if there are free buffers, `false` otherwise
    pub fn has_free_buf(&self) -> bool {
        !self.free_buf_ids.is_empty()
    }

    /// Allocates a buffer from the pool.
    ///
    /// # Returns
    /// A tuple containing:
    /// - A mutable reference to the allocated buffer
    /// - The buffer ID
    pub fn allocate_buf(&mut self) -> (&mut [u8], u32) {
        let id = self.free_buf_ids.pop().unwrap();
        (&mut self.buf_list[id as usize][..], id)
    }

    /// Gets a reference to a specific buffer.
    ///
    /// # Arguments
    /// * `id` - ID of the buffer
    /// * `offset` - Offset within the buffer
    ///
    /// # Returns
    /// A slice of the buffer starting at the specified offset
    pub fn get_buf(&self, id: u32, offset: usize) -> &[u8] {
        &self.buf_list[id as usize][offset..]
    }

    /// Returns a buffer to the pool.
    ///
    /// # Arguments
    /// * `id` - ID of the buffer to return
    pub fn return_buf(&mut self, id: u32) {
        self.free_buf_ids.push(id);
    }
}

/*
#[test]
fn test_run_task() {
    let temp_dir = tempfile::Builder::new()
        .prefix("prefetcher_ut_cache_hit")
        .tempdir()
        .unwrap();
    #[cfg(not(feature = "tee_cipher"))]
    let cipher = None;
    #[cfg(feature = "tee_cipher")]
    let cipher = {
        let key = Key::<Aes256Gcm>::from_slice(&[0; 32]);
        let cipher = Aes256Gcm::new(&key);
        Some(cipher)
    };

    let entry_file = Arc::new(EntryFile::new(
        8 * 1024,
        128 * 1024,
        temp_dir.path().to_str().unwrap().to_string(),
        false,
        cipher,
    ));
    let (mut entry_buffer_writer, _reader) = entrybuffer::new(0, 1024);
    let entry_cache = Arc::new(EntryCache::new());
    let indexer_arc = Arc::new(Indexer::new(1024));
    let task_hub = Arc::new(BlockPairTaskHub::<SimpleTask>::new());
    let pool = ThreadPool::new(1);

    let mut cs1 = ChangeSet::new();
    let key = "key".as_bytes();
    let kh = hasher::hash(key);
    let shard_id = byte0_to_shard_id(kh[0]) as u8;
    cs1.add_op(
        def::OP_DELETE,
        shard_id,
        &kh,
        key.as_ref(),
        &[1, 2, 3],
        Option::None,
    );
    cs1.sort();
    let task0 = SimpleTask::new(vec![ChangeSet::new()]);
    let task1 = SimpleTask::new(vec![cs1]);
    let tasks = vec![
        RwLock::new(Option::Some(task0)),
        RwLock::new(Option::Some(task1)),
    ];

    let entry = Entry {
        key: key,
        value: "val".as_bytes(),
        next_key_hash: &[0xab; 32],
        version: 12345,
        serial_number: 99999,
    };
    let pos = entry_buffer_writer.append(&entry, &[]);
    indexer_arc.add_kv(&kh[..10], pos, 0);

    for _i in 0..2000 {
        let _ = entry_buffer_writer.append(&entry, &[]);
    }

    task_hub.start_block(
        0x123,
        Arc::new(TasksManager::new(tasks, 0)),
        entry_cache.clone(),
    );

    let mut job_man = JobManager::new(0, 0, 0);
    let mut done_receivers = Vec::with_capacity(SHARD_COUNT);
    for _ in 0..SHARD_COUNT {
        let (done_sender, done_receiver) = bounded(1);
        job_man.add_shard(
            entry_buffer_writer.entry_buffer.clone(),
            entry_file.clone(),
            done_sender,
        );
        done_receivers.push(done_receiver);
    }

    let prefetcher = Prefetcher::new(
        task_hub,
        entry_cache.clone(),
        indexer_arc,
        Arc::new(pool),
        job_man,
    );

    prefetcher.run_task(0, 0x123000001);

    std::thread::sleep(std::time::Duration::from_secs(1));
    assert!(done_receivers[0].try_recv().is_ok());
    assert!(done_receivers[0].try_recv().is_err());
    assert!(done_receivers[shard_id as usize].try_recv().is_ok());
    assert!(done_receivers[shard_id as usize].try_recv().is_err());

    let mut filled_size = 0;
    entry_cache.lookup(shard_id as usize, 0, |entry_bz| {
        filled_size = entry_bz.bz.len();
        assert_eq!(entry_bz.value(), "val".as_bytes());
    });

    #[cfg(not(feature = "tee_cipher"))]
    assert_eq!(64, filled_size);
    #[cfg(feature = "tee_cipher")]
    assert_eq!(80, filled_size);
}*/
