// This file contains a preloader for speeding up replay of a blocks.dat file.
use std::sync::atomic::{AtomicUsize, Ordering};
use hpfile::{direct_io, IO_BLK_SIZE};

const UNIT: usize = 16;
const URING_BATCH: usize = 16;
const URING_SIZE: usize = 2048;
const URING_COUNT: usize = 4;

pub struct Preloader {
    cache: Arc<EntryCache>,
    entry_files: Vec<Arc<EntryFile>>,
    jobs: Vec<Job>,
    free_job_ids: Vec<usize>,
    index: usize,
    id: usize,
}

pub struct Job {
    shard_id: usize,
    file_pos: i64,
    buf: Vec<u8>,
}

impl Preloader {
    fn new(entry_files: Vec<Arc<EntryFile>>, id: usize) -> Self {
        let mut jobs = Vec::with_capacity(URING_SIZE);
        let mut free_job_ids = Vec::with_capacity(URING_SIZE);
        for i in 0..URING_SIZE {
            free_job_ids.push(i);
            let job = Job {
                shard_id: 0,
                file_pos: 0,
                buf: direct_io::allocate_aligned_vec(IO_BLK_SIZE * 2, IO_BLK_SIZE),
            };
            jobs.push(job);
        }
        Self {
            cache: Arc::new(EntryCache::new_uninit()),
            entry_files,
            jobs,
            free_job_ids,
            index: usize::MAX,
            id,
        }
    }

    fn start_new_block(&mut self, cache: Arc<EntryCache>) {
        self.index = UNIT - 1;
        self.cache = cache;
    }

    fn get_next_index(&mut self, shared_index: &Arc<AtomicUsize>, len: usize) -> (usize, bool) {
        if self.index % UNIT == UNIT - 1 {
            // we need to get the next unit
            self.index = shared_index.fetch_add(UNIT, Ordering::SeqCst);
        //println!("loader#{} index: {}", self.id, shared_index);
        } else {
            self.index += 1; //within the same unit
        }
        if self.index >= len {
            return (usize::MAX, true);
        }
        (self.index, false)
    }

    // fn get_read_e(&self, job_id: usize) -> squeue::Entry {
    //     let job = &self.jobs[job_id];
    //     let (file, offset) = self.entry_files[job.shard_id].get_file_and_pos(job.file_pos);
    //     let fd = file.0.as_raw_fd();
    //     let buf_ptr = job.buf.as_ptr() as *mut u8;
    //     let blk_size = IO_BLK_SIZE as i64;
    //     // we are sure one entry is no larger than IO_BLK_SIZE
    //     let blk_count = if offset % blk_size == 0 { 1 } else { 2 };
    //     let aligned_offset = (offset / blk_size) * blk_size;
    //     let read_len = (IO_BLK_SIZE * blk_count) as u32;
    //     opcode::Read::new(types::Fd(fd), buf_ptr, read_len)
    //         .offset(aligned_offset as u64)
    //         .build()
    //         .user_data(job_id as u64)
    // }

    // fn uring_loop(&mut self, offsets: &Arc<Vec<u8>>, shared_index: &Arc<AtomicUsize>) {
    //     let mut ring = IoUring::<squeue::Entry, cqueue::Entry>::builder()
    //         .setup_single_issuer()
    //         .build(URING_SIZE as u32)
    //         .unwrap();
    //     let (mut index, mut all_submit, mut all_completed) = (0, false, false);
    //     let vec_len = offsets.len() / 12;
    //     while !(all_submit && all_completed) {
    //         let mut can_submit = false;
    //         let mut sq = ring.submission();
    //         for _ in 0..URING_BATCH {
    //             if sq.is_full() {
    //                 //println!("is_full@{}", self.id);
    //                 break;
    //             }
    //             if self.free_job_ids.is_empty() {
    //                 break;
    //             }
    //             (index, all_submit) = self.get_next_index(shared_index, vec_len);
    //             if all_submit {
    //                 break;
    //             }
    //             can_submit = true;
    //             let start = index * 12 + 6;
    //             let mut buf = [0u8; 8];
    //             if start > offsets.len() {
    //                 panic!("WHY index={} all_submit={}", index, all_submit);
    //             }
    //             buf[..6].copy_from_slice(&offsets[start..start + 6]);
    //             let offset = i64::from_le_bytes(buf);
    //             //println!("index={} offset={}", index, offset);
    //             let job_id = self.free_job_ids.pop().unwrap();
    //             self.jobs[job_id].shard_id = (offset & 0xF) as usize;
    //             self.jobs[job_id].file_pos = offset >> 4;
    //             //println!("submit job_id: {} {} {}", job_id, self.jobs[job_id].shard_id, self.jobs[job_id].file_pos);
    //             let read_e = self.get_read_e(job_id);
    //             unsafe { sq.push(&read_e).unwrap() };
    //         }
    //         drop(sq);
    //         if can_submit {
    //             //println!("submit@{}", self.id);
    //             ring.submit().unwrap();
    //         }
    //         let mut cq = ring.completion();
    //         while !cq.is_empty() {
    //             let cqe = cq.next().unwrap();
    //             let job_id = cqe.user_data() as usize;
    //             //println!("complete job_id: {}", job_id);
    //             assert!(cqe.result() >= 0, "uring read error: {}", cqe.result());
    //             self.finish_job(job_id);
    //         }
    //         all_completed = self.free_job_ids.len() == self.jobs.len();
    //     }
    // }

    fn finish_job(&mut self, job_id: usize) {
        let job = &self.jobs[job_id];
        let offset = job.file_pos as usize % IO_BLK_SIZE;
        let buf = &job.buf[offset..];
        let len = EntryBz::get_entry_len(buf);
        let e = EntryBz { bz: &buf[..len] };
        //println!("len={}", len);
        if len > IO_BLK_SIZE {
            panic!(
                "AA file_pos={} offset={} len={}\n buf={:?}\n sjbuf={:?}",
                job.file_pos, offset, len, buf, job.buf
            );
        }
        self.cache.insert(job.shard_id, job.file_pos, &e);
        self.free_job_ids.push(job_id);
    }
}
