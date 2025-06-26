use std::{fs::File, iter::repeat_with, os::fd::AsRawFd, sync::Arc};

use crossbeam::channel::{unbounded, Receiver, Sender};
use io_uring::{cqueue, opcode, squeue, types, IoUring};

use qmdb::{
    def::DEFAULT_ENTRY_SIZE,
    entryfile::EntryFile,
    prefetcher::dioprefetcher::{BufList, BLK_SIZE},
};

struct Worker<F: Fn(usize, i64, &[u8]) + Send + Sync> {
    cb: Arc<F>,
    entry_files: Vec<Arc<EntryFile>>,
    max_queue_size: u32,
    receiver: Receiver<(usize, i64)>,
    buf_list: BufList,
    buf_id_to_req_and_file: Vec<(usize, i64, Option<Arc<(File, bool)>>)>,
}

impl<F: Fn(usize, i64, &[u8]) + Send + Sync> Worker<F> {
    pub fn new(
        entry_files: Vec<Arc<EntryFile>>,
        max_queue_size: u32,
        receiver: Receiver<(usize, i64)>,
        cb: Arc<F>,
    ) -> Self {
        Worker {
            entry_files,
            max_queue_size,
            receiver,
            buf_list: BufList::new(max_queue_size),
            buf_id_to_req_and_file: repeat_with(|| (0, 0, None))
                .take(max_queue_size as usize)
                .collect(),
            cb,
        }
    }

    pub fn start(mut self) {
        let buf_list = &mut self.buf_list;
        let mut ring = IoUring::<squeue::Entry, cqueue::Entry>::builder()
            .setup_single_issuer()
            .build(self.max_queue_size)
            .unwrap();

        let mut reaming_count = 0;
        #[allow(clippy::never_loop)]
        loop {
            let mut sq = ring.submission();
            let mut can_submit = false;
            while buf_list.has_free_buf() && !sq.is_full() {
                let (shard_id, file_pos) = if reaming_count == 0 {
                    match self.receiver.recv() {
                        Ok((shard_id, file_pos)) => (shard_id, file_pos),
                        Err(_) => {
                            eprintln!("uring worker receiver disconnected");
                            return;
                        }
                    }
                } else {
                    match self.receiver.try_recv() {
                        Ok((shard_id, file_pos)) => (shard_id, file_pos),
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
                let (buf, id) = buf_list.allocate_buf();
                let (read_e, file) =
                    Self::get_read_e(&self.entry_files, shard_id, file_pos, id, buf);
                let f = if file.1 {
                    None
                } else {
                    // still a file being appended
                    Some(file)
                };
                self.buf_id_to_req_and_file[id as usize] = (shard_id, file_pos, f);
                unsafe { sq.push(&read_e).unwrap() };
            }

            drop(sq);
            if can_submit {
                ring.submit().unwrap();
            }
            for cqe in ring.completion() {
                reaming_count -= 1;
                if cqe.result() <= 0 {
                    panic!("AA uring read error");
                }

                let buf_id = cqe.user_data() as u32;
                let (shard_id, file_pos, _) = self.buf_id_to_req_and_file[buf_id as usize];
                self.buf_id_to_req_and_file[buf_id as usize] = (0, 0, None);

                let offset = (file_pos % BLK_SIZE) as usize;
                let buf = buf_list.get_buf(buf_id, offset);

                (self.cb)(shard_id, file_pos, buf);
                buf_list.return_buf(buf_id);
            }
        }
    }

    fn get_read_e(
        entry_files: &Vec<Arc<EntryFile>>,
        shard_id: usize,
        file_pos: i64,
        buf_id: u32,
        buf: &mut [u8],
    ) -> (squeue::Entry, Arc<(File, bool)>) {
        let (file, offset) = entry_files[shard_id].get_file_and_pos(file_pos);
        let fd = file.0.as_raw_fd();
        let buf_ptr = buf.as_ptr() as *mut u8;
        // we are sure one entry is no larger than BLK_SIZE
        let end = offset % BLK_SIZE + (DEFAULT_ENTRY_SIZE as i64);
        let blk_count = if end > BLK_SIZE { 2 } else { 1 };
        let aligned_offset = (offset / BLK_SIZE) * BLK_SIZE;
        let read_len = (BLK_SIZE * blk_count) as u32;
        let e = opcode::Read::new(types::Fd(fd), buf_ptr, read_len)
            .offset(aligned_offset as u64)
            .build()
            .user_data(buf_id as u64);
        (e, file)
    }
}

pub struct WorkerManager {
    senders: Vec<Sender<(usize, i64)>>,
    count: usize,
}

impl WorkerManager {
    pub fn new<F>(
        count: usize,
        entry_files: &Vec<Arc<EntryFile>>,
        max_queue_size: u32,
        _cb: F,
    ) -> Self
    where
        F: Fn(usize, i64, &[u8]) + Send + Sync + 'static,
    {
        let cb = Arc::new(_cb);
        let mut senders = Vec::with_capacity(count);
        for _ in 0..count {
            let (sender, receiver) = unbounded();
            let worker = Worker::new(entry_files.clone(), max_queue_size, receiver, cb.clone());
            std::thread::spawn(move || {
                worker.start();
            });
            senders.push(sender);
        }
        WorkerManager { senders, count }
    }
    pub fn add_task(&self, shard_id: usize, file_pos: i64) {
        let worder_id = Self::get_worker_id(shard_id, file_pos, self.count);
        self.senders[worder_id].send((shard_id, file_pos)).unwrap();
    }

    fn get_worker_id(shard_id: usize, file_pos: i64, uring_count: usize) -> usize {
        let mut r = file_pos as usize;
        r = (r >> 10) ^ (r >> 3) ^ shard_id;
        r % uring_count
    }
}

#[cfg(test)]
mod tests {

    use qmdb::entryfile::{Entry, EntryBz, EntryCache, EntryFileWriter};

    use super::*;

    use std::sync::Arc;
    #[test]
    fn test_uring_worker() {
        let temp_dir = tempfile::Builder::new()
            .prefix("test_uring_worker")
            .tempdir()
            .unwrap();
        let entry_file = Arc::new(EntryFile::new(
            8 * 1024,
            128 * 1024,
            temp_dir.path().to_str().unwrap().to_string(),
            false,
            None,
        ));
        let mut entry_file_w = EntryFileWriter::new(entry_file.clone(), 8 * 1024);

        let mut entry = Entry {
            key: &[0; 32],
            value: &[0; 100],
            next_key_hash: &[0; 32],
            version: 0,
            serial_number: 0,
        };
        let mut read_pos = vec![];
        let entry_count = 10000i64;
        let mut entry_bz_buf: [u8; 1000] = [0; 1000];
        for i in 0..entry_count {
            entry.version = i;

            let entry_bz_size = entry.dump(&mut entry_bz_buf[..], &[]);
            let entry_bz = EntryBz {
                bz: &entry_bz_buf[..entry_bz_size],
            };
            let pos = entry_file_w.append(&entry_bz).unwrap();
            if pos % (entry_count / 100) == 0 {
                // read 100 positions
                read_pos.push(pos);
            }
        }

        entry_file_w.flush().unwrap();
        drop(entry_file_w);

        let cache = EntryCache::new();
        let cb = move |shard_id: usize, file_pos: i64, data: &[u8]| {
            let entry_bz = EntryBz { bz: data };
            println!(
                "shard_id={} file_pos={} data={:?}",
                shard_id, file_pos, data
            );
            cache.insert(shard_id, file_pos, &entry_bz);
        };
        let worker_manager = WorkerManager::new(4, &vec![entry_file], 10, cb);
        worker_manager.add_task(0, 2);
        worker_manager.add_task(0, 0);

        std::thread::sleep(std::time::Duration::from_secs(3));
    }
}
