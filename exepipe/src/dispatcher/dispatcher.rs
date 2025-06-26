use std::sync::{atomic::AtomicI64, Arc};

use crossbeam::channel::{bounded, Receiver, Sender};
use log::debug;
use rayon::ThreadPoolBuilder;
use threadpool::ThreadPool;
use tick_counter::TickCounter;

use super::{
    dashboard::{Dashboard, FIRST_FRAME},
    def::{BATCH_CHAN_SIZE, BATCH_SIZE, MAX_STEPS, SC, SCAN_COUNT},
    dispatchable::Dispatchable,
};

const SAFEGUARD_THRESHOLD: i64 = 1 << 24;

pub struct Dispatcher<D: Dispatchable> {
    pub end_block_receiver: Receiver<i64>,
    end_block_sender: Sender<i64>,
    warmup_tpool: ThreadPool,
    exe_tpool: rayon::ThreadPool,
    batch_senders: Vec<Sender<(D, i32, bool)>>,
    batch_receivers: Vec<Receiver<(D, i32, bool)>>,
    finished_height: AtomicI64,
    start_tick: TickCounter,
}

impl<D: Dispatchable + 'static> Dispatcher<D> {
    pub fn new(num_warmup_threads: usize, num_exe_threads: usize) -> Arc<Self> {
        let (sender, receiver) = bounded(2);
        let mut res = Self {
            end_block_sender: sender,
            end_block_receiver: receiver,
            warmup_tpool: ThreadPool::new(num_warmup_threads),
            exe_tpool: ThreadPoolBuilder::new()
                .num_threads(num_exe_threads)
                .build()
                .unwrap(),
            batch_senders: Vec::with_capacity(SCAN_COUNT),
            batch_receivers: Vec::with_capacity(SCAN_COUNT),
            finished_height: AtomicI64::new(-1),
            start_tick: TickCounter::current(),
        };
        for _ in 0..SCAN_COUNT {
            let (sender, receiver) = bounded(BATCH_CHAN_SIZE);
            res.batch_senders.push(sender);
            res.batch_receivers.push(receiver);
        }
        let res = Arc::new(res);
        for i in 0..SCAN_COUNT {
            let me = res.clone();
            std::thread::spawn(move || me.run_execute_thread(i));
        }
        res
    }

    fn wait_done_height(self: &Arc<Self>, h: i64) {
        let mut done_height = self.finished_height.load(SC);
        if done_height < 0 {
            self.finished_height.store(0, SC);
            return;
        }
        while done_height < h {
            done_height = self.end_block_receiver.recv().unwrap();
        }
    }

    pub fn add(self: &Arc<Self>, d: D) {
        let me = self.clone();
        if d.get_idx() == 0 {
            let board = d.get_dashboard();
            let height = board.height();
            self.wait_done_height(height - 1);
            debug!("start{} {}", height, self.start_tick.elapsed());
        }
        self.warmup_tpool.execute(move || {
            let eei = d.warm_up();
            me.handle_warmed(d, eei);
        });
    }

    fn handle_warmed(self: &Arc<Self>, d: D, eei: i32) {
        let my_idx = d.get_idx();
        let board = d.get_dashboard();
        //if my_idx > 80570 {
        //    debug!!("warmed{} my_idx={} eei={}", board.height(), my_idx, eei);
        //}
        board.set_eei(my_idx, eei);

        if let Some((batch_end, end_block)) = board.send_batch_end(my_idx) {
            if my_idx < board.batch_size {
                // before sending the first batch, execute the first frame
                for i in 0..board.ignite_ll.len() {
                    let next = board.ignite_ll[i].next.load(SC);
                    if next != FIRST_FRAME {
                        break;
                    }
                    //debug!!("dispatch in warmup {}", i);
                    let to_exe = d.get_sibling(i as i32);
                    self.exe_tpool.spawn(move || {
                        to_exe.execute();
                    });
                }
            }
            if end_block {
                debug!(
                    "batch_end{} {} {}",
                    board.height(),
                    batch_end,
                    self.start_tick.elapsed()
                );
            }
            //let batch_idx = my_idx / board.batch_size;
            //debug!("batch_send{} my_idx={} batch_idx={} {}", board.height(), my_idx, batch_idx, self.start_tick.elapsed());
            for sender in self.batch_senders.iter() {
                sender.send((d.clone(), batch_end, end_block)).unwrap();
            }
        }
    }

    fn run_execute_thread(self: &Arc<Self>, thread_id: usize) {
        let receiver = &self.batch_receivers[thread_id];
        loop {
            let (handler, end, end_block) = receiver.recv().unwrap();
            self.run_batch(&handler, end, end_block);
        }
    }

    fn safeguard_dispatch(self: &Arc<Self>, board: &Dashboard, handler: &D, end: i32) {
        let all_done = board.all_done_index.load(SC);
        if all_done >= end {
            return;
        }
        let idx = all_done + 1;
        if board.not_dispatched(idx) {
            let to_exe = handler.get_sibling(idx);
            if board.set_dispatched(idx) {
                self.exe_tpool.spawn(move || {
                    to_exe.execute();
                });
            }
        }
    }

    fn run_batch(self: &Arc<Self>, handler: &D, end: i32, end_block: bool) {
        let mut todo = Vec::new();
        let board = handler.get_dashboard();
        let mut all_done = board.all_done_index.load(SC);
        let mut loop_count = 0i64;
        while all_done < end {
            loop_count += 1;
            if loop_count % SAFEGUARD_THRESHOLD == 0 {
                debug!(
                    "safeguard_dispatch {} all_done={} end={}",
                    loop_count, all_done, end
                );
                self.safeguard_dispatch(board, handler, end);
                loop_count = 0;
            }
            //self.debug_print(&board, loop_count);
            let mut old_all_done = all_done;
            // try to increase all_done
            while board.is_executed(all_done + 1) {
                loop_count = 0;
                all_done += 1;
                if all_done >= end || all_done - old_all_done > MAX_STEPS {
                    break;
                }
            }
            if all_done == old_all_done {
                // load latest all_done_index because other threads might increase it
                all_done = board.all_done_index.load(SC);
                continue;
            }
            old_all_done = board.all_done_index.fetch_max(all_done, SC);
            if all_done <= old_all_done {
                //another thread has increased it more
                all_done = old_all_done;
                continue;
            }
            //println!("fetch_max{} old={} all_done={} end={}", board.height(), old_all_done, all_done, end);
            // now the range (old_all_done, all_done] is owned by me
            todo.clear();
            for i in (old_all_done + 1)..=all_done {
                board.get_ignited_list(i, &mut todo);
                let d = handler.get_sibling(i);
                if i == end && end_block {
                    d.end_block();
                    self.end_block_sender.send(board.height()).unwrap();
                    debug!("end_block {} {}", board.height(), self.start_tick.elapsed());
                }
            }
            self.execute(board, handler, &todo);
        }
    }

    fn execute(self: &Arc<Self>, _: &Dashboard, handler: &D, todo: &Vec<i32>) {
        for &i in todo.iter() {
            let to_exe = handler.get_sibling(i);
            self.exe_tpool.spawn(move || {
                to_exe.execute();
            });
        }
    }
}

pub mod testutils {
    use std::time::Instant;

    use log::info;

    use crate::dispatcher::def::EARLY_EXE_WINDOW_SIZE;

    use super::*;

    #[derive(Clone)]
    pub struct Task {
        pub idx: i32,
        pub dashboard: Arc<Dashboard>,
    }

    impl Dispatchable for Task {
        fn warm_up(&self) -> i32 {
            let my_idx = self.idx;
            if my_idx < EARLY_EXE_WINDOW_SIZE as i32 {
                return FIRST_FRAME;
            }
            my_idx - EARLY_EXE_WINDOW_SIZE as i32
        }

        fn get_sibling(&self, idx: i32) -> Self {
            Self {
                idx,
                dashboard: self.dashboard.clone(),
            }
        }

        fn execute(&self) {}

        fn get_dashboard(&self) -> &Dashboard {
            &self.dashboard
        }

        fn get_idx(&self) -> i32 {
            self.idx
        }

        fn end_block(&self) {}
    }

    pub fn test_dispatcher() {
        let start_time = Instant::now();
        let start_tick = TickCounter::current();

        let dispatcher = Dispatcher::new(6, 2);
        let total_blocks = 20;
        let size = 1000_000;
        for h in 1..=total_blocks {
            if h > 2 {
                dispatcher.wait_done_height(h - 2);
            }
            info!("Now height {}", h);

            let board = Arc::new(Dashboard::new(size, h, BATCH_SIZE));
            for i in 0..(size as i32) {
                dispatcher.add(Task {
                    idx: i,
                    dashboard: board.clone(),
                });
            }
        }
        dispatcher.wait_done_height(total_blocks);
        info!(
            "Time={:?} tick={}",
            start_time.elapsed(),
            start_tick.elapsed()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::testutils::test_dispatcher;

    #[test]
    #[ignore]
    fn test_dispatcher_() {
        test_dispatcher();
    }
}
