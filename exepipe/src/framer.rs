use crate::context::block_context::{BlockContext, UnlockTask};
use crate::context::utils::unlock_task;
use crate::dispatcher::dispatcher::Dispatcher;
use crate::dispatcher::dtask::DTask;
use crate::exetask::ExeTask;
use crate::utils::para_bloom::{PBElement, ParaBloom, BLOOM_BITS};
use crossbeam::channel::{unbounded, Receiver, Sender};
use log::debug;
use num_traits::PrimInt;
use qmdb::ADS;
use std::collections::VecDeque;
use std::sync::Arc;

const SET_MAX_SIZE: usize = BLOOM_BITS as usize / 8;
const RETRY_INTERVAL: usize = 8;
pub const TASK_CHAN_COUNT: usize = 4;

pub struct Framer<E: PBElement + PrimInt, T: ADS> {
    blk_ctx: Arc<BlockContext<T>>,
    dispatcher: Arc<Dispatcher<DTask<T>>>,
    frame_count: usize,
    max_tasks_len_in_frame: usize,
    pb: ParaBloom<E>,
    frames: Vec<VecDeque<Box<ExeTask>>>,
    out_idx: usize, // Number of transactions/tasks packed into frames
    pub task_sender: Arc<Sender<(usize, Option<Box<ExeTask>>)>>,
    task_receiver: Receiver<(usize, Option<Box<ExeTask>>)>,
    retry_queue: VecDeque<Box<ExeTask>>,
    unlock_senders: Option<Arc<Vec<Sender<UnlockTask>>>>,
}

impl<E: PBElement + PrimInt, T: ADS> Framer<E, T> {
    pub fn new(
        blk_ctx: Arc<BlockContext<T>>,
        d: Arc<Dispatcher<DTask<T>>>,
        max_len: usize,
        unlock_senders: Option<Arc<Vec<Sender<UnlockTask>>>>,
    ) -> Self {
        Self::with_frame_count(blk_ctx, d, max_len, E::BITS as usize, unlock_senders)
    }

    pub fn with_frame_count(
        blk_ctx: Arc<BlockContext<T>>,
        d: Arc<Dispatcher<DTask<T>>>,
        max_len: usize,
        frame_count: usize,
        unlock_senders: Option<Arc<Vec<Sender<UnlockTask>>>>,
    ) -> Self {
        assert!(frame_count <= E::BITS as usize);
        let (task_sender, task_receiver) = unbounded();

        Self {
            blk_ctx,
            dispatcher: d,
            frame_count,
            max_tasks_len_in_frame: max_len,
            pb: ParaBloom::<E>::new(),
            frames: (0..frame_count)
                .map(|_| VecDeque::with_capacity(max_len))
                .collect(),
            out_idx: 0,
            task_sender: Arc::new(task_sender),
            task_receiver,
            retry_queue: VecDeque::new(),
            unlock_senders,
        }
    }

    fn can_flush(&self, frame_id: usize) -> bool {
        self.frames[frame_id].len() >= self.max_tasks_len_in_frame
            || self.pb.get_rdo_set_size(frame_id) > SET_MAX_SIZE
            || self.pb.get_rnw_set_size(frame_id) > SET_MAX_SIZE
    }

    fn add_task(&mut self, task: Box<ExeTask>) -> Option<Box<ExeTask>> {
        if self.frame_count == 1 {
            self.add_task_if_single_frame(task);
            return None;
        }

        let mask = self.pb.get_dep_mask(&task.access_set);
        let mut frame_id = mask.trailing_ones() as usize;

        // if we cannot find a frame to insert task because
        // it collides with all the frames, so task fails
        // sequencer needs to put it back to mempool
        if frame_id == self.frame_count {
            frame_id = 0;
            self.pb.clear(frame_id);
            self.flush_frame(frame_id);
            // This is an indicator of contention.
            metrics::counter!("framer.frames_flushed.no_space").increment(1);
        }

        // now the task can be inserted into a frame
        self.pb.add(frame_id, &task.access_set);

        let frame = self.frames.get_mut(frame_id).unwrap();
        frame.push_back(task);

        // ensure still has frame when flush_all
        if frame_id != 0 || !self.frames[1].is_empty() {
            // flush the frame if it's large enough
            if self.can_flush(frame_id) {
                self.pb.clear(frame_id);
                self.flush_frame(frame_id);
                metrics::counter!("framer.frames_flushed.large_enough").increment(1);
            }
        }

        None
    }

    fn add_task_if_single_frame(&mut self, task: Box<ExeTask>) {
        let mask = self.pb.get_dep_mask(&task.access_set);
        let frame_id = mask.trailing_ones() as usize;
        assert!(frame_id <= 1);
        if frame_id == 1 {
            self.pb.clear(0);
            self.flush_frame(0);
        }
        self.pb.add(frame_id, &task.access_set);
        let frame = self.frames.get_mut(frame_id).unwrap();
        frame.push_back(task);
    }

    fn flush_frame(&mut self, frame_id: usize) {
        // task_out_start is the last txn of the preceding frame
        let task_out_start = self.out_idx;
        let target_frame = self.frames.get_mut(frame_id).unwrap();
        let mut frame_size_tasks = 0u64;
        let mut frame_size_txns = 0u64;
        while !target_frame.is_empty() {
            // move task from the target frame to tasks_manager
            let task = target_frame.pop_front().unwrap();
            task.set_task_out_start(task_out_start);
            frame_size_txns += task.tx_list.len() as u64;
            self.blk_ctx.tasks_manager.set_task(self.out_idx, *task);
            let dispatched_task = DTask::new(self.blk_ctx.clone(), self.out_idx);
            //if self.out_idx >= 262120 {
            //    println!("Now dispatcher.add {} remained={}", self.out_idx, self.retry_queue.len());
            //}
            self.dispatcher.add(dispatched_task);
            self.out_idx += 1;
            frame_size_tasks += 1;
        }
        // For TLP, record frame_size -- average and sample and sample for distribution
        metrics::counter!("framer.frames_generated").increment(1);
        metrics::counter!("framer.tasks_processed").increment(frame_size_tasks);
        metrics::counter!("framer.txns_processed").increment(frame_size_txns);
        metrics::histogram!("framer.frame_size").record(frame_size_tasks as f64);
    }

    fn flush_all(&mut self) {
        let start_out_idx = self.out_idx;
        for id in 0..self.frame_count {
            // Only flush frames that are not empty
            if !self.frames[id].is_empty() {
                self.flush_frame(id);
            }
        }
        // ensure has frame after set_last_task_id
        assert!(self.out_idx == 0 || self.out_idx > start_out_idx);
    }

    pub fn run(mut self) {
        let mut added_success = 0;
        let mut flushed_count = 0;
        let mut remaining_count = 0i64;

        while flushed_count != TASK_CHAN_COUNT || remaining_count != 0 {
            if self.frame_count > 1 && added_success % RETRY_INTERVAL == 0 {
                if let Some(task) = self.retry_queue.pop_front() {
                    // just retry once
                    if let Some(to_retry) = self.add_task(task) {
                        self._unlock_task(&to_retry);
                    } else {
                        added_success += 1;
                    }
                }
            }

            let (count, task) = self.task_receiver.recv().unwrap();

            if count != usize::MAX {
                assert!(task.is_none());
                remaining_count += count as i64;
                flushed_count += 1;
                continue;
            }

            remaining_count -= 1;
            if let Some(task) = task {
                if let Some(to_retry) = self.add_task(task) {
                    self.retry_queue.push_back(to_retry);
                } else {
                    added_success += 1;
                }
            }
        }

        while let Some(task) = self.retry_queue.pop_front() {
            if let Some(to_retry) = self.add_task(task) {
                self._unlock_task(&to_retry);
            } else {
                added_success += 1;
            }
        }

        // include endblock task
        debug!("set_valid_count with count={}", added_success + 1);
        self.blk_ctx.set_valid_count(added_success + 1);

        self.flush_all();
        debug!("exit framer.run");
    }

    fn _unlock_task(&self, task: &ExeTask) {
        if let Some(unlock_senders) = self.unlock_senders.as_ref() {
            unlock_task(unlock_senders, task);
        }
    }
}
