use crate::context::block_context::BlockContext;
use crate::context::utils::address_suffix_u64;
use crate::exetask::{ExeTask, Tx};
use crate::framer::TASK_CHAN_COUNT;
use crossbeam::channel::{unbounded, Sender};
use qmdb::{SharedAdsWrap, ADS};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use threadpool::ThreadPool;

use super::simulator_shard::SimulatorShard;

pub const SIM_SHARD_COUNT: usize = TASK_CHAN_COUNT;

pub struct Simulator<T: ADS> {
    input_senders: Vec<Sender<Box<Tx>>>,
    handles: Vec<JoinHandle<SimulatorShard<T>>>,
    tx_count: AtomicUsize,
}

impl<T: ADS> Simulator<T> {
    pub fn new(
        height: i64,
        ads: &SharedAdsWrap,
        task_sender: &Arc<Sender<(usize, Option<Box<ExeTask>>)>>,
        // unlock_senders: Arc<Vec<Sender<UnlockTask>>>,
        // unlock_receivers: Arc<Vec<Receiver<UnlockTask>>>,
        tpool: &Arc<ThreadPool>,
        mut shards: Option<VecDeque<SimulatorShard<T>>>,
        block_context: &Arc<BlockContext<T>>,
    ) -> Self {
        let mut input_senders = Vec::with_capacity(SIM_SHARD_COUNT);
        let mut handles = Vec::with_capacity(SIM_SHARD_COUNT);
        for i in 0..SIM_SHARD_COUNT {
            let (sender, receiver) = unbounded();
            let mut shard = if let Some(_shards) = shards.as_mut() {
                panic!("still no locker");
            } else {
                SimulatorShard::new(
                    i as u32,
                    height,
                    ads.clone(),
                    task_sender.clone(),
                    // unlock_senders.clone(),
                    // unlock_receivers.clone(),
                    tpool.clone(),
                    block_context.clone(),
                )
            };
            input_senders.push(sender);
            let handle = thread::spawn(move || {
                loop {
                    if let Ok(tx) = receiver.recv() {
                        shard.add_tx(tx);
                    } else {
                        shard.flush();
                        break;
                    }
                }
                //println!("exit s{}", shard.my_id);
                shard
            });
            handles.push(handle);
        }
        Self {
            input_senders,
            handles,
            tx_count: AtomicUsize::new(0),
        }
    }

    pub fn add_tx(&self, tx: Box<Tx>) {
        self.tx_count.fetch_add(1, Ordering::SeqCst);
        let last64 = address_suffix_u64(&tx.env.caller);
        let idx = last64 as usize % SIM_SHARD_COUNT;
        self.input_senders[idx].send(tx).unwrap();
    }

    pub fn end_block(self) -> VecDeque<SimulatorShard<T>> {
        for sender in self.input_senders {
            drop(sender);
        }
        let mut v = VecDeque::with_capacity(SIM_SHARD_COUNT);
        for handle in self.handles {
            v.push_back(handle.join().unwrap());
        }
        v
    }
}
