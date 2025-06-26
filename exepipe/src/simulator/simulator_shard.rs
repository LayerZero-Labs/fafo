use crate::context::block_context::BlockContext;
use crate::context::utils::{address_suffix_u64, warmup_task};
use crate::dispatcher::def::TX_IN_TASK;
use crate::exetask::{ExeTask, Tx};
use crossbeam::channel::Sender;
use exepipe_common::access_set::{addr_to_short_hash, CrwSets};
use qmdb::{SharedAdsWrap, ADS};
use revm::context::transaction::AuthorizationTr;
use revm::context::{Transaction, TransactionType};
use revm::primitives::{Address, HashSet, TxKind, U256};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::Arc;
use threadpool::ThreadPool;

use super::crw_sets::get_rw_sets;

pub struct SimulatorShard<T: ADS> {
    pub my_id: u32,
    pub height: i64,
    pub ads: SharedAdsWrap,
    pub task_sender: Arc<Sender<(usize, Option<Box<ExeTask>>)>>,

    tpool: Arc<ThreadPool>,
    tx_list: Vec<Box<Tx>>,
    task_count: usize,
    block_context: Arc<BlockContext<T>>,
}

impl<T: ADS> SimulatorShard<T> {
    pub fn new(
        my_id: u32,
        height: i64,
        ads: SharedAdsWrap,
        task_sender: Arc<Sender<(usize, Option<Box<ExeTask>>)>>,

        tpool: Arc<ThreadPool>,
        block_context: Arc<BlockContext<T>>,
    ) -> Self {
        Self {
            my_id,
            height,
            ads,
            task_sender,

            tpool,
            tx_list: Vec::with_capacity(TX_IN_TASK),
            task_count: 0,
            block_context,
        }
    }

    fn run_task(&mut self, mut tx_list_in_task: Vec<Box<Tx>>) {
        self.task_count += 1;
        let task_sender = self.task_sender.clone();
        // let locker_sender = self.unlock_senders[self.my_id as usize].clone();
        let height = self.height;
        let ads = self.ads.clone();
        let block_context = self.block_context.clone();
        self.tpool.execute(move || {
            // first warmup all the txs
            // Increase the probability of the caller being in the entry cache when need to extract the crw_sets
            for tx in tx_list_in_task.iter_mut() {
                // TODO: need review
                if tx.env.data.is_empty() {
                    tx.crw_sets = Some(CrwSets::default());
                }

                if tx.crw_sets.is_none() {
                    ads.warmup(height, &addr_to_short_hash(&tx.env.caller));
                    if let TxKind::Call(to_addr) = &tx.env.kind {
                        ads.warmup(height, &addr_to_short_hash(to_addr));
                    }
                    if tx.env.tx_type == TransactionType::Eip7702 {
                        for authorization in tx.env.authorization_list() {
                            ads.warmup(height, &addr_to_short_hash(&authorization.address));
                            let Some(authority) = authorization.authority() else {
                                continue;
                            };
                            ads.warmup(height, &addr_to_short_hash(&authority));
                        }
                    }
                }
            }

            let mut new_task_list = Vec::with_capacity(tx_list_in_task.len());
            let mut next_nonce_map: HashMap<Address, Option<u64>> =
                HashMap::with_capacity(tx_list_in_task.len());
            let mut caller_acc_info_map = HashMap::with_capacity(tx_list_in_task.len());

            let mut unlock_senders = tx_list_in_task
                .iter()
                .map(|tx| address_suffix_u64(&tx.env.caller))
                .collect::<HashSet<u64>>();
            let mut unlock_authorities: SmallVec<[u64; TX_IN_TASK]> =
                SmallVec::with_capacity(TX_IN_TASK);

            for mut tx in tx_list_in_task.into_iter() {
                let caller = &tx.env.caller.clone();
                // not need extract crw_sets
                if tx.crw_sets.is_some() {
                    let next_nonce_opt = next_nonce_map
                        .entry(*caller)
                        .or_insert_with(|| Some(tx.env.nonce));
                    // not need add tx when caller is not exist or balance is zero
                    if let Some(next_nonce) = next_nonce_opt {
                        unlock_senders.remove(&address_suffix_u64(caller));
                        new_task_list.push(tx);
                        *next_nonce += 1;
                 
                    }
                    continue;
                }

                // need extract crw_sets
                let acc_info = caller_acc_info_map
                    .entry(*caller)
                    .or_insert_with(|| block_context.basic(caller));
                let next_nonce_opt = next_nonce_map.entry(*caller).or_insert_with(|| {
                    acc_info.as_ref().and_then(|acc| {
                        let tx_env = &tx.env;
                        let coinbase_gas_price =
                            tx_env.effective_gas_price(block_context.blk.env.basefee as u128);
                        let required = tx_env.value
                            + U256::from(coinbase_gas_price)
                                .checked_mul(U256::from(tx_env.gas_limit))
                                .unwrap();
                        if acc.balance < required {
                            // drop tx
                            unlock_authorities.extend(Self::extract_authorities(&tx));
                            None
                        } else {
                            Some(acc.nonce)
                        }
                    })
                });
                // not need add tx when caller is not exist or balance is zero
                if let Some(next_nonce) = next_nonce_opt {
                    tx.crw_sets = Some(get_rw_sets(
                        &tx.env,
                        block_context.clone(),
                        acc_info.as_ref().unwrap(),
                    ));
                    *next_nonce += 1;
                    unlock_senders.remove(&address_suffix_u64(caller));
                    new_task_list.push(tx);
                } else {
                    unlock_authorities.extend(Self::extract_authorities(&tx));
                    panic!("nonce error");
                }
            }

            let task = if new_task_list.is_empty() {
                None
            } else {
                let task = Box::new(ExeTask::new(new_task_list));
                warmup_task(height, &task, &ads, &block_context.code_db);
                Some(task)
            };
            //println!("send with tx_count={}", task.tx_list.len());
            task_sender.send((usize::MAX, task)).unwrap();
            // if !unlock_senders.is_empty() || !unlock_authorities.is_empty() {
            //     locker_sender
            //         .send((
            //             unlock_senders.into_iter().collect::<SmallVec<[u64; 4]>>(),
            //             unlock_authorities,
            //         ))
            //         .unwrap();
            // }
        });
    }

    // fn handle_unlock(&mut self, blocking: bool) {
    //     let receiver = self.unlock_receivers[self.my_id as usize].clone();
    //     while let Some((senders, authorities)) = if blocking {
    //         receiver.recv().ok()
    //     } else {
    //         receiver.try_recv().ok()
    //     } {
    //         let txs_list = self.locker.unlock_task(senders, authorities);
    //         for txs in txs_list {
    //             self.add_transactions(txs);
    //         }
    //     }
    // }

    pub fn add_tx(&mut self, tx: Box<Tx>) {
        // self.handle_unlock(false);
        // if let Some(tx) = self.locker.add_tx(tx) {
        self.add_transactions(vec![tx]);
        // }
    }

    fn add_transactions(&mut self, txs: Vec<Box<Tx>>) {
        self.tx_list.extend(txs);
        if self.tx_list.len() >= TX_IN_TASK {
            self.flush_tx_list();
        }
    }

    fn flush_tx_list(&mut self) {
        let mut tx_list = Vec::with_capacity(TX_IN_TASK);
        std::mem::swap(&mut self.tx_list, &mut tx_list);
        self.run_task(tx_list);
    }

    pub fn flush(&mut self) {
        if !self.tx_list.is_empty() {
            self.flush_tx_list();
        }

        // the last 'None' task is also counted in task_count
        //println!("flush{} with count={}", i, self.task_count + 1);
        self.task_sender.send((self.task_count, None)).unwrap();
    }

    fn extract_authorities(tx: &Tx) -> Vec<u64> {
        tx.env
            .authorization_list()
            .filter_map(|authorization| {
                authorization
                    .authority()
                    .map(|addr| address_suffix_u64(&addr))
            })
            .collect()
    }
}
