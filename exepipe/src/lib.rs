#![allow(clippy::too_many_arguments)]
extern crate core;
// pub mod mempool;
// pub mod bundler;
pub mod context;
pub mod def;
pub mod dispatcher;
pub mod exetask;
pub mod framer;
pub mod simulator;
pub mod test_helper;
pub mod utils;

use crate::exetask::{ExeTask, Tx};
use codedb::CodeDB;
use context::block_context::BlockContext;
use dispatcher::def::BATCH_SIZE;
use exepipe_common::statecache::StateCache;
use qmdb::tasks::TasksManager;
use qmdb::{AdsWrap, SharedAdsWrap};
use revm::context::BlockEnv;
use revm::primitives::{Address, B256, U256};
use revm::state::Bytecode;
use simulator::simulator::Simulator;
use std::collections::HashMap;
use std::sync::Arc;
use threadpool::ThreadPool;

type Dispatcher = dispatcher::dispatcher::Dispatcher<dispatcher::dtask::DTask<SharedAdsWrap>>;
type Framer = framer::Framer<u8, SharedAdsWrap>;
type Framer64 = framer::Framer<u64, SharedAdsWrap>; //TODO

#[derive(Default)]
pub struct Block {
    height: i64,
    _hash: B256,
    _parent_hash: B256,
    _gas_limit: U256,
    _size: u64,
    _state_root: B256,
    _receipts_root: B256,
    pub env: BlockEnv,
}

impl Block {
    pub fn new(
        height: i64,
        _hash: B256,
        _parent_hash: B256,
        _gas_limit: U256,
        _size: u64,
        _state_root: B256,
        _receipts_root: B256,
        env: BlockEnv,
    ) -> Self {
        Self {
            height,
            _hash,
            _parent_hash,
            _gas_limit,
            _size,
            _state_root,
            _receipts_root,
            env,
        }
    }
}

pub struct ExePipe {
    dispatcher: Arc<Dispatcher>,
    ads: AdsWrap<ExeTask>,
    code_db: Arc<CodeDB>,
    block_hashes: HashMap<u64, B256>,
    sim_tpool: Arc<ThreadPool>,
}

impl ExePipe {
    pub fn new(ads: AdsWrap<ExeTask>, dir: &str) -> Self {
        let wrbuf_size = 8 * 1024 * 1024; //8MB
        let file_segment_size = 256 * 1024 * 1024; // 256MB

        let extra_data = ads.get_metadb().read().get_extra_data();
        let (codedb_size, _) = Self::split_extra_data(&extra_data);
        let code_db = Arc::new(CodeDB::new(dir, codedb_size, wrbuf_size, file_segment_size));

        let dispatcher = Dispatcher::new(12, 64); //8, 64 is bad
        Self {
            dispatcher,
            ads,
            code_db,
            block_hashes: HashMap::with_capacity(256),
            sim_tpool: Arc::new(ThreadPool::new(16)),
        }
    }

    pub fn run_block_with_sim(
        &mut self,
        task_in: Vec<Vec<Box<Tx>>>,
        blk: Block,
        cache: Arc<StateCache>,
    ) -> Arc<StateCache> {
        let size = task_in.len() * 110 / 100; //with some margin
        let task_manager = Arc::new(TasksManager::with_size(size));

        let height = blk.height;
        self.ads.start_block(height, task_manager.clone());

        let block_hashes = self.block_hashes.clone();
        // let (unlock_senders, unlock_receivers): (Vec<_>, Vec<_>) =
        //     (0..SIM_SHARD_COUNT).map(|_| channel::unbounded()).unzip();
        // let unlock_senders = Arc::new(unlock_senders);

        let mut blk_ctx = BlockContext::new(
            self.ads.get_shared(),
            self.code_db.clone(),
            block_hashes,
            None,
        );
        blk_ctx.with_simulator = true;

        blk_ctx.start_new_block(task_manager.clone(), blk, cache.clone(), BATCH_SIZE); //batch_size

        let blk_ctx = Arc::new(blk_ctx);

        let framer = Framer64::new(blk_ctx.clone(), self.dispatcher.clone(), 256, None); //max_len
                                                                                         //let framer = Framer::with_frame_count(blk_ctx.clone(), self.dispatcher.clone(), 256, 1); //note 512 is too large
        let task_sender = framer.task_sender.clone();
        let handler = std::thread::spawn(move || framer.run());

        let simulator = Simulator::new(
            height,
            &self.ads.get_shared(),
            &task_sender,
            // unlock_senders,
            // Arc::new(unlock_receivers),
            &self.sim_tpool,
            None,
            &blk_ctx,
        );

        for tx_list in task_in {
            for tx in tx_list {
                simulator.add_tx(tx);
            }
        }
        simulator.end_block();
        handler.join().unwrap();
        cache
    }

    pub fn run_block(
        &mut self,
        task_in: Vec<Vec<Box<Tx>>>,
        blk: Block,
        cache: Arc<StateCache>,
    ) -> Arc<StateCache> {
        // adswrap and blk_ctx will share the same last_task_id
        let task_manager = Arc::new(TasksManager::with_size(task_in.len() + 1));

        self.ads.start_block(blk.height, task_manager.clone());

        let block_hashes = self.block_hashes.clone();
        //block_hashes.insert(U256::from(blk.height), blk.hash);
        //if block_hashes.len() > 256 + 1 {
        //    block_hashes.remove(&U256::from(blk.height - 257));
        //}
        // create BlockContext for every new block
        let mut blk_ctx = BlockContext::new(
            self.ads.get_shared(),
            self.code_db.clone(),
            block_hashes,
            None,
        );
        blk_ctx.start_new_block(task_manager.clone(), blk, cache.clone(), BATCH_SIZE);

        let blk_ctx = Arc::new(blk_ctx);

        let framer =
            Framer::with_frame_count(blk_ctx.clone(), self.dispatcher.clone(), 256, 1, None); //note 512 is too large
        let task_sender = framer.task_sender.clone();
        let handler = std::thread::spawn(move || framer.run());

        //self.dispatcher.wait_done_height(blk_ctx.blk.height - 1);

        // None is also counted in task_count
        let count = task_in.len();
        for task in task_in {
            let task = Box::new(ExeTask::new(task));
            task_sender.send((usize::MAX, Some(task))).unwrap();
        }
        task_sender.send((count, None)).unwrap();
        handler.join().unwrap();

        //self.dispatcher.wait_done_height(blk_ctx.blk.height);
        cache
    }

    pub fn insert_code(&mut self, addr: &Address, code: Bytecode) {
        self.code_db.append(addr, code, 0).unwrap();
    }

    pub fn join_to_extra_data(codedb_size: i64, rpcdb_size: i64) -> String {
        let extra_data = format!("{}:{}", codedb_size, rpcdb_size);
        extra_data
    }

    fn split_extra_data(extra_data: &str) -> (i64, i64) {
        let mut codedb_size = -1;
        let mut rpcdb_size = -1;
        let parts: Vec<&str> = extra_data.split(':').collect();
        if parts.len() == 2 {
            codedb_size = parts[0].parse().unwrap();
            rpcdb_size = parts[1].parse().unwrap();
        }
        (codedb_size, rpcdb_size)
    }

    // pub fn run_rpc_tx(&self, tx: TxEnv) -> EVMResult<anyhow::Error> {
    //     let rpc_db = self.rpc_db.read();
    //     let recent_block_hashes = rpc_db
    //         .get_latest_block_hash()
    //         .unwrap()
    //         .into_iter()
    //         .collect::<HashMap<U256, B256>>();

    //     let block_ctx = BlockContext::new(
    //         self.ads.get_shared(),
    //         self.code_db.clone(),
    //         None,
    //         None,
    //         self.short_hasher.clone(),
    //         recent_block_hashes,
    //     );

    //     let rpc_ctx = RpcContext::new(Arc::new(block_ctx), None, None);
    //     let handler = EvmHandler::new(HandlerCfg::new(SpecId::LATEST));

    //     let block_env = BlockEnv::default(); // TODO
    //     let env = Box::new(Env {
    //         cfg: CfgEnv::default(),
    //         block: block_env,
    //         tx: tx.clone(),
    //     });

    //     let mut evm = Evm::builder()
    //         .with_db(rpc_ctx)
    //         .with_env(env)
    //         .with_handler(handler)
    //         .build();

    //     evm.transact()
    // }

    // only for test
    pub fn flush(&mut self) {
        self.ads.flush();
    }
}

#[cfg(test)]
mod test_exe_pipe {

    #[test]
    #[ignore = "WIP"]
    fn test_run_block() {
        // let temp_dir = tempfile::Builder::new()
        //     .prefix("test_exe_pipe")
        //     .tempdir()
        //     .unwrap();
        // let dir_path = temp_dir.path().to_str().unwrap().to_string();

        // let config: Config = Config::from_dir(&dir_path);

        // AdsCore::init_dir(&config);
        // RpcDB::init_dir(&dir_path);

        // let ads_exe_task = AdsWrap::<ExeTask>::new(&config);
        // let pipe = ExePipe::new(ads_exe_task, &dir_path);

        // let mut tx_env = TxEnv::default();
        // let mut access_list = vec![];
        // build_access_list(&mut access_list);
        // tx_env.access_list = AccessList::from(access_list);
        // let tx = Tx {
        //     tx_hash: Default::default(),
        //     crw_sets: None,
        //     env: tx_env,
        // };
        // let task = ExeTask::new(vec![Box::new(tx)]);
        // let task_in = [task];

        // let mut block_env = BlockEnv::default();
        // block_env.number = 1;
        // let blk = Block {
        //     height: 1,
        //     hash: Default::default(),
        //     parent_hash: Default::default(),
        //     gas_limit: U256::ZERO,
        //     size: 0,
        //     state_root: Default::default(),
        //     receipts_root: Default::default(),
        //     env: block_env,
        // };
        // pipe.run_block(task_in, blk, Arc::new(StateCache::new()));

        // todo: test rollback
    }
}
