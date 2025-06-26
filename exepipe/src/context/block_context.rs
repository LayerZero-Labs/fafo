use super::tx_context::TxContext;
use super::utils::{unlock_task, warmup_task};
use crate::dispatcher::dashboard::Dashboard;
use crate::dispatcher::def::TX_IN_TASK;
use crate::exetask::ExeTask;
use crate::utils::atomic_u256::AtomicU256;
use crate::Block;
use anyhow::{anyhow, Result};
use codedb::CodeDB;
use crossbeam::channel;
use exepipe_common::acc_data::decode_account_info_from_bz;
use exepipe_common::access_set::{addr_to_short_hash, CrwSets};
use exepipe_common::executor::error::ExecutionError;
use exepipe_common::executor::execute_tx::execute_tx;
use exepipe_common::executor::get_change_set::{get_change_set, ChangeSetConfig};
use exepipe_common::statecache::StateCache;
use parking_lot::{RwLock, RwLockReadGuard};
use qmdb::entryfile::readbuf::ReadBuf;
use qmdb::tasks::taskid::{join_task_id, split_task_id};
use qmdb::tasks::TasksManager;
use qmdb::utils::changeset::ChangeSet;
use qmdb::utils::hasher::{self};
use qmdb::ADS;
use revm::context::result::{ExecutionResult, Output, ResultAndState, SuccessReason};
use revm::primitives::B256;
use revm::primitives::{Address, Bytes};
use revm::state::{Account, AccountInfo, Bytecode, EvmState, EvmStorage};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::Arc;

pub type UnlockTask = (SmallVec<[u64; TX_IN_TASK]>, SmallVec<[u64; TX_IN_TASK]>);

// Support all the transactions in a block
pub struct BlockContext<T: ADS> {
    // TasksManage has a large enough vec to contain all the tasks
    // The last task's id in this block: not always equal to task_list.len()-1 because
    // scheduler may mark failed tasks
    pub tasks_manager: Arc<TasksManager<ExeTask>>,
    // StateCache for current block
    pub curr_state: Arc<StateCache>,
    // StateCache for previous block
    pub prev_state: Arc<StateCache>,
    pub ads: T,
    pub code_db: Arc<CodeDB>,
    pub blk: Block,
    pub dashboard: Arc<Dashboard>,
    pub with_simulator: bool,
    results: Vec<RwLock<Option<Vec<Result<ResultAndState, ExecutionError>>>>>,
    gas_fee_collect: AtomicU256,
    gas_used: AtomicU256,
    pub block_hashes: HashMap<u64, B256>,
    unlock_senders: Option<Arc<Vec<channel::Sender<UnlockTask>>>>,
}

impl<T: ADS> BlockContext<T> {
    pub fn new(
        ads: T,
        code_db: Arc<CodeDB>,
        block_hashes: HashMap<u64, B256>,
        unlock_senders: Option<Arc<Vec<channel::Sender<UnlockTask>>>>,
    ) -> Self {
        Self {
            tasks_manager: Arc::new(TasksManager::default()),
            curr_state: Arc::new(StateCache::new()),
            prev_state: Arc::new(StateCache::new()),
            ads,
            code_db,
            blk: Block::default(),
            dashboard: Arc::new(Dashboard::default()),
            with_simulator: false,
            results: Vec::new(),
            gas_fee_collect: AtomicU256::zero(),
            gas_used: AtomicU256::zero(),
            block_hashes,
            unlock_senders,
        }
    }

    pub fn get_curr_state(&self) -> &StateCache {
        self.curr_state.as_ref()
    }

    pub fn get_block_hash(&self, number: u64) -> Option<B256> {
        match self.block_hashes.get(&number) {
            Some(v) => Some(*v),
            None => None,
        }
    }

    pub fn read_result(
        &self,
        mut idx: isize,
    ) -> RwLockReadGuard<Option<Vec<Result<ResultAndState, ExecutionError>>>> {
        if idx < 0 {
            idx += self.results.len() as isize;
        }
        self.results[idx as usize].read()
    }

    pub fn start_new_block(
        &mut self,
        tasks_manager: Arc<TasksManager<ExeTask>>,
        blk: Block,
        prev_state: Arc<StateCache>,
        batch_size: i32,
    ) {
        self.tasks_manager = tasks_manager;
        let size = self.tasks_manager.tasks_len();
        self.dashboard = Arc::new(Dashboard::new(size, blk.height, batch_size));
        self.blk = blk;
        //mem::swap(&mut self.prev_state, &mut self.curr_state);
        self.prev_state = prev_state;
        self.curr_state = Arc::new(StateCache::new());
        self.results = (0..size).map(|_| RwLock::new(Option::None)).collect();
        self.gas_fee_collect = AtomicU256::zero();
        self.gas_used = AtomicU256::zero();
    }

    pub fn set_valid_count(&self, valid_count: usize) {
        self.tasks_manager.set_valid_count(valid_count);
        // excluding the end_block task
        self.dashboard.set_valid_count(valid_count);
    }

    pub fn warmup(&self, idx: usize) {
        if idx == 0 {
            // warmup coinbase account when the first task
            self.ads.warmup(
                self.blk.height,
                &addr_to_short_hash(&self.blk.env.beneficiary),
            );
        }
        let mut task_opt = self.tasks_manager.task_for_write(idx);
        let task = task_opt.as_mut().unwrap();
        warmup_task(self.blk.height, task, &self.ads, &self.code_db);
    }

    // Note: cannot read task from tasks_manager when calling this function
    pub fn execute_task(&self, idx: usize) {
        let mut task = {
            let mut task_opt = self.tasks_manager.task_for_write(idx);
            task_opt.take().unwrap()
        };
        let mut change_sets = Vec::with_capacity(task.tx_list.len());
        let mut task_result: Vec<Result<ResultAndState, ExecutionError>> =
            Vec::with_capacity(task.tx_list.len());
        for index in 0..task.tx_list.len() {
            let tx = &task.tx_list[index];
            let tx_ctx = TxContext::new(self);
            let x = tx.crw_sets.as_ref().unwrap();
            let (tx_result, mut result) = execute_tx(tx_ctx, &self.blk.env, &tx.env, x);

            task_result.push(tx_result);
            if let Some((change_set, _, _, created_codes_map, _, _)) = result.take() {
                for (addr, byte_code) in created_codes_map {
                    self.code_db
                        .append(&addr, byte_code, join_task_id(self.blk.height, idx, false))
                        .unwrap();
                }
                self.curr_state.apply_change(&change_set);
                change_sets.push(change_set);
            }
        }
        if let Some(unlock_senders) = self.unlock_senders.as_ref() {
            unlock_task(unlock_senders, &task);
        }
        //self.set_rpc_data_set(idx, rpc_data_set);
        self.set_task_result(idx, task_result);
        // from now on, change_set is a read-only field in task
        task.set_change_sets(Arc::new(change_sets));
        self.tasks_manager.set_task(idx, task);
    }

    fn set_task_result(
        &self,
        idx: usize,
        task_result: Vec<Result<ResultAndState, ExecutionError>>,
    ) {
        let mut result_opt = self.results[idx].write();
        *result_opt = Option::Some(task_result);
    }

    pub fn send_to_ads(&self, task_id: i64) {
        //self.ads.add_task(task_id);
        self.ads.fetch_prev_entries(task_id);
        self.ads.add_task_to_updater(task_id);
    }

    fn send_block_info_to_rpc_db(&self, _transaction_count: u32) {
        //let mut leaves: Vec<[u8; 32]> = vec![];
        //for i in 0..transaction_count {
        //    let task_opt = self.tasks_manager.task_for_read(i as usize);
        //    let task = task_opt.as_ref().unwrap();
        //    for tx in &task.tx_list {
        //        leaves.push(<[u8; 32]>::from(tx.tx_hash));
        //    }
        //}
        //let merkle_tree = MerkleTree::<Sha256>::from_leaves(&leaves);
        //let raw = BlockRawInfo {
        //    gas_limit: self.blk.gas_limit,
        //    gas_used: self.gas_used.to_u256(),
        //    hash: self.blk.hash,
        //    miner: self.blk.env.coinbase,
        //    number: self.blk.env.number,
        //    parent_hash: self.blk.parent_hash,
        //    receipts_root: self.blk.receipts_root,
        //    logs_bloom: vec![], //todo:
        //    size: self.blk.size,
        //    state_root: self.blk.state_root,
        //    timestamp: self.blk.env.timestamp,
        //    transactions_root: B256::from(merkle_tree.root().unwrap_or([0u8; 32])),
        //    transaction_count,
        //    transactions: leaves.iter().map(B256::from).collect(),
        //};
        //let blk_info = build_rpc_block(&raw, &self.hasher);
        //if let Some(ref sender) = self.rpc_channel {
        //    sender
        //        .send(vec![RpcData::Block(Box::from(blk_info))])
        //        .unwrap();
        //}
        //let code_db_size = self.code_db.write().flush().unwrap();
        //// insert code_db_size and rpc_db_size to metadb
        //let mut rpc_db_size = 0;
        //if let Some(ref receiver) = self.rpc_file_size_channel {
        //    rpc_db_size = receiver.lock().unwrap().recv().unwrap(); // block
        //}
        //let extra_data = ExePipe::join_to_extra_data(code_db_size, rpc_db_size);
        self.ads
            .insert_extra_data(self.blk.env.number as i64, "".to_owned());
    }

    pub fn end_block(&self) {
        let valid_count = self.tasks_manager.get_valid_count();
        let end_block_task_id = join_task_id(self.blk.height, valid_count - 1, true);
        let mut task = ExeTask::new(vec![]);

        let gas_fee_collected = self.gas_fee_collect.to_u256();
        if gas_fee_collected.is_zero() {
            task.set_change_sets(Arc::new(vec![ChangeSet::new_uninit()]));
            self.tasks_manager.set_task(valid_count - 1, task);
            self._end_block(end_block_task_id, Some(EvmState::default()));
            return;
        }

        let coinbase = self.blk.env.beneficiary;
        let acc_info_opt = self.basic(&coinbase);
        if acc_info_opt.is_none() {
            // coinbase account must exist
            // if not, gas will be burned
            task.set_change_sets(Arc::new(vec![ChangeSet::new_uninit()]));
            self.tasks_manager.set_task(valid_count - 1, task);
            self.set_task_result(
                valid_count - 1,
                vec![Err(ExecutionError::Other(anyhow!(
                    "Cannot find coinbase {} account then {} gas fee will be burn",
                    &coinbase,
                    gas_fee_collected
                )))],
            );
            self._end_block(end_block_task_id, None);
            return;
        }

        let mut orig_acc_map = HashMap::with_capacity(1);
        orig_acc_map.insert(coinbase, acc_info_opt.clone());
        let mut acc_info = acc_info_opt.unwrap();
        acc_info.balance = acc_info.balance.saturating_add(gas_fee_collected);
        let mut acc = Account {
            info: acc_info,
            storage: EvmStorage::default(),
            status: Default::default(),
        };
        acc.mark_touch();
        let mut state = EvmState::default();
        state.insert(coinbase, acc);
        let (change_set, _, _, _) = get_change_set(ChangeSetConfig {
            caller: &coinbase,
            to_addr_access: None,
            gas_price: 0,
            remained_gas_limit: 0,
            state: &state,
            orig_acc_map: &orig_acc_map,
            orig_slot_dust_map: &HashMap::with_capacity(0),
            crw_sets: &CrwSets::default(),
        })
        .unwrap();
        self.curr_state.apply_change(&change_set);
        task.set_change_sets(Arc::new(vec![change_set]));
        self.tasks_manager.set_task(valid_count - 1, task);
        self._end_block(end_block_task_id, Some(state));
    }

    fn _end_block(&self, end_block_task_id: i64, state_opt: Option<EvmState>) {
        let idx = split_task_id(end_block_task_id).1;
        if let Some(state) = state_opt {
            let state_and_result = ResultAndState {
                result: ExecutionResult::Success {
                    reason: SuccessReason::Return,
                    gas_used: 0,
                    gas_refunded: 0,
                    logs: vec![],
                    output: Output::Call(Bytes::new()),
                },
                state,
            };
            self.set_task_result(idx as usize, vec![Ok(state_and_result)]);
        }
        self.send_block_info_to_rpc_db((idx + 1) as u32);
        self.send_to_ads(end_block_task_id);
    }

    pub fn basic(&self, address: &Address) -> Option<AccountInfo> {
        let key_hash = hasher::hash(&address[..]);
        let mut buf = ReadBuf::new();
        self.curr_state.lookup_value(&key_hash, &mut buf);
        if buf.is_empty() {
            self.prev_state.lookup_value(&key_hash, &mut buf);
        }

        if buf.is_empty() {
            let _ = self.ads.read_entry_with_uring(
                self.blk.height,
                &key_hash[..],
                &address[..],
                &mut buf,
            );

            if buf.is_empty() {
                return None;
            }

            buf.initialize_from_entry_value();
        }
        let mut acc_info = decode_account_info_from_bz(buf.as_slice());
        if acc_info.is_empty_code_hash() {
            acc_info.code = Some(Bytecode::default());
        } else if acc_info.code.is_none() {
            let code = self.code_db.get_byte_code(address);
            assert_eq!(code.hash_slow(), acc_info.code_hash);
            acc_info.code = Some(code);
        }
        Some(acc_info)
    }

    pub fn storage_value(&self, addr_idx: &[u8; 20 + 32], use_uring: bool, buf: &mut ReadBuf) {
        let key_hash = hasher::hash(addr_idx);
        self.curr_state.lookup_value(&key_hash, buf);
        if buf.is_empty() {
            self.prev_state.lookup_value(&key_hash, buf);
        }
        if !buf.is_empty() {
            return;
        }

        if use_uring {
            let _ = self
                .ads
                .read_entry_with_uring(self.blk.height, &key_hash, addr_idx, buf);
        } else {
            let _ = self
                .ads
                .read_entry(self.blk.height, &key_hash, addr_idx, buf);
        }
        if buf.is_empty() {
            return;
        }
        buf.initialize_from_entry_value();
    }
}

#[cfg(test)]
mod tests_context {

    use exepipe_common::acc_data::encode_account_info;

    use revm::primitives::{B256, U256};

    use super::*;

    fn _encode_acc_info(bal: U256, nonce: u64, code_hash: B256) -> Vec<u8> {
        let mut acc = AccountInfo::default();
        acc.balance = bal;
        acc.nonce = nonce;
        acc.code_hash = code_hash;
        acc.code = None;
        encode_account_info(&acc).as_slice().to_vec()
    }

    /*
        #[test]
        #[serial]
        fn test_warmup_acc() {
            let temp_dir = tempfile::Builder::new()
                .prefix("context")
                .tempdir()
                .unwrap();
            let code_db = Arc::new(RwLock::new(CodeDB::new(
                temp_dir.path().to_str().unwrap(),
                0,
                512,
                2048,
            )));

            let mut ads = MockADS::new();

            let addr = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            let code = Bytecode::new_raw(Bytes::from([0x5f, 0x00]));
            let code_bytes = bincode::serialize(&code).unwrap();
            let code_hash = hasher::hash(&code_bytes);
            code_db.write().append(&code_hash, &code_bytes).unwrap();
            code_db.write().flush().unwrap();
            ads.add_account(
                &addr,
                &AccountInfo {
                    code_hash: code_hash.into(),
                    ..Default::default()
                },
            );

            let bytecode_map: Arc<CodeMap> = Arc::new(DashMap::<FixedBytes<32>, Bytecode>::new());
            let mut buf: Vec<u8> = vec![0u8; 1024];
            let _ = warmup_acc(
                0,
                &ads,
                &code_db,
                &U256::from_le_slice(&addr[..]),
                &bytecode_map,
                &mut buf,
            );
            assert!(bytecode_map.contains_key(&code_hash));
            assert_eq!(code.bytes(), bytecode_map.get(&code_hash).unwrap().bytes());
        }


        #[test]
        #[serial]
        fn test_warmup() {
            let temp_dir = tempfile::Builder::new()
                .prefix("context")
                .tempdir()
                .unwrap();
            let code_db = Arc::new(RwLock::new(CodeDB::new(
                temp_dir.path().to_str().unwrap(),
                0,
                512,
                2048,
            )));

            let mut ads = MockADS::new();
            let mut acc_list = Vec::new();

            let addr1 = READ_ACC;
            let addr2 = WRITE_ACC;
            let addr3 = READ_SLOT;
            let addr4 = WRITE_SLOT;

            let addr_a = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
            let addr_b: Address = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            let code_bytes1 = bincode::serialize(&Bytecode::new()).unwrap();
            let code_hash1 = hasher::hash(&code_bytes1);
            code_db.write().append(&code_hash1, &code_bytes1).unwrap();
            ads.add_account(
                &addr_a,
                &AccountInfo {
                    code_hash: code_hash1.into(),
                    ..Default::default()
                },
            );

            let code_bytes2 =
                bincode::serialize(&Bytecode::new_raw(Bytes::from([0x5f, 0x00]))).unwrap();
            let code_hash2 = hasher::hash(&code_bytes2);
            code_db.write().append(&code_hash2, &code_bytes2).unwrap();
            code_db.write().flush().unwrap();

            ads.add_account(
                &addr_b,
                &AccountInfo {
                    code_hash: code_hash2.into(),
                    ..Default::default()
                },
            );

            let entry_hash3 = ads.add_entry(EntryBuilder::kv([0; 1], [0; 1]).build_and_dump(&[]));
            let entry_hash4 = ads.add_entry(EntryBuilder::kv([0; 2], [0; 2]).build_and_dump(&[]));

            acc_list.push(AccessListItem {
                address: addr1,
                storage_keys: vec![B256::right_padding_from(&extract_short_hash(
                    &hasher::hash(&addr_a[..]),
                ))],
            });
            acc_list.push(AccessListItem {
                address: addr2,
                storage_keys: vec![B256::right_padding_from(&extract_short_hash(
                    &hasher::hash(&addr_b[..]),
                ))],
            });
            acc_list.push(AccessListItem {
                address: addr3,
                storage_keys: vec![B256::right_padding_from(&extract_short_hash(&entry_hash3))],
            });
            acc_list.push(AccessListItem {
                address: addr4,
                storage_keys: vec![B256::right_padding_from(&extract_short_hash(&entry_hash4))],
            });

            let mut tx = TxEnv::default();
            tx.access_list = acc_list;
            ads.add_account(&tx.caller, &AccountInfo::default());

            let bytecode_map: Arc<CodeMap> = Arc::new(DashMap::new());
            let _ = warmup_tx(0, &tx, &ads, &code_db, &bytecode_map);

            assert!(bytecode_map.contains_key(&code_hash1));
            assert!(bytecode_map.contains_key(&code_hash2));
        }


        #[test]
        #[serial]
        fn test_block_ctx_start_new_block() {
            let temp_dir = tempfile::Builder::new()
                .prefix("context")
                .tempdir()
                .unwrap();
            let code_db = Arc::new(RwLock::new(CodeDB::new(
                temp_dir.path().to_str().unwrap(),
                0,
                512,
                2048,
            )));
            let ads = MockADS::new();
            let mut block_ctx = BlockContext::new(
                ads,
                code_db,
                None,
                None,
                Arc::new(ShortHasher::new(0)),
                HashMap::new(),
            );

            let task_list = Vec::with_capacity(100);
            let last_task_id = 123;
            let block = Block::default();
            block_ctx.start_new_block(
                Arc::new(TasksManager::<ExeTask>::new(task_list, last_task_id)),
                block,
                Arc::new(StateCache::new()),
            );

            assert_eq!(123, block_ctx.tasks_manager.get_last_task_id());
        }

        #[test]
        #[serial]
        fn test_block_ctx_warmup() {
            let temp_dir = tempfile::Builder::new()
                .prefix("context")
                .tempdir()
                .unwrap();
            let code_db = Arc::new(RwLock::new(CodeDB::new(
                temp_dir.path().to_str().unwrap(),
                0,
                512,
                2048,
            )));

            let mut ads = MockADS::new();
            let addr1 = address!("0000000000000000000000000000000000000001");
            let code_bytes1 = bincode::serialize(&Bytecode::new()).unwrap();
            let code_hash1 = hasher::hash(&code_bytes1);
            code_db.write().append(&code_hash1, &code_bytes1).unwrap();
            code_db.write().flush().unwrap();

            let mut tx_env = TxEnv::default();
            tx_env.transact_to = TransactTo::Call(addr1);
            ads.add_account(&tx_env.caller, &AccountInfo::default());
            ads.add_account(
                &tx_env.caller,
                &AccountInfo {
                    code_hash: code_hash1.into(),
                    ..Default::default()
                },
            );
            let tx = Tx {
                tx_hash: Default::default(),
                receipt_hash: Default::default(),
                env: tx_env,
            };
            let tx_list = vec![tx];
            let task = RwLock::new(Option::Some(ExeTask::new(tx_list)));
            let task_list = vec![task];

            let last_task_id = 123;
            let block = Block::default();
            let mut block_ctx = BlockContext::new(
                ads,
                code_db,
                None,
                None,
                Arc::new(ShortHasher::new(0)),
                HashMap::new(),
            );
            block_ctx.start_new_block(
                Arc::new(TasksManager::<ExeTask>::new(task_list, last_task_id)),
                block,
                Arc::new(StateCache::new()),
            );
            block_ctx.warmup(0);
            assert!(block_ctx.curr_state.bytecode_map.contains_key(&code_hash1));
        }

        #[test]
        #[serial]
        fn test_tx_ctx_storage() {
            let temp_dir = tempfile::Builder::new()
                .prefix("context")
                .tempdir()
                .unwrap();
            let code_db = Arc::new(RwLock::new(CodeDB::new(
                temp_dir.path().to_str().unwrap(),
                0,
                512,
                2048,
            )));

            let addr_idx1 = "aaaaaaaaaaaaaaaaaaaa11111111111111111111111111111111".as_bytes();
            let addr_idx2 = "bbbbbbbbbbbbbbbbbbbb22222222222222222222222222222222".as_bytes();
            let a1 = Address::from_slice(&addr_idx1[0..20]);
            let a2 = Address::from_slice(&addr_idx2[0..20]);
            let idx1 = U256::from_le_slice(&addr_idx1[20..]);
            let idx2 = U256::from_le_slice(&addr_idx2[20..]);
            let kh1 = hasher::hash(addr_idx1);
            let kh2 = hasher::hash(addr_idx2);
            let v1 = [0x01; 32];
            let v2 = [0x02; 32];

            let ads = MockADS::new();
            let block_ctx = BlockContext::new(
                ads,
                code_db,
                None,
                None,
                Arc::new(ShortHasher::new(0)),
                HashMap::new(),
            );
            block_ctx
                .curr_state
                .insert_data(&FixedBytes::new(kh1), &v1, &[]);
            block_ctx
                .prev_state
                .insert_data(&FixedBytes::new(kh2), &v2, &[]);

            let mut acc_set = AccessSet::default();
            acc_set.rdo_set.insert(extract_short_hash(&kh1[..]));
            acc_set.rnw_set.insert(extract_short_hash(&kh2[..]));
            let mut orig_acc_map: HashMap<Address, AccData> = HashMap::new();
            let mut tx_ctx = TxContext::<MockADS> {
                orig_acc_map: &mut orig_acc_map,
                orig_slot_dust_map: &mut HashMap::new(),
                access_set: &acc_set,
                blk_ctx: &block_ctx,
            };
            assert_eq!(U256::from_be_bytes(v1), tx_ctx.storage(a1, idx1).unwrap());
            assert_eq!(U256::from_be_bytes(v2), tx_ctx.storage(a2, idx2).unwrap());
        }

        #[test]
        #[serial]
        fn test_block_ctx_storage() {
            let temp_dir = tempfile::Builder::new()
                .prefix("context")
                .tempdir()
                .unwrap();
            let code_db = Arc::new(RwLock::new(CodeDB::new(
                temp_dir.path().to_str().unwrap(),
                0,
                512,
                2048,
            )));

            let k1 = [0x11; 32];
            let v1 = [0x12; 32 + 7];
            let k2 = [0x21; 32];
            let v2 = [0x22; 32 + 7];
            let v3 = [0x33; 32 + 7];
            let entry = EntryBuilder::kv(v3, v3).build_and_dump(&[]);

            let mut ads = MockADS::new();
            let k3 = ads.add_entry(entry);
            let block_ctx = BlockContext::new(
                ads,
                code_db,
                None,
                None,
                Arc::new(ShortHasher::new(0)),
                HashMap::new(),
            );
            block_ctx
                .curr_state
                .insert_data(&FixedBytes::new(k1), &v1, &[]);
            block_ctx
                .prev_state
                .insert_data(&FixedBytes::new(k2), &v2, &[]);

            let mut orig_slot_dust_map = HashMap::new();
            assert_eq!(
                U256::from_be_slice(&v1[..32]),
                block_ctx.storage(&k1, &[0; 52], &mut orig_slot_dust_map)
            );
            assert_eq!(
                U256::from_be_slice(&v2[..32]),
                block_ctx.storage(&k2, &[0; 52], &mut orig_slot_dust_map)
            );
            assert_eq!(
                U256::from_be_slice(&v3[..32]),
                block_ctx.storage(&k3, &[0; 52], &mut orig_slot_dust_map)
            );
        }

    #[test]
    #[serial]
    fn test_block_ctx_code_by_hash() {
        let temp_dir = tempfile::Builder::new()
            .prefix("context")
            .tempdir()
            .unwrap();
        let code_db = Arc::new(RwLock::new(CodeDB::new(
            temp_dir.path().to_str().unwrap(),
            0,
            512,
            2048,
        )));

        let mut ads = MockADS::new();

        let a1 = Address::from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let c1 = Bytecode::new();
        let c2 = Bytecode::new_raw(Bytes::from([0x5f, 0x00]));
        let c3 = Bytecode::new_raw(Bytes::from([0x5f, 0x5f, 0x00]));
        let h1 = FixedBytes::from([0x11; 32]);
        let h2 = FixedBytes::from([0x22; 32]);
        let h3 = hasher::hash(bincode::serialize(&c3).unwrap());
        code_db
            .write()
            .append(&h3, &bincode::serialize(&c3).unwrap())
            .unwrap();
        ads.add_account(
            &a1,
            &AccountInfo {
                code_hash: h3.into(),
                ..Default::default()
            },
        );
        code_db.write().flush().unwrap();

        let block_ctx = BlockContext::new(
            ads,
            code_db,
            None,
            None,
            Arc::new(ShortHasher::new(0)),
            HashMap::new(),
        );
        block_ctx.curr_state.insert_code(&h1, &c1);
        block_ctx.prev_state.insert_code(&h2, &c2);
        assert_eq!(Option::Some(c1.clone()), block_ctx.code_by_hash(&h1));
        assert_eq!(Option::Some(c2.clone()), block_ctx.code_by_hash(&h2));
        assert_eq!(Option::Some(c3.clone()), block_ctx.code_by_hash(&h3.into()));

        // test TxContext
        let mut orig_acc_map: HashMap<Address, AccData> = HashMap::new();
        let mut tx_ctx = TxContext::<MockADS> {
            orig_acc_map: &mut orig_acc_map,
            orig_slot_dust_map: &mut HashMap::new(),
            access_set: &AccessSet::default(),
            blk_ctx: &block_ctx,
        };
        assert_eq!(c1, tx_ctx.code_by_hash(h1).unwrap());
        assert_eq!(c2, tx_ctx.code_by_hash(h2).unwrap());
        assert_eq!(c3, tx_ctx.code_by_hash(h3.into()).unwrap());
    }

    #[test]
    #[serial]
    fn test_block_ctx_basic() {
        let temp_dir = tempfile::Builder::new()
            .prefix("context")
            .tempdir()
            .unwrap();
        let code_db = Arc::new(RwLock::new(CodeDB::new(
            temp_dir.path().to_str().unwrap(),
            0,
            512,
            2048,
        )));

        let a1 = Address::from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let a2 = Address::from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let a3 = Address::from_hex("cccccccccccccccccccccccccccccccccccccccc").unwrap();
        let kh1 = hasher::hash(&a1[..]);
        let kh2 = hasher::hash(&a2[..]);
        let data = [
            vec![],
            _encode_acc_info(
                U256::from(0x1234),
                0x11,
                B256::from_slice(b"cccccccccccccccccccccccccccccccc"),
            ),
            _encode_acc_info(
                U256::from(0x5678),
                0x22,
                B256::from_slice(b"ffffffffffffffffffffffffffffffff"),
            ),
            _encode_acc_info(
                U256::from(0xabcd),
                0x33,
                B256::from_slice(b"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii"),
            ),
        ];

        let mut ads = MockADS::new();
        let entry = EntryBuilder::kv(&[0x33; 32][..], &data[3]).build_and_dump(&[]);
        let kh3 = ads.add_entry(entry);
        let block_ctx = BlockContext::new(
            ads,
            code_db,
            None,
            None,
            Arc::new(ShortHasher::new(0)),
            HashMap::new(),
        );
        block_ctx.curr_state.insert_data(&kh1, &data[1], &[]);
        block_ctx.prev_state.insert_data(&kh2, &data[2], &[]);

        let mut orig_acc_map: HashMap<Address, AccData> = HashMap::new();
        let result1 = block_ctx.basic(&kh1, &a1, &mut orig_acc_map);
        let result2 = block_ctx.basic(&kh2, &a2, &mut orig_acc_map);
        let result3 = block_ctx.basic(&kh3, &a3, &mut orig_acc_map);
        assert_eq!(0x11, result1.unwrap().nonce);
        assert_eq!(0x22, result2.unwrap().nonce);
        assert_eq!(0x33, result3.unwrap().nonce);
        assert_eq!(3, orig_acc_map.len());

        // test TxContext
        let mut acc_set = AccessSet::default();
        acc_set.rdo_set.insert(extract_short_hash(&kh1[..]));
        acc_set.rnw_set.insert(extract_short_hash(&kh2[..]));
        let mut orig_acc_map: HashMap<Address, AccData> = HashMap::new();
        let mut tx_ctx = TxContext::<MockADS> {
            orig_acc_map: &mut orig_acc_map,
            orig_slot_dust_map: &mut HashMap::new(),
            access_set: &acc_set,
            blk_ctx: &block_ctx,
        };
        assert_eq!(0x11, tx_ctx.basic(a1).unwrap().unwrap().nonce);
        assert_eq!(0x22, tx_ctx.basic(a2).unwrap().unwrap().nonce);
        assert_eq!(2, orig_acc_map.len());
    }    */
}
