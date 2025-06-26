use exepipe_common::access_set::{
    extract_short_hash, AccessSet, READ_ACC, READ_SLOT, WRITE_ACC, WRITE_SLOT,
};
use exepipe_common::mpex_handler::create_mpex_handler;
use parking_lot::RwLock;
use qmdb::utils::hasher;
use qmdb::ADS;
use revm::interpreter::primitives::TxKind;
use revm::precompile::primitives::{AccountInfo, Address, U256};
use revm::primitives::{AccessListItem, BlockEnv, CfgEnv, Env, TxEnv, B256};
use revm::Evm;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Bound::{self, Included};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use threadpool::ThreadPool;

use crate::bundler::{ParaBloom, SET_MAX_SIZE};
use crate::context::BlockContext;
use crate::rpccontext::RpcContext;
use exepipe_common::acc_data::encode_account_info;

const POOL_COUNT: usize = 64;

// This mempool is a sharded design with many small pools.  A Tx is assigned
// to a small pool according to the last byte of its from-address
fn pool_id_of(addr: &Address) -> usize {
    addr[addr.len() - 1] as usize % POOL_COUNT
}

// How many native token will this tx use?
fn cost_of(tx: &TxEnv) -> U256 {
    tx.value + tx.gas_price * U256::from(tx.gas_limit)
}

// IStateProvider returns the latest known balance of the caller (msg.sender)
// If the input tx has access list, it returns the tx as is, if not, it
// add access list to the tx and returns it.
pub trait IStateProvider: Send + Sync + 'static {
    fn get_account_info_and_access_list(&self, tx: Arc<TxEnv>)
        -> (Arc<TxEnv>, Option<AccountInfo>);
}

impl<T: ADS> IStateProvider for Arc<BlockContext<T>> {
    fn get_account_info_and_access_list(
        &self,
        tx: Arc<TxEnv>,
    ) -> (Arc<TxEnv>, Option<AccountInfo>) {
        let acc_info = self.get_account(&tx.caller);

        //transfer eth dose not need access_list
        if tx.access_list.is_empty() && tx.data.len() == 0 {
            return (tx, acc_info);
        }

        let mut new_tx = (*tx).clone();
        let mut orig_acc_map = HashMap::new();
        let mut read_slot_shorthash_list = HashSet::new();

        let env = Box::new(Env {
            cfg: CfgEnv::default(),
            block: BlockEnv::default(),
            tx: new_tx.clone(),
        });

        let state;
        {
            let rpc_context = RpcContext::new(
                self.clone(),
                Some(&mut orig_acc_map),
                Some(&mut read_slot_shorthash_list),
            );
            let handler = create_mpex_handler::<(), RpcContext<T>>((0, 0));
            let mut evm = Evm::builder()
                .with_db(rpc_context)
                .with_env(env)
                .with_handler(handler)
                .build();
            let evm_result = evm.transact();
            if evm_result.is_err() {
                new_tx.access_list = Vec::with_capacity(0);
                return (Arc::new(new_tx), acc_info);
            }
            state = evm_result.unwrap().state;
        }

        let mut tx_access_list_for_read_acc = vec![];
        let mut tx_access_list_for_write_acc = vec![];
        let mut tx_access_list_for_write_slot = vec![];
        for (address, account) in state {
            if !account.is_touched() {
                continue; // no change, so ignore
            }
            if account.is_empty() && account.is_loaded_as_not_existing() {
                continue; // no need to write not_existing and empty account
            }
            let acc_data = encode_account_info(&account.info);
            let orig_data = orig_acc_map.get(&address);

            let is_from_or_to = if address == new_tx.caller {
                true
            } else {
                match new_tx.transact_to {
                    TxKind::Call(addr) => address == addr,
                    _ => false,
                }
            };
            // access_list is not needed for the from and to addresses
            if !is_from_or_to {
                let addr_hash = hasher::hash(address);
                if orig_data.is_none() || orig_data.unwrap().bytes() != acc_data.bytes() {
                    tx_access_list_for_write_acc
                        .push(B256::right_padding_from(&extract_short_hash(&addr_hash)));
                } else {
                    tx_access_list_for_read_acc
                        .push(B256::right_padding_from(&extract_short_hash(&addr_hash)));
                }
            }

            let mut addr_idx = [0u8; 20 + 32];
            addr_idx[..20].copy_from_slice(&address[..]);
            for (idx, slot) in &account.storage {
                addr_idx[20..].copy_from_slice(idx.as_le_slice());
                let hash = hasher::hash(addr_idx);
                let short_hash = extract_short_hash(&hash);
                if slot.original_value != slot.present_value {
                    //  changed
                    read_slot_shorthash_list.remove(&short_hash);
                    tx_access_list_for_write_slot.push(B256::right_padding_from(&short_hash));
                }
            }
        }

        new_tx.access_list = vec![
            AccessListItem {
                address: READ_ACC,
                storage_keys: tx_access_list_for_read_acc,
            },
            AccessListItem {
                address: WRITE_ACC,
                storage_keys: tx_access_list_for_write_acc,
            },
            AccessListItem {
                address: READ_SLOT,
                storage_keys: read_slot_shorthash_list
                    .iter()
                    .map(|x| B256::right_padding_from(x))
                    .collect(),
            },
            AccessListItem {
                address: WRITE_SLOT,
                storage_keys: tx_access_list_for_write_slot,
            },
        ];
        (Arc::new(new_tx), acc_info)
    }
}

// When adding a tx into mempool, you'll get one of the following results:
#[derive(PartialEq, Debug)]
pub enum AddResult {
    PoolDrainFailed,
    Added,
    DuplicatedNonce,
    NonceNotContinuous,
    BalanceNotEnough,
    CannotGetAccessList,
    NonceMissing,
}

#[derive(PartialEq, Debug)]
pub enum Status {
    Normal,
    Draining, // Draining the current block's mempool to the next block's
    Reaping,  // Reaping TXs from mempool for the next block
}

#[derive(Debug, Default, Clone)]
pub struct Task {
    pub tx_list: Vec<Arc<TxEnv>>,
    pub access_set: AccessSet,
}

#[derive(Default)]
pub struct Pool {
    caller_map: HashMap<Address, Task>,
}

impl Pool {
    // Reap tasks from this pool and send them through a channel
    fn reap(&self, sender: &SyncSender<Task>) -> bool {
        for (_, task) in self.caller_map.iter() {
            match sender.send(task.clone()) {
                Ok(_) => (),
                Err(_) => {
                    return false;
                }
            }
        }
        true
    }

    // Given a TX's caller and nonce, then:
    // 1. check whether this pool has a TX with duplicated nonce for this caller,
    // 2. check whether the nonce is continuous for this caller
    // 3. sum the total cost of this caller in the mempool
    fn lookup(&self, addr_nonce: &(Address, u64)) -> (bool, bool, U256) {
        let mut cost = U256::from(0u64);
        let mut duplicated = false;
        let mut continuous = false;
        if let Some(task) = self.caller_map.get(&addr_nonce.0) {
            for tx in task.tx_list.iter() {
                assert_eq!(tx.caller, addr_nonce.0);
                let nonce = tx.nonce.unwrap();
                if addr_nonce.1 == nonce {
                    duplicated = true;
                }
                if addr_nonce.1 == nonce + 1 {
                    continuous = true;
                }

                cost += cost_of(tx);
            }
        }
        (duplicated, continuous, cost)
    }

    fn add(&mut self, tx: Arc<TxEnv>) {
        let caller = tx.caller;
        if let Some(task) = self.caller_map.get_mut(&caller) {
            task.access_set.add_tx(&tx, true);
            task.tx_list.push(tx);
        } else {
            let mut task = Task::default();
            task.access_set.add_tx(&tx, true);
            task.tx_list.push(tx);
            self.caller_map.insert(caller, task);
        }
    }
}

// Sum all cost for a given address in cost_map, iterating all the nonces
fn all_cost_in(addr: &Address, cost_map: &BTreeMap<(Address, u64), U256>) -> U256 {
    let mut all_cost = U256::from(0u64);
    let start = (*addr, 0u64);
    let end: (Address, u64) = (*addr, u64::MAX);
    let range = (Included(&start), Included(&end));
    for (&(_, _), &v) in
        cost_map.range::<(Address, u64), (Bound<&(Address, u64)>, Bound<&(Address, u64)>)>(range)
    {
        all_cost += v;
    }
    all_cost
}

pub struct MemPool<SP>
where
    SP: IStateProvider,
{
    // coming from a finalized block
    finalized_cost_map: BTreeMap<(Address, u64), U256>,
    // coming from a proposed block that is waiting for votes
    proposed_cost_map: BTreeMap<(Address, u64), U256>,
    // pools collect new TXs
    pools: VecDeque<RwLock<Pool>>,
    // when a new finalized_cost_map comes, pools are moved into draining_pools
    // and then start draining
    draining_pools: Arc<Vec<Pool>>,
    // it becomes false when draining_pools has been drained
    has_drained: Arc<AtomicBool>,
    sp: SP,
    status: Arc<RwLock<Status>>,
    tpool: Arc<ThreadPool>,
}

impl<SP: IStateProvider> MemPool<SP> {
    pub fn new(sp: SP, tpool: Arc<ThreadPool>) -> Self {
        MemPool {
            finalized_cost_map: BTreeMap::new(),
            proposed_cost_map: BTreeMap::new(),
            pools: (0..POOL_COUNT)
                .map(|_| RwLock::new(Pool::default()))
                .collect(),
            draining_pools: Arc::new(Vec::with_capacity(0)),
            has_drained: Arc::new(AtomicBool::new(false)),
            sp,
            status: Arc::new(RwLock::new(Status::Normal)),
            tpool,
        }
    }

    pub fn set_proposed(&mut self, m: BTreeMap<(Address, u64), U256>) {
        self.proposed_cost_map = m;
    }

    // When a finalized block comes, both sp and finalized_cost_map are changed
    pub fn set_finalized(
        &mut self,
        me: Arc<RwLock<Self>>,
        sp: SP,
        finalized_cost_map: BTreeMap<(Address, u64), U256>,
    ) {
        let mut status = self.status.write();
        *status = Status::Draining;
        self.sp = sp;
        self.finalized_cost_map = finalized_cost_map;
        // move 'pools' into 'draining_pools'
        let mut draining_pools = Vec::with_capacity(self.pools.len());
        while !self.pools.is_empty() {
            let pool = self.pools.pop_front().unwrap();
            draining_pools.push(pool.into_inner());
        }
        self.pools = (0..POOL_COUNT)
            .map(|_| RwLock::new(Pool::default()))
            .collect();
        let draining_pools = Arc::new(draining_pools);
        self.draining_pools = draining_pools.clone();
        // now start draining
        self.has_drained.store(false, Ordering::SeqCst);
        let tpool = self.tpool.clone();
        let status = self.status.clone();
        let has_drained = self.has_drained.clone();
        std::thread::spawn(move || {
            let mut status = status.write();
            let mut pb = ParaBloom::<u128>::new();
            let mask_bits = pb.get_mask_bits();

            for pool in draining_pools.iter() {
                for (_, task) in pool.caller_map.iter() {
                    // task.access_set has include from, to, and storage
                    let mask = pb.get_dep_mask(&task.access_set);
                    let bundle_id = mask.trailing_ones() as usize;
                    if bundle_id >= mask_bits {
                        continue;
                    }
                    pb.add(bundle_id, &task.access_set);

                    if pb.get_rdo_set_size(bundle_id) > SET_MAX_SIZE
                        || pb.get_rnw_set_size(bundle_id) > SET_MAX_SIZE
                    {
                        pb.clear(bundle_id);
                    }

                    for tx in task.tx_list.iter() {
                        let mempool = me.clone();
                        let tx = tx.clone();
                        tpool.execute(move || {
                            let mempool = mempool.read();
                            mempool._add_tx(tx, false);
                        });
                    }
                }
            }
            has_drained.store(true, Ordering::SeqCst);
            *status = Status::Normal;
        });
    }

    // run reaping for the pools, starting from the 'n'-th
    pub fn run_reaping(&self, n: usize, sender: &SyncSender<Task>) {
        let mut status = self.status.write();
        *status = Status::Reaping;
        for i in n..(n + POOL_COUNT) {
            let pool = self.pools[i % POOL_COUNT].read();
            if !pool.reap(sender) {
                break;
            }
        }
        *status = Status::Normal;
    }

    pub fn add_tx(&self, tx: Arc<TxEnv>) -> AddResult {
        self._add_tx(tx, true)
    }

    // add a new tx into mempool
    // external=false, then this is a drained tx from draining_pools
    // external=true, then this is a tx from external requester, when
    // draining is not finished, we'd also check it against draining_pools
    // pessimistically.
    fn _add_tx(&self, tx: Arc<TxEnv>, external: bool) -> AddResult {
        if tx.nonce.is_none() {
            return AddResult::NonceMissing;
        }

        let nonce = tx.nonce.unwrap();
        let addr_nonce = (tx.caller, nonce);
        let pool_id = pool_id_of(&tx.caller);
        let (tx, acc_info) = self.sp.get_account_info_and_access_list(tx);
        if tx.access_list.len() != 4 {
            return AddResult::CannotGetAccessList;
        }

        if acc_info.is_none() {
            return AddResult::BalanceNotEnough;
        }

        let acc_info = acc_info.unwrap();
        let mut balance = acc_info.balance;
        let mut continuous = if nonce == 0 {
            false
        } else {
            acc_info.nonce == nonce - 1
        };

        if external && !self.has_drained.load(Ordering::SeqCst) {
            let pool = &self.draining_pools[pool_id];
            let (dup, cont, cost) = pool.lookup(&addr_nonce);
            if dup {
                return AddResult::DuplicatedNonce;
            }
            continuous = continuous || cont;
            // we dedect balance pessimistically, although a TX can be in
            // both finalized_cost_map and draining_pools
            if balance < cost {
                return AddResult::BalanceNotEnough;
            }
            balance -= cost;
        }

        if self.finalized_cost_map.contains_key(&addr_nonce) {
            return AddResult::DuplicatedNonce;
        }

        let addr_nonce_m1 = if nonce == 0 {
            None
        } else {
            Some((tx.caller, nonce - 1))
        };
        if !continuous && addr_nonce_m1.is_some() {
            continuous = self
                .finalized_cost_map
                .contains_key(&addr_nonce_m1.unwrap());
        }
        let cost = all_cost_in(&tx.caller, &self.finalized_cost_map);
        if balance < cost {
            return AddResult::BalanceNotEnough;
        }
        balance -= cost;

        if self.proposed_cost_map.contains_key(&addr_nonce) {
            return AddResult::DuplicatedNonce;
        }
        if !continuous && addr_nonce_m1.is_some() {
            continuous = self.proposed_cost_map.contains_key(&addr_nonce_m1.unwrap());
        }
        let cost = all_cost_in(&tx.caller, &self.proposed_cost_map);
        if balance < cost {
            return AddResult::BalanceNotEnough;
        }
        balance -= cost;

        let mut pool = self.pools[pool_id].write();
        let (dup, cont, cost) = pool.lookup(&addr_nonce);
        if dup {
            return AddResult::DuplicatedNonce;
        }
        continuous = continuous || cont;
        if balance < cost {
            return AddResult::BalanceNotEnough;
        }
        if nonce > 0 && !continuous {
            return AddResult::NonceNotContinuous;
        }
        pool.add(tx);
        AddResult::Added
    }
}

/*
#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap, VecDeque},
        sync::{atomic::Ordering, mpsc::sync_channel, Arc},
        thread,
    };

    use codedb::CodeDB;
    use exepipe_common::access_set::{READ_ACC, READ_SLOT, WRITE_ACC, WRITE_SLOT};
    use parking_lot::RwLock;
    use qmdb::utils::hasher;
    use revm::primitives::{
        address, hex::FromHex, AccessListItem, AccountInfo, Address, Bytecode, Bytes, TransactTo,
        TxEnv, TxKind, B256, U256,
    };
    use rpcdb::utils::ShortHasher;
    use tempfile;

    use crate::{
        context::BlockContext,
        mempool::{pool_id_of, Pool, Status, POOL_COUNT},
        test_helper::{MockADS, ERC20_DEPLOY_HEX},
    };

    use super::{IStateProvider, MemPool, Task};

    #[test]
    fn test_get_account_info_and_access_list() {
        let temp_dir = tempfile::Builder::new()
            .prefix("get_account_info_and_access_list")
            .tempdir()
            .unwrap();
        let code_db = Arc::new(RwLock::new(CodeDB::new(
            temp_dir.path().to_str().unwrap(),
            0,
            256 * 100,
            1024 * 100,
        )));

        let mut ads = MockADS::new();

        let erc20_addr = address!("31256cb3d8cb35671f13b5b1680b2cf4fe55fc4f");
        let bc_bytes = Bytes::from_hex(ERC20_DEPLOY_HEX).unwrap();
        let bytecode = Bytecode::new_raw(bc_bytes);
        let code_data = bincode::serialize(&bytecode).unwrap();
        let bc_hash = hasher::hash(&code_data);
        let erc20_info = AccountInfo {
            nonce: 0,
            balance: U256::ZERO,
            code_hash: bc_hash.into(),
            code: Option::Some(bytecode),
        };
        ads.add_account(&erc20_addr, &erc20_info);
        code_db.write().append(&bc_hash, &code_data[..]).unwrap();
        code_db.write().flush().unwrap();

        let from_addr = address!("00000000000000000000000000000000000000a1");
        let from_info = AccountInfo::from_balance(U256::from(1_000_000_000_000_000_000u128));
        ads.add_account(&from_addr, &from_info);

        let mut tx = TxEnv::default();
        tx.caller = from_addr;
        tx.transact_to = TransactTo::Call(erc20_addr);
        tx.gas_price = U256::from(2);

        let block_ctx = BlockContext::new(
            ads,
            code_db,
            None,
            None,
            Arc::new(ShortHasher::new(0)),
            HashMap::new(),
        );
        let arc_block_ctx = Arc::new(block_ctx);

        {
            // transfer eth
            let arc_tx = Arc::new(tx.clone());
            let (_tx, _acc_info) = arc_block_ctx.get_account_info_and_access_list(arc_tx.clone());
            assert_eq!(_acc_info, Some(from_info.clone()));
            assert_eq!(_tx.access_list.len(), 0);
            assert!(Arc::ptr_eq(&_tx, &arc_tx));
        }

        tx.data = Bytes::from_hex("0xa9059cbb00000000000000000000000000000000000000000000000000000000000000a20000000000000000000000000000000000000000000000000000000000000001").unwrap();
        {
            // tx.access_list.len() == 0 && tx.data.len() != 0 => true
            let arc_tx = Arc::new(tx.clone());
            let (_tx, _acc_info) = arc_block_ctx.get_account_info_and_access_list(arc_tx.clone());
            assert_eq!(_acc_info, Some(from_info.clone()));
            assert_eq!(_tx.access_list.len(), 0);
            assert!(!Arc::ptr_eq(&_tx, &arc_tx));
        }

        tx.access_list = vec![AccessListItem::default()];
        {
            // evm_result.is_err() == true
            let (_tx, _acc_info) =
                arc_block_ctx.get_account_info_and_access_list(Arc::new(TxEnv {
                    access_list: vec![AccessListItem::default()],
                    ..tx.clone()
                }));
            assert_eq!(_acc_info, Some(from_info.clone()));
            assert_eq!(_tx.access_list.len(), 0);
        }

        {
            tx.gas_limit = 1_000_000_000u64;
            let (_tx, _acc_info) =
                arc_block_ctx.get_account_info_and_access_list(Arc::new(tx.clone()));
            assert_eq!(_acc_info, Some(from_info.clone()));
            let _access_list =
                [
                    AccessListItem {
                        address: READ_ACC,
                        storage_keys: vec![],
                    },
                    AccessListItem {
                        address: WRITE_ACC,
                        storage_keys: vec![],
                    },
                    AccessListItem {
                        address: READ_SLOT,
                        storage_keys: vec![],
                    },
                    AccessListItem {
                        address: WRITE_SLOT,
                        storage_keys: vec![
                        B256::from_slice(&hex::decode(
                            "bb4cd8542ba076e3e19900000000000000000000000000000000000000000000",
                        ).unwrap()),
                        B256::from_slice(&hex::decode(
                            "f57ea2b74007f3ba32d100000000000000000000000000000000000000000000",
                        ).unwrap()),
                        B256::from_slice(&hex::decode(
                            "da8f2e22f41b2cfc907700000000000000000000000000000000000000000000",
                        ).unwrap()),
                        B256::from_slice(&hex::decode(
                            "ee039453127e68154b3300000000000000000000000000000000000000000000",
                        ).unwrap()),
                        B256::from_slice(&hex::decode(
                            "16c474bb5e6e03490d3100000000000000000000000000000000000000000000",
                        ).unwrap()),
                    ],
                    },
                ];
        }
    }

    #[test]
    fn test_pool_add() {
        let mut pool = Pool::default();

        let tx = Arc::new(TxEnv {
            caller: Address::right_padding_from(&[1]),
            ..Default::default()
        });

        pool.add(tx.clone());

        assert!(pool.caller_map.contains_key(&tx.caller));
        assert_eq!(pool.caller_map.len(), 1);
        let task = pool.caller_map.get(&tx.caller).unwrap();
        assert_eq!(task.tx_list.len(), 1);
        assert_eq!(task.tx_list[0], tx);

        pool.add(tx.clone());
        assert!(pool.caller_map.contains_key(&tx.caller));
        assert_eq!(pool.caller_map.len(), 1);
        let task = pool.caller_map.get(&tx.caller).unwrap();
        assert_eq!(task.tx_list.len(), 2);
        assert_eq!(task.tx_list[0], tx);
        assert_eq!(task.tx_list[1], tx);
    }

    #[test]
    fn test_pool_lookup() {
        let mut pool = Pool::default();

        let tx1 = Arc::new(TxEnv {
            nonce: Some(0),
            value: U256::from(1000u64),
            gas_price: U256::from(10u64),
            gas_limit: 100,
            ..Default::default()
        });

        let tx2 = Arc::new(TxEnv {
            nonce: Some(1),
            value: U256::from(1000u64),
            ..Default::default()
        });

        pool.add(tx1.clone());
        pool.add(tx2.clone());

        let addr_nonce1 = (tx1.caller, tx1.nonce.unwrap());
        let (duplicated, continuous, total_cost) = pool.lookup(&addr_nonce1);
        assert!(duplicated);
        assert!(!continuous);
        assert_eq!(total_cost, U256::from(3000u64));

        let addr_nonce2 = (tx2.caller, tx2.nonce.unwrap());
        let (duplicated, continuous, total_cost) = pool.lookup(&addr_nonce2);
        assert!(duplicated);
        assert!(continuous);
        assert_eq!(total_cost, U256::from(3000u64));

        let addr_nonce3 = (Address::right_padding_from(&[1]), tx2.nonce.unwrap());
        let (duplicated, continuous, total_cost) = pool.lookup(&addr_nonce3);
        assert!(!duplicated);
        assert!(!continuous);
        assert_eq!(total_cost, U256::from(0));

        let (sender, receiver) = sync_channel(10);
        assert!(pool.reap(&sender));

        let received_task = receiver.recv().unwrap();
        assert_eq!(received_task.tx_list.len(), 2);
        assert_eq!(received_task.tx_list[0], tx1);
        assert_eq!(received_task.tx_list[1], tx2);
    }

    #[derive(Clone, PartialEq, Debug)]
    struct MockStateProvider {
        acc_info: Option<AccountInfo>,
        access_list: Vec<AccessListItem>,
    }

    impl IStateProvider for MockStateProvider {
        fn get_account_info_and_access_list(
            &self,
            tx: Arc<TxEnv>,
        ) -> (Arc<TxEnv>, Option<AccountInfo>) {
            let mut new_tx = (*tx).clone();
            new_tx.access_list = self.access_list.clone();
            (Arc::new(new_tx), self.acc_info.clone())
        }
    }

    #[test]
    fn test_mem_pool_new() {
        let tpool = Arc::new(threadpool::ThreadPool::new(1));
        let sp = MockStateProvider {
            acc_info: None,
            access_list: vec![],
        };
        let mem_pool = MemPool::new(sp, tpool.clone());
        assert_eq!(mem_pool.finalized_cost_map.len(), 0);
        assert_eq!(mem_pool.proposed_cost_map.len(), 0);
        assert_eq!(mem_pool.pools.len(), POOL_COUNT);
        assert_eq!(mem_pool.draining_pools.len(), 0);
        assert!(!mem_pool
            .has_drained
            .load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(mem_pool.sp.acc_info, None);
        let _status = mem_pool.status.read();
        assert_eq!(*_status, super::Status::Normal);
    }

    #[test]
    fn test_mem_pool_add_tx() {
        let tpool = Arc::new(threadpool::ThreadPool::new(1));
        {
            let sp = MockStateProvider {
                acc_info: None,
                access_list: vec![],
            };
            let mem_pool = MemPool::new(sp, tpool.clone());
            let result = mem_pool.add_tx(Arc::new(TxEnv::default()));
            assert_eq!(result, super::AddResult::NonceMissing);
        }
        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo::default()),
                access_list: vec![],
            };
            let tx = TxEnv {
                nonce: Some(1),
                ..Default::default()
            };
            let mem_pool = MemPool::new(sp, tpool.clone());
            let result = mem_pool.add_tx(Arc::new(tx.clone()));
            assert_eq!(result, super::AddResult::CannotGetAccessList);

            let sp = MockStateProvider {
                acc_info: None,
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let result = mem_pool.add_tx(Arc::new(tx.clone()));
            assert_eq!(result, super::AddResult::BalanceNotEnough);
        }

        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo::default()),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(1),
                ..Default::default()
            };
            let mut mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let mut pools: Vec<Pool> = (0..POOL_COUNT).map(|_| Pool::default()).collect();
            pools[pool_id_of(&tx.caller)].add(Arc::new(tx.clone()));
            mem_pool.draining_pools = Arc::new(pools);
            let result = mem_pool.add_tx(Arc::new(tx.clone()));
            assert_eq!(result, super::AddResult::DuplicatedNonce);

            let mut mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let mut finalized_cost_map = BTreeMap::new();
            finalized_cost_map.insert((tx.caller, tx.nonce.unwrap()), U256::ZERO);
            mem_pool.finalized_cost_map = finalized_cost_map;
            let result = mem_pool._add_tx(Arc::new(tx.clone()), false);
            assert_eq!(result, super::AddResult::DuplicatedNonce);

            mem_pool.has_drained = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let result = mem_pool.add_tx(Arc::new(tx.clone()));
            assert_eq!(result, super::AddResult::DuplicatedNonce);
        }

        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo::default()),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(0),
                value: U256::from(1000u64),
                ..Default::default()
            };
            let mut mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let mut pools: Vec<Pool> = (0..POOL_COUNT).map(|_| Pool::default()).collect();
            pools[pool_id_of(&tx.caller)].add(Arc::new(TxEnv {
                nonce: Some(1),
                ..tx.clone()
            }));
            mem_pool.draining_pools = Arc::new(pools);
            let result = mem_pool.add_tx(Arc::new(tx.clone()));
            assert_eq!(result, super::AddResult::BalanceNotEnough);
        }
        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo::default()),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(1),
                ..Default::default()
            };
            let mut mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let pools: Vec<Pool> = (0..POOL_COUNT).map(|_| Pool::default()).collect();
            mem_pool.draining_pools = Arc::new(pools);
            let mut finalized_cost_map = BTreeMap::new();
            finalized_cost_map.insert((tx.caller, 0), U256::from(1000u64));
            mem_pool.finalized_cost_map = finalized_cost_map;
            let result = mem_pool.add_tx(Arc::new(tx.clone()));
            assert_eq!(result, super::AddResult::BalanceNotEnough);
        }
        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo::default()),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(0),
                ..Default::default()
            };
            let mut mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let pools: Vec<Pool> = (0..POOL_COUNT).map(|_| Pool::default()).collect();
            mem_pool.draining_pools = Arc::new(pools);
            let mut proposed_cost_map = BTreeMap::new();
            proposed_cost_map.insert((tx.caller, tx.nonce.unwrap()), U256::from(1000u64));
            mem_pool.proposed_cost_map = proposed_cost_map;
            let result = mem_pool.add_tx(Arc::new(tx.clone()));
            assert_eq!(result, super::AddResult::DuplicatedNonce);
        }
        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo::default()),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(0),
                ..Default::default()
            };
            let mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let result = mem_pool._add_tx(Arc::new(tx.clone()), false);
            assert_eq!(result, super::AddResult::Added);
        }
        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo::from_balance(U256::from(1000u64))),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(1),
                value: U256::from(1000u64),
                ..Default::default()
            };
            let mut mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let mut pools: Vec<Pool> = (0..POOL_COUNT).map(|_| Pool::default()).collect();
            pools[pool_id_of(&tx.caller)].add(Arc::new(TxEnv {
                nonce: Some(0),
                ..tx.clone()
            }));
            mem_pool.draining_pools = Arc::new(pools);
            let result = mem_pool._add_tx(Arc::new(tx.clone()), true);
            assert_eq!(result, super::AddResult::Added);
        }
        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo::from_balance(U256::from(1000u64))),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(1),
                value: U256::from(1000u64),
                ..Default::default()
            };
            let mut mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let mut finalized_cost_map = BTreeMap::new();
            finalized_cost_map.insert((tx.caller, 0), U256::from(1000u64));
            mem_pool.finalized_cost_map = finalized_cost_map;
            let result = mem_pool._add_tx(Arc::new(tx.clone()), false);
            assert_eq!(result, super::AddResult::Added);
        }
        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo {
                    nonce: 1,
                    ..AccountInfo::from_balance(U256::from(1000u64))
                }),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(1),
                value: U256::from(1000u64),
                ..Default::default()
            };
            let mut mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let mut proposed_cost_map = BTreeMap::new();
            proposed_cost_map.insert((tx.caller, 0), U256::from(1000u64));
            mem_pool.proposed_cost_map = proposed_cost_map;
            let result = mem_pool._add_tx(Arc::new(tx.clone()), false);
            assert_eq!(result, super::AddResult::Added);

            let pool = &mem_pool.pools[pool_id_of(&tx.caller)].read();
            assert_eq!(pool.caller_map.len(), 1);
            let task = pool.caller_map.get(&tx.caller).unwrap();
            assert_eq!(task.tx_list.len(), 1);
            assert_eq!(
                task.tx_list[0],
                TxEnv {
                    access_list: (0..4).map(|_| AccessListItem::default()).collect(),
                    ..tx.clone()
                }
                .into()
            );
        }
        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo {
                    nonce: 1,
                    ..AccountInfo::from_balance(U256::from(0))
                }),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(1),
                value: U256::from(1000u64),
                ..Default::default()
            };
            let mut mem_pool = MemPool::new(sp, tpool.clone());
            let pools: VecDeque<_> = (0..POOL_COUNT)
                .map(|_| RwLock::new(Pool::default()))
                .collect();
            pools[pool_id_of(&tx.clone().caller)]
                .write()
                .add(Arc::new(tx.clone()));
            mem_pool.pools = pools;
            let result = mem_pool._add_tx(Arc::new(tx.clone()), false);
            assert_eq!(result, super::AddResult::DuplicatedNonce);
        }
        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo {
                    nonce: 1,
                    ..AccountInfo::from_balance(U256::from(0))
                }),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(1),
                value: U256::from(1000u64),
                ..Default::default()
            };
            let mut mem_pool = MemPool::new(sp, tpool.clone());
            let pools: VecDeque<_> = (0..POOL_COUNT)
                .map(|_| RwLock::new(Pool::default()))
                .collect();
            pools[pool_id_of(&tx.clone().caller)]
                .write()
                .add(Arc::new(TxEnv {
                    nonce: Some(0),
                    ..tx.clone()
                }));
            mem_pool.pools = pools;
            let result = mem_pool._add_tx(Arc::new(tx.clone()), false);
            assert_eq!(result, super::AddResult::BalanceNotEnough);
        }
        {
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo {
                    nonce: 1,
                    ..AccountInfo::from_balance(U256::from(1000u64))
                }),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            let tx = TxEnv {
                nonce: Some(1),
                value: U256::from(1000u64),
                ..Default::default()
            };
            let mut mem_pool = MemPool::new(sp, tpool.clone());
            let pools: VecDeque<_> = (0..POOL_COUNT)
                .map(|_| RwLock::new(Pool::default()))
                .collect();
            pools[pool_id_of(&tx.clone().caller)]
                .write()
                .add(Arc::new(TxEnv {
                    nonce: Some(2),
                    ..tx.clone()
                }));
            mem_pool.pools = pools;
            let result = mem_pool._add_tx(Arc::new(tx.clone()), false);
            assert_eq!(result, super::AddResult::NonceNotContinuous);
        }
    }

    #[test]
    fn test_set_proposed() {
        let tpool = Arc::new(threadpool::ThreadPool::new(1));
        let sp = MockStateProvider {
            acc_info: None,
            access_list: vec![],
        };
        let mut mem_pool = MemPool::new(sp, tpool.clone());
        let mut proposed_cost_map = BTreeMap::new();
        proposed_cost_map.insert((Address::right_padding_from(&[1]), 0), U256::from(1000u64));
        mem_pool.set_proposed(proposed_cost_map.clone());
        assert_eq!(mem_pool.proposed_cost_map, proposed_cost_map);
    }

    #[test]
    #[serial_test::serial]
    fn test_run_reaping() {
        let tpool = Arc::new(threadpool::ThreadPool::new(1));
        let sp = MockStateProvider {
            acc_info: None,
            access_list: vec![],
        };
        let mut mem_pool = MemPool::new(sp, tpool.clone());
        let pools: VecDeque<_> = (0..POOL_COUNT)
            .map(|_| RwLock::new(Pool::default()))
            .collect();
        let tx = TxEnv {
            nonce: Some(1),
            value: U256::from(1000u64),
            ..Default::default()
        };
        pools[pool_id_of(&tx.clone().caller)]
            .write()
            .add(Arc::new(tx.clone()));
        mem_pool.pools = pools;
        let (sender, receiver) = sync_channel(10);
        mem_pool.run_reaping(0, &sender);

        let received_task = receiver.recv().unwrap();
        assert_eq!(received_task.tx_list.len(), 1);
        assert_eq!(received_task.tx_list[0], tx.into());
        assert_eq!(*mem_pool.status.read(), super::Status::Normal);
    }

    #[test]
    #[serial_test::serial]
    fn test_set_finalized() {
        let tpool = Arc::new(threadpool::ThreadPool::new(1));
        let sp = MockStateProvider {
            acc_info: None,
            access_list: vec![],
        };
        let mut mem_pool = MemPool::new(sp, tpool.clone());
        let pools: VecDeque<_> = (0..POOL_COUNT)
            .map(|_| RwLock::new(Pool::default()))
            .collect();
        let tx = TxEnv {
            nonce: Some(1),
            value: U256::from(1000u64),
            ..Default::default()
        };
        for _ in 0..1000000u64 {
            pools[pool_id_of(&tx.clone().caller)]
                .write()
                .add(Arc::new(tx.clone()));
        }

        mem_pool.pools = pools;

        let arc_mem_pool = Arc::new(RwLock::new(mem_pool));
        let finalized_cost_map = BTreeMap::new();
        let sp = MockStateProvider {
            acc_info: Some(AccountInfo {
                nonce: 0,
                ..Default::default()
            }),
            access_list: (0..4).map(|_| AccessListItem::default()).collect(),
        };
        arc_mem_pool.write().set_finalized(
            arc_mem_pool.clone(),
            sp.clone(),
            finalized_cost_map.clone(),
        );

        {
            let mempool_read = arc_mem_pool.read();
            assert_eq!(*mempool_read.status.read(), Status::Draining);
            assert!(!mempool_read.has_drained.load(Ordering::SeqCst));
            assert!(mempool_read.draining_pools.len() == POOL_COUNT);
            let pool = &mempool_read.draining_pools[pool_id_of(&tx.caller)];
            assert_eq!(pool.caller_map.len(), 1);
            assert_eq!(mempool_read.pools.len(), 64);
            assert_eq!(mempool_read.finalized_cost_map, finalized_cost_map);
            assert_eq!(mempool_read.sp, sp);
            let pool = &mempool_read.pools[pool_id_of(&tx.caller)].read();
            assert_eq!(pool.caller_map.len(), 0);
        }

        // Wait for draining to complete
        thread::sleep(std::time::Duration::from_secs(1));

        {
            let mempool_read = arc_mem_pool.read();
            assert_eq!(*mempool_read.status.read(), Status::Normal);
            assert!(mempool_read.has_drained.load(Ordering::SeqCst));
            let pool = &mempool_read.pools[pool_id_of(&tx.caller)].read();
            assert_eq!(pool.caller_map.len(), 1);
        }
    }

    #[test]
    #[serial_test::serial]
    fn test_set_finalized_use_para_bloom() {
        let sp = MockStateProvider {
            acc_info: None,
            access_list: vec![],
        };
        {
            let tpool = Arc::new(threadpool::ThreadPool::new(1));
            let mut mem_pool = MemPool::new(sp.clone(), tpool.clone());
            let mut pool = Pool::default();
            for i in 0..128 + 1 + 1 {
                let tx = TxEnv {
                    nonce: Some(0),
                    value: U256::from(1000u64),
                    caller: Address::right_padding_from(&[i as u8]),
                    transact_to: TransactTo::Call(Address::ZERO),
                    ..TxEnv::default()
                };
                let mut task = Task::default();
                task.access_set.add_tx(&tx, true);
                task.tx_list.push(Arc::new(tx));
                pool.caller_map.insert(task.tx_list[0].caller, task);
            }
            let mut pools: VecDeque<_> = (0..POOL_COUNT)
                .map(|_| RwLock::new(Pool::default()))
                .collect();
            pools[0] = RwLock::new(pool);
            mem_pool.pools = pools;

            let arc_mem_pool = Arc::new(RwLock::new(mem_pool));
            let finalized_cost_map = BTreeMap::new();
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo {
                    nonce: 0,
                    ..Default::default()
                }),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            arc_mem_pool.write().set_finalized(
                arc_mem_pool.clone(),
                sp.clone(),
                finalized_cost_map.clone(),
            );

            thread::sleep(std::time::Duration::from_secs(1));

            {
                let mempool_read = arc_mem_pool.read();
                let pool = &mempool_read.pools[0].read();
                assert_eq!(pool.caller_map.len(), 128);
            }
        }
        {
            let tpool = Arc::new(threadpool::ThreadPool::new(128));
            let mut mem_pool = MemPool::new(sp, tpool.clone());
            let mut pool = Pool::default();
            for i in 0..150000 {
                let tx = TxEnv {
                    nonce: Some(0),
                    value: U256::from(1000u64),
                    caller: Address::right_padding_from(
                        &hasher::hash(U256::from(i).as_le_bytes())[0..20],
                    ),
                    transact_to: TxKind::Create,
                    ..TxEnv::default()
                };
                let mut task = Task::default();
                task.access_set.add_tx(&tx, true);
                task.tx_list.push(Arc::new(tx));
                pool.caller_map.insert(task.tx_list[0].caller, task);
            }

            let mut pools: VecDeque<_> = (0..POOL_COUNT)
                .map(|_| RwLock::new(Pool::default()))
                .collect();
            pools[0] = RwLock::new(pool);
            mem_pool.pools = pools;

            let arc_mem_pool = Arc::new(RwLock::new(mem_pool));
            let finalized_cost_map = BTreeMap::new();
            let sp = MockStateProvider {
                acc_info: Some(AccountInfo {
                    nonce: 0,
                    ..Default::default()
                }),
                access_list: (0..4).map(|_| AccessListItem::default()).collect(),
            };
            arc_mem_pool.write().set_finalized(
                arc_mem_pool.clone(),
                sp.clone(),
                finalized_cost_map.clone(),
            );

            thread::sleep(std::time::Duration::from_secs(2));

            {
                let mut count = 0;
                let mempool_read = arc_mem_pool.read();
                for i in 0..POOL_COUNT {
                    let pool = &mempool_read.pools[i].read();
                    count += pool.caller_map.len();
                }
                assert_eq!(count, 150000);
            }
        }
    }
}
*/
