use exepipe_common::access_set::{AccessSet, CrwSets};
use exepipe_common::def::SHORT_HASH_LEN_FOR_TASK_ACCESS_SET;
use qmdb::tasks::Task;
use qmdb::utils::changeset::ChangeSet;
use qmdb::utils::hasher::{self};
use revm::context::transaction::{AccessList, AccessListItem};
use revm::context::TxEnv;
use revm::primitives::B256;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tx {
    pub tx_hash: B256,
    pub crw_sets: Option<CrwSets>,
    pub env: TxEnv,
}

#[derive(Debug)]
pub struct ExeTask {
    pub tx_list: Vec<Box<Tx>>,
    pub access_set: AccessSet,
    change_sets: Option<Arc<Vec<ChangeSet>>>,
    task_out_start: AtomicU32,
}

impl Task for ExeTask {
    fn get_change_sets(&self) -> Arc<Vec<ChangeSet>> {
        self.change_sets.as_ref().unwrap().clone()
    }
}

impl ExeTask {
    pub fn new(tx_list: Vec<Box<Tx>>) -> Self {
        Self::_new(tx_list, true)
    }

    pub fn new_for_test(tx_list: Vec<Box<Tx>>) -> Self {
        let mut tx_list = tx_list.clone();
        for tx in &mut tx_list {
            for item in &mut tx.env.access_list.0 {
                for b256 in &mut item.storage_keys {
                    // use short hash
                    let mut hash = hasher::hash(&b256);
                    hash[SHORT_HASH_LEN_FOR_TASK_ACCESS_SET..]
                        .copy_from_slice(&[0; 32 - SHORT_HASH_LEN_FOR_TASK_ACCESS_SET]);
                    *b256 = B256::from_slice(&hash);
                }
            }
        }
        Self::_new(tx_list, false)
    }

    fn _new(tx_list: Vec<Box<Tx>>, include_caller_and_to: bool) -> Self {
        let mut task = Self {
            tx_list,
            access_set: AccessSet::default(),
            change_sets: None,
            task_out_start: AtomicU32::new(u32::MAX),
        };
        task._init(include_caller_and_to);
        task
    }

    fn _init(&mut self, include_caller_and_to: bool) {
        self.access_set.clear();

        for tx in &self.tx_list {
            self.access_set
                .add_tx(&tx.env, &tx.crw_sets, include_caller_and_to);
        }
        self.access_set.sort();
    }

    pub fn set_txs_access_list(&mut self, txs_access_list: Vec<Vec<AccessListItem>>) {
        for (tx, access_list) in self.tx_list.iter_mut().zip(txs_access_list) {
            tx.env.access_list = AccessList::from(access_list);
        }
        self._init(false);
    }

    pub fn set_change_sets(&mut self, change_sets: Arc<Vec<ChangeSet>>) {
        self.change_sets = Some(change_sets);
    }
    pub fn set_task_out_start(&self, bundle_start: usize) {
        self.task_out_start
            .store(bundle_start as u32, Ordering::SeqCst);
    }
    pub fn get_task_out_start(&self) -> usize {
        self.task_out_start.load(Ordering::SeqCst) as usize
    }

    //'a' task collide with 'b' task
    pub fn has_collision(a: &ExeTask, b: &ExeTask) -> bool {
        a.access_set.has_collision(&b.access_set)
    }
}
/*
TODO
#[cfg(test)]
pub mod test_exe_task {
    use crate::exetask::{ExeTask, Tx};
    use exepipe_common::access_set::tests::build_access_list;
    use revm::primitives::TxEnv;
    use std::collections::HashSet;

    #[test]
    fn test_exe_task() {
        let mut tx_env = TxEnv::default();
        let mut access_list = vec![];
        build_access_list(&mut access_list);
        tx_env.access_list = access_list;
        let tx = Tx {
            tx_hash: Default::default(),
            receipt_hash: Default::default(),
            env: tx_env,
        };
        let task = ExeTask::new(vec![tx]);
        assert_eq!(task.access_set.rdo_set.len(), 3);
        assert_eq!(task.access_set.rnw_set.len(), 3);
        task.set_task_out_start(1);
        assert_eq!(task.get_task_out_start(), 1);
        task.set_min_all_done_index(1);
        assert_eq!(task.get_min_all_done_index(), 1);
    }

    #[test]
    fn test_set_txs_access_list() {
        let mut tx_env = TxEnv::default();
        let mut access_list = vec![];
        build_access_list(&mut access_list);
        tx_env.access_list = access_list;
        let tx = Tx {
            tx_hash: Default::default(),
            receipt_hash: Default::default(),
            env: tx_env,
        };
        let mut task = ExeTask::new(vec![tx]);

        let mut txs_access_list = vec![];
        let mut access_list = vec![];
        build_access_list(&mut access_list);
        txs_access_list.push(access_list);
        task.set_txs_access_list(txs_access_list);
        assert_eq!(task.tx_list.len(), 1);
        assert_eq!(task.tx_list[0].env.access_list.len(), 4);

        assert_eq!(
            task.access_set.rdo_set,
            HashSet::from_iter(vec![
                [241, 241, 241, 113, 0, 0, 0, 0, 0, 0],
                [243, 243, 243, 115, 0, 0, 0, 0, 0, 0]
            ])
        );
        assert_eq!(
            task.access_set.rnw_set,
            HashSet::from_iter(vec![
                [242, 242, 242, 114, 0, 0, 0, 0, 0, 0],
                [244, 244, 244, 116, 0, 0, 0, 0, 0, 0]
            ])
        );
    }
}
*/
