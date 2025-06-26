use std::collections::HashMap;

use crate::access_set::{get_to_addr_access, CrwSets};
use crate::revm::run_tx;
use anyhow::Result;
use qmdb::utils::changeset::ChangeSet;
use qmdb::utils::hasher::Hash32;
use revm::context::result::{EVMError, ResultAndState};
use revm::context::{BlockEnv, ContextTr, Transaction, TxEnv};
use revm::primitives::{Address, U256};
use revm::state::Bytecode;

use super::error::{DBError, ExecutionError};
use super::exector_database::ExectorDatabase;
use super::get_change_set::{get_change_set, ChangeSetConfig};
use super::ExecutorProxy;

pub fn execute_tx<DB: ExectorDatabase>(
    mut db: DB,
    blk_env: &BlockEnv,
    tx: &TxEnv,
    crw_sets: &CrwSets,
) -> (
    Result<ResultAndState, ExecutionError>,
    Option<(
        ChangeSet,
        u128,                       // deletion_reward
        u64,                        // total_gas_used
        HashMap<Address, Bytecode>, // created_codes_map
        Vec<Hash32>,                // touched_kh_list
        Vec<Hash32>,                // exclusion_kh_list>,
    )>,
) {
    let caller = tx.caller;
    let caller_info = match db.basic(&caller) {
        Some(info) => info,
        None => {
            return (Err(ExecutionError::CallerNotFound(caller)), None);
        }
    };

    // each tx has its own orig_acc_map and  orig_slot_dust_map
    let mut orig_acc_map = HashMap::new();

    let coinbase_gas_price = tx.effective_gas_price(blk_env.basefee as u128);
    let balance = caller_info.balance;
    let required = tx.value
        + U256::from(coinbase_gas_price)
            .checked_mul(U256::from(tx.gas_limit))
            .unwrap();
    if balance < required {
        return (
            Err(ExecutionError::InsufficientBalanceForCall { balance, required }),
            None,
        );
    };
    orig_acc_map.insert(caller, Some(caller_info));

    let db = ExecutorProxy::new(tx, db, &mut orig_acc_map, crw_sets);

    let (mut evm, evm_result) = run_tx(&blk_env, &tx, db);
    if let Err(err) = evm_result {
        let need_drop = matches!(
            err,
            EVMError::Database(DBError::AccountNotIncluded(..))
                | EVMError::Database(DBError::SlotNotIncluded { .. })
        );

        if need_drop {
            return (Err(ExecutionError::EVMError(err)), None);
        }

        let (touched_kh_list, change_set, gas_used) =
            handle_tx_execute_mpex_err(tx, coinbase_gas_price);
        return (
            Err(ExecutionError::EVMError(err)),
            Some((
                change_set,
                0,
                gas_used,
                HashMap::new(),
                touched_kh_list,
                Vec::with_capacity(0),
            )),
        );
    }

    let res_and_state = evm_result.unwrap();
    let gas_used = res_and_state.result.gas_used();

    // we must check not writing the readonly account and slot.
    let db = evm.db();
    let get_cs_result = get_change_set(ChangeSetConfig {
        caller: &tx.caller,
        to_addr_access: get_to_addr_access(tx),
        gas_price: coinbase_gas_price,
        remained_gas_limit: tx.gas_limit - gas_used,
        state: &res_and_state.state,
        orig_acc_map: db.orig_acc_map,
        orig_slot_dust_map: &db.orig_slot_dust_map,
        crw_sets,
    });

    if let Err(err) = get_cs_result {
        return (Err(err), None);
    }

    let (touched_kh_list, exclusion_kh_list) = evm.db().get_touched_and_exclusion_kh_list();
    let (change_set, deletion_reward, gas_used_with_creation_cost, created_codes_map) =
        get_cs_result.unwrap();

    (
        Ok(res_and_state),
        Some((
            change_set,
            deletion_reward,
            gas_used_with_creation_cost + gas_used,
            created_codes_map,
            touched_kh_list,
            exclusion_kh_list,
        )),
    )
}

// at errors, cannot apply state change, but we need deduct all gas from caller.
// handle_tx_execute_mpex_err will be call at errors because:
// 1. warmup generates errors
// 2. revm.transact gets errors from 'Database' or handlers and returns them
// 2. get_change_set_and_check_access_rw returns error of writing readonly data
fn handle_tx_execute_mpex_err(_: &TxEnv, _: u128) -> (Vec<Hash32>, ChangeSet, u64) {
    panic!("not implemented");
}
