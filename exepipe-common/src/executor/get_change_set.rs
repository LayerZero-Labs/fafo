use crate::{
    acc_data::encode_account_info,
    access_set::{extract_short_hash, CrwSets},
    def::{BYTECODE_BURN_GAS_PER_BYTE, SLOT_LOCK_GAS},
    slot_entry_value::SlotEntryValue,
};
use qmdb::{
    def::{OP_CREATE, OP_DELETE, OP_WRITE},
    utils::{
        byte0_to_shard_id,
        changeset::ChangeSet,
        hasher::{self, Hash32},
    },
};
use revm::{
    primitives::{Address, U256},
    state::{AccountInfo, Bytecode, EvmState},
};
use std::collections::HashMap;

use super::error::ExecutionError;

pub type ChangeSetResult =
    Result<(ChangeSet, u128, u64, HashMap<Address, Bytecode>), ExecutionError>;

pub struct ChangeSetConfig<'a> {
    pub caller: &'a Address,
    pub to_addr_access: Option<(Address, bool /* read_only */)>,
    pub gas_price: u128,
    pub remained_gas_limit: u64,
    pub state: &'a EvmState,
    pub orig_acc_map: &'a HashMap<Address, Option<AccountInfo>>,
    pub orig_slot_dust_map: &'a HashMap<Hash32, Option<u128>>,
    pub crw_sets: &'a CrwSets,
}

pub fn get_change_set(config: ChangeSetConfig) -> ChangeSetResult {
    let mut change_set = ChangeSet::new();
    let mut created_code_map = HashMap::with_capacity(1);

    let mut addr_idx = [0u8; 20 + 32];

    let mut creation_cost_gas: u64 = 0;
    let mut deletion_reward: u128 = 0;
    let _gas_price = config.gas_price;

    for (address, account) in config.state {
        if !account.is_touched() {
            continue; // no change, so ignore
        }
        if account.is_empty() && account.is_loaded_as_not_existing() {
            continue; // no need to write not_existing and empty account
        }
        if account.is_created()
            && !account.is_selfdestructed()
            && !account.info.is_empty_code_hash()
        {
            if let Some(bytecode) = account.info.code.as_ref() {
                assert!(!bytecode.is_eip7702());
                // create bytecode dust gas
                creation_cost_gas += bytecode.len() as u64 * BYTECODE_BURN_GAS_PER_BYTE;
                created_code_map.insert(*address, bytecode.clone());
            }
        }

        // caller account be added to change_set at last
        if address != config.caller {
            let orig_acc_info = config.orig_acc_map.get(address).unwrap();

            let op_type_opt = match orig_acc_info {
                Some(_acc_info) if _acc_info == &account.info => None,
                Some(_) => Some(OP_WRITE),
                None => Some(OP_CREATE),
            };

            if let Some(op_type) = op_type_opt {
                let hash = hasher::hash(address);
                if !(config
                    .to_addr_access
                    .is_some_and(|(addr, ro)| !ro && &addr == address)
                    || config.crw_sets.has_write_access(&extract_short_hash(&hash)))
                {
                    return Err(ExecutionError::WritedAccountNotIncluded(*address));
                }

                change_set.add_op_with_old_value(
                    op_type,
                    byte0_to_shard_id(hash[0]) as u8,
                    &hash,        //used during sort as key_hash
                    &address[..], //the key
                    encode_account_info(&account.info).as_slice(), //the value
                    orig_acc_info
                        .as_ref()
                        .map(encode_account_info)
                        .unwrap_or_default()
                        .as_slice(),
                    None,
                );
            }
        }

        addr_idx[..20].copy_from_slice(&address[..]);
        for (idx, slot) in account.changed_storage_slots() {
            let mut op = OP_WRITE;
            if slot.original_value == U256::ZERO {
                op = OP_CREATE;
            } else if slot.present_value == U256::ZERO {
                op = OP_DELETE;
            }
            addr_idx[20..].copy_from_slice(idx.as_le_slice());
            let hash = hasher::hash(addr_idx);
            if !config.crw_sets.has_write_access(&extract_short_hash(&hash)) {
                return Err(ExecutionError::WritedSlotNotIncluded {
                    addr: *address,
                    index: *idx,
                });
            }

            let mut v = slot.present_value.to_be_bytes_vec();
            let mut old_v = slot.original_value.to_be_bytes_vec();
            match op {
                OP_CREATE => {
                    // create slot dust gas
                    let dust = SLOT_LOCK_GAS as u128 * _gas_price;
                    SlotEntryValue::encode(&mut v, dust);
                    creation_cost_gas += SLOT_LOCK_GAS;
                }
                OP_DELETE => {
                    // deletion reward to caller
                    let dust = config.orig_slot_dust_map.get(&hash).unwrap().unwrap();
                    SlotEntryValue::encode(&mut old_v, dust);
                    deletion_reward += dust;
                }
                OP_WRITE => {
                    let dust = config.orig_slot_dust_map.get(&hash).unwrap().unwrap();
                    SlotEntryValue::encode(&mut v, dust);
                    SlotEntryValue::encode(&mut old_v, dust);
                }
                _ => {}
            }
            change_set.add_op_with_old_value(
                op,
                byte0_to_shard_id(hash[0]) as u8,
                &hash,         //used during sort as key_hash
                &addr_idx[..], //the key
                &v[..],        //the value
                &old_v[..],
                None,
            );
        }
    }

    let mut caller_info = config.state.get(config.caller).unwrap().info.clone();
    // need process dust fee for caller account
    let creation_cost = creation_cost_gas as u128 * _gas_price;
    if creation_cost > deletion_reward {
        let dust_gas_fee = creation_cost - deletion_reward;
        let remaining = config.remained_gas_limit as u128 * _gas_price;
        if dust_gas_fee > remaining {
            // spend dust gas fee more than gas limit
            return Err(ExecutionError::DustFeeExceedsRemainingGas {
                remaining,
                required: dust_gas_fee,
            });
        }
        // caller_info.balance mut be enough to pay dust gas fee because revm check balance larger than gas_limit
        caller_info.balance = caller_info
            .balance
            .checked_sub(U256::from(dust_gas_fee))
            .unwrap();
    } else {
        caller_info.balance = caller_info
            .balance
            .checked_add(U256::from(deletion_reward - creation_cost))
            .unwrap();
    }

    let new_caller_info = encode_account_info(&caller_info);
    let orig_acc_info = config.orig_acc_map.get(config.caller).unwrap();
    let hash = hasher::hash(config.caller);
    change_set.add_op_with_old_value(
        OP_WRITE,
        byte0_to_shard_id(hash[0]) as u8,
        &hash,                      //used during sort as key_hash
        &config.caller[..],         //the key
        new_caller_info.as_slice(), //the value
        encode_account_info(orig_acc_info.as_ref().unwrap()).as_slice(),
        None,
    );

    change_set.sort();
    Ok((
        change_set,
        deletion_reward,
        creation_cost_gas,
        created_code_map,
    ))
}

#[cfg(test)]
mod tests {

    #[test]
    #[ignore = "WIP"]
    fn test_commit_state_change() {
        /*
        let temp_dir = tempfile::Builder::new()
            .prefix("commit_state")
            .tempdir()
            .unwrap();

        let mut state = EvmState::default();
        let from_address = Address::from(U160::from(10));
        let mut from_acc = Account::from(AccountInfo::default());
        from_acc.mark_touch();
        from_acc.mark_created();
        // prepare write slot
        let slot_w_idx = U256::from(101);
        let slot_w = EvmStorageSlot::new_changed(U256::from(1), U256::from(2));
        from_acc.storage.insert(slot_w_idx, slot_w.clone());
        // prepare create slot
        let slot_c_idx = U256::from(102);
        let slot_c = EvmStorageSlot::new_changed(U256::from(0), U256::from(3));
        from_acc.storage.insert(slot_c_idx, slot_c.clone());
        // prepare delete slot
        let slot_d_idx = U256::from(103);
        let slot_d = EvmStorageSlot::new_changed(U256::from(4), U256::from(0));
        from_acc.storage.insert(slot_d_idx, slot_d.clone());
        state.insert(from_address, from_acc.clone());

        // prepare create account1
        let bytecode = Bytecode::new_raw(Bytes::from_hex("0x1122").unwrap());
        let bc_bytes = bincode::serialize(&bytecode).unwrap();
        let bc_hash = hasher::hash(&bc_bytes);
        let mut acc1 = Account::from(AccountInfo::new(
            U256::from(10000),
            0,
            bc_hash.into(),
            bytecode,
        ));
        acc1.mark_touch();
        acc1.mark_created();
        let addr1 = Address::from(U160::from(1));
        state.insert(addr1, acc1);

        // prepare write account2
        let mut acc2 = Account::from(AccountInfo::default());
        acc2.mark_touch();
        let addr2 = Address::from(U160::from(2));
        state.insert(addr2, acc2);

        // prepare orig_acc_map
        let mut orig_acc_map = HashMap::<Address, AccData>::new();
        orig_acc_map.insert(from_address, AccData::new(&[0; ACC_DATA_DEFAULT_LEN]));
        orig_acc_map.insert(addr2, AccData::new(&[0; ACC_DATA_DEFAULT_LEN]));

        let mut orig_slot_dust_map = HashMap::<Hash32, u128>::new();
        let state_cache = StateCache::new();
        let mut access_set = AccessSet::default();
        let buf32 = hasher::hash(from_address);
        access_set.rnw_set.insert(extract_short_hash(&buf32[..]));
        let buf32 = hasher::hash(addr1);
        access_set.rnw_set.insert(extract_short_hash(&buf32[..]));
        let buf32 = hasher::hash(addr2);
        access_set.rnw_set.insert(extract_short_hash(&buf32[..]));

        let mut addr_idx = [0u8; 32 + 20];
        addr_idx[..20].copy_from_slice(&from_address[..]);
        let buf32: [u8; 32] = slot_w_idx.to_le_bytes();
        addr_idx[20..].copy_from_slice(&buf32[..]);
        let slot_w_hash = hasher::hash(addr_idx);
        access_set
            .rnw_set
            .insert(extract_short_hash(&slot_w_hash[..]));
        orig_slot_dust_map.insert(slot_w_hash, 1 << 48);

        let buf32: [u8; 32] = slot_c_idx.to_le_bytes();
        addr_idx[20..].copy_from_slice(&buf32[..]);
        let slot_c_hash = hasher::hash(addr_idx);
        access_set
            .rnw_set
            .insert(extract_short_hash(&slot_c_hash[..]));

        let buf32: [u8; 32] = slot_d_idx.to_le_bytes();
        addr_idx[20..].copy_from_slice(&buf32[..]);
        let slot_d_hash = hasher::hash(addr_idx);
        access_set
            .rnw_set
            .insert(extract_short_hash(&slot_d_hash[..]));
        orig_slot_dust_map.insert(slot_d_hash, 1 << 48);

        let gas_price = 2;
        {
            let res = get_change_set_and_check_access_rw(ChangeSetConfig {
                caller: &from_address,
                gas_price: &U256::from(gas_price),
                remained_gas_limit: 0,
                state: &state,
                orig_acc_map: &orig_acc_map,
                orig_slot_dust_map: &orig_slot_dust_map,
                state_cache: &state_cache,
                access_set: &access_set,
                check_access: true,
            });
            assert_eq!(
                res.err().unwrap().to_string(),
                InvalidTransaction::CallGasCostMoreThanGasLimit.to_string()
            );
        }
        let initial_balance = 1000000000000000000u128;
        from_acc.info.balance = U256::from(initial_balance);
        state.insert(from_address, from_acc.clone());

        let res = get_change_set_and_check_access_rw(ChangeSetConfig {
            caller: &from_address,
            gas_price: &U256::from(gas_price),
            remained_gas_limit: 100000000000000000,
            state: &state,
            orig_acc_map: &orig_acc_map,
            orig_slot_dust_map: &orig_slot_dust_map,
            state_cache: &state_cache,
            access_set: &access_set,
            check_access: true,
        });

        assert!(res.is_ok());
        let code_db = Arc::new(RwLock::new(CodeDB::new(
            temp_dir.path().to_str().unwrap(),
            0,
            512,
            2048,
        )));
        let (mut change_set, deletion_reward, creation_cost, created_codes_map) = res.unwrap();
        for (bc_hash, bc_bytes) in created_codes_map.iter() {
            code_db.write().append(bc_hash, bc_bytes).unwrap();
        }
        code_db.write().flush().unwrap();

        assert_eq!(deletion_reward, 1 << 48);
        let mut expected_creation_cost = 0;
        {
            let dust = SLOT_LOCK_GAS * gas_price;
            expected_creation_cost += SlotEntryValue::encode(&mut vec![], dust);
            expected_creation_cost += NEW_ADDR_BURN_GAS * gas_price;
            expected_creation_cost +=
                bc_bytes.len() as u128 * BYTECODE_BURN_GAS_PER_BYTE * gas_price;

            assert_eq!(creation_cost, expected_creation_cost);
        }

        change_set.sort();

        let count = Rc::new(Mutex::new(0));
        assert_eq!(change_set.op_list.len(), 6);

        let mut buf = vec![0; DEFAULT_ENTRY_SIZE];
        let size = code_db.read().read_code_entry(&bc_hash[..], &mut buf);
        let entry_bz = EntryBz { bz: &buf[..size] };
        assert!(entry_bz.value() == bc_bytes.as_slice());

        let _count = count.clone();
        change_set.apply_op_in_range(move |op, key_hash, k, v, old_v, _| {
            if key_hash == &slot_w_hash {
                *_count.lock().unwrap() += 1;
                assert_eq!(op, OP_WRITE);
                assert_eq!(key_hash, &hasher::hash(k));
                let mut old_value = slot_w.original_value.to_be_bytes_vec();
                SlotEntryValue::encode(&mut old_value, 1 << 48);
                assert_eq!(old_v, old_value);
                let mut new_value = slot_w.present_value.to_be_bytes_vec();
                SlotEntryValue::encode(&mut new_value, 1 << 48);
                assert_eq!(v, new_value);
            }
            if key_hash == &slot_c_hash {
                *_count.lock().unwrap() += 1;
                assert_eq!(op, OP_CREATE);
                assert_eq!(key_hash, &hasher::hash(k));
                let mut new_value = slot_c.present_value.to_be_bytes_vec();
                let dust = SLOT_LOCK_GAS * gas_price;
                SlotEntryValue::encode(&mut new_value, dust);
                assert_eq!(v[..], new_value);
                assert_eq!(old_v, vec![0; 32]);
            }
            if key_hash == &slot_d_hash {
                *_count.lock().unwrap() += 1;
                assert_eq!(op, OP_DELETE);
                assert_eq!(key_hash, &hasher::hash(k));
                let mut old_value = slot_d.original_value.to_be_bytes_vec();
                SlotEntryValue::encode(&mut old_value, 1 << 48);
                assert_eq!(old_v, old_value);
                let new_value = slot_d.present_value.to_be_bytes_vec();
                assert_eq!(v, new_value);
            }
            if k == &from_address[..] {
                *_count.lock().unwrap() += 1;
                assert_eq!(op, OP_WRITE);
                assert_eq!(key_hash, &hasher::hash(k));
                let old_value = orig_acc_map.get(k).unwrap().bytes();
                assert_eq!(old_v, old_value);
                let (new_acc_info, _) = decode_account_info(v);
                assert_eq!(
                    new_acc_info.balance,
                    U256::from(initial_balance + deletion_reward - creation_cost)
                );
                assert_eq!(new_acc_info.nonce, 0);
                assert_eq!(new_acc_info.code_hash, KECCAK_EMPTY);
            }
            if k == &addr1[..] {
                *_count.lock().unwrap() += 1;
                assert_eq!(op, OP_CREATE);
                assert_eq!(key_hash, &hasher::hash(k));
                assert_eq!(v.len(), 37);
                let (new_acc_info, _) = decode_account_info(v);
                assert_eq!(new_acc_info.balance, U256::from(10000));
                assert_eq!(new_acc_info.nonce, 0);
                assert_eq!(new_acc_info.code_hash, bc_hash);
            }
            if k == &addr2[..] {
                *_count.lock().unwrap() += 1;
                assert_eq!(op, OP_WRITE);
                assert_eq!(key_hash, &hasher::hash(k));
                let old_value = orig_acc_map.get(k).unwrap().bytes();
                assert_eq!(old_v, old_value);
                let (new_acc_info, _) = decode_account_info(v);
                assert_eq!(new_acc_info.balance, U256::from(0));
                assert_eq!(new_acc_info.nonce, 0);
                assert_eq!(new_acc_info.code_hash, KECCAK_EMPTY);
            }
        });
        assert_eq!(*count.lock().unwrap(), 6);     */
    }
}
