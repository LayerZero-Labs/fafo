use exepipe_common::{
    access_set::{addr_to_short_hash, extract_short_hash, CrwSets},
    def::SHORT_HASH_LEN_FOR_TASK_ACCESS_SET,
    revm::{run_tx_with_disable_nonce_check, SimpleDBError},
    slot_entry_value::SlotEntryValue,
    utils::join_address_index,
};
use qmdb::{entryfile::readbuf::ReadBuf, utils::hasher, ADS};
use revm::primitives::TxKind;
use revm::{
    context::TxEnv,
    primitives::{Address, FixedBytes, U256},
    state::{AccountInfo, Bytecode},
    Database,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::context::block_context::BlockContext;

struct TxContext<'a, T: ADS> {
    blk_ctx: &'a Arc<BlockContext<T>>,
    read_slot_shorthash_list: &'a mut HashSet<[u8; SHORT_HASH_LEN_FOR_TASK_ACCESS_SET]>,
    orig_acc_map: &'a mut HashMap<Address, Option<AccountInfo>>,
    temp_buf: ReadBuf,
}

// Evm use TxContext as a Database
// It uses blk_ctx as datasource, and uses access_set as constrain
impl<T: ADS> Database for TxContext<'_, T> {
    type Error = SimpleDBError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // if the account is in orig_acc_map, return it directly
        if let Some(info) = self.orig_acc_map.get(&address) {
            return Ok(info.clone());
        }

        let res = self.blk_ctx.basic(&address);
        self.orig_acc_map.insert(address, res.clone());
        Ok(res)
    }

    fn code_by_hash(&mut self, code_hash: FixedBytes<32>) -> Result<Bytecode, Self::Error> {
        panic!("code_hash {:?} not supported", code_hash);
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let addr_idx = join_address_index(&address, &index);
        let key_hash = hasher::hash(&addr_idx[..]);
        self.read_slot_shorthash_list
            .insert(extract_short_hash(&key_hash));
        self.temp_buf.clear();
        self.blk_ctx
            .storage_value(&addr_idx, false, &mut self.temp_buf);
        if self.temp_buf.is_empty() {
            return Ok(U256::ZERO);
        }
        let slot_entry_value = SlotEntryValue::from_bz(self.temp_buf.as_slice());
        Ok(U256::from_be_slice(slot_entry_value.get_value()))
    }

    fn block_hash(&mut self, number: u64) -> Result<FixedBytes<32>, Self::Error> {
        Ok(self.blk_ctx.get_block_hash(number).unwrap())
    }
}

pub fn get_rw_sets<T: ADS>(
    tx: &TxEnv,
    blk_ctx: Arc<BlockContext<T>>,
    caller_acc_info: &AccountInfo,
) -> CrwSets {
    assert!(!tx.data.is_empty());

    let mut orig_acc_map = HashMap::new();
    orig_acc_map.insert(tx.caller, Some(caller_acc_info.clone()));
    let mut read_slot_shorthash_list = HashSet::new();

    let evm_result = {
        let tx_ctx = TxContext {
            orig_acc_map: &mut orig_acc_map,
            blk_ctx: &blk_ctx,
            read_slot_shorthash_list: &mut read_slot_shorthash_list,
            temp_buf: ReadBuf::new(),
        };
        run_tx_with_disable_nonce_check(&blk_ctx.blk.env, tx, tx_ctx).1
    };

    let mut rw_sets = CrwSets::default();
    if evm_result.is_err() {
        update_rw_sets(orig_acc_map, read_slot_shorthash_list, &mut rw_sets);
        return rw_sets;
    }

    let state = &evm_result.unwrap().state;
    for (address, account) in state {
        if !account.is_touched() {
            continue; // no change, so ignore
        }
        if account.is_empty() && account.is_loaded_as_not_existing() {
            continue; // no need to write not_existing and empty account
        }

        let orig_acc_info = orig_acc_map.remove(address).unwrap();

        let changed = match orig_acc_info {
            Some(_acc_info) if _acc_info == account.info => false,
            _ => true,
        };

        let short_hash = addr_to_short_hash(address);
        if is_ca(&account.info) {
            if changed {
                rw_sets.ca_write_hashes.push(short_hash);
            } else {
                rw_sets.ca_read_hashes.push(short_hash);
            }
        } else {
            // rw_sets does not include the caller and to address
            let is_from_or_to =
                matches!(tx.kind, TxKind::Call(addr) if address == &addr) || address == &tx.caller;
            if !is_from_or_to {
                if changed {
                    rw_sets.write_hashes.push(short_hash);
                } else {
                    rw_sets.read_hashes.push(short_hash);
                }
            }
        }

        let mut addr_idx = [0u8; 20 + 32];
        addr_idx[..20].copy_from_slice(&address[..]);
        account.changed_storage_slots().for_each(|(idx, _)| {
            addr_idx[20..].copy_from_slice(idx.as_le_slice());
            let hash = hasher::hash(addr_idx);
            let short_hash = extract_short_hash(&hash);
            read_slot_shorthash_list.remove(&short_hash);
            rw_sets.write_hashes.push(short_hash);
        });
    }

    update_rw_sets(orig_acc_map, read_slot_shorthash_list, &mut rw_sets);
    rw_sets
}

fn update_rw_sets(
    orig_acc_map: HashMap<Address, Option<AccountInfo>>,
    read_slot_shorthash_list: HashSet<[u8; 10]>,
    rw_sets: &mut CrwSets,
) {
    orig_acc_map.iter().for_each(|(addr, info_opt)| {
        if info_opt.is_none() {
            return;
        }

        let short_hash = extract_short_hash(&hasher::hash(addr));
        if is_ca(info_opt.as_ref().unwrap()) {
            rw_sets.ca_read_hashes.push(short_hash);
        } else {
            rw_sets.read_hashes.push(short_hash);
        }
    });
    rw_sets.read_hashes.extend(read_slot_shorthash_list);
}

fn is_ca(acc: &AccountInfo) -> bool {
    if acc.is_empty_code_hash() {
        return false;
    }
    if matches!(acc.code, Some(Bytecode::Eip7702(_))) {
        return false; // EIP7702
    }
    true
}
