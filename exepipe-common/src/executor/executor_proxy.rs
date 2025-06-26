use super::{error::DBError, exector_database::ExectorDatabase};
use crate::{
    access_set::{extract_short_hash, will_read_address, CrwSets},
    slot_entry_value::SlotEntryValue,
    utils::join_address_index,
};
use qmdb::utils::hasher::{self, Hash32};
use revm::{
    context::TxEnv,
    primitives::{Address, B256, U256},
    state::{AccountInfo, Bytecode},
    Database,
};
use std::collections::HashMap;

pub struct ExecutorProxy<'a, DB: ExectorDatabase> {
    tx: &'a TxEnv,
    db: DB,
    pub orig_acc_map: &'a mut HashMap<Address, Option<AccountInfo>>,
    crw_sets: &'a CrwSets,
    pub orig_slot_dust_map: HashMap<Hash32, Option<u128>>,
}

impl<'a, DB: ExectorDatabase> ExecutorProxy<'a, DB> {
    pub fn new(
        tx: &'a TxEnv,
        db: DB,
        orig_acc_map: &'a mut HashMap<Address, Option<AccountInfo>>,
        crw_sets: &'a CrwSets,
    ) -> Self {
        Self {
            tx,
            db,
            orig_acc_map,
            crw_sets,
            orig_slot_dust_map: HashMap::new(),
        }
    }
}

impl<DB: ExectorDatabase> Database for ExecutorProxy<'_, DB> {
    type Error = DBError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(info) = self.orig_acc_map.get(&address) {
            return Ok(info.clone());
        }

        let key_hash = hasher::hash(&address[..]);
        let short_hash = extract_short_hash(&key_hash[..]);
        if !(will_read_address(self.tx, &address) || self.crw_sets.has_read_access(&short_hash)) {
            return Err(DBError::AccountNotIncluded(address));
        }

        let result = self.db.basic(&address);
        self.orig_acc_map.insert(address, result.clone());

        Ok(result)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        panic!("code_by_hash {:?} not supported", code_hash);
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let k: [u8; 52] = join_address_index(&address, &index);
        let key_hash = hasher::hash(k);
        let short_hash = extract_short_hash(&key_hash[..]);
        if !self.crw_sets.has_read_access(&short_hash) {
            return Err(DBError::SlotNotIncluded {
                addr: address,
                index,
            })?;
        }

        let entry_value = self.db.storage_value(address, &index);
        if entry_value.is_none() {
            self.orig_slot_dust_map.insert(key_hash, None);
            return Ok(U256::ZERO);
        }

        let slot_entry_value = SlotEntryValue::from_bz(entry_value.as_ref().unwrap());

        let value = U256::from_be_slice(slot_entry_value.get_value());
        let dust = slot_entry_value.get_dust();

        self.orig_slot_dust_map.insert(key_hash, Some(dust));

        Ok(value)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        Ok(self.db.block_hash(number).unwrap())
    }
}

impl<DB: ExectorDatabase> ExecutorProxy<'_, DB> {
    pub fn get_touched_and_exclusion_kh_list(&self) -> (Vec<Hash32>, Vec<Hash32>) {
        let mut touched_kh_list = Vec::new();
        let mut exclusion_kh_list = Vec::new();
        for (key_hash, dust) in &self.orig_slot_dust_map {
            if dust.is_some() {
                touched_kh_list.push(*key_hash);
            } else {
                exclusion_kh_list.push(*key_hash);
            }
        }
        for (address, acc_info) in self.orig_acc_map.iter() {
            let key_hash = hasher::hash(&address[..]);
            if acc_info.is_some() {
                touched_kh_list.push(key_hash);
            } else {
                exclusion_kh_list.push(key_hash);
            }
        }
        (touched_kh_list, exclusion_kh_list)
    }
}
