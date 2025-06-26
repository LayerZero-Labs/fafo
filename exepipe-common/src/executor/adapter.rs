use anyhow::Result;
use qmdb::{entryfile::readbuf::ReadBuf, utils::hasher, ADS};
use revm::{
    primitives::{Address, B256, U256},
    state::{AccountInfo, Bytecode},
    Database,
};
use std::cell::RefCell;
use std::collections::HashMap;

use crate::{
    acc_data::decode_account_info_from_bz, revm::SimpleDBError, slot_entry_value::SlotEntryValue,
    utils::join_address_index,
};

pub struct AdsDatabaseAdapter<T: ADS> {
    ads: T,
    height: i64,
    dust_map: RefCell<HashMap<[u8; 32], u128>>,
}

impl<T: ADS> AdsDatabaseAdapter<T> {
    pub fn new(ads: T, height: i64, dust_map: RefCell<HashMap<[u8; 32], u128>>) -> Self {
        Self {
            ads,
            height,
            dust_map,
        }
    }
}

impl<T: ADS> Database for AdsDatabaseAdapter<T> {
    type Error = SimpleDBError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let mut buf = ReadBuf::new();
        let key_hash = hasher::hash(&address[..]);
        let _ = self
            .ads
            .read_entry(self.height, &key_hash, &address[..], &mut buf);

        if buf.is_empty() {
            return Ok(None);
        }

        let entry_bz = buf.as_entry_bz();
        let mut info = decode_account_info_from_bz(entry_bz.value());
        if info.code.is_none() {
            if info.is_empty_code_hash() {
                info.code = Some(Bytecode::new());
            } else {
                // FIXME: should use code_by_hash or code_db??
                // let bc = self.get_bytecode(&address);
                // assert!(bc.hash_slow() == info.code_hash);
                // info.code = Some(bc);
            }
        }
        Ok(Some(info))
    }

    fn code_by_hash(&mut self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        // This should be handled by CodeDB, not the ADS
        Err(SimpleDBError::new(anyhow::anyhow!(
            "code_by_hash not supported in ADS"
        )))
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let k: [u8; 52] = join_address_index(&address, &index);
        let key_hash = hasher::hash(k);
        let mut buf = ReadBuf::new();
        let _ = self.ads.read_entry(self.height, &key_hash, &k, &mut buf);

        if buf.is_empty() {
            return Ok(U256::ZERO);
        }

        let entry_bz = buf.as_entry_bz();
        let slot_entry_value = SlotEntryValue::from_bz(entry_bz.value());
        self.dust_map
            .borrow_mut()
            .insert(key_hash, slot_entry_value.get_dust());
        Ok(U256::from_be_slice(slot_entry_value.get_value()))
    }

    fn block_hash(&mut self, _number: u64) -> Result<B256, Self::Error> {
        Ok(B256::ZERO) // Default implementation matching kvstore.rs
    }
}
