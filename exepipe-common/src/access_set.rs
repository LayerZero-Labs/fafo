use qmdb::utils::hasher;
use revm::{
    context::{TransactionType, TxEnv},
    primitives::{Address, TxKind},
};

use crate::def::SHORT_HASH_LEN_FOR_TASK_ACCESS_SET;

type ShortHashVec = Vec<[u8; SHORT_HASH_LEN_FOR_TASK_ACCESS_SET]>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CrwSets {
    /// Short hashes of contract addresses to be read
    pub ca_read_hashes: ShortHashVec,
    /// Short hashes of contract addresses to be written
    pub ca_write_hashes: ShortHashVec,
    /// Short hashes of EOA addresses and storage slots to be read
    pub read_hashes: ShortHashVec,
    /// Short hashes of EOA and storage slots to be written
    pub write_hashes: ShortHashVec,
}

impl CrwSets {
    pub fn new(
        code_read_hashes: ShortHashVec,
        code_write_hashes: ShortHashVec,
        read_hashes: ShortHashVec,
        write_hashes: ShortHashVec,
    ) -> Self {
        Self {
            ca_read_hashes: code_read_hashes,
            ca_write_hashes: code_write_hashes,
            read_hashes,
            write_hashes,
        }
    }

    pub fn has_read_access(&self, hash: &[u8; SHORT_HASH_LEN_FOR_TASK_ACCESS_SET]) -> bool {
        self.ca_read_hashes.contains(hash)
            || self.read_hashes.contains(hash)
            || self.write_hashes.contains(hash)
            || self.has_write_access(hash)
    }

    pub fn has_write_access(&self, hash: &[u8; SHORT_HASH_LEN_FOR_TASK_ACCESS_SET]) -> bool {
        self.ca_write_hashes.contains(hash) || self.write_hashes.contains(hash)
    }
}

pub fn extract_short_hash(bz: &[u8]) -> [u8; SHORT_HASH_LEN_FOR_TASK_ACCESS_SET] {
    let mut k80 = [0u8; SHORT_HASH_LEN_FOR_TASK_ACCESS_SET];
    k80.copy_from_slice(&bz[..SHORT_HASH_LEN_FOR_TASK_ACCESS_SET]);
    k80
}

pub fn addr_to_short_hash(addr: &Address) -> [u8; SHORT_HASH_LEN_FOR_TASK_ACCESS_SET] {
    let hash = hasher::hash(addr.as_slice());
    extract_short_hash(&hash)
}

#[derive(Debug, Default, Clone)]
pub struct AccessSet {
    pub rdo_set: Vec<[u8; 10]>,
    pub rnw_set: Vec<[u8; 10]>,
}

pub fn get_to_addr_access(tx: &TxEnv) -> Option<(Address, bool /* read_only */)> {
    if let TxKind::Call(to_address) = tx.kind {
        // to with zero-value and not Eip7702 -- readonly;
        // to with non-zero-value -- write
        let ro = tx.value.is_zero() && tx.tx_type != TransactionType::Eip7702;
        return Some((to_address, ro));
    }
    None
}

pub fn will_read_address(tx: &TxEnv, addr: &Address) -> bool {
    if addr == &tx.caller {
        return true; // caller is always modifying
    }
    get_to_addr_access(tx).is_some_and(|(to_address, _)| to_address == *addr)
}

impl AccessSet {
    pub fn clear(&mut self) {
        self.rdo_set.clear();
        self.rnw_set.clear();
    }

    pub fn add_tx(&mut self, tx: &TxEnv, crw_sets: &Option<CrwSets>, include_caller_and_to: bool) {
        if include_caller_and_to {
            self.add_rnw_address(&tx.caller);
            if let Some((to_address, ro)) = get_to_addr_access(tx) {
                if ro {
                    self.add_rdo_address(&to_address);
                } else {
                    self.add_rnw_address(&to_address);
                }
            }
        }

        if let Some(crw_sets) = crw_sets {
            self.rdo_set.extend(&crw_sets.ca_read_hashes);
            self.rnw_set.extend(&crw_sets.ca_write_hashes);
            self.rdo_set.extend(&crw_sets.read_hashes);
            self.rnw_set.extend(&crw_sets.write_hashes);
        }
    }

    fn add_rnw_address(&mut self, address: &Address) {
        let hash = hasher::hash(address);
        let short_hash = extract_short_hash(&hash);
        self.rnw_set.push(short_hash);
    }

    fn add_rdo_address(&mut self, address: &Address) {
        let hash = hasher::hash(address);
        let short_hash = extract_short_hash(&hash);
        self.rdo_set.push(short_hash);
    }

    pub fn sort(&mut self) {
        self.rdo_set.sort();
        self.rnw_set.sort();
    }

    pub fn has_collision(&self, other: &Self) -> bool {
        if Self::has_common_element(&self.rnw_set, &other.rnw_set) {
            return true;
        }
        if Self::has_common_element(&self.rnw_set, &other.rdo_set) {
            return true;
        }
        if Self::has_common_element(&self.rdo_set, &other.rnw_set) {
            return true;
        }
        false
    }

    fn has_common_element(a: &Vec<[u8; 10]>, b: &Vec<[u8; 10]>) -> bool {
        let mut i = 0;
        let mut j = 0;

        while i < a.len() && j < b.len() {
            if a[i] == b[j] {
                return true;
            } else if a[i] < b[j] {
                i += 1;
            } else {
                j += 1;
            }
        }
        false
    }
}

// pub mod tests {

//    use revm::{
//        context::transaction::{AccessList, AccessListItem},
//        primitives::{B256, U256},
//    };

//    use super::*;

//    #[test]
//    fn test_access_set() {
//        use revm::primitives::alloy_primitives::U160;
//        let mut set = AccessSet::default();
//        let mut access_list = vec![];
//        let mut tx = TxEnv::default();
//        tx.caller = Address::from(U160::from(10));
//        tx.kind = TxKind::Call(Address::from(U160::from(20)));
//        tx.value = U256::from(100);
//        build_access_list(&mut access_list);
//        tx.access_list = AccessList::from(access_list.clone());
//        set.add_tx(&tx, true);
//        assert_eq!(set.rdo_set.len(), 2);
//        let bytes32_1 = U256::from(0x71_f1_f1_f1);
//        assert!(set
//            .rdo_set
//            .contains(&extract_short_hash(bytes32_1.as_le_slice())));
//        let bytes32_3 = U256::from(0x73_f3_f3_f3);
//        assert!(set
//            .rdo_set
//            .contains(&extract_short_hash(bytes32_3.as_le_slice())));

//        assert_eq!(set.rnw_set.len(), 4);
//    }

//    pub fn build_access_list(access_list: &mut Vec<AccessListItem>) {
//        access_list.push(AccessListItem {
//            address: READ_ACC,
//            storage_keys: vec![B256::from_slice(&U256::from(0x71_f1_f1_f1).as_le_bytes())],
//        });
//        access_list.push(AccessListItem {
//            address: WRITE_ACC,
//            storage_keys: vec![B256::from_slice(&U256::from(0x72_f2_f2_f2).as_le_bytes())],
//        });
//        access_list.push(AccessListItem {
//            address: READ_SLOT,
//            storage_keys: vec![B256::from_slice(&U256::from(0x73_f3_f3_f3).as_le_bytes())],
//        });
//        access_list.push(AccessListItem {
//            address: WRITE_SLOT,
//            storage_keys: vec![B256::from_slice(&U256::from(0x74_f4_f4_f4).as_le_bytes())],
//        });
//    }
// }
