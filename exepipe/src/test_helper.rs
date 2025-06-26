use byteorder::{BigEndian, ByteOrder};
use exepipe_common::acc_data::encode_account_info;
use qmdb::entryfile::readbuf::ReadBuf;
use qmdb::entryfile::EntryBz;
use qmdb::test_helper::EntryBuilder;
use qmdb::utils::hasher::{self, Hash32};
use qmdb::ADS;
use revm::primitives::{Address, B256, U256};
use revm::state::{AccountInfo, Bytecode};
use std::collections::HashMap;

// pub type AdsTestConfig = (
//     SharedAdsWrap,
//     Arc<RwLock<CodeDB>>,
//     Arc<ThreadPool>,
//     mpsc::SyncSender<EarlyExeInfo>,
//     mpsc::Receiver<EarlyExeInfo>,
//     SyncSender<i32>,
//     Receiver<i32>,
// );

// pub fn generate_ads_wrap(dir: &str) -> AdsTestConfig {
//     let tpool = Arc::new(ThreadPool::new(128));
//     let (sender, receiver) = sync_channel(10240);
//     let config = qmdb::config::Config::from_dir(dir);
//     AdsCore::init_dir(&config);
//     RpcDB::init_dir(dir);
//     let task_hub = Arc::new(BlockPairTaskHub::<SimpleTask>::new());
//     let (ads, _, _) = AdsCore::new(task_hub, &config);
//     let shared_ads_wrap =
//         SharedAdsWrap::new(Arc::new(ads), Arc::new(entrycache::EntryCache::new()));
//     let (s, r) = mpsc::sync_channel(10240);
//     let code_db = RwLock::new(CodeDB::new(dir, 0, 256, 1024));
//     (
//         shared_ads_wrap,
//         Arc::new(code_db),
//         tpool,
//         sender,
//         receiver,
//         s,
//         r,
//     )
// }

// ------ ADS Mock ------
pub struct MockADS {
    entry_map: HashMap<Hash32, Vec<u8>>,
    addr_to_entry: HashMap<Address, Hash32>,
    // task_list: Vec<i64>,
}

impl Default for MockADS {
    fn default() -> Self {
        Self::new()
    }
}

impl MockADS {
    pub fn new() -> Self {
        MockADS {
            entry_map: HashMap::new(),
            addr_to_entry: HashMap::new(),
            // task_list: Vec::new(),
        }
    }

    pub fn add_entry(&mut self, entry_data: Vec<u8>) -> Hash32 {
        let entry_bz = EntryBz { bz: &entry_data };
        let k = entry_bz.key_hash();
        self.entry_map.insert(k, entry_data);
        k
    }

    pub fn add_account(&mut self, addr: &Address, info: &AccountInfo) -> Hash32 {
        if let Some(code) = &info.code {
            if code != &Bytecode::new() {
                let code_data = bincode::serialize(code).unwrap();
                let code_hash = hasher::hash(&code_data);
                assert_eq!(info.code_hash, B256::new(code_hash));
            }
        }

        let v = encode_account_info(info);
        let entry = EntryBuilder::kv(&addr[..], v.as_slice()).build_and_dump(&[]);
        let h = self.add_entry(entry);
        self.addr_to_entry.insert(*addr, h);
        h
    }

    pub fn get_account(&self, addr: &Address) -> Option<AccountInfo> {
        let h = self.addr_to_entry.get(addr);
        h?;

        let v = self.entry_map.get(h.unwrap());
        v?;

        let bz = EntryBz { bz: v.unwrap() };
        let acc_data = bz.value();
        Option::Some(AccountInfo {
            balance: U256::from_be_slice(&acc_data[0..32]),
            nonce: BigEndian::read_u64(&acc_data[32..32 + 8]),
            code_hash: B256::from_slice(&acc_data[32 + 8..]),
            code: Option::None,
        })
    }
}

impl ADS for MockADS {
    fn read_entry(&self, _height: i64, key_hash: &[u8], _key: &[u8], buf: &mut ReadBuf) -> i64 {
        let mut k: Hash32 = [0u8; 32];
        k.copy_from_slice(key_hash);
        match self.entry_map.get(&k) {
            None => 0,
            Some(v) => {
                buf.resize(v.len());
                buf.as_mut_slice().copy_from_slice(v);
                0
            }
        }
    }

    fn read_entry_with_uring(
        &self,
        _height: i64,
        key_hash: &[u8],
        _key: &[u8],
        buf: &mut ReadBuf,
    ) -> i64 {
        self.read_entry(-1, key_hash, &[], buf)
    }

    fn warmup(&self, _height: i64, _: &[u8]) {
        panic!("warmup")
    }

    fn add_task(&self, _task_id: i64) {
        // self.task_list.push(task_id);
    }

    fn add_task_to_updater(&self, _task_id: i64) {}

    fn fetch_prev_entries(&self, _task_id: i64) {}

    fn insert_extra_data(&self, _height: i64, _data: String) {}

    fn get_root_hash_of_height(&self, _height: i64) -> [u8; 32] {
        [0u8; 32]
    }
}

pub fn addr_idx_hash(addr: &Address, idx: &U256) -> U256 {
    let mut addr_idx = [0u8; 20 + 32];
    addr_idx[..20].copy_from_slice(&addr[..]);
    addr_idx[20..].copy_from_slice(&idx.to_be_bytes_vec());
    U256::from_be_bytes(hasher::hash(&addr_idx[..]))
}

pub fn encode_ec20_transfer_data(recipient: &Address, amount: u128) -> String {
    let str = recipient.to_string();
    let recipient = str.trim_start_matches("0x");
    format!("0xa9059cbb{:0>64}{:064x}", recipient, amount)
}
