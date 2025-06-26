use byteorder::{BigEndian, ByteOrder};
use cache::CodeCache;
use def::{CACHE_RETENTION, URING_COUNT, URING_SIZE};
use exepipe_common::access_set::{addr_to_short_hash, extract_short_hash};
use exepipe_common::def::SHORT_HASH_LEN_FOR_TASK_ACCESS_SET;
use parking_lot::Mutex;
use qmdb::entryfile::{Entry, EntryBz, EntryFile, EntryFileWithPreReader, EntryFileWriter};
use qmdb::indexer::memidx::InMemIndexer;
use qmdb::indexer::utils::poslist::PosList;
use qmdb::indexer::IndexerTrait;
use rand_core::{OsRng, RngCore};
use revm::primitives::bytes::BytesMut;
use revm::primitives::{Address, Bytes};
use revm::state::Bytecode;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64_with_secret;
pub mod cache;
pub mod def;

#[cfg(target_os = "linux")]
pub mod uring;

struct CodeIndexer {
    bti: InMemIndexer,
    secret: [u8; 136],
}

impl CodeIndexer {
    pub fn new() -> Self {
        let mut secret = [0u8; 136];
        OsRng.fill_bytes(secret.as_mut());

        Self {
            // only 256 shards are needed because we clear high 8 bits of k64
            bti: InMemIndexer::new(256),
            secret,
        }
    }

    pub fn add_kv(&self, key: &[u8], v_in: i64) {
        let k64 = xxh3_64_with_secret(key, &self.secret[..]) >> 8;
        let mut k80 = [0u8; 10];
        BigEndian::write_u64(&mut k80[..8], k64);
        self.bti.add_kv(&k80[..], v_in, 0).unwrap();
    }

    pub fn for_each_value(&self, key: &[u8]) -> PosList {
        let k64 = xxh3_64_with_secret(key, &self.secret[..]) >> 8;
        let mut k80 = [0u8; 10];
        BigEndian::write_u64(&mut k80[..8], k64);
        self.bti.for_each_value(-1, &k80[..])
    }
}

pub struct CodeDB {
    writer: Mutex<EntryFileWriter>,
    code_indexer: CodeIndexer,
    entry_file: Arc<EntryFile>,
    code_cache: Arc<CodeCache>,
    #[cfg(target_os = "linux")]
    uring_worker_manager: uring::WorkerManager,
}

impl CodeDB {
    pub fn new(dir: &str, truncate_size: i64, wrbuf_size: usize, file_segment_size: usize) -> Self {
        let code_dir = format!("{}/{}", dir, "code");
        let _ = std::fs::create_dir_all(&code_dir);
        let code_file = Arc::new(EntryFile::new(
            wrbuf_size,
            file_segment_size as i64,
            code_dir,
            true,
            None,
        ));
        if truncate_size > 0 && code_file.size() > truncate_size {
            code_file.truncate(truncate_size).unwrap();
        }
        let code_indexer = CodeIndexer::new();
        Self::index_code(&code_file, &code_indexer);
        let code_cache = Arc::new(CodeCache::new(CACHE_RETENTION));
        let _code_cache = code_cache.clone();
        Self {
            writer: Mutex::new(EntryFileWriter::new(code_file.clone(), wrbuf_size)),
            code_indexer,
            code_cache,
            #[cfg(target_os = "linux")]
            uring_worker_manager: uring::WorkerManager::new(
                URING_COUNT,
                &vec![code_file.clone()],
                URING_SIZE,
                move |_: usize, _: i64, data: &[u8]| {
                    let len = EntryBz::get_entry_len(data);
                    let entry_bz: EntryBz<'_> = EntryBz { bz: &data[..len] };
                    let value = entry_bz.value();
                    let mut bytes_mut = BytesMut::with_capacity(value.len());
                    bytes_mut.extend_from_slice(value);
                    let addr = Address::from_slice(entry_bz.key());
                    let bytes = Bytes::from(bytes_mut.freeze());
                    let byte_code = Bytecode::new_raw(bytes);
                    _code_cache.clone().insert(&addr, byte_code);
                },
            ),
            entry_file: code_file,
        }
    }

    fn index_code(code_file: &Arc<EntryFile>, code_indexer: &CodeIndexer) {
        let mut code_file_rd = EntryFileWithPreReader::new(code_file);
        code_file_rd.scan_entries_lite(0, |kh: &[u8], _code_hash, pos, _sn| {
            code_indexer.add_kv(&extract_short_hash(kh), pos);
        });
    }

    fn get_entry_bytes_mut(&self, addr: &Address) -> BytesMut {
        let short_hash = addr_to_short_hash(addr);
        let pos_list = self.code_indexer.for_each_value(&short_hash);
        // pos_list mostly has one value
        for file_pos in pos_list.enumerate() {
            let bytes_mut = self.entry_file.get_bytes_mut(file_pos);
            let entry_bz = EntryBz {
                bz: bytes_mut.as_ref(),
            };
            if entry_bz.key() == addr.as_slice() {
                return bytes_mut;
            }
        }
        panic!("{} code not found", addr);
    }

    fn get_bytes(&self, addr: &Address) -> Bytes {
        let mut bytes_mut = self.get_entry_bytes_mut(addr);
        let entry_bz = EntryBz {
            bz: bytes_mut.as_ref(),
        };
        let (start, len) = entry_bz.get_value_start_and_len();
        bytes_mut.copy_within(start..start + len, 0);
        bytes_mut.truncate(len);
        Bytes::from(bytes_mut.freeze())
    }

    pub fn get_byte_code(&self, addr: &Address) -> Bytecode {
        if let Some(bytes) = self.code_cache.get(addr) {
            return bytes;
        }

        let bytes = self.get_bytes(addr);
        let byte_code = Bytecode::new_raw(bytes);
        self.code_cache.insert(addr, byte_code.clone());

        byte_code
    }

    pub fn append(
        &self,
        addr: &Address,
        byte_code: Bytecode,
        task_id: i64,
    ) -> Result<(), std::io::Error> {
        assert!(!byte_code.is_empty());
        self.code_cache.insert(addr, byte_code.clone());

        let short_hash = addr_to_short_hash(addr);
        let code_hash = byte_code.hash_slow();

        let entry = Entry {
            key: &addr[..],
            value: byte_code.original_byte_slice(),
            next_key_hash: &code_hash[..],
            version: task_id,
            serial_number: 0, // TODO: need count?
        };
        let mut buf = vec![0u8; entry.get_serialized_len(0)];
        entry.dump(&mut buf, &[]);
        let entry_bz = EntryBz { bz: &buf[..] };
        let pos = self.writer.lock().append(&entry_bz)?;
        self.code_indexer.add_kv(&short_hash, pos);
        Ok(())
    }

    pub fn flush(&self) -> std::io::Result<i64> {
        self.code_cache.tick();
        self.writer.lock().flush()?;
        Ok(self.entry_file.size())
    }

    pub fn warmup(&self, short_hash: &[u8; SHORT_HASH_LEN_FOR_TASK_ACCESS_SET]) {
        if self.code_cache.contains(short_hash) {
            return;
        }

        let code_cache = &self.code_cache;
        let pos_list = self.code_indexer.for_each_value(short_hash);

        #[cfg(not(target_os = "linux"))]
        {
            for file_pos in pos_list.enumerate() {
                let mut bytes_mut = self.entry_file.get_bytes_mut(file_pos);
                let entry_bz = EntryBz {
                    bz: bytes_mut.as_ref(),
                };
                let addr = Address::from_slice(entry_bz.key());
                let (start, len) = entry_bz.get_value_start_and_len();
                bytes_mut.copy_within(start..start + len, 0);
                bytes_mut.truncate(len);
                let bytes = Bytes::from(bytes_mut.freeze());
                let byte_code = Bytecode::new_raw(bytes);
                code_cache.insert(&addr, byte_code);
            }
        }

        #[cfg(target_os = "linux")]
        {
            for file_pos in pos_list.enumerate() {
                let _code_cache = code_cache.clone();
                self.uring_worker_manager.add_task(0, file_pos);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::{CodeDB, CodeIndexer};
    use qmdb::entryfile::EntryBz;
    use revm::{
        primitives::{Address, Bytes},
        state::Bytecode,
    };

    #[test]
    fn test_code_indexer() {
        let bt = CodeIndexer::new();
        let key = "key".as_bytes();
        bt.add_kv(key, 0x10);
        let pos_list = bt.for_each_value(key);
        assert_eq!(1, pos_list.len());
        assert_eq!(0x10, pos_list.get(0).unwrap());
    }

    #[test]
    fn test_code_db() {
        let temp_dir = tempfile::Builder::new()
            .prefix("code_db")
            .tempdir()
            .unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        let addr0 = Address::left_padding_from(&[0]);
        let value0 = Bytes::from(vec![0u8; 10]);
        let bytecode0 = Bytecode::new_raw(value0.clone());
        let code_hash0 = bytecode0.hash_slow();

        let addr1 = Address::left_padding_from(&[1]);
        let value1 = Bytes::from(vec![1u8; 100]);
        let bytecode1 = Bytecode::new_raw(value1.clone());
        let code_hash1 = bytecode1.hash_slow();

        {
            let code_db = CodeDB::new(&dir_path, 0, 1024, 1024);
            code_db.append(&addr0, bytecode0, 1).unwrap();
            assert_eq!(code_db.flush().unwrap(), 88);
        }
        {
            // will create indexer from file
            let code_db = CodeDB::new(&dir_path, 104, 1024, 1024);

            code_db.append(&addr1, bytecode1, 2).unwrap();
            assert_eq!(code_db.flush().unwrap(), 264);

            let bytes_mut0 = code_db.get_entry_bytes_mut(&addr0);
            let entry_bz = EntryBz {
                bz: bytes_mut0.as_ref(),
            };
            assert_eq!(value0.as_ref(), entry_bz.value());
            assert_eq!(addr0, entry_bz.key());
            assert_eq!(code_hash0, entry_bz.next_key_hash());
            assert_eq!(0, entry_bz.serial_number());
            assert_eq!(code_db.get_bytes(&addr0), value0);
            assert_eq!(code_db.get_byte_code(&addr0).original_bytes(), value0);

            let bytes_mut1 = code_db.get_entry_bytes_mut(&addr1);
            let entry_bz = EntryBz {
                bz: bytes_mut1.as_ref(),
            };
            assert_eq!(value1.as_ref(), entry_bz.value());
            assert_eq!(addr1, entry_bz.key());
            assert_eq!(code_hash1, entry_bz.next_key_hash());
            assert_eq!(0, entry_bz.serial_number());
            assert_eq!(code_db.get_bytes(&addr1), value1);
            assert_eq!(code_db.get_byte_code(&addr1).original_bytes(), value1);
        }
    }
}
