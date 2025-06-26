use byteorder::{BigEndian, ByteOrder};
use dashmap::DashMap;
use qmdb::def::{BYTES_CACHE_SHARD_COUNT, OP_CREATE, OP_DELETE, OP_WRITE};
use qmdb::entryfile::readbuf::ReadBuf;
use qmdb::utils::bytescache::BytesCache;
use qmdb::utils::changeset::ChangeSet;
use revm::primitives::Address;
use revm::state::Bytecode;

pub type CodeMap = DashMap<Address, Bytecode>;
type ValueMap = BytesCache<[u8; 32]>;

pub struct StateCache {
    // used only for post-update of world state
    // entrycache is used for warmup of world state
    value_map: ValueMap,
    old_value_map: ValueMap,
}

impl Default for StateCache {
    fn default() -> Self {
        Self::new()
    }
}

impl StateCache {
    pub fn new() -> Self {
        Self {
            value_map: BytesCache::new(),
            old_value_map: BytesCache::new(),
        }
    }

    pub fn insert_data(&self, key_hash: &[u8; 32], value: &[u8], old_value: &[u8]) {
        let idx = BigEndian::read_u64(&key_hash[..8]) as usize;
        let idx = idx % BYTES_CACHE_SHARD_COUNT;
        self.value_map.insert(key_hash, idx, value);
        self.old_value_map
            .insert_if_missing(key_hash, idx, old_value);
    }

    pub fn lookup_value(&self, key_hash: &[u8; 32], data: &mut ReadBuf) {
        let idx = BigEndian::read_u64(&key_hash[..8]) as usize;
        let idx = idx % BYTES_CACHE_SHARD_COUNT;
        self.value_map.lookup(key_hash, idx, |bz| {
            data.initialize_from(bz);
        })
    }

    pub fn lookup_old_value(&self, key_hash: &[u8; 32], data: &mut ReadBuf) {
        let idx = BigEndian::read_u64(&key_hash[..8]) as usize;
        let idx = idx % BYTES_CACHE_SHARD_COUNT;
        self.old_value_map.lookup(key_hash, idx, |bz| {
            let _n = usize::min(bz.len(), data.len());
            data.initialize_from(bz);
        })
    }

    pub fn apply_change(&self, change_set: &ChangeSet) {
        change_set.apply_op_in_range(|op, key_hash, _k, value, old_value, _rec| {
            if op == OP_CREATE || op == OP_WRITE || op == OP_DELETE {
                self.insert_data(key_hash, value, old_value);
            }
        });
    }
}

#[cfg(test)]
mod tests {

    use qmdb::def::OP_READ;

    use super::*;



    #[test]
    fn test_data() {
        let test_data = [
            (&[0x01u8; 32], "aaa".as_bytes()),
            (&[0x02u8; 32], "bbb".as_bytes()),
            (&[0x03u8; 32], "ccc".as_bytes()),
            (&[0x04u8; 32], "ddd".as_bytes()),
        ];

        let cache = StateCache::new();
        for (k, v) in test_data {
            cache.insert_data(k, v, &k[..3]);
        }

        for (k, v) in test_data {
            let mut buf0 = ReadBuf::new();
            cache.lookup_value(k, &mut buf0);
            assert!(!buf0.is_empty());
            assert_eq!(v, buf0.as_slice());
            let mut buf1 = ReadBuf::new();
            cache.lookup_old_value(k, &mut buf1);
            assert!(!buf1.is_empty());
            assert_eq!(&k[..3], buf1.as_slice());
        }

        let k5 = &[0x05u8; 32];
        let mut buf = ReadBuf::new();
        cache.lookup_value(k5, &mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_apply_change() {
        let test_data = [
            (OP_CREATE, &[0x11u8; 32], &[0xaau8; 32]),
            (OP_WRITE, &[0x22u8; 32], &[0xbbu8; 32]),
            (OP_DELETE, &[0x33u8; 32], &[0xccu8; 32]),
            (OP_READ, &[0x44u8; 32], &[0xddu8; 32]),
        ];

        let mut cs = ChangeSet::new();
        for (op, kh, v) in test_data {
            cs.add_op(op, 1, kh, kh, v, Option::None);
        }
        cs.sort();

        let cache = StateCache::new();
        cache.apply_change(&cs);

        let mut buf = ReadBuf::new();
        for (op, kh, v) in test_data {
            buf.clear();
            if op == OP_READ {
                cache.lookup_value(kh, &mut buf);
                assert!(buf.is_empty());
            } else {
                cache.lookup_value(kh, &mut buf);
                assert!(!buf.is_empty());
                assert_eq!(v, buf.as_slice());
            }
        }
    }
}
