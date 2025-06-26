use crate::def::{
    ENTRY_BASE_LENGTH, ENTRY_FIXED_LENGTH, NULL_ENTRY_VERSION, SHARD_COUNT, TAG_SIZE,
};
use crate::utils::hasher::{self, Hash32};
use byteorder::{BigEndian, ByteOrder, LittleEndian};

#[derive(Debug)]
pub struct Entry<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub next_key_hash: &'a [u8],
    pub version: i64,
    pub serial_number: u64,
}

pub struct EntryBz<'a> {
    pub bz: &'a [u8],
}

impl<'a> Entry<'a> {
    pub fn from_bz(e: &'a EntryBz) -> Entry<'a> {
        Self {
            key: e.key(),
            value: e.value(),
            next_key_hash: e.next_key_hash(),
            version: e.version(),
            serial_number: e.serial_number(),
        }
    }

    pub fn get_serialized_len(&self, deactived_sn_count: usize) -> usize {
        let length = ENTRY_BASE_LENGTH + self.key.len() + self.value.len();
        ((length + 7) / 8) * 8 + deactived_sn_count * 8
    }

    // 1B KeyLength
    // 3B-valueLength
    // 1B DeactivedSNList length
    // ----------- encryption start
    // Key
    // Value
    // 32B NextKeyHash
    // 8B Height
    // 8B LastHeight
    // 8B SerialNumber
    // DeactivedSerialNumList (list of 8B-int)
    // ----------- encryption end
    // padding-zero-bytes
    // AES-GCM tag placeholder (if feature = "tee_cipher")
    pub fn dump(&self, b: &mut [u8], deactived_sn_list: &[u64]) -> usize {
        if b.len() < self.get_serialized_len(deactived_sn_list.len()) {
            panic!("Not enough space for dumping");
        }
        let first32 = (self.value.len() * 256 + self.key.len()) as u32;
        b[4] = deactived_sn_list.len() as u8;
        LittleEndian::write_u32(&mut b[..4], first32);
        let mut i = 5;
        b[i..i + self.key.len()].copy_from_slice(self.key);
        i += self.key.len();
        b[i..i + self.value.len()].copy_from_slice(self.value);
        i += self.value.len();

        if self.next_key_hash.len() != 32 {
            panic!("NextKeyHash is not 32-byte");
        }
        b[i..i + 32].copy_from_slice(self.next_key_hash);
        i += 32;
        LittleEndian::write_i64(&mut b[i..i + 8], self.version);
        i += 8;
        LittleEndian::write_u64(&mut b[i..i + 8], self.serial_number);
        i += 8;

        for &sn in deactived_sn_list {
            LittleEndian::write_u64(&mut b[i..i + 8], sn);
            i += 8;
        }

        while i % 8 != 0 {
            b[i] = 0;
            i += 1;
        }
        if cfg!(feature = "tee_cipher") {
            i += TAG_SIZE;
        }
        i
    }

    pub fn key_hash(&self) -> Hash32 {
        if self.value.is_empty() {
            let mut res: Hash32 = [0; 32];
            res[0] = self.key[0]; // first byte of key
            res[1] = self.key[1]; // second byte of key
            return res;
        }
        hasher::hash(self.key)
    }
}

pub fn sentry_entry(shard_id: usize, sn: u64, bz: &mut [u8]) -> EntryBz {
    if shard_id >= SHARD_COUNT || (sn as usize) >= (1 << 16) / SHARD_COUNT {
        panic!("SentryEntry Overflow");
    }
    let first16 = ((shard_id * (1 << 16) / SHARD_COUNT) | (sn as usize)) as u16;
    let mut key: [u8; 32] = [0; 32];
    BigEndian::write_u16(&mut key[0..2], first16);
    let mut next_key_hash: [u8; 32];
    if first16 == 0xFFFF {
        next_key_hash = [0xFF; 32];
    } else {
        next_key_hash = [0; 32];
        BigEndian::write_u16(&mut next_key_hash[0..2], first16 + 1);
    }
    let e = Entry {
        key: &key[..],
        value: &[] as &[u8],
        next_key_hash: &next_key_hash[..],
        version: 0,
        serial_number: sn,
    };
    let i = e.dump(bz, &[] as &[u64]);
    EntryBz { bz: &bz[..i] }
}

pub fn sentry_entry_key_hash(key: &[u8]) -> Hash32 {
    let mut res: Hash32 = [0; 32];
    res[0] = key[0]; // first byte of key
    res[1] = key[1]; // second byte of key
    res
}

pub fn null_entry(bz: &mut [u8]) -> EntryBz {
    let next_key_hash: [u8; 32] = [0; 32];
    let e = Entry {
        key: &[] as &[u8],
        value: &[] as &[u8],
        next_key_hash: &next_key_hash[..],
        version: NULL_ENTRY_VERSION,
        serial_number: u64::MAX,
    };
    let i = e.dump(bz, &[] as &[u64]);
    EntryBz { bz: &bz[..i] }
}

// only for test
pub fn entry_to_bytes<'a>(
    e: &'a Entry<'a>,
    deactived_sn_list: &'a [u64],
    bz: &'a mut [u8],
) -> EntryBz<'a> {
    let total_len = e.get_serialized_len(deactived_sn_list.len());
    e.dump(&mut bz[..], deactived_sn_list);
    EntryBz {
        bz: &bz[..total_len],
    }
}

pub fn get_kv_len(len_bytes: &[u8]) -> (usize, usize) {
    let first32 = LittleEndian::read_u32(&len_bytes[..4]);
    let key_len = (first32 & 0xff) as usize;
    let value_len = (first32 >> 8) as usize;
    (key_len, value_len)
}

impl<'a> EntryBz<'a> {
    pub fn get_entry_len(len_bytes: &[u8]) -> usize {
        let (key_len, value_len) = get_kv_len(len_bytes);
        let dsn_count = len_bytes[4] as usize;
        ((ENTRY_BASE_LENGTH + key_len + value_len + 7) / 8) * 8 + dsn_count * 8
    }

    pub fn len(&self) -> usize {
        self.bz.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bz.is_empty()
    }

    pub fn payload_len(&self) -> usize {
        let (key_len, value_len) = get_kv_len(self.bz);
        let dsn_count = self.bz[4] as usize;
        ENTRY_FIXED_LENGTH + key_len + value_len + dsn_count * 8
    }

    pub fn hash(&self) -> Hash32 {
        hasher::hash(&self.bz[..self.payload_len()])
    }

    pub fn value(&self) -> &[u8] {
        let (start, len) = self.get_value_start_and_len();
        &self.bz[start..start + len]
    }

    pub fn get_value_start_and_len(&self) -> (usize, usize) {
        let (key_len, value_len) = get_kv_len(self.bz);
        let start = 5 + key_len;
        (start, value_len)
    }

    pub fn key(&self) -> &[u8] {
        let key_len = self.bz[0] as usize;
        &self.bz[5..5 + key_len]
    }

    pub fn key_hash(&self) -> Hash32 {
        let key = self.key();

        // nullentry: key is empty
        if key.is_empty() {
            return [0; 32];
        }

        // sentry entry: value is empty
        if self.value().is_empty() {
            return sentry_entry_key_hash(key);
        }

        hasher::hash(key)
    }

    fn next_key_hash_start(&self) -> usize {
        let (key_len, value_len) = get_kv_len(&self.bz[0..4]);
        5 + key_len + value_len
    }

    pub fn next_key_hash(&self) -> &[u8] {
        let start = self.next_key_hash_start();
        &self.bz[start..start + 32]
    }

    pub fn version(&self) -> i64 {
        let start = self.next_key_hash_start() + 32;
        read_u64_with_alignment(self.bz, start) as i64
    }

    pub fn serial_number(&self) -> u64 {
        let start = self.next_key_hash_start() + 40;
        read_u64_with_alignment(self.bz, start)
    }

    pub fn dsn_count(&self) -> usize {
        self.bz[4] as usize
    }

    pub fn get_deactived_sn(&self, n: usize) -> u64 {
        let dsn_cnt = self.dsn_count();
        assert!(n < dsn_cnt);
        let (key_len, value_len) = get_kv_len(self.bz);
        let start = ENTRY_FIXED_LENGTH + key_len + value_len + n * 8;
        read_u64_with_alignment(self.bz, start)
    }

    pub fn dsn_iter(&'a self) -> DSNIter<'a> {
        DSNIter {
            e: self,
            count: self.dsn_count(),
            idx: 0,
        }
    }
}

fn read_u64_with_alignment(buf: &[u8], offset: usize) -> u64 {
    #[cfg(not(target_os = "zkvm"))]
    {
        LittleEndian::read_u64(&buf[offset..offset + 8])
    }

    #[cfg(target_os = "zkvm")]
    {
        let align_offset = (&buf[offset] as *const u8 as usize) % 8;
        if align_offset == 0 {
            LittleEndian::read_u64(&buf[offset..offset + 8])
        } else {
            let base = offset & !0x7;
            let head = LittleEndian::read_u64(&buf[base..base + 8]);
            let tail = LittleEndian::read_u64(&buf[base + 8..base + 16]);
            let shift = (offset - base) * 8;
            let low = head >> shift;
            let high = tail << (64 - shift);
            low | high
        }
    }
}

pub struct DSNIter<'a> {
    e: &'a EntryBz<'a>,
    count: usize,
    idx: usize,
}

impl Iterator for DSNIter<'_> {
    type Item = (usize, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.count {
            return None;
        }
        let sn = self.e.get_deactived_sn(self.idx);
        let idx = self.idx;
        self.idx += 1;
        Some((idx, sn))
    }
}

pub fn entry_to_vec(e: &Entry, deactived_sn_list: &[u64]) -> Vec<u8> {
    let total_len = ((ENTRY_BASE_LENGTH + e.key.len() + e.value.len() + 7) / 8) * 8
        + deactived_sn_list.len() * 8;
    let mut v = vec![0; total_len];
    e.dump(&mut v[..], deactived_sn_list);
    v
}

pub fn entry_equal(bz: &[u8], e: &Entry, deactived_sn_list: &[u64]) -> bool {
    &entry_to_vec(e, deactived_sn_list)[..] == bz
}

#[cfg(test)]
mod entry_bz_tests {
    use super::*;

    #[test]
    fn test_dump() {
        let key = "key".as_bytes();
        let val = "value".as_bytes();
        let next_key_hash: Hash32 = [0xab; 32];
        let deactived_sn_list: [u64; 4] = [0xf1, 0xf2, 0xf3, 0xf4];

        let entry = Entry {
            key,
            value: val,
            next_key_hash: &next_key_hash,
            version: 12345,
            serial_number: 99999,
        };

        let mut buf: [u8; 1024] = [0; 1024];
        let n = entry.dump(&mut buf, &deactived_sn_list);
        let mut len = 96;
        if cfg!(feature = "tee_cipher") {
            len += TAG_SIZE;
        }
        assert_eq!(len, n);

        #[rustfmt::skip]
        assert_eq!(
            hex::encode(&buf[0..n]),
            [
                "03",         // key len
                "050000",     // val len
                "04",         // deactived_sn_list len
                "6b6579",     // key
                "76616c7565", // val
                "abababababababababababababababababababababababababababababababab", // next key hash
                "3930000000000000", // version
                "9f86010000000000", // serial number
                "f100000000000000", // deactived_sn_list
                "f200000000000000",
                "f300000000000000",
                "f400000000000000",
                "000000",     // padding
                #[cfg(feature = "tee_cipher")]
                "00000000000000000000000000000000" // AES-GCM tag placeholder
        ].join(""),
        );
    }
}
