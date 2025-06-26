use super::readbuf::ReadBuf;
use crate::def::{DEFAULT_ENTRY_SIZE, HPFILE_RANGE, NONCE_SIZE, TAG_SIZE};
use crate::entryfile::entry::EntryBz;
#[cfg(target_os = "linux")]
use crate::utils::bitmap::Bitmap;
use aes_gcm::aead::AeadInPlace;
use aes_gcm::Aes256Gcm;
use byteorder::{ByteOrder, LittleEndian};
use bytes::BytesMut;
use hpfile::{HPFile, PreReader};
#[cfg(target_os = "linux")]
use parking_lot::Mutex;
use std::fs::File;
use std::sync::Arc;

static URING_COUNT_PER_SHARD: usize = 8;

pub struct EntryFile {
    hp_file: HPFile,
    cipher: Option<Aes256Gcm>,
    #[cfg(target_os = "linux")]
    bitmap: Arc<Bitmap>,
    #[cfg(target_os = "linux")]
    uring_list: Arc<[Mutex<io_uring::IoUring>; URING_COUNT_PER_SHARD]>,
    #[cfg(target_os = "linux")]
    buf_list: Arc<[Mutex<Box<[u8]>>; URING_COUNT_PER_SHARD]>,
}

impl EntryFile {
    pub fn new(
        buffer_size: usize,
        segment_size: i64,
        dir_name: String,
        directio: bool,
        cipher: Option<Aes256Gcm>,
    ) -> EntryFile {
        let buf_size = buffer_size as i64;
        EntryFile {
            hp_file: HPFile::new(buf_size, segment_size, dir_name, directio).unwrap(),
            cipher,
            #[cfg(target_os = "linux")]
            bitmap: Arc::new(Bitmap::new(URING_COUNT_PER_SHARD)),
            #[cfg(target_os = "linux")]
            uring_list: Arc::new(std::array::from_fn(|_| {
                Mutex::new(io_uring::IoUring::new(1).unwrap())
            })),
            #[cfg(target_os = "linux")]
            buf_list: Arc::new(std::array::from_fn(|_| {
                Mutex::new(
                    hpfile::direct_io::allocate_aligned_vec(
                        (crate::prefetcher::dioprefetcher::BLK_SIZE * 2) as usize,
                        hpfile::IO_BLK_SIZE,
                    )
                    .into_boxed_slice(),
                )
            })),
        }
    }

    pub fn size(&self) -> i64 {
        self.hp_file.size()
    }

    pub fn size_on_disk(&self) -> i64 {
        self.hp_file.size_on_disk()
    }

    pub fn read_entry_with_pre_reader(
        &self,
        off: i64,
        end: i64,
        buf: &mut Vec<u8>,
        pre_reader: &mut PreReader,
    ) -> std::io::Result<usize> {
        let off = off % HPFILE_RANGE;
        // first we get the exact size
        self.hp_file
            .read_at_with_pre_reader(buf, 5, off, pre_reader)?;
        let size = EntryBz::get_entry_len(&buf[0..5]);
        if end < off + size as i64 {
            return Ok(0);
        }
        if buf.len() < size {
            buf.resize(size, 0);
        }
        // then we copy the exact bytes out from pre-reader
        let res = self
            .hp_file
            .read_at_with_pre_reader(buf, size, off, pre_reader);
        decrypt(&self.cipher, off, &mut buf[..size]);
        res
    }

    #[cfg(target_os = "linux")]
    pub fn read_entry_with_uring(&self, file_pos: i64, buf: &mut ReadBuf) -> bool {
        use crate::prefetcher::JobManager;

        let idx_opt = self.bitmap.try_acquire();
        if idx_opt.is_none() {
            println!("No available uring slot");
            return false;
        }
        let idx = idx_opt.unwrap();
        let mut uring_lock = self.uring_list[idx].try_lock().unwrap();
        let mut buf_lock = self.buf_list[idx].try_lock().unwrap();
        let (file, offset) = self.get_file_and_pos(file_pos);
        let read_e = JobManager::get_read_e(&file.0, offset, buf_lock.as_mut(), 0);
        unsafe {
            uring_lock.submission().push(&read_e).unwrap();
        }
        uring_lock.submit_and_wait(1).unwrap();
        let cqe = uring_lock.completion().next().unwrap();
        assert_eq!(cqe.user_data(), 0);
        let n = cqe.result();
        if n <= 0 {
            panic!("read error: {}", std::io::Error::from_raw_os_error(-n));
        }
        let _offset = (offset % crate::prefetcher::dioprefetcher::BLK_SIZE) as usize;
        let len = EntryBz::get_entry_len(&buf_lock[_offset..]) as i64;
        if len == 0 {
            panic!("filepos {} is empty", file_pos);
        }
        buf.initialize_from(&buf_lock[_offset.._offset + len as usize]);
        drop(uring_lock);
        drop(buf_lock);
        self.bitmap.release(idx);
        true
    }

    pub fn read_entry(&self, file_pos: i64, buf: &mut ReadBuf) {
        assert!(buf.is_empty(), "ReadBuf should be empty");
        buf.resize(DEFAULT_ENTRY_SIZE);
        let size = self._read_entry(file_pos, buf.as_mut_slice());
        buf.resize(size);
        if DEFAULT_ENTRY_SIZE >= size {
            return;
        }

        self._read_entry(file_pos, buf.as_mut_slice());
    }

    fn _read_entry(&self, off: i64, buf: &mut [u8]) -> usize {
        let off = off % HPFILE_RANGE;
        let len = buf.len();
        self.hp_file.read_at(&mut buf[..len], off).unwrap();
        let size = EntryBz::get_entry_len(&buf[0..5]);
        if size > len {
            return size;
        }
        decrypt(&self.cipher, off, &mut buf[..size]);

        if self.cipher.is_some() {
            buf[size - TAG_SIZE..].fill(0); // revert the tag to 0
        }
        size
    }

    pub fn get_bytes_mut(&self, file_pos: i64) -> BytesMut {
        let mut arr = [0u8; 5];
        let size0 = self._read_entry(file_pos, arr.as_mut_slice());
        let mut buf = BytesMut::with_capacity(size0);
        buf.resize(size0, 0);
        let size1 = self._read_entry(file_pos, buf.as_mut());
        assert!(
            size1 == size0,
            "file_pos {} {} != {}",
            file_pos,
            size0,
            size1
        );
        buf
    }

    fn append(&self, e: &EntryBz, tmp: &mut Vec<u8>, buffer: &mut Vec<u8>) -> std::io::Result<i64> {
        if self.cipher.is_none() {
            return self.hp_file.append(e.bz, buffer);
        }
        tmp.clear();
        tmp.extend_from_slice(e.bz);
        let cipher = self.cipher.as_ref().unwrap();
        let mut nonce_arr = [0u8; NONCE_SIZE];
        LittleEndian::write_i64(&mut nonce_arr[..8], self.hp_file.size());
        //encrypt the part after 5 size-bytesE
        let bz = &mut tmp[5..e.payload_len()];
        match cipher.encrypt_in_place_detached(&nonce_arr.into(), b"", bz) {
            Err(err) => panic!("{}", err),
            Ok(tag) => {
                // overwrite tag placeholder with real tag
                let tag_start = tmp.len() - TAG_SIZE;
                tmp[tag_start..].copy_from_slice(tag.as_slice());
            }
        };
        self.hp_file.append(&tmp[..], buffer)
    }

    pub fn get_file_and_pos(&self, offset: i64) -> (Arc<(File, bool)>, i64) {
        self.hp_file.get_file_and_pos(offset)
    }

    pub fn truncate(&self, size: i64) -> std::io::Result<()> {
        self.hp_file.truncate(size)
    }

    pub fn close(&self) {
        self.hp_file.close();
    }

    pub fn prune_head(&self, off: i64) -> std::io::Result<()> {
        self.hp_file.prune_head(off)
    }

    pub fn read_range(&self, buf: &mut [u8], offset: i64) -> std::io::Result<()> {
        assert!(self.cipher.is_none());
        self.hp_file.read_range(buf, offset)
    }
}

fn decrypt(cipher: &Option<Aes256Gcm>, nonce: i64, bz: &mut [u8]) {
    if cipher.is_none() {
        return;
    }
    let cipher = (*cipher).as_ref().unwrap();
    let mut nonce_arr = [0u8; NONCE_SIZE];
    LittleEndian::write_i64(&mut nonce_arr[..8], nonce);

    let entry = EntryBz { bz: &bz[..] };
    let tag_start = bz.len() - TAG_SIZE;
    let mut tag = [0u8; TAG_SIZE];
    tag[..].copy_from_slice(&bz[tag_start..]);

    let payload_len = entry.payload_len();
    let enc = &mut bz[5..payload_len];

    if let Err(e) = cipher.decrypt_in_place_detached(&nonce_arr.into(), b"", enc, &tag.into()) {
        panic!("{:?}", e)
    };
}

pub struct EntryFileWithPreReader {
    entry_file: Arc<EntryFile>,
    pre_reader: PreReader,
}

impl EntryFileWithPreReader {
    pub fn new(ef: &Arc<EntryFile>) -> Self {
        Self {
            entry_file: ef.clone(),
            pre_reader: PreReader::new(),
        }
    }

    pub fn read_entry(&mut self, off: i64, end: i64, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        self.entry_file
            .read_entry_with_pre_reader(off, end, buf, &mut self.pre_reader)
    }

    pub fn scan_entries_lite<F>(&mut self, start_pos: i64, mut access: F)
    where
        F: FnMut(&[u8], &[u8], i64, u64),
    {
        let mut pos = start_pos;
        // let mut last_pos = start_pos;
        let size = self.entry_file.size();
        // let total = size - start_pos;
        // let step = total / 20;
        let mut buf = Vec::with_capacity(DEFAULT_ENTRY_SIZE);

        while pos < size {
            /* debug print
            if (pos - start_pos) / step != (last_pos - start_pos) / step {
                println!(
                    "ScanEntriesLite {:.2} {}/{}",
                    (pos - start_pos) as f64 / total as f64,
                    pos - start_pos,
                    total
                );
            }
            */
            // last_pos = pos;
            let read_len = self.read_entry(pos, size, &mut buf).unwrap();
            if read_len == 0 {
                panic!("Met file end when reading at {}", pos);
            }
            let entry = EntryBz {
                bz: &buf[..read_len],
            };
            let kh = entry.key_hash();
            access(&kh[..], entry.next_key_hash(), pos, entry.serial_number());
            pos += entry.len() as i64;
        }
    }
}

pub struct EntryFileWriter {
    pub entry_file: Arc<EntryFile>,
    wrbuf: Vec<u8>,
    scratchpad: Vec<u8>,
}

impl EntryFileWriter {
    pub fn new(entry_file: Arc<EntryFile>, buffer_size: usize) -> EntryFileWriter {
        EntryFileWriter {
            entry_file,
            wrbuf: Vec::with_capacity(buffer_size),
            scratchpad: Vec::new(),
        }
    }

    pub fn temp_clone(&self) -> EntryFileWriter {
        EntryFileWriter {
            entry_file: self.entry_file.clone(),
            wrbuf: Vec::with_capacity(0),
            scratchpad: Vec::with_capacity(0),
        }
    }

    pub fn append(&mut self, e: &EntryBz) -> std::io::Result<i64> {
        self.entry_file
            .append(e, &mut self.scratchpad, &mut self.wrbuf)
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.entry_file.hp_file.flush(&mut self.wrbuf, false)
    }
}

#[cfg(test)]
mod entry_file_tests {

    use super::*;
    use crate::def::LEAF_COUNT_IN_TWIG;
    use crate::entryfile::helpers::create_cipher;
    use crate::entryfile::{entry::entry_to_bytes, Entry};

    fn pad32(bz: &[u8]) -> [u8; 32] {
        let mut res = [0; 32];
        let l = bz.len();
        let mut i = 0;
        while i < l {
            res[i] = bz[i];
            i += 1;
        }
        res
    }

    fn make_entries(next_key_hashes: &[[u8; 32]; 3]) -> Box<[Entry]> {
        let e0 = Entry {
            key: "Key0Key0Key0Key0Key0Key0Key0Key0Key0".as_bytes(),
            value: "Value0Value0Value0Value0Value0Value0".as_bytes(),
            next_key_hash: next_key_hashes[0].as_slice(),
            version: 0,
            serial_number: 0,
        };
        let e1 = Entry {
            key: "Key1Key ILOVEYOU 1Key1Key1".as_bytes(),
            value: "Value1Value1".as_bytes(),
            next_key_hash: next_key_hashes[1].as_slice(),
            version: 10,
            serial_number: 1,
        };
        let e2 = Entry {
            key: "Key2Key2Key2 ILOVEYOU Key2".as_bytes(),
            value: "Value2 ILOVEYOU Value2".as_bytes(),
            next_key_hash: next_key_hashes[2].as_slice(),
            version: 20,
            serial_number: 2,
        };
        let null_entry = Entry {
            key: &[],
            value: &[],
            next_key_hash: &[0; 32],
            version: -2,
            serial_number: u64::MAX,
        };
        Box::new([e0, e1, e2, null_entry])
    }

    fn equal_entry(e: &Entry, s_list: &[u64], entry_bz: &EntryBz) {
        assert_eq!(e.key, entry_bz.key());
        assert_eq!(e.value, entry_bz.value());
        assert_eq!(e.next_key_hash, entry_bz.next_key_hash());
        assert_eq!(e.version, entry_bz.version());
        assert_eq!(e.serial_number, entry_bz.serial_number());
        assert_eq!(s_list.len(), entry_bz.dsn_count());
        let mut i = 0;
        while i < s_list.len() {
            assert_eq!(s_list[i], entry_bz.get_deactived_sn(i));
            i += 1;
        }
    }

    #[test]
    fn entry_file() -> std::io::Result<()> {
        let hashes: [[u8; 32]; 3] = [
            pad32("NextKey0".as_bytes()),
            pad32("NextKey1".as_bytes()),
            pad32("NextKey2".as_bytes()),
        ];
        let entries = make_entries(&hashes);
        let temp_dir = tempfile::Builder::new().prefix("entryF").tempdir().unwrap();
        let dir_path = temp_dir.path().to_str().unwrap().to_string();

        let d_snl0 = [1, 2, 3, 4];
        let d_snl1 = [5];
        let d_snl2 = [];
        let d_snl3 = [10, 1];

        let cipher = create_cipher();
        let ef = EntryFile::new(8 * 1024, 128 * 1024, dir_path.clone(), true, cipher.clone());
        let mut total_len = ((crate::def::ENTRY_FIXED_LENGTH
            + &entries[0].key.len()
            + &entries[0].value.len()
            + 7)
            / 8)
            * 8
            + &d_snl0.len() * 8;
        if cfg!(feature = "tee_cipher") {
            total_len += TAG_SIZE;
        }
        let mut bz0 = vec![0; total_len];
        entry_to_bytes(&entries[0], d_snl0.as_slice(), &mut bz0);
        let mut buffer = vec![];
        let mut scratchpad = vec![];
        let pos0 = ef.append(&EntryBz { bz: &bz0 }, &mut scratchpad, &mut buffer)?;
        let mut total_len = ((crate::def::ENTRY_FIXED_LENGTH
            + &entries[1].key.len()
            + &entries[1].value.len()
            + 7)
            / 8)
            * 8
            + &d_snl1.len() * 8;
        if cfg!(feature = "tee_cipher") {
            total_len += TAG_SIZE;
        }
        let mut bz1 = vec![0; total_len];
        entry_to_bytes(&entries[1], d_snl1.as_slice(), &mut bz1);
        let pos1 = ef.append(&EntryBz { bz: &bz1 }, &mut scratchpad, &mut buffer)?;
        let mut total_len = ((crate::def::ENTRY_FIXED_LENGTH
            + &entries[2].key.len()
            + &entries[2].value.len()
            + 7)
            / 8)
            * 8
            + &d_snl2.len() * 8;
        if cfg!(feature = "tee_cipher") {
            total_len += TAG_SIZE;
        }
        let mut bz2 = vec![0; total_len];
        entry_to_bytes(&entries[2], d_snl2.as_slice(), &mut bz2);
        let pos2 = ef.append(&EntryBz { bz: &bz2 }, &mut scratchpad, &mut buffer)?;
        let mut total_len = ((crate::def::ENTRY_FIXED_LENGTH
            + &entries[3].key.len()
            + &entries[3].value.len()
            + 7)
            / 8)
            * 8
            + &d_snl3.len() * 8;
        if cfg!(feature = "tee_cipher") {
            total_len += TAG_SIZE;
        }
        let mut bz3 = vec![0; total_len];
        entry_to_bytes(&entries[3], d_snl3.as_slice(), &mut bz3);
        let pos3 = ef.append(&EntryBz { bz: &bz3 }, &mut scratchpad, &mut buffer)?;

        let mut i = 0;
        while i < LEAF_COUNT_IN_TWIG {
            ef.append(&EntryBz { bz: &bz0 }, &mut scratchpad, &mut buffer)?;
            ef.append(&EntryBz { bz: &bz1 }, &mut scratchpad, &mut buffer)?;
            ef.append(&EntryBz { bz: &bz2 }, &mut scratchpad, &mut buffer)?;
            ef.append(&EntryBz { bz: &bz3 }, &mut scratchpad, &mut buffer)?;
            i += 4;
        }
        ef.hp_file.flush(&mut buffer, false)?;
        ef.close();

        let ef = EntryFile::new(8 * 1024, 128 * 1024, dir_path, true, cipher.clone());
        let mut buf = ReadBuf::new();
        ef.read_entry(pos0, &mut buf);
        equal_entry(&entries[0], &d_snl0, &buf.as_entry_bz());
        assert_eq!(pos1, pos0 + buf.as_entry_bz().len() as i64);

        let mut buf = ReadBuf::new();
        ef.read_entry(pos1, &mut buf);
        let entry_bz = buf.as_entry_bz();
        equal_entry(&entries[1], &d_snl1, &entry_bz);
        assert_eq!(pos2, pos1 + entry_bz.len() as i64);

        let mut buf = ReadBuf::new();
        ef.read_entry(pos2, &mut buf);
        let entry_bz = buf.as_entry_bz();
        equal_entry(&entries[2], &d_snl2, &entry_bz);
        assert_eq!(pos3, pos2 + entry_bz.len() as i64);

        let mut buf = ReadBuf::new();
        ef.read_entry(pos3, &mut buf);
        let entry_bz = buf.as_entry_bz();
        equal_entry(&entries[3], &d_snl3, &entry_bz);

        ef.close();
        Ok(())
    }
}
