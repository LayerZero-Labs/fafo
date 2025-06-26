//! # Head-prunable file
//!
//! Normal files can not be pruned\(truncated\) from the beginning to some middle position.
//! A `HPFile` use a sequence of small files to simulate one big virtual file. Thus, pruning
//! from the beginning is to delete the first several small files.
//!
//! A `HPFile` can only be read and appended. Any byteslice which was written to it is
//! immutable.
//!
//! To append a new byteslice into a `HPFile`, use the `append` function, which will return
//! the start position of this byteslice. Later, just pass this start position to `read_at`
//! for reading this byteslice out. The position passed to `read_at` must be the beginning of a
//! byteslice that was written before, instead of its middle. Do NOT try to read the later
//! half (from a middle point to the end) of a byteslice.
//!
//! A `HPFile` can also be truncated: discarding the content from a given position to the
//! end of the file. During trucation, several small files may be removed and one small file
//! may get truncated.
//!
//! A `HPFile` can serve many reader threads. If a reader thread just read random positions,
//! plain `read_at` is enough. If a reader tends to read many adjacent byteslices in sequence, it
//! can take advantage of spatial locality by using `read_at_with_pre_reader`, which uses a
//! `PreReader` to read large chunks of data from file and cache them. Each reader thread can have
//! its own `PreReader`. A `PreReader` cannot be shared by different `HPFile`s.
//!
//! A `HPFile` can serve only one writer thread. The writer thread must own a write buffer that
//! collects small pieces of written data into one big single write to the underlying OS file,
//! to avoid the cost of many syscalls writing the OS file. This write buffer must be provided
//! when calling `append` and `flush`. It is owned by the writer thead, instead of `HPFile`,
//! because we want `HPFile` to be shared between many reader threads.

pub mod file;
pub use std::io::Read;

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use std::{
    fs,
    io::{self, Seek, SeekFrom, Write},
    sync::atomic::{AtomicI64, Ordering},
    sync::Arc,
};

use std::fs::File;

#[cfg(unix)]
use std::os::unix::fs::{FileExt, OpenOptionsExt};

/// A trait for reading at a given offset without affecting the cursor position
pub trait ReadAt {
    /// Reads a number of bytes starting from a given offset.
    ///
    /// Returns the number of bytes read.
    ///
    /// The offset is relative to the start of the file and thus independent from the current cursor.
    /// The current file cursor is not affected by this function.
    ///
    /// Note that similar to File::read, it is not an error to return with a short read.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
}

#[cfg(target_os = "zkvm")]
impl ReadAt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        panic!("read_at is not supported in zkvm target");
        // let mut temp_file = self.try_clone()?;
        // temp_file.seek(SeekFrom::Start(offset))?;
        // temp_file.read(buf)
    }
}

const PRE_READ_BUF_SIZE: usize = 512 * 1024;
pub const IO_BLK_SIZE: usize = 512;

#[cfg(target_os = "linux")]
const DIRECT: i32 = libc::O_DIRECT;
#[cfg(not(target_os = "linux"))]
const DIRECT: i32 = i32::MIN; //will cause error

type FileMap = DashMap<
    i64,
    Arc<(
        File,
        bool, /* Whether the file is written only used with directio */
    )>,
>;

/// Head-prunable file
#[derive(Debug)]
pub struct HPFile {
    dir_name: String,  // where we store the small files
    segment_size: i64, // the size of each small file
    buffer_size: i64,  // the write buffer's size
    file_map: FileMap,
    largest_id: AtomicI64,
    latest_file_size: AtomicI64,
    file_size: AtomicI64,
    file_size_on_disk: AtomicI64,
    directio: bool,
}

impl HPFile {
    /// Create a `HPFile` with a directory. If this directory was used by an old HPFile, the old
    /// HPFile must have the same `segment_size` as this one.
    ///
    /// # Parameters
    ///
    /// - `wr_buf_size`: The write buffer used in `append` will not exceed this size
    /// - `segment_size`: The target size of the small files
    /// - `dir_name`: The name of the directory used to store the small files
    /// - `directio`: Enable directio for readonly files (only use this feature on Linux)
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    ///
    /// - `Ok`: A successfully initialized `HPFile`
    /// - `Err`: Encounted some file system error.
    ///
    pub fn new(
        wr_buf_size: i64,
        segment_size: i64,
        dir_name: String,
        directio: bool,
    ) -> Result<HPFile> {
        if segment_size % wr_buf_size != 0 {
            return Err(anyhow!(
                "Invalid segmentSize:{} writeBufferSize:{}",
                segment_size,
                wr_buf_size
            ));
        }

        if directio && cfg!(not(target_os = "linux")) {
            eprintln!("Directio is only supported on Linux");
        }

        let (id_list, largest_id) = Self::get_file_ids(&dir_name, segment_size)?;
        let (file_map, latest_file_size) =
            Self::load_file_map(&dir_name, segment_size, id_list, largest_id, directio)?;

        let file_size = largest_id * segment_size + latest_file_size;
        Ok(HPFile {
            dir_name: dir_name.clone(),
            segment_size,
            buffer_size: wr_buf_size,
            file_map,
            largest_id: AtomicI64::new(largest_id),
            latest_file_size: AtomicI64::new(latest_file_size),
            file_size: AtomicI64::new(file_size),
            file_size_on_disk: AtomicI64::new(file_size),
            directio,
        })
    }

    /// Create an empty `HPFile` that has no function and can only be used as placeholder.
    pub fn empty() -> HPFile {
        HPFile {
            dir_name: "".to_owned(),
            segment_size: 0,
            buffer_size: 0,
            file_map: DashMap::with_capacity(0),
            largest_id: AtomicI64::new(0),
            latest_file_size: AtomicI64::new(0),
            file_size: AtomicI64::new(0),
            file_size_on_disk: AtomicI64::new(0),
            directio: false,
        }
    }

    /// Returns whether this `HPFile` is empty.
    pub fn is_empty(&self) -> bool {
        self.segment_size == 0
    }

    fn get_file_ids(dir_name: &str, segment_size: i64) -> Result<(Vec<i64>, i64)> {
        let mut largest_id = 0;
        let mut id_list = Vec::new();

        for entry in fs::read_dir(dir_name)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                continue;
            }

            let file_name = entry.file_name().to_string_lossy().to_string();
            let id = Self::parse_filename(segment_size, &file_name)?;
            largest_id = largest_id.max(id);
            id_list.push(id);
        }

        Ok((id_list, largest_id))
    }

    fn parse_filename(segment_size: i64, file_name: &str) -> Result<i64> {
        let parts: Vec<_> = file_name.split("-").collect();
        if parts.len() != 2 {
            return Err(anyhow!(
                "{} does not match the pattern 'FileId-segmentSize'",
                file_name
            ));
        }

        let id: i64 = parts[0].parse()?;
        let size: i64 = parts[1].parse()?;

        if segment_size != size {
            return Err(anyhow!("Invalid Size! {}!={}", size, segment_size));
        }

        Ok(id)
    }

    fn load_file_map(
        dir_name: &str,
        segment_size: i64,
        id_list: Vec<i64>,
        largest_id: i64,
        directio: bool,
    ) -> Result<(FileMap, i64)> {
        let file_map = DashMap::new();
        let mut latest_file_size = 0;

        for &id in &id_list {
            let file_name = format!("{}/{}-{}", dir_name, id, segment_size);
            let mut options = File::options();
            let file_and_ro = if id == largest_id {
                let file = options.read(true).write(true).open(file_name)?;
                latest_file_size = file.metadata()?.len() as i64;
                (file, false)
            } else {
                if directio {
                    #[cfg(target_os = "linux")]
                    options.custom_flags(DIRECT);
                }
                (options.read(true).open(file_name)?, true)
            };
            file_map.insert(id, Arc::new(file_and_ro));
        }

        if id_list.is_empty() {
            let file_name = format!("{}/{}-{}", &dir_name, 0, segment_size);
            let file = File::create_new(file_name)?;
            file_map.insert(0, Arc::new((file, false)));
        }

        Ok((file_map, latest_file_size))
    }

    /// Returns the size of the virtual large file, including the non-flushed bytes
    pub fn size(&self) -> i64 {
        self.file_size.load(Ordering::SeqCst)
    }

    /// Returns the flushed size of the virtual large file
    pub fn size_on_disk(&self) -> i64 {
        self.file_size_on_disk.load(Ordering::SeqCst)
    }

    /// Truncate the file to make it smaller
    ///
    /// # Parameters
    ///
    /// - `size`: the size of the virtual large file after truncation. It must be smaller
    ///           than the original size.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    ///
    /// - `Ok`: It's truncated successfully
    /// - `Err`: Encounted some file system error.
    ///
    pub fn truncate(&self, size: i64) -> io::Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        let mut largest_id = self.largest_id.load(Ordering::SeqCst);

        while size < largest_id * self.segment_size {
            self.file_map.remove(&largest_id);

            #[cfg(unix)]
            {
                let file_name = format!("{}/{}-{}", self.dir_name, largest_id, self.segment_size);
                fs::remove_file(file_name)?;
            }

            self.largest_id.fetch_sub(1, Ordering::SeqCst);
            largest_id -= 1;
        }

        let remaining_size = size - largest_id * self.segment_size;
        let file_name = format!("{}/{}-{}", self.dir_name, largest_id, self.segment_size);
        let mut f = File::options().read(true).write(true).open(file_name)?;
        f.set_len(remaining_size as u64)?;
        f.seek(SeekFrom::End(0))?;

        self.file_map.insert(largest_id, Arc::new((f, false)));
        self.latest_file_size
            .store(remaining_size, Ordering::SeqCst);
        self.file_size.store(size, Ordering::SeqCst);
        self.file_size_on_disk.store(size, Ordering::SeqCst);

        Ok(())
    }

    /// Flush the remained data in `buffer` into file system
    ///
    /// # Parameters
    ///
    /// - `buffer`: the write buffer, which is used by the client to call `append`.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    ///
    /// - `Ok`: It's flushed successfully
    /// - `Err`: Encounted some file system error.
    ///
    pub fn flush(&self, buffer: &mut Vec<u8>, eof: bool) -> io::Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        let largest_id = self.largest_id.load(Ordering::SeqCst);
        let mut opt = self.file_map.get_mut(&largest_id);
        let mut f = &opt.as_mut().unwrap().value().0;
        if !buffer.is_empty() {
            let tail_len = buffer.len() % IO_BLK_SIZE;
            if eof && tail_len != 0 {
                // force the file size aligned with IO_BLK_SIZE
                buffer.resize(buffer.len() + IO_BLK_SIZE - tail_len, 0);
            }
            f.seek(SeekFrom::End(0)).unwrap();
            f.write_all(buffer)?;
            self.file_size_on_disk
                .fetch_add(buffer.len() as i64, Ordering::SeqCst);
            buffer.clear();
        }

        f.sync_all()
    }

    /// Close the opened small files
    pub fn close(&self) {
        self.file_map.clear();
    }

    /// Returns the corresponding file and in-file position given a logical offset. When we
    /// use io_uring to read data from HPFile, the underlying segment files must be exposed.
    ///
    /// # Parameters
    ///
    /// - `offset`: a logical offset of this HPFile
    ///
    /// # Returns
    ///
    /// A tuple. Its first entry is the underlying File and its readonly attribute,
    /// and its sencond entry is the position within this underlying File.
    ///
    pub fn get_file_and_pos(&self, offset: i64) -> (Arc<(File, bool)>, i64) {
        let file_id = offset / self.segment_size;
        let opt = self.file_map.get(&file_id);
        let f = opt.as_ref().unwrap().value();
        (f.clone(), offset % self.segment_size)
    }

    /// Read data from file at `offset` to fill `bz`
    ///
    /// # Parameters
    ///
    /// - `offset`: the start position of a byteslice that was written before
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    ///
    /// - `Ok`: Number of bytes that was filled into `bz`
    /// - `Err`: Encounted some file system error.
    ///
    pub fn read_at(&self, bz: &mut [u8], offset: i64) -> io::Result<usize> {
        let file_id = offset / self.segment_size;
        let pos = offset % self.segment_size;
        let opt = self.file_map.get(&file_id);
        let f = &opt.as_ref().unwrap().value();
        if self.directio && f.1 {
            //readonly file, so we add alignment requirement
            Self::read_at_aligned(&f.0, bz, pos)
        } else {
            f.0.read_at(bz, pos as u64)
        }
    }

    pub fn read_range(&self, buf: &mut [u8], offset: i64) -> io::Result<()> {
        let size = self.file_size_on_disk.load(Ordering::SeqCst);
        let end_offset = offset + buf.len() as i64;
        if end_offset > size as i64 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Read out of range: {} + {} > {}", offset, buf.len(), size),
            ));
        }

        let start_file_id = offset / self.segment_size;
        let end_file_id = end_offset / self.segment_size;
        let mut has_read_size = 0usize;
        for file_id in start_file_id..=end_file_id {
            let opt = self.file_map.get(&file_id);
            if opt.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("File ID {} not found", file_id),
                ));
            }

            let f = &opt.as_ref().unwrap().value();
            let pos = (offset + has_read_size as i64) % self.segment_size;
            let read_len = if file_id == end_file_id {
                end_offset - offset - has_read_size as i64
            } else {
                self.segment_size - pos
            } as usize;
            let size = f.0.read_at(
                &mut buf[has_read_size as usize..has_read_size as usize + read_len],
                pos as u64,
            );
            match size {
                Ok(n) if n < read_len => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "Short read from file ID {}: expected {}, got {}",
                            file_id, read_len, n
                        ),
                    ));
                }
                Ok(_) => {}
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("Failed to read from file ID {}: {}", file_id, e),
                    ));
                }
            }
            has_read_size += read_len;
        }

        Ok(())
    }

    fn read_at_aligned(f: &File, bz: &mut [u8], offset: i64) -> io::Result<usize> {
        if bz.len() > 2 * IO_BLK_SIZE {
            panic!("Cannot read more than two io blocks");
        }
        let off_in_blk = offset % (IO_BLK_SIZE as i64);
        let off_start = offset - off_in_blk;
        let mut buf = [0u8; 3 * IO_BLK_SIZE];
        let buf_start = IO_BLK_SIZE - (buf.as_ptr() as usize % IO_BLK_SIZE);
        let mut buf_end = buf_start + IO_BLK_SIZE;
        if off_in_blk != 0 {
            buf_end += IO_BLK_SIZE; //we must read two blocks
        }
        let buf = &mut buf[buf_start..buf_end];
        if buf.as_ptr() as usize % IO_BLK_SIZE != 0 {
            panic!("Buffer still not aligned!");
        }
        if off_start as usize % IO_BLK_SIZE != 0 {
            panic!("File offset still not aligned!");
        }
        let res = f.read_at(buf, off_start as u64);
        if let Err(e) = res {
            panic!("aligned {}", e);
        }
        if let Ok(read_len) = res {
            let copy_len = usize::min(read_len, bz.len());
            let copy_start = off_in_blk as usize;
            bz[..copy_len].copy_from_slice(&buf[copy_start..copy_start + copy_len]);
            return Ok(copy_len);
        }
        res
    }

    /// Read at most `num_bytes` from file at `offset` to fill `buf`
    ///
    /// # Parameters
    ///
    /// - `buf`: a vector to be filled
    /// - `num_bytes`: the wanted number of bytes to be read
    /// - `offset`: the start position of a byteslice that was written before
    /// - `pre_reader`: a PreReader used to take advantage of spatial locality
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    ///
    /// - `Ok`: Number of bytes that was filled into `buf`
    /// - `Err`: Encounted some file system error.
    ///
    pub fn read_at_with_pre_reader(
        &self,
        buf: &mut Vec<u8>,
        num_bytes: usize,
        offset: i64,
        pre_reader: &mut PreReader,
    ) -> io::Result<usize> {
        if buf.len() < num_bytes {
            buf.resize(num_bytes, 0);
        }

        let file_id = offset / self.segment_size;
        let pos = offset % self.segment_size;

        if pre_reader.try_read(file_id, pos, &mut buf[..num_bytes]) {
            return Ok(num_bytes);
        }

        let opt = self.file_map.get(&file_id);
        let f = &opt.as_ref().unwrap().value().0;

        if num_bytes >= PRE_READ_BUF_SIZE {
            panic!("Read too many bytes");
        }

        if pos + num_bytes as i64 > self.segment_size {
            return Self::read_at_aligned(f, &mut buf[..num_bytes], pos);
        }

        let blk_size = IO_BLK_SIZE as i64;
        let aligned_pos = (pos / blk_size) * blk_size;
        pre_reader.fill_slice(file_id, aligned_pos, |slice| {
            let end = usize::min(slice.len(), (self.segment_size - aligned_pos) as usize);
            f.read_at(&mut slice[..end], aligned_pos as u64)
                .map(|n| n as i64)
        })?;

        if !pre_reader.try_read(file_id, pos, &mut buf[..num_bytes]) {
            panic!(
                "Internal error: cannot read data just fetched in {} fileID {}",
                self.dir_name, file_id
            );
        }

        Ok(num_bytes)
    }

    /// Append a byteslice to the file. This byteslice may be temporarily held in
    /// `buffer` before flushing.
    ///
    /// # Parameters
    ///
    /// - `bz`: the byteslice to append. It cannot be longer than `wr_buf_size` specified
    ///         in `HPFile::new`.
    /// - `buffer`: the write buffer. It will never be larger than `wr_buf_size`.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    ///
    /// - `Ok`: the start position where this byteslice locates in the file
    /// - `Err`: Encounted some file system error.
    ///
    pub fn append(&self, bz: &[u8], buffer: &mut Vec<u8>) -> io::Result<i64> {
        if self.is_empty() {
            return Ok(0);
        }

        if bz.len() as i64 > self.buffer_size {
            panic!("bz is too large");
        }

        let mut largest_id = self.largest_id.load(Ordering::SeqCst);
        let start_pos = self.size();
        let old_size = self
            .latest_file_size
            .fetch_add(bz.len() as i64, Ordering::SeqCst);
        self.file_size.fetch_add(bz.len() as i64, Ordering::SeqCst);
        let mut split_pos = 0;
        let extra_bytes = (buffer.len() + bz.len()) as i64 - self.buffer_size;
        if extra_bytes > 0 {
            // flush buffer_size bytes to disk
            split_pos = bz.len() - extra_bytes as usize;
            buffer.extend_from_slice(&bz[0..split_pos]);
            let mut opt = self.file_map.get_mut(&largest_id);
            let mut f = &opt.as_mut().unwrap().value().0;
            if f.write_all(buffer).is_err() {
                panic!("Fail to write file");
            }
            self.file_size_on_disk
                .fetch_add(buffer.len() as i64, Ordering::SeqCst);
            buffer.clear();
        }
        buffer.extend_from_slice(&bz[split_pos..]); //put remained bytes into buffer
        let overflow_byte_count = old_size + bz.len() as i64 - self.segment_size;
        if overflow_byte_count >= 0 {
            self.flush(buffer, true)?;
            if self.directio {
                let done_file = format!("{}/{}-{}", self.dir_name, largest_id, self.segment_size);
                //re-open it as readonly&directio
                // A file that has been written in non-direct mode cannot be read in direct mode unless it is reopened.
                let mut options = File::options();
                #[cfg(target_os = "linux")]
                options.custom_flags(DIRECT);
                let f = options.read(true).open(&done_file).unwrap();
                self.file_map.insert(largest_id, Arc::new((f, true)));
            }
            largest_id += 1;
            self.largest_id.fetch_add(1, Ordering::SeqCst);
            //open new file as writable
            let new_file = format!("{}/{}-{}", self.dir_name, largest_id, self.segment_size);
            let f = match File::create_new(&new_file) {
                Ok(file) => file,
                Err(_) => File::options()
                    .read(true)
                    .write(true)
                    .open(&new_file)
                    .unwrap(),
            };
            if overflow_byte_count != 0 {
                // write zero bytes as placeholder
                buffer.clear();
                buffer.resize(overflow_byte_count as usize, 0);
            }
            self.file_map.insert(largest_id, Arc::new((f, false)));
            self.latest_file_size
                .store(overflow_byte_count, Ordering::SeqCst);
        }

        Ok(start_pos)
    }

    /// Prune from the beginning to `offset`. This part of the file cannot be read hereafter.
    pub fn prune_head(&self, offset: i64) -> io::Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        let file_id = offset / self.segment_size;
        let ids_to_remove: Vec<i64> = self
            .file_map
            .iter()
            .filter(|entry| *entry.key() < file_id)
            .map(|entry| *entry.key())
            .collect();

        for id in ids_to_remove {
            self.file_map.remove(&id);

            #[cfg(unix)]
            {
                let file_name = format!("{}/{}-{}", self.dir_name, id, self.segment_size);
                fs::remove_file(file_name)?;
            }
        }

        Ok(())
    }
}

/// Pre-read a large chunk of data from file for caching
#[derive(Debug)]
pub struct PreReader {
    buffer: Box<[u8]>, // size is PRE_READ_BUF_SIZE
    file_id: i64,
    start: i64,
    end: i64,
}

impl Default for PreReader {
    fn default() -> Self {
        Self::new()
    }
}

impl PreReader {
    pub fn new() -> Self {
        let v = direct_io::allocate_aligned_vec(PRE_READ_BUF_SIZE, IO_BLK_SIZE);
        Self {
            buffer: v.into_boxed_slice(),
            file_id: 0,
            start: 0,
            end: 0,
        }
    }

    fn fill_slice<F>(&mut self, file_id: i64, start: i64, access: F) -> io::Result<()>
    where
        F: FnOnce(&mut [u8]) -> io::Result<i64>,
    {
        self.file_id = file_id;
        self.start = start;
        let n = access(&mut self.buffer[..])?;
        self.end = start + n;
        Ok(())
    }

    fn try_read(&self, file_id: i64, start: i64, bz: &mut [u8]) -> bool {
        if file_id == self.file_id && self.start <= start && start + bz.len() as i64 <= self.end {
            let offset = (start - self.start) as usize;
            bz.copy_from_slice(&self.buffer[offset..offset + bz.len()]);
            true
        } else {
            false
        }
    }
}

pub mod direct_io {
    use std::alloc::alloc;
    use std::alloc::Layout;

    pub fn is_aligned(ptr: *const u8, alignment: usize) -> bool {
        (ptr as usize) % alignment == 0
    }

    pub fn allocate_aligned_vec(size: usize, alignment: usize) -> Vec<u8> {
        assert!(
            alignment.is_power_of_two(),
            "Alignment must be a power of two"
        );
        let layout = Layout::from_size_align(size, alignment).expect("Invalid layout");
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            panic!("Failed to allocate memory");
        }
        unsafe { Vec::from_raw_parts(ptr, size, size) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hp_file_new() {
        let temp_dir = tempfile::Builder::new()
            .prefix("hp_file_test")
            .tempdir()
            .unwrap();
        let dir = temp_dir.path().to_str().unwrap();
        let buffer_size = 64;
        let segment_size = 128;
        let hp = HPFile::new(buffer_size, segment_size, dir.to_string(), false).unwrap();
        assert_eq!(hp.buffer_size, buffer_size);
        assert_eq!(hp.segment_size, segment_size);
        assert_eq!(hp.file_map.len(), 1);

        let slice0 = [1; 44];
        let mut buffer = vec![];
        let mut pos = hp.append(slice0.as_ref(), &mut buffer).unwrap();
        assert_eq!(0, pos);
        assert_eq!(44, hp.size());

        let slice1a = [2; 16];
        let slice1b = [3; 10];
        let mut slice1 = vec![];
        slice1.extend_from_slice(&slice1a);
        slice1.extend_from_slice(&slice1b);
        pos = hp.append(slice1.as_ref(), &mut buffer).unwrap();
        assert_eq!(44, pos);
        assert_eq!(70, hp.size());

        let slice2a = [4; 25];
        let slice2b = [5; 25];
        let mut slice2 = vec![];
        slice2.extend_from_slice(&slice2a);
        slice2.extend_from_slice(&slice2b);
        pos = hp.append(slice2.as_ref(), &mut buffer).unwrap();
        assert_eq!(70, pos);
        assert_eq!(120, hp.size());

        let mut check0 = [0; 44];
        hp.read_at(&mut check0, 0).unwrap();
        assert_eq!(slice0.to_vec(), check0.to_vec());

        hp.flush(&mut buffer, false).unwrap();

        {
            // read_range
            let mut check0 = [0; 44];
            hp.read_range(&mut check0, 0).unwrap();
            assert_eq!(slice0.to_vec(), check0.to_vec());

            let mut check0 = [0; 25];
            hp.read_range(&mut check0, 70).unwrap();
            assert_eq!(slice2a.to_vec(), check0.to_vec());
        }

        let mut check1 = [0; 26];
        hp.read_at(&mut check1, 44).unwrap();
        assert_eq!(slice1, check1);

        let mut check2 = [0; 50];
        hp.read_at(&mut check2, 70).unwrap();
        assert_eq!(slice2, check2);

        let slice3 = [0; 16];
        pos = hp.append(slice3.to_vec().as_ref(), &mut buffer).unwrap();
        assert_eq!(120, pos);
        assert_eq!(136, hp.size());

        hp.flush(&mut buffer, false).unwrap();

        let hp_new = HPFile::new(64, 128, dir.to_string(), false).unwrap();

        hp_new.read_at(&mut check0, 0).unwrap();
        assert_eq!(slice0.to_vec(), check0.to_vec());

        hp_new.read_at(&mut check1, 44).unwrap();
        assert_eq!(slice1, check1);

        hp_new.read_at(&mut check2, 70).unwrap();
        assert_eq!(slice2, check2);

        let mut check3 = [0; 16];
        hp_new.read_at(&mut check3, 120).unwrap();
        assert_eq!(slice3.to_vec(), check3.to_vec());

        hp_new.prune_head(64).unwrap();
        hp_new.truncate(120).unwrap();
        assert_eq!(hp_new.size(), 120);
        let mut slice4 = vec![];
        hp_new.read_at(&mut slice4, 120).unwrap();
        assert_eq!(slice4.len(), 0);
    }
}
