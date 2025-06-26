use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::RwLock;
use std::time::SystemTime;

/// Represents an in-memory file, mimicking `std::fs::File`.
#[derive(Debug)]
pub struct File {
    data: RwLock<Vec<u8>>,
    cursor: RwLock<usize>,
    options: OpenOptions,
    metadata: RwLock<Metadata>,
}

/// Represents file metadata, mimicking `std::fs::Metadata`.
#[derive(Debug, Clone)]
pub struct Metadata {
    len: u64,
    created: SystemTime,
    modified: SystemTime,
    accessed: SystemTime,
}

impl Metadata {
    /// Returns the file size in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the creation time of the file.
    pub fn created(&self) -> io::Result<SystemTime> {
        Ok(self.created)
    }

    /// Returns the last modification time of the file.
    pub fn modified(&self) -> io::Result<SystemTime> {
        Ok(self.modified)
    }

    /// Returns the last access time of the file.
    pub fn accessed(&self) -> io::Result<SystemTime> {
        Ok(self.accessed)
    }

    /// Updates the modification time to now.
    pub fn update_modified(&mut self) {
        self.modified = SystemTime::now();
    }

    /// Updates the access time to now.
    pub fn update_accessed(&mut self) {
        self.accessed = SystemTime::now();
    }

    /// Updates the file length.
    pub fn update_len(&mut self, new_len: u64) {
        self.len = new_len;
        self.update_modified();
    }
}

/// Represents file opening options, mimicking `std::fs::OpenOptions`.
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    create: bool,
    create_new: bool,
    truncate: bool,
    custom_flags: i32,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_new(&mut self, value: bool) -> &mut Self {
        self.create_new = value;
        self
    }

    pub fn read(&mut self, value: bool) -> &mut Self {
        self.read = value;
        self
    }

    pub fn write(&mut self, value: bool) -> &mut Self {
        self.write = value;
        self
    }

    pub fn append(&mut self, value: bool) -> &mut Self {
        self.append = value;
        self
    }

    pub fn create(&mut self, value: bool) -> &mut Self {
        self.create = value;
        self
    }

    pub fn truncate(&mut self, value: bool) -> &mut Self {
        self.truncate = value;
        self
    }

    pub fn custom_flags(&mut self, flags: i32) -> &mut Self {
        self.custom_flags = flags;
        self
    }

    /// Opens a file, mimicking `std::fs::OpenOptions::open()`.
    pub fn open<P: AsRef<Path>>(&self, _path: P) -> io::Result<File> {
        let file = File {
            data: RwLock::new(Vec::new()),
            cursor: RwLock::new(0),
            options: self.clone(),
            metadata: RwLock::new(Metadata {
                len: 0,
                created: SystemTime::now(),
                modified: SystemTime::now(),
                accessed: SystemTime::now(),
            }),
        };
        Ok(file)
    }
}

impl File {
    /// Returns a new instance of `OpenOptions`, mimicking `std::fs::File::options()`.
    pub fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub fn metadata(&self) -> io::Result<Metadata> {
        Ok(self.metadata.read().unwrap().clone())
    }

    /// Mimics `std::fs::File::create_new()`, failing if the file already exists.
    pub fn create_new<P: AsRef<Path>>(_path: P) -> io::Result<File> {
        OpenOptions::new().write(true).create_new(true).open(_path)
    }

    /// Opens a file, mimicking `std::fs::File::open()`.
    pub fn open<P: AsRef<Path>>(_path: P) -> io::Result<File> {
        OpenOptions::new().read(true).open(_path)
    }

    /// Sets the length of the file (truncates or extends).
    pub fn set_len(&self, new_size: u64) -> io::Result<()> {
        let mut data = self.data.write().unwrap();
        let mut meta = self.metadata.write().unwrap();

        if new_size > data.len() as u64 {
            data.resize(new_size as usize, 0);
        } else {
            data.truncate(new_size as usize);
        }

        meta.len = new_size;
        meta.update_modified();

        Ok(())
    }

    /// Mimics `std::fs::File::sync_all()`.
    /// Since this is an in-memory file, this is a no-op but included for compatibility.
    pub fn sync_all(&self) -> io::Result<()> {
        // In a real FS, this would flush to disk; here we just update metadata.
        self.metadata.write().unwrap().update_modified();
        Ok(())
    }

    /// Mimics `std::fs::File::read_at()`.
    /// Reads at a specific position without modifying the cursor.
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let data = self.data.read().unwrap();
        let start = offset as usize;

        if start >= data.len() {
            return Ok(0); // Beyond EOF
        }

        let end = (start + buf.len()).min(data.len());
        let bytes_read = end - start;
        buf[..bytes_read].copy_from_slice(&data[start..end]);

        // Update last accessed timestamp
        self.metadata.write().unwrap().update_accessed();

        Ok(bytes_read)
    }

    /// Opens a file in write-only mode.
    ///
    /// This function will create a file if it does not exist,
    /// and will truncate it if it does.
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<File> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())
    }

    /// Attempts to sync all OS-internal metadata to disk.
    pub fn sync_data(&self) -> io::Result<()> {
        self.metadata.write().unwrap().update_modified();
        Ok(())
    }

    /// Creates a new `File` instance that shares the same underlying file handle.
    pub fn try_clone(&self) -> io::Result<File> {
        Ok(File {
            data: RwLock::new(self.data.read().unwrap().clone()),
            cursor: RwLock::new(*self.cursor.read().unwrap()),
            options: self.options.clone(),
            metadata: RwLock::new(self.metadata.read().unwrap().clone()),
        })
    }
}

impl Read for &File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.options.read {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "File is not readable",
            ));
        }

        let data = self.data.read().unwrap();
        let mut cursor = self.cursor.write().unwrap();

        if *cursor >= data.len() {
            return Ok(0); // EOF
        }

        let bytes_to_read = buf.len().min(data.len() - *cursor);
        buf[..bytes_to_read].copy_from_slice(&data[*cursor..*cursor + bytes_to_read]);
        *cursor += bytes_to_read;

        self.metadata.write().unwrap().accessed = SystemTime::now();

        Ok(bytes_to_read)
    }
}

impl Write for &File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.options.write {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "File is not writable",
            ));
        }

        let mut data = self.data.write().unwrap();
        let mut cursor = self.cursor.write().unwrap();
        let mut meta = self.metadata.write().unwrap();

        if self.options.append {
            *cursor = data.len();
        }

        if *cursor > data.len() {
            data.resize(*cursor, 0);
        }

        let to_write = buf.len();
        if *cursor + to_write > data.len() {
            data.resize(*cursor + to_write, 0);
        }

        data[*cursor..*cursor + to_write].copy_from_slice(buf);
        *cursor += to_write;

        meta.len = data.len() as u64;
        meta.update_modified();

        Ok(to_write)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.metadata.write().unwrap().update_modified();
        Ok(())
    }
}

impl Seek for &File {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let mut cursor = self.cursor.write().unwrap();
        let data_len = self.data.read().unwrap().len() as u64;

        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                if offset.is_negative() {
                    data_len.checked_sub(offset.unsigned_abs()).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "Seek before start")
                    })?
                } else {
                    data_len.checked_add(offset as u64).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "Seek out of bounds")
                    })?
                }
            }
            SeekFrom::Current(offset) => {
                let cur = *cursor as i64;
                let new_pos = cur.checked_add(offset).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "Seek out of bounds")
                })?;

                if new_pos < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Seek before start",
                    ));
                }
                new_pos as u64
            }
        };

        *cursor = new_pos as usize;
        Ok(new_pos)
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self).read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        (&*self).read_exact(buf)
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self).flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        (&*self).write_all(buf)
    }
}

impl Seek for File {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        (&*self).seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_options() {
        let file = File::options()
            .read(true)
            .write(true)
            .open("test.txt")
            .unwrap();
        assert!(file.options.read);
        assert!(file.options.write);
    }

    #[test]
    fn test_create_new_file() {
        let file_name = "test.txt";
        let file = File::create_new(file_name);
        assert!(file.is_ok());
    }

    #[test]
    fn test_write_and_read() {
        let file_name = "test_write_read.txt";
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)
            .unwrap();
        let mut file_mut = file;

        file_mut.write_all(b"hello world").unwrap();
        assert_eq!(file_mut.metadata().unwrap().len(), 11);

        let mut buffer = vec![0; 11];
        file_mut.seek(SeekFrom::Start(0)).unwrap();
        file_mut.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"hello world");
    }

    #[test]
    fn test_truncate() {
        let file_name = "test_truncate.txt";
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)
            .unwrap();
        let mut file_mut = file;

        file_mut.write_all(b"truncate test").unwrap();
        assert_eq!(file_mut.metadata().unwrap().len(), 13);

        file_mut.set_len(8).unwrap();
        assert_eq!(file_mut.metadata().unwrap().len(), 8);

        let mut buffer = vec![0; 8];
        file_mut.seek(SeekFrom::Start(0)).unwrap();
        file_mut.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"truncate");
    }

    #[test]
    fn test_seek() {
        let file_name = "test_seek.txt";
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)
            .unwrap();
        let mut file_mut = file;

        file_mut.write_all(b"abcdef").unwrap();
        file_mut.seek(SeekFrom::Start(3)).unwrap();
        let mut buffer = [0; 3];
        file_mut.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"def");

        file_mut.seek(SeekFrom::End(-3)).unwrap();
        file_mut.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"def");
    }

    #[test]
    fn test_append() {
        let file_name = "test_append.txt";
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)
            .unwrap();
        let mut file_mut = file;

        file_mut.write_all(b"first part").unwrap();
        file_mut.seek(SeekFrom::End(0)).unwrap();
        file_mut.write_all(b" second part").unwrap();
        assert_eq!(file_mut.metadata().unwrap().len(), 22);

        let mut buffer = vec![0; 22];
        file_mut.seek(SeekFrom::Start(0)).unwrap();
        file_mut.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"first part second part");
    }

    #[test]
    fn test_read_at() {
        let file_name = "test_read_at.txt";
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)
            .unwrap();
        let mut file_mut = file;

        file_mut.write_all(b"read at test").unwrap();
        let mut buffer = vec![0; 4];
        file_mut.read_at(&mut buffer, 5).unwrap();
        assert_eq!(&buffer, b"at t");
    }
}
