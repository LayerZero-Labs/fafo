use std::fs::remove_file;
use std::io::Write;

use std::fs::File;

#[cfg(target_os = "zkvm")]
use hpfile::ReadAt;
#[cfg(unix)]
use std::os::unix::fs::FileExt;

// #![allow(dead_code)]

pub struct TempFile {
    file: File,
    fname: String,
}

impl TempFile {
    pub fn new(fname: String) -> Self {
        match File::options()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(fname.clone())
        {
            Ok(file) => Self { file, fname },
            Err(_) => panic!("Fail to open file: {}", fname),
        }
    }

    pub fn get_name(&self) -> String {
        self.fname.clone()
    }

    pub fn read_at(&self, buf: &mut [u8], off: u64) -> usize {
        if buf.is_empty() {
            return 0;
        }
        self.file.read_at(buf, off).unwrap()
    }

    pub fn write(&mut self, buf: &[u8]) {
        if buf.is_empty() {
            return;
        }
        self.file.write_all(buf).unwrap();
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if let Err(e) = remove_file(self.fname.clone()) {
            if e.kind() != std::io::ErrorKind::NotFound {
                panic!("Failed to remove file {}: {}", self.fname, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    #[test]
    #[serial]
    fn test_tempfile_new() {
        let fname = "./test_file.txt".to_string();
        let temp_file = TempFile::new(fname.clone());
        assert_eq!(temp_file.fname, fname);
    }

    #[test]
    #[serial]
    fn test_tempfile_write_and_read_at() {
        let fname = "./test_file.txt".to_string();
        let mut temp_file = TempFile::new(fname.clone());
        {
            let mut buf = [0u8; 5];
            temp_file.write(b"Hello");
            let bytes_read = temp_file.read_at(&mut buf, 0);
            assert_eq!(bytes_read, 5);
            assert_eq!(&buf, b"Hello");
        }
        {
            let buf = b"World";
            temp_file.write(buf);
            let mut read_buf = [0u8; 5];
            let bytes_read = temp_file.read_at(&mut read_buf, 5);
            assert_eq!(bytes_read, 5);
            assert_eq!(&read_buf, buf);
        }
    }

    #[test]
    #[serial]
    fn test_tempfile_drop() {
        let fname = "./test_file.txt".to_string();
        {
            let _temp_file = TempFile::new(fname.clone());
        }
        assert!(!std::path::Path::new(&fname).exists());
    }
}
