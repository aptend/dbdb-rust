//! Append-only storage for an immutable tree.
//!
use crate::serde_interface::{SerdeBincode, SerdeInterface};

use serde::{Deserialize, Serialize};

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

// use log::{debug, info};

use anyhow::{Context, Result};

use cluFlock::{element::FlockElement, ExclusiveFlock, FlockLock};

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, RawHandle};

const SUPERBLOCK: u64 = 512;

pub trait Storage: Write + Read + Seek {
    /// Block until we acquire an advisory lock of the current storage.
    fn lock(&self) -> Result<FileStorageGuard>;

    /// Get the address where the next write will happen.
    fn get_write_addr(&mut self) -> Result<u64>;

    /// Get the address of the current root node.
    fn get_root_addr(&mut self) -> Result<Option<u64>>;

    /// Commit the addr of the new root node
    fn commit_root_addr(&mut self, addr: u64) -> Result<()>;
}

/// The underlying storage of an immutable tree structure
///
/// # Examples
/// ```no_run
/// let storage = FileStorage::new("some.db")?;
/// assert_eq!(0, storage.get_root_addr()?);
/// storage.write(b"hello world")?;
/// ```
///
pub struct FileStorage {
    path: PathBuf,
    file: File,
}

/// Manage the exculsive access right of the storage
///
/// `FileStorageGuard` implements `DerefMut<Target=FileStorage>` trait, so
/// you can use it like `Box<FileStorage>`. When `FileStorageGuard` is dropped,
/// the lock on `FileStorage` will be released.
pub struct FileStorageGuard {
    inner: FlockLock<FileStorage>,
}

#[derive(Serialize, Deserialize)]
struct Meta {
    root_addr: Option<u64>,
}

impl FileStorageGuard {
    pub fn new(file_store: FileStorage) -> Result<Self> {
        let inner = ExclusiveFlock::wait_lock(file_store).map_err(|e| e.err())?;
        Ok(FileStorageGuard { inner })
    }
}

impl Deref for FileStorageGuard {
    type Target = FileStorage;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for FileStorageGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Write for FileStorage {
    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        // append only
        self.file.seek(SeekFrom::End(0))?;
        self.file.write(data)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.file.flush()
    }
}

impl Read for FileStorage {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.file.read(buf)
    }
}

impl Seek for FileStorage {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, std::io::Error> {
        self.file.seek(pos)
    }
}

#[cfg(unix)]
impl FlockElement for FileStorage {
    type FilePtr = RawFd;

    fn as_file_ptr(&self) -> Self::FilePtr {
        AsRawFd::as_raw_fd(&self.file)
    }
}

#[cfg(windows)]
impl FlockElement for FileStorage {
    type FilePtr = RawHandle;

    fn as_file_ptr(&self) -> Self::FilePtr {
        AsRawHandle::as_raw_handle(&self.file)
    }
}

impl FileStorage {
    /// Open the file, write superblock matadata and return a `FileStorage`
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = PathBuf::from(path.as_ref());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .with_context(|| format!("can't open storage file {:?}", path))?;

        let mut storage = FileStorage { path, file };
        storage.ensure_superblock()?;
        Ok(storage)
    }

    fn ensure_superblock(&mut self) -> Result<()> {
        let mut guard = self.lock()?;
        let end_idx = guard.seek(SeekFrom::End(0))?;
        if end_idx < SUPERBLOCK {
            // init the db file
            guard.write_all(&vec![0; SUPERBLOCK as usize])?;
            guard.commit_root_addr(0)?;
        }
        Ok(())
    }

    fn try_clone(&self) -> Result<FileStorage> {
        Ok(FileStorage {
            path: self.path.clone(),
            file: self.file.try_clone()?,
        })
    }
}

impl Storage for FileStorage {
    fn lock(&self) -> Result<FileStorageGuard> {
        FileStorageGuard::new(self.try_clone()?)
    }

    fn get_write_addr(&mut self) -> Result<u64> {
        let pos = self.file.seek(SeekFrom::End(0))?;
        Ok(pos)
    }

    fn get_root_addr(&mut self) -> Result<Option<u64>> {
        let _ = self.seek(SeekFrom::Start(0))?;
        let meta: Meta = SerdeBincode::from_reader(self)?;
        Ok(meta.root_addr)
    }

    fn commit_root_addr(&mut self, addr: u64) -> Result<()> {
        self.seek(SeekFrom::Start(0))?;
        let meta = if addr == 0 {
            Meta { root_addr: None }
        } else {
            Meta {
                root_addr: Some(addr),
            }
        };

        Ok(SerdeBincode::to_writer(&mut self.file, &meta)?)
    }
}

#[cfg(test)]
mod storage_test {
    use super::{FileStorage, Storage, SUPERBLOCK};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::thread;
    use std::time;
    use tempfile;

    #[test]
    fn test_storage_lock() {
        let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let mut storage = FileStorage::new(&path).unwrap();
        let mut guard = storage.lock().unwrap();
        guard.write_all(b"hello").unwrap();

        let one_sec = time::Duration::from_secs(1);

        let start = time::Instant::now();
        let handle = thread::spawn(move || -> time::Duration {
            let storage = FileStorage::new(path).unwrap();
            let mut guard = storage.lock().unwrap();
            let d = start.elapsed();
            guard.write_all(b" world").unwrap();
            d
        });
        // wait 1s to free the lock
        thread::sleep(one_sec);
        drop(guard);

        match handle.join() {
            Ok(d) => assert!(
                d >= one_sec,
                format!("another process did't block for enough time, only {:?}", d)
            ),
            Err(e) => assert!(false, format!("something wrong: {:?}", e)),
        }

        let mut record = String::new();
        storage.seek(SeekFrom::Start(SUPERBLOCK)).unwrap();
        storage.read_to_string(&mut record).unwrap();
        assert_eq!("hello world", record);
    }

    #[test]
    fn test_storage_superblock() {
        let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let mut storage = FileStorage::new(path).unwrap();
        assert_eq!(None, storage.get_root_addr().unwrap());
        storage.commit_root_addr(42).unwrap();
        assert_eq!(Some(42), storage.get_root_addr().unwrap());
    }

    #[test]
    fn test_storage_write() {
        let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let mut storage = FileStorage::new(path).unwrap();
        assert_eq!(SUPERBLOCK, storage.get_write_addr().unwrap());
        storage.write_all(b"hello world").unwrap();
        assert_eq!(SUPERBLOCK + 11, storage.get_write_addr().unwrap());
    }
}
