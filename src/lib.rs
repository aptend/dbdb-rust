use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use log::{debug, info};
use pretty_env_logger;

use anyhow::{Context, Result};

use cluFlock::{ExclusiveFlock, FlockLock, ToFlock};

const SUPERBLOCK: usize = 4096;

pub struct FileStorage {
    file: File,
    pos: u64,
}

#[derive(Serialize, Deserialize)]
struct Meta {
    root_addr: u64,
}

struct FileStorageGuard<'a> {
    inner: &'a mut FileStorage,
}

impl<'a> FileStorageGuard<'a> {
    pub fn new(inner: &'a mut FileStorage) -> Result<Self> {
        // let exfile = ExclusiveFlock::wait_lock(&inner.file).map_err(|e| e.err())?;
        Ok(FileStorageGuard {
            inner,
        })
    }
}

impl<'a> Deref for FileStorageGuard<'a> {
    type Target = FileStorage;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> DerefMut for FileStorageGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Write for FileStorage {
    fn write(&mut self, data: &[u8]) -> Result<usize, std::io::Error> {
        match self.file.write(data) {
            Ok(n) => {
                self.pos += n as u64;
                Ok(n)
            }
            Err(e) => Err(e),
        }
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

        let pos = 0;
        let storage = FileStorage { file, pos };

        Ok(storage)
    }

    pub fn tell(&self) -> u64 {
        self.pos
    }

    fn ensure_superblock(&mut self) -> Result<()> {
        let end_idx = self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    // Block util we accquire the lock of the current file.
    // return `Result<FileStorageGuard>` when we accquire the lock successfully,
    fn lock(&mut self) -> Result<FileStorageGuard> {
        FileStorageGuard::new(self)
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_storage_open() {
        unimplemented!();
    }
}
