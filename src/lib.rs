pub mod storage;

use std::marker::PhantomData;
use storage::{SerdeInterface, Storage};

use std::io::{Read, Seek, SeekFrom, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use anyhow::{Context, Result};

pub struct Ref<T, S> {
    inner: Option<T>,
    pub addr: Option<u64>,
    format: PhantomData<S>,
}

impl<T, S> Ref<T, S>
where
    T: DeserializeOwned + Serialize,
    S: SerdeInterface,
{
    fn new(inner: Option<T>, addr: Option<u64>) -> Self {
        Ref {
            inner,
            addr,
            format: PhantomData,
        }
    }

    fn get(&mut self, mut storage: impl Storage) -> Result<Option<&T>> {
        if self.inner.is_none() && self.addr.is_some() {
            let _ = storage.seek(SeekFrom::Start(self.addr.unwrap()))?;
            self.inner = Some(S::from_reader(storage)?);
        }
        Ok(self.inner.as_ref())
    }

    fn store(&mut self, mut storage: impl Storage) -> Result<()> {
        // Write to disk only when addr is None, which means it is a new item
        // Remember, we has a immutable storage structure.
        // Once an item was stored, we will not write it again, ever.
        if self.inner.is_some() && self.addr.is_none() {
            self.addr = Some(storage.get_write_addr()?);
            S::to_writer(storage, self.inner.as_ref().unwrap())?;
        }
        Ok(())
    }
}
