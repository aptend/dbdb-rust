#![allow(dead_code)]
pub mod storage;

use std::marker::PhantomData;

use std::convert::From;

use std::rc::Rc;

use storage::{FileStorage, SerdeInterface, SerdeJson, Storage};

use std::io::{Read, Seek, SeekFrom, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use anyhow::{Context, Result};

pub struct StoreAgent<T, S> {
    inner: Option<T>,
    pub addr: Option<u64>,
    format: PhantomData<S>,
}

impl<T, S> StoreAgent<T, S>
where
    T: DeserializeOwned + Serialize,
    S: SerdeInterface,
{
    fn new(inner: Option<T>, addr: Option<u64>) -> Self {
        StoreAgent {
            inner,
            addr,
            format: PhantomData,
        }
    }

    fn get(&mut self, storage: &mut impl Storage) -> Result<Option<&T>> {
        if self.inner.is_none() && self.addr.is_some() {
            let _ = storage.seek(SeekFrom::Start(self.addr.unwrap()))?;
            self.inner = Some(S::from_reader(storage)?);
        }
        Ok(self.inner.as_ref())
    }

    fn store(&mut self, storage: &mut impl Storage) -> Result<()> {
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

// NodeHD: TreeNode on Hard Disk
#[derive(Deserialize, Serialize)]
struct NodeHD {
    key: String,
    value_addr: Option<u64>,
    left_addr: Option<u64>,
    right_addr: Option<u64>,
    size: usize,
}

type TreeNodeToken = StoreAgent<NodeHD, SerdeJson>;
type ValueToken = StoreAgent<String, SerdeJson>;

// TreeNode: TreeNode in memory, which we use to search the tree
struct TreeNode {
    key: String,
    value_token: ValueToken,
    left_token: Rc<Option<TreeNodeToken>>,
    right_token: Rc<Option<TreeNodeToken>>,
    size: usize,
}

// impl From<NodeHD> for TreeNode {
//     fn from(token: NodeHD) -> Self {
//         let key = token.key;
//         let value_token = StoreAgent::new(None, token.value_addr);
//         let left_token = StoreAgent::new(None, token.left_addr);
//         let right_token = StoreAgent::new(None, token.right_addr);
//         let size = token.size;
//         TreeNode {
//             key,
//             value_token,
//             left_token,
//             right_token,
//             size,
//         }
//     }
// }

// impl TreeNode {
//     fn store(&mut self, storage: &mut impl Storage) -> Result<()> {
//         self.value_token.store(storage)?;
//         self.left_token.store(storage)?;
//         self.right_token.store(storage)?;
//         let token = NodeHD {
//             key: self.key.clone(),
//             value: self.value.addr,
//             left: self.left.addr,
//             right: self.right.addr,
//             size: self.size,
//         };
//         Ok(())
//     }

// }

struct Logical {
    storage: FileStorage,
    root: Option<TreeNodeToken>,
}

impl Logical {
    fn new() -> Result<Self> {
        let storage = FileStorage::new("db.db")?;
        let root = None;
        Ok(Logical { storage, root })
    }

    fn insert(
        &mut self,
        node: Rc<Option<TreeNodeToken>>,
        key: String,
        value: ValueToken,
    ) -> Result<TreeNodeToken> {
        let mut token: NodeHD;
        if node.is_none() {
            token = NodeHD {
                key: key,
                value_addr: value.addr,
                left_addr: None,
                right_addr: None,
                size: 1,
            };
        } else {
            // we need a TreeNode in memory
            // let node = node.unwrap().get(&mut self.storage)?;
        }
        Ok(StoreAgent::new(None, None))
    }
}

#[cfg(test)]
mod tree_test {
    use super::*;
    use crate::storage::{FileStorage, SerdeJson};

    #[test]
    fn test_store_token() {
        let mut v: StoreAgent<String, SerdeJson> = StoreAgent::new(Some("hello".to_owned()), None);

        let mut storage = FileStorage::new("db.db").unwrap();
        v.store(&mut storage).unwrap();
        assert!(v.addr.is_some());
        println!("{}", v.addr.as_ref().unwrap());
        let mut r: StoreAgent<String, SerdeJson> = StoreAgent::new(None, v.addr);
        let inner = r.get(&mut storage).unwrap();
        println!("{}", inner.unwrap());
    }
}
