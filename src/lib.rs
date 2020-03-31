#![allow(dead_code)]
pub mod storage;

use std::marker::PhantomData;

use std::convert::From;

use std::rc::Rc;

use std::cell::RefCell;

use std::clone::Clone;

use storage::{FileStorage, SerdeInterface, SerdeJson, Storage};

use std::io::{Read, Seek, SeekFrom, Write};

// use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use anyhow::{Context, Result};

use log::debug;

// NodeHD: TreeNode on Hard Disk
#[derive(Deserialize, Serialize)]
struct NodeHD {
    key: String,
    value_addr: Option<u64>,
    left_addr: Option<u64>,
    right_addr: Option<u64>,
    size: usize,
}

pub struct ValueAgent<S> {
    inner: Option<String>,
    pub addr: Option<u64>,
    format: PhantomData<S>,
}

// TreeNode: TreeNode in memory, which we use to search the tree
struct TreeNode {
    key: String,
    value_agent: ValueJsonAgent,
    left_agent: Option<NodeJsonAgent>,
    right_agent: Option<NodeJsonAgent>,
    size: usize,
}

pub struct NodeAgent<S> {
    inner: Option<TreeNode>,
    pub addr: Option<u64>,
    format: PhantomData<S>,
}

type NodeJsonAgent = Rc<RefCell<NodeAgent<SerdeJson>>>;
type ValueJsonAgent = Rc<RefCell<ValueAgent<SerdeJson>>>;

struct Logical {
    storage: FileStorage,
    root: Option<NodeJsonAgent>,
}

macro_rules! rc {
    ($v: expr) => {
        Rc::new(RefCell::new($v))
    };
}

impl TreeNode {
    fn new(key: String, value: String) -> TreeNode {
        TreeNode {
            key: key,
            value_agent: Rc::new(RefCell::new(ValueAgent::new(Some(value), None))),
            left_agent: None,
            right_agent: None,
            size: 1,
        }
    }
}

impl Clone for TreeNode {
    fn clone(&self) -> Self {
        TreeNode {
            key: self.key.clone(),
            value_agent: self.value_agent.clone(),
            left_agent: self.left_agent.as_ref().map(|rc| rc.clone()),
            right_agent: self.right_agent.as_ref().map(|rc| rc.clone()),
            size: self.size,
        }
    }
}

impl From<NodeHD> for TreeNode {
    fn from(nodehd: NodeHD) -> Self {
        let key = nodehd.key;
        let value_agent = rc!(ValueAgent::new(None, nodehd.value_addr));
        let left_agent = nodehd
            .left_addr
            .map(|addr| rc!(NodeAgent::new(None, Some(addr))));
        let right_agent = nodehd
            .right_addr
            .map(|addr| rc!(NodeAgent::new(None, Some(addr))));
        let size = nodehd.size;
        TreeNode {
            key,
            value_agent,
            left_agent,
            right_agent,
            size,
        }
    }
}

impl Logical {
    fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let storage = FileStorage::new(path)?;
        let root = None;
        Ok(Logical { storage, root })
    }

    fn get(&mut self, key: &str) -> Result<Option<String>> {
        debug!("[get] begin with {:?}", key);
        let node = self.root.as_ref().map(|rc| rc.clone());
        self.find(node, key)
    }

    fn put(&mut self, key: String, value: String) -> Result<()> {
        debug!("[put] begin with {:?}:{:?}", key, value);
        let node = self.root.as_ref().map(|rc| rc.clone());
        self.root = Some(self.insert(node, key, value)?);
        Ok(())
    }

    fn find(&mut self, agent: Option<NodeJsonAgent>, key: &str) -> Result<Option<String>> {
        if let Some(agent) = agent {
            let mut agent = agent.borrow_mut();
            let node = agent.get_mut(&mut self.storage)?.unwrap();
            debug!("find alone node {:?}", node.key);
            if key < &node.key {
                self.find(node.left_agent.clone(), key)
            } else if key > &node.key {
                self.find(node.right_agent.clone(), key)
            } else {
                match node.value_agent.borrow_mut().get(&mut self.storage)? {
                    Some(s) => Ok(Some(String::from(s))),
                    None => Ok(None),
                }
            }
        } else {
            Ok(None)
        }
    }

    fn insert(
        &mut self,
        agent: Option<NodeJsonAgent>,
        key: String,
        value: String,
    ) -> Result<NodeJsonAgent> {
        if let Some(agent) = agent {
            let mut agent = agent.borrow_mut();
            let node = agent.get(&mut self.storage)?.unwrap();
            let mut new_node = node.clone();
            if key < node.key {
                new_node.left_agent = Some(self.insert(node.left_agent.clone(), key, value)?);
                new_node.size += 1;
            } else if key > node.key {
                new_node.right_agent = Some(self.insert(node.right_agent.clone(), key, value)?);
                new_node.size += 1;
            } else {
                new_node.value_agent = rc!(ValueAgent::new(Some(value), None));
            }
            debug!(
                "return insert alone node {:?} with size {}",
                new_node.key, new_node.size
            );
            Ok(rc!(NodeAgent::new(Some(new_node), None)))
        } else {
            // new a TreeNode
            debug!("New a TreeNode with {}:{} with size 1", key, value);
            Ok(rc!(NodeAgent::new(Some(TreeNode::new(key, value)), None)))
        }
    }
}

impl<S: SerdeInterface> NodeAgent<S> {
    fn new(inner: Option<TreeNode>, addr: Option<u64>) -> Self {
        NodeAgent {
            inner,
            addr,
            format: PhantomData,
        }
    }

    fn get_mut(&mut self, storage: &mut impl Storage) -> Result<Option<&mut TreeNode>> {
        self.load(storage)?;
        Ok(self.inner.as_mut())
    }

    fn get(&mut self, storage: &mut impl Storage) -> Result<Option<&TreeNode>> {
        self.load(storage)?;
        Ok(self.inner.as_ref())
    }

    fn load(&mut self, storage: &mut impl Storage) -> Result<()> {
        if self.inner.is_none() && self.addr.is_some() {
            let _ = storage.seek(SeekFrom::Start(self.addr.unwrap()))?;
            let nodehd: NodeHD = S::from_reader(storage)?;
            self.inner = Some(nodehd.into());
            debug!(
                "agent load a TreeNode with key {:?} from disk",
                self.inner.as_ref().map(|node| &node.key)
            );
        }
        Ok(())
    }

    // fn store(&mut self, storage: &mut impl Storage) -> Result<()> {
    //     // Write to disk only when addr is None, which means it is a new item
    //     // Remember, we has a immutable storage structure.
    //     // Once an item was stored, we will not write it again, ever.
    //     if self.inner.is_some() && self.addr.is_none() {
    //         self.addr = Some(storage.get_write_addr()?);
    //         S::to_writer(storage, self.inner.as_ref().unwrap())?;
    //     }
    //     Ok(())
    // }
}

impl<S: SerdeInterface> ValueAgent<S> {
    fn new(inner: Option<String>, addr: Option<u64>) -> Self {
        ValueAgent {
            inner,
            addr,
            format: PhantomData,
        }
    }

    fn get(&mut self, storage: &mut impl Storage) -> Result<Option<&String>> {
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

#[cfg(test)]
mod tree_test {
    use super::*;
    use crate::storage::{FileStorage, SerdeJson};
    use pretty_env_logger;

    #[test]
    fn test_binary_tree() {
        pretty_env_logger::init();
        let mut tree = Logical::new("db.db").unwrap();
        assert_eq!(None, tree.get("hi").unwrap());
        tree.put("hello".to_owned(), "world".to_owned()).unwrap();
        tree.put("hi".to_owned(), "alice".to_owned()).unwrap();
        tree.put("arc".to_owned(), "shadow".to_owned()).unwrap();
        tree.put("before".to_owned(), "end".to_owned()).unwrap();
        assert_eq!(Some("end".to_owned()), tree.get("before").unwrap());
        assert_eq!(None, tree.get("zx").unwrap());
    }
}
