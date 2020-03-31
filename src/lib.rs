#![allow(dead_code)]

pub mod storage;

use std::marker::PhantomData;
use std::io::SeekFrom;

use std::rc::Rc;
use std::cell::RefCell;

use std::clone::Clone;
use std::convert::From;

use serde::{Deserialize, Serialize};

use anyhow::Result;
use log::debug;

use storage::{FileStorage, SerdeInterface, SerdeJson, Storage};

macro_rules! rc {
    ($v: expr) => {
        Rc::new(RefCell::new($v))
    };
}

// NodeHd: TreeNode on Hard Disk
#[derive(Deserialize, Serialize)]
struct NodeHd {
    key: String,
    value_addr: Option<u64>,
    left_addr: Option<u64>,
    right_addr: Option<u64>,
    size: usize,
}

impl From<&TreeNode> for NodeHd {
    fn from(node: &TreeNode) -> NodeHd {
        NodeHd {
            key: node.key.clone(),
            value_addr: node.value_agent.borrow().addr,
            left_addr: node.left_agent.as_ref().and_then(|rc| rc.borrow().addr),
            right_addr: node.right_agent.as_ref().and_then(|rc| rc.borrow().addr),
            size: node.size,
        }
    }
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

pub struct ValueAgent<S> {
    inner: Option<String>,
    pub addr: Option<u64>,
    format: PhantomData<S>,
}

type NodeJsonAgent = Rc<RefCell<NodeAgent<SerdeJson>>>;
type ValueJsonAgent = Rc<RefCell<ValueAgent<SerdeJson>>>;

impl TreeNode {
    fn new(key: String, value: String) -> TreeNode {
        TreeNode {
            key,
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
            left_agent: self.left_agent.as_ref().cloned(),
            right_agent: self.right_agent.as_ref().cloned(),
            size: self.size,
        }
    }
}

impl From<NodeHd> for TreeNode {
    fn from(nodehd: NodeHd) -> Self {
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

struct Logical {
    storage: FileStorage,
    root: Option<NodeJsonAgent>,
}

impl Logical {
    fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let storage = FileStorage::new(path)?;
        let root = None;
        let mut logical = Logical { storage, root };
        logical.refresh_tree_view()?;
        Ok(logical)
    }

    fn refresh_tree_view(&mut self) -> Result<()> {
        debug!("Refresh");
        if let Some(addr) = self.storage.get_root_addr()? {
            debug!("Get tree view at {}", addr);
            self.root = Some(rc!(NodeAgent::new(None, Some(addr))));
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        debug!("[commit] Begin");
        if let Some(ref node) = self.root {
            node.borrow_mut().store(&mut self.storage)?;
            self.storage.commit_root_addr(node.borrow().addr.unwrap())?;
        }
        Ok(())
    }

    fn get(&mut self, key: &str) -> Result<Option<String>> {
        debug!("[get] Begin with {:?}", key);
        let node = self.root.as_ref().cloned();
        self.find(node, key)
    }

    fn put(&mut self, key: String, value: String) -> Result<()> {
        debug!("[put] Begin with {:?}:{:?}", key, value);
        let node = self.root.as_ref().cloned();
        self.root = Some(self.insert(node, key, value)?);
        Ok(())
    }

    fn find(&mut self, agent: Option<NodeJsonAgent>, key: &str) -> Result<Option<String>> {
        if let Some(agent) = agent {
            let mut agent = agent.borrow_mut();
            let node = agent.get_mut(&mut self.storage)?.unwrap();
            debug!("[find] Find alone node {:?}", node.key);
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
                "[insert] Return insert alone node {:?} with size {}",
                new_node.key, new_node.size
            );
            Ok(rc!(NodeAgent::new(Some(new_node), None)))
        } else {
            // new a TreeNode
            debug!("[insert] New a TreeNode with {}:{} with size 1", key, value);
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
            let nodehd: NodeHd = S::from_reader(storage)?;
            self.inner = Some(nodehd.into());
            debug!(
                "[Agent] loads a TreeNode with key {:?} from disk",
                self.inner.as_ref().map(|node| &node.key)
            );
        }
        Ok(())
    }

    fn store(&mut self, storage: &mut impl Storage) -> Result<()> {
        // Write to disk only when addr is None, which means it is a new item
        // Remember, we has a immutable storage structure.
        // Once an item was stored, we will not write it again, ever.
        if self.inner.is_some() && self.addr.is_none() {
            let node = self.inner.as_ref().unwrap();
            node.value_agent.borrow_mut().store(storage)?;
            if let Some(ref left) = node.left_agent {
                left.borrow_mut().store(storage)?;
            }
            if let Some(ref right) = node.right_agent {
                right.borrow_mut().store(storage)?;
            }
            self.addr = Some(storage.get_write_addr()?);
            let nodehd: NodeHd = node.into();
            debug!("[Agent] writes down a tree node {:?}", node.key);
            S::to_writer(storage, &nodehd)?;
        }
        Ok(())
    }
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
            debug!("[Agent] loads a value node");
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
            debug!("[Agent] writes down a value node");
            S::to_writer(storage, self.inner.as_ref().unwrap())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tree_test {
    use super::*;
    use pretty_env_logger;
    use tempfile;

    #[test]
    fn test_binary_tree_in_memory() {
        let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let mut tree = Logical::new(path).unwrap();
        assert_eq!(None, tree.get("hi").unwrap());
        tree.put("hello".to_owned(), "world".to_owned()).unwrap();
        tree.put("hi".to_owned(), "alice".to_owned()).unwrap();
        tree.put("arc".to_owned(), "shadow".to_owned()).unwrap();
        tree.put("before".to_owned(), "end".to_owned()).unwrap();
        assert_eq!(Some("end".to_owned()), tree.get("before").unwrap());
        assert_eq!(None, tree.get("zoo").unwrap());
    }

    #[test]
    fn test_binary_tree_store() {
        // pretty_env_logger::init();
        let mut tree = Logical::new("db.db").unwrap();
        tree.put("hello".to_owned(), "world".to_owned()).unwrap();
        tree.put("hi".to_owned(), "alice".to_owned()).unwrap();
        tree.put("arc".to_owned(), "shadow".to_owned()).unwrap();
        tree.put("before".to_owned(), "end".to_owned()).unwrap();
        tree.commit().unwrap();
        drop(tree);
        let mut tree = Logical::new("db.db").unwrap();
        assert_eq!(Some("shadow".to_owned()), tree.get("arc").unwrap());
        assert_eq!(None, tree.get("zoo").unwrap());
    }
}
