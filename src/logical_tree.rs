use std::io::SeekFrom;
use std::marker::PhantomData;

use std::cell::RefCell;
use std::rc::Rc;

use std::clone::Clone;
use std::convert::From;

use serde::{Deserialize, Serialize};

use anyhow::Result;
use log::debug;

use crate::serde_interface::{SerdeInterface, SerdeJson};
use crate::storage::{FileStorage, FileStorageGuard, Storage};

macro_rules! rc {
    ($v: expr) => {
        Rc::new(RefCell::new($v))
    };
}

/// Agent acts like a data bridge between memory and hard disk
///
/// Every agent knows how to dump its inner data to disk and how to load data
/// from disk
trait Agent {
    type Inner;
    /// Create a new Agent. There are usually two use cases:
    ///
    /// 1. `Agent::new(Some(T), None)` happens when inserting a new pair of
    /// <KEY:VALUE>. It creates a data T without addr, waiting to be dumped.
    /// 2. `Agent::new(None, Some(u64))` happends when we load a Agent from
    /// disk. The value of T won't be loaded to memory until `Agent::get`
    /// or `Agent::get_mut` is called explicitly.
    fn new(inner: Option<Self::Inner>, addr: Option<u64>) -> Self;

    /// Get the addr of the inner data.
    fn addr(&self) -> Option<u64>;

    /// Get a mut reference of value T. The first call of `Agent::get_mut`
    /// might deserialize data from storage.
    fn get_mut(&mut self, storage: &mut impl Storage) -> Result<Option<&mut Self::Inner>>;

    /// Get a reference of value T. The first call of `Agent::get` might
    /// deserialize data from storage.
    fn get(&mut self, storage: &mut impl Storage) -> Result<Option<&Self::Inner>>;

    /// Store the inner data to storage
    fn store(&mut self, storage: &mut impl Storage) -> Result<()>;
}

/// StringAgent works for String
///
/// `S`: how to serialize / deserialize data
struct StringAgent<S = SerdeJson> {
    inner: Option<String>,
    pub addr: Option<u64>,
    format: PhantomData<S>,
}

impl<S: SerdeInterface> Agent for StringAgent<S> {
    type Inner = String;
    fn new(inner: Option<String>, addr: Option<u64>) -> Self {
        StringAgent {
            inner,
            addr,
            format: PhantomData,
        }
    }

    fn addr(&self) -> Option<u64> {
        self.addr
    }

    fn get(&mut self, storage: &mut impl Storage) -> Result<Option<&String>> {
        if self.inner.is_none() && self.addr.is_some() {
            let _ = storage.seek(SeekFrom::Start(self.addr.unwrap()))?;
            debug!("[Agent] loads a value node");
            self.inner = Some(S::from_reader(storage)?);
        }
        Ok(self.inner.as_ref())
    }

    fn get_mut(&mut self, storage: &mut impl Storage) -> Result<Option<&mut String>> {
        if self.inner.is_none() && self.addr.is_some() {
            let _ = storage.seek(SeekFrom::Start(self.addr.unwrap()))?;
            debug!("[Agent] loads a value node");
            self.inner = Some(S::from_reader(storage)?);
        }
        Ok(self.inner.as_mut())
    }

    fn store(&mut self, storage: &mut impl Storage) -> Result<()> {
        // Write to disk only when addr is None, which means it is a new item.
        // Remember, we have an immutable storage structure,
        // once an item was stored, we will never write it again.
        if self.inner.is_some() && self.addr.is_none() {
            self.addr = Some(storage.get_write_addr()?);
            debug!("[Agent] writes down a value node");
            S::to_writer(storage, self.inner.as_ref().unwrap())?;
        }
        Ok(())
    }
}

/// TreeNodeAgent works for TreeNode<V, Self>
///
/// `S`: how to serialize / deserialize data
///
/// `V`: TreeNode's value agent type
struct TreeNodeAgent<V, S = SerdeJson> {
    // this is a recursive struct, be careful
    inner: Option<TreeNode<V, Self>>,
    pub addr: Option<u64>,
    format: PhantomData<S>,
}

/// TreeNode on Hard Disk.
///
/// When serializing, NodeAgent { inner: TreeNode } -> NodeHD -> File.
///
/// Whene deserializing, File -> NodeHD -> NodeAgent { inner: TreeNode }
#[derive(Deserialize, Serialize)]
struct TreeNodeHD {
    key: String,
    value_addr: Option<u64>,
    left_addr: Option<u64>,
    right_addr: Option<u64>,
    size: usize,
}

impl<V, S> TreeNodeAgent<V, S>
where
    V: Agent,
    S: SerdeInterface,
{
    fn load(&mut self, storage: &mut impl Storage) -> Result<()> {
        if self.inner.is_none() && self.addr.is_some() {
            let _ = storage.seek(SeekFrom::Start(self.addr.unwrap()))?;
            let nodehd: TreeNodeHD = S::from_reader(storage)?;
            self.inner = Some(nodehd.into());
            debug!(
                "[Agent] loads a TreeNode with key {:?} from disk",
                self.inner.as_ref().map(|node| &node.key)
            );
        }
        Ok(())
    }
}

impl<V, S> Agent for TreeNodeAgent<V, S>
where
    S: SerdeInterface,
    V: Agent,
{
    type Inner = TreeNode<V, Self>;
    fn new(inner: Option<Self::Inner>, addr: Option<u64>) -> Self {
        TreeNodeAgent {
            inner,
            addr,
            format: PhantomData,
        }
    }

    fn addr(&self) -> Option<u64> {
        self.addr
    }

    fn get_mut(&mut self, storage: &mut impl Storage) -> Result<Option<&mut Self::Inner>> {
        self.load(storage)?;
        Ok(self.inner.as_mut())
    }

    fn get(&mut self, storage: &mut impl Storage) -> Result<Option<&Self::Inner>> {
        self.load(storage)?;
        Ok(self.inner.as_ref())
    }

    fn store(&mut self, storage: &mut impl Storage) -> Result<()> {
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
            let nodehd: TreeNodeHD = node.into();
            debug!("[Agent] writes down a tree node {:?}", node.key);
            S::to_writer(storage, &nodehd)?;
        }
        Ok(())
    }
}

type NodeAgent = TreeNodeAgent<StringAgent>;
type NodeAgentCell = Rc<RefCell<NodeAgent>>;

/// TreeNode in memory, which we use to search the tree.
///
/// Generic V means the type of value agent and generic N means
/// the type of left, right node agent.
struct TreeNode<V, N> {
    key: String,
    size: usize,
    value_agent: Rc<RefCell<V>>,
    left_agent: Option<Rc<RefCell<N>>>,
    right_agent: Option<Rc<RefCell<N>>>,
}

impl<V, N> TreeNode<V, N>
where
    V: Agent,
    N: Agent,
{
    fn new(key: String, value: V::Inner) -> Self {
        TreeNode {
            key,
            value_agent: rc!(V::new(Some(value), None)),
            left_agent: None,
            right_agent: None,
            size: 1,
        }
    }
}

impl<V, N> Clone for TreeNode<V, N> {
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

impl<V, N> From<TreeNodeHD> for TreeNode<V, N>
where
    V: Agent,
    N: Agent,
{
    fn from(nodehd: TreeNodeHD) -> Self {
        let key = nodehd.key;
        let value_agent = rc!(V::new(None, nodehd.value_addr));
        let left_agent = nodehd.left_addr.map(|addr| rc!(N::new(None, Some(addr))));
        let right_agent = nodehd.right_addr.map(|addr| rc!(N::new(None, Some(addr))));
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

impl<V, N> From<&TreeNode<V, N>> for TreeNodeHD
where
    V: Agent,
    N: Agent,
{
    fn from(node: &TreeNode<V, N>) -> TreeNodeHD {
        TreeNodeHD {
            key: node.key.clone(),
            value_addr: node.value_agent.borrow().addr(),
            left_addr: node.left_agent.as_ref().and_then(|rc| rc.borrow().addr()),
            right_addr: node.right_agent.as_ref().and_then(|rc| rc.borrow().addr()),
            size: node.size,
        }
    }
}

/// Tree, an immutable tree, knows how to handle a type A agent.
///
/// Tree will work with LogicalTree and do read and write things
trait DBTree<T> {
    /// Create a new Tree.
    fn new() -> Result<Self>
    where
        Self: std::marker::Sized;

    fn chroot(&mut self, addr: u64) -> Result<()>;

    fn root_addr(&self) -> Result<u64>;
    /// Search the tree for the given KEY
    fn find(&mut self, key: &str, storage: &mut impl Storage) -> Result<Option<String>>;

    /// Insert a new pair of KEY:VALUE
    fn insert(&mut self, key: String, value: String, storage: &mut impl Storage) -> Result<T>;

    /// Delete a TreeNode, if there is any.
    fn delete(&mut self, key: &str, storage: &mut impl Storage) -> Result<T>;
}

// struct BinaryTree {}

// impl BinaryTree {
//     fn _find(
//         &mut self,
//         key: &str,
//         agent: Option<NodeAgentCell>,
//         storage: &mut impl Storage,
//     ) -> Result<Option<ValueAgentCell>> {
//         if let Some(agent) = agent {
//             let mut agent = agent.borrow_mut();
//             let node = agent.get_mut(storage)?.unwrap();
//             debug!("[_find] Find alone node {:?}", node.key);
//             if key < &node.key {
//                 self._find(key, node.left_agent.clone(), storage)
//             } else if key > &node.key {
//                 self._find(key, node.right_agent.clone(), storage)
//             } else {
//                 Ok(Some(node.value_agent.clone()))
//             }
//         } else {
//             Ok(None)
//         }
//     }

//     fn _insert(
//         &mut self,
//         key: String,
//         value: String,
//         agent: Option<NodeAgentCell>,
//         storage: &mut impl Storage,
//     ) -> Result<(NodeAgentCell, usize)> {
//         if let Some(agent) = agent {
//             let mut agent = agent.borrow_mut();
//             let node = agent.get(storage)?.unwrap();
//             let mut new_node = node.clone();
//             let mut size_delta = 0;
//             if key < node.key {
//                 let result = self._insert(key, value, node.left_agent.clone(), storage)?;
//                 new_node.left_agent = Some(result.0);
//                 size_delta = result.1;
//                 new_node.size += size_delta;
//             } else if key > node.key {
//                 let result = self._insert(key, value, node.right_agent.clone(), storage)?;
//                 new_node.right_agent = Some(result.0);
//                 size_delta = result.1;
//                 new_node.size += size_delta;
//             } else {
//                 new_node.value_agent = rc!(StringAgent::new(Some(value), None));
//             }
//             debug!(
//                 "[_insert] Return insert alone node {:?} with size {}",
//                 new_node.key, new_node.size
//             );
//             Ok((rc!(TreeNodeAgent::new(Some(new_node), None)), size_delta))
//         } else {
//             // new a TreeNode
//             debug!(
//                 "[_insert] New a TreeNode with {}:{} with size 1",
//                 key, value
//             );
//             Ok((
//                 rc!(TreeNodeAgent::new(Some(TreeNode::new(key, value)), None)),
//                 1,
//             ))
//         }
//     }
// }

// impl Tree<NodeAgentCell> for BinaryTree {
//     fn new() -> Result<Self> {
//         Ok(BinaryTree {})
//     }

//     fn find(
//         &mut self,
//         key: &str,
//         agent: Option<NodeAgentCell>,
//         storage: &mut impl Storage,
//     ) -> Result<Option<String>> {
//         match self._find(key, agent, storage)? {
//             Some(agent) => {
//                 // every value agent must have a value after `get`
//                 // it is safe to unwrap it
//                 let value_ref = agent.borrow_mut().get(storage)?.unwrap();
//                 Ok(Some(String::from(value_ref)))
//             }
//             None => Ok(None),
//         }
//     }

//     fn insert(
//         &mut self,
//         key: String,
//         value: String,
//         agent: Option<NodeAgentCell>,
//         storage: &mut impl Storage,
//     ) -> Result<NodeAgentCell> {
//         let (agent, _) = self._insert(key, value, agent, storage)?;
//         Ok(agent)
//     }

//     fn delete(
//         &mut self,
//         key: &str,
//         agent: Option<NodeAgentCell>,
//         storage: &mut impl Storage,
//     ) -> Result<NodeAgentCell> {
//         unimplemented!()
//     }
// }

pub struct LogicalTree {
    storage: FileStorage,
    guard: Option<FileStorageGuard>,
    root: Option<NodeAgentCell>,
}

impl LogicalTree {
    /// Create a new LogicalTree
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let storage = FileStorage::new(path)?;
        let root = None;
        let guard = None;
        let mut logical = LogicalTree {
            storage,
            guard,
            root,
        };
        logical.refresh_tree_view()?;
        Ok(logical)
    }

    /// Begin a transaction
    pub fn begin(&mut self) -> Result<()> {
        if self.guard.is_none() {
            let guard: FileStorageGuard = self.storage.lock()?;
            self.guard = Some(guard);
            // now we get an exclusive write access of the underlying file
            self.refresh_tree_view()?;
        }
        Ok(())
    }

    /// Commit a transaction
    pub fn commit(&mut self) -> Result<()> {
        debug!("[commit] Begin");
        let node = self.root.as_ref().cloned();
        if let Some(node) = node {
            node.borrow_mut().store(self.get_storage())?;
            let addr = node.borrow().addr.unwrap();
            debug!("commit root addr {}", addr);
            self.storage.commit_root_addr(addr)?;
        }
        // end a transacation if there is one
        let _ = self.guard.take();
        Ok(())
    }

    /// Get value by key from the current db
    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        debug!("[get] Begin with {:?}", key);
        if self.guard.is_none() {
            self.refresh_tree_view()?;
        }
        let node = self.root.as_ref().cloned();
        self.find(node, key)
    }

    /// Put a pair of key:value into the currnent db
    /// If use this function without a trasaction context, it will be executed
    /// as a single-command transaction. That is:
    /// ```no_run
    /// tree.put("answer".to_owned(), "42".to_owned())?;
    /// ```
    /// is equivalent to  
    /// ```no_run
    /// tree.begin()?;
    /// tree.put("answer".to_owned(), "42".to_owned())?;
    /// tree.commit()?;
    /// ```
    pub fn put(&mut self, key: String, value: String) -> Result<()> {
        debug!("[put] Begin with {:?}:{:?}", key, value);
        if self.guard.is_none() {
            self.begin()?;
            let node = self.root.as_ref().cloned();
            self.root = Some(self.insert(node, key, value)?.0);
            self.commit()?;
        } else {
            let node = self.root.as_ref().cloned();
            self.root = Some(self.insert(node, key, value)?.0);
        }
        Ok(())
    }

    fn get_storage(&mut self) -> &mut impl Storage {
        match self.guard.as_mut() {
            Some(g) => &mut (**g),
            None => &mut self.storage,
        }
    }

    fn refresh_tree_view(&mut self) -> Result<()> {
        debug!("Try to refresh view");
        let storage = self.get_storage();
        if let Some(addr) = storage.get_root_addr()? {
            debug!("Get an version of tree view, at addr {}", addr);
            self.root = Some(rc!(NodeAgent::new(None, Some(addr))));
        }
        Ok(())
    }

    fn find(&mut self, agent: Option<NodeAgentCell>, key: &str) -> Result<Option<String>> {
        if let Some(agent) = agent {
            let mut agent = agent.borrow_mut();
            let node = agent.get_mut(self.get_storage())?.unwrap();
            debug!("[_find] Find alone node {:?}", node.key);
            if key < &node.key {
                self.find(node.left_agent.clone(), key)
            } else if key > &node.key {
                self.find(node.right_agent.clone(), key)
            } else {
                match node.value_agent.borrow_mut().get(self.get_storage())? {
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
        agent: Option<NodeAgentCell>,
        key: String,
        value: String,
    ) -> Result<(NodeAgentCell, usize)> {
        if let Some(agent) = agent {
            let mut agent = agent.borrow_mut();
            let node = agent.get(self.get_storage())?.unwrap();
            let mut new_node = node.clone();
            let mut size_delta = 0;
            if key < node.key {
                let result = self.insert(node.left_agent.clone(), key, value)?;
                new_node.left_agent = Some(result.0);
                size_delta = result.1;
                new_node.size += size_delta;
            } else if key > node.key {
                let result = self.insert(node.right_agent.clone(), key, value)?;
                new_node.right_agent = Some(result.0);
                size_delta = result.1;
                new_node.size += size_delta;
            } else {
                new_node.value_agent = rc!(StringAgent::new(Some(value), None));
            }
            debug!(
                "[_insert] Return insert alone node {:?} with size {}",
                new_node.key, new_node.size
            );
            Ok((rc!(NodeAgent::new(Some(new_node), None)), size_delta))
        } else {
            // new a TreeNode
            debug!(
                "[_insert] New a TreeNode with {}:{} with size 1",
                key, value
            );
            Ok((
                rc!(NodeAgent::new(Some(TreeNode::new(key, value)), None)),
                1,
            ))
        }
    }
}

#[cfg(test)]
mod tree_test {
    use super::*;
    use pretty_env_logger;
    use std::path::PathBuf;
    use std::thread;
    use std::time;
    use tempfile;

    #[test]
    #[cfg(unix)]
    fn test_binary_tree_no_dirty_read() {
        let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let mut tree = LogicalTree::new(&path).unwrap();
        let mut another_tree = LogicalTree::new(&path).unwrap();
        tree.begin().unwrap();
        tree.put("a".to_owned(), "1".to_owned()).unwrap();
        // we can't read the new a:1 pair in another tree
        assert_eq!(None, another_tree.get("a").unwrap());
        tree.commit().unwrap();
        // we can see the new pair after committing
        assert_eq!(Some("1".to_owned()), another_tree.get("a").unwrap());
    }

    #[test]
    fn test_binary_tree_concurrent_exclusive_write() {
        pretty_env_logger::init();
        // let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let path = PathBuf::from("db.db");
        let mut tree = LogicalTree::new(&path).unwrap();
        tree.begin().unwrap();
        let start_time = time::Instant::now();
        let handle = thread::spawn(move || -> time::Duration {
            let mut tree = LogicalTree::new(path).unwrap();
            tree.begin().unwrap();
            let gap = start_time.elapsed();
            assert_eq!(Some("1".to_owned()), tree.get("a").unwrap());
            tree.put("b".to_owned(), "2".to_owned()).unwrap();
            tree.commit().unwrap();
            assert_eq!(Some("2".to_owned()), tree.get("b").unwrap());
            gap
        });
        tree.put("a".to_owned(), "1".to_owned()).unwrap();
        let one_sec = time::Duration::from_secs(1);
        thread::sleep(one_sec);
        tree.commit().unwrap();
        match handle.join() {
            Ok(d) => assert!(
                d >= one_sec,
                format!("another process did't block for enough time, only {:?}", d)
            ),
            Err(e) => assert!(false, format!("subthread panic: {:?}", e)),
        }

        tree.put("c".to_owned(), "3".to_owned()).unwrap();
        assert_eq!(Some("2".to_owned()), tree.get("b").unwrap());
        assert_eq!(Some("3".to_owned()), tree.get("c").unwrap());
        assert_eq!(Some("1".to_owned()), tree.get("a").unwrap());
        assert_eq!(None, tree.get("z").unwrap());
    }

    #[test]
    fn test_binary_tree_in_memory() {
        let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let mut tree = LogicalTree::new(path).unwrap();
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
        let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        let mut tree = LogicalTree::new(&path).unwrap();
        tree.put("hello".to_owned(), "world".to_owned()).unwrap();
        tree.put("hi".to_owned(), "alice".to_owned()).unwrap();
        tree.put("arc".to_owned(), "shadow".to_owned()).unwrap();
        tree.put("before".to_owned(), "end".to_owned()).unwrap();
        tree.commit().unwrap();
        drop(tree);
        let mut tree = LogicalTree::new(&path).unwrap();
        assert_eq!(Some("shadow".to_owned()), tree.get("arc").unwrap());
        assert_eq!(None, tree.get("zoo").unwrap());
    }
}
