//! CoW B+ Tree implementation / CoW B+ 树实现

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use jdb_alloc::AlignedBuf;
use jdb_page::{PageStore, page_data, page_data_mut};
use jdb_trait::ValRef;

use crate::node::{Internal, Leaf, Node, PageId, MAX_KEYS};
use crate::{Error, Result};

/// CoW B+ Tree / CoW B+ 树
pub struct BTree {
  store: PageStore,
  root: PageId,
}

/// Insert result / 插入结果
enum InsertResult {
  /// No split / 无分裂
  Done(PageId),
  /// Split occurred / 发生分裂
  Split {
    left: PageId,
    right: PageId,
    key: Bytes,
  },
}

impl BTree {
  /// Create new tree / 创建新树
  pub async fn new(mut store: PageStore) -> Result<Self> {
    // Create empty root leaf / 创建空根叶子
    let root = store.alloc();
    let leaf = Leaf::new();
    let mut buf = AlignedBuf::page()?;
    Node::Leaf(leaf).serialize(&mut buf)?;
    store.write(root, &mut buf).await?;

    Ok(Self { store, root })
  }

  /// Open existing tree / 打开已有树
  pub fn open(store: PageStore, root: PageId) -> Self {
    Self { store, root }
  }

  /// Get root page id / 获取根页 ID
  pub fn root(&self) -> PageId {
    self.root
  }

  /// Get store reference / 获取存储引用
  pub fn store(&self) -> &PageStore {
    &self.store
  }

  /// Get mutable store / 获取可变存储
  pub fn store_mut(&mut self) -> &mut PageStore {
    &mut self.store
  }

  /// Read node from page / 从页读取节点
  async fn read_node(&self, page_id: PageId) -> Result<Node> {
    let buf = self.store.read(page_id).await?;
    Node::deserialize(page_data(&buf))
  }

  /// Write node to new page (CoW) / 写入节点到新页
  async fn write_node(&mut self, node: &Node) -> Result<PageId> {
    let page_id = self.store.alloc();
    let mut buf = AlignedBuf::page()?;
    node.serialize(page_data_mut(&mut buf))?;
    self.store.write(page_id, &mut buf).await?;
    Ok(page_id)
  }

  /// Get value for key / 获取 key 对应的值
  pub async fn get(&self, key: &[u8]) -> Result<Option<ValRef>> {
    let mut page_id = self.root;

    loop {
      let node = self.read_node(page_id).await?;
      match node {
        Node::Internal(n) => {
          let idx = n.find_child(key);
          page_id = n.children[idx];
        }
        Node::Leaf(n) => {
          let (found, idx) = n.find(key);
          return Ok(if found { Some(n.vals[idx]) } else { None });
        }
      }
    }
  }

  /// Insert key-value, return new root / 插入键值，返回新根
  pub async fn put(&mut self, key: &[u8], val: ValRef) -> Result<PageId> {
    let result = self.insert_recursive(self.root, key, val).await?;

    self.root = match result {
      InsertResult::Done(page_id) => page_id,
      InsertResult::Split { left, right, key } => {
        // Create new root / 创建新根
        let mut internal = Internal::new();
        internal.keys.push(key);
        internal.children.push(left);
        internal.children.push(right);
        self.write_node(&Node::Internal(internal)).await?
      }
    };

    Ok(self.root)
  }

  /// Recursive insert / 递归插入
  fn insert_recursive<'a>(
    &'a mut self,
    page_id: PageId,
    key: &'a [u8],
    val: ValRef,
  ) -> Pin<Box<dyn Future<Output = Result<InsertResult>> + 'a>> {
    Box::pin(async move {
    let node = self.read_node(page_id).await?;

    match node {
      Node::Internal(mut n) => {
        let idx = n.find_child(key);
        let child_result = self.insert_recursive(n.children[idx], key, val).await?;

        match child_result {
          InsertResult::Done(new_child) => {
            n.children[idx] = new_child;
            let new_page = self.write_node(&Node::Internal(n)).await?;
            Ok(InsertResult::Done(new_page))
          }
          InsertResult::Split {
            left,
            right,
            key: split_key,
          } => {
            n.children[idx] = left;
            n.keys.insert(idx, split_key);
            n.children.insert(idx + 1, right);

            if n.keys.len() >= MAX_KEYS {
              // Split internal / 分裂内部节点
              let mid = n.keys.len() / 2;
              let up_key = n.keys[mid].clone();

              let mut right_internal = Internal::new();
              right_internal.keys = n.keys.drain(mid + 1..).collect();
              right_internal.children = n.children.drain(mid + 1..).collect();
              n.keys.pop(); // Remove middle key / 移除中间键

              let left_page = self.write_node(&Node::Internal(n)).await?;
              let right_page = self.write_node(&Node::Internal(right_internal)).await?;

              Ok(InsertResult::Split {
                left: left_page,
                right: right_page,
                key: up_key,
              })
            } else {
              let new_page = self.write_node(&Node::Internal(n)).await?;
              Ok(InsertResult::Done(new_page))
            }
          }
        }
      }
      Node::Leaf(mut n) => {
        n.insert(key, val);

        if n.need_split() {
          let (split_key, right_leaf) = n.split();
          let left_page = self.write_node(&Node::Leaf(n)).await?;
          let right_page = self.write_node(&Node::Leaf(right_leaf)).await?;

          Ok(InsertResult::Split {
            left: left_page,
            right: right_page,
            key: split_key,
          })
        } else {
          let new_page = self.write_node(&Node::Leaf(n)).await?;
          Ok(InsertResult::Done(new_page))
        }
      }
    }
    })
  }

  /// Delete key, return (new_root, old_value) / 删除 key
  pub async fn del(&mut self, key: &[u8]) -> Result<(PageId, Option<ValRef>)> {
    let (new_root, old_val) = self.delete_recursive(self.root, key).await?;
    self.root = new_root;
    Ok((self.root, old_val))
  }

  /// Recursive delete / 递归删除
  fn delete_recursive<'a>(
    &'a mut self,
    page_id: PageId,
    key: &'a [u8],
  ) -> Pin<Box<dyn Future<Output = Result<(PageId, Option<ValRef>)>> + 'a>> {
    Box::pin(async move {
      let node = self.read_node(page_id).await?;

      match node {
        Node::Internal(mut n) => {
          let idx = n.find_child(key);
          let (new_child, old_val) = self.delete_recursive(n.children[idx], key).await?;
          n.children[idx] = new_child;
          let new_page = self.write_node(&Node::Internal(n)).await?;
          Ok((new_page, old_val))
        }
        Node::Leaf(mut n) => {
          let old_val = n.delete(key);
          let new_page = self.write_node(&Node::Leaf(n)).await?;
          Ok((new_page, old_val))
        }
      }
    })
  }

  /// Find leaf for key / 查找 key 所在叶子
  pub async fn find_leaf(&self, key: &[u8]) -> Result<(PageId, Leaf)> {
    let mut page_id = self.root;

    loop {
      let node = self.read_node(page_id).await?;
      match node {
        Node::Internal(n) => {
          let idx = n.find_child(key);
          page_id = n.children[idx];
        }
        Node::Leaf(n) => {
          return Ok((page_id, n));
        }
      }
    }
  }

  /// Read leaf by page id / 按页 ID 读取叶子
  pub async fn read_leaf(&self, page_id: PageId) -> Result<Leaf> {
    let node = self.read_node(page_id).await?;
    match node {
      Node::Leaf(n) => Ok(n),
      _ => Err(Error::InvalidNodeType(1)),
    }
  }

  /// Sync to disk / 同步到磁盘
  pub async fn sync(&self) -> Result<()> {
    self.store.sync().await?;
    Ok(())
  }
}
