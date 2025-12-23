//! B+ 树核心 B+ tree core
//!
//! 并发模型: 使用 Arc<Mutex<Pool>> 保护 Pool，支持并发访问
//! Concurrency: Uses Arc<Mutex<Pool>> to protect Pool, supports concurrent access

use std::sync::{
  Arc,
  atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering},
};

use jdb_page::Pool;
use jdb_trait::Val;
use parking_lot::Mutex;

use crate::{
  error::{Error, Result},
  key::Key,
  view::{InternalMut, InternalView, LeafMut, LeafView, is_leaf},
};

/// 乐观读最大重试次数 Max optimistic read retries
const MAX_RETRY: usize = 100;

/// B+ 树 B+ tree
pub struct BTree {
  pool: Arc<Mutex<Pool>>,
  root: AtomicU32,
  height: AtomicU16,
  count: AtomicU64,
  unique: bool,
}

impl BTree {
  /// 创建新 B+ 树 Create new B+ tree
  pub async fn create(mut pool: Pool, unique: bool) -> Result<Self> {
    let page = pool.alloc()?;
    let root_id = page.id();
    LeafMut::new(page.buf_mut()).init();

    Ok(Self {
      pool: Arc::new(Mutex::new(pool)),
      root: AtomicU32::new(root_id),
      height: AtomicU16::new(1),
      count: AtomicU64::new(0),
      unique,
    })
  }

  /// 打开已有 B+ 树 Open existing B+ tree
  pub fn open(pool: Pool, root: u32, height: u16, count: u64, unique: bool) -> Self {
    Self {
      pool: Arc::new(Mutex::new(pool)),
      root: AtomicU32::new(root),
      height: AtomicU16::new(height),
      count: AtomicU64::new(count),
      unique,
    }
  }

  #[inline]
  pub fn root(&self) -> u32 {
    self.root.load(Ordering::Acquire)
  }

  #[inline]
  pub fn height(&self) -> u16 {
    self.height.load(Ordering::Acquire)
  }

  #[inline]
  pub fn len(&self) -> u64 {
    self.count.load(Ordering::Acquire)
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len() == 0
  }

  #[inline]
  pub fn is_unique(&self) -> bool {
    self.unique
  }

  // ==========================================================================
  // 读操作 Read operations
  // ==========================================================================

  /// 点查 Point lookup
  pub async fn get(&self, key: &[Val]) -> Result<Option<u64>> {
    let encoded = Key::encode(key);
    let key_bytes = encoded.as_bytes();

    for _ in 0..MAX_RETRY {
      if let Some(result) = self.get_optimistic(key_bytes).await? {
        return Ok(result);
      }
    }

    self.get_pessimistic(key_bytes).await
  }

  async fn get_optimistic(&self, key: &[u8]) -> Result<Option<Option<u64>>> {
    let mut node_id = self.root();

    loop {
      let (_buf, _is_leaf_node, child_or_result) = {
        let mut pool = self.pool.lock();
        let page = pool.get(node_id).await?;
        let buf = page.buf().to_vec(); // 复制数据以避免生命周期问题
        let is_leaf_node = is_leaf(&buf);

        if is_leaf_node {
          let leaf = LeafView::new(&buf);
          let ver = leaf.version();
          let result = leaf.search(key).ok().map(|idx| leaf.value(idx));
          if !leaf.validate(ver) {
            (buf, is_leaf_node, None) // 版本验证失败，需要重试
          } else {
            return Ok(Some(result));
          }
        } else {
          let internal = InternalView::new(&buf);
          let ver = internal.version();
          let child_id = internal.child(internal.find_child(key));
          if !internal.validate(ver) {
            (buf, is_leaf_node, None) // 版本验证失败，需要重试
          } else {
            (buf, is_leaf_node, Some(child_id))
          }
        }
      };

      if let Some(child_id) = child_or_result {
        node_id = child_id;
      } else {
        return Ok(None); // 版本验证失败，需要重试
      }
    }
  }

  async fn get_pessimistic(&self, key: &[u8]) -> Result<Option<u64>> {
    let leaf_id = self.find_leaf(key).await?;
    let mut pool = self.pool.lock();
    let page = pool.get(leaf_id).await?;
    let leaf = LeafView::new(page.buf());
    Ok(leaf.search(key).ok().map(|idx| leaf.value(idx)))
  }

  pub async fn contains(&self, key: &[Val]) -> Result<bool> {
    Ok(self.get(key).await?.is_some())
  }

  // ==========================================================================
  // 写操作 Write operations
  // ==========================================================================

  /// 插入 Insert
  pub async fn insert(&self, key: &[Val], value: u64) -> Result<()> {
    let encoded = Key::encode(key);
    let key_bytes = encoded.as_bytes();

    if self.unique {
      let leaf_id = self.find_leaf(key_bytes).await?;
      let mut pool = self.pool.lock();
      let page = pool.get(leaf_id).await?;
      if LeafView::new(page.buf()).search(key_bytes).is_ok() {
        return Err(Error::Duplicate);
      }
    }

    self.insert_key(key_bytes, value).await
  }

  async fn insert_key(&self, key: &[u8], value: u64) -> Result<()> {
    let path = self.find_path(key).await?;
    let leaf_id = *path.last().ok_or(Error::EmptyTree)?;

    // 尝试直接插入 Try direct insert
    {
      let mut pool = self.pool.lock();
      let page = pool.get(leaf_id).await?;
      let mut leaf = LeafMut::new(page.buf_mut());

      if let Ok(old_version) = leaf.try_lock() {
        let result = leaf.insert(key, value);
        leaf.unlock(old_version);

        if result.is_some() {
          self.count.fetch_add(1, Ordering::Relaxed);
          return Ok(());
        }
      }
    }

    // 空间不足或锁争用，进行分裂 Insufficient space or lock contention, split
    self.split_leaf(key, value, path).await
  }

  async fn split_leaf(&self, key: &[u8], value: u64, path: Vec<u32>) -> Result<()> {
    let leaf_id = *path.last().ok_or(Error::EmptyTree)?;

    // 第一步：读取现有数据 Step 1: Read existing data
    let (mut keys, mut values, old_next) = {
      let mut pool = self.pool.lock();
      let page = pool.get(leaf_id).await?;
      let buf_copy = page.buf().to_vec();
      drop(pool); // 释放锁 Release lock

      let view = LeafView::new(&buf_copy);
      let (keys, values) = view.entries();
      let old_next = view.next();
      (keys, values, old_next)
    };

    // 第二步：在内存中插入新数据 Step 2: Insert new data in memory
    let pos = keys
      .binary_search_by(|k| k.as_slice().cmp(key))
      .unwrap_or_else(|i| i);
    keys.insert(pos, key.to_vec());
    values.insert(pos, value);

    let total = keys.len();
    let mid = total / 2;

    // 第三步：分配新页面并写入数据 Step 3: Allocate new page and write data
    let new_id = {
      let mut pool = self.pool.lock();
      let new_page = pool.alloc()?;
      let new_id = new_page.id();

      // 初始化右叶子 Initialize right leaf
      let mut right_leaf = LeafMut::new(new_page.buf_mut());
      right_leaf.init();
      for i in mid..total {
        if right_leaf.insert(&keys[i], values[i]).is_none() {
          return Err(Error::Full);
        }
      }
      right_leaf.set_prev(leaf_id);
      right_leaf.set_next(old_next);

      new_id
    };

    // 第四步：重新初始化左叶子 Step 4: Reinitialize left leaf
    {
      let mut pool = self.pool.lock();
      let page = pool.get(leaf_id).await?;
      let mut leaf = LeafMut::new(page.buf_mut());

      let old_version = leaf.lock_with_retry(100).map_err(|_| Error::Full)?;
      leaf.init();
      for i in 0..mid {
        if leaf.insert(&keys[i], values[i]).is_none() {
          leaf.unlock(old_version);
          return Err(Error::Full);
        }
      }
      leaf.set_next(new_id);
      leaf.unlock(old_version);
    }

    self.count.fetch_add(1, Ordering::Relaxed);
    self.propagate_split(path, &keys[mid], new_id).await
  }

  async fn propagate_split(&self, mut path: Vec<u32>, key: &[u8], right_child: u32) -> Result<()> {
    let mut cur_key = key.to_vec();
    let mut cur_child = right_child;

    loop {
      path.pop();

      if path.is_empty() {
        return self.new_root(&cur_key, cur_child).await;
      }

      let parent_id = *path.last().ok_or(Error::EmptyTree)?;

      let need_split = {
        let mut pool = self.pool.lock();
        let page = pool.get(parent_id).await?;
        let mut parent = InternalMut::new(page.buf_mut());

        if let Ok(old_version) = parent.try_lock() {
          let result = parent.insert(&cur_key, cur_child);
          parent.unlock(old_version);
          result.is_none()
        } else {
          true // 锁争用，假设需要分裂
        }
      };

      if !need_split {
        return Ok(());
      }

      let (mid_key, new_id) = self.split_internal(parent_id, &cur_key, cur_child).await?;
      cur_key = mid_key;
      cur_child = new_id;
    }
  }

  async fn split_internal(
    &self,
    node_id: u32,
    key: &[u8],
    right_child: u32,
  ) -> Result<(Vec<u8>, u32)> {
    // 第一步：读取现有数据 Step 1: Read existing data
    let (mut keys, mut children, level) = {
      let mut pool = self.pool.lock();
      let page = pool.get(node_id).await?;
      let buf_copy = page.buf().to_vec();
      drop(pool); // 释放锁 Release lock

      let view = InternalView::new(&buf_copy);
      let (keys, children) = view.entries();
      let level = view.level();
      (keys, children, level)
    };

    // 第二步：在内存中插入新数据 Step 2: Insert new data in memory
    let pos = keys
      .binary_search_by(|k| k.as_slice().cmp(key))
      .unwrap_or_else(|i| i);
    keys.insert(pos, key.to_vec());
    children.insert(pos + 1, right_child);

    let total = keys.len();
    let mid = total / 2;

    // 第三步：分配新页面并写入数据 Step 3: Allocate new page and write data
    let new_id = {
      let mut pool = self.pool.lock();
      let new_page = pool.alloc()?;
      let new_id = new_page.id();

      // 初始化右节点 Initialize right node
      let mut right_node = InternalMut::new(new_page.buf_mut());
      right_node.init(level);
      right_node.set_first_child(children[mid + 1]);
      for i in (mid + 1)..total {
        if right_node.insert(&keys[i], children[i + 1]).is_none() {
          return Err(Error::Full);
        }
      }

      new_id
    };

    // 第四步：重新初始化左节点 Step 4: Reinitialize left node
    {
      let mut pool = self.pool.lock();
      let page = pool.get(node_id).await?;
      let mut node = InternalMut::new(page.buf_mut());

      let old_version = node.lock_with_retry(100).map_err(|_| Error::Full)?;
      node.init(level);
      node.set_first_child(children[0]);
      for i in 0..mid {
        if node.insert(&keys[i], children[i + 1]).is_none() {
          node.unlock(old_version);
          return Err(Error::Full);
        }
      }
      node.unlock(old_version);
    }

    Ok((keys[mid].clone(), new_id))
  }

  async fn new_root(&self, key: &[u8], right_child: u32) -> Result<()> {
    let old_root = self.root();
    let height = self.height();

    let new_root_id = {
      let mut pool = self.pool.lock();
      let new_page = pool.alloc()?;
      let new_root_id = new_page.id();

      let mut root = InternalMut::new(new_page.buf_mut());
      root.init(height);
      root.set_first_child(old_root);
      root.insert(key, right_child);

      new_root_id
    };

    self.root.store(new_root_id, Ordering::Release);
    self.height.fetch_add(1, Ordering::Relaxed);
    Ok(())
  }

  /// 删除 Delete
  pub async fn delete(&self, key: &[Val]) -> Result<bool> {
    let encoded = Key::encode(key);
    let key_bytes = encoded.as_bytes();
    let leaf_id = self.find_leaf(key_bytes).await?;

    let mut pool = self.pool.lock();
    let page = pool.get(leaf_id).await?;

    // 创建缓冲区副本来查找索引 Create buffer copy to find index
    let buf_copy = page.buf().to_vec();
    let view = LeafView::new(&buf_copy);

    let idx = match view.search(key_bytes) {
      Ok(idx) => idx,
      Err(_) => return Ok(false),
    };

    // 获取锁并删除 Acquire lock and delete
    let mut leaf = LeafMut::new(page.buf_mut());
    let old_version = leaf.lock_with_retry(100).map_err(|_| Error::NotFound)?;
    leaf.delete(idx);
    leaf.unlock(old_version);

    self.count.fetch_sub(1, Ordering::Relaxed);
    Ok(true)
  }

  // ==========================================================================
  // 辅助方法 Helper methods
  // ==========================================================================

  async fn find_leaf(&self, key: &[u8]) -> Result<u32> {
    let mut node_id = self.root();
    loop {
      let mut pool = self.pool.lock();
      let page = pool.get(node_id).await?;
      if is_leaf(page.buf()) {
        return Ok(node_id);
      }
      let internal = InternalView::new(page.buf());
      node_id = internal.child(internal.find_child(key));
    }
  }

  async fn find_path(&self, key: &[u8]) -> Result<Vec<u32>> {
    let mut path = Vec::with_capacity(self.height() as usize);
    let mut node_id = self.root();
    loop {
      path.push(node_id);
      let mut pool = self.pool.lock();
      let page = pool.get(node_id).await?;
      if is_leaf(page.buf()) {
        return Ok(path);
      }
      let internal = InternalView::new(page.buf());
      node_id = internal.child(internal.find_child(key));
    }
  }

  pub async fn lower_bound(&self, key: &[Val]) -> Result<(u32, usize)> {
    let encoded = Key::encode(key);
    let key_bytes = encoded.as_bytes();
    let leaf_id = self.find_leaf(key_bytes).await?;
    let mut pool = self.pool.lock();
    let page = pool.get(leaf_id).await?;
    let leaf = LeafView::new(page.buf());
    let idx = leaf.search(key_bytes).unwrap_or_else(|i| i);
    Ok((leaf_id, idx))
  }

  pub async fn first_leaf(&self) -> Result<u32> {
    let mut node_id = self.root();
    loop {
      let mut pool = self.pool.lock();
      let page = pool.get(node_id).await?;
      if is_leaf(page.buf()) {
        return Ok(node_id);
      }
      node_id = InternalView::new(page.buf()).child(0);
    }
  }

  pub async fn read_leaf(&self, page_id: u32) -> Result<(Vec<Vec<u8>>, Vec<u64>, u32)> {
    let mut pool = self.pool.lock();
    let page = pool.get(page_id).await?;
    let leaf = LeafView::new(page.buf());
    let (keys, values) = leaf.entries();
    Ok((keys, values, leaf.next()))
  }

  pub async fn sync(&self) -> Result<()> {
    let mut pool = self.pool.lock();
    pool.sync().await?;
    Ok(())
  }
}
