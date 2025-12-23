//! B+ 树核心 B+ tree core
//!
//! 并发模型: 子表内写操作串行化，读操作乐观验证
//! Concurrency: Serialized writes within subtable, optimistic reads

use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};

use jdb_page::Pool;
use jdb_trait::Val;

use crate::error::{Error, Result};
use crate::key::Key;
use crate::view::{is_leaf, InternalMut, InternalView, LeafMut, LeafView};

/// 乐观读最大重试次数 Max optimistic read retries
const MAX_RETRY: usize = 100;

/// B+ 树 B+ tree
pub struct BTree {
  pool: Pool,
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
      pool,
      root: AtomicU32::new(root_id),
      height: AtomicU16::new(1),
      count: AtomicU64::new(0),
      unique,
    })
  }

  /// 打开已有 B+ 树 Open existing B+ tree
  pub fn open(pool: Pool, root: u32, height: u16, count: u64, unique: bool) -> Self {
    Self {
      pool,
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
  pub async fn get(&mut self, key: &[Val]) -> Result<Option<u64>> {
    let encoded = Key::encode(key);
    let key_bytes = encoded.as_bytes();

    for _ in 0..MAX_RETRY {
      if let Some(result) = self.get_optimistic(key_bytes).await? {
        return Ok(result);
      }
    }

    self.get_pessimistic(key_bytes).await
  }

  async fn get_optimistic(&mut self, key: &[u8]) -> Result<Option<Option<u64>>> {
    let mut node_id = self.root();

    loop {
      let page = self.pool.get(node_id).await?;
      let buf = page.buf();

      if is_leaf(buf) {
        let leaf = LeafView::new(buf);
        let ver = leaf.version();
        let result = leaf.search(key).ok().map(|idx| leaf.value(idx));
        if !leaf.validate(ver) {
          return Ok(None);
        }
        return Ok(Some(result));
      }

      let internal = InternalView::new(buf);
      let ver = internal.version();
      let child_id = internal.child(internal.find_child(key));
      if !internal.validate(ver) {
        return Ok(None);
      }
      node_id = child_id;
    }
  }

  async fn get_pessimistic(&mut self, key: &[u8]) -> Result<Option<u64>> {
    let leaf_id = self.find_leaf(key).await?;
    let page = self.pool.get(leaf_id).await?;
    let leaf = LeafView::new(page.buf());
    Ok(leaf.search(key).ok().map(|idx| leaf.value(idx)))
  }

  pub async fn contains(&mut self, key: &[Val]) -> Result<bool> {
    Ok(self.get(key).await?.is_some())
  }

  // ==========================================================================
  // 写操作 Write operations
  // ==========================================================================

  /// 插入 Insert
  pub async fn insert(&mut self, key: &[Val], value: u64) -> Result<()> {
    let encoded = Key::encode(key);
    let key_bytes = encoded.as_bytes();

    if self.unique {
      let leaf_id = self.find_leaf(key_bytes).await?;
      let page = self.pool.get(leaf_id).await?;
      if LeafView::new(page.buf()).search(key_bytes).is_ok() {
        return Err(Error::Duplicate);
      }
    }

    self.insert_key(key_bytes, value).await
  }

  async fn insert_key(&mut self, key: &[u8], value: u64) -> Result<()> {
    let path = self.find_path(key).await?;
    let leaf_id = *path.last().ok_or(Error::EmptyTree)?;

    {
      let page = self.pool.get(leaf_id).await?;
      let mut leaf = LeafMut::new(page.buf_mut());
      leaf.lock();
      if leaf.insert(key, value).is_some() {
        self.count.fetch_add(1, Ordering::Relaxed);
        leaf.unlock();
        return Ok(());
      }
      leaf.unlock();
    }

    self.split_leaf(key, value, path).await
  }

  async fn split_leaf(&mut self, key: &[u8], value: u64, path: Vec<u32>) -> Result<()> {
    let leaf_id = *path.last().ok_or(Error::EmptyTree)?;

    let (mut keys, mut values, old_next) = {
      let page = self.pool.get(leaf_id).await?;
      let view = LeafView::new(page.buf());
      let (ks, vs) = view.entries();
      (ks, vs, view.next())
    };

    let pos = keys.binary_search_by(|k| k.as_slice().cmp(key)).unwrap_or_else(|i| i);
    keys.insert(pos, key.to_vec());
    values.insert(pos, value);

    let total = keys.len();
    let mid = total / 2;
    let new_id = self.pool.alloc()?.id();

    {
      let page = self.pool.get(leaf_id).await?;
      let mut leaf = LeafMut::new(page.buf_mut());
      leaf.lock();
      leaf.init();
      for i in 0..mid {
        leaf.insert(&keys[i], values[i]);
      }
      leaf.set_next(new_id);
      leaf.unlock();
    }

    {
      let page = self.pool.get(new_id).await?;
      let mut leaf = LeafMut::new(page.buf_mut());
      leaf.init();
      for i in mid..total {
        leaf.insert(&keys[i], values[i]);
      }
      leaf.set_prev(leaf_id);
      leaf.set_next(old_next);
    }

    self.count.fetch_add(1, Ordering::Relaxed);
    self.propagate_split(path, &keys[mid], new_id).await
  }

  async fn propagate_split(&mut self, mut path: Vec<u32>, key: &[u8], right_child: u32) -> Result<()> {
    let mut cur_key = key.to_vec();
    let mut cur_child = right_child;

    loop {
      path.pop();

      if path.is_empty() {
        return self.new_root(&cur_key, cur_child).await;
      }

      let parent_id = *path.last().ok_or(Error::EmptyTree)?;

      let need_split = {
        let page = self.pool.get(parent_id).await?;
        let mut parent = InternalMut::new(page.buf_mut());
        parent.lock();
        let ok = parent.insert(&cur_key, cur_child).is_some();
        parent.unlock();
        !ok
      };

      if !need_split {
        return Ok(());
      }

      let (mid_key, new_id) = self.split_internal(parent_id, &cur_key, cur_child).await?;
      cur_key = mid_key;
      cur_child = new_id;
    }
  }

  async fn split_internal(&mut self, node_id: u32, key: &[u8], right_child: u32) -> Result<(Vec<u8>, u32)> {
    let (mut keys, mut children, level) = {
      let page = self.pool.get(node_id).await?;
      let view = InternalView::new(page.buf());
      let (ks, cs) = view.entries();
      (ks, cs, view.level())
    };

    let pos = keys.binary_search_by(|k| k.as_slice().cmp(key)).unwrap_or_else(|i| i);
    keys.insert(pos, key.to_vec());
    children.insert(pos + 1, right_child);

    let total = keys.len();
    let mid = total / 2;
    let new_id = self.pool.alloc()?.id();

    {
      let page = self.pool.get(node_id).await?;
      let mut node = InternalMut::new(page.buf_mut());
      node.lock();
      node.init(level);
      node.set_first_child(children[0]);
      for i in 0..mid {
        node.insert(&keys[i], children[i + 1]);
      }
      node.unlock();
    }

    {
      let page = self.pool.get(new_id).await?;
      let mut node = InternalMut::new(page.buf_mut());
      node.init(level);
      node.set_first_child(children[mid + 1]);
      for i in (mid + 1)..total {
        node.insert(&keys[i], children[i + 1]);
      }
    }

    Ok((keys[mid].clone(), new_id))
  }

  async fn new_root(&mut self, key: &[u8], right_child: u32) -> Result<()> {
    let old_root = self.root();
    let height = self.height();

    let new_page = self.pool.alloc()?;
    let new_root_id = new_page.id();

    let mut root = InternalMut::new(new_page.buf_mut());
    root.init(height);
    root.set_first_child(old_root);
    root.insert(key, right_child);

    self.root.store(new_root_id, Ordering::Release);
    self.height.fetch_add(1, Ordering::Relaxed);
    Ok(())
  }

  /// 删除 Delete
  pub async fn delete(&mut self, key: &[Val]) -> Result<bool> {
    let encoded = Key::encode(key);
    let key_bytes = encoded.as_bytes();
    let leaf_id = self.find_leaf(key_bytes).await?;

    let page = self.pool.get(leaf_id).await?;

    let idx = match LeafView::new(page.buf()).search(key_bytes) {
      Ok(idx) => idx,
      Err(_) => return Ok(false),
    };

    let mut leaf = LeafMut::new(page.buf_mut());
    leaf.lock();
    leaf.delete(idx);
    leaf.unlock();

    self.count.fetch_sub(1, Ordering::Relaxed);
    Ok(true)
  }

  // ==========================================================================
  // 辅助方法 Helper methods
  // ==========================================================================

  async fn find_leaf(&mut self, key: &[u8]) -> Result<u32> {
    let mut node_id = self.root();
    loop {
      let page = self.pool.get(node_id).await?;
      if is_leaf(page.buf()) {
        return Ok(node_id);
      }
      let internal = InternalView::new(page.buf());
      node_id = internal.child(internal.find_child(key));
    }
  }

  async fn find_path(&mut self, key: &[u8]) -> Result<Vec<u32>> {
    let mut path = Vec::with_capacity(self.height() as usize);
    let mut node_id = self.root();
    loop {
      path.push(node_id);
      let page = self.pool.get(node_id).await?;
      if is_leaf(page.buf()) {
        return Ok(path);
      }
      let internal = InternalView::new(page.buf());
      node_id = internal.child(internal.find_child(key));
    }
  }

  pub async fn lower_bound(&mut self, key: &[Val]) -> Result<(u32, usize)> {
    let encoded = Key::encode(key);
    let key_bytes = encoded.as_bytes();
    let leaf_id = self.find_leaf(key_bytes).await?;
    let page = self.pool.get(leaf_id).await?;
    let leaf = LeafView::new(page.buf());
    let idx = leaf.search(key_bytes).unwrap_or_else(|i| i);
    Ok((leaf_id, idx))
  }

  pub async fn first_leaf(&mut self) -> Result<u32> {
    let mut node_id = self.root();
    loop {
      let page = self.pool.get(node_id).await?;
      if is_leaf(page.buf()) {
        return Ok(node_id);
      }
      node_id = InternalView::new(page.buf()).child(0);
    }
  }

  pub async fn read_leaf(&mut self, page_id: u32) -> Result<(Vec<Vec<u8>>, Vec<u64>, u32)> {
    let page = self.pool.get(page_id).await?;
    let leaf = LeafView::new(page.buf());
    let (keys, values) = leaf.entries();
    Ok((keys, values, leaf.next()))
  }

  pub async fn sync(&mut self) -> Result<()> {
    self.pool.sync().await?;
    Ok(())
  }
}
