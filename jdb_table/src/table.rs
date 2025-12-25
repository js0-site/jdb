//! Table implementation / 表实现

use std::{path::Path, rc::Rc};

use bytes::Bytes;
use jdb_page::PageStore;
use jdb_trait::{Order, Rev, ValRef};
use jdb_tree::{BTree, PageId};
use jdb_vlog::VLog;

use crate::{Conf, Error, Keep, Result};

/// Commit record / 提交记录
#[derive(Debug, Clone, Copy)]
pub struct Commit {
  pub rev: Rev,
  pub root: PageId,
}

/// Table (sub-database) / 表（子数据库）
pub struct Table {
  tree: BTree,
  vlog: Rc<VLog>,
  commit: Commit,
  keep: Keep,
}

impl Table {
  /// Open or create table / 打开或创建表
  pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
    let dir = dir.as_ref();

    // Open page store / 打开页存储
    let page_path = dir.join("pages.jdb");
    let store = PageStore::open(&page_path).await?;

    // Open vlog / 打开值日志
    let vlog_dir = dir.join("vlog");
    let vlog = Rc::new(VLog::open(&vlog_dir).await?);

    // Create or load tree / 创建或加载树
    let tree = if store.page_count() <= 1 {
      BTree::new(store).await?
    } else {
      // TODO: load root from meta / 从元数据加载根
      BTree::open(store, 1)
    };

    let commit = Commit {
      rev: 0,
      root: tree.root(),
    };

    Ok(Self {
      tree,
      vlog,
      commit,
      keep: Keep::default(),
    })
  }

  /// Create from existing components / 从已有组件创建
  pub fn from_parts(tree: BTree, vlog: Rc<VLog>, commit: Commit) -> Self {
    Self {
      tree,
      vlog,
      commit,
      keep: Keep::default(),
    }
  }

  /// Create with keep policy / 带保留策略创建
  pub fn with_keep(tree: BTree, vlog: Rc<VLog>, commit: Commit, keep: Keep) -> Self {
    Self {
      tree,
      vlog,
      commit,
      keep,
    }
  }

  /// Apply configuration / 应用配置
  pub fn conf(&mut self, conf: &[Conf]) {
    for c in conf {
      let Conf::Keep(k) = c;
      self.keep = *k;
    }
  }

  /// Get keep policy / 获取保留策略
  pub fn keep(&self) -> Keep {
    self.keep
  }

  /// Get current revision / 获取当前修订号
  pub fn rev(&self) -> Rev {
    self.commit.rev
  }

  /// Get current root / 获取当前根
  pub fn root(&self) -> PageId {
    self.commit.root
  }

  /// Put key-value / 写入键值
  pub async fn put(
    &mut self,
    key: impl AsRef<[u8]>,
    val: impl AsRef<[u8]>,
  ) -> Result<Option<ValRef>> {
    let key = key.as_ref();
    let val = val.as_ref();

    // Get old value ref / 获取旧值引用
    let old_ref = self.tree.get(key).await?;

    // Write to vlog / 写入值日志
    let new_ref = self.vlog.append(val, old_ref.as_ref()).await?;

    // Insert to tree / 插入树
    let new_root = self.tree.put(key, new_ref).await?;

    // Update commit / 更新提交
    self.commit.rev += 1;
    self.commit.root = new_root;

    Ok(old_ref)
  }

  /// Get value / 获取值
  pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
    let key = key.as_ref();

    // Find in tree / 在树中查找
    let vref = match self.tree.get(key).await? {
      Some(v) => v,
      None => return Ok(None),
    };

    // Check tombstone / 检查墓碑
    if vref.is_tombstone() {
      return Ok(None);
    }

    // Read from vlog / 从值日志读取
    self.vlog.get(&vref).await.map_err(Error::from)
  }

  /// Remove key / 删除键
  pub async fn rm(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
    let key = key.as_ref();

    // Get old value ref / 获取旧值引用
    let old_ref = self.tree.get(key).await?;

    // Write tombstone to vlog / 写入墓碑
    let tomb_ref = self.vlog.append_tombstone(old_ref.as_ref()).await?;

    // Update tree with tombstone / 用墓碑更新树
    let new_root = self.tree.put(key, tomb_ref).await?;

    // Update commit / 更新提交
    self.commit.rev += 1;
    self.commit.root = new_root;

    Ok(())
  }

  /// Get value by ref / 根据引用获取值
  pub async fn val(&self, vref: ValRef) -> Result<Option<Bytes>> {
    if vref.is_tombstone() {
      return Ok(None);
    }
    self.vlog.get(&vref).await.map_err(Error::from)
  }

  /// Get history of key / 获取键的历史
  pub async fn history(&self, key: impl AsRef<[u8]>) -> Result<Vec<ValRef>> {
    let key = key.as_ref();

    // Get current ref / 获取当前引用
    let mut current = match self.tree.get(key).await? {
      Some(v) => v,
      None => return Ok(Vec::new()),
    };

    let mut history = Vec::new();

    // Follow prev chain / 沿前驱链遍历
    loop {
      if let Some((_, full, _ts)) = self.vlog.get_full(&current).await? {
        history.push(full);
        if !full.has_prev() {
          break;
        }
        current = ValRef {
          file_id: full.prev_file_id,
          offset: full.prev_offset,
          prev_file_id: 0,
          prev_offset: 0,
        };
      } else {
        history.push(current);
        break;
      }
    }

    Ok(history)
  }

  /// Scan from key / 从键开始扫描
  pub async fn scan(&self, start: impl AsRef<[u8]>, order: Order) -> Result<Vec<(Bytes, Bytes)>> {
    let start = start.as_ref();
    let (_, leaf) = self.tree.find_leaf(start).await?;

    let mut results = Vec::new();

    // Get all keys from leaf / 从叶子获取所有键
    for i in 0..leaf.suffixes.len() {
      let key = leaf.key(i);
      let vref = leaf.vals[i];

      if vref.is_tombstone() {
        continue;
      }

      // Check start condition / 检查起始条件
      match order {
        Order::Asc => {
          if key.as_ref() < start {
            continue;
          }
        }
        Order::Desc => {
          if key.as_ref() > start {
            continue;
          }
        }
      }

      if let Some(val) = self.vlog.get(&vref).await? {
        results.push((key, val));
      }
    }

    // Sort by order / 按顺序排序
    match order {
      Order::Asc => results.sort_by(|a, b| a.0.cmp(&b.0)),
      Order::Desc => results.sort_by(|a, b| b.0.cmp(&a.0)),
    }

    Ok(results)
  }

  /// Fork at revision / 在修订号处 Fork
  pub fn fork(&self, _rev: Rev, _order: Order) -> Option<Self> {
    // TODO: implement fork with commit chain / 实现带提交链的 fork
    None
  }

  /// Sync to disk / 同步到磁盘
  pub async fn sync(&self) -> Result<()> {
    self.tree.sync().await?;
    self.vlog.sync().await?;
    Ok(())
  }

  /// Scan keys for GC marking / 扫描键用于 GC 标记
  /// Returns (keys, vrefs, next_cursor)
  pub async fn scan_keys_for_gc(
    &self,
    cursor: Option<&Bytes>,
    limit: usize,
  ) -> Result<(Vec<Bytes>, Vec<ValRef>, Option<Bytes>)> {
    self
      .tree
      .scan_keys(cursor.map(|c| c.as_ref()), limit)
      .await
      .map_err(Error::from)
  }

  /// Get VLog reference for history traversal / 获取 VLog 引用用于历史遍历
  pub fn vlog(&self) -> &VLog {
    &self.vlog
  }
}
