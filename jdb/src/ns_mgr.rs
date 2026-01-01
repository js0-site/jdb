//! Namespace manager for multi-tenant isolation
//! 命名空间管理器，用于多租户隔离
//!
//! Each namespace (site_id, user_id) has its own directory and LSM-Tree index.
//! 每个命名空间 (site_id, user_id) 有独立目录和 LSM-Tree 索引。

use std::path::{Path, PathBuf};

use hashlink::lru_cache::Entry as LruEntry;
use jdb_lru::Lru;

use crate::{Conf, Index, Result};

/// Namespace identifier
/// 命名空间标识符
#[derive(Clone, Copy, Hash, Eq, PartialEq, Debug)]
pub struct NsId {
  pub site_id: u64,
  pub user_id: u64,
}

impl NsId {
  /// Create new namespace ID
  /// 创建新命名空间 ID
  #[inline]
  pub const fn new(site_id: u64, user_id: u64) -> Self {
    Self { site_id, user_id }
  }

  /// Get namespace directory path: root/ns/{site_id}/{user_id}/
  /// 获取命名空间目录路径
  #[inline]
  pub fn dir(&self, root: &Path) -> PathBuf {
    root
      .join("ns")
      .join(self.site_id.to_string())
      .join(self.user_id.to_string())
  }
}

/// Per-namespace index (independent LSM-Tree)
/// 每命名空间索引（独立 LSM-Tree）
pub struct NsIndex {
  /// Namespace ID
  /// 命名空间 ID
  pub id: NsId,
  /// LSM-Tree index
  /// LSM-Tree 索引
  pub index: Index,
  /// Has unflushed data
  /// 是否有未刷新数据
  pub dirty: bool,
}

impl NsIndex {
  /// Create new namespace index
  /// 创建新命名空间索引
  #[inline]
  pub fn new(id: NsId, dir: PathBuf, conf: Conf) -> Self {
    Self {
      id,
      index: Index::new(dir, conf),
      dirty: false,
    }
  }

  /// Estimate memory size for LRU eviction (bytes)
  /// 估算内存大小用于 LRU 淘汰（字节）
  #[inline]
  pub fn mem_size(&self) -> u64 {
    // Base overhead + memtable size + sealed memtables
    // 基础开销 + 内存表大小 + 密封内存表
    let base = 256u64;
    let memtable = self.index.memtable_size();
    let sealed = self.index.sealed_count() as u64 * 1024;
    base + memtable + sealed
  }
}

/// Default NsIndex LRU cache capacity
/// 默认 NsIndex LRU 缓存容量
const DEFAULT_NS_CACHE_CAP: usize = 1024;

/// Namespace manager with LRU eviction
/// 命名空间管理器（LRU 淘汰）
pub struct NsMgr {
  /// Root directory
  /// 根目录
  root: PathBuf,
  /// Active NsIndex cache (LRU)
  /// 活跃 NsIndex 缓存（LRU）
  cache: Lru<NsId, NsIndex>,
  /// Configuration
  /// 配置
  conf: Conf,
}

impl NsMgr {
  /// Create new namespace manager
  /// 创建新命名空间管理器
  pub fn new(root: PathBuf, conf: Conf) -> Self {
    Self {
      root,
      cache: Lru::new(DEFAULT_NS_CACHE_CAP),
      conf,
    }
  }

  /// Create with custom cache capacity
  /// 使用自定义缓存容量创建
  pub fn with_cache_cap(root: PathBuf, conf: Conf, cache_cap: usize) -> Self {
    Self {
      root,
      cache: Lru::new(cache_cap.max(4)),
      conf,
    }
  }

  /// Get or load NsIndex (lazy loading)
  /// 获取或加载 NsIndex（懒加载）
  pub async fn get(&mut self, id: NsId) -> Result<&mut NsIndex> {
    // Check if already in cache
    // 检查是否已在缓存中
    match self.cache.0.entry(id) {
      LruEntry::Occupied(entry) => Ok(entry.into_mut()),
      LruEntry::Vacant(entry) => {
        // Load or create NsIndex
        // 加载或创建 NsIndex
        let dir = id.dir(&self.root);
        let ns_index = NsIndex::new(id, dir, self.conf.clone());
        Ok(entry.insert(ns_index))
      }
    }
  }

  /// Get NsIndex if exists in cache (no loading)
  /// 获取 NsIndex（如果在缓存中，不加载）
  #[inline]
  pub fn peek(&mut self, id: &NsId) -> Option<&NsIndex> {
    self.cache.0.get(id)
  }

  /// Get mutable NsIndex if exists in cache (no loading)
  /// 获取可变 NsIndex（如果在缓存中，不加载）
  #[inline]
  pub fn peek_mut(&mut self, id: &NsId) -> Option<&mut NsIndex> {
    self.cache.0.get_mut(id)
  }

  /// Remove namespace from cache
  /// 从缓存移除命名空间
  #[inline]
  pub fn remove(&mut self, id: &NsId) -> Option<NsIndex> {
    self.cache.0.remove(id)
  }

  /// Delete namespace (remove directory)
  /// 删除命名空间（删除目录）
  pub async fn drop(&mut self, id: NsId) -> Result<()> {
    // Remove from cache first
    // 先从缓存移除
    self.cache.0.remove(&id);

    // Delete directory using std::fs (compio doesn't have remove_dir_all)
    // 使用 std::fs 删除目录（compio 没有 remove_dir_all）
    let dir = id.dir(&self.root);
    if dir.exists() {
      std::fs::remove_dir_all(&dir)?;
    }

    Ok(())
  }

  /// Flush all dirty NsIndex
  /// Flush 所有脏 NsIndex
  pub async fn flush_all(&mut self) -> Result<()> {
    for (_, ns_index) in self.cache.0.iter_mut() {
      if ns_index.dirty {
        ns_index.index.seal_memtable();
        while ns_index.index.sealed_count() > 0 {
          ns_index.index.flush_sealed().await?;
        }
        ns_index.dirty = false;
      }
    }
    Ok(())
  }

  /// Check if cache is empty
  /// 检查缓存是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.cache.0.is_empty()
  }

  /// Get cache entry count
  /// 获取缓存条目数量
  #[inline]
  pub fn len(&self) -> usize {
    self.cache.0.len()
  }

  /// Get root directory
  /// 获取根目录
  #[inline]
  pub fn root(&self) -> &Path {
    &self.root
  }

  /// Iterate all cached NsIndex
  /// 迭代所有缓存的 NsIndex
  #[inline]
  pub fn iter(&self) -> impl Iterator<Item = (&NsId, &NsIndex)> {
    self.cache.0.iter()
  }

  /// Iterate all cached NsIndex mutably
  /// 可变迭代所有缓存的 NsIndex
  #[inline]
  pub fn iter_mut(&mut self) -> impl Iterator<Item = (&NsId, &mut NsIndex)> {
    self.cache.0.iter_mut()
  }
}
