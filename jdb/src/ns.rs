//! Namespace handle for multi-tenant operations
//! 命名空间句柄，用于多租户操作
//!
//! Provides get/put/del operations directly on NsIndex without prefix encoding.
//! 直接在 NsIndex 上提供 get/put/del 操作，无需前缀编码。

use std::ops::Bound;

use jdb_base::Pos;

use crate::{Entry, MergeIter, NsId, NsMgr, Result};

/// Namespace handle (no prefix encoding needed)
/// 命名空间句柄（无需前缀编码）
pub struct Ns<'a> {
  /// Namespace manager reference
  /// 命名空间管理器引用
  ns_mgr: &'a mut NsMgr,
  /// Namespace ID
  /// 命名空间 ID
  id: NsId,
}

impl<'a> Ns<'a> {
  /// Create new namespace handle
  /// 创建新命名空间句柄
  #[inline]
  pub fn new(ns_mgr: &'a mut NsMgr, id: NsId) -> Self {
    Self { ns_mgr, id }
  }

  /// Get namespace ID
  /// 获取命名空间 ID
  #[inline]
  pub fn id(&self) -> NsId {
    self.id
  }

  /// Get value by key (no prefix)
  /// 按 key 获取值（无前缀）
  pub async fn get(&mut self, key: &[u8]) -> Result<Option<Entry>> {
    let ns_index = self.ns_mgr.get(self.id).await?;
    ns_index.index.get(key).await
  }

  /// Put key-value (no prefix)
  /// 写入 key-value（无前缀）
  pub async fn put(&mut self, key: &[u8], pos: Pos) -> Result<()> {
    let ns_index = self.ns_mgr.get(self.id).await?;
    ns_index.index.put(key.into(), pos);
    ns_index.dirty = true;
    Ok(())
  }

  /// Delete key (no prefix)
  /// 删除 key（无前缀）
  pub async fn del(&mut self, key: &[u8]) -> Result<()> {
    let ns_index = self.ns_mgr.get(self.id).await?;
    ns_index.index.del(key.into());
    ns_index.dirty = true;
    Ok(())
  }

  /// Range iteration (no prefix handling)
  /// 范围迭代（无前缀处理）
  pub async fn range(&mut self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<MergeIter> {
    let ns_index = self.ns_mgr.get(self.id).await?;
    ns_index.index.range(start, end).await
  }

  /// Prefix iteration (no prefix handling)
  /// 前缀迭代（无前缀处理）
  pub async fn prefix(&mut self, prefix: &[u8]) -> Result<MergeIter> {
    let ns_index = self.ns_mgr.get(self.id).await?;
    ns_index.index.prefix(prefix).await
  }

  /// Iterate all entries
  /// 迭代所有条目
  pub async fn iter(&mut self) -> Result<MergeIter> {
    let ns_index = self.ns_mgr.get(self.id).await?;
    ns_index.index.iter().await
  }

  /// Check if memtable should be flushed
  /// 检查内存表是否应该刷新
  pub async fn should_flush(&mut self) -> Result<bool> {
    let ns_index = self.ns_mgr.get(self.id).await?;
    Ok(ns_index.index.should_flush())
  }

  /// Seal current memtable
  /// 密封当前内存表
  pub async fn seal_memtable(&mut self) -> Result<()> {
    let ns_index = self.ns_mgr.get(self.id).await?;
    ns_index.index.seal_memtable();
    Ok(())
  }

  /// Flush oldest sealed memtable to SSTable
  /// 将最旧的密封内存表刷新到 SSTable
  pub async fn flush_sealed(&mut self) -> Result<Option<u64>> {
    let ns_index = self.ns_mgr.get(self.id).await?;
    let result = ns_index.index.flush_sealed().await?;
    if result.is_some() {
      ns_index.dirty = false;
    }
    Ok(result)
  }

  /// Run compaction if needed
  /// 如果需要则运行压缩
  pub async fn maybe_compact(&mut self) -> Result<bool> {
    let ns_index = self.ns_mgr.get(self.id).await?;
    ns_index.index.maybe_compact().await
  }
}

/// Site-level operations
/// 站点级操作
pub struct Site<'a> {
  /// Namespace manager reference
  /// 命名空间管理器引用
  ns_mgr: &'a mut NsMgr,
  /// Site ID
  /// 站点 ID
  site_id: u64,
}

impl<'a> Site<'a> {
  /// Create new site handle
  /// 创建新站点句柄
  #[inline]
  pub fn new(ns_mgr: &'a mut NsMgr, site_id: u64) -> Self {
    Self { ns_mgr, site_id }
  }

  /// Get site ID
  /// 获取站点 ID
  #[inline]
  pub fn site_id(&self) -> u64 {
    self.site_id
  }

  /// Get namespace handle for a user
  /// 获取用户的命名空间句柄
  #[inline]
  pub fn ns(&mut self, user_id: u64) -> Ns<'_> {
    Ns::new(self.ns_mgr, NsId::new(self.site_id, user_id))
  }

  /// List all user_ids under this site (by scanning directory)
  /// 列出此站点下所有 user_id（通过扫描目录）
  pub async fn list_users(&self) -> Result<Vec<u64>> {
    let site_dir = self.ns_mgr.root().join("ns").join(self.site_id.to_string());

    let mut users = Vec::new();

    if compio::fs::metadata(&site_dir).await.is_err() {
      return Ok(users);
    }

    // Use std::fs for directory listing (compio doesn't have read_dir)
    // 使用 std::fs 列出目录（compio 没有 read_dir）
    if let Ok(entries) = std::fs::read_dir(&site_dir) {
      for entry in entries.flatten() {
        if let Ok(file_type) = entry.file_type()
          && file_type.is_dir()
          && let Some(name) = entry.file_name().to_str()
          && let Ok(user_id) = name.parse::<u64>()
        {
          users.push(user_id);
        }
      }
    }

    users.sort_unstable();
    Ok(users)
  }

  /// Delete all namespaces under this site
  /// 删除此站点下所有命名空间
  pub async fn drop_all(&mut self) -> Result<()> {
    let users = self.list_users().await?;
    for user_id in users {
      let id = NsId::new(self.site_id, user_id);
      self.ns_mgr.drop(id).await?;
    }

    // Remove site directory if empty
    // 如果为空则删除站点目录
    let site_dir = self.ns_mgr.root().join("ns").join(self.site_id.to_string());
    let _ = compio::fs::remove_dir(&site_dir).await;

    Ok(())
  }
}
