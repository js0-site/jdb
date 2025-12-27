//! GC (Garbage Collection) / 垃圾回收
//!
//! ## Flow / 流程
//!
//! 1. Create new WAL with MaxSize(u64::MAX) to disable auto-rotate
//!    创建新 WAL，MaxSize 设为 u64::MAX 禁用自动轮转
//! 2. Merge multiple old WALs, write live entries to new WAL
//!    合并多个旧 WAL，将有效条目写入新 WAL
//! 3. Call `Gcable::batch_update()` to update index
//!    调用 `Gcable::batch_update()` 更新索引
//! 4. Delete old WAL files / 删除旧 WAL 文件
//!
//! Note: GC uses independent WAL, won't affect main WAL writes.
//! 注意：GC 使用独立 WAL，不影响主 WAL 写入。

use std::{
  fs::{self, File},
  future::Future,
  path::PathBuf,
};

use fd_lock::RwLock;

use crate::{Conf, Pos, Result, Wal};

/// Lock file extension / 锁文件扩展名
const LOCK_EXT: &str = ".lock";
/// GC ratio numerator / GC 比例分子
const GC_RATIO_NUM: usize = 3;
/// GC ratio denominator / GC 比例分母
const GC_RATIO_DEN: usize = 4;

/// Position mapping entry / 位置映射条目
pub struct PosMap {
  pub key: Vec<u8>,
  pub old: Pos,
  pub new: Pos,
}

/// GC trait / GC 特征
pub trait Gcable {
  /// Check if key is deleted / 检查键是否已删除
  fn is_rm(&self, key: &[u8]) -> impl Future<Output = bool> + Send;
  /// Batch update index, return true if success / 批量更新索引，成功返回 true
  fn batch_update(
    &self,
    mapping: impl IntoIterator<Item = PosMap>,
  ) -> impl Future<Output = bool> + Send;
}

/// GC state / GC 状态
pub struct GcState {
  dir: PathBuf,
  /// Max WAL size for new merged file / 合并后文件最大大小
  max_size: u64,
}

/// File lock guard / 文件锁守卫
pub struct FileLock {
  _lock: RwLock<File>,
  path: PathBuf,
}

impl Drop for FileLock {
  fn drop(&mut self) {
    let _ = fs::remove_file(&self.path);
  }
}

impl GcState {
  /// Create GC state / 创建 GC 状态
  pub fn new(dir: impl Into<PathBuf>, max_size: u64) -> Self {
    Self {
      dir: dir.into(),
      max_size,
    }
  }

  /// Get oldest 75% WAL IDs / 获取最旧的 75% WAL ID
  fn oldest_75(ids: &[u64], cur_id: u64) -> Vec<u64> {
    let mut old: Vec<u64> = ids.iter().copied().filter(|&id| id < cur_id).collect();
    old.sort_unstable();
    // ceil(len * 3/4) = (len * 3 + 3) / 4
    let n = (old.len() * GC_RATIO_NUM).div_ceil(GC_RATIO_DEN);
    old.truncate(n);
    old
  }

  /// Get lock file path / 获取锁文件路径
  fn lock_path(&self, id: u64) -> PathBuf {
    self.dir.join(format!("{id}{LOCK_EXT}"))
  }

  /// Try acquire exclusive lock / 尝试获取排他锁
  fn try_lock(&self, id: u64) -> Option<FileLock> {
    let path = self.lock_path(id);
    let file = File::create(&path).ok()?;
    let mut lock = RwLock::new(file);
    let _ = lock.try_write().ok()?;
    Some(FileLock { _lock: lock, path })
  }
}

impl Wal {
  /// GC and merge multiple WAL files / GC 并合并多个 WAL 文件
  ///
  /// Returns (reclaimed, total) / 返回 (回收数, 总数)
  pub async fn gc_merge<T: Gcable>(
    &mut self,
    ids: &[u64],
    checker: &T,
    state: &mut GcState,
  ) -> Result<(usize, usize)> {
    if ids.is_empty() {
      return Ok((0, 0));
    }

    // Acquire locks for all files / 获取所有文件的锁
    let mut locks = Vec::with_capacity(ids.len());
    for &id in ids {
      if id >= self.cur_id() {
        return Err(crate::Error::CannotRemoveCurrent);
      }
      let lock = state.try_lock(id).ok_or(crate::Error::Locked)?;
      locks.push(lock);
    }

    // Create new WAL with u64::MAX to disable rotate / 创建新 WAL，禁用轮转
    let mut gc_wal = Wal::new(&state.dir, &[Conf::MaxSize(u64::MAX)]);
    gc_wal.open().await?;

    let mut mapping: Vec<PosMap> = Vec::with_capacity(ids.len() * 8);
    let mut reclaimed = 0usize;
    let mut total = 0usize;
    let mut val_buf = Vec::with_capacity(crate::INFILE_MAX);

    // Process each WAL file / 处理每个 WAL 文件
    for &id in ids {
      let mut iter = self.iter_entries(id).await?;
      while let Some((pos, head)) = iter.next().await? {
        total += 1;
        let old_pos = Pos::new(id, pos);
        let key = self.head_key(&head).await?;

        if checker.is_rm(&key).await {
          reclaimed += 1;
          continue;
        }

        // FILE mode: reuse file_id directly, no copy / FILE 模式：直接复用 file_id，无需复制
        let new_pos = if head.val_flag.is_file() {
          let file_id = head.val_pos().id();
          gc_wal
            .put_with_file(&key, file_id, head.val_len.get(), head.val_crc32())
            .await?
        } else if head.val_flag.is_inline() {
          // Optimization: use slice directly for inline value, avoid allocation / 优化：内联值直接用切片，避免分配
          gc_wal.put(&key, head.val_data()).await?
        } else {
          val_buf.clear();
          self.read_val_into(&head, &mut val_buf).await?;
          gc_wal.put(&key, &val_buf).await?
        };

        mapping.push(PosMap {
          key,
          old: old_pos,
          new: new_pos,
        });
      }
    }

    // Sync new WAL / 同步新 WAL
    gc_wal.sync_all().await?;

    // Check if need rotate (exceeds max_size) / 检查是否需要轮转
    if gc_wal.cur_pos() > state.max_size {
      gc_wal.rotate().await?;
    }

    // Batch update index / 批量更新索引
    if !mapping.is_empty() && !checker.batch_update(mapping).await {
      return Err(crate::Error::UpdateFailed);
    }

    // Delete old WAL files (keep bin files, they are reused) / 删除旧 WAL 文件（保留 bin 文件，已被复用）
    for &id in ids {
      self.remove(id)?;
    }

    Ok((reclaimed, total))
  }

}
