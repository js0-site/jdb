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
/// GC ratio: only gc oldest 75% files / GC 比例：只回收最旧的 75% 文件
const GC_RATIO: f64 = 0.75;

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
    let mut old: Vec<u64> = ids.iter().filter(|&&id| id < cur_id).copied().collect();
    old.sort_unstable();
    let n = ((old.len() as f64) * GC_RATIO).ceil() as usize;
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

    let mut mapping: Vec<PosMap> = Vec::new();
    let mut reclaimed = 0usize;
    let mut total = 0usize;

    // Process each WAL file / 处理每个 WAL 文件
    for &id in ids {
      let mut entries = Vec::new();
      self
        .scan(id, |pos, head| {
          entries.push((pos, *head));
          true
        })
        .await?;

      total += entries.len();

      for (pos, head) in entries {
        let old_pos = Pos::new(id, pos);
        let key = self.head_key(&head).await?;

        if checker.is_rm(&key).await {
          reclaimed += 1;
          continue;
        }

        let val = self.head_val(&head).await?;
        let new_pos = gc_wal.put(&key, &val).await?;
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

    // Delete old WAL files / 删除旧 WAL 文件
    for &id in ids {
      self.remove(id)?;
    }

    Ok((reclaimed, total))
  }

  /// Auto GC: merge oldest 75% WALs
  /// 自动 GC：合并最旧的 75% WAL
  pub async fn gc_auto<T: Gcable>(&mut self, checker: &T, state: &mut GcState) -> Result<()> {
    let cur_id = self.cur_id();
    let ids: Vec<u64> = self.iter().collect();
    let to_gc = GcState::oldest_75(&ids, cur_id);

    if to_gc.is_empty() {
      return Ok(());
    }

    let (reclaimed, total) = match self.gc_merge(&to_gc, checker, state).await {
      Ok(r) => r,
      Err(crate::Error::Locked) => return Ok(()),
      Err(e) => return Err(e),
    };

    // Log ratio for debugging / 记录回收率用于调试
    let ratio = if total > 0 {
      reclaimed as f64 / total as f64
    } else {
      0.0
    };
    log::debug!("GC: reclaimed {reclaimed}/{total} ({ratio:.2})");

    Ok(())
  }
}
