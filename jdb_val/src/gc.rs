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

use std::{fs, future::Future, mem, path::PathBuf};

use compio_fs::OpenOptions;
use fd_lock::RwLock;

use crate::{Conf, Error, Pos, Result, Wal};

/// Lock file extension / 锁文件扩展名
const LOCK_EXT: &str = ".lock";

/// Default mapping capacity / 默认映射容量
const MAP_CAP: usize = 1024;

/// Position mapping entry / 位置映射条目
#[derive(Debug, Clone)]
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
}

/// File lock guard / 文件锁守卫
pub struct FileLock {
  _lock: RwLock<compio_fs::File>,
  path: PathBuf,
}

impl Drop for FileLock {
  fn drop(&mut self) {
    let _ = fs::remove_file(&self.path);
  }
}

impl GcState {
  /// Create GC state / 创建 GC 状态
  pub fn new(dir: impl Into<PathBuf>) -> Self {
    Self { dir: dir.into() }
  }

  /// Get lock file path / 获取锁文件路径
  fn lock_path(&self, id: u64) -> PathBuf {
    self.dir.join(format!("{id}{LOCK_EXT}"))
  }

  /// Try acquire exclusive lock / 尝试获取排他锁
  async fn try_lock(&self, id: u64) -> Option<FileLock> {
    let path = self.lock_path(id);
    // Use async open to avoid blocking runtime / 使用异步打开避免阻塞运行时
    let file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await
      .ok()?;
    let mut lock = RwLock::new(file);
    // Must hold the guard to keep lock / 必须持有 guard 保持锁定
    // fd_lock keeps lock while RwLock exists / fd_lock 在 RwLock 存在时保持锁定
    if lock.try_write().is_err() {
      return None;
    }
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
        return Err(Error::CannotRemoveCurrent);
      }
      let lock = state.try_lock(id).await.ok_or(Error::Locked)?;
      locks.push(lock);
    }

    // Create new WAL with u64::MAX to disable rotate / 创建新 WAL，禁用轮转
    // Disable caches for GC writer to save memory / 禁用 GC 写入器的缓存以节省内存
    let mut gc_wal = Wal::new(
      &state.dir,
      &[
        Conf::MaxSize(u64::MAX),
        Conf::HeadLru(0),
        Conf::DataLru(0),
        Conf::FileLru(0),
      ],
    );
    gc_wal.open().await?;

    let mut mapping: Vec<PosMap> = Vec::with_capacity(MAP_CAP);
    let mut reclaimed = 0usize;
    let mut total = 0usize;
    // Reuse buffers: capacity grows to max key/val size, avoiding repeated malloc
    // 复用缓冲区：容量增长到最大 key/val 大小，避免重复 malloc
    let mut val_buf = Vec::new();
    let mut key_buf = Vec::new();

    // Process each WAL file / 处理每个 WAL 文件
    for &id in ids {
      let mut iter = self.iter_entries(id).await?;

      while let Some((pos, head)) = iter.next().await? {
        total += 1;
        let old_pos = Pos::new(id, pos);

        // read_key_into clears buf internally / read_key_into 内部会清空 buf
        self.read_key_into(&head, &mut key_buf).await?;

        if checker.is_rm(&key_buf).await {
          reclaimed += 1;
          continue;
        }

        // FILE mode: reuse file_id directly, no copy / FILE 模式：直接复用 file_id，无需复制
        let new_pos = if head.val_flag.is_file() {
          let file_id = head.val_pos().id();
          gc_wal
            .put_with_file(&key_buf, file_id, head.val_len.get(), head.val_crc32())
            .await?
        } else if head.val_flag.is_inline() {
          gc_wal.put(&key_buf, head.val_data()).await?
        } else {
          // read_val_into clears buf internally / read_val_into 内部会清空 buf
          self.read_val_into(&head, &mut val_buf).await?;
          gc_wal.put(&key_buf, &val_buf).await?
        };

        mapping.push(PosMap {
          key: mem::take(&mut key_buf),
          old: old_pos,
          new: new_pos,
        });
      }
    }

    // Sync new WAL / 同步新 WAL
    gc_wal.sync_all().await?;

    // Batch update index / 批量更新索引
    if !mapping.is_empty() && !checker.batch_update(mapping).await {
      return Err(Error::UpdateFailed);
    }

    // Delete old WAL files (keep bin files, they are reused)
    // 删除旧 WAL 文件（保留 bin 文件，已被复用）
    for &id in ids {
      self.remove(id).await?;
    }

    // Drop locks explicitly / 显式释放锁
    drop(locks);

    Ok((reclaimed, total))
  }
}
