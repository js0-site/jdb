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

use std::{fs, future::Future, path::PathBuf};

use fd_lock::RwLock;
use hipstr::HipByt;

use crate::{
  Conf, Error, Pos, Result, Wal, WalNoCache,
  wal::consts::{LOCK_SUBDIR, WAL_LOCK_TYPE},
};

/// Default mapping capacity / 默认映射容量
const MAP_CAP: usize = 1024;

/// Position mapping entry / 位置映射条目
#[derive(Debug, Clone)]
pub struct PosMap {
  // 使用 HipByt 替代 Vec<u8>，对小 Key (<=23 bytes) 避免堆分配，大 Key 引用计数减少 clone 开销
  pub key: HipByt<'static>,
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
  // Box to allow self-referential struct / Box 允许自引用结构
  // Guard dropped before lock via drop order / Guard 通过 drop 顺序在 lock 之前释放
  _guard: fd_lock::RwLockWriteGuard<'static, fs::File>,
  _lock: Box<RwLock<fs::File>>,
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
    self
      .dir
      .join(LOCK_SUBDIR)
      .join(WAL_LOCK_TYPE)
      .join(Wal::encode_id(id))
  }

  /// Try acquire exclusive lock / 尝试获取排他锁
  fn try_lock(&self, id: u64) -> Option<FileLock> {
    let path = self.lock_path(id);

    // Ensure lock directory exists / 确保锁目录存在
    if let Some(parent) = path.parent() {
      let _ = fs::create_dir_all(parent);
    }

    let file = fs::OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(&path)
      .ok()?;

    // Box to get stable address / Box 获取稳定地址
    let lock = Box::new(RwLock::new(file));
    // SAFETY: Box provides stable address, guard lives shorter than lock
    // 安全：Box 提供稳定地址，guard 生命周期短于 lock
    let lock_ptr: *mut RwLock<fs::File> = Box::into_raw(lock);
    let guard = unsafe { (*lock_ptr).try_write().ok()? };
    // Transmute to 'static, safe because we control lifetime via struct
    // 转换为 'static，安全因为我们通过结构体控制生命周期
    let guard: fd_lock::RwLockWriteGuard<'static, fs::File> = unsafe { std::mem::transmute(guard) };
    let lock = unsafe { Box::from_raw(lock_ptr) };

    Some(FileLock {
      _guard: guard,
      _lock: lock,
      path,
    })
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
      let lock = state.try_lock(id).ok_or(Error::Locked)?;
      locks.push(lock);
    }

    // Create new WAL without cache for GC / 创建无缓存的 GC WAL
    let mut gc_wal = WalNoCache::new(&state.dir, &[Conf::MaxSize(u64::MAX)]);
    gc_wal.open().await?;

    let mut mapping: Vec<PosMap> = Vec::with_capacity(MAP_CAP);
    let mut reclaimed = 0usize;
    let mut total = 0usize;
    // Reuse buffers: capacity grows to max key/val size, avoiding repeated malloc
    // 复用缓冲区：容量增长到最大 key/val 大小，避免重复 malloc
    let mut val_buf = Vec::new();
    let mut key_buf = Vec::new();
    // Collect orphaned bin files for deletion / 收集需要删除的孤儿 Bin 文件
    let mut stale_bins = Vec::new();

    // Process each WAL file / 处理每个 WAL 文件
    for &id in ids {
      let mut iter = self.iter_entries(id).await?;

      while let Some((pos, head)) = iter.next().await? {
        total += 1;
        let old_pos = Pos::new(id, pos);

        // read_key_into clears buf internally / read_key_into 内部会清空 buf
        self.read_key_into(&head, &mut key_buf).await?;

        if checker.is_rm(&key_buf).await {
          // Collect stale bin files for FILE mode / 收集 FILE 模式的过期 bin 文件
          if head.key_flag.is_file() {
            stale_bins.push(head.key_pos().id());
          }
          if head.val_flag.is_file() {
            stale_bins.push(head.val_pos().id());
          }
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
          // Zero-copy for small keys or efficient ref-counting for large keys
          // 小 Key 零拷贝，大 Key 高效引用计数
          key: HipByt::from(&key_buf[..]),
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

    // Remove stale bin files (deduplicate first) / 删除过期的 Bin 文件（先去重）
    stale_bins.sort_unstable();
    stale_bins.dedup();
    for id in stale_bins {
      let _ = self.rm_bin(id).await;
    }

    // Delete old WAL files (keep bin files, they are reused)
    // 删除旧 WAL 文件（保留 bin 文件，已被复用）
    for &id in ids {
      self.rm(id).await?;
    }

    // Drop locks explicitly / 显式释放锁
    drop(locks);

    Ok((reclaimed, total))
  }
}
