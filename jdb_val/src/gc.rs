//! GC (Garbage Collection)
//! 垃圾回收
//!
//! ## Flow
//! 流程
//!
//! 1. Create new WAL in gc dir with MaxSize(u64::MAX) to disable auto-rotate
//!    在 gc 目录创建新 WAL，MaxSize 设为 u64::MAX 禁用自动轮转
//! 2. Merge multiple old WALs, write live entries to new WAL
//!    合并多个旧 WAL，将有效条目写入新 WAL
//! 3. Call `Gcable::batch_update()` to update index
//!    调用 `Gcable::batch_update()` 更新索引
//! 4. Move gc WAL to wal dir, delete old WAL files
//!    将 gc WAL 移动到 wal 目录，删除旧 WAL 文件
//!
//! ## GC ID Strategy
//! GC ID 策略
//!
//! GC WAL ID starts from (current_id - 1) and searches backward until finding
//! a non-existent file.
//! GC WAL ID 从 (当前 ID - 1) 开始向前搜索，直到找到不存在的文件。
//!
//! ## GC Thread Scheduling
//! GC 线程调度
//!
//! GC runs in separate low-priority thread bound to least busy CPU core.
//! GC 在独立低优先级线程运行，绑定到最闲的 CPU 核心。

use std::{fs, future::Future, path::PathBuf, thread::JoinHandle, time::Duration};

use hipstr::HipByt;
use jdb_lock::gc::Lock as GcLock;

use crate::{
  Conf, Error, GcTrait, Pos, Result, Store, Wal, WalNoCache,
  wal::{
    consts::{GC_SUBDIR, LOCK_SUBDIR, WAL_LOCK_TYPE, WAL_SUBDIR},
    lz4,
  },
};

/// Default GC with LZ4 compression
/// 默认 GC（带 LZ4 压缩）
pub struct DefaultGc;

impl GcTrait for DefaultGc {
  fn process(&mut self, store: Store, data: &[u8], buf: &mut Vec<u8>) -> (Store, Option<usize>) {
    // Skip if already compressed or probed
    // 跳过已压缩或已探测
    if store.is_compressed() || store.is_probed() {
      return (store, None);
    }

    // Try compress
    // 尝试压缩
    if let Some(len) = lz4::try_compress(data, buf) {
      (store.to_lz4(), Some(len))
    } else {
      // Mark as probed (incompressible)
      // 标记为已探测（不可压缩）
      (store.to_probed(), None)
    }
  }
}

/// Default mapping capacity
/// 默认映射容量
const MAP_CAP: usize = 1024;

/// Position mapping entry
/// 位置映射条目
#[derive(Debug, Clone)]
pub struct PosMap {
  pub key: HipByt<'static>,
  pub old: Pos,
  pub new: Pos,
}

/// Position map trait for GC updates
/// GC 更新的位置映射 trait
pub trait PosMapUpdate: Send + Sync {
  /// Insert or update position
  /// 插入或更新位置
  fn insert(&self, key: &[u8], pos: Pos);
  /// Remove key
  /// 删除键
  fn remove(&self, key: &[u8]);
}

/// GC trait
/// GC 特征
pub trait Gcable {
  /// Check if key is deleted
  /// 检查键是否已删除
  fn is_rm(&self, key: &[u8]) -> impl Future<Output = bool> + Send;

  /// Batch update index, return true if success
  /// 批量更新索引，成功返回 true
  fn batch_update(
    &self,
    mapping: impl IntoIterator<Item = PosMap>,
  ) -> impl Future<Output = bool> + Send;
}

/// GC state
/// GC 状态
pub struct GcState {
  dir: PathBuf,
  gc_dir: PathBuf,
  wal_dir: PathBuf,
}

impl GcState {
  /// Create GC state
  /// 创建 GC 状态
  pub fn new(dir: impl Into<PathBuf>) -> Self {
    let dir = dir.into();
    Self {
      gc_dir: dir.join(GC_SUBDIR),
      wal_dir: dir.join(WAL_SUBDIR),
      dir,
    }
  }

  fn lock_path(&self, id: u64) -> PathBuf {
    self
      .dir
      .join(LOCK_SUBDIR)
      .join(WAL_LOCK_TYPE)
      .join(Wal::encode_id(id))
  }

  fn try_lock(&self, id: u64) -> Result<GcLock> {
    Ok(GcLock::try_new(self.lock_path(id))?)
  }

  /// Find available GC WAL ID
  /// 查找可用的 GC WAL ID
  fn find_gc_id(&self, cur_id: u64) -> u64 {
    if cur_id == 0 {
      return 0;
    }
    let mut id = cur_id - 1;
    while id > 0 {
      let path = self.wal_dir.join(Wal::encode_id(id));
      if !path.exists() {
        return id;
      }
      id -= 1;
    }
    0
  }

  fn gc_wal_path(&self, id: u64) -> PathBuf {
    self.gc_dir.join(WAL_SUBDIR).join(Wal::encode_id(id))
  }

  fn final_wal_path(&self, id: u64) -> PathBuf {
    self.wal_dir.join(Wal::encode_id(id))
  }

  fn move_gc_wal(&self, id: u64) -> Result<()> {
    let src = self.gc_wal_path(id);
    let dst = self.final_wal_path(id);
    fs::rename(src, dst)?;
    Ok(())
  }
}

impl Wal {
  /// GC and merge multiple WAL files
  /// GC 并合并多个 WAL 文件
  pub async fn gc_merge<T: Gcable>(
    &mut self,
    ids: &[u64],
    checker: &T,
    state: &mut GcState,
  ) -> Result<(usize, usize)> {
    if ids.is_empty() {
      return Ok((0, 0));
    }

    let mut locks = Vec::with_capacity(ids.len());
    for &id in ids {
      if id >= self.cur_id() {
        return Err(Error::CannotRemoveCurrent);
      }
      locks.push(state.try_lock(id)?);
    }

    let gc_id = state.find_gc_id(self.cur_id());

    fs::create_dir_all(&state.gc_dir)?;
    let mut gc_wal = WalNoCache::new(&state.gc_dir, &[Conf::MaxSize(u64::MAX)]);
    gc_wal.ider.init(gc_id);
    gc_wal.open().await?;

    let mut mapping: Vec<PosMap> = Vec::with_capacity(MAP_CAP);
    let mut reclaimed = 0usize;
    let mut total = 0usize;
    let mut key_buf = Vec::new();
    let mut stale_bins = Vec::new();

    for &id in ids {
      let mut iter = self.iter_entries(id).await?;

      while let Some((pos, head, head_data)) = iter.next().await? {
        total += 1;
        let old_pos = Pos::new(id, pos);

        self.read_key_into(&head, head_data, &mut key_buf).await?;

        if checker.is_rm(&key_buf).await {
          if head.key_store().is_file() {
            let fpos = head.key_file_pos(head_data);
            stale_bins.push(fpos.file_id);
          }
          if !head.is_tombstone() && head.val_store().is_file() {
            let fpos = head.val_file_pos(head_data);
            stale_bins.push(fpos.file_id);
          }
          reclaimed += 1;
          continue;
        }

        let new_pos = if head.is_tombstone() {
          gc_wal.del(&key_buf).await?
        } else if head.val_store().is_file() {
          let fpos = head.val_file_pos(head_data);
          gc_wal
            .put_with_file(
              &key_buf,
              head.val_store(),
              fpos.file_id,
              head.val_len.unwrap_or(0),
              fpos.hash,
            )
            .await?
        } else {
          let val_data = head.val_data(head_data);
          gc_wal.put(&key_buf, val_data).await?
        };

        mapping.push(PosMap {
          key: HipByt::from(&key_buf[..]),
          old: old_pos,
          new: new_pos,
        });
      }
    }

    gc_wal.sync_all().await?;

    if !mapping.is_empty() && !checker.batch_update(mapping).await {
      return Err(Error::UpdateFailed);
    }

    let gc_wal_id = gc_wal.cur_id();
    drop(gc_wal);
    state.move_gc_wal(gc_wal_id)?;

    stale_bins.sort_unstable();
    stale_bins.dedup();
    for id in stale_bins {
      let _ = self.rm_bin(id).await;
    }

    for &id in ids {
      self.rm(id).await?;
    }

    drop(locks);
    Ok((reclaimed, total))
  }

  /// GC with compression and papaya update
  /// 带压缩和 papaya 更新的 GC
  pub async fn gc_merge_compress<T, G, M>(
    &mut self,
    ids: &[u64],
    checker: &T,
    state: &mut GcState,
    gc: &mut G,
    pos_map: &M,
  ) -> Result<(usize, usize)>
  where
    T: Gcable,
    G: GcTrait,
    M: PosMapUpdate,
  {
    if ids.is_empty() {
      return Ok((0, 0));
    }

    let mut locks = Vec::with_capacity(ids.len());
    for &id in ids {
      if id >= self.cur_id() {
        return Err(Error::CannotRemoveCurrent);
      }
      locks.push(state.try_lock(id)?);
    }

    let gc_id = state.find_gc_id(self.cur_id());

    fs::create_dir_all(&state.gc_dir)?;
    let mut gc_wal = WalNoCache::new(&state.gc_dir, &[Conf::MaxSize(u64::MAX)]);
    gc_wal.ider.init(gc_id);
    gc_wal.open().await?;

    let mut reclaimed = 0usize;
    let mut total = 0usize;
    let mut val_buf = Vec::new();
    let mut key_buf = Vec::new();
    let mut compress_buf = Vec::new();
    let mut stale_bins = Vec::new();

    for &id in ids {
      let mut iter = self.iter_entries(id).await?;

      while let Some((pos, head, head_data)) = iter.next().await? {
        total += 1;
        let _old_pos = Pos::new(id, pos);

        self.read_key_into(&head, head_data, &mut key_buf).await?;

        if checker.is_rm(&key_buf).await {
          if head.key_store().is_file() {
            let fpos = head.key_file_pos(head_data);
            stale_bins.push(fpos.file_id);
          }
          if !head.is_tombstone() && head.val_store().is_file() {
            let fpos = head.val_file_pos(head_data);
            stale_bins.push(fpos.file_id);
          }
          pos_map.remove(&key_buf);
          reclaimed += 1;
          continue;
        }

        let new_pos = if head.is_tombstone() {
          gc_wal.del(&key_buf).await?
        } else if head.val_store().is_file() {
          self.read_val_into(&head, head_data, &mut val_buf).await?;
          let (new_store, compressed_len) =
            gc.process(head.val_store(), &val_buf, &mut compress_buf);

          if let Some(len) = compressed_len {
            let compressed_data = compress_buf[..len].to_vec();
            let hash = gxhash::gxhash128(&compressed_data, 0);
            let file_id = gc_wal.ider.get();
            gc_wal.write_file_io(file_id, compressed_data).await?;
            gc_wal
              .put_with_file(
                &key_buf,
                new_store,
                file_id,
                head.val_len.unwrap_or(0),
                hash,
              )
              .await?
          } else {
            let fpos = head.val_file_pos(head_data);
            gc_wal
              .put_with_file(
                &key_buf,
                new_store,
                fpos.file_id,
                head.val_len.unwrap_or(0),
                fpos.hash,
              )
              .await?
          }
        } else {
          self.read_val_into(&head, head_data, &mut val_buf).await?;
          let (new_store, compressed_len) =
            gc.process(head.val_store(), &val_buf, &mut compress_buf);

          if let Some(len) = compressed_len {
            gc_wal
              .put_infile_lz4(&key_buf, &compress_buf[..len], val_buf.len() as u64)
              .await?
          } else if new_store != head.val_store() {
            gc_wal.put_with_store(&key_buf, &val_buf, new_store).await?
          } else {
            gc_wal.put(&key_buf, &val_buf).await?
          }
        };

        pos_map.insert(&key_buf, new_pos);
      }

      self.rm(id).await?;
    }

    gc_wal.sync_all().await?;

    let gc_wal_id = gc_wal.cur_id();
    drop(gc_wal);
    state.move_gc_wal(gc_wal_id)?;

    stale_bins.sort_unstable();
    stale_bins.dedup();
    for id in stale_bins {
      let _ = self.rm_bin(id).await;
    }

    drop(locks);
    Ok((reclaimed, total))
  }
}

// ============================================================================
// GC Thread Scheduling
// GC 线程调度
// ============================================================================

const GC_CHECK_INTERVAL: usize = 8;
const CPU_REFRESH_MS: u64 = 100;

/// Find least busy CPU core
/// 找到最闲的 CPU 核心
pub fn find_idle_core() -> Option<usize> {
  use sysinfo::{CpuRefreshKind, RefreshKind, System};

  let mut sys =
    System::new_with_specifics(RefreshKind::nothing().with_cpu(CpuRefreshKind::everything()));
  std::thread::sleep(Duration::from_millis(CPU_REFRESH_MS));
  sys.refresh_cpu_all();

  sys
    .cpus()
    .iter()
    .enumerate()
    .min_by(|(_, a), (_, b)| {
      a.cpu_usage()
        .partial_cmp(&b.cpu_usage())
        .unwrap_or(std::cmp::Ordering::Equal)
    })
    .map(|(id, _)| id)
}

/// GC result
/// GC 结果
#[derive(Debug)]
pub enum GcResult {
  Done(usize, usize),
  Restart {
    remaining: Vec<u64>,
    reclaimed: usize,
    total: usize,
  },
}

/// Spawn GC in separate thread
/// 在独立线程中启动 GC
pub fn spawn_gc<G, T, F>(ids: Vec<u64>, gc_fn: F) -> JoinHandle<Result<GcResult>>
where
  G: GcTrait,
  T: Gcable + Send + 'static,
  F: FnOnce(Vec<u64>, Option<usize>) -> Result<GcResult> + Send + 'static,
{
  let core_id = find_idle_core();

  std::thread::Builder::new()
    .name("gc".into())
    .spawn(move || {
      if let Some(id) = core_id {
        let _ = core_affinity::set_for_current(core_affinity::CoreId { id });
      }

      #[cfg(unix)]
      unsafe {
        libc::nice(19);
      }

      gc_fn(ids, core_id)
    })
    .expect("spawn gc thread")
}

/// Check if should restart on different core
/// 检查是否应在不同核心重启
#[inline]
pub fn should_restart(current_core: Option<usize>, processed: usize) -> bool {
  if !processed.is_multiple_of(GC_CHECK_INTERVAL) {
    return false;
  }
  let new_idle = find_idle_core();
  new_idle != current_core
}

/// Run GC with auto-restart on core change
/// 自动重启的 GC
pub fn run_gc<F>(mut ids: Vec<u64>, mut gc_factory: F) -> Result<(usize, usize)>
where
  F: FnMut(Vec<u64>, Option<usize>) -> Result<GcResult>,
{
  let mut total_reclaimed = 0;
  let mut total_count = 0;

  while !ids.is_empty() {
    let core_id = find_idle_core();

    if let Some(id) = core_id {
      let _ = core_affinity::set_for_current(core_affinity::CoreId { id });
    }

    #[cfg(unix)]
    unsafe {
      libc::nice(19);
    }

    let batch = std::mem::take(&mut ids);
    match gc_factory(batch, core_id)? {
      GcResult::Done(r, t) => {
        total_reclaimed += r;
        total_count += t;
        break;
      }
      GcResult::Restart {
        remaining,
        reclaimed,
        total,
      } => {
        total_reclaimed += reclaimed;
        total_count += total;
        ids = remaining;
      }
    }
  }

  Ok((total_reclaimed, total_count))
}

/// Run GC in separate thread with auto-restart
/// 在独立线程中运行自动重启的 GC
pub fn run_gc_threaded<F>(mut ids: Vec<u64>, gc_factory: F) -> JoinHandle<Result<(usize, usize)>>
where
  F: Fn(Vec<u64>, Option<usize>) -> Result<GcResult> + Send + Sync + 'static,
{
  std::thread::Builder::new()
    .name("gc-main".into())
    .spawn(move || {
      let mut total_reclaimed = 0;
      let mut total_count = 0;

      while !ids.is_empty() {
        let core_id = find_idle_core();

        if let Some(id) = core_id {
          let _ = core_affinity::set_for_current(core_affinity::CoreId { id });
        }

        #[cfg(unix)]
        unsafe {
          libc::nice(19);
        }

        let batch = std::mem::take(&mut ids);
        match gc_factory(batch, core_id)? {
          GcResult::Done(r, t) => {
            total_reclaimed += r;
            total_count += t;
            break;
          }
          GcResult::Restart {
            remaining,
            reclaimed,
            total,
          } => {
            total_reclaimed += reclaimed;
            total_count += total;
            ids = remaining;
          }
        }
      }

      Ok((total_reclaimed, total_count))
    })
    .expect("spawn gc-main thread")
}
