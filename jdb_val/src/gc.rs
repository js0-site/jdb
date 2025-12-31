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

use std::{fs, future::Future, path::PathBuf, thread::JoinHandle, time::Duration};

use hipstr::HipByt;
use jdb_lock::gc::Lock as GcLock;

use crate::{
  Conf, Error, Flag, GcTrait, Pos, RecPos, Result, Wal, WalNoCache, id_path,
  wal::{
    consts::{GC_SUBDIR, LOCK_SUBDIR, WAL_LOCK_TYPE, WAL_SUBDIR},
    lz4,
  },
};

/// Default GC with LZ4 compression
/// 默认 GC（带 LZ4 压缩）
pub struct DefaultGc;

impl GcTrait for DefaultGc {
  fn process(&mut self, store: Flag, data: &[u8], buf: &mut Vec<u8>) -> (Flag, Option<usize>) {
    if store.is_compressed() || store.is_probed() {
      return (store, None);
    }

    if let Some(len) = lz4::try_compress(data, buf) {
      (store.to_lz4(), Some(len))
    } else {
      (store.to_probed(), None)
    }
  }
}

// Mapping capacity
// 映射容量
const MAP_CAP: usize = 1024;

/// Position mapping entry
/// 位置映射条目
#[derive(Debug, Clone)]
pub struct PosMap {
  pub key: HipByt<'static>,
  pub old: RecPos,
  pub new: Pos,
}

/// Position map trait for GC updates
/// GC 更新的位置映射 trait
pub trait PosMapUpdate: Send + Sync {
  fn insert(&self, key: &[u8], pos: Pos);
  fn remove(&self, key: &[u8]);
}

/// GC trait
/// GC 特征
pub trait Gcable {
  fn is_rm(&self, key: &[u8]) -> impl Future<Output = bool> + Send;
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
  pub fn new(dir: impl Into<PathBuf>) -> Self {
    let dir = dir.into();
    Self {
      gc_dir: dir.join(GC_SUBDIR),
      wal_dir: dir.join(WAL_SUBDIR),
      dir,
    }
  }

  fn lock_path(&self, id: u64) -> PathBuf {
    id_path(&self.dir.join(LOCK_SUBDIR).join(WAL_LOCK_TYPE), id)
  }

  fn try_lock(&self, id: u64) -> Result<GcLock> {
    Ok(GcLock::try_new(self.lock_path(id))?)
  }

  fn find_gc_id(&self, cur_id: u64) -> u64 {
    if cur_id == 0 {
      return 0;
    }
    let mut id = cur_id - 1;
    while id > 0 {
      let path = id_path(&self.wal_dir, id);
      if !path.exists() {
        return id;
      }
      id -= 1;
    }
    0
  }

  fn gc_wal_path(&self, id: u64) -> PathBuf {
    id_path(&self.gc_dir.join(WAL_SUBDIR), id)
  }

  fn final_wal_path(&self, id: u64) -> PathBuf {
    id_path(&self.wal_dir, id)
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
    let mut stale_bins = Vec::new();

    for &id in ids {
      let mut iter = self.iter_entries(id).await?;

      while let Some((pos, head, record)) = iter.next().await? {
        total += 1;
        let old_pos = RecPos::new(id, pos);
        let key = head.key_data(record);

        if checker.is_rm(key).await {
          // Collect stale bin files
          // 收集过期的 bin 文件
          if !head.flag().is_tombstone() && head.flag().is_file() {
            stale_bins.push(head.val_file_id);
          }
          reclaimed += 1;
          continue;
        }

        let new_pos = if head.flag().is_tombstone() {
          gc_wal.del(key).await?
        } else if head.flag().is_file() {
          gc_wal
            .put_with_file(key, head.flag(), head.val_file_id, head.val_len)
            .await?
        } else {
          let val = head.val_data(record);
          gc_wal.put(key, val).await?
        };

        mapping.push(PosMap {
          key: HipByt::from(key),
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
    let mut compress_buf = Vec::new();
    let mut stale_bins = Vec::new();

    for &id in ids {
      let mut iter = self.iter_entries(id).await?;

      while let Some((_pos, head, record)) = iter.next().await? {
        total += 1;
        let key = head.key_data(record);

        if checker.is_rm(key).await {
          if !head.is_tombstone() && head.flag().is_file() {
            stale_bins.push(head.val_file_id);
          }
          pos_map.remove(key);
          reclaimed += 1;
          continue;
        }

        let new_pos = if head.is_tombstone() {
          gc_wal.del(key).await?
        } else if head.flag().is_file() {
          // Read and try compress FILE val
          // 读取并尝试压缩 FILE val
          self.read_file_into(head.val_file_id, &mut val_buf).await?;
          let (new_store, compressed_len) = gc.process(head.flag(), &val_buf, &mut compress_buf);

          if let Some(len) = compressed_len {
            let file_id = gc_wal.ider.get();
            gc_wal
              .write_file_io(file_id, compress_buf[..len].to_vec())
              .await?;
            gc_wal
              .put_with_file(key, new_store, file_id, len as u32)
              .await?
          } else {
            gc_wal
              .put_with_file(key, new_store, head.val_file_id, head.val_len)
              .await?
          }
        } else {
          // Try compress INFILE val
          // 尝试压缩 INFILE val
          let val = head.val_data(record);
          let (new_store, compressed_len) = gc.process(head.flag(), val, &mut compress_buf);

          if let Some(len) = compressed_len {
            gc_wal
              .put_with_store(key, &compress_buf[..len], new_store)
              .await?
          } else if new_store != head.flag() {
            gc_wal.put_with_store(key, val, new_store).await?
          } else {
            gc_wal.put(key, val).await?
          }
        };

        pos_map.insert(key, new_pos);
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

// GC check interval for core switch
// GC 核心切换检查间隔
const GC_CHECK_INTERVAL: usize = 8;

// CPU refresh interval (ms)
// CPU 刷新间隔（毫秒）
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
