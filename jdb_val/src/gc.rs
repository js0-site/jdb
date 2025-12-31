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
//! 3. Call `IndexUpdate::update()` to batch update index
//!    调用 `IndexUpdate::update()` 批量更新索引
//! 4. Move gc WAL to wal dir, delete old WAL files
//!    将 gc WAL 移动到 wal 目录，删除旧 WAL 文件

use std::{fs, future::Future, path::PathBuf};

use hipstr::HipByt;
use jdb_lock::gc::Lock as GcLock;

use crate::{
  Error, Flag, Pos, Result,
  fs::id_path,
  wal::{
    Conf, Gc as GcTrait, Wal, WalNoCache,
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
  pub new: Pos,
}

/// Index batch update trait for GC
/// GC 索引批量更新 trait
pub trait IndexUpdate: Send + Sync {
  fn update(&self, mapping: &[PosMap]);
}

/// GC check trait
/// GC 检查 trait
pub trait Gcable {
  fn is_rm(&self, key: &[u8]) -> impl Future<Output = bool> + Send;
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
  pub async fn gc<T, G, M>(
    &mut self,
    ids: &[u64],
    checker: &T,
    state: &mut GcState,
    gc: &mut G,
    index: &M,
  ) -> Result<(usize, usize)>
  where
    T: Gcable,
    G: GcTrait,
    M: IndexUpdate,
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

    let mut mapping: Vec<PosMap> = Vec::with_capacity(MAP_CAP);
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

        mapping.push(PosMap {
          key: HipByt::from(key),
          new: new_pos,
        });
      }

      self.rm(id).await?;
    }

    gc_wal.sync_all().await?;

    // Batch update index
    // 批量更新索引
    index.update(&mapping);

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
