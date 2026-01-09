//! GC (Garbage Collection)
//! 垃圾回收
//!
//! ## Flow / 流程
//!
//! 1. Create new WAL in gc dir / 在 gc 目录创建新 WAL
//! 2. Merge old WALs, write live entries / 合并旧 WAL，写入有效条目
//! 3. Call `IndexUpdate::update()` / 调用 `IndexUpdate::update()`
//! 4. Move gc WAL to wal dir / 将 gc WAL 移动到 wal 目录

use std::{
  fs,
  future::Future,
  path::{Path, PathBuf},
};

use jdb_base::Pos;
use jdb_fs::fs_id::id_path;
use jdb_lock::gc::Lock as GcLock;

use crate::{
  Error, Result,
  wal::{
    Conf, Gc, Wal, WalConf, WalInner, WalNoCache,
    consts::{GC_SUBDIR, LOCK_SUBDIR, WAL_LOCK_TYPE, WAL_SUBDIR},
  },
};

// Mapping capacity / 映射容量
const MAP_CAP: usize = 1024;

/// Position mapping entry / 位置映射条目
#[derive(Debug, Clone)]
pub struct PosMap {
  pub key: Box<[u8]>,
  pub new: Pos,
}

/// Index batch update trait / 索引批量更新 trait
pub trait IndexUpdate: Send + Sync {
  fn update(&self, mapping: &[PosMap]);
}

/// GC check trait / GC 检查 trait
pub trait Gcable {
  fn is_rm(&self, key: &[u8]) -> impl Future<Output = bool> + Send;
}

/// GC state (internal) / GC 状态（内部）
pub(crate) struct GcState {
  dir: PathBuf,
  gc_dir: PathBuf,
  wal_dir: PathBuf,
}

impl GcState {
  pub(crate) fn new(dir: &Path) -> Self {
    Self {
      gc_dir: dir.join(GC_SUBDIR),
      wal_dir: dir.join(WAL_SUBDIR),
      dir: dir.to_path_buf(),
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
      if !id_path(&self.wal_dir, id).exists() {
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
    fs::rename(self.gc_wal_path(id), self.final_wal_path(id))?;
    Ok(())
  }
}

impl<C: WalConf> WalInner<C> {
  fn gc_state(&self) -> GcState {
    GcState::new(&self.dir())
  }
}

impl Wal {
  /// GC and merge WAL files / GC 并合并 WAL 文件
  ///
  /// Returns (reclaimed, total) / 返回 (回收数, 总数)
  pub async fn gc<T, M>(&mut self, ids: &[u64], checker: &T, index: &M) -> Result<(usize, usize)>
  where
    T: Gcable,
    M: IndexUpdate,
  {
    if ids.is_empty() {
      return Ok((0, 0));
    }

    let state = self.gc_state();

    let mut locks = Vec::with_capacity(ids.len());
    for &id in ids {
      if id >= self.cur_id() {
        return Err(Error::CannotRemoveCurrent);
      }
      locks.push(state.try_lock(id)?);
    }

    let gc_id = state.find_gc_id(self.cur_id());

    fs::create_dir_all(&state.gc_dir)?;
    let mut gc_wal =
      WalNoCache::open(&state.gc_dir, &[Conf::MaxSize(u64::MAX)], None, |_, _| {}).await?;
    jdb_base::id_init(gc_id);

    let mut gc_proc = <crate::wal::DefaultConf as WalConf>::Gc::default();
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
          // Collect stale file IDs for cleanup
          // 收集过期文件 ID 用于清理
          if head.flag().is_file() {
            stale_bins.push(head.val_file_id);
          }
          reclaimed += 1;
          continue;
        }

        let new_pos = if head.is_tombstone() {
          // Tombstone: rebuild with stored position info
          // 墓碑：使用存储的位置信息重建
          let old_pos = Pos::new(
            head.id,
            head.flag().storage(),
            id,
            if head.flag().is_file() {
              head.val_file_id
            } else {
              0
            },
            head.val_len,
          );
          gc_wal.rm(key, old_pos).await?
        } else if head.flag().is_file() {
          self.read_file_into(head.val_file_id, &mut val_buf).await?;
          let (new_flag, compressed_len) =
            gc_proc.process(head.flag(), &val_buf, &mut compress_buf);

          if let Some(len) = compressed_len {
            let file_id = jdb_base::id();
            gc_wal
              .write_file_io(file_id, compress_buf[..len].to_vec())
              .await?;
            gc_wal
              .put_with_file(key, new_flag, file_id, len as u32)
              .await?
          } else {
            gc_wal
              .put_with_file(key, new_flag, head.val_file_id, head.val_len)
              .await?
          }
        } else {
          let val = head.val_data(record);
          let (new_flag, compressed_len) = gc_proc.process(head.flag(), val, &mut compress_buf);

          if let Some(len) = compressed_len {
            gc_wal
              .put_with_store(key, &compress_buf[..len], new_flag)
              .await?
          } else if new_flag != head.flag() {
            gc_wal.put_with_store(key, val, new_flag).await?
          } else {
            gc_wal.put(key, val).await?
          }
        };

        mapping.push(PosMap {
          key: key.to_vec().into_boxed_slice(),
          new: new_pos,
        });
      }

      self.rm_wal_id(id).await?;
    }

    gc_wal.sync().await?;
    index.update(&mapping);

    let gc_wal_id = gc_wal.cur_id();
    drop(gc_wal);
    state.move_gc_wal(gc_wal_id)?;

    stale_bins.sort_unstable();
    stale_bins.dedup();
    for id in stale_bins {
      let _ = self.rm_file_id(id).await;
    }

    drop(locks);
    Ok((reclaimed, total))
  }
}
