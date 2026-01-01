//! WAL open/recover
//! WAL 打开/恢复

use std::{fs, sync::atomic::Ordering};

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use compio_fs::File;
use jdb_base::Load;
use jdb_lock::WalLock;
use log::{info, warn};

use super::{
  WalConf, WalEntry, WalInner,
  consts::{GC_SUBDIR, HEADER_SIZE, MIN_FILE_SIZE},
  header::{HeaderState, build_header, check_header},
  replay::ReplayIter,
};
use crate::{
  Ckp,
  error::Result,
};

use jdb_base::{
  decode_id, open_read_write, open_read_write_create,
};

impl<C: WalConf> WalInner<C> {
  /// Open WAL and return replay iterator
  /// 打开 WAL 并返回回放迭代器
  ///
  /// Auto loads checkpoint, returns iterator for entries after checkpoint
  /// 自动加载检查点，返回检查点之后条目的迭代器
  pub async fn open(&mut self) -> Result<ReplayIter> {
    fs::create_dir_all(&self.wal_dir)?;
    fs::create_dir_all(&self.bin_dir)?;

    let gc_dir = self.dir().join(GC_SUBDIR);
    if gc_dir.exists() {
      let _ = fs::remove_dir_all(&gc_dir);
    }
    fs::create_dir_all(&gc_dir)?;

    // Open checkpoint / 打开检查点
    let ckp = Ckp::open(&self.dir()).await?;
    let replay_info = ckp.load_replay().await?;
    if let Some((ptr, ref rotates)) = replay_info {
      info!(
        "checkpoint loaded: id={}, offset={}, rotates={rotates:?}",
        ptr.id, ptr.offset
      );
    }
    self.ckp = Some(ckp);

    // Find newest WAL or create new / 查找最新 WAL 或创建新的
    if let Some((id, file, pos)) = self.find_newest().await {
      let path = self.wal_path(id);
      self.cur_lock.try_lock(&path)?;
      self.cur_id.store(id, Ordering::Release);
      *self.shared.file() = Some(file);
      self.cur_pos = pos;
      self.ider.init(id);
    } else {
      self.cur_id.store(self.ider.get(), Ordering::Release);
      self.create_wal_file().await?;
    }

    // Create replay iterator / 创建回放迭代器
    Ok(self.create_replay_iter(replay_info))
  }

  async fn create_wal_file(&mut self) -> Result<()> {
    let path = self.wal_path(self.cur_id());

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = open_read_write_create(&path).await?;

    file.write_all_at(build_header(), 0).await.0?;
    self.cur_pos = HEADER_SIZE as u64;
    *self.shared.file() = Some(file);
    self.cur_lock.try_lock(&path)?;
    Ok(())
  }

  #[allow(clippy::uninit_vec)]
  async fn find_newest(&self) -> Option<(u64, File, u64)> {
    let entries = fs::read_dir(&self.wal_dir).ok()?;

    let mut ids: Vec<u64> = entries
      .flatten()
      .filter_map(|e| {
        let name = e.file_name();
        let name = name.to_str()?;
        decode_id(name)
      })
      .collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));

    let mut header_buf = Vec::with_capacity(HEADER_SIZE);

    for id in ids {
      let path = self.wal_path(id);
      let Ok(file) = open_read_write(&path).await else {
        continue;
      };

      let Ok(meta) = file.metadata().await else {
        continue;
      };

      let len = meta.len();
      if len < MIN_FILE_SIZE {
        warn!("WAL too small: {path:?}, len={len}");
        continue;
      }

      unsafe { header_buf.set_len(HEADER_SIZE) };
      let slice = header_buf.slice(0..HEADER_SIZE);
      let res = file.read_exact_at(slice, 0).await;
      header_buf = res.1.into_inner();
      if res.0.is_err() {
        continue;
      }

      if matches!(check_header(&mut header_buf), HeaderState::Invalid) {
        warn!("WAL header invalid: {path:?}");
        continue;
      }

      let valid_pos = WalEntry::recover(&file, HEADER_SIZE as u64, len).await;
      log::info!("WAL recovered: {path:?}, pos={valid_pos}");

      return Some((id, file, valid_pos));
    }

    None
  }

  pub(crate) async fn rotate_inner(&mut self) -> Result<()> {
    // Sync old file before rotation
    // 轮转前同步旧文件
    if let Some(file) = self.shared.file() {
      file.sync_all().await?;
    }

    let new_id = self.ider.get();

    // Record rotation in checkpoint / 记录轮转到检查点
    if let Some(ckp) = &mut self.ckp {
      ckp.rotate(new_id).await?;
    }

    self.cur_id.store(new_id, Ordering::Release);
    let path = self.wal_path(new_id);

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = open_read_write_create(&path).await?;

    file.write_all_at(build_header(), 0).await.0?;
    *self.shared.file() = Some(file);
    self.cur_pos = HEADER_SIZE as u64;
    self.cur_lock.try_lock(&path)?;
    Ok(())
  }
}
