//! WAL open/recover
//! WAL 打开/恢复

use std::{fs, path::PathBuf, sync::atomic::Ordering};

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use compio_fs::File;
use futures::{Stream, StreamExt, pin_mut, stream};
use jdb_base::Ckp;
use jdb_fs::{
  fs::{open_read_write, open_read_write_create},
  fs_id::decode_id,
  head::Head,
  load::{HeadEnd, Load},
};
use jdb_lock::WalLock;
use log::warn;

use super::{
  WalConf, WalEntry, WalInner,
  consts::{GC_SUBDIR, HEADER_SIZE, MIN_FILE_SIZE},
  header::{HeaderState, build_header, check_header},
};
use crate::error::Result;

/// Create recovery stream from paths
/// 从路径创建恢复流
fn recover_stream(paths: Vec<PathBuf>, start_offset: u64) -> impl Stream<Item = HeadEnd<Head>> {
  stream::iter(paths.into_iter().enumerate()).flat_map(move |(i, path)| {
    let offset = if i == 0 {
      start_offset
    } else {
      HEADER_SIZE as u64
    };
    WalEntry::recover(path, offset)
  })
}

impl<C: WalConf> WalInner<C> {
  /// Open WAL with checkpoint info and return recovery stream
  /// 使用检查点信息打开 WAL 并返回恢复流
  pub async fn open(&mut self, ckp: Option<&Ckp>) -> Result<impl Stream<Item = HeadEnd<Head>>> {
    fs::create_dir_all(&self.wal_dir)?;
    fs::create_dir_all(&self.bin_dir)?;

    let gc_dir = self.dir().join(GC_SUBDIR);
    if gc_dir.exists() {
      let _ = fs::remove_dir_all(&gc_dir);
    }
    fs::create_dir_all(&gc_dir)?;

    // Determine current WAL ID and start offset
    // 确定当前 WAL ID 和起始偏移
    let (cur_id, start_offset, wal_ids) = if let Some(ckp) = ckp {
      // Current is last rotate or wal_id
      // 当前是最后一个轮转或 wal_id
      let cur = ckp.rotate_wal_id_li.last().copied().unwrap_or(ckp.wal_id);
      // Build ID list for recovery
      // 构建恢复用的 ID 列表
      let mut ids = Vec::with_capacity(1 + ckp.rotate_wal_id_li.len());
      ids.push(ckp.wal_id);
      ids.extend_from_slice(&ckp.rotate_wal_id_li);
      (Some(cur), ckp.offset, ids)
    } else {
      // No checkpoint, scan for newest
      // 无检查点，扫描最新的
      let id = self.scan_newest_id();
      (id, HEADER_SIZE as u64, Vec::new())
    };

    // Try to open current WAL
    // 尝试打开当前 WAL
    if let Some(id) = cur_id
      && let Some((file, end)) = self.try_open_wal(id).await
    {
      let path = self.wal_path(id);
      self.cur_lock.try_lock(&path)?;
      self.cur_id.store(id, Ordering::Release);
      *self.shared.file() = Some(file);
      self.cur_pos = end;
      self.ider.init(id);

      // Build paths for recovery
      // 构建恢复用的路径
      let paths: Vec<_> = wal_ids.iter().map(|&id| self.wal_path(id)).collect();
      return Ok(recover_stream(paths, start_offset));
    }

    // No valid WAL, create new
    // 无有效 WAL，创建新的
    self.cur_id.store(self.ider.get(), Ordering::Release);
    self.create_wal_file().await?;
    Ok(recover_stream(Vec::new(), 0))
  }

  /// Scan directory for newest WAL ID
  /// 扫描目录查找最新的 WAL ID
  fn scan_newest_id(&self) -> Option<u64> {
    let entries = fs::read_dir(&self.wal_dir).ok()?;
    entries
      .flatten()
      .filter_map(|e| decode_id(e.file_name().to_str()?))
      .max()
  }

  /// Try to open and validate a WAL file
  /// 尝试打开并验证 WAL 文件
  async fn try_open_wal(&self, id: u64) -> Option<(File, u64)> {
    let path = self.wal_path(id);
    let file = open_read_write(&path).await.ok()?;
    let meta = file.metadata().await.ok()?;

    let len = meta.len();
    if len < MIN_FILE_SIZE {
      warn!("WAL too small: {path:?}, len={len}");
      return None;
    }

    // Check header
    // 检查文件头
    let mut buf = vec![0u8; HEADER_SIZE];
    let slice = buf.slice(0..HEADER_SIZE);
    let res = file.read_exact_at(slice, 0).await;
    buf = res.1.into_inner();
    if res.0.is_err() {
      return None;
    }

    if matches!(check_header(&mut buf), HeaderState::Invalid) {
      warn!("WAL header invalid: {path:?}");
      return None;
    }

    // Scan to find valid end position
    // 扫描找到有效结束位置
    let mut end = HEADER_SIZE as u64;
    let stream = WalEntry::recover(path, HEADER_SIZE as u64);
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
      end = item.end;
    }

    log::info!("WAL opened: id={id}, end={end}");
    Some((file, end))
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

  pub(crate) async fn rotate_inner(&mut self) -> Result<()> {
    if let Some(file) = self.shared.file() {
      file.sync_all().await?;
    }

    let new_id = self.ider.get();
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
