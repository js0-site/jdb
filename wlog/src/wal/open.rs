//! WAL open/recover
//! WAL 打开/恢复

use std::{fs, path::PathBuf, sync::atomic::Ordering};

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use compio_fs::File;
use futures::StreamExt;
use jdb_base::{Ckp, Pos};
use jdb_fs::{
  fs::{open_read_write, open_read_write_create},
  fs_id::decode_id,
  head::HEAD_CRC,
  load::Load,
};
use jdb_lock::WalLock;
use log::warn;

use super::{
  Conf, WalConf, WalEntry, WalInner,
  consts::{GC_SUBDIR, HEADER_SIZE, MIN_FILE_SIZE},
  header::{HeaderState, build_header, check_header},
};
use crate::error::Result;

impl<C: WalConf> WalInner<C> {
  /// Open WAL with checkpoint info, call on_entry for each recovered entry
  /// 使用检查点信息打开 WAL，对每个恢复的条目调用 on_entry
  pub async fn open<F>(
    dir: impl Into<PathBuf>,
    conf: &[Conf],
    ckp: Option<&Ckp>,
    mut on_entry: F,
  ) -> Result<Self>
  where
    F: FnMut(&[u8], Pos),
  {
    let mut wal = Self::new(dir, conf);
    fs::create_dir_all(&wal.wal_dir)?;
    fs::create_dir_all(&wal.bin_dir)?;

    let gc_dir = wal.dir().join(GC_SUBDIR);
    if gc_dir.exists() {
      let _ = fs::remove_dir_all(&gc_dir);
    }
    fs::create_dir_all(&gc_dir)?;

    // Determine current WAL ID and start offset
    // 确定当前 WAL ID 和起始偏移
    let (cur_id, start_offset, wal_ids) = if let Some(ckp) = ckp {
      // Current is last rotate or wal_id
      // 当前是最后一个轮转或 wal_id
      let (cur, offset) = if let Some(last_id) = ckp.rotate_wal_ids.last() {
        (*last_id, HEADER_SIZE as u64)
      } else {
        (ckp.wal_id, ckp.offset)
      };
      // Build ID list for recovery
      // 构建恢复用的 ID 列表
      let mut ids = Vec::with_capacity(1 + ckp.rotate_wal_ids.len());
      ids.push(ckp.wal_id);
      ids.extend_from_slice(&ckp.rotate_wal_ids);
      (Some(cur), offset, ids)
    } else {
      // No checkpoint, scan for newest
      // 无检查点，扫描最新的
      (wal.scan_newest_id(), HEADER_SIZE as u64, Vec::new())
    };

    // Try to open current WAL
    // 尝试打开当前 WAL
    if let Some(id) = cur_id
      && let Some((file, end)) = wal.try_open_wal(id).await
    {
      wal.cur_lock.try_lock(&wal.wal_path(id))?;
      wal.cur_id.store(id, Ordering::Release);
      *wal.shared.file() = Some(file);
      wal.cur_pos = end;
      ider::id_init(id);

      // Recover entries via callback
      // 通过回调恢复条目
      for (i, &wal_id) in wal_ids.iter().enumerate() {
        let offset = if i == 0 {
          start_offset
        } else {
          HEADER_SIZE as u64
        };
        let path = wal.wal_path(wal_id);
        let stream = WalEntry::recover(path, offset);
        futures::pin_mut!(stream);

        while let Some(entry) = stream.next().await {
          let flag = entry.head.flag();
          let key = entry.head.key_data(&entry.data);

          // Calculate head position: end - record_size
          // 计算 head 位置：end - record_size
          let head_pos = entry.end - entry.head.record_size() as u64;

          // Build Pos from Head
          // 从 Head 构建 Pos
          let ver = entry.head.id;
          let pos = if flag.is_infile() {
            // INFILE: val offset = head_pos + HEAD_CRC
            // INFILE: val 偏移 = head_pos + HEAD_CRC
            let val_offset = head_pos + HEAD_CRC as u64;
            Pos::new(ver, flag, wal_id, val_offset, entry.head.val_len)
          } else {
            // FILE mode (or tombstone with file)
            // FILE 模式（或带文件的墓碑）
            Pos::new(
              ver,
              flag,
              wal_id,
              entry.head.val_file_id,
              entry.head.val_len,
            )
          };

          on_entry(key, pos);
        }
      }

      return Ok(wal);
    }

    // No valid WAL, create new
    // 无有效 WAL，创建新的
    wal.cur_id.store(ider::id(), Ordering::Release);
    wal.wal_new().await?;
    Ok(wal)
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
    let file = open_read_write(&self.wal_path(id)).await.ok()?;
    let meta = file.metadata().await.ok()?;

    if meta.len() < MIN_FILE_SIZE {
      warn!("WAL too small: {:?}, len={}", self.wal_path(id), meta.len());
      return None;
    }

    // Check header
    // 检查文件头
    let mut buf = vec![0u8; HEADER_SIZE];
    let res = file.read_exact_at(buf.slice(0..HEADER_SIZE), 0).await;
    buf = res.1.into_inner();
    if res.0.is_err() {
      return None;
    }

    if matches!(check_header(&mut buf), HeaderState::Invalid) {
      warn!("WAL header invalid: {:?}", self.wal_path(id));
      return None;
    }

    // Fast backward scan to find valid end position
    // 快速反向扫描找到有效结束位置
    let end = WalEntry::find_end(&file, HEADER_SIZE as u64)
      .await
      .unwrap_or(HEADER_SIZE as u64);

    log::debug!("WAL opened: id={id}, end={end}");
    Some((file, end))
  }

  async fn wal_new(&mut self) -> Result<()> {
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

    let new_id = ider::id();
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
