//! WAL open/recover / WAL 打开/恢复

use std::fs;

use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio_fs::{File, OpenOptions};
use fast32::base32::CROCKFORD_LOWER;
use log::warn;
use zerocopy::FromBytes;

use super::{
  Wal,
  header::{HEADER_SIZE, HeaderState, build_header, check_header},
};
use crate::{Head, error::Result};

// Log messages / 日志消息
const LOG_RECOVER_SMALL: &str = "WAL file too small for recovery";
const LOG_RECOVER_INVALID: &str = "WAL header invalid for recovery";
const LOG_RECOVER_OK: &str = "WAL recovered";

impl Wal {
  /// Open or create current WAL file / 打开或创建当前 WAL 文件
  ///
  /// If existing files found, opens the newest valid one and recovers.
  /// 如果存在文件，打开最新的有效文件并恢复。
  pub async fn open(&mut self) -> Result<()> {
    fs::create_dir_all(&self.wal_dir)?;

    // Try to find and open newest valid WAL / 尝试找到并打开最新的有效 WAL
    if let Some((id, file, pos)) = self.find_newest().await {
      self.cur_id = id;
      self.cur_file = Some(file);
      self.cur_pos = pos;
      return Ok(());
    }

    // No valid file, create new / 没有有效文件，创建新的
    self.cur_id = self.gen_id.id();
    let path = self.wal_path(self.cur_id);

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    file.write_all_at(build_header().to_vec(), 0).await.0?;
    self.cur_pos = HEADER_SIZE as u64;
    self.cur_file = Some(file);
    Ok(())
  }

  /// Find newest valid WAL and recover / 找到最新的有效 WAL 并恢复
  ///
  /// Returns (id, file, valid_pos) / 返回 (id, 文件, 有效位置)
  async fn find_newest(&self) -> Option<(u64, File, u64)> {
    let entries = fs::read_dir(&self.wal_dir).ok()?;

    // Collect and sort by id desc / 收集并按 id 降序排序
    let mut ids: Vec<u64> = entries
      .flatten()
      .filter_map(|e| {
        let name = e.file_name();
        let name = name.to_str()?;
        CROCKFORD_LOWER.decode_u64(name.as_bytes()).ok()
      })
      .collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));

    // Try each file from newest / 从最新的开始尝试
    for id in ids {
      let path = self.wal_path(id);
      let Ok(file) = OpenOptions::new().read(true).write(true).open(&path).await else {
        continue;
      };

      let Ok(meta) = file.metadata().await else {
        continue;
      };

      let len = meta.len();
      if len < HEADER_SIZE as u64 {
        warn!("{LOG_RECOVER_SMALL}: {path:?}");
        continue;
      }

      // Check header / 检查文件头
      let mut buf = vec![0u8; HEADER_SIZE];
      let res = file.read_exact_at(buf, 0).await;
      if res.0.is_err() {
        continue;
      }
      buf = res.1;

      if matches!(check_header(&mut buf), HeaderState::Invalid) {
        warn!("{LOG_RECOVER_INVALID}: {path:?}");
        continue;
      }

      // Scan to find last valid position / 扫描找到最后有效位置
      let valid_pos = recover_scan(&file, len).await;
      log::info!("{LOG_RECOVER_OK}: {path:?}, pos={valid_pos}");
      return Some((id, file, valid_pos));
    }

    None
  }

  /// Rotate to new WAL file / 轮转到新 WAL 文件
  ///
  /// Triggered when cur_pos + data_len > max_size
  /// 当 cur_pos + 数据长度 > max_size 时触发
  pub async fn rotate(&mut self) -> Result<()> {
    self.cur_id = self.gen_id.id();
    let path = self.wal_path(self.cur_id);

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    file.write_all_at(build_header().to_vec(), 0).await.0?;
    self.cur_file = Some(file);
    self.cur_pos = HEADER_SIZE as u64;
    Ok(())
  }
}

/// Scan file to find last valid Head position / 扫描文件找到最后有效 Head 位置
async fn recover_scan(file: &File, len: u64) -> u64 {
  let mut pos = HEADER_SIZE as u64;
  let mut valid_pos = pos;

  while pos + Head::SIZE as u64 <= len {
    let buf = vec![0u8; Head::SIZE];
    let res = file.read_exact_at(buf, pos).await;
    if res.0.is_err() {
      break;
    }

    let Ok(head) = Head::read_from_bytes(&res.1) else {
      break;
    };

    // Verify head CRC / 验证 Head CRC
    let crc = crc32fast::hash(&res.1[..Head::CRC_RANGE]);
    if crc != head.head_crc32.get() {
      break;
    }

    // Calculate data length / 计算数据长度
    let data_len = if head.key_flag.is_infile() {
      head.key_len.get() as u64
    } else {
      0
    } + if head.val_flag.is_infile() {
      head.val_len.get() as u64
    } else {
      0
    };

    let next_pos = pos + Head::SIZE as u64 + data_len;
    if next_pos > len {
      // Data truncated / 数据被截断
      break;
    }

    valid_pos = next_pos;
    pos = next_pos;
  }

  valid_pos
}
