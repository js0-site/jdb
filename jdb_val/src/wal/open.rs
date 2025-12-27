//! WAL open/recover / WAL 打开/恢复

use std::fs;

use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio_fs::{File, OpenOptions};
use fast32::base32::CROCKFORD_LOWER;
use log::warn;
use zerocopy::FromBytes;

use super::{
  Wal,
  end::{END_SIZE, parse_end},
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
      // Sync generator to prevent ID collision / 同步生成器防止 ID 碰撞
      self.gen_id.init_last_id(id);
      return Ok(());
    }

    // No valid file, create new / 没有有效文件，创建新的
    self.cur_id = self.gen_id.next_id();
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

      // Try fast recovery first / 先尝试快速恢复
      let valid_pos = if let Some(pos) = try_fast_recover(&file, len).await {
        log::info!("{LOG_RECOVER_OK} (fast): {path:?}, pos={pos}");
        pos
      } else {
        // Fallback to scan / 回退到扫描
        let pos = recover_scan(&file, len).await;
        log::info!("{LOG_RECOVER_OK} (scan): {path:?}, pos={pos}");
        pos
      };

      return Some((id, file, valid_pos));
    }

    None
  }

  /// Rotate to new WAL file / 轮转到新 WAL 文件
  ///
  /// Triggered when cur_pos + data_len > max_size
  /// 当 cur_pos + 数据长度 > max_size 时触发
  pub async fn rotate(&mut self) -> Result<()> {
    self.cur_id = self.gen_id.next_id();
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

/// Scan buffer size (64KB) / 扫描缓冲区大小
const SCAN_BUF_SIZE: usize = 64 * 1024;

/// Min file size for fast recovery / 快速恢复最小文件大小
/// Header(12) + Head(64) + End(12) = 88
const MIN_FAST_RECOVER_SIZE: u64 = (HEADER_SIZE + Head::SIZE + END_SIZE) as u64;

/// Try fast recovery using end marker / 尝试使用尾部标记快速恢复
///
/// Returns Some(file_len) if valid, None to fallback to scan
/// 成功返回 Some(文件长度)，失败返回 None 回退到扫描
async fn try_fast_recover(file: &File, len: u64) -> Option<u64> {
  if len < MIN_FAST_RECOVER_SIZE {
    return None;
  }

  // Read last 12 bytes / 读取最后 12 字节
  let buf = vec![0u8; END_SIZE];
  let res = file.read_exact_at(buf, len - END_SIZE as u64).await;
  if res.0.is_err() {
    return None;
  }
  let buf = res.1;

  // Parse end marker / 解析尾部标记
  let head_offset = parse_end(&buf)?;

  // Validate head_offset bounds / 验证 head_offset 边界
  if head_offset < HEADER_SIZE as u64 || head_offset + Head::SIZE as u64 > len {
    return None;
  }

  // Read head at offset / 读取偏移处的 Head
  let head_buf = vec![0u8; Head::SIZE];
  let res = file.read_exact_at(head_buf, head_offset).await;
  if res.0.is_err() {
    return None;
  }
  let head_buf = res.1;

  // Parse and verify CRC / 解析并验证 CRC
  let Ok(head) = Head::read_from_bytes(&head_buf) else {
    return None;
  };

  let crc = crc32fast::hash(&head_buf[..Head::CRC_RANGE]);
  if crc != head.head_crc32.get() {
    return None;
  }

  // Fast recovery OK: cur_pos = file_len / 快速恢复成功
  Some(len)
}

/// Scan file to find last valid Head position / 扫描文件找到最后有效 Head 位置
async fn recover_scan(file: &File, len: u64) -> u64 {
  let mut pos = HEADER_SIZE as u64;
  let mut valid_pos = pos;
  let mut buf = vec![0u8; SCAN_BUF_SIZE];

  while pos < len {
    let read_len = ((len - pos) as usize).min(SCAN_BUF_SIZE);
    buf.truncate(read_len);

    let res = file.read_exact_at(std::mem::take(&mut buf), pos).await;
    if res.0.is_err() {
      break;
    }
    buf = res.1;

    let mut off = 0;
    while off + Head::SIZE <= buf.len() {
      let head_bytes = &buf[off..off + Head::SIZE];
      let Ok(head) = Head::read_from_bytes(head_bytes) else {
        return valid_pos;
      };

      // Verify head CRC / 验证 Head CRC
      let crc = crc32fast::hash(&head_bytes[..Head::CRC_RANGE]);
      if crc != head.head_crc32.get() {
        return valid_pos;
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

      let entry_len = Head::SIZE as u64 + data_len;
      let next_pos = pos + off as u64 + entry_len;

      if next_pos > len {
        return valid_pos;
      }

      valid_pos = next_pos;
      off += entry_len as usize;

      // Entry spans beyond buffer / 条目跨越缓冲区边界
      if off > buf.len() {
        break;
      }
    }

    pos = valid_pos;
  }

  valid_pos
}
