//! WAL open/recover / WAL 打开/恢复

use std::fs;

use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio_fs::{File, OpenOptions};
use log::warn;
use zerocopy::FromBytes;

use super::{
  Wal,
  consts::{END_SIZE, HEADER_SIZE, MAGIC_BYTES, MIN_FAST_SIZE, SCAN_BUF_SIZE},
  end::parse_end,
  header::{HeaderState, build_header, check_header},
};
use crate::{Head, error::Result};

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
        fast32::base32::CROCKFORD_LOWER
          .decode_u64(name.as_bytes())
          .ok()
      })
      .collect();
    ids.sort_unstable();

    // Try each file from newest / 从最新的开始尝试
    for id in ids.into_iter().rev() {
      let path = self.wal_path(id);
      let Ok(mut file) = OpenOptions::new().read(true).write(true).open(&path).await else {
        continue;
      };

      let Ok(meta) = file.metadata().await else {
        continue;
      };

      let len = meta.len();
      if len < HEADER_SIZE as u64 {
        warn!("WAL too small: {path:?}");
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
        warn!("WAL header invalid: {path:?}");
        continue;
      }

      // Try fast recovery first / 先尝试快速恢复
      let valid_pos = if let Some(pos) = try_fast_recover(&file, len).await {
        log::info!("WAL recovered (fast): {path:?}, pos={pos}");
        pos
      } else {
        // Fallback to scan / 回退到扫描
        let (pos, repairs) = scan_recover(&file, len).await;

        // Apply repairs / 应用修复
        for repair in repairs {
          let res = file.write_all_at(repair.buf, repair.head_off).await;
          if res.0.is_ok() {
            log::info!("Head repaired at {}", repair.head_off);
          }
        }

        log::info!("WAL recovered (scan): {path:?}, pos={pos}");
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

/// Try fast recovery using end marker / 尝试使用尾部标记快速恢复
///
/// Returns Some(file_len) if valid, None to fallback to scan
/// 成功返回 Some(文件长度)，失败返回 None 回退到扫描
async fn try_fast_recover(file: &File, len: u64) -> Option<u64> {
  if len < MIN_FAST_SIZE {
    return None;
  }

  // Read last 12 bytes / 读取最后 12 字节
  let buf = vec![0u8; END_SIZE];
  let res = file.read_exact_at(buf, len - END_SIZE as u64).await;
  let buf = res.0.ok().map(|_| res.1)?;

  // Parse end marker / 解析尾部标记
  let head_off = parse_end(&buf)?;

  // Validate and read head / 验证并读取 Head
  verify_head(file, head_off, len).await?;

  // Fast recovery OK: cur_pos = file_len / 快速恢复成功
  Some(len)
}

/// Verify head at offset / 验证偏移处的 Head
async fn verify_head(file: &File, head_off: u64, len: u64) -> Option<()> {
  if head_off < HEADER_SIZE as u64 || head_off + Head::SIZE as u64 > len {
    return None;
  }

  let buf = vec![0u8; Head::SIZE];
  let res = file.read_exact_at(buf, head_off).await;
  let buf = res.0.ok().map(|_| res.1)?;

  let head = Head::read_from_bytes(&buf).ok()?;
  // SAFETY: Head::CRC_RANGE < Head::SIZE / 常量范围安全
  let crc = crc32fast::hash(unsafe { buf.get_unchecked(..Head::CRC_RANGE) });
  (crc == head.head_crc32.get()).then_some(())
}

/// Try repair head using end marker info / 尝试用尾部标记信息修复 Head
///
/// Returns repaired head bytes if successful / 成功返回修复后的 Head 字节
async fn try_repair_head(file: &File, head_off: u64, entry_end: u64, len: u64) -> Option<Vec<u8>> {
  if head_off < HEADER_SIZE as u64 || head_off + Head::SIZE as u64 > len {
    return None;
  }

  let buf = vec![0u8; Head::SIZE];
  let res = file.read_exact_at(buf, head_off).await;
  let Ok(_) = res.0 else { return None };
  let mut buf = res.1;

  let head = Head::read_from_bytes(&buf).ok()?;

  // Compute infile data length / 计算 infile 数据长度
  let total_infile = entry_end
    .checked_sub(head_off)?
    .checked_sub((Head::SIZE + END_SIZE) as u64)?;

  let k_infile = head.key_flag.is_infile();
  let v_infile = head.val_flag.is_infile();

  // Try repair based on which fields are infile / 根据哪些字段是 infile 尝试修复
  let repaired = match (k_infile, v_infile) {
    (true, false) => {
      // Only key is infile, repair key_len / 只有 key 是 infile，修复 key_len
      let new_key_len = total_infile as u32;
      try_repair_with_key_len(&mut buf, new_key_len)
    }
    (false, true) => {
      // Only val is infile, repair val_len / 只有 val 是 infile，修复 val_len
      let new_val_len = total_infile as u32;
      try_repair_with_val_len(&mut buf, new_val_len)
    }
    (true, true) => {
      // Both infile, try repair key_len (val_len = total - key_len) / 两者都是 infile
      let cur_key_len = head.key_len.get() as u64;
      let cur_val_len = head.val_len.get() as u64;

      // Try keeping key_len, fix val_len / 尝试保持 key_len，修复 val_len
      if cur_key_len <= total_infile {
        let new_val_len = (total_infile - cur_key_len) as u32;
        if try_repair_with_val_len(&mut buf, new_val_len) {
          true
        } else if cur_val_len <= total_infile {
          // Try keeping val_len, fix key_len / 尝试保持 val_len，修复 key_len
          let new_key_len = (total_infile - cur_val_len) as u32;
          try_repair_with_key_len(&mut buf, new_key_len)
        } else {
          false
        }
      } else {
        false
      }
    }
    (false, false) => {
      // Neither infile, cannot repair (data is in Head) / 都不是 infile，无法修复
      false
    }
  };

  repaired.then_some(buf)
}

/// Try repair by setting key_len / 尝试通过设置 key_len 修复
fn try_repair_with_key_len(buf: &mut [u8], new_key_len: u32) -> bool {
  // Set key_len at offset 0 / 在偏移 0 设置 key_len
  buf[0..4].copy_from_slice(&new_key_len.to_le_bytes());
  // Recalc CRC / 重新计算 CRC
  let crc = crc32fast::hash(&buf[..Head::CRC_RANGE]);
  buf[Head::CRC_RANGE..Head::SIZE].copy_from_slice(&crc.to_le_bytes());
  // Verify / 验证
  Head::read_from_bytes(buf).is_ok()
}

/// Try repair by setting val_len / 尝试通过设置 val_len 修复
fn try_repair_with_val_len(buf: &mut [u8], new_val_len: u32) -> bool {
  // Set val_len at offset 4 / 在偏移 4 设置 val_len
  buf[4..8].copy_from_slice(&new_val_len.to_le_bytes());
  // Recalc CRC / 重新计算 CRC
  let crc = crc32fast::hash(&buf[..Head::CRC_RANGE]);
  buf[Head::CRC_RANGE..Head::SIZE].copy_from_slice(&crc.to_le_bytes());
  // Verify / 验证
  Head::read_from_bytes(buf).is_ok()
}

/// Repair info for deferred write / 延迟写入的修复信息
struct RepairInfo {
  head_off: u64,
  buf: Vec<u8>,
}

/// Scan file with skip on corruption / 扫描文件，遇到损坏时跳过
///
/// Returns (last valid entry end position, repairs to apply) / 返回 (最后有效条目结尾位置, 待修复列表)
async fn scan_recover(file: &File, len: u64) -> (u64, Vec<RepairInfo>) {
  let mut pos = HEADER_SIZE as u64;
  let mut valid_pos = pos;
  let mut repairs = Vec::new();

  while pos < len {
    // Search for magic marker / 搜索魔数标记
    let Some(magic_pos) = search_magic(file, pos, len).await else {
      break;
    };

    // Need 8 bytes before magic for head_offset / magic 前需要 8 字节存放 head_offset
    if magic_pos < 8 {
      pos = magic_pos + 4;
      continue;
    }

    let end_start = magic_pos - 8;
    let end_buf = vec![0u8; END_SIZE];
    let res = file.read_exact_at(end_buf, end_start).await;
    let Ok(_) = res.0 else {
      pos = magic_pos + 4;
      continue;
    };

    // Parse end marker / 解析尾部标记
    let Some(head_off) = parse_end(&res.1) else {
      pos = magic_pos + 4;
      continue;
    };

    let entry_end = magic_pos + 4;

    // Verify head / 验证 Head
    if verify_head(file, head_off, len).await.is_none() {
      // Try repair head using end marker / 尝试用尾部标记修复 Head
      if let Some(buf) = try_repair_head(file, head_off, entry_end, len).await {
        // Collect repair info / 收集修复信息
        repairs.push(RepairInfo { head_off, buf });
        valid_pos = entry_end;
        pos = entry_end;
        continue;
      }

      // Cannot repair, skip / 无法修复，跳过
      warn!("Corrupted entry at {head_off}, skipped");
      pos = magic_pos + 4;
      continue;
    }

    // Entry valid / 条目有效
    valid_pos = entry_end;
    pos = entry_end;
  }

  (valid_pos, repairs)
}

/// Search for magic bytes forward / 向前搜索魔数
async fn search_magic(file: &File, start: u64, end: u64) -> Option<u64> {
  let mut pos = start;

  while pos < end {
    let read_len = ((end - pos) as usize).min(SCAN_BUF_SIZE);
    let buf = vec![0u8; read_len];

    let res = file.read_exact_at(buf, pos).await;
    let buf = res.0.ok().map(|_| res.1)?;

    // Search for magic pattern / 搜索魔数模式
    if let Some(idx) = find_magic(&buf) {
      return Some(pos + idx as u64);
    }

    // Move forward, overlap by 3 bytes / 前进，重叠 3 字节
    if buf.len() < 4 {
      break;
    }
    pos += (buf.len() - 3) as u64;
  }

  None
}

/// Find magic bytes in buffer / 在缓冲区中查找魔数
#[inline]
fn find_magic(buf: &[u8]) -> Option<usize> {
  buf.windows(4).position(|w| w == MAGIC_BYTES)
}
