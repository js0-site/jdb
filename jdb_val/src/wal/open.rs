//! WAL open/recover / WAL 打开/恢复

use std::fs;

use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio_fs::{File, OpenOptions};
use log::warn;
use memchr::memmem::Finder;
use zerocopy::FromBytes;

use super::{
  Wal,
  consts::{END_SIZE, HEADER_SIZE, MAGIC_BYTES, MIN_FAST_SIZE, SCAN_BUF_SIZE},
  end::parse_end,
  header::{HeaderState, build_header, check_header},
};
use crate::{Head, error::Result};

/// SIMD-accelerated magic finder / SIMD 加速的魔数查找器
static MAGIC_FINDER: std::sync::LazyLock<Finder<'static>> =
  std::sync::LazyLock::new(|| Finder::new(&MAGIC_BYTES));

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
      // 防止覆盖已经有的文件
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

    // Reuse buffer for header reads / 复用头读取缓冲区
    let mut header_buf = Vec::with_capacity(HEADER_SIZE);

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
      header_buf.clear();
      // SAFETY: read_exact_at will overwrite / read_exact_at 会覆盖
      unsafe { header_buf.set_len(HEADER_SIZE) };
      let res = file.read_exact_at(std::mem::take(&mut header_buf), 0).await;
      if res.0.is_err() {
        header_buf = res.1;
        continue;
      }
      header_buf = res.1;

      if matches!(check_header(&mut header_buf), HeaderState::Invalid) {
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
#[allow(clippy::uninit_vec)]
async fn try_fast_recover(file: &File, len: u64) -> Option<u64> {
  if len < MIN_FAST_SIZE {
    return None;
  }

  // Read last 12 bytes / 读取最后 12 字节
  let mut buf = Vec::with_capacity(END_SIZE);
  unsafe { buf.set_len(END_SIZE) };
  let res = file.read_exact_at(buf, len - END_SIZE as u64).await;
  let buf = res.0.ok().map(|_| res.1)?;

  // Parse end marker / 解析尾部标记
  let head_off = parse_end(&buf)?;

  // Validate head / 验证 Head
  let (valid, _) = verify_head(file, head_off, len, buf).await;
  if !valid {
    return None;
  }

  // Note: We don't verify prev end marker because:
  // 1. First entry with infile data has head_off > HEADER_SIZE but no prev end marker
  // 2. Head CRC validation is sufficient for fast recovery
  // 注意：不验证前一个 End Marker，因为：
  // 1. 第一个条目如果有 infile 数据，head_off > HEADER_SIZE 但没有前一个 End Marker
  // 2. Head CRC 验证对于快速恢复已经足够

  // Fast recovery OK: cur_pos = file_len / 快速恢复成功
  Some(len)
}

/// Verify head at offset / 验证偏移处的 Head
#[allow(clippy::uninit_vec)]
async fn verify_head(file: &File, head_off: u64, len: u64, mut buf: Vec<u8>) -> (bool, Vec<u8>) {
  if head_off < HEADER_SIZE as u64 || head_off + Head::SIZE as u64 > len {
    return (false, buf);
  }

  buf.clear();
  buf.reserve(Head::SIZE);
  unsafe { buf.set_len(Head::SIZE) };

  let res = file.read_exact_at(buf, head_off).await;
  if res.0.is_err() {
    return (false, res.1);
  }
  let buf = res.1;

  // Note: compio may return buffer with different length / compio 可能返回不同长度的缓冲区
  if buf.len() < Head::SIZE {
    return (false, buf);
  }

  let Ok(head) = Head::read_from_bytes(&buf[..Head::SIZE]) else {
    return (false, buf);
  };

  // SAFETY: Head::CRC_RANGE < Head::SIZE, bounds checked above / 常量范围安全，上方已检查边界
  let crc = crc32fast::hash(unsafe { buf.get_unchecked(..Head::CRC_RANGE) });
  (crc == head.head_crc32.get(), buf)
}

/// Try repair head using end marker info / 尝试用尾部标记信息修复 Head
///
/// Returns repaired head bytes if successful / 成功返回修复后的 Head 字节
#[allow(clippy::uninit_vec)]
async fn try_repair_head(
  file: &File,
  head_off: u64,
  entry_end: u64,
  len: u64,
  mut buf: Vec<u8>,
) -> (Option<Vec<u8>>, Vec<u8>) {
  if head_off < HEADER_SIZE as u64 || head_off + Head::SIZE as u64 > len {
    return (None, buf);
  }

  buf.clear();
  buf.reserve(Head::SIZE);
  unsafe { buf.set_len(Head::SIZE) };

  let res = file.read_exact_at(buf, head_off).await;
  if res.0.is_err() {
    return (None, res.1);
  }
  let mut buf = res.1;

  let Some(head) = Head::read_from_bytes(&buf).ok() else {
    return (None, buf);
  };

  // Compute infile data length / 计算 infile 数据长度
  let Some(total_infile) = entry_end
    .checked_sub(head_off)
    .and_then(|v| v.checked_sub((Head::SIZE + END_SIZE) as u64))
  else {
    return (None, buf);
  };

  let k_infile = head.key_flag.is_infile();
  let v_infile = head.val_flag.is_infile();

  // Try repair based on which fields are infile / 根据哪些字段是 infile 尝试修复
  let repaired = match (k_infile, v_infile) {
    (true, false) => try_repair_len(&mut buf, KEY_LEN_OFF, total_infile as u32),
    (false, true) => try_repair_len(&mut buf, VAL_LEN_OFF, total_infile as u32),
    (true, true) => {
      // Both infile, try repair key_len (val_len = total - key_len) / 两者都是 infile
      let cur_key_len = head.key_len.get() as u64;
      let cur_val_len = head.val_len.get() as u64;

      // Try keeping key_len, fix val_len / 尝试保持 key_len，修复 val_len
      if cur_key_len <= total_infile {
        let new_val_len = (total_infile - cur_key_len) as u32;
        if try_repair_len(&mut buf, VAL_LEN_OFF, new_val_len) {
          true
        } else if cur_val_len <= total_infile {
          // Try keeping val_len, fix key_len / 尝试保持 val_len，修复 key_len
          let new_key_len = (total_infile - cur_val_len) as u32;
          try_repair_len(&mut buf, KEY_LEN_OFF, new_key_len)
        } else {
          false
        }
      } else {
        false
      }
    }
    (false, false) => false,
  };

  if repaired {
    (Some(buf), Vec::with_capacity(Head::SIZE))
  } else {
    (None, buf)
  }
}

/// Offset for key_len field / key_len 字段偏移
const KEY_LEN_OFF: usize = 0;
/// Offset for val_len field / val_len 字段偏移
const VAL_LEN_OFF: usize = 4;

/// Try repair by setting len at offset / 尝试通过设置偏移处的长度修复
fn try_repair_len(buf: &mut [u8], off: usize, new_len: u32) -> bool {
  buf[off..off + 4].copy_from_slice(&new_len.to_le_bytes());
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
#[allow(clippy::uninit_vec)]
async fn scan_recover(file: &File, len: u64) -> (u64, Vec<RepairInfo>) {
  let mut pos = HEADER_SIZE as u64;
  let mut valid_pos = pos;
  let mut repairs = Vec::new();

  // Reuse head buffer / 复用 head 缓冲区
  let mut head_buf = Vec::with_capacity(Head::SIZE);

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

    // Read end marker / 读取尾部标记
    // Allocate new buffer (compio takes ownership)
    // 分配新缓冲区（compio 会获取所有权）
    let mut end_buf = Vec::with_capacity(END_SIZE);
    unsafe { end_buf.set_len(END_SIZE) };
    let res = file.read_exact_at(end_buf, end_start).await;
    if res.0.is_err() {
      pos = magic_pos + 4;
      continue;
    }
    let end_buf = res.1;

    // Parse end marker / 解析尾部标记
    let Some(head_off) = parse_end(&end_buf) else {
      pos = magic_pos + 4;
      continue;
    };

    let entry_end = magic_pos + 4;

    // Verify head / 验证 Head
    let (valid, buf) = verify_head(file, head_off, len, head_buf).await;
    head_buf = buf;

    if !valid {
      // Try repair head using end marker / 尝试用尾部标记修复 Head
      let (repaired, buf) = try_repair_head(file, head_off, entry_end, len, head_buf).await;
      head_buf = buf;

      if let Some(repair_buf) = repaired {
        // Collect repair info / 收集修复信息
        repairs.push(RepairInfo {
          head_off,
          buf: repair_buf,
        });
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
// SAFETY: uninit_vec is safe here because read_exact_at will fully overwrite the buffer.
// If read fails, buffer is dropped without accessing uninitialized data.
// 安全性：read_exact_at 会完全覆盖缓冲区，读取失败时缓冲区直接丢弃，不会访问未初始化数据
#[allow(clippy::uninit_vec)]
async fn search_magic(file: &File, start: u64, end: u64) -> Option<u64> {
  let mut pos = start;

  while pos < end {
    // Prevent usize overflow on 32-bit systems
    let read_len = (end - pos).min(SCAN_BUF_SIZE as u64) as usize;

    // Allocate buffer for each read (compio takes ownership)
    // 每次读取分配缓冲区（compio 会获取所有权）
    let mut buf = Vec::with_capacity(read_len);
    unsafe { buf.set_len(read_len) };
    let res = file.read_exact_at(buf, pos).await;
    if res.0.is_err() {
      return None;
    }
    let buf = res.1;

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

/// Find magic bytes in buffer (SIMD-accelerated) / 在缓冲区中查找魔数（SIMD 加速）
#[inline]
fn find_magic(buf: &[u8]) -> Option<usize> {
  MAGIC_FINDER.find(buf)
}
