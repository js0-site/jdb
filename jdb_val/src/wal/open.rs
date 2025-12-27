//! WAL open/recover / WAL 打开/恢复

use std::fs;

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
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
    fs::create_dir_all(&self.bin_dir)?;

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
  #[allow(clippy::uninit_vec)]
  async fn find_newest(&self) -> Option<(u64, File, u64)> {
    let entries = fs::read_dir(&self.wal_dir).ok()?;

    // Collect and sort by id desc / 收集并按 id 降序排序
    let mut ids: Vec<u64> = entries
      .flatten()
      .filter_map(|e| {
        let name = e.file_name();
        let name = name.to_str()?;
        Wal::decode_id(name)
      })
      .collect();
    ids.sort_unstable();

    // Reuse buffer for header reading / 复用 buffer 读取头
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
      // Use slice to enforce exact read length / 使用 slice 强制精确读取长度
      if header_buf.capacity() < HEADER_SIZE {
        header_buf = Vec::with_capacity(HEADER_SIZE);
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
  let slice = buf.slice(0..END_SIZE);
  let res = file.read_exact_at(slice, len - END_SIZE as u64).await;
  let buf = res.0.ok().map(|_| res.1.into_inner())?;

  // Parse end marker / 解析尾部标记
  let head_off = parse_end(&buf)?;

  // Validate head / 验证 Head
  let head_buf = Vec::with_capacity(Head::SIZE);
  let (ok, _) = verify_head(file, head_off, len, head_buf).await;
  if ok { Some(len) } else { None }
}

/// Verify head at offset / 验证偏移处的 Head
///
/// Takes ownership of buffer to avoid allocation, returns (is_valid, buffer)
/// 获取 buffer 所有权以避免分配，返回 (是否有效, buffer)
#[allow(clippy::uninit_vec)]
async fn verify_head(file: &File, head_off: u64, len: u64, mut buf: Vec<u8>) -> (bool, Vec<u8>) {
  if head_off < HEADER_SIZE as u64 || head_off + Head::SIZE as u64 > len {
    return (false, buf);
  }

  if buf.capacity() < Head::SIZE {
    buf.reserve(Head::SIZE - buf.len());
  }
  unsafe { buf.set_len(Head::SIZE) };

  let slice = buf.slice(0..Head::SIZE);
  let res = file.read_exact_at(slice, head_off).await;
  buf = res.1.into_inner();
  if res.0.is_err() {
    return (false, buf);
  }

  let Ok(head) = Head::read_from_bytes(&buf) else {
    return (false, buf);
  };

  // SAFETY: Head::CRC_RANGE < Head::SIZE / 常量范围安全
  let crc = crc32fast::hash(unsafe { buf.get_unchecked(..Head::CRC_RANGE) });
  (crc == head.head_crc32.get(), buf)
}

/// Try repair head using existing buffer / 使用现有缓冲区尝试修复 Head
///
/// Modifies buf in-place if repairable. Returns true if repaired.
/// 如果可修复则就地修改 buf。成功返回 true。
fn try_repair_head_inplace(buf: &mut [u8], head_off: u64, entry_end: u64) -> bool {
  let Ok(head) = Head::read_from_bytes(buf) else {
    return false;
  };

  // Compute infile data length / 计算 infile 数据长度
  let Some(total_infile) = entry_end
    .checked_sub(head_off)
    .and_then(|v| v.checked_sub((Head::SIZE + END_SIZE) as u64))
  else {
    return false;
  };

  let k_infile = head.key_flag.is_infile();
  let v_infile = head.val_flag.is_infile();

  match (k_infile, v_infile) {
    (true, false) => {
      repair_len(buf, KEY_LEN_OFF, total_infile as u32);
      true
    }
    (false, true) => {
      repair_len(buf, VAL_LEN_OFF, total_infile as u32);
      true
    }
    (true, true) => {
      let cur_key_len = head.key_len.get() as u64;
      let cur_val_len = head.val_len.get() as u64;

      if cur_key_len <= total_infile {
        let new_val_len = (total_infile - cur_key_len) as u32;
        repair_len(buf, VAL_LEN_OFF, new_val_len);
        true
      } else if cur_val_len <= total_infile {
        let new_key_len = (total_infile - cur_val_len) as u32;
        repair_len(buf, KEY_LEN_OFF, new_key_len);
        true
      } else {
        false
      }
    }
    (false, false) => false,
  }
}

/// Offset for key_len field / key_len 字段偏移
const KEY_LEN_OFF: usize = 0;
/// Offset for val_len field / val_len 字段偏移
const VAL_LEN_OFF: usize = 4;

/// Repair len at offset and recalc CRC / 修复偏移处的长度并重算 CRC
fn repair_len(buf: &mut [u8], off: usize, new_len: u32) {
  buf[off..off + 4].copy_from_slice(&new_len.to_le_bytes());
  let crc = crc32fast::hash(&buf[..Head::CRC_RANGE]);
  buf[Head::CRC_RANGE..Head::SIZE].copy_from_slice(&crc.to_le_bytes());
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
  let mut scan_buf = Vec::with_capacity(SCAN_BUF_SIZE);
  // Reuse end_buf across iterations / 复用 end_buf
  let mut end_buf = Vec::with_capacity(END_SIZE);
  // Reuse head_buf across iterations / 复用 head_buf
  let mut head_buf = Vec::with_capacity(Head::SIZE);

  while pos < len {
    // Search for magic marker / 搜索魔数标记
    let (found, buf) = search_magic(file, pos, len, scan_buf).await;
    scan_buf = buf;
    let Some(magic_pos) = found else {
      break;
    };

    if magic_pos < 8 {
      pos = magic_pos + 4;
      continue;
    }

    let end_start = magic_pos - 8;

    // Read end marker / 读取尾部标记
    if end_buf.capacity() < END_SIZE {
      end_buf = Vec::with_capacity(END_SIZE);
    }
    unsafe { end_buf.set_len(END_SIZE) };
    let slice = end_buf.slice(0..END_SIZE);
    let res = file.read_exact_at(slice, end_start).await;
    end_buf = res.1.into_inner();
    if res.0.is_err() {
      pos = magic_pos + 4;
      continue;
    }

    let Some(head_off) = parse_end(&end_buf) else {
      pos = magic_pos + 4;
      continue;
    };

    let entry_end = magic_pos + 4;

    // Verify head with reuse of head_buf / 使用 head_buf 复用验证 Head
    let (ok, buf) = verify_head(file, head_off, len, head_buf).await;
    head_buf = buf; // reclaim buffer / 取回 buffer

    if !ok {
      // Try repair in-place without new IO / 尝试就地修复，无需新 IO
      if try_repair_head_inplace(&mut head_buf, head_off, entry_end) {
        repairs.push(RepairInfo {
          head_off,
          buf: head_buf,
        });
        // Allocate new buffer for next iteration since we moved the old one
        // 因为旧 buffer 已移动，为下次迭代分配新 buffer
        head_buf = Vec::with_capacity(Head::SIZE);
        valid_pos = entry_end;
        pos = entry_end;
        continue;
      }

      warn!("Corrupted entry at {head_off}, skipped");
      pos = magic_pos + 4;
      continue;
    }

    valid_pos = entry_end;
    pos = entry_end;
  }

  (valid_pos, repairs)
}

/// Search for magic bytes forward / 向前搜索魔数
#[allow(clippy::uninit_vec)]
async fn search_magic(
  file: &File,
  start: u64,
  end: u64,
  mut buf: Vec<u8>,
) -> (Option<u64>, Vec<u8>) {
  let mut pos = start;

  while pos < end {
    let read_len = (end - pos).min(SCAN_BUF_SIZE as u64) as usize;

    if buf.capacity() < read_len {
      buf.reserve(read_len - buf.len());
    }
    unsafe { buf.set_len(read_len) };

    let slice = buf.slice(0..read_len);
    let res = file.read_exact_at(slice, pos).await;
    buf = res.1.into_inner();
    if let Err(e) = res.0 {
      log::debug!("search_magic: read failed at pos={pos}, error={e:?}");
      return (None, buf);
    }

    if let Some(idx) = find_magic(&buf) {
      return (Some(pos + idx as u64), buf);
    }

    if buf.len() < 4 {
      break;
    }
    pos += (buf.len() - 3) as u64;
  }

  (None, buf)
}

#[inline]
fn find_magic(buf: &[u8]) -> Option<usize> {
  MAGIC_FINDER.find(buf)
}
