//! Value Log for KV separation
//! KV 分离的值日志

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::await_holding_refcell_ref)] // compio single-thread runtime / compio 单线程运行时

mod error;

use std::{
  cell::RefCell,
  path::{Path, PathBuf},
};

use bytes::Bytes;
use coarsetime::Clock;
pub use error::{Error, Result};
use jdb_alloc::{AlignedBuf, PAGE_SIZE};
use jdb_fs::File;
use jdb_trait::ValRef;

/// Tombstone flag in offset / offset 中的 tombstone 标记
const TOMBSTONE_FLAG: u64 = 1 << 63;

/// Record flag: normal value / 记录标记：正常值
const FLAG_VALUE: u8 = 0;

/// Record flag: tombstone / 记录标记：墓碑
const FLAG_TOMBSTONE: u8 = 1;

/// Max file size before rotation (256MB) / 轮转前最大文件大小
const MAX_FILE_SIZE: u64 = 256 * 1024 * 1024;

/// Max key size (64KB) / 最大 key 大小
pub const MAX_KEY_SIZE: usize = u16::MAX as usize;

/// File extension / 文件扩展名
const EXT: &str = "vlog";

/// Body CRC size / Body CRC 大小
const CRC_B_SIZE: usize = 4;

/// Record layout (Header CRC + Body CRC):
/// 记录布局（头部 CRC + 体部 CRC）:
/// ```text
/// [0..4]     crc_h (u32)       - header CRC, covers [4..39] / 头部校验和
/// [4..12]    len (u64)         - record length / 记录长度
/// [12]       flag (u8)         - 0=value, 1=tombstone / 标记
/// [13..21]   ts (u64)          - timestamp seconds / 时间戳秒
/// [21..29]   prev_file_id (u64)
/// [29..37]   prev_offset (u64)
/// [37..39]   key_len (u16)     - max 64KB / 最大 64KB
/// [39..]     key + value
/// [len-4..len] crc_b (u32)     - body CRC, covers [39..len-4] / 体部校验和
/// ```
/// Header: 39 bytes, Body CRC: 4 bytes
/// 头部: 39 字节, 体部 CRC: 4 字节
const HEADER_SIZE: usize = 39;

/// Value Log / 值日志
pub struct VLog {
  dir: PathBuf,
  state: RefCell<VLogState>,
}

struct VLogState {
  active_id: u64,
  active: File,
  active_size: u64,
}

/// Get current timestamp seconds / 获取当前时间戳秒
#[inline]
fn now_secs() -> u64 {
  Clock::recent_since_epoch().as_secs()
}

impl VLog {
  /// Open or create VLog / 打开或创建 VLog
  pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
    Clock::update();

    let dir = dir.as_ref().to_path_buf();
    jdb_fs::mkdir(&dir).await?;

    let files = jdb_fs::ls(&dir).await?;
    let mut max_id = 0u64;
    for f in &files {
      if let Some(name) = f.file_name().and_then(|n| n.to_str())
        && let Some(id_str) = name.strip_suffix(&format!(".{EXT}"))
        && let Ok(id) = id_str.parse::<u64>()
      {
        max_id = max_id.max(id);
      }
    }

    let active_id = if max_id == 0 { 1 } else { max_id };
    let active_path = Self::file_path(&dir, active_id);
    let (active, active_size) = if jdb_fs::exists(&active_path) {
      let f = File::open_rw(&active_path).await?;
      let size = f.size().await?;
      (f, size)
    } else {
      let f = File::create(&active_path).await?;
      (f, 0)
    };

    Ok(Self {
      dir,
      state: RefCell::new(VLogState {
        active_id,
        active,
        active_size,
      }),
    })
  }

  fn file_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:08}.{EXT}"))
  }

  /// Append value / 追加值
  pub async fn append(&self, key: &[u8], val: &[u8], prev: Option<&ValRef>) -> Result<ValRef> {
    if key.len() > MAX_KEY_SIZE {
      return Err(Error::KeyTooLarge(key.len()));
    }
    self.append_inner(key, Some(val), prev, FLAG_VALUE).await
  }

  /// Append tombstone / 追加墓碑
  pub async fn append_tombstone(&self, key: &[u8], prev: Option<&ValRef>) -> Result<ValRef> {
    if key.len() > MAX_KEY_SIZE {
      return Err(Error::KeyTooLarge(key.len()));
    }
    self.append_inner(key, None, prev, FLAG_TOMBSTONE).await
  }

  async fn append_inner(
    &self,
    key: &[u8],
    val: Option<&[u8]>,
    prev: Option<&ValRef>,
    flag: u8,
  ) -> Result<ValRef> {
    let mut state = self.state.borrow_mut();

    if state.active_size >= MAX_FILE_SIZE {
      self.rotate_inner(&mut state).await?;
    }

    let val_len = val.map(|v| v.len()).unwrap_or(0);
    let record_len = HEADER_SIZE + key.len() + val_len + CRC_B_SIZE;
    let aligned_len = align_up(record_len);

    let mut buf = AlignedBuf::zeroed(aligned_len)?;

    // len (8B) at [4..12]
    buf[4..12].copy_from_slice(&(record_len as u64).to_le_bytes());

    // flag (1B) at [12]
    buf[12] = flag;

    // ts (8B) at [13..21]
    let ts = now_secs();
    buf[13..21].copy_from_slice(&ts.to_le_bytes());

    // prev_file_id (8B) + prev_offset (8B) at [21..37]
    let (pfid, poff) = prev.map(|p| (p.file_id, p.offset)).unwrap_or((0, 0));
    buf[21..29].copy_from_slice(&pfid.to_le_bytes());
    buf[29..37].copy_from_slice(&poff.to_le_bytes());

    // key_len (2B) at [37..39]
    buf[37..39].copy_from_slice(&(key.len() as u16).to_le_bytes());

    // crc_h (4B) at [0..4] - covers [4..39]
    let crc_h = crc32(&buf[4..HEADER_SIZE]);
    buf[0..4].copy_from_slice(&crc_h.to_le_bytes());

    // key + value at [39..]
    buf[HEADER_SIZE..HEADER_SIZE + key.len()].copy_from_slice(key);
    if let Some(v) = val {
      buf[HEADER_SIZE + key.len()..HEADER_SIZE + key.len() + v.len()].copy_from_slice(v);
    }

    // crc_b (4B) at [len-4..len] - covers [39..len-4]
    let body_end = record_len - CRC_B_SIZE;
    let crc_b = crc32(&buf[HEADER_SIZE..body_end]);
    buf[body_end..record_len].copy_from_slice(&crc_b.to_le_bytes());

    // Write
    let offset = state.active_size;
    state.active.write_at(buf, offset).await?;
    state.active_size += aligned_len as u64;

    let mut result_offset = offset;
    if flag == FLAG_TOMBSTONE {
      result_offset |= TOMBSTONE_FLAG;
    }

    Ok(ValRef {
      file_id: state.active_id,
      offset: result_offset,
      prev_file_id: prev.map(|p| p.file_id).unwrap_or(0),
      prev_offset: prev.map(|p| p.offset).unwrap_or(0),
    })
  }

  /// Get value / 获取值
  pub async fn get(&self, vref: &ValRef) -> Result<Option<Bytes>> {
    if vref.is_tombstone() {
      return Ok(None);
    }
    self
      .get_full(vref)
      .await
      .map(|res| res.map(|(val, ..)| val))
  }

  /// Get value, full ValRef, and timestamp / 获取值、完整引用和时间戳
  pub async fn get_full(&self, vref: &ValRef) -> Result<Option<(Bytes, ValRef, u64)>> {
    if vref.is_tombstone() && vref.real_offset() == 0 {
      return Ok(None);
    }

    let file = self.open_file(vref.file_id).await?;
    let offset = vref.real_offset();

    // Read header (39 bytes, aligned to PAGE_SIZE)
    let header_buf = AlignedBuf::zeroed(PAGE_SIZE)?;
    let header_buf = file.read_at(header_buf, offset).await?;

    // Verify header CRC first / 先校验头部 CRC
    let crc_h_stored =
      u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
    let crc_h_calc = crc32(&header_buf[4..HEADER_SIZE]);
    if crc_h_stored != crc_h_calc {
      return Err(Error::HeaderCrc {
        expected: crc_h_stored,
        got: crc_h_calc,
      });
    }

    // Parse header fields / 解析头部字段
    let len = u64::from_le_bytes([
      header_buf[4],
      header_buf[5],
      header_buf[6],
      header_buf[7],
      header_buf[8],
      header_buf[9],
      header_buf[10],
      header_buf[11],
    ]) as usize;

    let flag = header_buf[12];

    let ts = u64::from_le_bytes([
      header_buf[13],
      header_buf[14],
      header_buf[15],
      header_buf[16],
      header_buf[17],
      header_buf[18],
      header_buf[19],
      header_buf[20],
    ]);

    let prev_file_id = u64::from_le_bytes([
      header_buf[21],
      header_buf[22],
      header_buf[23],
      header_buf[24],
      header_buf[25],
      header_buf[26],
      header_buf[27],
      header_buf[28],
    ]);

    let prev_offset = u64::from_le_bytes([
      header_buf[29],
      header_buf[30],
      header_buf[31],
      header_buf[32],
      header_buf[33],
      header_buf[34],
      header_buf[35],
      header_buf[36],
    ]);

    let key_len = u16::from_le_bytes([header_buf[37], header_buf[38]]) as usize;

    let full_vref = ValRef {
      file_id: vref.file_id,
      offset: vref.offset,
      prev_file_id,
      prev_offset,
    };

    if flag == FLAG_TOMBSTONE {
      return Ok(Some((Bytes::new(), full_vref, ts)));
    }

    // Read body if needed / 读取 body
    let body_start = HEADER_SIZE;
    let body_end = len - CRC_B_SIZE;
    let body_len = body_end - body_start;

    // Check if body fits in header_buf / 检查 body 是否在 header_buf 中
    let body_data = if len <= PAGE_SIZE {
      // Body in header_buf / Body 在 header_buf 中
      &header_buf[body_start..body_end]
    } else {
      // Need to read more / 需要读取更多
      // For now, assume small records fit in PAGE_SIZE
      // TODO: support large records / 支持大记录
      return Err(Error::RecordTooLarge(len));
    };

    // Verify body CRC / 校验 body CRC
    let crc_b_stored = u32::from_le_bytes([
      header_buf[body_end],
      header_buf[body_end + 1],
      header_buf[body_end + 2],
      header_buf[body_end + 3],
    ]);
    let crc_b_calc = crc32(body_data);
    if crc_b_stored != crc_b_calc {
      return Err(Error::BodyCrc {
        expected: crc_b_stored,
        got: crc_b_calc,
      });
    }

    let val_start = key_len;
    let val_end = body_len;

    Ok(Some((
      Bytes::copy_from_slice(&body_data[val_start..val_end]),
      full_vref,
      ts,
    )))
  }

  async fn open_file(&self, file_id: u64) -> Result<File> {
    let path = Self::file_path(&self.dir, file_id);
    if !jdb_fs::exists(&path) {
      return Err(Error::FileNotFound(file_id));
    }
    Ok(File::open(&path).await?)
  }

  /// Rotate to new file / 轮转到新文件
  pub async fn rotate(&self) -> Result<()> {
    let mut state = self.state.borrow_mut();
    self.rotate_inner(&mut state).await
  }

  async fn rotate_inner(&self, state: &mut VLogState) -> Result<()> {
    state.active.sync_data().await?;
    state.active_id += 1;
    let path = Self::file_path(&self.dir, state.active_id);
    state.active = File::create(&path).await?;
    state.active_size = 0;
    Ok(())
  }

  /// Sync to disk / 同步到磁盘
  pub async fn sync(&self) -> Result<()> {
    let state = self.state.borrow();
    state.active.sync_data().await?;
    Ok(())
  }

  /// Get active file id / 获取活跃文件 ID
  pub fn active_id(&self) -> u64 {
    self.state.borrow().active_id
  }

  /// Get active file size / 获取活跃文件大小
  pub fn active_size(&self) -> u64 {
    self.state.borrow().active_size
  }

  /// Update time cache / 更新时间缓存
  #[inline]
  pub fn update_time() {
    Clock::update();
  }
}

#[inline]
fn align_up(n: usize) -> usize {
  (n + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
}

fn crc32(data: &[u8]) -> u32 {
  let mut crc = 0xFFFF_FFFFu32;
  for &byte in data {
    crc ^= byte as u32;
    for _ in 0..8 {
      crc = if crc & 1 != 0 {
        (crc >> 1) ^ 0xEDB8_8320
      } else {
        crc >> 1
      };
    }
  }
  !crc
}
