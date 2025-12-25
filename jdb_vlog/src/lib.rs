//! Value Log for KV separation
//! KV 分离的值日志

#![cfg_attr(docsrs, feature(doc_cfg))]

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

/// Record layout:
/// 记录布局:
/// ```text
/// [0..8]     len (u64)         - record length / 记录长度
/// [8..12]    crc32 (u32)       - checksum of [12..len-8] / 校验和
/// [12]       flag (u8)         - 0=value, 1=tombstone / 标记
/// [13..21]   ts (u64)          - timestamp seconds / 时间戳秒
/// [21..29]   prev_file_id (u64)
/// [29..37]   prev_offset (u64)
/// [37..39]   key_len (u16)     - max 64KB / 最大 64KB
/// [39..]     key + value
/// [len-8..len] len (u64)       - tail length for reverse scan / 尾部长度
/// ```
/// Header size: 39 bytes, Tail size: 8 bytes
/// 头部: 39 字节, 尾部: 8 字节
const HEADER_SIZE: usize = 39;
const TAIL_SIZE: usize = 8;

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
    let record_len = HEADER_SIZE + key.len() + val_len + TAIL_SIZE;
    let aligned_len = align_up(record_len);

    let mut buf = AlignedBuf::zeroed(aligned_len)?;

    // len (8B)
    buf[0..8].copy_from_slice(&(record_len as u64).to_le_bytes());

    // flag (1B)
    buf[12] = flag;

    // ts (8B)
    let ts = now_secs();
    buf[13..21].copy_from_slice(&ts.to_le_bytes());

    // prev_file_id (8B) + prev_offset (8B)
    let (pfid, poff) = prev.map(|p| (p.file_id, p.offset)).unwrap_or((0, 0));
    buf[21..29].copy_from_slice(&pfid.to_le_bytes());
    buf[29..37].copy_from_slice(&poff.to_le_bytes());

    // key_len (2B)
    buf[37..39].copy_from_slice(&(key.len() as u16).to_le_bytes());

    // key + value
    buf[39..39 + key.len()].copy_from_slice(key);
    if let Some(v) = val {
      buf[39 + key.len()..39 + key.len() + v.len()].copy_from_slice(v);
    }

    // tail len (8B)
    let tail_start = record_len - TAIL_SIZE;
    buf[tail_start..record_len].copy_from_slice(&(record_len as u64).to_le_bytes());

    // crc32 (4B) - covers [12..len-8]
    let crc = crc32(&buf[12..tail_start]);
    buf[8..12].copy_from_slice(&crc.to_le_bytes());

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

    let header_buf = AlignedBuf::zeroed(PAGE_SIZE)?;
    let header_buf = file.read_at(header_buf, offset).await?;

    let len = u64::from_le_bytes([
      header_buf[0],
      header_buf[1],
      header_buf[2],
      header_buf[3],
      header_buf[4],
      header_buf[5],
      header_buf[6],
      header_buf[7],
    ]) as usize;

    // Verify tail len / 校验尾部长度
    let tail_start = len - TAIL_SIZE;
    let tail_len = u64::from_le_bytes([
      header_buf[tail_start],
      header_buf[tail_start + 1],
      header_buf[tail_start + 2],
      header_buf[tail_start + 3],
      header_buf[tail_start + 4],
      header_buf[tail_start + 5],
      header_buf[tail_start + 6],
      header_buf[tail_start + 7],
    ]) as usize;

    if len != tail_len {
      return Err(Error::LenMismatch {
        head: len,
        tail: tail_len,
      });
    }

    let crc_stored =
      u32::from_le_bytes([header_buf[8], header_buf[9], header_buf[10], header_buf[11]]);

    let crc_calc = crc32(&header_buf[12..tail_start]);
    if crc_stored != crc_calc {
      return Err(Error::Crc {
        expected: crc_stored,
        got: crc_calc,
      });
    }

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

    let val_start = HEADER_SIZE + key_len;
    let val_end = tail_start;

    Ok(Some((
      Bytes::copy_from_slice(&header_buf[val_start..val_end]),
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
