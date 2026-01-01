//! Fixed-size head with zerocopy
//! 定长头（zerocopy）
//!
//! ## Record Layout
//! 记录布局
//!
//! ```text
//! | magic(1) | Head(24) | crc32(4) | val_data? | key_data |
//!            |<-- crc32 covers -->|
//! ```
//!
//! - magic: 0xFF, for fast record location
//!   魔数，用于快速定位记录
//! - Head: 24 bytes fixed-size header
//!   24 字节定长头
//! - crc32: CRC32 of Head (excludes magic)
//!   Head 的 CRC32（不含 magic）
//! - val_data: val bytes if INFILE mode
//!   INFILE 模式的 val 数据
//! - key_data: key bytes (always INFILE)
//!   key 数据（永远 INFILE）
//!
//! ## Key Storage
//! Key 存储
//!
//! Key is always stored inline (INFILE mode).
//! Key 永远内联存储（INFILE 模式）
//!
//! ## Val Storage
//! Val 存储
//!
//! | val_id_or_pos | Meaning                    |
//! |---------------|----------------------------|
//! | 0             | Tombstone (deleted)        |
//! | HEAD_TOTAL    | INFILE mode                |
//! | other         | FILE mode (file_id)        |
//!
//! ## Tombstone
//! 删除标记
//!
//! val_id_or_pos = 0 means tombstone, val_len = 0, no val_data
//! val_id_or_pos = 0 表示删除标记，val_len = 0，无 val_data

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
  Flag,
  error::{Error, Result},
  load::Load,
};

/// Magic byte
/// 魔数
pub const MAGIC: u8 = 0xFF;

/// Head struct size (24 bytes)
/// Head 结构体大小
pub const HEAD_SIZE: usize = 24;

/// CRC32 size
/// CRC32 大小
pub const CRC_SIZE: usize = 4;

/// Header size without magic: Head(24) + crc32(4) = 28
/// 不含 magic 的头大小
pub const HEAD_CRC: usize = HEAD_SIZE + CRC_SIZE;

/// Total record header size: magic(1) + Head(24) + crc32(4) = 29
/// 总记录头大小（含 magic）
pub const HEAD_TOTAL: usize = 1 + HEAD_CRC;

/// Max infile data size (4MB)
/// 最大内联数据大小（4MB）
pub const INFILE_MAX: usize = 4 * 1024 * 1024;

/// Max key size (64KB)
/// 最大 key 大小（64KB）
pub const KEY_MAX: usize = 64 * 1024;

/// Verify CRC of head bytes / 验证头字节的 CRC
#[inline(always)]
fn verify_crc(head_bytes: &[u8], crc_bytes: &[u8]) -> bool {
  let got = u32::from_le_bytes(unsafe { *crc_bytes.as_ptr().cast::<[u8; 4]>() });
  crc32fast::hash(head_bytes) == got
}

/// WAL entry type for Load trait / WAL 条目类型用于 Load trait
pub struct WalEntry;

impl Load for WalEntry {
  const MAGIC: u8 = MAGIC;
  const HEAD_SIZE: usize = HEAD_TOTAL;

  #[inline]
  fn parse(buf: &[u8]) -> Option<usize> {
    if buf.len() < HEAD_TOTAL || buf[0] != MAGIC {
      return None;
    }

    let head_bytes = &buf[1..1 + HEAD_SIZE];
    let crc_bytes = &buf[1 + HEAD_SIZE..HEAD_TOTAL];
    if !verify_crc(head_bytes, crc_bytes) {
      return None;
    }

    let head = Head::read_from_bytes(head_bytes).ok()?;
    Some(1 + head.record_size())
  }
}

/// Fixed-size head (24 bytes)
/// 定长头（24 字节）
///
/// ## Layout
/// 布局
///
/// | Field       | Size | Description                    |
/// |-------------|------|--------------------------------|
/// | id          | 8    | Incremental ID                 |
/// | val_len     | 4    | Val length on disk             |
/// | key_len     | 2    | Key length (max 64KB)          |
/// | flag        | 1    | Flag (INFILE/FILE/Tombstone)   |
/// | _pad        | 1    | Reserved                       |
/// | val_file_id | 8    | FILE mode file ID (ignored for INFILE) |
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Debug)]
#[repr(C)]
pub struct Head {
  /// Incremental ID
  /// 递增 ID
  pub id: u64,

  /// Val length on disk
  /// Val 磁盘长度
  pub val_len: u32,

  /// Key length on disk (max 64KB)
  /// Key 磁盘长度（最大 64KB）
  pub key_len: u16,

  /// Flag (INFILE/FILE/Tombstone)
  /// 标志
  pub flag: u8,

  /// Reserved padding
  /// 保留填充
  pub _pad: u8,

  /// Val file ID (FILE mode only, ignored for INFILE)
  /// Val 文件 ID（仅 FILE 模式，INFILE 忽略）
  pub val_file_id: u64,
}

impl Head {
  /// Create new head for INFILE val
  /// 创建 INFILE val 的头
  #[inline]
  pub fn new_infile(id: u64, flag: Flag, val_len: u32, key_len: u16) -> Self {
    Self {
      id,
      val_len,
      key_len,
      flag: flag as u8,
      _pad: 0,
      val_file_id: 0,
    }
  }

  /// Create new head for FILE val
  /// 创建 FILE val 的头
  #[inline]
  pub fn new_file(id: u64, flag: Flag, val_file_id: u64, val_len: u32, key_len: u16) -> Self {
    Self {
      id,
      val_len,
      key_len,
      flag: flag as u8,
      _pad: 0,
      val_file_id,
    }
  }

  /// Create tombstone head
  /// 创建删除标记头
  #[inline]
  pub fn new_tombstone(id: u64, key_len: u16) -> Self {
    Self {
      id,
      val_len: 0,
      key_len,
      flag: Flag::Tombstone as u8,
      _pad: 0,
      val_file_id: 0,
    }
  }

  /// Parse head from buffer (buf starts at Head, not magic)
  /// 从缓冲区解析头（buf 从 Head 开始，不含 magic）
  #[inline]
  pub fn parse(buf: &[u8], file_id: u64, pos: u64) -> Result<Self> {
    if buf.len() < HEAD_CRC {
      return Err(Error::InvalidHead);
    }

    let head_bytes = &buf[..HEAD_SIZE];
    let crc_bytes = &buf[HEAD_SIZE..HEAD_CRC];
    if !verify_crc(head_bytes, crc_bytes) {
      return Err(Error::CrcMismatch { file_id, pos });
    }

    Ok(Self::read_from_bytes(head_bytes).unwrap())
  }

  /// Parse head without CRC verification (for recovery, buf includes magic)
  /// 解析头不验证 CRC（用于恢复，buf 包含 magic）
  #[inline]
  pub fn parse_unchecked(buf: &[u8]) -> Option<Self> {
    if buf.len() < HEAD_TOTAL || buf[0] != MAGIC {
      return None;
    }
    Self::read_from_bytes(&buf[1..1 + HEAD_SIZE]).ok()
  }

  /// Write head to buffer (writes magic, head, crc)
  /// 写入头到缓冲区（写入 magic、head、crc）
  #[inline]
  pub fn write(&self, buf: &mut [u8]) {
    debug_assert!(buf.len() >= HEAD_TOTAL);
    buf[0] = MAGIC;
    buf[1..1 + HEAD_SIZE].copy_from_slice(self.as_bytes());
    let crc = crc32fast::hash(&buf[1..1 + HEAD_SIZE]);
    buf[1 + HEAD_SIZE..HEAD_TOTAL].copy_from_slice(&crc.to_le_bytes());
  }

  /// Get flag
  /// 获取标志
  #[inline(always)]
  pub fn flag(&self) -> Flag {
    Flag::from_u8(self.flag)
  }

  /// Check if tombstone
  /// 检查是否删除标记
  #[inline(always)]
  pub fn is_tombstone(&self) -> bool {
    self.flag().is_tombstone()
  }

  /// Check if val is infile
  /// 检查 val 是否内联
  #[inline(always)]
  pub fn val_is_infile(&self) -> bool {
    self.flag().is_infile()
  }

  /// Get record size without magic (Head + CRC + data)
  /// 获取不含 magic 的记录大小
  #[inline(always)]
  pub fn record_size(&self) -> usize {
    let f = self.flag();
    if f.is_infile() {
      HEAD_CRC + self.val_len as usize + self.key_len as usize
    } else {
      // FILE or Tombstone: no val data inline
      // FILE 或 Tombstone：无内联 val 数据
      HEAD_CRC + self.key_len as usize
    }
  }

  /// Get val data offset in record (HEAD_CRC for INFILE)
  /// 获取 val 数据在记录中的偏移
  #[inline(always)]
  pub fn val_off(&self) -> usize {
    HEAD_CRC
  }

  /// Get key data offset in record
  /// 获取 key 数据在记录中的偏移
  #[inline(always)]
  pub fn key_off(&self) -> usize {
    if self.flag().is_infile() {
      HEAD_CRC + self.val_len as usize
    } else {
      HEAD_CRC
    }
  }

  /// Get val data from record buffer (buf starts at Head)
  /// 从记录缓冲区获取 val 数据（buf 从 Head 开始）
  #[inline(always)]
  pub fn val_data<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
    &buf[HEAD_CRC..HEAD_CRC + self.val_len as usize]
  }

  /// Get key data from record buffer (buf starts at Head)
  /// 从记录缓冲区获取 key 数据（buf 从 Head 开始）
  #[inline(always)]
  pub fn key_data<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
    let off = self.key_off();
    &buf[off..off + self.key_len as usize]
  }
}

/// Head builder for writing records
/// 头构建器（用于写入记录）
pub struct HeadBuilder {
  buf: Vec<u8>,
}

impl Default for HeadBuilder {
  fn default() -> Self {
    Self::new()
  }
}

impl HeadBuilder {
  pub fn new() -> Self {
    Self {
      buf: Vec::with_capacity(128),
    }
  }

  /// Build record: INFILE val
  /// 构建记录：INFILE val
  pub fn infile(&mut self, id: u64, flag: Flag, val: &[u8], key: &[u8]) -> &[u8] {
    let head = Head::new_infile(id, flag, val.len() as u32, key.len() as u16);
    self.build(&head, Some(val), key)
  }

  /// Build record: FILE val
  /// 构建记录：FILE val
  pub fn file(&mut self, id: u64, flag: Flag, val_file_id: u64, val_len: u32, key: &[u8]) -> &[u8] {
    let head = Head::new_file(id, flag, val_file_id, val_len, key.len() as u16);
    self.build(&head, None, key)
  }

  /// Build tombstone record
  /// 构建删除标记记录
  pub fn tombstone(&mut self, id: u64, key: &[u8]) -> &[u8] {
    let head = Head::new_tombstone(id, key.len() as u16);
    self.build(&head, None, key)
  }

  #[inline]
  fn build(&mut self, head: &Head, val: Option<&[u8]>, key: &[u8]) -> &[u8] {
    self.buf.clear();
    self.buf.resize(HEAD_TOTAL, 0);
    head.write(&mut self.buf);

    if let Some(v) = val {
      self.buf.extend_from_slice(v);
    }
    self.buf.extend_from_slice(key);

    &self.buf
  }
}
