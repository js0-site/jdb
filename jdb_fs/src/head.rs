//! Fixed-size head with zerocopy
//! 定长头（zerocopy）
//!
//! ## Record Layout
//! ```text
//! | magic(1) | Head(24) | crc32(4) | val_data? | key_data |
//!            |<-- crc32 covers -->|
//! ```

use jdb_base::{Flag, Pos};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// Magic byte
pub const MAGIC: u8 = 0xFF;

/// Head struct size (24 bytes)
pub const HEAD_SIZE: usize = 24;

/// CRC32 size
pub const CRC_SIZE: usize = 4;

/// Header size without magic: Head(24) + crc32(4) = 28
pub const HEAD_CRC: usize = HEAD_SIZE + CRC_SIZE;

/// Total record header size: magic(1) + Head(24) + crc32(4) = 29
pub const HEAD_TOTAL: usize = 1 + HEAD_CRC;

/// Max infile data size (4MB)
pub const INFILE_MAX: usize = 4 * 1024 * 1024;

/// Max key size (u16::MAX)
pub const KEY_MAX: usize = u16::MAX as usize;

/// Fixed-size head (24 bytes)
/// 定长头（24 字节）
///
/// | Field       | Size | Description                    |
/// |-------------|------|--------------------------------|
/// | id          | 8    | Incremental ID                 |
/// | val_len     | 4    | Val length on disk             |
/// | key_len     | 2    | Key length (max 64KB)          |
/// | flag        | 1    | Flag (INFILE/FILE/Tombstone)   |
/// | _pad        | 1    | Reserved                       |
/// | val_file_id | 8    | FILE mode file ID              |
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Debug)]
#[repr(C)]
pub struct Head {
  pub id: u64,
  pub val_len: u32,
  pub key_len: u16,
  pub flag: u8,
  pub _pad: u8,
  pub val_file_id: u64,
}

/// Head parse error
#[derive(Debug, Clone, Copy)]
pub struct HeadError {
  pub file_id: u64,
  pub pos: u64,
}

impl Head {
  /// Create INFILE head
  #[inline]
  pub fn infile(id: u64, flag: Flag, val_len: u32, key_len: u16) -> Self {
    Self {
      id,
      val_len,
      key_len,
      flag: flag.as_u8(),
      _pad: 0,
      val_file_id: 0,
    }
  }

  /// Create FILE head
  #[inline]
  pub fn file(id: u64, flag: Flag, val_file_id: u64, val_len: u32, key_len: u16) -> Self {
    Self {
      id,
      val_len,
      key_len,
      flag: flag.as_u8(),
      _pad: 0,
      val_file_id,
    }
  }

  /// Create tombstone head from existing pos
  /// 从已有位置创建墓碑头
  #[inline]
  pub fn tombstone(id: u64, pos: Pos, key_len: u16) -> Self {
    let tombstone_pos = pos.to_tombstone();
    Self {
      id,
      val_len: tombstone_pos.len(),
      key_len,
      flag: tombstone_pos.flag().as_u8(),
      _pad: 0,
      val_file_id: if tombstone_pos.is_file() {
        tombstone_pos.file_id()
      } else {
        0
      },
    }
  }

  /// Parse head from buffer (buf starts at Head, not magic)
  #[inline]
  pub fn parse(buf: &[u8], file_id: u64, pos: u64) -> Result<Self, HeadError> {
    if buf.len() < HEAD_CRC {
      return Err(HeadError { file_id, pos });
    }

    let head_bytes = &buf[..HEAD_SIZE];

    // Safe: we've verified buf.len() >= HEAD_CRC, so HEAD_SIZE..HEAD_CRC is valid
    let got = u32::from_le_bytes([
      buf[HEAD_SIZE],
      buf[HEAD_SIZE + 1],
      buf[HEAD_SIZE + 2],
      buf[HEAD_SIZE + 3],
    ]);
    let expected = crc32fast::hash(head_bytes);
    if got != expected {
      return Err(HeadError { file_id, pos });
    }

    // Safe: head_bytes has exactly HEAD_SIZE bytes
    Self::read_from_bytes(head_bytes).map_err(|_| HeadError { file_id, pos })
  }

  /// Parse head without CRC verification (buf includes magic)
  #[inline]
  pub fn parse_unchecked(buf: &[u8]) -> Option<Self> {
    if buf.len() < HEAD_TOTAL || buf[0] != MAGIC {
      return None;
    }
    Self::read_from_bytes(&buf[1..1 + HEAD_SIZE]).ok()
  }

  /// Write head to buffer (writes magic, head, crc)
  #[inline]
  pub fn write(&self, buf: &mut [u8]) {
    debug_assert!(buf.len() >= HEAD_TOTAL);
    buf[0] = MAGIC;
    buf[1..1 + HEAD_SIZE].copy_from_slice(self.as_bytes());
    let crc = crc32fast::hash(&buf[1..1 + HEAD_SIZE]);
    buf[1 + HEAD_SIZE..HEAD_TOTAL].copy_from_slice(&crc.to_le_bytes());
  }

  /// Get flag
  #[inline(always)]
  pub fn flag(&self) -> Flag {
    Flag::from_u8(self.flag)
  }

  /// Is tombstone
  #[inline(always)]
  pub fn is_tombstone(&self) -> bool {
    self.flag().is_tombstone()
  }

  /// Is val infile (not tombstone, not file)
  /// 值是否在文件内（非墓碑、非独立文件）
  #[inline(always)]
  pub fn val_is_infile(&self) -> bool {
    let f = self.flag();
    !f.is_tombstone() && f.is_infile()
  }

  /// Check if val is not infile (tombstone or file mode)
  /// 检查 val 是否不在文件内（墓碑或独立文件模式）
  #[inline(always)]
  fn val_not_infile(&self) -> bool {
    let f = self.flag();
    f.is_tombstone() || f.is_file()
  }

  /// Record size without magic (Head + CRC + data)
  /// Tombstone: no val data, only key
  /// 墓碑：无 val 数据，仅 key
  #[inline(always)]
  pub fn record_size(&self) -> usize {
    if self.val_not_infile() {
      // Tombstone and FILE: no val data in record
      // 墓碑和 FILE：记录中无 val 数据
      HEAD_CRC + self.key_len as usize
    } else {
      // INFILE: val data in record
      // INFILE：记录中有 val 数据
      HEAD_CRC + self.val_len as usize + self.key_len as usize
    }
  }

  /// Val data offset in record
  #[inline(always)]
  pub const fn val_offset() -> usize {
    HEAD_CRC
  }

  /// Key data offset in record
  #[inline(always)]
  pub fn key_off(&self) -> usize {
    if self.val_not_infile() {
      HEAD_CRC
    } else {
      HEAD_CRC + self.val_len as usize
    }
  }

  /// Get val data from record buffer (buf starts at Head)
  #[inline(always)]
  pub fn val_data<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
    &buf[HEAD_CRC..HEAD_CRC + self.val_len as usize]
  }

  /// Get key data from record buffer (buf starts at Head)
  #[inline(always)]
  pub fn key_data<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
    let off = self.key_off();
    &buf[off..off + self.key_len as usize]
  }
}

/// Head builder for writing records
pub struct HeadBuilder {
  pub buf: Vec<u8>,
}

impl Default for HeadBuilder {
  fn default() -> Self {
    Self::new()
  }
}

impl HeadBuilder {
  pub fn new() -> Self {
    Self {
      buf: Vec::with_capacity(512), // Optimized capacity / 优化容量
    }
  }

  /// Build INFILE record
  pub fn infile(&mut self, id: u64, flag: Flag, val: &[u8], key: &[u8]) -> &[u8] {
    let head = Head::infile(id, flag, val.len() as u32, key.len() as u16);
    self.build(&head, Some(val), key)
  }

  /// Build FILE record
  pub fn file(&mut self, id: u64, flag: Flag, val_file_id: u64, val_len: u32, key: &[u8]) -> &[u8] {
    let head = Head::file(id, flag, val_file_id, val_len, key.len() as u16);
    self.build(&head, None, key)
  }

  /// Build tombstone record
  pub fn tombstone(&mut self, id: u64, pos: Pos, key: &[u8]) -> &[u8] {
    let head = Head::tombstone(id, pos, key.len() as u16);
    self.build(&head, None, key)
  }

  #[inline]
  fn build(&mut self, head: &Head, val: Option<&[u8]>, key: &[u8]) -> &[u8] {
    self.buf.clear();

    let total_len = HEAD_TOTAL + val.map_or(0, |v| v.len()) + key.len();
    self.buf.reserve(total_len);

    // Direct write to avoid zero-initialization overhead
    // 直接写入以避免零初始化开销
    self.buf.push(MAGIC);
    self.buf.extend_from_slice(head.as_bytes());
    let crc = crc32fast::hash(head.as_bytes());
    self.buf.extend_from_slice(&crc.to_le_bytes());

    if let Some(v) = val {
      self.buf.extend_from_slice(v);
    }

    self.buf.extend_from_slice(key);

    &self.buf
  }
}
