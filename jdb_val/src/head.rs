use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, byteorder::little_endian::U32};

use crate::{
  Flag, Pos,
  error::{Error, Result},
};

/// WAL head (64B) / WAL 头（64字节）
///
/// # Structure / 结构
///
/// | Field      | Type     | Offset | Size | Description                    |
/// |------------|----------|--------|------|--------------------------------|
/// | key_len    | u32      | 0      | 4    | Key length / 键长度             |
/// | val_len    | u32      | 4      | 4    | Value length / 值长度           |
/// | key_flag   | Flag     | 8      | 1    | Key storage flag / 键存储标志   |
/// | val_flag   | Flag     | 9      | 1    | Value storage flag / 值存储标志 |
/// | data       | [u8;50]  | 10     | 50   | Flexible data / 灵活数据区      |
/// | head_crc32 | u32      | 60     | 4    | CRC32 of [0..60] / 头部校验     |
///
/// # Data Layout / 数据区布局
///
/// ## INLINE + INLINE (key+val <= 50B)
///
/// | Offset | Size       | Content           |
/// |--------|------------|-------------------|
/// | 0      | key_len    | key               |
/// | key_len| val_len    | val               |
///
/// ## INLINE + FILE (key <= 30B, val in file)
///
/// | Offset | Size | Content              |
/// |--------|------|----------------------|
/// | 0      | 30   | key (padded)         |
/// | 30     | 16   | val_pos (Pos)        |
/// | 46     | 4    | val_crc32            |
///
/// ## FILE + INLINE (key in file, val <= 34B)
///
/// | Offset | Size | Content              |
/// |--------|------|----------------------|
/// | 0      | 16   | key_pos (Pos)        |
/// | 16     | 34   | val (padded)         |
///
/// ## FILE + FILE (both in file)
///
/// | Offset | Size | Content              |
/// |--------|------|----------------------|
/// | 0      | 16   | key_pos (Pos)        |
/// | 16     | 16   | val_pos (Pos)        |
/// | 46     | 4    | val_crc32            |
///
/// # Layout Selection / 布局选择
///
/// ```
/// use jdb_val::{Head, Flag};
///
/// fn load(head: &Head) -> (&[u8], &[u8]) {
///   let key = if head.key_flag.is_inline() {
///     head.key_data()
///   } else {
///     let loc = head.key_pos();
///     // read key from file by pos / 从文件读取key
///     &[]
///   };
///   let val = if head.val_flag.is_inline() {
///     head.val_data()
///   } else {
///     let pos = head.val_pos();
///     let crc = head.val_crc32();
///     // read val from file by pos, verify crc / 从文件读取val并校验crc
///     &[]
///   };
///   (key, val)
/// }
/// ```
#[repr(C, align(8))]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct Head {
  pub key_len: U32,
  pub val_len: U32,
  pub key_flag: Flag,
  pub val_flag: Flag,
  data: [u8; 50],
  pub head_crc32: U32,
}

const _: () = assert!(size_of::<Head>() == 64);
const _: () = assert!(align_of::<Head>() == 8);

/// Storage mode thresholds / 存储模式阈值
///
/// | Mode   | Condition                    | Description                |
/// |--------|------------------------------|----------------------------|
/// | INLINE | key+val <= 50B               | Embedded in Head           |
/// | INFILE | data <= 1MB                  | Same WAL file              |
/// | FILE   | data > 1MB                   | Separate file              |
pub const INFILE_MAX: usize = 1024 * 1024; // 1MB

impl Head {
  pub const SIZE: usize = 64;
  const DATA_CAP: usize = 50;
  /// CRC range (bytes 0..60) / CRC 校验范围
  pub const CRC_RANGE: usize = 60;

  const K_LOC_END: usize = 16;
  const V_LOC_START: usize = 16;
  const V_LOC_ALT_START: usize = 30;
  const V_CRC_START: usize = 46;

  /// Max inline size when both inline / 两者都内联时最大内联大小
  pub const MAX_BOTH_INLINE: usize = 50;
  /// Max inline key size when val in file / val在文件时最大内联key大小
  pub const MAX_KEY_INLINE: usize = 30;
  /// Max inline val size when key in file / key在文件时最大内联val大小
  pub const MAX_VAL_INLINE: usize = 34;

  /// Create head with both inline / 创建两者都内联的头
  pub fn both_inline(key: &[u8], val: &[u8]) -> Result<Self> {
    let k_len = key.len();
    let v_len = val.len();
    if k_len + v_len > Self::MAX_BOTH_INLINE {
      return Err(Error::KeyTooLong(k_len + v_len, Self::MAX_BOTH_INLINE));
    }
    let mut head = Self::new_empty(Flag::INLINE, Flag::INLINE, k_len as u32, v_len as u32);
    head.data[..k_len].copy_from_slice(key);
    head.data[k_len..k_len + v_len].copy_from_slice(val);
    head.update_crc();
    Ok(head)
  }

  /// Create head with inline key and file val / 创建内联键和文件值的头
  pub fn key_inline(
    key: &[u8],
    val_flag: Flag,
    val_pos: Pos,
    val_len: u32,
    val_crc32: u32,
  ) -> Result<Self> {
    let k_len = key.len();
    if k_len > Self::MAX_KEY_INLINE {
      return Err(Error::KeyTooLong(k_len, Self::MAX_KEY_INLINE));
    }
    if val_flag.is_inline() {
      return Err(Error::InvalidFlag(Flag::INLINE.into(), val_flag.into()));
    }
    let mut head = Self::new_empty(Flag::INLINE, val_flag, k_len as u32, val_len);
    head.data[..k_len].copy_from_slice(key);
    head.write_pos(Self::V_LOC_ALT_START, &val_pos);
    head.write_u32(Self::V_CRC_START, val_crc32);
    head.update_crc();
    Ok(head)
  }

  /// Create head with file key and inline val / 创建文件键和内联值的头
  pub fn val_inline(key_flag: Flag, key_pos: Pos, key_len: u32, val: &[u8]) -> Result<Self> {
    let v_len = val.len();
    if v_len > Self::MAX_VAL_INLINE {
      return Err(Error::ValTooLong(v_len, Self::MAX_VAL_INLINE));
    }
    if key_flag.is_inline() {
      return Err(Error::InvalidFlag(key_flag.into(), Flag::INLINE.into()));
    }
    let mut head = Self::new_empty(key_flag, Flag::INLINE, key_len, v_len as u32);
    head.write_pos(0, &key_pos);
    head.data[Self::K_LOC_END..Self::K_LOC_END + v_len].copy_from_slice(val);
    head.update_crc();
    Ok(head)
  }

  /// Create head with both file / 创建两者都是文件的头
  pub fn both_file(
    key_flag: Flag,
    key_pos: Pos,
    key_len: u32,
    val_flag: Flag,
    val_pos: Pos,
    val_len: u32,
    val_crc32: u32,
  ) -> Result<Self> {
    if key_flag.is_inline() || val_flag.is_inline() {
      return Err(Error::InvalidFlag(key_flag.into(), val_flag.into()));
    }
    let mut head = Self::new_empty(key_flag, val_flag, key_len, val_len);
    head.write_pos(0, &key_pos);
    head.write_pos(Self::V_LOC_START, &val_pos);
    head.write_u32(Self::V_CRC_START, val_crc32);
    head.update_crc();
    Ok(head)
  }

  /// Get inline key / 获取内联键
  #[inline(always)]
  pub fn key_data(&self) -> &[u8] {
    debug_assert!(self.key_flag.is_inline());
    let len = self.key_len.get() as usize;
    // SAFETY: key_len checked on creation, max 50 / 创建时已检查
    unsafe { self.data.get_unchecked(..len) }
  }

  /// Get inline val / 获取内联值
  #[inline(always)]
  pub fn val_data(&self) -> &[u8] {
    debug_assert!(self.val_flag.is_inline());
    let len = self.val_len.get() as usize;
    // SAFETY: val range checked on creation / 创建时已检查范围
    unsafe {
      if self.key_flag.is_inline() {
        let start = self.key_len.get() as usize;
        self.data.get_unchecked(start..start + len)
      } else {
        self
          .data
          .get_unchecked(Self::K_LOC_END..Self::K_LOC_END + len)
      }
    }
  }

  /// Get key position / 获取键位置
  #[inline(always)]
  pub fn key_pos(&self) -> Pos {
    debug_assert!(!self.key_flag.is_inline());
    // SAFETY: offset 0..16 < DATA_CAP=50
    // Zero-copy: Pos is LE in memory, read directly
    unsafe {
      let p = self.data.as_ptr() as *const Pos;
      p.read_unaligned()
    }
  }

  /// Get val position / 获取值位置
  #[inline(always)]
  pub fn val_pos(&self) -> Pos {
    debug_assert!(!self.val_flag.is_inline());
    // SAFETY: all offsets < DATA_CAP=50
    unsafe {
      let base = if self.key_flag.is_inline() {
        Self::V_LOC_ALT_START
      } else {
        Self::V_LOC_START
      };
      let p = self.data.as_ptr().add(base) as *const Pos;
      p.read_unaligned()
    }
  }

  /// Get val CRC32 (only when val in file) / 获取val CRC32
  #[inline(always)]
  pub fn val_crc32(&self) -> u32 {
    debug_assert!(!self.val_flag.is_inline());
    // SAFETY: V_CRC_START=46 < DATA_CAP=50 / 常量索引在范围内
    unsafe {
      let ptr = self.data.as_ptr().add(Self::V_CRC_START) as *const u32;
      ptr.read_unaligned().to_le()
    }
  }

  #[inline]
  fn new_empty(key_flag: Flag, val_flag: Flag, key_len: u32, val_len: u32) -> Self {
    Self {
      key_len: U32::new(key_len),
      val_len: U32::new(val_len),
      key_flag,
      val_flag,
      data: [0u8; Self::DATA_CAP],
      head_crc32: U32::new(0),
    }
  }

  /// Write Pos at offset / 在偏移处写入Pos
  #[inline(always)]
  fn write_pos(&mut self, off: usize, pos: &Pos) {
    self.data[off..off + Pos::SIZE].copy_from_slice(pos.as_bytes());
  }

  /// Write u32 at offset / 在偏移处写入u32
  #[inline(always)]
  fn write_u32(&mut self, off: usize, v: u32) {
    self.data[off..off + 4].copy_from_slice(&v.to_le_bytes());
  }

  #[inline]
  fn update_crc(&mut self) {
    let crc = crc32fast::hash(&self.as_bytes()[..Self::CRC_RANGE]);
    self.head_crc32 = U32::new(crc);
  }
}
