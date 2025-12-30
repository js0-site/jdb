//! Variable-length head
//! 变长头
//!
//! ## Structure
//! 结构
//!
//! | Field      | Size     | Description                           |
//! |------------|----------|---------------------------------------|
//! | magic      | 1B       | 0xFF                                  |
//! | id         | 8B       | Incremental ID from ider              |
//! | flag       | 1B       | key(3bit) + val(3bit)                 |
//! | hkv_len    | 1B       | VByte encoded length (0-25)           |
//! | lens       | var      | VByte: [head_len, key_len, val_len]   |
//! | head_data  | head_len | key/val data area                     |
//! | head_crc32 | 4B       | CRC32 (not in head_len)               |
//!
//! 中文说明：
//! - magic: 魔数，用于快速定位记录
//! - id: 递增 ID，用于识别写入顺序
//! - flag: 存储标志，包含 key 和 val 的存储模式
//! - hkv_len: VByte 编码的长度字段占用的字节数
//! - lens: VByte 编码的 [head_len, key_len, val_len]
//! - head_data: key/val 数据区
//! - head_crc32: CRC32 校验（不计入 head_len）
//!
//! ## Delete record
//! 删除记录
//!
//! lens only has 2 numbers: [head_len, key_len], no val_len means tombstone
//! lens 只有2个数字：[head_len, key_len]，没有 val_len 表示删除标记
//!
//! ## Data layout
//! 数据布局
//!
//! ### INFILE mode
//! INFILE 模式
//!
//! Data stored directly in head_data
//! 数据直接存储在 head_data 中
//!
//! ### FILE mode (32B per entry)
//! FILE 模式（每条目 32 字节）
//!
//! | file_id | offset | hash    |
//! | 8B      | 8B     | 16B     |
//!
//! - file_id: 文件 ID
//! - offset: 文件内偏移
//! - hash: gxhash128 哈希值，用于校验

use gxhash::gxhash128;
use vb::{d_li, e_li};

use crate::{
  Flag, Store,
  error::{Error, Result},
};

/// Magic byte
/// 魔数字节
pub const MAGIC: u8 = 0xFF;

/// Fixed header size: magic(1) + id(8) + flag(1) + hkv_len(1) = 11
/// 固定头大小
pub const FIXED_SIZE: usize = 11;

/// CRC32 size
/// CRC32 大小
pub const CRC_SIZE: usize = 4;

/// FILE mode entry size: file_id(8) + offset(8) + hash(16) = 32
/// FILE 模式条目大小
pub const FILE_ENTRY_SIZE: usize = 32;

/// Max VByte encoded length for 3 u64 (first <= u32)
/// 最大 VByte 编码长度
pub const MAX_HKV_LEN: usize = 25;

/// Max infile data size (4MB)
/// 最大同文件数据大小
pub const INFILE_MAX: usize = 4 * 1024 * 1024;

/// FILE mode position
/// FILE 模式位置
#[derive(Debug, Clone, Copy, Default)]
pub struct FilePos {
  pub file_id: u64,
  pub offset: u64,
  pub hash: u128,
}

impl FilePos {
  /// Create with hash from data
  /// 从数据创建并计算哈希
  #[inline]
  pub fn with_hash(file_id: u64, offset: u64, data: &[u8]) -> Self {
    Self {
      file_id,
      offset,
      hash: gxhash128(data, 0),
    }
  }

  /// Read from bytes
  /// 从字节读取
  #[inline(always)]
  pub fn from_bytes(b: &[u8]) -> Self {
    debug_assert!(b.len() >= FILE_ENTRY_SIZE);
    // SAFETY: length checked by caller
    // 安全：长度由调用者检查
    unsafe {
      Self {
        file_id: u64::from_le_bytes(*(b.as_ptr() as *const [u8; 8])),
        offset: u64::from_le_bytes(*(b.as_ptr().add(8) as *const [u8; 8])),
        hash: u128::from_le_bytes(*(b.as_ptr().add(16) as *const [u8; 16])),
      }
    }
  }

  /// Write to bytes
  /// 写入字节
  #[inline(always)]
  pub fn write(&self, buf: &mut [u8]) {
    debug_assert!(buf.len() >= FILE_ENTRY_SIZE);
    buf[0..8].copy_from_slice(&self.file_id.to_le_bytes());
    buf[8..16].copy_from_slice(&self.offset.to_le_bytes());
    buf[16..32].copy_from_slice(&self.hash.to_le_bytes());
  }

  /// Verify hash
  /// 验证哈希
  #[inline]
  pub fn verify(&self, data: &[u8]) -> bool {
    gxhash128(data, 0) == self.hash
  }
}

/// Head builder
/// 头构建器
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

  /// Build head for infile key + infile val
  /// 构建同文件 key + 同文件 val 的头
  pub fn infile_infile(
    &mut self,
    id: u64,
    key_store: Store,
    key: &[u8],
    val_store: Store,
    val: &[u8],
  ) -> &[u8] {
    let head_len = key.len() + val.len();
    self.build(
      id,
      key_store,
      val_store,
      head_len,
      key.len(),
      Some(val.len()),
    );
    self.buf.extend_from_slice(key);
    self.buf.extend_from_slice(val);
    self.finish()
  }

  /// Build head for infile key + file val
  /// 构建同文件 key + 文件 val 的头
  pub fn infile_file(
    &mut self,
    id: u64,
    key_store: Store,
    key: &[u8],
    val_store: Store,
    val_pos: &FilePos,
    val_len: u64,
  ) -> &[u8] {
    let head_len = key.len() + FILE_ENTRY_SIZE;
    self.build(
      id,
      key_store,
      val_store,
      head_len,
      key.len(),
      Some(val_len as usize),
    );
    self.buf.extend_from_slice(key);
    let start = self.buf.len();
    self.buf.resize(start + FILE_ENTRY_SIZE, 0);
    val_pos.write(&mut self.buf[start..]);
    self.finish()
  }

  /// Build head for file key + infile val
  /// 构建文件 key + 同文件 val 的头
  pub fn file_infile(
    &mut self,
    id: u64,
    key_store: Store,
    key_pos: &FilePos,
    key_len: u64,
    val_store: Store,
    val: &[u8],
  ) -> &[u8] {
    let head_len = FILE_ENTRY_SIZE + val.len();
    self.build(
      id,
      key_store,
      val_store,
      head_len,
      key_len as usize,
      Some(val.len()),
    );
    let start = self.buf.len();
    self.buf.resize(start + FILE_ENTRY_SIZE, 0);
    key_pos.write(&mut self.buf[start..]);
    self.buf.extend_from_slice(val);
    self.finish()
  }

  /// Build head for file key + file val
  /// 构建文件 key + 文件 val 的头
  #[allow(clippy::too_many_arguments)]
  pub fn file_file(
    &mut self,
    id: u64,
    key_store: Store,
    key_pos: &FilePos,
    key_len: u64,
    val_store: Store,
    val_pos: &FilePos,
    val_len: u64,
  ) -> &[u8] {
    let head_len = FILE_ENTRY_SIZE * 2;
    self.build(
      id,
      key_store,
      val_store,
      head_len,
      key_len as usize,
      Some(val_len as usize),
    );
    let start = self.buf.len();
    self.buf.resize(start + FILE_ENTRY_SIZE * 2, 0);
    key_pos.write(&mut self.buf[start..]);
    val_pos.write(&mut self.buf[start + FILE_ENTRY_SIZE..]);
    self.finish()
  }

  /// Build tombstone for infile key
  /// 构建同文件 key 的删除标记
  pub fn tombstone_infile(&mut self, id: u64, key_store: Store, key: &[u8]) -> &[u8] {
    let head_len = key.len();
    self.build(id, key_store, Store::Infile, head_len, key.len(), None);
    self.buf.extend_from_slice(key);
    self.finish()
  }

  /// Build tombstone for file key
  /// 构建文件 key 的删除标记
  pub fn tombstone_file(
    &mut self,
    id: u64,
    key_store: Store,
    key_pos: &FilePos,
    key_len: u64,
  ) -> &[u8] {
    let head_len = FILE_ENTRY_SIZE;
    self.build(
      id,
      key_store,
      Store::Infile,
      head_len,
      key_len as usize,
      None,
    );
    let start = self.buf.len();
    self.buf.resize(start + FILE_ENTRY_SIZE, 0);
    key_pos.write(&mut self.buf[start..]);
    self.finish()
  }

  #[inline(always)]
  fn build(
    &mut self,
    id: u64,
    key_store: Store,
    val_store: Store,
    head_len: usize,
    key_len: usize,
    val_len: Option<usize>,
  ) {
    self.buf.clear();
    // magic
    self.buf.push(MAGIC);
    // id
    self.buf.extend_from_slice(&id.to_le_bytes());
    // flag
    self.buf.push(Flag::new(key_store, val_store).as_u8());
    // hkv_len placeholder
    let hkv_pos = self.buf.len();
    self.buf.push(0);
    // lens (VByte encoded)
    let lens_start = self.buf.len();
    let lens = if let Some(vl) = val_len {
      e_li([head_len as u64, key_len as u64, vl as u64])
    } else {
      e_li([head_len as u64, key_len as u64])
    };
    self.buf.extend_from_slice(&lens);
    // update hkv_len
    self.buf[hkv_pos] = (self.buf.len() - lens_start) as u8;
  }

  #[inline(always)]
  fn finish(&mut self) -> &[u8] {
    // CRC32 from id (skip magic)
    // CRC32 从 id 开始计算（跳过 magic）
    let crc = crc32fast::hash(&self.buf[1..]);
    self.buf.extend_from_slice(&crc.to_le_bytes());
    &self.buf
  }
}

/// Parsed head
/// 解析后的头
#[derive(Debug, Clone)]
pub struct Head {
  pub id: u64,
  pub flag: Flag,
  pub head_len: u64,
  pub key_len: u64,
  pub val_len: Option<u64>,
  /// Offset to head_data from record start
  /// 从记录开始到 head_data 的偏移
  pub data_off: usize,
  /// Total record size including CRC
  /// 包含 CRC 的总记录大小
  pub size: usize,
}

impl Head {
  /// Parse head from bytes
  /// 从字节解析头
  pub fn parse(buf: &[u8]) -> Result<Self> {
    if buf.len() < FIXED_SIZE {
      return Err(Error::InvalidHead);
    }

    // Check magic
    // 检查魔数
    if buf[0] != MAGIC {
      return Err(Error::InvalidMagic);
    }

    // Parse fixed fields
    // 解析固定字段
    let id = u64::from_le_bytes(unsafe { *(buf.as_ptr().add(1) as *const [u8; 8]) });
    let flag = Flag::from_u8(buf[9]);
    let hkv_len = buf[10] as usize;

    if buf.len() < FIXED_SIZE + hkv_len {
      return Err(Error::InvalidHead);
    }

    // Decode lens
    // 解码长度
    let lens_bytes = &buf[FIXED_SIZE..FIXED_SIZE + hkv_len];
    let lens = d_li(lens_bytes).map_err(|_| Error::InvalidHead)?;

    let (head_len, key_len, val_len) = match lens.len() {
      2 => (lens[0], lens[1], None),
      3 => (lens[0], lens[1], Some(lens[2])),
      _ => return Err(Error::InvalidHead),
    };

    let data_off = FIXED_SIZE + hkv_len;
    let size = data_off + head_len as usize + CRC_SIZE;

    // Verify CRC if we have enough data (skip magic)
    // 如果有足够数据则验证 CRC（跳过 magic）
    if buf.len() >= size {
      let crc_off = size - CRC_SIZE;
      let stored = u32::from_le_bytes(unsafe { *(buf.as_ptr().add(crc_off) as *const [u8; 4]) });
      let computed = crc32fast::hash(&buf[1..crc_off]);
      if stored != computed {
        return Err(Error::CrcMismatch(computed, stored));
      }
    }

    Ok(Self {
      id,
      flag,
      head_len,
      key_len,
      val_len,
      data_off,
      size,
    })
  }

  /// Check if tombstone
  /// 检查是否为删除标记
  #[inline(always)]
  pub fn is_tombstone(&self) -> bool {
    self.val_len.is_none()
  }

  /// Get key store
  /// 获取 key 存储模式
  #[inline(always)]
  pub fn key_store(&self) -> Store {
    self.flag.key()
  }

  /// Get val store
  /// 获取 val 存储模式
  #[inline(always)]
  pub fn val_store(&self) -> Store {
    self.flag.val()
  }

  /// Get key data size in head_data
  /// 获取 key 在 head_data 中的数据大小
  #[inline(always)]
  pub fn key_data_size(&self) -> usize {
    if self.key_store().is_file() {
      FILE_ENTRY_SIZE
    } else {
      self.key_len as usize
    }
  }

  /// Get key data from head_data
  /// 从 head_data 获取 key 数据
  ///
  /// For INFILE: returns key bytes
  /// For FILE: returns FilePos bytes (32B)
  #[inline(always)]
  pub fn key_data<'a>(&self, head_data: &'a [u8]) -> &'a [u8] {
    &head_data[..self.key_data_size()]
  }

  /// Get key FilePos (FILE mode only)
  /// 获取 key FilePos（仅 FILE 模式）
  #[inline(always)]
  pub fn key_file_pos(&self, head_data: &[u8]) -> FilePos {
    FilePos::from_bytes(head_data)
  }

  /// Get val data from head_data
  /// 从 head_data 获取 val 数据
  ///
  /// For INFILE: returns val bytes
  /// For FILE: returns FilePos bytes (32B)
  #[inline(always)]
  pub fn val_data<'a>(&self, head_data: &'a [u8]) -> &'a [u8] {
    let key_size = self.key_data_size();
    if self.val_store().is_file() {
      &head_data[key_size..key_size + FILE_ENTRY_SIZE]
    } else {
      let val_len = self.val_len.unwrap_or(0) as usize;
      &head_data[key_size..key_size + val_len]
    }
  }

  /// Get val FilePos (FILE mode only)
  /// 获取 val FilePos（仅 FILE 模式）
  #[inline(always)]
  pub fn val_file_pos(&self, head_data: &[u8]) -> FilePos {
    let key_size = self.key_data_size();
    FilePos::from_bytes(&head_data[key_size..])
  }
}
