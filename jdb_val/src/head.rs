use std::{fmt, mem};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{Compress, Error, Key, KeyRef, Kind, Result, Val, ValRef};

#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct Head {
  // === Cache Line 0: Meta (64B) ===
  /// Checksum of the header / 头部校验和
  pub header_crc: u32, // 0
  /// Time to live (Expiration = ts + ttl) / 过期时间 = ts + ttl
  pub ttl: u32, // 4
  /// Timestamp / 时间戳
  pub ts: u64, // 8
  /// Sequence identifier for versioning / 序列号，用于版本控制
  pub seq_id: u64, // 16
  /// Hash of the key for fast lookup / 键的哈希值，用于快速查找
  pub key_hash: u64, // 24
  /// Offset of the previous version / 前一版本的偏移量
  pub prev_offset: u64, // 32
  /// ID of the file containing the previous version / 包含前一版本的文件ID
  pub prev_file: u32, // 40
  /// Length of the key / 键的长度
  pub key_len: u16, // 44
  /// Kind and compression type information / 记录和压缩类型信息 (Kind 4bit | Compress 4bit)
  pub info_bits: u8, // 46
  /// Extra metadata (e.g., inline value length) / 额外元数据（如内联值长度）
  pub extra_meta: u8, // 47

  /// Mixed field for inline value (<= 16B) or [length(8B) + crc(4B) + padding(4B)]
  pub val: Val, // 48

  // === Cache Line 1: Data (64B) ===
  /// Buffer for inlined key / 用于内联存储键的缓冲区 (max 64B)
  pub key: Key, // 64
}

// Ensure 128B fixed size
const _: () = assert!(mem::size_of::<Head>() == 128);

impl Default for Head {
  fn default() -> Self {
    unsafe { mem::zeroed() }
  }
}

impl Head {
  /// Load from binary data
  pub fn load(bin: impl AsRef<[u8]>) -> Result<Self> {
    let bin = bin.as_ref();
    if bin.len() != 128 {
      return Err(Error::InvalidValType);
    }
    Self::read_from_bytes(bin).map_err(|_| Error::InvalidValType)
  }

  /// Create new record
  pub fn new(
    seq_id: u64,
    key_hash: u64,
    key_len: u16,
    key: Key,
    val_bytes: impl AsRef<[u8]>,
    ttl: u32,
    prev_offset: u64,
    prev_file: u32,
  ) -> Self {
    let mut head = Self {
      seq_id,
      key_hash,
      key_len,
      key,
      ttl,
      prev_offset,
      prev_file,
      ts: coarsetime::Clock::now_since_epoch().as_secs(),
      ..Self::default()
    };
    head.set_val(val_bytes.as_ref());
    head.update_crc();
    head
  }

  /// Create removal record
  pub fn new_rm(
    seq_id: u64,
    key_hash: u64,
    key_len: u16,
    key: Key,
    ttl: u32,
    prev_offset: u64,
    prev_file: u32,
  ) -> Result<Self> {
    let mut head = Self {
      seq_id,
      key_hash,
      key_len,
      key,
      ttl,
      prev_offset,
      prev_file,
      ts: coarsetime::Clock::now_since_epoch().as_secs(),
      ..Self::default()
    };
    head.set_info(Kind::Rm, Compress::None);
    head.update_crc();
    Ok(head)
  }

  #[inline]
  #[must_use]
  pub fn kind(&self) -> Kind {
    Kind::from(self.info_bits)
  }

  #[inline]
  #[must_use]
  pub fn compress(&self) -> Compress {
    Compress::from(self.info_bits)
  }

  #[inline]
  #[must_use]
  fn is_val_inline(&self) -> bool {
    self.kind() == Kind::Inline
  }

  #[inline]
  #[must_use]
  pub fn is_rm(&self) -> bool {
    self.kind() == Kind::Rm
  }

  /// Get value reference
  #[must_use]
  pub fn val_ref(&self) -> ValRef<'_> {
    if self.is_val_inline() {
      ValRef::Inline(self.val.inline(self.extra_meta as usize))
    } else {
      let (len, crc) = self.val.external();
      ValRef::External { len, crc }
    }
  }

  /// Get key reference
  #[must_use]
  pub fn key_ref(&self) -> KeyRef<'_> {
    let len = self.key_len as usize;
    if len <= 64 {
      KeyRef::Inline(self.key.inline(len))
    } else {
      let (prefix, file_id, offset, crc) = self.key.external();
      KeyRef::External {
        prefix,
        len: self.key_len,
        file_id,
        offset,
        crc,
      }
    }
  }

  #[inline]
  fn set_info(&mut self, rec: Kind, comp: Compress) {
    self.info_bits = (rec as u8) | (comp as u8);
  }

  fn set_val(&mut self, data: &[u8]) {
    let len = data.len();
    if len <= 16 {
      self.set_info(Kind::Inline, Compress::None);
      self.extra_meta = len as u8;
      self.val.new_inline(data);
    } else {
      let crc = crc32fast::hash(data);
      self.set_info(Kind::Val, Compress::None);
      self.extra_meta = 0;
      self.val.new_ext(len as u64, crc);
    }
  }

  fn update_crc(&mut self) {
    self.header_crc = 0;
    self.header_crc = crc32fast::hash(self.as_bytes());
  }
}

impl fmt::Debug for Head {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut s = f.debug_struct("Head");
    s.field("seq_id", &self.seq_id)
      .field("key_hash", &self.key_hash)
      .field("kind", &self.kind())
      .field("compress", &self.compress());

    match self.val_ref() {
      ValRef::Inline(data) => {
        s.field("storage", &"INLINE")
          .field("len", &data.len())
          .field("data", &data);
      }
      ValRef::External { len, crc } => {
        s.field("storage", &"NORMAL")
          .field("len", &len)
          .field("crc", &format!("{:#x}", crc));
      }
    }

    match self.key_ref() {
      KeyRef::Inline(data) => {
        s.field("key_storage", &"INLINE").field("key", &data);
      }
      KeyRef::External {
        prefix,
        len,
        file_id,
        offset,
        crc,
      } => {
        s.field("key_storage", &"EXTERNAL")
          .field("key_len", &len)
          .field("key_prefix", &prefix)
          .field("key_file", &file_id)
          .field("key_offset", &offset)
          .field("key_crc", &format!("{:#x}", crc));
      }
    }
    s.finish()
  }
}
