use std::{fmt, mem};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{Compress, Error, Key, KeyRef, Kind, Result, Val, ValRef};

/// Parameters for creating a new Head record
pub struct HeadArgs<'a> {
  pub ts: u64,
  pub seq_id: u64,
  pub key_len: u16,
  pub key: Key,
  pub val_bytes: &'a [u8],
  pub ttl: u32,
  pub prev_offset: u64,
  pub prev_file: u32,
}

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
  /// Offset of the previous version / 前一版本的偏移量
  pub prev_offset: u64, // 24
  /// ID of the file containing the previous version / 包含前一版本的文件ID
  pub prev_file: u32, // 32
  /// Length of the key / 键的长度
  pub key_len: u16, // 36
  /// Kind and compression type information / 记录和压缩类型信息 (Kind 4bit | Compress 4bit)
  pub info_bits: u8, // 38
  /// Extra metadata (e.g., inline value length) / 额外元数据（如内联值长度）
  pub extra_meta: u8, // 39

  /// Mixed field for inline value (<= 16B) or [length(8B) + crc(4B) + padding(4B)]
  pub val: Val, // 40

  // === Cache Line 1: Data (72B, starts from 56) ===
  /// Buffer for inlined key or external key info / 用于内联存储键或外部键信息的缓冲区 (max 72B)
  pub key: Key, // 56
}

// Ensure 128B fixed size
const _: () = assert!(mem::size_of::<Head>() == 128);

impl Head {
  /// Load from binary data
  pub fn load(bin: impl AsRef<[u8]>) -> Result<Self> {
    let bin = bin.as_ref();
    if bin.len() != 128 {
      return Err(Error::InvalidValType);
    }

    // Verify CRC before converting to struct
    let header_crc = u32::from_le_bytes(bin[0..4].try_into().unwrap());
    let mut hasher = crc32fast::Hasher::new();
    // Use first 4 bytes as 0 for CRC calculation
    hasher.update(&[0u8; 4]);
    hasher.update(&bin[4..]);
    if hasher.finalize() != header_crc {
      return Err(Error::ChecksumMismatch);
    }

    Self::read_from_bytes(bin).map_err(|_| Error::InvalidValType)
  }

  /// Create new record with parameters
  pub fn new(params: HeadArgs) -> Self {
    let HeadArgs {
      ts,
      seq_id,
      key_len,
      key,
      val_bytes,
      ttl,
      prev_offset,
      prev_file,
    } = params;

    let (info_bits, extra_meta, val) = if val_bytes.len() <= 16 {
      (
        (Kind::Inline as u8) | (Compress::None as u8),
        val_bytes.len() as u8,
        Val::new_inline(val_bytes),
      )
    } else {
      let crc = crc32fast::hash(val_bytes);
      (
        (Kind::Val as u8) | (Compress::None as u8),
        0,
        Val::new_ext(val_bytes.len() as u64, crc),
      )
    };

    let mut head = Self {
      header_crc: 0,
      ttl,
      ts,
      seq_id,
      prev_offset,
      prev_file,
      key_len,
      info_bits,
      extra_meta,
      val,
      key,
    };

    head.header_crc = crc32fast::hash(head.as_bytes());
    head
  }

  /// Create removal record
  pub fn new_rm(
    ts: u64,
    seq_id: u64,
    key_len: u16,
    key: Key,
    ttl: u32,
    prev_offset: u64,
    prev_file: u32,
  ) -> Self {
    let mut head = Self {
      header_crc: 0,
      ttl,
      ts,
      seq_id,
      prev_offset,
      prev_file,
      key_len,
      info_bits: (Kind::Rm as u8) | (Compress::None as u8),
      extra_meta: 0,
      val: Val::default(),
      key,
    };
    head.header_crc = crc32fast::hash(head.as_bytes());
    head
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


  /// Get key reference
  #[must_use]
  pub fn key_ref(&self) -> KeyRef<'_> {
    let len = self.key_len as usize;
    if len <= 72 {
      KeyRef::Inline(self.key.inline(len))
    } else {
      let (hash, prefix, file_id, offset, crc) = self.key.external();
      KeyRef::External {
        hash,
        prefix,
        len: self.key_len,
        file_id,
        offset,
        crc,
      }
    }
  }

  /// Get key hash for fast filtering
  #[must_use]
  pub fn key_hash(&self) -> u64 {
    if self.key_len <= 72 {
      // For inline keys, we don't store hash separately, 
      // return a hash of the inline data if needed, or 0.
      // Here we choose to return 0 and let caller handle it, 
      // or we could compute it. Usually caller uses it to skip disk IO.
      // Given the user's intent to save space, we don't store it.
      0
    } else {
      let (hash, ..) = self.key.external();
      hash
    }
  }
}

impl fmt::Debug for Head {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut s = f.debug_struct("Head");
    s.field("seq_id", &self.seq_id)
      .field("key_hash", &self.key_hash())
      .field("kind", &self.kind())
      .field("compress", &self.compress());

    match self.kind() {
      Kind::Inline => {
        let data = self.val.inline(self.extra_meta as usize);
        s.field("storage", &"INLINE")
          .field("len", &data.len())
          .field("data", &data);
      }
      Kind::Val | Kind::Blob | Kind::Unknown => {
        let (len, crc) = self.val.external();
        s.field("storage", &"NORMAL")
          .field("len", &len)
          .field("crc", &format!("{:#x}", crc));
      }
      Kind::Rm => {
        s.field("storage", &"RM");
      }
    }

    match self.key_ref() {
      KeyRef::Inline(data) => {
        s.field("key_storage", &"INLINE").field("key", &data);
      }
      KeyRef::External {
        hash: _,
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
