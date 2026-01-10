//! Compact trait for log-structured data
//! 日志结构数据的压缩 trait

use crate::{
  Len, Load,
  consts::COMPACT_INTERVAL,
  item::{Decode, Encode, Item, ParseResult},
};

/// Increment count flag
/// 增加计数标志
pub type IncrCount = bool;

/// Load result for compact
/// 压缩加载结果
pub struct Loaded {
  pub pos: u64,
  pub count: usize,
}

/// Compactable trait for log-structured data
/// 可压缩 trait，用于日志结构数据
pub trait Compact: Sized + Item + Encode + Decode {
  /// Compact operation interval (operations per compaction)
  /// 压缩操作间隔（每次压缩的操作次数）
  const INTERVAL: usize = COMPACT_INTERVAL;

  /// Handle decoded item, return true if should increment compact count
  /// 处理解码的条目，如果需要增加压缩计数则返回 true
  fn on_item(&mut self, item: <Self as Item>::Data<'_>) -> IncrCount;

  /// Iterate bytes for full rewrite
  /// 迭代字节用于完全重写
  fn rewrite(&self) -> impl Iterator<Item = <Self as Item>::Data<'_>>;
}

/// Wrapper for Compact to implement Load
/// Compact 的包装器以实现 Load
pub struct CompactLoad<T> {
  pub inner: T,
  pub pos: u64,
  pub count: usize,
}

impl<T> CompactLoad<T> {
  pub fn new(inner: T) -> Self {
    Self {
      inner,
      pos: 0,
      count: 0,
    }
  }
}

impl<T: Compact> Item for CompactLoad<T> {
  const MAGIC: u8 = T::MAGIC;
  const LEN_BYTES: usize = T::LEN_BYTES;
  type Data<'a>
    = <T as Item>::Data<'a>
  where
    T: 'a;

  fn len(byte: &[u8]) -> usize {
    T::len(byte)
  }
}

impl<T: Compact> Decode for CompactLoad<T> {
  fn decode_item(bin: &[u8]) -> Option<Self::Data<'_>> {
    T::decode_item(bin)
  }
}

impl<T: Compact> Load for CompactLoad<T> {
  type Loaded = Loaded;

  fn on_parse(&mut self, result: ParseResult<Self::Data<'_>>) -> Len {
    match result {
      ParseResult::Ok(item, len) => {
        if self.inner.on_item(item) {
          self.count += 1;
        }
        self.pos += len as u64;
        len as Len
      }
      ParseResult::NeedMore => 0,
      ParseResult::Err(e, skip) => {
        log::warn!("CompactLoad parse error: {e}, skipping {skip}");
        self.pos += skip as u64;
        skip as Len
      }
    }
  }

  fn end(&self) -> Self::Loaded {
    Loaded {
      pos: self.pos,
      count: self.count,
    }
  }
}
