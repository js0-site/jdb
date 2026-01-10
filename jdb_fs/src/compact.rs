//! Compact trait for log-structured data
//! 日志结构数据的压缩 trait

use crate::{
  Parse,
  consts::COMPACT_INTERVAL,
  parse::ParseResult,
};

/// Increment count flag
/// 增加计数标志
pub type IncrCount = bool;

/// Decode result
/// 解码结果
#[derive(Debug, Clone, Copy)]
pub struct Decoded {
  pub len: usize,
  /// If entry exists, increment compact count
  /// 如果之前存在条目，表示需要增加压缩计数
  pub incr: bool,
}

/// Decode operation result
/// 解码操作结果
#[derive(Debug, Clone, Copy)]
pub enum DecodeResult {
  /// Successfully decoded
  /// 解码成功
  Ok(Decoded),
  /// Need more bytes
  /// 需要更多字节
  NeedMore,
  /// Corrupted data, skip bytes
  /// 损坏数据，跳过字节
  Skip(usize),
}

/// Compactable trait for log-structured data
/// 可压缩 trait，用于日志结构数据
pub trait Compact: Sized + Parse {
  /// Compact operation interval (operations per compaction)
  /// 压缩操作间隔（每次压缩的操作次数）
  const INTERVAL: usize = COMPACT_INTERVAL;

  /// Decode single item and load into self
  /// 解码单条并加载到 self
  fn decode(&mut self, buf: &[u8]) -> DecodeResult {
    match <Self as Parse>::parse(buf) {
      ParseResult::Ok(item, len) => {
        let incr = self.on_item(item);
        DecodeResult::Ok(Decoded { len, incr })
      }
      ParseResult::NeedMore => DecodeResult::NeedMore,
      ParseResult::Corrupted(skip) => DecodeResult::Skip(skip),
    }
  }

  /// Handle decoded item, return true if should increment compact count
  /// 处理解码的条目，如果需要增加压缩计数则返回 true
  fn on_item(&mut self, item: Self::Item<'_>) -> IncrCount;

  /// Iterate bytes for full rewrite
  /// 迭代字节用于完全重写
  fn rewrite(&self) -> impl Iterator<Item = Self::Item<'_>>;
}
