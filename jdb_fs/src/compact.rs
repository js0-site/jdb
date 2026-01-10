//! Compact trait for log-structured data
//! 日志结构数据的压缩 trait

use crate::{
  consts::COMPACT_INTERVAL,
  item::{Decode, Encode, Item, ParseResult},
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

/// Compactable trait for log-structured data
/// 可压缩 trait，用于日志结构数据
pub trait Compact: Sized + Item + Encode + Decode {
  /// Compact operation interval (operations per compaction)
  /// 压缩操作间隔（每次压缩的操作次数）
  const INTERVAL: usize = COMPACT_INTERVAL;

  /// Parse single item and load into self
  /// 解析单条并加载到 self
  fn parse(&mut self, buf: &[u8]) -> ParseResult<Decoded> {
    match <Self as Decode>::decode(buf) {
      ParseResult::Ok(item, len) => {
        let incr = self.on_item(item);
        ParseResult::Ok(Decoded { len, incr }, len)
      }
      ParseResult::NeedMore => ParseResult::NeedMore,
      ParseResult::Corrupted(skip) => ParseResult::Corrupted(skip),
    }
  }

  /// Handle decoded item, return true if should increment compact count
  /// 处理解码的条目，如果需要增加压缩计数则返回 true
  fn on_item(&mut self, item: Self::Data<'_>) -> IncrCount;

  /// Iterate bytes for full rewrite
  /// 迭代字节用于完全重写
  fn rewrite(&self) -> impl Iterator<Item = Self::Data<'_>>;
}
