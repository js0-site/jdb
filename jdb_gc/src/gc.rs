//! GC trait and implementations
//! GC trait 和实现

use jdb_base::Flag;
use lz4_flex::compress_prepend_size;

use crate::MIN_COMPRESS_SIZE;

/// GC trait for data processing during GC
/// GC 数据处理 trait
pub trait Gc: Default {
  /// Process data, may compress
  /// 处理数据，可能压缩
  ///
  /// Returns (new_flag, compressed_len)
  /// - If compressed_len is Some, data was compressed into buf
  /// - If compressed_len is None, use original data
  fn process(&mut self, flag: Flag, data: &[u8], buf: &mut Vec<u8>) -> (Flag, Option<usize>);
}

/// LZ4 GC (auto compress during GC)
/// LZ4 GC（GC 时自动压缩）
#[derive(Default)]
pub struct Lz4Gc;

impl Gc for Lz4Gc {
  fn process(&mut self, flag: Flag, data: &[u8], buf: &mut Vec<u8>) -> (Flag, Option<usize>) {
    // Skip if already compressed or probed
    // 跳过已压缩或已探测的数据
    if flag.is_compressed() || flag.is_probed() || flag.is_tombstone() {
      return (flag, None);
    }

    // Skip small data
    // 跳过小数据
    if data.len() < MIN_COMPRESS_SIZE {
      return (flag.to_probed(), None);
    }

    // Try compress
    // 尝试压缩
    buf.clear();
    let compressed = compress_prepend_size(data);
    let compressed_len = compressed.len();

    // Only use if compression is effective (at least 10% smaller)
    // 仅当压缩有效时使用（至少小 10%）
    if compressed_len < data.len() * 9 / 10 {
      buf.extend_from_slice(&compressed);
      return (flag.to_lz4(), Some(compressed_len));
    }

    // Mark as probed (incompressible)
    // 标记为已探测（不可压缩）
    (flag.to_probed(), None)
  }
}

/// No-op GC (no compression)
/// 无操作 GC（不压缩）
#[derive(Default)]
pub struct NoGc;

impl Gc for NoGc {
  fn process(&mut self, flag: Flag, _data: &[u8], _buf: &mut Vec<u8>) -> (Flag, Option<usize>) {
    (flag, None)
  }
}
