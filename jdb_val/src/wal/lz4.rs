//! LZ4 compression module / LZ4 压缩模块
//!
//! Heuristic compression strategy / 启发式压缩策略:
//! - < 1KB: skip / 跳过
//! - 1KB-16KB: compress directly / 直接压缩
//! - > 16KB: probe first / 先探测

use crate::error::{Error, Result};

/// Min size to attempt compression / 尝试压缩的最小大小 (1KB)
pub const MIN_COMPRESS_SIZE: usize = 1024;

/// Threshold for probe-first strategy / 探测优先策略的阈值 (16KB)
pub const PROBE_THRESHOLD: usize = 16 * 1024;

/// Probe size for large data / 大数据探测大小 (8KB)
pub const PROBE_SIZE: usize = 8 * 1024;

/// Savings ratio denominator (1/8 = 12.5%) / 节省比例分母
pub const SAVINGS_RATIO: usize = 8;

/// Try compress data, return compressed length if beneficial
/// 尝试压缩数据，如果有益则返回压缩后长度
///
/// Returns None if:
/// - data < 1KB (skip)
/// - compression doesn't reduce size
/// - probe fails for large data
#[inline]
pub fn try_compress(data: &[u8], buf: &mut Vec<u8>) -> Option<usize> {
  let len = data.len();

  // Skip small data / 跳过小数据
  if len < MIN_COMPRESS_SIZE {
    return None;
  }

  // Large data: probe first / 大数据先探测
  if len > PROBE_THRESHOLD {
    // Quick probe: compress 8KB prefix to estimate ratio / 快速探测：压缩 8KB 前缀估算比率
    let probe_bound = lz4_flex::block::get_maximum_output_size(PROBE_SIZE);
    if buf.capacity() < probe_bound {
      buf.reserve(probe_bound - buf.len());
    }
    // SAFETY: compress_into writes to uninitialized memory / 安全：compress_into 写入未初始化内存
    unsafe { buf.set_len(probe_bound) };
    let probe_compressed = lz4_flex::compress_into(&data[..PROBE_SIZE], buf).ok()?;

    // Check savings threshold (12.5%) / 检查节省阈值
    let threshold = PROBE_SIZE - PROBE_SIZE / SAVINGS_RATIO;
    if probe_compressed > threshold {
      return None;
    }
  }

  // Compress full data / 压缩完整数据
  let max_compressed = lz4_flex::block::get_maximum_output_size(len);
  if buf.capacity() < max_compressed {
    buf.reserve(max_compressed - buf.len());
  }
  // SAFETY: compress_into writes to uninitialized memory / 安全：compress_into 写入未初始化内存
  unsafe { buf.set_len(max_compressed) };

  let compressed_len = lz4_flex::compress_into(data, buf).ok()?;

  // Only use if smaller / 仅在更小时使用
  if compressed_len < len {
    unsafe { buf.set_len(compressed_len) };
    Some(compressed_len)
  } else {
    None
  }
}

/// Decompress LZ4 data / 解压缩 LZ4 数据
#[inline]
pub fn decompress(compressed: &[u8], original_len: usize, buf: &mut Vec<u8>) -> Result<()> {
  if buf.capacity() < original_len {
    buf.reserve(original_len - buf.len());
  }
  // SAFETY: decompress_into writes to uninitialized memory / 安全：decompress_into 写入未初始化内存
  unsafe { buf.set_len(original_len) };

  lz4_flex::decompress_into(compressed, buf).map_err(|_| Error::DecompressFailed)?;

  Ok(())
}
