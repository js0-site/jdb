//! Block iteration utilities
//! 块迭代工具

use jdb_base::Pos;

use super::DataBlock;

/// Restore full key from prefix and suffix
/// 从前缀和后缀恢复完整键
#[inline]
pub(crate) fn restore_key(prefix: &[u8], suffix: &[u8]) -> Box<[u8]> {
  if prefix.is_empty() {
    return suffix.into();
  }
  // Optimized: exact capacity allocation
  // 优化：精确容量分配
  let mut buf = Vec::with_capacity(prefix.len() + suffix.len());
  buf.extend_from_slice(prefix);
  buf.extend_from_slice(suffix);
  buf.into_boxed_slice()
}

/// Read entry at offset, return (new_offset, pos)
/// 读取指定偏移的条目，返回 (新偏移, pos)
pub(crate) fn read_entry(
  data: &[u8],
  offset: usize,
  is_restart: bool,
  buf: &mut Vec<u8>,
) -> Option<(usize, Pos)> {
  let mut p = offset;

  if is_restart {
    let suffix_len = u16::from_le_bytes(data.get(p..p + 2)?.try_into().ok()?) as usize;
    p += 2;
    let suffix = data.get(p..p + suffix_len)?;
    buf.clear();
    buf.extend_from_slice(suffix);
    p += suffix_len;
  } else {
    let shared = u16::from_le_bytes(data.get(p..p + 2)?.try_into().ok()?) as usize;
    let unshared = u16::from_le_bytes(data.get(p + 2..p + 4)?.try_into().ok()?) as usize;
    p += 4;

    // Optimized: truncate is safe and fast (will panic if shared > len in debug, logical error)
    // 优化：truncate 安全且快速
    if shared > buf.len() {
      return None;
    }
    buf.truncate(shared);
    buf.extend_from_slice(data.get(p..p + unshared)?);
    p += unshared;
  }

  let pos: Pos = zerocopy::FromBytes::read_from_bytes(data.get(p..p + Pos::SIZE)?).ok()?;
  Some((p + Pos::SIZE, pos))
}

/// Get last key in block (optimized: only reads last restart interval)
/// 获取块中最后一个键（优化：只读最后一个重启区间）
pub(crate) fn last_key(block: &DataBlock) -> Option<Box<[u8]>> {
  if block.item_count == 0 || block.restart_count == 0 {
    return None;
  }

  let last_restart = block.restart_count - 1;
  let start = block.restart_offset(last_restart) as usize;
  let end = block.data_end as usize;
  let data = &block.data[..end];

  let mut buf = Vec::with_capacity(256);
  let mut offset = start;
  let mut is_first = true;

  // Only keep last suffix in buf, build key at end
  // 只在 buf 中保留最后的后缀，最后再构建完整键
  while offset < end {
    match read_entry(data, offset, is_first, &mut buf) {
      Some((new_offset, _)) => {
        offset = new_offset;
        is_first = false;
      }
      None => break,
    }
  }

  if buf.is_empty() && is_first {
    return None;
  }

  // Build final key only once
  // 只构建一次最终键
  Some(restore_key(&block.prefix, &buf))
}
