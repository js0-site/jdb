//! Block iteration utilities
//! 块迭代工具

use jdb_base::Pos;

use super::Block;

/// Restore full key from prefix and suffix
/// 从前缀和后缀恢复完整键
#[inline]
pub(crate) fn restore_key(prefix: &[u8], suffix: &[u8]) -> Box<[u8]> {
  if prefix.is_empty() {
    return suffix.into();
  }

  // Optimized: exact capacity allocation
  // 优化：精确容量分配
  let p_len = prefix.len();
  let s_len = suffix.len();
  let mut buf = Vec::with_capacity(p_len + s_len);

  // Safe: capacity is reserved
  unsafe {
    std::ptr::copy_nonoverlapping(prefix.as_ptr(), buf.as_mut_ptr(), p_len);
    std::ptr::copy_nonoverlapping(suffix.as_ptr(), buf.as_mut_ptr().add(p_len), s_len);
    buf.set_len(p_len + s_len);
  }
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
    // Restart entry: [suffix_len: u16][suffix]
    let suffix_len = u16::from_le_bytes(data.get(p..p + 2)?.try_into().ok()?) as usize;
    p += 2;
    let suffix = data.get(p..p + suffix_len)?;
    buf.clear();
    buf.extend_from_slice(suffix);
    p += suffix_len;
  } else {
    // Shared entry: [shared: u16][unshared: u16][suffix]
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

  let pos: Pos =
    zerocopy::FromBytes::read_from_bytes(data.get(p..p + std::mem::size_of::<Pos>())?).ok()?;
  Some((p + std::mem::size_of::<Pos>(), pos))
}

/// Iterator over block entries
/// 块条目迭代器
pub struct BlockIter<'a> {
  block: &'a Block,
  offset: usize,
  next_restart_idx: u32,
  key_buf: Vec<u8>,
}

impl<'a> BlockIter<'a> {
  pub(crate) fn new(block: &'a Block) -> Self {
    Self {
      block,
      offset: block.entries_start as usize,
      next_restart_idx: 0,
      key_buf: Vec::with_capacity(256),
    }
  }
}

impl<'a> Iterator for BlockIter<'a> {
  type Item = (Box<[u8]>, Pos);

  fn next(&mut self) -> Option<Self::Item> {
    if self.offset >= self.block.data_end as usize {
      return None;
    }

    // Check if current offset matches the next restart point
    // 检查当前偏移是否匹配下一个重启点
    let mut is_restart = false;
    if self.next_restart_idx < self.block.restart_count {
      let restart_off = self.block.restart_offset(self.next_restart_idx);
      if self.offset == restart_off as usize {
        is_restart = true;
        self.next_restart_idx += 1;
      }
    }

    let (new_off, pos) = read_entry(&self.block.data, self.offset, is_restart, &mut self.key_buf)?;
    self.offset = new_off;

    // Restore full key
    // 恢复完整键
    let key = restore_key(&self.block.prefix, &self.key_buf);
    Some((key, pos))
  }
}

/// Get last key in block (optimized: only reads last restart interval)
/// 获取块中最后一个键（优化：只读最后一个重启区间）
pub fn last_key(block: &Block) -> Option<Box<[u8]>> {
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
