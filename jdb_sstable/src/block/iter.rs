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

/// Forward iterator from a restart point
/// 从重启点开始的正向迭代器
pub(crate) struct BlockIter<'a> {
  block: &'a DataBlock,
  pub offset: usize,
  pub restart_idx: u32,
  in_interval: u16,
  buf: Vec<u8>,
  count: u32,
}

impl<'a> BlockIter<'a> {
  #[inline]
  pub fn new(block: &'a DataBlock) -> Self {
    Self {
      block,
      offset: block.entries_start as usize,
      restart_idx: 0,
      in_interval: 0,
      buf: Vec::with_capacity(256),
      count: 0,
    }
  }

  #[inline]
  pub fn from_restart(block: &'a DataBlock, restart_idx: u32) -> Self {
    Self {
      block,
      offset: block.restart_offset(restart_idx) as usize,
      restart_idx,
      in_interval: 0,
      buf: Vec::with_capacity(256),
      count: 0,
    }
  }
}

impl Iterator for BlockIter<'_> {
  type Item = (Box<[u8]>, Pos);

  fn next(&mut self) -> Option<Self::Item> {
    if self.count >= self.block.item_count {
      return None;
    }

    let is_restart = self.in_interval == 0;
    if is_restart && self.restart_idx < self.block.restart_count {
      self.offset = self.block.restart_offset(self.restart_idx) as usize;
      self.restart_idx += 1;
    }

    let data = self.block.data.get(..self.block.data_end as usize)?;
    let (new_offset, pos) = read_entry(data, self.offset, is_restart, &mut self.buf)?;
    let key = restore_key(&self.block.prefix, &self.buf);

    self.offset = new_offset;
    self.in_interval += 1;
    self.count += 1;

    if self.restart_idx < self.block.restart_count {
      let next = self.block.restart_offset(self.restart_idx) as usize;
      if self.offset >= next {
        self.in_interval = 0;
      }
    }

    Some((key, pos))
  }
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
  let data = block.data.get(..end)?;

  let mut buf = Vec::with_capacity(256);
  let mut offset = start;
  let mut is_first = true;
  let mut last: Option<Box<[u8]>> = None;

  while offset < end {
    if let Some((new_offset, _)) = read_entry(data, offset, is_first, &mut buf) {
      let mut key = Vec::with_capacity(block.prefix.len() + buf.len());
      key.extend_from_slice(&block.prefix);
      key.extend_from_slice(&buf);
      last = Some(key.into_boxed_slice());
      offset = new_offset;
      is_first = false;
    } else {
      break;
    }
  }

  last
}
