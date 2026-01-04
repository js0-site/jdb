//! Data block reader
//! 数据块读取器

use super::iter::BlockIter;

/// Data block with prefix compression
/// 带前缀压缩的数据块
#[derive(Debug, Clone)]
pub(crate) struct DataBlock {
  pub data: Vec<u8>,
  pub prefix: Box<[u8]>,
  pub entries_start: u32,
  pub data_end: u32,
  pub restart_count: u32,
  pub item_count: u32,
}

impl DataBlock {
  pub(crate) fn from_bytes(data: Vec<u8>) -> Option<Self> {
    let len = data.len();
    if len < 10 {
      return None;
    }

    let restart_count = u32::from_le_bytes(data[len - 8..len - 4].try_into().ok()?);
    let item_count = u32::from_le_bytes(data[len - 4..].try_into().ok()?);

    let trailer_size = (restart_count as usize).checked_mul(4)?.checked_add(8)?;
    if len < trailer_size + 2 {
      return None;
    }
    let prefix_len = u16::from_le_bytes(data.get(0..2)?.try_into().ok()?) as usize;
    if 2 + prefix_len > len - trailer_size {
      return None;
    }

    let prefix: Box<[u8]> = data[2..2 + prefix_len].into();
    let entries_start = (2 + prefix_len) as u32;
    let data_end = (len - trailer_size) as u32;

    Some(Self {
      data,
      prefix,
      entries_start,
      data_end,
      restart_count,
      item_count,
    })
  }

  #[inline]
  pub(crate) fn restart_offset(&self, idx: u32) -> u32 {
    debug_assert!(idx < self.restart_count);
    let p = (self.data_end as usize) + (idx as usize) * 4;
    if p + 4 > self.data.len() {
      return 0;
    }
    u32::from_le_bytes([
      self.data[p],
      self.data[p + 1],
      self.data[p + 2],
      self.data[p + 3],
    ])
  }

  /// Binary search restart points using upper_bound logic
  /// 使用 upper_bound 逻辑二分查找重启点
  pub(crate) fn search_restart(&self, key: &[u8]) -> u32 {
    if self.restart_count == 0
      || key.len() < self.prefix.len()
      || key[..self.prefix.len()] < *self.prefix
    {
      return 0;
    }

    let target = &key[self.prefix.len()..];
    let mut lo = 0u32;
    let mut hi = self.restart_count;

    while lo < hi {
      let mid = (lo + hi) >> 1;
      let offset = self.restart_offset(mid);
      if offset + 2 > self.data_end {
        break;
      }
      let off = offset as usize;
      let len = u16::from_le_bytes([self.data[off], self.data[off + 1]]) as u32;
      if offset + 2 + len > self.data_end {
        break;
      }
      let key_end = (offset + 2 + len) as usize;
      if &self.data[off + 2..key_end] <= target {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    lo.saturating_sub(1)
  }

  #[inline]
  pub(crate) fn iter(&self) -> BlockIter<'_> {
    BlockIter::new(self)
  }

  #[inline]
  pub(crate) fn iter_from_restart(&self, restart_idx: u32) -> BlockIter<'_> {
    BlockIter::from_restart(self, restart_idx)
  }
}
