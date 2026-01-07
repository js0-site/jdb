//! Data block reader
//! 数据块读取器

use std::cmp::Ordering;

use jdb_base::Pos;

use super::iter::read_entry;

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

    // Safe: foot checks ensure data is large enough for restart array
    // 安全：Foot 检查确保数据足够大以容纳重启数组
    // Performance: Remove bounds check in hot loop
    // 性能：移除热循环中的边界检查
    unsafe {
      let ptr = self.data.as_ptr().add(p);
      let bytes: [u8; 4] = *(ptr as *const [u8; 4]);
      u32::from_le_bytes(bytes)
    }
  }

  /// Binary search restart points using upper_bound logic
  /// 使用 upper_bound 逻辑二分查找重启点
  #[inline]
  pub(crate) fn search_restart(&self, key: &[u8]) -> u32 {
    let plen = self.prefix.len();
    if self.restart_count == 0 {
      return 0;
    }

    // Fast path: key shorter than prefix or key < prefix
    // 快速路径：key 比前缀短或 key < 前缀
    if key.len() < plen || key[..plen] < *self.prefix {
      return 0;
    }

    let target = &key[plen..];
    let mut lo = 0u32;
    let mut hi = self.restart_count;

    // Find the first restart point > target (upper_bound)
    // 查找第一个大于 target 的重启点 (upper_bound)
    while lo < hi {
      let mid = (lo + hi) >> 1;
      let offset = self.restart_offset(mid) as usize;

      // Direct access: data validated in from_bytes
      // 直接访问：数据已在 from_bytes 中验证
      let slice = &self.data[offset..];
      if slice.len() < 2 {
        break;
      }

      let len = u16::from_le_bytes([slice[0], slice[1]]) as usize;
      if slice.len() < 2 + len {
        break;
      }
      let suffix = &slice[2..2 + len];

      if suffix <= target {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }

    lo.saturating_sub(1)
  }

  /// Find specific key within a restart interval without allocation
  /// 在重启区间内查找指定键，无内存分配
  #[inline]
  pub(crate) fn find_key(
    &self,
    target_suffix: &[u8],
    restart_idx: u32,
    buf: &mut Vec<u8>,
  ) -> Option<Pos> {
    if restart_idx >= self.restart_count {
      return None;
    }

    let mut offset = self.restart_offset(restart_idx) as usize;
    // Calculate the end limit for this interval
    // 计算该区间的结束边界
    let limit = if restart_idx + 1 < self.restart_count {
      self.restart_offset(restart_idx + 1) as usize
    } else {
      self.data_end as usize
    };

    // Reusable buffer for suffix reconstruction
    // 用于后缀重构的复用缓冲区
    let mut is_restart = true;

    while offset < limit {
      let (new_off, pos) = read_entry(&self.data, offset, is_restart, buf)?;

      // Compare reconstructed suffix directly with target suffix
      // 直接比较重构的后缀与目标后缀
      match buf.as_slice().cmp(target_suffix) {
        Ordering::Equal => return Some(pos),
        Ordering::Greater => return None, // Keys are sorted, so we passed it / 键有序，已错过
        Ordering::Less => {}
      }

      offset = new_off;
      is_restart = false;
    }
    None
  }

  /// Decode all keys in a specific restart interval
  /// 解码指定重启区间内的所有键
  pub(crate) fn read_interval(&self, restart_idx: u32, buf: &mut Vec<(Box<[u8]>, Pos)>) {
    if restart_idx >= self.restart_count {
      return;
    }

    let mut offset = self.restart_offset(restart_idx) as usize;
    let limit = if restart_idx + 1 < self.restart_count {
      self.restart_offset(restart_idx + 1) as usize
    } else {
      self.data_end as usize
    };

    // Reusable buffer for delta decoding
    // 用于增量解码的复用缓冲区
    let mut key_buf = Vec::with_capacity(256);
    let mut is_restart = true;

    while offset < limit {
      // read_entry and restore_key are from super::iter
      if let Some((new_off, pos)) =
        super::iter::read_entry(&self.data, offset, is_restart, &mut key_buf)
      {
        let key = super::iter::restore_key(&self.prefix, &key_buf);
        buf.push((key, pos));
        offset = new_off;
        is_restart = false;
      } else {
        break;
      }
    }
  }
}
