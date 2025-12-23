//! 零拷贝节点视图 Zero-copy node view

use std::cmp::Ordering;

use jdb_comm::PAGE_HEADER_SIZE;
use jdb_layout::page_type;

/// 节点头偏移 Node header offsets
mod off {
  pub const VERSION: usize = 0;
  pub const NODE_TYPE: usize = 8;
  pub const COUNT: usize = 9;
  pub const LEVEL: usize = 11;      // Internal only
  pub const NEXT: usize = 11;       // Leaf only
  pub const PREV: usize = 15;       // Leaf only
  pub const FREE_END: usize = 19;
  pub const HEADER_SIZE: usize = 24;
}

const LEAF_SLOT: usize = 12;     // key_off(2) + key_len(2) + value(8)
const INTERNAL_SLOT: usize = 8; // child(4) + key_off(2) + key_len(2)
const LOCK_BIT: u64 = 1 << 63;

// ============================================================================
// 辅助函数 Helper functions
// ============================================================================

#[inline]
fn read_u16(data: &[u8], off: usize) -> u16 {
  u16::from_le_bytes([data[off], data[off + 1]])
}

#[inline]
fn read_u32(data: &[u8], off: usize) -> u32 {
  u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
}

#[inline]
fn read_u64(data: &[u8], off: usize) -> u64 {
  u64::from_le_bytes([
    data[off], data[off + 1], data[off + 2], data[off + 3],
    data[off + 4], data[off + 5], data[off + 6], data[off + 7],
  ])
}

#[inline]
fn write_u16(data: &mut [u8], off: usize, v: u16) {
  data[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

#[inline]
fn write_u32(data: &mut [u8], off: usize, v: u32) {
  data[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

#[inline]
fn write_u64(data: &mut [u8], off: usize, v: u64) {
  data[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

// ============================================================================
// Version 乐观锁版本控制
// ============================================================================

#[inline]
pub fn read_version(data: &[u8]) -> u64 {
  read_u64(data, off::VERSION)
}

#[inline]
pub fn validate_version(data: &[u8], expected: u64) -> bool {
  let current = read_version(data);
  current == expected && (current & LOCK_BIT) == 0
}

#[inline]
pub fn begin_write(data: &mut [u8]) -> u64 {
  let v = read_version(data);
  write_u64(data, off::VERSION, v | LOCK_BIT);
  v
}

#[inline]
pub fn end_write(data: &mut [u8]) {
  let v = read_version(data);
  write_u64(data, off::VERSION, (v & !LOCK_BIT) + 1);
}

// ============================================================================
// LeafView 叶子节点只读视图
// ============================================================================

pub struct LeafView<'a> {
  data: &'a [u8],
}

impl<'a> LeafView<'a> {
  #[inline]
  pub fn new(page_data: &'a [u8]) -> Self {
    Self { data: &page_data[PAGE_HEADER_SIZE..] }
  }

  #[inline]
  pub fn version(&self) -> u64 {
    read_version(self.data)
  }

  #[inline]
  pub fn validate(&self, expected: u64) -> bool {
    validate_version(self.data, expected)
  }

  #[inline]
  pub fn count(&self) -> usize {
    read_u16(self.data, off::COUNT) as usize
  }

  #[inline]
  pub fn next(&self) -> u32 {
    read_u32(self.data, off::NEXT)
  }

  #[inline]
  pub fn prev(&self) -> u32 {
    read_u32(self.data, off::PREV)
  }

  #[inline]
  fn slot(&self, idx: usize) -> (usize, usize, u64) {
    let pos = off::HEADER_SIZE + idx * LEAF_SLOT;
    let key_off = read_u16(self.data, pos) as usize;
    let key_len = read_u16(self.data, pos + 2) as usize;
    let value = read_u64(self.data, pos + 4);
    (key_off, key_len, value)
  }

  #[inline]
  pub fn key(&self, idx: usize) -> &[u8] {
    let (off, len, _) = self.slot(idx);
    &self.data[off..off + len]
  }

  #[inline]
  pub fn value(&self, idx: usize) -> u64 {
    self.slot(idx).2
  }

  pub fn search(&self, key: &[u8]) -> Result<usize, usize> {
    let (mut lo, mut hi) = (0, self.count());
    while lo < hi {
      let mid = lo + (hi - lo) / 2;
      match self.key(mid).cmp(key) {
        Ordering::Less => lo = mid + 1,
        Ordering::Greater => hi = mid,
        Ordering::Equal => return Ok(mid),
      }
    }
    Err(lo)
  }
}

// ============================================================================
// LeafMut 叶子节点可变操作
// ============================================================================

pub struct LeafMut<'a> {
  data: &'a mut [u8],
}

impl<'a> LeafMut<'a> {
  #[inline]
  pub fn new(page_data: &'a mut [u8]) -> Self {
    Self { data: &mut page_data[PAGE_HEADER_SIZE..] }
  }

  pub fn init(&mut self) {
    write_u64(self.data, off::VERSION, 0);
    self.data[off::NODE_TYPE] = page_type::INDEX_LEAF;
    write_u16(self.data, off::COUNT, 0);
    write_u32(self.data, off::NEXT, u32::MAX);
    write_u32(self.data, off::PREV, u32::MAX);
    write_u16(self.data, off::FREE_END, self.data.len() as u16);
  }

  #[inline]
  pub fn lock(&mut self) -> u64 {
    begin_write(self.data)
  }

  #[inline]
  pub fn unlock(&mut self) {
    end_write(self.data)
  }

  #[inline]
  fn count(&self) -> usize {
    read_u16(self.data, off::COUNT) as usize
  }

  #[inline]
  fn set_count(&mut self, n: usize) {
    write_u16(self.data, off::COUNT, n as u16);
  }

  #[inline]
  fn free_end(&self) -> usize {
    read_u16(self.data, off::FREE_END) as usize
  }

  #[inline]
  fn set_free_end(&mut self, pos: usize) {
    write_u16(self.data, off::FREE_END, pos as u16);
  }

  #[inline]
  pub fn set_next(&mut self, next: u32) {
    write_u32(self.data, off::NEXT, next);
  }

  #[inline]
  pub fn set_prev(&mut self, prev: u32) {
    write_u32(self.data, off::PREV, prev);
  }

  #[inline]
  pub fn free_space(&self) -> usize {
    let slot_end = off::HEADER_SIZE + self.count() * LEAF_SLOT;
    self.free_end().saturating_sub(slot_end)
  }

  pub fn insert(&mut self, key: &[u8], value: u64) -> Option<usize> {
    let count = self.count();
    let needed = LEAF_SLOT + key.len();

    if self.free_space() < needed {
      return None;
    }

    let idx = {
      let view = LeafView { data: self.data };
      match view.search(key) {
        Ok(idx) => {
          // 键已存在，更新值
          write_u64(self.data, off::HEADER_SIZE + idx * LEAF_SLOT + 4, value);
          return Some(idx);
        }
        Err(idx) => idx,
      }
    };

    // 写入键数据
    let free_end = self.free_end() - key.len();
    self.data[free_end..free_end + key.len()].copy_from_slice(key);
    self.set_free_end(free_end);

    // 移动后续槽
    if idx < count {
      let src = off::HEADER_SIZE + idx * LEAF_SLOT;
      let len = (count - idx) * LEAF_SLOT;
      self.data.copy_within(src..src + len, src + LEAF_SLOT);
    }

    // 写入新槽
    let pos = off::HEADER_SIZE + idx * LEAF_SLOT;
    write_u16(self.data, pos, free_end as u16);
    write_u16(self.data, pos + 2, key.len() as u16);
    write_u64(self.data, pos + 4, value);

    self.set_count(count + 1);
    Some(idx)
  }

  pub fn delete(&mut self, idx: usize) {
    let count = self.count();
    if idx >= count {
      return;
    }

    if idx + 1 < count {
      let src = off::HEADER_SIZE + (idx + 1) * LEAF_SLOT;
      let len = (count - idx - 1) * LEAF_SLOT;
      self.data.copy_within(src..src + len, src - LEAF_SLOT);
    }

    self.set_count(count - 1);
  }
}

// ============================================================================
// InternalView 内部节点只读视图
// ============================================================================

pub struct InternalView<'a> {
  data: &'a [u8],
}

impl<'a> InternalView<'a> {
  #[inline]
  pub fn new(page_data: &'a [u8]) -> Self {
    Self { data: &page_data[PAGE_HEADER_SIZE..] }
  }

  #[inline]
  pub fn version(&self) -> u64 {
    read_version(self.data)
  }

  #[inline]
  pub fn validate(&self, expected: u64) -> bool {
    validate_version(self.data, expected)
  }

  #[inline]
  pub fn count(&self) -> usize {
    read_u16(self.data, off::COUNT) as usize
  }

  #[inline]
  pub fn level(&self) -> u16 {
    read_u16(self.data, off::LEVEL)
  }

  #[inline]
  pub fn child(&self, idx: usize) -> u32 {
    if idx == 0 {
      read_u32(self.data, off::HEADER_SIZE)
    } else {
      read_u32(self.data, off::HEADER_SIZE + 4 + (idx - 1) * INTERNAL_SLOT)
    }
  }

  #[inline]
  pub fn key(&self, idx: usize) -> &[u8] {
    let pos = off::HEADER_SIZE + 4 + idx * INTERNAL_SLOT + 4;
    let key_off = read_u16(self.data, pos) as usize;
    let key_len = read_u16(self.data, pos + 2) as usize;
    &self.data[key_off..key_off + key_len]
  }

  /// 二分查找子节点索引 Binary search for child index
  pub fn find_child(&self, key: &[u8]) -> usize {
    let (mut lo, mut hi) = (0, self.count());
    while lo < hi {
      let mid = lo + (hi - lo) / 2;
      if self.key(mid) <= key {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    lo
  }
}

// ============================================================================
// InternalMut 内部节点可变操作
// ============================================================================

pub struct InternalMut<'a> {
  data: &'a mut [u8],
}

impl<'a> InternalMut<'a> {
  #[inline]
  pub fn new(page_data: &'a mut [u8]) -> Self {
    Self { data: &mut page_data[PAGE_HEADER_SIZE..] }
  }

  pub fn init(&mut self, level: u16) {
    write_u64(self.data, off::VERSION, 0);
    self.data[off::NODE_TYPE] = page_type::INDEX_INTERNAL;
    write_u16(self.data, off::COUNT, 0);
    write_u16(self.data, off::LEVEL, level);
    write_u16(self.data, off::FREE_END, self.data.len() as u16);
  }

  #[inline]
  pub fn lock(&mut self) -> u64 {
    begin_write(self.data)
  }

  #[inline]
  pub fn unlock(&mut self) {
    end_write(self.data)
  }

  #[inline]
  pub fn set_first_child(&mut self, child: u32) {
    write_u32(self.data, off::HEADER_SIZE, child);
  }

  #[inline]
  fn count(&self) -> usize {
    read_u16(self.data, off::COUNT) as usize
  }

  #[inline]
  fn set_count(&mut self, n: usize) {
    write_u16(self.data, off::COUNT, n as u16);
  }

  #[inline]
  fn free_end(&self) -> usize {
    read_u16(self.data, off::FREE_END) as usize
  }

  #[inline]
  fn set_free_end(&mut self, pos: usize) {
    write_u16(self.data, off::FREE_END, pos as u16);
  }

  #[inline]
  pub fn free_space(&self) -> usize {
    let slot_end = off::HEADER_SIZE + 4 + self.count() * INTERNAL_SLOT;
    self.free_end().saturating_sub(slot_end)
  }

  pub fn insert(&mut self, key: &[u8], right_child: u32) -> Option<usize> {
    let count = self.count();
    let needed = INTERNAL_SLOT + key.len();

    if self.free_space() < needed {
      return None;
    }

    // 二分查找插入位置
    let idx = {
      let view = InternalView { data: self.data };
      let (mut lo, mut hi) = (0, count);
      while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if view.key(mid) < key {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      lo
    };

    // 写入键数据
    let free_end = self.free_end() - key.len();
    self.data[free_end..free_end + key.len()].copy_from_slice(key);
    self.set_free_end(free_end);

    // 移动后续槽
    if idx < count {
      let src = off::HEADER_SIZE + 4 + idx * INTERNAL_SLOT;
      let len = (count - idx) * INTERNAL_SLOT;
      self.data.copy_within(src..src + len, src + INTERNAL_SLOT);
    }

    // 写入新槽
    let pos = off::HEADER_SIZE + 4 + idx * INTERNAL_SLOT;
    write_u32(self.data, pos, right_child);
    write_u16(self.data, pos + 4, free_end as u16);
    write_u16(self.data, pos + 6, key.len() as u16);

    self.set_count(count + 1);
    Some(idx)
  }
}

// ============================================================================
// 节点类型判断
// ============================================================================

#[inline]
pub fn node_type(page_data: &[u8]) -> u8 {
  page_data[PAGE_HEADER_SIZE + off::NODE_TYPE]
}

#[inline]
pub fn is_leaf(page_data: &[u8]) -> bool {
  node_type(page_data) == page_type::INDEX_LEAF
}
