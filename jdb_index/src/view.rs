//! 零拷贝节点视图 Zero-copy node view
//!
//! 使用原子操作确保版本号的并发安全
//! Uses atomic operations for version concurrency safety

use std::cmp::Ordering;
use std::sync::atomic::{AtomicU64, Ordering as AO};

use jdb_comm::PAGE_HEADER_SIZE;
use jdb_layout::page_type;

/// 节点头偏移 Node header offsets
mod off {
  pub const VERSION: usize = 0;
  pub const NODE_TYPE: usize = 8;
  pub const COUNT: usize = 9;
  pub const LEVEL_OR_NEXT: usize = 11;
  pub const PREV: usize = 15;
  pub const FREE_END: usize = 19;
  pub const HEADER_SIZE: usize = 24;
}

/// 叶子槽大小 Leaf slot: key_off(2) + key_len(2) + value(8)
const LEAF_SLOT: usize = 12;
/// 内部槽大小 Internal slot: child(4) + key_off(2) + key_len(2)
const INTERNAL_SLOT: usize = 8;
/// 锁标志位 Lock bit
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

/// 原子读版本 Atomic read version
#[inline]
fn atomic_version(data: &[u8]) -> u64 {
  let ptr = data.as_ptr() as *const AtomicU64;
  unsafe { (*ptr).load(AO::Acquire) }
}

/// 原子设置版本 Atomic set version
#[inline]
fn atomic_set_version(data: &[u8], val: u64) {
  let ptr = data.as_ptr() as *const AtomicU64;
  unsafe { (*ptr).store(val, AO::Release) }
}

// ============================================================================
// LeafView 叶子节点只读视图
// ============================================================================

pub struct LeafView<'a> {
  data: &'a [u8],
}

impl<'a> LeafView<'a> {
  #[inline]
  pub fn new(page: &'a [u8]) -> Self {
    Self { data: &page[PAGE_HEADER_SIZE..] }
  }

  #[inline]
  pub fn version(&self) -> u64 {
    atomic_version(self.data)
  }

  #[inline]
  pub fn validate(&self, expected: u64) -> bool {
    let cur = self.version();
    cur == expected && (cur & LOCK_BIT) == 0
  }

  #[inline]
  pub fn count(&self) -> usize {
    read_u16(self.data, off::COUNT) as usize
  }

  #[inline]
  pub fn next(&self) -> u32 {
    read_u32(self.data, off::LEVEL_OR_NEXT)
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
    let value = u64::from_le_bytes([
      self.data[pos + 4], self.data[pos + 5], self.data[pos + 6], self.data[pos + 7],
      self.data[pos + 8], self.data[pos + 9], self.data[pos + 10], self.data[pos + 11],
    ]);
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

  /// 二分查找 Binary search
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

  /// 获取所有键值对 Get all entries
  pub fn entries(&self) -> (Vec<Vec<u8>>, Vec<u64>) {
    let n = self.count();
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
      keys.push(self.key(i).to_vec());
      vals.push(self.value(i));
    }
    (keys, vals)
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
  pub fn new(page: &'a mut [u8]) -> Self {
    Self { data: &mut page[PAGE_HEADER_SIZE..] }
  }

  pub fn init(&mut self) {
    write_u64(self.data, off::VERSION, 0);
    self.data[off::NODE_TYPE] = page_type::INDEX_LEAF;
    write_u16(self.data, off::COUNT, 0);
    write_u32(self.data, off::LEVEL_OR_NEXT, u32::MAX);
    write_u32(self.data, off::PREV, u32::MAX);
    write_u16(self.data, off::FREE_END, self.data.len() as u16);
  }

  #[inline]
  pub fn lock(&mut self) -> u64 {
    let old = atomic_version(self.data);
    atomic_set_version(self.data, old | LOCK_BIT);
    old
  }

  #[inline]
  pub fn unlock(&mut self) {
    let old = atomic_version(self.data);
    atomic_set_version(self.data, (old & !LOCK_BIT) + 1);
  }

  #[inline]
  pub fn count(&self) -> usize {
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
    write_u32(self.data, off::LEVEL_OR_NEXT, next);
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

  /// 插入键值对 Insert key-value pair
  /// 返回 None 表示空间不足
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
          // 键已存在，更新值 Key exists, update value
          let pos = off::HEADER_SIZE + idx * LEAF_SLOT + 4;
          write_u64(self.data, pos, value);
          return Some(idx);
        }
        Err(idx) => idx,
      }
    };

    // 写入键数据 Write key data
    let free_end = self.free_end() - key.len();
    self.data[free_end..free_end + key.len()].copy_from_slice(key);
    self.set_free_end(free_end);

    // 移动后续槽 Move subsequent slots
    if idx < count {
      let src = off::HEADER_SIZE + idx * LEAF_SLOT;
      let len = (count - idx) * LEAF_SLOT;
      self.data.copy_within(src..src + len, src + LEAF_SLOT);
    }

    // 写入新槽 Write new slot
    let pos = off::HEADER_SIZE + idx * LEAF_SLOT;
    write_u16(self.data, pos, free_end as u16);
    write_u16(self.data, pos + 2, key.len() as u16);
    write_u64(self.data, pos + 4, value);

    self.set_count(count + 1);
    Some(idx)
  }

  /// 删除指定位置 Delete at index
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
  pub fn new(page: &'a [u8]) -> Self {
    Self { data: &page[PAGE_HEADER_SIZE..] }
  }

  #[inline]
  pub fn version(&self) -> u64 {
    atomic_version(self.data)
  }

  #[inline]
  pub fn validate(&self, expected: u64) -> bool {
    let cur = self.version();
    cur == expected && (cur & LOCK_BIT) == 0
  }

  #[inline]
  pub fn count(&self) -> usize {
    read_u16(self.data, off::COUNT) as usize
  }

  #[inline]
  pub fn level(&self) -> u16 {
    read_u16(self.data, off::LEVEL_OR_NEXT)
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

  /// 获取所有键和子节点 Get all entries
  pub fn entries(&self) -> (Vec<Vec<u8>>, Vec<u32>) {
    let n = self.count();
    let mut keys = Vec::with_capacity(n);
    let mut children = Vec::with_capacity(n + 1);
    children.push(self.child(0));
    for i in 0..n {
      keys.push(self.key(i).to_vec());
      children.push(self.child(i + 1));
    }
    (keys, children)
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
  pub fn new(page: &'a mut [u8]) -> Self {
    Self { data: &mut page[PAGE_HEADER_SIZE..] }
  }

  pub fn init(&mut self, level: u16) {
    write_u64(self.data, off::VERSION, 0);
    self.data[off::NODE_TYPE] = page_type::INDEX_INTERNAL;
    write_u16(self.data, off::COUNT, 0);
    write_u16(self.data, off::LEVEL_OR_NEXT, level);
    write_u16(self.data, off::FREE_END, self.data.len() as u16);
  }

  #[inline]
  pub fn lock(&mut self) -> u64 {
    let old = atomic_version(self.data);
    atomic_set_version(self.data, old | LOCK_BIT);
    old
  }

  #[inline]
  pub fn unlock(&mut self) {
    let old = atomic_version(self.data);
    atomic_set_version(self.data, (old & !LOCK_BIT) + 1);
  }

  #[inline]
  pub fn set_first_child(&mut self, child: u32) {
    write_u32(self.data, off::HEADER_SIZE, child);
  }

  #[inline]
  pub fn count(&self) -> usize {
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

  /// 插入键和右子节点 Insert key and right child
  pub fn insert(&mut self, key: &[u8], right_child: u32) -> Option<usize> {
    let count = self.count();
    let needed = INTERNAL_SLOT + key.len();

    if self.free_space() < needed {
      return None;
    }

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

    // 写入键数据 Write key data
    let free_end = self.free_end() - key.len();
    self.data[free_end..free_end + key.len()].copy_from_slice(key);
    self.set_free_end(free_end);

    // 移动后续槽 Move subsequent slots
    if idx < count {
      let src = off::HEADER_SIZE + 4 + idx * INTERNAL_SLOT;
      let len = (count - idx) * INTERNAL_SLOT;
      self.data.copy_within(src..src + len, src + INTERNAL_SLOT);
    }

    // 写入新槽 Write new slot
    let pos = off::HEADER_SIZE + 4 + idx * INTERNAL_SLOT;
    write_u32(self.data, pos, right_child);
    write_u16(self.data, pos + 4, free_end as u16);
    write_u16(self.data, pos + 6, key.len() as u16);

    self.set_count(count + 1);
    Some(idx)
  }
}

// ============================================================================
// 节点类型判断 Node type check
// ============================================================================

#[inline]
pub fn is_leaf(page: &[u8]) -> bool {
  page[PAGE_HEADER_SIZE + off::NODE_TYPE] == page_type::INDEX_LEAF
}
