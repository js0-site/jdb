//! Single memtable
//! 单个内存表

use std::{cmp::Ordering, collections::BTreeMap};

use jdb_base::{Pos, id};

/// Memtable - In-memory sorted key-value store
/// 内存表 - 内存有序键值存储
#[derive(Debug)]
pub struct Mem {
  id: u64,
  data: BTreeMap<Box<[u8]>, Pos>,
  size: u64,
}

impl Mem {
  #[inline]
  pub fn new() -> Self {
    Self {
      id: id(),
      data: BTreeMap::new(),
      size: 0,
    }
  }

  #[inline]
  pub fn id(&self) -> u64 {
    self.id
  }

  #[inline]
  pub fn size(&self) -> u64 {
    self.size
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.data.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.data.is_empty()
  }

  #[inline]
  pub fn data(&self) -> &BTreeMap<Box<[u8]>, Pos> {
    &self.data
  }

  #[inline]
  fn upsert(&mut self, key: Box<[u8]>, val: Pos) {
    let key_len = key.len() as u64;
    // If key exists, only update val, size might not change significantly for fixed-size Pos
    if self.data.insert(key, val).is_none() {
      self.size += key_len + Pos::SIZE as u64;
    }
  }
}

impl Eq for Mem {}

impl PartialEq for Mem {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl PartialOrd for Mem {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for Mem {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.id.cmp(&other.id)
  }
}

impl Default for Mem {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl Mem {
  #[inline]
  pub fn get(&self, key: &[u8]) -> Option<Pos> {
    self.data.get(key).copied()
  }

  #[inline]
  pub fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    self.upsert(key.into(), pos);
  }

  #[inline]
  pub fn rm(&mut self, key: impl Into<Box<[u8]>>) {
    self.upsert(key.into(), Pos::tombstone(id(), 0, 0));
  }
}
