//! Single memtable
//! 单个内存表

use std::{
  collections::{BTreeMap, btree_map::Entry},
  mem,
  rc::Rc,
};

use jdb_base::{Pos, id};

use crate::Key;

/// Memtable inner data
/// 内存表内部数据
#[derive(Debug, Clone, Default)]
pub struct MemInner {
  pub id: u64,
  pub data: BTreeMap<Key, Pos>,
  pub size: u64,
}

/// Memtable with Rc
/// 带引用计数的内存表
pub type Mem = Rc<MemInner>;

#[inline]
pub fn new() -> Mem {
  Rc::new(MemInner::new())
}

impl MemInner {
  #[inline]
  fn new() -> Self {
    Self {
      id: id(),
      data: BTreeMap::new(),
      size: 0,
    }
  }

  #[inline]
  pub fn get(&self, key: &[u8]) -> Option<Pos> {
    self.data.get(key).copied()
  }

  #[inline]
  pub fn put(&mut self, key: impl Into<Key>, pos: Pos) {
    let key = key.into();
    let key_len = key.len();
    match self.data.entry(key) {
      Entry::Vacant(e) => {
        // Use mem::size_of for accuracy
        // 使用 mem::size_of 确保准确性
        self.size += (key_len + mem::size_of::<Pos>()) as u64;
        e.insert(pos);
      }
      Entry::Occupied(mut e) => {
        e.insert(pos);
      }
    }
  }

  #[inline]
  pub fn rm(&mut self, key: impl Into<Key>, old_pos: Pos) {
    self.put(key, old_pos.to_tombstone());
  }
}
