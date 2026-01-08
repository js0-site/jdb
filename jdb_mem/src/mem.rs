//! Single memtable
//! 单个内存表

use std::{
  collections::{BTreeMap, btree_map::Entry},
  rc::Rc,
};

use jdb_base::{Pos, id};

/// Memtable inner data
/// 内存表内部数据
#[derive(Debug, Clone, Default)]
pub struct MemInner {
  pub id: u64,
  pub data: BTreeMap<Box<[u8]>, Pos>,
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
  pub fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    let key = key.into();
    match self.data.entry(key) {
      Entry::Vacant(e) => {
        // Size = Key len + Pos struct size
        // 大小 = 键长度 + Pos 结构体大小
        self.size += (e.key().len() + Pos::SIZE) as u64;
        e.insert(pos);
      }
      Entry::Occupied(mut e) => {
        e.insert(pos);
      }
    }
  }

  #[inline]
  pub fn rm(&mut self, key: impl Into<Box<[u8]>>, old_pos: Pos) {
    self.put(key, old_pos.to_tombstone());
  }
}
