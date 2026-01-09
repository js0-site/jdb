//! Single memtable
//! 单个内存表

use std::{
  collections::{BTreeMap, btree_map::Entry},
  mem,
  rc::Rc,
};

use ider::id;
use jdb_base::Pos;
use jdb_gc::GcLog;

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

  /// Put key-value, record old pos to gc_log if overwriting
  /// 插入键值，如果覆盖则记录旧 pos 到 gc_log
  #[inline]
  pub fn put(&mut self, key: impl Into<Key>, pos: Pos, gc_log: &GcLog) {
    let key = key.into();
    let key_len = key.len();
    match self.data.entry(key) {
      Entry::Vacant(e) => {
        self.size += (key_len + mem::size_of::<Pos>()) as u64;
        e.insert(pos);
      }
      Entry::Occupied(mut e) => {
        // Record old pos for GC
        // 记录旧 pos 用于 GC
        gc_log.discard(*e.get());
        e.insert(pos);
      }
    }
  }

  /// Remove key (insert tombstone), record old pos to gc_log if overwriting
  /// 删除键（插入墓碑），如果覆盖则记录旧 pos 到 gc_log
  #[inline]
  pub fn rm(&mut self, key: impl Into<Key>, old_pos: Pos, gc_log: &GcLog) {
    self.put(key, old_pos.to_tombstone(), gc_log);
  }
}
