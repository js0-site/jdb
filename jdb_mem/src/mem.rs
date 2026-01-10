//! Single memtable
//! 单个内存表

use std::{
  collections::{BTreeMap, HashMap, btree_map::Entry},
  mem,
  rc::Rc,
};

use ider::id;
use jdb_base::{Pos, WalId, entry_size};

use crate::Key;

/// Size type
/// 大小类型
pub type Size = usize;

/// Memtable inner data
/// 内存表内部数据
#[derive(Debug, Clone, Default)]
pub struct MemInner {
  pub id: u64,
  pub data: BTreeMap<Key, Pos>,
  pub size: u64,
  /// Discarded positions (old pos when overwriting)
  /// 被丢弃的位置（覆盖时的旧 pos）
  pub discarded: Vec<Pos>,
  /// GC size by wal_id
  /// 按 wal_id 的 GC 大小
  pub gc_size: HashMap<WalId, Size>,
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
      discarded: Vec::new(),
      gc_size: HashMap::new(),
    }
  }

  #[inline]
  pub fn get(&self, key: &[u8]) -> Option<Pos> {
    self.data.get(key).copied()
  }

  /// Put key-value, collect old pos if overwriting
  /// 插入键值，如果覆盖则收集旧 pos
  #[inline]
  pub fn put(&mut self, key: impl Into<Key>, pos: Pos) {
    let key = key.into();
    let key_len = key.len();
    match self.data.entry(key) {
      Entry::Vacant(e) => {
        self.size += (key_len + mem::size_of::<Pos>()) as u64;
        e.insert(pos);
      }
      Entry::Occupied(mut e) => {
        let old = *e.get();
        // Collect old pos for GC
        // 收集旧 pos 用于 GC
        self.discarded.push(old);
        *self.gc_size.entry(old.wal_id()).or_default() += entry_size(key_len, old.len() as usize);
        e.insert(pos);
      }
    }
  }

  /// Remove key (insert tombstone), collect old pos if overwriting
  /// 删除键（插入墓碑），如果覆盖则收集旧 pos
  #[inline]
  pub fn rm(&mut self, key: impl Into<Key>, old_pos: Pos) {
    self.put(key, old_pos.to_tombstone());
  }
}
