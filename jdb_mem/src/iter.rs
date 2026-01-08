//! Range iterators with reference counting
//! 带引用计数的范围迭代器

use std::{cmp::Ordering, collections::btree_map, iter::FusedIterator, ops::Bound, rc::Rc};

use jdb_base::Pos;
use crate::{Handle, Kv};

/// Forward range iterator with Rc ownership
/// 带 Rc 所有权的正向范围迭代器
pub struct MemIter {
  handle: Rc<Handle>,
  // SAFETY: handle keeps Mem alive, inner borrows from handle.mem.data
  // 安全：handle 保持 Mem 存活，inner 借用自 handle.mem.data
  inner: btree_map::Range<'static, Box<[u8]>, Pos>,
  cur: Option<Kv>,
}

impl MemIter {
  pub fn new(handle: Rc<Handle>, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self {
    // SAFETY: extend lifetime, handle owns the Mem
    // 安全：延长生命周期，handle 拥有 Mem
    let inner = unsafe {
      std::mem::transmute::<
        btree_map::Range<'_, Box<[u8]>, Pos>,
        btree_map::Range<'static, Box<[u8]>, Pos>,
      >(handle.mem.data().range::<[u8], _>((start, end)))
    };
    let mut iter = Self {
      handle,
      inner,
      cur: None,
    };
    iter.advance();
    iter
  }

  #[inline]
  pub fn id(&self) -> u64 {
    self.handle.mem.id()
  }

  #[inline]
  pub fn peek(&self) -> Option<&Kv> {
    self.cur.as_ref()
  }

  #[inline]
  pub fn advance(&mut self) {
    // Map internal reference to owned Kv
    self.cur = self.inner.next().map(|(k, v)| (k.clone(), *v));
  }

  #[inline]
  pub fn take(&mut self) -> Option<Kv> {
    let item = self.cur.take();
    self.advance();
    item
  }
}

impl Iterator for MemIter {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if let Some(item) = self.cur.take() {
      self.advance();
      return Some(item);
    }
    self.inner.next().map(|(k, v)| (k.clone(), *v))
  }
}

impl DoubleEndedIterator for MemIter {
  #[inline]
  fn next_back(&mut self) -> Option<Self::Item> {
    if self.cur.is_some() {
      if let Some(item) = self.inner.next_back() {
        return Some((item.0.clone(), *item.1)); // optimization: avoid peek clone if possible, but cur is blocked
      }
      return self.cur.take();
    }
    self.inner.next_back().map(|(k, v)| (k.clone(), *v))
  }
}

impl FusedIterator for MemIter {}

impl Eq for MemIter {}

impl PartialEq for MemIter {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.id() == other.id()
  }
}

impl PartialOrd for MemIter {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MemIter {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    match (&self.cur, &other.cur) {
      (Some((k1, _)), Some((k2, _))) => match k2.as_ref().cmp(k1.as_ref()) {
        Ordering::Equal => self.id().cmp(&other.id()), // Stability check
        ord => ord,
      },
      (Some(_), None) => Ordering::Greater,
      (None, Some(_)) => Ordering::Less,
      (None, None) => Ordering::Equal,
    }
  }
}

/// Reverse range iterator with Rc ownership
/// 带 Rc 所有权的反向范围迭代器
pub struct MemRevIter {
  handle: Rc<Handle>,
  // SAFETY: Same as MemIter
  inner: btree_map::Range<'static, Box<[u8]>, Pos>,
  cur: Option<Kv>,
}

impl MemRevIter {
  pub fn new(handle: Rc<Handle>, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Self {
    let inner = unsafe {
      std::mem::transmute::<
        btree_map::Range<'_, Box<[u8]>, Pos>,
        btree_map::Range<'static, Box<[u8]>, Pos>,
      >(handle.mem.data().range::<[u8], _>((start, end)))
    };
    let mut iter = Self {
      inner,
      cur: None,
      handle,
    };
    iter.advance();
    iter
  }

  #[inline]
  pub fn id(&self) -> u64 {
    self.handle.mem.id()
  }

  #[inline]
  pub fn peek(&self) -> Option<&Kv> {
    self.cur.as_ref()
  }

  #[inline]
  pub fn advance(&mut self) {
    self.cur = self.inner.next_back().map(|(k, v)| (k.clone(), *v));
  }

  #[inline]
  pub fn take(&mut self) -> Option<Kv> {
    let item = self.cur.take();
    self.advance();
    item
  }
}

impl Iterator for MemRevIter {
  type Item = Kv;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.cur.is_some() {
      let item = self.cur.take();
      self.advance();
      item
    } else {
      self.inner.next_back().map(|(k, v)| (k.clone(), *v))
    }
  }
}

impl FusedIterator for MemRevIter {}

impl Eq for MemRevIter {}

impl PartialEq for MemRevIter {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.id() == other.id()
  }
}

impl PartialOrd for MemRevIter {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for MemRevIter {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    match (&self.cur, &other.cur) {
      (Some((k1, _)), Some((k2, _))) => match k1.as_ref().cmp(k2.as_ref()) {
        Ordering::Equal => self.id().cmp(&other.id()),
        ord => ord,
      },
      (Some(_), None) => Ordering::Greater,
      (None, Some(_)) => Ordering::Less,
      (None, None) => Ordering::Equal,
    }
  }
}
