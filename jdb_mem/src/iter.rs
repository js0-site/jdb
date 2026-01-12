//! Iterator types for memory table
//! 内存表的迭代器类型

use std::collections::btree_map;

use jdb_base::{Pos, order::Order};

pub use crate::merge::MergeIter;
use crate::merge::{Three, Two};

pub enum Merged<'a, I, O: Order>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  One(I),
  Two(MergeIter<'a, I, O, 2, Two>),
  Three(MergeIter<'a, I, O, 3, Three>),
}

impl<'a, I, O: Order> Iterator for Merged<'a, I, O>
where
  I: Iterator<Item = (&'a [u8], Pos)>,
{
  type Item = (&'a [u8], Pos);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    match self {
      Self::One(iter) => iter.next(),
      Self::Two(iter) => iter.next(),
      Self::Three(iter) => iter.next(),
    }
  }
}

macro_rules! map_iter {
  ($name:ident, $inner:ty) => {
    pub struct $name<'a>(pub $inner);

    impl<'a> Iterator for $name<'a> {
      type Item = (&'a [u8], Pos);

      #[inline]
      fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (k.as_ref(), *v))
      }
    }
  };
}

map_iter!(MapIter, btree_map::Range<'a, Box<[u8]>, Pos>);
map_iter!(
  MapRevIter,
  std::iter::Rev<btree_map::Range<'a, Box<[u8]>, Pos>>
);
