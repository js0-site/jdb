//! Iterator types for memory table
//! 内存表的迭代器类型

use std::collections::btree_map;

use jdb_base::Pos;

pub use crate::merge::MergeIter;

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
