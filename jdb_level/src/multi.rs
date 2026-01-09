//! Multi-table merge stream for read queries
//! 读取查询用多表合并流
//!
//! Reuses jdb_sst::Multi with NoDiscard for read-only operations
//! 复用 jdb_sst::Multi 配合 NoDiscard 用于只读操作

use std::{cell::RefCell, ops::Bound, pin::Pin, rc::Rc, vec};

use futures_core::Stream;
use jdb_base::{Kv, sst::NoDiscard};
use jdb_fs::FileLru;
use jdb_sst::{Asc, Desc};
use jdb_sst_read::{Table, asc_stream, desc_stream, to_owned};

type Key = Box<[u8]>;
type Lru = Rc<RefCell<FileLru>>;

/// Multi-table merge stream (type alias for read queries)
/// 多表合并流（读取查询的类型别名）
pub type Multi<O, S> = jdb_sst::Multi<O, S, NoDiscard>;

// ============================================================================
// Constructors
// 构造函数
// ============================================================================

pub fn new_asc<'a>(
  tables: &'a [Table],
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
) -> Multi<Asc, Pin<Box<impl Stream<Item = Kv> + 'a>>> {
  new_multi(tables.iter(), lru, start, end, asc_stream)
}

pub fn new_asc_from_refs<'a>(
  tables: vec::IntoIter<&'a Table>,
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
) -> Multi<Asc, Pin<Box<impl Stream<Item = Kv> + 'a>>> {
  new_multi(tables, lru, start, end, asc_stream)
}

pub fn new_desc<'a>(
  tables: &'a [Table],
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
) -> Multi<Desc, Pin<Box<impl Stream<Item = Kv> + 'a>>> {
  new_multi(tables.iter(), lru, start, end, desc_stream)
}

pub fn new_desc_from_refs<'a>(
  tables: vec::IntoIter<&'a Table>,
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
) -> Multi<Desc, Pin<Box<impl Stream<Item = Kv> + 'a>>> {
  new_multi(tables, lru, start, end, desc_stream)
}

fn new_multi<'a, O: jdb_sst::Order, S: Stream<Item = Kv> + Unpin>(
  tables: impl Iterator<Item = &'a Table>,
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
  make_stream: impl Fn(&'a Table, Lru, Bound<Key>, Bound<Key>) -> S,
) -> Multi<O, S> {
  let tables: Vec<_> = tables.collect();
  let start = to_owned(start);
  let end = to_owned(end);
  let cap = tables.len();
  let mut streams = Vec::with_capacity(cap);
  let mut src_ids = Vec::with_capacity(cap);

  for t in tables {
    streams.push(make_stream(t, Rc::clone(&lru), start.clone(), end.clone()));
    src_ids.push(t.meta().id);
  }

  jdb_sst::Multi::new(streams, src_ids, NoDiscard, false)
}
