//! Multi-table merge stream for sink
//! 下沉用多表合并流

use std::{cell::RefCell, ops::Bound, pin::Pin, rc::Rc, vec};

use futures_core::Stream;
use jdb_base::Kv;
use jdb_fs::FileLru;
use jdb_sst::{Asc, Multi, NoDiscard, OnDiscard};
use jdb_sst_read::{Table, asc_stream, to_owned};

type Lru = Rc<RefCell<FileLru>>;

/// Create ascending merge stream with discard callback
/// 创建带丢弃回调的升序合并流
pub fn new_asc<'a, D: OnDiscard>(
  tables: &'a [Table],
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
  discard: D,
  bottom: bool,
) -> Multi<'a, Asc, Pin<Box<impl Stream<Item = Kv> + 'a>>, D> {
  new_multi(tables.iter(), lru, start, end, discard, bottom)
}

/// Create ascending merge stream from refs with discard callback
/// 从引用创建带丢弃回调的升序合并流
pub fn new_asc_from_refs<'a, D: OnDiscard>(
  tables: vec::IntoIter<&'a Table>,
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
  discard: D,
  bottom: bool,
) -> Multi<'a, Asc, Pin<Box<impl Stream<Item = Kv> + 'a>>, D> {
  new_multi(tables, lru, start, end, discard, bottom)
}

/// Create ascending merge stream without discard (for read-only)
/// 创建无丢弃回调的升序合并流（只读用）
pub fn new_asc_no_discard<'a>(
  tables: &'a [Table],
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
) -> Multi<'a, Asc, Pin<Box<impl Stream<Item = Kv> + 'a>>, NoDiscard> {
  new_multi(tables.iter(), lru, start, end, NoDiscard, false)
}

fn new_multi<'a, D: OnDiscard>(
  tables: impl Iterator<Item = &'a Table>,
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
  discard: D,
  bottom: bool,
) -> Multi<'a, Asc, Pin<Box<impl Stream<Item = Kv> + 'a>>, D> {
  let tables: Vec<_> = tables.collect();
  let start = to_owned(start);
  let end = to_owned(end);
  let cap = tables.len();
  let mut streams = Vec::with_capacity(cap);
  let mut src_ids = Vec::with_capacity(cap);

  for t in tables {
    streams.push(asc_stream(t, Rc::clone(&lru), start.clone(), end.clone()));
    src_ids.push(t.meta().id);
  }

  Multi::new(streams, src_ids, discard, bottom)
}
