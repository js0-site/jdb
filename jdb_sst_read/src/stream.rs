//! Single table stream
//! 单表流

use std::{cell::RefCell, ops::Bound, pin::Pin, rc::Rc};

use async_stream::stream;
use futures_core::Stream;
use jdb_base::Kv;
use jdb_fs::FileLru;

use crate::Table;

type Key = Box<[u8]>;
type Lru = Rc<RefCell<FileLru>>;

#[inline]
pub fn to_owned(bound: Bound<&[u8]>) -> Bound<Key> {
  match bound {
    Bound::Unbounded => Bound::Unbounded,
    Bound::Included(k) => Bound::Included(k.into()),
    Bound::Excluded(k) => Bound::Excluded(k.into()),
  }
}

#[inline]
fn in_range(key: &[u8], start: &Bound<Key>, end: &Bound<Key>) -> bool {
  let after = match start {
    Bound::Unbounded => true,
    Bound::Included(k) => key >= k.as_ref(),
    Bound::Excluded(k) => key > k.as_ref(),
  };
  if !after {
    return false;
  }
  match end {
    Bound::Unbounded => true,
    Bound::Included(k) => key <= k.as_ref(),
    Bound::Excluded(k) => key < k.as_ref(),
  }
}

#[inline]
fn past_end(key: &[u8], end: &Bound<Key>) -> bool {
  match end {
    Bound::Unbounded => false,
    Bound::Included(k) => key > k.as_ref(),
    Bound::Excluded(k) => key >= k.as_ref(),
  }
}

#[inline]
fn before_start(key: &[u8], start: &Bound<Key>) -> bool {
  match start {
    Bound::Unbounded => false,
    Bound::Included(k) => key < k.as_ref(),
    Bound::Excluded(k) => key <= k.as_ref(),
  }
}

/// Ascending stream with start block optimization
/// 升序流（带起始块优化）
#[allow(clippy::await_holding_refcell_ref)]
pub fn asc_stream<'a>(
  table: &'a Table,
  lru: Lru,
  start: Bound<Key>,
  end: Bound<Key>,
) -> Pin<Box<impl Stream<Item = Kv> + 'a>> {
  Box::pin(stream! {
    // Find start block using PGM index
    // 使用 PGM 索引定位起始块
    let mut block_idx = match &start {
      Bound::Unbounded => 0,
      Bound::Included(k) | Bound::Excluded(k) => table.find_block(k),
    };

    while block_idx < table.block_count() {
      #[allow(clippy::await_holding_refcell_ref)]
      let block = {
        let mut lru = lru.borrow_mut();
        table.read_block(block_idx, &mut lru).await.ok()
      };

      if let Some(block) = block {
        for (key, pos) in block.iter() {
          if past_end(key.as_ref(), &end) {
            return;
          }
          if in_range(key.as_ref(), &start, &end) {
            yield (key, pos);
          }
        }
      }

      block_idx += 1;
    }
  })
}

/// Descending stream with end block optimization
/// 降序流（带结束块优化）
#[allow(clippy::await_holding_refcell_ref)]
pub fn desc_stream<'a>(
  table: &'a Table,
  lru: Lru,
  start: Bound<Key>,
  end: Bound<Key>,
) -> Pin<Box<impl Stream<Item = Kv> + 'a>> {
  Box::pin(stream! {
    let mut buf = Vec::new();
    // Find end block using PGM index
    // 使用 PGM 索引定位结束块
    let mut block_idx = match &end {
      Bound::Unbounded => table.block_count(),
      Bound::Included(k) | Bound::Excluded(k) => table.find_block(k).saturating_add(1),
    };

    while block_idx > 0 {
      block_idx -= 1;

      #[allow(clippy::await_holding_refcell_ref)]
      let block = {
        let mut lru = lru.borrow_mut();
        table.read_block(block_idx, &mut lru).await.ok()
      };

      if let Some(block) = block {
        for i in (0..block.restart_count).rev() {
          buf.clear();
          block.read_interval(i, &mut buf);

          for (key, pos) in buf.iter().rev() {
            if before_start(key, &start) {
              return;
            }
            if in_range(key, &start, &end) {
              yield (key.clone(), *pos);
            }
          }
        }
      }
    }
  })
}
