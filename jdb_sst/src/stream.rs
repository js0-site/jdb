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

// ============================================================================
// Bound helpers
// 边界辅助函数
// ============================================================================

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

// ============================================================================
// Ascending stream
// 升序流
// ============================================================================

#[allow(clippy::await_holding_refcell_ref)]
pub fn asc_stream<'a>(
  table: &'a Table,
  lru: Lru,
  start: Bound<Key>,
  end: Bound<Key>,
) -> Pin<Box<impl Stream<Item = Kv> + 'a>> {
  Box::pin(stream! {
    let mut block_idx = 0;
    while block_idx < table.block_count() {
      // Read block
      // 读块
      #[allow(clippy::await_holding_refcell_ref)]
      let block = {
        let mut lru = lru.borrow_mut();
        table.read_block(block_idx, &mut lru).await.ok()
      };

      if let Some(block) = block {
        // Yield from block
        // 从块中产出
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

// ============================================================================
// Descending stream
// 降序流
// ============================================================================

#[allow(clippy::await_holding_refcell_ref)]
pub fn desc_stream<'a>(
  table: &'a Table,
  lru: Lru,
  start: Bound<Key>,
  end: Bound<Key>,
) -> Pin<Box<impl Stream<Item = Kv> + 'a>> {
  Box::pin(stream! {
    let mut buf = Vec::new();
    let mut block_idx = table.block_count();
    while block_idx > 0 {
      block_idx -= 1;

      // Read block
      // 读块
      #[allow(clippy::await_holding_refcell_ref)]
      let block = {
        let mut lru = lru.borrow_mut();
        table.read_block(block_idx, &mut lru).await.ok()
      };

      if let Some(block) = block {
        // Yield from block (reverse)
        // 从块中产出（逆序）
        // Optimize: Read one interval at a time to keep memory O(IntervalSize)
        // 优化：一次读取一个区间，保持 O(IntervalSize) 内存
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
