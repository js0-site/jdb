//! Ascending stream (forward iteration)
//! 升序流（正向迭代）

use std::ops::Bound;

use futures::stream::{Stream, unfold};
use jdb_base::Kv;

use super::{Key, Lru, StreamInit, before_start, past_end};
use crate::{Table, block::DataBlock};

/// Iterator state within a block
/// 块内迭代器状态
struct IterState {
  offset: usize,
  restart_idx: u32,
  next_restart: usize,
  buf: Vec<u8>,
  count: u32,
}

impl IterState {
  #[inline]
  fn new() -> Self {
    Self {
      offset: 0,
      restart_idx: 0,
      next_restart: usize::MAX,
      buf: Vec::with_capacity(256),
      count: 0,
    }
  }

  #[inline]
  fn reset(&mut self, block: &DataBlock) {
    self.offset = block.entries_start as usize;
    self.restart_idx = 0;
    self.next_restart = if block.restart_count > 1 {
      block.restart_offset(1) as usize
    } else {
      usize::MAX
    };
    self.buf.clear();
    self.count = 0;
  }

  fn next(&mut self, block: &DataBlock) -> Option<Kv> {
    if self.count >= block.item_count {
      return None;
    }

    let is_restart = self.offset >= self.next_restart || self.count == 0;
    if is_restart && self.restart_idx < block.restart_count {
      self.offset = block.restart_offset(self.restart_idx) as usize;
      self.restart_idx += 1;
      self.next_restart = if self.restart_idx < block.restart_count {
        block.restart_offset(self.restart_idx) as usize
      } else {
        usize::MAX
      };
    }

    let data = block.data.get(..block.data_end as usize)?;
    let (new_offset, pos) = crate::block::read_entry(data, self.offset, is_restart, &mut self.buf)?;
    let key = crate::block::restore_key(&block.prefix, &self.buf);

    self.offset = new_offset;
    self.count += 1;

    Some((key, pos))
  }
}

struct State<'a> {
  info: &'a Table,
  lru: Lru,
  start: Bound<Key>,
  end: Bound<Key>,
  cursor: usize,
  end_idx: usize,
  block: Option<DataBlock>,
  iter: IterState,
  done: bool,
}

#[allow(clippy::await_holding_refcell_ref)]
pub fn asc_stream<'a>(
  info: &'a Table,
  lru: Lru,
  start: Bound<&[u8]>,
  end: Bound<&[u8]>,
) -> impl Stream<Item = Kv> + 'a {
  let init = StreamInit::new(info, start, end);

  let state = State {
    info,
    lru,
    start: init.start,
    end: init.end,
    cursor: init.start_idx,
    end_idx: init.end_idx,
    block: None,
    iter: IterState::new(),
    done: init.empty,
  };

  unfold(state, |mut s| async move {
    if s.done {
      return None;
    }
    loop {
      if let Some(block) = &s.block {
        if let Some((key, pos)) = s.iter.next(block) {
          if past_end(&key, &s.end) {
            return None;
          }
          if !before_start(&key, &s.start) {
            return Some(((key, pos), s));
          }
          continue;
        }
        s.block = None;
      }

      if s.cursor <= s.end_idx && s.cursor < s.info.block_count() {
        let idx = s.cursor;
        s.cursor += 1;
        let block = {
          let mut lru = s.lru.borrow_mut();
          s.info.read_block(idx, &mut lru).await
        };
        match block {
          Ok(b) => {
            s.iter.reset(&b);
            s.block = Some(b);
          }
          Err(e) => {
            log::warn!("load block failed: {e}");
            return None;
          }
        }
      } else {
        return None;
      }
    }
  })
}
