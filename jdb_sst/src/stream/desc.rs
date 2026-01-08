//! Descending stream (reverse iteration)
//! 降序流（反向迭代）

use std::ops::Bound;

use futures::stream::{Stream, unfold};
use jdb_base::Kv;

use super::{Key, Lru, StreamInit, before_start, past_end};
use crate::{Table, block::DataBlock};

struct State<'a> {
  info: &'a Table,
  lru: Lru,
  start: Bound<Key>,
  end: Bound<Key>,
  cursor: usize,
  start_idx: usize,
  block: Option<DataBlock>,
  restart_idx: i32,
  buf: Vec<Kv>,
  done: bool,
}

#[allow(clippy::await_holding_refcell_ref)]
pub fn desc_stream<'a>(
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
    cursor: init.end_idx,
    start_idx: init.start_idx,
    block: None,
    restart_idx: -1,
    buf: Vec::with_capacity(32),
    done: init.empty,
  };

  unfold(state, |mut s| async move {
    if s.done {
      return None;
    }
    loop {
      if let Some((key, pos)) = s.buf.pop() {
        if before_start(&key, &s.start) {
          return None;
        }
        if !past_end(&key, &s.end) {
          return Some(((key, pos), s));
        }
        continue;
      }

      if let Some(block) = &s.block {
        if s.restart_idx >= 0 {
          s.buf.clear();
          block.read_interval(s.restart_idx as u32, &mut s.buf);
          s.restart_idx -= 1;
          continue;
        }
        s.block = None;
      }

      let count = s.info.block_count();
      if s.cursor != usize::MAX && s.cursor >= s.start_idx && s.cursor < count {
        let idx = s.cursor;
        s.cursor = s.cursor.wrapping_sub(1);
        let block = {
          let mut lru = s.lru.borrow_mut();
          s.info.read_block(idx, &mut lru).await
        };
        match block {
          Ok(b) => {
            s.restart_idx = b.restart_count as i32 - 1;
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
