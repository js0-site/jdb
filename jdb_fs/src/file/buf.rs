//! Single buffer slot
//! 单个缓冲槽

use super::consts::MAX_BUF_SIZE;
use crate::Pos;

pub(super) struct Buf {
  pub(super) data: Vec<u8>,
  pub(super) offset: Pos,
}

impl Buf {
  #[inline]
  pub(super) const fn new() -> Self {
    Self {
      data: Vec::new(),
      offset: 0,
    }
  }

  #[inline]
  pub(super) fn clear(&mut self) {
    self.data.clear();
    if self.data.capacity() > MAX_BUF_SIZE {
      self.data.shrink_to(MAX_BUF_SIZE);
    }
  }

  #[inline]
  pub(super) fn push(&mut self, pos: Pos, src: &[u8]) {
    if self.data.is_empty() {
      self.offset = pos;
    }
    self.data.extend_from_slice(src);
  }

  #[inline]
  pub(super) fn find(&self, pos: Pos, len: usize) -> Option<&[u8]> {
    if self.data.is_empty() {
      return None;
    }
    let off = pos.wrapping_sub(self.offset) as usize;
    if off >= self.data.len() {
      return None;
    }
    let end = (off + len).min(self.data.len());
    Some(unsafe { self.data.get_unchecked(off..end) })
  }
}
