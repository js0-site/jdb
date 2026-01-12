//! Inner state for double-buffered file
//! 双缓冲文件内部状态

use compio_fs::File as FsFile;

use super::buf::Buf;
use crate::Pos;

pub(super) struct Inner {
  pub(super) buf0: Buf,
  pub(super) buf1: Buf,
  /// None = idle, Some(false) = flushing buf0, Some(true) = flushing buf1
  /// None = 空闲, Some(false) = 刷 buf0, Some(true) = 刷 buf1
  pub(super) flushing: Option<bool>,
  pub(super) file: Option<FsFile>,
  pub(super) waker: Option<std::task::Waker>,
  pub(super) ing: bool,
}

impl Inner {
  pub(super) fn new(cap: usize) -> Self {
    let mut buf0 = Buf::new();
    buf0.data.reserve(cap);
    Self {
      buf0,
      buf1: Buf::new(),
      flushing: None,
      file: None,
      waker: None,
      ing: false,
    }
  }

  #[inline(always)]
  pub(super) fn cur(&mut self) -> &mut Buf {
    if self.flushing == Some(false) {
      &mut self.buf1
    } else {
      &mut self.buf0
    }
  }

  #[inline(always)]
  pub(super) fn cur_len(&self) -> usize {
    if self.flushing == Some(false) {
      self.buf1.data.len()
    } else {
      self.buf0.data.len()
    }
  }

  #[inline(always)]
  pub(super) fn is_idle(&self) -> bool {
    self.flushing.is_none() && self.buf0.data.is_empty() && self.buf1.data.is_empty()
  }

  /// Try start flush, return (buf_ptr, offset, len)
  /// 尝试开始刷盘，返回 (缓冲指针, 偏移, 长度)
  #[inline]
  pub(super) fn try_flush(&mut self) -> Option<(*const u8, Pos, usize)> {
    if self.flushing.is_some() {
      return None;
    }
    let (target_buf, is_buf1) = if !self.buf0.data.is_empty() {
      (&self.buf0, false)
    } else if !self.buf1.data.is_empty() {
      (&self.buf1, true)
    } else {
      return None;
    };
    self.flushing = Some(is_buf1);
    Some((
      target_buf.data.as_ptr(),
      target_buf.offset,
      target_buf.data.len(),
    ))
  }

  #[inline]
  pub(super) fn end_flush(&mut self) {
    if let Some(is_buf1) = self.flushing {
      if is_buf1 {
        self.buf1.clear();
      } else {
        self.buf0.clear();
      }
      self.flushing = None;
    }
  }

  #[inline]
  pub(super) fn find(&self, pos: Pos, len: usize) -> Option<&[u8]> {
    let (flush_buf, cur_buf) = match self.flushing {
      Some(false) => (&self.buf0, &self.buf1),
      Some(true) => (&self.buf1, &self.buf0),
      None => return self.buf0.find(pos, len),
    };
    flush_buf.find(pos, len).or_else(|| cur_buf.find(pos, len))
  }

  #[inline]
  pub(super) fn file_read_len(&self, pos: Pos, max_len: usize) -> usize {
    let mut limit = max_len;
    for buf in [&self.buf0, &self.buf1] {
      if limit == 0 {
        break;
      }
      if buf.data.is_empty() {
        continue;
      }
      let start = buf.offset;
      if pos < start {
        let dist = (start - pos) as usize;
        if dist < limit {
          limit = dist;
        }
      } else if pos < start + buf.data.len() as u64 {
        limit = 0;
      }
    }
    limit
  }
}
