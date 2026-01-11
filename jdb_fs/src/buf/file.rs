//! Double-buffered fs with background flush
//! 双缓冲文件，后台刷新

use std::{cell::Cell, io, rc::Rc, time::Duration};

use compio::{
  buf::{IoBuf, IoBufMut, IoVectoredBuf},
  io::{AsyncReadAt, AsyncWrite, AsyncWriteAtExt},
};
use compio_fs::File as FsFile;
use compio_runtime::spawn;

use crate::Pos;

const SLEEP_DUR: Duration = Duration::from_millis(1);
const MAX_BUF_SIZE: usize = 4 * 1024 * 1024;
const MAX_WRITE_SIZE: usize = 128 * 1024 * 1024;

/// 0 = idle, 1 = flushing slot0, 2 = flushing slot1
/// 0 = 空闲, 1 = 刷槽0, 2 = 刷槽1
type State = u8;
const IDLE: State = 0;
const FLUSH0: State = 1;
const FLUSH1: State = 2;

struct Buf {
  buf: Vec<u8>,
  offset: Pos,
}

impl Buf {
  const fn new() -> Self {
    Self {
      buf: Vec::new(),
      offset: 0,
    }
  }

  #[inline(always)]
  fn clear(&mut self) {
    self.buf.clear();
    if self.buf.capacity() > MAX_BUF_SIZE {
      self.buf.shrink_to(MAX_BUF_SIZE);
    }
  }

  #[inline(always)]
  fn find(&self, pos: Pos, len: usize) -> Option<&[u8]> {
    let buf_len = self.buf.len();
    if buf_len == 0 {
      return None;
    }
    if pos >= self.offset {
      let off = (pos - self.offset) as usize;
      if off < buf_len {
        let end = (off + len).min(buf_len);
        return Some(&self.buf[off..end]);
      }
    }
    None
  }
}

struct Inner {
  slots: [Buf; 2],
  state: State,
  fs: Option<FsFile>,
}

struct Shared {
  inner: Cell<Inner>,
}

impl Shared {
  fn new(cap: usize) -> Self {
    let mut slot0 = Buf::new();
    slot0.buf.reserve(cap);
    Self {
      inner: Cell::new(Inner {
        slots: [slot0, Buf::new()],
        state: IDLE,
        fs: None,
      }),
    }
  }

  #[inline(always)]
  #[allow(clippy::mut_from_ref)]
  fn inner(&self) -> &mut Inner {
    unsafe { &mut *self.inner.as_ptr() }
  }

  /// Current write slot index (opposite of flushing slot)
  /// 当前写入槽索引（刷盘槽的对面）
  #[inline(always)]
  fn cur_idx(&self) -> usize {
    match self.inner().state {
      FLUSH0 => 1,
      FLUSH1 => 0,
      _ => 0,
    }
  }

  #[inline(always)]
  fn push(&self, pos: Pos, data: &[u8]) {
    let i = self.inner();
    let slot = &mut i.slots[self.cur_idx()];
    if slot.buf.is_empty() {
      slot.offset = pos;
    }
    slot.buf.extend_from_slice(data);
  }

  #[inline(always)]
  fn cur_len(&self) -> usize {
    self.inner().slots[self.cur_idx()].buf.len()
  }

  #[inline(always)]
  fn is_idle(&self) -> bool {
    let i = self.inner();
    i.state == IDLE && i.slots[0].buf.is_empty() && i.slots[1].buf.is_empty()
  }

  /// Try start flush, return (slot_idx, offset, len) if has data
  /// 尝试开始刷盘，有数据则返回 (槽索引, 偏移, 长度)
  fn try_flush(&self) -> Option<(usize, Pos, usize)> {
    let i = self.inner();
    if i.state != IDLE {
      return None;
    }
    // Prefer slot that's not current (was just filled)
    // 优先刷非当前槽（刚填满的）
    for idx in [1usize, 0] {
      let slot = &i.slots[idx];
      if !slot.buf.is_empty() {
        i.state = if idx == 0 { FLUSH0 } else { FLUSH1 };
        return Some((idx, slot.offset, slot.buf.len()));
      }
    }
    None
  }

  #[inline(always)]
  fn end_flush(&self, idx: usize) {
    let i = self.inner();
    i.slots[idx].clear();
    i.state = IDLE;
  }

  fn find(&self, pos: Pos, len: usize) -> Option<&[u8]> {
    let i = self.inner();
    // Check flushing slot first (recent data)
    // 先查刷盘槽（最近数据）
    match i.state {
      FLUSH0 => {
        if let Some(r) = i.slots[0].find(pos, len) {
          return Some(r);
        }
      }
      FLUSH1 => {
        if let Some(r) = i.slots[1].find(pos, len) {
          return Some(r);
        }
      }
      _ => {}
    }
    i.slots[self.cur_idx()].find(pos, len)
  }
}

async fn flush_task(shared: Rc<Shared>) {
  loop {
    let Some((idx, offset, len)) = shared.try_flush() else {
      break;
    };

    if let Some(f) = &mut shared.inner().fs {
      let ptr = shared.inner().slots[idx].buf.as_ptr();
      let mut written = 0;
      while written < len {
        let chunk = (len - written).min(MAX_WRITE_SIZE);
        let slice = unsafe { std::slice::from_raw_parts(ptr.add(written), chunk) };
        let _ = f.write_all_at(slice, offset + written as u64).await;
        written += chunk;
      }
    }

    shared.end_flush(idx);
  }
}

/// Double-buffered fs with background flush
/// 双缓冲文件，后台刷新
pub struct File {
  shared: Rc<Shared>,
  pos: Pos,
  buf_max: usize,
  task_spawned: Cell<bool>,
}

impl File {
  pub fn new(fs: FsFile, pos: Pos, cap: usize, buf_max: usize) -> Self {
    let shared = Rc::new(Shared::new(cap));
    shared.inner().fs = Some(fs);
    Self {
      shared,
      pos,
      buf_max,
      task_spawned: Cell::new(false),
    }
  }

  #[inline(always)]
  pub fn pos(&self) -> Pos {
    self.pos
  }

  #[inline(always)]
  pub fn set_pos(&mut self, pos: Pos) {
    self.pos = pos;
  }

  #[inline(always)]
  fn maybe_spawn(&self) {
    if !self.task_spawned.get() {
      self.task_spawned.set(true);
      let shared = Rc::clone(&self.shared);
      spawn(async move {
        flush_task(shared).await;
      })
      .detach();
    }
  }

  async fn wait_if_full(&self) {
    while self.shared.cur_len() >= self.buf_max {
      compio_runtime::time::sleep(SLEEP_DUR).await;
    }
  }

  pub async fn flush(&self) {
    // Spawn if has data but task not running
    // 有数据但任务未运行则启动
    if !self.shared.is_idle() && !self.task_spawned.get() {
      self.maybe_spawn();
    }
    while !self.shared.is_idle() {
      compio_runtime::time::sleep(SLEEP_DUR).await;
    }
    self.task_spawned.set(false);
  }

  pub async fn sync(&self) -> io::Result<()> {
    self.flush().await;
    if let Some(f) = &self.shared.inner().fs {
      f.sync_all().await?;
    }
    Ok(())
  }

  #[inline(always)]
  pub fn has_pending(&self) -> bool {
    !self.shared.is_idle()
  }

  pub fn take_fs(&self) -> Option<FsFile> {
    self.shared.inner().fs.take()
  }

  pub fn set_fs(&self, fs: FsFile) {
    self.shared.inner().fs = Some(fs);
  }
}

impl AsyncWrite for File {
  async fn write<T: IoBuf>(&mut self, buf: T) -> compio::BufResult<usize, T> {
    let slice = buf.as_slice();
    let len = slice.len();
    if len == 0 {
      return compio::BufResult(Ok(0), buf);
    }

    self.wait_if_full().await;
    self.shared.push(self.pos, slice);
    self.pos += len as u64;
    self.maybe_spawn();

    compio::BufResult(Ok(len), buf)
  }

  async fn write_vectored<T: IoVectoredBuf>(&mut self, buf: T) -> compio::BufResult<usize, T> {
    let mut total = 0;
    for slice in buf.iter_slice() {
      if !slice.is_empty() {
        self.wait_if_full().await;
        self.shared.push(self.pos, slice);
        self.pos += slice.len() as u64;
        total += slice.len();
      }
    }
    if total > 0 {
      self.maybe_spawn();
    }
    compio::BufResult(Ok(total), buf)
  }

  async fn flush(&mut self) -> io::Result<()> {
    File::flush(self).await;
    Ok(())
  }

  async fn shutdown(&mut self) -> io::Result<()> {
    self.sync().await
  }
}

impl AsyncReadAt for File {
  async fn read_at<T: IoBufMut>(&self, mut buf: T, pos: u64) -> compio::BufResult<usize, T> {
    let len = buf.buf_capacity();
    if len == 0 {
      return compio::BufResult(Ok(0), buf);
    }

    // Try buffer first
    // 先查缓冲
    if let Some(data) = self.shared.find(pos, len) {
      let n = data.len();
      unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), buf.as_buf_mut_ptr(), n);
        buf.set_buf_init(n);
      }
      return compio::BufResult(Ok(n), buf);
    }

    if let Some(f) = &self.shared.inner().fs {
      f.read_at(buf, pos).await
    } else {
      compio::BufResult(Err(io::Error::new(io::ErrorKind::NotFound, "no fs")), buf)
    }
  }
}

impl Drop for File {
  fn drop(&mut self) {
    if self.has_pending() {
      log::warn!("File dropped with pending writes, call flush().await before drop");
    }
  }
}
