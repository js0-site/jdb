//! Double-buffered file with background flush
//! 双缓冲文件，后台刷新

mod buf;
mod consts;
mod flush;
mod inner;
mod read_at;
mod write;

use std::{
  cell::UnsafeCell,
  future::Future,
  io,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll},
};

use compio_fs::File as FsFile;
use compio_runtime::spawn;
use flush::flush_task;
use inner::Inner;

use crate::Pos;

struct WaitFor<F: Fn(&Inner) -> bool> {
  inner: Rc<UnsafeCell<Inner>>,
  cond: F,
}

impl<F: Fn(&Inner) -> bool> Future for WaitFor<F> {
  type Output = ();
  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    let inner = unsafe { &mut *self.inner.get() };
    if (self.cond)(inner) {
      return Poll::Ready(());
    }
    inner.waker = Some(cx.waker().clone());
    Poll::Pending
  }
}

/// Double-buffered file with background flush
/// 双缓冲文件，后台刷新
pub struct BufFile {
  inner: Rc<UnsafeCell<Inner>>,
  pos: Pos,
  buf_max: usize,
}

impl BufFile {
  pub fn new(file: FsFile, pos: Pos, buf_max: usize) -> Self {
    let mut inner = Inner::new(buf_max);
    inner.file = Some(file);
    Self {
      inner: Rc::new(UnsafeCell::new(inner)),
      pos,
      buf_max,
    }
  }

  #[inline(always)]
  #[allow(clippy::mut_from_ref)]
  fn i(&self) -> &mut Inner {
    unsafe { &mut *self.inner.get() }
  }

  #[inline(always)]
  pub fn pos(&self) -> Pos {
    self.pos
  }

  #[inline(always)]
  fn spawn_flush(&self) {
    let inner = self.i();
    if inner.ing {
      return;
    }
    inner.ing = true;
    let inner = Rc::clone(&self.inner);
    spawn(async move { flush_task(inner).await }).detach();
  }

  async fn wait_if_full(&self) {
    let buf_max = self.buf_max;
    WaitFor {
      inner: self.inner.clone(),
      cond: move |i| i.cur_len() < buf_max,
    }
    .await;
  }

  pub async fn flush(&self) {
    if !self.i().is_idle() {
      self.spawn_flush();
      WaitFor {
        inner: self.inner.clone(),
        cond: Inner::is_idle,
      }
      .await;
    }
  }

  pub async fn sync(&self) -> io::Result<()> {
    self.flush().await;
    if let Some(f) = &self.i().file {
      f.sync_all().await?;
    }
    Ok(())
  }
}

impl Drop for BufFile {
  #[cold]
  fn drop(&mut self) {
    if !self.i().is_idle() {
      log::warn!("BufFile dropped with pending writes");
    }
  }
}
