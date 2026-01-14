//! Background flush task
//! 后台刷盘任务

use std::{cell::UnsafeCell, rc::Rc};

use compio::io::AsyncWriteAtExt;
use compio_fs::File as FsFile;

use super::{consts::MAX_WRITE_SIZE, inner::Inner};
use crate::Pos;

pub(super) async fn flush_task(inner: Rc<UnsafeCell<Inner>>) {
  loop {
    // Safe: single-threaded runtime
    // 安全：单线程运行时
    let i = unsafe { &mut *inner.get() };
    let Some((ptr, offset, total_len)) = i.try_flush() else {
      i.ing = false;
      break;
    };

    let Some(f) = i.file.as_mut() else {
      i.end_flush();
      continue;
    };

    let f_ptr: *mut FsFile = f;

    let mut written = 0;
    while written < total_len {
      let chunk = (total_len - written).min(MAX_WRITE_SIZE);
      // Safe: ptr valid during flush, chunk within bounds
      // 安全：刷盘期间 ptr 有效，chunk 在范围内
      let slice = unsafe { std::slice::from_raw_parts(ptr.add(written), chunk) };
      let f = unsafe { &mut *f_ptr };
      if let Err(e) = f.write_all_at(slice, offset + written as Pos).await.0 {
        log::error!("Flush write error: {e}");
        break;
      }
      written += chunk;
    }

    let i = unsafe { &mut *inner.get() };
    i.end_flush();
    if let Some(waker) = i.waker.take() {
      waker.wake();
    }
  }
}
