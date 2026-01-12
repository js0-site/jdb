//! Background flush task
//! 后台刷盘任务

use std::{cell::UnsafeCell, rc::Rc};

use compio::io::AsyncWriteAtExt;
use compio_fs::File as FsFile;

use super::{consts::MAX_WRITE_SIZE, inner::Inner};
use crate::Pos;

pub(super) async fn flush_task(inner: Rc<UnsafeCell<Inner>>) {
  loop {
    let (ptr, offset, total_len, file_ptr) = {
      let i = unsafe { &mut *inner.get() };
      match i.try_flush() {
        Some(req) => {
          let f_ptr: *mut FsFile = i
            .file
            .as_mut()
            .map(|f| f as *mut FsFile)
            .unwrap_or(std::ptr::null_mut());
          (req.0, req.1, req.2, f_ptr)
        }
        None => {
          i.ing = false;
          break;
        }
      }
    };

    if !file_ptr.is_null() {
      let mut written = 0;
      let f = unsafe { &mut *file_ptr };
      while written < total_len {
        let chunk = (total_len - written).min(MAX_WRITE_SIZE);
        let slice = unsafe { std::slice::from_raw_parts(ptr.add(written), chunk) };
        if let Err(e) = f.write_all_at(slice, offset + written as Pos).await.0 {
          log::error!("Flush write error: {e}");
          break;
        }
        written += chunk;
      }
    }

    let inner_mut = unsafe { &mut *inner.get() };
    inner_mut.end_flush();
    if let Some(waker) = inner_mut.waker.take() {
      waker.wake();
    }
  }
}
