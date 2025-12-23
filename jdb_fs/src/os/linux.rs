//! Linux specific implementations

use std::{mem::MaybeUninit, os::fd::RawFd};

use compio::fs::OpenOptions;

use crate::{Error, Result};

pub fn direct(o: &mut OpenOptions) {
  o.custom_flags(libc::O_DIRECT | libc::O_NOATIME);
}

pub fn dsync(o: &mut OpenOptions) {
  o.custom_flags(libc::O_DIRECT | libc::O_NOATIME | libc::O_DSYNC);
}

pub fn post_open(_fd: RawFd) -> Result<()> {
  Ok(())
}

pub fn preallocate(fd: RawFd, len: i64) -> Result<()> {
  // Use fstat to avoid changing shared file offset
  // 使用 fstat 避免修改共享的文件偏移量
  let mut stat = MaybeUninit::<libc::stat>::uninit();
  if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } < 0 {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }
  let cur = unsafe { stat.assume_init().st_size };
  if cur >= len {
    return Ok(());
  }

  if unsafe { libc::fallocate(fd, 0, 0, len) } < 0 {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }
  Ok(())
}
