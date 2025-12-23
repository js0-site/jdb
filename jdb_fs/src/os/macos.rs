//! macOS specific implementations

use std::{mem::MaybeUninit, os::fd::RawFd};

use compio::fs::OpenOptions;

use crate::{Error, Result};

/// macOS Direct IO via F_NOCACHE in post_open
/// macOS 通过 post_open 中的 F_NOCACHE 实现 Direct IO
pub fn direct(_o: &mut OpenOptions) {}

/// Ensure WAL durability consistent with Linux
/// 确保 WAL 持久性与 Linux 一致
pub fn dsync(o: &mut OpenOptions) {
  o.custom_flags(libc::O_DSYNC);
}

pub fn post_open(fd: RawFd) -> Result<()> {
  if unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) } == -1 {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }
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

  // Try contiguous allocation first
  // 优先尝试连续分配
  let mut fstore = libc::fstore_t {
    fst_flags: libc::F_ALLOCATECONTIG,
    fst_posmode: libc::F_PEOFPOSMODE,
    fst_offset: 0,
    fst_length: len,
    fst_bytesalloc: 0,
  };
  if unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut fstore) } == -1 {
    // Fallback to fragmented allocation
    // 回退到碎片化分配
    fstore.fst_flags = libc::F_ALLOCATEALL;
    if unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut fstore) } == -1 {
      return Err(Error::Io(std::io::Error::last_os_error()));
    }
  }

  if unsafe { libc::ftruncate(fd, len) } == -1 {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }
  Ok(())
}
