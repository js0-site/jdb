//! macOS specific implementations

use std::os::fd::RawFd;

use compio::fs::OpenOptions;

use crate::{Error, Result};

pub fn direct(_o: &mut OpenOptions) {}

pub fn dsync(_o: &mut OpenOptions) {}

pub fn post_open(fd: RawFd) -> Result<()> {
  if unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) } == -1 {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }
  Ok(())
}

pub fn preallocate(fd: RawFd, len: i64) -> Result<()> {
  let mut fstore = libc::fstore_t {
    fst_flags: libc::F_ALLOCATECONTIG,
    fst_posmode: libc::F_PEOFPOSMODE,
    fst_offset: 0,
    fst_length: len,
    fst_bytesalloc: 0,
  };
  if unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut fstore) } == -1 {
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
