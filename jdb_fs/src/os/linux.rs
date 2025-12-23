//! Linux specific implementations

use std::os::{fd::RawFd, unix::fs::OpenOptionsExt};

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
  if unsafe { libc::fallocate(fd, 0, 0, len) } < 0 {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }
  Ok(())
}
