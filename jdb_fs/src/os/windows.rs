//! Windows specific implementations

use std::os::windows::{fs::OpenOptionsExt, io::RawHandle};

use compio::fs::OpenOptions;
use windows_sys::Win32::Storage::FileSystem::{FILE_FLAG_NO_BUFFERING, FILE_FLAG_WRITE_THROUGH};

use crate::Result;

pub fn direct(o: &mut OpenOptions) {
  o.custom_flags(FILE_FLAG_NO_BUFFERING);
}

pub fn dsync(o: &mut OpenOptions) {
  o.custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH);
}

pub fn post_open(_handle: RawHandle) -> Result<()> {
  Ok(())
}

pub fn preallocate(_handle: RawHandle, _len: i64) -> Result<()> {
  Ok(())
}
