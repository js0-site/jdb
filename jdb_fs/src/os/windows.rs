//! Windows specific implementations

use std::{
  mem::size_of,
  os::windows::io::{AsRawHandle, OwnedHandle},
};

use compio::fs::OpenOptions;
use windows_sys::Win32::{
  Foundation::HANDLE,
  Storage::FileSystem::{
    FILE_ALLOCATION_INFO, FILE_END_OF_FILE_INFO, FILE_FLAG_NO_BUFFERING, FILE_FLAG_WRITE_THROUGH,
    FILE_STANDARD_INFO, FileAllocationInfo, FileEndOfFileInfo, FileStandardInfo,
    GetFileInformationByHandleEx, SetFileInformationByHandle,
  },
};

use crate::{Error, Result};

pub fn direct(o: &mut OpenOptions) {
  o.custom_flags(FILE_FLAG_NO_BUFFERING);
}

pub fn dsync(o: &mut OpenOptions) {
  o.custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH);
}

fn cur_size(raw: HANDLE) -> Result<i64> {
  let mut info: FILE_STANDARD_INFO = unsafe { std::mem::zeroed() };
  if unsafe {
    GetFileInformationByHandleEx(
      raw,
      FileStandardInfo,
      std::ptr::addr_of_mut!(info).cast(),
      size_of::<FILE_STANDARD_INFO>() as _,
    )
  } == 0
  {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }
  Ok(info.EndOfFile)
}

pub fn preallocate(handle: OwnedHandle, len: i64) -> Result<()> {
  let raw = handle.as_raw_handle() as HANDLE;

  if cur_size(raw)? >= len {
    return Ok(());
  }

  // Preallocate physical space 预分配物理空间
  let mut alloc = FILE_ALLOCATION_INFO {
    AllocationSize: len,
  };
  if unsafe {
    SetFileInformationByHandle(
      raw,
      FileAllocationInfo,
      std::ptr::addr_of_mut!(alloc).cast(),
      size_of::<FILE_ALLOCATION_INFO>() as _,
    )
  } == 0
  {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }

  // Update logical EOF (safe, no file pointer change)
  // 更新逻辑 EOF（安全，不改变文件指针）
  let mut eof = FILE_END_OF_FILE_INFO { EndOfFile: len };
  if unsafe {
    SetFileInformationByHandle(
      raw,
      FileEndOfFileInfo,
      std::ptr::addr_of_mut!(eof).cast(),
      size_of::<FILE_END_OF_FILE_INFO>() as _,
    )
  } == 0
  {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }

  Ok(())
}

/// Truncate file to len 截断文件到指定长度
pub fn truncate(handle: OwnedHandle, len: i64) -> Result<()> {
  let raw = handle.as_raw_handle() as HANDLE;
  let mut eof = FILE_END_OF_FILE_INFO { EndOfFile: len };
  if unsafe {
    SetFileInformationByHandle(
      raw,
      FileEndOfFileInfo,
      std::ptr::addr_of_mut!(eof).cast(),
      size_of::<FILE_END_OF_FILE_INFO>() as _,
    )
  } == 0
  {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }
  Ok(())
}
