//! Windows specific implementations

use std::{
  mem::MaybeUninit,
  os::windows::io::{AsRawHandle, OwnedHandle},
};

use compio::fs::OpenOptions;
use windows_sys::Win32::{
  Foundation::HANDLE,
  Storage::FileSystem::{
    FILE_ALLOCATION_INFO, FILE_BEGIN, FILE_FLAG_NO_BUFFERING, FILE_FLAG_WRITE_THROUGH,
    FILE_STANDARD_INFO, FileAllocationInfo, FileStandardInfo, GetFileInformationByHandleEx,
    SetEndOfFile, SetFileInformationByHandle, SetFilePointer,
  },
};

use crate::{Error, Result};

pub fn direct(o: &mut OpenOptions) {
  o.custom_flags(FILE_FLAG_NO_BUFFERING);
}

pub fn dsync(o: &mut OpenOptions) {
  o.custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH);
}

pub fn preallocate(handle: OwnedHandle, len: i64) -> Result<()> {
  let raw = handle.as_raw_handle() as HANDLE;

  // Get current size via GetFileInformationByHandleEx
  // 通过 GetFileInformationByHandleEx 获取当前大小
  let mut info = MaybeUninit::<FILE_STANDARD_INFO>::uninit();
  if unsafe {
    GetFileInformationByHandleEx(
      raw,
      FileStandardInfo,
      info.as_mut_ptr().cast(),
      size_of::<FILE_STANDARD_INFO>() as _,
    )
  } == 0
  {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }
  let info = unsafe { info.assume_init() };
  if info.EndOfFile >= len {
    return Ok(());
  }

  // Preallocate physical space via FileAllocationInfo
  // 通过 FileAllocationInfo 预分配物理空间
  let mut alloc = FILE_ALLOCATION_INFO {
    AllocationSize: len,
  };
  if unsafe {
    SetFileInformationByHandle(
      raw,
      FileAllocationInfo,
      (&raw mut alloc).cast(),
      size_of::<FILE_ALLOCATION_INFO>() as _,
    )
  } == 0
  {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }

  // Update logical EOF
  // 更新逻辑文件结束位置
  let low = len as i32;
  let mut high = (len >> 32) as i32;
  if unsafe { SetFilePointer(raw, low, &mut high, FILE_BEGIN) } == u32::MAX
    && std::io::Error::last_os_error().raw_os_error() != Some(0)
  {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }
  if unsafe { SetEndOfFile(raw) } == 0 {
    return Err(Error::Io(std::io::Error::last_os_error()));
  }

  Ok(())
}
