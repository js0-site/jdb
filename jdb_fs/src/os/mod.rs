//! Platform specific implementations
//! 平台特定实现

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(windows)]
mod windows;

#[cfg(target_os = "linux")]
pub use linux::*;
#[cfg(target_os = "macos")]
pub use macos::*;
#[cfg(windows)]
pub use windows::*;

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
compile_error!("unsupported platform");

#[cfg(unix)]
pub(crate) fn cur_size(fd: std::os::fd::RawFd) -> crate::Result<i64> {
  // zeroed is safer for some libc/sanitizer
  // zeroed 对某些 libc/sanitizer 更安全
  let mut stat: libc::stat = unsafe { std::mem::zeroed() };
  if unsafe { libc::fstat(fd, &mut stat) } < 0 {
    return Err(crate::Error::Io(std::io::Error::last_os_error()));
  }
  Ok(stat.st_size as i64)
}
