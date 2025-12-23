#![cfg_attr(docsrs, feature(doc_cfg))]

//! Aligned memory allocator for Direct I/O
//! Direct I/O 对齐内存分配器

mod consts;
mod error;

use std::{
  alloc::{Layout, alloc, dealloc},
  ops::{Deref, DerefMut},
  ptr::NonNull,
};

use compio_buf::{IoBuf, IoBufMut, SetBufInit};
pub use consts::PAGE_SIZE;
pub use error::{Error, Result};

/// Aligned buffer for Direct I/O (4KB aligned)
/// Direct I/O 对齐缓冲区（4KB 对齐）
pub struct AlignedBuf {
  ptr: NonNull<u8>,
  len: usize,
  cap: usize,
}

unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
  /// Create with capacity 创建指定容量
  #[inline]
  pub fn with_cap(cap: usize) -> Result<Self> {
    let cap = cap.max(PAGE_SIZE);
    let layout =
      Layout::from_size_align(cap, PAGE_SIZE).map_err(|e| Error::InvalidLayout(e.to_string()))?;
    let ptr = unsafe { alloc(layout) };
    let ptr = NonNull::new(ptr).ok_or(Error::AllocFailed)?;
    Ok(Self { ptr, len: 0, cap })
  }

  /// Create zeroed buffer 创建零初始化缓冲区
  #[inline]
  pub fn zeroed(size: usize) -> Result<Self> {
    let mut buf = Self::with_cap(size)?;
    unsafe {
      std::ptr::write_bytes(buf.ptr.as_ptr(), 0, size);
    }
    buf.len = size;
    Ok(buf)
  }

  /// Create one page 创建单页
  #[inline]
  pub fn page() -> Result<Self> {
    Self::zeroed(PAGE_SIZE)
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.len
  }

  #[inline]
  pub fn cap(&self) -> usize {
    self.cap
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }

  /// Set length (unsafe: caller must ensure data is initialized)
  /// 设置长度（不安全：调用者需确保数据已初始化）
  #[inline]
  pub unsafe fn set_len(&mut self, len: usize) {
    debug_assert!(len <= self.cap);
    self.len = len;
  }

  #[inline]
  pub fn as_ptr(&self) -> *const u8 {
    self.ptr.as_ptr()
  }

  #[inline]
  pub fn as_mut_ptr(&mut self) -> *mut u8 {
    self.ptr.as_ptr()
  }

  /// Clear buffer 清空缓冲区
  #[inline]
  pub fn clear(&mut self) {
    self.len = 0;
  }

  /// Extend from slice 从切片扩展
  #[inline]
  pub fn extend(&mut self, data: &[u8]) -> Result<()> {
    let new_len = self.len + data.len();
    if new_len > self.cap {
      return Err(Error::BufferOverflow {
        requested: new_len,
        capacity: self.cap,
      });
    }
    unsafe {
      std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr().add(self.len), data.len());
    }
    self.len = new_len;
    Ok(())
  }
}

impl Drop for AlignedBuf {
  fn drop(&mut self) {
    if let Ok(layout) = Layout::from_size_align(self.cap, PAGE_SIZE) {
      unsafe {
        dealloc(self.ptr.as_ptr(), layout);
      }
    }
  }
}

impl Deref for AlignedBuf {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &[u8] {
    unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
  }
}

impl DerefMut for AlignedBuf {
  #[inline]
  fn deref_mut(&mut self) -> &mut [u8] {
    unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
  }
}

impl AsRef<[u8]> for AlignedBuf {
  #[inline]
  fn as_ref(&self) -> &[u8] {
    self
  }
}

impl AsMut<[u8]> for AlignedBuf {
  #[inline]
  fn as_mut(&mut self) -> &mut [u8] {
    self
  }
}

impl Clone for AlignedBuf {
  fn clone(&self) -> Self {
    let mut buf = Self::with_cap(self.cap).unwrap();
    buf.extend(self).unwrap();
    buf
  }
}

impl std::fmt::Debug for AlignedBuf {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("AlignedBuf")
      .field("len", &self.len)
      .field("cap", &self.cap)
      .finish()
  }
}

// compio buffer traits compio 缓冲区 trait 实现

unsafe impl IoBuf for AlignedBuf {
  #[inline]
  fn as_buf_ptr(&self) -> *const u8 {
    self.ptr.as_ptr()
  }

  #[inline]
  fn buf_len(&self) -> usize {
    self.len
  }

  #[inline]
  fn buf_capacity(&self) -> usize {
    self.cap
  }
}

unsafe impl IoBufMut for AlignedBuf {
  #[inline]
  fn as_buf_mut_ptr(&mut self) -> *mut u8 {
    self.ptr.as_ptr()
  }
}

impl SetBufInit for AlignedBuf {
  #[inline]
  unsafe fn set_buf_init(&mut self, len: usize) {
    debug_assert!(len <= self.cap);
    self.len = len;
  }
}
