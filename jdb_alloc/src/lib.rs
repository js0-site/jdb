#![cfg_attr(docsrs, feature(doc_cfg))]

//! Aligned memory allocator & Raw buffer wrapper for Direct I/O
//! Direct I/O 对齐内存分配器与原始缓冲区包装器

mod error;

use std::{
  alloc::{Layout, LayoutError, alloc, alloc_zeroed, dealloc, handle_alloc_error},
  fmt::{Debug, Formatter, Result as FmtResult},
  mem::ManuallyDrop,
  ops::{Deref, DerefMut},
  ptr::{NonNull, copy_nonoverlapping},
  slice::{from_raw_parts, from_raw_parts_mut},
};

use Error::{AllocFailed, Overflow};
use compio_buf::{IoBuf, IoBufMut, SetBufInit};
pub use error::{Error, Result};

/// Page size 4KB / 页大小
pub const PAGE_SIZE: usize = 4096;

/// Direct I/O alignment (must be power of 2) / 对齐要求（必须是2的幂）
pub const ALIGNMENT: usize = 4096;

/// Mask for alignment checks / 对齐检查掩码
const ALIGN_MASK: usize = ALIGNMENT - 1;

// --- Layout Cache ---

static PAGE_LAYOUT: Layout = unsafe { Layout::from_size_align_unchecked(PAGE_SIZE, ALIGNMENT) };

/// Get layout for given size / 获取指定大小的布局
#[inline(always)]
fn layout(size: usize) -> std::result::Result<Layout, LayoutError> {
  if size == PAGE_SIZE {
    Ok(PAGE_LAYOUT)
  } else {
    Layout::from_size_align(size, ALIGNMENT)
  }
}

/// Get layout unchecked (for Drop/Clone where cap is known valid)
/// 无检查获取布局（用于 Drop/Clone，cap 已知有效）
#[inline(always)]
fn layout_unchecked(cap: usize) -> Layout {
  if cap == PAGE_SIZE {
    PAGE_LAYOUT
  } else {
    unsafe { Layout::from_size_align_unchecked(cap, ALIGNMENT) }
  }
}

// ============================================================================
// AlignedBuf (Owning)
// ============================================================================

/// Aligned buffer for Direct I/O (Owns memory)
/// 拥有所有权的对齐缓冲区，Drop 时释放内存
pub struct AlignedBuf {
  ptr: NonNull<u8>,
  len: usize,
  cap: usize,
}

unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
  /// Create with capacity (uninitialized memory)
  /// 创建指定容量（未初始化内存）
  #[inline]
  pub fn with_cap(cap: usize) -> Result<Self> {
    let cap = cap.max(PAGE_SIZE);
    let lo = layout(cap)?;
    let ptr = unsafe { alloc(lo) };
    let ptr = NonNull::new(ptr).ok_or(AllocFailed)?;
    Ok(Self { ptr, len: 0, cap })
  }

  /// Create zeroed buffer (OS optimized zero pages)
  /// 创建零初始化缓冲区（OS 优化零页）
  #[inline]
  pub fn zeroed(size: usize) -> Result<Self> {
    let cap = size.max(PAGE_SIZE);
    let lo = layout(cap)?;
    let ptr = unsafe { alloc_zeroed(lo) };
    let ptr = NonNull::new(ptr).ok_or(AllocFailed)?;
    Ok(Self {
      ptr,
      len: size,
      cap,
    })
  }

  /// Create one page (4KB, uses cached layout)
  /// 创建单页（使用缓存 Layout）
  #[inline]
  pub fn page() -> Result<Self> {
    let ptr = unsafe { alloc_zeroed(PAGE_LAYOUT) };
    let ptr = NonNull::new(ptr).ok_or(AllocFailed)?;
    Ok(Self {
      ptr,
      len: PAGE_SIZE,
      cap: PAGE_SIZE,
    })
  }

  /// Slice into chunks for Buffer Pool Arena initialization
  /// 切分为多个 RawIoBuf，用于 Buffer Pool Arena 初始化
  /// # Safety
  /// AlignedBuf must outlive all returned RawIoBufs
  pub unsafe fn slice_into_raws(&self, chunk: usize) -> impl Iterator<Item = RawIoBuf> + '_ {
    debug_assert!(
      chunk > 0 && (chunk & ALIGN_MASK) == 0,
      "chunk alignment mismatch"
    );
    let count = self.cap / chunk;
    let base = self.ptr.as_ptr();
    (0..count).map(move |i| unsafe { RawIoBuf::new(base.add(i * chunk), chunk) })
  }

  /// Convert to RawIoBuf for zero-copy I/O
  /// 转为 RawIoBuf 进行零拷贝 I/O
  /// # Safety
  /// Caller must ensure `AlignedBuf` outlives the I/O operation
  #[inline(always)]
  pub unsafe fn as_raw(&mut self) -> RawIoBuf {
    unsafe { RawIoBuf::new(self.ptr.as_ptr(), self.cap).with_len(self.len) }
  }

  /// Get RawIoBuf view from shared reference
  /// 从共享引用获取 RawIoBuf 视图
  /// # Safety
  /// Caller must ensure synchronization via latches/locks
  #[inline(always)]
  pub unsafe fn as_raw_view(&self) -> RawIoBuf {
    unsafe { RawIoBuf::new(self.ptr.as_ptr(), self.cap).with_len(self.len) }
  }

  /// Deconstruct into raw parts (leak memory)
  /// 解构为原始部分（放弃所有权，不释放内存）
  #[inline(always)]
  pub fn into_raw_parts(self) -> (NonNull<u8>, usize, usize) {
    let me = ManuallyDrop::new(self);
    (me.ptr, me.len, me.cap)
  }

  /// Reconstruct from raw parts
  /// 从原始部分重建
  /// # Safety
  /// Must be created from `into_raw_parts` with same allocator
  #[inline(always)]
  pub unsafe fn from_raw_parts(ptr: NonNull<u8>, len: usize, cap: usize) -> Self {
    Self { ptr, len, cap }
  }

  #[inline(always)]
  pub fn len(&self) -> usize {
    self.len
  }

  #[inline(always)]
  pub fn cap(&self) -> usize {
    self.cap
  }

  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }

  #[inline(always)]
  pub fn as_ptr(&self) -> *const u8 {
    self.ptr.as_ptr()
  }

  #[inline(always)]
  pub fn as_mut_ptr(&mut self) -> *mut u8 {
    self.ptr.as_ptr()
  }

  #[inline(always)]
  pub fn clear(&mut self) {
    self.len = 0;
  }

  /// Truncate to length / 截断到指定长度
  #[inline(always)]
  pub fn truncate(&mut self, len: usize) {
    if len < self.len {
      self.len = len;
    }
  }

  /// Set length
  /// # Safety
  /// Caller must ensure data is initialized up to `len`
  #[inline(always)]
  pub unsafe fn set_len(&mut self, len: usize) {
    debug_assert!(len <= self.cap);
    self.len = len;
  }

  /// Try clone, returns error on OOM instead of panic
  /// 尝试克隆，OOM 时返回错误而非 panic
  pub fn try_clone(&self) -> Result<Self> {
    let lo = layout(self.cap)?;
    let ptr = unsafe { alloc(lo) };
    let ptr = NonNull::new(ptr).ok_or(AllocFailed)?;
    unsafe { copy_nonoverlapping(self.ptr.as_ptr(), ptr.as_ptr(), self.len) };
    Ok(Self {
      ptr,
      len: self.len,
      cap: self.cap,
    })
  }

  /// Extend from slice / 从切片扩展
  #[inline]
  pub fn extend(&mut self, data: &[u8]) -> Result<()> {
    let new_len = self.len + data.len();
    if new_len > self.cap {
      return Err(Overflow(new_len, self.cap));
    }
    unsafe { copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr().add(self.len), data.len()) };
    self.len = new_len;
    Ok(())
  }
}

impl Drop for AlignedBuf {
  fn drop(&mut self) {
    unsafe { dealloc(self.ptr.as_ptr(), layout_unchecked(self.cap)) }
  }
}

impl Deref for AlignedBuf {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &[u8] {
    unsafe { from_raw_parts(self.ptr.as_ptr(), self.len) }
  }
}

impl DerefMut for AlignedBuf {
  #[inline]
  fn deref_mut(&mut self) -> &mut [u8] {
    unsafe { from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
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
    self
      .try_clone()
      .unwrap_or_else(|_| handle_alloc_error(layout_unchecked(self.cap)))
  }
}

impl Debug for AlignedBuf {
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    f.debug_struct("AlignedBuf")
      .field("len", &self.len)
      .field("cap", &self.cap)
      .finish()
  }
}

// --- Compio Traits for AlignedBuf ---

unsafe impl IoBuf for AlignedBuf {
  #[inline(always)]
  fn as_buf_ptr(&self) -> *const u8 {
    self.ptr.as_ptr()
  }

  #[inline(always)]
  fn buf_len(&self) -> usize {
    self.len
  }

  #[inline(always)]
  fn buf_capacity(&self) -> usize {
    self.cap
  }
}

unsafe impl IoBufMut for AlignedBuf {
  #[inline(always)]
  fn as_buf_mut_ptr(&mut self) -> *mut u8 {
    self.ptr.as_ptr()
  }
}

impl SetBufInit for AlignedBuf {
  #[inline(always)]
  unsafe fn set_buf_init(&mut self, len: usize) {
    self.len = len;
  }
}

// ============================================================================
// RawIoBuf (Non-owning, Copy)
// ============================================================================

/// Raw I/O buffer (Non-owning, Copy)
/// 原始 I/O 缓冲区（不持有所有权，可复制）
///
/// For Buffer Pool Arena mode. Implements `Copy` for ergonomic async IO.
/// 用于 Buffer Pool Arena 模式，实现 `Copy` 便于异步 IO
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawIoBuf {
  ptr: *mut u8,
  len: usize,
  cap: usize,
}

// SAFETY: Caller must ensure synchronized access when sharing across threads
// 调用者需确保跨线程访问时的同步（如 Buffer Pool Latch）
unsafe impl Send for RawIoBuf {}
unsafe impl Sync for RawIoBuf {}

impl RawIoBuf {
  /// Create new raw buffer wrapper
  /// # Safety
  /// ptr must be valid and aligned for Direct I/O
  #[inline(always)]
  pub const unsafe fn new(ptr: *mut u8, cap: usize) -> Self {
    Self { ptr, len: 0, cap }
  }

  /// Set length (chainable) / 设置长度（链式调用）
  #[inline(always)]
  pub const fn with_len(mut self, len: usize) -> Self {
    self.len = len;
    self
  }

  /// Create from mutable slice / 从可变切片创建
  #[inline(always)]
  pub fn from_slice(slice: &mut [u8]) -> Self {
    Self {
      ptr: slice.as_mut_ptr(),
      len: slice.len(),
      cap: slice.len(),
    }
  }

  /// Slice for reading (len = 0, waiting for IO to fill)
  /// 切片用于读取（len = 0，等待 IO 填充）
  /// # Safety
  /// 1. `offset + len` must not exceed capacity
  /// 2. `offset` must be aligned for Direct I/O
  #[inline]
  pub unsafe fn slice(self, offset: usize, len: usize) -> Self {
    debug_assert!(offset + len <= self.cap, "slice out of bounds");
    debug_assert!(
      (self.ptr as usize + offset) & ALIGN_MASK == 0,
      "slice not aligned"
    );
    Self {
      ptr: unsafe { self.ptr.add(offset) },
      len: 0,
      cap: len,
    }
  }

  /// Slice for writing (len = cap, contains data)
  /// 切片用于写入（len = cap，包含待写数据）
  /// # Safety
  /// 1. `offset + len` must not exceed capacity
  /// 2. `offset` must be aligned for Direct I/O
  #[inline]
  pub unsafe fn slice_data(self, offset: usize, len: usize) -> Self {
    debug_assert!(offset + len <= self.cap, "slice out of bounds");
    debug_assert!(
      (self.ptr as usize + offset) & ALIGN_MASK == 0,
      "slice not aligned"
    );
    Self {
      ptr: unsafe { self.ptr.add(offset) },
      len,
      cap: len,
    }
  }

  /// Slice unchecked (extreme performance) / 无检查切片（极致性能）
  /// # Safety
  /// Caller MUST ensure bounds and alignment
  #[inline(always)]
  pub unsafe fn slice_unchecked(self, offset: usize, len: usize) -> Self {
    Self {
      ptr: unsafe { self.ptr.add(offset) },
      len: 0,
      cap: len,
    }
  }

  #[inline(always)]
  pub const fn len(&self) -> usize {
    self.len
  }

  #[inline(always)]
  pub const fn cap(&self) -> usize {
    self.cap
  }

  #[inline(always)]
  pub const fn is_empty(&self) -> bool {
    self.len == 0
  }

  #[inline(always)]
  pub const fn as_ptr(&self) -> *const u8 {
    self.ptr
  }

  #[inline(always)]
  pub const fn as_mut_ptr(&self) -> *mut u8 {
    self.ptr
  }

  #[inline(always)]
  pub fn as_slice(&self) -> &[u8] {
    unsafe { from_raw_parts(self.ptr, self.len) }
  }

  #[inline(always)]
  pub fn as_mut_slice(&mut self) -> &mut [u8] {
    unsafe { from_raw_parts_mut(self.ptr, self.len) }
  }

  /// Set length
  /// # Safety
  /// Caller must ensure data is initialized up to `len`
  #[inline(always)]
  pub unsafe fn set_len(&mut self, len: usize) {
    debug_assert!(len <= self.cap);
    self.len = len;
  }
}

impl Debug for RawIoBuf {
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    f.debug_struct("RawIoBuf")
      .field("ptr", &self.ptr)
      .field("len", &self.len)
      .field("cap", &self.cap)
      .finish()
  }
}

// --- Compio Traits for RawIoBuf ---

unsafe impl IoBuf for RawIoBuf {
  #[inline(always)]
  fn as_buf_ptr(&self) -> *const u8 {
    self.ptr
  }

  #[inline(always)]
  fn buf_len(&self) -> usize {
    self.len
  }

  #[inline(always)]
  fn buf_capacity(&self) -> usize {
    self.cap
  }
}

unsafe impl IoBufMut for RawIoBuf {
  #[inline(always)]
  fn as_buf_mut_ptr(&mut self) -> *mut u8 {
    self.ptr
  }
}

impl SetBufInit for RawIoBuf {
  #[inline(always)]
  unsafe fn set_buf_init(&mut self, len: usize) {
    self.len = len;
  }
}
