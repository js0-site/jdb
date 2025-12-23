//! Cached Layout for common allocations
//! 常用分配的缓存 Layout

use crate::consts::{ALIGNMENT, PAGE_SIZE};
use std::alloc::Layout;

/// Cached page layout (4KB aligned)
/// 缓存的页 Layout
pub static PAGE_LAYOUT: Layout = unsafe { Layout::from_size_align_unchecked(PAGE_SIZE, ALIGNMENT) };

/// Get layout for given size with alignment
/// 获取指定大小的对齐 Layout
#[inline]
pub fn layout(size: usize) -> Option<Layout> {
  if size == PAGE_SIZE {
    Some(PAGE_LAYOUT)
  } else {
    Layout::from_size_align(size, ALIGNMENT).ok()
  }
}
