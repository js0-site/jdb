//! Page abstraction 页面抽象

use jdb_alloc::AlignedBuf;
use jdb_comm::PageID;
use jdb_layout::PageHeader;

/// Page state 页面状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageState {
  /// Clean, in memory 干净，在内存中
  Clean,
  /// Modified, needs flush 已修改，需要刷盘
  Dirty,
}

/// Page in buffer pool 缓冲池中的页面
pub struct Page {
  pub id: PageID,
  pub state: PageState,
  pub buf: AlignedBuf,
  pub pin_count: u32,
}

impl Page {
  /// Create new page 创建新页面
  #[inline]
  pub fn new(id: PageID) -> Self {
    Self {
      id,
      state: PageState::Clean,
      buf: AlignedBuf::page(),
      pin_count: 0,
    }
  }

  /// Create from buffer 从缓冲区创建
  #[inline]
  pub fn from_buf(id: PageID, buf: AlignedBuf) -> Self {
    Self {
      id,
      state: PageState::Clean,
      buf,
      pin_count: 0,
    }
  }

  /// Get header 获取页头
  #[inline]
  pub fn header(&self) -> PageHeader {
    PageHeader::read(&self.buf)
  }

  /// Mark dirty 标记为脏
  #[inline]
  pub fn mark_dirty(&mut self) {
    self.state = PageState::Dirty;
  }

  /// Is dirty 是否脏
  #[inline]
  pub fn is_dirty(&self) -> bool {
    self.state == PageState::Dirty
  }

  /// Pin page 固定页面
  #[inline]
  pub fn pin(&mut self) {
    self.pin_count += 1;
  }

  /// Unpin page 取消固定
  #[inline]
  pub fn unpin(&mut self) {
    if self.pin_count > 0 {
      self.pin_count -= 1;
    }
  }

  /// Is pinned 是否被固定
  #[inline]
  pub fn is_pinned(&self) -> bool {
    self.pin_count > 0
  }
}
