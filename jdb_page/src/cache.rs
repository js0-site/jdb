//! Buffer pool cache 缓冲池缓存

use crate::{Page, PageState};
use jdb_fs::File;
use std::collections::HashMap;

// Page ID type
pub type PageID = u32;

// Result type alias
pub type JdbResult<T> = std::result::Result<T, JdbError>;

// Error types
#[derive(Debug, thiserror::Error)]
pub enum JdbError {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),
  #[error("Filesystem error: {0}")]
  Fs(#[from] jdb_fs::Error),
  #[error("Allocation error: {0}")]
  Alloc(#[from] jdb_alloc::Error),
  #[error("Page size mismatch: expected {expected}, actual {actual}")]
  PageSizeMismatch { expected: usize, actual: usize },
}

// PageID helper functions
#[inline]
pub fn page_id(value: u32) -> PageID {
  value
}

/// Buffer pool 缓冲池
pub struct BufferPool {
  file: File,
  pages: HashMap<u32, Page>,
  cap: usize,
}

impl BufferPool {
  /// Create buffer pool 创建缓冲池
  pub fn new(file: File, cap: usize) -> Self {
    Self {
      file,
      pages: HashMap::with_capacity(cap),
      cap,
    }
  }

  /// Get page, load if not cached 获取页面，未缓存则加载
  pub async fn get(&mut self, id: PageID) -> JdbResult<&mut Page> {
    if !self.pages.contains_key(&id) {
      // Evict if full 满则驱逐
      if self.pages.len() >= self.cap {
        self.evict_one().await?;
      }

      // Load from disk 从磁盘加载
      let buf = self.file.read_page(id).await?;
      let page = Page::from_buf(id, buf);
      self.pages.insert(id, page);
    }

    Ok(self.pages.get_mut(&id).expect("just inserted"))
  }

  /// Allocate new page 分配新页面
  pub fn alloc(&mut self, id: PageID) -> JdbResult<&mut Page> {
    let page = Page::new(id)?;
    self.pages.insert(id, page);
    Ok(self.pages.get_mut(&id).expect("just inserted"))
  }

  /// Flush dirty page 刷新脏页
  pub async fn flush(&mut self, id: PageID) -> JdbResult<()> {
    if let Some(page) = self.pages.get_mut(&id) {
      if page.is_dirty() {
        let new_buf = jdb_alloc::AlignedBuf::page()?;
        let buf = std::mem::replace(&mut page.buf, new_buf);
        let buf = self.file.write_page(id, buf).await?;
        page.buf = buf;
        page.state = PageState::Clean;
      }
    }
    Ok(())
  }

  /// Flush all dirty pages 刷新所有脏页
  pub async fn flush_all(&mut self) -> JdbResult<()> {
    let dirty_ids: Vec<u32> = self
      .pages
      .iter()
      .filter(|(_, p)| p.is_dirty())
      .map(|(id, _)| *id)
      .collect();

    for id in dirty_ids {
      self.flush(page_id(id)).await?;
    }

    self.file.sync().await?;
    Ok(())
  }

  /// Evict one unpinned page 驱逐一个未固定的页面
  async fn evict_one(&mut self) -> JdbResult<()> {
    // Find unpinned page 找未固定的页面
    let victim = self
      .pages
      .iter()
      .find(|(_, p)| !p.is_pinned())
      .map(|(id, _)| *id);

    if let Some(id) = victim {
      // Flush if dirty 脏则刷新
      self.flush(page_id(id)).await?;
      self.pages.remove(&id);
    }

    Ok(())
  }

  /// Sync file 同步文件
  pub async fn sync(&mut self) -> JdbResult<()> {
    self.file.sync().await?;
    Ok(())
  }
}
