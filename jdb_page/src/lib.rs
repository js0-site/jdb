#![cfg_attr(docsrs, feature(doc_cfg))]

//! 缓冲区管理 Buffer manager

use std::collections::HashMap;

use jdb_alloc::AlignedBuf;
use jdb_comm::{PAGE_HEADER_SIZE, PAGE_SIZE, R};
use jdb_fs::File;
use jdb_layout::PageHeader;

/// 页面状态 Page state
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum State {
  Clean,
  Dirty,
}

/// 缓冲页 Buffer page
pub struct Page {
  id: u32,
  state: State,
  buf: AlignedBuf,
  pin: u32,
}

impl Page {
  /// 创建新页 Create new page
  pub fn new(id: u32) -> Self {
    let mut buf = AlignedBuf::zeroed(PAGE_SIZE);
    // 写入默认 header
    let header = PageHeader::new(id, 0);
    header.encode(&mut buf[..PAGE_HEADER_SIZE]);
    Self {
      id,
      state: State::Dirty,
      buf,
      pin: 0,
    }
  }

  /// 从缓冲区创建 Create from buffer
  pub fn from_buf(id: u32, buf: AlignedBuf) -> Self {
    Self {
      id,
      state: State::Clean,
      buf,
      pin: 0,
    }
  }

  #[inline]
  pub fn id(&self) -> u32 {
    self.id
  }

  /// 读取页头 Read page header
  #[inline]
  pub fn header(&self) -> PageHeader {
    PageHeader::decode(&self.buf[..PAGE_HEADER_SIZE])
  }

  /// 标记脏页 Mark dirty
  #[inline]
  pub fn mark_dirty(&mut self) {
    self.state = State::Dirty;
  }

  #[inline]
  pub fn is_dirty(&self) -> bool {
    self.state == State::Dirty
  }

  /// 固定页面 Pin page
  #[inline]
  pub fn pin(&mut self) {
    self.pin += 1;
  }

  /// 取消固定 Unpin page
  #[inline]
  pub fn unpin(&mut self) {
    if self.pin > 0 {
      self.pin -= 1;
    }
  }

  #[inline]
  pub fn is_pinned(&self) -> bool {
    self.pin > 0
  }

  /// 数据区 (跳过 header) Data area
  #[inline]
  pub fn data(&self) -> &[u8] {
    &self.buf[PAGE_HEADER_SIZE..]
  }

  /// 可变数据区 Mutable data area
  #[inline]
  pub fn data_mut(&mut self) -> &mut [u8] {
    &mut self.buf[PAGE_HEADER_SIZE..]
  }

  /// 原始缓冲区 Raw buffer
  #[inline]
  pub fn buf(&self) -> &AlignedBuf {
    &self.buf
  }

  /// 可变原始缓冲区 Mutable raw buffer
  #[inline]
  pub fn buf_mut(&mut self) -> &mut AlignedBuf {
    &mut self.buf
  }
}

/// 缓冲池 Buffer pool
pub struct Pool {
  file: File,
  pages: HashMap<u32, Page>,
  lru: Vec<u32>, // 简单 LRU: 尾部最近使用
  cap: usize,
  next_id: u32,
}

impl Pool {
  /// 打开缓冲池 Open buffer pool
  pub async fn open(file: File, cap: usize) -> R<Self> {
    let size = file.size().await?;
    let next_id = (size / PAGE_SIZE as u64) as u32;
    Ok(Self {
      file,
      pages: HashMap::with_capacity(cap),
      lru: Vec::with_capacity(cap),
      cap,
      next_id,
    })
  }

  /// 获取页面 Get page
  pub async fn get(&mut self, id: u32) -> R<&mut Page> {
    // 已在缓存中
    if self.pages.contains_key(&id) {
      self.touch(id);
      return Ok(self.pages.get_mut(&id).unwrap());
    }

    // 需要驱逐
    if self.pages.len() >= self.cap {
      self.evict().await?;
    }

    // 从磁盘读取
    let buf = self.file.read_page(id).await?;
    let page = Page::from_buf(id, buf);
    self.pages.insert(id, page);
    self.lru.push(id);

    Ok(self.pages.get_mut(&id).unwrap())
  }

  /// 分配新页 Allocate new page
  pub fn alloc(&mut self) -> R<&mut Page> {
    let id = self.next_id;
    self.next_id += 1;

    let page = Page::new(id);
    self.pages.insert(id, page);
    self.lru.push(id);

    Ok(self.pages.get_mut(&id).unwrap())
  }

  /// 刷新单页 Flush single page
  pub async fn flush(&mut self, id: u32) -> R<()> {
    if let Some(page) = self.pages.get_mut(&id) {
      if page.is_dirty() {
        let buf = std::mem::replace(&mut page.buf, AlignedBuf::zeroed(PAGE_SIZE));
        let buf = self.file.write_page(id, buf).await?;
        page.buf = buf;
        page.state = State::Clean;
      }
    }
    Ok(())
  }

  /// 刷新所有脏页 Flush all dirty pages
  pub async fn flush_all(&mut self) -> R<()> {
    let ids: Vec<u32> = self
      .pages
      .iter()
      .filter(|(_, p)| p.is_dirty())
      .map(|(&id, _)| id)
      .collect();

    for id in ids {
      self.flush(id).await?;
    }
    Ok(())
  }

  /// 同步到磁盘 Sync to disk
  pub async fn sync(&mut self) -> R<()> {
    self.flush_all().await?;
    self.file.sync().await?;
    Ok(())
  }

  /// 更新 LRU Update LRU
  fn touch(&mut self, id: u32) {
    if let Some(pos) = self.lru.iter().position(|&x| x == id) {
      self.lru.remove(pos);
      self.lru.push(id);
    }
  }

  /// 驱逐页面 Evict page
  async fn evict(&mut self) -> R<()> {
    // 找到第一个未固定的页面
    let mut evict_id = None;
    for &id in &self.lru {
      if let Some(page) = self.pages.get(&id) {
        if !page.is_pinned() {
          evict_id = Some(id);
          break;
        }
      }
    }

    if let Some(id) = evict_id {
      self.flush(id).await?;
      self.pages.remove(&id);
      self.lru.retain(|&x| x != id);
    }

    Ok(())
  }

  /// 下一个页面 ID Next page ID
  #[inline]
  pub fn next_id(&self) -> u32 {
    self.next_id
  }

  /// 缓存页数 Cached page count
  #[inline]
  pub fn len(&self) -> usize {
    self.pages.len()
  }

  /// 是否为空 Is empty
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.pages.is_empty()
  }
}
