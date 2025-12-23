#![cfg_attr(docsrs, feature(doc_cfg))]

//! 高性能缓冲池 High-performance buffer pool
//!
//! Frame-based 设计，CLOCK 驱逐，无锁 I/O
//! Frame-based design, CLOCK eviction, lock-free I/O

mod cache;
mod consts;
mod error;
mod page;

pub use page::{Page, PageState};

use std::sync::{
  Arc,
  atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
  Mutex,
};

use jdb_alloc::AlignedBuf;
use jdb_fs::File;
use jdb_layout::PageHeader;
use papaya::HashMap;
use parking_lot::RwLock;

use crate::consts::{
  INVALID_PAGE, PAGE_HEADER_SIZE, PAGE_SIZE, PIN_MASK, USAGE_BIT, DIRTY_BIT, VALID_BIT,
};
use crate::error::Result;

type FrameID = usize;

/// 物理帧 Physical frame
struct Frame {
  /// 保护页面 ID 变化和 I/O Protects page_id changes and I/O
  latch: RwLock<()>,
  /// 当前页面 ID Current page ID
  page_id: AtomicU32,
  /// 压缩状态位 Packed state bits
  state: AtomicU64,
  /// 数据缓冲区 Data buffer
  buf: RwLock<AlignedBuf>,
}

impl Frame {
  fn new() -> Self {
    Self {
      latch: RwLock::new(()),
      page_id: AtomicU32::new(INVALID_PAGE),
      state: AtomicU64::new(0),
      buf: RwLock::new(AlignedBuf::zeroed(PAGE_SIZE).unwrap()),
    }
  }

  #[inline]
  fn pin_count(state: u64) -> u64 {
    state & PIN_MASK
  }

  #[inline]
  fn is_dirty(state: u64) -> bool {
    state & DIRTY_BIT != 0
  }

  #[inline]
  fn is_valid(state: u64) -> bool {
    state & VALID_BIT != 0
  }

  /// 尝试 pin，成功返回 true Try to pin, return true on success
  #[inline]
  fn try_pin(&self) -> bool {
    let old = self.state.fetch_add(1 | USAGE_BIT, Ordering::AcqRel);
    Self::pin_count(old) < PIN_MASK
  }

  /// 取消 pin Unpin
  #[inline]
  fn unpin(&self) {
    self.state.fetch_sub(1, Ordering::Release);
  }

  /// 标记脏 Mark dirty
  #[inline]
  fn mark_dirty(&self) {
    self.state.fetch_or(DIRTY_BIT, Ordering::Release);
  }

  /// 清除脏位 Clear dirty bit
  #[inline]
  fn clear_dirty(&self) {
    self.state.fetch_and(!DIRTY_BIT, Ordering::Release);
  }

  /// 清除 usage 位 Clear usage bit
  #[inline]
  fn clear_usage(&self) -> bool {
    let old = self.state.fetch_and(!USAGE_BIT, Ordering::AcqRel);
    old & USAGE_BIT != 0
  }

  /// 重置帧 Reset frame
  fn reset(&self, page_id: u32) {
    self.page_id.store(page_id, Ordering::Release);
    self.state.store(VALID_BIT | USAGE_BIT | 1, Ordering::Release); // pin=1, usage=1, valid=1
  }
}

/// 页面守卫 Page guard (RAII)
pub struct PageGuard {
  pool: Arc<Pool>,
  frame_idx: FrameID,
  page_id: u32,
}

impl PageGuard {
  #[inline]
  pub fn id(&self) -> u32 {
    self.page_id
  }

  /// 只读访问 Read-only access
  #[inline]
  pub fn with_read<F, T>(&self, f: F) -> T
  where
    F: FnOnce(&[u8]) -> T,
  {
    let frame = &self.pool.frames[self.frame_idx];
    let buf = frame.buf.read();
    f(&buf[PAGE_HEADER_SIZE..])
  }

  /// 可变访问 Mutable access
  #[inline]
  pub fn with_write<F, T>(&self, f: F) -> T
  where
    F: FnOnce(&mut [u8]) -> T,
  {
    let frame = &self.pool.frames[self.frame_idx];
    frame.mark_dirty();
    let mut buf = frame.buf.write();
    f(&mut buf[PAGE_HEADER_SIZE..])
  }

  /// 数据区只读 Data area read-only
  #[inline]
  pub fn data(&self) -> parking_lot::MappedRwLockReadGuard<'_, [u8]> {
    let frame = &self.pool.frames[self.frame_idx];
    parking_lot::RwLockReadGuard::map(frame.buf.read(), |b| &b[PAGE_HEADER_SIZE..])
  }

  /// 数据区可变 Data area mutable
  #[inline]
  pub fn data_mut(&self) -> parking_lot::MappedRwLockWriteGuard<'_, [u8]> {
    let frame = &self.pool.frames[self.frame_idx];
    frame.mark_dirty();
    parking_lot::RwLockWriteGuard::map(frame.buf.write(), |b| &mut b[PAGE_HEADER_SIZE..])
  }

  /// 原始缓冲区 Raw buffer
  #[inline]
  pub fn buf(&self) -> parking_lot::RwLockReadGuard<'_, AlignedBuf> {
    self.pool.frames[self.frame_idx].buf.read()
  }

  /// 可变原始缓冲区 Mutable raw buffer
  #[inline]
  pub fn buf_mut(&self) -> parking_lot::RwLockWriteGuard<'_, AlignedBuf> {
    let frame = &self.pool.frames[self.frame_idx];
    frame.mark_dirty();
    frame.buf.write()
  }

  /// 是否脏页 Is dirty
  #[inline]
  pub fn is_dirty(&self) -> bool {
    let state = self.pool.frames[self.frame_idx].state.load(Ordering::Acquire);
    Frame::is_dirty(state)
  }

  /// 固定页面 Pin page
  #[inline]
  pub fn pin(&self) {
    self.pool.frames[self.frame_idx].try_pin();
  }

  /// 取消固定 Unpin page
  #[inline]
  pub fn unpin(&self) {
    self.pool.frames[self.frame_idx].unpin();
  }

  /// 页头 Page header
  #[inline]
  pub fn header(&self) -> PageHeader {
    let buf = self.pool.frames[self.frame_idx].buf.read();
    PageHeader::decode(&buf[..PAGE_HEADER_SIZE])
  }
}

impl Drop for PageGuard {
  fn drop(&mut self) {
    self.pool.frames[self.frame_idx].unpin();
  }
}

impl Clone for PageGuard {
  fn clone(&self) -> Self {
    self.pool.frames[self.frame_idx].try_pin();
    Self {
      pool: Arc::clone(&self.pool),
      frame_idx: self.frame_idx,
      page_id: self.page_id,
    }
  }
}

/// 高性能缓冲池 High-performance buffer pool
pub struct Pool {
  /// 文件句柄（支持并发 pread/pwrite）File handle (concurrent pread/pwrite)
  file: Arc<Mutex<File>>,
  /// 物理帧数组 Physical frame array
  frames: Vec<Frame>,
  /// 页表 Page table: PageID -> FrameID
  page_table: HashMap<u32, FrameID>,
  /// CLOCK 指针 CLOCK hand
  clock_hand: AtomicUsize,
  /// 下一个页面 ID Next page ID
  next_id: AtomicU32,
  /// 容量 Capacity
  cap: usize,
}

impl Pool {
  /// 打开缓冲池 Open buffer pool
  pub async fn open(file: File, cap: usize) -> Result<Arc<Self>> {
    let size = file.size().await?;
    let next_id = (size / PAGE_SIZE as u64) as u32;

    // 预分配所有帧 Pre-allocate all frames
    let mut frames = Vec::with_capacity(cap);
    for _ in 0..cap {
      frames.push(Frame::new());
    }

    Ok(Arc::new(Self {
      file: Arc::new(Mutex::new(file)),
      frames,
      page_table: HashMap::new(),
      clock_hand: AtomicUsize::new(0),
      next_id: AtomicU32::new(next_id),
      cap,
    }))
  }

  /// 获取页面 Get page
  pub async fn get(self: &Arc<Self>, id: u32) -> Result<PageGuard> {
    loop {
      // 快速路径：检查页表 Fast path: check page table
      let frame_idx = {
        let guard = self.page_table.pin();
        guard.get(&id).copied()
      };

      if let Some(f_idx) = frame_idx {
        let frame = &self.frames[f_idx];

        // 尝试 pin Try to pin
        if frame.try_pin() {
          // 验证一致性 Verify consistency
          let state = frame.state.load(Ordering::Acquire);
          if frame.page_id.load(Ordering::Acquire) == id && Frame::is_valid(state) {
            return Ok(PageGuard {
              pool: Arc::clone(self),
              frame_idx: f_idx,
              page_id: id,
            });
          }
          // 失败回滚 Rollback on failure
          frame.unpin();
        }
        // 重试 Retry
        continue;
      }

      // 慢路径：加载页面 Slow path: load page
      return self.load_page(id).await;
    }
  }

  /// 加载页面 Load page from disk
  async fn load_page(self: &Arc<Self>, id: u32) -> Result<PageGuard> {
    // 双重检查 Double check
    {
      let guard = self.page_table.pin();
      if let Some(&f_idx) = guard.get(&id) {
        let frame = &self.frames[f_idx];
        if frame.try_pin() {
          let state = frame.state.load(Ordering::Acquire);
          if frame.page_id.load(Ordering::Acquire) == id && Frame::is_valid(state) {
            return Ok(PageGuard {
              pool: Arc::clone(self),
              frame_idx: f_idx,
              page_id: id,
            });
          }
          frame.unpin();
        }
      }
    }

    // 找空闲帧或驱逐 Find free frame or evict
    let f_idx = self.find_victim().await?;
    let frame = &self.frames[f_idx];

    // 获取 latch 独占 Acquire exclusive latch
    let _latch = frame.latch.write();

    // 读取磁盘 Read from disk
    let buf = self.file.lock().unwrap().read_page(id).await?;
    *frame.buf.write() = buf;
    frame.reset(id);

    // 更新页表 Update page table
    self.page_table.pin().insert(id, f_idx);

    Ok(PageGuard {
      pool: Arc::clone(self),
      frame_idx: f_idx,
      page_id: id,
    })
  }

  /// CLOCK 算法找牺牲帧 CLOCK algorithm to find victim frame
  async fn find_victim(&self) -> Result<FrameID> {
    let mut attempts = 0;
    let max_attempts = self.cap * 2;

    loop {
      let hand = self.clock_hand.fetch_add(1, Ordering::Relaxed) % self.cap;
      let frame = &self.frames[hand];

      let state = frame.state.load(Ordering::Acquire);

      // 未使用的帧 Unused frame
      if !Frame::is_valid(state) {
        return Ok(hand);
      }

      // 无 pin 且无 usage Not pinned and no usage
      if Frame::pin_count(state) == 0 {
        if !frame.clear_usage() {
          // usage 已清除，可驱逐 Usage cleared, can evict
          let _latch = frame.latch.write();

          // 再次检查 Double check
          let state = frame.state.load(Ordering::Acquire);
          if Frame::pin_count(state) == 0 {
            // 刷脏 Flush if dirty
            if Frame::is_dirty(state) {
              let page_id = frame.page_id.load(Ordering::Acquire);
              let buf = frame.buf.read();
              self.file.lock().unwrap().write_page(page_id, buf.clone()).await?;
              frame.clear_dirty();
            }

            // 从页表移除 Remove from page table
            let old_id = frame.page_id.load(Ordering::Acquire);
            if old_id != INVALID_PAGE {
              self.page_table.pin().remove(&old_id);
            }

            // 重置状态 Reset state
            frame.state.store(0, Ordering::Release);
            return Ok(hand);
          }
        }
      }

      attempts += 1;
      if attempts >= max_attempts {
        // 所有帧都被 pin，强制等待 All frames pinned, force wait
        // Simple yield
        compio::time::sleep(std::time::Duration::from_millis(1)).await;
        attempts = 0;
      }
    }
  }

  /// 分配新页 Allocate new page
  pub fn alloc(self: &Arc<Self>) -> Result<PageGuard> {
    let id = self.next_id.fetch_add(1, Ordering::Relaxed);

    // 找空闲帧 Find free frame
    for (f_idx, frame) in self.frames.iter().enumerate() {
      let state = frame.state.load(Ordering::Acquire);
      if !Frame::is_valid(state) {
        let _latch = frame.latch.write();

        // 再次检查 Double check
        let state = frame.state.load(Ordering::Acquire);
        if !Frame::is_valid(state) {
          // 初始化新页 Initialize new page
          let mut buf = frame.buf.write();
          buf.fill(0);
          let header = PageHeader::new(id, 0);
          header.encode(&mut buf[..PAGE_HEADER_SIZE]);
          drop(buf);

          frame.reset(id);
          frame.mark_dirty();
          self.page_table.pin().insert(id, f_idx);

          return Ok(PageGuard {
            pool: Arc::clone(self),
            frame_idx: f_idx,
            page_id: id,
          });
        }
      }
    }

    // 需要驱逐 Need eviction
    // 同步版本：简单遍历找未 pin 的 Sync version: simple scan for unpinned
    for (f_idx, frame) in self.frames.iter().enumerate() {
      let state = frame.state.load(Ordering::Acquire);
      if Frame::pin_count(state) == 0 && !frame.clear_usage() {
        let _latch = frame.latch.write();

        let state = frame.state.load(Ordering::Acquire);
        if Frame::pin_count(state) == 0 {
          // 从页表移除旧页 Remove old page from table
          let old_id = frame.page_id.load(Ordering::Acquire);
          if old_id != INVALID_PAGE {
            self.page_table.pin().remove(&old_id);
          }

          // 初始化新页 Initialize new page
          let mut buf = frame.buf.write();
          buf.fill(0);
          let header = PageHeader::new(id, 0);
          header.encode(&mut buf[..PAGE_HEADER_SIZE]);
          drop(buf);

          frame.reset(id);
          frame.mark_dirty();
          self.page_table.pin().insert(id, f_idx);

          return Ok(PageGuard {
            pool: Arc::clone(self),
            frame_idx: f_idx,
            page_id: id,
          });
        }
      }
    }

    // 所有帧都被 pin All frames pinned
    // 回滚 ID Rollback ID
    self.next_id.fetch_sub(1, Ordering::Relaxed);
    Err("all frames pinned".into())
  }

  /// 刷新单页 Flush single page
  pub async fn flush(&self, id: u32) -> Result<()> {
    let guard = self.page_table.pin();
    if let Some(&f_idx) = guard.get(&id) {
      let frame = &self.frames[f_idx];
      let state = frame.state.load(Ordering::Acquire);

      if Frame::is_dirty(state) {
        let _latch = frame.latch.read();
        let buf = frame.buf.read();
        self.file.lock().unwrap().write_page(id, buf.clone()).await?;
        frame.clear_dirty();
      }
    }
    Ok(())
  }

  /// 刷新所有脏页 Flush all dirty pages
  pub async fn flush_all(&self) -> Result<()> {
    for frame in &self.frames {
      let state = frame.state.load(Ordering::Acquire);
      if Frame::is_valid(state) && Frame::is_dirty(state) {
        let _latch = frame.latch.read();
        let page_id = frame.page_id.load(Ordering::Acquire);
        let buf = frame.buf.read();
        self.file.lock().unwrap().write_page(page_id, buf.clone()).await?;
        frame.clear_dirty();
      }
    }
    Ok(())
  }

  /// 同步到磁盘 Sync to disk
  pub async fn sync(&self) -> Result<()> {
    self.flush_all().await?;
    self.file.lock().unwrap().sync().await?;
    Ok(())
  }

  #[inline]
  pub fn next_id(&self) -> u32 {
    self.next_id.load(Ordering::Relaxed)
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.page_table.pin().len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.page_table.pin().is_empty()
  }
}
