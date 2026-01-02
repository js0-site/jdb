//! Write buffer with double buffering
//! 双缓冲写入缓冲区

use std::cell::Cell;

use compio_fs::File;

use crate::INFILE_MAX;

// Sleep duration for waiting (1ms)
// 等待时的休眠时间
pub(crate) const SLEEP_DUR: std::time::Duration = std::time::Duration::from_millis(1);

// Average entry size for buffer allocation
// 缓冲分配的平均条目大小
const AVG_ENTRY_SIZE: usize = 128;

// Max buffer size before shrinking
// 收缩前的最大缓冲区大小
const MAX_BUF_SIZE: usize = 2 * INFILE_MAX + 256;

/// Buffer slot with offset
/// 带偏移的缓冲槽
struct Buf {
  buf: Vec<u8>,
  offset: u64,
}

impl Buf {
  const fn new() -> Self {
    Self {
      buf: Vec::new(),
      offset: 0,
    }
  }

  #[inline(always)]
  fn is_empty(&self) -> bool {
    self.buf.is_empty()
  }

  #[inline(always)]
  fn len(&self) -> usize {
    self.buf.len()
  }

  /// Get raw pointer and length for reading (valid while slot not cleared)
  /// 获取原始指针和长度用于读取（槽未清空时有效）
  #[inline(always)]
  fn find(&self, pos: u64, len: usize) -> Option<(*const u8, usize)> {
    let buf_len = self.buf.len();
    if buf_len == 0 {
      return None;
    }
    let start = self.offset;
    let end = start + buf_len as u64;
    if pos >= start && pos < end {
      let off = (pos - start) as usize;
      let avail = buf_len - off;
      let actual = if len < avail { len } else { avail };
      Some((unsafe { self.buf.as_ptr().add(off) }, actual))
    } else {
      None
    }
  }

  /// Get buffer pointer and length for writing (data stays in place)
  /// 获取缓冲区指针和长度用于写入（数据保持原位）
  #[inline(always)]
  fn as_write_slice(&self) -> (*const u8, usize) {
    (self.buf.as_ptr(), self.buf.len())
  }

  /// Clear and reuse buffer
  /// 清空并复用缓冲区
  #[inline(always)]
  fn clear(&mut self) {
    self.buf.clear();
    if self.buf.capacity() > MAX_BUF_SIZE {
      self.buf.shrink_to(MAX_BUF_SIZE);
    }
    self.offset = 0;
  }
}

/// Double buffer write state
/// 双缓冲写入状态
struct WriteState {
  slots: [Buf; 2],
  /// Current write slot index (0 or 1)
  /// 当前写入槽索引
  cur: u8,
  /// Slot being written to disk (-1 = none)
  /// 正在写入磁盘的槽（-1 = 无）
  writing: i8,
  /// Writer task running
  /// 写入任务运行中
  task_running: bool,
}

/// Write slot info for writer task
/// 写入槽信息（用于写入任务）
pub(crate) struct WriteSlot {
  pub idx: u8,
  pub offset: u64,
  pub len: usize,
}

/// Shared state for write buffer
/// 写入缓冲区共享状态
pub(crate) struct SharedState {
  state: Cell<WriteState>,
  file: Cell<Option<File>>,
  buf_max: usize,
}

impl SharedState {
  pub fn new(cap: usize, buf_max: usize) -> Self {
    let mut slot0 = Buf::new();
    slot0.buf.reserve(cap * AVG_ENTRY_SIZE);
    Self {
      state: Cell::new(WriteState {
        slots: [slot0, Buf::new()],
        cur: 0,
        writing: -1,
        task_running: false,
      }),
      file: Cell::new(None),
      buf_max,
    }
  }

  #[inline(always)]
  #[allow(clippy::mut_from_ref)]
  fn state(&self) -> &mut WriteState {
    unsafe { &mut *self.state.as_ptr() }
  }

  #[inline(always)]
  #[allow(clippy::mut_from_ref)]
  pub fn file(&self) -> &mut Option<File> {
    unsafe { &mut *self.file.as_ptr() }
  }

  /// Push data to current slot
  /// 推送数据到当前槽
  #[inline(always)]
  pub fn push(&self, pos: u64, data: &[u8]) {
    let s = self.state();
    let slot = unsafe { s.slots.get_unchecked_mut(s.cur as usize) };
    if slot.buf.is_empty() {
      slot.offset = pos;
    }
    slot.buf.extend_from_slice(data);
  }

  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    let s = self.state();
    s.writing < 0
      && unsafe { s.slots.get_unchecked(0).is_empty() && s.slots.get_unchecked(1).is_empty() }
  }

  /// Current slot size
  /// 当前槽大小
  #[inline(always)]
  pub fn cur_len(&self) -> usize {
    let s = self.state();
    unsafe { s.slots.get_unchecked(s.cur as usize) }.buf.len()
  }

  #[inline(always)]
  pub fn buf_max(&self) -> usize {
    self.buf_max
  }

  #[inline(always)]
  pub fn is_task_running(&self) -> bool {
    self.state().task_running
  }

  #[inline(always)]
  pub fn set_task_running(&self, v: bool) {
    self.state().task_running = v;
  }

  /// Get slot to write, mark it as writing, switch cur
  /// 获取要写入的槽，标记为正在写入，切换 cur
  #[inline(always)]
  pub fn begin_write(&self) -> Option<WriteSlot> {
    let s = self.state();

    // Already writing
    // 已经在写入
    if s.writing >= 0 {
      return None;
    }

    let other = 1 - s.cur;
    let other_slot = unsafe { s.slots.get_unchecked(other as usize) };

    if !other_slot.is_empty() {
      // Other slot has data, write it
      // 另一个槽有数据，写入它
      s.writing = other as i8;
      s.cur = other;
      return Some(WriteSlot {
        idx: other,
        offset: other_slot.offset,
        len: other_slot.len(),
      });
    }

    // Other is empty, check current
    // 另一个为空，检查当前槽
    let cur_slot = unsafe { s.slots.get_unchecked(s.cur as usize) };
    if !cur_slot.is_empty() {
      // Current has data, write it and switch cur to other
      // 当前槽有数据，写入它并切换 cur 到另一个
      let cur = s.cur;
      s.writing = cur as i8;
      s.cur = other;
      return Some(WriteSlot {
        idx: cur,
        offset: cur_slot.offset,
        len: cur_slot.len(),
      });
    }

    // Both empty
    // 都为空
    s.task_running = false;
    None
  }

  /// Get buffer pointer for writing (data stays in slot for reading)
  /// 获取缓冲区指针用于写入（数据保留在槽中供读取）
  #[inline(always)]
  pub fn get_write_ptr(&self, idx: u8) -> (*const u8, usize) {
    let s = self.state();
    unsafe { s.slots.get_unchecked(idx as usize) }.as_write_slice()
  }

  /// End write, clear the slot
  /// 结束写入，清空槽
  #[inline(always)]
  pub fn end_write(&self, idx: u8) {
    let s = self.state();
    let slot = unsafe { s.slots.get_unchecked_mut(idx as usize) };
    slot.clear();
    s.writing = -1;
  }

  /// Find data by file position (returns raw pointer, valid until end_write)
  /// 根据文件位置查找数据（返回原始指针，在 end_write 前有效）
  #[inline(always)]
  pub fn find_by_pos(&self, pos: u64, len: usize) -> Option<(*const u8, usize)> {
    let s = self.state();
    // Check writing slot first (more likely for recent reads)
    // 先检查正在写入的槽（最近读取更可能命中）
    let writing = s.writing;
    if writing >= 0
      && let Some(r) = unsafe { s.slots.get_unchecked(writing as usize) }.find(pos, len)
    {
      return Some(r);
    }
    // Then check current slot
    // 然后检查当前槽
    unsafe { s.slots.get_unchecked(s.cur as usize) }.find(pos, len)
  }
}
