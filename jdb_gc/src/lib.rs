//! Garbage Collection for Page and VLog
//! 页和值日志的垃圾回收

#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;
mod stat;
mod worker;

use std::{
  cell::RefCell,
  collections::{HashMap, HashSet},
  rc::Rc,
};

pub use error::{Error, Result};
use jdb_table::Keep;
use jdb_trait::ValRef;
use roaring::RoaringBitmap;
pub use stat::{FileStat, GcStats};
pub use worker::{GcState, GcWorker};

/// GC configuration / GC 配置
#[derive(Debug, Clone, Copy)]
pub struct GcConf {
  /// Garbage ratio threshold for compaction / 压缩的垃圾比例阈值
  pub compact_threshold: f64,
  /// Batch size for marking phase / 标记阶段的批大小
  pub mark_batch: usize,
  /// GC interval in seconds / GC 间隔（秒）
  pub interval_secs: u64,
  /// VLog size threshold to trigger GC (bytes) / 触发 GC 的 VLog 大小阈值（字节）
  pub gc_threshold_bytes: u64,
}

/// 1GB default threshold / 默认 1GB 阈值
const DEFAULT_GC_THRESHOLD_BYTES: u64 = 1024 * 1024 * 1024;

impl Default for GcConf {
  fn default() -> Self {
    Self {
      compact_threshold: 0.5,
      mark_batch: 1000,
      interval_secs: 60,
      gc_threshold_bytes: DEFAULT_GC_THRESHOLD_BYTES,
    }
  }
}

/// Live record tracker / 存活记录追踪器
pub struct LiveTracker {
  /// file_id -> live offsets (offset / PAGE_SIZE)
  live: HashMap<u64, RoaringBitmap>,
  /// Live bin ids / 存活 bin ID
  live_bins: HashSet<u64>,
}

impl LiveTracker {
  pub fn new() -> Self {
    Self {
      live: HashMap::new(),
      live_bins: HashSet::new(),
    }
  }

  /// Mark ValRef as live / 标记 ValRef 为存活
  #[inline]
  pub fn mark(&mut self, vref: &ValRef) {
    let offset_idx = (vref.real_offset() / 4096) as u32;
    self
      .live
      .entry(vref.file_id)
      .or_default()
      .insert(offset_idx);
  }

  /// Mark history chain with Keep policy / 根据 Keep 策略标记历史链
  pub fn mark_history(&mut self, history: &[ValRef], keep: Keep, now_ms: u64, timestamps: &[u64]) {
    for (idx, vref) in history.iter().enumerate() {
      let age_ms = timestamps
        .get(idx)
        .map(|&ts| now_ms.saturating_sub(ts * 1000));
      if keep.should_keep(idx, age_ms) {
        self.mark(vref);
      } else {
        break;
      }
    }
  }

  /// Mark bin as live / 标记 bin 为存活
  #[inline]
  pub fn mark_bin(&mut self, bin_id: u64) {
    self.live_bins.insert(bin_id);
  }

  /// Check if bin is live / 检查 bin 是否存活
  #[inline]
  pub fn is_bin_live(&self, bin_id: u64) -> bool {
    self.live_bins.contains(&bin_id)
  }

  /// Check if record is live / 检查记录是否存活
  #[inline]
  pub fn is_live(&self, file_id: u64, offset: u64) -> bool {
    let offset_idx = (offset / 4096) as u32;
    self
      .live
      .get(&file_id)
      .is_some_and(|bm| bm.contains(offset_idx))
  }

  /// Get live count in file / 获取文件中的存活数
  pub fn live_count(&self, file_id: u64) -> u64 {
    self.live.get(&file_id).map(|bm| bm.len()).unwrap_or(0)
  }

  /// Get all tracked file ids / 获取所有追踪的文件 ID
  pub fn file_ids(&self) -> Vec<u64> {
    self.live.keys().copied().collect()
  }

  /// Merge another tracker / 合并另一个追踪器
  pub fn merge(&mut self, other: &LiveTracker) {
    for (&file_id, bitmap) in &other.live {
      self.live.entry(file_id).or_default().bitor_assign(bitmap);
    }
    self.live_bins.extend(&other.live_bins);
  }

  /// Clear all / 清空
  pub fn clear(&mut self) {
    self.live.clear();
    self.live_bins.clear();
  }

  /// Memory usage / 内存使用
  pub fn mem_size(&self) -> usize {
    self.live.values().map(|bm| bm.serialized_size()).sum()
  }
}

impl Default for LiveTracker {
  fn default() -> Self {
    Self::new()
  }
}

/// GC handle for background task control / 后台任务控制句柄
pub struct GcHandle {
  /// Stop signal / 停止信号
  stop: Rc<RefCell<bool>>,
}

impl GcHandle {
  pub fn new() -> Self {
    Self {
      stop: Rc::new(RefCell::new(false)),
    }
  }

  /// Get stop flag reference / 获取停止标志引用
  pub fn stop_flag(&self) -> Rc<RefCell<bool>> {
    Rc::clone(&self.stop)
  }

  /// Signal stop / 发送停止信号
  pub fn stop(&self) {
    *self.stop.borrow_mut() = true;
  }

  /// Check if stopped / 检查是否已停止
  pub fn is_stopped(&self) -> bool {
    *self.stop.borrow()
  }
}

impl Default for GcHandle {
  fn default() -> Self {
    Self::new()
  }
}

use std::ops::BitOrAssign;
