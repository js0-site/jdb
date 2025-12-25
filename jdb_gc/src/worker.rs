//! GC worker / GC 工作器

use bytes::Bytes;

use crate::{FileStat, GcConf, GcStats, LiveTracker};

/// GC state machine / GC 状态机
#[derive(Debug, Clone, Default)]
pub enum GcState {
  /// Idle, waiting for trigger / 空闲，等待触发
  #[default]
  Idle,
  /// Marking phase / 标记阶段
  Marking {
    /// Current table index / 当前表索引
    table_idx: usize,
    /// Key cursor for resume / 恢复用的键游标
    key_cursor: Option<Bytes>,
  },
  /// Sweeping phase / 清扫阶段
  Sweeping {
    /// Current file index / 当前文件索引
    file_idx: usize,
  },
  /// Completed / 已完成
  Done,
}

/// GC worker for incremental garbage collection
/// 增量垃圾回收工作器
pub struct GcWorker {
  /// Current state / 当前状态
  state: GcState,
  /// Configuration / 配置
  conf: GcConf,
  /// Live record tracker / 存活记录追踪器
  live: LiveTracker,
  /// File statistics / 文件统计
  file_stats: Vec<FileStat>,
  /// Result statistics / 结果统计
  stats: GcStats,
}

impl GcWorker {
  /// Create new worker / 创建新工作器
  pub fn new() -> Self {
    Self {
      state: GcState::Idle,
      conf: GcConf::default(),
      live: LiveTracker::new(),
      file_stats: Vec::new(),
      stats: GcStats::default(),
    }
  }

  /// Create with config / 带配置创建
  pub fn with_conf(conf: GcConf) -> Self {
    Self {
      state: GcState::Idle,
      conf,
      live: LiveTracker::new(),
      file_stats: Vec::new(),
      stats: GcStats::default(),
    }
  }

  /// Get current state / 获取当前状态
  pub fn state(&self) -> &GcState {
    &self.state
  }

  /// Get statistics / 获取统计
  pub fn stats(&self) -> &GcStats {
    &self.stats
  }

  /// Get configuration / 获取配置
  pub fn conf(&self) -> &GcConf {
    &self.conf
  }

  /// Start GC cycle / 开始 GC 周期
  pub fn start(&mut self) {
    self.state = GcState::Marking {
      table_idx: 0,
      key_cursor: None,
    };
    self.live.clear();
    self.file_stats.clear();
    self.stats = GcStats::default();
  }

  /// Check if done / 检查是否完成
  pub fn is_done(&self) -> bool {
    matches!(self.state, GcState::Done)
  }

  /// Check if idle / 检查是否空闲
  pub fn is_idle(&self) -> bool {
    matches!(self.state, GcState::Idle)
  }

  /// Reset to idle / 重置为空闲
  pub fn reset(&mut self) {
    self.state = GcState::Idle;
    self.live.clear();
    self.file_stats.clear();
  }

  /// Get live tracker / 获取存活追踪器
  pub fn live_tracker(&self) -> &LiveTracker {
    &self.live
  }

  /// Get mutable live tracker / 获取可变存活追踪器
  pub fn live_tracker_mut(&mut self) -> &mut LiveTracker {
    &mut self.live
  }

  /// Get file stats / 获取文件统计
  pub fn file_stats(&self) -> &[FileStat] {
    &self.file_stats
  }

  /// Set file stats / 设置文件统计
  pub fn set_file_stats(&mut self, stats: Vec<FileStat>) {
    self.file_stats = stats;
  }

  /// Transition to sweeping / 转换到清扫阶段
  pub fn start_sweep(&mut self) {
    self.state = GcState::Sweeping { file_idx: 0 };
  }

  /// Mark as done / 标记为完成
  pub fn finish(&mut self) {
    self.state = GcState::Done;
  }

  /// Update marking state / 更新标记状态
  pub fn update_marking(&mut self, table_idx: usize, key_cursor: Option<Bytes>) {
    self.state = GcState::Marking {
      table_idx,
      key_cursor,
    };
  }

  /// Update sweeping state / 更新清扫状态
  pub fn update_sweeping(&mut self, file_idx: usize) {
    self.state = GcState::Sweeping { file_idx };
  }

  /// Add to stats / 添加统计
  pub fn add_stats(&mut self, delta: &GcStats) {
    self.stats.merge(delta);
  }

  /// Increment keys scanned / 增加扫描键数
  pub fn inc_keys(&mut self, n: u64) {
    self.stats.keys_scanned += n;
  }

  /// Increment tables scanned / 增加扫描表数
  pub fn inc_tables(&mut self) {
    self.stats.tables_scanned += 1;
  }

  /// Increment files deleted / 增加删除文件数
  pub fn inc_files_deleted(&mut self, bytes: u64) {
    self.stats.files_deleted += 1;
    self.stats.bytes_reclaimed += bytes;
  }

  /// Increment files compacted / 增加压缩文件数
  pub fn inc_files_compacted(&mut self) {
    self.stats.files_compacted += 1;
  }
}

impl Default for GcWorker {
  fn default() -> Self {
    Self::new()
  }
}
