mod run;
mod state;

use jdb_base::{Discard, sst::Sst};
pub use state::State;

/// Disk handler for SST and Discard operations
/// SST 和 Discard 操作的磁盘处理器
pub struct Disk<S, D> {
  /// SST writer implementation
  /// SST 写入器实现
  pub sst: S,
  /// Discard manager implementation
  /// 丢弃管理器实现
  pub discard: D,
}

impl<S: Sst, D: Discard> Disk<S, D> {
  /// Create a new Disk handler
  /// 创建新的磁盘处理器
  pub const fn new(sst: S, discard: D) -> Self {
    Self { sst, discard }
  }
}
