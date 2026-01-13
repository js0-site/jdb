use std::rc::Rc;

use jdb_base::{Discard, sst::Sst};

use crate::{Disk, Map, disk::State};

/// Memory-resident part of the database with layered maps
/// 数据库的内存储存部分，具有分层映射
pub struct Mem<F, D>
where
  F: Sst,
  D: Discard + 'static,
{
  /// Current active map for writes
  /// 当前用于写入的活跃 Map
  pub now: Map,
  /// The newer frozen map (Level 1)
  /// 较新的冻结 Map (Level 1)
  pub freeze1: Option<Rc<Map>>,
  /// The older frozen map (Level 2), currently being flushed
  /// 较旧的冻结 Map (Level 2)，当前正在刷盘
  pub freeze2: Option<Rc<Map>>,
  /// Total size of keys and values in the current active map
  /// 当前活跃 Map 中键和值的总大小
  pub size: usize,
  /// Maximum size before rotating the current map to old
  /// 轮转当前 Map 之前的最大大小
  pub rotate_size: usize,

  /// Disk flush state manager
  /// 磁盘刷盘状态管理器
  pub(crate) state: State<F, D>,
}

impl<F, D> Mem<F, D>
where
  F: Sst,
  D: Discard,
{
  /// Create a new empty Mem with specified rotation size
  /// 创建一个新的空 Mem，并指定轮转大小
  #[inline]
  pub fn new(rotate_size: usize, sst: F, discard: D) -> Self {
    Self {
      now: Map::default(),
      freeze1: None,
      freeze2: None,
      size: 0,
      rotate_size,
      state: State::Idle(Disk::new(sst, discard)),
    }
  }

  /// Rotate current map to old maps and initialize a new one
  /// 将当前 Map 轮转到旧 Map 列表并初始化一个新的 Map
  #[cold]
  pub fn rotate(&mut self) {
    let now = std::mem::take(&mut self.now);
    let now = Rc::new(now);

    // Shift: freeze1 -> freeze2, now -> freeze1
    // 移位：freeze1 -> freeze2, now -> freeze1
    // Note: freeze2 should be None here due to backpressure in put(),
    // but if it's not (e.g. manual rotate call), we silently drop it (or overwrite it).
    // The strict logic in put() ensures we wait if freeze2 is some.
    // 注意：由于 put() 中的反压，此处 freeze2 应为 None，
    // 但如果不是（例如手动调用 rotate），我们会静默丢弃（或覆盖）它。
    // put() 中的严格逻辑确保如果 freeze2 存在则等待。

    // If freeze1 is None, it means we are filling up the first slot.
    // If freeze1 is Some, we move it to freeze2.
    // 如果 freeze1 为 None，意味着我们正在填充第一个槽位。
    // 如果 freeze1 为 Some，我们将它移动到 freeze2。
    if self.freeze1.is_some() {
      // Ensure we don't silently overwrite freeze2 (should be cleared by backpressure)
      // 确保我们不会静默覆盖 freeze2（应通过反压清除）
      debug_assert!(
        self.freeze2.is_none(),
        "freeze2 should be empty before rotate"
      );
      self.freeze2 = self.freeze1.take();
    }
    self.freeze1 = Some(now);

    self.size = 0;
    self.state.flush(&mut self.freeze2);
  }

  /// Helper: Wait for freeze2 to be flushed with consistency check and retry
  /// 辅助函数：等待 freeze2 刷盘，带一致性检查和重试
  pub(crate) async fn wait_freeze2(&mut self) -> Result<(), crate::Error<F::Error>> {
    while self.freeze2.is_some() {
      if let Err(e) = self.state.wait(&mut self.freeze2).await {
        if let crate::Error::Sst(_) = e {
          crate::log_err("flush freeze2 failed (retrying in 1s)", &e);
          compio::time::sleep(std::time::Duration::from_secs(1)).await;
          continue;
        }
        return Err(e);
      }
    }
    Ok(())
  }
}
