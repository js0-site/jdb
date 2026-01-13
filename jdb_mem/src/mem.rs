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
  /// The frozen map, currently being flushed
  /// 冻结 Map，当前正在刷盘
  pub freeze: Option<Rc<Map>>,
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
      freeze: None,
      size: 0,
      rotate_size,
      state: State::new(Disk::new(sst, discard)),
    }
  }

  /// Rotate current map to frozen map and trigger flush
  /// 将当前 Map 轮转为冻结 Map 并触发刷盘
  #[cold]
  pub fn rotate(&mut self) -> Result<(), crate::Error<F::Error>> {
    let now = std::mem::take(&mut self.now);
    let now = Rc::new(now);

    debug_assert!(
      self.freeze.is_none(),
      "freeze should be empty before rotate"
    );
    self.freeze = Some(now);

    self.size = 0;
    self.state.flush(&mut self.freeze)
  }

  /// Helper: Wait for freeze to be flushed with consistency check and retry
  /// 辅助函数：等待 freeze 刷盘，带一致性检查和重试
  #[cold]
  pub(crate) async fn wait_freeze(&mut self) -> Result<(), crate::Error<F::Error>> {
    while self.freeze.is_some() {
      if let Err(e) = self.state.wait(&mut self.freeze).await {
        if let crate::Error::Sst(_) = e {
          crate::log_err("flush freeze failed (retrying in 1s)", &e);
          compio::time::sleep(std::time::Duration::from_secs(1)).await;
          continue;
        }
        return Err(e);
      }
    }
    Ok(())
  }
}
