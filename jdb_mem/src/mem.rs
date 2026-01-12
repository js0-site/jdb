use std::{cell::RefCell, rc::Rc};

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
  /// Immutable/older maps pending sst
  /// 等待刷盘的不可变/旧 Map
  pub old: [Option<Rc<Map>>; 2],
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
      old: [None, None],
      size: 0,
      rotate_size,
      state: State::Idle(Rc::new(RefCell::new(Disk::new(sst, discard)))),
    }
  }

  /// Rotate current map to old maps and initialize a new one
  /// 将当前 Map 轮转到旧 Map 列表并初始化一个新的 Map
  #[cold]
  pub fn rotate(&mut self) {
    let now = std::mem::take(&mut self.now);
    let now = Rc::new(now);
    if self.old[0].is_none() {
      self.old[0] = Some(now);
    } else {
      self.old[1] = Some(now);
    }
    self.size = 0;
    self.state.flush(&mut self.old);
  }
}
