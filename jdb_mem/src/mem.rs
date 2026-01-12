use crate::Map;

/// Memory-resident part of the database with layered maps
/// 数据库的内存储存部分，具有分层映射
#[derive(Debug)]
pub struct Mem {
  /// Current active map for writes
  /// 当前用于写入的活跃 Map
  pub now: Map,
  /// Immutable/older maps pending flush
  /// 等待刷盘的不可变/旧 Map
  pub old: Vec<Map>,
  /// Total size of keys and values in the current active map
  /// 当前活跃 Map 中键和值的总大小
  pub size: usize,
  /// Maximum size before rotating the current map to old
  /// 轮转当前 Map 之前的最大大小
  pub rotate_size: usize,
}

impl Mem {
  /// Create a new empty Mem with specified rotation size
  /// 创建一个新的空 Mem，并指定轮转大小
  #[inline]
  pub fn new(rotate_size: usize) -> Self {
    Self {
      now: Map::default(),
      old: Vec::new(),
      size: 0,
      rotate_size,
    }
  }

  /// Rotate current map to old maps and initialize a new one
  /// 将当前 Map 轮转到旧 Map 列表并初始化一个新的 Map
  pub fn rotate(&mut self) {
    let now = std::mem::take(&mut self.now);
    self.old.push(now);
    self.size = 0;
  }
}
