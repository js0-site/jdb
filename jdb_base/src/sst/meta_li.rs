use sorted_vec::SortedVec;

use crate::sst::{CkpOp, Meta};

/// Meta list managing sorted SST metadatas
/// 管理有序 SST 元数据的列表
#[derive(Debug, Default)]
pub struct MetaLi([SortedVec<Meta>; 7]);

impl MetaLi {
  /// Create new MetaLi
  /// 创建新 MetaLi
  pub fn new() -> Self {
    Self::default()
  }

  /// Get reference to inner sorted vec for a specific level
  /// 获取指定层级的内部有序 Vec 的引用
  pub fn get(&self, level: usize) -> &SortedVec<Meta> {
    &self.0[level]
  }

  /// Update state with operation (apply only to memory)
  /// 使用操作更新状态（仅应用到内存）
  pub fn update(&mut self, op: &CkpOp) {
    let inner = &mut self.0;

    match op {
      CkpOp::Flush { meta } => {
        if let Some(vec) = inner.get_mut(meta.level as usize) {
          vec.push(meta.clone());
        }
      }
      CkpOp::Compact { adds, rms } => {
        // 1. Remove rms from all levels (IDs are unique)
        // 1. 从所有层级移除 rms (ID 唯一)
        if !rms.is_empty() {
          for level_vec in inner.iter_mut() {
            level_vec.retain(|m| !rms.contains(&m.id));
          }
        }

        // 2. Add adds to respective levels
        // 2. 将 adds 添加到对应层级
        for add in adds {
          if let Some(vec) = inner.get_mut(add.level as usize) {
            vec.push(add.clone());
          }
        }
      }
    }
  }
}
