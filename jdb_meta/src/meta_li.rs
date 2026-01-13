use std::{cell::Cell, path::PathBuf, rc::Rc};

use jdb_base::ckp::Op;
use sorted_vec::SortedVec;

use crate::MetaWithAutoRm;

/// Meta list managing sorted SST metadatas
/// 管理有序 SST 元数据的列表
#[derive(Debug)]
pub struct MetaLi {
  levels: [SortedVec<Rc<MetaWithAutoRm>>; 7],
  pub dir: Rc<PathBuf>,
}

impl MetaLi {
  /// Create new MetaLi
  /// 创建新 MetaLi
  pub fn new(dir: Rc<PathBuf>) -> Self {
    Self {
      levels: Default::default(),
      dir,
    }
  }

  /// Get reference to inner sorted vec for a specific level
  /// 获取指定层级的内部有序 Vec 的引用
  pub fn get(&self, level: usize) -> &SortedVec<Rc<MetaWithAutoRm>> {
    &self.levels[level]
  }

  /// Get overlapping Metas in a specific level
  /// 获取指定层级中重叠的 Meta
  pub fn overlap<R>(&self, level: usize, range: R) -> Vec<Rc<MetaWithAutoRm>>
  where
    R: std::ops::RangeBounds<[u8]>,
  {
    use std::ops::Bound::*;

    let mut res = Vec::new();

    if let Some(vec) = self.levels.get(level) {
      if level == 0 {
        // L0 is unordered (on max key), must scan from start
        // L0 (Max Key) 无序，必须从头扫描
        // Tail optimization still valid because min key ensures we can stop early
        // 尾部优化仍然有效，因为 min key 保证我们可以提前停止
        for meta in vec.iter() {
          // Check tail pruning
          match range.end_bound() {
            Included(end) if meta.min.as_ref() > end => break,
            Excluded(end) if meta.min.as_ref() >= end => break,
            _ => {}
          }
          if meta.is_overlap(&range) {
            res.push(meta.clone());
          }
        }
      } else {
        // L1+ are disjoint and sorted.
        // L1+ 不重叠且有序。
        res.extend(xrange::overlap_for_sorted::<_, [u8], MetaWithAutoRm, _>(range, vec).cloned());
      }
    }
    res
  }

  /// Update state with operation (apply only to memory)
  /// 使用操作更新状态（仅应用到内存）
  pub fn update(&mut self, op: &Op) {
    let inner = &mut self.levels;

    match op {
      Op::Mem2Sst { meta } => {
        if let Some(vec) = inner.get_mut(meta.sst_level as usize) {
          vec.push(Rc::new(MetaWithAutoRm {
            inner: Rc::new(meta.meta.clone()),
            is_rm: Cell::new(false),
            dir: self.dir.clone(),
          }));
        }
      }
      Op::Compact { adds, rms } => {
        // 1. Remove rms from all levels (IDs are unique)
        // 1. 从所有层级移除 rms (ID 唯一)
        if !rms.is_empty() {
          for level_vec in inner.iter_mut() {
            level_vec.retain(|m| {
              if rms.contains(&m.inner.id) {
                m.is_rm.set(true);
                false
              } else {
                true
              }
            });
          }
        }

        // 2. Add adds to respective levels
        // 2. 将 adds 添加到对应层级
        for add in adds {
          if let Some(vec) = inner.get_mut(add.sst_level as usize) {
            vec.push(Rc::new(MetaWithAutoRm {
              inner: Rc::new(add.meta.clone()),
              is_rm: Cell::new(false),
              dir: self.dir.clone(),
            }));
          }
        }
      }
    }
  }
}
