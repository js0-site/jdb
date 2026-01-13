//! Meta list
//! 元数据列表

use std::{cell::RefCell, rc::Rc};

use sorted_vec::SortedVec;

use crate::sst::{CkpOp, Meta};

/// Meta list managing sorted SST metadatas
/// 管理有序 SST 元数据的列表
#[derive(Debug, Default, Clone)]
pub struct MetaLi(Rc<RefCell<SortedVec<Meta>>>);

impl MetaLi {
  /// Create new MetaLi
  /// 创建新 MetaLi
  pub fn new() -> Self {
    Self::default()
  }

  /// Get reference to inner sorted vec
  /// 获取内部有序 Vec 的引用
  pub fn inner(&self) -> std::cell::Ref<'_, SortedVec<Meta>> {
    self.0.borrow()
  }

  /// Update state with operation (apply only to memory)
  /// 使用操作更新状态（仅应用到内存）
  pub fn update(&self, op: &CkpOp) {
    let mut inner = self.0.borrow_mut();
    // Take ownership of the underlying vector to avoid cloning and allow efficient batch modification
    // 接管底层 Vector 的所有权，避免克隆并允许高效的批量修改
    let mut vec = std::mem::take(&mut *inner).into_vec();

    match op {
      CkpOp::Flush { meta, .. } => {
        vec.push(meta.clone());
        // Just sort at the end
        // 最后统一排序
      }
      CkpOp::Compact { adds, rms } => {
        // 1. Remove rms (Batch remove)
        // 1. 批量移除 rms
        if !rms.is_empty() {
          vec.retain(|m| !rms.contains(&m.id));
        }
        // 2. Add adds (Batch add)
        // 2. 批量添加 adds
        // Since adds is SortedVec, its elements are already sorted.
        // Appending them and doing a final sort_unstable is efficient enough (O(N*logN) or O(N) for nearly sorted).
        // Since we don't own `op`, we must clone elements.
        // adds 已经是 SortedVec，元素有序。
        // 直接追加并最后执行 sort_unstable 效率足够高（对于近乎有序的数据为 O(N)）。
        // 由于没有 op 的所有权，必须克隆元素。
        vec.reserve(adds.len());
        for add in adds.iter() {
          vec.push(add.clone());
        }
      }
    }

    // 3. Re-sort and simplify to reconstruct SortedVec
    // Reconstruction is O(N) or O(N log N) depending on data distribution,
    // which is better than O(M * N) shifts from repeated pushes.
    // 重新排序并重建 SortedVec
    // 重建复杂度取决于数据分布，介于 O(N) 和 O(N log N) 之间，
    // 优于重复 push 导致的 O(M * N) 移位操作。
    vec.sort_unstable(); // Meta impls Ord
    *inner = SortedVec::from_unsorted(vec);
  }
}
