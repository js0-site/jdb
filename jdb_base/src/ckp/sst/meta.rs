use bitcode::{Decode, Encode};

use crate::sst;

#[derive(Debug, Clone, Copy, Encode, Decode)]
pub struct Sst {
  pub level: crate::sst::Level,
  /// Tombstone size (key_len + val_len + overhead)
  /// 墓碑大小（key_len + val_len + 固定开销）
  pub rmed: u64,
  /// File size / 文件大小
  pub size: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Meta {
  pub sst: Sst,
  pub meta: sst::Meta,
}

impl Sst {
  /// Calculate compensated size (file_size + rmed_size) to prioritize GC
  /// 计算补偿大小（文件大小 + 墓碑大小），以优先进行 GC。
  ///
  /// Rationale (RocksDB Compaction Priority):
  /// Even if a file has a lot of tombstones (rmed data), its physical size might be small.
  /// If we only use physical size, such files might not trigger compaction (score too low).
  /// By adding `rmed` size to the physical size, we artificially inflate the "size" of
  /// tombstone-heavy files. This increases the level's score, making it more likely
  /// to be selected for compaction/sinking, thus cleaning up the garbage faster.
  ///
  /// 原理（RocksDB 压缩优先级）：
  /// 即使文件包含大量墓碑（已删除数据），其物理大小可能很小。
  /// 如果仅使用物理大小，这些文件可能不会触发压缩（得分太低）。
  /// 通过将 `rmed` 大小加到物理大小上，我们人为地增加了墓碑密集型文件的“大小”。
  /// 这增加了该层级的得分，使其更有可能被选中进行压缩/下沉，从而更快地清理垃圾。
  #[inline]
  pub fn virtual_size(&self) -> u64 {
    self.size.saturating_add(self.rmed)
  }
}
