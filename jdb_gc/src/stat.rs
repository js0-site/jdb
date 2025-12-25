//! GC statistics / GC 统计

/// VLog file statistics / VLog 文件统计
#[derive(Debug, Clone, Copy, Default)]
pub struct FileStat {
  /// Total records in file / 文件中的总记录数
  pub total: u64,
  /// Live records count / 存活记录数
  pub live: u64,
  /// File size in bytes / 文件大小（字节）
  pub size: u64,
}

impl FileStat {
  /// Garbage ratio / 垃圾比例
  #[inline]
  pub fn garbage_ratio(&self) -> f64 {
    if self.total == 0 {
      0.0
    } else {
      1.0 - (self.live as f64 / self.total as f64)
    }
  }

  /// Is empty (no records) / 是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.total == 0
  }

  /// Has garbage / 是否有垃圾
  #[inline]
  pub fn has_garbage(&self) -> bool {
    self.live < self.total
  }
}

/// GC result statistics / GC 结果统计
#[derive(Debug, Clone, Copy, Default)]
pub struct GcStats {
  /// Pages freed / 释放的页数
  pub pages_freed: u64,
  /// Files deleted / 删除的文件数
  pub files_deleted: u64,
  /// Files compacted / 压缩的文件数
  pub files_compacted: u64,
  /// Bytes reclaimed / 回收的字节数
  pub bytes_reclaimed: u64,
  /// Tables scanned / 扫描的表数
  pub tables_scanned: u64,
  /// Keys scanned / 扫描的键数
  pub keys_scanned: u64,
}

impl GcStats {
  /// Merge stats / 合并统计
  pub fn merge(&mut self, other: &GcStats) {
    self.pages_freed += other.pages_freed;
    self.files_deleted += other.files_deleted;
    self.files_compacted += other.files_compacted;
    self.bytes_reclaimed += other.bytes_reclaimed;
    self.tables_scanned += other.tables_scanned;
    self.keys_scanned += other.keys_scanned;
  }
}
