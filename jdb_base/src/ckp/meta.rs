use bitcode::{Decode, Encode};

use crate::sst;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Meta {
  /// Level number (0 = L0, 1 = L1, ...)
  /// 层级编号
  pub sst_level: u8,
  /// Tombstone size (key_len + val_len + overhead)
  /// 墓碑大小（key_len + val_len + 固定开销）
  pub rmed_size: u64,
  /// File size / 文件大小
  pub file_size: u64,
  pub meta: sst::Meta,
}

impl Meta {
  /// Calculate compensated size (file_size - rmed_size)
  /// 计算补偿大小（file_size - rmed_size）
  #[inline]
  pub fn size_without_rmed(&self) -> u64 {
    self.file_size.saturating_sub(self.rmed_size)
  }
}
