use bitcode::{Decode, Encode};

use crate::sst;

#[derive(Debug, Clone, Encode, Decode)]
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
  /// Calculate compensated size (file_size - rmed_size)
  /// 计算补偿大小（file_size - rmed_size）
  #[inline]
  pub fn size_without_rmed(&self) -> u64 {
    self.size.saturating_sub(self.rmed)
  }
}
