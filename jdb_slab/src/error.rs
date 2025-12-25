//! Error types / 错误类型

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
  /// CRC mismatch / CRC 校验失败
  #[error("crc mismatch: expected {expected:#x}, got {actual:#x}")]
  CrcMismatch { expected: u32, actual: u32 },

  /// Data overflow / 数据溢出
  #[error("overflow: {len} bytes exceeds max {max}")]
  Overflow { len: usize, max: usize },

  /// Invalid slot id / 无效槽位 ID
  #[error("invalid slot: {0}")]
  InvalidSlot(u64),

  /// Invalid class index / 无效层级索引
  #[error("invalid class: {0}")]
  InvalidClass(usize),

  /// No fitting class / 无合适层级
  #[error("no fitting class for size {0}")]
  NoFittingClass(usize),

  /// I/O error / I/O 错误
  #[error("io: {0}")]
  Io(#[from] std::io::Error),

  /// Filesystem error / 文件系统错误
  #[error("fs: {0}")]
  Fs(#[from] jdb_fs::Error),

  /// Allocation error / 分配错误
  #[error("alloc: {0}")]
  Alloc(#[from] jdb_alloc::Error),

  /// Serialization error / 序列化错误
  #[error("serialize: {0}")]
  Serialize(String),
}
