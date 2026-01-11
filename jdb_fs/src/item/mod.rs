//! Item encoding and decoding
//! 条目编码和解码

mod decode;
mod encode;

use std::io;

pub use decode::{ParseResult, find_next_magic, parse};
pub use encode::{encode, write};
use thiserror::Error;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

pub type Offset = usize;

/// Item error type
/// 条目错误类型
#[derive(Debug, Error)]
pub enum Error {
  #[error("IO: {0}")]
  Io(#[from] io::Error),

  #[error("Decode failed")]
  DecodeFailed,

  #[error("Magic mismatch")]
  MagicMismatch,

  #[error("CRC mismatch")]
  CrcMismatch,
}

pub type Result<T> = std::result::Result<T, Error>;

/// Trait for Head to provide data length
/// Head 提供数据长度的 trait
pub trait DataLen {
  fn data_len(&self) -> usize;
}

/// Item trait for log-structured data
/// 日志结构数据的 Item trait
pub trait Item {
  const MAGIC: u8;

  type Head: IntoBytes + FromBytes + Immutable + KnownLayout + DataLen + Copy;
}

/// Row for disk storage: magic(1) + head + crc32(4)
/// 磁盘存储的行: magic(1) + head + crc32(4)
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C, packed)]
pub struct Row<H> {
  pub magic: u8,
  pub head: H,
  pub crc32: u32,
}
