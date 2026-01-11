//! Item encoding and decoding
//! 条目编码和解码

mod decode;
mod encode;

use std::{io, mem::size_of};

use compio::buf::IoBuf;
pub use decode::{ParseResult, find_next_magic, parse};
pub use encode::{encode, write};
use thiserror::Error;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

pub type Offset = u64;

/// Row size for Item
/// Item 的 Row 大小
pub const fn row_size<H>() -> usize {
  size_of::<Row<H>>()
}

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
pub trait Item: Sized {
  const MAGIC: u8;
  const ROW_SIZE: usize = row_size::<Self::Head>();

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

unsafe impl<H: IntoBytes + Immutable + KnownLayout + Copy + 'static> IoBuf for Row<H> {
  fn as_buf_ptr(&self) -> *const u8 {
    self as *const _ as *const u8
  }

  fn buf_len(&self) -> usize {
    size_of::<Self>()
  }

  fn buf_capacity(&self) -> usize {
    size_of::<Self>()
  }
}
