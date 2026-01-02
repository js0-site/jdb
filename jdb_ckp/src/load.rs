//! Load impl for checkpoint
//! 检查点的 Load 实现

use jdb_fs::{INVALID, Load};

pub use crate::row::Head;
use crate::row::{
  CRC_SIZE, KIND_ROTATE, KIND_SAVE, MAGIC, MAGIC_SIZE, ROTATE_META, ROTATE_SIZE, SAVE_META,
  SAVE_SIZE,
};

/// Load impl for checkpoint
/// 检查点的 Load 实现
pub struct CkpLoad;

impl Load for CkpLoad {
  type Head = Head;

  const MAGIC: u8 = MAGIC;
  const MIN_SIZE: usize = ROTATE_SIZE;
  const META_OFFSET: usize = 1;

  #[inline]
  fn len(buf: &[u8]) -> usize {
    if buf.len() < Self::MIN_SIZE || buf[0] != Self::MAGIC {
      return INVALID;
    }
    match buf[1] {
      KIND_SAVE => SAVE_SIZE,
      KIND_ROTATE => ROTATE_SIZE,
      _ => INVALID,
    }
  }

  #[inline]
  fn crc_offset(len: usize) -> usize {
    len - CRC_SIZE
  }

  #[inline]
  fn meta_len(len: usize) -> usize {
    len - MAGIC_SIZE - CRC_SIZE
  }

  fn parse_head(buf: &[u8], len: usize) -> Option<Self::Head> {
    if buf.len() < ROTATE_META {
      return None;
    }
    // Safe: checked buf.len() >= ROTATE_META
    // 安全：已检查 buf.len() >= ROTATE_META
    let wal_id = u64::from_le_bytes(unsafe { *buf.as_ptr().add(1).cast::<[u8; 8]>() });

    match buf[0] {
      KIND_SAVE if len == SAVE_SIZE && buf.len() >= SAVE_META => {
        let offset =
          u64::from_le_bytes(unsafe { *buf.as_ptr().add(ROTATE_META).cast::<[u8; 8]>() });
        Some(Head::Save { wal_id, offset })
      }
      KIND_ROTATE if len == ROTATE_SIZE => Some(Head::Rotate { wal_id }),
      _ => None,
    }
  }
}
