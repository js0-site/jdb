//! Load impl for checkpoint
//! 检查点的 Load 实现

use jdb_fs::{
  kv::{CRC_SIZE, MAGIC},
  load::{INVALID, Load},
};

use crate::{
  disk::{ROTATE_SIZE, SAVE_SIZE, SST_ADD_SIZE, SST_RM_SIZE},
  row::{KIND_ROTATE, KIND_SAVE, KIND_SST_ADD, KIND_SST_RM, Row},
};

const ROTATE_META: usize = 9;
const SAVE_META: usize = 17;
const SST_ADD_META: usize = 10;

/// Load impl for checkpoint
/// 检查点的 Load 实现
pub struct CkpLoad;

impl Load for CkpLoad {
  type Head = Row;

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
      KIND_SST_ADD => SST_ADD_SIZE,
      KIND_SST_RM => SST_RM_SIZE,
      _ => INVALID,
    }
  }

  #[inline]
  fn crc_offset(len: usize) -> usize {
    len - CRC_SIZE
  }

  #[inline]
  fn meta_len(len: usize) -> usize {
    // CRC covers kind + data (skip magic only)
    // CRC 覆盖 kind + data（只跳过 magic）
    len - 1 - CRC_SIZE
  }

  fn parse_head(buf: &[u8], len: usize) -> Option<Self::Head> {
    let buf_len = buf.len();
    if buf_len < ROTATE_META {
      return None;
    }
    // Safe: checked buf.len() >= ROTATE_META (9 bytes)
    // 安全：已检查 buf.len()，使用 try_into 避免 unsafe
    let id = u64::from_le_bytes(buf[1..9].try_into().unwrap());

    Some(match buf[0] {
      KIND_SAVE if len == SAVE_SIZE && buf_len >= SAVE_META => {
        let offset = u64::from_le_bytes(buf[ROTATE_META..ROTATE_META + 8].try_into().unwrap());
        Row::SaveWalPtr { wal_id: id, offset }
      }
      KIND_ROTATE if len == ROTATE_SIZE => Row::Rotate { wal_id: id },
      KIND_SST_ADD if len == SST_ADD_SIZE && buf_len >= SST_ADD_META => {
        Row::SstAdd { id, level: buf[9] }
      }
      KIND_SST_RM if len == SST_RM_SIZE => Row::SstRm { id },
      _ => return None,
    })
  }
}
