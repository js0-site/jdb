//! Rewrite checkpoint file
//! 重写检查点文件

use std::{
  collections::{HashMap, VecDeque},
  path::Path,
};

use jdb_base::{WalId, WalOffset};
use jdb_fs::fs::atomic_write;

use crate::{disk, error::Result};

/// Rewrite checkpoint file
/// 重写检查点文件
pub async fn rewrite(
  path: &Path,
  saves: &VecDeque<(WalId, WalOffset)>,
  rotates: &[WalId],
  sst: &HashMap<u64, u8>,
) -> Result<u64> {
  // Pre-allocate buffer
  // 预分配缓冲区
  let cap = saves.len() * disk::SAVE_SIZE
    + rotates.len() * disk::ROTATE_SIZE
    + sst.len() * disk::SST_ADD_SIZE;

  if cap == 0 {
    let _ = compio::fs::remove_file(path).await;
    return Ok(0);
  }

  let mut buf = Vec::with_capacity(cap);

  for &(wal_id, offset) in saves {
    buf.extend_from_slice(&disk::SaveWalPtr::new(wal_id, offset).to_array());
  }

  for &wal_id in rotates {
    buf.extend_from_slice(&disk::Rotate::new(wal_id).to_array());
  }

  // Sort SST by id for deterministic output
  // 按 id 排序 SST 以保证确定性输出
  let mut sst_vec: Vec<_> = sst.iter().collect();
  sst_vec.sort_unstable_by_key(|(id, _)| *id);
  for (&id, &level) in sst_vec {
    buf.extend_from_slice(&disk::SstAdd::new(id, level).to_array());
  }

  atomic_write(path, buf).await?;
  Ok(cap as u64)
}
