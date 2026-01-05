//! Rewrite checkpoint file
//! 重写检查点文件

use std::{collections::VecDeque, path::Path};

use compio::io::AsyncWriteAtExt;
use jdb_base::{WalId, WalOffset};
use jdb_fs::fs::open_read_write_create;

use crate::{
  disk::{self, ToDiskBytes},
  error::Result,
  row::{ROTATE_SIZE, SAVE_SIZE},
};

const CKP_TMP: &str = "ckp.tmp";

/// Rewrite checkpoint file
/// 重写检查点文件
pub async fn rewrite(
  path: &Path,
  saves: &VecDeque<(WalId, WalOffset)>,
  rotates: &[WalId],
) -> Result<u64> {
  let tmp = path.with_file_name(CKP_TMP);

  // Pre-allocate buffer for all data
  // 预分配所有数据的缓冲区
  let mut buf = Vec::with_capacity(saves.len() * SAVE_SIZE + rotates.len() * ROTATE_SIZE);

  for &(wal_id, offset) in saves {
    buf.extend_from_slice(&disk::SaveWalPtr::new(wal_id, offset).to_array());
  }

  for &wal_id in rotates {
    buf.extend_from_slice(&disk::Rotate::new(wal_id).to_array());
  }

  let mut tmp_file = open_read_write_create(&tmp).await?;

  // Single write for all data
  // 一次写入所有数据
  let len = buf.len() as u64;
  tmp_file.write_all_at(buf, 0).await.0?;
  tmp_file.sync_all().await?;
  drop(tmp_file);

  compio::fs::rename(&tmp, path).await?;

  Ok(len)
}
