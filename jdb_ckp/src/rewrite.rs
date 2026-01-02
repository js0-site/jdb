//! Rewrite checkpoint file
//! 重写检查点文件

use std::{collections::VecDeque, path::Path};

use compio::io::AsyncWriteAtExt;
use jdb_base::{WalId, WalOffset};
use jdb_fs::open_read_write;

use crate::{
  error::Result,
  row::{ROTATE_SIZE, Row, SAVE_SIZE},
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

  std::fs::File::create(&tmp)?;
  let mut tmp_file = open_read_write(&tmp).await?;

  let mut cursor = 0u64;

  for (wal_id, offset) in saves {
    let data = Row::Save {
      wal_id: *wal_id,
      offset: *offset,
    }
    .to_vec();
    tmp_file.write_all_at(data, cursor).await.0?;
    cursor += SAVE_SIZE as u64;
  }

  for wal_id in rotates {
    let data = Row::Rotate { wal_id: *wal_id }.to_vec();
    tmp_file.write_all_at(data, cursor).await.0?;
    cursor += ROTATE_SIZE as u64;
  }

  tmp_file.sync_all().await?;
  drop(tmp_file);

  compio::fs::rename(&tmp, path).await?;

  Ok(cursor)
}
