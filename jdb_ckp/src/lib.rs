//! Checkpoint management for JDB
//! JDB 检查点管理

use std::{collections::VecDeque, path::Path};

use futures::StreamExt;
use jdb_fs::{
  fs::open_read_write_create,
  load::{HeadEnd, Load},
};

// Internal modules / 内部模块
pub mod ckp;
mod disk;
pub mod error;
mod load;
mod rewrite;
mod row;

pub use ckp::{Ckp, Conf};
pub use error::{Error, Result};
pub use load::CkpLoad;
use rewrite::rewrite;
use row::Row;

const DEFAULT_TRUNCATE: usize = 65536;
const DEFAULT_KEEP: usize = 3;
const CKP_WAL: &str = "ckp.wal";

/// Open or create checkpoint manager
/// 打开或创建检查点管理器
pub async fn open(dir: &Path, conf: &[Conf]) -> Result<(Ckp, Option<jdb_base::Ckp>)> {
  let path = dir.join(CKP_WAL);

  let mut truncate = DEFAULT_TRUNCATE;
  let mut keep = DEFAULT_KEEP;
  for c in conf {
    match c {
      Conf::Truncate(n) => truncate = *n,
      Conf::Keep(n) => keep = *n,
    }
  }

  // Scan
  // 扫描
  let mut saves = VecDeque::new();
  let mut rotates = Vec::new();
  let mut count = 0usize;
  let mut file_pos = 0u64;

  let stream = CkpLoad::recover(path.clone(), 0);
  futures::pin_mut!(stream);

  while let Some(HeadEnd { head, end }) = stream.next().await {
    match head {
      Row::SaveWalPtr { wal_id, offset } => {
        saves.push_back((wal_id, offset));
        if saves.len() > keep {
          saves.pop_front();
        }
      }
      Row::Rotate { wal_id } => rotates.push(wal_id),
    }
    count += 1;
    file_pos = end;
  }

  // Filter rotates by min save wal_id
  // 按最小 save wal_id 过滤 rotates
  rotates.sort_unstable();
  if let Some((min_wal_id, _)) = saves.front() {
    let idx = rotates.partition_point(|id| *id <= *min_wal_id);
    rotates.drain(..idx);
  }

  // Build replay info from last save
  // 从最后一个 save 构建回放信息
  let after = saves.back().map(|(wal_id, offset)| {
    // rotates already filtered by min_wal_id, now filter by last wal_id
    // rotates 已按 min_wal_id 过滤，现在按 last wal_id 过滤
    let idx = rotates.partition_point(|id| *id <= *wal_id);
    jdb_base::Ckp {
      wal_id: *wal_id,
      offset: *offset,
      rotate_wal_ids: rotates[idx..].to_vec(),
    }
  });

  // Rewrite if has garbage
  // 有垃圾时重写
  if count > saves.len() + rotates.len() {
    file_pos = rewrite(&path, &saves, &rotates).await?;
    count = saves.len() + rotates.len();
  }

  Ok((
    Ckp {
      file: open_read_write_create(&path).await?,
      path,
      file_pos,
      count,
      truncate,
      keep,
      saves,
      rotates,
    },
    after,
  ))
}
