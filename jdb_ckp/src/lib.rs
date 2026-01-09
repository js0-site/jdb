//! Checkpoint management for JDB
//! JDB 检查点管理

use std::path::Path;

use futures::StreamExt;
use jdb_fs::{
  AutoCompact,
  load::{HeadWithData, Load},
};

// Internal modules
// 内部模块
pub mod ckp;
mod disk;
pub mod error;
mod load;
mod row;
pub mod state;

pub use ckp::{Ckp, Conf};
pub use error::{Error, Result};
pub use load::CkpLoad;
pub use row::Op;
use row::Row;
use state::CkpState;

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
  let mut state = CkpState::new(keep);
  let mut count = 0usize;

  let stream = CkpLoad::recover(path.clone(), 0);
  futures::pin_mut!(stream);

  while let Some(HeadWithData { head, .. }) = stream.next().await {
    match head {
      Row::SaveWalPtr { wal_id, offset } => {
        state.saves.push_back((wal_id, offset));
        if state.saves.len() > keep {
          state.saves.pop_front();
        }
      }
      Row::Rotate { wal_id } => state.rotates.push(wal_id),
      Row::SstAdd { id, level } => {
        state.sst.insert(id, level);
      }
      Row::SstRm { id } => {
        state.sst.remove(&id);
      }
    }
    count += 1;
  }

  // Filter rotates by min save wal_id
  // 按最小 save wal_id 过滤 rotates
  state.rotates.sort_unstable();
  state.filter_rotates();

  // Build replay info from last save
  // 从最后一个 save 构建回放信息
  let after = state.saves.back().map(|(wal_id, offset)| {
    let idx = state.rotates.partition_point(|id| *id <= *wal_id);
    jdb_base::Ckp {
      wal_id: *wal_id,
      offset: *offset,
      rotate_wal_ids: state.rotates[idx..].to_vec(),
    }
  });

  let log = AutoCompact::new(state, path, count, truncate).await?;

  Ok((Ckp { log }, after))
}
