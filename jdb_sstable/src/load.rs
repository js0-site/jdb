//! SSTable loading utilities (concurrent)
//! SSTable 加载工具（并发）

use std::path::Path;

use coarsetime::Clock;
use compio::fs;
use futures::{StreamExt, stream::iter};
use ider::id_to_ts;
use jdb_fs::fs_id::{decode_id, id_path};

use crate::{
  Result, SstLevels, Table,
  consts::{HOUR_MS, TMP_DIR},
  level::new_levels,
};

/// Load all SSTables from directory
/// 从目录加载所有 SSTable
///
/// Level info is read from each file's foot
/// 层级信息从每个文件的 foot 读取
pub async fn load(dir: &Path) -> Result<SstLevels> {
  // Clean up stale temp files (older than 1 hour)
  // 清理过期临时文件（超过 1 小时）
  clean_tmp(dir).await;

  // Return empty if directory doesn't exist
  // 目录不存在时返回空
  let Ok(entries) = std::fs::read_dir(dir) else {
    return Ok(new_levels());
  };

  // Scan directory, decode base32 id from filename
  // 扫描目录，从文件名解码 base32 id
  let mut ids = Vec::new();
  for entry in entries.flatten() {
    let name = entry.file_name();
    let Some(name) = name.to_str() else { continue };
    // Skip hidden files/directories
    // 跳过隐藏文件/目录
    if name.starts_with('.') {
      continue;
    }
    if let Some(id) = decode_id(name) {
      ids.push(id);
    }
  }

  // Load tables concurrently (IO bound optimization)
  // 并发加载表（IO 密集型优化）
  // buffer_unordered(16) balances FD usage and throughput
  // buffer_unordered(16) 平衡文件描述符使用和吞吐量
  let tables: Vec<Table> = iter(ids)
    .map(|id| {
      let path = id_path(dir, id);
      async move {
        match Table::load(&path, id).await {
          Ok(info) => Some(info),
          Err(e) => {
            log::warn!("Failed to load SSTable {id}: {e}");
            None
          }
        }
      }
    })
    .buffer_unordered(16)
    .filter_map(|t| async { t })
    .collect()
    .await;

  // Group tables by level
  // 按层级分组表
  let mut levels = new_levels();
  for t in tables {
    if let Some(level) = levels.li.get_mut(t.level as usize) {
      level.add(t);
    }
  }

  Ok(levels)
}

/// Clean up stale temp files older than 1 hour
/// 清理超过 1 小时的过期临时文件
async fn clean_tmp(dir: &Path) {
  let tmp_dir = dir.join(TMP_DIR);
  let Ok(entries) = std::fs::read_dir(&tmp_dir) else {
    return;
  };

  let now = Clock::now_since_epoch().as_millis();

  for entry in entries.flatten() {
    let path = entry.path();
    let name = entry.file_name();
    let Some(name) = name.to_str() else {
      // Invalid filename, delete it
      // 无效文件名，删除
      let _ = fs::remove_file(&path).await;
      continue;
    };

    let Some(id) = decode_id(name) else {
      // Not a valid id, delete it
      // 不是有效 id，删除
      let _ = fs::remove_file(&path).await;
      continue;
    };

    let ts = id_to_ts(id);
    if now.saturating_sub(ts) > HOUR_MS {
      let _ = fs::remove_file(&path).await;
    }
  }
}
