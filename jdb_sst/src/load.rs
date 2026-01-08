//! SSTable loading utilities (concurrent)
//! SSTable 加载工具（并发）

use std::{cell::RefCell, collections::HashSet, path::Path, rc::Rc};

use coarsetime::Clock;
use compio::fs;
use futures::{StreamExt, stream::iter};
use ider::id_to_ts;
use jdb_ckp::Ckp;
use jdb_fs::{
  FileLru,
  fs_id::{decode_id, id_path},
};
use jdb_level::new;

use crate::{
  Handle, Result, Table,
  consts::{HOUR_MS, TMP_DIR},
  level::HandleLevels,
};

type Lru = Rc<RefCell<FileLru>>;

/// Load all SSTables from directory using Ckp for level info
/// 从目录加载所有 SSTable，使用 Ckp 获取层级信息
///
/// Files not in Ckp will be deleted (orphaned files)
/// 不在 Ckp 中的文件将被删除（孤立文件）
pub async fn load(dir: &Path, ckp: Rc<RefCell<Ckp>>, lru: Lru) -> Result<HandleLevels> {
  // Clean up stale temp files (older than 1 hour)
  // 清理过期临时文件（超过 1 小时）
  clean_tmp(dir).await;

  // Get SST ids from ckp
  // 从 ckp 获取 SST id
  let sst_map = ckp.borrow().sst_all().clone();
  let valid_ids: HashSet<u64> = sst_map.keys().copied().collect();

  // Return empty if directory doesn't exist
  // 目录不存在时返回空
  let Ok(entries) = std::fs::read_dir(dir) else {
    return Ok(new(ckp));
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
      if valid_ids.contains(&id) {
        ids.push(id);
      } else {
        // Delete orphaned file not in ckp
        // 删除不在 ckp 中的孤立文件
        let path = id_path(dir, id);
        let _ = fs::remove_file(&path).await;
      }
    }
  }

  // Shared dir for all handles
  // 所有句柄共享的目录
  let dir_rc = Rc::new(dir.to_path_buf());

  // Load tables concurrently (IO bound optimization)
  // 并发加载表（IO 密集型优化）
  let tables: Vec<Handle> = iter(ids)
    .map(|id| {
      let path = id_path(dir, id);
      let dir_rc = Rc::clone(&dir_rc);
      let lru = Rc::clone(&lru);
      async move {
        match Table::load(&path, id).await {
          Ok(table) => Some(Handle::new(table, dir_rc, lru)),
          Err(e) => {
            log::warn!("load sst {id}: {e}");
            None
          }
        }
      }
    })
    .buffer_unordered(16)
    .filter_map(|t| async { t })
    .collect()
    .await;

  // Group tables by level from ckp
  // 按 ckp 中的层级分组表
  let mut levels = new(ckp);
  for h in tables {
    let level = sst_map.get(&h.meta().id).copied().unwrap_or(0);
    if let Some(lv) = levels.levels.get_mut(level as usize) {
      lv.add(h);
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
