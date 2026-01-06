//! SSTable loading utilities
//! SSTable 加载工具

use std::{fs, path::Path};

use jdb_fs::fs_id::{decode_id, id_path};

use crate::{Result, TableInfo};

/// Load all SSTables from directory
/// 从目录加载所有 SSTable
///
/// Returns SSTables sorted by id ascending (oldest first)
/// 返回按 id 升序排列的 SSTable（最旧在前）
pub async fn load(dir: &Path) -> Result<Vec<TableInfo>> {
  // Return empty if directory doesn't exist
  // 目录不存在时返回空
  let Ok(entries) = fs::read_dir(dir) else {
    return Ok(Vec::new());
  };

  // Scan directory, decode base32 id from filename
  // 扫描目录，从文件名解码 base32 id
  let mut ids = Vec::new();
  for entry in entries.flatten() {
    let name = entry.file_name();
    let Some(name) = name.to_str() else { continue };
    if let Some(id) = decode_id(name) {
      ids.push(id);
    }
  }

  // Sort by id ascending (oldest first, newest at end)
  // 按 id 升序排列（最旧在前，最新在末尾）
  ids.sort_unstable();

  // Load TableInfo for each SSTable
  // 为每个 SSTable 加载 TableInfo
  let mut tables = Vec::with_capacity(ids.len());
  for id in ids {
    let path = id_path(dir, id);
    match TableInfo::load(&path, id).await {
      Ok(info) => tables.push(info),
      Err(e) => {
        log::warn!(
          "Failed to load SSTable {}: {} / 加载 SSTable {} 失败: {}",
          id,
          e,
          id,
          e
        );
      }
    }
  }

  Ok(tables)
}
