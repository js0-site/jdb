//! SSTable path utilities
//! SSTable 路径工具

use std::path::Path;

use jdb_fs::fs_id::encode_id;

// SSTable directory name
// SSTable 目录名
pub(crate) const SST_DIR: &str = "sst";

/// Get SSTable file path
/// 获取 SSTable 文件路径
#[inline]
pub fn sst_path(dir: &Path, id: u64) -> std::path::PathBuf {
  dir.join(SST_DIR).join(encode_id(id))
}
