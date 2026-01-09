//! SSTable id generator
//! SSTable id 生成器

use std::path::Path;

use jdb_fs::fs_id::id_path;
use jdb_sst::TMP_DIR;

/// Generate unique id for SSTable
/// 为 SSTable 生成唯一 id
pub fn new(dir: &Path) -> u64 {
  let tmp_dir = dir.join(TMP_DIR);
  loop {
    let id = jdb_base::id();
    let path = id_path(dir, id);
    let tmp_path = id_path(&tmp_dir, id);
    if !path.exists() && !tmp_path.exists() {
      return id;
    }
  }
}
