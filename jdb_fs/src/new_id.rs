//! Unique id generator
//! 唯一 id 生成器

use std::path::Path;

use crate::fs_id::id_path;

/// Generate unique id (check path and path.tmp)
/// 生成唯一 id（检查路径和路径.tmp）
pub fn new_id(dir: &Path) -> u64 {
  loop {
    let id = ider::id();
    let path = id_path(dir, id);
    if !path.exists() {
      let tmp = path.with_extension("tmp");
      if !tmp.exists() {
        return id;
      }
    }
  }
}
