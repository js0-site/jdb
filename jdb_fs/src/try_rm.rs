use std::{fs, path::Path};

use fs4::fs_std::FileExt;

/// Try to delete tmp file if not locked, return true if deleted
/// 尝试删除未锁定的临时文件，删除成功返回 true
pub fn try_rm(path: impl AsRef<Path>) -> bool {
  let path = path.as_ref();
  // Open with std::fs to check lock (sync operation)
  // 使用 std::fs 打开以检查锁（同步操作）
  if let Ok(file) = fs::File::open(path) {
    // Try exclusive lock to ensure no one else is writing
    // 尝试排他锁以确保没有其他人在写入
    if file.try_lock_exclusive().is_ok() {
      // Unlock by dropping file, then remove
      // 通过 drop 文件解锁，然后删除
      drop(file);
      return fs::remove_file(path).is_ok();
    }
  }
  false
}
