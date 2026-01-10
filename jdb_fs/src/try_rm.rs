use std::{fs, path::Path};

use fs4::fs_std::FileExt;

/// Try to delete tmp file if not locked, return true if deleted
/// 尝试删除未锁定的临时文件，删除成功返回 true
pub fn try_rm(path: impl AsRef<Path>) -> bool {
  let path = path.as_ref();
  // Open with std::fs to check lock (sync operation, use with care in async context)
  // 使用 std::fs 打开以检查锁（同步操作，在异步上下文中需谨慎）
  if let Ok(file) = fs::File::open(path) {
    if file.try_lock_exclusive().is_ok() {
      drop(file);
      return fs::remove_file(path).is_ok();
    }
    false
  } else {
    false
  }
}
