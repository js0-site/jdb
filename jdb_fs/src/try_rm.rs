//! Try remove fs if not locked
//! 尝试删除未锁定的文件

use std::{fs, path::Path};

/// Try to delete fs if not locked, return true if deleted
/// 尝试删除未锁定的文件，删除成功返回 true
pub fn try_rm(path: impl AsRef<Path>) -> bool {
  let path = path.as_ref();
  if let Ok(fs) = fs::OpenOptions::new().write(true).open(path)
    && fs.try_lock().is_ok()
  {
    // Windows: must release handle before delete
    // Windows: 删除前必须释放句柄
    #[cfg(windows)]
    drop(fs);

    return fs::remove_file(path).is_ok();
  }
  false
}
