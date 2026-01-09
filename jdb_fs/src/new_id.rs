//! Unique id generator
//! 唯一 id 生成器

use std::path::Path;

/// Generate unique id (check path and path.tmp)
/// 生成唯一 id（检查路径和路径.tmp）
///
/// Async to strictly avoid blocking IO.
/// 异步以严格避免阻塞 IO。
pub async fn new_id(dir: &Path) -> u64 {
  let mut path_buf = dir.to_path_buf();
  loop {
    let id = ider::id();
    let name = crate::fs_id::encode_id(id);

    path_buf.push(&name);
    // Check existence using async metadata
    // 使用异步 metadata 检查存在性
    let exists = compio::fs::metadata(&path_buf).await.is_ok();

    if !exists {
      // Also check tmp extension
      // 同时也检查 tmp 后缀
      path_buf.set_extension("tmp");
      let tmp_exists = compio::fs::metadata(&path_buf).await.is_ok();
      // Restore for next loop or return logic
      // 恢复路径
      path_buf.pop();

      if !tmp_exists {
        return id;
      }
    }
    // Pop name
    // 弹出文件名
    path_buf.pop();
  }
}
