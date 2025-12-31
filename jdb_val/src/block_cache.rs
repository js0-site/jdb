//! Block cache for WAL random reads
//! WAL 随机读取的块缓存

use std::path::PathBuf;

use compio::{buf::IoBufMut, io::AsyncReadAtExt};
use compio_fs::File;
use jdb_lru::{Cache, Lru};

use crate::{
  Result,
  fs::{id_path, open_read},
};

// Min file cache capacity
// 最小文件缓存容量
const MIN_FILE_CAP: usize = 4;

/// WAL block cache with file handle cache
/// WAL 块缓存（含文件句柄缓存）
pub struct BlockLru {
  dir: PathBuf,
  files: Lru<u64, File>,
}

impl BlockLru {
  /// Create from dir, cache size and file capacity
  /// 从目录、缓存大小和文件容量创建
  #[inline]
  pub fn new(dir: impl Into<PathBuf>, file_cap: usize) -> Self {
    Self {
      dir: dir.into(),
      files: Lru::new(file_cap.max(MIN_FILE_CAP)),
    }
  }

  /// Read data at offset into caller's buffer (zero-copy)
  /// 在偏移处读取数据到调用者缓冲区（零拷贝）
  #[inline(always)]
  pub async fn read_into<B: IoBufMut>(
    &mut self,
    file_id: u64,
    buf: B,
    offset: u64,
  ) -> (Result<()>, B) {
    let file = match self.open(file_id).await {
      Ok(f) => f,
      Err(e) => return (Err(e), buf),
    };
    let res = file.read_exact_at(buf, offset).await;
    (res.0.map_err(Into::into).map(drop), res.1)
  }

  async fn open(&mut self, file_id: u64) -> Result<&File> {
    if self.files.get(&file_id).is_none() {
      let path = id_path(&self.dir, file_id);
      let file = open_read(&path).await?;
      self.files.set(file_id, file);
    }
    Ok(self.files.get(&file_id).unwrap())
  }

  /// Remove file from cache
  /// 从缓存移除文件
  pub fn rm(&mut self, file_id: u64) {
    self.files.rm(&file_id);
  }
}
