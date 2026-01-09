//! Block cache for WAL random reads
//! WAL 随机读取的块缓存

use std::path::PathBuf;

use compio::{buf::IoBufMut, io::AsyncReadAtExt};
use compio_fs::File;
use hashlink::lru_cache::Entry;
use jdb_lru::Lru;
use log::error;

use crate::{fs::open_read, fs_id::id_path};

// Min file cache capacity
// 最小文件缓存容量
const MIN_FILE_CAP: usize = 4;

/// WAL block cache with file handle cache
/// WAL 块缓存（含文件句柄缓存）
pub struct FileLru {
  dir: PathBuf,
  files: Lru<u64, File>,
}

impl FileLru {
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
  ) -> std::io::Result<B> {
    let file = self.open(file_id).await?;
    let res = file.read_exact_at(buf, offset).await;
    res.0?;
    Ok(res.1)
  }

  async fn open(&mut self, file_id: u64) -> std::io::Result<&File> {
    match self.files.0.entry(file_id) {
      Entry::Occupied(e) => Ok(e.into_mut()),
      Entry::Vacant(e) => {
        let path = id_path(&self.dir, file_id);
        let file = open_read(&path).await?;
        Ok(e.insert(file))
      }
    }
  }

  /// Evict file from cache (without deleting from disk)
  /// 从缓存移除文件（不删除磁盘文件）
  #[inline]
  pub fn evict(&mut self, file_id: u64) {
    self.files.0.remove(&file_id);
  }

  /// Remove file from cache and delete from disk
  /// 从缓存移除文件并从磁盘删除
  #[inline]
  pub fn rm(&mut self, file_id: u64) {
    self.evict(file_id);
    let path = id_path(&self.dir, file_id);
    // Spawn background task to delete file
    // 启动后台任务删除文件
    compio::runtime::spawn(async move {
      if let Err(e) = compio_fs::remove_file(&path).await {
        error!("remove {}, error={}", path.display(), e);
      }
    })
    .detach();
  }
}
