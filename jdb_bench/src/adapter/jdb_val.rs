// JdbVal WAL adapter / JdbVal WAL 适配器

use std::{
  collections::HashMap,
  path::{Path, PathBuf},
};

use jdb_val::{Conf, Pos, Wal};

use crate::{BenchEngine, Result};

const ENGINE_NAME: &str = "jdb_val";
/// 8MB cache like RocksDB / 8MB 缓存，与 RocksDB 一致
const CACHE_SIZE: u64 = 8 * 1024 * 1024;

/// JdbVal adapter / JdbVal 适配器
pub struct JdbValAdapter {
  wal: Wal,
  /// key -> pos mapping / 键到位置的映射
  index: HashMap<Vec<u8>, Pos>,
  /// Data directory / 数据目录
  path: PathBuf,
}

impl JdbValAdapter {
  /// Create new adapter / 创建新适配器
  pub async fn new(path: &Path) -> Result<Self> {
    let path_buf = path.to_path_buf();
    // 8MB cache to match RocksDB / 8MB 缓存匹配 RocksDB
    let mut wal = Wal::new(path, &[Conf::CacheSize(CACHE_SIZE)]);
    wal.open().await?;

    Ok(Self {
      wal,
      index: HashMap::new(),
      path: path_buf,
    })
  }
}

impl BenchEngine for JdbValAdapter {
  fn name(&self) -> &str {
    ENGINE_NAME
  }

  fn data_path(&self) -> &Path {
    &self.path
  }

  async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
    let pos = self.wal.put(key, val).await?;
    self.index.insert(key.to_vec(), pos);
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let Some(&pos) = self.index.get(key) else {
      return Ok(None);
    };
    let data = self.wal.val(pos).await?;
    Ok(Some(data))
  }

  async fn del(&mut self, key: &[u8]) -> Result<()> {
    if self.index.remove(key).is_some() {
      self.wal.del(key).await?;
    }
    Ok(())
  }

  async fn sync(&mut self) -> Result<()> {
    self.wal.sync_all().await?;
    Ok(())
  }
}
