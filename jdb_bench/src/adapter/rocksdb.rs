// RocksDB adapter / RocksDB 适配器

use std::path::{Path, PathBuf};

use crate::{BenchEngine, Result};

const ENGINE_NAME: &str = "rocksdb";

/// RocksDB adapter / RocksDB 适配器
pub struct RocksDbAdapter {
  db: rocksdb::DB,
  /// Data directory / 数据目录
  path: PathBuf,
}

impl RocksDbAdapter {
  /// Create new adapter / 创建新适配器
  pub fn new(path: &Path) -> Result<Self> {
    let db = rocksdb::DB::open_default(path)?;
    Ok(Self {
      db,
      path: path.to_path_buf(),
    })
  }
}

impl BenchEngine for RocksDbAdapter {
  fn name(&self) -> &str {
    ENGINE_NAME
  }

  fn data_path(&self) -> &Path {
    &self.path
  }

  async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
    self.db.put(key, val)?;
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    Ok(self.db.get(key)?)
  }

  async fn del(&mut self, key: &[u8]) -> Result<()> {
    self.db.delete(key)?;
    Ok(())
  }

  async fn sync(&self) -> Result<()> {
    self.db.flush()?;
    Ok(())
  }
}
