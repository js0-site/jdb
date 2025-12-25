// Sled adapter / Sled 适配器

use std::path::{Path, PathBuf};

use crate::{BenchEngine, Result};

const ENGINE_NAME: &str = "sled";

/// Sled adapter / Sled 适配器
pub struct SledAdapter {
  db: sled::Db,
  /// Data directory / 数据目录
  path: PathBuf,
}

impl SledAdapter {
  /// Create new adapter / 创建新适配器
  pub fn new(path: &Path) -> Result<Self> {
    let db = sled::open(path)?;
    Ok(Self {
      db,
      path: path.to_path_buf(),
    })
  }
}

impl BenchEngine for SledAdapter {
  fn name(&self) -> &str {
    ENGINE_NAME
  }

  fn data_path(&self) -> &Path {
    &self.path
  }

  async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
    self.db.insert(key, val)?;
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    Ok(self.db.get(key)?.map(|v| v.to_vec()))
  }

  async fn del(&mut self, key: &[u8]) -> Result<()> {
    self.db.remove(key)?;
    Ok(())
  }

  async fn sync(&self) -> Result<()> {
    self.db.flush_async().await?;
    Ok(())
  }
}
