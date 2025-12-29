// Fjall adapter / Fjall 适配器

use std::path::{Path, PathBuf};

use crate::{BenchEngine, Result};

const ENGINE_NAME: &str = "fjall";
const DEFAULT_KEYSPACE: &str = "default";
/// 8MB cache like RocksDB / 8MB 缓存，与 RocksDB 一致
const CACHE_SIZE: u64 = 8 * 1024 * 1024;

/// Fjall adapter / Fjall 适配器
pub struct FjallAdapter {
  db: fjall::Database,
  keyspace: fjall::Keyspace,
  /// Data directory / 数据目录
  path: PathBuf,
}

impl FjallAdapter {
  /// Create new adapter / 创建新适配器
  pub fn new(path: &Path) -> Result<Self> {
    // 8MB cache to match RocksDB / 8MB 缓存匹配 RocksDB
    let db = fjall::Database::builder(path)
      .cache_size(CACHE_SIZE)
      .open()?;
    let keyspace = db.keyspace(DEFAULT_KEYSPACE, fjall::KeyspaceCreateOptions::default)?;
    Ok(Self {
      db,
      keyspace,
      path: path.to_path_buf(),
    })
  }
}

impl BenchEngine for FjallAdapter {
  type Val = Vec<u8>;

  fn name(&self) -> &str {
    ENGINE_NAME
  }

  fn data_path(&self) -> &Path {
    &self.path
  }

  async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
    self.keyspace.insert(key, val)?;
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Self::Val>> {
    Ok(self.keyspace.get(key)?.map(|v| v.to_vec()))
  }

  async fn del(&mut self, key: &[u8]) -> Result<()> {
    self.keyspace.remove(key)?;
    Ok(())
  }

  async fn sync(&mut self) -> Result<()> {
    self.db.persist(fjall::PersistMode::SyncAll)?;
    Ok(())
  }
}
