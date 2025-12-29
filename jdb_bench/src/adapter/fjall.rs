//! Fjall adapter
//! Fjall 适配器

use std::path::{Path, PathBuf};

use fjall::KeyspaceCreateOptions;

use crate::{BenchEngine, Result};

const NAME: &str = "fjall";
const KEYSPACE: &str = "default";

/// 8MB block cache
/// 8MB 块缓存
const CACHE_SIZE: u64 = 8 * 1024 * 1024;

/// 64MB memtable
/// 64MB 内存表
const MEMTABLE_SIZE: u64 = 64 * 1024 * 1024;

pub struct FjallAdapter {
  db: fjall::Database,
  keyspace: fjall::Keyspace,
  path: PathBuf,
}

impl FjallAdapter {
  pub fn new(path: &Path) -> Result<Self> {
    let db = fjall::Database::builder(path)
      .cache_size(CACHE_SIZE)
      .open()?;
    let keyspace = db.keyspace(KEYSPACE, || {
      KeyspaceCreateOptions::default().max_memtable_size(MEMTABLE_SIZE)
    })?;
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
    NAME
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
