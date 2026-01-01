//! Fjall adapter
//! Fjall 适配器

use std::path::{Path, PathBuf};

use fjall::{AbstractTree, KeyspaceCreateOptions};

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

  /// Get active memtable size / 获取活跃 memtable 大小
  pub fn memtable_size(&self) -> u64 {
    self.keyspace.tree.active_memtable().size()
  }

  /// Get sealed memtable count / 获取已封存 memtable 数量
  pub fn sealed_memtable_count(&self) -> usize {
    self.keyspace.tree.sealed_memtable_count()
  }

  /// Flush memtable to disk / 将 memtable 刷到磁盘
  pub fn flush_memtable(&self) {
    let _ = self.keyspace.rotate_memtable_and_wait();
  }
}

impl Drop for FjallAdapter {
  fn drop(&mut self) {
    let mt_size = self.memtable_size();
    let sealed = self.sealed_memtable_count();
    println!(
      "   memtable: {:.2} MB, sealed: {sealed}",
      mt_size as f64 / 1024.0 / 1024.0
    );
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

  fn flush_before_read(&mut self) {
    self.flush_memtable();
  }
}
