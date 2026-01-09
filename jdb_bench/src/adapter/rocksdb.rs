//! RocksDB adapter
//! RocksDB 适配器

use std::path::{Path, PathBuf};

use rocksdb::{BlockBasedOptions, Cache, Options};

use crate::{BenchEngine, Result};

const NAME: &str = "rocksdb";

/// 8MB block cache
/// 8MB 块缓存
const CACHE_SIZE: usize = 8 * 1024 * 1024;

/// 64MB memtable
/// 64MB 内存表
const MEMTABLE_SIZE: usize = 64 * 1024 * 1024;

pub struct RocksDbAdapter {
  db: rocksdb::DB,
  path: PathBuf,
}

impl RocksDbAdapter {
  pub fn new(path: &Path) -> Result<Self> {
    let cache = Cache::new_lru_cache(CACHE_SIZE);
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_cache(&cache);

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_block_based_table_factory(&block_opts);
    opts.set_write_buffer_size(MEMTABLE_SIZE);

    let db = rocksdb::DB::open(&opts, path)?;
    Ok(Self {
      db,
      path: path.to_path_buf(),
    })
  }
}

impl BenchEngine for RocksDbAdapter {
  type Val = Vec<u8>;

  fn name(&self) -> &str {
    NAME
  }

  fn data_path(&self) -> &Path {
    &self.path
  }

  async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
    self.db.put(key, val)?;
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Self::Val>> {
    Ok(self.db.get(key)?)
  }

  async fn rm(&mut self, key: &[u8]) -> Result<()> {
    self.db.delete(key)?;
    Ok(())
  }

  async fn sync(&mut self) -> Result<()> {
    self.db.flush()?;
    self.db.flush_wal(true)?;
    Ok(())
  }
}
