// RocksDB adapter / RocksDB 适配器

use std::path::{Path, PathBuf};

use rocksdb::{BlockBasedOptions, Cache, Options};

use crate::{BenchEngine, Result};

const ENGINE_NAME: &str = "rocksdb";
/// 8MB cache / 8MB 缓存
const CACHE_SIZE: usize = 8 * 1024 * 1024;

/// RocksDB adapter / RocksDB 适配器
pub struct RocksDbAdapter {
  db: rocksdb::DB,
  /// Data directory / 数据目录
  path: PathBuf,
}

impl RocksDbAdapter {
  /// Create new adapter / 创建新适配器
  pub fn new(path: &Path) -> Result<Self> {
    let cache = Cache::new_lru_cache(CACHE_SIZE);
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_cache(&cache);

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_block_based_table_factory(&block_opts);

    let db = rocksdb::DB::open(&opts, path)?;
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

  async fn sync(&mut self) -> Result<()> {
    self.db.flush()?;
    // fsync WAL / 同步 WAL 到磁盘
    self.db.flush_wal(true)?;
    Ok(())
  }
}
