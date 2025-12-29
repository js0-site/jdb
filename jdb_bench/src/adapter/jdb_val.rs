// JdbVal WAL adapter / JdbVal WAL 适配器

use std::{
  collections::HashMap,
  path::{Path, PathBuf},
};

use hipstr::HipByt;
use jdb_val::{Conf, Pos, Wal};

use crate::{BenchEngine, Result};

const ENGINE_NAME: &str = "jdb_val";

/// Cache size = block cache + memtable (same for all engines)
/// 缓存大小 = 块缓存 + 内存表（所有引擎相同）
/// rocksdb: 8MB block cache + 64MB memtable = 72MB
/// fjall: 8MB block cache + 64MB memtable = 72MB
/// jdb_val: 72MB val_cache
const CACHE_SIZE: u64 = 72 * 1024 * 1024;

/// JdbVal adapter / JdbVal 适配器
pub struct JdbValAdapter {
  wal: Wal,
  /// key -> pos mapping (HipByt for small key optimization)
  /// 键到位置的映射（HipByt 优化小 key）
  index: HashMap<HipByt<'static>, Pos>,
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
  type Val = jdb_val::CachedData;

  fn name(&self) -> &str {
    ENGINE_NAME
  }

  fn data_path(&self) -> &Path {
    &self.path
  }

  async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
    let pos = self.wal.put(key, val).await?;
    self.index.insert(HipByt::from(key), pos);
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Self::Val>> {
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
