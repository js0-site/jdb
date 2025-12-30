//! JdbVal WAL adapter with cuckoo filter for fast miss
//! JdbVal WAL 适配器，带布谷鸟过滤器快速过滤 miss

use std::{
  collections::HashMap,
  path::{Path, PathBuf},
};

use autoscale_cuckoo_filter::CuckooFilter;
use hipstr::HipByt;
use jdb_val::{Conf, Pos, Wal};

use crate::{BenchEngine, Result};

const NAME: &str = "jdb_val";

/// Cache size = block cache + memtable (same for all engines)
/// 缓存大小 = 块缓存 + 内存表（所有引擎相同）
const CACHE_SIZE: u64 = 72 * 1024 * 1024;

/// Initial filter capacity / 过滤器初始容量
const FILTER_CAP: usize = 1 << 24;

/// False positive rate / 假阳性率
const FILTER_FPR: f64 = 0.001;

pub struct JdbValAdapter {
  wal: Wal,
  index: HashMap<HipByt<'static>, Pos>,
  /// Scalable cuckoo filter for fast miss detection / 可扩容布谷鸟过滤器快速检测 miss
  filter: CuckooFilter<[u8]>,
  path: PathBuf,
  hits: u64,
  misses: u64,
  /// Filter blocked miss count (key not in filter) / 过滤器拦截的 miss 计数
  filter_blocked: u64,
  /// Total get count / 总读取次数
  total_gets: u64,
}

impl JdbValAdapter {
  pub async fn new(path: &Path) -> Result<Self> {
    let path_buf = path.to_path_buf();
    let mut wal = Wal::new(path, &[Conf::CacheSize(CACHE_SIZE)]);
    wal.open().await?;

    Ok(Self {
      wal,
      index: HashMap::new(),
      filter: CuckooFilter::new(FILTER_CAP, FILTER_FPR),
      path: path_buf,
      hits: 0,
      misses: 0,
      filter_blocked: 0,
      total_gets: 0,
    })
  }
}

impl Drop for JdbValAdapter {
  fn drop(&mut self) {
    self.print_stats();
  }
}

impl BenchEngine for JdbValAdapter {
  type Val = jdb_val::CachedData;

  fn name(&self) -> &str {
    NAME
  }

  fn data_path(&self) -> &Path {
    &self.path
  }

  fn reset_stats(&mut self) {
    self.hits = 0;
    self.misses = 0;
    self.filter_blocked = 0;
    self.total_gets = 0;
  }

  fn print_stats(&self) {
    let total = self.hits + self.misses;
    if total > 0 {
      let rate = self.hits as f64 / total as f64 * 100.0;
      println!(
        "   cache: hits={}, misses={}, rate={rate:.1}%",
        self.hits, self.misses
      );
    }
    if self.total_gets > 0 {
      let filter_rate = self.filter_blocked as f64 / self.total_gets as f64 * 100.0;
      println!(
        "   filter: blocked={}, total={}, rate={filter_rate:.1}%",
        self.filter_blocked, self.total_gets
      );
    }
  }

  async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
    let pos = self.wal.put(key, val).await?;
    let key = HipByt::from(key);
    // Only add to filter if key is new / 仅新 key 才加入过滤器
    if self.index.insert(key.clone(), pos).is_none() {
      self.filter.add(key.as_slice());
    }
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Self::Val>> {
    self.total_gets += 1;
    // Fast miss via cuckoo filter / 布谷鸟过滤器快速 miss
    if !self.filter.contains(key) {
      self.filter_blocked += 1;
      return Ok(None);
    }
    let Some(&pos) = self.index.get(key) else {
      return Ok(None);
    };
    if self.wal.cache_contains(&pos) {
      self.hits += 1;
    } else {
      self.misses += 1;
    }
    let data = self.wal.val(pos).await?;
    Ok(Some(data))
  }

  async fn del(&mut self, key: &[u8]) -> Result<()> {
    if self.index.remove(key).is_some() {
      self.filter.remove(key);
      self.wal.del(key).await?;
    }
    Ok(())
  }

  async fn sync(&mut self) -> Result<()> {
    self.wal.sync_all().await?;
    Ok(())
  }
}
