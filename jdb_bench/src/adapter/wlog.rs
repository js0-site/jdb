//! JdbVal WAL adapter with cuckoo filter for fast miss
//! JdbVal WAL 适配器，带布谷鸟过滤器快速过滤 miss

use std::path::{Path, PathBuf};

use autoscale_cuckoo_filter::CuckooFilter;
use gxhash::{HashMap, HashMapExt};
use jdb_base::Pos;
use wlog::{Conf, Wal};

use crate::{BenchEngine, Result};

const NAME: &str = "wlog";

/// Cache size = block cache + memtable (same for all engines)
/// 缓存大小 = 块缓存 + 内存表（所有引擎相同）
const CACHE_SIZE: u64 = 72 * 1024 * 1024;

/// Initial filter capacity / 过滤器初始容量
const FILTER_CAP: usize = 1 << 24;

/// False positive rate / 假阳性率
const FILTER_FPR: f64 = 0.001;

pub struct JdbValAdapter {
  wal: Wal,
  index: HashMap<Box<[u8]>, Pos>,
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
    let wal = Wal::open(path, &[Conf::CacheSize(CACHE_SIZE)], None, |_, _| {}).await?;

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
  fn drop(&mut self) {}
}

impl BenchEngine for JdbValAdapter {
  type Val = wlog::Val;

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

  /// Simulated index memory: HashMap + CuckooFilter
  /// 模拟索引内存：HashMap + 布谷鸟过滤器
  fn sim_mem(&self) -> u64 {
    // HashMap: ~48 bytes per entry (Box<[u8]> + Pos + overhead)
    // HashMap: 每条目约 48 字节（Box<[u8]> + Pos + 开销）
    let map_mem = self.index.len() as u64 * 48;
    // CuckooFilter: ~2 bytes per item
    // 布谷鸟过滤器：每条目约 2 字节
    let filter_mem = self.filter.len() as u64 * 2;
    map_mem + filter_mem
  }

  async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
    // Check cuckoo filter first / 先检查布谷鸟过滤器
    if self.filter.contains(key) {
      // Key may exist, check map / key 可能存在，检查 map
      if let Some(&pos) = self.index.get(key) {
        let old = self.wal.val(pos).await?;
        // Same value, skip WAL write / 值相同，跳过 WAL 写入
        if old.as_ref() == val {
          return Ok(());
        }
      }
    } else {
      // New key, add to filter / 新 key，加入过滤器
      self.filter.add(key);
    }
    let pos = self.wal.put(key, val).await?;
    self.index.insert(key.to_vec().into_boxed_slice(), pos);
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Self::Val>> {
    // Fast miss via cuckoo filter / 布谷鸟过滤器快速 miss
    if !self.filter.contains(key) {
      return Ok(None);
    }
    let Some(&pos) = self.index.get(key) else {
      return Ok(None);
    };
    let data = self.wal.val(pos).await?;
    Ok(Some(data))
  }

  async fn rm(&mut self, key: &[u8]) -> Result<()> {
    if let Some(old_pos) = self.index.remove(key) {
      self.filter.remove(key);
      self.wal.rm(key, old_pos).await?;
    }
    Ok(())
  }

  async fn sync(&mut self) -> Result<()> {
    self.wal.sync().await?;
    Ok(())
  }
}
