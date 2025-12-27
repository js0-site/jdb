//! WAL (Write-Ahead Log) implementation / WAL 预写日志实现
//!
//! Single-threaded async with compio / 基于 compio 的单线程异步

mod header;
mod open;
mod read;
mod stream;
mod write;

use std::{fs, path::PathBuf};

use compio_fs::File;
use fast32::base32::CROCKFORD_LOWER;
use hashlink::LruCache;
pub use header::{HEADER_SIZE, WAL_VERSION};
pub use stream::DataStream;

use crate::{GenId, Head, Pos, error::Result};

/// Default max file size (256MB, ref RocksDB) / 默认最大文件大小
const DEFAULT_MAX_SIZE: u64 = 256 * 1024 * 1024;
/// Default head cache capacity (ref RocksDB block_cache) / 默认头缓存容量
const DEFAULT_HEAD_CACHE_CAP: usize = 8192;
/// Default data cache capacity / 默认数据缓存容量
const DEFAULT_DATA_CACHE_CAP: usize = 1024;
/// Default file cache capacity / 默认文件缓存容量
const DEFAULT_FILE_CACHE_CAP: usize = 64;

/// WAL subdirectory name / WAL 子目录名
const WAL_SUBDIR: &str = "wal";
/// Bin subdirectory name / Bin 子目录名
const BIN_SUBDIR: &str = "bin";

/// WAL configuration / WAL 配置
#[derive(Debug, Clone, Copy)]
pub enum Conf {
  /// Max WAL file size in bytes / 最大文件大小（字节）
  MaxSize(u64),
  /// Head cache capacity / 头缓存容量
  HeadCacheCap(usize),
  /// Data cache capacity / 数据缓存容量
  DataCacheCap(usize),
  /// File handle cache capacity / 文件句柄缓存容量
  FileCacheCap(usize),
}

/// Storage mode / 存储模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
  Inline,
  Infile,
  File,
}

/// WAL manager / WAL 管理器
pub struct Wal {
  wal_dir: PathBuf,
  bin_dir: PathBuf,
  cur_id: u64,
  cur_file: Option<File>,
  cur_pos: u64,
  max_size: u64,
  gen_id: GenId,
  head_cache: LruCache<Pos, Head>,
  file_cache: LruCache<u64, File>,
  data_cache: LruCache<Pos, Vec<u8>>,
}

impl Wal {
  /// Create WAL manager / 创建 WAL 管理器
  pub fn new(dir: impl Into<PathBuf>, conf: &[Conf]) -> Self {
    let dir = dir.into();
    let mut max_size = DEFAULT_MAX_SIZE;
    let mut head_cap = DEFAULT_HEAD_CACHE_CAP;
    let mut data_cap = DEFAULT_DATA_CACHE_CAP;
    let mut file_cap = DEFAULT_FILE_CACHE_CAP;

    for c in conf {
      match *c {
        Conf::MaxSize(v) => max_size = v,
        Conf::HeadCacheCap(v) => head_cap = v,
        Conf::DataCacheCap(v) => data_cap = v,
        Conf::FileCacheCap(v) => file_cap = v,
      }
    }

    Self {
      wal_dir: dir.join(WAL_SUBDIR),
      bin_dir: dir.join(BIN_SUBDIR),
      cur_id: 0,
      cur_file: None,
      cur_pos: 0,
      max_size,
      gen_id: GenId::new(),
      head_cache: LruCache::new(head_cap),
      file_cache: LruCache::new(file_cap),
      data_cache: LruCache::new(data_cap),
    }
  }

  #[inline]
  fn wal_path(&self, id: u64) -> PathBuf {
    self.wal_dir.join(CROCKFORD_LOWER.encode_u64(id))
  }

  #[inline]
  fn bin_path(&self, id: u64) -> PathBuf {
    self.bin_dir.join(CROCKFORD_LOWER.encode_u64(id))
  }

  /// Iterate all WAL file ids / 迭代所有 WAL 文件 id
  pub fn iter(&self) -> impl Iterator<Item = u64> {
    let entries = fs::read_dir(&self.wal_dir).ok();
    entries
      .into_iter()
      .flatten()
      .filter_map(|e| e.ok())
      .filter_map(|e| {
        let name = e.file_name();
        let name = name.to_str()?;
        CROCKFORD_LOWER.decode_u64(name.as_bytes()).ok()
      })
  }

  /// Remove WAL file (for GC) / 删除 WAL 文件（用于垃圾回收）
  pub fn remove(&self, id: u64) -> Result<()> {
    use crate::Error;
    if id == self.cur_id {
      return Err(Error::CannotRemoveCurrent);
    }
    let path = self.wal_path(id);
    fs::remove_file(&path)?;
    Ok(())
  }

  /// Remove bin file / 删除二进制文件
  pub fn remove_bin(&self, id: u64) -> Result<()> {
    let path = self.bin_path(id);
    fs::remove_file(&path)?;
    Ok(())
  }

  pub async fn sync_data(&self) -> Result<()> {
    if let Some(file) = &self.cur_file {
      file.sync_data().await?;
    }
    Ok(())
  }

  pub async fn sync_all(&self) -> Result<()> {
    if let Some(file) = &self.cur_file {
      file.sync_all().await?;
    }
    Ok(())
  }

  #[inline]
  pub fn cur_id(&self) -> u64 {
    self.cur_id
  }

  #[inline]
  pub fn cur_pos(&self) -> u64 {
    self.cur_pos
  }
}
