//! WAL (Write-Ahead Log) implementation / WAL 预写日志实现
//!
//! Single-threaded async with compio / 基于 compio 的单线程异步

mod consts;
mod end;
mod header;
mod open;
mod read;
mod stream;
mod write;

use std::{fs, path::PathBuf, rc::Rc};

use compio_fs::{File, OpenOptions};
use consts::{
  BIN_SUBDIR, DEFAULT_DATA_CAP, DEFAULT_FILE_CAP, DEFAULT_HEAD_CAP, DEFAULT_MAX_SIZE, WAL_SUBDIR,
};
pub use consts::{END_MAGIC, END_SIZE, HEADER_SIZE, WAL_VERSION};
pub use end::{build_end, parse_end};
use fast32::base32::CROCKFORD_LOWER;
use hashlink::{LruCache, lru_cache::Entry};
pub use read::LogIter;
pub use stream::DataStream;

use crate::{Error, GenId, Head, Pos, error::Result};

/// Cached data type / 缓存数据类型
pub type CachedData = Rc<[u8]>;

/// WAL configuration / WAL 配置
#[derive(Debug, Clone, Copy)]
pub enum Conf {
  /// Max WAL file size in bytes / 最大文件大小（字节）
  MaxSize(u64),
  /// Head cache capacity / 头缓存容量
  HeadLru(usize),
  /// Data cache capacity / 数据缓存容量
  DataLru(usize),
  /// File handle cache capacity / 文件句柄缓存容量
  FileLru(usize),
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
  /// Scratch buffer for writing headers/metadata to avoid repeated allocations
  /// 用于写入头/元数据的临时缓冲区，避免重复分配
  scratch: Vec<u8>,
  /// Data buffer for infile writes / Infile 写入的数据缓冲区
  data_buf: Vec<u8>,
  /// Buffer for file writes to avoid allocation / 文件写入缓冲区，避免分配
  file_buf: Vec<u8>,
  /// Buffer for reading data to avoid allocation / 读取数据缓冲区，避免分配
  read_buf: Vec<u8>,
  head_cache: LruCache<Pos, Head>,
  file_cache: LruCache<u64, File>,
  data_cache: LruCache<Pos, CachedData>,
}

impl Wal {
  /// Create WAL manager / 创建 WAL 管理器
  pub fn new(dir: impl Into<PathBuf>, conf: &[Conf]) -> Self {
    let dir = dir.into();
    let mut max_size = DEFAULT_MAX_SIZE;
    let mut head_cap = DEFAULT_HEAD_CAP;
    let mut data_cap = DEFAULT_DATA_CAP;
    let mut file_cap = DEFAULT_FILE_CAP;

    for c in conf {
      match *c {
        Conf::MaxSize(v) => max_size = v,
        Conf::HeadLru(v) => head_cap = v,
        Conf::DataLru(v) => data_cap = v,
        Conf::FileLru(v) => file_cap = v,
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
      // Capacity for Head + End marker / 容量足以容纳 Head + 尾部标记
      scratch: Vec::with_capacity(Head::SIZE + END_SIZE),
      data_buf: Vec::new(),
      file_buf: Vec::new(),
      read_buf: Vec::new(),
      head_cache: LruCache::new(head_cap),
      file_cache: LruCache::new(file_cap),
      data_cache: LruCache::new(data_cap),
    }
  }

  /// Helper to encode ID to filename / ID 编码为文件名的辅助函数
  #[inline]
  pub(crate) fn encode_id(id: u64) -> String {
    CROCKFORD_LOWER.encode_u64(id)
  }

  /// Helper to decode ID from filename / 文件名解码为 ID 的辅助函数
  #[inline]
  pub(crate) fn decode_id(name: &str) -> Option<u64> {
    CROCKFORD_LOWER.decode_u64(name.as_bytes()).ok()
  }

  #[inline]
  fn wal_path(&self, id: u64) -> PathBuf {
    self.wal_dir.join(Self::encode_id(id))
  }

  #[inline]
  fn bin_path(&self, id: u64) -> PathBuf {
    self.bin_dir.join(Self::encode_id(id))
  }

  /// Get cached file or open new one / 获取缓存文件或打开新文件
  pub(crate) async fn get_cached_file(&mut self, id: u64, is_wal: bool) -> Result<&File> {
    match self.file_cache.entry(id) {
      Entry::Occupied(e) => Ok(e.into_mut()),
      Entry::Vacant(e) => {
        let path = if is_wal {
          self.wal_dir.join(Self::encode_id(id))
        } else {
          self.bin_dir.join(Self::encode_id(id))
        };
        let file = OpenOptions::new().read(true).open(&path).await?;
        Ok(e.insert(file))
      }
    }
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
        Self::decode_id(name)
      })
  }

  /// Remove WAL file (for GC) / 删除 WAL 文件（用于垃圾回收）
  pub fn remove(&self, id: u64) -> Result<()> {
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
