//! WAL (Write-Ahead Log) implementation / WAL 预写日志实现
//!
//! Single-threaded async with compio / 基于 compio 的单线程异步

use std::fs;
use std::path::PathBuf;

use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};
use compio_fs::{File, OpenOptions};
use fast32::base32::CROCKFORD_LOWER;
use hashlink::LruCache;
use log::warn;
use zerocopy::{FromBytes, IntoBytes};

use crate::error::{Error, Result};
use crate::flag::Flag;
use crate::gen_id::GenId;
use crate::{Head, Loc, INFILE_MAX};

/// WAL file header size / WAL 文件头大小
pub const HEADER_SIZE: usize = 12;
/// Current version / 当前版本
pub const WAL_VERSION: u32 = 1;

/// Default max file size (256MB, ref RocksDB) / 默认最大文件大小
const DEFAULT_MAX_SIZE: u64 = 256 * 1024 * 1024;
/// Default head cache capacity (ref RocksDB block_cache) / 默认头缓存容量
const DEFAULT_HEAD_CACHE_CAP: usize = 8192;
/// Default data cache capacity / 默认数据缓存容量
const DEFAULT_DATA_CACHE_CAP: usize = 1024;
/// Default file cache capacity / 默认文件缓存容量
const DEFAULT_FILE_CACHE_CAP: usize = 64;

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

/// Build WAL file header (12 bytes) / 构建 WAL 文件头
/// [Version u32] [Version u32 copy] [CRC32 of first 4B]
#[inline]
fn build_header() -> [u8; HEADER_SIZE] {
  let mut buf = [0u8; HEADER_SIZE];
  let ver = WAL_VERSION.to_le_bytes();
  buf[0..4].copy_from_slice(&ver);
  buf[4..8].copy_from_slice(&ver);
  let crc = crc32fast::hash(&buf[0..4]);
  buf[8..12].copy_from_slice(&crc.to_le_bytes());
  buf
}

/// Header check result / 头校验结果
enum HeaderState {
  /// Valid, no repair needed / 有效，无需修复
  Ok(u32),
  /// Repaired in place / 已原地修复
  Repaired(u32),
  /// Cannot repair / 无法修复
  Invalid,
}

/// Check and repair header / 校验并修复头
#[inline]
fn check_header(buf: &mut [u8]) -> HeaderState {
  if buf.len() < HEADER_SIZE {
    return HeaderState::Invalid;
  }

  let ver1 = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
  let ver2 = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
  let stored_crc = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
  let crc1 = crc32fast::hash(&buf[0..4]);

  // Case 1: all valid / 全部正确
  if ver1 == ver2 && crc1 == stored_crc {
    return HeaderState::Ok(ver1);
  }

  // Case 2: ver1 + crc valid, fix ver2 / ver1 + crc 正确，修复 ver2
  if crc1 == stored_crc {
    let v = ver1.to_le_bytes();
    buf[4..8].copy_from_slice(&v);
    return HeaderState::Repaired(ver1);
  }

  // Case 3: ver2 + crc valid, fix ver1 / ver2 + crc 正确，修复 ver1
  let crc2 = crc32fast::hash(&buf[4..8]);
  if crc2 == stored_crc {
    let v = ver2.to_le_bytes();
    buf[0..4].copy_from_slice(&v);
    return HeaderState::Repaired(ver2);
  }

  // Case 4: ver1 == ver2, fix crc / ver1 == ver2，修复 crc
  if ver1 == ver2 {
    buf[8..12].copy_from_slice(&crc1.to_le_bytes());
    return HeaderState::Repaired(ver1);
  }

  HeaderState::Invalid
}

/// Storage mode / 存储模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
  Inline,
  Infile,
  File,
}

/// Select storage mode by data size / 根据数据大小选择存储模式
#[inline]
fn select_mode(key_len: usize, val_len: usize) -> (Mode, Mode) {
  let k_mode = if key_len <= Head::MAX_KEY_INLINE {
    Mode::Inline
  } else if key_len <= INFILE_MAX {
    Mode::Infile
  } else {
    Mode::File
  };

  let v_mode = if key_len + val_len <= Head::MAX_BOTH_INLINE {
    Mode::Inline
  } else if val_len <= Head::MAX_VAL_INLINE && k_mode != Mode::Inline {
    Mode::Inline
  } else if val_len <= INFILE_MAX {
    Mode::Infile
  } else {
    Mode::File
  };

  (k_mode, v_mode)
}

/// WAL subdirectory name / WAL 子目录名
const WAL_SUBDIR: &str = "wal";
/// Bin subdirectory name / Bin 子目录名
const BIN_SUBDIR: &str = "bin";

// Log messages / 日志消息
const LOG_HEADER_SMALL: &str = "WAL file too small";
const LOG_HEADER_OK: &str = "WAL header ok";
const LOG_HEADER_REPAIR: &str = "WAL header corrupted, repairing";
const LOG_HEADER_INVALID: &str = "WAL header invalid";
const LOG_SCAN_SMALL: &str = "WAL file too small for scan";
const LOG_SCAN_INVALID: &str = "WAL header invalid for scan";

/// WAL manager / WAL 管理器
pub struct Wal {
  wal_dir: PathBuf,
  bin_dir: PathBuf,
  cur_id: u64,
  cur_file: Option<File>,
  cur_pos: u64,
  max_size: u64,
  gen_id: GenId,
  head_cache: LruCache<Loc, Head>,
  file_cache: LruCache<u64, File>,
  data_cache: LruCache<Loc, Vec<u8>>,
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

  /// Open or create current WAL file / 打开或创建当前 WAL 文件
  pub async fn open(&mut self) -> Result<()> {
    fs::create_dir_all(&self.wal_dir)?;
    self.cur_id = self.iter_valid().await.unwrap_or(0);

    loop {
      let path = self.wal_path(self.cur_id);

      if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
      }

      let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .await?;

      let meta = file.metadata().await?;
      let len = meta.len();

      if len == 0 {
        // New file, write header / 新文件，写入头
        file.write_all_at(build_header().to_vec(), 0).await.0?;
        self.cur_pos = HEADER_SIZE as u64;
        self.cur_file = Some(file);
        return Ok(());
      }

      if len < HEADER_SIZE as u64 {
        // File too small, skip / 文件太小，跳过
        warn!("{LOG_HEADER_SMALL}: {path:?}, len={len}");
        self.cur_id += 1;
        continue;
      }

      // Read and check header / 读取并校验头
      let mut buf = vec![0u8; HEADER_SIZE];
      let res = file.read_exact_at(buf, 0).await;
      res.0?;
      buf = res.1;

      match check_header(&mut buf) {
        HeaderState::Ok(ver) => {
          log::debug!("{LOG_HEADER_OK}: {path:?}, ver={ver}");
        }
        HeaderState::Repaired(ver) => {
          warn!("{LOG_HEADER_REPAIR}: {path:?}, ver={ver}");
          file.write_all_at(buf, 0).await.0?;
          file.sync_all().await?;
        }
        HeaderState::Invalid => {
          warn!("{LOG_HEADER_INVALID}: {path:?}");
          self.cur_id += 1;
          continue;
        }
      }
      self.cur_pos = len;
      self.cur_file = Some(file);
      return Ok(());
    }
  }

  async fn iter_valid(&self) -> Option<u64> {
    let entries = match fs::read_dir(&self.wal_dir) {
      Ok(e) => e,
      Err(_) => return None,
    };

    let mut max_id = None;
    for entry in entries.flatten() {
      let name = entry.file_name();
      let Some(name) = name.to_str() else { continue };
      let Ok(id) = CROCKFORD_LOWER.decode_u64(name.as_bytes()) else {
        continue;
      };

      let path = entry.path();
      let Ok(mut file) = OpenOptions::new().read(true).write(true).open(&path).await else {
        continue;
      };

      let Ok(meta) = file.metadata().await else {
        continue;
      };

      if meta.len() < HEADER_SIZE as u64 {
        warn!("{LOG_HEADER_SMALL}: {path:?}");
        continue;
      }

      let mut buf = vec![0u8; HEADER_SIZE];
      let res = file.read_exact_at(buf, 0).await;
      if res.0.is_err() {
        continue;
      }
      buf = res.1;

      match check_header(&mut buf) {
        HeaderState::Ok(ver) => {
          log::debug!("{LOG_HEADER_OK}: {path:?}, ver={ver}");
          max_id = Some(max_id.map_or(id, |m: u64| m.max(id)));
        }
        HeaderState::Repaired(ver) => {
          warn!("{LOG_HEADER_REPAIR}: {path:?}, ver={ver}");
          if file.write_all_at(buf, 0).await.0.is_ok() {
            let _ = file.sync_all().await;
          }
          max_id = Some(max_id.map_or(id, |m: u64| m.max(id)));
        }
        HeaderState::Invalid => warn!("{LOG_HEADER_INVALID}: {path:?}"),
      }
    }
    max_id
  }

  /// Rotate to new WAL file / 轮转到新 WAL 文件
  ///
  /// Triggered when cur_pos + data_len > max_size
  /// 当 cur_pos + 数据长度 > max_size 时触发
  async fn rotate(&mut self) -> Result<()> {
    self.cur_id += 1;
    let path = self.wal_path(self.cur_id);

    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    file.write_all_at(build_header().to_vec(), 0).await.0?;
    self.cur_file = Some(file);
    self.cur_pos = HEADER_SIZE as u64;
    Ok(())
  }

  /// Put key-value with auto mode selection / 自动选择模式写入键值
  pub async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<Loc> {
    let (k_mode, v_mode) = select_mode(key.len(), val.len());
    let k_len = key.len() as u32;
    let v_len = val.len() as u32;

    let (key_flag, key_loc) = match k_mode {
      Mode::Inline => (Flag::INLINE, Loc::default()),
      Mode::Infile => (Flag::INFILE, self.write_data(key).await?),
      Mode::File => {
        let id = self.gen_id.next();
        self.write_file(id, key).await?;
        (Flag::FILE, Loc::new(id, 0))
      }
    };

    let (val_flag, val_loc, val_crc) = match v_mode {
      Mode::Inline => (Flag::INLINE, Loc::default(), 0),
      Mode::Infile => {
        let crc = crc32fast::hash(val);
        let loc = self.write_data(val).await?;
        (Flag::INFILE, loc, crc)
      }
      Mode::File => {
        let crc = crc32fast::hash(val);
        let id = self.gen_id.next();
        self.write_file(id, val).await?;
        (Flag::FILE, Loc::new(id, 0), crc)
      }
    };

    let head = match (k_mode, v_mode) {
      (Mode::Inline, Mode::Inline) => Head::both_inline(key, val)?,
      (Mode::Inline, _) => Head::key_inline(key, val_flag, val_loc, v_len, val_crc)?,
      (_, Mode::Inline) => Head::val_inline(key_flag, key_loc, k_len, val)?,
      (_, _) => Head::both_file(key_flag, key_loc, k_len, val_flag, val_loc, v_len, val_crc)?,
    };

    self.write_head(&head).await
  }

  async fn write_head(&mut self, head: &Head) -> Result<Loc> {
    if self.cur_pos + Head::SIZE as u64 > self.max_size {
      self.rotate().await?;
    }

    let file = self.cur_file.as_mut().ok_or(Error::NotOpen)?;
    let pos = self.cur_pos;

    file.write_all_at(head.as_bytes().to_vec(), pos).await.0?;
    self.cur_pos += Head::SIZE as u64;
    Ok(Loc::new(self.cur_id, pos))
  }

  async fn write_data(&mut self, data: &[u8]) -> Result<Loc> {
    let len = data.len() as u64;
    if self.cur_pos + len > self.max_size {
      self.rotate().await?;
    }

    let file = self.cur_file.as_mut().ok_or(Error::NotOpen)?;
    let pos = self.cur_pos;

    file.write_all_at(data.to_vec(), pos).await.0?;
    self.cur_pos += len;
    Ok(Loc::new(self.cur_id, pos))
  }

  async fn write_file(&self, id: u64, data: &[u8]) -> Result<()> {
    let path = self.bin_path(id);
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await?;

    file.write_all_at(data.to_vec(), 0).await.0?;
    file.sync_all().await?;
    Ok(())
  }

  /// Read head at location / 在位置读取头
  pub async fn read_head(&mut self, loc: Loc) -> Result<Head> {
    if let Some(head) = self.head_cache.get(&loc) {
      return Ok(*head);
    }

    let buf = vec![0u8; Head::SIZE];
    let res = if loc.id() == self.cur_id {
      let file = self.cur_file.as_ref().ok_or(Error::NotOpen)?;
      file.read_exact_at(buf, loc.pos()).await
    } else {
      let file = self.get_file(loc.id()).await?;
      file.read_exact_at(buf, loc.pos()).await
    };
    res.0?;
    let head = Head::read_from_bytes(&res.1).map_err(|_| Error::InvalidHead)?;
    self.head_cache.insert(loc, head);
    Ok(head)
  }

  async fn get_file(&mut self, id: u64) -> Result<&File> {
    if !self.file_cache.contains_key(&id) {
      let path = self.wal_path(id);
      let file = OpenOptions::new().read(true).open(&path).await?;
      self.file_cache.insert(id, file);
    }
    Ok(self.file_cache.get(&id).unwrap())
  }

  /// Read data from WAL file / 从 WAL 文件读取数据
  pub async fn read_data(&mut self, loc: Loc, len: usize) -> Result<Vec<u8>> {
    if let Some(data) = self.data_cache.get(&loc) {
      return Ok(data.clone());
    }

    let buf = vec![0u8; len];
    let res = if loc.id() == self.cur_id {
      let file = self.cur_file.as_ref().ok_or(Error::NotOpen)?;
      file.read_exact_at(buf, loc.pos()).await
    } else {
      let file = self.get_file(loc.id()).await?;
      file.read_exact_at(buf, loc.pos()).await
    };
    res.0?;
    self.data_cache.insert(loc, res.1.clone());
    Ok(res.1)
  }

  /// Read data from separate file / 从独立文件读取数据
  pub async fn read_file(&self, id: u64) -> Result<Vec<u8>> {
    let path = self.bin_path(id);
    let file = OpenOptions::new().read(true).open(&path).await?;
    let meta = file.metadata().await?;
    let len = meta.len() as usize;

    let buf = vec![0u8; len];
    let res = file.read_exact_at(buf, 0).await;
    res.0?;
    Ok(res.1)
  }

  /// Get key by head / 根据头获取键
  pub async fn get_key(&mut self, head: &Head) -> Result<Vec<u8>> {
    if head.key_flag.is_inline() {
      Ok(head.key_data().to_vec())
    } else {
      let loc = head.key_loc();
      if head.key_flag.is_infile() {
        let len = head.key_len.get() as usize;
        self.read_data(loc, len).await
      } else {
        self.read_file(loc.id()).await
      }
    }
  }

  /// Get val by head / 根据头获取值
  pub async fn get_val(&mut self, head: &Head) -> Result<Vec<u8>> {
    if head.val_flag.is_inline() {
      Ok(head.val_data().to_vec())
    } else {
      let loc = head.val_loc();
      let data = if head.val_flag.is_infile() {
        let len = head.val_len.get() as usize;
        self.read_data(loc, len).await?
      } else {
        self.read_file(loc.id()).await?
      };
      let crc = crc32fast::hash(&data);
      if crc != head.val_crc32() {
        return Err(Error::CrcMismatch(head.val_crc32(), crc));
      }
      Ok(data)
    }
  }

  /// Scan all entries in a WAL file / 扫描 WAL 文件中的所有条目
  pub async fn scan<F>(&self, id: u64, mut f: F) -> Result<()>
  where
    F: FnMut(u64, &Head) -> bool,
  {
    let path = self.wal_path(id);
    let file = OpenOptions::new().read(true).open(&path).await?;
    let meta = file.metadata().await?;
    let len = meta.len();

    if len < HEADER_SIZE as u64 {
      warn!("{LOG_SCAN_SMALL}: {path:?}");
      return Err(Error::InvalidHeader);
    }

    let mut buf = vec![0u8; HEADER_SIZE];
    let res = file.read_exact_at(buf, 0).await;
    res.0?;
    buf = res.1;
    if matches!(check_header(&mut buf), HeaderState::Invalid) {
      warn!("{LOG_SCAN_INVALID}: {path:?}");
      return Err(Error::InvalidHeader);
    }

    let mut pos = HEADER_SIZE as u64;
    let mut buf = vec![0u8; Head::SIZE];

    while pos + Head::SIZE as u64 <= len {
      let res = file.read_exact_at(buf, pos).await;
      res.0?;
      buf = res.1;

      let head = Head::read_from_bytes(&buf).map_err(|_| Error::InvalidHead)?;

      if !f(pos, &head) {
        break;
      }

      let data_len = if head.key_flag.is_infile() {
        head.key_len.get() as u64
      } else {
        0
      } + if head.val_flag.is_infile() {
        head.val_len.get() as u64
      } else {
        0
      };

      pos += Head::SIZE as u64 + data_len;
    }

    Ok(())
  }

  /// GC a WAL file, rewrite live entries / 对 WAL 文件进行 GC，重写有效条目
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
