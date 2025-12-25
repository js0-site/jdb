//! Value Log for KV separation
//! KV 分离的值日志

#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;

use std::path::{Path, PathBuf};

use bytes::Bytes;
use jdb_alloc::{AlignedBuf, PAGE_SIZE};
use jdb_fs::File;
use jdb_trait::ValRef;

pub use error::{Error, Result};

/// Tombstone flag in offset / offset 中的 tombstone 标记
const TOMBSTONE_FLAG: u64 = 1 << 63;

/// Record flag: normal value / 记录标记：正常值
const FLAG_VALUE: u8 = 0;

/// Record flag: tombstone / 记录标记：墓碑
const FLAG_TOMBSTONE: u8 = 1;

/// Max file size before rotation (256MB) / 轮转前最大文件大小
const MAX_FILE_SIZE: u64 = 256 * 1024 * 1024;

/// File extension / 文件扩展名
const EXT: &str = "vlog";

/// Value Log / 值日志
pub struct VLog {
  dir: PathBuf,
  active_id: u64,
  active: File,
  active_size: u64,
}

impl VLog {
  /// Open or create VLog / 打开或创建 VLog
  pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
    let dir = dir.as_ref().to_path_buf();
    jdb_fs::mkdir(&dir).await?;

    // Find max file id / 查找最大文件 ID
    let files = jdb_fs::ls(&dir).await?;
    let mut max_id = 0u64;
    for f in &files {
      if let Some(name) = f.file_name().and_then(|n| n.to_str()) {
        if let Some(id_str) = name.strip_suffix(&format!(".{EXT}")) {
          if let Ok(id) = id_str.parse::<u64>() {
            max_id = max_id.max(id);
          }
        }
      }
    }

    // Open or create active file / 打开或创建活跃文件
    let active_id = if max_id == 0 { 1 } else { max_id };
    let active_path = Self::file_path(&dir, active_id);
    let (active, active_size) = if jdb_fs::exists(&active_path) {
      let f = File::open_rw(&active_path).await?;
      let size = f.size().await?;
      (f, size)
    } else {
      let f = File::create(&active_path).await?;
      (f, 0)
    };

    Ok(Self {
      dir,
      active_id,
      active,
      active_size,
    })
  }

  fn file_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:08}.{EXT}"))
  }

  /// Append value / 追加值
  pub async fn append(
    &mut self,
    key: &[u8],
    val: &[u8],
    prev: Option<&ValRef>,
  ) -> Result<ValRef> {
    self.append_inner(key, Some(val), prev, FLAG_VALUE).await
  }

  /// Append tombstone / 追加墓碑
  pub async fn append_tombstone(
    &mut self,
    key: &[u8],
    prev: Option<&ValRef>,
  ) -> Result<ValRef> {
    self.append_inner(key, None, prev, FLAG_TOMBSTONE).await
  }

  async fn append_inner(
    &mut self,
    key: &[u8],
    val: Option<&[u8]>,
    prev: Option<&ValRef>,
    flag: u8,
  ) -> Result<ValRef> {
    // Check rotation / 检查轮转
    if self.active_size >= MAX_FILE_SIZE {
      self.rotate().await?;
    }

    // Encode record / 编码记录
    let val_len = val.map(|v| v.len()).unwrap_or(0);
    let record_len = 8 + 4 + 1 + varint_len(key.len()) + key.len() + val_len;
    let aligned_len = align_up(record_len);

    let mut buf = AlignedBuf::zeroed(aligned_len)?;

    // len (8B) / 长度
    buf[0..8].copy_from_slice(&(record_len as u64).to_le_bytes());

    // flag (1B) / 标记
    buf[12] = flag;

    // key_len (varint) + key / 键长度 + 键
    let mut pos = 13;
    pos += encode_varint(key.len(), &mut buf[pos..]);
    buf[pos..pos + key.len()].copy_from_slice(key);
    pos += key.len();

    // value / 值
    if let Some(v) = val {
      buf[pos..pos + v.len()].copy_from_slice(v);
    }

    // crc32 (4B) / CRC32
    let crc = crc32(&buf[12..record_len]);
    buf[8..12].copy_from_slice(&crc.to_le_bytes());

    // Write / 写入
    let offset = self.active_size;
    self.active.write_at(buf, offset).await?;
    self.active_size += aligned_len as u64;

    // Build ValRef / 构建 ValRef
    let mut result_offset = offset;
    if flag == FLAG_TOMBSTONE {
      result_offset |= TOMBSTONE_FLAG;
    }

    Ok(ValRef {
      file_id: self.active_id,
      offset: result_offset,
      prev_file_id: prev.map(|p| p.file_id).unwrap_or(0),
      prev_offset: prev.map(|p| p.offset).unwrap_or(0),
    })
  }

  /// Get value / 获取值
  pub async fn get(&self, vref: &ValRef) -> Result<Option<Bytes>> {
    if vref.is_tombstone() {
      return Ok(None);
    }

    let file = self.open_file(vref.file_id).await?;
    let offset = vref.real_offset();

    // Read header / 读取头部
    let header_buf = AlignedBuf::zeroed(PAGE_SIZE)?;
    let header_buf = file.read_at(header_buf, offset).await?;

    let len = u64::from_le_bytes([
      header_buf[0],
      header_buf[1],
      header_buf[2],
      header_buf[3],
      header_buf[4],
      header_buf[5],
      header_buf[6],
      header_buf[7],
    ]) as usize;

    let crc_stored = u32::from_le_bytes([
      header_buf[8],
      header_buf[9],
      header_buf[10],
      header_buf[11],
    ]);

    // Verify CRC / 校验 CRC
    let crc_calc = crc32(&header_buf[12..len]);
    if crc_stored != crc_calc {
      return Err(Error::Crc {
        expected: crc_stored,
        got: crc_calc,
      });
    }

    let flag = header_buf[12];
    if flag == FLAG_TOMBSTONE {
      return Ok(None);
    }

    // Decode key_len / 解码键长度
    let (key_len, key_len_size) = decode_varint(&header_buf[13..])?;
    let val_start = 13 + key_len_size + key_len;
    let val_end = len;

    Ok(Some(Bytes::copy_from_slice(&header_buf[val_start..val_end])))
  }

  async fn open_file(&self, file_id: u64) -> Result<File> {
    let path = Self::file_path(&self.dir, file_id);
    if !jdb_fs::exists(&path) {
      return Err(Error::FileNotFound(file_id));
    }
    Ok(File::open(&path).await?)
  }

  /// Rotate to new file / 轮转到新文件
  pub async fn rotate(&mut self) -> Result<()> {
    self.active.sync_data().await?;
    self.active_id += 1;
    let path = Self::file_path(&self.dir, self.active_id);
    self.active = File::create(&path).await?;
    self.active_size = 0;
    Ok(())
  }

  /// Sync to disk / 同步到磁盘
  pub async fn sync(&self) -> Result<()> {
    self.active.sync_data().await?;
    Ok(())
  }

  /// Get active file id / 获取活跃文件 ID
  pub fn active_id(&self) -> u64 {
    self.active_id
  }

  /// Get active file size / 获取活跃文件大小
  pub fn active_size(&self) -> u64 {
    self.active_size
  }
}

/// Align up to PAGE_SIZE / 向上对齐到 PAGE_SIZE
#[inline]
fn align_up(n: usize) -> usize {
  (n + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
}

/// Varint encoded length / Varint 编码长度
#[inline]
fn varint_len(n: usize) -> usize {
  if n < 128 {
    1
  } else if n < 16384 {
    2
  } else if n < 2097152 {
    3
  } else {
    4
  }
}

/// Encode varint / 编码 varint
fn encode_varint(mut n: usize, buf: &mut [u8]) -> usize {
  let mut i = 0;
  while n >= 128 {
    buf[i] = (n as u8) | 0x80;
    n >>= 7;
    i += 1;
  }
  buf[i] = n as u8;
  i + 1
}

/// Decode varint / 解码 varint
fn decode_varint(buf: &[u8]) -> Result<(usize, usize)> {
  let mut n = 0usize;
  let mut shift = 0;
  for (i, &b) in buf.iter().enumerate() {
    n |= ((b & 0x7F) as usize) << shift;
    if b & 0x80 == 0 {
      return Ok((n, i + 1));
    }
    shift += 7;
    if shift > 28 {
      return Err(Error::InvalidRecord);
    }
  }
  Err(Error::InvalidRecord)
}

/// CRC32 (IEEE polynomial) / CRC32 校验
fn crc32(data: &[u8]) -> u32 {
  let mut crc = 0xFFFF_FFFFu32;
  for &byte in data {
    crc ^= byte as u32;
    for _ in 0..8 {
      crc = if crc & 1 != 0 {
        (crc >> 1) ^ 0xEDB8_8320
      } else {
        crc >> 1
      };
    }
  }
  !crc
}
