//! Page store for B+ Tree persistence
//! B+ Tree 持久化页存储

#![cfg_attr(docsrs, feature(doc_cfg))]

mod error;

use std::path::Path;

use jdb_alloc::{AlignedBuf, PAGE_SIZE};
use jdb_fs::File;

pub use error::{Error, Result};

/// Page ID (0 = null) / 页 ID (0 = 空)
pub type PageId = u64;

/// Null page ID / 空页 ID
pub const NULL_PAGE: PageId = 0;

/// Page header size / 页头大小
const HEADER_SIZE: usize = 16;

/// Magic number for page file / 页文件魔数
const MAGIC: u32 = 0x4A444250; // "JDBP"

/// Page header layout / 页头布局
/// ```text
/// ┌────────────┬────────────┬────────────┐
/// │ crc32 (4B) │ flags (2B) │ _pad (10B) │
/// └────────────┴────────────┴────────────┘
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PageHeader {
  pub crc: u32,
  pub flags: u16,
  pub _pad: [u8; 10],
}

impl PageHeader {
  fn encode(&self, buf: &mut [u8]) {
    buf[0..4].copy_from_slice(&self.crc.to_le_bytes());
    buf[4..6].copy_from_slice(&self.flags.to_le_bytes());
    buf[6..16].fill(0);
  }

  fn decode(buf: &[u8]) -> Self {
    Self {
      crc: u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]),
      flags: u16::from_le_bytes([buf[4], buf[5]]),
      _pad: [0; 10],
    }
  }
}

/// Page store / 页存储
pub struct PageStore {
  file: File,
  page_count: u64,
  free_list: Vec<PageId>,
}

impl PageStore {
  /// Open or create page store / 打开或创建页存储
  pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
    let path = path.as_ref();

    if jdb_fs::exists(path) {
      Self::open_existing(path).await
    } else {
      Self::create_new(path).await
    }
  }

  async fn create_new(path: &Path) -> Result<Self> {
    // Create parent dir / 创建父目录
    if let Some(parent) = path.parent() {
      jdb_fs::mkdir(parent).await?;
    }

    let file = File::create(path).await?;

    // Write file header / 写入文件头
    let mut header = AlignedBuf::page()?;
    header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    header[4..12].copy_from_slice(&1u64.to_le_bytes()); // page_count = 1 (header page)
    file.write_at(header, 0).await?;
    file.sync_data().await?;

    Ok(Self {
      file,
      page_count: 1,
      free_list: Vec::new(),
    })
  }

  async fn open_existing(path: &Path) -> Result<Self> {
    let file = File::open_rw(path).await?;

    // Read file header / 读取文件头
    let header = AlignedBuf::page()?;
    let header = file.read_at(header, 0).await?;

    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if magic != MAGIC {
      return Err(Error::InvalidPage("bad magic"));
    }

    let page_count = u64::from_le_bytes([
      header[4], header[5], header[6], header[7], header[8], header[9], header[10], header[11],
    ]);

    Ok(Self {
      file,
      page_count,
      free_list: Vec::new(),
    })
  }

  /// Allocate new page / 分配新页
  pub fn alloc(&mut self) -> PageId {
    if let Some(id) = self.free_list.pop() {
      id
    } else {
      let id = self.page_count;
      self.page_count += 1;
      id
    }
  }

  /// Free page / 释放页
  pub fn free(&mut self, id: PageId) {
    if id != NULL_PAGE {
      self.free_list.push(id);
    }
  }

  /// Read page / 读取页
  pub async fn read(&self, id: PageId) -> Result<AlignedBuf> {
    if id == NULL_PAGE || id >= self.page_count {
      return Err(Error::NotFound(id));
    }

    let offset = id * PAGE_SIZE as u64;
    let buf = AlignedBuf::page()?;
    let buf = self.file.read_at(buf, offset).await?;

    // Verify checksum / 校验 CRC
    let header = PageHeader::decode(&buf[..HEADER_SIZE]);
    let data_crc = crc32(&buf[HEADER_SIZE..]);
    if header.crc != data_crc {
      return Err(Error::Checksum {
        expected: header.crc,
        got: data_crc,
      });
    }

    Ok(buf)
  }

  /// Write page / 写入页
  pub async fn write(&self, id: PageId, buf: &mut AlignedBuf) -> Result<()> {
    if id == NULL_PAGE {
      return Err(Error::InvalidPage("cannot write to null page"));
    }

    // Compute and write checksum / 计算并写入 CRC
    let data_crc = crc32(&buf[HEADER_SIZE..]);
    let header = PageHeader {
      crc: data_crc,
      flags: 0,
      _pad: [0; 10],
    };
    header.encode(&mut buf[..HEADER_SIZE]);

    let offset = id * PAGE_SIZE as u64;
    // SAFETY: buf 生命周期足够
    let raw = unsafe { buf.as_raw() };
    self.file.write_at(raw, offset).await?;

    Ok(())
  }

  /// Sync to disk / 同步到磁盘
  pub async fn sync(&self) -> Result<()> {
    // Update file header / 更新文件头
    let mut header = AlignedBuf::page()?;
    header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    header[4..12].copy_from_slice(&self.page_count.to_le_bytes());
    self.file.write_at(header, 0).await?;
    self.file.sync_data().await?;
    Ok(())
  }

  /// Get page count / 获取页数
  pub fn page_count(&self) -> u64 {
    self.page_count
  }

  /// Get usable data size per page / 获取每页可用数据大小
  pub const fn data_size() -> usize {
    PAGE_SIZE - HEADER_SIZE
  }
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

/// Get page data slice (skip header) / 获取页数据切片（跳过页头）
#[inline]
pub fn page_data(buf: &AlignedBuf) -> &[u8] {
  &buf[HEADER_SIZE..]
}

/// Get mutable page data slice / 获取可变页数据切片
#[inline]
pub fn page_data_mut(buf: &mut AlignedBuf) -> &mut [u8] {
  &mut buf[HEADER_SIZE..]
}
