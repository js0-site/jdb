#![cfg_attr(docsrs, feature(doc_cfg))]

//! 值日志 Value log for KV separation

use std::path::{Path, PathBuf};

use jdb_alloc::AlignedBuf;
use jdb_compress::{dec, enc, Codec};
use jdb_fs::File;
use jdb_layout::{crc32, BlobPtr};

use crate::consts::{HEADER, PAGE_SIZE};
use crate::error::{E, R};

/// VLog 写入器 VLog writer
pub struct Writer {
  dir: PathBuf,
  file: File,
  file_id: u32,
  offset: u64,
  buf: AlignedBuf,
  pos: usize,
  codec: Option<Codec>,
}

impl Writer {
  /// 创建新 VLog 文件 Create new VLog file
  pub async fn create(dir: impl AsRef<Path>, file_id: u32, codec: Option<Codec>) -> R<Self> {
    let dir = dir.as_ref().to_path_buf();
    let path = vlog_path(&dir, file_id);
    let file = File::create(&path).await?;
    Ok(Self {
      dir,
      file,
      file_id,
      offset: 0,
      buf: AlignedBuf::zeroed(PAGE_SIZE),
      pos: 0,
      codec,
    })
  }

  /// 打开已有 VLog 文件 Open existing VLog file
  pub async fn open(dir: impl AsRef<Path>, file_id: u32, codec: Option<Codec>) -> R<Self> {
    let dir = dir.as_ref().to_path_buf();
    let path = vlog_path(&dir, file_id);
    let file = File::open_rw(&path).await?;
    let offset = file.size().await?;
    Ok(Self {
      dir,
      file,
      file_id,
      offset,
      buf: AlignedBuf::zeroed(PAGE_SIZE),
      pos: 0,
      codec,
    })
  }

  /// 追加数据 Append data, returns BlobPtr
  pub async fn append(&mut self, data: &[u8]) -> R<BlobPtr> {
    // 压缩 Compress
    let data = match &self.codec {
      Some(c) => enc(*c, data),
      None => data.to_vec(),
    };

    let record_len = HEADER + data.len();

    // 超过单页，直接写入
    if record_len > PAGE_SIZE {
      self.flush().await?;
      return self.write_large(&data).await;
    }

    // 缓冲区空间不足
    if self.pos + record_len > PAGE_SIZE {
      self.flush().await?;
    }

    let ptr = BlobPtr::new(self.file_id, self.offset + self.pos as u64, data.len() as u32);

    // 写入 header: len(4) + crc(4)
    let crc = crc32(&data);
    self.buf[self.pos..self.pos + 4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    self.buf[self.pos + 4..self.pos + 8].copy_from_slice(&crc.to_le_bytes());
    self.buf[self.pos + 8..self.pos + record_len].copy_from_slice(&data);

    self.pos += record_len;
    Ok(ptr)
  }

  /// 写入大数据 Write large data
  async fn write_large(&mut self, data: &[u8]) -> R<BlobPtr> {
    let ptr = BlobPtr::new(self.file_id, self.offset, data.len() as u32);

    // 写入 header
    let mut header = [0u8; HEADER];
    header[0..4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    header[4..8].copy_from_slice(&crc32(data).to_le_bytes());

    let mut buf = AlignedBuf::zeroed(HEADER + data.len());
    buf[..HEADER].copy_from_slice(&header);
    buf[HEADER..].copy_from_slice(data);

    let buf = self.file.write_at(self.offset, buf).await?;
    self.offset += buf.len() as u64;

    Ok(ptr)
  }

  /// 刷新缓冲区 Flush buffer
  pub async fn flush(&mut self) -> R<()> {
    if self.pos == 0 {
      return Ok(());
    }

    let buf = std::mem::replace(&mut self.buf, AlignedBuf::zeroed(PAGE_SIZE));
    let buf = self.file.write_at(self.offset, buf).await?;

    self.offset += PAGE_SIZE as u64;
    self.pos = 0;
    self.buf = buf;
    self.buf.fill(0);

    Ok(())
  }

  /// 同步到磁盘 Sync to disk
  pub async fn sync(&mut self) -> R<()> {
    self.flush().await?;
    self.file.sync().await?;
    Ok(())
  }

  /// 滚动到新文件 Roll to new file
  pub async fn roll(&mut self) -> R<()> {
    self.sync().await?;
    self.file_id += 1;
    let path = vlog_path(&self.dir, self.file_id);
    self.file = File::create(&path).await?;
    self.offset = 0;
    Ok(())
  }

  /// 当前文件 ID Current file ID
  #[inline]
  pub fn file_id(&self) -> u32 {
    self.file_id
  }

  /// 当前偏移 Current offset
  #[inline]
  pub fn offset(&self) -> u64 {
    self.offset + self.pos as u64
  }

  /// 是否需要滚动 Should roll
  #[inline]
  pub fn should_roll(&self, max_size: u64) -> bool {
    self.offset() >= max_size
  }
}

/// VLog 读取器 VLog reader
pub struct Reader {
  dir: PathBuf,
  codec: Option<Codec>,
}

impl Reader {
  /// 创建读取器 Create reader
  pub fn new(dir: impl AsRef<Path>, codec: Option<Codec>) -> Self {
    Self {
      dir: dir.as_ref().to_path_buf(),
      codec,
    }
  }

  /// 读取数据 Read data
  pub async fn read(&self, ptr: &BlobPtr) -> R<Vec<u8>> {
    if !ptr.is_valid() {
      return Err(E::NotFound);
    }

    let path = vlog_path(&self.dir, ptr.file_id);
    let file = File::open(&path).await?;

    // 读取 header + data
    let buf = file.read_at(ptr.offset, HEADER + ptr.len as usize).await?;

    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let data = &buf[HEADER..HEADER + len];

    // CRC 校验
    if crc32(data) != crc {
      return Err(E::Checksum(crc, crc32(data)));
    }

    // 解压 Decompress
    match &self.codec {
      Some(c) => Ok(dec(*c, data)?),
      None => Ok(data.to_vec()),
    }
  }
}

/// VLog 文件路径 VLog file path
#[inline]
fn vlog_path(dir: &Path, file_id: u32) -> PathBuf {
  dir.join(format!("{file_id:08}.vlog"))
}
