//! Vlog writer/reader Vlog 读写器

use jdb_alloc::AlignedBuf;
use jdb_fs::File;
use jdb_layout::{crc32, BlobHeader, BlobPtr, BLOB_HEADER_SIZE};
use std::path::Path;

// Page size constant - 4KB
pub const PAGE_SIZE: usize = 4096;

// Result type alias
pub type JdbResult<T> = Result<T, JdbError>;

// Error types
#[derive(Debug, thiserror::Error)]
pub enum JdbError {
  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),
  #[error("Checksum mismatch: expected {expected}, actual {actual}")]
  Checksum { expected: u32, actual: u32 },
  #[error("Page size mismatch: expected {expected}, actual {actual}")]
  PageSizeMismatch { expected: usize, actual: usize },
}

/// Vlog writer (append-only) Vlog 写入器（追加写）
pub struct VlogWriter {
  file: File,
  file_id: u32,
  offset: u64,
}

impl VlogWriter {
  /// Create new vlog file 创建新 vlog 文件
  pub async fn create(path: impl AsRef<Path>, file_id: u32) -> JdbResult<Self> {
    let file = File::create(path).await?;
    Ok(Self {
      file,
      file_id,
      offset: 0,
    })
  }

  /// Open existing vlog 打开已有 vlog
  pub async fn open(path: impl AsRef<Path>, file_id: u32) -> JdbResult<Self> {
    let file = File::open_rw(path).await?;
    let offset = file.size().await?;
    Ok(Self {
      file,
      file_id,
      offset,
    })
  }

  /// Current offset 当前偏移
  #[inline]
  pub fn offset(&self) -> u64 {
    self.offset
  }

  /// File ID 文件 ID
  #[inline]
  pub fn file_id(&self) -> u32 {
    self.file_id
  }

  /// Append blob, returns pointer 追加 blob，返回指针
  pub async fn append(&mut self, data: &[u8], ts: u64) -> JdbResult<BlobPtr> {
    let len = data.len() as u32;
    let checksum = crc32(data);
    let hdr = BlobHeader::new(len, checksum, ts);

    // Build buffer: header + data + padding 构建缓冲区：头 + 数据 + 填充
    let total = BLOB_HEADER_SIZE + data.len();
    let aligned = ((total + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;

    let mut buf = AlignedBuf::with_cap(aligned);

    // Write header 写入头
    let mut hdr_buf = [0u8; BLOB_HEADER_SIZE];
    hdr.write(&mut hdr_buf);
    buf.extend(&hdr_buf);

    // Write data 写入数据
    buf.extend(data);

    // Pad to page boundary 填充到页边界
    let pad = aligned - total;
    if pad > 0 {
      let zeros = vec![0u8; pad];
      buf.extend(&zeros);
    }

    let ptr = BlobPtr::new(self.file_id, self.offset, len);

    let _ = self.file.write_at(self.offset, buf).await?;
    self.offset += aligned as u64;

    Ok(ptr)
  }

  /// Sync to disk 同步到磁盘
  pub async fn sync(&mut self) -> JdbResult<()> {
    self.file.sync().await
  }
}

/// Vlog reader Vlog 读取器
pub struct VlogReader {
  file: File,
}

impl VlogReader {
  /// Open vlog for reading 打开 vlog 读取
  pub async fn open(path: impl AsRef<Path>) -> JdbResult<Self> {
    let file = File::open(path).await?;
    Ok(Self { file })
  }

  /// Read blob by pointer 通过指针读取 blob
  pub async fn read(&self, ptr: &BlobPtr) -> JdbResult<Vec<u8>> {
    // Read header + data 读取头 + 数据
    let total = BLOB_HEADER_SIZE + ptr.len as usize;
    let buf = self.file.read_at(ptr.offset, total).await?;

    // Parse header 解析头
    let hdr = BlobHeader::read(&buf[..BLOB_HEADER_SIZE]);

    // Verify checksum 验证校验和
    let data = &buf[BLOB_HEADER_SIZE..BLOB_HEADER_SIZE + ptr.len as usize];
    let actual_crc = crc32(data);

    if actual_crc != hdr.checksum {
      return Err(JdbError::Checksum {
        expected: hdr.checksum,
        actual: actual_crc,
      });
    }

    Ok(data.to_vec())
  }
}
