//! Large blob storage / 大文件存储
//!
//! Store large files as individual files in blob/ directory.
//! 将大文件作为独立文件存储在 blob/ 目录。

use std::path::{Path, PathBuf};

use compio::{
  fs::File,
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use crc32fast::Hasher;
use fast32::base32::CROCKFORD_LOWER;
use roaring::RoaringBitmap;

use crate::{Compress, Error, Header, Result};

/// Blob directory name / Blob 目录名
const BLOB_DIR: &str = "blob";

/// Large blob storage / 大文件存储
pub struct BlobStore {
  /// Base path / 基础路径
  base_path: PathBuf,
  /// Next blob id / 下一个 blob ID
  next_id: u64,
  /// Free blob ids / 空闲 blob ID
  free_ids: RoaringBitmap,
}

impl BlobStore {
  /// Create blob store / 创建 blob 存储
  pub fn new(base_path: impl Into<PathBuf>) -> Result<Self> {
    let base_path = base_path.into();
    let blob_dir = base_path.join(BLOB_DIR);
    std::fs::create_dir_all(&blob_dir).map_err(Error::Io)?;

    Ok(Self {
      base_path,
      next_id: 0,
      free_ids: RoaringBitmap::new(),
    })
  }

  /// Get blob file path / 获取 blob 文件路径
  fn blob_path(&self, id: u64) -> PathBuf {
    let encoded = CROCKFORD_LOWER.encode_u64(id);
    let padded = format!("{encoded:0>6}");
    let (d1, rest) = padded.split_at(2);
    let (d2, name) = rest.split_at(2);
    self.base_path.join(BLOB_DIR).join(d1).join(d2).join(name)
  }

  /// Allocate blob id / 分配 blob ID
  fn alloc_id(&mut self) -> u64 {
    if let Some(id) = self.free_ids.iter().next() {
      self.free_ids.remove(id);
      return id as u64;
    }
    let id = self.next_id;
    self.next_id += 1;
    id
  }

  /// Write blob / 写入 blob
  pub async fn put(&mut self, data: &[u8], compress: Compress) -> Result<u64> {
    use std::borrow::Cow;

    // Compress if needed / 按需压缩
    let (payload, compress): (Cow<[u8]>, Compress) = match compress {
      Compress::None => (Cow::Borrowed(data), Compress::None),
      Compress::Lz4 => {
        let compressed = lz4_flex::compress_prepend_size(data);
        if compressed.len() < data.len() {
          (Cow::Owned(compressed), Compress::Lz4)
        } else {
          (Cow::Borrowed(data), Compress::None)
        }
      }
      Compress::Zstd => {
        let compressed = zstd::encode_all(data, 3).map_err(|e| Error::Serialize(e.to_string()))?;
        if compressed.len() < data.len() {
          (Cow::Owned(compressed), Compress::Zstd)
        } else {
          (Cow::Borrowed(data), Compress::None)
        }
      }
    };

    // Compute CRC32 / 计算 CRC32
    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let crc32 = hasher.finalize();

    // Build header / 构建头部
    let header = Header::new(crc32, payload.len() as u32, compress);

    // Allocate id and get path / 分配 ID 并获取路径
    let id = self.alloc_id();
    let path = self.blob_path(id);

    // Create parent dirs / 创建父目录
    if let Some(parent) = path.parent() {
      std::fs::create_dir_all(parent).map_err(Error::Io)?;
    }

    // Build buffer / 构建缓冲区
    let mut buf = Vec::with_capacity(Header::SIZE + payload.len());
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(&payload);

    // Write file async / 异步写入文件
    let mut file = File::create(&path).await.map_err(Error::Io)?;
    file.write_all_at(buf, 0).await.0.map_err(Error::Io)?;

    Ok(id)
  }

  /// Read blob / 读取 blob
  pub async fn get(&self, id: u64) -> Result<Vec<u8>> {
    let path = self.blob_path(id);
    let file = File::open(&path).await.map_err(Error::Io)?;
    let meta = file.metadata().await.map_err(Error::Io)?;
    let size = meta.len() as usize;

    if size < Header::SIZE {
      return Err(Error::Serialize("blob too short".into()));
    }

    // Read file async / 异步读取文件
    let buf = vec![0u8; size];
    let compio::buf::BufResult(res, buf) = file.read_exact_at(buf, 0).await;
    res.map_err(Error::Io)?;

    // Parse header / 解析头部
    let header = Header::decode(&buf)?;
    let payload_end = Header::SIZE + header.payload_len as usize;

    if buf.len() < payload_end {
      return Err(Error::Serialize("blob truncated".into()));
    }

    let payload = &buf[Header::SIZE..payload_end];

    // Verify CRC32 / 校验 CRC32
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let actual_crc = hasher.finalize();

    if actual_crc != header.crc32 {
      return Err(Error::CrcMismatch {
        expected: header.crc32,
        actual: actual_crc,
      });
    }

    // Decompress / 解压
    let data = match header.compress() {
      Compress::None => payload.to_vec(),
      Compress::Lz4 => {
        lz4_flex::decompress_size_prepended(payload).map_err(|e| Error::Serialize(e.to_string()))?
      }
      Compress::Zstd => zstd::decode_all(payload).map_err(|e| Error::Serialize(e.to_string()))?,
    };

    Ok(data)
  }

  /// Delete blob / 删除 blob
  pub fn del(&mut self, id: u64) {
    let path = self.blob_path(id);
    let _ = std::fs::remove_file(&path);
    self.free_ids.insert(id as u32);
  }

  /// Flush metadata / 刷新元数据
  pub fn flush(&self) -> Result<()> {
    use std::io::Write;

    let meta_path = self.base_path.join(BLOB_DIR).join("meta");

    // Serialize: next_id(8) + free_ids
    let mut data = Vec::new();
    data.extend_from_slice(&self.next_id.to_le_bytes());
    self
      .free_ids
      .serialize_into(&mut data)
      .map_err(|e| Error::Serialize(e.to_string()))?;

    std::fs::File::create(&meta_path)
      .and_then(|mut f| f.write_all(&data))
      .map_err(Error::Io)?;

    Ok(())
  }

  /// Recovery from metadata / 从元数据恢复
  pub fn recovery(&mut self) -> Result<()> {
    let meta_path = self.base_path.join(BLOB_DIR).join("meta");

    if !meta_path.exists() {
      return Ok(());
    }

    let data = std::fs::read(&meta_path).map_err(Error::Io)?;

    if data.len() < 8 {
      return Ok(());
    }

    self.next_id = u64::from_le_bytes([
      data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);

    if data.len() > 8 {
      self.free_ids =
        RoaringBitmap::deserialize_from(&data[8..]).map_err(|e| Error::Serialize(e.to_string()))?;
    }

    Ok(())
  }

  /// Get base path / 获取基础路径
  #[inline]
  pub fn base_path(&self) -> &Path {
    &self.base_path
  }

  /// Get next id / 获取下一个 ID
  #[inline]
  pub const fn next_id(&self) -> u64 {
    self.next_id
  }
}
