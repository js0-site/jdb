//! Slab storage class / Slab 存储类
//!
//! Fixed-size slot storage with Direct I/O.
//! 固定大小槽位存储，支持 Direct I/O。

use std::{
  borrow::Cow,
  path::{Path, PathBuf},
};

use crc32fast::Hasher;
use jdb_alloc::AlignedBuf;
use jdb_fs::File;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use roaring::RoaringBitmap;

use crate::{Compress, Error, Header, HeatTracker, Result, SlabReader, SlabWriter, SlotId};

/// Slab storage class for fixed-size slots / 固定大小槽位存储类
pub struct SlabClass {
  /// Direct I/O file / 直接 I/O 文件
  file: File,
  /// Slot size (must be aligned) / 槽位大小（必须对齐）
  class_size: usize,
  /// Free slot bitmap / 空闲位图
  free_map: RoaringBitmap,
  /// Access statistics / 访问统计
  heat: HeatTracker,
  /// Total slots / 总槽位数
  slot_count: u64,
  /// Data directory / 数据目录
  base_path: PathBuf,
}

impl SlabClass {
  /// Create or open SlabClass / 创建或打开
  pub async fn open(base_path: &Path, class_size: usize) -> Result<Self> {
    let slab_path = base_path.join(format!("{class_size}.slab"));
    let file = File::open_rw(&slab_path).await?;
    let file_size = file.size().await?;
    let slot_count = file_size / class_size as u64;

    Ok(Self {
      file,
      class_size,
      free_map: RoaringBitmap::new(),
      heat: HeatTracker::with_cap(slot_count as usize),
      slot_count,
      base_path: base_path.to_path_buf(),
    })
  }

  /// Max payload size for this class / 本层最大载荷大小
  #[inline]
  pub const fn max_payload(&self) -> usize {
    self.class_size - Header::SIZE
  }

  /// Class size / 层级大小
  #[inline]
  pub const fn class_size(&self) -> usize {
    self.class_size
  }

  /// Total slot count / 总槽位数
  #[inline]
  pub const fn slot_count(&self) -> u64 {
    self.slot_count
  }

  /// Check if slot is free / 检查槽位是否空闲
  #[inline]
  pub fn is_free(&self, slot_id: SlotId) -> bool {
    self.free_map.contains(slot_id as u32)
  }

  /// Get heat tracker ref / 获取热度追踪器引用
  #[inline]
  pub fn heat(&self) -> &HeatTracker {
    &self.heat
  }

  /// Get mutable heat tracker / 获取可变热度追踪器
  #[inline]
  pub fn heat_mut(&mut self) -> &mut HeatTracker {
    &mut self.heat
  }

  /// Get base path / 获取基础路径
  #[inline]
  pub fn base_path(&self) -> &Path {
    &self.base_path
  }

  /// Allocate a slot (from free_map or extend) / 分配槽位
  async fn alloc_slot(&mut self) -> Result<SlotId> {
    if let Some(slot_id) = self.free_map.iter().next() {
      self.free_map.remove(slot_id);
      return Ok(slot_id as SlotId);
    }
    // Extend file / 扩展文件
    let slot_id = self.slot_count;
    self.slot_count += 1;
    let new_size = self.slot_count * self.class_size as u64;
    self.file.preallocate(new_size).await?;
    Ok(slot_id)
  }

  /// Compute slot offset / 计算槽位偏移
  #[inline]
  const fn slot_offset(&self, slot_id: SlotId) -> u64 {
    slot_id * self.class_size as u64
  }

  /// Write data, return SlotId / 写入数据，返回槽位 ID
  pub async fn put(&mut self, data: &[u8]) -> Result<SlotId> {
    self.put_with(data, Compress::None).await
  }

  /// Write data with compression / 写入数据（带压缩）
  pub async fn put_with(&mut self, data: &[u8], compress: Compress) -> Result<SlotId> {
    let max = self.max_payload();
    if data.len() > max {
      return Err(Error::Overflow {
        len: data.len(),
        max,
      });
    }

    // Use Cow to avoid allocation when no compression / 使用 Cow 避免无压缩时的分配
    let (payload, compress): (Cow<[u8]>, Compress) = match compress {
      Compress::None => (Cow::Borrowed(data), Compress::None),
      Compress::Lz4 => {
        let compressed = compress_prepend_size(data);
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

    // Check compressed size / 检查压缩后大小
    if payload.len() > max {
      return Err(Error::Overflow {
        len: payload.len(),
        max,
      });
    }

    // Compute CRC32 / 计算 CRC32
    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let crc32 = hasher.finalize();

    // Build header / 构建头部
    let header = Header::new(crc32, payload.len() as u32, compress);

    // Allocate slot / 分配槽位
    let slot_id = self.alloc_slot().await?;
    let offset = self.slot_offset(slot_id);

    // Build aligned buffer / 构建对齐缓冲区
    let mut buf = AlignedBuf::zeroed(self.class_size)?;
    buf[..Header::SIZE].copy_from_slice(&header.encode());
    buf[Header::SIZE..Header::SIZE + payload.len()].copy_from_slice(&payload);

    // Write to file / 写入文件
    self.file.write_at(buf, offset).await?;

    Ok(slot_id)
  }

  /// Read data by SlotId / 按槽位 ID 读取
  pub async fn get(&mut self, slot_id: SlotId) -> Result<Vec<u8>> {
    if slot_id >= self.slot_count {
      return Err(Error::InvalidSlot(slot_id));
    }

    let offset = self.slot_offset(slot_id);
    let buf = AlignedBuf::zeroed(self.class_size)?;
    let buf = self.file.read_at(buf, offset).await?;

    // Parse header / 解析头部
    let header = Header::decode(&buf)?;

    // Verify CRC32 / 校验 CRC32
    let payload_end = Header::SIZE + header.payload_len as usize;
    let payload = &buf[Header::SIZE..payload_end];

    let mut hasher = Hasher::new();
    hasher.update(payload);
    let actual_crc = hasher.finalize();

    if actual_crc != header.crc32 {
      return Err(Error::CrcMismatch {
        expected: header.crc32,
        actual: actual_crc,
      });
    }

    // Update heat / 更新热度
    self.heat.access(slot_id);

    // Decompress if needed / 按需解压
    let data = match header.compress() {
      Compress::None => payload.to_vec(),
      Compress::Lz4 => {
        decompress_size_prepended(payload).map_err(|e| Error::Serialize(e.to_string()))?
      }
      Compress::Zstd => zstd::decode_all(payload).map_err(|e| Error::Serialize(e.to_string()))?,
    };

    Ok(data)
  }

  /// Delete slot (logical) / 删除槽位（逻辑删除）
  pub fn del(&mut self, slot_id: SlotId) {
    self.free_map.insert(slot_id as u32);
    self.heat.clear(slot_id);
  }
}

impl SlabClass {
  /// Sync metadata to disk / 同步元数据到磁盘
  pub async fn sync_meta(&self) -> Result<()> {
    use std::io::Write;

    // Serialize free_map to {class_size}.roaring
    let roaring_path = self.base_path.join(format!("{}.roaring", self.class_size));
    let mut roaring_data = Vec::new();
    self
      .free_map
      .serialize_into(&mut roaring_data)
      .map_err(|e| Error::Serialize(e.to_string()))?;
    std::fs::File::create(&roaring_path)
      .and_then(|mut f| f.write_all(&roaring_data))
      .map_err(Error::Io)?;

    // Serialize heat to {class_size}.heat
    let heat_path = self.base_path.join(format!("{}.heat", self.class_size));
    let heat_data = self.heat.serialize();
    std::fs::File::create(&heat_path)
      .and_then(|mut f| f.write_all(&heat_data))
      .map_err(Error::Io)?;

    Ok(())
  }

  /// Recover from metadata files / 从元数据文件恢复
  pub async fn recovery(&mut self) -> Result<()> {
    use std::io::Read;

    // Try load free_map from {class_size}.roaring
    let roaring_path = self.base_path.join(format!("{}.roaring", self.class_size));
    if roaring_path.exists() {
      let mut data = Vec::new();
      std::fs::File::open(&roaring_path)
        .and_then(|mut f| f.read_to_end(&mut data))
        .map_err(Error::Io)?;
      self.free_map =
        RoaringBitmap::deserialize_from(&data[..]).map_err(|e| Error::Serialize(e.to_string()))?;
    }

    // Try load heat from {class_size}.heat
    let heat_path = self.base_path.join(format!("{}.heat", self.class_size));
    if heat_path.exists() {
      let mut data = Vec::new();
      std::fs::File::open(&heat_path)
        .and_then(|mut f| f.read_to_end(&mut data))
        .map_err(Error::Io)?;
      self.heat = HeatTracker::deserialize(&data)?;
    }

    Ok(())
  }

  /// Get free_map ref / 获取空闲位图引用
  #[inline]
  pub fn free_map(&self) -> &RoaringBitmap {
    &self.free_map
  }
}

impl SlabClass {
  /// Get streaming reader / 获取流式读取器
  pub fn reader(&self, slot_id: SlotId, total_len: u64) -> SlabReader<'_> {
    SlabReader::new(&self.file, slot_id, self.class_size, total_len)
  }

  /// Get streaming writer for new slot / 获取新槽位的流式写入器
  pub async fn writer(&mut self) -> Result<SlabWriter<'_>> {
    self.writer_with(Compress::None).await
  }

  /// Get streaming writer with compression / 获取带压缩的流式写入器
  pub async fn writer_with(&mut self, compress: Compress) -> Result<SlabWriter<'_>> {
    let slot_id = self.alloc_slot().await?;
    SlabWriter::new(&self.file, slot_id, self.class_size, compress)
  }

  /// Get file reference / 获取文件引用
  pub fn file(&self) -> &File {
    &self.file
  }
}
