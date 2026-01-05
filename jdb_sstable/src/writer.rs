//! SSTable writer with PGM-index
//! 使用 PGM 索引的 SSTable 写入器

use std::path::PathBuf;

use compio::{buf::IoBuf, fs::File, io::AsyncWriteAtExt};
use crc32fast::Hasher;
use gxhash::gxhash64;
use jdb_base::Pos;
use jdb_pgm::{PGMIndex, key_to_u64};
use jdb_xorf::BinaryFuse8;
use shared_prefix_len::shared_prefix_len;
use zbin::Bin;
use zerocopy::IntoBytes;

use crate::{
  Error, Result, TableMeta,
  block::BlockBuilder,
  footer::{Footer, VERSION},
};

/// Default block size (16KB, optimal for NVMe SSD)
/// 默认块大小（16KB，NVMe SSD 最优）
pub const DEFAULT_BLOCK_SIZE: usize = 16384;

/// PGM epsilon (error bound)
/// PGM 误差范围
const PGM_EPSILON: usize = 64;

/// SSTable writer
/// SSTable 写入器
pub struct Writer {
  path: PathBuf,
  file: File,
  builder: BlockBuilder,
  block_size: usize,
  hashes: Vec<u64>,
  first_keys: Vec<Box<[u8]>>,
  offsets: Vec<u64>,
  meta: TableMeta,
  offset: u64,
  last_key: Vec<u8>,
}

impl Writer {
  pub async fn new(path: PathBuf, id: u64, cap: usize) -> Result<Self> {
    let file = File::create(&path).await?;
    // Pre-allocate based on expected capacity
    // 根据预期容量预分配
    let block_cap = cap / 1000 + 1;
    Ok(Self {
      path,
      file,
      builder: BlockBuilder::with_default(),
      block_size: DEFAULT_BLOCK_SIZE,
      hashes: Vec::with_capacity(cap),
      first_keys: Vec::with_capacity(block_cap),
      offsets: Vec::with_capacity(block_cap),
      meta: TableMeta::new(id),
      offset: 0,
      last_key: Vec::with_capacity(256),
    })
  }

  #[inline]
  pub fn block_size(mut self, size: usize) -> Self {
    self.block_size = size.max(1024);
    self
  }

  /// Add key-pos pair (must be sorted)
  /// 添加键-位置对（必须有序）
  pub async fn add(&mut self, key: &[u8], pos: &Pos) -> Result<()> {
    if key.len() > u16::MAX as usize {
      return Err(Error::KeyTooLarge(key.len()));
    }

    if self.meta.min_key.is_empty() {
      self.meta.min_key = key.into();
    }

    // Record first key and offset at block start
    // 在 block 开始时记录首键和偏移
    if self.builder.item_count == 0 {
      self.first_keys.push(key.into());
      self.offsets.push(self.offset);
    }

    // Tombstones MUST be in filter to prevent "resurrection" of old data
    // Tombstone 必须加入过滤器，防止旧数据"复活"
    self.hashes.push(gxhash64(key, 0));

    self.builder.add(key, pos);

    // Reuse buffer for last key
    // 复用缓冲区存储最后一个键
    // Optimization: avoid reallocation if capacity is sufficient
    // 优化：如果容量足够则避免重新分配
    if self.last_key.capacity() < key.len() {
      self.last_key.reserve(key.len() - self.last_key.len());
    }
    self.last_key.clear();
    self.last_key.extend_from_slice(key);

    self.meta.item_count += 1;

    if self.builder.size() >= self.block_size {
      self.flush_block().await?;
    }
    Ok(())
  }

  async fn flush_block(&mut self) -> Result<()> {
    if self.builder.item_count == 0 {
      return Ok(());
    }

    let data = self.builder.build_encoded();
    self.write(data).await
  }

  async fn write<'a>(&mut self, data: impl Bin<'a>) -> Result<()> {
    let len = data.len();
    let buf = data.io();
    let slice = buf.slice(..);
    let res = self.file.write_all_at(slice, self.offset).await;
    res.0?;
    self.offset += len as u64;
    Ok(())
  }

  pub async fn finish(mut self) -> Result<TableMeta> {
    self.flush_block().await?;

    if self.meta.item_count == 0 {
      drop(self.file);
      let _ = compio::fs::remove_file(&self.path).await;
      return Ok(self.meta);
    }

    // Set max_key from last_key buffer (only once at end)
    // 从 last_key 缓冲区设置 max_key（仅在结束时设置一次）
    self.meta.max_key = std::mem::take(&mut self.last_key).into_boxed_slice();

    let block_count = self.offsets.len() as u32;

    // Checksum only covers metadata (filter + index + offsets + pgm)
    // 校验和只覆盖元数据（过滤器 + 索引 + 偏移量 + PGM）
    let mut hasher = Hasher::new();

    // Write filter
    // 写入过滤器
    let filter_offset = self.offset;
    let filter = BinaryFuse8::try_from(&self.hashes).map_err(|_| Error::FilterBuildFailed)?;
    let filter_data = bitcode::encode(&filter);
    let filter_size = filter_data.len() as u32;
    hasher.update(&filter_data);
    self.write(filter_data).await?;

    // Write index (first keys)
    // 写入索引（首键数组）
    let index_data = bitcode::encode(&self.first_keys);
    let index_size = index_data.len() as u32;
    hasher.update(&index_data);
    self.write(index_data).await?;

    // Write offsets
    // 写入偏移数组
    let offsets_data = bitcode::encode(&self.offsets);
    let offsets_size = offsets_data.len() as u32;
    hasher.update(&offsets_data);
    self.write(offsets_data).await?;

    // Build PGM with common prefix stripped
    // 去掉共同前缀后建 PGM
    let (pgm_data, prefix_len) = self.build_pgm();
    let pgm_size = pgm_data.len() as u32;

    // Write PGM
    // 写入 PGM 索引
    hasher.update(&pgm_data);
    self.write(pgm_data).await?;

    // Write footer
    // 写入尾部
    let checksum = hasher.finalize();
    let footer = Footer {
      version: VERSION,
      filter_offset,
      filter_size,
      index_size,
      offsets_size,
      pgm_size,
      block_count,
      prefix_len,
      checksum,
    };
    self.write(footer.as_bytes()).await?;
    self.file.sync_all().await?;

    self.meta.file_size = self.offset;
    Ok(self.meta)
  }

  /// Build PGM index, returns (encoded data, common prefix length)
  /// 构建 PGM 索引，返回（编码数据，共同前缀长度）
  fn build_pgm(&self) -> (Vec<u8>, u8) {
    if self.first_keys.len() <= 1 {
      return (Vec::new(), 0);
    }

    // Find common prefix length
    // 查找共同前缀长度
    // Optimization: strip full prefix to maximize PGM discrimination
    // 优化：剥离完整前缀以最大化 PGM 区分度
    let prefix_len = self.common_prefix_len().min(255) as u8;

    // Convert keys to u64 with prefix stripped
    // 去掉前缀后转换为 u64
    // Use iterator directly to avoid intermediate allocation
    // 直接使用迭代器避免中间分配
    let mut data: Vec<u64> = Vec::with_capacity(self.first_keys.len());
    data.extend(
      self
        .first_keys
        .iter()
        .map(|k| key_to_u64(&k[prefix_len as usize..])),
    );
    // Keys are already sorted, dedup is O(n)
    // 键已排序，dedup 是 O(n)
    data.dedup();

    if data.len() <= 1 {
      return (Vec::new(), prefix_len);
    }

    // Data is already sorted and deduped, so skip sort check for performance
    // 数据已经排序并去重，跳过排序检查以提升性能
    let Ok(pgm) = PGMIndex::load(data, PGM_EPSILON, false) else {
      return (Vec::new(), prefix_len);
    };
    (bitcode::encode(&pgm), prefix_len)
  }

  /// Find common prefix length of all first keys (O(1) since keys are sorted)
  /// 查找所有首键的共同前缀长度（O(1)，因为键是有序的）
  fn common_prefix_len(&self) -> usize {
    // Keys are sorted, so common prefix of all is common prefix of first and last
    // 键是有序的，所以所有键的公共前缀就是首尾键的公共前缀
    let (Some(first), Some(last)) = (self.first_keys.first(), self.first_keys.last()) else {
      return 0;
    };
    shared_prefix_len(first, last)
  }
}
