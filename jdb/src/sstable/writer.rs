//! SSTable writer
//! SSTable 写入器
//!
//! Writes data blocks, filter block, index block and footer.
//! 写入数据块、过滤器块、索引块和尾部。

use std::path::PathBuf;

use autoscale_cuckoo_filter::CuckooFilter;
use compio::{buf::IoBuf, fs::File, io::AsyncWriteAtExt};
use crc32fast::Hasher;
use zerocopy::IntoBytes;

use crate::{BlockBuilder, Entry, Result};

use super::{Footer, TableMeta};

/// Default block size (4KB)
/// 默认块大小（4KB）
pub const DEFAULT_BLOCK_SIZE: usize = 4096;

/// Default filter false positive rate
/// 默认过滤器假阳性率
pub const DEFAULT_FILTER_FPR: f64 = 0.01;

/// Index entry: last key + block offset + block size
/// 索引条目：最后一个键 + 块偏移 + 块大小
#[derive(Debug, Clone)]
struct IndexEntry {
  last_key: Box<[u8]>,
  offset: u64,
  size: u32,
}

/// SSTable writer
/// SSTable 写入器
pub struct Writer {
  path: PathBuf,
  file: File,
  block_builder: BlockBuilder,
  block_size: usize,
  filter: CuckooFilter<[u8]>,
  index: Vec<IndexEntry>,
  meta: TableMeta,
  offset: u64,
  first_key: Option<Box<[u8]>>,
  last_key: Option<Box<[u8]>>,
  hasher: Hasher,
}

impl Writer {
  /// Create new writer
  /// 创建新写入器
  pub async fn new(path: PathBuf, id: u64, capacity_hint: usize) -> Result<Self> {
    let file = File::create(&path).await?;
    let filter = CuckooFilter::new(capacity_hint.max(100), DEFAULT_FILTER_FPR);

    Ok(Self {
      path,
      file,
      block_builder: BlockBuilder::with_default(),
      block_size: DEFAULT_BLOCK_SIZE,
      filter,
      index: Vec::new(),
      meta: TableMeta::new(id),
      offset: 0,
      first_key: None,
      last_key: None,
      hasher: Hasher::new(),
    })
  }

  /// Set block size
  /// 设置块大小
  #[inline]
  pub fn block_size(mut self, size: usize) -> Self {
    self.block_size = size.max(1024);
    self
  }

  /// Add key-entry pair (must be sorted)
  /// 添加键-条目对（必须有序）
  pub async fn add(&mut self, key: &[u8], entry: &Entry) -> Result<()> {
    // Record first key
    // 记录第一个键
    if self.first_key.is_none() {
      self.first_key = Some(key.into());
    }

    // Add to filter (skip tombstones)
    // 添加到过滤器（跳过删除标记）
    if !entry.is_tombstone() {
      self.filter.add_if_not_exist(key);
    }

    // Add to block builder
    // 添加到块构建器
    self.block_builder.add(key, entry);
    self.last_key = Some(key.into());
    self.meta.item_count += 1;

    // Flush block if size exceeded
    // 如果超过大小则刷新块
    if self.block_builder.size() >= self.block_size {
      self.flush_block().await?;
    }

    Ok(())
  }

  /// Flush current block to file
  /// 将当前块刷新到文件
  async fn flush_block(&mut self) -> Result<()> {
    if self.block_builder.is_empty() {
      return Ok(());
    }

    let last_key = self.last_key.clone();
    let block = std::mem::replace(&mut self.block_builder, BlockBuilder::with_default()).finish();
    let data = block.as_bytes();

    // Record index entry
    // 记录索引条目
    if let Some(key) = last_key {
      self.index.push(IndexEntry {
        last_key: key,
        offset: self.offset,
        size: data.len() as u32,
      });
    }

    // Write block and update checksum
    // 写入块并更新校验和
    self.hasher.update(data);
    self.write_all(data).await?;

    Ok(())
  }

  /// Write bytes to file
  /// 写入字节到文件
  async fn write_all(&mut self, data: &[u8]) -> Result<()> {
    let buf: Vec<u8> = data.to_vec();
    let len = buf.len();
    let slice = buf.slice(0..len);
    let res = self.file.write_all_at(slice, self.offset).await;
    res.0?;
    self.offset += len as u64;
    Ok(())
  }

  /// Finish writing and return metadata
  /// 完成写入并返回元数据
  pub async fn finish(mut self) -> Result<TableMeta> {
    // Flush remaining block
    // 刷新剩余块
    self.flush_block().await?;

    if self.meta.item_count == 0 {
      // Empty table, remove file
      // 空表，删除文件
      drop(self.file);
      let _ = compio::fs::remove_file(&self.path).await;
      return Ok(self.meta);
    }

    // Write filter block
    // 写入过滤器块
    let filter_offset = self.offset;
    let filter_data = bitcode::encode(&self.filter);
    self.hasher.update(&filter_data);
    self.write_all(&filter_data).await?;
    let filter_size = self.offset - filter_offset;

    // Write index block
    // 写入索引块
    let index_offset = self.offset;
    let index_data = self.encode_index();
    self.hasher.update(&index_data);
    self.write_all(&index_data).await?;
    let index_size = self.offset - index_offset;

    // Write footer
    // 写入尾部
    let checksum = self.hasher.clone().finalize();
    let footer = Footer::new(filter_offset, filter_size, index_offset, index_size, checksum);
    let footer_bytes = footer.as_bytes();
    self.write_all(footer_bytes).await?;

    // Sync file
    // 同步文件
    self.file.sync_all().await?;

    // Update metadata
    // 更新元数据
    self.meta.file_size = self.offset;
    if let Some(key) = self.first_key {
      self.meta.min_key = key;
    }
    if let Some(key) = self.last_key {
      self.meta.max_key = key;
    }

    Ok(self.meta)
  }

  /// Encode index block
  /// 编码索引块
  ///
  /// Format: [entry_count: u32] [entries...]
  /// Entry: [key_len: u16] [key] [offset: u64] [size: u32]
  fn encode_index(&self) -> Vec<u8> {
    let mut buf = Vec::new();

    // Entry count
    // 条目数量
    buf.extend_from_slice(&(self.index.len() as u32).to_le_bytes());

    // Entries
    // 条目
    for entry in &self.index {
      buf.extend_from_slice(&(entry.last_key.len() as u16).to_le_bytes());
      buf.extend_from_slice(&entry.last_key);
      buf.extend_from_slice(&entry.offset.to_le_bytes());
      buf.extend_from_slice(&entry.size.to_le_bytes());
    }

    buf
  }
}
