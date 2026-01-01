//! SSTable writer with PGM-index
//! 使用 PGM 索引的 SSTable 写入器
//!
//! File layout:
//! [Data Block 0] [Data Block 1] ... [Filter] [Offset Array] [PGM Index] [Footer]
//! 文件布局：
//! [数据块0] [数据块1] ... [过滤器] [偏移数组] [PGM索引] [尾部]

use std::path::PathBuf;

use compio::{buf::IoBuf, fs::File, io::AsyncWriteAtExt};
use crc32fast::Hasher;
use gxhash::gxhash64;
use jdb_pgm_index::PGMIndex;
use jdb_xorf::BinaryFuse8;
use zerocopy::IntoBytes;

use super::{FooterBuilder, TableMeta, key_to_u64};
use crate::{BlockBuilder, Entry, Error, Result};

/// Default block size (4KB)
/// 默认块大小（4KB）
pub const DEFAULT_BLOCK_SIZE: usize = 4096;

/// PGM epsilon (error bound)
/// PGM 误差范围
const PGM_EPSILON: usize = 32;

struct BlockMeta {
  prefix: u64,
  offset: u64,
}

/// SSTable writer
/// SSTable 写入器
pub struct Writer {
  path: PathBuf,
  file: File,
  builder: BlockBuilder,
  block_size: usize,
  hashes: Vec<u64>,
  blocks: Vec<BlockMeta>,
  meta: TableMeta,
  offset: u64,
  cur_prefix: Option<u64>,
  first_key: Option<Box<[u8]>>,
  last_key: Option<Box<[u8]>>,
  hasher: Hasher,
}

impl Writer {
  pub async fn new(path: PathBuf, id: u64, cap: usize) -> Result<Self> {
    let file = File::create(&path).await?;
    Ok(Self {
      path,
      file,
      builder: BlockBuilder::with_default(),
      block_size: DEFAULT_BLOCK_SIZE,
      hashes: Vec::with_capacity(cap),
      blocks: Vec::new(),
      meta: TableMeta::new(id),
      offset: 0,
      cur_prefix: None,
      first_key: None,
      last_key: None,
      hasher: Hasher::new(),
    })
  }

  #[inline]
  pub fn block_size(mut self, size: usize) -> Self {
    self.block_size = size.max(1024);
    self
  }

  /// Add key-entry pair (must be sorted)
  /// 添加键-条目对（必须有序）
  pub async fn add(&mut self, key: &[u8], entry: &Entry) -> Result<()> {
    if self.first_key.is_none() {
      self.first_key = Some(key.into());
    }

    if self.cur_prefix.is_none() {
      self.cur_prefix = Some(key_to_u64(key));
    }

    if !entry.is_tombstone() {
      self.hashes.push(gxhash64(key, 0));
    }

    self.builder.add(key, entry);
    self.last_key = Some(key.into());
    self.meta.item_count += 1;

    if self.builder.size() >= self.block_size {
      self.flush_block().await?;
    }
    Ok(())
  }

  async fn flush_block(&mut self) -> Result<()> {
    if self.builder.is_empty() {
      return Ok(());
    }

    if let Some(prefix) = self.cur_prefix.take() {
      self.blocks.push(BlockMeta {
        prefix,
        offset: self.offset,
      });
    }

    let block = std::mem::replace(&mut self.builder, BlockBuilder::with_default()).finish();
    let data = block.as_bytes();
    self.hasher.update(data);
    self.write(data).await
  }

  async fn write(&mut self, data: &[u8]) -> Result<()> {
    let buf: Vec<u8> = data.to_vec();
    let len = buf.len();
    let slice = buf.slice(0..len);
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

    let block_count = self.blocks.len() as u32;

    // Write filter
    // 写入过滤器
    let filter_offset = self.offset;
    let filter = BinaryFuse8::try_from(&self.hashes).map_err(|_| Error::FilterBuildFailed)?;
    let filter_data = bitcode::encode(&filter);
    self.hasher.update(&filter_data);
    self.write(&filter_data).await?;
    let filter_size = (self.offset - filter_offset) as u32;

    // Write offsets
    // 写入偏移数组
    let offsets_offset = self.offset;
    let offsets: Vec<u64> = self.blocks.iter().map(|b| b.offset).collect();
    let offsets_data = bitcode::encode(&offsets);
    self.hasher.update(&offsets_data);
    self.write(&offsets_data).await?;
    let offsets_size = (self.offset - offsets_offset) as u32;

    // Write PGM
    // 写入 PGM 索引
    let pgm_offset = self.offset;
    let pgm_data = self.build_pgm();
    self.hasher.update(&pgm_data);
    self.write(&pgm_data).await?;
    let pgm_size = (self.offset - pgm_offset) as u32;

    // Write footer
    // 写入尾部
    let checksum = self.hasher.clone().finalize();
    let footer = FooterBuilder {
      filter_offset,
      filter_size,
      offsets_offset,
      offsets_size,
      pgm_offset,
      pgm_size,
      block_count,
      checksum,
    }
    .build();
    self.write(footer.as_bytes()).await?;
    self.file.sync_all().await?;

    self.meta.file_size = self.offset;
    if let Some(key) = self.first_key {
      self.meta.min_key = key;
    }
    if let Some(key) = self.last_key {
      self.meta.max_key = key;
    }
    Ok(self.meta)
  }

  fn build_pgm(&self) -> Vec<u8> {
    if self.blocks.is_empty() {
      return Vec::new();
    }

    let mut data: Vec<u64> = self.blocks.iter().map(|b| b.prefix).collect();
    data.dedup();

    if data.len() == 1 {
      return bitcode::encode(&data);
    }

    let pgm = PGMIndex::new(data, PGM_EPSILON);
    bitcode::encode(&pgm)
  }
}
