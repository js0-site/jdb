//! SSTable writer with PGM-index
//! 使用 PGM 索引的 SSTable 写入器

use std::path::Path;

use compio::{buf::IoBuf, fs::File, io::AsyncWriteAtExt};
use crc32fast::Hasher;
use gxhash::gxhash64;
use jdb_base::table::Table;
use jdb_fs::fs_id::id_path;
use jdb_pgm::{Pgm, key_to_u64};
use jdb_xorf::BinaryFuse8;
use shared_prefix_len::shared_prefix_len;
use zbin::Bin;
use zerocopy::IntoBytes;

use crate::{
  Conf, Error, Result, TableMeta,
  block::BlockBuilder,
  conf::default,
  footer::{Footer, VERSION},
};

/// Create SSTable from Table (single pass)
/// 从 Table 创建 SSTable（单次遍历）
///
/// Returns TableMeta with id = max ver of all entries
/// 返回 TableMeta，id = 所有条目的最大 ver
pub async fn new(dir: &Path, table: &impl Table, conf_li: &[Conf]) -> Result<TableMeta> {
  let mut w = State::new(conf_li);

  // Single pass: collect metadata and build blocks
  // 单次遍历：收集元数据并构建块
  let mut last_key: Box<[u8]> = Box::default();
  for (key, pos) in table.iter() {
    if key.len() > u16::MAX as usize {
      return Err(Error::KeyTooLarge(key.len()));
    }

    let ver = pos.ver();
    if ver > w.max_ver {
      w.max_ver = ver;
    }

    if w.meta.min_key.is_empty() {
      w.meta.min_key = key.clone();
    }

    // Record first key and offset at block start
    // 在 block 开始时记录首键和偏移
    if w.builder.item_count == 0 {
      w.first_keys.push(key.clone());
      w.offsets.push(w.buf_len as u64);
    }

    w.hashes.push(gxhash64(&key, 0));
    w.builder.add(&key, &pos);
    w.meta.item_count += 1;
    last_key = key;

    // Flush block to buffer when full
    // 块满时刷新到缓冲区
    if w.builder.size() >= w.block_size {
      let data = w.builder.build_encoded();
      w.buf_len += data.len();
      w.blocks.push(data);
    }
  }

  if w.meta.item_count == 0 {
    return Ok(w.meta);
  }

  w.meta.max_key = last_key;

  // Flush remaining block
  // 刷新剩余块
  if w.builder.item_count > 0 {
    let data = w.builder.build_encoded();
    w.buf_len += data.len();
    w.blocks.push(data);
  }

  // Create file with max_ver as name
  // 用 max_ver 作为文件名创建文件
  let path = id_path(dir, w.max_ver);
  let mut file = File::create(&path).await?;
  w.meta.id = w.max_ver;

  // Write all blocks
  // 写入所有块
  let mut offset = 0u64;
  for block in &w.blocks {
    offset += write_at(&mut file, block.as_slice(), offset).await?;
  }

  // Write metadata and footer
  // 写入元数据和尾部
  write_footer(&mut file, &mut w, offset).await?;
  file.sync_all().await?;

  Ok(w.meta)
}

async fn write_at(file: &mut File, data: &[u8], offset: u64) -> Result<u64> {
  let len = data.len() as u64;
  let buf = data.io();
  let slice = buf.slice(..);
  let res = file.write_all_at(slice, offset).await;
  res.0?;
  Ok(len)
}

async fn write_footer(file: &mut File, w: &mut State, mut offset: u64) -> Result<()> {
  let mut hasher = Hasher::new();

  // Write filter
  // 写入过滤器
  let filter_offset = offset;
  let filter = BinaryFuse8::try_from(&w.hashes).map_err(|_| Error::FilterBuildFailed)?;
  let filter_data = bitcode::encode(&filter);
  let filter_size = filter_data.len() as u32;
  hasher.update(&filter_data);
  offset += write_at(file, &filter_data, offset).await?;

  // Write index (first keys)
  // 写入索引（首键数组）
  let index_data = bitcode::encode(&w.first_keys);
  let index_size = index_data.len() as u32;
  hasher.update(&index_data);
  offset += write_at(file, &index_data, offset).await?;

  // Write offsets
  // 写入偏移数组
  let offsets_data = bitcode::encode(&w.offsets);
  let offsets_size = offsets_data.len() as u32;
  hasher.update(&offsets_data);
  offset += write_at(file, &offsets_data, offset).await?;

  // Build PGM with common prefix stripped
  // 去掉共同前缀后建 PGM
  let (pgm_data, prefix_len) = build_pgm(&w.first_keys, w.epsilon);
  let pgm_size = pgm_data.len() as u32;
  hasher.update(&pgm_data);
  offset += write_at(file, &pgm_data, offset).await?;

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
    block_count: w.offsets.len() as u32,
    prefix_len,
    checksum,
  };
  offset += write_at(file, footer.as_bytes(), offset).await?;

  w.meta.file_size = offset;
  Ok(())
}

/// Build PGM index
/// 构建 PGM 索引
fn build_pgm(first_keys: &[Box<[u8]>], epsilon: usize) -> (Vec<u8>, u8) {
  if first_keys.len() <= 1 {
    return (Vec::new(), 0);
  }

  // Common prefix of sorted keys = prefix of first and last
  // 有序键的公共前缀 = 首尾键的前缀
  let prefix_len =
    shared_prefix_len(first_keys.first().unwrap(), first_keys.last().unwrap()).min(255) as u8;

  // Convert keys to u64 with prefix stripped
  // 去掉前缀后转换为 u64
  let mut data: Vec<u64> = Vec::with_capacity(first_keys.len());
  data.extend(
    first_keys
      .iter()
      .map(|k| key_to_u64(&k[prefix_len as usize..])),
  );
  data.dedup();

  if data.len() <= 1 {
    return (Vec::new(), prefix_len);
  }

  let Ok(pgm) = Pgm::new(&data, epsilon, false) else {
    return (Vec::new(), prefix_len);
  };
  (bitcode::encode(&pgm), prefix_len)
}

/// Internal writer state
/// 内部写入器状态
struct State {
  builder: BlockBuilder,
  block_size: usize,
  epsilon: usize,
  hashes: Vec<u64>,
  first_keys: Vec<Box<[u8]>>,
  offsets: Vec<u64>,
  blocks: Vec<Vec<u8>>,
  buf_len: usize,
  meta: TableMeta,
  max_ver: u64,
}

impl State {
  fn new(conf_li: &[Conf]) -> Self {
    let mut block_size = default::BLOCK_SIZE;
    let mut epsilon = default::PGM_EPSILON;
    let mut restart_interval = default::RESTART_INTERVAL;

    for conf in conf_li {
      match conf {
        Conf::BlockSize(size) => block_size = (*size).max(1024),
        Conf::PgmEpsilon(e) => epsilon = (*e).max(1),
        Conf::RestartInterval(r) => restart_interval = (*r).max(1),
      }
    }

    Self {
      builder: BlockBuilder::new(restart_interval),
      block_size,
      epsilon,
      hashes: Vec::new(),
      first_keys: Vec::new(),
      offsets: Vec::new(),
      blocks: Vec::new(),
      buf_len: 0,
      meta: TableMeta::default(),
      max_ver: 0,
    }
  }
}
