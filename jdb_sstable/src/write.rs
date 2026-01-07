//! SSTable writer with PGM-index (streaming write)
//! 使用 PGM 索引的 SSTable 写入器（流式写入）

use std::path::Path;

use compio::{
  buf::IoBuf,
  fs::{self, File},
  io::AsyncWriteAtExt,
};
use crc32fast::Hasher;
use defer_lite::defer;
use gxhash::gxhash64;
use jdb_base::table::Table;
use jdb_fs::fs_id::id_path;
use jdb_pgm::{Pgm, key_to_u64};
use jdb_xorf::BinaryFuse8;
use shared_prefix_len::shared_prefix_len;
use zbin::Bin;
use zerocopy::IntoBytes;

use crate::{
  Conf, Error, Meta, Result,
  block::BlockBuilder,
  conf::default,
  consts::TMP_DIR,
  foot::{Foot, VERSION},
};

/// Create SSTable from Table (streaming write)
/// 从 Table 创建 SSTable（流式写入）
///
/// Returns Meta with id = max ver of all entries
/// 返回 Meta，id = 所有条目的最大 ver
pub async fn write(dir: &Path, level: u8, table: &impl Table, conf_li: &[Conf]) -> Result<Meta> {
  let mut w = State::new(level, conf_li);

  // Create .tmp directory if not exists
  // 如果不存在则创建 .tmp 目录
  let tmp_dir = dir.join(TMP_DIR);
  fs::create_dir_all(&tmp_dir).await?;

  // Generate unique id before write
  // 写入前生成唯一 id
  let id = loop {
    let id = jdb_base::id();
    let path = id_path(dir, id);
    let tmp_path = id_path(&tmp_dir, id);
    if !path.exists() && !tmp_path.exists() {
      break id;
    }
  };
  w.meta.id = id;

  // Use temporary file for streaming write
  // 使用临时文件进行流式写入
  let temp_path = id_path(&tmp_dir, id);
  let mut file = File::create(&temp_path).await?;

  // Auto cleanup temp file if exists on exit
  // 退出时自动清理临时文件（如果存在）
  defer! { let _ = std::fs::remove_file(&temp_path); }

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
      w.offsets.push(w.file_offset);
    }

    w.hashes.push(gxhash64(&key, 0));
    w.builder.add(&key, &pos);
    w.meta.item_count += 1;
    last_key = key;

    // Flush block to file when full (streaming write)
    // 块满时刷新到文件（流式写入）
    if w.builder.size() >= w.block_size {
      let data = w.builder.build_encoded();
      w.file_offset += write_at(&mut file, &data, w.file_offset).await?;
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
    w.file_offset += write_at(&mut file, &data, w.file_offset).await?;
  }

  // Write metadata and foot
  // 写入元数据和尾部
  let offset = w.file_offset;
  write_foot(&mut file, &mut w, offset).await?;
  file.sync_all().await?;

  // Close file before rename
  // 重命名前关闭文件
  drop(file);

  // Move to final path
  // 移动到最终路径
  let final_path = id_path(dir, id);
  fs::rename(&temp_path, &final_path).await?;

  Ok(w.meta)
}

#[inline]
async fn write_at(file: &mut File, data: &[u8], offset: u64) -> Result<u64> {
  let len = data.len() as u64;
  if len == 0 {
    return Ok(0);
  }
  let buf = data.io();
  let slice = buf.slice(..);
  let res = file.write_all_at(slice, offset).await;
  res.0?;
  Ok(len)
}

async fn write_foot(file: &mut File, w: &mut State, mut offset: u64) -> Result<()> {
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

  // Build foot (checksum includes version)
  // 构建尾部（校验和包含 version）
  hasher.update(&[VERSION]);
  let checksum = hasher.finalize();
  let foot = Foot {
    filter_offset,
    filter_size,
    index_size,
    offsets_size,
    pgm_size,
    block_count: w.offsets.len() as u32,
    max_ver: w.max_ver,
    prefix_len,
    level: w.level,
    version: VERSION,
    checksum,
  };
  offset += write_at(file, foot.as_bytes(), offset).await?;

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
  // Safe: checked len > 1 above
  // 安全：上面已检查 len > 1
  let first = &first_keys[0];
  let last = &first_keys[first_keys.len() - 1];
  let prefix_len = shared_prefix_len(first, last).min(255) as u8;

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
  file_offset: u64,
  meta: Meta,
  max_ver: u64,
  level: u8,
}

/// Default capacity for hashes/keys/offsets
/// 哈希/键/偏移的默认容量
const DEFAULT_CAP: usize = 1024;

impl State {
  fn new(level: u8, conf_li: &[Conf]) -> Self {
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
      hashes: Vec::with_capacity(DEFAULT_CAP),
      first_keys: Vec::with_capacity(DEFAULT_CAP / 16),
      offsets: Vec::with_capacity(DEFAULT_CAP / 16),
      file_offset: 0,
      meta: Meta::default(),
      max_ver: 0,
      level,
    }
  }
}
