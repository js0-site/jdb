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
use futures::StreamExt;
use gxhash::gxhash64;
use jdb_base::{
  Pos,
  table::{Kv, mem::Table},
};
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
/// Returns Meta with auto-generated id
/// 返回 Meta，id 自动生成
pub async fn write(dir: &Path, level: u8, table: &impl Table, conf_li: &[Conf]) -> Result<Meta> {
  let id = gen_id(dir);
  write_with_id(dir, level, table, conf_li, id).await
}

/// Generate unique id for SSTable
/// 为 SSTable 生成唯一 id
pub fn gen_id(dir: &Path) -> u64 {
  let tmp_dir = dir.join(TMP_DIR);
  loop {
    let id = jdb_base::id();
    let path = id_path(dir, id);
    let tmp_path = id_path(&tmp_dir, id);
    if !path.exists() && !tmp_path.exists() {
      return id;
    }
  }
}

/// Create SSTable from Table with specified id
/// 使用指定 id 从 Table 创建 SSTable
pub async fn write_with_id(
  dir: &Path,
  level: u8,
  table: &impl Table,
  conf_li: &[Conf],
  id: u64,
) -> Result<Meta> {
  let mut w = State::new(level, conf_li, id);
  let tmp_dir = dir.join(TMP_DIR);
  fs::create_dir_all(&tmp_dir).await?;

  let temp_path = id_path(&tmp_dir, id);
  let mut file = File::create(&temp_path).await?;
  defer! { let _ = std::fs::remove_file(&temp_path); }

  let mut last_key: Box<[u8]> = Box::default();
  for (key, pos) in table.iter() {
    if key.len() > u16::MAX as usize {
      return Err(Error::KeyTooLarge(key.len()));
    }
    w.add_entry(&key, &pos, &mut file).await?;
    last_key = key;
  }

  w.finish(&mut file, last_key, dir, id).await
}

/// Create SSTable from async stream with specified id
/// 使用指定 id 从异步流创建 SSTable
pub async fn write_stream<S>(
  dir: &Path,
  level: u8,
  mut stream: S,
  conf_li: &[Conf],
  id: u64,
) -> Result<Meta>
where
  S: futures::Stream<Item = Kv> + Unpin,
{
  let mut w = State::new(level, conf_li, id);
  let tmp_dir = dir.join(TMP_DIR);
  fs::create_dir_all(&tmp_dir).await?;

  let temp_path = id_path(&tmp_dir, id);
  let mut file = File::create(&temp_path).await?;
  defer! { let _ = std::fs::remove_file(&temp_path); }

  let mut last_key: Box<[u8]> = Box::default();
  while let Some((key, pos)) = stream.next().await {
    if key.len() > u16::MAX as usize {
      return Err(Error::KeyTooLarge(key.len()));
    }
    w.add_entry(&key, &pos, &mut file).await?;
    last_key = key;
  }

  w.finish(&mut file, last_key, dir, id).await
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
  fn new(level: u8, conf_li: &[Conf], id: u64) -> Self {
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
      meta: Meta {
        id,
        ..Meta::default()
      },
      max_ver: 0,
      level,
    }
  }

  /// Add entry to SSTable
  /// 添加条目到 SSTable
  async fn add_entry(&mut self, key: &[u8], pos: &Pos, file: &mut File) -> Result<()> {
    self.hashes.push(gxhash64(key, 0));
    self.meta.item_count += 1;
    self.max_ver = self.max_ver.max(pos.ver());

    // Record first key of block
    // 记录块的首键
    if self.builder.item_count == 0 {
      self.first_keys.push(key.into());
      self.offsets.push(self.file_offset);
    }

    self.builder.add(key, pos);

    // Flush block if size exceeded
    // 超过大小则刷新块
    if self.builder.size() >= self.block_size {
      self.flush_block(file).await?;
    }

    Ok(())
  }

  /// Flush current block to file
  /// 将当前块刷新到文件
  async fn flush_block(&mut self, file: &mut File) -> Result<()> {
    let data = self.builder.build_encoded();
    if !data.is_empty() {
      self.file_offset += write_at(file, &data, self.file_offset).await?;
    }
    Ok(())
  }

  /// Finish writing SSTable
  /// 完成 SSTable 写入
  async fn finish(
    mut self,
    file: &mut File,
    last_key: Box<[u8]>,
    dir: &Path,
    id: u64,
  ) -> Result<Meta> {
    // Flush remaining block
    // 刷新剩余块
    self.flush_block(file).await?;

    if self.meta.item_count == 0 {
      return Ok(Meta::default());
    }

    self.meta.min_key = self.first_keys.first().cloned().unwrap_or_default();
    self.meta.max_key = last_key;

    let offset = self.file_offset;
    write_foot(file, &mut self, offset).await?;

    // Rename temp file to final path
    // 重命名临时文件到最终路径
    let tmp_dir = dir.join(TMP_DIR);
    let temp_path = id_path(&tmp_dir, id);
    let final_path = id_path(dir, id);
    fs::rename(&temp_path, &final_path).await?;

    Ok(self.meta)
  }
}
