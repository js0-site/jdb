//! Internal writer state
//! 内部写入器状态

use std::path::Path;

use compio::fs::{self, File};
use gxhash::gxhash64;
use jdb_base::Pos;
use jdb_fs::fs_id::id_path;
use jdb_sst::{Conf, Meta, Result, TMP_DIR, block::BlockBuilder, default};

use crate::{foot, write_at};

/// Default capacity for hashes/keys/offsets
/// 哈希/键/偏移的默认容量
const DEFAULT_CAP: usize = 1024;

pub(crate) struct State {
  pub builder: BlockBuilder,
  pub block_size: usize,
  pub epsilon: usize,
  pub hashes: Vec<u64>,
  pub first_keys: Vec<Box<[u8]>>,
  pub offsets: Vec<u64>,
  pub file_offset: u64,
  pub meta: Meta,
  pub max_ver: u64,
  pub rmed_size: u64,
  pub level: u8,
}

impl State {
  pub fn new(level: u8, conf_li: &[Conf], id: u64) -> Self {
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
      rmed_size: 0,
      level,
    }
  }

  /// Add entry to SSTable
  /// 添加条目到 SSTable
  pub async fn add(&mut self, key: &[u8], pos: &Pos, file: &mut File) -> Result<()> {
    self.hashes.push(gxhash64(key, 0));
    self.meta.item_count += 1;
    self.max_ver = self.max_ver.max(pos.ver());

    // Tombstone size: key_len + val_len + Pos size (32B)
    // 墓碑大小：key_len + val_len + Pos 大小（32B）
    if pos.is_tombstone() {
      self.rmed_size += key.len() as u64 + pos.len() as u64 + 32;
    }

    if self.builder.item_count == 0 {
      self.first_keys.push(key.into());
      self.offsets.push(self.file_offset);
    }

    self.builder.add(key, pos);

    if self.builder.size() >= self.block_size {
      self.flush_block(file).await?;
    }

    Ok(())
  }

  /// Flush current block to file
  /// 将当前块刷新到文件
  pub async fn flush_block(&mut self, file: &mut File) -> Result<()> {
    let data = self.builder.build_encoded();
    if !data.is_empty() {
      self.file_offset += write_at(file, &data, self.file_offset).await?;
    }
    Ok(())
  }

  /// Finish writing SSTable
  /// 完成 SSTable 写入
  pub async fn finish(
    mut self,
    file: &mut File,
    last_key: Box<[u8]>,
    dir: &Path,
    id: u64,
  ) -> Result<Meta> {
    self.flush_block(file).await?;

    if self.meta.item_count == 0 {
      return Ok(Meta::default());
    }

    self.meta.min_key = self.first_keys.first().cloned().unwrap_or_default();
    self.meta.max_key = last_key;

    let offset = self.file_offset;
    foot::write(file, &mut self, offset).await?;

    let tmp_dir = dir.join(TMP_DIR);
    let temp_path = id_path(&tmp_dir, id);
    let final_path = id_path(dir, id);
    fs::rename(&temp_path, &final_path).await?;

    Ok(self.meta)
  }
}
