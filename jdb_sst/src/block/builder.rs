//! Block builder
//! 块构建器

use jdb_base::Pos;
use shared_prefix_len::shared_prefix_len;
use zerocopy::IntoBytes;

use crate::{Error, Result};

/// Block builder with two-level prefix compression
/// 带两级前缀压缩的块构建器
///
/// Format: [prefix_len: u16][prefix][entries...][restarts...][restart_count: u32][item_count: u32]
/// 格式：[前缀长度: u16][前缀][条目...][重启点...][重启点数: u32][条目数: u32]
pub struct BlockBuilder {
  pub key_arena: Vec<u8>,
  /// (offset, len, pos) - offset into key_arena
  /// (偏移, 长度, 位置) - key_arena 中的偏移
  pub entries: Vec<(u32, u16, Pos)>,
  pub restart_interval: usize,
  pub item_count: usize,
  // Reusable buffers to reduce allocation
  // 可复用缓冲区以减少分配
  restarts: Vec<u32>,
  buf: Vec<u8>,
  pub estimated_size: usize,
}

impl BlockBuilder {
  #[inline]
  pub fn new(restart_interval: usize) -> Self {
    Self {
      key_arena: Vec::with_capacity(8192),
      entries: Vec::with_capacity(1024),
      restart_interval: restart_interval.max(1),
      item_count: 0,
      restarts: Vec::with_capacity(64),
      buf: Vec::with_capacity(256),
      estimated_size: 0,
    }
  }

  /// Reset builder for reuse
  /// 重置构建器以复用
  pub fn reset(&mut self) {
    self.key_arena.clear();
    self.entries.clear();
    self.item_count = 0;
    self.restarts.clear();
    self.buf.clear();
    self.estimated_size = 0;
  }

  /// Add key-pos pair
  /// 添加键-位置对
  #[inline]
  pub fn add(&mut self, key: &[u8], pos: &Pos) -> Result<()> {
    let key_len = key.len();
    // Strict check to prevent silent data corruption via u16 truncation
    // 严格检查以防止 u16 截断导致的静默数据损坏
    if key_len > u16::MAX as usize {
      return Err(Error::KeyTooLarge(key_len));
    }

    // Safe: key.len() checked above, arena offset < 4GB in practice
    // 安全：key.len() 已检查，arena 偏移实际 < 4GB
    debug_assert!(
      (self.key_arena.len() + key_len) as u64 <= u32::MAX as u64,
      "Block size exceeded 4GB"
    );

    // Conservative estimate: key + pos + overhead (shared/unshared u16s)
    self.estimated_size += key_len + std::mem::size_of::<Pos>() + 4;
    self
      .entries
      .push((self.key_arena.len() as u32, key_len as u16, *pos));
    self.key_arena.extend_from_slice(key);
    self.item_count += 1;
    Ok(())
  }

  /// Build encoded block data with prefix compression
  /// 构建带前缀压缩的编码块数据
  pub fn build_encoded(&mut self) -> Vec<u8> {
    if self.entries.is_empty() {
      self.reset();
      return Vec::new();
    }

    let prefix_len = self.find_common_prefix_len();
    let prefix = &self.key_arena[..prefix_len];

    let mut buf = Vec::with_capacity(self.estimated_size);

    // Reuse restarts buffer
    // 复用 restarts 缓冲区
    self.restarts.clear();

    // Reuse buffer for delta encoding
    // 复用缓冲区进行增量编码
    self.buf.clear();

    buf.extend_from_slice(&(prefix_len as u16).to_le_bytes());
    buf.extend_from_slice(prefix);

    let mut restart_counter = 0;
    // Eliminate bounds check in loop
    for &(offset, len, pos) in self.entries.iter() {
      let key = &self.key_arena[offset as usize..offset as usize + len as usize];
      let is_restart = restart_counter == 0;
      let suffix = &key[prefix_len..];

      if is_restart {
        restart_counter = self.restart_interval;
        self.restarts.push(buf.len() as u32);
        buf.extend_from_slice(&(suffix.len() as u16).to_le_bytes());
        buf.extend_from_slice(suffix);
        // Update buf for delta encoding of next item
        // 更新 buf 用于下一项的增量编码
        self.buf.clear();
        self.buf.extend_from_slice(suffix);
      } else {
        // Delta encoding
        // 增量编码
        let shared = shared_prefix_len(&self.buf, suffix);
        let unshared = suffix.len() - shared;

        buf.extend_from_slice(&(shared as u16).to_le_bytes());
        buf.extend_from_slice(&(unshared as u16).to_le_bytes());
        buf.extend_from_slice(&suffix[shared..]);

        self.buf.truncate(shared);
        self.buf.extend_from_slice(&suffix[shared..]);
      }

      buf.extend_from_slice(pos.as_bytes());
      restart_counter -= 1;
    }

    for &restart in &self.restarts {
      buf.extend_from_slice(&restart.to_le_bytes());
    }
    buf.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(self.item_count as u32).to_le_bytes());

    self.reset();
    buf
  }

  fn find_common_prefix_len(&self) -> usize {
    let Some(&(first_off, first_len, _)) = self.entries.first() else {
      return 0;
    };
    let Some(&(last_off, last_len, _)) = self.entries.last() else {
      return 0;
    };

    let first_key = &self.key_arena[first_off as usize..first_off as usize + first_len as usize];
    let last_key = &self.key_arena[last_off as usize..last_off as usize + last_len as usize];

    shared_prefix_len(first_key, last_key)
  }

  #[inline]
  pub fn size(&self) -> usize {
    self.estimated_size
  }
}
