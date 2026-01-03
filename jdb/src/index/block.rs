//! Block format with prefix compression
//! 带前缀压缩的块格式
//!
//! Stores key-Pos pairs with prefix compression for space efficiency.
//! 使用前缀压缩存储键-位置对以提高空间效率。
//!
//! Format per entry:
//! - Restart head: [key_len: u16] [key] [Pos: 24 bytes]
//! - Truncated: [shared_len: u16] [unshared_len: u16] [unshared_key] [Pos: 24 bytes]

use jdb_base::Pos;

/// Default restart interval
/// 默认重启间隔
pub const DEFAULT_RESTART_INTERVAL: usize = 16;

/// Block builder with prefix compression
/// 带前缀压缩的块构建器
pub struct BlockBuilder {
  buf: Vec<u8>,
  restarts: Vec<u32>,
  last_key: Vec<u8>,
  restart_interval: usize,
  counter: usize,
  item_count: usize,
}

impl BlockBuilder {
  /// Create new block builder
  /// 创建新的块构建器
  #[inline]
  pub fn new(restart_interval: usize) -> Self {
    Self {
      buf: Vec::new(),
      restarts: Vec::new(),
      last_key: Vec::new(),
      restart_interval: restart_interval.max(1),
      counter: 0,
      item_count: 0,
    }
  }

  /// Create with default restart interval
  /// 使用默认重启间隔创建
  #[inline]
  pub fn with_default() -> Self {
    Self::new(DEFAULT_RESTART_INTERVAL)
  }

  /// Add key-pos pair
  /// 添加键-位置对
  pub fn add(&mut self, key: &[u8], pos: &Pos) {
    let is_restart = self.counter == 0;

    if is_restart {
      // Record restart point
      // 记录重启点
      self.restarts.push(self.buf.len() as u32);

      // Write full key: [key_len: u16] [key]
      // 写入完整键
      let key_len = key.len() as u16;
      self.buf.extend_from_slice(&key_len.to_le_bytes());
      self.buf.extend_from_slice(key);

      self.last_key.clear();
      self.last_key.extend_from_slice(key);
    } else {
      // Calculate shared prefix length
      // 计算共享前缀长度
      let shared_len = shared_prefix_len(&self.last_key, key);
      let unshared_len = key.len() - shared_len;

      // Write truncated: [shared_len: u16] [unshared_len: u16] [unshared_key]
      // 写入截断格式
      self
        .buf
        .extend_from_slice(&(shared_len as u16).to_le_bytes());
      self
        .buf
        .extend_from_slice(&(unshared_len as u16).to_le_bytes());
      self.buf.extend_from_slice(&key[shared_len..]);

      self.last_key.truncate(shared_len);
      self.last_key.extend_from_slice(&key[shared_len..]);
    }

    // Write Pos (24 bytes)
    // 写入 Pos（24 字节）
    self
      .buf
      .extend_from_slice(zerocopy::IntoBytes::as_bytes(pos));

    self.counter = (self.counter + 1) % self.restart_interval;
    self.item_count += 1;
  }

  /// Finish building and return DataBlock
  /// 完成构建并返回 DataBlock
  pub fn finish(mut self) -> DataBlock {
    let data_end = self.buf.len();

    // Write restart points
    // 写入重启点
    for &restart in &self.restarts {
      self.buf.extend_from_slice(&restart.to_le_bytes());
    }

    // Write restart count
    // 写入重启点数量
    let restart_count = self.restarts.len() as u32;
    self.buf.extend_from_slice(&restart_count.to_le_bytes());

    // Write item count
    // 写入条目数量
    let item_count = self.item_count as u32;
    self.buf.extend_from_slice(&item_count.to_le_bytes());

    DataBlock {
      data: self.buf,
      data_end,
      restart_count: restart_count as usize,
      item_count: self.item_count,
    }
  }

  /// Get current buffer size (approximate block size)
  /// 获取当前缓冲区大小（近似块大小）
  #[inline]
  pub fn size(&self) -> usize {
    self.buf.len()
  }

  /// Check if empty
  /// 检查是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.item_count == 0
  }

  /// Get item count
  /// 获取条目数量
  #[inline]
  pub fn len(&self) -> usize {
    self.item_count
  }
}

/// Data block with prefix compression
/// 带前缀压缩的数据块
#[derive(Debug, Clone)]
pub struct DataBlock {
  data: Vec<u8>,
  data_end: usize,
  restart_count: usize,
  item_count: usize,
}

impl DataBlock {
  /// Create from raw bytes
  /// 从原始字节创建
  pub fn from_bytes(data: Vec<u8>) -> Option<Self> {
    if data.len() < 8 {
      return None;
    }

    // Read trailer: restart_count (u32) + item_count (u32)
    // 读取尾部
    let len = data.len();
    let item_count =
      u32::from_le_bytes([data[len - 4], data[len - 3], data[len - 2], data[len - 1]]) as usize;
    let restart_count =
      u32::from_le_bytes([data[len - 8], data[len - 7], data[len - 6], data[len - 5]]) as usize;

    // Calculate data_end
    // 计算数据结束位置
    let trailer_size = 8 + restart_count * 4;
    if data.len() < trailer_size {
      return None;
    }
    let data_end = data.len() - trailer_size;

    Some(Self {
      data,
      data_end,
      restart_count,
      item_count,
    })
  }

  /// Get raw bytes
  /// 获取原始字节
  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    &self.data
  }

  /// Get data section (without trailer)
  /// 获取数据部分（不含尾部）
  #[inline]
  pub fn data_section(&self) -> &[u8] {
    &self.data[..self.data_end]
  }

  /// Get item count
  /// 获取条目数量
  #[inline]
  pub fn len(&self) -> usize {
    self.item_count
  }

  /// Check if empty
  /// 检查是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.item_count == 0
  }

  /// Get restart point offset
  /// 获取重启点偏移
  #[inline]
  fn restart_offset(&self, idx: usize) -> usize {
    let p = self.data_end + idx * 4;
    u32::from_le_bytes([
      self.data[p],
      self.data[p + 1],
      self.data[p + 2],
      self.data[p + 3],
    ]) as usize
  }

  /// Create iterator
  /// 创建迭代器
  #[inline]
  pub fn iter(&self) -> BlockIter<'_> {
    BlockIter::new(self)
  }
}

/// Block iterator supporting DoubleEndedIterator
/// 支持双向迭代的块迭代器
pub struct BlockIter<'a> {
  block: &'a DataBlock,
  // Forward state
  // 正向状态
  lo_offset: usize,
  lo_restart_idx: usize,
  lo_in_interval: usize,
  lo_base_key: Vec<u8>,
  // Backward state
  // 反向状态
  hi_restart_idx: usize,
  hi_stack: Vec<(usize, Vec<u8>, Pos)>,
  hi_filled: bool,
  // Consumed count
  // 已消费数量
  consumed_lo: usize,
  consumed_hi: usize,
}

impl<'a> BlockIter<'a> {
  fn new(block: &'a DataBlock) -> Self {
    Self {
      block,
      lo_offset: 0,
      lo_restart_idx: 0,
      lo_in_interval: 0,
      lo_base_key: Vec::new(),
      hi_restart_idx: block.restart_count,
      hi_stack: Vec::new(),
      hi_filled: false,
      consumed_lo: 0,
      consumed_hi: 0,
    }
  }

  fn parse_at(
    &self,
    offset: usize,
    is_restart: bool,
    base_key: &[u8],
  ) -> Option<(usize, Vec<u8>, Pos)> {
    let data = self.block.data_section();
    if offset >= data.len() {
      return None;
    }

    let mut p = offset;

    let key = if is_restart {
      if p + 2 > data.len() {
        return None;
      }
      let key_len = u16::from_le_bytes([data[p], data[p + 1]]) as usize;
      p += 2;

      if p + key_len > data.len() {
        return None;
      }
      let key = data[p..p + key_len].to_vec();
      p += key_len;
      key
    } else {
      if p + 4 > data.len() {
        return None;
      }
      let shared_len = u16::from_le_bytes([data[p], data[p + 1]]) as usize;
      let unshared_len = u16::from_le_bytes([data[p + 2], data[p + 3]]) as usize;
      p += 4;

      if p + unshared_len > data.len() || shared_len > base_key.len() {
        return None;
      }

      let mut key = base_key[..shared_len].to_vec();
      key.extend_from_slice(&data[p..p + unshared_len]);
      p += unshared_len;
      key
    };

    if p + Pos::SIZE > data.len() {
      return None;
    }
    let pos_bytes = &data[p..p + Pos::SIZE];
    let pos: Pos = zerocopy::FromBytes::read_from_bytes(pos_bytes).ok()?;
    p += Pos::SIZE;

    Some((p, key, pos))
  }

  fn fill_hi_stack(&mut self) {
    if self.hi_restart_idx == 0 {
      self.hi_filled = true;
      return;
    }

    self.hi_restart_idx -= 1;
    self.hi_stack.clear();

    let start_offset = self.block.restart_offset(self.hi_restart_idx);
    let end_offset = if self.hi_restart_idx + 1 < self.block.restart_count {
      self.block.restart_offset(self.hi_restart_idx + 1)
    } else {
      self.block.data_end
    };

    let mut offset = start_offset;
    let mut base_key = Vec::new();
    let mut is_first = true;

    while offset < end_offset {
      if let Some((new_offset, key, pos)) = self.parse_at(offset, is_first, &base_key) {
        base_key.clone_from(&key);
        is_first = false;
        self.hi_stack.push((offset, key, pos));
        offset = new_offset;
      } else {
        break;
      }
    }

    self.hi_filled = true;
  }
}

impl Iterator for BlockIter<'_> {
  type Item = (Vec<u8>, Pos);

  fn next(&mut self) -> Option<Self::Item> {
    if self.consumed_lo + self.consumed_hi >= self.block.item_count {
      return None;
    }

    let is_restart = self.lo_in_interval == 0;

    if is_restart && self.lo_restart_idx < self.block.restart_count {
      self.lo_offset = self.block.restart_offset(self.lo_restart_idx);
      self.lo_restart_idx += 1;
    }

    let (new_offset, key, pos) = self.parse_at(self.lo_offset, is_restart, &self.lo_base_key)?;

    self.lo_base_key.clone_from(&key);
    self.lo_offset = new_offset;
    self.lo_in_interval += 1;

    if self.lo_restart_idx < self.block.restart_count {
      let next_restart_offset = self.block.restart_offset(self.lo_restart_idx);
      if self.lo_offset >= next_restart_offset {
        self.lo_in_interval = 0;
      }
    }

    self.consumed_lo += 1;
    Some((key, pos))
  }
}

impl DoubleEndedIterator for BlockIter<'_> {
  fn next_back(&mut self) -> Option<Self::Item> {
    if self.consumed_lo + self.consumed_hi >= self.block.item_count {
      return None;
    }

    loop {
      if let Some((_, key, pos)) = self.hi_stack.pop() {
        self.consumed_hi += 1;
        return Some((key, pos));
      }

      if self.hi_restart_idx == 0 {
        return None;
      }

      self.hi_filled = false;
      self.fill_hi_stack();
    }
  }
}

#[inline]
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
  a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_shared_prefix_len() {
    assert_eq!(shared_prefix_len(b"hello", b"hello"), 5);
    assert_eq!(shared_prefix_len(b"hello", b"help"), 3);
    assert_eq!(shared_prefix_len(b"hello", b"world"), 0);
    assert_eq!(shared_prefix_len(b"", b"hello"), 0);
  }

  #[test]
  fn test_block_builder_empty() {
    let builder = BlockBuilder::new(4);
    assert!(builder.is_empty());
    assert_eq!(builder.len(), 0);

    let block = builder.finish();
    assert!(block.is_empty());
    assert_eq!(block.len(), 0);
  }

  #[test]
  fn test_block_roundtrip_single() {
    let mut builder = BlockBuilder::new(4);
    let pos = Pos::infile(1, 100, 50);
    builder.add(b"hello", &pos);

    let block = builder.finish();
    assert_eq!(block.len(), 1);

    let items: Vec<_> = block.iter().collect();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0, b"hello");
    assert_eq!(items[0].1, pos);
  }

  #[test]
  fn test_block_roundtrip_multiple() {
    let mut builder = BlockBuilder::new(4);

    let entries = [
      (b"aaa".to_vec(), Pos::infile(1, 100, 10)),
      (b"aab".to_vec(), Pos::infile(1, 200, 20)),
      (b"abc".to_vec(), Pos::tombstone(1, 0)),
      (b"bbb".to_vec(), Pos::infile(1, 300, 30)),
      (b"ccc".to_vec(), Pos::infile(1, 400, 40)),
    ];

    for (key, pos) in &entries {
      builder.add(key, pos);
    }

    let block = builder.finish();
    assert_eq!(block.len(), 5);

    let items: Vec<_> = block.iter().collect();
    assert_eq!(items.len(), 5);
    for (i, (key, pos)) in items.iter().enumerate() {
      assert_eq!(key, &entries[i].0);
      assert_eq!(pos, &entries[i].1);
    }
  }

  #[test]
  fn test_block_iter_backward() {
    let mut builder = BlockBuilder::new(2);

    let entries = [
      (b"a".to_vec(), Pos::infile(1, 100, 10)),
      (b"b".to_vec(), Pos::infile(1, 200, 20)),
      (b"c".to_vec(), Pos::infile(1, 300, 30)),
      (b"d".to_vec(), Pos::infile(1, 400, 40)),
    ];

    for (key, pos) in &entries {
      builder.add(key, pos);
    }

    let block = builder.finish();

    let items: Vec<_> = block.iter().rev().collect();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0].0, b"d");
    assert_eq!(items[1].0, b"c");
    assert_eq!(items[2].0, b"b");
    assert_eq!(items[3].0, b"a");
  }

  #[test]
  fn test_block_iter_ping_pong() {
    let mut builder = BlockBuilder::new(2);

    let entries = [
      (b"a".to_vec(), Pos::infile(1, 100, 10)),
      (b"b".to_vec(), Pos::infile(1, 200, 20)),
      (b"c".to_vec(), Pos::infile(1, 300, 30)),
      (b"d".to_vec(), Pos::infile(1, 400, 40)),
    ];

    for (key, pos) in &entries {
      builder.add(key, pos);
    }

    let block = builder.finish();
    let mut iter = block.iter();

    assert_eq!(iter.next().map(|(k, _)| k), Some(b"a".to_vec()));
    assert_eq!(iter.next_back().map(|(k, _)| k), Some(b"d".to_vec()));
    assert_eq!(iter.next().map(|(k, _)| k), Some(b"b".to_vec()));
    assert_eq!(iter.next_back().map(|(k, _)| k), Some(b"c".to_vec()));
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());
  }

  #[test]
  fn test_block_from_bytes() {
    let mut builder = BlockBuilder::new(4);
    builder.add(b"key1", &Pos::infile(1, 100, 10));
    builder.add(b"key2", &Pos::tombstone(1, 0));

    let block = builder.finish();
    let bytes = block.as_bytes().to_vec();

    let block2 = DataBlock::from_bytes(bytes).expect("should parse");
    assert_eq!(block2.len(), 2);

    let items: Vec<_> = block2.iter().collect();
    assert_eq!(items[0].0, b"key1");
    assert_eq!(items[1].0, b"key2");
    assert!(items[1].1.is_tombstone());
  }

  #[test]
  fn test_block_prefix_compression() {
    let mut builder = BlockBuilder::new(16);

    let keys = [
      b"user:1000:name".to_vec(),
      b"user:1000:email".to_vec(),
      b"user:1000:age".to_vec(),
      b"user:1001:name".to_vec(),
      b"user:1001:email".to_vec(),
    ];

    for key in &keys {
      builder.add(key, &Pos::infile(1, 100, 10));
    }

    let block = builder.finish();

    let items: Vec<_> = block.iter().collect();
    for (i, (key, _)) in items.iter().enumerate() {
      assert_eq!(key, &keys[i], "Forward mismatch at index {i}");
    }

    let rev_items: Vec<_> = block.iter().rev().collect();
    for (i, (key, _)) in rev_items.iter().enumerate() {
      let expected_idx = keys.len() - 1 - i;
      assert_eq!(
        key, &keys[expected_idx],
        "Backward mismatch at index {i}, expected index {expected_idx}"
      );
    }
  }
}
