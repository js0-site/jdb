//! B+ Tree node types / B+ 树节点类型

use bytes::Bytes;
use jdb_trait::ValRef;

use crate::{Error, Result};

/// Page ID type / 页 ID 类型
pub type PageId = u64;

/// Node type marker / 节点类型标记
const NODE_INTERNAL: u8 = 1;
const NODE_LEAF: u8 = 2;

/// Header size / 头部大小
const HEADER_SIZE: usize = 4; // type(1) + count(2) + reserved(1)

/// Max keys per node (approximate) / 每节点最大键数
pub const MAX_KEYS: usize = 128;

/// Internal node / 内部节点
#[derive(Debug, Clone)]
pub struct Internal {
  pub keys: Vec<Bytes>,
  pub children: Vec<PageId>,
}

/// Leaf node with prefix compression / 前缀压缩叶子节点
#[derive(Debug, Clone)]
pub struct Leaf {
  pub prefix: Bytes,
  pub suffixes: Vec<Bytes>,
  pub vals: Vec<ValRef>,
  pub prev: PageId,
  pub next: PageId,
}

/// Node enum / 节点枚举
#[derive(Debug, Clone)]
pub enum Node {
  Internal(Internal),
  Leaf(Leaf),
}

impl Default for Internal {
  fn default() -> Self {
    Self::new()
  }
}

impl Internal {
  pub fn new() -> Self {
    Self {
      keys: Vec::new(),
      children: Vec::new(),
    }
  }

  /// Find child index for key / 查找 key 对应的子节点索引
  pub fn find_child(&self, key: &[u8]) -> usize {
    match self.keys.binary_search_by(|k| k.as_ref().cmp(key)) {
      Ok(i) => i + 1,
      Err(i) => i,
    }
  }

  /// Serialize to buffer / 序列化到缓冲区
  pub fn serialize(&self, buf: &mut [u8]) -> Result<()> {
    let count = self.keys.len();
    if count > u16::MAX as usize {
      return Err(Error::NodeOverflow);
    }

    buf[0] = NODE_INTERNAL;
    buf[1..3].copy_from_slice(&(count as u16).to_le_bytes());

    let mut pos = HEADER_SIZE;

    // keys / 键
    for key in &self.keys {
      let len = key.len();
      if pos + 2 + len > buf.len() {
        return Err(Error::NodeOverflow);
      }
      buf[pos..pos + 2].copy_from_slice(&(len as u16).to_le_bytes());
      pos += 2;
      buf[pos..pos + len].copy_from_slice(key);
      pos += len;
    }

    // children (count + 1) / 子节点
    for &child in &self.children {
      if pos + 8 > buf.len() {
        return Err(Error::NodeOverflow);
      }
      buf[pos..pos + 8].copy_from_slice(&child.to_le_bytes());
      pos += 8;
    }

    Ok(())
  }

  /// Deserialize from buffer / 从缓冲区反序列化
  pub fn deserialize(buf: &[u8]) -> Result<Self> {
    let count = u16::from_le_bytes([buf[1], buf[2]]) as usize;
    let mut pos = HEADER_SIZE;

    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
      let len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
      pos += 2;
      keys.push(Bytes::copy_from_slice(&buf[pos..pos + len]));
      pos += len;
    }

    let mut children = Vec::with_capacity(count + 1);
    for _ in 0..=count {
      let child = u64::from_le_bytes([
        buf[pos],
        buf[pos + 1],
        buf[pos + 2],
        buf[pos + 3],
        buf[pos + 4],
        buf[pos + 5],
        buf[pos + 6],
        buf[pos + 7],
      ]);
      pos += 8;
      children.push(child);
    }

    Ok(Self { keys, children })
  }
}

impl Default for Leaf {
  fn default() -> Self {
    Self::new()
  }
}

impl Leaf {
  pub fn new() -> Self {
    Self {
      prefix: Bytes::new(),
      suffixes: Vec::new(),
      vals: Vec::new(),
      prev: 0,
      next: 0,
    }
  }

  /// Restore full key / 恢复完整 key
  pub fn key(&self, idx: usize) -> Bytes {
    let mut full = Vec::with_capacity(self.prefix.len() + self.suffixes[idx].len());
    full.extend_from_slice(&self.prefix);
    full.extend_from_slice(&self.suffixes[idx]);
    Bytes::from(full)
  }

  /// Find key, return (found, index) / 查找 key
  pub fn find(&self, key: &[u8]) -> (bool, usize) {
    // Check prefix / 检查前缀
    if key.len() < self.prefix.len() || &key[..self.prefix.len()] != self.prefix.as_ref() {
      // Key doesn't match prefix / key 不匹配前缀
      let cmp = key.cmp(self.prefix.as_ref());
      return match cmp {
        std::cmp::Ordering::Less => (false, 0),
        _ => (false, self.suffixes.len()),
      };
    }

    let suffix = &key[self.prefix.len()..];
    match self.suffixes.binary_search_by(|s| s.as_ref().cmp(suffix)) {
      Ok(i) => (true, i),
      Err(i) => (false, i),
    }
  }

  /// Insert key-value / 插入键值
  pub fn insert(&mut self, key: &[u8], val: ValRef) {
    // Restore all keys first / 先恢复所有 key
    let mut keys: Vec<Bytes> = (0..self.suffixes.len()).map(|i| self.key(i)).collect();

    // Find insert position / 查找插入位置
    let key_bytes = Bytes::copy_from_slice(key);
    match keys.binary_search(&key_bytes) {
      Ok(i) => {
        // Update existing / 更新已有
        self.vals[i] = val;
        return;
      }
      Err(i) => {
        // Insert new / 插入新的
        keys.insert(i, key_bytes);
        self.vals.insert(i, val);
      }
    }

    // Recompute prefix from full keys / 从完整 key 重算前缀
    self.recompute_prefix_from_keys(&keys);
  }

  /// Recompute prefix from full keys / 从完整 key 重算前缀
  fn recompute_prefix_from_keys(&mut self, keys: &[Bytes]) {
    if keys.is_empty() {
      self.prefix = Bytes::new();
      self.suffixes.clear();
      return;
    }

    if keys.len() == 1 {
      self.prefix = keys[0].clone();
      self.suffixes = vec![Bytes::new()];
      return;
    }

    // Find LCP / 查找 LCP
    let first = &keys[0];
    let mut lcp_len = first.len();
    for k in &keys[1..] {
      let common = first
        .iter()
        .zip(k.iter())
        .take_while(|(a, b)| a == b)
        .count();
      lcp_len = lcp_len.min(common);
    }

    self.prefix = Bytes::copy_from_slice(&first[..lcp_len]);
    self.suffixes = keys
      .iter()
      .map(|k| Bytes::copy_from_slice(&k[lcp_len..]))
      .collect();
  }

  /// Delete key, return old value / 删除 key
  pub fn delete(&mut self, key: &[u8]) -> Option<ValRef> {
    // Restore all keys / 恢复所有 key
    let keys: Vec<Bytes> = (0..self.suffixes.len()).map(|i| self.key(i)).collect();
    let key_bytes = Bytes::copy_from_slice(key);

    match keys.binary_search(&key_bytes) {
      Ok(i) => {
        self.suffixes.remove(i);
        let val = self.vals.remove(i);

        // Rebuild keys without deleted one / 重建 keys
        let new_keys: Vec<Bytes> = keys
          .into_iter()
          .enumerate()
          .filter(|(j, _)| *j != i)
          .map(|(_, k)| k)
          .collect();
        self.recompute_prefix_from_keys(&new_keys);

        Some(val)
      }
      Err(_) => None,
    }
  }

  /// Split leaf, return (split_key, new_leaf) / 分裂叶子
  pub fn split(&mut self) -> (Bytes, Leaf) {
    // Restore all keys first / 先恢复所有 key
    let keys: Vec<Bytes> = (0..self.suffixes.len()).map(|i| self.key(i)).collect();
    let mid = keys.len() / 2;

    // Split key / 分裂键
    let split_key = keys[mid].clone();

    // Split vals / 分裂值
    let right_vals: Vec<_> = self.vals.drain(mid..).collect();

    // Left keys and right keys / 左右 keys
    let left_keys: Vec<Bytes> = keys[..mid].to_vec();
    let right_keys: Vec<Bytes> = keys[mid..].to_vec();

    // Recompute left / 重算左边
    self.recompute_prefix_from_keys(&left_keys);

    // Create right leaf / 创建右叶子
    let mut right = Leaf {
      prefix: Bytes::new(),
      suffixes: Vec::new(),
      vals: right_vals,
      prev: 0,
      next: self.next,
    };
    right.recompute_prefix_from_keys(&right_keys);

    (split_key, right)
  }

  /// Compute longest common prefix / 计算最长公共前缀
  pub fn recompute_prefix(&mut self) {
    if self.suffixes.is_empty() {
      self.prefix = Bytes::new();
      return;
    }

    if self.suffixes.len() == 1 {
      // Single key: prefix = full key, suffix = empty / 单键
      let full = if self.prefix.is_empty() {
        self.suffixes[0].clone()
      } else {
        let mut v = Vec::with_capacity(self.prefix.len() + self.suffixes[0].len());
        v.extend_from_slice(&self.prefix);
        v.extend_from_slice(&self.suffixes[0]);
        Bytes::from(v)
      };
      self.prefix = full;
      self.suffixes[0] = Bytes::new();
      return;
    }

    // Restore all full keys / 恢复所有完整 key
    let keys: Vec<Bytes> = (0..self.suffixes.len()).map(|i| self.key(i)).collect();

    // Find LCP / 查找 LCP
    let first = &keys[0];
    let mut lcp_len = first.len();
    for k in &keys[1..] {
      let common = first
        .iter()
        .zip(k.iter())
        .take_while(|(a, b)| a == b)
        .count();
      lcp_len = lcp_len.min(common);
    }

    // Update prefix and suffixes / 更新前缀和后缀
    self.prefix = Bytes::copy_from_slice(&first[..lcp_len]);
    self.suffixes = keys
      .iter()
      .map(|k| Bytes::copy_from_slice(&k[lcp_len..]))
      .collect();
  }

  /// Serialize to buffer / 序列化
  pub fn serialize(&self, buf: &mut [u8]) -> Result<()> {
    let count = self.suffixes.len();
    if count > u16::MAX as usize {
      return Err(Error::NodeOverflow);
    }

    buf[0] = NODE_LEAF;
    buf[1..3].copy_from_slice(&(count as u16).to_le_bytes());

    let mut pos = HEADER_SIZE;

    // prefix / 前缀
    let plen = self.prefix.len();
    if pos + 2 + plen > buf.len() {
      return Err(Error::NodeOverflow);
    }
    buf[pos..pos + 2].copy_from_slice(&(plen as u16).to_le_bytes());
    pos += 2;
    buf[pos..pos + plen].copy_from_slice(&self.prefix);
    pos += plen;

    // suffixes + vals / 后缀 + 值
    for i in 0..count {
      let slen = self.suffixes[i].len();
      if pos + 2 + slen + 32 > buf.len() {
        return Err(Error::NodeOverflow);
      }
      buf[pos..pos + 2].copy_from_slice(&(slen as u16).to_le_bytes());
      pos += 2;
      buf[pos..pos + slen].copy_from_slice(&self.suffixes[i]);
      pos += slen;

      // ValRef (32 bytes) / 值引用
      let v = &self.vals[i];
      buf[pos..pos + 8].copy_from_slice(&v.file_id.to_le_bytes());
      buf[pos + 8..pos + 16].copy_from_slice(&v.offset.to_le_bytes());
      buf[pos + 16..pos + 24].copy_from_slice(&v.prev_file_id.to_le_bytes());
      buf[pos + 24..pos + 32].copy_from_slice(&v.prev_offset.to_le_bytes());
      pos += 32;
    }

    // prev, next / 前驱后继
    if pos + 16 > buf.len() {
      return Err(Error::NodeOverflow);
    }
    buf[pos..pos + 8].copy_from_slice(&self.prev.to_le_bytes());
    buf[pos + 8..pos + 16].copy_from_slice(&self.next.to_le_bytes());

    Ok(())
  }

  /// Deserialize from buffer / 反序列化
  pub fn deserialize(buf: &[u8]) -> Result<Self> {
    let count = u16::from_le_bytes([buf[1], buf[2]]) as usize;
    let mut pos = HEADER_SIZE;

    // prefix / 前缀
    let plen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
    pos += 2;
    let prefix = Bytes::copy_from_slice(&buf[pos..pos + plen]);
    pos += plen;

    // suffixes + vals / 后缀 + 值
    let mut suffixes = Vec::with_capacity(count);
    let mut vals = Vec::with_capacity(count);
    for _ in 0..count {
      let slen = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
      pos += 2;
      suffixes.push(Bytes::copy_from_slice(&buf[pos..pos + slen]));
      pos += slen;

      let file_id = u64::from_le_bytes([
        buf[pos],
        buf[pos + 1],
        buf[pos + 2],
        buf[pos + 3],
        buf[pos + 4],
        buf[pos + 5],
        buf[pos + 6],
        buf[pos + 7],
      ]);
      let offset = u64::from_le_bytes([
        buf[pos + 8],
        buf[pos + 9],
        buf[pos + 10],
        buf[pos + 11],
        buf[pos + 12],
        buf[pos + 13],
        buf[pos + 14],
        buf[pos + 15],
      ]);
      let prev_file_id = u64::from_le_bytes([
        buf[pos + 16],
        buf[pos + 17],
        buf[pos + 18],
        buf[pos + 19],
        buf[pos + 20],
        buf[pos + 21],
        buf[pos + 22],
        buf[pos + 23],
      ]);
      let prev_offset = u64::from_le_bytes([
        buf[pos + 24],
        buf[pos + 25],
        buf[pos + 26],
        buf[pos + 27],
        buf[pos + 28],
        buf[pos + 29],
        buf[pos + 30],
        buf[pos + 31],
      ]);
      pos += 32;
      vals.push(ValRef {
        file_id,
        offset,
        prev_file_id,
        prev_offset,
      });
    }

    // prev, next / 前驱后继
    let prev = u64::from_le_bytes([
      buf[pos],
      buf[pos + 1],
      buf[pos + 2],
      buf[pos + 3],
      buf[pos + 4],
      buf[pos + 5],
      buf[pos + 6],
      buf[pos + 7],
    ]);
    let next = u64::from_le_bytes([
      buf[pos + 8],
      buf[pos + 9],
      buf[pos + 10],
      buf[pos + 11],
      buf[pos + 12],
      buf[pos + 13],
      buf[pos + 14],
      buf[pos + 15],
    ]);

    Ok(Self {
      prefix,
      suffixes,
      vals,
      prev,
      next,
    })
  }

  /// Check if need split / 检查是否需要分裂
  pub fn need_split(&self) -> bool {
    self.suffixes.len() >= MAX_KEYS
  }
}

impl Node {
  /// Serialize node / 序列化节点
  pub fn serialize(&self, buf: &mut [u8]) -> Result<()> {
    match self {
      Node::Internal(n) => n.serialize(buf),
      Node::Leaf(n) => n.serialize(buf),
    }
  }

  /// Deserialize node / 反序列化节点
  pub fn deserialize(buf: &[u8]) -> Result<Self> {
    match buf[0] {
      NODE_INTERNAL => Ok(Node::Internal(Internal::deserialize(buf)?)),
      NODE_LEAF => Ok(Node::Leaf(Leaf::deserialize(buf)?)),
      t => Err(Error::InvalidNodeType(t)),
    }
  }
}
