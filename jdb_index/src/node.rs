//! B+ tree node B+ 树节点

use jdb_comm::{PageID, PAGE_SIZE};
use jdb_layout::{page_type, PageHeader};

/// Max keys per node (approximate) 每节点最大键数（近似）
pub const MAX_KEYS: usize = 128;

/// B+ tree node B+ 树节点
pub struct Node {
  pub header: PageHeader,
  pub keys: Vec<Vec<u8>>,
  pub children: Vec<PageID>, // Internal: child pages 内部节点：子页面
  pub values: Vec<Vec<u8>>,  // Leaf: values 叶子节点：值
  pub next: Option<PageID>,  // Leaf: next leaf 叶子节点：下一叶子
}

impl Node {
  /// Create leaf node 创建叶子节点
  pub fn leaf(id: PageID) -> Self {
    Self {
      header: PageHeader::new(id, page_type::LEAF, jdb_comm::Lsn::new(0)),
      keys: Vec::new(),
      children: Vec::new(),
      values: Vec::new(),
      next: None,
    }
  }

  /// Create internal node 创建内部节点
  pub fn internal(id: PageID) -> Self {
    Self {
      header: PageHeader::new(id, page_type::INTERNAL, jdb_comm::Lsn::new(0)),
      keys: Vec::new(),
      children: Vec::new(),
      values: Vec::new(),
      next: None,
    }
  }

  /// Is leaf 是否叶子
  #[inline]
  pub fn is_leaf(&self) -> bool {
    self.header.typ == page_type::LEAF
  }

  /// Key count 键数量
  #[inline]
  pub fn len(&self) -> usize {
    self.keys.len()
  }

  /// Is empty 是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.keys.is_empty()
  }

  /// Is full 是否已满
  #[inline]
  pub fn is_full(&self) -> bool {
    self.keys.len() >= MAX_KEYS
  }

  /// Find key position (binary search) 查找键位置（二分查找）
  pub fn find_key(&self, key: &[u8]) -> Result<usize, usize> {
    self.keys.binary_search_by(|k| k.as_slice().cmp(key))
  }

  /// Insert into leaf 插入到叶子
  pub fn insert_leaf(&mut self, key: Vec<u8>, val: Vec<u8>) {
    match self.find_key(&key) {
      Ok(i) => {
        // Update existing 更新已有
        self.values[i] = val;
      }
      Err(i) => {
        // Insert new 插入新的
        self.keys.insert(i, key);
        self.values.insert(i, val);
      }
    }
  }

  /// Get from leaf 从叶子获取
  pub fn get_leaf(&self, key: &[u8]) -> Option<&[u8]> {
    match self.find_key(key) {
      Ok(i) => Some(&self.values[i]),
      Err(_) => None,
    }
  }

  /// Delete from leaf 从叶子删除
  pub fn delete_leaf(&mut self, key: &[u8]) -> bool {
    match self.find_key(key) {
      Ok(i) => {
        self.keys.remove(i);
        self.values.remove(i);
        true
      }
      Err(_) => false,
    }
  }

  /// Find child for key (internal node) 查找键对应的子节点（内部节点）
  pub fn find_child(&self, key: &[u8]) -> PageID {
    let i = match self.find_key(key) {
      Ok(i) => i + 1,
      Err(i) => i,
    };
    self.children[i]
  }

  /// Serialize to bytes 序列化为字节
  pub fn serialize(&self) -> Vec<u8> {
    let mut buf = vec![0u8; PAGE_SIZE];

    // Write header 写入头
    self.header.write(&mut buf[..32]);

    let mut off = 32usize;

    // Write key count 写入键数量
    let key_count = self.keys.len() as u16;
    buf[off..off + 2].copy_from_slice(&key_count.to_le_bytes());
    off += 2;

    // Write keys 写入键
    for key in &self.keys {
      let len = key.len() as u16;
      buf[off..off + 2].copy_from_slice(&len.to_le_bytes());
      off += 2;
      buf[off..off + key.len()].copy_from_slice(key);
      off += key.len();
    }

    if self.is_leaf() {
      // Write values 写入值
      for val in &self.values {
        let len = val.len() as u16;
        buf[off..off + 2].copy_from_slice(&len.to_le_bytes());
        off += 2;
        buf[off..off + val.len()].copy_from_slice(val);
        off += val.len();
      }

      // Write next pointer 写入下一指针
      let next = self.next.map(|p| p.0).unwrap_or(u32::MAX);
      buf[off..off + 4].copy_from_slice(&next.to_le_bytes());
    } else {
      // Write children 写入子节点
      for child in &self.children {
        buf[off..off + 4].copy_from_slice(&child.0.to_le_bytes());
        off += 4;
      }
    }

    buf
  }

  /// Deserialize from bytes 从字节反序列化
  pub fn deserialize(buf: &[u8]) -> Self {
    let header = PageHeader::read(&buf[..32]);
    let is_leaf = header.typ == page_type::LEAF;

    let mut off = 32usize;

    // Read key count 读取键数量
    let key_count = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
    off += 2;

    // Read keys 读取键
    let mut keys = Vec::with_capacity(key_count);
    for _ in 0..key_count {
      let len = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
      off += 2;
      keys.push(buf[off..off + len].to_vec());
      off += len;
    }

    let (values, children, next) = if is_leaf {
      // Read values 读取值
      let mut values = Vec::with_capacity(key_count);
      for _ in 0..key_count {
        let len = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
        off += 2;
        values.push(buf[off..off + len].to_vec());
        off += len;
      }

      // Read next 读取下一指针
      let next_val = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
      let next = if next_val == u32::MAX {
        None
      } else {
        Some(PageID::new(next_val))
      };

      (values, Vec::new(), next)
    } else {
      // Read children 读取子节点
      let child_count = key_count + 1;
      let mut children = Vec::with_capacity(child_count);
      for _ in 0..child_count {
        let id = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        children.push(PageID::new(id));
        off += 4;
      }

      (Vec::new(), children, None)
    };

    Self {
      header,
      keys,
      children,
      values,
      next,
    }
  }
}
