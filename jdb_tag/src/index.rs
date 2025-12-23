//! Tag index implementation 标签索引实现

use roaring::RoaringBitmap;
use std::collections::HashMap;

/// Tag inverted index 标签倒排索引
pub struct TagIndex {
  /// tag_key:tag_value -> id set
  index: HashMap<Vec<u8>, RoaringBitmap>,
}

impl TagIndex {
  /// Create new index 创建新索引
  pub fn new() -> Self {
    Self {
      index: HashMap::new(),
    }
  }

  /// Build tag key (key:value) 构建标签键
  #[inline]
  fn tag_key(key: &[u8], val: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(key.len() + 1 + val.len());
    k.extend_from_slice(key);
    k.push(b':');
    k.extend_from_slice(val);
    k
  }

  /// Add tag to id 为 id 添加标签
  pub fn add(&mut self, id: u32, key: &[u8], val: &[u8]) {
    let tag = Self::tag_key(key, val);
    self.index.entry(tag).or_default().insert(id);
  }

  /// Remove tag from id 从 id 移除标签
  pub fn remove(&mut self, id: u32, key: &[u8], val: &[u8]) {
    let tag = Self::tag_key(key, val);
    if let Some(bitmap) = self.index.get_mut(&tag) {
      bitmap.remove(id);
    }
  }

  /// Get ids by tag 通过标签获取 id 集合
  pub fn get(&self, key: &[u8], val: &[u8]) -> Option<&RoaringBitmap> {
    let tag = Self::tag_key(key, val);
    self.index.get(&tag)
  }

  /// AND query: ids matching all tags AND 查询：匹配所有标签的 id
  pub fn and(&self, tags: &[(&[u8], &[u8])]) -> RoaringBitmap {
    let mut result: Option<RoaringBitmap> = None;

    for (key, val) in tags {
      let bitmap = match self.get(key, val) {
        Some(b) => b.clone(),
        None => return RoaringBitmap::new(),
      };

      result = Some(match result {
        Some(r) => r & bitmap,
        None => bitmap,
      });
    }

    result.unwrap_or_default()
  }

  /// OR query: ids matching any tag OR 查询：匹配任一标签的 id
  pub fn or(&self, tags: &[(&[u8], &[u8])]) -> RoaringBitmap {
    let mut result = RoaringBitmap::new();

    for (key, val) in tags {
      if let Some(bitmap) = self.get(key, val) {
        result |= bitmap;
      }
    }

    result
  }

  /// NOT query: exclude ids with tag NOT 查询：排除有标签的 id
  pub fn not(&self, base: &RoaringBitmap, key: &[u8], val: &[u8]) -> RoaringBitmap {
    match self.get(key, val) {
      Some(bitmap) => base - bitmap,
      None => base.clone(),
    }
  }

  /// Count ids with tag 统计有标签的 id 数量
  pub fn count(&self, key: &[u8], val: &[u8]) -> u64 {
    self.get(key, val).map(|b| b.len()).unwrap_or(0)
  }
}

impl Default for TagIndex {
  fn default() -> Self {
    Self::new()
  }
}
