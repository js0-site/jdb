//! 范围扫描游标 Range scan cursor

use jdb_comm::R;
use jdb_trait::Val;

use crate::key::Key;
use crate::tree::BTree;

/// 游标 Cursor for range iteration
pub struct Cursor<'a> {
  tree: &'a mut BTree,
  leaf_id: u32,
  slot_idx: usize,
  end_key: Option<Vec<u8>>,
  exhausted: bool,
  // 缓存当前叶子数据
  keys: Vec<Vec<u8>>,
  values: Vec<u64>,
  next_leaf: u32,
}

impl<'a> Cursor<'a> {
  /// 创建全表扫描游标 Create full scan cursor
  pub async fn scan(tree: &'a mut BTree) -> R<Cursor<'a>> {
    let first = tree.first_leaf().await?;
    let (keys, values, next) = tree.read_leaf(first).await?;

    Ok(Self {
      tree,
      leaf_id: first,
      slot_idx: 0,
      end_key: None,
      exhausted: keys.is_empty(),
      keys,
      values,
      next_leaf: next,
    })
  }

  /// 创建范围扫描游标 Create range scan cursor
  pub async fn range(
    tree: &'a mut BTree,
    start: Option<&[Val]>,
    end: Option<&[Val]>,
  ) -> R<Cursor<'a>> {
    let (leaf_id, slot_idx) = match start {
      Some(key) => tree.lower_bound(key).await?,
      None => (tree.first_leaf().await?, 0),
    };

    let end_key = end.map(|k| Key::encode(k).as_bytes().to_vec());
    let (keys, values, next) = tree.read_leaf(leaf_id).await?;

    Ok(Self {
      tree,
      leaf_id,
      slot_idx,
      end_key,
      exhausted: slot_idx >= keys.len(),
      keys,
      values,
      next_leaf: next,
    })
  }

  /// 获取下一个键值对 Get next key-value pair
  pub async fn next(&mut self) -> R<Option<(Vec<Val>, u64)>> {
    if self.exhausted {
      return Ok(None);
    }

    loop {
      // 当前叶子还有数据
      if self.slot_idx < self.keys.len() {
        let key = &self.keys[self.slot_idx];

        // 检查是否超过结束键
        if let Some(ref end) = self.end_key {
          if key >= end {
            self.exhausted = true;
            return Ok(None);
          }
        }

        let vals = Key::from_bytes(key.clone()).decode();
        let value = self.values[self.slot_idx];
        self.slot_idx += 1;

        return Ok(Some((vals, value)));
      }

      // 移动到下一个叶子
      if self.next_leaf == u32::MAX {
        self.exhausted = true;
        return Ok(None);
      }

      self.leaf_id = self.next_leaf;
      self.slot_idx = 0;

      let (keys, values, next) = self.tree.read_leaf(self.leaf_id).await?;
      self.keys = keys;
      self.values = values;
      self.next_leaf = next;

      if self.keys.is_empty() {
        self.exhausted = true;
        return Ok(None);
      }
    }
  }

  /// 批量获取 Batch get
  pub async fn next_batch(&mut self, limit: usize) -> R<Vec<(Vec<Val>, u64)>> {
    let mut results = Vec::with_capacity(limit);

    for _ in 0..limit {
      match self.next().await? {
        Some(item) => results.push(item),
        None => break,
      }
    }

    Ok(results)
  }

  /// 是否已耗尽 Is exhausted
  #[inline]
  pub fn is_exhausted(&self) -> bool {
    self.exhausted
  }
}
