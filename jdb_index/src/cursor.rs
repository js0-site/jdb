//! 范围扫描游标 Range scan cursor

use jdb_trait::Val;

use crate::{error::Result, key::Key, tree::BTree};

/// 游标 Cursor for range iteration
pub struct Cursor<'a> {
  tree: &'a BTree,
  slot_idx: usize,
  end_key: Option<Vec<u8>>,
  exhausted: bool,
  keys: Vec<Vec<u8>>,
  values: Vec<u64>,
  next_leaf: u32,
}

impl<'a> Cursor<'a> {
  /// 创建全表扫描游标 Create full scan cursor
  pub async fn scan(tree: &'a BTree) -> Result<Cursor<'a>> {
    let first = tree.first_leaf().await?;
    let (keys, values, next) = tree.read_leaf(first).await?;

    Ok(Self {
      tree,
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
    tree: &'a BTree,
    start: Option<&[Val]>,
    end: Option<&[Val]>,
  ) -> Result<Cursor<'a>> {
    let (leaf_id, slot_idx) = match start {
      Some(key) => tree.lower_bound(key).await?,
      None => (tree.first_leaf().await?, 0),
    };

    let end_key = end.map(|k| Key::encode(k).as_bytes().to_vec());
    let (keys, values, next) = tree.read_leaf(leaf_id).await?;

    Ok(Self {
      tree,
      slot_idx,
      end_key,
      exhausted: slot_idx >= keys.len(),
      keys,
      values,
      next_leaf: next,
    })
  }

  /// 获取下一个键值对 Get next key-value pair
  pub async fn next(&mut self) -> Result<Option<(Vec<Val>, u64)>> {
    if self.exhausted {
      return Ok(None);
    }

    loop {
      if self.slot_idx < self.keys.len() {
        let key = &self.keys[self.slot_idx];

        if let Some(ref end) = self.end_key
          && key >= end
        {
          self.exhausted = true;
          return Ok(None);
        }

        let vals = Key::from_bytes(key.clone()).decode();
        let value = self.values[self.slot_idx];
        self.slot_idx += 1;

        return Ok(Some((vals, value)));
      }

      if self.next_leaf == u32::MAX {
        self.exhausted = true;
        return Ok(None);
      }

      let next_id = self.next_leaf;
      self.slot_idx = 0;

      let (keys, values, next) = self.tree.read_leaf(next_id).await?;
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
  pub async fn next_batch(&mut self, limit: usize) -> Result<Vec<(Vec<Val>, u64)>> {
    let mut results = Vec::with_capacity(limit);

    for _ in 0..limit {
      match self.next().await? {
        Some(item) => results.push(item),
        None => break,
      }
    }

    Ok(results)
  }

  #[inline]
  pub fn is_exhausted(&self) -> bool {
    self.exhausted
  }
}
