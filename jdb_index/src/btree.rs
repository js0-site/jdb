//! B+ tree implementation B+ 树实现

use crate::Node;
use jdb_comm::PageID;
use std::collections::HashMap;

/// B+ tree B+ 树
pub struct BTree {
  root: Option<PageID>,
  next_page: u32,
  nodes: HashMap<u32, Node>,
}

impl BTree {
  /// Create new B+ tree 创建新 B+ 树
  pub fn new() -> Self {
    Self {
      root: None,
      next_page: 0,
      nodes: HashMap::new(),
    }
  }

  /// Allocate new page ID 分配新页面 ID
  fn alloc_page(&mut self) -> PageID {
    let id = PageID::new(self.next_page);
    self.next_page += 1;
    id
  }

  /// Insert key-value 插入键值
  pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) {
    if self.root.is_none() {
      let id = self.alloc_page();
      let mut node = Node::leaf(id);
      node.insert_leaf(key, val);
      self.nodes.insert(id.0, node);
      self.root = Some(id);
      return;
    }

    let root_id = self.root.unwrap();
    self.insert_recursive(root_id, key, val);

    // Check if root needs split 检查根是否需要分裂
    let root = self.nodes.get(&root_id.0).unwrap();
    if root.is_full() {
      self.split_root(root_id);
    }
  }

  /// Insert recursively 递归插入
  fn insert_recursive(&mut self, id: PageID, key: Vec<u8>, val: Vec<u8>) {
    let is_leaf = self.nodes.get(&id.0).map(|n| n.is_leaf()).unwrap_or(true);

    if is_leaf {
      if let Some(node) = self.nodes.get_mut(&id.0) {
        node.insert_leaf(key, val);
      }
    } else {
      let child_id = {
        let node = self.nodes.get(&id.0).unwrap();
        node.find_child(&key)
      };

      self.insert_recursive(child_id, key.clone(), val);

      // Check child split 检查子节点分裂
      let child_full = self.nodes.get(&child_id.0).map(|n| n.is_full()).unwrap_or(false);
      if child_full {
        self.split_child(id, child_id);
      }
    }
  }

  /// Split root node 分裂根节点
  fn split_root(&mut self, old_root: PageID) {
    let is_leaf = self.nodes.get(&old_root.0).map(|n| n.is_leaf()).unwrap_or(true);

    if is_leaf {
      let new_id = self.alloc_page();
      let (mid, right_keys, right_vals, old_next) = {
        let node = self.nodes.get(&old_root.0).unwrap();
        let mid = node.len() / 2;
        (
          mid,
          node.keys[mid..].to_vec(),
          node.values[mid..].to_vec(),
          node.next,
        )
      };

      let mut right = Node::leaf(new_id);
      right.keys = right_keys;
      right.values = right_vals;
      right.next = old_next;

      let split_key = right.keys[0].clone();

      if let Some(left) = self.nodes.get_mut(&old_root.0) {
        left.keys.truncate(mid);
        left.values.truncate(mid);
        left.next = Some(new_id);
      }

      self.nodes.insert(new_id.0, right);

      let root_id = self.alloc_page();
      let mut root = Node::internal(root_id);
      root.keys.push(split_key);
      root.children.push(old_root);
      root.children.push(new_id);
      self.nodes.insert(root_id.0, root);
      self.root = Some(root_id);
    } else {
      // Split internal root 分裂内部根
      let new_id = self.alloc_page();
      let (mid, split_key, right_keys, right_children) = {
        let node = self.nodes.get(&old_root.0).unwrap();
        let mid = node.len() / 2;
        (
          mid,
          node.keys[mid].clone(),
          node.keys[mid + 1..].to_vec(),
          node.children[mid + 1..].to_vec(),
        )
      };

      let mut right = Node::internal(new_id);
      right.keys = right_keys;
      right.children = right_children;

      if let Some(left) = self.nodes.get_mut(&old_root.0) {
        left.keys.truncate(mid);
        left.children.truncate(mid + 1);
      }

      self.nodes.insert(new_id.0, right);

      let root_id = self.alloc_page();
      let mut root = Node::internal(root_id);
      root.keys.push(split_key);
      root.children.push(old_root);
      root.children.push(new_id);
      self.nodes.insert(root_id.0, root);
      self.root = Some(root_id);
    }
  }

  /// Split child of parent 分裂父节点的子节点
  fn split_child(&mut self, parent_id: PageID, child_id: PageID) {
    let is_leaf = self.nodes.get(&child_id.0).map(|n| n.is_leaf()).unwrap_or(true);
    let new_id = self.alloc_page();

    let (split_key, child_idx) = if is_leaf {
      let (mid, right_keys, right_vals, old_next) = {
        let node = self.nodes.get(&child_id.0).unwrap();
        let mid = node.len() / 2;
        (
          mid,
          node.keys[mid..].to_vec(),
          node.values[mid..].to_vec(),
          node.next,
        )
      };

      let mut right = Node::leaf(new_id);
      right.keys = right_keys;
      right.values = right_vals;
      right.next = old_next;

      let split_key = right.keys[0].clone();

      if let Some(left) = self.nodes.get_mut(&child_id.0) {
        left.keys.truncate(mid);
        left.values.truncate(mid);
        left.next = Some(new_id);
      }

      self.nodes.insert(new_id.0, right);

      let idx = self
        .nodes
        .get(&parent_id.0)
        .unwrap()
        .children
        .iter()
        .position(|c| c.0 == child_id.0)
        .unwrap();

      (split_key, idx)
    } else {
      let (mid, split_key, right_keys, right_children) = {
        let node = self.nodes.get(&child_id.0).unwrap();
        let mid = node.len() / 2;
        (
          mid,
          node.keys[mid].clone(),
          node.keys[mid + 1..].to_vec(),
          node.children[mid + 1..].to_vec(),
        )
      };

      let mut right = Node::internal(new_id);
      right.keys = right_keys;
      right.children = right_children;

      if let Some(left) = self.nodes.get_mut(&child_id.0) {
        left.keys.truncate(mid);
        left.children.truncate(mid + 1);
      }

      self.nodes.insert(new_id.0, right);

      let idx = self
        .nodes
        .get(&parent_id.0)
        .unwrap()
        .children
        .iter()
        .position(|c| c.0 == child_id.0)
        .unwrap();

      (split_key, idx)
    };

    // Insert into parent 插入父节点
    if let Some(parent) = self.nodes.get_mut(&parent_id.0) {
      parent.keys.insert(child_idx, split_key);
      parent.children.insert(child_idx + 1, new_id);
    }
  }

  /// Get value by key 通过键获取值
  pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
    let root_id = self.root?;
    let leaf_id = self.find_leaf(root_id, key);
    let node = self.nodes.get(&leaf_id.0)?;
    node.get_leaf(key).map(|v| v.to_vec())
  }

  /// Find leaf node for key 查找键对应的叶子节点
  fn find_leaf(&self, start: PageID, key: &[u8]) -> PageID {
    let node = match self.nodes.get(&start.0) {
      Some(n) => n,
      None => return start,
    };

    if node.is_leaf() {
      return start;
    }

    let child = node.find_child(key);
    self.find_leaf(child, key)
  }

  /// Delete key 删除键
  pub fn delete(&mut self, key: &[u8]) -> bool {
    let root_id = match self.root {
      Some(id) => id,
      None => return false,
    };

    let leaf_id = self.find_leaf(root_id, key);

    if let Some(node) = self.nodes.get_mut(&leaf_id.0) {
      node.delete_leaf(key)
    } else {
      false
    }
  }

  /// Range scan 范围扫描
  pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut result = Vec::new();

    let root_id = match self.root {
      Some(id) => id,
      None => return result,
    };

    let mut leaf_id = Some(self.find_leaf(root_id, start));

    while let Some(id) = leaf_id {
      let node = match self.nodes.get(&id.0) {
        Some(n) => n,
        None => break,
      };

      for (i, key) in node.keys.iter().enumerate() {
        if key.as_slice() >= start && key.as_slice() <= end {
          result.push((key.clone(), node.values[i].clone()));
        }
        if key.as_slice() > end {
          return result;
        }
      }

      leaf_id = node.next;
    }

    result
  }
}

impl Default for BTree {
  fn default() -> Self {
    Self::new()
  }
}
