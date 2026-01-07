use std::ops::Bound;

use aok::{OK, Void};
use jdb_base::{
  Pos,
  table::{Table, TableMut},
};
use jdb_mem::Mem;
use log::trace;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_memtable_new() -> Void {
  let mt = Mem::new();
  assert!(mt.id() > 0);
  assert!(mt.is_empty());
  assert_eq!(mt.len(), 0);
  assert_eq!(mt.size(), 0);
  OK
}

#[test]
fn test_memtable_put_get() -> Void {
  let mut mt = Mem::new();
  let key = b"hello".to_vec().into_boxed_slice();
  let pos = Pos::infile(1, 1, 100, 50);

  mt.put(key, pos);

  assert_eq!(mt.len(), 1);
  assert!(!mt.is_empty());

  let entry = mt.get(b"hello").unwrap();
  assert_eq!(entry, pos);
  assert!(!entry.is_tombstone());

  // Non-existent key
  // 不存在的键
  assert!(mt.get(b"world").is_none());
  OK
}

#[test]
fn test_memtable_del() -> Void {
  let mut mt = Mem::new();
  let key = b"hello".to_vec().into_boxed_slice();
  let pos = Pos::infile(1, 1, 100, 50);

  // Put then delete
  // 先插入再删除
  mt.put(key.clone(), pos);
  mt.rm(key);

  let entry = mt.get(b"hello").unwrap();
  assert!(entry.is_tombstone());
  OK
}

#[test]
fn test_memtable_iter() -> Void {
  let mut mt = Mem::new();

  // Insert in random order
  // 随机顺序插入
  mt.put(b"c".to_vec().into_boxed_slice(), Pos::infile(1, 1, 300, 30));
  mt.put(b"a".to_vec().into_boxed_slice(), Pos::infile(2, 1, 100, 10));
  mt.put(b"b".to_vec().into_boxed_slice(), Pos::infile(3, 1, 200, 20));

  // Forward iteration should be sorted
  // 正向迭代应该是有序的
  let keys: Vec<_> = mt.iter().map(|(k, _)| k).collect();
  assert_eq!(
    keys.iter().map(|k| k.as_ref()).collect::<Vec<_>>(),
    vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
  );

  // Backward iteration
  // 反向迭代
  let keys: Vec<_> = mt.iter().rev().map(|(k, _)| k).collect();
  assert_eq!(
    keys.iter().map(|k| k.as_ref()).collect::<Vec<_>>(),
    vec![b"c".as_slice(), b"b".as_slice(), b"a".as_slice()]
  );
  OK
}

#[test]
fn test_memtable_range() -> Void {
  let mut mt = Mem::new();

  mt.put(b"a".to_vec().into_boxed_slice(), Pos::infile(1, 1, 100, 10));
  mt.put(b"b".to_vec().into_boxed_slice(), Pos::infile(2, 1, 200, 20));
  mt.put(b"c".to_vec().into_boxed_slice(), Pos::infile(3, 1, 300, 30));
  mt.put(b"d".to_vec().into_boxed_slice(), Pos::infile(4, 1, 400, 40));

  // Range [b, d)
  let keys: Vec<_> = mt
    .range(
      Bound::Included(b"b".as_slice()),
      Bound::Excluded(b"d".as_slice()),
    )
    .map(|(k, _)| k)
    .collect();
  assert_eq!(
    keys.iter().map(|k| k.as_ref()).collect::<Vec<_>>(),
    vec![b"b".as_slice(), b"c".as_slice()]
  );

  // Reverse range
  // 反向范围
  let keys: Vec<_> = mt
    .range(
      Bound::Included(b"b".as_slice()),
      Bound::Excluded(b"d".as_slice()),
    )
    .rev()
    .map(|(k, _)| k)
    .collect();
  assert_eq!(
    keys.iter().map(|k| k.as_ref()).collect::<Vec<_>>(),
    vec![b"c".as_slice(), b"b".as_slice()]
  );
  OK
}

#[test]
fn test_memtable_size_tracking() -> Void {
  let mut mt = Mem::new();

  // Initial size is 0
  // 初始大小为 0
  assert_eq!(mt.size(), 0);

  // Put adds key + Pos size
  // 插入增加键 + Pos 大小
  let key = b"hello".to_vec().into_boxed_slice();
  mt.put(key, Pos::infile(1, 1, 100, 50));
  let expected_size = 5 + Pos::SIZE as u64; // "hello" = 5 bytes
  assert_eq!(mt.size(), expected_size);

  // Delete same key: size unchanged (tombstone replaces value)
  // 删除相同键：大小不变（tombstone 替换值）
  mt.rm(b"hello".to_vec().into_boxed_slice());
  assert_eq!(mt.size(), expected_size);
  OK
}

/// Test memtable with prefix keys (blart prefix issue)
/// 测试带前缀键的内存表（blart 前缀问题）
#[test]
fn test_memtable_prefix_keys() -> Void {
  let mut mt = Mem::new();

  // Insert key [0]
  // 插入键 [0]
  let key0 = vec![0u8].into_boxed_slice();
  let pos0 = Pos::infile(1, 0, 0, 0);
  mt.put(key0, pos0);
  trace!("Ckp put [0]: len={}", mt.len());

  // Insert key [0, 1] - this is NOT a prefix of [0], but [0] is a prefix of [0, 1]
  // 插入键 [0, 1] - 这不是 [0] 的前缀，但 [0] 是 [0, 1] 的前缀
  let key01 = vec![0u8, 1u8].into_boxed_slice();
  let pos01 = Pos::infile(2, 0, 100, 10);
  mt.put(key01, pos01);
  trace!("Ckp put [0, 1]: len={}", mt.len());

  // Both keys should exist
  // 两个键都应该存在
  let entry0 = mt.get(&[0u8]);
  let entry01 = mt.get(&[0u8, 1u8]);
  trace!("Get [0]: {:?}", entry0);
  trace!("Get [0, 1]: {:?}", entry01);

  assert!(entry0.is_some(), "Key [0] should exist");
  assert!(entry01.is_some(), "Key [0, 1] should exist");
  assert_eq!(mt.len(), 2, "Should have 2 entries");

  // Iterate and check
  // 迭代并检查
  let keys: Vec<_> = mt.iter().map(|(k, _)| k.to_vec()).collect();
  trace!("All keys: {:?}", keys);
  assert!(keys.contains(&vec![0u8]), "Should contain [0]");
  assert!(keys.contains(&vec![0u8, 1u8]), "Should contain [0, 1]");

  OK
}

/// Test keys containing null bytes with prefix relationships
/// 测试包含空字节且有前缀关系的键
#[test]
fn test_null_byte_prefix_keys() -> Void {
  let mut mt = Mem::new();

  // Keys with null bytes and prefix relationships
  // 包含空字节且有前缀关系的键
  let k1 = vec![0u8].into_boxed_slice();
  let k2 = vec![0u8, 0u8].into_boxed_slice();
  let k3 = vec![0u8, 0u8, 0u8].into_boxed_slice();
  let k4 = vec![0u8, 1u8].into_boxed_slice();
  let k5 = vec![0u8, 0u8, 1u8].into_boxed_slice();

  mt.put(k1.clone(), Pos::infile(1, 1, 10, 1));
  mt.put(k2.clone(), Pos::infile(2, 1, 20, 2));
  mt.put(k3.clone(), Pos::infile(3, 1, 30, 3));
  mt.put(k4.clone(), Pos::infile(4, 1, 40, 4));
  mt.put(k5.clone(), Pos::infile(5, 1, 50, 5));

  // All keys should exist
  // 所有键都应该存在
  assert_eq!(mt.len(), 5);
  assert!(mt.get(&[0u8]).is_some(), "[0] should exist");
  assert!(mt.get(&[0u8, 0u8]).is_some(), "[0,0] should exist");
  assert!(mt.get(&[0u8, 0u8, 0u8]).is_some(), "[0,0,0] should exist");
  assert!(mt.get(&[0u8, 1u8]).is_some(), "[0,1] should exist");
  assert!(mt.get(&[0u8, 0u8, 1u8]).is_some(), "[0,0,1] should exist");

  // Iteration should be sorted
  // 迭代应该是有序的
  let keys: Vec<Vec<u8>> = mt.iter().map(|(k, _)| k.to_vec()).collect();
  trace!("Keys with nulls: {:?}", keys);

  let mut sorted = keys.clone();
  sorted.sort();
  assert_eq!(keys, sorted, "Keys should be sorted");

  // Range query with null byte keys
  // 包含空字节键的范围查询
  let range_keys: Vec<Vec<u8>> = mt
    .range(
      Bound::Included([0u8, 0u8].as_slice()),
      Bound::Excluded([0u8, 1u8].as_slice()),
    )
    .map(|(k, _)| k.to_vec())
    .collect();

  assert!(range_keys.contains(&vec![0u8, 0u8]));
  assert!(range_keys.contains(&vec![0u8, 0u8, 0u8]));
  assert!(range_keys.contains(&vec![0u8, 0u8, 1u8]));
  assert!(!range_keys.contains(&vec![0u8]));
  assert!(!range_keys.contains(&vec![0u8, 1u8]));

  OK
}

// Property-based tests
// 属性测试
mod proptest_memtable {
  use jdb_base::{
    Pos,
    table::{Table, TableMut},
  };
  use jdb_mem::Mem;
  use proptest::prelude::*;

  // Generate random key-value pairs
  // 生成随机键值对
  fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..64)
  }

  fn arb_pos() -> impl Strategy<Value = Pos> {
    (any::<u64>(), any::<u64>(), any::<u64>(), any::<u32>())
      .prop_map(|(ver, id, offset, len)| Pos::infile(ver, id, offset, len))
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jdb-kv-database, Property 3: Iteration Ordering
    /// For any range of keys in the database, forward iteration should return keys
    /// in lexicographic ascending order, and backward iteration should return keys
    /// in lexicographic descending order.
    /// **Validates: Requirements 4.2, 4.3, 4.4, 4.5**
    #[test]
    fn prop_iteration_ordering(
      entries in prop::collection::vec((arb_key(), arb_pos()), 0..50)
    ) {
      let mut mt = Mem::new();

      // Insert all entries
      // 插入所有条目
      for (key, pos) in entries {
        mt.put(key.into_boxed_slice(), pos);
      }

      // Forward iteration should be sorted ascending
      // 正向迭代应该是升序排列
      let keys: Vec<_> = mt.iter().map(|(k, _)| k.to_vec()).collect();
      let mut sorted_keys = keys.clone();
      sorted_keys.sort();
      prop_assert_eq!(&keys, &sorted_keys, "Forward iteration not sorted");

      // Backward iteration should be sorted descending
      // 反向迭代应该是降序排列
      let rev_keys: Vec<_> = mt.iter().rev().map(|(k, _)| k.to_vec()).collect();
      let mut sorted_desc = keys;
      sorted_desc.reverse();
      prop_assert_eq!(rev_keys, sorted_desc, "Backward iteration not sorted descending");
    }

    /// Feature: jdb-kv-database, Property 3: Iteration Ordering (with tombstones)
    /// Tombstones should appear in iteration but can be filtered by caller.
    /// **Validates: Requirements 4.2, 4.3, 4.4, 4.5**
    #[test]
    fn prop_iteration_with_tombstones(
      puts in prop::collection::vec((arb_key(), arb_pos()), 0..30),
      dels in prop::collection::vec(arb_key(), 0..10)
    ) {
      let mut mt = Mem::new();

      // Insert values
      // 插入值
      for (key, pos) in puts {
        mt.put(key.into_boxed_slice(), pos);
      }

      // Delete some keys
      // 删除一些键
      for key in dels {
        mt.rm(key.into_boxed_slice());
      }

      // All keys (including tombstones) should be sorted
      // 所有键（包括删除标记）应该是有序的
      let keys: Vec<_> = mt.iter().map(|(k, _)| k.to_vec()).collect();
      let mut sorted_keys = keys.clone();
      sorted_keys.sort();
      prop_assert_eq!(keys, sorted_keys, "Keys not sorted with tombstones");

      // Filter out tombstones - remaining should still be sorted
      // 过滤删除标记 - 剩余的应该仍然有序
      let value_keys: Vec<_> = mt
        .iter()
        .filter(|(_, e)| !e.is_tombstone())
        .map(|(k, _)| k.to_vec())
        .collect();
      let mut sorted_value_keys = value_keys.clone();
      sorted_value_keys.sort();
      prop_assert_eq!(value_keys, sorted_value_keys, "Value keys not sorted");
    }

    /// Feature: jdb-kv-database, Property 3: Range iteration ordering
    /// Range queries should return keys in sorted order within bounds.
    /// **Validates: Requirements 4.2, 4.3, 4.4, 4.5**
    #[test]
    fn prop_range_iteration_ordering(
      entries in prop::collection::vec((arb_key(), arb_pos()), 1..50),
      start_idx in any::<usize>(),
      end_idx in any::<usize>()
    ) {
      use std::ops::Bound;

      let mut mt = Mem::new();

      // Insert all entries
      // 插入所有条目
      for (key, pos) in &entries {
        mt.put(key.clone().into_boxed_slice(), *pos);
      }

      // Get all keys sorted
      // 获取所有排序后的键
      let all_keys: Vec<_> = mt.iter().map(|(k, _)| k.to_vec()).collect();
      if all_keys.is_empty() {
        return Ok(());
      }

      // Pick range bounds from existing keys
      // 从现有键中选择范围边界
      let start_idx = start_idx % all_keys.len();
      let end_idx = end_idx % all_keys.len();
      let (start, end) = if start_idx <= end_idx {
        (start_idx, end_idx)
      } else {
        (end_idx, start_idx)
      };

      let start_key = &all_keys[start];
      let end_key = &all_keys[end];

      // Range query
      // 范围查询
      let range_keys: Vec<_> = mt
        .range(
          Bound::Included(start_key.as_slice()),
          Bound::Included(end_key.as_slice()),
        )
        .map(|(k, _)| k.to_vec())
        .collect();

      // Should be sorted
      // 应该是有序的
      let mut sorted_range = range_keys.clone();
      sorted_range.sort();
      prop_assert_eq!(&range_keys, &sorted_range, "Range keys not sorted");

      // Reverse should be descending
      // 反向应该是降序
      let rev_range_keys: Vec<_> = mt
        .range(
          Bound::Included(start_key.as_slice()),
          Bound::Included(end_key.as_slice()),
        )
        .rev()
        .map(|(k, _)| k.to_vec())
        .collect();

      let mut sorted_desc = range_keys;
      sorted_desc.reverse();
      prop_assert_eq!(rev_range_keys, sorted_desc, "Reverse range not descending");
    }
  }
}

#[test]
fn test_mem_basic() {
  let mut mem = Mem::new();

  // Test put via TableMut trait
  // 通过 TableMut trait 测试插入
  mem.put(b"key1".as_slice(), Pos::infile(1, 1, 100, 10));
  mem.put(b"key2".as_slice(), Pos::infile(2, 1, 200, 20));
  mem.put(b"key3".as_slice(), Pos::infile(3, 1, 300, 30));

  assert_eq!(mem.len(), 3);
  assert!(!mem.is_empty());
  assert!(mem.id() > 0);

  // Test get via Table trait
  // 通过 Table trait 测试获取
  let pos = mem.get(b"key1").unwrap();
  assert_eq!(pos.offset(), 100);
  assert_eq!(pos.len(), 10);

  assert!(mem.get(b"nonexistent").is_none());

  // Test rm (tombstone) via TableMut trait
  // 通过 TableMut trait 测试删除（墓碑标记）
  mem.rm(b"key2".as_slice());
  let pos = mem.get(b"key2").unwrap();
  assert!(pos.is_tombstone());
}

#[test]
fn test_mem_range() {
  let mut mem = Mem::new();

  mem.put(b"a".as_slice(), Pos::infile(1, 1, 1, 1));
  mem.put(b"b".as_slice(), Pos::infile(2, 1, 2, 2));
  mem.put(b"c".as_slice(), Pos::infile(3, 1, 3, 3));
  mem.put(b"d".as_slice(), Pos::infile(4, 1, 4, 4));

  // Test range via Table trait
  // 通过 Table trait 测试范围查询
  use std::ops::Bound;
  let items: Vec<_> = mem
    .range(
      Bound::Included(b"b".as_slice()),
      Bound::Excluded(b"d".as_slice()),
    )
    .collect();
  assert_eq!(items.len(), 2);
  assert_eq!(items[0].0.as_ref(), b"b");
  assert_eq!(items[1].0.as_ref(), b"c");
}

#[test]
fn test_mem_iter() {
  let mut mem = Mem::new();

  mem.put(b"z".as_slice(), Pos::infile(1, 1, 1, 1));
  mem.put(b"a".as_slice(), Pos::infile(2, 1, 2, 2));
  mem.put(b"m".as_slice(), Pos::infile(3, 1, 3, 3));

  // Test iter via Table trait (should be sorted)
  // 通过 Table trait 测试迭代（应该是有序的）
  let items: Vec<_> = Table::iter(&mem).collect();
  assert_eq!(items.len(), 3);
  assert_eq!(items[0].0.as_ref(), b"a");
  assert_eq!(items[1].0.as_ref(), b"m");
  assert_eq!(items[2].0.as_ref(), b"z");
}

#[test]
fn test_mem_prefix() {
  let mut mem = Mem::new();

  mem.put(b"user:1".as_slice(), Pos::infile(1, 1, 1, 1));
  mem.put(b"user:2".as_slice(), Pos::infile(2, 1, 2, 2));
  mem.put(b"user:10".as_slice(), Pos::infile(3, 1, 3, 3));
  mem.put(b"item:1".as_slice(), Pos::infile(4, 1, 4, 4));

  // Test prefix via Table trait
  // 通过 Table trait 测试前缀查询
  let items: Vec<_> = mem.prefix(b"user:").collect();
  assert_eq!(items.len(), 3);

  let items: Vec<_> = mem.prefix(b"item:").collect();
  assert_eq!(items.len(), 1);
}

#[test]
fn test_mem_size() {
  let mut mem = Mem::new();

  assert_eq!(mem.size(), 0);

  // key1 (4 bytes) + Pos (32 bytes) = 36
  mem.put(b"key1".as_slice(), Pos::infile(1, 1, 100, 10));
  assert_eq!(mem.size(), 36);

  // Replace same key, size unchanged
  // 替换相同键，大小不变
  mem.put(b"key1".as_slice(), Pos::infile(1, 1, 200, 20));
  assert_eq!(mem.size(), 36);

  // key2 (4 bytes) + Pos (32 bytes) = 36, total = 72
  mem.put(b"key2".as_slice(), Pos::infile(1, 1, 300, 30));
  assert_eq!(mem.size(), 72);
}
