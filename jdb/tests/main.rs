use std::ops::Bound;

use aok::{OK, Void};
use jdb::{Entry, Memtable};
use jdb_base::Pos;
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test() -> Void {
  info!("> test {}", 123456);
  OK
}

#[test]
fn test_memtable_new() -> Void {
  let mt = Memtable::new(1);
  assert_eq!(mt.id(), 1);
  assert!(mt.is_empty());
  assert_eq!(mt.len(), 0);
  assert_eq!(mt.size(), 0);
  OK
}

#[test]
fn test_memtable_put_get() -> Void {
  let mut mt = Memtable::new(1);
  let key = b"hello".to_vec().into_boxed_slice();
  let pos = Pos::infile(1, 100, 50);

  mt.put(key, pos);

  assert_eq!(mt.len(), 1);
  assert!(!mt.is_empty());

  let entry = mt.get(b"hello").unwrap();
  assert_eq!(*entry, Entry::Value(pos));
  assert_eq!(entry.pos(), Some(pos));
  assert!(!entry.is_tombstone());

  // Non-existent key
  // 不存在的键
  assert!(mt.get(b"world").is_none());
  OK
}

#[test]
fn test_memtable_del() -> Void {
  let mut mt = Memtable::new(1);
  let key = b"hello".to_vec().into_boxed_slice();
  let pos = Pos::infile(1, 100, 50);

  // Put then delete
  // 先插入再删除
  mt.put(key.clone(), pos);
  mt.del(key);

  let entry = mt.get(b"hello").unwrap();
  assert!(entry.is_tombstone());
  assert_eq!(entry.pos(), None);
  OK
}

#[test]
fn test_memtable_iter() -> Void {
  let mut mt = Memtable::new(1);

  // Insert in random order
  // 随机顺序插入
  mt.put(b"c".to_vec().into_boxed_slice(), Pos::infile(1, 300, 30));
  mt.put(b"a".to_vec().into_boxed_slice(), Pos::infile(1, 100, 10));
  mt.put(b"b".to_vec().into_boxed_slice(), Pos::infile(1, 200, 20));

  // Forward iteration should be sorted
  // 正向迭代应该是有序的
  let keys: Vec<_> = mt.iter().map(|(k, _)| k).collect();
  assert_eq!(
    keys,
    vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
  );

  // Backward iteration
  // 反向迭代
  let keys: Vec<_> = mt.iter().rev().map(|(k, _)| k).collect();
  assert_eq!(
    keys,
    vec![b"c".as_slice(), b"b".as_slice(), b"a".as_slice()]
  );
  OK
}

#[test]
fn test_memtable_range() -> Void {
  let mut mt = Memtable::new(1);

  mt.put(b"a".to_vec().into_boxed_slice(), Pos::infile(1, 100, 10));
  mt.put(b"b".to_vec().into_boxed_slice(), Pos::infile(1, 200, 20));
  mt.put(b"c".to_vec().into_boxed_slice(), Pos::infile(1, 300, 30));
  mt.put(b"d".to_vec().into_boxed_slice(), Pos::infile(1, 400, 40));

  // Range [b, d)
  let keys: Vec<_> = mt
    .range(
      Bound::Included(b"b".as_slice()),
      Bound::Excluded(b"d".as_slice()),
    )
    .map(|(k, _)| k)
    .collect();
  assert_eq!(keys, vec![b"b".as_slice(), b"c".as_slice()]);

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
  assert_eq!(keys, vec![b"c".as_slice(), b"b".as_slice()]);
  OK
}

#[test]
fn test_memtable_size_tracking() -> Void {
  let mut mt = Memtable::new(1);

  // Initial size is 0
  // 初始大小为 0
  assert_eq!(mt.size(), 0);

  // Put adds key + Pos size
  // 插入增加键 + Pos 大小
  let key = b"hello".to_vec().into_boxed_slice();
  mt.put(key, Pos::infile(1, 100, 50));
  let expected_size = 5 + Pos::SIZE as u64; // "hello" = 5 bytes
  assert_eq!(mt.size(), expected_size);

  // Delete same key: size should decrease by Pos size
  // 删除相同键：大小应减少 Pos 大小
  mt.del(b"hello".to_vec().into_boxed_slice());
  assert_eq!(mt.size(), expected_size - Pos::SIZE as u64);
  OK
}

// Property-based tests
// 属性测试
mod proptest_memtable {
  use jdb::Memtable;
  use jdb_base::Pos;
  use proptest::prelude::*;

  // Generate random key-value pairs
  // 生成随机键值对
  fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..64)
  }

  fn arb_pos() -> impl Strategy<Value = Pos> {
    (any::<u64>(), any::<u64>(), any::<u32>())
      .prop_map(|(id, offset, len)| Pos::infile(id, offset, len))
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
      let mut mt = Memtable::new(1);

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
      let mut mt = Memtable::new(1);

      // Insert values
      // 插入值
      for (key, pos) in puts {
        mt.put(key.into_boxed_slice(), pos);
      }

      // Delete some keys
      // 删除一些键
      for key in dels {
        mt.del(key.into_boxed_slice());
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

      let mut mt = Memtable::new(1);

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
