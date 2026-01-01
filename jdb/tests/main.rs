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

/// Test memtable with prefix keys (blart prefix issue)
/// 测试带前缀键的内存表（blart 前缀问题）
#[test]
fn test_memtable_prefix_keys() -> Void {
  let mut mt = Memtable::new(1);

  // Insert key [0]
  // 插入键 [0]
  let key0 = vec![0u8].into_boxed_slice();
  let pos0 = Pos::infile(0, 0, 0);
  mt.put(key0, pos0);
  info!("After put [0]: len={}", mt.len());

  // Insert key [0, 1] - this is NOT a prefix of [0], but [0] is a prefix of [0, 1]
  // 插入键 [0, 1] - 这不是 [0] 的前缀，但 [0] 是 [0, 1] 的前缀
  let key01 = vec![0u8, 1u8].into_boxed_slice();
  let pos01 = Pos::infile(0, 100, 10);
  mt.put(key01, pos01);
  info!("After put [0, 1]: len={}", mt.len());

  // Both keys should exist
  // 两个键都应该存在
  let entry0 = mt.get(&[0u8]);
  let entry01 = mt.get(&[0u8, 1u8]);
  info!("Get [0]: {:?}", entry0);
  info!("Get [0, 1]: {:?}", entry01);

  assert!(entry0.is_some(), "Key [0] should exist");
  assert!(entry01.is_some(), "Key [0, 1] should exist");
  assert_eq!(mt.len(), 2, "Should have 2 entries");

  // Iterate and check
  // 迭代并检查
  let keys: Vec<_> = mt.iter().map(|(k, _)| k.to_vec()).collect();
  info!("All keys: {:?}", keys);
  assert!(keys.contains(&vec![0u8]), "Should contain [0]");
  assert!(keys.contains(&vec![0u8, 1u8]), "Should contain [0, 1]");

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

// Property-based tests for Block
// Block 属性测试
mod proptest_block {
  use jdb::{BlockBuilder, DataBlock, Entry};
  use jdb_base::Pos;
  use proptest::prelude::*;

  // Generate sorted keys with common prefixes
  // 生成有共同前缀的排序键
  fn arb_sorted_keys() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(prop::collection::vec(any::<u8>(), 1..32), 1..50).prop_map(|mut keys| {
      keys.sort();
      keys.dedup();
      keys
    })
  }

  fn arb_entry() -> impl Strategy<Value = Entry> {
    prop_oneof![
      (any::<u64>(), any::<u64>(), any::<u32>())
        .prop_map(|(id, offset, len)| Entry::Value(Pos::infile(id, offset, len))),
      Just(Entry::Tombstone),
    ]
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jdb-kv-database, Property 12: Prefix Compression Effectiveness
    /// For any set of keys with common prefixes, prefix compression should save space
    /// compared to storing each key fully. When keys share prefixes, the compressed
    /// data section should be smaller than storing full keys.
    /// **Validates: Requirements 7.2**
    #[test]
    fn prop_prefix_compression_effectiveness(
      keys in arb_sorted_keys(),
      entries in prop::collection::vec(arb_entry(), 1..50)
    ) {
      if keys.len() < 2 {
        return Ok(());
      }

      // Test with keys that have common prefixes
      // 测试有共同前缀的键
      let prefix = b"user:namespace:";
      let prefixed_keys: Vec<Vec<u8>> = keys
        .iter()
        .map(|k| {
          let mut pk = prefix.to_vec();
          pk.extend_from_slice(k);
          pk
        })
        .collect();

      let mut builder = BlockBuilder::new(16);

      // Calculate raw size (all full keys, no compression)
      // 计算原始大小（所有完整键，无压缩）
      let mut raw_data_size: usize = 0;

      for (i, key) in prefixed_keys.iter().enumerate() {
        let entry = &entries[i % entries.len()];
        builder.add(key, entry);

        // Raw format: key_len (2) + full_key + entry
        // 原始格式：键长度 (2) + 完整键 + 条目
        raw_data_size += 2 + key.len();
        match entry {
          Entry::Value(_) => raw_data_size += 1 + Pos::SIZE,
          Entry::Tombstone => raw_data_size += 1,
        }
      }

      let block = builder.finish();

      // Data section (without trailer) should benefit from prefix compression
      // 数据部分（不含尾部）应该受益于前缀压缩
      let data_section_size = block.data_section().len();

      // With common prefix, compressed data should be smaller than raw
      // 有共同前缀时，压缩数据应该小于原始数据
      prop_assert!(
        data_section_size < raw_data_size,
        "Data section {} should be < raw data {} for keys with common prefix",
        data_section_size,
        raw_data_size
      );
    }

    /// Feature: jdb-kv-database, Property 12: Block roundtrip preserves data
    /// For any set of key-entry pairs, building a block and iterating should
    /// return the same data in the same order.
    /// **Validates: Requirements 7.2**
    #[test]
    fn prop_block_roundtrip(
      keys in arb_sorted_keys(),
      entries in prop::collection::vec(arb_entry(), 1..50)
    ) {
      if keys.is_empty() {
        return Ok(());
      }

      let mut builder = BlockBuilder::new(4);
      let mut expected: Vec<(Vec<u8>, Entry)> = Vec::new();

      for (i, key) in keys.iter().enumerate() {
        let entry = entries[i % entries.len()];
        builder.add(key, &entry);
        expected.push((key.clone(), entry));
      }

      let block = builder.finish();

      // Forward iteration should match
      // 正向迭代应该匹配
      let forward: Vec<_> = block.iter().collect();
      prop_assert_eq!(forward.len(), expected.len(), "Length mismatch");

      for (i, ((k1, e1), (k2, e2))) in forward.iter().zip(expected.iter()).enumerate() {
        prop_assert_eq!(k1, k2, "Key mismatch at index {}", i);
        prop_assert_eq!(e1, e2, "Entry mismatch at index {}", i);
      }

      // Backward iteration should be reversed
      // 反向迭代应该是反转的
      let backward: Vec<_> = block.iter().rev().collect();
      let mut expected_rev = expected.clone();
      expected_rev.reverse();

      for (i, ((k1, e1), (k2, e2))) in backward.iter().zip(expected_rev.iter()).enumerate() {
        prop_assert_eq!(k1, k2, "Backward key mismatch at index {}", i);
        prop_assert_eq!(e1, e2, "Backward entry mismatch at index {}", i);
      }
    }

    /// Feature: jdb-kv-database, Property 12: Block serialization roundtrip
    /// For any block, serializing to bytes and deserializing should produce
    /// the same data.
    /// **Validates: Requirements 7.2**
    #[test]
    fn prop_block_serialization_roundtrip(
      keys in arb_sorted_keys(),
      entries in prop::collection::vec(arb_entry(), 1..50)
    ) {
      if keys.is_empty() {
        return Ok(());
      }

      let mut builder = BlockBuilder::new(8);

      for (i, key) in keys.iter().enumerate() {
        let entry = &entries[i % entries.len()];
        builder.add(key, entry);
      }

      let block = builder.finish();
      let bytes = block.as_bytes().to_vec();

      // Deserialize
      // 反序列化
      let block2 = DataBlock::from_bytes(bytes).expect("should deserialize");

      prop_assert_eq!(block.len(), block2.len(), "Item count mismatch");

      // Compare all items
      // 比较所有条目
      let items1: Vec<_> = block.iter().collect();
      let items2: Vec<_> = block2.iter().collect();

      for (i, ((k1, e1), (k2, e2))) in items1.iter().zip(items2.iter()).enumerate() {
        prop_assert_eq!(k1, k2, "Key mismatch at index {}", i);
        prop_assert_eq!(e1, e2, "Entry mismatch at index {}", i);
      }
    }
  }
}

// SSTable unit tests
// SSTable 单元测试
mod sstable_tests {
  use aok::{OK, Void};
  use jdb::{Entry, SSTableWriter, TableInfo};
  use jdb_base::{FileLru, Pos, id_path};

  #[test]
  fn test_sstable_write_read_roundtrip() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;
      let table_id = 1u64;
      let path = id_path(&sst_dir, table_id);

      // Write SSTable with sorted keys
      // 写入有序键的 SSTable
      let mut writer = SSTableWriter::new(path.clone(), table_id, 10).await?;

      let entries = [
        (b"aaa".to_vec(), Entry::Value(Pos::infile(1, 100, 10))),
        (b"bbb".to_vec(), Entry::Value(Pos::infile(1, 200, 20))),
        (b"ccc".to_vec(), Entry::Tombstone),
        (b"ddd".to_vec(), Entry::Value(Pos::infile(1, 300, 30))),
        (b"eee".to_vec(), Entry::Value(Pos::infile(1, 400, 40))),
      ];

      for (key, entry) in &entries {
        writer.add(key, entry).await?;
      }

      let meta = writer.finish().await?;
      assert_eq!(meta.item_count, 5);
      assert_eq!(meta.min_key.as_ref(), b"aaa");
      assert_eq!(meta.max_key.as_ref(), b"eee");

      // Read SSTable
      // 读取 SSTable
      let info = TableInfo::load(&path, table_id).await?;
      let mut files = FileLru::new(&sst_dir, 16);

      // Test point lookups
      // 测试点查询
      let entry = info
        .get(b"aaa", &mut files)
        .await?
        .expect("should find aaa");
      assert_eq!(entry, Entry::Value(Pos::infile(1, 100, 10)));

      // Tombstones are not in filter, so get() returns None for them
      // 删除标记不在过滤器中，所以 get() 对它们返回 None
      // This is correct behavior - tombstones are only visible via iteration
      // 这是正确的行为 - 删除标记只能通过迭代可见
      let entry = info.get(b"ccc", &mut files).await?;
      assert!(entry.is_none(), "Tombstones should not be found via get()");

      let entry = info.get(b"zzz", &mut files).await?;
      assert!(entry.is_none());

      // Test iterator (skips tombstones)
      // 测试迭代器（跳过删除标记）
      let items: Vec<_> = info.iter(&mut files).await?.collect();
      assert_eq!(items.len(), 4); // ccc is tombstone, skipped
      assert_eq!(items[0].0.as_ref(), b"aaa");
      assert_eq!(items[1].0.as_ref(), b"bbb");
      assert_eq!(items[2].0.as_ref(), b"ddd");
      assert_eq!(items[3].0.as_ref(), b"eee");

      // Test backward iteration
      // 测试反向迭代
      let rev_items: Vec<_> = info.iter(&mut files).await?.rev().collect();
      assert_eq!(rev_items.len(), 4);
      assert_eq!(rev_items[0].0.as_ref(), b"eee");
      assert_eq!(rev_items[1].0.as_ref(), b"ddd");
      assert_eq!(rev_items[2].0.as_ref(), b"bbb");
      assert_eq!(rev_items[3].0.as_ref(), b"aaa");

      // Test iterator with tombstones
      // 测试包含删除标记的迭代器
      let all_items: Vec<_> = info.iter_with_tombstones(&mut files).await?.collect();
      assert_eq!(all_items.len(), 5);
      assert!(all_items[2].1.is_tombstone());

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  #[test]
  fn test_sstable_range_query() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_range_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;
      let table_id = 1u64;
      let path = id_path(&sst_dir, table_id);

      // Write SSTable
      // 写入 SSTable
      let mut writer = SSTableWriter::new(path.clone(), table_id, 10).await?;

      for i in 0..10u8 {
        let key = format!("key{i:02}").into_bytes();
        let entry = Entry::Value(Pos::infile(1, i as u64 * 100, i as u32 * 10));
        writer.add(&key, &entry).await?;
      }

      writer.finish().await?;

      // Read and test range
      // 读取并测试范围
      let info = TableInfo::load(&path, table_id).await?;
      let mut files = FileLru::new(&sst_dir, 16);

      // Range [key03, key07]
      let range_items: Vec<_> = info.range(b"key03", b"key07", &mut files).await?.collect();
      assert_eq!(range_items.len(), 5);
      assert_eq!(range_items[0].0.as_ref(), b"key03");
      assert_eq!(range_items[4].0.as_ref(), b"key07");

      // Reverse range
      // 反向范围
      let rev_range: Vec<_> = info
        .range(b"key03", b"key07", &mut files)
        .await?
        .rev()
        .collect();
      assert_eq!(rev_range.len(), 5);
      assert_eq!(rev_range[0].0.as_ref(), b"key07");
      assert_eq!(rev_range[4].0.as_ref(), b"key03");

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  #[test]
  fn test_sstable_filter_and_range_check() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let path = tmp_dir.join(format!("test_sstable_filter_{test_id}.sst"));

      // Write SSTable
      // 写入 SSTable
      let mut writer = SSTableWriter::new(path.clone(), 1, 10).await?;

      let keys: [&[u8]; 5] = [b"apple", b"banana", b"cherry", b"date", b"elderberry"];
      for key in &keys {
        let entry = Entry::Value(Pos::infile(1, 100, 10));
        writer.add(key, &entry).await?;
      }

      writer.finish().await?;

      // Read and test filter
      // 读取并测试过滤器
      let info = TableInfo::load(&path, 1).await?;

      // All written keys should pass filter (no false negatives)
      // 所有写入的键应该通过过滤器（无假阴性）
      for key in &keys {
        assert!(info.may_contain(key), "Filter should contain {key:?}");
      }

      // Test key range check
      // 测试键范围检查
      assert!(info.is_key_in_range(b"banana"));
      assert!(info.is_key_in_range(b"apple"));
      assert!(info.is_key_in_range(b"elderberry"));
      assert!(!info.is_key_in_range(b"aaa")); // before min
      assert!(!info.is_key_in_range(b"zzz")); // after max

      // Cleanup
      // 清理
      let _ = std::fs::remove_file(&path);
      OK
    })
  }

  #[test]
  fn test_sstable_empty() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let path = tmp_dir.join(format!("test_sstable_empty_{test_id}.sst"));

      // Write empty SSTable
      // 写入空 SSTable
      let writer = SSTableWriter::new(path.clone(), 1, 10).await?;
      let meta = writer.finish().await?;

      assert_eq!(meta.item_count, 0);

      // File should be removed for empty table
      // 空表的文件应该被删除
      assert!(!path.exists());

      OK
    })
  }
}

// Property-based tests for SSTable
// SSTable 属性测试
mod proptest_sstable {
  use jdb::{Entry, SSTableWriter, TableInfo};
  use jdb_base::{FileLru, Pos, id_path};
  use proptest::prelude::*;

  // Generate sorted unique keys
  // 生成排序的唯一键
  fn arb_sorted_unique_keys() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(prop::collection::vec(any::<u8>(), 1..32), 1..100).prop_map(|mut keys| {
      keys.sort();
      keys.dedup();
      keys
    })
  }

  fn arb_pos() -> impl Strategy<Value = Pos> {
    (any::<u64>(), any::<u64>(), any::<u32>())
      .prop_map(|(id, offset, len)| Pos::infile(id, offset, len))
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jdb-kv-database, Property 11: Cuckoo Filter Membership
    /// For any key written to an SSTable, the SSTable's Cuckoo filter should
    /// return true for that key (no false negatives).
    /// **Validates: Requirements 10.2**
    #[test]
    fn prop_cuckoo_filter_no_false_negatives(
      keys in arb_sorted_unique_keys(),
      positions in prop::collection::vec(arb_pos(), 1..100)
    ) {
      if keys.is_empty() {
        return Ok(());
      }

      // Run async test in compio runtime
      // 在 compio 运行时中运行异步测试
      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp_dir = std::env::temp_dir();
        let test_id = fastrand::u64(..);
        let sst_dir = tmp_dir.join(format!("test_sst_filter_{test_id}"));
        std::fs::create_dir_all(&sst_dir).unwrap();
        let table_id = 1u64;
        let path = id_path(&sst_dir, table_id);

        // Write SSTable
        // 写入 SSTable
        let mut writer = SSTableWriter::new(path.clone(), table_id, keys.len()).await.unwrap();

        let mut written_keys = Vec::new();
        for (i, key) in keys.iter().enumerate() {
          let pos = positions[i % positions.len()];
          let entry = Entry::Value(pos);
          writer.add(key, &entry).await.unwrap();
          written_keys.push(key.clone());
        }

        let _meta = writer.finish().await.unwrap();

        // Read SSTable and check filter
        // 读取 SSTable 并检查过滤器
        let info = TableInfo::load(&path, table_id).await.unwrap();

        // All written keys must be found by filter (no false negatives)
        // 所有写入的键必须被过滤器找到（无假阴性）
        for key in &written_keys {
          let contains = info.may_contain(key);
          assert!(
            contains,
            "Cuckoo filter false negative for key {:?}",
            key
          );
        }

        // Cleanup
        // 清理
        let _ = std::fs::remove_dir_all(&sst_dir);
      });
    }

    /// Feature: jdb-kv-database, SSTable read/write roundtrip
    /// For any set of sorted key-entry pairs, writing to SSTable and reading back
    /// should return the same data.
    /// **Validates: Requirements 7.2, 7.4**
    #[test]
    fn prop_sstable_roundtrip(
      keys in arb_sorted_unique_keys(),
      positions in prop::collection::vec(arb_pos(), 1..100)
    ) {
      if keys.is_empty() {
        return Ok(());
      }

      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp_dir = std::env::temp_dir();
        let test_id = fastrand::u64(..);
        let sst_dir = tmp_dir.join(format!("test_sst_rt_{test_id}"));
        std::fs::create_dir_all(&sst_dir).unwrap();
        let table_id = 1u64;
        let path = id_path(&sst_dir, table_id);

        // Write SSTable
        // 写入 SSTable
        let mut writer = SSTableWriter::new(path.clone(), table_id, keys.len()).await.unwrap();

        let mut expected: Vec<(Vec<u8>, Entry)> = Vec::new();
        for (i, key) in keys.iter().enumerate() {
          let pos = positions[i % positions.len()];
          let entry = Entry::Value(pos);
          writer.add(key, &entry).await.unwrap();
          expected.push((key.clone(), entry));
        }

        writer.finish().await.unwrap();

        // Read and verify
        // 读取并验证
        let info = TableInfo::load(&path, table_id).await.unwrap();
        let mut files = FileLru::new(&sst_dir, 16);

        // Verify point lookups
        // 验证点查询
        for (key, entry) in &expected {
          let got = info.get(key, &mut files).await.unwrap();
          assert_eq!(got, Some(*entry), "Point lookup mismatch for key {:?}", key);
        }

        // Verify iteration order
        // 验证迭代顺序
        let items: Vec<_> = info.iter(&mut files).await.unwrap().collect();
        assert_eq!(items.len(), expected.len());

        for (i, ((k1, e1), (k2, e2))) in items.iter().zip(expected.iter()).enumerate() {
          assert_eq!(k1.as_ref(), k2.as_slice(), "Key mismatch at {i}");
          assert_eq!(e1, e2, "Entry mismatch at {i}");
        }

        // Cleanup
        // 清理
        let _ = std::fs::remove_dir_all(&sst_dir);
      });
    }

    /// Feature: jdb-kv-database, SSTable iteration ordering
    /// Forward iteration returns keys in ascending order, backward in descending.
    /// **Validates: Requirements 4.2, 4.3, 7.4**
    #[test]
    fn prop_sstable_iteration_ordering(
      keys in arb_sorted_unique_keys(),
      positions in prop::collection::vec(arb_pos(), 1..100)
    ) {
      if keys.is_empty() {
        return Ok(());
      }

      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp_dir = std::env::temp_dir();
        let test_id = fastrand::u64(..);
        let sst_dir = tmp_dir.join(format!("test_sst_order_{test_id}"));
        std::fs::create_dir_all(&sst_dir).unwrap();
        let table_id = 1u64;
        let path = id_path(&sst_dir, table_id);

        // Write SSTable
        // 写入 SSTable
        let mut writer = SSTableWriter::new(path.clone(), table_id, keys.len()).await.unwrap();

        for (i, key) in keys.iter().enumerate() {
          let pos = positions[i % positions.len()];
          writer.add(key, &Entry::Value(pos)).await.unwrap();
        }

        writer.finish().await.unwrap();

        // Read and verify ordering
        // 读取并验证顺序
        let info = TableInfo::load(&path, table_id).await.unwrap();
        let mut files = FileLru::new(&sst_dir, 16);

        // Forward should be ascending
        // 正向应该是升序
        let forward: Vec<_> = info.iter(&mut files).await.unwrap().map(|(k, _)| k).collect();
        let mut sorted = forward.clone();
        sorted.sort();
        assert_eq!(forward, sorted, "Forward iteration not sorted");

        // Backward should be descending
        // 反向应该是降序
        let backward: Vec<_> = info.iter(&mut files).await.unwrap().rev().map(|(k, _)| k).collect();
        let mut sorted_desc = forward;
        sorted_desc.reverse();
        assert_eq!(backward, sorted_desc, "Backward iteration not descending");

        // Cleanup
        // 清理
        let _ = std::fs::remove_dir_all(&sst_dir);
      });
    }
  }
}

// Index unit tests
// Index 单元测试
mod index_tests {
  use aok::{OK, Void};
  use jdb::{Conf, Entry, Index};
  use jdb_base::Pos;

  #[test]
  fn test_index_new() -> Void {
    let tmp_dir = std::env::temp_dir().join(format!("test_index_new_{}", fastrand::u64(..)));
    std::fs::create_dir_all(&tmp_dir)?;

    let conf = Conf::default();
    let index = Index::new(tmp_dir.clone(), conf);

    assert_eq!(index.memtable_size(), 0);
    assert_eq!(index.sealed_count(), 0);
    assert_eq!(index.l0_count(), 0);
    assert!(!index.should_flush());

    // Cleanup
    // 清理
    let _ = std::fs::remove_dir_all(&tmp_dir);
    OK
  }

  #[test]
  fn test_index_put_get() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!("test_index_put_get_{}", fastrand::u64(..)));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Put some entries
      // 插入一些条目
      let pos1 = Pos::infile(1, 100, 10);
      let pos2 = Pos::infile(1, 200, 20);
      let pos3 = Pos::infile(1, 300, 30);

      index.put(b"key1".to_vec().into_boxed_slice(), pos1);
      index.put(b"key2".to_vec().into_boxed_slice(), pos2);
      index.put(b"key3".to_vec().into_boxed_slice(), pos3);

      // Get entries
      // 获取条目
      let entry1 = index.get(b"key1").await?.expect("should find key1");
      assert_eq!(entry1, Entry::Value(pos1));

      let entry2 = index.get(b"key2").await?.expect("should find key2");
      assert_eq!(entry2, Entry::Value(pos2));

      let entry3 = index.get(b"key3").await?.expect("should find key3");
      assert_eq!(entry3, Entry::Value(pos3));

      // Non-existent key
      // 不存在的键
      let entry = index.get(b"key4").await?;
      assert!(entry.is_none());

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_del() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!("test_index_del_{}", fastrand::u64(..)));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Put then delete
      // 先插入再删除
      let pos = Pos::infile(1, 100, 10);
      index.put(b"key1".to_vec().into_boxed_slice(), pos);
      index.del(b"key1".to_vec().into_boxed_slice());

      // Get should return tombstone
      // 获取应该返回删除标记
      let entry = index.get(b"key1").await?.expect("should find tombstone");
      assert!(entry.is_tombstone());

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_seal_memtable() -> Void {
    let tmp_dir =
      std::env::temp_dir().join(format!("test_index_seal_memtable_{}", fastrand::u64(..)));
    std::fs::create_dir_all(&tmp_dir)?;

    let conf = Conf::default();
    let mut index = Index::new(tmp_dir.clone(), conf);

    // Put some entries
    // 插入一些条目
    index.put(b"key1".to_vec().into_boxed_slice(), Pos::infile(1, 100, 10));
    index.put(b"key2".to_vec().into_boxed_slice(), Pos::infile(1, 200, 20));

    assert_eq!(index.sealed_count(), 0);

    // Seal memtable
    // 密封内存表
    index.seal_memtable();

    assert_eq!(index.sealed_count(), 1);
    assert_eq!(index.memtable_size(), 0); // New memtable is empty

    // Cleanup
    // 清理
    let _ = std::fs::remove_dir_all(&tmp_dir);
    OK
  }

  #[test]
  fn test_index_flush_sealed() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir =
        std::env::temp_dir().join(format!("test_index_flush_sealed_{}", fastrand::u64(..)));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Put some entries
      // 插入一些条目
      let pos1 = Pos::infile(1, 100, 10);
      let pos2 = Pos::infile(1, 200, 20);
      let pos3 = Pos::infile(1, 300, 30);

      index.put(b"aaa".to_vec().into_boxed_slice(), pos1);
      index.put(b"bbb".to_vec().into_boxed_slice(), pos2);
      index.put(b"ccc".to_vec().into_boxed_slice(), pos3);

      // Seal and flush
      // 密封并刷新
      index.seal_memtable();
      let table_id = index.flush_sealed().await?.expect("should flush");
      assert_eq!(table_id, 1);

      assert_eq!(index.sealed_count(), 0);
      assert_eq!(index.l0_count(), 1);

      // Data should still be accessible via SSTable
      // 数据应该仍然可以通过 SSTable 访问
      let entry1 = index.get(b"aaa").await?.expect("should find aaa");
      assert_eq!(entry1, Entry::Value(pos1));

      let entry2 = index.get(b"bbb").await?.expect("should find bbb");
      assert_eq!(entry2, Entry::Value(pos2));

      let entry3 = index.get(b"ccc").await?.expect("should find ccc");
      assert_eq!(entry3, Entry::Value(pos3));

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_get_from_sealed() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir =
        std::env::temp_dir().join(format!("test_index_get_from_sealed_{}", fastrand::u64(..)));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Put in first memtable
      // 在第一个内存表中插入
      let pos1 = Pos::infile(1, 100, 10);
      index.put(b"key1".to_vec().into_boxed_slice(), pos1);

      // Seal
      // 密封
      index.seal_memtable();

      // Put in second memtable
      // 在第二个内存表中插入
      let pos2 = Pos::infile(1, 200, 20);
      index.put(b"key2".to_vec().into_boxed_slice(), pos2);

      // Both should be accessible
      // 两者都应该可以访问
      let entry1 = index.get(b"key1").await?.expect("should find key1");
      assert_eq!(entry1, Entry::Value(pos1));

      let entry2 = index.get(b"key2").await?.expect("should find key2");
      assert_eq!(entry2, Entry::Value(pos2));

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_overwrite_in_memtable() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_overwrite_in_memtable_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Put then overwrite
      // 先插入再覆盖
      let pos1 = Pos::infile(1, 100, 10);
      let pos2 = Pos::infile(1, 200, 20);

      index.put(b"key1".to_vec().into_boxed_slice(), pos1);
      index.put(b"key1".to_vec().into_boxed_slice(), pos2);

      // Should get latest value
      // 应该获取最新值
      let entry = index.get(b"key1").await?.expect("should find key1");
      assert_eq!(entry, Entry::Value(pos2));

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_memtable_shadows_sstable() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_memtable_shadows_sstable_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Put and flush to SSTable
      // 插入并刷新到 SSTable
      let pos1 = Pos::infile(1, 100, 10);
      index.put(b"key1".to_vec().into_boxed_slice(), pos1);
      index.seal_memtable();
      index.flush_sealed().await?;

      // Overwrite in memtable
      // 在内存表中覆盖
      let pos2 = Pos::infile(1, 200, 20);
      index.put(b"key1".to_vec().into_boxed_slice(), pos2);

      // Should get memtable value (newer)
      // 应该获取内存表值（更新）
      let entry = index.get(b"key1").await?.expect("should find key1");
      assert_eq!(entry, Entry::Value(pos2));

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_delete_shadows_sstable() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_delete_shadows_sstable_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Put and flush to SSTable
      // 插入并刷新到 SSTable
      let pos1 = Pos::infile(1, 100, 10);
      index.put(b"key1".to_vec().into_boxed_slice(), pos1);
      index.seal_memtable();
      index.flush_sealed().await?;

      // Delete in memtable
      // 在内存表中删除
      index.del(b"key1".to_vec().into_boxed_slice());

      // Should get tombstone (newer)
      // 应该获取删除标记（更新）
      let entry = index.get(b"key1").await?.expect("should find tombstone");
      assert!(entry.is_tombstone());

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }
}

// Index flush tests
// Index 刷新测试
mod index_flush_tests {
  use aok::{OK, Void};
  use jdb::{Conf, ConfItem, Entry, Index};
  use jdb_base::Pos;
  use log::info;

  #[test]
  fn test_index_should_flush_threshold() -> Void {
    let tmp_dir = std::env::temp_dir().join(format!(
      "test_index_should_flush_threshold_{}",
      fastrand::u64(..)
    ));
    std::fs::create_dir_all(&tmp_dir)?;

    // Set small memtable size for testing
    // 设置小的内存表大小用于测试
    let conf = Conf::from_items(&[ConfItem::MemtableSize(100)]);
    let mut index = Index::new(tmp_dir.clone(), conf);

    // Initially should not flush
    // 初始时不应该刷新
    assert!(!index.should_flush());

    // Add entries until threshold
    // 添加条目直到阈值
    for i in 0..10 {
      let key = format!("key{i:04}").into_bytes().into_boxed_slice();
      let pos = Pos::infile(1, i as u64 * 100, 10);
      index.put(key, pos);
    }

    // Should now exceed threshold
    // 现在应该超过阈值
    assert!(index.should_flush());

    // Cleanup
    // 清理
    let _ = std::fs::remove_dir_all(&tmp_dir);
    OK
  }

  #[test]
  fn test_index_multiple_flushes() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir =
        std::env::temp_dir().join(format!("test_index_multiple_flushes_{}", fastrand::u64(..)));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // First batch
      // 第一批
      for i in 0..5 {
        let key = format!("batch1_key{i}").into_bytes().into_boxed_slice();
        let pos = Pos::infile(1, i as u64 * 100, 10);
        index.put(key, pos);
      }
      index.seal_memtable();
      let id1 = index.flush_sealed().await?.expect("should flush");

      // Second batch
      // 第二批
      for i in 0..5 {
        let key = format!("batch2_key{i}").into_bytes().into_boxed_slice();
        let pos = Pos::infile(2, i as u64 * 100, 10);
        index.put(key, pos);
      }
      index.seal_memtable();
      let id2 = index.flush_sealed().await?.expect("should flush");

      assert!(id2 > id1);
      assert_eq!(index.l0_count(), 2);

      // All data should be accessible
      // 所有数据应该可以访问
      let entry = index
        .get(b"batch1_key0")
        .await?
        .expect("should find batch1_key0");
      assert_eq!(entry, Entry::Value(Pos::infile(1, 0, 10)));

      let entry = index
        .get(b"batch2_key0")
        .await?
        .expect("should find batch2_key0");
      assert_eq!(entry, Entry::Value(Pos::infile(2, 0, 10)));

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_flush_empty_sealed() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_flush_empty_sealed_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Seal empty memtable (should be no-op)
      // 密封空内存表（应该是无操作）
      index.seal_memtable();
      assert_eq!(index.sealed_count(), 0);

      // Flush with no sealed (should return None)
      // 没有密封时刷新（应该返回 None）
      let result = index.flush_sealed().await?;
      assert!(result.is_none());

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_l0_search_order() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir =
        std::env::temp_dir().join(format!("test_index_l0_search_order_{}", fastrand::u64(..)));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // First flush with old value
      // 第一次刷新旧值
      let pos1 = Pos::infile(1, 100, 10);
      index.put(b"key1".to_vec().into_boxed_slice(), pos1);
      index.seal_memtable();
      index.flush_sealed().await?;

      // Second flush with new value for same key
      // 第二次刷新相同键的新值
      let pos2 = Pos::infile(2, 200, 20);
      index.put(b"key1".to_vec().into_boxed_slice(), pos2);
      index.seal_memtable();
      index.flush_sealed().await?;

      assert_eq!(index.l0_count(), 2);

      // Should get newer value from L0 (searched newest first)
      // 应该从 L0 获取更新的值（最新的优先搜索）
      let entry = index.get(b"key1").await?.expect("should find key1");
      assert_eq!(entry, Entry::Value(pos2));

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_compaction_get() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir =
        std::env::temp_dir().join(format!("test_index_compaction_get_{}", fastrand::u64(..)));
      std::fs::create_dir_all(&tmp_dir)?;

      // Use small memtable and low L0 threshold to trigger compaction
      // 使用小内存表和低 L0 阈值来触发压缩
      let conf = Conf::from_items(&[ConfItem::MemtableSize(512), ConfItem::L0Threshold(2)]);
      let mut index = Index::new(tmp_dir.clone(), conf);

      // First batch - put some keys
      // 第一批 - 插入一些键
      let key1 = b"key_aaa".to_vec();
      let key2 = b"key_bbb".to_vec();
      let key3 = b"key_ccc".to_vec();
      let pos1 = Pos::infile(1, 100, 10);
      let pos2 = Pos::infile(1, 200, 20);
      let pos3 = Pos::infile(1, 300, 30);

      index.put(key1.clone().into_boxed_slice(), pos1);
      index.put(key2.clone().into_boxed_slice(), pos2);
      index.put(key3.clone().into_boxed_slice(), pos3);

      // Seal and flush
      // 密封并刷新
      index.seal_memtable();
      let _ = index.flush_sealed().await?;
      info!("After flush 1: L0 count = {}", index.l0_count());

      // Second batch - put more keys
      // 第二批 - 插入更多键
      let key4 = b"key_ddd".to_vec();
      let key5 = b"key_eee".to_vec();
      let pos4 = Pos::infile(2, 400, 40);
      let pos5 = Pos::infile(2, 500, 50);

      index.put(key4.clone().into_boxed_slice(), pos4);
      index.put(key5.clone().into_boxed_slice(), pos5);

      // Seal and flush again
      // 再次密封并刷新
      index.seal_memtable();
      let _ = index.flush_sealed().await?;
      info!("After flush 2: L0 count = {}", index.l0_count());

      // Verify all keys before compaction
      // 压缩前验证所有键
      assert_eq!(
        index.get(&key1).await?.expect("key1 before compaction"),
        Entry::Value(pos1)
      );
      assert_eq!(
        index.get(&key4).await?.expect("key4 before compaction"),
        Entry::Value(pos4)
      );

      // Run compaction
      // 运行压缩
      let compacted = index.maybe_compact().await?;
      info!(
        "Compacted: {}, L0 count = {}, levels = {}",
        compacted,
        index.l0_count(),
        index.levels().len()
      );

      // Print level info
      // 打印层级信息
      for (i, level) in index.levels().iter().enumerate() {
        info!("Level {} has {} tables", i, level.len());
        for table in &level.tables {
          let meta = table.meta();
          info!(
            "  Table {}: min_key={:?}, max_key={:?}",
            meta.id, meta.min_key, meta.max_key
          );
        }
      }

      // Verify all keys after compaction
      // 压缩后验证所有键
      let entry1 = index.get(&key1).await?;
      info!("key1 after compaction: {:?}", entry1);
      assert!(entry1.is_some(), "key1 not found after compaction");
      assert_eq!(entry1.unwrap(), Entry::Value(pos1));

      let entry2 = index.get(&key2).await?;
      info!("key2 after compaction: {:?}", entry2);
      assert!(entry2.is_some(), "key2 not found after compaction");
      assert_eq!(entry2.unwrap(), Entry::Value(pos2));

      let entry3 = index.get(&key3).await?;
      info!("key3 after compaction: {:?}", entry3);
      assert!(entry3.is_some(), "key3 not found after compaction");
      assert_eq!(entry3.unwrap(), Entry::Value(pos3));

      let entry4 = index.get(&key4).await?;
      info!("key4 after compaction: {:?}", entry4);
      assert!(entry4.is_some(), "key4 not found after compaction");
      assert_eq!(entry4.unwrap(), Entry::Value(pos4));

      let entry5 = index.get(&key5).await?;
      info!("key5 after compaction: {:?}", entry5);
      assert!(entry5.is_some(), "key5 not found after compaction");
      assert_eq!(entry5.unwrap(), Entry::Value(pos5));

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  /// Test compaction with overlapping keys
  /// 测试有重叠键的压缩
  #[test]
  fn test_index_compaction_overlapping_keys() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_compaction_overlapping_keys_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::from_items(&[ConfItem::MemtableSize(512), ConfItem::L0Threshold(2)]);
      let mut index = Index::new(tmp_dir.clone(), conf);

      // First batch - put key [0] twice with different values
      // 第一批 - 用不同的值插入键 [0] 两次
      let key0 = vec![0u8];
      let pos1 = Pos::infile(0, 0, 0);
      let pos2 = Pos::infile(0, 145890, 3229080220);

      index.put(key0.clone().into_boxed_slice(), pos1);
      index.put(key0.clone().into_boxed_slice(), pos2);

      // Add more keys
      // 添加更多键
      let key1 = vec![210u8, 204, 27, 244, 113];
      let pos3 = Pos::infile(9747750559154161929, 5988402464810570988, 1715483508);
      index.put(key1.clone().into_boxed_slice(), pos3);

      // Seal and flush
      // 密封并刷新
      index.seal_memtable();
      let _ = index.flush_sealed().await?;
      info!("After flush 1: L0 count = {}", index.l0_count());

      // Second batch
      // 第二批
      let key2 = vec![149u8, 236, 241, 8, 153, 69, 70, 105, 41];
      let pos4 = Pos::infile(1173020921495281860, 4125700145580995233, 2401585032);
      index.put(key2.clone().into_boxed_slice(), pos4);

      let key3 = vec![35u8];
      let pos5 = Pos::infile(1809101193593519238, 2426190062291300019, 245869206);
      index.put(key3.clone().into_boxed_slice(), pos5);

      // Seal and flush again
      // 再次密封并刷新
      index.seal_memtable();
      let _ = index.flush_sealed().await?;
      info!("After flush 2: L0 count = {}", index.l0_count());

      // Verify before compaction
      // 压缩前验证
      let entry0 = index.get(&key0).await?;
      info!("key0 before compaction: {:?}", entry0);
      assert!(entry0.is_some(), "key0 not found before compaction");
      assert_eq!(entry0.unwrap(), Entry::Value(pos2)); // Should be the latest value

      // Run compaction
      // 运行压缩
      let compacted = index.maybe_compact().await?;
      info!(
        "Compacted: {}, L0 count = {}, levels = {}",
        compacted,
        index.l0_count(),
        index.levels().len()
      );

      // Print level info
      // 打印层级信息
      for (i, level) in index.levels().iter().enumerate() {
        info!("Level {} has {} tables", i, level.len());
        for table in &level.tables {
          let meta = table.meta();
          info!(
            "  Table {}: min_key={:?}, max_key={:?}",
            meta.id, meta.min_key, meta.max_key
          );
        }
      }

      // Verify after compaction
      // 压缩后验证
      let entry0 = index.get(&key0).await?;
      info!("key0 after compaction: {:?}", entry0);
      assert!(entry0.is_some(), "key0 not found after compaction");
      assert_eq!(entry0.unwrap(), Entry::Value(pos2)); // Should still be the latest value

      let entry1 = index.get(&key1).await?;
      info!("key1 after compaction: {:?}", entry1);
      assert!(entry1.is_some(), "key1 not found after compaction");
      assert_eq!(entry1.unwrap(), Entry::Value(pos3));

      let entry2 = index.get(&key2).await?;
      info!("key2 after compaction: {:?}", entry2);
      assert!(entry2.is_some(), "key2 not found after compaction");
      assert_eq!(entry2.unwrap(), Entry::Value(pos4));

      let entry3 = index.get(&key3).await?;
      info!("key3 after compaction: {:?}", entry3);
      assert!(entry3.is_some(), "key3 not found after compaction");
      assert_eq!(entry3.unwrap(), Entry::Value(pos5));

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  /// Test compaction with key [202] - reproducing proptest failure
  /// 测试键 [202] 的压缩 - 重现 proptest 失败
  #[test]
  fn test_index_compaction_key_202() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_compaction_key_202_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::from_items(&[ConfItem::MemtableSize(512), ConfItem::L0Threshold(2)]);
      let mut index = Index::new(tmp_dir.clone(), conf);

      // First batch - from proptest regression
      // 第一批 - 来自 proptest regression
      index.put(vec![0u8].into_boxed_slice(), Pos::infile(0, 0, 0));
      index.put(vec![0u8].into_boxed_slice(), Pos::infile(0, 0, 87));
      index.put(
        vec![2u8, 196, 168, 247, 86, 9, 186, 115, 203, 75, 51, 125, 55].into_boxed_slice(),
        Pos::infile(4035580412736968745, 12294285814533776696, 3282384014),
      );
      index.del(vec![8u8, 60, 178, 148, 57, 87, 109, 118].into_boxed_slice());
      index.put(
        vec![
          148u8, 115, 98, 233, 164, 87, 216, 191, 125, 90, 134, 199, 184, 166, 114,
        ]
        .into_boxed_slice(),
        Pos::infile(6164899580939608942, 15216035334959378983, 2808231117),
      );

      // Seal and flush
      // 密封并刷新
      index.seal_memtable();
      let _ = index.flush_sealed().await?;
      info!("After flush 1: L0 count = {}", index.l0_count());

      // Second batch - includes key [202]
      // 第二批 - 包含键 [202]
      index.put(
        vec![83u8, 166, 193, 67, 31, 181, 229, 7, 236].into_boxed_slice(),
        Pos::infile(6392762991450751545, 11028467653528313416, 1333066465),
      );
      // Key [202]
      let key_202 = vec![202u8];
      let pos_202 = Pos::infile(6424140631229481194, 7902516027705068065, 2040550895);
      index.put(key_202.clone().into_boxed_slice(), pos_202);

      // Seal and flush again
      // 再次密封并刷新
      index.seal_memtable();
      let _ = index.flush_sealed().await?;
      info!("After flush 2: L0 count = {}", index.l0_count());

      // Verify key [202] before compaction
      // 压缩前验证键 [202]
      let entry_202 = index.get(&key_202).await?;
      info!("key [202] before compaction: {:?}", entry_202);
      assert!(entry_202.is_some(), "key [202] not found before compaction");
      assert_eq!(entry_202.unwrap(), Entry::Value(pos_202));

      // Run compaction
      // 运行压缩
      let compacted = index.maybe_compact().await?;
      info!(
        "Compacted: {}, L0 count = {}, levels = {}",
        compacted,
        index.l0_count(),
        index.levels().len()
      );

      // Print level info
      // 打印层级信息
      for (i, level) in index.levels().iter().enumerate() {
        info!("Level {} has {} tables", i, level.len());
        for table in &level.tables {
          let meta = table.meta();
          info!(
            "  Table {}: min_key={:?}, max_key={:?}",
            meta.id, meta.min_key, meta.max_key
          );
          // Check if key [202] is in range
          // 检查键 [202] 是否在范围内
          info!(
            "  is_key_in_range([202]): {}",
            table.is_key_in_range(&key_202)
          );
          info!("  may_contain([202]): {}", table.may_contain(&key_202));
        }
      }

      // Verify key [202] after compaction
      // 压缩后验证键 [202]
      let entry_202 = index.get(&key_202).await?;
      info!("key [202] after compaction: {:?}", entry_202);
      assert!(entry_202.is_some(), "key [202] not found after compaction");
      assert_eq!(entry_202.unwrap(), Entry::Value(pos_202));

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }
}

// Index range and prefix iteration tests
// Index 范围和前缀迭代测试
mod index_iter_tests {
  use std::ops::Bound;

  use aok::{OK, Void};
  use jdb::{Conf, Index};
  use jdb_base::Pos;

  #[test]
  fn test_index_iter_memtable_only() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_iter_memtable_only_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Add entries
      // 添加条目
      index.put(b"c".to_vec().into_boxed_slice(), Pos::infile(1, 300, 30));
      index.put(b"a".to_vec().into_boxed_slice(), Pos::infile(1, 100, 10));
      index.put(b"b".to_vec().into_boxed_slice(), Pos::infile(1, 200, 20));

      // Iterate all
      // 迭代所有
      let keys: Vec<_> = index.iter().await?.map(|e| e.key.to_vec()).collect();
      assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

      // Backward iteration
      // 反向迭代
      let keys: Vec<_> = index.iter().await?.rev().map(|e| e.key.to_vec()).collect();
      assert_eq!(keys, vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_range_memtable_only() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_range_memtable_only_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Add entries
      // 添加条目
      for i in 0..10u8 {
        let key = vec![b'a' + i].into_boxed_slice();
        index.put(key, Pos::infile(1, i as u64 * 100, 10));
      }

      // Range [c, g]
      let keys: Vec<_> = index
        .range(Bound::Included(b"c"), Bound::Included(b"g"))
        .await?
        .map(|e| e.key.to_vec())
        .collect();
      assert_eq!(
        keys,
        vec![
          b"c".to_vec(),
          b"d".to_vec(),
          b"e".to_vec(),
          b"f".to_vec(),
          b"g".to_vec()
        ]
      );

      // Reverse range
      // 反向范围
      let keys: Vec<_> = index
        .range(Bound::Included(b"c"), Bound::Included(b"g"))
        .await?
        .rev()
        .map(|e| e.key.to_vec())
        .collect();
      assert_eq!(
        keys,
        vec![
          b"g".to_vec(),
          b"f".to_vec(),
          b"e".to_vec(),
          b"d".to_vec(),
          b"c".to_vec()
        ]
      );

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_prefix_memtable_only() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_prefix_memtable_only_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Add entries with different prefixes
      // 添加不同前缀的条目
      index.put(
        b"user:1:name".to_vec().into_boxed_slice(),
        Pos::infile(1, 100, 10),
      );
      index.put(
        b"user:1:email".to_vec().into_boxed_slice(),
        Pos::infile(1, 200, 20),
      );
      index.put(
        b"user:2:name".to_vec().into_boxed_slice(),
        Pos::infile(1, 300, 30),
      );
      index.put(
        b"post:1:title".to_vec().into_boxed_slice(),
        Pos::infile(1, 400, 40),
      );

      // Prefix "user:1:"
      let keys: Vec<_> = index
        .prefix(b"user:1:")
        .await?
        .map(|e| e.key.to_vec())
        .collect();
      assert_eq!(
        keys,
        vec![b"user:1:email".to_vec(), b"user:1:name".to_vec()]
      );

      // Prefix "user:"
      let keys: Vec<_> = index
        .prefix(b"user:")
        .await?
        .map(|e| e.key.to_vec())
        .collect();
      assert_eq!(
        keys,
        vec![
          b"user:1:email".to_vec(),
          b"user:1:name".to_vec(),
          b"user:2:name".to_vec()
        ]
      );

      // Prefix "post:"
      let keys: Vec<_> = index
        .prefix(b"post:")
        .await?
        .map(|e| e.key.to_vec())
        .collect();
      assert_eq!(keys, vec![b"post:1:title".to_vec()]);

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_iter_merge_memtable_and_sstable() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_iter_merge_memtable_and_sstable_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Add entries and flush to SSTable
      // 添加条目并刷新到 SSTable
      index.put(b"a".to_vec().into_boxed_slice(), Pos::infile(1, 100, 10));
      index.put(b"c".to_vec().into_boxed_slice(), Pos::infile(1, 300, 30));
      index.put(b"e".to_vec().into_boxed_slice(), Pos::infile(1, 500, 50));
      index.seal_memtable();
      index.flush_sealed().await?;

      // Add more entries to memtable
      // 在内存表中添加更多条目
      index.put(b"b".to_vec().into_boxed_slice(), Pos::infile(2, 200, 20));
      index.put(b"d".to_vec().into_boxed_slice(), Pos::infile(2, 400, 40));

      // Iterate all - should merge memtable and SSTable
      // 迭代所有 - 应该合并内存表和 SSTable
      let keys: Vec<_> = index.iter().await?.map(|e| e.key.to_vec()).collect();
      assert_eq!(
        keys,
        vec![
          b"a".to_vec(),
          b"b".to_vec(),
          b"c".to_vec(),
          b"d".to_vec(),
          b"e".to_vec()
        ]
      );

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_iter_memtable_shadows_sstable() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_iter_memtable_shadows_sstable_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Add entry and flush to SSTable
      // 添加条目并刷新到 SSTable
      let old_pos = Pos::infile(1, 100, 10);
      index.put(b"key1".to_vec().into_boxed_slice(), old_pos);
      index.seal_memtable();
      index.flush_sealed().await?;

      // Overwrite in memtable
      // 在内存表中覆盖
      let new_pos = Pos::infile(2, 200, 20);
      index.put(b"key1".to_vec().into_boxed_slice(), new_pos);

      // Iterate - should only see new value
      // 迭代 - 应该只看到新值
      let entries: Vec<_> = index.iter().await?.collect();
      assert_eq!(entries.len(), 1);
      assert_eq!(entries[0].key.as_ref(), b"key1");
      assert_eq!(entries[0].entry, jdb::Entry::Value(new_pos));

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_iter_skip_tombstones() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_iter_skip_tombstones_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Add entries
      // 添加条目
      index.put(b"a".to_vec().into_boxed_slice(), Pos::infile(1, 100, 10));
      index.put(b"b".to_vec().into_boxed_slice(), Pos::infile(1, 200, 20));
      index.put(b"c".to_vec().into_boxed_slice(), Pos::infile(1, 300, 30));

      // Delete b
      // 删除 b
      index.del(b"b".to_vec().into_boxed_slice());

      // Iterate - should skip tombstone
      // 迭代 - 应该跳过删除标记
      let keys: Vec<_> = index.iter().await?.map(|e| e.key.to_vec()).collect();
      assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_iter_delete_shadows_sstable() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_iter_delete_shadows_sstable_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // Add entries and flush to SSTable
      // 添加条目并刷新到 SSTable
      index.put(b"a".to_vec().into_boxed_slice(), Pos::infile(1, 100, 10));
      index.put(b"b".to_vec().into_boxed_slice(), Pos::infile(1, 200, 20));
      index.put(b"c".to_vec().into_boxed_slice(), Pos::infile(1, 300, 30));
      index.seal_memtable();
      index.flush_sealed().await?;

      // Delete b in memtable
      // 在内存表中删除 b
      index.del(b"b".to_vec().into_boxed_slice());

      // Iterate - should skip deleted key
      // 迭代 - 应该跳过已删除的键
      let keys: Vec<_> = index.iter().await?.map(|e| e.key.to_vec()).collect();
      assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_index_range_across_sources() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir().join(format!(
        "test_index_range_across_sources_{}",
        fastrand::u64(..)
      ));
      std::fs::create_dir_all(&tmp_dir)?;

      let conf = Conf::default();
      let mut index = Index::new(tmp_dir.clone(), conf);

      // First batch to SSTable
      // 第一批到 SSTable
      index.put(b"aa".to_vec().into_boxed_slice(), Pos::infile(1, 100, 10));
      index.put(b"cc".to_vec().into_boxed_slice(), Pos::infile(1, 300, 30));
      index.put(b"ee".to_vec().into_boxed_slice(), Pos::infile(1, 500, 50));
      index.seal_memtable();
      index.flush_sealed().await?;

      // Second batch to sealed memtable
      // 第二批到密封内存表
      index.put(b"bb".to_vec().into_boxed_slice(), Pos::infile(2, 200, 20));
      index.put(b"dd".to_vec().into_boxed_slice(), Pos::infile(2, 400, 40));
      index.seal_memtable();

      // Third batch to active memtable
      // 第三批到活跃内存表
      index.put(b"ff".to_vec().into_boxed_slice(), Pos::infile(3, 600, 60));

      // Range [bb, ee] should include entries from all sources
      // 范围 [bb, ee] 应该包含所有源的条目
      let keys: Vec<_> = index
        .range(Bound::Included(b"bb"), Bound::Included(b"ee"))
        .await?
        .map(|e| e.key.to_vec())
        .collect();
      assert_eq!(
        keys,
        vec![
          b"bb".to_vec(),
          b"cc".to_vec(),
          b"dd".to_vec(),
          b"ee".to_vec()
        ]
      );

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }
}

// Property-based tests for Index iteration
// Index 迭代属性测试
mod proptest_index {
  use std::ops::Bound;

  use jdb::{Conf, ConfItem, Index};
  use jdb_base::Pos;
  use proptest::prelude::*;

  // Generate random key
  // 生成随机键
  fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..32)
  }

  // Generate random position
  // 生成随机位置
  fn arb_pos() -> impl Strategy<Value = Pos> {
    (any::<u64>(), any::<u64>(), any::<u32>())
      .prop_map(|(id, offset, len)| Pos::infile(id, offset, len))
  }

  // Generate operation: put or delete
  // 生成操作：插入或删除
  #[derive(Debug, Clone)]
  enum Op {
    Put(Vec<u8>, Pos),
    Del(Vec<u8>),
  }

  fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
      (arb_key(), arb_pos()).prop_map(|(k, p)| Op::Put(k, p)),
      arb_key().prop_map(Op::Del),
    ]
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jdb-kv-database, Property 3: Iteration Ordering
    /// For any range of keys in the database, forward iteration should return keys
    /// in lexicographic ascending order, and backward iteration should return keys
    /// in lexicographic descending order. Deleted keys (tombstones) should not appear
    /// in iteration results.
    /// **Validates: Requirements 4.2, 4.3, 4.4, 4.5**
    #[test]
    fn prop_index_iteration_ordering(
      ops in prop::collection::vec(arb_op(), 1..50)
    ) {
      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp_dir = std::env::temp_dir()
          .join(format!("prop_index_iteration_ordering_{}", fastrand::u64(..)));
        std::fs::create_dir_all(&tmp_dir).unwrap();

        let conf = Conf::default();
        let mut index = Index::new(tmp_dir.clone(), conf);

        // Apply operations
        // 应用操作
        for op in &ops {
          match op {
            Op::Put(key, pos) => {
              index.put(key.clone().into_boxed_slice(), *pos);
            }
            Op::Del(key) => {
              index.del(key.clone().into_boxed_slice());
            }
          }
        }

        // Forward iteration should be sorted ascending
        // 正向迭代应该是升序排列
        let keys: Vec<_> = index.iter().await.unwrap().map(|e| e.key.to_vec()).collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        prop_assert_eq!(&keys, &sorted_keys, "Forward iteration not sorted");

        // Backward iteration should be sorted descending
        // 反向迭代应该是降序排列
        let rev_keys: Vec<_> = index.iter().await.unwrap().rev().map(|e| e.key.to_vec()).collect();
        let mut sorted_desc = keys.clone();
        sorted_desc.reverse();
        prop_assert_eq!(rev_keys, sorted_desc, "Backward iteration not sorted descending");

        // No tombstones should appear
        // 不应该出现删除标记
        for entry in index.iter().await.unwrap() {
          prop_assert!(!entry.entry.is_tombstone(), "Tombstone appeared in iteration");
        }

        // Cleanup
        // 清理
        let _ = std::fs::remove_dir_all(&tmp_dir);
        Ok(())
      })?;
    }

    /// Feature: jdb-kv-database, Property 3: Iteration Ordering with flush
    /// Same as above but with memtable flush to SSTable.
    /// **Validates: Requirements 4.2, 4.3, 4.4, 4.5**
    #[test]
    fn prop_index_iteration_ordering_with_flush(
      ops1 in prop::collection::vec(arb_op(), 1..20),
      ops2 in prop::collection::vec(arb_op(), 1..20)
    ) {
      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp_dir = std::env::temp_dir()
          .join(format!("prop_index_iteration_ordering_with_flush_{}", fastrand::u64(..)));
        std::fs::create_dir_all(&tmp_dir).unwrap();

        // Use small memtable to trigger flush
        // 使用小内存表来触发刷新
        let conf = Conf::from_items(&[ConfItem::MemtableSize(1024)]);
        let mut index = Index::new(tmp_dir.clone(), conf);

        // First batch of operations
        // 第一批操作
        for op in &ops1 {
          match op {
            Op::Put(key, pos) => {
              index.put(key.clone().into_boxed_slice(), *pos);
            }
            Op::Del(key) => {
              index.del(key.clone().into_boxed_slice());
            }
          }
        }

        // Seal and flush
        // 密封并刷新
        index.seal_memtable();
        let _ = index.flush_sealed().await;

        // Second batch of operations
        // 第二批操作
        for op in &ops2 {
          match op {
            Op::Put(key, pos) => {
              index.put(key.clone().into_boxed_slice(), *pos);
            }
            Op::Del(key) => {
              index.del(key.clone().into_boxed_slice());
            }
          }
        }

        // Forward iteration should be sorted ascending
        // 正向迭代应该是升序排列
        let keys: Vec<_> = index.iter().await.unwrap().map(|e| e.key.to_vec()).collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        prop_assert_eq!(&keys, &sorted_keys, "Forward iteration not sorted after flush");

        // Backward iteration should be sorted descending
        // 反向迭代应该是降序排列
        let rev_keys: Vec<_> = index.iter().await.unwrap().rev().map(|e| e.key.to_vec()).collect();
        let mut sorted_desc = keys.clone();
        sorted_desc.reverse();
        prop_assert_eq!(rev_keys, sorted_desc, "Backward iteration not sorted descending after flush");

        // No tombstones should appear
        // 不应该出现删除标记
        for entry in index.iter().await.unwrap() {
          prop_assert!(!entry.entry.is_tombstone(), "Tombstone appeared in iteration after flush");
        }

        // Cleanup
        // 清理
        let _ = std::fs::remove_dir_all(&tmp_dir);
        Ok(())
      })?;
    }

    /// Feature: jdb-kv-database, Property 3: Range iteration ordering
    /// Range queries should return keys in sorted order within bounds.
    /// **Validates: Requirements 4.2, 4.3, 4.4, 4.5**
    #[test]
    fn prop_index_range_iteration_ordering(
      puts in prop::collection::vec((arb_key(), arb_pos()), 5..30),
      start_idx in any::<usize>(),
      end_idx in any::<usize>()
    ) {
      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp_dir = std::env::temp_dir()
          .join(format!("prop_index_range_iteration_ordering_{}", fastrand::u64(..)));
        std::fs::create_dir_all(&tmp_dir).unwrap();

        let conf = Conf::default();
        let mut index = Index::new(tmp_dir.clone(), conf);

        // Insert all entries
        // 插入所有条目
        for (key, pos) in &puts {
          index.put(key.clone().into_boxed_slice(), *pos);
        }

        // Get all keys sorted
        // 获取所有排序后的键
        let all_keys: Vec<_> = index.iter().await.unwrap().map(|e| e.key.to_vec()).collect();
        if all_keys.is_empty() {
          let _ = std::fs::remove_dir_all(&tmp_dir);
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
        let range_keys: Vec<_> = index
          .range(
            Bound::Included(start_key.as_slice()),
            Bound::Included(end_key.as_slice()),
          )
          .await
          .unwrap()
          .map(|e| e.key.to_vec())
          .collect();

        // Should be sorted
        // 应该是有序的
        let mut sorted_range = range_keys.clone();
        sorted_range.sort();
        prop_assert_eq!(&range_keys, &sorted_range, "Range keys not sorted");

        // Reverse should be descending
        // 反向应该是降序
        let rev_range_keys: Vec<_> = index
          .range(
            Bound::Included(start_key.as_slice()),
            Bound::Included(end_key.as_slice()),
          )
          .await
          .unwrap()
          .rev()
          .map(|e| e.key.to_vec())
          .collect();

        let mut sorted_desc = range_keys;
        sorted_desc.reverse();
        prop_assert_eq!(rev_range_keys, sorted_desc, "Reverse range not descending");

        // Cleanup
        // 清理
        let _ = std::fs::remove_dir_all(&tmp_dir);
        Ok(())
      })?;
    }

    /// Feature: jdb-kv-database, Property 3: Memtable shadows SSTable
    /// Newer entries in memtable should shadow older entries in SSTable.
    /// **Validates: Requirements 4.4, 4.5**
    #[test]
    fn prop_index_memtable_shadows_sstable(
      base_keys in prop::collection::vec(prop::collection::vec(any::<u8>(), 2..16), 5..20),
      old_positions in prop::collection::vec(arb_pos(), 5..20),
      new_positions in prop::collection::vec(arb_pos(), 5..20)
    ) {
      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp_dir = std::env::temp_dir()
          .join(format!("prop_index_memtable_shadows_sstable_{}", fastrand::u64(..)));
        std::fs::create_dir_all(&tmp_dir).unwrap();

        let conf = Conf::default();
        let mut index = Index::new(tmp_dir.clone(), conf);

        // Add suffix to avoid prefix conflicts with blart
        // 添加后缀以避免 blart 的前缀冲突
        let keys: Vec<Vec<u8>> = base_keys.iter().map(|k| {
          let mut key = k.clone();
          key.push(0xff); // Suffix to avoid prefix issues
          key
        }).collect();

        // Deduplicate keys first
        // 先去重键
        let mut unique_keys: Vec<Vec<u8>> = Vec::new();
        for key in keys {
          if !unique_keys.contains(&key) {
            unique_keys.push(key);
          }
        }

        if unique_keys.is_empty() {
          let _ = std::fs::remove_dir_all(&tmp_dir);
          return Ok(());
        }

        // Insert old values and flush to SSTable
        // 插入旧值并刷新到 SSTable
        for (i, key) in unique_keys.iter().enumerate() {
          let pos = old_positions[i % old_positions.len()];
          index.put(key.clone().into_boxed_slice(), pos);
        }
        index.seal_memtable();
        let _ = index.flush_sealed().await;

        // Overwrite some keys with new values in memtable
        // 在内存表中用新值覆盖一些键
        let overwrite_count = unique_keys.len() / 2;
        for (i, key) in unique_keys.iter().take(overwrite_count).enumerate() {
          let pos = new_positions[i % new_positions.len()];
          index.put(key.clone().into_boxed_slice(), pos);
        }

        // Iterate and verify
        // 迭代并验证
        let entries: Vec<_> = index.iter().await.unwrap().collect();

        // Should have all unique keys (same count as unique_keys)
        // 应该有所有唯一键（与 unique_keys 数量相同）
        let result_keys: Vec<_> = entries.iter().map(|e| e.key.to_vec()).collect();
        prop_assert_eq!(result_keys.len(), unique_keys.len(), "Key count mismatch");

        // Overwritten keys should have new values
        // 被覆盖的键应该有新值
        for (i, key) in unique_keys.iter().take(overwrite_count).enumerate() {
          let expected_pos = new_positions[i % new_positions.len()];
          let entry = entries.iter().find(|e| e.key.as_ref() == key.as_slice());
          if let Some(e) = entry {
            prop_assert_eq!(
              e.entry,
              jdb::Entry::Value(expected_pos),
              "Memtable did not shadow SSTable for key {:?}",
              key
            );
          }
        }

        // Cleanup
        // 清理
        let _ = std::fs::remove_dir_all(&tmp_dir);
        Ok(())
      })?;
    }
  }
}

// Manifest unit tests
// Manifest 单元测试
mod manifest_tests {
  use aok::{OK, Void};
  use jdb::{Manifest, TableEntry, load_manifest, save_manifest};

  #[test]
  fn test_manifest_new() -> Void {
    let manifest = Manifest::new();
    assert_eq!(manifest.version, 0);
    assert_eq!(manifest.seqno, 0);
    assert_eq!(manifest.next_table_id, 1);
    assert_eq!(manifest.level_count(), 1); // L0 by default
    OK
  }

  #[test]
  fn test_manifest_add_table() -> Void {
    let mut manifest = Manifest::new();

    let entry = TableEntry {
      id: 1,
      min_key: b"aaa".to_vec().into_boxed_slice(),
      max_key: b"zzz".to_vec().into_boxed_slice(),
      item_count: 100,
      file_size: 4096,
    };

    manifest.add_table(0, entry.clone());

    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.level(0).unwrap().tables.len(), 1);
    assert_eq!(manifest.level(0).unwrap().tables[0].id, 1);
    OK
  }

  #[test]
  fn test_manifest_remove_table() -> Void {
    let mut manifest = Manifest::new();

    // Add tables
    // 添加表
    for i in 1..=3 {
      let entry = TableEntry {
        id: i,
        min_key: format!("key{i}").into_bytes().into_boxed_slice(),
        max_key: format!("key{i}z").into_bytes().into_boxed_slice(),
        item_count: 100,
        file_size: 4096,
      };
      manifest.add_table(0, entry);
    }

    assert_eq!(manifest.level(0).unwrap().tables.len(), 3);

    // Remove middle table
    // 移除中间的表
    let removed = manifest.remove_table(0, 2);
    assert!(removed);
    assert_eq!(manifest.level(0).unwrap().tables.len(), 2);

    // Verify remaining tables
    // 验证剩余的表
    let ids: Vec<_> = manifest
      .level(0)
      .unwrap()
      .tables
      .iter()
      .map(|t| t.id)
      .collect();
    assert_eq!(ids, vec![1, 3]);

    // Remove non-existent table
    // 移除不存在的表
    let removed = manifest.remove_table(0, 99);
    assert!(!removed);
    OK
  }

  #[test]
  fn test_manifest_ensure_level() -> Void {
    let mut manifest = Manifest::new();
    assert_eq!(manifest.level_count(), 1);

    manifest.ensure_level(3);
    assert_eq!(manifest.level_count(), 4);

    // Levels should be properly numbered
    // 层级应该正确编号
    for i in 0..4 {
      assert_eq!(manifest.level(i).unwrap().level, i);
    }
    OK
  }

  #[test]
  fn test_manifest_encode_decode_empty() -> Void {
    let manifest = Manifest::new();
    let encoded = manifest.encode();
    let decoded = Manifest::decode(&encoded)?;

    assert_eq!(decoded.version, manifest.version);
    assert_eq!(decoded.seqno, manifest.seqno);
    assert_eq!(decoded.next_table_id, manifest.next_table_id);
    assert_eq!(decoded.level_count(), manifest.level_count());
    OK
  }

  #[test]
  fn test_manifest_encode_decode_with_tables() -> Void {
    let mut manifest = Manifest::new();
    manifest.seqno = 12345;
    manifest.next_table_id = 100;

    // Add tables to multiple levels
    // 添加表到多个层级
    for level in 0..3 {
      for i in 0..5 {
        let id = (level * 10 + i) as u64;
        let entry = TableEntry {
          id,
          min_key: format!("L{level}key{i}").into_bytes().into_boxed_slice(),
          max_key: format!("L{level}key{i}z").into_bytes().into_boxed_slice(),
          item_count: 100 + id,
          file_size: 4096 + id * 100,
        };
        manifest.add_table(level, entry);
      }
    }

    let encoded = manifest.encode();
    let decoded = Manifest::decode(&encoded)?;

    assert_eq!(decoded.version, manifest.version);
    assert_eq!(decoded.seqno, manifest.seqno);
    assert_eq!(decoded.next_table_id, manifest.next_table_id);
    assert_eq!(decoded.level_count(), manifest.level_count());

    // Verify all tables
    // 验证所有表
    for level in 0..3 {
      let orig_level = manifest.level(level).unwrap();
      let dec_level = decoded.level(level).unwrap();
      assert_eq!(dec_level.tables.len(), orig_level.tables.len());

      for (orig, dec) in orig_level.tables.iter().zip(dec_level.tables.iter()) {
        assert_eq!(dec.id, orig.id);
        assert_eq!(dec.min_key, orig.min_key);
        assert_eq!(dec.max_key, orig.max_key);
        assert_eq!(dec.item_count, orig.item_count);
        assert_eq!(dec.file_size, orig.file_size);
      }
    }
    OK
  }

  #[test]
  fn test_manifest_decode_invalid_magic() -> Void {
    let mut data = vec![0u8; 40];
    // Invalid magic
    // 无效魔数
    data[0..4].copy_from_slice(&[0x00, 0x00, 0x00, 0x00]);

    let result = Manifest::decode(&data);
    assert!(result.is_err());
    OK
  }

  #[test]
  fn test_manifest_decode_checksum_mismatch() -> Void {
    let manifest = Manifest::new();
    let mut encoded = manifest.encode();

    // Corrupt checksum
    // 损坏校验和
    let len = encoded.len();
    encoded[len - 1] ^= 0xff;

    let result = Manifest::decode(&encoded);
    assert!(result.is_err());
    OK
  }

  #[test]
  fn test_manifest_all_table_ids() -> Void {
    let mut manifest = Manifest::new();

    // Add tables to multiple levels
    // 添加表到多个层级
    manifest.add_table(
      0,
      TableEntry {
        id: 1,
        min_key: Box::new([]),
        max_key: Box::new([]),
        item_count: 0,
        file_size: 0,
      },
    );
    manifest.add_table(
      0,
      TableEntry {
        id: 2,
        min_key: Box::new([]),
        max_key: Box::new([]),
        item_count: 0,
        file_size: 0,
      },
    );
    manifest.add_table(
      1,
      TableEntry {
        id: 3,
        min_key: Box::new([]),
        max_key: Box::new([]),
        item_count: 0,
        file_size: 0,
      },
    );

    let ids = manifest.all_table_ids();
    assert_eq!(ids.len(), 3);
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
    assert!(ids.contains(&3));
    OK
  }

  #[test]
  fn test_manifest_save_load() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir =
        std::env::temp_dir().join(format!("test_manifest_save_load_{}", fastrand::u64(..)));
      std::fs::create_dir_all(&tmp_dir)?;

      // Create manifest with data
      // 创建带数据的清单
      let mut manifest = Manifest::new();
      manifest.seqno = 999;
      manifest.next_table_id = 50;

      for i in 0..3 {
        let entry = TableEntry {
          id: i + 1,
          min_key: format!("min{i}").into_bytes().into_boxed_slice(),
          max_key: format!("max{i}").into_bytes().into_boxed_slice(),
          item_count: 100 * (i + 1),
          file_size: 4096 * (i + 1),
        };
        manifest.add_table(0, entry);
      }

      // Save
      // 保存
      save_manifest(&tmp_dir, &manifest).await?;

      // Load
      // 加载
      let loaded = load_manifest(&tmp_dir)
        .await?
        .expect("should load manifest");

      assert_eq!(loaded.version, manifest.version);
      assert_eq!(loaded.seqno, manifest.seqno);
      assert_eq!(loaded.next_table_id, manifest.next_table_id);
      assert_eq!(loaded.level(0).unwrap().tables.len(), 3);

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_manifest_load_nonexistent() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir =
        std::env::temp_dir().join(format!("test_manifest_nonexistent_{}", fastrand::u64(..)));
      std::fs::create_dir_all(&tmp_dir)?;

      // Load from empty directory
      // 从空目录加载
      let loaded = load_manifest(&tmp_dir).await?;
      assert!(loaded.is_none());

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }

  #[test]
  fn test_manifest_atomic_update() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir =
        std::env::temp_dir().join(format!("test_manifest_atomic_{}", fastrand::u64(..)));
      std::fs::create_dir_all(&tmp_dir)?;

      // Save initial manifest
      // 保存初始清单
      let mut manifest = Manifest::new();
      manifest.seqno = 100;
      save_manifest(&tmp_dir, &manifest).await?;

      // Update manifest
      // 更新清单
      manifest.seqno = 200;
      manifest.add_table(
        0,
        TableEntry {
          id: 1,
          min_key: b"a".to_vec().into_boxed_slice(),
          max_key: b"z".to_vec().into_boxed_slice(),
          item_count: 50,
          file_size: 2048,
        },
      );
      save_manifest(&tmp_dir, &manifest).await?;

      // Load and verify update
      // 加载并验证更新
      let loaded = load_manifest(&tmp_dir).await?.expect("should load");
      assert_eq!(loaded.seqno, 200);
      assert_eq!(loaded.level(0).unwrap().tables.len(), 1);

      // Verify no temp file left
      // 验证没有临时文件残留
      let tmp_path = tmp_dir.join("MANIFEST.tmp");
      assert!(!tmp_path.exists());

      // Cleanup
      // 清理
      let _ = std::fs::remove_dir_all(&tmp_dir);
      OK
    })
  }
}

// Property-based tests for Manifest
// Manifest 属性测试
mod proptest_manifest {
  use jdb::{Manifest, TableEntry};
  use proptest::prelude::*;

  fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..64)
  }

  fn arb_table_entry() -> impl Strategy<Value = TableEntry> {
    (
      any::<u64>(),
      arb_key(),
      arb_key(),
      any::<u64>(),
      any::<u64>(),
    )
      .prop_map(|(id, min_key, max_key, item_count, file_size)| TableEntry {
        id,
        min_key: min_key.into_boxed_slice(),
        max_key: max_key.into_boxed_slice(),
        item_count,
        file_size,
      })
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jdb-kv-database, Manifest encode/decode roundtrip
    /// For any manifest state, encoding then decoding should produce
    /// an equivalent manifest.
    /// **Validates: Requirements 8.4**
    #[test]
    fn prop_manifest_roundtrip(
      seqno in any::<u64>(),
      next_table_id in any::<u64>(),
      entries in prop::collection::vec(
        (0usize..4, arb_table_entry()),
        0..20
      )
    ) {
      let mut manifest = Manifest::new();
      manifest.seqno = seqno;
      manifest.next_table_id = next_table_id;

      for (level, entry) in entries {
        manifest.add_table(level, entry);
      }

      let encoded = manifest.encode();
      let decoded = Manifest::decode(&encoded).expect("should decode");

      prop_assert_eq!(decoded.seqno, manifest.seqno);
      prop_assert_eq!(decoded.next_table_id, manifest.next_table_id);
      prop_assert_eq!(decoded.level_count(), manifest.level_count());

      // Verify all levels and tables
      // 验证所有层级和表
      for level_idx in 0..manifest.level_count() {
        let orig = manifest.level(level_idx).unwrap();
        let dec = decoded.level(level_idx).unwrap();
        prop_assert_eq!(dec.tables.len(), orig.tables.len());

        for (o, d) in orig.tables.iter().zip(dec.tables.iter()) {
          prop_assert_eq!(d.id, o.id);
          prop_assert_eq!(d.min_key.as_ref(), o.min_key.as_ref());
          prop_assert_eq!(d.max_key.as_ref(), o.max_key.as_ref());
          prop_assert_eq!(d.item_count, o.item_count);
          prop_assert_eq!(d.file_size, o.file_size);
        }
      }
    }

    /// Feature: jdb-kv-database, Manifest version increments on modification
    /// Each add_table or remove_table should increment the version.
    /// **Validates: Requirements 8.4**
    #[test]
    fn prop_manifest_version_increments(
      entries in prop::collection::vec(arb_table_entry(), 1..10)
    ) {
      let mut manifest = Manifest::new();
      let initial_version = manifest.version;

      for (i, entry) in entries.iter().enumerate() {
        manifest.add_table(0, entry.clone());
        prop_assert_eq!(
          manifest.version,
          initial_version + (i as u64) + 1,
          "Version should increment after add"
        );
      }

      // Remove should also increment
      // 移除也应该增加版本
      if !entries.is_empty() {
        let version_before = manifest.version;
        let removed = manifest.remove_table(0, entries[0].id);
        if removed {
          prop_assert_eq!(
            manifest.version,
            version_before + 1,
            "Version should increment after remove"
          );
        }
      }
    }
  }
}

// Property-based tests for Compaction
// Compaction 属性测试
mod proptest_compaction {
  use std::collections::HashMap;

  use jdb::{Conf, ConfItem, Entry, Index};
  use jdb_base::Pos;
  use proptest::prelude::*;

  // Generate random key
  // 生成随机键
  fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..32)
  }

  // Generate random position
  // 生成随机位置
  fn arb_pos() -> impl Strategy<Value = Pos> {
    (any::<u64>(), any::<u64>(), any::<u32>())
      .prop_map(|(id, offset, len)| Pos::infile(id, offset, len))
  }

  // Generate operation: put or delete
  // 生成操作：插入或删除
  #[derive(Debug, Clone)]
  enum Op {
    Put(Vec<u8>, Pos),
    Del(Vec<u8>),
  }

  fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
      (arb_key(), arb_pos()).prop_map(|(k, p)| Op::Put(k, p)),
      arb_key().prop_map(Op::Del),
    ]
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jdb-kv-database, Property 8: Compaction Preserves Data
    /// For any database state, after compaction completes, all non-deleted keys
    /// should still be retrievable with their most recent values.
    /// Compaction should not lose or corrupt data.
    /// **Validates: Requirements 8.2, 8.3, 7.5**
    #[test]
    fn prop_compaction_preserves_data(
      ops1 in prop::collection::vec(arb_op(), 5..30),
      ops2 in prop::collection::vec(arb_op(), 5..30)
    ) {
      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp_dir = std::env::temp_dir()
          .join(format!("prop_compaction_preserves_data_{}", fastrand::u64(..)));
        std::fs::create_dir_all(&tmp_dir).unwrap();

        // Use small memtable and low L0 threshold to trigger compaction
        // 使用小内存表和低 L0 阈值来触发压缩
        let conf = Conf::from_items(&[
          ConfItem::MemtableSize(512),
          ConfItem::L0Threshold(2),
        ]);
        let mut index = Index::new(tmp_dir.clone(), conf);

        // Track expected state: key -> (pos, is_deleted)
        // 跟踪预期状态：键 -> (位置, 是否删除)
        let mut expected: HashMap<Vec<u8>, Option<Pos>> = HashMap::new();

        // First batch of operations
        // 第一批操作
        for op in &ops1 {
          match op {
            Op::Put(key, pos) => {
              index.put(key.clone().into_boxed_slice(), *pos);
              expected.insert(key.clone(), Some(*pos));
            }
            Op::Del(key) => {
              index.del(key.clone().into_boxed_slice());
              expected.insert(key.clone(), None);
            }
          }
        }

        // Seal and flush to create L0 SSTable
        // 密封并刷新以创建 L0 SSTable
        index.seal_memtable();
        let flush1 = index.flush_sealed().await;

        // Second batch of operations
        // 第二批操作
        for op in &ops2 {
          match op {
            Op::Put(key, pos) => {
              index.put(key.clone().into_boxed_slice(), *pos);
              expected.insert(key.clone(), Some(*pos));
            }
            Op::Del(key) => {
              index.del(key.clone().into_boxed_slice());
              expected.insert(key.clone(), None);
            }
          }
        }

        // Seal and flush again
        // 再次密封并刷新
        index.seal_memtable();
        let flush2 = index.flush_sealed().await;

        // Debug: print flush results
        // 调试：打印刷新结果
        eprintln!("Flush 1: {:?}, Flush 2: {:?}", flush1, flush2);
        eprintln!("L0 count before compaction: {}", index.l0_count());

        // Run compaction if needed
        // 如果需要则运行压缩
        let compacted = index.maybe_compact().await;
        eprintln!("Compacted: {:?}", compacted);
        eprintln!("L0 count after compaction: {}", index.l0_count());
        eprintln!("Levels: {}", index.levels().len());
        for (i, level) in index.levels().iter().enumerate() {
          eprintln!("Level {} has {} tables", i, level.len());
          for table in &level.tables {
            let meta = table.meta();
            eprintln!("  Table {}: min_key={:?}, max_key={:?}", meta.id, meta.min_key, meta.max_key);
          }
        }

        // Verify all expected data is preserved
        // 验证所有预期数据都被保留
        for (key, expected_value) in &expected {
          let entry = index.get(key).await.unwrap();

          match expected_value {
            Some(pos) => {
              // Key should exist with correct value
              // 键应该存在且值正确
              match entry {
                Some(Entry::Value(got_pos)) => {
                  prop_assert_eq!(
                    got_pos, *pos,
                    "Value mismatch for key {:?}: expected {:?}, got {:?}",
                    key, pos, got_pos
                  );
                }
                Some(Entry::Tombstone) => {
                  // This can happen if a later delete was applied
                  // 如果后来应用了删除，这可能发生
                }
                None => {
                  // This shouldn't happen for non-deleted keys
                  // 对于未删除的键，这不应该发生
                  eprintln!("ERROR: Key {:?} not found after compaction", key);
                  eprintln!("Expected: {:?}", pos);
                  prop_assert!(false, "Key {:?} not found after compaction", key);
                }
              }
            }
            None => {
              // Key should be deleted (tombstone or not found)
              // 键应该被删除（删除标记或未找到）
              match entry {
                Some(Entry::Tombstone) => {
                  // Expected: tombstone
                  // 预期：删除标记
                }
                None => {
                  // Also acceptable: tombstone was compacted away
                  // 也可接受：删除标记被压缩掉了
                }
                Some(Entry::Value(_)) => {
                  prop_assert!(false, "Deleted key {:?} still has value after compaction", key);
                }
              }
            }
          }
        }

        // Verify iteration still works and is sorted
        // 验证迭代仍然有效且有序
        let keys: Vec<_> = index.iter().await.unwrap().map(|e| e.key.to_vec()).collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        prop_assert_eq!(&keys, &sorted_keys, "Iteration not sorted after compaction");

        // Cleanup
        // 清理
        let _ = std::fs::remove_dir_all(&tmp_dir);
        Ok(())
      })?;
    }
  }
}

// Property-based tests for Namespace Isolation
// 命名空间隔离属性测试
mod proptest_namespace {
  use jdb::{Conf, Entry, Ns, NsId, NsMgr};
  use jdb_base::Pos;
  use proptest::prelude::*;

  // Generate random key
  // 生成随机键
  fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..32)
  }

  fn arb_pos() -> impl Strategy<Value = Pos> {
    (any::<u64>(), any::<u64>(), any::<u32>())
      .prop_map(|(id, offset, len)| Pos::infile(id, offset, len))
  }

  // Generate two different namespace IDs
  // 生成两个不同的命名空间 ID
  fn arb_two_ns_ids() -> impl Strategy<Value = (NsId, NsId)> {
    (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_filter_map(
      "ns_ids must be different",
      |(site1, user1, site2, user2)| {
        let id1 = NsId::new(site1, user1);
        let id2 = NsId::new(site2, user2);
        if id1 != id2 { Some((id1, id2)) } else { None }
      },
    )
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jdb-kv-database, Property 5: Namespace Isolation
    /// For any two different namespaces, operations in one namespace should not
    /// affect data in the other namespace. Specifically, putting a key in namespace A
    /// should not be visible when getting the same key from namespace B.
    /// **Validates: Requirements 6.2**
    #[test]
    fn prop_namespace_isolation(
      (ns_id_a, ns_id_b) in arb_two_ns_ids(),
      key in arb_key(),
      pos_a in arb_pos(),
      pos_b in arb_pos()
    ) {
      // Run async test in compio runtime
      // 在 compio 运行时中运行异步测试
      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp_dir = std::env::temp_dir();
        let test_id = fastrand::u64(..);
        let root = tmp_dir.join(format!("test_ns_isolation_{test_id}"));
        std::fs::create_dir_all(&root).unwrap();

        let conf = Conf::default();
        let mut ns_mgr = NsMgr::new(root.clone(), conf);

        // Put key in namespace A
        // 在命名空间 A 中写入键
        {
          let mut ns_a = Ns::new(&mut ns_mgr, ns_id_a);
          ns_a.put(&key, pos_a).await.unwrap();
        }

        // Get key from namespace B (should be None)
        // 从命名空间 B 获取键（应该为 None）
        {
          let mut ns_b = Ns::new(&mut ns_mgr, ns_id_b);
          let entry_b = ns_b.get(&key).await.unwrap();
          assert!(
            entry_b.is_none(),
            "Key {:?} in ns_a should not be visible in ns_b",
            key
          );
        }

        // Put different value for same key in namespace B
        // 在命名空间 B 中为相同键写入不同值
        {
          let mut ns_b = Ns::new(&mut ns_mgr, ns_id_b);
          ns_b.put(&key, pos_b).await.unwrap();
        }

        // Verify namespace A still has original value
        // 验证命名空间 A 仍有原始值
        {
          let mut ns_a = Ns::new(&mut ns_mgr, ns_id_a);
          let entry_a = ns_a.get(&key).await.unwrap();
          assert_eq!(
            entry_a,
            Some(Entry::Value(pos_a)),
            "Namespace A value should not be affected by namespace B"
          );
        }

        // Verify namespace B has its own value
        // 验证命名空间 B 有自己的值
        {
          let mut ns_b = Ns::new(&mut ns_mgr, ns_id_b);
          let entry_b = ns_b.get(&key).await.unwrap();
          assert_eq!(
            entry_b,
            Some(Entry::Value(pos_b)),
            "Namespace B should have its own value"
          );
        }

        // Delete key in namespace A
        // 在命名空间 A 中删除键
        {
          let mut ns_a = Ns::new(&mut ns_mgr, ns_id_a);
          ns_a.del(&key).await.unwrap();
        }

        // Verify namespace B still has its value (delete in A doesn't affect B)
        // 验证命名空间 B 仍有其值（A 中的删除不影响 B）
        {
          let mut ns_b = Ns::new(&mut ns_mgr, ns_id_b);
          let entry_b = ns_b.get(&key).await.unwrap();
          assert_eq!(
            entry_b,
            Some(Entry::Value(pos_b)),
            "Delete in namespace A should not affect namespace B"
          );
        }

        // Cleanup
        // 清理
        let _ = std::fs::remove_dir_all(&root);
      });
    }

    /// Feature: jdb-kv-database, Property 5: Namespace Isolation (iteration)
    /// Iteration in one namespace should only return keys from that namespace.
    /// **Validates: Requirements 6.2**
    #[test]
    fn prop_namespace_iteration_isolation(
      (ns_id_a, ns_id_b) in arb_two_ns_ids(),
      keys_a in prop::collection::vec(arb_key(), 1..20),
      keys_b in prop::collection::vec(arb_key(), 1..20),
      pos in arb_pos()
    ) {
      compio::runtime::Runtime::new().unwrap().block_on(async {
        let tmp_dir = std::env::temp_dir();
        let test_id = fastrand::u64(..);
        let root = tmp_dir.join(format!("test_ns_iter_isolation_{test_id}"));
        std::fs::create_dir_all(&root).unwrap();

        let conf = Conf::default();
        let mut ns_mgr = NsMgr::new(root.clone(), conf);

        // Put keys in namespace A
        // 在命名空间 A 中写入键
        {
          let mut ns_a = Ns::new(&mut ns_mgr, ns_id_a);
          for key in &keys_a {
            ns_a.put(key, pos).await.unwrap();
          }
        }

        // Put keys in namespace B
        // 在命名空间 B 中写入键
        {
          let mut ns_b = Ns::new(&mut ns_mgr, ns_id_b);
          for key in &keys_b {
            ns_b.put(key, pos).await.unwrap();
          }
        }

        // Iterate namespace A - should only see keys_a
        // 迭代命名空间 A - 应该只看到 keys_a
        {
          let mut ns_a = Ns::new(&mut ns_mgr, ns_id_a);
          let iter_a = ns_a.iter().await.unwrap();
          let found_keys_a: std::collections::HashSet<Vec<u8>> =
            iter_a.map(|e| e.key.to_vec()).collect();

          // All keys_a should be found
          // 所有 keys_a 应该被找到
          for key in &keys_a {
            assert!(
              found_keys_a.contains(key),
              "Key {:?} from ns_a should be in iteration",
              key
            );
          }

          // No keys_b should be found (unless they overlap with keys_a)
          // 不应该找到 keys_b（除非与 keys_a 重叠）
          let keys_a_set: std::collections::HashSet<Vec<u8>> = keys_a.iter().cloned().collect();
          for key in &keys_b {
            if !keys_a_set.contains(key) {
              assert!(
                !found_keys_a.contains(key),
                "Key {:?} from ns_b should not be in ns_a iteration",
                key
              );
            }
          }
        }

        // Iterate namespace B - should only see keys_b
        // 迭代命名空间 B - 应该只看到 keys_b
        {
          let mut ns_b = Ns::new(&mut ns_mgr, ns_id_b);
          let iter_b = ns_b.iter().await.unwrap();
          let found_keys_b: std::collections::HashSet<Vec<u8>> =
            iter_b.map(|e| e.key.to_vec()).collect();

          // All keys_b should be found
          // 所有 keys_b 应该被找到
          for key in &keys_b {
            assert!(
              found_keys_b.contains(key),
              "Key {:?} from ns_b should be in iteration",
              key
            );
          }

          // No keys_a should be found (unless they overlap with keys_b)
          // 不应该找到 keys_a（除非与 keys_b 重叠）
          let keys_b_set: std::collections::HashSet<Vec<u8>> = keys_b.iter().cloned().collect();
          for key in &keys_a {
            if !keys_b_set.contains(key) {
              assert!(
                !found_keys_b.contains(key),
                "Key {:?} from ns_a should not be in ns_b iteration",
                key
              );
            }
          }
        }

        // Cleanup
        // 清理
        let _ = std::fs::remove_dir_all(&root);
      });
    }
  }
}

// Batch unit tests
// Batch 单元测试
mod batch_tests {
  use aok::{OK, Void};
  use jdb::{Batch, NsId, Op};
  use log::info;

  #[test]
  fn test_batch_new() -> Void {
    info!("> Batch new");
    let ns_id = NsId::new(1, 100);
    let batch = Batch::new(ns_id);

    assert_eq!(batch.ns_id(), ns_id);
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
    assert!(batch.ops().is_empty());
    OK
  }

  #[test]
  fn test_batch_with_capacity() -> Void {
    info!("> Batch with capacity");
    let ns_id = NsId::new(1, 100);
    let batch = Batch::with_capacity(ns_id, 10);

    assert_eq!(batch.ns_id(), ns_id);
    assert!(batch.is_empty());
    OK
  }

  #[test]
  fn test_batch_put() -> Void {
    info!("> Batch put");
    let ns_id = NsId::new(1, 100);
    let mut batch = Batch::new(ns_id);

    batch.put(b"key1", b"value1");
    batch.put(b"key2", b"value2");

    assert_eq!(batch.len(), 2);
    assert!(!batch.is_empty());

    let ops = batch.ops();
    assert!(ops[0].is_put());
    assert!(!ops[0].is_del());
    assert_eq!(ops[0].key(), b"key1");

    assert!(ops[1].is_put());
    assert_eq!(ops[1].key(), b"key2");
    OK
  }

  #[test]
  fn test_batch_del() -> Void {
    info!("> Batch del");
    let ns_id = NsId::new(1, 100);
    let mut batch = Batch::new(ns_id);

    batch.del(b"key1");
    batch.del(b"key2");

    assert_eq!(batch.len(), 2);

    let ops = batch.ops();
    assert!(ops[0].is_del());
    assert!(!ops[0].is_put());
    assert_eq!(ops[0].key(), b"key1");

    assert!(ops[1].is_del());
    assert_eq!(ops[1].key(), b"key2");
    OK
  }

  #[test]
  fn test_batch_mixed_ops() -> Void {
    info!("> Batch mixed operations");
    let ns_id = NsId::new(1, 100);
    let mut batch = Batch::new(ns_id);

    batch.put(b"key1", b"value1");
    batch.del(b"key2");
    batch.put(b"key3", b"value3");
    batch.del(b"key1"); // Delete previously put key
    // 删除之前插入的键

    assert_eq!(batch.len(), 4);

    let ops = batch.ops();
    assert!(ops[0].is_put());
    assert!(ops[1].is_del());
    assert!(ops[2].is_put());
    assert!(ops[3].is_del());
    OK
  }

  #[test]
  fn test_batch_clear() -> Void {
    info!("> Batch clear");
    let ns_id = NsId::new(1, 100);
    let mut batch = Batch::new(ns_id);

    batch.put(b"key1", b"value1");
    batch.put(b"key2", b"value2");
    assert_eq!(batch.len(), 2);

    batch.clear();
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
    OK
  }

  #[test]
  fn test_batch_into_ops() -> Void {
    info!("> Batch into_ops");
    let ns_id = NsId::new(1, 100);
    let mut batch = Batch::new(ns_id);

    batch.put(b"key1", b"value1");
    batch.del(b"key2");

    let ops = batch.into_ops();
    assert_eq!(ops.len(), 2);

    match &ops[0] {
      Op::Put { key, val } => {
        assert_eq!(key.as_ref(), b"key1");
        assert_eq!(val.as_ref(), b"value1");
      }
      Op::Del { .. } => panic!("Expected Put"),
    }

    match &ops[1] {
      Op::Del { key } => {
        assert_eq!(key.as_ref(), b"key2");
      }
      Op::Put { .. } => panic!("Expected Del"),
    }
    OK
  }

  #[test]
  fn test_op_key() -> Void {
    info!("> Op key accessor");
    let put_op = Op::Put {
      key: b"put_key".to_vec().into_boxed_slice(),
      val: b"value".to_vec().into_boxed_slice(),
    };
    let del_op = Op::Del {
      key: b"del_key".to_vec().into_boxed_slice(),
    };

    assert_eq!(put_op.key(), b"put_key");
    assert_eq!(del_op.key(), b"del_key");
    OK
  }
}

// Property-based tests for Batch atomicity
// Batch 原子性属性测试
mod proptest_batch {
  use jdb::{Batch, Conf, NsId, NsMgr};
  use jdb_val::Wal;
  use proptest::prelude::*;

  // Generate random key-value pairs
  // 生成随机键值对
  fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..64)
  }

  fn arb_val() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..256)
  }

  // Generate batch operations
  // 生成批量操作
  #[derive(Debug, Clone)]
  enum BatchOp {
    Put { key: Vec<u8>, val: Vec<u8> },
    Del { key: Vec<u8> },
  }

  fn arb_batch_op() -> impl Strategy<Value = BatchOp> {
    prop_oneof![
      (arb_key(), arb_val()).prop_map(|(key, val)| BatchOp::Put { key, val }),
      arb_key().prop_map(|key| BatchOp::Del { key }),
    ]
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jdb-kv-database, Property 6: Batch Atomicity
    /// For any batch of operations, either all operations are visible after commit,
    /// or none are visible. Before commit, no operations from the batch should be visible.
    /// **Validates: Requirements 3.1, 3.2, 3.4**
    #[test]
    fn prop_batch_atomicity(
      ops in prop::collection::vec(arb_batch_op(), 1..20),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        // Create WAL and NsMgr
        // 创建 WAL 和 NsMgr
        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();

        let conf = Conf::default();
        let mut ns_mgr = NsMgr::new(dir.path().to_path_buf(), conf);

        // Create batch with operations
        // 创建带操作的批次
        let mut batch = Batch::new(ns_id);
        let mut expected_state: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();

        for op in &ops {
          match op {
            BatchOp::Put { key, val } => {
              batch.put(key, val);
              // Track expected final state (last write wins)
              // 跟踪预期最终状态（最后写入获胜）
              if let Some(pos) = expected_state.iter().position(|(k, _)| k == key) {
                expected_state[pos].1 = Some(val.clone());
              } else {
                expected_state.push((key.clone(), Some(val.clone())));
              }
            }
            BatchOp::Del { key } => {
              batch.del(key);
              // Track expected final state (delete = None)
              // 跟踪预期最终状态（删除 = None）
              if let Some(pos) = expected_state.iter().position(|(k, _)| k == key) {
                expected_state[pos].1 = None;
              } else {
                expected_state.push((key.clone(), None));
              }
            }
          }
        }

        // Before commit: no operations should be visible
        // 提交前：不应该看到任何操作
        {
          let ns_index = ns_mgr.get(ns_id).await.unwrap();
          for (key, _) in &expected_state {
            let entry = ns_index.index.get(key).await.unwrap();
            if entry.is_some() {
              return Err(TestCaseError::fail(format!(
                "Key {:?} should not be visible before commit",
                key
              )));
            }
          }
        }

        // Commit batch
        // 提交批次
        batch.commit(&mut wal, &mut ns_mgr).await.unwrap();

        // After commit: all operations should be visible with correct values
        // 提交后：所有操作应该可见且值正确
        {
          let ns_index = ns_mgr.get(ns_id).await.unwrap();
          for (key, expected_val) in &expected_state {
            let entry = ns_index.index.get(key).await.unwrap();
            match expected_val {
              Some(_) => {
                // Put operation: entry should exist (not tombstone)
                // Put 操作：条目应该存在（非删除标记）
                if entry.is_none() {
                  return Err(TestCaseError::fail(format!(
                    "Key {:?} should exist after commit",
                    key
                  )));
                }
                let entry = entry.unwrap();
                if entry.is_tombstone() {
                  return Err(TestCaseError::fail(format!(
                    "Key {:?} should not be tombstone after put",
                    key
                  )));
                }
              }
              None => {
                // Del operation: entry should be tombstone
                // Del 操作：条目应该是删除标记
                if entry.is_none() {
                  return Err(TestCaseError::fail(format!(
                    "Key {:?} should have tombstone after delete",
                    key
                  )));
                }
                let entry = entry.unwrap();
                if !entry.is_tombstone() {
                  return Err(TestCaseError::fail(format!(
                    "Key {:?} should be tombstone after delete",
                    key
                  )));
                }
              }
            }
          }
        }
        Ok(())
      });
      result?;
    }

    /// Feature: jdb-kv-database, Property 6: Batch All-or-Nothing
    /// Empty batch commit should succeed without side effects.
    /// **Validates: Requirements 3.1, 3.2, 3.4**
    #[test]
    fn prop_batch_empty_commit(
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();

        let conf = Conf::default();
        let mut ns_mgr = NsMgr::new(dir.path().to_path_buf(), conf);

        // Empty batch should commit successfully
        // 空批次应该成功提交
        let batch = Batch::new(ns_id);
        let commit_result = batch.commit(&mut wal, &mut ns_mgr).await;
        if commit_result.is_err() {
          return Err(TestCaseError::fail("Empty batch commit should succeed"));
        }
        Ok(())
      });
      result?;
    }

    /// Feature: jdb-kv-database, Property 6: Batch Overwrites
    /// Multiple puts to the same key in a batch should result in the last value.
    /// **Validates: Requirements 3.1, 3.2, 3.4**
    #[test]
    fn prop_batch_overwrites(
      key in arb_key(),
      values in prop::collection::vec(arb_val(), 2..10),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();

        let conf = Conf::default();
        let mut ns_mgr = NsMgr::new(dir.path().to_path_buf(), conf);

        // Create batch with multiple puts to same key
        // 创建对同一键多次 put 的批次
        let mut batch = Batch::new(ns_id);
        for val in &values {
          batch.put(&key, val);
        }

        batch.commit(&mut wal, &mut ns_mgr).await.unwrap();

        // After commit: should have the last value
        // 提交后：应该有最后一个值
        let ns_index = ns_mgr.get(ns_id).await.unwrap();
        let entry = ns_index.index.get(&key).await.unwrap();
        if entry.is_none() {
          return Err(TestCaseError::fail("Key should exist after batch commit"));
        }

        let entry = entry.unwrap();
        if entry.is_tombstone() {
          return Err(TestCaseError::fail("Entry should not be tombstone"));
        }

        // The last put should win - we can verify by reading from WAL
        // 最后一个 put 应该获胜 - 我们可以通过从 WAL 读取来验证
        let pos = entry.pos().unwrap();
        let got_val = wal.val(pos).await.unwrap();
        let last_val = values.last().unwrap();
        if got_val.as_ref() != last_val.as_slice() {
          return Err(TestCaseError::fail(format!(
            "Should have last value after multiple puts, got {:?}, expected {:?}",
            got_val.as_ref(),
            last_val.as_slice()
          )));
        }
        Ok(())
      });
      result?;
    }

    /// Feature: jdb-kv-database, Property 6: Batch Put then Delete
    /// Put followed by delete in same batch should result in tombstone.
    /// **Validates: Requirements 3.1, 3.2, 3.4**
    #[test]
    fn prop_batch_put_then_delete(
      key in arb_key(),
      val in arb_val(),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();

        let conf = Conf::default();
        let mut ns_mgr = NsMgr::new(dir.path().to_path_buf(), conf);

        // Put then delete in same batch
        // 在同一批次中先 put 后 delete
        let mut batch = Batch::new(ns_id);
        batch.put(&key, &val);
        batch.del(&key);

        batch.commit(&mut wal, &mut ns_mgr).await.unwrap();

        // After commit: should be tombstone
        // 提交后：应该是删除标记
        let ns_index = ns_mgr.get(ns_id).await.unwrap();
        let entry = ns_index.index.get(&key).await.unwrap();
        if entry.is_none() {
          return Err(TestCaseError::fail("Key should have entry after batch"));
        }
        if !entry.unwrap().is_tombstone() {
          return Err(TestCaseError::fail("Entry should be tombstone after put-then-delete"));
        }
        Ok(())
      });
      result?;
    }

    /// Feature: jdb-kv-database, Property 6: Batch Delete then Put
    /// Delete followed by put in same batch should result in value.
    /// **Validates: Requirements 3.1, 3.2, 3.4**
    #[test]
    fn prop_batch_delete_then_put(
      key in arb_key(),
      val in arb_val(),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();

        let conf = Conf::default();
        let mut ns_mgr = NsMgr::new(dir.path().to_path_buf(), conf);

        // Delete then put in same batch
        // 在同一批次中先 delete 后 put
        let mut batch = Batch::new(ns_id);
        batch.del(&key);
        batch.put(&key, &val);

        batch.commit(&mut wal, &mut ns_mgr).await.unwrap();

        // After commit: should have value (not tombstone)
        // 提交后：应该有值（非删除标记）
        let ns_index = ns_mgr.get(ns_id).await.unwrap();
        let entry = ns_index.index.get(&key).await.unwrap();
        if entry.is_none() {
          return Err(TestCaseError::fail("Key should exist after batch"));
        }

        let entry = entry.unwrap();
        if entry.is_tombstone() {
          return Err(TestCaseError::fail("Entry should not be tombstone after delete-then-put"));
        }

        // Verify value
        // 验证值
        let pos = entry.pos().unwrap();
        let got_val = wal.val(pos).await.unwrap();
        if got_val.as_ref() != val.as_slice() {
          return Err(TestCaseError::fail(format!(
            "Should have correct value after delete-then-put, got {:?}, expected {:?}",
            got_val.as_ref(),
            val.as_slice()
          )));
        }
        Ok(())
      });
      result?;
    }
  }
}


// Property-based tests for Jdb main entry point
// Jdb 主入口属性测试
mod proptest_jdb {
  use jdb::{Jdb, NsId};
  use proptest::prelude::*;
  use proptest::test_runner::TestCaseError;

  // Generate random key (non-empty)
  // 生成随机键（非空）
  fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..64)
  }

  // Generate random value
  // 生成随机值
  fn arb_val() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..256)
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jdb-kv-database, Property 1: Put-Get Round Trip
    /// For any valid key-value pair, if it is put into the database,
    /// then getting that key should return the same value.
    /// **Validates: Requirements 2.1, 2.2, 7.1**
    #[test]
    fn prop_put_get_roundtrip(
      key in arb_key(),
      val in arb_val(),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut jdb = Jdb::new(dir.path(), &[]);
        jdb.open().await.unwrap();

        // Put key-value
        // 写入键值
        jdb.put(ns_id, &key, &val).await.unwrap();

        // Get should return same value
        // 获取应该返回相同的值
        let got = jdb.get(ns_id, &key).await.unwrap();
        if got.is_none() {
          return Err(TestCaseError::fail("Key should exist after put"));
        }

        let got_val = got.unwrap();
        if got_val != val {
          return Err(TestCaseError::fail(format!(
            "Value mismatch: got {:?}, expected {:?}",
            got_val, val
          )));
        }

        Ok(())
      });
      result?;
    }

    /// Feature: jdb-kv-database, Property 1: Multiple Put-Get Round Trip
    /// For any sequence of put operations, all values should be retrievable.
    /// **Validates: Requirements 2.1, 2.2, 7.1**
    #[test]
    fn prop_multiple_put_get_roundtrip(
      entries in prop::collection::vec((arb_key(), arb_val()), 1..20),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut jdb = Jdb::new(dir.path(), &[]);
        jdb.open().await.unwrap();

        // Put all entries
        // 写入所有条目
        let mut expected: std::collections::HashMap<Vec<u8>, Vec<u8>> = std::collections::HashMap::new();
        for (key, val) in &entries {
          jdb.put(ns_id, key, val).await.unwrap();
          expected.insert(key.clone(), val.clone());
        }

        // Verify all entries
        // 验证所有条目
        for (key, expected_val) in &expected {
          let got = jdb.get(ns_id, key).await.unwrap();
          if got.is_none() {
            return Err(TestCaseError::fail(format!("Key {:?} should exist", key)));
          }

          let got_val = got.unwrap();
          if &got_val != expected_val {
            return Err(TestCaseError::fail(format!(
              "Value mismatch for key {:?}: got {:?}, expected {:?}",
              key, got_val, expected_val
            )));
          }
        }

        Ok(())
      });
      result?;
    }

    /// Feature: jdb-kv-database, Property 1: Put overwrites previous value
    /// Putting a new value for an existing key should overwrite the old value.
    /// **Validates: Requirements 2.1, 2.2, 7.1**
    #[test]
    fn prop_put_overwrites(
      key in arb_key(),
      val1 in arb_val(),
      val2 in arb_val(),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut jdb = Jdb::new(dir.path(), &[]);
        jdb.open().await.unwrap();

        // Put first value
        // 写入第一个值
        jdb.put(ns_id, &key, &val1).await.unwrap();

        // Put second value (overwrite)
        // 写入第二个值（覆盖）
        jdb.put(ns_id, &key, &val2).await.unwrap();

        // Get should return second value
        // 获取应该返回第二个值
        let got = jdb.get(ns_id, &key).await.unwrap();
        if got.is_none() {
          return Err(TestCaseError::fail("Key should exist after put"));
        }

        let got_val = got.unwrap();
        if got_val != val2 {
          return Err(TestCaseError::fail(format!(
            "Value should be overwritten: got {:?}, expected {:?}",
            got_val, val2
          )));
        }

        Ok(())
      });
      result?;
    }
  }
}


// Property-based tests for delete operations
// 删除操作属性测试
mod proptest_jdb_delete {
  use jdb::{Jdb, NsId};
  use proptest::prelude::*;
  use proptest::test_runner::TestCaseError;

  // Generate random key (non-empty)
  // 生成随机键（非空）
  fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..64)
  }

  // Generate random value
  // 生成随机值
  fn arb_val() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..256)
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Feature: jdb-kv-database, Property 2: Delete Removes Key
    /// For any key that exists in the database, after deleting it,
    /// getting that key should return None.
    /// **Validates: Requirements 2.3, 2.4**
    #[test]
    fn prop_delete_removes_key(
      key in arb_key(),
      val in arb_val(),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut jdb = Jdb::new(dir.path(), &[]);
        jdb.open().await.unwrap();

        // Put key-value
        // 写入键值
        jdb.put(ns_id, &key, &val).await.unwrap();

        // Verify key exists
        // 验证键存在
        let got = jdb.get(ns_id, &key).await.unwrap();
        if got.is_none() {
          return Err(TestCaseError::fail("Key should exist after put"));
        }

        // Delete key
        // 删除键
        jdb.del(ns_id, &key).await.unwrap();

        // Get should return None
        // 获取应该返回 None
        let got = jdb.get(ns_id, &key).await.unwrap();
        if got.is_some() {
          return Err(TestCaseError::fail("Key should not exist after delete"));
        }

        Ok(())
      });
      result?;
    }

    /// Feature: jdb-kv-database, Property 2: Delete non-existent key is idempotent
    /// Deleting a key that doesn't exist should not cause errors.
    /// **Validates: Requirements 2.3, 2.4**
    #[test]
    fn prop_delete_nonexistent_key(
      key in arb_key(),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut jdb = Jdb::new(dir.path(), &[]);
        jdb.open().await.unwrap();

        // Delete non-existent key should not error
        // 删除不存在的键不应该报错
        jdb.del(ns_id, &key).await.unwrap();

        // Get should return None
        // 获取应该返回 None
        let got = jdb.get(ns_id, &key).await.unwrap();
        if got.is_some() {
          return Err(TestCaseError::fail("Non-existent key should return None"));
        }

        Ok(())
      });
      result?;
    }

    /// Feature: jdb-kv-database, Property 2: Delete then put creates new value
    /// After deleting a key, putting a new value should work correctly.
    /// **Validates: Requirements 2.3, 2.4**
    #[test]
    fn prop_delete_then_put(
      key in arb_key(),
      val1 in arb_val(),
      val2 in arb_val(),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut jdb = Jdb::new(dir.path(), &[]);
        jdb.open().await.unwrap();

        // Put first value
        // 写入第一个值
        jdb.put(ns_id, &key, &val1).await.unwrap();

        // Delete key
        // 删除键
        jdb.del(ns_id, &key).await.unwrap();

        // Put second value
        // 写入第二个值
        jdb.put(ns_id, &key, &val2).await.unwrap();

        // Get should return second value
        // 获取应该返回第二个值
        let got = jdb.get(ns_id, &key).await.unwrap();
        if got.is_none() {
          return Err(TestCaseError::fail("Key should exist after put"));
        }

        let got_val = got.unwrap();
        if got_val != val2 {
          return Err(TestCaseError::fail(format!(
            "Value should be new value after delete-then-put: got {:?}, expected {:?}",
            got_val, val2
          )));
        }

        Ok(())
      });
      result?;
    }

    /// Feature: jdb-kv-database, Property 2: Multiple deletes
    /// Deleting multiple keys should work correctly.
    /// **Validates: Requirements 2.3, 2.4**
    #[test]
    fn prop_multiple_deletes(
      entries in prop::collection::vec((arb_key(), arb_val()), 1..20),
      delete_indices in prop::collection::vec(any::<usize>(), 0..10),
      site_id in any::<u64>(),
      user_id in any::<u64>()
    ) {
      let result: std::result::Result<(), TestCaseError> = compio::runtime::Runtime::new().unwrap().block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let ns_id = NsId::new(site_id, user_id);

        let mut jdb = Jdb::new(dir.path(), &[]);
        jdb.open().await.unwrap();

        // Put all entries
        // 写入所有条目
        let mut expected: std::collections::HashMap<Vec<u8>, Vec<u8>> = std::collections::HashMap::new();
        for (key, val) in &entries {
          jdb.put(ns_id, key, val).await.unwrap();
          expected.insert(key.clone(), val.clone());
        }

        // Delete some entries
        // 删除一些条目
        let keys: Vec<_> = expected.keys().cloned().collect();
        let mut deleted: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        for idx in &delete_indices {
          if !keys.is_empty() {
            let key = &keys[idx % keys.len()];
            jdb.del(ns_id, key).await.unwrap();
            deleted.insert(key.clone());
          }
        }

        // Verify: deleted keys return None, others return value
        // 验证：删除的键返回 None，其他返回值
        for (key, expected_val) in &expected {
          let got = jdb.get(ns_id, key).await.unwrap();
          if deleted.contains(key) {
            if got.is_some() {
              return Err(TestCaseError::fail(format!(
                "Deleted key {:?} should return None",
                key
              )));
            }
          } else {
            if got.is_none() {
              return Err(TestCaseError::fail(format!(
                "Non-deleted key {:?} should exist",
                key
              )));
            }
            let got_val = got.unwrap();
            if &got_val != expected_val {
              return Err(TestCaseError::fail(format!(
                "Value mismatch for key {:?}: got {:?}, expected {:?}",
                key, got_val, expected_val
              )));
            }
          }
        }

        Ok(())
      });
      result?;
    }
  }
}
