#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

mod sstable_tests {
  use std::ops::Bound;

  use aok::{OK, Void};
  use futures_core::Stream;
  use jdb_base::{Pos, table::AsyncTable};
  use jdb_fs::{FileLru, fs_id::id_path};
  use jdb_sstable::{SSTable, TableInfo, Writer};

  /// Collect stream to vec
  /// 收集流到 vec
  async fn collect_stream<S: Stream<Item = jdb_base::table::Kv> + Unpin>(
    mut stream: S,
  ) -> Vec<jdb_base::table::Kv> {
    use std::{pin::Pin, task::Poll};
    let mut out = Vec::new();
    std::future::poll_fn(|cx| {
      loop {
        match Pin::new(&mut stream).poll_next(cx) {
          Poll::Ready(Some(item)) => out.push(item),
          Poll::Ready(None) => return Poll::Ready(()),
          Poll::Pending => return Poll::Pending,
        }
      }
    })
    .await;
    out
  }

  #[test]
  fn test_sstable_write_read_roundtrip() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;
      let table_id = 1u64;
      let path = id_path(&sst_dir, table_id);

      let mut writer = Writer::new(path.clone(), table_id, 10).await?;
      let entries = [
        (b"aaa".to_vec(), Pos::infile(1, 100, 10)),
        (b"bbb".to_vec(), Pos::infile(1, 200, 20)),
        (b"ccc".to_vec(), Pos::tombstone(0, 0)),
        (b"ddd".to_vec(), Pos::infile(1, 300, 30)),
        (b"eee".to_vec(), Pos::infile(1, 400, 40)),
      ];
      for (key, entry) in &entries {
        writer.add(key, entry).await?;
      }
      let meta = writer.finish().await?;
      assert_eq!(meta.item_count, 5);
      assert_eq!(meta.min_key.as_ref(), b"aaa");
      assert_eq!(meta.max_key.as_ref(), b"eee");

      let info = TableInfo::load(&path, table_id).await?;
      let mut file_lru = FileLru::new(&sst_dir, 16);
      let entry = info
        .get_pos(b"aaa", &mut file_lru)
        .await?
        .expect("should find");
      assert_eq!(entry, Pos::infile(1, 100, 10));
      // Tombstone should be found in filter and returned
      // Tombstone 应该在过滤器中被找到并返回
      let entry = info
        .get_pos(b"ccc", &mut file_lru)
        .await?
        .expect("tombstone should be found");
      assert!(entry.is_tombstone());
      let mut table = SSTable::new(info, &mut file_lru);
      let items = collect_stream(table.iter()).await;
      // 5 items (including tombstone, iter returns all)
      // 5 条（包含删除标记，iter 返回全部）
      assert_eq!(items.len(), 5);
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

      let mut writer = Writer::new(path.clone(), table_id, 10).await?;
      for i in 0..10u8 {
        let key = format!("key{i:02}").into_bytes();
        let entry = Pos::infile(1, i as u64 * 100, i as u32 * 10);
        writer.add(&key, &entry).await?;
      }
      writer.finish().await?;

      let info = TableInfo::load(&path, table_id).await?;
      let mut file_lru = FileLru::new(&sst_dir, 16);
      let mut table = SSTable::new(info, &mut file_lru);
      let range_items = collect_stream(table.range(
        Bound::Included(b"key03".as_ref()),
        Bound::Included(b"key07".as_ref()),
      ))
      .await;
      assert_eq!(range_items.len(), 5);
      assert_eq!(range_items[0].0.as_ref(), b"key03");
      assert_eq!(range_items[4].0.as_ref(), b"key07");
      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  #[test]
  fn test_sstable_empty() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let path = tmp_dir.join(format!("test_sstable_empty_{test_id}.sst"));
      let writer = Writer::new(path.clone(), 1, 10).await?;
      let meta = writer.finish().await?;
      assert_eq!(meta.item_count, 0);
      assert!(!path.exists());
      OK
    })
  }

  /// Test PGM index and bloom filter with large dataset
  /// 测试大数据量下的 PGM 索引和布隆过滤器
  #[test]
  fn test_sstable_pgm_and_filter() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_pgm_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;
      let table_id = 1u64;
      let path = id_path(&sst_dir, table_id);

      // Write 10000 entries to create multiple blocks
      // 写入 10000 条数据以创建多个 block
      let count = 10000u32;
      let mut writer = Writer::new(path.clone(), table_id, count as usize)
        .await?
        .block_size(4096);
      for i in 0..count {
        let key = format!("key{i:08}").into_bytes();
        let entry = Pos::infile(1, i as u64 * 100, i * 10);
        writer.add(&key, &entry).await?;
      }
      let meta = writer.finish().await?;
      assert_eq!(meta.item_count, count as u64);

      let info = TableInfo::load(&path, table_id).await?;
      let mut file_lru = FileLru::new(&sst_dir, 16);

      assert!(info.block_count() > 1, "should have multiple blocks");

      // Test bloom filter
      // 测试布隆过滤器
      for key in [b"key00000000".as_ref(), b"key00005000", b"key00009999"] {
        assert!(info.may_contain(key));
      }

      // Test PGM index point queries
      // 测试 PGM 索引点查询
      let cases = [
        (b"key00000000".as_ref(), Some(Pos::infile(1, 0, 0))),
        (b"key00005000", Some(Pos::infile(1, 500000, 50000))),
        (b"key99999999", None),
      ];
      for (key, expected) in cases {
        let pos = info.get_pos(key, &mut file_lru).await?;
        assert_eq!(pos, expected);
      }

      // Test range query across blocks
      // 测试跨 block 的范围查询
      let mut table = SSTable::new(info, &mut file_lru);
      let range_items = collect_stream(table.range(
        Bound::Included(b"key00004990".as_ref()),
        Bound::Included(b"key00005010".as_ref()),
      ))
      .await;
      assert_eq!(range_items.len(), 21);

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  /// Test common prefix optimization for PGM
  /// 测试 PGM 的共同前缀优化
  #[test]
  fn test_sstable_common_prefix() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_prefix_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;
      let table_id = 1u64;
      let path = id_path(&sst_dir, table_id);

      // All keys share common prefix "user:profile:"
      // 所有键共享前缀 "user:profile:"
      let count = 5000u32;
      let mut writer = Writer::new(path.clone(), table_id, count as usize)
        .await?
        .block_size(4096);
      for i in 0..count {
        let key = format!("user:profile:{i:08}").into_bytes();
        let entry = Pos::infile(1, i as u64 * 100, i * 10);
        writer.add(&key, &entry).await?;
      }
      writer.finish().await?;

      let info = TableInfo::load(&path, table_id).await?;
      let mut file_lru = FileLru::new(&sst_dir, 16);

      // Verify lookups with common prefix
      // 验证共同前缀下的查询
      let cases = [
        (b"user:profile:00000000".as_ref(), true),
        (b"user:profile:00002500", true),
        (b"user:profile:00004999", true),
        (b"user:profile:99999999", false),
      ];
      for (key, should_exist) in cases {
        let pos = info.get_pos(key, &mut file_lru).await?;
        assert_eq!(pos.is_some(), should_exist, "key: {key:?}");
      }

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  #[test]
  fn test_sstable_async_table_trait() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_table_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;
      let table_id = 1u64;
      let path = id_path(&sst_dir, table_id);

      let mut writer = Writer::new(path.clone(), table_id, 10).await?;
      for i in 0..10u8 {
        let key = format!("key{i:02}").into_bytes();
        let entry = Pos::infile(1, i as u64 * 100, i as u32 * 10);
        writer.add(&key, &entry).await?;
      }
      writer.finish().await?;

      let info = TableInfo::load(&path, table_id).await?;
      let mut file_lru = FileLru::new(&sst_dir, 16);
      let mut table = SSTable::new(info, &mut file_lru);

      // Test async get
      // 测试异步 get
      assert!(table.get(b"key00").await.is_some());
      assert!(table.get(b"key05").await.is_some());
      assert!(table.get(b"notexist").await.is_none());

      // Test range
      // 测试 range
      let items = collect_stream(table.range(
        Bound::Included(b"key03".as_ref()),
        Bound::Included(b"key07".as_ref()),
      ))
      .await;
      assert_eq!(items.len(), 5);
      assert_eq!(items[0].0.as_ref(), b"key03");
      assert_eq!(items[4].0.as_ref(), b"key07");

      // Test iter
      // 测试 iter
      let all = collect_stream(table.iter()).await;
      assert_eq!(all.len(), 10);

      // Test rev_iter
      // 测试反向迭代
      let rev = collect_stream(table.rev_iter()).await;
      assert_eq!(rev[0].0.as_ref(), b"key09");
      assert_eq!(rev[9].0.as_ref(), b"key00");

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  /// Test cross-block ordering for forward and reverse iteration
  /// 测试跨 block 正向和反向迭代的有序性
  #[test]
  fn test_cross_block_ordering() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_order_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;
      let table_id = 1u64;
      let path = id_path(&sst_dir, table_id);

      // Use small block size to force multiple blocks
      // 使用小块大小强制创建多个块
      let count = 1000u32;
      let mut writer = Writer::new(path.clone(), table_id, count as usize)
        .await?
        .block_size(512);
      for i in 0..count {
        let key = format!("k{i:06}").into_bytes();
        let entry = Pos::infile(1, i as u64, i);
        writer.add(&key, &entry).await?;
      }
      writer.finish().await?;

      let info = TableInfo::load(&path, table_id).await?;
      let mut file_lru = FileLru::new(&sst_dir, 16);

      assert!(
        info.block_count() > 5,
        "should have many blocks, got {}",
        info.block_count()
      );

      let mut table = SSTable::new(info, &mut file_lru);

      // Test forward iteration is strictly ascending
      // 测试正向迭代严格升序
      let fwd = collect_stream(table.iter()).await;
      assert_eq!(fwd.len(), count as usize);
      for i in 1..fwd.len() {
        assert!(
          fwd[i - 1].0.as_ref() < fwd[i].0.as_ref(),
          "forward not sorted at {i}: {:?} >= {:?}",
          fwd[i - 1].0,
          fwd[i].0
        );
      }

      // Test reverse iteration is strictly descending
      // 测试反向迭代严格降序
      let rev = collect_stream(table.rev_iter()).await;
      assert_eq!(rev.len(), count as usize);
      for i in 1..rev.len() {
        assert!(
          rev[i - 1].0.as_ref() > rev[i].0.as_ref(),
          "reverse not sorted at {i}: {:?} <= {:?}",
          rev[i - 1].0,
          rev[i].0
        );
      }

      // Test range across multiple blocks
      // 测试跨多个块的范围查询
      let range = collect_stream(table.range(
        Bound::Included(b"k000100".as_ref()),
        Bound::Included(b"k000900".as_ref()),
      ))
      .await;
      assert_eq!(range.len(), 801);
      for i in 1..range.len() {
        assert!(
          range[i - 1].0.as_ref() < range[i].0.as_ref(),
          "range not sorted at {i}"
        );
      }

      // Test reverse range across multiple blocks
      // 测试跨多个块的反向范围查询
      let rev_range = collect_stream(table.rev_range(
        Bound::Included(b"k000100".as_ref()),
        Bound::Included(b"k000900".as_ref()),
      ))
      .await;
      assert_eq!(rev_range.len(), 801);
      for i in 1..rev_range.len() {
        assert!(
          rev_range[i - 1].0.as_ref() > rev_range[i].0.as_ref(),
          "rev_range not sorted at {i}"
        );
      }

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }
}
