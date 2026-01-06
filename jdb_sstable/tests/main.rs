#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

mod sstable_tests {
  use std::ops::Bound;

  use aok::{OK, Void};
  use futures::StreamExt;
  use jdb_base::{
    Pos,
    table::{Kv, SsTable, TableMut},
  };
  use jdb_fs::FileLru;
  use jdb_mem::Mem;
  use jdb_sstable::{Conf, Read, TableInfo, new};

  /// Collect stream to vec
  /// 收集流到 vec
  async fn collect_stream<S: futures::Stream<Item = Kv>>(stream: S) -> Vec<Kv> {
    std::pin::pin!(stream).collect().await
  }

  #[test]
  fn test_sstable_write_read_roundtrip() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let mut mem = Mem::new();
      mem.put(b"aaa".to_vec(), Pos::infile(1, 1, 100, 10));
      mem.put(b"bbb".to_vec(), Pos::infile(2, 1, 200, 20));
      mem.put(b"ccc".to_vec(), Pos::tombstone(3, 0, 0));
      mem.put(b"ddd".to_vec(), Pos::infile(4, 1, 300, 30));
      mem.put(b"eee".to_vec(), Pos::infile(5, 1, 400, 40));

      let meta = new(&sst_dir, &mem, &[]).await?;
      assert_eq!(meta.item_count, 5);
      assert_eq!(meta.min_key.as_ref(), b"aaa");
      assert_eq!(meta.max_key.as_ref(), b"eee");
      assert_eq!(meta.id, 5); // max ver

      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = TableInfo::load(&path, meta.id).await?;
      let mut file_lru = FileLru::new(&sst_dir, 16);
      let entry = info
        .get_pos(b"aaa", &mut file_lru)
        .await?
        .expect("should find");
      assert_eq!(entry, Pos::infile(1, 1, 100, 10));
      // Tombstone should be found
      // Tombstone 应该被找到
      let entry = info
        .get_pos(b"ccc", &mut file_lru)
        .await?
        .expect("tombstone should be found");
      assert!(entry.is_tombstone());

      let mut mgr = Read::new(&sst_dir, 16);
      mgr.add(info);
      let items = collect_stream(mgr.iter_table(0)).await;
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

      let mut mem = Mem::new();
      for i in 0..10u8 {
        let key = format!("key{i:02}").into_bytes();
        let entry = Pos::infile(i as u64 + 1, 1, i as u64 * 100, i as u32 * 10);
        mem.put(key, entry);
      }

      let meta = new(&sst_dir, &mem, &[]).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = TableInfo::load(&path, meta.id).await?;
      let mut mgr = Read::new(&sst_dir, 16);
      mgr.add(info);
      let range_items = collect_stream(mgr.range_table(
        0,
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
      let sst_dir = tmp_dir.join(format!("test_sstable_empty_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let mem = Mem::new();
      let meta = new(&sst_dir, &mem, &[]).await?;
      assert_eq!(meta.item_count, 0);
      let _ = std::fs::remove_dir_all(&sst_dir);
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

      let count = 10000usize;
      let conf = [Conf::BlockSize(4096)];
      let mut mem = Mem::new();
      for i in 0..count as u32 {
        let key = format!("key{i:08}").into_bytes();
        let entry = Pos::infile(i as u64 + 1, 1, i as u64 * 100, i * 10);
        mem.put(key, entry);
      }

      let meta = new(&sst_dir, &mem, &conf).await?;
      assert_eq!(meta.item_count, count as u64);

      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = TableInfo::load(&path, meta.id).await?;
      let mut file_lru = FileLru::new(&sst_dir, 16);

      assert!(info.block_count() > 1, "should have multiple blocks");

      for key in [b"key00000000".as_ref(), b"key00005000", b"key00009999"] {
        assert!(info.may_contain(key));
      }

      let cases = [
        (b"key00000000".as_ref(), Some(Pos::infile(1, 1, 0, 0))),
        (b"key00005000", Some(Pos::infile(5001, 1, 500000, 50000))),
        (b"key99999999", None),
      ];
      for (key, expected) in cases {
        let pos = info.get_pos(key, &mut file_lru).await?;
        assert_eq!(pos, expected);
      }

      let mut mgr = Read::new(&sst_dir, 16);
      mgr.add(info);
      let range_items = collect_stream(mgr.range_table(
        0,
        Bound::Included(b"key00004990".as_ref()),
        Bound::Included(b"key00005010".as_ref()),
      ))
      .await;
      assert_eq!(range_items.len(), 21);

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  /// Test common prefix optimization
  /// 测试共同前缀优化
  #[test]
  fn test_sstable_common_prefix() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_prefix_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let count = 5000usize;
      let conf = [Conf::BlockSize(4096)];
      let mut mem = Mem::new();
      for i in 0..count as u32 {
        let key = format!("user:profile:{i:08}").into_bytes();
        let entry = Pos::infile(i as u64 + 1, 1, i as u64 * 100, i * 10);
        mem.put(key, entry);
      }

      let meta = new(&sst_dir, &mem, &conf).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = TableInfo::load(&path, meta.id).await?;
      let mut file_lru = FileLru::new(&sst_dir, 16);

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
  fn test_table_api() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_table_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let mut mem = Mem::new();
      for i in 0..10u8 {
        let key = format!("key{i:02}").into_bytes();
        let entry = Pos::infile(i as u64 + 1, 1, i as u64 * 100, i as u32 * 10);
        mem.put(key, entry);
      }

      let meta = new(&sst_dir, &mem, &[]).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = TableInfo::load(&path, meta.id).await?;
      let mut mgr = Read::new(&sst_dir, 16);
      mgr.add(info);

      assert!(mgr.get(b"key00").await.is_some());
      assert!(mgr.get(b"key05").await.is_some());
      assert!(mgr.get(b"notexist").await.is_none());

      let items = collect_stream(mgr.range_table(
        0,
        Bound::Included(b"key03".as_ref()),
        Bound::Included(b"key07".as_ref()),
      ))
      .await;
      assert_eq!(items.len(), 5);
      assert_eq!(items[0].0.as_ref(), b"key03");
      assert_eq!(items[4].0.as_ref(), b"key07");

      let all = collect_stream(mgr.iter_table(0)).await;
      assert_eq!(all.len(), 10);

      let rev = collect_stream(mgr.rev_iter_table(0)).await;
      assert_eq!(rev[0].0.as_ref(), b"key09");
      assert_eq!(rev[9].0.as_ref(), b"key00");

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  /// Test cross-block ordering
  /// 测试跨 block 有序性
  #[test]
  fn test_cross_block_ordering() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_order_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let count = 1000usize;
      let conf = [Conf::BlockSize(512)];
      let mut mem = Mem::new();
      for i in 0..count as u32 {
        let key = format!("k{i:06}").into_bytes();
        let entry = Pos::infile(i as u64 + 1, 1, i as u64, i);
        mem.put(key, entry);
      }

      let meta = new(&sst_dir, &mem, &conf).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = TableInfo::load(&path, meta.id).await?;

      assert!(
        info.block_count() > 5,
        "should have many blocks, got {}",
        info.block_count()
      );

      let mut mgr = Read::new(&sst_dir, 16);
      mgr.add(info);

      let fwd = collect_stream(mgr.iter_table(0)).await;
      assert_eq!(fwd.len(), count);
      for i in 1..fwd.len() {
        assert!(
          fwd[i - 1].0.as_ref() < fwd[i].0.as_ref(),
          "forward not sorted at {i}: {:?} >= {:?}",
          fwd[i - 1].0,
          fwd[i].0
        );
      }

      let rev = collect_stream(mgr.rev_iter_table(0)).await;
      assert_eq!(rev.len(), count);
      for i in 1..rev.len() {
        assert!(
          rev[i - 1].0.as_ref() > rev[i].0.as_ref(),
          "reverse not sorted at {i}: {:?} <= {:?}",
          rev[i - 1].0,
          rev[i].0
        );
      }

      let range = collect_stream(mgr.range_table(
        0,
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

      let rev_range = collect_stream(mgr.rev_range_table(
        0,
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
