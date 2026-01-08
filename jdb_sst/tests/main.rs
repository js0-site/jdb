#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[allow(clippy::await_holding_refcell_ref)]
mod sst_tests {
  use std::{cell::RefCell, ops::Bound, rc::Rc};

  use aok::{OK, Void};
  use futures::StreamExt;
  use jdb_base::{Kv, Pos, SsTable};
  use jdb_ckp::Ckp;
  use jdb_fs::FileLru;
  use jdb_mem::MemInner;
  use jdb_sst::{Conf, Read, Table, write};

  /// Create MemInner with data from closure
  /// 用闭包创建带数据的 MemInner
  fn make_mem<F: FnOnce(&mut MemInner)>(f: F) -> Rc<MemInner> {
    let mut inner = MemInner::default();
    f(&mut inner);
    Rc::new(inner)
  }

  /// Create test Ckp for SSTable tests
  /// 为 SSTable 测试创建测试用 Ckp
  async fn test_ckp(dir: &std::path::Path) -> Rc<RefCell<Ckp>> {
    let (ckp, _) = jdb_ckp::open(dir, &[]).await.unwrap();
    Rc::new(RefCell::new(ckp))
  }

  /// Load Read manager for tests with SST id registered
  /// 为测试加载 Read 管理器，并注册 SST id
  async fn test_read_with_sst(dir: &std::path::Path, sst_id: u64) -> Read {
    let ckp = test_ckp(dir).await;
    ckp.borrow_mut().sst_add(sst_id, 0).await.unwrap();
    Read::load(dir, 16, ckp).await.unwrap()
  }

  /// Collect stream to vec
  /// 收集流到 vec
  async fn collect_stream<S: futures::Stream<Item = Kv>>(stream: S) -> Vec<Kv> {
    std::pin::pin!(stream).collect().await
  }

  /// Get first table from L0
  /// 从 L0 获取第一个表
  fn get_l0_table(mgr: &Read) -> &Table {
    let levels = mgr.levels();
    let l0 = levels.levels.first().expect("L0 should exist");
    let meta = l0.get(0).expect("should have table");
    mgr.get_handle(meta.id).expect("handle exists").table()
  }

  #[test]
  fn test_sst_write_read_roundtrip() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let mem = make_mem(|m| {
        m.put(b"aaa".to_vec(), Pos::infile(1, 1, 100, 10));
        m.put(b"bbb".to_vec(), Pos::infile(2, 1, 200, 20));
        m.put(b"ccc".to_vec(), Pos::tombstone(3, 0, 0));
        m.put(b"ddd".to_vec(), Pos::infile(4, 1, 300, 30));
        m.put(b"eee".to_vec(), Pos::infile(5, 1, 400, 40));
      });

      let meta = write(&sst_dir, 0, mem.data.iter(), &[]).await?;
      assert_eq!(meta.item_count, 5);
      assert_eq!(meta.min_key.as_ref(), b"aaa");
      assert_eq!(meta.max_key.as_ref(), b"eee");

      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = Table::load(&path, meta.id).await?;
      let mut file_lru = FileLru::new(&sst_dir, 16);
      let entry = info
        .get_pos(b"aaa", &mut file_lru)
        .await?
        .expect("should find");
      assert_eq!(entry, Pos::infile(1, 1, 100, 10));
      let entry = info
        .get_pos(b"ccc", &mut file_lru)
        .await?
        .expect("tombstone should be found");
      assert!(entry.is_tombstone());

      let mgr = test_read_with_sst(&sst_dir, meta.id).await;
      let tbl = get_l0_table(&mgr);
      let items: Vec<Kv> =
        collect_stream(mgr.range_table(tbl, Bound::Unbounded, Bound::Unbounded)).await;
      assert_eq!(items.len(), 5);
      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  #[test]
  fn test_sst_range_query() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_range_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let mem = make_mem(|m| {
        for i in 0..10u8 {
          let key = format!("key{i:02}").into_bytes();
          let entry = Pos::infile(i as u64 + 1, 1, i as u64 * 100, i as u32 * 10);
          m.put(key, entry);
        }
      });

      let meta = write(&sst_dir, 0, mem.data.iter(), &[]).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = Table::load(&path, meta.id).await?;
      let mut mgr = test_read_with_sst(&sst_dir, meta.id).await;
      mgr.add_table(info);
      let tbl = get_l0_table(&mgr);
      let range_items: Vec<Kv> = collect_stream(mgr.range_table(
        tbl,
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
  fn test_sst_empty() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_empty_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let mem = make_mem(|_| {});
      let meta = write(&sst_dir, 0, mem.data.iter(), &[]).await?;
      assert_eq!(meta.item_count, 0);
      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  #[test]
  fn test_sst_pgm_and_filter() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_pgm_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let count = 10000usize;
      let conf = [Conf::BlockSize(4096)];
      let mem = make_mem(|m| {
        for i in 0..count as u32 {
          let key = format!("key{i:08}").into_bytes();
          let entry = Pos::infile(i as u64 + 1, 1, i as u64 * 100, i * 10);
          m.put(key, entry);
        }
      });

      let meta = write(&sst_dir, 0, mem.data.iter(), &conf).await?;
      assert_eq!(meta.item_count, count as u64);

      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = Table::load(&path, meta.id).await?;
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

      let mut mgr = test_read_with_sst(&sst_dir, meta.id).await;
      mgr.add_table(info);
      let tbl = get_l0_table(&mgr);
      let range_items: Vec<Kv> = collect_stream(mgr.range_table(
        tbl,
        Bound::Included(b"key00004990".as_ref()),
        Bound::Included(b"key00005010".as_ref()),
      ))
      .await;
      assert_eq!(range_items.len(), 21);

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  #[test]
  fn test_sst_common_prefix() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_prefix_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let count = 5000usize;
      let conf = [Conf::BlockSize(4096)];
      let mem = make_mem(|m| {
        for i in 0..count as u32 {
          let key = format!("user:profile:{i:08}").into_bytes();
          let entry = Pos::infile(i as u64 + 1, 1, i as u64 * 100, i * 10);
          m.put(key, entry);
        }
      });

      let meta = write(&sst_dir, 0, mem.data.iter(), &conf).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = Table::load(&path, meta.id).await?;
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

      let mem = make_mem(|m| {
        for i in 0..10u8 {
          let key = format!("key{i:02}").into_bytes();
          let entry = Pos::infile(i as u64 + 1, 1, i as u64 * 100, i as u32 * 10);
          m.put(key, entry);
        }
      });

      let meta = write(&sst_dir, 0, mem.data.iter(), &[]).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = Table::load(&path, meta.id).await?;
      let mut mgr = test_read_with_sst(&sst_dir, meta.id).await;
      mgr.add_table(info);

      assert!(SsTable::get(&mut mgr, b"key00").await.is_some());
      assert!(SsTable::get(&mut mgr, b"key05").await.is_some());
      assert!(SsTable::get(&mut mgr, b"notexist").await.is_none());

      let tbl = get_l0_table(&mgr);
      let items: Vec<Kv> = collect_stream(mgr.range_table(
        tbl,
        Bound::Included(b"key03".as_ref()),
        Bound::Included(b"key07".as_ref()),
      ))
      .await;
      assert_eq!(items.len(), 5);
      assert_eq!(items[0].0.as_ref(), b"key03");
      assert_eq!(items[4].0.as_ref(), b"key07");

      let tbl = get_l0_table(&mgr);
      let all: Vec<Kv> =
        collect_stream(mgr.range_table(tbl, Bound::Unbounded, Bound::Unbounded)).await;
      assert_eq!(all.len(), 10);

      let tbl = get_l0_table(&mgr);
      let rev: Vec<Kv> =
        collect_stream(mgr.rev_range_table(tbl, Bound::Unbounded, Bound::Unbounded)).await;
      assert_eq!(rev[0].0.as_ref(), b"key09");
      assert_eq!(rev[9].0.as_ref(), b"key00");

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  #[test]
  fn test_cross_block_ordering() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_sst_order_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let count = 1000usize;
      let conf = [Conf::BlockSize(512)];
      let mem = make_mem(|m| {
        for i in 0..count as u32 {
          let key = format!("k{i:06}").into_bytes();
          let entry = Pos::infile(i as u64 + 1, 1, i as u64, i);
          m.put(key, entry);
        }
      });

      let meta = write(&sst_dir, 0, mem.data.iter(), &conf).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = Table::load(&path, meta.id).await?;

      assert!(
        info.block_count() > 5,
        "should have many blocks, got {}",
        info.block_count()
      );

      let mgr = test_read_with_sst(&sst_dir, meta.id).await;

      let tbl = get_l0_table(&mgr);
      let fwd: Vec<Kv> =
        collect_stream(mgr.range_table(tbl, Bound::Unbounded, Bound::Unbounded)).await;
      assert_eq!(fwd.len(), count);
      for i in 1..fwd.len() {
        assert!(
          fwd[i - 1].0.as_ref() < fwd[i].0.as_ref(),
          "forward not sorted at {i}: {:?} >= {:?}",
          fwd[i - 1].0,
          fwd[i].0
        );
      }

      let tbl = get_l0_table(&mgr);
      let rev: Vec<Kv> =
        collect_stream(mgr.rev_range_table(tbl, Bound::Unbounded, Bound::Unbounded)).await;
      assert_eq!(rev.len(), count);
      for i in 1..rev.len() {
        assert!(
          rev[i - 1].0.as_ref() > rev[i].0.as_ref(),
          "reverse not sorted at {i}: {:?} <= {:?}",
          rev[i - 1].0,
          rev[i].0
        );
      }

      let tbl = get_l0_table(&mgr);
      let range: Vec<Kv> = collect_stream(mgr.range_table(
        tbl,
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

      let tbl = get_l0_table(&mgr);
      let rev_range: Vec<Kv> = collect_stream(mgr.rev_range_table(
        tbl,
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
