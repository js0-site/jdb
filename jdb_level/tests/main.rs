#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

mod stream_tests {
  use std::{cell::RefCell, ops::Bound, rc::Rc};

  use aok::{OK, Void};
  use futures::StreamExt;
  use jdb_base::{Flag, Kv, Pos};
  use jdb_fs::FileLru;
  use jdb_level::{new_asc, new_desc};
  use jdb_mem::MemInner;
  use jdb_sst::{Conf, Table, write};

  fn make_mem<F: FnOnce(&mut MemInner)>(f: F) -> Rc<MemInner> {
    let mut inner = MemInner::default();
    f(&mut inner);
    Rc::new(inner)
  }

  async fn collect_stream<S: futures::Stream<Item = Kv>>(stream: S) -> Vec<Kv> {
    std::pin::pin!(stream).collect().await
  }

  #[test]
  fn test_multi_asc_single_table() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_multi_asc_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let mem = make_mem(|m: &mut MemInner| {
        m.put(b"aaa".to_vec(), Pos::new(1, Flag::INFILE, 1, 100, 10));
        m.put(b"bbb".to_vec(), Pos::new(2, Flag::INFILE, 1, 200, 20));
        m.put(b"ccc".to_vec(), Pos::new(3, Flag::INFILE, 1, 300, 30));
      });

      let meta = write(&sst_dir, 0, mem.data.iter(), &[]).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = Table::load(&path, meta.id).await?;

      let lru = Rc::new(RefCell::new(FileLru::new(&sst_dir, 16)));
      let items: Vec<Kv> =
        collect_stream(new_asc(&[info], lru, Bound::Unbounded, Bound::Unbounded)).await;

      assert_eq!(items.len(), 3);
      assert_eq!(items[0].0.as_ref(), b"aaa");
      assert_eq!(items[2].0.as_ref(), b"ccc");

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  #[test]
  fn test_range_query() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_range_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let mem = make_mem(|m: &mut MemInner| {
        for i in 0..10u8 {
          let key = format!("key{i:02}").into_bytes();
          let entry = Pos::new(i as u64 + 1, Flag::INFILE, 1, i as u64 * 100, i as u32 * 10);
          m.put(key, entry);
        }
      });

      let meta = write(&sst_dir, 0, mem.data.iter(), &[]).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = Table::load(&path, meta.id).await?;

      let lru = Rc::new(RefCell::new(FileLru::new(&sst_dir, 16)));
      let items: Vec<Kv> = collect_stream(new_asc(
        &[info],
        lru,
        Bound::Included(b"key03".as_ref()),
        Bound::Included(b"key07".as_ref()),
      ))
      .await;

      assert_eq!(items.len(), 5);
      assert_eq!(items[0].0.as_ref(), b"key03");
      assert_eq!(items[4].0.as_ref(), b"key07");

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }

  #[test]
  fn test_cross_block_ordering() -> Void {
    compio::runtime::Runtime::new()?.block_on(async {
      let tmp_dir = std::env::temp_dir();
      let test_id = fastrand::u64(..);
      let sst_dir = tmp_dir.join(format!("test_order_{test_id}"));
      std::fs::create_dir_all(&sst_dir)?;

      let count = 1000usize;
      let conf = [Conf::BlockSize(512)];
      let mem = make_mem(|m: &mut MemInner| {
        for i in 0..count as u32 {
          let key = format!("k{i:06}").into_bytes();
          let entry = Pos::new(i as u64 + 1, Flag::INFILE, 1, i as u64, i);
          m.put(key, entry);
        }
      });

      let meta = write(&sst_dir, 0, mem.data.iter(), &conf).await?;
      let path = jdb_fs::fs_id::id_path(&sst_dir, meta.id);
      let info = Table::load(&path, meta.id).await?;

      assert!(info.block_count() > 5, "should have many blocks");

      let lru = Rc::new(RefCell::new(FileLru::new(&sst_dir, 16)));

      // Test ascending
      // 测试升序
      let fwd: Vec<Kv> = collect_stream(new_asc(
        &[info],
        Rc::clone(&lru),
        Bound::Unbounded,
        Bound::Unbounded,
      ))
      .await;
      assert_eq!(fwd.len(), count);
      for i in 1..fwd.len() {
        assert!(
          fwd[i - 1].0.as_ref() < fwd[i].0.as_ref(),
          "asc not sorted at {i}"
        );
      }

      // Reload for desc test
      // 重新加载用于降序测试
      let info = Table::load(&path, meta.id).await?;
      let rev: Vec<Kv> = collect_stream(new_desc(
        &[info],
        Rc::clone(&lru),
        Bound::Unbounded,
        Bound::Unbounded,
      ))
      .await;
      assert_eq!(rev.len(), count);
      for i in 1..rev.len() {
        assert!(
          rev[i - 1].0.as_ref() > rev[i].0.as_ref(),
          "desc not sorted at {i}"
        );
      }

      // Test range ascending
      // 测试范围升序
      let info = Table::load(&path, meta.id).await?;
      let range: Vec<Kv> = collect_stream(new_asc(
        &[info],
        Rc::clone(&lru),
        Bound::Included(b"k000100".as_ref()),
        Bound::Included(b"k000900".as_ref()),
      ))
      .await;
      assert_eq!(range.len(), 801);

      // Test range descending
      // 测试范围降序
      let info = Table::load(&path, meta.id).await?;
      let rev_range: Vec<Kv> = collect_stream(new_desc(
        &[info],
        Rc::clone(&lru),
        Bound::Included(b"k000100".as_ref()),
        Bound::Included(b"k000900".as_ref()),
      ))
      .await;
      assert_eq!(rev_range.len(), 801);

      let _ = std::fs::remove_dir_all(&sst_dir);
      OK
    })
  }
}
