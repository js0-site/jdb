//! Checkpoint module tests / 检查点模块测试

use jdb_ckp::{Ckp, After};

fn run<F: std::future::Future>(f: F) -> F::Output {
  compio_runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn test_ckp_open_empty() {
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let (ckp, after) = Ckp::open(dir.path(), &[]).await.unwrap();
    assert!(after.is_none());
    assert!(ckp.wal_id_offset().await.is_none());
  });
}

#[test]
fn test_ckp_save_and_load() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = Ckp::open(dir.path(), &[]).await.unwrap().0;
      ckp.set(100, 200).await.unwrap();
    }

    {
      let (ckp, after) = Ckp::open(dir.path(), &[]).await.unwrap();
      let (wal_id, offset) = ckp.wal_id_offset().await.unwrap();
      assert_eq!(wal_id, 100);
      assert_eq!(offset, 200);
      if let Some(a) = after {
        assert_eq!(a.wal_id, 100);
        assert_eq!(a.offset, 200);
      }
    }
  });
}

#[test]
fn test_ckp_multiple_saves() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = Ckp::open(dir.path(), &[]).await.unwrap().0;
      for i in 0..10u64 {
        ckp.set(i, i * 100).await.unwrap();
      }
    }

    {
      let (ckp, after) = Ckp::open(dir.path(), &[]).await.unwrap();
      let (wal_id, offset) = ckp.wal_id_offset().await.unwrap();
      assert_eq!(wal_id, 9);
      assert_eq!(offset, 900);
      if let Some(a) = after {
        assert_eq!(a.wal_id, 9);
        assert_eq!(a.offset, 900);
      }
    }
  });
}

#[test]
fn test_ckp_rotate() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = Ckp::open(dir.path(), &[]).await.unwrap().0;
      ckp.set(1, 100).await.unwrap();
      ckp.rotate(2).await.unwrap();
      ckp.rotate(3).await.unwrap();
    }

    {
      let (_ckp, after) = Ckp::open(dir.path(), &[]).await.unwrap();
      let a = after.unwrap();
      assert_eq!(a.wal_id, 1);
      assert_eq!(a.offset, 100);
      assert_eq!(a.rotate_wal_id_li, vec![2, 3]);
    }
  });
}

#[test]
fn test_ckp_save_clears_rotates() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = Ckp::open(dir.path(), &[]).await.unwrap().0;
      ckp.set(1, 100).await.unwrap();
      ckp.rotate(2).await.unwrap();
      ckp.rotate(3).await.unwrap();
      // New save should clear previous rotates
      ckp.set(3, 500).await.unwrap();
      ckp.rotate(4).await.unwrap();
    }

    {
      let (_ckp, after) = Ckp::open(dir.path(), &[]).await.unwrap();
      let a = after.unwrap();
      assert_eq!(a.wal_id, 3);
      assert_eq!(a.offset, 500);
      assert_eq!(a.rotate_wal_id_li, vec![4]);
    }
  });
}

#[test]
fn test_ckp_last_save_id() {
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut ckp = Ckp::open(dir.path(), &[]).await.unwrap().0;

    assert!(ckp.wal_id_offset().await.is_none());

    ckp.set(42, 1000).await.unwrap();
    assert_eq!(ckp.wal_id_offset().await, Some((42, 1000)));
  });
}

#[test]
fn test_after() {
  let after = After {
    wal_id: 123,
    offset: 456,
    rotate_wal_id_li: vec![1, 2, 3],
  };

  assert_eq!(after.wal_id, 123);
  assert_eq!(after.offset, 456);
  assert_eq!(after.rotate_wal_id_li, vec![1, 2, 3]);
}