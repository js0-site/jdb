//! Checkpoint module tests / 检查点模块测试

use jdb_val::{Ckp, WalPtr};

fn run<F: std::future::Future>(f: F) -> F::Output {
  compio_runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn test_ckp_open_empty() {
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let ckp = Ckp::open(dir.path()).await.unwrap();
    assert!(ckp.last_save().is_none());
  });
}

#[test]
fn test_ckp_save_and_load() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = Ckp::open(dir.path()).await.unwrap();
      ckp.save(100, 200).await.unwrap();
    }

    {
      let ckp = Ckp::open(dir.path()).await.unwrap();
      let ptr = ckp.last_save().unwrap();
      assert_eq!(ptr.id, 100);
      assert_eq!(ptr.offset, 200);
    }
  });
}

#[test]
fn test_ckp_multiple_saves() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = Ckp::open(dir.path()).await.unwrap();
      for i in 0..10u64 {
        ckp.save(i, i * 100).await.unwrap();
      }
    }

    {
      let ckp = Ckp::open(dir.path()).await.unwrap();
      let ptr = ckp.last_save().unwrap();
      assert_eq!(ptr.id, 9);
      assert_eq!(ptr.offset, 900);
    }
  });
}

#[test]
fn test_ckp_rotate() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = Ckp::open(dir.path()).await.unwrap();
      ckp.save(1, 100).await.unwrap();
      ckp.rotate(2).await.unwrap();
      ckp.rotate(3).await.unwrap();
    }

    {
      let ckp = Ckp::open(dir.path()).await.unwrap();
      let (ptr, file_ids) = ckp.load_replay().await.unwrap().unwrap();
      assert_eq!(ptr.id, 1);
      assert_eq!(ptr.offset, 100);
      assert_eq!(file_ids, vec![1, 2, 3]);
    }
  });
}

#[test]
fn test_ckp_save_clears_rotates() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = Ckp::open(dir.path()).await.unwrap();
      ckp.save(1, 100).await.unwrap();
      ckp.rotate(2).await.unwrap();
      ckp.rotate(3).await.unwrap();
      // New save should clear previous rotates
      ckp.save(3, 500).await.unwrap();
      ckp.rotate(4).await.unwrap();
    }

    {
      let ckp = Ckp::open(dir.path()).await.unwrap();
      let (ptr, file_ids) = ckp.load_replay().await.unwrap().unwrap();
      assert_eq!(ptr.id, 3);
      assert_eq!(ptr.offset, 500);
      assert_eq!(file_ids, vec![3, 4]);
    }
  });
}

#[test]
fn test_ckp_last_save_id() {
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut ckp = Ckp::open(dir.path()).await.unwrap();

    assert!(ckp.last_save_id().is_none());

    ckp.save(42, 1000).await.unwrap();
    assert_eq!(ckp.last_save_id(), Some(42));
  });
}

#[test]
fn test_walptr() {
  let ptr = WalPtr::new(123, 456);
  assert_eq!(ptr.id, 123);
  assert_eq!(ptr.offset, 456);

  let default_ptr = WalPtr::default();
  assert_eq!(default_ptr.id, 0);
  assert_eq!(default_ptr.offset, 0);
}
