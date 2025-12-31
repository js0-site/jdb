//! Checkpoint module tests / 检查点模块测试

use std::path::Path;

use jdb_val::Checkpoint;

fn run<F: std::future::Future>(f: F) -> F::Output {
  compio_runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn test_checkpoint_new() {
  let cp = Checkpoint::new(123, 456);
  assert_eq!(cp.wal_id, 123);
  assert_eq!(cp.wal_pos, 456);
  assert!(cp.is_valid());
}

#[test]
fn test_checkpoint_size() {
  assert_eq!(Checkpoint::SIZE, 32);
  assert_eq!(std::mem::size_of::<Checkpoint>(), 32);
}

#[test]
fn test_checkpoint_save_load() {
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint");

    let cp = Checkpoint::new(100, 200);
    cp.save(&path).await.unwrap();

    let loaded = Checkpoint::load(&path).await.unwrap().unwrap();
    assert_eq!(loaded.wal_id, 100);
    assert_eq!(loaded.wal_pos, 200);
    assert!(loaded.is_valid());
  });
}

#[test]
fn test_checkpoint_load_nonexistent() {
  run(async {
    let path = Path::new("/nonexistent/checkpoint");
    let result = Checkpoint::load(path).await.unwrap();
    assert!(result.is_none());
  });
}

#[test]
fn test_checkpoint_load_corrupt() {
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint");

    // Write invalid data / 写入无效数据
    std::fs::write(&path, b"invalid checkpoint data!!!!!!!!!!").unwrap();

    let result = Checkpoint::load(&path).await;
    assert!(result.is_err());
  });
}

#[test]
fn test_checkpoint_load_too_small() {
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint");

    // Write too small data / 写入过小数据
    std::fs::write(&path, b"small").unwrap();

    let result = Checkpoint::load(&path).await;
    assert!(result.is_err());
  });
}
