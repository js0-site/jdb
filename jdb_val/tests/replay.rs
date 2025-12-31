//! WAL replay tests / WAL 回放测试

use std::sync::Once;

use jdb_val::{Checkpoint, Conf, Wal};

static INIT: Once = Once::new();

fn init_log() {
  INIT.call_once(|| {
    log_init::init();
  });
}

fn run<F: std::future::Future>(f: F) -> F::Output {
  compio_runtime::Runtime::new().unwrap().block_on(f)
}

fn ckpt_path(dir: &std::path::Path) -> std::path::PathBuf {
  dir.join("checkpoint")
}

#[test]
fn test_replay_empty() {
  init_log();
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    wal.open().await.unwrap();

    let mut iter = wal.open_replay(None).await.unwrap();
    assert!(iter.next().await.unwrap().is_none());
  });
}

#[test]
fn test_replay_single_entry() {
  init_log();
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    wal.open().await.unwrap();

    let key = b"test_key";
    let val = b"test_value";
    let pos = wal.put(key, val).await.unwrap();
    wal.flush().await.unwrap();

    // Replay
    let mut iter = wal.open_replay(None).await.unwrap();
    let (k, p) = iter.next().await.unwrap().unwrap();
    assert_eq!(k, key);
    assert_eq!(p.id(), pos.id());
    assert_eq!(p.offset(), pos.offset());
    assert_eq!(p.len(), pos.len());

    assert!(iter.next().await.unwrap().is_none());
  });
}

#[test]
fn test_replay_multiple_entries() {
  init_log();
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    wal.open().await.unwrap();

    // Write multiple entries
    let entries: Vec<_> = (0..10)
      .map(|i| (format!("key_{i}"), format!("value_{i}")))
      .collect();

    for (k, v) in &entries {
      wal.put(k.as_bytes(), v.as_bytes()).await.unwrap();
    }
    wal.flush().await.unwrap();

    // Replay and verify order
    let mut iter = wal.open_replay(None).await.unwrap();
    for (k, _v) in &entries {
      let (key, _pos) = iter.next().await.unwrap().unwrap();
      assert_eq!(key, k.as_bytes());
    }
    assert!(iter.next().await.unwrap().is_none());
  });
}

#[test]
fn test_replay_with_checkpoint() {
  init_log();
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    wal.open().await.unwrap();

    // Write entries before checkpoint
    for i in 0..5 {
      wal
        .put(format!("before_{i}").as_bytes(), b"val")
        .await
        .unwrap();
    }
    wal.flush().await.unwrap();

    // Save checkpoint
    let cp = wal.checkpoint();
    wal.save_checkpoint(&ckpt_path(dir.path())).await.unwrap();

    // Write entries after checkpoint
    for i in 0..5 {
      wal
        .put(format!("after_{i}").as_bytes(), b"val")
        .await
        .unwrap();
    }
    wal.flush().await.unwrap();

    // Replay from checkpoint - should only get entries after checkpoint
    let mut iter = wal.open_replay(Some(&cp)).await.unwrap();
    for i in 0..5 {
      let (key, _pos) = iter.next().await.unwrap().unwrap();
      assert_eq!(key, format!("after_{i}").as_bytes());
    }
    assert!(iter.next().await.unwrap().is_none());
  });
}

#[test]
fn test_replay_tombstone() {
  init_log();
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    wal.open().await.unwrap();

    let key = b"delete_me";
    wal.put(key, b"value").await.unwrap();
    let del_pos = wal.del(key).await.unwrap();
    wal.flush().await.unwrap();

    // Replay should include tombstone
    let mut iter = wal.open_replay(None).await.unwrap();

    // First: put
    let (k, pos) = iter.next().await.unwrap().unwrap();
    assert_eq!(k, key);
    assert!(!pos.is_tombstone());

    // Second: delete (tombstone)
    let (k, pos) = iter.next().await.unwrap().unwrap();
    assert_eq!(k, key);
    assert!(pos.is_tombstone());
    assert_eq!(pos.id(), del_pos.id());

    assert!(iter.next().await.unwrap().is_none());
  });
}

#[test]
fn test_replay_across_files() {
  init_log();
  run(async {
    let dir = tempfile::tempdir().unwrap();
    // Small max size to force multiple files
    let mut wal = Wal::new(dir.path(), &[Conf::MaxSize(4096)]);
    wal.open().await.unwrap();

    // Write enough data to span multiple files
    let mut keys = Vec::new();
    for i in 0..100 {
      let key = format!("key_{i:04}");
      let val = vec![b'x'; 100];
      wal.put(key.as_bytes(), &val).await.unwrap();
      keys.push(key);
    }
    wal.flush().await.unwrap();

    // Replay all entries
    let mut iter = wal.open_replay(None).await.unwrap();
    for key in &keys {
      let (k, _pos) = iter.next().await.unwrap().unwrap();
      assert_eq!(k, key.as_bytes());
    }
    assert!(iter.next().await.unwrap().is_none());
  });
}

#[test]
fn test_checkpoint_persistence() {
  init_log();
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let ckpt = ckpt_path(dir.path());

    // First session: write and checkpoint
    {
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();

      for i in 0..5 {
        wal
          .put(format!("key_{i}").as_bytes(), b"val")
          .await
          .unwrap();
      }
      wal.flush().await.unwrap();

      wal.save_checkpoint(&ckpt).await.unwrap();
    }

    // Second session: load checkpoint and continue
    {
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();

      let cp = Checkpoint::load(&ckpt).await.unwrap();
      assert!(cp.is_some());
      let cp = cp.unwrap();

      // Write more after checkpoint
      for i in 5..10 {
        wal
          .put(format!("key_{i}").as_bytes(), b"val")
          .await
          .unwrap();
      }
      wal.flush().await.unwrap();

      // Replay from checkpoint
      let mut iter = wal.open_replay(Some(&cp)).await.unwrap();
      for i in 5..10 {
        let (key, _pos) = iter.next().await.unwrap().unwrap();
        assert_eq!(key, format!("key_{i}").as_bytes());
      }
      assert!(iter.next().await.unwrap().is_none());
    }
  });
}
