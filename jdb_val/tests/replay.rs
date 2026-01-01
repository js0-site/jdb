//! WAL replay tests / WAL 回放测试

use std::sync::Once;

use jdb_val::{Conf, Wal, WalPtr};

static INIT: Once = Once::new();

fn init_log() {
  INIT.call_once(|| {
    log_init::init();
  });
}

fn run<F: std::future::Future>(f: F) -> F::Output {
  compio_runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn test_replay_empty() {
  init_log();
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    let mut iter = wal.open().await.unwrap();
    assert!(iter.next().await.unwrap().is_none());
  });
}

#[test]
fn test_replay_single_entry() {
  init_log();
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    let mut iter = wal.open().await.unwrap();

    // No entries yet, replay should be empty
    assert!(iter.next().await.unwrap().is_none());

    let key = b"test_key";
    let val = b"test_value";
    let pos = wal.put(key, val).await.unwrap();
    wal.flush().await.unwrap();

    // Replay from beginning
    let mut iter = wal.replay(None);
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

    let entries: Vec<_> = (0..10)
      .map(|i| (format!("key_{i}"), format!("value_{i}")))
      .collect();

    for (k, v) in &entries {
      wal.put(k.as_bytes(), v.as_bytes()).await.unwrap();
    }
    wal.flush().await.unwrap();

    let mut iter = wal.replay(None);
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

    for i in 0..5 {
      wal
        .put(format!("before_{i}").as_bytes(), b"val")
        .await
        .unwrap();
    }
    wal.flush().await.unwrap();

    // Save checkpoint
    wal.save_ckp().await.unwrap();
    let save_ptr = WalPtr::new(wal.cur_id(), wal.cur_pos());

    for i in 0..5 {
      wal
        .put(format!("after_{i}").as_bytes(), b"val")
        .await
        .unwrap();
    }
    wal.flush().await.unwrap();

    // Replay from checkpoint should only get "after_*" entries
    let mut iter = wal.replay(Some(save_ptr));
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

    let mut iter = wal.replay(None);

    let (k, pos) = iter.next().await.unwrap().unwrap();
    assert_eq!(k, key);
    assert!(!pos.is_tombstone());

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
    let mut wal = Wal::new(dir.path(), &[Conf::MaxSize(4096)]);
    wal.open().await.unwrap();

    let mut keys = Vec::new();
    for i in 0..100 {
      let key = format!("key_{i:04}");
      let val = vec![b'x'; 100];
      wal.put(key.as_bytes(), &val).await.unwrap();
      keys.push(key);
    }
    wal.flush().await.unwrap();

    let mut iter = wal.replay(None);
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

      wal.save_ckp().await.unwrap();
    }

    {
      let mut wal = Wal::new(dir.path(), &[]);
      // open() returns replay iter from last checkpoint to WAL open position
      let mut iter = wal.open().await.unwrap();

      // Should be empty since checkpoint was at end of data
      assert!(iter.next().await.unwrap().is_none());

      // Write more data
      for i in 5..10 {
        wal
          .put(format!("key_{i}").as_bytes(), b"val")
          .await
          .unwrap();
      }
      wal.flush().await.unwrap();

      // replay(None) replays from beginning to current position
      // Should get all 10 entries (key_0 to key_9)
      let mut iter = wal.replay(None);
      for i in 0..10 {
        let (key, _pos) = iter.next().await.unwrap().unwrap();
        assert_eq!(key, format!("key_{i}").as_bytes());
      }
      assert!(iter.next().await.unwrap().is_none());
    }
  });
}

#[test]
fn test_replay_same_start_end() {
  init_log();
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    wal.open().await.unwrap();

    // Write some data
    wal.put(b"key", b"value").await.unwrap();
    wal.flush().await.unwrap();

    // Get current position
    let ptr = WalPtr::new(wal.cur_id(), wal.cur_pos());

    // Replay from current position should be empty
    let mut iter = wal.replay(Some(ptr));
    assert!(iter.next().await.unwrap().is_none());
  });
}
