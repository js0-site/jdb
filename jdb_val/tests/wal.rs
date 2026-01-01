//! WAL storage mode tests
//! WAL 存储模式测试

use jdb_val::{INFILE_MAX, Wal};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

fn make(size: usize, fill: u8) -> Vec<u8> {
  vec![fill; size]
}

#[compio::test]
async fn test_infile_infile() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  let key = make(100, 0x41);
  let val = make(200, 0x61);

  let loc = wal.put(&key, &val).await.unwrap();
  let got_val = wal.val(loc).await.unwrap();
  assert_eq!(got_val.as_ref(), val.as_slice());
}

#[compio::test]
async fn test_file_mode() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  let key = make(100, 0x41);
  let val = make(INFILE_MAX + 200, 0x61);

  let loc = wal.put(&key, &val).await.unwrap();
  assert!(!loc.is_infile());

  let got_val = wal.val(loc).await.unwrap();
  assert_eq!(got_val.as_ref(), val.as_slice());
}

#[compio::test]
async fn test_rotate() {
  use jdb_val::Conf;

  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[Conf::MaxSize(200)]);
  wal.open().await.unwrap();

  let id1 = wal.cur_id();
  for i in 0..5 {
    let key = format!("key{i}").into_bytes();
    let val = vec![i as u8; 50];
    wal.put(&key, &val).await.unwrap();
  }
  wal.flush().await.unwrap();

  let id2 = wal.cur_id();
  assert!(id2 > id1, "should have rotated to new file");
}

#[compio::test]
async fn test_sync() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  wal.put(b"key", b"val").await.unwrap();
  wal.sync_data().await.unwrap();
  wal.sync_all().await.unwrap();
}

#[compio::test]
async fn test_iter() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  wal.put(b"k1", b"v1").await.unwrap();
  wal.put(b"k2", b"v2").await.unwrap();

  let ids: Vec<_> = wal.iter().collect();
  assert_eq!(ids.len(), 1);
  assert_eq!(ids[0], wal.cur_id());
}

#[compio::test]
async fn test_scan() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  wal.put(b"k1", b"v1").await.unwrap();
  wal.put(b"k2", b"v2").await.unwrap();
  wal.sync_all().await.unwrap();

  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  let mut count = 0;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, head, _| {
      count += 1;
      assert!(head.val_is_infile());
      true
    })
    .await
    .unwrap();
  assert_eq!(count, 2);
}

#[compio::test]
async fn test_del() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  wal.put(b"key1", b"val1").await.unwrap();
  let loc = wal.del(b"key1").await.unwrap();
  wal.sync_all().await.unwrap();

  // del returns Pos with len=0
  // del 返回 len=0 的 Pos
  assert!(loc.is_empty());

  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  let mut count = 0;
  let mut tombstone_found = false;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, h, _| {
      count += 1;
      if h.is_tombstone() {
        tombstone_found = true;
      }
      true
    })
    .await
    .unwrap();
  assert_eq!(count, 2);
  assert!(tombstone_found);
}

#[compio::test]
async fn test_read_pending_queue() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  let loc = wal.put(b"key1", b"val1").await.unwrap();

  let got_val = wal.val(loc).await.unwrap();
  assert_eq!(got_val.as_ref(), b"val1");

  wal.sync_all().await.unwrap();
  let got_val = wal.val(loc).await.unwrap();
  assert_eq!(got_val.as_ref(), b"val1");
}

mod prop {
  use std::fs;

  use jdb_val::Wal;
  use proptest::prelude::*;

  fn prop_write_and_recovery(key: Vec<u8>, val: Vec<u8>) {
    compio::runtime::Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();

      let loc = {
        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();

        let loc = wal.put(&key, &val).await.unwrap();
        wal.sync_all().await.unwrap();
        loc
      };

      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();

      let got_val = wal.val(loc).await.unwrap();
      assert_eq!(got_val.as_ref(), val.as_slice());
    });
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn test_prop_write_and_recovery(
      key in prop::collection::vec(any::<u8>(), 1..100),
      val in prop::collection::vec(any::<u8>(), 1..100)
    ) {
      prop_write_and_recovery(key, val);
    }
  }

  fn prop_corrupted_magic_recovery(key: Vec<u8>, val: Vec<u8>) {
    use std::io::{Seek, Write};

    compio::runtime::Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();

      let first_entry_end;
      {
        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();
        wal.put(&[1u8], &[2u8]).await.unwrap();
        first_entry_end = wal.cur_pos();
        wal.put(&key, &val).await.unwrap();
        wal.sync_all().await.unwrap();
      }

      let wal_path = dir.path().join("wal");
      let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
      let file_path = entries[0].as_ref().unwrap().path();

      {
        let mut file = fs::OpenOptions::new().write(true).open(&file_path).unwrap();
        file
          .seek(std::io::SeekFrom::Start(first_entry_end))
          .unwrap();
        file.write_all(&[0u8]).unwrap();
        file.sync_all().unwrap();
      }

      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();
      assert_eq!(wal.cur_pos(), first_entry_end);
    });
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]

    #[test]
    fn test_prop_corrupted_magic_recovery(
      key in prop::collection::vec(any::<u8>(), 1..50),
      val in prop::collection::vec(any::<u8>(), 1..50)
    ) {
      prop_corrupted_magic_recovery(key, val);
    }
  }
}

mod gc {
  use std::future::Future;

  use jdb_val::{Conf, Gcable, IndexUpdate, PosMap, Wal};

  struct MockGc {
    deleted: Vec<Vec<u8>>,
  }

  impl MockGc {
    fn new(deleted: Vec<Vec<u8>>) -> Self {
      Self { deleted }
    }
  }

  #[allow(clippy::manual_async_fn)]
  impl Gcable for MockGc {
    fn is_rm(&self, key: &[u8]) -> impl Future<Output = bool> + Send {
      let found = self.deleted.iter().any(|k| k == key);
      async move { found }
    }
  }

  struct MockIndex;

  impl IndexUpdate for MockIndex {
    fn update(&self, _mapping: &[PosMap]) {}
  }

  #[compio::test]
  async fn test_gc_cannot_remove_current() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    wal.open().await.unwrap();
    wal.put(b"k", b"v").await.unwrap();

    let checker = MockGc::new(vec![]);
    let index = MockIndex;

    let result = wal.gc(&[wal.cur_id()], &checker, &index).await;
    assert!(result.is_err());
  }

  #[compio::test]
  async fn test_gc_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    wal.open().await.unwrap();

    let checker = MockGc::new(vec![]);
    let index = MockIndex;

    let (reclaimed, total) = wal.gc(&[], &checker, &index).await.unwrap();
    assert_eq!(reclaimed, 0);
    assert_eq!(total, 0);
  }

  #[compio::test]
  async fn test_gc_merge() {
    let dir = tempfile::tempdir().unwrap();

    let mut wal = Wal::new(dir.path(), &[Conf::MaxSize(150)]);
    wal.open().await.unwrap();

    for i in 0..20u8 {
      let key = [b'k', i];
      let val = vec![i; 20];
      wal.put(&key, &val).await.unwrap();
    }
    wal.sync_all().await.unwrap();

    let cur_id = wal.cur_id();
    let ids: Vec<_> = wal.iter().filter(|&id| id < cur_id).collect();
    if ids.is_empty() {
      return;
    }

    let checker = MockGc::new(vec![vec![b'k', 0], vec![b'k', 1]]);
    let index = MockIndex;

    let (reclaimed, total) = wal.gc(&ids, &checker, &index).await.unwrap();
    assert!(reclaimed <= total);
  }
}
