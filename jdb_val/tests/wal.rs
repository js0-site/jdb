//! WAL storage mode tests
//! WAL 存储模式测试

use jdb_val::{INFILE_MAX, Wal};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Generate test data by size
/// 根据大小生成测试数据
fn make(size: usize, fill: u8) -> Vec<u8> {
  vec![fill; size]
}

/// Test infile + infile mode
/// 测试 infile + infile 模式
#[compio::test]
async fn test_infile_infile() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  let key = make(100, 0x41);
  let val = make(200, 0x61);

  let loc = wal.put(&key, &val).await.unwrap();
  let head = wal.read_head(loc).await.unwrap();

  assert!(head.key_store().is_infile());
  assert!(head.val_store().is_infile());
  assert!(!head.is_tombstone());

  // Read head_data
  // 读取 head_data
  let head_data = wal.read_head_data(loc, &head).await.unwrap();

  let got_key = wal.head_key(&head, &head_data).await.unwrap();
  let got_val = wal.head_val(&head, &head_data).await.unwrap();
  assert_eq!(got_key, key);
  assert_eq!(got_val, val);
}

/// Test file mode (key > 1MB)
/// 测试文件模式（key > 1MB）
#[compio::test]
async fn test_file_mode() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Key > 1MB triggers file mode
  // Key > 1MB 触发文件模式
  let key = make(INFILE_MAX + 100, 0x41);
  let val = make(INFILE_MAX + 200, 0x61);

  let loc = wal.put(&key, &val).await.unwrap();
  let head = wal.read_head(loc).await.unwrap();

  assert!(head.key_store().is_file());
  assert!(head.val_store().is_file());

  let head_data = wal.read_head_data(loc, &head).await.unwrap();

  let got_key = wal.head_key(&head, &head_data).await.unwrap();
  let got_val = wal.head_val(&head, &head_data).await.unwrap();
  assert_eq!(got_key, key);
  assert_eq!(got_val, val);
}

/// Test WAL rotate
/// 测试 WAL 轮转
#[compio::test]
async fn test_rotate() {
  use jdb_val::Conf;

  let dir = tempfile::tempdir().unwrap();
  // Small max size to trigger rotate
  // 小的最大大小以触发轮转
  let mut wal = Wal::new(dir.path(), &[Conf::MaxSize(200)]);
  wal.open().await.unwrap();

  let id1 = wal.cur_id();
  // Write enough to trigger rotate
  // 写入足够数据触发轮转
  for i in 0..5 {
    let key = format!("key{i}").into_bytes();
    let val = vec![i as u8; 50];
    wal.put(&key, &val).await.unwrap();
  }

  let id2 = wal.cur_id();
  assert!(id2 > id1, "should have rotated to new file");
}

/// Test sync operations
/// 测试同步操作
#[compio::test]
async fn test_sync() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  wal.put(b"key", b"val").await.unwrap();
  wal.sync_data().await.unwrap();
  wal.sync_all().await.unwrap();
}

/// Test iter
/// 测试迭代
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

/// Test scan
/// 测试扫描
#[compio::test]
async fn test_scan() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  wal.put(b"k1", b"v1").await.unwrap();
  wal.put(b"k2", b"v2").await.unwrap();
  wal.sync_all().await.unwrap();

  // Close and reopen to ensure data is persisted
  // 关闭并重新打开以确保数据持久化
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  let mut count = 0;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, head, _data| {
      count += 1;
      assert!(head.key_store().is_infile());
      true
    })
    .await
    .unwrap();
  assert_eq!(count, 2);
}

/// Test delete (tombstone)
/// 测试删除（墓碑标记）
#[compio::test]
async fn test_del() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Write then delete
  // 写入后删除
  wal.put(b"key1", b"val1").await.unwrap();
  let loc = wal.del(b"key1").await.unwrap();
  wal.sync_all().await.unwrap();

  // Verify tombstone head
  // 验证删除标记头
  let head = wal.read_head(loc).await.unwrap();
  assert!(head.is_tombstone());
  assert_eq!(head.val_len, None);

  // Verify key
  // 验证 key
  let head_data = wal.read_head_data(loc, &head).await.unwrap();
  let got_key = wal.head_key(&head, &head_data).await.unwrap();
  assert_eq!(got_key, b"key1");

  // Close and reopen to verify recovery
  // 关闭并重新打开验证恢复
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
  assert_eq!(count, 2, "should have 2 entries (put + del)");
  assert!(tombstone_found, "should find tombstone");
}

/// Test delete with file key
/// 测试文件 key 的删除
#[compio::test]
async fn test_del_file_key() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Key > 1MB triggers file mode
  // Key > 1MB 触发文件模式
  let key = make(INFILE_MAX + 100, b'k');
  let loc = wal.del(&key).await.unwrap();
  wal.sync_all().await.unwrap();

  let head = wal.read_head(loc).await.unwrap();
  assert!(head.key_store().is_file());
  assert!(head.is_tombstone());
  assert_eq!(head.val_len, None);

  let head_data = wal.read_head_data(loc, &head).await.unwrap();
  let got_key = wal.head_key(&head, &head_data).await.unwrap();
  assert_eq!(got_key, key);

  // Verify recovery
  // 验证恢复
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  let mut found = false;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, h, _| {
      assert!(h.key_store().is_file());
      assert!(h.is_tombstone());
      found = true;
      true
    })
    .await
    .unwrap();
  assert!(found);
}

/// Test read pending data from queue before flush
/// 测试在刷新前从队列读取待写入数据
#[compio::test]
async fn test_read_pending_queue() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Write without sync (data in queue)
  // 写入但不同步（数据在队列中）
  let loc = wal.put(b"key1", b"val1").await.unwrap();

  // Read immediately (should read from queue)
  // 立即读取（应从队列读取）
  let head = wal.read_head(loc).await.unwrap();
  let head_data = wal.read_head_data(loc, &head).await.unwrap();
  let got_key = wal.head_key(&head, &head_data).await.unwrap();
  let got_val = wal.head_val(&head, &head_data).await.unwrap();
  assert_eq!(got_key, b"key1");
  assert_eq!(got_val, b"val1");

  // Now sync and verify again
  // 同步后再次验证
  wal.sync_all().await.unwrap();
  let head = wal.read_head(loc).await.unwrap();
  let head_data = wal.read_head_data(loc, &head).await.unwrap();
  assert_eq!(wal.head_key(&head, &head_data).await.unwrap(), b"key1");
  assert_eq!(wal.head_val(&head, &head_data).await.unwrap(), b"val1");
}

mod prop {
  use std::fs;

  use jdb_val::{Pos, Wal};
  use proptest::prelude::*;

  /// Combined property test for WAL magic and recovery
  /// 合并的 WAL 魔数和恢复属性测试
  fn prop_write_and_recovery(key: Vec<u8>, val: Vec<u8>) {
    compio::runtime::Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();

      // Write data
      // 写入数据
      let (head_pos, file_len) = {
        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();

        let loc = wal.put(&key, &val).await.unwrap();
        let head_pos = loc.pos();
        wal.sync_all().await.unwrap();

        // Get file length
        // 获取文件长度
        let wal_path = dir.path().join("wal");
        let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
        assert_eq!(entries.len(), 1);
        let file_path = entries[0].as_ref().unwrap().path();
        let file_len = fs::metadata(&file_path).unwrap().len();

        (head_pos, file_len)
      };

      // Reopen and verify recovery
      // 重新打开并验证恢复
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();
      assert_eq!(
        wal.cur_pos(),
        file_len,
        "Recovery should set cur_pos to file_len"
      );

      // Verify data integrity
      // 验证数据完整性
      let loc = Pos::new(wal.cur_id(), head_pos);
      let head = wal.read_head(loc).await.unwrap();
      let head_data = wal.read_head_data(loc, &head).await.unwrap();
      let got_key = wal.head_key(&head, &head_data).await.unwrap();
      let got_val = wal.head_val(&head, &head_data).await.unwrap();
      assert_eq!(got_key, key);
      assert_eq!(got_val, val);
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

  /// Property test for recovery with corrupted magic
  /// 损坏魔数的恢复属性测试
  fn prop_corrupted_magic_recovery(key: Vec<u8>, val: Vec<u8>) {
    use std::io::{Seek, Write};

    compio::runtime::Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();

      // Write two entries
      // 写入两个条目
      let first_entry_end;
      {
        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();
        // First entry
        // 第一个条目
        wal.put(&[1u8], &[2u8]).await.unwrap();
        first_entry_end = wal.cur_pos();
        // Second entry
        // 第二个条目
        wal.put(&key, &val).await.unwrap();
        wal.sync_all().await.unwrap();
      }

      // Corrupt second entry's magic
      // 损坏第二个条目的魔数
      let wal_path = dir.path().join("wal");
      let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
      let file_path = entries[0].as_ref().unwrap().path();

      {
        let mut file = fs::OpenOptions::new().write(true).open(&file_path).unwrap();
        // Corrupt magic at second entry start
        // 损坏第二个条目开头的魔数
        file
          .seek(std::io::SeekFrom::Start(first_entry_end))
          .unwrap();
        file.write_all(&[0u8]).unwrap();
        file.sync_all().unwrap();
      }

      // Reopen and verify recovery
      // 重新打开并验证恢复
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();

      // Recovery should stop at first valid entry
      // 恢复应停在第一个有效条目
      assert_eq!(
        wal.cur_pos(),
        first_entry_end,
        "Recovery should stop at first_entry_end"
      );
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

  use jdb_val::{Conf, GcState, Gcable, PosMap, Wal};

  /// Mock GC checker
  /// 模拟 GC 检查器
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

    fn batch_update(
      &self,
      _mapping: impl IntoIterator<Item = PosMap>,
    ) -> impl Future<Output = bool> + Send {
      async { true }
    }
  }

  #[compio::test]
  async fn test_gc_cannot_remove_current() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    wal.open().await.unwrap();
    wal.put(b"k", b"v").await.unwrap();

    let checker = MockGc::new(vec![]);
    let mut state = GcState::new(dir.path());

    // Try to GC current WAL
    // 尝试 GC 当前 WAL
    let result = wal.gc_merge(&[wal.cur_id()], &checker, &mut state).await;
    assert!(result.is_err());
  }

  #[compio::test]
  async fn test_gc_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::new(dir.path(), &[]);
    wal.open().await.unwrap();

    let checker = MockGc::new(vec![]);
    let mut state = GcState::new(dir.path());

    let (reclaimed, total) = wal.gc_merge(&[], &checker, &mut state).await.unwrap();
    assert_eq!(reclaimed, 0);
    assert_eq!(total, 0);
  }

  #[compio::test]
  async fn test_gc_merge() {
    let dir = tempfile::tempdir().unwrap();

    // Create WAL with small max size
    // 创建小最大大小的 WAL
    let mut wal = Wal::new(dir.path(), &[Conf::MaxSize(150)]);
    wal.open().await.unwrap();

    // Write entries to trigger rotate
    // 写入条目触发轮转
    for i in 0..20u8 {
      let key = [b'k', i];
      let val = vec![i; 20];
      wal.put(&key, &val).await.unwrap();
    }
    wal.sync_all().await.unwrap();

    // Get old WAL ids
    // 获取旧 WAL id
    let cur_id = wal.cur_id();
    let ids: Vec<_> = wal.iter().filter(|&id| id < cur_id).collect();
    if ids.is_empty() {
      return; // No old files to GC
    }

    // GC with some deleted keys
    // GC 并删除一些键
    let checker = MockGc::new(vec![vec![b'k', 0], vec![b'k', 1]]);
    let mut state = GcState::new(dir.path());

    let (reclaimed, total) = wal.gc_merge(&ids, &checker, &mut state).await.unwrap();
    assert!(reclaimed <= total);
  }
}
