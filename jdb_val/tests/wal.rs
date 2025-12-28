//! WAL storage mode tests / WAL 存储模式测试

use jdb_val::{INFILE_MAX, Wal};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Generate test data by size / 根据大小生成测试数据
fn make(size: usize, fill: u8) -> Vec<u8> {
  vec![fill; size]
}

/// 3x3 storage mode test / 3x3 存储模式测试
///
/// | Mode   | Key Size    | Val Size    |
/// |--------|-------------|-------------|
/// | INLINE | ≤30B        | ≤50B (both) |
/// | INFILE | 31B~64KB    | 51B~64KB    |
/// | FILE   | >64KB       | >64KB       |
#[compio::test]
async fn test_3x3_modes() {
  // Key sizes: inline(10), infile(100), file(1MB+100)
  let key_sizes = [10, 100, INFILE_MAX + 100];
  // Val sizes: inline(10), infile(1000), file(1MB+200)
  let val_sizes = [10, 1000, INFILE_MAX + 200];

  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  for (ki, &k_size) in key_sizes.iter().enumerate() {
    for (vi, &v_size) in val_sizes.iter().enumerate() {
      let key = make(k_size, 0x41 + ki as u8);
      let val = make(v_size, 0x61 + vi as u8);

      let loc = wal.put(&key, &val).await.unwrap();
      let head = wal.read_head(loc).await.unwrap();

      // Verify flags / 验证标志
      let k_inline = ki == 0;
      let k_infile = ki == 1;
      let k_file = ki == 2;
      let v_inline = vi == 0;
      let v_infile = !v_inline && vi == 1;
      let v_file = vi == 2;

      assert_eq!(
        head.key_flag.is_inline(),
        k_inline,
        "key inline mismatch: ki={ki}, vi={vi}"
      );
      assert_eq!(
        head.key_flag.is_infile(),
        k_infile,
        "key infile mismatch: ki={ki}, vi={vi}"
      );
      assert_eq!(
        head.key_flag.is_file(),
        k_file,
        "key file mismatch: ki={ki}, vi={vi}"
      );

      if k_inline && v_inline {
        assert!(
          head.val_flag.is_inline(),
          "val should be inline: ki={ki}, vi={vi}"
        );
      } else if v_infile {
        assert!(
          head.val_flag.is_infile(),
          "val should be infile: ki={ki}, vi={vi}"
        );
      } else if v_file {
        assert!(
          head.val_flag.is_file(),
          "val should be file: ki={ki}, vi={vi}"
        );
      }

      // Verify data / 验证数据
      let got_key = wal.head_key(&head).await.unwrap();
      let got_val = wal.head_val(&head).await.unwrap();
      assert_eq!(got_key, key, "key mismatch: ki={ki}, vi={vi}");
      assert_eq!(got_val, val, "val mismatch: ki={ki}, vi={vi}");
    }
  }
}

/// Test WAL rotate / 测试 WAL 轮转
#[compio::test]
async fn test_rotate() {
  use jdb_val::Conf;

  let dir = tempfile::tempdir().unwrap();
  // Small max size to trigger rotate / 小的最大大小以触发轮转
  let mut wal = Wal::new(dir.path(), &[Conf::MaxSize(200)]);
  wal.open().await.unwrap();

  let id1 = wal.cur_id();
  // Write enough to trigger rotate / 写入足够数据触发轮转
  for i in 0..5 {
    let key = format!("key{i}").into_bytes();
    let val = vec![i as u8; 50];
    wal.put(&key, &val).await.unwrap();
  }

  let id2 = wal.cur_id();
  assert!(id2 > id1, "should have rotated to new file");
}

/// Test sync operations / 测试同步操作
#[compio::test]
async fn test_sync() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  wal.put(b"key", b"val").await.unwrap();
  wal.sync_data().await.unwrap();
  wal.sync_all().await.unwrap();
}

/// Test iter / 测试迭代
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

/// Test scan / 测试扫描
#[compio::test]
async fn test_scan() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  wal.put(b"k1", b"v1").await.unwrap();
  wal.put(b"k2", b"v2").await.unwrap();
  wal.sync_all().await.unwrap();

  // Close and reopen to ensure data is persisted / 关闭并重新打开以确保数据持久化
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  let mut count = 0;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, _| {
      count += 1;
      true
    })
    .await
    .unwrap();
  assert_eq!(count, 2);
}

/// Test scan with infile mode / 测试 infile 模式下的扫描
/// Regression test: WAL layout should be [Head, Key, Val, End], not [Key, Val, Head, End]
/// 回归测试：WAL 布局应为 [Head, Key, Val, End]，而非 [Key, Val, Head, End]
#[compio::test]
async fn test_scan_infile() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Key > 30B triggers infile mode / Key > 30B 触发 infile 模式
  let key = vec![b'k'; 100];
  let val = vec![b'v'; 100];
  let loc = wal.put(&key, &val).await.unwrap();
  wal.sync_all().await.unwrap();

  // Verify read works / 验证读取正常
  let head = wal.read_head(loc).await.unwrap();
  let got_key = wal.head_key(&head).await.unwrap();
  let got_val = wal.head_val(&head).await.unwrap();
  assert_eq!(got_key, key);
  assert_eq!(got_val, val);

  // Close and reopen / 关闭并重新打开
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  // Scan should find the entry / 扫描应找到条目
  let mut found = false;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, h| {
      assert!(h.key_flag.is_infile(), "key should be infile");
      assert!(h.val_flag.is_infile(), "val should be infile");
      found = true;
      true
    })
    .await
    .unwrap();
  assert!(found, "scan should find infile entry");
}

mod prop {
  use std::fs;

  use jdb_val::{MAGIC_SIZE, Wal};
  use proptest::prelude::*;

  /// Combined property test for WAL magic and recovery
  /// 合并的 WAL 魔数和恢复属性测试
  fn prop_write_and_recovery(key: Vec<u8>, val: Vec<u8>) {
    compio::runtime::Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();

      // Write data / 写入数据
      let (head_pos, file_len) = {
        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();

        let loc = wal.put(&key, &val).await.unwrap();
        let head_pos = loc.pos();
        wal.sync_all().await.unwrap();

        // Get file length / 获取文件长度
        let wal_path = dir.path().join("wal");
        let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
        assert_eq!(entries.len(), 1);
        let file_path = entries[0].as_ref().unwrap().path();
        let file_len = fs::metadata(&file_path).unwrap().len();

        (head_pos, file_len)
      };

      // Reopen and verify recovery / 重新打开并验证恢复
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();
      assert_eq!(
        wal.cur_pos(),
        file_len,
        "Recovery should set cur_pos to file_len"
      );

      // Verify data integrity / 验证数据完整性
      let head = wal
        .read_head(jdb_val::Pos::new(wal.cur_id(), head_pos))
        .await
        .unwrap();
      let got_key = wal.head_key(&head).await.unwrap();
      let got_val = wal.head_val(&head).await.unwrap();
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

      // Write two entries / 写入两个条目
      let first_entry_end;
      {
        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();
        // First entry / 第一个条目
        wal.put(&[1u8], &[2u8]).await.unwrap();
        first_entry_end = wal.cur_pos();
        // Second entry / 第二个条目
        wal.put(&key, &val).await.unwrap();
        wal.sync_all().await.unwrap();
      }

      // Corrupt second entry's magic / 损坏第二个条目的魔数
      let wal_path = dir.path().join("wal");
      let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
      let file_path = entries[0].as_ref().unwrap().path();

      {
        let mut file = fs::OpenOptions::new().write(true).open(&file_path).unwrap();
        // Corrupt magic at second entry start / 损坏第二个条目开头的魔数
        file
          .seek(std::io::SeekFrom::Start(first_entry_end))
          .unwrap();
        file.write_all(&[0u8; MAGIC_SIZE]).unwrap();
        file.sync_all().unwrap();
      }

      // Reopen and verify recovery / 重新打开并验证恢复
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();

      // Recovery should stop at first valid entry / 恢复应停在第一个有效条目
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

/// Test streaming write / 测试流式写入
#[compio::test]
async fn test_put_stream() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Small stream (inline) / 小流（内联）
  let chunks = vec![vec![1u8, 2, 3], vec![4u8, 5, 6]];
  let loc = wal.put_stream(b"key1", chunks.into_iter()).await.unwrap();
  let head = wal.read_head(loc).await.unwrap();
  let val = wal.head_val(&head).await.unwrap();
  assert_eq!(val, vec![1, 2, 3, 4, 5, 6]);
}

/// Test put_stream with infile key / 测试 infile key 的流式写入
/// Regression: put_stream should produce [Head, Key, Val, End] layout
/// 回归测试：put_stream 应产生 [Head, Key, Val, End] 布局
#[compio::test]
async fn test_put_stream_infile_key() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Key > 30B triggers infile mode / Key > 30B 触发 infile 模式
  let key = vec![b'k'; 100];
  // Large val triggers FILE mode / 大 val 触发 FILE 模式
  let val_chunks: Vec<Vec<u8>> = (0..20).map(|i| vec![i; INFILE_MAX / 10]).collect();
  let expected_val: Vec<u8> = val_chunks.iter().flatten().copied().collect();

  let loc = wal.put_stream(&key, val_chunks.into_iter()).await.unwrap();
  wal.sync_all().await.unwrap();

  // Verify read works / 验证读取正常
  let head = wal.read_head(loc).await.unwrap();
  assert!(head.key_flag.is_infile(), "key should be infile");
  assert!(head.val_flag.is_file(), "val should be file");

  let got_key = wal.head_key(&head).await.unwrap();
  let got_val = wal.head_val(&head).await.unwrap();
  assert_eq!(got_key, key);
  assert_eq!(got_val, expected_val);

  // Close and reopen to verify recovery / 关闭并重新打开验证恢复
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  // Scan should find the entry / 扫描应找到条目
  let mut found = false;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, h| {
      assert!(
        h.key_flag.is_infile(),
        "key should be infile after recovery"
      );
      assert!(h.val_flag.is_file(), "val should be file after recovery");
      found = true;
      true
    })
    .await
    .unwrap();
  assert!(found, "scan should find infile key entry");
}

/// Test put_stream with file key / 测试 file key 的流式写入
#[compio::test]
async fn test_put_stream_file_key() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Key > 1MB triggers file mode / Key > 1MB 触发 file 模式
  let key = vec![b'k'; INFILE_MAX + 100];
  // Large val triggers FILE mode / 大 val 触发 FILE 模式
  let val_chunks: Vec<Vec<u8>> = (0..5).map(|i| vec![i; INFILE_MAX / 2]).collect();
  let expected_val: Vec<u8> = val_chunks.iter().flatten().copied().collect();

  let loc = wal.put_stream(&key, val_chunks.into_iter()).await.unwrap();
  wal.sync_all().await.unwrap();

  // Verify read works / 验证读取正常
  let head = wal.read_head(loc).await.unwrap();
  assert!(head.key_flag.is_file(), "key should be file");
  assert!(head.val_flag.is_file(), "val should be file");

  let got_key = wal.head_key(&head).await.unwrap();
  let got_val = wal.head_val(&head).await.unwrap();
  assert_eq!(got_key, key);
  assert_eq!(got_val, expected_val);

  // Close and reopen to verify recovery / 关闭并重新打开验证恢复
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  // Scan should find the entry / 扫描应找到条目
  let mut found = false;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, h| {
      assert!(h.key_flag.is_file(), "key should be file after recovery");
      assert!(h.val_flag.is_file(), "val should be file after recovery");
      found = true;
      true
    })
    .await
    .unwrap();
  assert!(found, "scan should find file key entry");
}

/// Test put_with_file / 测试 put_with_file
#[compio::test]
async fn test_put_with_file() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // First write a large value to get file id / 先写入大值获取 file id
  let val = vec![0xABu8; INFILE_MAX + 1000];
  let loc1 = wal.put(b"key1", &val).await.unwrap();
  let head1 = wal.read_head(loc1).await.unwrap();
  assert!(head1.val_flag.is_file(), "val should be file");

  let val_file_id = head1.val_pos().id();
  let val_len = head1.val_len.get();
  let val_crc = head1.val_crc32();

  // Use put_with_file with inline key / 使用 put_with_file 和 inline key
  let loc2 = wal
    .put_with_file(b"key2", val_file_id, val_len, val_crc)
    .await
    .unwrap();
  let head2 = wal.read_head(loc2).await.unwrap();
  assert!(head2.key_flag.is_inline(), "key should be inline");
  assert!(head2.val_flag.is_file(), "val should be file");

  let got_key = wal.head_key(&head2).await.unwrap();
  let got_val = wal.head_val(&head2).await.unwrap();
  assert_eq!(got_key, b"key2");
  assert_eq!(got_val, val);
}

/// Test put_with_file with infile key / 测试 infile key 的 put_with_file
#[compio::test]
async fn test_put_with_file_infile_key() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // First write a large value to get file id / 先写入大值获取 file id
  let val = vec![0xCDu8; INFILE_MAX + 500];
  let loc1 = wal.put(b"k", &val).await.unwrap();
  let head1 = wal.read_head(loc1).await.unwrap();

  let val_file_id = head1.val_pos().id();
  let val_len = head1.val_len.get();
  let val_crc = head1.val_crc32();

  // Use put_with_file with infile key (> 30B) / 使用 put_with_file 和 infile key
  let key = vec![b'k'; 100];
  let loc2 = wal
    .put_with_file(&key, val_file_id, val_len, val_crc)
    .await
    .unwrap();
  wal.sync_all().await.unwrap();

  let head2 = wal.read_head(loc2).await.unwrap();
  assert!(head2.key_flag.is_infile(), "key should be infile");
  assert!(head2.val_flag.is_file(), "val should be file");

  let got_key = wal.head_key(&head2).await.unwrap();
  let got_val = wal.head_val(&head2).await.unwrap();
  assert_eq!(got_key, key);
  assert_eq!(got_val, val);

  // Close and reopen to verify recovery / 关闭并重新打开验证恢复
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  // Scan should find both entries / 扫描应找到两个条目
  let mut count = 0;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, h| {
      count += 1;
      if count == 2 {
        assert!(
          h.key_flag.is_infile(),
          "key should be infile after recovery"
        );
        assert!(h.val_flag.is_file(), "val should be file after recovery");
      }
      true
    })
    .await
    .unwrap();
  assert_eq!(count, 2, "scan should find 2 entries");
}

/// Test put_with_file with file key / 测试 file key 的 put_with_file
#[compio::test]
async fn test_put_with_file_file_key() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // First write a large value to get file id / 先写入大值获取 file id
  let val = vec![0xEFu8; INFILE_MAX + 200];
  let loc1 = wal.put(b"k", &val).await.unwrap();
  let head1 = wal.read_head(loc1).await.unwrap();

  let val_file_id = head1.val_pos().id();
  let val_len = head1.val_len.get();
  let val_crc = head1.val_crc32();

  // Use put_with_file with file key (> 1MB) / 使用 put_with_file 和 file key
  let key = vec![b'k'; INFILE_MAX + 50];
  let loc2 = wal
    .put_with_file(&key, val_file_id, val_len, val_crc)
    .await
    .unwrap();
  wal.sync_all().await.unwrap();

  let head2 = wal.read_head(loc2).await.unwrap();
  assert!(head2.key_flag.is_file(), "key should be file");
  assert!(head2.val_flag.is_file(), "val should be file");

  let got_key = wal.head_key(&head2).await.unwrap();
  let got_val = wal.head_val(&head2).await.unwrap();
  assert_eq!(got_key, key);
  assert_eq!(got_val, val);

  // Close and reopen to verify recovery / 关闭并重新打开验证恢复
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  // Scan should find both entries / 扫描应找到两个条目
  let mut count = 0;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, h| {
      count += 1;
      if count == 2 {
        assert!(h.key_flag.is_file(), "key should be file after recovery");
        assert!(h.val_flag.is_file(), "val should be file after recovery");
      }
      true
    })
    .await
    .unwrap();
  assert_eq!(count, 2, "scan should find 2 entries");
}

/// Test streaming read / 测试流式读取
#[compio::test]
async fn test_stream_read() {
  use jdb_val::DataStream;

  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Write large value / 写入大值
  let val = vec![0xABu8; INFILE_MAX + 1000];
  let loc = wal.put(b"key", &val).await.unwrap();
  let head = wal.read_head(loc).await.unwrap();

  // Stream read / 流式读取
  let mut stream = wal.head_val_stream(&head).await.unwrap();
  let mut result = Vec::new();
  while let Some(chunk) = stream.next().await.unwrap() {
    result.extend_from_slice(&chunk);
  }
  assert_eq!(result, val);

  // Test read_all / 测试 read_all
  let mut stream = wal.head_val_stream(&head).await.unwrap();
  let all = stream.read_all().await.unwrap();
  assert_eq!(all, val);

  // Test inline stream / 测试内联流
  let loc = wal.put(b"k", b"v").await.unwrap();
  let head = wal.read_head(loc).await.unwrap();
  let mut stream = wal.head_val_stream(&head).await.unwrap();
  if let DataStream::Inline(data) = &stream {
    assert_eq!(data, b"v");
  }
  let all = stream.read_all().await.unwrap();
  assert_eq!(all, b"v");
}

/// Test delete (tombstone) / 测试删除（墓碑标记）
#[compio::test]
async fn test_del() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Write then delete / 写入后删除
  wal.put(b"key1", b"val1").await.unwrap();
  let loc = wal.del(b"key1").await.unwrap();
  wal.sync_all().await.unwrap();

  // Verify tombstone head / 验证删除标记头
  let head = wal.read_head(loc).await.unwrap();
  assert!(head.val_flag.is_tombstone(), "val_flag should be tombstone");
  assert!(
    head.key_flag.is_tombstone(),
    "key_flag should be tombstone for inline key"
  );
  assert_eq!(head.val_len.get(), 0, "val_len should be 0");

  // Verify key / 验证 key
  let got_key = wal.head_key(&head).await.unwrap();
  assert_eq!(got_key, b"key1");

  // Close and reopen to verify recovery / 关闭并重新打开验证恢复
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  let mut count = 0;
  let mut tombstone_found = false;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, h| {
      count += 1;
      if h.val_flag.is_tombstone() {
        tombstone_found = true;
      }
      true
    })
    .await
    .unwrap();
  assert_eq!(count, 2, "should have 2 entries (put + del)");
  assert!(tombstone_found, "should find tombstone");
}

/// Test delete with infile key / 测试 infile key 的删除
#[compio::test]
async fn test_del_infile_key() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Key > 30B triggers infile mode / Key > 30B 触发 infile 模式
  let key = vec![b'k'; 100];
  let loc = wal.del(&key).await.unwrap();
  wal.sync_all().await.unwrap();

  let head = wal.read_head(loc).await.unwrap();
  assert!(head.key_flag.is_infile(), "key_flag should be infile");
  assert!(head.val_flag.is_tombstone(), "val_flag should be tombstone");
  assert_eq!(head.val_len.get(), 0);

  let got_key = wal.head_key(&head).await.unwrap();
  assert_eq!(got_key, key);

  // Verify recovery / 验证恢复
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  let mut found = false;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, h| {
      assert!(
        h.key_flag.is_infile(),
        "key should be infile after recovery"
      );
      assert!(
        h.val_flag.is_tombstone(),
        "val should be tombstone after recovery"
      );
      found = true;
      true
    })
    .await
    .unwrap();
  assert!(found);
}

/// Test delete with file key / 测试 file key 的删除
#[compio::test]
async fn test_del_file_key() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Key > 1MB triggers file mode / Key > 1MB 触发 file 模式
  let key = vec![b'k'; INFILE_MAX + 100];
  let loc = wal.del(&key).await.unwrap();
  wal.sync_all().await.unwrap();

  let head = wal.read_head(loc).await.unwrap();
  assert!(head.key_flag.is_file(), "key_flag should be file");
  assert!(head.val_flag.is_tombstone(), "val_flag should be tombstone");
  assert_eq!(head.val_len.get(), 0);

  let got_key = wal.head_key(&head).await.unwrap();
  assert_eq!(got_key, key);

  // Verify recovery / 验证恢复
  drop(wal);
  let wal = Wal::new(dir.path(), &[]);

  let mut found = false;
  let id = wal.iter().next().unwrap();
  wal
    .scan(id, |_, h| {
      assert!(h.key_flag.is_file(), "key should be file after recovery");
      assert!(
        h.val_flag.is_tombstone(),
        "val should be tombstone after recovery"
      );
      found = true;
      true
    })
    .await
    .unwrap();
  assert!(found);
}

mod gc {

  use jdb_val::{Conf, GcState, Gcable, PosMap, Wal};

  /// Mock GC checker / 模拟 GC 检查器
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
      async move { self.deleted.iter().any(|k| k == key) }
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

    // Try to GC current WAL / 尝试 GC 当前 WAL
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

    // Create WAL with small max size / 创建小最大大小的 WAL
    let mut wal = Wal::new(dir.path(), &[Conf::MaxSize(150)]);
    wal.open().await.unwrap();

    // Write entries to trigger rotate / 写入条目触发轮转
    for i in 0..20u8 {
      let key = [b'k', i];
      let val = vec![i; 20];
      wal.put(&key, &val).await.unwrap();
    }
    wal.sync_all().await.unwrap();

    // Get old WAL ids / 获取旧 WAL id
    let cur_id = wal.cur_id();
    let ids: Vec<_> = wal.iter().filter(|&id| id < cur_id).collect();
    if ids.is_empty() {
      return; // No old files to GC / 没有旧文件需要 GC
    }

    // GC with some deleted keys / GC 并删除一些键
    let checker = MockGc::new(vec![vec![b'k', 0], vec![b'k', 1]]);
    let mut state = GcState::new(dir.path());

    let (reclaimed, total) = wal.gc_merge(&ids, &checker, &mut state).await.unwrap();
    assert!(reclaimed <= total);
  }
}

/// Test read pending data from queue before flush
/// 测试在刷新前从队列读取待写入数据
#[compio::test]
async fn test_read_pending_queue() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Write without sync (data in queue) / 写入但不同步（数据在队列中）
  let loc = wal.put(b"key1", b"val1").await.unwrap();

  // Read immediately (should read from queue) / 立即读取（应从队列读取）
  let head = wal.read_head(loc).await.unwrap();
  let got_key = wal.head_key(&head).await.unwrap();
  let got_val = wal.head_val(&head).await.unwrap();
  assert_eq!(got_key, b"key1");
  assert_eq!(got_val, b"val1");

  // Write more / 写入更多
  let loc2 = wal.put(b"key2", b"val2").await.unwrap();
  let loc3 = wal.put(b"key3", b"val3").await.unwrap();

  // Read all without sync / 不同步直接读取
  let head2 = wal.read_head(loc2).await.unwrap();
  let head3 = wal.read_head(loc3).await.unwrap();
  assert_eq!(wal.head_key(&head2).await.unwrap(), b"key2");
  assert_eq!(wal.head_val(&head2).await.unwrap(), b"val2");
  assert_eq!(wal.head_key(&head3).await.unwrap(), b"key3");
  assert_eq!(wal.head_val(&head3).await.unwrap(), b"val3");

  // Now sync and verify again / 同步后再次验证
  wal.sync_all().await.unwrap();
  let head = wal.read_head(loc).await.unwrap();
  assert_eq!(wal.head_key(&head).await.unwrap(), b"key1");
  assert_eq!(wal.head_val(&head).await.unwrap(), b"val1");
}

/// Test read pending infile data from queue
/// 测试从队列读取待写入的 infile 数据
#[compio::test]
async fn test_read_pending_infile() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  wal.open().await.unwrap();

  // Key > 30B triggers infile mode / Key > 30B 触发 infile 模式
  let key = vec![b'k'; 100];
  let val = vec![b'v'; 200];

  // Write without sync / 写入但不同步
  let loc = wal.put(&key, &val).await.unwrap();

  // Read immediately / 立即读取
  let head = wal.read_head(loc).await.unwrap();
  assert!(head.key_flag.is_infile());
  assert!(head.val_flag.is_infile());

  let got_key = wal.head_key(&head).await.unwrap();
  let got_val = wal.head_val(&head).await.unwrap();
  assert_eq!(got_key, key);
  assert_eq!(got_val, val);
}
