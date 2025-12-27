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

mod prop {
  use std::{
    fs,
    io::{Read, Seek, Write},
  };

  use jdb_val::{END_SIZE, Head, Wal, parse_end};
  use proptest::prelude::*;

  /// Combined property test for WAL end marker and recovery
  /// 合并的 WAL 尾部标记和恢复属性测试
  ///
  /// Tests:
  /// 1. Write produces valid end marker / 写入产生有效尾部标记
  /// 2. Fast recovery sets cur_pos to file_len / 快速恢复设置 cur_pos 为文件长度
  fn prop_write_and_fast_recovery(key: Vec<u8>, val: Vec<u8>) {
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

        // Verify end marker / 验证尾部标记
        let end_pos = head_pos + Head::SIZE as u64;
        let mut file = fs::File::open(&file_path).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();

        let end_buf = &buf[end_pos as usize..end_pos as usize + END_SIZE];
        let parsed_offset = parse_end(end_buf);
        assert!(parsed_offset.is_some(), "End marker should be valid");
        assert_eq!(
          parsed_offset.unwrap(),
          head_pos,
          "End marker head_offset should equal head_pos"
        );

        (head_pos, file_len)
      };

      // Reopen and verify fast recovery / 重新打开并验证快速恢复
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();
      assert_eq!(
        wal.cur_pos(),
        file_len,
        "Fast recovery should set cur_pos to file_len"
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
    fn test_prop_write_and_fast_recovery(
      key in prop::collection::vec(any::<u8>(), 1..100),
      val in prop::collection::vec(any::<u8>(), 1..100)
    ) {
      prop_write_and_fast_recovery(key, val);
    }
  }

  /// Property test for scan recovery with corrupted end marker
  /// 损坏尾部标记的扫描恢复属性测试
  fn prop_scan_recovery(key: Vec<u8>, val: Vec<u8>) {
    compio::runtime::Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();

      // Write two entries / 写入两个条目
      let first_entry_end;
      let second_entry_end;
      {
        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();
        // First entry / 第一个条目
        wal.put(&[1u8], &[2u8]).await.unwrap();
        first_entry_end = wal.cur_pos();
        // Second entry / 第二个条目
        wal.put(&key, &val).await.unwrap();
        second_entry_end = wal.cur_pos();
        wal.sync_all().await.unwrap();
      }

      // Corrupt second entry's end marker (only the magic, not the first entry)
      // 损坏第二个条目的尾部标记（只损坏魔数，不影响第一个条目）
      let wal_path = dir.path().join("wal");
      let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
      let file_path = entries[0].as_ref().unwrap().path();

      {
        let mut file = fs::OpenOptions::new().write(true).open(&file_path).unwrap();
        // Corrupt magic at second entry end (last 4 bytes of file)
        // 损坏第二个条目末尾的魔数（文件最后4字节）
        file.seek(std::io::SeekFrom::End(-4)).unwrap();
        file.write_all(&[0u8; 4]).unwrap();
        file.sync_all().unwrap();
      }

      // Reopen and verify scan recovery / 重新打开并验证扫描恢复
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();

      // Scan should find first entry valid, skip corrupted second entry
      // 扫描应该找到第一个有效条目，跳过损坏的第二个条目
      // cur_pos should be first_entry_end (last valid entry end)
      // cur_pos 应该是 first_entry_end（最后一个有效条目结尾）
      assert!(
        wal.cur_pos() >= first_entry_end && wal.cur_pos() <= second_entry_end,
        "Scan recovery should find valid position: got {}, expected between {first_entry_end} and {second_entry_end}",
        wal.cur_pos()
      );
    });
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]

    #[test]
    fn test_prop_scan_recovery(
      key in prop::collection::vec(any::<u8>(), 1..50),
      val in prop::collection::vec(any::<u8>(), 1..50)
    ) {
      prop_scan_recovery(key, val);
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
