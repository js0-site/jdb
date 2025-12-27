//! WAL storage mode tests / WAL 存储模式测试

use jdb_val::{INFILE_MAX, Wal};

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


mod prop {
  use std::fs;
  use std::io::Read;

  use jdb_val::{END_SIZE, Head, Wal, parse_end};
  use proptest::prelude::*;

  /// Feature: wal-end-marker, Property 2: Write Produces Valid End Marker
  /// *For any* key-value pair written via `put()`, the resulting file SHALL contain
  /// a valid End_Marker at position `head_pos + Head::SIZE`,
  /// where the End_Marker's `head_offset` equals `head_pos`.
  /// **Validates: Requirements 2.1, 2.2, 2.3**
  fn prop_write_produces_valid_end_marker(key: Vec<u8>, val: Vec<u8>) {
    compio::runtime::Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();

      let loc = wal.put(&key, &val).await.unwrap();
      let head_pos = loc.pos();

      // End marker is right after Head (infile data is written before Head)
      // 尾部标记紧跟在 Head 之后（infile 数据在 Head 之前写入）
      let end_pos = head_pos + Head::SIZE as u64;

      // Read end marker from file / 从文件读取尾部标记
      let wal_path = dir.path().join("wal");
      let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
      assert_eq!(entries.len(), 1);
      let file_path = entries[0].as_ref().unwrap().path();

      let mut file = fs::File::open(&file_path).unwrap();
      let mut buf = Vec::new();
      file.read_to_end(&mut buf).unwrap();

      // Verify end marker / 验证尾部标记
      let end_buf = &buf[end_pos as usize..end_pos as usize + END_SIZE];
      let parsed_offset = parse_end(end_buf);
      assert!(parsed_offset.is_some(), "End marker should be valid");
      assert_eq!(
        parsed_offset.unwrap(),
        head_pos,
        "End marker head_offset should equal head_pos"
      );
    });
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_prop_write_produces_valid_end_marker(
      key in prop::collection::vec(any::<u8>(), 1..100),
      val in prop::collection::vec(any::<u8>(), 1..100)
    ) {
      prop_write_produces_valid_end_marker(key, val);
    }
  }

  /// Feature: wal-end-marker, Property 3: Fast Recovery Correctness
  /// *For any* WAL file with valid End_Marker (correct magic and valid Head CRC at offset),
  /// fast recovery SHALL set `cur_pos` to `file_len` (file end, preserving all data).
  /// **Validates: Requirements 3.2, 3.3**
  fn prop_fast_recovery_correctness(key: Vec<u8>, val: Vec<u8>) {
    compio::runtime::Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();

      // Write data / 写入数据
      {
        let mut wal = Wal::new(dir.path(), &[]);
        wal.open().await.unwrap();
        wal.put(&key, &val).await.unwrap();
      }

      // Get file length / 获取文件长度
      let wal_path = dir.path().join("wal");
      let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
      assert_eq!(entries.len(), 1);
      let file_path = entries[0].as_ref().unwrap().path();
      let file_len = fs::metadata(&file_path).unwrap().len();

      // Reopen and verify cur_pos = file_len / 重新打开并验证 cur_pos = 文件长度
      let mut wal = Wal::new(dir.path(), &[]);
      wal.open().await.unwrap();
      let cur_pos = wal.cur_pos();

      assert_eq!(
        cur_pos, file_len,
        "Fast recovery should set cur_pos to file_len"
      );
    });
  }

  proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_prop_fast_recovery_correctness(
      key in prop::collection::vec(any::<u8>(), 1..100),
      val in prop::collection::vec(any::<u8>(), 1..100)
    ) {
      prop_fast_recovery_correctness(key, val);
    }
  }
}
