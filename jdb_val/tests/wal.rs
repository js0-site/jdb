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
