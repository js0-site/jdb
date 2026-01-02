//! Checkpoint module tests
//! 检查点模块测试

fn run<F: std::future::Future>(f: F) -> F::Output {
  compio_runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn test_ckp_open_empty() {
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let (ckp, after) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
    assert!(after.is_none());
    assert!(ckp.wal_id_offset().is_none());
  });
}

#[test]
fn test_ckp_save_and_load() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = jdb_ckp::open(dir.path(), &[]).await.unwrap().0;
      ckp.set_wal_ptr(100, 200).await.unwrap();
    }

    {
      let (ckp, after) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
      let (wal_id, offset) = ckp.wal_id_offset().unwrap();
      assert_eq!(wal_id, 100);
      assert_eq!(offset, 200);
      if let Some(a) = after {
        assert_eq!(a.wal_id, 100);
        assert_eq!(a.offset, 200);
      }
    }
  });
}

#[test]
fn test_ckp_multiple_saves() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = jdb_ckp::open(dir.path(), &[]).await.unwrap().0;
      for i in 0..10u64 {
        ckp.set_wal_ptr(i, i * 100).await.unwrap();
      }
    }

    {
      let (ckp, after) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
      let (wal_id, offset) = ckp.wal_id_offset().unwrap();
      assert_eq!(wal_id, 9);
      assert_eq!(offset, 900);
      if let Some(a) = after {
        assert_eq!(a.wal_id, 9);
        assert_eq!(a.offset, 900);
      }
    }
  });
}

#[test]
fn test_ckp_rotate() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = jdb_ckp::open(dir.path(), &[]).await.unwrap().0;
      ckp.set_wal_ptr(1, 100).await.unwrap();
      ckp.rotate(2).await.unwrap();
      ckp.rotate(3).await.unwrap();
    }

    {
      let (_ckp, after) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
      let a = after.unwrap();
      assert_eq!(a.wal_id, 1);
      assert_eq!(a.offset, 100);
      assert_eq!(a.rotate_wal_id_li, vec![2, 3]);
    }
  });
}

#[test]
fn test_ckp_save_clears_rotates() {
  run(async {
    let dir = tempfile::tempdir().unwrap();

    {
      let mut ckp = jdb_ckp::open(dir.path(), &[]).await.unwrap().0;
      ckp.set_wal_ptr(1, 100).await.unwrap();
      ckp.rotate(2).await.unwrap();
      ckp.rotate(3).await.unwrap();
      ckp.set_wal_ptr(3, 500).await.unwrap();
      ckp.rotate(4).await.unwrap();
    }

    {
      let (_ckp, after) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
      let a = after.unwrap();
      assert_eq!(a.wal_id, 3);
      assert_eq!(a.offset, 500);
      assert_eq!(a.rotate_wal_id_li, vec![4]);
    }
  });
}

#[test]
fn test_ckp_last_save_id() {
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let mut ckp = jdb_ckp::open(dir.path(), &[]).await.unwrap().0;

    assert!(ckp.wal_id_offset().is_none());

    ckp.set_wal_ptr(42, 1000).await.unwrap();
    assert_eq!(ckp.wal_id_offset(), Some((42, 1000)));
  });
}

#[test]
fn test_after() {
  let after = jdb_base::Ckp {
    wal_id: 123,
    offset: 456,
    rotate_wal_id_li: vec![1, 2, 3],
  };

  assert_eq!(after.wal_id, 123);
  assert_eq!(after.offset, 456);
  assert_eq!(after.rotate_wal_id_li, vec![1, 2, 3]);
}

#[test]
fn test_ckp_corrupt_recovery() {
  use std::io::Write;

  run(async {
    let dir = tempfile::tempdir().unwrap();
    let ckp_path = dir.path().join("ckp.wal");

    // Write valid data first
    // 先写入有效数据
    {
      let mut ckp = jdb_ckp::open(dir.path(), &[]).await.unwrap().0;
      ckp.set_wal_ptr(1, 100).await.unwrap();
      ckp.set_wal_ptr(2, 200).await.unwrap();
    }

    // Append garbage to corrupt file
    // 追加垃圾数据损坏文件
    {
      let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&ckp_path)
        .unwrap();
      f.write_all(&[0xff, 0xfe, 0xfd, 0xfc, 0x00, 0x01]).unwrap();
    }

    // Should recover valid entries, ignore garbage
    // 应恢复有效条目，忽略垃圾
    {
      let (ckp, after) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
      let (wal_id, offset) = ckp.wal_id_offset().unwrap();
      assert_eq!(wal_id, 2);
      assert_eq!(offset, 200);
      assert!(after.is_some());
    }
  });
}

#[test]
fn test_ckp_truncated_entry_recovery() {
  use std::io::Write;

  run(async {
    let dir = tempfile::tempdir().unwrap();
    let ckp_path = dir.path().join("ckp.wal");

    // Write valid data
    // 写入有效数据
    {
      let mut ckp = jdb_ckp::open(dir.path(), &[]).await.unwrap().0;
      ckp.set_wal_ptr(1, 100).await.unwrap();
    }

    // Append partial/truncated entry (valid magic but incomplete)
    // 追加部分/截断条目（有效魔数但不完整）
    {
      let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&ckp_path)
        .unwrap();
      f.write_all(&[0x42, 0x01, 0x00, 0x00]).unwrap(); // magic + kind + partial data
    }

    // Should recover only complete entries
    // 应只恢复完整条目
    {
      let (ckp, _) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
      let (wal_id, offset) = ckp.wal_id_offset().unwrap();
      assert_eq!(wal_id, 1);
      assert_eq!(offset, 100);
    }
  });
}

#[test]
fn test_ckp_compact() {
  run(async {
    let dir = tempfile::tempdir().unwrap();
    let ckp_path = dir.path().join("ckp.wal");

    // Use small truncate threshold to trigger compaction
    // 使用小的截断阈值触发压缩
    let conf = [jdb_ckp::Conf::Truncate(5), jdb_ckp::Conf::Keep(2)];

    {
      let mut ckp = jdb_ckp::open(dir.path(), &conf).await.unwrap().0;
      for i in 0..10u64 {
        ckp.set_wal_ptr(i, i * 100).await.unwrap();
      }
    }

    // File should be compacted
    // 文件应被压缩
    let file_size = std::fs::metadata(&ckp_path).unwrap().len();
    // keep=2 means at most 2 saves, but implementation keeps up to keep+1 before pop
    // After compaction: at most (keep+1) saves * 22 bytes
    // keep=2 意味着最多 2 个保存点，但实现在 pop 前保留 keep+1
    // 压缩后：最多 (keep+1) * 22 字节
    assert!(
      file_size < 10 * 22,
      "file should be compacted, got {file_size}"
    );

    // Verify data integrity after compaction
    // 验证压缩后数据完整性
    {
      let (ckp, after) = jdb_ckp::open(dir.path(), &conf).await.unwrap();
      let (wal_id, offset) = ckp.wal_id_offset().unwrap();
      assert_eq!(wal_id, 9);
      assert_eq!(offset, 900);
      assert!(after.is_some());
    }
  });
}
