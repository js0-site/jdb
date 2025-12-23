use jdb_wal::{Reader, Writer};

// Result type alias for tests
pub type R<T> = Result<T, Box<dyn std::error::Error>>;

fn run<F: std::future::Future<Output = R<()>>>(f: F) -> R<()> {
  compio::runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn test_write_read_single() -> R<()> {
  run(async {
    let path = "/tmp/jdb_wal_test_single.wal";

    // 写入
    {
      let mut w = Writer::create(path).await?;
      let lsn = w.append(b"hello").await?;
      assert_eq!(lsn, 1);
      w.sync().await?;
    }

    // 读取
    {
      let mut r = Reader::open(path).await?;
      let (lsn, data) = r.next().await?.unwrap();
      assert_eq!(lsn, 1);
      assert_eq!(data, b"hello");
      assert!(r.next().await?.is_none());
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_write_read_multiple() -> R<()> {
  run(async {
    let path = "/tmp/jdb_wal_test_multi.wal";

    // 写入多条
    {
      let mut w = Writer::create(path).await?;
      for i in 0..10u32 {
        let lsn = w.append(&i.to_le_bytes()).await?;
        assert_eq!(lsn, i as u64 + 1);
      }
      w.sync().await?;
    }

    // 读取
    {
      let mut r = Reader::open(path).await?;
      for i in 0..10u32 {
        let (lsn, data) = r.next().await?.unwrap();
        assert_eq!(lsn, i as u64 + 1);
        assert_eq!(data, i.to_le_bytes());
      }
      assert!(r.next().await?.is_none());
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_last_lsn() -> R<()> {
  run(async {
    let path = "/tmp/jdb_wal_test_last_lsn.wal";

    {
      let mut w = Writer::create(path).await?;
      for i in 0..5 {
        w.append(&[i]).await?;
      }
      w.sync().await?;
    }

    {
      let mut r = Reader::open(path).await?;
      let last = r.last_lsn().await?;
      assert_eq!(last, 5);
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_cross_page() -> R<()> {
  run(async {
    let path = "/tmp/jdb_wal_test_cross_page.wal";

    // 写入大量数据触发跨页
    {
      let mut w = Writer::create(path).await?;
      let data = vec![0xABu8; 1000];
      for i in 0..10 {
        let lsn = w.append(&data).await?;
        assert_eq!(lsn, i + 1);
      }
      w.sync().await?;
    }

    // 读取验证
    {
      let mut r = Reader::open(path).await?;
      let data = vec![0xABu8; 1000];
      for i in 0..10 {
        let (lsn, d) = r.next().await?.unwrap();
        assert_eq!(lsn, i + 1);
        assert_eq!(d, data);
      }
      assert!(r.next().await?.is_none());
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_empty_file() -> R<()> {
  run(async {
    let path = "/tmp/jdb_wal_test_empty.wal";

    {
      let w = Writer::create(path).await?;
      // 不写入任何数据，直接关闭
      drop(w);
    }

    {
      let mut r = Reader::open(path).await?;
      assert!(r.next().await?.is_none());
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_reopen_append() -> R<()> {
  run(async {
    let path = "/tmp/jdb_wal_test_reopen.wal";

    // 第一次写入
    {
      let mut w = Writer::create(path).await?;
      w.append(b"first").await?;
      w.sync().await?;
    }

    // 重新打开追加
    {
      let mut w = Writer::open(path).await?;
      // 恢复 LSN
      let mut r = Reader::open(path).await?;
      let last = r.last_lsn().await?;
      w.set_lsn(last + 1);

      w.append(b"second").await?;
      w.sync().await?;
    }

    // 读取验证
    {
      let mut r = Reader::open(path).await?;
      let (lsn1, d1) = r.next().await?.unwrap();
      assert_eq!(lsn1, 1);
      assert_eq!(d1, b"first");

      let (lsn2, d2) = r.next().await?.unwrap();
      assert_eq!(lsn2, 2);
      assert_eq!(d2, b"second");
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}
