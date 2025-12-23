use jdb_comm::R;
use jdb_compress::Codec;
use jdb_vlog::{Reader, Writer};

fn run<F: std::future::Future<Output = R<()>>>(f: F) -> R<()> {
  compio::runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn test_write_read() -> R<()> {
  run(async {
    let dir = "/tmp/jdb_vlog_test_wr";
    std::fs::create_dir_all(dir).ok();

    // 写入
    let ptr = {
      let mut w = Writer::create(dir, 0, None).await?;
      let ptr = w.append(b"hello world").await?;
      w.sync().await?;
      ptr
    };

    // 读取
    {
      let r = Reader::new(dir, None);
      let data = r.read(&ptr).await?;
      assert_eq!(data, b"hello world");
    }

    std::fs::remove_dir_all(dir).ok();
    Ok(())
  })
}

#[test]
fn test_write_multiple() -> R<()> {
  run(async {
    let dir = "/tmp/jdb_vlog_test_multi";
    std::fs::create_dir_all(dir).ok();

    let ptrs = {
      let mut w = Writer::create(dir, 0, None).await?;
      let mut ptrs = Vec::new();
      for i in 0..10u32 {
        let data = format!("data-{i}");
        let ptr = w.append(data.as_bytes()).await?;
        ptrs.push((ptr, data));
      }
      w.sync().await?;
      ptrs
    };

    {
      let r = Reader::new(dir, None);
      for (ptr, expected) in &ptrs {
        let data = r.read(ptr).await?;
        assert_eq!(data, expected.as_bytes());
      }
    }

    std::fs::remove_dir_all(dir).ok();
    Ok(())
  })
}

#[test]
fn test_large_data() -> R<()> {
  run(async {
    let dir = "/tmp/jdb_vlog_test_large";
    std::fs::create_dir_all(dir).ok();

    // 写入大数据 (超过 PAGE_SIZE)
    let large = vec![0xABu8; 8192];
    let ptr = {
      let mut w = Writer::create(dir, 0, None).await?;
      let ptr = w.append(&large).await?;
      w.sync().await?;
      ptr
    };

    {
      let r = Reader::new(dir, None);
      let data = r.read(&ptr).await?;
      assert_eq!(data, large);
    }

    std::fs::remove_dir_all(dir).ok();
    Ok(())
  })
}

#[test]
fn test_with_compression() -> R<()> {
  run(async {
    let dir = "/tmp/jdb_vlog_test_compress";
    std::fs::create_dir_all(dir).ok();

    let codec = Some(Codec::Lz4);
    let data = b"hello world hello world hello world";

    let ptr = {
      let mut w = Writer::create(dir, 0, codec).await?;
      let ptr = w.append(data).await?;
      w.sync().await?;
      ptr
    };

    {
      let r = Reader::new(dir, codec);
      let result = r.read(&ptr).await?;
      assert_eq!(result, data);
    }

    std::fs::remove_dir_all(dir).ok();
    Ok(())
  })
}

#[test]
fn test_roll() -> R<()> {
  run(async {
    let dir = "/tmp/jdb_vlog_test_roll";
    std::fs::create_dir_all(dir).ok();

    let (ptr1, ptr2) = {
      let mut w = Writer::create(dir, 0, None).await?;
      let ptr1 = w.append(b"file0").await?;
      w.roll().await?;
      let ptr2 = w.append(b"file1").await?;
      w.sync().await?;
      (ptr1, ptr2)
    };

    assert_eq!(ptr1.file_id, 0);
    assert_eq!(ptr2.file_id, 1);

    {
      let r = Reader::new(dir, None);
      assert_eq!(r.read(&ptr1).await?, b"file0");
      assert_eq!(r.read(&ptr2).await?, b"file1");
    }

    std::fs::remove_dir_all(dir).ok();
    Ok(())
  })
}

#[test]
fn test_invalid_ptr() -> R<()> {
  run(async {
    let dir = "/tmp/jdb_vlog_test_invalid";
    std::fs::create_dir_all(dir).ok();

    let r = Reader::new(dir, None);
    let ptr = jdb_layout::BlobPtr::INVALID;
    let result = r.read(&ptr).await;
    assert!(result.is_err());

    std::fs::remove_dir_all(dir).ok();
    Ok(())
  })
}
