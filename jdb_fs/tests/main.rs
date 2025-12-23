use jdb_alloc::{AlignedBuf, PAGE_SIZE};
use jdb_fs::{File, R};

fn run<F: std::future::Future<Output = R<()>>>(f: F) -> R<()> {
  compio::runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn test_create_and_size() -> R<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_create.dat";
    let file = File::create(path).await?;
    assert_eq!(file.size().await?, 0);
    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_write_read_at() -> R<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_write_read.dat";
    let mut file = File::create(path).await?;

    // 写入数据
    let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
    buf[0..5].copy_from_slice(b"hello");
    let _buf = file.write_at(0, buf).await?;

    // 同步
    file.sync().await?;

    // 读取数据
    let buf = file.read_at(0, PAGE_SIZE).await?;
    assert_eq!(&buf[0..5], b"hello");

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_write_read_page() -> R<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_page.dat";
    let mut file = File::create(path).await?;

    // 写入第 0 页
    let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
    buf[0..4].copy_from_slice(b"page");
    buf[PAGE_SIZE - 4..].copy_from_slice(b"end!");
    let _buf = file.write_page(0, buf).await?;

    // 写入第 1 页
    let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
    buf[0..5].copy_from_slice(b"page1");
    let _buf = file.write_page(1, buf).await?;

    file.sync().await?;

    // 读取第 0 页
    let buf = file.read_page(0).await?;
    assert_eq!(&buf[0..4], b"page");
    assert_eq!(&buf[PAGE_SIZE - 4..], b"end!");

    // 读取第 1 页
    let buf = file.read_page(1).await?;
    assert_eq!(&buf[0..5], b"page1");

    // 检查文件大小
    assert_eq!(file.size().await?, (PAGE_SIZE * 2) as u64);

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_read_pages() -> R<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_pages.dat";
    let mut file = File::create(path).await?;

    // 写入 4 页
    for i in 0..4u32 {
      let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
      buf[0] = i as u8;
      let _buf = file.write_page(i, buf).await?;
    }
    file.sync().await?;

    // 读取 4 页
    let buf = file.read_pages(0, 4).await?;
    assert_eq!(buf.len(), PAGE_SIZE * 4);
    assert_eq!(buf[0], 0);
    assert_eq!(buf[PAGE_SIZE], 1);
    assert_eq!(buf[PAGE_SIZE * 2], 2);
    assert_eq!(buf[PAGE_SIZE * 3], 3);

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_open_rw() -> R<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_open_rw.dat";

    // 创建并写入
    {
      let mut file = File::create(path).await?;
      let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
      buf[0..4].copy_from_slice(b"test");
      let _buf = file.write_page(0, buf).await?;
      file.sync().await?;
    }

    // 重新打开并读取
    {
      let file = File::open_rw(path).await?;
      let buf = file.read_page(0).await?;
      assert_eq!(&buf[0..4], b"test");
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_open_readonly() -> R<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_readonly.dat";

    // 先创建文件
    {
      let mut file = File::create(path).await?;
      let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
      buf[0..4].copy_from_slice(b"read");
      let _buf = file.write_page(0, buf).await?;
      file.sync().await?;
    }

    // 只读打开
    {
      let file = File::open(path).await?;
      let buf = file.read_page(0).await?;
      assert_eq!(&buf[0..4], b"read");
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}
