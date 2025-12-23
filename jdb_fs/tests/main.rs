use jdb_alloc::{AlignedBuf, PAGE_SIZE};
use jdb_fs::{File, Result};

fn run<F: std::future::Future<Output = Result<()>>>(f: F) -> Result<()> {
  compio::runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn test_create_and_size() -> Result<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_create.dat";
    let file = File::create(path).await?;
    assert_eq!(file.size().await?, 0);
    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_write_read_at() -> Result<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_write_read.dat";
    let file = File::create(path).await?;

    // 写入数据 Write data
    let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
    buf[0..5].copy_from_slice(b"hello");
    let _buf = file.write_at(buf, 0).await?;

    file.sync_data().await?;

    // 读取数据 Read data
    let buf = AlignedBuf::with_cap(PAGE_SIZE)?;
    let buf = file.read_at(buf, 0).await?;
    assert_eq!(&buf[0..5], b"hello");

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_write_read_pages() -> Result<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_page.dat";
    let file = File::create(path).await?;

    // 写入第 0 页 Write page 0
    let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
    buf[0..4].copy_from_slice(b"page");
    buf[PAGE_SIZE - 4..].copy_from_slice(b"end!");
    let _buf = file.write_at(buf, 0).await?;

    // 写入第 1 页 Write page 1
    let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
    buf[0..5].copy_from_slice(b"page1");
    let _buf = file.write_at(buf, PAGE_SIZE as u64).await?;

    file.sync_data().await?;

    // 读取第 0 页 Read page 0
    let buf = AlignedBuf::with_cap(PAGE_SIZE)?;
    let buf = file.read_at(buf, 0).await?;
    assert_eq!(&buf[0..4], b"page");
    assert_eq!(&buf[PAGE_SIZE - 4..], b"end!");

    // 读取第 1 页 Read page 1
    let buf = AlignedBuf::with_cap(PAGE_SIZE)?;
    let buf = file.read_at(buf, PAGE_SIZE as u64).await?;
    assert_eq!(&buf[0..5], b"page1");

    // 检查文件大小 Check file size
    assert_eq!(file.size().await?, (PAGE_SIZE * 2) as u64);

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_read_multi_pages() -> Result<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_pages.dat";
    let file = File::create(path).await?;

    // 写入 4 页 Write 4 pages
    for i in 0..4u32 {
      let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
      buf[0] = i as u8;
      let _buf = file.write_at(buf, i as u64 * PAGE_SIZE as u64).await?;
    }
    file.sync_data().await?;

    // 读取 4 页 Read 4 pages
    let buf = AlignedBuf::with_cap(PAGE_SIZE * 4)?;
    let buf = file.read_at(buf, 0).await?;
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
fn test_open_rw() -> Result<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_open_rw.dat";

    // 创建并写入 Create and write
    {
      let file = File::create(path).await?;
      let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
      buf[0..4].copy_from_slice(b"test");
      let _buf = file.write_at(buf, 0).await?;
      file.sync_data().await?;
    }

    // 重新打开并读取 Reopen and read
    {
      let file = File::open_rw(path).await?;
      let buf = AlignedBuf::with_cap(PAGE_SIZE)?;
      let buf = file.read_at(buf, 0).await?;
      assert_eq!(&buf[0..4], b"test");
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_open_readonly() -> Result<()> {
  run(async {
    let path = "/tmp/jdb_fs_test_readonly.dat";

    // 先创建文件 Create file first
    {
      let file = File::create(path).await?;
      let mut buf = AlignedBuf::zeroed(PAGE_SIZE)?;
      buf[0..4].copy_from_slice(b"read");
      let _buf = file.write_at(buf, 0).await?;
      file.sync_data().await?;
    }

    // 只读打开 Open readonly
    {
      let file = File::open(path).await?;
      let buf = AlignedBuf::with_cap(PAGE_SIZE)?;
      let buf = file.read_at(buf, 0).await?;
      assert_eq!(&buf[0..4], b"read");
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}
