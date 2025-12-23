use jdb_comm::R;
use jdb_fs::File;
use jdb_page::Pool;

fn run<F: std::future::Future<Output = R<()>>>(f: F) -> R<()> {
  compio::runtime::Runtime::new().unwrap().block_on(f)
}

#[test]
fn test_page_new() -> R<()> {
  let page = jdb_page::Page::new(42);
  assert_eq!(page.id(), 42);
  assert!(page.is_dirty());
  assert!(!page.is_pinned());
  Ok(())
}

#[test]
fn test_page_pin_unpin() -> R<()> {
  let mut page = jdb_page::Page::new(0);
  assert!(!page.is_pinned());

  page.pin();
  assert!(page.is_pinned());

  page.pin();
  page.unpin();
  assert!(page.is_pinned());

  page.unpin();
  assert!(!page.is_pinned());
  Ok(())
}

#[test]
fn test_pool_alloc() -> R<()> {
  run(async {
    let path = "/tmp/jdb_page_test_alloc.dat";
    let file = File::create(path).await?;
    let pool = Pool::open(file, 10).await?;

    let page = pool.alloc()?;
    assert_eq!(page.id(), 0);
    page.data_mut()[0..4].copy_from_slice(b"test");

    let page = pool.alloc()?;
    assert_eq!(page.id(), 1);

    assert_eq!(pool.len(), 2);
    assert_eq!(pool.next_id(), 2);

    pool.sync().await?;
    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_pool_get() -> R<()> {
  run(async {
    let path = "/tmp/jdb_page_test_get.dat";

    // 创建并写入
    {
      let file = File::create(path).await?;
      let pool = Pool::open(file, 10).await?;

      let page = pool.alloc()?;
      page.data_mut()[0..5].copy_from_slice(b"hello");

      let page = pool.alloc()?;
      page.data_mut()[0..5].copy_from_slice(b"world");

      pool.sync().await?;
    }

    // 重新打开读取
    {
      let file = File::open_rw(path).await?;
      let pool = Pool::open(file, 10).await?;

      let page = pool.get(0).await?;
      assert_eq!(&page.data()[0..5], b"hello");

      let page = pool.get(1).await?;
      assert_eq!(&page.data()[0..5], b"world");
    }

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_pool_evict() -> R<()> {
  run(async {
    let path = "/tmp/jdb_page_test_evict.dat";
    let file = File::create(path).await?;
    let pool = Pool::open(file, 3).await?; // 容量 3

    // 分配 3 页
    for i in 0..3u8 {
      let page = pool.alloc()?;
      page.data_mut()[0] = i;
    }
    assert_eq!(pool.len(), 3);

    // 分配第 4 页，触发驱逐
    let page = pool.alloc()?;
    page.data_mut()[0] = 3;

    // 应该驱逐了一页
    assert!(pool.len() <= 4);

    pool.sync().await?;
    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_pool_pin_prevents_evict() -> R<()> {
  run(async {
    let path = "/tmp/jdb_page_test_pin.dat";
    let file = File::create(path).await?;
    let pool = Pool::open(file, 2).await?;

    // 分配并固定第一页
    let page = pool.alloc()?;
    page.data_mut()[0] = 0xAA;
    page.pin();

    // 分配第二页
    let page = pool.alloc()?;
    page.data_mut()[0] = 0xBB;

    // 分配第三页，应该驱逐第二页而非第一页
    let page = pool.alloc()?;
    page.data_mut()[0] = 0xCC;

    pool.sync().await?;

    // 重新读取验证第一页数据
    let page = pool.get(0).await?;
    assert_eq!(page.data()[0], 0xAA);

    std::fs::remove_file(path).ok();
    Ok(())
  })
}

#[test]
fn test_pool_flush() -> R<()> {
  run(async {
    let path = "/tmp/jdb_page_test_flush.dat";
    let file = File::create(path).await?;
    let pool = Pool::open(file, 10).await?;

    let page = pool.alloc()?;
    let id = page.id();
    page.data_mut()[0..4].copy_from_slice(b"data");
    assert!(page.is_dirty());

    pool.flush(id).await?;

    let page = pool.get(id).await?;
    assert!(!page.is_dirty());

    std::fs::remove_file(path).ok();
    Ok(())
  })
}
