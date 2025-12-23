use aok::{OK, Void};
use jdb_comm::PageID;
use jdb_fs::File;
use jdb_page::{BufferPool, PageState};
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_buffer_pool() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let path = "/tmp/jdb_page_test.dat";

    // Create file 创建文件
    let file = File::create(path).await.expect("create");

    let mut pool = BufferPool::new(file, 10);

    // Allocate pages 分配页面
    let p0 = pool.alloc(PageID::new(0));
    p0.buf[0] = 0xAA;
    p0.mark_dirty();

    let p1 = pool.alloc(PageID::new(1));
    p1.buf[0] = 0xBB;
    p1.mark_dirty();

    // Flush all 刷新所有
    pool.flush_all().await.expect("flush_all");

    // Reopen and verify 重新打开并验证
    let file = File::open(path).await.expect("open");
    let mut pool = BufferPool::new(file, 10);

    let p0 = pool.get(PageID::new(0)).await.expect("get");
    assert_eq!(p0.buf[0], 0xAA);
    assert_eq!(p0.state, PageState::Clean);

    let p1 = pool.get(PageID::new(1)).await.expect("get");
    assert_eq!(p1.buf[0], 0xBB);

    std::fs::remove_file(path).ok();
    info!("buffer pool ok");
  });
  OK
}

#[test]
fn test_page_pin() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let path = "/tmp/jdb_page_pin.dat";

    let file = File::create(path).await.expect("create");
    let mut pool = BufferPool::new(file, 2);

    // Allocate and pin 分配并固定
    let p0 = pool.alloc(PageID::new(0));
    p0.pin();
    assert!(p0.is_pinned());

    p0.unpin();
    assert!(!p0.is_pinned());

    std::fs::remove_file(path).ok();
    info!("page pin ok");
  });
  OK
}

#[test]
fn test_eviction() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let path = "/tmp/jdb_page_evict.dat";

    let file = File::create(path).await.expect("create");
    let mut pool = BufferPool::new(file, 2);

    // Fill pool 填满池
    let p0 = pool.alloc(PageID::new(0));
    p0.buf[0] = 0x11;
    p0.mark_dirty();

    let p1 = pool.alloc(PageID::new(1));
    p1.buf[0] = 0x22;
    p1.mark_dirty();

    // Allocate third, should evict one 分配第三个，应驱逐一个
    let p2 = pool.alloc(PageID::new(2));
    p2.buf[0] = 0x33;
    p2.mark_dirty();

    pool.flush_all().await.expect("flush");

    std::fs::remove_file(path).ok();
    info!("eviction ok");
  });
  OK
}
