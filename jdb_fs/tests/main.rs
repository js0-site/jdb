use aok::{OK, Void};
use jdb_alloc::AlignedBuf;
use jdb_comm::PAGE_SIZE;
use jdb_fs::File;
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_file_rw() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let path = "/tmp/jdb_fs_test.dat";

    // Create and write 创建并写入
    let mut file = File::create(path).await.expect("create");
    let mut buf = AlignedBuf::page();
    buf[0] = 0xAB;
    buf[PAGE_SIZE - 1] = 0xCD;

    let _ = file.write_page(0, buf).await.expect("write");
    file.sync().await.expect("sync");

    // Read back 读回
    let file = File::open(path).await.expect("open");
    let buf = file.read_page(0).await.expect("read");

    assert_eq!(buf[0], 0xAB);
    assert_eq!(buf[PAGE_SIZE - 1], 0xCD);

    // Cleanup 清理
    std::fs::remove_file(path).ok();

    info!("file rw ok");
  });
  OK
}

#[test]
fn test_file_multi_page() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let path = "/tmp/jdb_fs_multi.dat";

    let mut file = File::create(path).await.expect("create");

    // Write 3 pages 写入 3 页
    for i in 0..3u32 {
      let mut buf = AlignedBuf::page();
      buf[0] = i as u8;
      let _ = file.write_page(i, buf).await.expect("write");
    }
    file.sync().await.expect("sync");

    // Verify 验证
    let file = File::open(path).await.expect("open");
    for i in 0..3u32 {
      let buf = file.read_page(i).await.expect("read");
      assert_eq!(buf[0], i as u8);
    }

    let size = file.size().await.expect("size");
    assert_eq!(size, 3 * PAGE_SIZE as u64);

    std::fs::remove_file(path).ok();

    info!("multi page ok");
  });
  OK
}
