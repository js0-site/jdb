use aok::{OK, Void};
use jdb_vlog::{VlogReader, VlogWriter};
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_vlog_write_read() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let path = "/tmp/jdb_vlog_test.vlog";

    // Write blobs 写入 blob
    let mut writer = VlogWriter::create(path, 1).await.expect("create");

    let data1 = b"hello world";
    let ptr1 = writer.append(data1, 100).await.expect("append");

    let data2 = vec![0xABu8; 8192]; // Large blob 大 blob
    let ptr2 = writer.append(&data2, 200).await.expect("append");

    let data3 = b"small";
    let ptr3 = writer.append(data3, 300).await.expect("append");

    writer.sync().await.expect("sync");

    assert_eq!(ptr1.file_id, 1);
    assert_eq!(ptr1.len, data1.len() as u32);
    assert_eq!(ptr2.len, data2.len() as u32);
    assert_eq!(ptr3.len, data3.len() as u32);

    // Read back 读回
    let reader = VlogReader::open(path).await.expect("open");

    let r1 = reader.read(&ptr1).await.expect("read");
    assert_eq!(r1, data1);

    let r2 = reader.read(&ptr2).await.expect("read");
    assert_eq!(r2, data2);

    let r3 = reader.read(&ptr3).await.expect("read");
    assert_eq!(r3, data3);

    std::fs::remove_file(path).ok();
    info!("vlog write/read ok");
  });
  OK
}

#[test]
fn test_vlog_checksum() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let path = "/tmp/jdb_vlog_cksum.vlog";

    let mut writer = VlogWriter::create(path, 2).await.expect("create");
    let data = b"checksum test data";
    let ptr = writer.append(data, 999).await.expect("append");
    writer.sync().await.expect("sync");

    let reader = VlogReader::open(path).await.expect("open");
    let result = reader.read(&ptr).await.expect("read");
    assert_eq!(result, data);

    std::fs::remove_file(path).ok();
    info!("vlog checksum ok");
  });
  OK
}
