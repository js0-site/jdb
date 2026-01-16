//! Tests for BufFile
//! BufFile 测试

use compio::io::{AsyncReadAt, AsyncWrite};
use jdb_fs::BufFile;
use tempfile::tempdir;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

async fn create_buf_file() -> (BufFile, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let path = dir.path().join("test.dat");
  let file = compio_fs::OpenOptions::new()
    .read(true)
    .write(true)
    .create(true)
    .open(&path)
    .await
    .unwrap();
  // buf_max=4096
  (BufFile::new(file, 0), dir)
}

#[compio::test]
async fn test_write_and_flush() {
  let (mut f, _dir) = create_buf_file().await;

  let data = b"hello world";
  let compio::BufResult(r, _) = f.write(data.as_slice()).await;
  assert_eq!(r.unwrap(), 11);
  assert_eq!(f.pos(), 11);

  f.flush().await;
}

#[compio::test]
async fn test_read_from_buf0() {
  let (mut f, _dir) = create_buf_file().await;

  // Write data (goes to buf0 since nothing is flushing)
  // 写入数据（进入 buf0，因为没有刷盘）
  let data = b"0123456789";
  let compio::BufResult(r, _) = f.write(data.as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Read from buffer before flush
  // 刷盘前从缓冲读取
  let buf = vec![0u8; 5];
  let compio::BufResult(r, buf) = f.read_at(buf, 2).await;
  assert_eq!(r.unwrap(), 5);
  assert_eq!(&buf, b"23456");

  f.flush().await;
}

#[compio::test]
async fn test_read_from_flushing_buf() {
  let (mut f, _dir) = create_buf_file().await;

  // Write first batch
  // 写入第一批
  let data1 = b"AAAAAAAAAA";
  let compio::BufResult(r, _) = f.write(data1.as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Write second batch (should go to buf1 while buf0 is flushing)
  // 写入第二批（应进入 buf1，因为 buf0 正在刷盘）
  let data2 = b"BBBBBBBBBB";
  let compio::BufResult(r, _) = f.write(data2.as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Read from first batch (might be in flushing buf or already flushed)
  // 从第一批读取（可能在刷盘缓冲或已刷盘）
  let buf = vec![0u8; 5];
  let compio::BufResult(r, buf) = f.read_at(buf, 0).await;
  assert_eq!(r.unwrap(), 5);
  assert_eq!(&buf, b"AAAAA");

  // Read from second batch (in current write buf)
  // 从第二批读取（在当前写入缓冲）
  let buf = vec![0u8; 5];
  let compio::BufResult(r, buf) = f.read_at(buf, 10).await;
  assert_eq!(r.unwrap(), 5);
  assert_eq!(&buf, b"BBBBB");

  f.flush().await;
}

#[compio::test]
async fn test_read_across_boundary() {
  let (mut f, _dir) = create_buf_file().await;

  let data = b"0123456789ABCDEF";
  let compio::BufResult(r, _) = f.write(data.as_slice()).await;
  assert_eq!(r.unwrap(), 16);

  let buf = vec![0u8; 4];
  let compio::BufResult(r, buf) = f.read_at(buf, 8).await;
  assert_eq!(r.unwrap(), 4);
  assert_eq!(&buf, b"89AB");

  f.flush().await;
}

#[compio::test]
async fn test_read_after_flush() {
  let (mut f, _dir) = create_buf_file().await;

  let data = b"test data for read after flush";
  let compio::BufResult(r, _) = f.write(data.as_slice()).await;
  assert_eq!(r.unwrap(), data.len());

  f.flush().await;

  // Read from file (not buffer)
  // 从文件读取（非缓冲）
  let buf = vec![0u8; 9];
  let compio::BufResult(r, buf) = f.read_at(buf, 5).await;
  assert_eq!(r.unwrap(), 9);
  assert_eq!(&buf, b"data for ");
}

#[compio::test]
async fn test_multiple_writes_and_reads() {
  let (mut f, _dir) = create_buf_file().await;

  // Multiple writes
  // 多次写入
  for i in 0..10 {
    let data: Vec<u8> = format!("chunk{i:02}___").into_bytes();
    let compio::BufResult(r, _) = f.write(data).await;
    assert_eq!(r.unwrap(), 10);
  }

  // Read various positions
  // 读取不同位置
  let buf = vec![0u8; 10];
  let compio::BufResult(r, buf) = f.read_at(buf, 0).await;
  assert_eq!(r.unwrap(), 10);
  assert_eq!(&buf, b"chunk00___");

  let buf = vec![0u8; 10];
  let compio::BufResult(r, buf) = f.read_at(buf, 50).await;
  assert_eq!(r.unwrap(), 10);
  assert_eq!(&buf, b"chunk05___");

  f.flush().await;

  // Read after flush
  // 刷盘后读取
  let buf = vec![0u8; 10];
  let compio::BufResult(r, buf) = f.read_at(buf, 90).await;
  assert_eq!(r.unwrap(), 10);
  assert_eq!(&buf, b"chunk09___");
}

#[compio::test]
async fn test_sync() {
  let (mut f, _dir) = create_buf_file().await;

  let data = b"sync test";
  let compio::BufResult(r, _) = f.write(data.as_slice()).await;
  assert_eq!(r.unwrap(), 9);

  f.sync().await.unwrap();
}

#[compio::test]
async fn test_read_empty() {
  let (f, _dir) = create_buf_file().await;

  let buf = vec![0u8; 10];
  let compio::BufResult(r, _) = f.read_at(buf, 0).await;
  // Read from empty file returns 0
  // 从空文件读取返回 0
  assert_eq!(r.unwrap(), 0);
}

#[compio::test]
async fn test_write_empty() {
  let (mut f, _dir) = create_buf_file().await;

  let data: &[u8] = b"";
  let compio::BufResult(r, _) = f.write(data).await;
  assert_eq!(r.unwrap(), 0);
  assert_eq!(f.pos(), 0);
}

#[compio::test]
async fn test_read_while_flushing_buf0() {
  let (mut f, _dir) = create_buf_file().await;

  // Write to buf0
  // 写入 buf0
  let data1 = b"AAAA1111BBBB2222";
  let compio::BufResult(r, _) = f.write(data1.as_slice()).await;
  assert_eq!(r.unwrap(), 16);

  // This triggers flush of buf0, subsequent writes go to buf1
  // 这会触发 buf0 刷盘，后续写入进入 buf1

  // Write to buf1 while buf0 is flushing
  // buf0 刷盘时写入 buf1
  let data2 = b"CCCC3333DDDD4444";
  let compio::BufResult(r, _) = f.write(data2.as_slice()).await;
  assert_eq!(r.unwrap(), 16);

  // Read from buf0 (flushing)
  // 从 buf0 读取（正在刷盘）
  let buf = vec![0u8; 4];
  let compio::BufResult(r, buf) = f.read_at(buf, 0).await;
  assert_eq!(r.unwrap(), 4);
  assert_eq!(&buf, b"AAAA");

  let buf = vec![0u8; 4];
  let compio::BufResult(r, buf) = f.read_at(buf, 8).await;
  assert_eq!(r.unwrap(), 4);
  assert_eq!(&buf, b"BBBB");

  // Read from buf1 (current write buf)
  // 从 buf1 读取（当前写入缓冲）
  let buf = vec![0u8; 4];
  let compio::BufResult(r, buf) = f.read_at(buf, 16).await;
  assert_eq!(r.unwrap(), 4);
  assert_eq!(&buf, b"CCCC");

  let buf = vec![0u8; 4];
  let compio::BufResult(r, buf) = f.read_at(buf, 24).await;
  assert_eq!(r.unwrap(), 4);
  assert_eq!(&buf, b"DDDD");

  f.flush().await;
}

#[compio::test]
async fn test_read_partial_from_buf() {
  let (mut f, _dir) = create_buf_file().await;

  let data = b"0123456789ABCDEFGHIJ";
  let compio::BufResult(r, _) = f.write(data.as_slice()).await;
  assert_eq!(r.unwrap(), 20);

  // Read less than available
  // 读取少于可用数据
  let buf = vec![0u8; 3];
  let compio::BufResult(r, buf) = f.read_at(buf, 5).await;
  assert_eq!(r.unwrap(), 3);
  assert_eq!(&buf, b"567");

  // Read at end of buffer (partial)
  // 在缓冲末尾读取（部分）
  let buf = vec![0u8; 10];
  let compio::BufResult(r, buf) = f.read_at(buf, 15).await;
  assert_eq!(r.unwrap(), 5);
  assert_eq!(&buf[..5], b"FGHIJ");

  f.flush().await;
}

#[compio::test]
async fn test_read_beyond_buf() {
  let (mut f, _dir) = create_buf_file().await;

  let data = b"short";
  let compio::BufResult(r, _) = f.write(data.as_slice()).await;
  assert_eq!(r.unwrap(), 5);

  // Read beyond buffer (should return partial or from file)
  // 读取超出缓冲（应返回部分或从文件读取）
  let buf = vec![0u8; 10];
  let compio::BufResult(r, buf) = f.read_at(buf, 3).await;
  assert_eq!(r.unwrap(), 2);
  assert_eq!(&buf[..2], b"rt");

  f.flush().await;
}

#[compio::test]
async fn test_alternating_buf_writes() {
  let (mut f, _dir) = create_buf_file().await;

  // Write batch 1 -> buf0
  // 写入批次1 -> buf0
  let compio::BufResult(r, _) = f.write(b"BATCH1____".as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Write batch 2 -> triggers flush, goes to buf1
  // 写入批次2 -> 触发刷盘，进入 buf1
  let compio::BufResult(r, _) = f.write(b"BATCH2____".as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Write batch 3 -> may go to buf0 or buf1 depending on flush state
  // 写入批次3 -> 根据刷盘状态可能进入 buf0 或 buf1
  let compio::BufResult(r, _) = f.write(b"BATCH3____".as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Read all batches
  // 读取所有批次
  let buf = vec![0u8; 6];
  let compio::BufResult(r, buf) = f.read_at(buf, 0).await;
  assert_eq!(r.unwrap(), 6);
  assert_eq!(&buf, b"BATCH1");

  let buf = vec![0u8; 6];
  let compio::BufResult(r, buf) = f.read_at(buf, 10).await;
  assert_eq!(r.unwrap(), 6);
  assert_eq!(&buf, b"BATCH2");

  let buf = vec![0u8; 6];
  let compio::BufResult(r, buf) = f.read_at(buf, 20).await;
  assert_eq!(r.unwrap(), 6);
  assert_eq!(&buf, b"BATCH3");

  f.flush().await;
}

#[compio::test]
async fn test_read_not_in_buf() {
  let (mut f, _dir) = create_buf_file().await;

  let data = b"hello";
  let compio::BufResult(r, _) = f.write(data.as_slice()).await;
  assert_eq!(r.unwrap(), 5);

  // Read at position before buffer offset (should fail or read from file)
  // 在缓冲偏移之前的位置读取（应失败或从文件读取）
  // Since we start at pos 0, this tests reading at a gap
  // 因为我们从 pos 0 开始，这测试读取间隙

  f.flush().await;

  // After flush, read from file
  // 刷盘后，从文件读取
  let buf = vec![0u8; 5];
  let compio::BufResult(r, buf) = f.read_at(buf, 0).await;
  assert_eq!(r.unwrap(), 5);
  assert_eq!(&buf, b"hello");
}

#[compio::test]
async fn test_read_cross_disk_buf() {
  let (mut f, _dir) = create_buf_file().await;

  // Write first part and flush to disk
  // 写入第一部分并刷盘
  let data1 = b"DISK_DATA_";
  let compio::BufResult(r, _) = f.write(data1.as_slice()).await;
  assert_eq!(r.unwrap(), 10);
  f.flush().await;

  // Write second part (in buffer)
  // 写入第二部分（在缓冲中）
  let data2 = b"BUF_DATA__";
  let compio::BufResult(r, _) = f.write(data2.as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Read across disk-buf boundary
  // 跨磁盘-缓冲边界读取
  let buf = vec![0u8; 14];
  let compio::BufResult(r, buf) = f.read_at(buf, 5).await;
  assert_eq!(r.unwrap(), 14);
  assert_eq!(&buf, b"DATA_BUF_DATA_");

  f.flush().await;
}

#[compio::test]
async fn test_read_cross_buf_buf() {
  let (mut f, _dir) = create_buf_file().await;

  // Write to buf0
  // 写入 buf0
  let data1 = b"BUF0_DATA_";
  let compio::BufResult(r, _) = f.write(data1.as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Write to buf1 (triggers flush of buf0)
  // 写入 buf1（触发 buf0 刷盘）
  let data2 = b"BUF1_DATA_";
  let compio::BufResult(r, _) = f.write(data2.as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Read across buf0-buf1 boundary (both in memory)
  // 跨 buf0-buf1 边界读取（都在内存中）
  let buf = vec![0u8; 14];
  let compio::BufResult(r, buf) = f.read_at(buf, 5).await;
  assert_eq!(r.unwrap(), 14);
  assert_eq!(&buf, b"DATA_BUF1_DATA");

  f.flush().await;
}

#[compio::test]
async fn test_read_cross_disk_buf_buf() {
  let (mut f, _dir) = create_buf_file().await;

  // Write and flush to disk
  // 写入并刷盘
  let data1 = b"DISK______";
  let compio::BufResult(r, _) = f.write(data1.as_slice()).await;
  assert_eq!(r.unwrap(), 10);
  f.flush().await;

  // Write to buf0
  // 写入 buf0
  let data2 = b"BUF0______";
  let compio::BufResult(r, _) = f.write(data2.as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Write to buf1
  // 写入 buf1
  let data3 = b"BUF1______";
  let compio::BufResult(r, _) = f.write(data3.as_slice()).await;
  assert_eq!(r.unwrap(), 10);

  // Read across disk-buf0-buf1
  // 跨磁盘-buf0-buf1 读取
  let buf = vec![0u8; 24];
  let compio::BufResult(r, buf) = f.read_at(buf, 3).await;
  assert_eq!(r.unwrap(), 24);
  assert_eq!(&buf, b"K______BUF0______BUF1___");

  f.flush().await;
}

#[compio::test]
async fn test_read_full_cross_boundary() {
  let (mut f, _dir) = create_buf_file().await;

  // Write 30 bytes and flush
  // 写入 30 字节并刷盘
  let data1: Vec<u8> = (0..30).collect();
  let compio::BufResult(r, _) = f.write(data1).await;
  assert_eq!(r.unwrap(), 30);
  f.flush().await;

  // Write 30 more bytes (in buffer)
  // 再写入 30 字节（在缓冲中）
  let data2: Vec<u8> = (30..60).collect();
  let compio::BufResult(r, _) = f.write(data2).await;
  assert_eq!(r.unwrap(), 30);

  // Read across boundary
  // 跨边界读取
  let buf = vec![0u8; 20];
  let compio::BufResult(r, buf) = f.read_at(buf, 20).await;
  assert_eq!(r.unwrap(), 20);
  let expected: Vec<u8> = (20..40).collect();
  assert_eq!(buf, expected);

  f.flush().await;
}
