//! WAL header module tests / WAL 头部模块测试
//!
//! Since header module functions are pub(crate) only, we test through the public API.
//! 由于头部模块函数仅为 pub(crate)，我们通过公共 API 进行测试。

use jdb_val::Wal;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[compio::test]
async fn test_wal_header_on_open() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);

  // Opening should create a valid header / 打开应该创建有效的头部
  let _ = wal.open(None).await.unwrap();

  // Verify we can write data (implies header is valid) / 验证我们可以写入数据（意味着头部有效）
  wal.put(b"key", b"value").await.unwrap();
  wal.sync_all().await.unwrap();

  // Close and reopen / 关闭并重新打开
  drop(wal);
  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();

  // Should be able to read the data / 应该能够读取数据
  let ids: Vec<_> = wal.iter().collect();
  assert_eq!(ids.len(), 1);
}
