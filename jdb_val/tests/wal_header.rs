//! WAL header module tests / WAL 头部模块测试
//!
//! Since header module functions are pub(crate) only, we test through the public API.
//! 由于头部模块函数仅为 pub(crate)，我们通过公共 API 进行测试。

use jdb_val::Wal;

#[compio::test]
async fn test_wal_header_on_open() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::open(dir.path(), &[], None, |_, _| {}).await.unwrap();

  // Verify we can write data (implies header is valid)
  // 验证我们可以写入数据（意味着头部有效）
  wal.put(b"key", b"value").await.unwrap();
  wal.sync().await.unwrap();

  // Close and reopen
  // 关闭并重新打开
  drop(wal);
  let wal = Wal::open(dir.path(), &[], None, |_, _| {}).await.unwrap();

  // Should be able to read the data
  // 应该能够读取数据
  let ids: Vec<_> = wal.iter().collect();
  assert_eq!(ids.len(), 1);
}
