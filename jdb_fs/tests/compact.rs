//! Tests for compact module
//! compact 模块测试

use aok::{OK, Void};
use jdb_fs::{AutoCompact, Compact, Parse};
use tempfile::tempdir;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

const MAGIC: u8 = 0xAB;

/// Simple key-value store for testing
/// 简单键值存储用于测试
#[derive(Default)]
struct KvStore {
  data: Vec<(u32, Vec<u8>)>,
}

impl Parse for KvStore {
  const MAGIC: u8 = MAGIC;
  const LEN_BYTES: usize = 4;
  type Item<'a> = Vec<u8>;

  fn len(byte: &[u8]) -> usize {
    u32::from_le_bytes([byte[0], byte[1], byte[2], byte[3]]) as usize
  }

  fn len_bytes(len: usize) -> Vec<u8> {
    (len as u32).to_le_bytes().to_vec()
  }

  fn parse_item(bin: &[u8]) -> Option<Self::Item<'_>> {
    Some(bin.to_vec())
  }
}

impl Compact for KvStore {
  fn on_item(&mut self, item: Self::Item<'_>) -> bool {
    // Format in data: 4 bytes key + value
    // 数据格式：4 字节 key + value
    if item.len() < 4 {
      return false;
    }
    let key = u32::from_le_bytes([item[0], item[1], item[2], item[3]]);
    let value = item[4..].to_vec();
    self.data.push((key, value));
    true
  }

  fn rewrite(&self) -> impl Iterator<Item = Vec<u8>> {
    self.data.iter().map(|(k, v)| Self::encode_kv(*k, v))
  }
}

impl KvStore {
  /// Encode key-value pair as raw data (without parse format)
  /// 编码键值对为原始数据（不含 parse 格式）
  fn kv_data(key: u32, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + value.len());
    out.extend_from_slice(&key.to_le_bytes());
    out.extend_from_slice(value);
    out
  }

  /// Encode key-value pair with full parse format
  /// 编码键值对为完整 parse 格式
  fn encode_kv(key: u32, value: &[u8]) -> Vec<u8> {
    Self::encode(&Self::kv_data(key, value))
  }
}

#[compio::test]
async fn test_compact_empty() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
  assert!(ac.inner.data.is_empty());
  assert_eq!(ac.pos, 0);
  assert_eq!(ac.count, 0);

  OK
}

#[compio::test]
async fn test_compact_write_and_reload() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  {
    let mut ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    ac.push(&KvStore::kv_data(1, b"hello"), true).await?;
    ac.push(&KvStore::kv_data(2, b"world"), true).await?;
    assert_eq!(ac.count, 2);
  }

  {
    let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 2);
    assert_eq!(ac.inner.data[0], (1, b"hello".to_vec()));
    assert_eq!(ac.inner.data[1], (2, b"world".to_vec()));
    assert_eq!(ac.count, 2);
  }

  OK
}

#[compio::test]
async fn test_compact_append_is_real_append() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  // First write
  // 第一次写入
  let size1;
  {
    let mut ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    ac.push(&KvStore::kv_data(1, b"first"), true).await?;
    size1 = ac.pos;
  }

  // Verify file size
  // 验证文件大小
  let meta1 = std::fs::metadata(&path)?;
  assert_eq!(meta1.len(), size1);

  // Second open and append
  // 第二次打开并追加
  let size2;
  {
    let mut ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    assert_eq!(ac.pos, size1); // cursor at end
    ac.push(&KvStore::kv_data(2, b"second"), true).await?;
    size2 = ac.pos;
    assert!(size2 > size1);
  }

  // Verify file grew
  // 验证文件增长
  let meta2 = std::fs::metadata(&path)?;
  assert_eq!(meta2.len(), size2);

  // Third open: all data present
  // 第三次打开：所有数据都在
  {
    let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 2);
    assert_eq!(ac.inner.data[0], (1, b"first".to_vec()));
    assert_eq!(ac.inner.data[1], (2, b"second".to_vec()));
  }

  OK
}

#[compio::test]
async fn test_compact_rewrite_and_reload() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  // Write some data (use put to update inner.data for rewrite)
  // 写入一些数据（用 put 更新 inner.data 以便 rewrite）
  {
    let mut ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    let data1 = KvStore::kv_data(1, b"aaa");
    let data2 = KvStore::kv_data(2, b"bbb");
    let data3 = KvStore::kv_data(3, b"ccc");
    // Update inner.data for rewrite
    // 更新 inner.data 以便 rewrite
    ac.inner.data.push((1, b"aaa".to_vec()));
    ac.inner.data.push((2, b"bbb".to_vec()));
    ac.inner.data.push((3, b"ccc".to_vec()));
    ac.push_iter([
      (data1.as_slice(), true),
      (data2.as_slice(), true),
      (data3.as_slice(), true),
    ])
    .await?;

    // Force compact/rewrite
    // 强制压缩/重写
    ac.compact().await?;
    assert_eq!(ac.count, 0);
  }

  // Reload and verify data unchanged
  // 重新加载并验证数据不变
  {
    let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 3);
    assert_eq!(ac.inner.data[0], (1, b"aaa".to_vec()));
    assert_eq!(ac.inner.data[1], (2, b"bbb".to_vec()));
    assert_eq!(ac.inner.data[2], (3, b"ccc".to_vec()));
  }

  OK
}

#[compio::test]
async fn test_compact_rewrite_then_append() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  {
    let mut ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    let data1 = KvStore::kv_data(1, b"x");
    let data2 = KvStore::kv_data(2, b"y");
    ac.inner.data.push((1, b"x".to_vec()));
    ac.inner.data.push((2, b"y".to_vec()));
    ac.push_iter([(data1.as_slice(), true), (data2.as_slice(), true)])
      .await?;
    ac.compact().await?;

    // Append after rewrite
    // 重写后追加
    let data3 = KvStore::kv_data(3, b"z");
    ac.inner.data.push((3, b"z".to_vec()));
    ac.push(&data3, true).await?;
  }

  // Verify all data
  // 验证所有数据
  {
    let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 3);
    assert_eq!(ac.inner.data[0], (1, b"x".to_vec()));
    assert_eq!(ac.inner.data[1], (2, b"y".to_vec()));
    assert_eq!(ac.inner.data[2], (3, b"z".to_vec()));
  }

  OK
}

#[compio::test]
async fn test_compact_write_multiple() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  {
    let mut ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    let data1 = KvStore::kv_data(1, b"a");
    let data2 = KvStore::kv_data(2, b"b");
    let data3 = KvStore::kv_data(3, b"c");
    ac.push_iter([
      (data1.as_slice(), true),
      (data2.as_slice(), true),
      (data3.as_slice(), true),
    ])
    .await?;
    assert_eq!(ac.count, 3);
  }

  {
    let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 3);
  }

  OK
}


#[compio::test]
async fn test_compact_corrupted_crc() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  // Write valid data first
  // 先写入有效数据
  {
    let mut ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    ac.push(&KvStore::kv_data(1, b"first"), true).await?;
    ac.push(&KvStore::kv_data(2, b"second"), true).await?;
    ac.push(&KvStore::kv_data(3, b"third"), true).await?;
  }

  // Corrupt CRC of second record
  // 损坏第二条记录的 CRC
  {
    let mut data = std::fs::read(&path)?;
    // First record: magic(1) + len(4) + data(4+5) + crc(4) = 18 bytes
    // 第一条记录：magic(1) + len(4) + data(4+5) + crc(4) = 18 字节
    let first_len = 1 + 4 + (4 + 5) + 4;
    // Corrupt CRC at end of second record
    // 损坏第二条记录末尾的 CRC
    let second_len = 1 + 4 + (4 + 6) + 4;
    let crc_pos = first_len + second_len - 1;
    data[crc_pos] ^= 0xFF;
    std::fs::write(&path, data)?;
  }

  // Reload should skip corrupted record and recover third
  // 重新加载应跳过损坏记录并恢复第三条
  {
    let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    // First and third should be recovered
    // 第一条和第三条应该被恢复
    assert_eq!(ac.inner.data.len(), 2);
    assert_eq!(ac.inner.data[0], (1, b"first".to_vec()));
    assert_eq!(ac.inner.data[1], (3, b"third".to_vec()));
  }

  OK
}

#[compio::test]
async fn test_compact_corrupted_magic() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  // Write valid data first
  // 先写入有效数据
  {
    let mut ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    ac.push(&KvStore::kv_data(1, b"first"), true).await?;
    ac.push(&KvStore::kv_data(2, b"second"), true).await?;
    ac.push(&KvStore::kv_data(3, b"third"), true).await?;
  }

  // Corrupt magic of second record
  // 损坏第二条记录的 magic
  {
    let mut data = std::fs::read(&path)?;
    let first_len = 1 + 4 + (4 + 5) + 4;
    // Corrupt magic byte
    // 损坏 magic 字节
    data[first_len] = 0x00;
    std::fs::write(&path, data)?;
  }

  // Reload should skip corrupted record and recover third
  // 重新加载应跳过损坏记录并恢复第三条
  {
    let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 2);
    assert_eq!(ac.inner.data[0], (1, b"first".to_vec()));
    assert_eq!(ac.inner.data[1], (3, b"third".to_vec()));
  }

  OK
}

#[compio::test]
async fn test_compact_corrupted_first_record() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  // Write valid data
  // 写入有效数据
  {
    let mut ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    ac.push(&KvStore::kv_data(1, b"first"), true).await?;
    ac.push(&KvStore::kv_data(2, b"second"), true).await?;
  }

  // Corrupt first record magic
  // 损坏第一条记录的 magic
  {
    let mut data = std::fs::read(&path)?;
    data[0] = 0x00;
    std::fs::write(&path, data)?;
  }

  // Reload should skip first and recover second
  // 重新加载应跳过第一条并恢复第二条
  {
    let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 1);
    assert_eq!(ac.inner.data[0], (2, b"second".to_vec()));
  }

  OK
}

#[compio::test]
async fn test_compact_all_corrupted() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  // Write garbage data
  // 写入垃圾数据
  std::fs::write(&path, b"garbage data without magic")?;

  // Reload should handle gracefully
  // 重新加载应优雅处理
  {
    let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    assert!(ac.inner.data.is_empty());
  }

  OK
}
