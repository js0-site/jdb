//! Tests for compact module
//! compact 模块测试

use std::io;

use aok::{OK, Void};
use jdb_fs::{AutoCompact, Compact, Decoded};
use tempfile::tempdir;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Simple key-value store for testing
/// 简单键值存储用于测试
#[derive(Default)]
struct KvStore {
  data: Vec<(u32, Vec<u8>)>,
}

impl Compact for KvStore {
  type Item<'a> = Vec<u8>;

  fn decode(&mut self, buf: &[u8]) -> io::Result<Decoded> {
    // Format: 4 bytes key + 4 bytes len + data
    // 格式：4 字节 key + 4 字节 len + data
    if buf.len() < 8 {
      return Ok(Decoded {
        len: 0,
        count: false,
      });
    }

    let key = u32::from_le_bytes(buf[..4].try_into().unwrap());
    let data_len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
    let total = 8 + data_len;

    if buf.len() < total {
      return Ok(Decoded {
        len: 0,
        count: false,
      });
    }

    let value = buf[8..total].to_vec();
    self.data.push((key, value));

    Ok(Decoded {
      len: total,
      count: true,
    })
  }

  fn rewrite(&self) -> impl Iterator<Item = Self::Item<'_>> {
    self.data.iter().map(|(k, v)| {
      let mut out = Vec::with_capacity(8 + v.len());
      out.extend_from_slice(&k.to_le_bytes());
      out.extend_from_slice(&(v.len() as u32).to_le_bytes());
      out.extend_from_slice(v);
      out
    })
  }
}

impl KvStore {
  fn encode(key: u32, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + value.len());
    out.extend_from_slice(&key.to_le_bytes());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
    out
  }

  fn put(&mut self, key: u32, value: &[u8]) -> Vec<u8> {
    self.data.push((key, value.to_vec()));
    Self::encode(key, value)
  }
}

#[compio::test]
async fn test_compact_open_empty() -> Void {
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
    ac.push(KvStore::encode(1, b"hello"), true).await?;
    ac.push(KvStore::encode(2, b"world"), true).await?;
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
    ac.push(KvStore::encode(1, b"first"), true).await?;
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
    ac.push(KvStore::encode(2, b"second"), true).await?;
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
    let item1 = ac.inner.put(1, b"aaa");
    let item2 = ac.inner.put(2, b"bbb");
    let item3 = ac.inner.put(3, b"ccc");
    ac.push_iter([(item1, true), (item2, true), (item3, true)])
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
    let item1 = ac.inner.put(1, b"x");
    let item2 = ac.inner.put(2, b"y");
    ac.push_iter([(item1, true), (item2, true)]).await?;
    ac.compact().await?;

    // Append after rewrite
    // 重写后追加
    let item3 = ac.inner.put(3, b"z");
    ac.push(item3, true).await?;
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
    let items = [
      (KvStore::encode(1, b"a"), true),
      (KvStore::encode(2, b"b"), true),
      (KvStore::encode(3, b"c"), true),
    ];
    ac.push_iter(items).await?;
    assert_eq!(ac.count, 3);
  }

  {
    let ac = AutoCompact::open(KvStore::default(), path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 3);
  }

  OK
}
