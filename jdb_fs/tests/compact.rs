//! Tests for compact module
//! compact 模块测试

use aok::{OK, Void};
use jdb_fs::{AutoCompact, Compact, DataLen, Item};
use tempfile::tempdir;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

const MAGIC: u8 = 0xAB;

/// Key-value entry head (fixed size)
/// 键值条目头（定长）
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned,
)]
#[repr(C, packed)]
struct KvHead {
  key: u32,
  data_len: u32,
}

impl DataLen for KvHead {
  fn data_len(&self) -> usize {
    self.data_len as usize
  }
}

impl KvHead {
  fn new(key: u32, data_len: u32) -> Self {
    Self { key, data_len }
  }
}

/// Simple key-value store for testing
/// 简单键值存储用于测试
#[derive(Default)]
struct KvStore {
  data: Vec<KvHead>,
}

impl Item for KvStore {
  const MAGIC: u8 = MAGIC;
  type Head = KvHead;
}

impl Compact for KvStore {
  fn on_data(&mut self, data: Self::Head) -> bool {
    self.data.push(data);
    true
  }

  fn rewrite(&self) -> impl Iterator<Item = &Self::Head> {
    self.data.iter()
  }
}

#[compio::test]
async fn test_compact_empty() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  let ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
  assert!(ac.inner.data.is_empty());
  assert_eq!(ac.pos, 0);
  assert_eq!(ac.count, 0);

  OK
}

#[compio::test]
async fn test_compact_write_and_reload() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  let head1 = KvHead::new(1, 0);
  let head2 = KvHead::new(2, 0);

  {
    let mut ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    ac.push(head1, true).await?;
    ac.push(head2, true).await?;
    assert_eq!(ac.count, 2);
  }

  {
    let ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 2);
    assert_eq!(ac.inner.data[0], head1);
    assert_eq!(ac.inner.data[1], head2);
    assert_eq!(ac.count, 2);
  }

  OK
}

#[compio::test]
async fn test_compact_append_is_real_append() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  let head1 = KvHead::new(1, 0);
  let head2 = KvHead::new(2, 0);

  // First write
  // 第一次写入
  let size1;
  {
    let mut ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    ac.push(head1, true).await?;
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
    let mut ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    assert_eq!(ac.pos, size1);
    ac.push(head2, true).await?;
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
    let ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 2);
    assert_eq!(ac.inner.data[0], head1);
    assert_eq!(ac.inner.data[1], head2);
  }

  OK
}

#[compio::test]
async fn test_compact_rewrite_and_reload() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  let head1 = KvHead::new(1, 0);
  let head2 = KvHead::new(2, 0);
  let head3 = KvHead::new(3, 0);

  {
    let mut ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    ac.inner.data.push(head1);
    ac.inner.data.push(head2);
    ac.inner.data.push(head3);
    ac.push_iter([(head1, true), (head2, true), (head3, true)])
      .await?;

    // Force compact/rewrite
    // 强制压缩/重写
    ac.compact().await?;
    assert_eq!(ac.count, 0);
  }

  // Reload and verify data unchanged
  // 重新加载并验证数据不变
  {
    let ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 3);
    assert_eq!(ac.inner.data[0], head1);
    assert_eq!(ac.inner.data[1], head2);
    assert_eq!(ac.inner.data[2], head3);
  }

  OK
}

#[compio::test]
async fn test_compact_rewrite_then_append() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  let head1 = KvHead::new(1, 0);
  let head2 = KvHead::new(2, 0);
  let head3 = KvHead::new(3, 0);

  {
    let mut ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    ac.inner.data.push(head1);
    ac.inner.data.push(head2);
    ac.push_iter([(head1, true), (head2, true)]).await?;
    ac.compact().await?;

    // Append after rewrite
    // 重写后追加
    ac.inner.data.push(head3);
    ac.push(head3, true).await?;
  }

  // Verify all data
  // 验证所有数据
  {
    let ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 3);
    assert_eq!(ac.inner.data[0], head1);
    assert_eq!(ac.inner.data[1], head2);
    assert_eq!(ac.inner.data[2], head3);
  }

  OK
}

#[compio::test]
async fn test_compact_write_multiple() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  let head1 = KvHead::new(1, 0);
  let head2 = KvHead::new(2, 0);
  let head3 = KvHead::new(3, 0);

  {
    let mut ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    ac.push_iter([(head1, true), (head2, true), (head3, true)])
      .await?;
    assert_eq!(ac.count, 3);
  }

  {
    let ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 3);
  }

  OK
}

#[compio::test]
async fn test_compact_corrupted_crc() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  let head1 = KvHead::new(1, 0);
  let head2 = KvHead::new(2, 0);
  let head3 = KvHead::new(3, 0);

  // Write valid data first
  // 先写入有效数据
  {
    let mut ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    ac.push(head1, true).await?;
    ac.push(head2, true).await?;
    ac.push(head3, true).await?;
  }

  // Corrupt CRC of second record
  // 损坏第二条记录的 CRC
  {
    let mut data = std::fs::read(&path)?;
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    std::fs::write(&path, data)?;
  }

  // Reload should skip corrupted record
  // 重新加载应跳过损坏记录
  {
    let ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    assert!(!ac.inner.data.is_empty());
    assert_eq!(ac.inner.data[0], head1);
  }

  OK
}

#[compio::test]
async fn test_compact_corrupted_magic() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  let head1 = KvHead::new(1, 0);
  let head2 = KvHead::new(2, 0);

  // Write valid data first
  // 先写入有效数据
  {
    let mut ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    ac.push(head1, true).await?;
    ac.push(head2, true).await?;
  }

  // Corrupt magic of first record
  // 损坏第一条记录的 magic
  {
    let mut data = std::fs::read(&path)?;
    data[0] = 0x00;
    std::fs::write(&path, data)?;
  }

  // Reload should skip first and recover second
  // 重新加载应跳过第一条并恢复第二条
  {
    let ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 1);
    assert_eq!(ac.inner.data[0], head2);
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
    let ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    assert!(ac.inner.data.is_empty());
  }

  OK
}

#[compio::test]
async fn test_compact_with_data_len() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("test.log");

  // Test with different keys
  // 测试不同的 key
  let head1 = KvHead::new(100, 0);
  let head2 = KvHead::new(200, 0);
  let head3 = KvHead::new(300, 0);

  {
    let mut ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    ac.push(head1, true).await?;
    ac.push(head2, true).await?;
    ac.push(head3, true).await?;
  }

  {
    let ac: AutoCompact<KvStore> = AutoCompact::open(path.clone()).await?;
    assert_eq!(ac.inner.data.len(), 3);
    assert_eq!(ac.inner.data[0], head1);
    assert_eq!(ac.inner.data[1], head2);
    assert_eq!(ac.inner.data[2], head3);
  }

  OK
}
