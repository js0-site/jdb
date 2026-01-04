//! WAL checkpoint recovery tests
//! WAL 检查点恢复测试

use futures::StreamExt;
use jdb_val::{Conf, Wal};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Test recovery stream without checkpoint
/// 测试无检查点的恢复流
#[compio::test]
async fn test_recovery_no_ckp() {
  let dir = tempfile::tempdir().unwrap();

  // Write data
  // 写入数据
  {
    let (mut wal, _) = Wal::open(dir.path(), &[], None).await.unwrap();
    wal.put(b"k1", b"v1").await.unwrap();
    wal.put(b"k2", b"v2").await.unwrap();
    wal.put(b"k3", b"v3").await.unwrap();
    wal.sync().await.unwrap();
  }

  // Reopen without checkpoint, stream should be empty (no ckp = no replay)
  // 无检查点重新打开，流应为空
  let (_, stream) = Wal::open(dir.path(), &[], None).await.unwrap();
  futures::pin_mut!(stream);

  let mut count = 0;
  while stream.next().await.is_some() {
    count += 1;
  }
  assert_eq!(count, 0);
}

/// Test recovery stream with checkpoint
/// 测试有检查点的恢复流
#[compio::test]
async fn test_recovery_with_ckp() {
  let dir = tempfile::tempdir().unwrap();

  // First open, write and save checkpoint
  // 首次打开，写入并保存检查点
  let (ckp_wal_id, ckp_offset);
  {
    let (mut ckp, _) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
    let (mut wal, _) = Wal::open(dir.path(), &[], None).await.unwrap();

    wal.put(b"k1", b"v1").await.unwrap();
    wal.put(b"k2", b"v2").await.unwrap();
    wal.sync().await.unwrap();

    // Save checkpoint
    // 保存检查点
    ckp_wal_id = wal.cur_id();
    ckp_offset = wal.cur_pos();
    ckp.set_wal_ptr(ckp_wal_id, ckp_offset).await.unwrap();

    // Write more data after checkpoint
    // 检查点后写入更多数据
    wal.put(b"k3", b"v3").await.unwrap();
    wal.put(b"k4", b"v4").await.unwrap();
    wal.sync().await.unwrap();
  }

  // Reopen with checkpoint, stream should contain 2 entries (k3, k4)
  // 使用检查点重新打开，流应包含 2 个条目
  let (_, last) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
  assert!(last.is_some());
  let last = last.unwrap();
  assert_eq!(last.wal_id, ckp_wal_id);
  assert_eq!(last.offset, ckp_offset);

  let (_, stream) = Wal::open(dir.path(), &[], Some(&last)).await.unwrap();
  futures::pin_mut!(stream);

  let mut count = 0;
  while let Some(entry) = stream.next().await {
    count += 1;
    // Verify head has valid data
    // 验证 head 有有效数据
    assert!(entry.head.key_len > 0);
    assert!(entry.end > ckp_offset);
  }

  assert_eq!(count, 2);
}

/// Test recovery with multiple checkpoints
/// 测试多次检查点的恢复
#[compio::test]
async fn test_recovery_multiple_ckp() {
  let dir = tempfile::tempdir().unwrap();

  // Write data with multiple checkpoints
  // 多次检查点写入数据
  {
    let (mut ckp, _) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
    let (mut wal, _) = Wal::open(dir.path(), &[], None).await.unwrap();

    wal.put(b"k1", b"v1").await.unwrap();
    wal.sync().await.unwrap();
    ckp.set_wal_ptr(wal.cur_id(), wal.cur_pos()).await.unwrap();

    wal.put(b"k2", b"v2").await.unwrap();
    wal.sync().await.unwrap();
    ckp.set_wal_ptr(wal.cur_id(), wal.cur_pos()).await.unwrap();

    wal.put(b"k3", b"v3").await.unwrap();
    wal.put(b"k4", b"v4").await.unwrap();
    wal.sync().await.unwrap();
  }

  // Reopen, should only replay 2 entries (after last checkpoint)
  // 重新打开，应只回放 2 个条目（最后检查点之后）
  let (_, last) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
  let last = last.unwrap();

  let (_, stream) = Wal::open(dir.path(), &[], Some(&last)).await.unwrap();
  futures::pin_mut!(stream);

  let mut count = 0;
  while stream.next().await.is_some() {
    count += 1;
  }

  assert_eq!(count, 2);
}

/// Test recovery with rotation
/// 测试带轮转的恢复
#[compio::test]
async fn test_recovery_with_rotate() {
  let dir = tempfile::tempdir().unwrap();

  {
    let (mut ckp, _) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
    let (mut wal, _) = Wal::open(dir.path(), &[Conf::MaxSize(200)], None)
      .await
      .unwrap();

    // Write and checkpoint
    // 写入并检查点
    wal.put(b"k1", b"v1").await.unwrap();
    wal.sync().await.unwrap();
    ckp.set_wal_ptr(wal.cur_id(), wal.cur_pos()).await.unwrap();

    let old_id = wal.cur_id();

    // Write more to trigger rotation
    // 写入更多触发轮转
    for i in 0..10u8 {
      let key = format!("key{i}").into_bytes();
      let val = vec![i; 50];
      wal.put(&key, &val).await.unwrap();
    }
    wal.sync().await.unwrap();

    let new_id = wal.cur_id();
    if new_id != old_id {
      ckp.rotate(new_id).await.unwrap();
    }

    // Write after rotation
    // 轮转后写入
    wal.put(b"final", b"value").await.unwrap();
    wal.sync().await.unwrap();
  }

  // Reopen and verify recovery
  // 重新打开并验证恢复
  let (_, last) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
  let last = last.unwrap();

  let (_, stream) = Wal::open(dir.path(), &[], Some(&last)).await.unwrap();
  futures::pin_mut!(stream);

  let mut count = 0;
  while stream.next().await.is_some() {
    count += 1;
  }

  assert!(count > 0);
}

/// Test recovery stream head fields
/// 测试恢复流 head 字段
#[compio::test]
async fn test_recovery_head_fields() {
  let dir = tempfile::tempdir().unwrap();

  {
    let (mut ckp, _) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
    let (mut wal, _) = Wal::open(dir.path(), &[], None).await.unwrap();

    wal.put(b"before", b"ckp").await.unwrap();
    wal.sync().await.unwrap();
    ckp.set_wal_ptr(wal.cur_id(), wal.cur_pos()).await.unwrap();

    wal.put(b"test_key", b"test_value").await.unwrap();
    wal.sync().await.unwrap();
  }

  let (_, last) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
  let (_, stream) = Wal::open(dir.path(), &[], last.as_ref()).await.unwrap();
  futures::pin_mut!(stream);

  let entry = stream.next().await.unwrap();
  // key_len = 8 ("test_key")
  assert_eq!(entry.head.key_len, 8);
  // val_len = 10 ("test_value")
  assert_eq!(entry.head.val_len, 10);
  // end > 0
  assert!(entry.end > 0);
}

/// Test tombstone in recovery stream
/// 测试恢复流中的墓碑
#[compio::test]
async fn test_recovery_tombstone() {
  let dir = tempfile::tempdir().unwrap();

  {
    let (mut ckp, _) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
    let (mut wal, _) = Wal::open(dir.path(), &[], None).await.unwrap();

    wal.put(b"k1", b"v1").await.unwrap();
    wal.sync().await.unwrap();
    ckp.set_wal_ptr(wal.cur_id(), wal.cur_pos()).await.unwrap();

    wal.put(b"k2", b"v2").await.unwrap();
    wal.rm(b"k1").await.unwrap();
    wal.sync().await.unwrap();
  }

  let (_, last) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
  let (_, stream) = Wal::open(dir.path(), &[], last.as_ref()).await.unwrap();
  futures::pin_mut!(stream);

  let mut has_put = false;
  let mut has_tombstone = false;
  while let Some(entry) = stream.next().await {
    if entry.head.is_tombstone() {
      has_tombstone = true;
    } else {
      has_put = true;
    }
  }

  assert!(has_put);
  assert!(has_tombstone);
}

/// Test empty checkpoint recovery
/// 测试空检查点恢复
#[compio::test]
async fn test_recovery_empty_ckp() {
  let dir = tempfile::tempdir().unwrap();

  // First open without any data
  // 首次打开无任何数据
  let (_, last) = jdb_ckp::open(dir.path(), &[]).await.unwrap();
  assert!(last.is_none());

  let (_, stream) = Wal::open(dir.path(), &[], last.as_ref()).await.unwrap();
  futures::pin_mut!(stream);

  let mut count = 0;
  while stream.next().await.is_some() {
    count += 1;
  }
  assert_eq!(count, 0);
}
