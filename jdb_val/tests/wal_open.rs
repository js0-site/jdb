//! WAL open/recovery tests
//! WAL 打开/恢复测试

use std::{
  fs::{self, OpenOptions},
  io::{Seek, SeekFrom, Write},
};

use jdb_val::{HEADER_SIZE, Wal};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

/// Magic size is 1 byte in new format
/// 新格式中魔数大小为 1 字节
const MAGIC_SIZE: usize = 1;

/// Test normal open
/// 测试正常打开
#[compio::test]
async fn test_open_new() {
  let dir = tempfile::tempdir().unwrap();
  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();

  assert_eq!(wal.cur_pos(), HEADER_SIZE as u64);
}

/// Test reopen empty WAL
/// 测试重新打开空 WAL
#[compio::test]
async fn test_reopen_empty() {
  let dir = tempfile::tempdir().unwrap();

  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    wal.sync_all().await.unwrap();
  }

  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  assert_eq!(wal.cur_pos(), HEADER_SIZE as u64);
}

/// Test reopen with data
/// 测试重新打开有数据的 WAL
#[compio::test]
async fn test_reopen_with_data() {
  let dir = tempfile::tempdir().unwrap();

  let expected_pos;
  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    wal.put(b"key1", b"val1").await.unwrap();
    wal.put(b"key2", b"val2").await.unwrap();
    expected_pos = wal.cur_pos();
    wal.sync_all().await.unwrap();
  }

  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  dbg!(expected_pos, wal.cur_pos());
  assert_eq!(wal.cur_pos(), expected_pos);
}

/// Test recovery with truncated file
/// 测试截断文件的恢复
#[compio::test]
async fn test_recover_truncated() {
  let dir = tempfile::tempdir().unwrap();

  let first_entry_end;
  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    wal.put(b"key1", b"val1").await.unwrap();
    first_entry_end = wal.cur_pos();
    wal.put(b"key2", b"val2").await.unwrap();
    wal.sync_all().await.unwrap();
  }

  // Truncate file to middle of second entry
  // 截断文件到第二个条目中间
  let wal_path = dir.path().join("wal");
  let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
  let file_path = entries[0].as_ref().unwrap().path();
  let file = OpenOptions::new().write(true).open(&file_path).unwrap();
  file.set_len(first_entry_end + 10).unwrap();

  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  dbg!(first_entry_end, wal.cur_pos());
  assert_eq!(wal.cur_pos(), first_entry_end);
}

/// Test recovery with corrupted magic
/// 测试损坏魔数的恢复
#[compio::test]
async fn test_recover_corrupted_magic() {
  let dir = tempfile::tempdir().unwrap();

  let first_entry_end;
  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    wal.put(b"key1", b"val1").await.unwrap();
    first_entry_end = wal.cur_pos();
    wal.put(b"key2", b"val2").await.unwrap();
    wal.sync_all().await.unwrap();
  }

  // Corrupt second entry's magic
  // 损坏第二个条目的魔数
  let wal_path = dir.path().join("wal");
  let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
  let file_path = entries[0].as_ref().unwrap().path();

  {
    let mut file = OpenOptions::new().write(true).open(&file_path).unwrap();
    file.seek(SeekFrom::Start(first_entry_end)).unwrap();
    file.write_all(&[0u8; MAGIC_SIZE]).unwrap();
    file.sync_all().unwrap();
  }

  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  dbg!(first_entry_end, wal.cur_pos());
  assert_eq!(wal.cur_pos(), first_entry_end);
}

/// Test recovery with corrupted head CRC
/// 测试损坏 head CRC 的恢复
#[compio::test]
async fn test_recover_corrupted_head() {
  let dir = tempfile::tempdir().unwrap();

  let first_entry_end;
  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    wal.put(b"key1", b"val1").await.unwrap();
    first_entry_end = wal.cur_pos();
    wal.put(b"key2", b"val2").await.unwrap();
    wal.sync_all().await.unwrap();
  }

  // Corrupt second entry's head (corrupt id field)
  // 损坏第二个条目的 head（损坏 id 字段）
  let wal_path = dir.path().join("wal");
  let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
  let file_path = entries[0].as_ref().unwrap().path();

  {
    let mut file = OpenOptions::new().write(true).open(&file_path).unwrap();
    // Corrupt id field (after magic byte)
    // 损坏 id 字段（在魔数字节之后）
    let id_pos = first_entry_end + MAGIC_SIZE as u64;
    file.seek(SeekFrom::Start(id_pos)).unwrap();
    file.write_all(&[0xFFu8; 8]).unwrap();
    file.sync_all().unwrap();
  }

  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  dbg!(first_entry_end, wal.cur_pos());
  assert_eq!(wal.cur_pos(), first_entry_end);
}

/// Test backward search recovery
/// 测试反向搜索恢复
#[compio::test]
async fn test_recover_backward_search() {
  let dir = tempfile::tempdir().unwrap();

  let first_entry_end;
  let second_entry_end;
  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    wal.put(b"key1", b"val1").await.unwrap();
    first_entry_end = wal.cur_pos();
    wal.put(b"key2", b"val2").await.unwrap();
    second_entry_end = wal.cur_pos();
    wal.sync_all().await.unwrap();
  }

  // Corrupt first entry's magic (forward will fail at start)
  // 损坏第一个条目的魔数（向前搜索会在开始时失败）
  let wal_path = dir.path().join("wal");
  let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
  let file_path = entries[0].as_ref().unwrap().path();

  {
    let mut file = OpenOptions::new().write(true).open(&file_path).unwrap();
    // Corrupt first magic at HEADER_SIZE
    // 损坏 HEADER_SIZE 处的第一个魔数
    file.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
    file.write_all(&[0u8; MAGIC_SIZE]).unwrap();
    file.sync_all().unwrap();
  }

  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  dbg!(first_entry_end, second_entry_end, wal.cur_pos());
  // Should find second entry via backward search
  // 应通过反向搜索找到第二个条目
  assert_eq!(wal.cur_pos(), second_entry_end);
}

/// Test backward search with multiple corruptions
/// 测试多处损坏的反向搜索
#[compio::test]
async fn test_recover_backward_multiple_corrupt() {
  let dir = tempfile::tempdir().unwrap();

  let first_entry_end;
  let second_entry_end;
  let third_entry_end;
  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    wal.put(b"key1", b"val1").await.unwrap();
    first_entry_end = wal.cur_pos();
    wal.put(b"key2", b"val2").await.unwrap();
    second_entry_end = wal.cur_pos();
    wal.put(b"key3", b"val3").await.unwrap();
    third_entry_end = wal.cur_pos();
    wal.sync_all().await.unwrap();
  }

  let wal_path = dir.path().join("wal");
  let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
  let file_path = entries[0].as_ref().unwrap().path();

  {
    let mut file = OpenOptions::new().write(true).open(&file_path).unwrap();
    // Corrupt first and third magic
    // 损坏第一和第三个魔数
    file.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
    file.write_all(&[0u8; MAGIC_SIZE]).unwrap();
    file.seek(SeekFrom::Start(second_entry_end)).unwrap();
    file.write_all(&[0u8; MAGIC_SIZE]).unwrap();
    file.sync_all().unwrap();
  }

  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  dbg!(
    first_entry_end,
    second_entry_end,
    third_entry_end,
    wal.cur_pos()
  );
  // Should find second entry (last valid) via backward search
  // 应通过反向搜索找到第二个条目（最后一个有效的）
  assert_eq!(wal.cur_pos(), second_entry_end);
}

/// Test file too small
/// 测试文件太小
#[compio::test]
async fn test_file_too_small() {
  let dir = tempfile::tempdir().unwrap();

  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    wal.sync_all().await.unwrap();
  }

  // Truncate to less than MIN_FILE_SIZE
  // 截断到小于 MIN_FILE_SIZE
  let wal_path = dir.path().join("wal");
  let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
  let file_path = entries[0].as_ref().unwrap().path();
  let file = OpenOptions::new().write(true).open(&file_path).unwrap();
  file.set_len(10).unwrap();

  // Should create new file
  // 应创建新文件
  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  assert_eq!(wal.cur_pos(), HEADER_SIZE as u64);
}

/// Test invalid header
/// 测试无效文件头
#[compio::test]
async fn test_invalid_header() {
  let dir = tempfile::tempdir().unwrap();

  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    wal.put(b"key", b"val").await.unwrap();
    wal.sync_all().await.unwrap();
  }

  // Corrupt header
  // 损坏文件头
  let wal_path = dir.path().join("wal");
  let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
  let file_path = entries[0].as_ref().unwrap().path();

  {
    let mut file = OpenOptions::new().write(true).open(&file_path).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[0xFFu8; HEADER_SIZE]).unwrap();
    file.sync_all().unwrap();
  }

  // Should create new file since header is invalid
  // 应创建新文件因为头无效
  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  // New file starts at HEADER_SIZE
  // 新文件从 HEADER_SIZE 开始
  assert_eq!(wal.cur_pos(), HEADER_SIZE as u64);
  // Should have 2 files now (old corrupted + new)
  // 现在应该有 2 个文件
  let count = fs::read_dir(&wal_path).unwrap().count();
  assert_eq!(count, 2);
}

/// Test recovery with infile data
/// 测试 infile 数据的恢复
#[compio::test]
async fn test_recover_infile() {
  let dir = tempfile::tempdir().unwrap();

  let expected_pos;
  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    // Key > 30B triggers infile mode
    // Key > 30B 触发 infile 模式
    let key = vec![b'k'; 100];
    let val = vec![b'v'; 200];
    wal.put(&key, &val).await.unwrap();
    expected_pos = wal.cur_pos();
    wal.sync_all().await.unwrap();
  }

  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  dbg!(expected_pos, wal.cur_pos());
  assert_eq!(wal.cur_pos(), expected_pos);
}

/// Test all entries corrupted
/// 测试所有条目都损坏
#[compio::test]
async fn test_all_corrupted() {
  let dir = tempfile::tempdir().unwrap();

  {
    let mut wal = Wal::new(dir.path(), &[]);
    let _ = wal.open(None).await.unwrap();
    wal.put(b"key1", b"val1").await.unwrap();
    wal.put(b"key2", b"val2").await.unwrap();
    wal.sync_all().await.unwrap();
  }

  // Corrupt all magic bytes
  // 损坏所有魔数
  let wal_path = dir.path().join("wal");
  let entries: Vec<_> = fs::read_dir(&wal_path).unwrap().collect();
  let file_path = entries[0].as_ref().unwrap().path();
  let file_len = fs::metadata(&file_path).unwrap().len();

  {
    let mut file = OpenOptions::new().write(true).open(&file_path).unwrap();
    // Overwrite everything after header with zeros
    // 用零覆盖头之后的所有内容
    file.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
    let zeros = vec![0u8; (file_len - HEADER_SIZE as u64) as usize];
    file.write_all(&zeros).unwrap();
    file.sync_all().unwrap();
  }

  let mut wal = Wal::new(dir.path(), &[]);
  let _ = wal.open(None).await.unwrap();
  // Should fallback to checkpoint (HEADER_SIZE)
  // 应回退到检查点
  assert_eq!(wal.cur_pos(), HEADER_SIZE as u64);
}
