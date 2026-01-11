//! Tests for atom_write module
//! atom_write 模块测试

use std::fs;

use aok::{OK, Void};
use compio::io::AsyncWriteExt;
use jdb_fs::AtomWrite;
use tempfile::tempdir;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[compio::test]
async fn test_atom_write_success() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("success.txt");
  let tmp = path.with_extension("txt.tmp");

  let content = b"hello world";
  let mut writer = AtomWrite::open(&path).await?;
  writer.write_all(content).await.0?;
  writer.rename().await?;

  assert!(path.exists());
  assert_eq!(fs::read(&path)?, content);
  assert!(!tmp.exists());

  OK
}

#[compio::test]
async fn test_atom_write_drop_cleanup() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("drop_test.txt");
  let tmp = path.with_extension("txt.tmp");

  {
    let mut writer = AtomWrite::open(&path).await?;
    writer.write_all(b"test").await.0?;
  }

  assert!(!path.exists());
  assert!(!tmp.exists());

  OK
}

#[compio::test]
async fn test_atom_write_path() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("path_test.txt");

  let writer = AtomWrite::open(&path).await?;
  assert_eq!(writer.path(), &path);

  OK
}

#[compio::test]
async fn test_atom_write_large_content() -> Void {
  let dir = tempdir()?;
  let path = dir.path().join("large.txt");

  let mut writer = AtomWrite::open(&path).await?;
  for _ in 0..100 {
    writer.write_all(b"0123456789").await.0?;
  }
  writer.rename().await?;

  let result = fs::read(&path)?;
  assert_eq!(result.len(), 1000);

  OK
}

/// Test multi-process atomic write exclusion
/// 测试多进程原子写互斥
#[test]
fn test_atom_write_multiprocess() {
  use std::{
    env,
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
  };

  const ENV_KEY: &str = "JDB_FS_TEST_LOCK";
  const ENV_HOLD: &str = "HOLD";
  const ENV_TRY: &str = "TRY";
  const ENV_PATH: &str = "JDB_FS_TEST_PATH";

  // Child process: hold lock
  // 子进程：持有锁
  if env::var(ENV_KEY).as_deref() == Ok(ENV_HOLD) {
    let path = env::var(ENV_PATH).unwrap();
    let tmp = format!("{path}.tmp");
    let file = fs::OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(&tmp)
      .unwrap();
    file.try_lock().unwrap();
    eprintln!("LOCKED");

    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line).ok();
    return;
  }

  // Child process: try lock
  // 子进程：尝试锁
  if env::var(ENV_KEY).as_deref() == Ok(ENV_TRY) {
    let path = env::var(ENV_PATH).unwrap();
    let tmp = format!("{path}.tmp");
    let file = fs::OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(&tmp)
      .unwrap();
    if file.try_lock().is_ok() {
      eprintln!("LOCKED");
    } else {
      eprintln!("LOCK_FAILED");
    }
    return;
  }

  // Parent process
  // 父进程
  let dir = tempdir().unwrap();
  let path = dir.path().join("multiproc.txt");
  let tmp = path.with_extension("txt.tmp");

  let exe = env::current_exe().unwrap();
  let path_str = path.to_str().unwrap();

  // Spawn first child to hold lock
  // 启动第一个子进程持有锁
  let mut child1 = Command::new(&exe)
    .arg("test_atom_write_multiprocess")
    .arg("--exact")
    .arg("--nocapture")
    .env(ENV_KEY, ENV_HOLD)
    .env(ENV_PATH, path_str)
    .stdin(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();

  let stderr1 = child1.stderr.take().unwrap();
  let mut stdin1 = child1.stdin.take().unwrap();
  let mut reader1 = BufReader::new(stderr1);

  let mut line = String::new();
  reader1.read_line(&mut line).unwrap();
  assert!(line.contains("LOCKED"), "child1 should lock: {line}");

  // Spawn second child, should fail
  // 启动第二个子进程，应失败
  let output2 = Command::new(&exe)
    .arg("test_atom_write_multiprocess")
    .arg("--exact")
    .arg("--nocapture")
    .env(ENV_KEY, ENV_TRY)
    .env(ENV_PATH, path_str)
    .output()
    .unwrap();

  let stderr2 = String::from_utf8_lossy(&output2.stderr);
  assert!(
    stderr2.contains("LOCK_FAILED"),
    "child2 should fail: {stderr2}"
  );

  // Release lock
  // 释放锁
  writeln!(stdin1, "done").ok();
  drop(stdin1);
  child1.wait().ok();

  let _ = fs::remove_file(&tmp);
}
