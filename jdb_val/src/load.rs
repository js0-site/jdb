//! Load trait for WAL/CKP recovery
//! WAL/CKP 恢复的加载 trait

use compio::{
  buf::{IntoInner, IoBuf},
  io::AsyncReadAtExt,
};
use compio_fs::File;

/// Scan buffer size (64KB)
/// 扫描缓冲区大小
const SCAN_BUF: usize = 64 * 1024;

/// Load trait for entry parsing
/// 条目解析的加载 trait
pub(crate) trait Load {
  /// Magic byte
  /// 魔数
  const MAGIC: u8;

  /// Header size
  /// 头大小
  const HEAD_SIZE: usize;

  /// Parse entry, returns entry size if valid
  /// 解析条目，有效则返回条目大小
  fn parse(buf: &[u8]) -> Option<usize>;
}

/// Find first magic byte in buffer (test only)
/// 在缓冲区中查找第一个 magic 字节（仅测试）
#[cfg(test)]
#[inline]
fn find_magic<L: Load>(buf: &[u8]) -> Option<usize> {
  memchr::memchr(L::MAGIC, buf)
}

/// Find last magic byte in buffer
/// 在缓冲区中查找最后一个 magic 字节
#[inline]
fn rfind_magic<L: Load>(buf: &[u8]) -> Option<usize> {
  memchr::memrchr(L::MAGIC, buf)
}

/// Recover valid end position from file
/// 从文件恢复有效结束位置
///
/// 1. Scan forward from start
/// 2. If failed, search backward for magic, then scan forward
#[allow(clippy::uninit_vec)]
pub(crate) async fn recover<L: Load>(file: &File, start: u64, len: u64) -> u64 {
  if let Some(pos) = scan_forward::<L>(file, start, len).await {
    return pos;
  }

  if let Some(magic_pos) = search_backward::<L>(file, start, len).await {
    if let Some(end_pos) = scan_forward::<L>(file, magic_pos, len).await {
      return end_pos;
    }
    return magic_pos;
  }

  start
}

/// Scan forward for valid entries
/// 向前扫描有效条目
#[allow(clippy::uninit_vec)]
async fn scan_forward<L: Load>(file: &File, start: u64, len: u64) -> Option<u64> {
  let mut pos = start;
  let mut valid_end = None;
  let mut buf = Vec::with_capacity(SCAN_BUF);

  while pos < len {
    let read_len = (len - pos).min(SCAN_BUF as u64) as usize;
    if read_len < L::HEAD_SIZE {
      break;
    }

    unsafe { buf.set_len(read_len) };
    let slice = buf.slice(0..read_len);
    let res = file.read_exact_at(slice, pos).await;
    buf = res.1.into_inner();
    if res.0.is_err() {
      break;
    }

    let Some(size) = L::parse(&buf) else {
      break;
    };

    if pos + size as u64 > len {
      break;
    }

    pos += size as u64;
    valid_end = Some(pos);
  }

  valid_end
}

/// Search backward for valid magic position
/// 向后搜索有效的 magic 位置
#[allow(clippy::uninit_vec)]
async fn search_backward<L: Load>(file: &File, start: u64, len: u64) -> Option<u64> {
  let mut pos = len;
  let mut buf = Vec::with_capacity(SCAN_BUF);

  while pos > start {
    let read_start = pos.saturating_sub(SCAN_BUF as u64).max(start);
    let read_len = (pos - read_start) as usize;

    buf.reserve(read_len.saturating_sub(buf.capacity()));
    unsafe { buf.set_len(read_len) };

    let tmp = std::mem::take(&mut buf);
    let slice = tmp.slice(0..read_len);
    let res = file.read_exact_at(slice, read_start).await;
    buf = res.1.into_inner();
    if res.0.is_err() {
      return None;
    }

    // Search magic from end to start
    // 从后向前搜索 magic
    let mut search_end = buf.len();
    while let Some(idx) = rfind_magic::<L>(&buf[..search_end]) {
      let remaining = &buf[idx..];
      if remaining.len() >= L::HEAD_SIZE && L::parse(remaining).is_some() {
        return Some(read_start + idx as u64);
      }
      search_end = idx;
    }

    if read_start == start {
      break;
    }
    pos = read_start;
  }

  None
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Test entry type
  /// 测试条目类型
  struct TestEntry;

  impl Load for TestEntry {
    const MAGIC: u8 = 0xAA;
    const HEAD_SIZE: usize = 4;

    fn parse(buf: &[u8]) -> Option<usize> {
      if buf.len() < 4 || buf[0] != Self::MAGIC {
        return None;
      }
      // Format: magic(1) + len(1) + data(len) + crc(1)
      let len = buf[1] as usize;
      let total = 3 + len;
      if buf.len() < total {
        return None;
      }
      // Simple crc: sum of data bytes
      let data = &buf[2..2 + len];
      let crc = data.iter().fold(0u8, |a, b| a.wrapping_add(*b));
      if buf[2 + len] == crc {
        Some(total)
      } else {
        None
      }
    }
  }

  fn build_entry(data: &[u8]) -> Vec<u8> {
    let crc = data.iter().fold(0u8, |a, b| a.wrapping_add(*b));
    let mut buf = vec![TestEntry::MAGIC, data.len() as u8];
    buf.extend_from_slice(data);
    buf.push(crc);
    buf
  }

  #[test]
  fn test_find_magic() {
    let buf = [0x00, 0x01, 0xAA, 0x02, 0xAA];
    assert_eq!(find_magic::<TestEntry>(&buf), Some(2));
    assert_eq!(rfind_magic::<TestEntry>(&buf), Some(4));

    let no_magic = [0x00, 0x01, 0x02];
    assert_eq!(find_magic::<TestEntry>(&no_magic), None);
  }

  #[test]
  fn test_parse_valid() {
    let entry = build_entry(b"hello");
    assert_eq!(TestEntry::parse(&entry), Some(8)); // 3 + 5
  }

  #[test]
  fn test_parse_invalid_magic() {
    let mut entry = build_entry(b"hi");
    entry[0] = 0xBB;
    assert_eq!(TestEntry::parse(&entry), None);
  }

  #[test]
  fn test_parse_invalid_crc() {
    let mut entry = build_entry(b"hi");
    entry[4] = 0xFF; // corrupt crc
    assert_eq!(TestEntry::parse(&entry), None);
  }

  #[test]
  fn test_parse_truncated() {
    let entry = build_entry(b"hello");
    assert_eq!(TestEntry::parse(&entry[..3]), None);
  }

  fn run<F: std::future::Future>(f: F) -> F::Output {
    compio_runtime::Runtime::new().unwrap().block_on(f)
  }

  #[test]
  fn test_recover_empty() {
    run(async {
      let dir = tempfile::tempdir().unwrap();
      let path = dir.path().join("test.dat");
      std::fs::write(&path, []).unwrap();

      let file = compio_fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .await
        .unwrap();

      let pos = recover::<TestEntry>(&file, 0, 0).await;
      assert_eq!(pos, 0);
    });
  }

  #[test]
  fn test_recover_single_entry() {
    run(async {
      let dir = tempfile::tempdir().unwrap();
      let path = dir.path().join("test.dat");

      let entry = build_entry(b"test");
      std::fs::write(&path, &entry).unwrap();

      let file = compio_fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .await
        .unwrap();

      let pos = recover::<TestEntry>(&file, 0, entry.len() as u64).await;
      assert_eq!(pos, entry.len() as u64);
    });
  }

  #[test]
  fn test_recover_multiple_entries() {
    run(async {
      let dir = tempfile::tempdir().unwrap();
      let path = dir.path().join("test.dat");

      let e1 = build_entry(b"one");
      let e2 = build_entry(b"two");
      let mut data = e1.clone();
      data.extend(&e2);
      std::fs::write(&path, &data).unwrap();

      let file = compio_fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .await
        .unwrap();

      let pos = recover::<TestEntry>(&file, 0, data.len() as u64).await;
      assert_eq!(pos, data.len() as u64);
    });
  }

  #[test]
  fn test_recover_with_corruption() {
    run(async {
      let dir = tempfile::tempdir().unwrap();
      let path = dir.path().join("test.dat");

      let e1 = build_entry(b"good");
      let mut data = e1.clone();
      data.extend(b"corrupt!"); // invalid data
      std::fs::write(&path, &data).unwrap();

      let file = compio_fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .await
        .unwrap();

      let pos = recover::<TestEntry>(&file, 0, data.len() as u64).await;
      assert_eq!(pos, e1.len() as u64);
    });
  }

  #[test]
  fn test_recover_backward_search() {
    run(async {
      let dir = tempfile::tempdir().unwrap();
      let path = dir.path().join("test.dat");

      // Corrupt start, valid entry later
      // 开头损坏，后面有有效条目
      let mut data = vec![0x00; 10]; // garbage
      let entry = build_entry(b"found");
      data.extend(&entry);
      std::fs::write(&path, &data).unwrap();

      let file = compio_fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .await
        .unwrap();

      let pos = recover::<TestEntry>(&file, 0, data.len() as u64).await;
      // Should find entry at offset 10 and return end position
      // 应该在偏移 10 处找到条目并返回结束位置
      assert_eq!(pos, data.len() as u64);
    });
  }

  #[test]
  fn test_recover_all_corrupt() {
    run(async {
      let dir = tempfile::tempdir().unwrap();
      let path = dir.path().join("test.dat");

      let data = vec![0x00; 100];
      std::fs::write(&path, &data).unwrap();

      let file = compio_fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .await
        .unwrap();

      let pos = recover::<TestEntry>(&file, 0, data.len() as u64).await;
      assert_eq!(pos, 0); // fallback to start
    });
  }
}
