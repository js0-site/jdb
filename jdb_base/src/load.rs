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
pub trait Load {
  /// Magic byte
  /// 魔数
  const MAGIC: u8;

  /// Header size
  /// 头大小
  const HEAD_SIZE: usize;

  /// Meta offset for CRC32 calculation
  /// 用于计算 CRC32 的元信息偏移
  const META_OFFSET: usize;

  /// Get entry length from buffer, returns 0 if invalid
  /// 从缓冲区获取条目长度，无效返回0
  fn len(buf: &[u8]) -> usize;

  /// CRC32 offset (can depend on entry length)
  /// CRC32 偏移（可依赖条目长度）
  fn crc_offset(len: usize) -> usize;

  /// Meta length for CRC32 calculation
  /// 用于计算 CRC32 的元信息长度
  fn meta_len(len: usize) -> usize;

  /// Verify CRC32
  /// 验证 CRC32
  #[inline]
  fn verify(bin: &[u8], len: usize) -> bool {
    if len == 0 || bin.len() < len {
      return false;
    }
    let crc_offset = Self::crc_offset(len);
    let meta_offset = Self::META_OFFSET;
    let meta_len = Self::meta_len(len);
    if crc_offset + 4 > len || meta_offset + meta_len > len {
      return false;
    }

    // Safety: bounds checked above. Use try_into to handle potential unaligned access safely.
    let stored_bytes = &bin[crc_offset..crc_offset + 4];
    let stored = u32::from_le_bytes(stored_bytes.try_into().unwrap());

    let computed = crc32fast::hash(&bin[meta_offset..meta_offset + meta_len]);
    stored == computed
  }

  /// Parse and verify entry, returns entry length or 0 if invalid
  /// 解析并验证条目，返回条目长度，无效返回0
  #[inline]
  fn parse(bin: &[u8]) -> usize {
    let len = Self::len(bin);
    if len > 0 && Self::verify(bin, len) {
      len
    } else {
      0
    }
  }

  /// Find next magic byte in buffer
  /// 在缓冲区中查找下一个 magic 字节
  #[inline]
  fn find_magic(buf: &[u8]) -> Option<usize> {
    memchr::memchr(Self::MAGIC, buf)
  }

  /// Find last magic byte in buffer
  /// 在缓冲区中查找最后一个 magic 字节
  #[inline]
  fn rfind_magic(buf: &[u8]) -> Option<usize> {
    memchr::memrchr(Self::MAGIC, buf)
  }

  /// Recover valid end position from file
  /// 从文件恢复有效结束位置
  ///
  /// 1. Scan forward from start
  /// 2. If failed, search backward for magic, then scan forward
  #[allow(clippy::uninit_vec)]
  fn recover(file: &File, start: u64, len: u64) -> impl std::future::Future<Output = u64> + '_ {
    async move {
      if let Some(pos) = Self::scan_forward(file, start, len).await {
        return pos;
      }

      if let Some(magic_pos) = Self::search_backward(file, start, len).await {
        if let Some(end_pos) = Self::scan_forward(file, magic_pos, len).await {
          return end_pos;
        }
        return magic_pos;
      }

      start
    }
  }

  /// Scan forward for valid entries
  /// 向前扫描有效条目
  #[allow(clippy::uninit_vec)]
  fn scan_forward(
    file: &File,
    start: u64,
    len: u64,
  ) -> impl std::future::Future<Output = Option<u64>> + '_ {
    async move {
      let mut pos = start;
      let mut valid_end = None;
      let mut buf = Vec::with_capacity(SCAN_BUF);

      while pos < len {
        let read_len = (len - pos).min(SCAN_BUF as u64) as usize;
        if read_len < Self::HEAD_SIZE {
          break;
        }

        // Reuse buffer capacity, reset length
        buf.clear();
        buf.reserve(read_len);
        // Safety: We reserve enough capacity just above.
        unsafe { buf.set_len(read_len) };
        let slice = buf.slice(0..read_len);
        let res = file.read_exact_at(slice, pos).await;
        buf = res.1.into_inner();
        if res.0.is_err() {
          break;
        }

        let size = Self::parse(&buf);
        if size == 0 || pos + size as u64 > len {
          break;
        }

        pos += size as u64;
        valid_end = Some(pos);
      }

      valid_end
    }
  }

  /// Search backward for valid magic position
  /// 向后搜索有效的 magic 位置
  #[allow(clippy::uninit_vec)]
  fn search_backward(
    file: &File,
    start: u64,
    len: u64,
  ) -> impl std::future::Future<Output = Option<u64>> + '_ {
    async move {
      let mut pos = len;
      let mut buf = Vec::with_capacity(SCAN_BUF);

      while pos > start {
        let read_start = pos.saturating_sub(SCAN_BUF as u64).max(start);
        let read_len = (pos - read_start) as usize;

        // Reuse buffer capacity, reset length
        buf.clear();
        buf.reserve(read_len);
        // Safety: We reserve enough capacity just above.
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
        while let Some(idx) = Self::rfind_magic(&buf[..search_end]) {
          let remaining = &buf[idx..];
          if remaining.len() >= Self::HEAD_SIZE && Self::parse(remaining) > 0 {
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
  }
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
    // Format: magic(1) + len(1) + data(len) + crc(1)
    const META_OFFSET: usize = 2;

    fn len(buf: &[u8]) -> usize {
      if buf.len() < 3 || buf[0] != Self::MAGIC {
        return 0;
      }
      let data_len = buf[1] as usize;
      3 + data_len
    }

    fn crc_offset(len: usize) -> usize {
      len - 1
    }

    fn meta_len(len: usize) -> usize {
      len - 3 // total - magic(1) - len_byte(1) - crc(1)
    }

    fn verify(buf: &[u8], len: usize) -> bool {
      if len == 0 || buf.len() < len {
        return false;
      }
      let data_len = buf[1] as usize;
      let data = &buf[2..2 + data_len];
      let crc = data.iter().fold(0u8, |a, b| a.wrapping_add(*b));
      buf[2 + data_len] == crc
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
    assert_eq!(TestEntry::find_magic(&buf), Some(2));
    assert_eq!(TestEntry::rfind_magic(&buf), Some(4));

    let no_magic = [0x00, 0x01, 0x02];
    assert_eq!(TestEntry::find_magic(&no_magic), None);
  }

  #[test]
  fn test_len_valid() {
    let entry = build_entry(b"hello");
    assert_eq!(TestEntry::len(&entry), 8); // 3 + 5
  }

  #[test]
  fn test_verify_valid() {
    let entry = build_entry(b"hello");
    let len = TestEntry::len(&entry);
    assert!(TestEntry::verify(&entry, len));
  }

  #[test]
  fn test_len_invalid_magic() {
    let mut entry = build_entry(b"hi");
    entry[0] = 0xBB;
    assert_eq!(TestEntry::len(&entry), 0);
  }

  #[test]
  fn test_verify_invalid_crc() {
    let mut entry = build_entry(b"hi");
    entry[4] = 0xFF; // corrupt crc
    let len = TestEntry::len(&entry);
    assert!(!TestEntry::verify(&entry, len));
  }

  #[test]
  fn test_len_truncated() {
    let entry = build_entry(b"hello");
    assert_eq!(TestEntry::len(&entry[..2]), 0);
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

      let pos = TestEntry::recover(&file, 0, 0).await;
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

      let pos = TestEntry::recover(&file, 0, entry.len() as u64).await;
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

      let pos = TestEntry::recover(&file, 0, data.len() as u64).await;
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

      let pos = TestEntry::recover(&file, 0, data.len() as u64).await;
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

      let pos = TestEntry::recover(&file, 0, data.len() as u64).await;
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

      let pos = TestEntry::recover(&file, 0, data.len() as u64).await;
      assert_eq!(pos, 0); // fallback to start
    });
  }
}
