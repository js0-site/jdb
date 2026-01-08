//! Load trait for WAL/CKP recovery
//! WAL/CKP 恢复的加载 trait

use std::path::PathBuf;

use compio::{
  buf::{IntoInner, IoBuf},
  io::AsyncReadAtExt,
};
use compio_fs::File;
use either::Either;
use futures::{Stream, stream};

use crate::fs::open_read;

/// Scan buffer size (64KB)
/// 扫描缓冲区大小
const SCAN_BUF: usize = 64 * 1024;

/// Backward scan buffer size (4KB, smaller for efficiency)
/// 反向扫描缓冲区大小（4KB，更小以提高效率）
const SCAN_BACK: usize = 4 * 1024;

/// Invalid entry length
/// 无效条目长度
pub const INVALID: usize = 0;

/// Head with data and end position
/// 带数据和结束位置的头
#[derive(Debug, Clone)]
pub struct HeadWithData<H> {
  /// Parsed head
  /// 解析的头
  pub head: H,
  /// Record data (after magic: head bytes + crc + val + key)
  /// 记录数据（magic 之后：head 字节 + crc + val + key）
  pub data: Box<[u8]>,
  /// Position after this entry
  /// 此条目后的位置
  pub end: u64,
}

/// Recovery state
/// 恢复状态
struct State {
  file: File,
  pos: u64,
  file_len: u64,
  buf: Vec<u8>,
  buf_pos: usize,
}

/// Load trait for entry parsing
/// 条目解析的加载 trait
#[expect(
  async_fn_in_trait,
  reason = "compio single-threaded runtime, Send not needed"
)]
pub trait Load {
  /// Head type
  /// 头类型
  type Head;

  /// Magic byte
  /// 魔数
  const MAGIC: u8;

  /// Minimum entry size
  /// 最小条目大小
  const MIN_SIZE: usize;

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

  /// Parse head from buffer (buf starts after magic byte)
  /// 从缓冲区解析头（buf 从 magic 字节后开始）
  fn parse_head(buf: &[u8], len: usize) -> Option<Self::Head>;

  /// Find last valid entry end position (backward scan)
  /// 查找最后一个有效条目的结束位置（反向扫描）
  async fn find_end(file: &File, start: u64) -> Option<u64> {
    let file_len = file.metadata().await.ok()?.len();
    if file_len <= start {
      return Some(start);
    }

    // Backward scan, return start if no valid entry found
    // 反向扫描，找不到有效条目则返回 start
    Some(
      scan_backward::<Self>(file, start, file_len)
        .await
        .unwrap_or(start),
    )
  }

  /// Recover from path (owns file, fully lazy)
  /// 从路径恢复（拥有文件，完全懒加载）
  fn recover(path: PathBuf, start: u64) -> impl Stream<Item = HeadWithData<Self::Head>> {
    stream::unfold(Either::Left((path, start)), |state| async move {
      let mut s = match state {
        Either::Left((path, start)) => {
          let file = open_read(&path).await.ok()?;
          let file_len = file.metadata().await.ok()?.len();
          State {
            file,
            pos: start,
            file_len,
            buf: Vec::new(),
            buf_pos: 0,
          }
        }
        Either::Right(s) => s,
      };
      let r =
        scan_next::<Self>(&s.file, &mut s.pos, s.file_len, &mut s.buf, &mut s.buf_pos).await?;
      Some((r, Either::Right(s)))
    })
  }
}

/// Scan next entry from buffer/file
/// 从缓冲区/文件扫描下一个条目
async fn scan_next<L: Load + ?Sized>(
  file: &File,
  pos: &mut u64,
  file_len: u64,
  buf: &mut Vec<u8>,
  buf_pos: &mut usize,
) -> Option<HeadWithData<L::Head>> {
  loop {
    let remain = buf.len() - *buf_pos;

    if remain >= L::MIN_SIZE {
      let slice = &buf[*buf_pos..];
      let size = parse::<L>(slice);

      if size > 0
        && size <= remain
        && let Some(head) = L::parse_head(&slice[1..], size)
      {
        // Copy data (after magic)
        // 复制数据（magic 之后）
        let data = slice[1..size].to_vec().into_boxed_slice();
        *buf_pos += size;
        *pos += size as u64;
        return Some(HeadWithData {
          head,
          data,
          end: *pos,
        });
      }

      // Invalid entry, search next magic
      // 无效条目，搜索下一个 magic
      if size == 0 || size > remain {
        if let Some(next) = find_magic::<L>(&buf[*buf_pos + 1..]) {
          let skip = next + 1;
          *buf_pos += skip;
          *pos += skip as u64;
          continue;
        }
        *pos += remain as u64;
        *buf_pos = buf.len();
        continue;
      }
    }

    if *pos >= file_len {
      return None;
    }

    let read_len = (file_len - *pos).min(SCAN_BUF as u64) as usize;
    if read_len < L::MIN_SIZE {
      return None;
    }

    // Reuse buffer
    // 复用缓冲区
    buf.clear();
    buf.resize(read_len, 0);
    *buf_pos = 0;

    let slice = std::mem::take(buf).slice(0..read_len);
    let res = file.read_exact_at(slice, *pos).await;
    *buf = res.1.into_inner();

    if res.0.is_err() {
      return None;
    }
  }
}

/// Find next magic byte in buffer
/// 在缓冲区中查找下一个 magic 字节
#[inline]
fn find_magic<L: Load + ?Sized>(buf: &[u8]) -> Option<usize> {
  memchr::memchr(L::MAGIC, buf)
}

/// Verify CRC32
/// 验证 CRC32
#[inline]
fn verify<L: Load + ?Sized>(bin: &[u8], len: usize) -> bool {
  if len == 0 || bin.len() < len {
    return false;
  }
  let crc_off = L::crc_offset(len);
  let meta_off = L::META_OFFSET;
  let meta_len = L::meta_len(len);
  if crc_off + 4 > len || meta_off + meta_len > len {
    return false;
  }

  // Safe: verified crc_off + 4 <= len && crc_off + 4 <= bin.len()
  // 安全：已验证 crc_off + 4 <= len && crc_off + 4 <= bin.len()
  let stored = unsafe {
    u32::from_le_bytes([
      *bin.get_unchecked(crc_off),
      *bin.get_unchecked(crc_off + 1),
      *bin.get_unchecked(crc_off + 2),
      *bin.get_unchecked(crc_off + 3),
    ])
  };
  let computed = crc32fast::hash(&bin[meta_off..meta_off + meta_len]);
  stored == computed
}

/// Parse and verify entry, returns entry length or 0 if invalid
/// 解析并验证条目，返回条目长度，无效返回0
#[inline]
fn parse<L: Load + ?Sized>(bin: &[u8]) -> usize {
  let len = L::len(bin);
  if len > 0 && verify::<L>(bin, len) {
    len
  } else {
    0
  }
}

/// Backward scan to find last valid entry end
/// 反向扫描查找最后一个有效条目的结束位置
async fn scan_backward<L: Load + ?Sized>(file: &File, start: u64, file_len: u64) -> Option<u64> {
  let mut buf = vec![0u8; SCAN_BACK];
  let mut scan_pos = file_len;

  while scan_pos > start {
    let read_start = scan_pos.saturating_sub(SCAN_BACK as u64).max(start);
    let read_len = (scan_pos - read_start) as usize;

    buf.resize(read_len, 0);
    let slice = std::mem::take(&mut buf).slice(0..read_len);
    let res = file.read_exact_at(slice, read_start).await;
    buf = res.1.into_inner();
    if res.0.is_err() {
      return None;
    }

    // Search magic bytes from end to start using memrchr
    // 使用 memrchr 从后向前搜索 magic 字节
    let mut search_end = read_len;
    while let Some(i) = memchr::memrchr(L::MAGIC, &buf[..search_end]) {
      let slice = &buf[i..];
      let size = parse::<L>(slice);
      if size > 0 && size <= slice.len() {
        return Some(read_start + i as u64 + size as u64);
      }
      search_end = i;
    }

    scan_pos = read_start;
  }

  None
}

#[cfg(test)]
mod tests {
  use futures::StreamExt;
  use zerocopy::FromBytes;

  use super::*;

  // Format: magic(1) + len(1) + data(len) + crc32(4)
  // META_OFFSET=1 (from len byte), meta_len = 1 + data_len
  // crc_offset = 2 + data_len

  #[derive(FromBytes, Debug, Clone, Copy)]
  #[repr(C)]
  struct TestHead {
    len: u8,
  }

  struct TestEntry;

  impl Load for TestEntry {
    type Head = TestHead;

    const MAGIC: u8 = 0xAA;
    const MIN_SIZE: usize = 6; // magic + len + crc32
    const META_OFFSET: usize = 1;

    fn len(buf: &[u8]) -> usize {
      if buf.len() < Self::MIN_SIZE || buf[0] != Self::MAGIC {
        return 0;
      }
      let data_len = buf[1] as usize;
      2 + data_len + 4 // magic + len + data + crc32
    }

    fn crc_offset(len: usize) -> usize {
      len - 4
    }

    fn meta_len(len: usize) -> usize {
      len - 5 // total - magic - crc32
    }

    fn parse_head(buf: &[u8], _len: usize) -> Option<Self::Head> {
      if buf.is_empty() {
        return None;
      }
      Some(TestHead { len: buf[0] })
    }
  }

  fn build_entry(data: &[u8]) -> Vec<u8> {
    let mut buf = vec![TestEntry::MAGIC, data.len() as u8];
    buf.extend_from_slice(data);
    let crc = crc32fast::hash(&buf[1..]);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
  }

  #[test]
  fn test_len_valid() {
    let entry = build_entry(b"hello");
    assert_eq!(TestEntry::len(&entry), 11);
  }

  #[test]
  fn test_verify_valid() {
    let entry = build_entry(b"hello");
    let len = TestEntry::len(&entry);
    assert!(verify::<TestEntry>(&entry, len));
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
    let len = entry.len();
    entry[len - 1] = 0xFF;
    assert!(!verify::<TestEntry>(&entry, len));
  }

  #[test]
  fn test_len_truncated() {
    let entry = build_entry(b"hello");
    assert_eq!(TestEntry::len(&entry[..2]), 0);
  }

  fn run<F: std::future::Future>(f: F) -> F::Output {
    compio_runtime::Runtime::new()
      .expect("create runtime")
      .block_on(f)
  }

  #[test]
  fn test_recover_multiple_entries() {
    run(async {
      use futures::pin_mut;

      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path().join("test.dat");

      let e1 = build_entry(b"one");
      let e2 = build_entry(b"two");
      let e3 = build_entry(b"three");
      let mut data = e1.clone();
      data.extend(&e2);
      data.extend(&e3);
      std::fs::write(&path, &data).expect("write test file");

      let stream = TestEntry::recover(path, 0);
      pin_mut!(stream);

      let mut count = 0;
      let mut last_pos = 0;
      while let Some(item) = stream.next().await {
        count += 1;
        last_pos = item.end;
      }

      assert_eq!(count, 3);
      assert_eq!(last_pos, data.len() as u64);
    });
  }

  #[test]
  fn test_find_end_basic() {
    run(async {
      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path().join("test.dat");

      let e1 = build_entry(b"one");
      let e2 = build_entry(b"two");
      let e3 = build_entry(b"three");
      let mut data = e1.clone();
      data.extend(&e2);
      data.extend(&e3);
      std::fs::write(&path, &data).expect("write test file");

      let file = open_read(&path).await.expect("open file");
      let end = TestEntry::find_end(&file, 0).await.expect("find end");
      assert_eq!(end, data.len() as u64);
    });
  }

  #[test]
  fn test_find_end_with_trailing_garbage() {
    run(async {
      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path().join("test.dat");

      let e1 = build_entry(b"one");
      let e2 = build_entry(b"two");
      let mut data = e1.clone();
      data.extend(&e2);
      let valid_end = data.len();
      // Add garbage
      // 添加垃圾数据
      data.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0xFC]);
      std::fs::write(&path, &data).expect("write test file");

      let file = open_read(&path).await.expect("open file");
      let end = TestEntry::find_end(&file, 0).await.expect("find end");
      assert_eq!(end, valid_end as u64);
    });
  }

  #[test]
  fn test_find_end_empty() {
    run(async {
      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path().join("test.dat");

      std::fs::write(&path, []).expect("write test file");

      let file = open_read(&path).await.expect("open file");
      let end = TestEntry::find_end(&file, 0).await.expect("find end");
      assert_eq!(end, 0);
    });
  }

  #[test]
  fn test_find_end_with_start_offset() {
    run(async {
      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path().join("test.dat");

      // Header + entries
      // 头部 + 条目
      let header = vec![0u8; 12];
      let e1 = build_entry(b"one");
      let e2 = build_entry(b"two");
      let mut data = header.clone();
      data.extend(&e1);
      data.extend(&e2);
      std::fs::write(&path, &data).expect("write test file");

      let file = open_read(&path).await.expect("open file");
      let end = TestEntry::find_end(&file, 12).await.expect("find end");
      assert_eq!(end, data.len() as u64);
    });
  }

  #[test]
  fn test_find_end_is_entry_end_not_start() {
    run(async {
      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path().join("test.dat");

      // Build entries with known sizes
      // 构建已知大小的条目
      let e1 = build_entry(b"aaa"); // size = 2 + 3 + 4 = 9
      let e2 = build_entry(b"bbbbb"); // size = 2 + 5 + 4 = 11
      let e3 = build_entry(b"cc"); // size = 2 + 2 + 4 = 8

      let e1_end = e1.len(); // 9
      let e2_end = e1_end + e2.len(); // 20
      let e3_end = e2_end + e3.len(); // 28

      let mut data = e1.clone();
      data.extend(&e2);
      data.extend(&e3);
      std::fs::write(&path, &data).expect("write test file");

      let file = open_read(&path).await.expect("open file");
      let end = TestEntry::find_end(&file, 0).await.expect("find end");

      // Must be e3's end (28), not e3's start (20)
      // 必须是 e3 的结束位置 (28)，而不是 e3 的开始位置 (20)
      assert_eq!(end, e3_end as u64);
      assert_ne!(end, e2_end as u64); // Not e3's start
    });
  }

  #[test]
  fn test_find_end_single_entry() {
    run(async {
      let dir = tempfile::tempdir().expect("create tempdir");
      let path = dir.path().join("test.dat");

      let e1 = build_entry(b"hello"); // size = 2 + 5 + 4 = 11
      std::fs::write(&path, &e1).expect("write test file");

      let file = open_read(&path).await.expect("open file");
      let end = TestEntry::find_end(&file, 0).await.expect("find end");

      // Must be 11, not 0
      // 必须是 11，而不是 0
      assert_eq!(end, 11);
      assert_ne!(end, 0);
    });
  }
}
