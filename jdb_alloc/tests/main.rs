use jdb_alloc::{AlignedBuf, PAGE_SIZE};

#[test]
fn test_with_cap() {
  let buf = AlignedBuf::with_cap(1024).unwrap();
  assert_eq!(buf.len(), 0);
  assert!(buf.cap() >= PAGE_SIZE); // 最小 4KB
  assert!(buf.is_empty());
  // 检查对齐
  assert_eq!(buf.as_ptr() as usize % PAGE_SIZE, 0);
}

#[test]
fn test_zeroed() {
  let buf = AlignedBuf::zeroed(100).unwrap();
  assert_eq!(buf.len(), 100);
  // 检查是否全零
  for &b in buf.as_ref() {
    assert_eq!(b, 0);
  }
}

#[test]
fn test_page() {
  let buf = AlignedBuf::page().unwrap();
  assert_eq!(buf.len(), PAGE_SIZE);
  assert_eq!(buf.cap(), PAGE_SIZE);
  // 检查对齐
  assert_eq!(buf.as_ptr() as usize % PAGE_SIZE, 0);
}

#[test]
fn test_extend() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  assert_eq!(buf.len(), 0);

  buf.extend(b"hello").unwrap();
  assert_eq!(buf.len(), 5);
  assert_eq!(&buf[..], b"hello");

  buf.extend(b" world").unwrap();
  assert_eq!(buf.len(), 11);
  assert_eq!(&buf[..], b"hello world");
}

#[test]
fn test_clear() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(b"hello").unwrap();
  assert_eq!(buf.len(), 5);

  buf.clear();
  assert_eq!(buf.len(), 0);
  assert!(buf.is_empty());
}

#[test]
fn test_deref() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(b"test").unwrap();

  // Deref
  let slice: &[u8] = &buf;
  assert_eq!(slice, b"test");

  // DerefMut
  let slice_mut: &mut [u8] = &mut buf;
  slice_mut[0] = b'T';
  assert_eq!(&buf[..], b"Test");
}

#[test]
fn test_clone() {
  let mut buf1 = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf1.extend(b"hello").unwrap();

  let buf2 = buf1.clone();
  assert_eq!(buf2.len(), buf1.len());
  assert_eq!(&buf2[..], &buf1[..]);
  // 确保是独立的内存
  assert_ne!(buf1.as_ptr(), buf2.as_ptr());
}

#[test]
fn test_set_len() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  assert_eq!(buf.len(), PAGE_SIZE);

  unsafe { buf.set_len(100) };
  assert_eq!(buf.len(), 100);
}

#[test]
fn test_alignment() {
  // 测试多个 buffer 都是对齐的
  for _ in 0..10 {
    let buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
    assert_eq!(buf.as_ptr() as usize % PAGE_SIZE, 0);
  }
}

#[test]
fn test_large_buffer() {
  // 测试大缓冲区
  let size = PAGE_SIZE * 16; // 64KB
  let buf = AlignedBuf::zeroed(size).unwrap();
  assert_eq!(buf.len(), size);
  assert!(buf.cap() >= size);
  assert_eq!(buf.as_ptr() as usize % PAGE_SIZE, 0);
}

#[test]
fn test_debug() {
  let buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  let debug_str = format!("{:?}", buf);
  assert!(debug_str.contains("AlignedBuf"));
  assert!(debug_str.contains("len"));
  assert!(debug_str.contains("cap"));
}
