use jdb_alloc::{ALIGNMENT, AlignedBuf, PAGE_SIZE, RawIoBuf};

// ============================================================================
// AlignedBuf Tests
// ============================================================================

#[test]
fn test_with_cap() {
  let buf = AlignedBuf::with_cap(1024).unwrap();
  assert_eq!(buf.len(), 0);
  assert!(buf.cap() >= PAGE_SIZE);
  assert!(buf.is_empty());
  assert_eq!(buf.as_ptr() as usize % ALIGNMENT, 0);
}

#[test]
fn test_with_cap_large() {
  let cap = PAGE_SIZE * 4;
  let buf = AlignedBuf::with_cap(cap).unwrap();
  assert_eq!(buf.cap(), cap);
  assert_eq!(buf.as_ptr() as usize % ALIGNMENT, 0);
}

#[test]
fn test_zeroed() {
  let buf = AlignedBuf::zeroed(100).unwrap();
  assert_eq!(buf.len(), 100);
  for &b in buf.as_ref() {
    assert_eq!(b, 0);
  }
}

#[test]
fn test_zeroed_large() {
  let size = PAGE_SIZE * 8;
  let buf = AlignedBuf::zeroed(size).unwrap();
  assert_eq!(buf.len(), size);
  assert!(buf.cap() >= size);
}

#[test]
fn test_page() {
  let buf = AlignedBuf::page().unwrap();
  assert_eq!(buf.len(), PAGE_SIZE);
  assert_eq!(buf.cap(), PAGE_SIZE);
  assert_eq!(buf.as_ptr() as usize % ALIGNMENT, 0);
  // page() returns zeroed memory
  for &b in buf.as_ref() {
    assert_eq!(b, 0);
  }
}

#[test]
fn test_as_raw() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  buf.as_mut()[0] = 42;
  let raw = unsafe { buf.as_raw() };
  assert_eq!(raw.cap(), PAGE_SIZE);
  assert_eq!(raw.len(), PAGE_SIZE);
  assert_eq!(raw.as_ptr(), buf.as_ptr());
}

#[test]
fn test_as_raw_view() {
  let buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let raw = unsafe { buf.as_raw_view() };
  assert_eq!(raw.cap(), PAGE_SIZE);
  assert_eq!(raw.as_ptr(), buf.as_ptr());
}

#[test]
fn test_len_cap_is_empty() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  assert_eq!(buf.len(), 0);
  assert!(buf.cap() >= PAGE_SIZE);
  assert!(buf.is_empty());

  buf.extend(b"data").unwrap();
  assert_eq!(buf.len(), 4);
  assert!(!buf.is_empty());
}

#[test]
fn test_as_ptr_as_mut_ptr() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let ptr = buf.as_ptr();
  let mut_ptr = buf.as_mut_ptr();
  assert_eq!(ptr, mut_ptr as *const u8);
  assert_eq!(ptr as usize % ALIGNMENT, 0);
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
fn test_truncate() {
  let mut buf = AlignedBuf::zeroed(100).unwrap();
  assert_eq!(buf.len(), 100);

  buf.truncate(50);
  assert_eq!(buf.len(), 50);

  // truncate to larger does nothing
  buf.truncate(200);
  assert_eq!(buf.len(), 50);

  buf.truncate(0);
  assert_eq!(buf.len(), 0);
}

#[test]
fn test_set_len() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  unsafe { buf.set_len(100) };
  assert_eq!(buf.len(), 100);
  unsafe { buf.set_len(0) };
  assert_eq!(buf.len(), 0);
}

#[test]
fn test_try_clone() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(b"test data").unwrap();

  let cloned = buf.try_clone().unwrap();
  assert_eq!(cloned.len(), buf.len());
  assert_eq!(&cloned[..], &buf[..]);
  assert_ne!(cloned.as_ptr(), buf.as_ptr());
}

#[test]
fn test_extend() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(b"hello").unwrap();
  assert_eq!(&buf[..], b"hello");

  buf.extend(b" world").unwrap();
  assert_eq!(&buf[..], b"hello world");
}

#[test]
fn test_extend_overflow() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  let large_data = vec![0u8; PAGE_SIZE + 1];
  assert!(buf.extend(&large_data).is_err());
}

#[test]
fn test_deref() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(b"test").unwrap();

  let slice: &[u8] = &buf;
  assert_eq!(slice, b"test");

  let slice_mut: &mut [u8] = &mut buf;
  slice_mut[0] = b'T';
  assert_eq!(&buf[..], b"Test");
}

#[test]
fn test_as_ref_as_mut() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(b"data").unwrap();

  let r: &[u8] = buf.as_ref();
  assert_eq!(r, b"data");

  let m: &mut [u8] = buf.as_mut();
  m[0] = b'D';
  assert_eq!(buf.as_ref(), b"Data");
}

#[test]
fn test_clone() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(b"clone test").unwrap();

  let cloned = buf.clone();
  assert_eq!(cloned.len(), buf.len());
  assert_eq!(&cloned[..], &buf[..]);
  assert_ne!(cloned.as_ptr(), buf.as_ptr());
}

#[test]
fn test_debug_aligned_buf() {
  let buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  let s = format!("{buf:?}");
  assert!(s.contains("AlignedBuf"));
  assert!(s.contains("len"));
  assert!(s.contains("cap"));
}

#[test]
fn test_alignment_multiple() {
  for _ in 0..10 {
    let buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
    assert_eq!(buf.as_ptr() as usize % ALIGNMENT, 0);
  }
}

// ============================================================================
// RawIoBuf Tests
// ============================================================================

#[test]
fn test_raw_new() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let raw = unsafe { RawIoBuf::new(buf.as_mut_ptr(), PAGE_SIZE) };
  assert_eq!(raw.len(), 0);
  assert_eq!(raw.cap(), PAGE_SIZE);
}

#[test]
fn test_raw_from_slice() {
  let mut data = [0u8; 256];
  data[0] = 42;
  let raw = RawIoBuf::from_slice(&mut data);
  assert_eq!(raw.len(), 256);
  assert_eq!(raw.cap(), 256);
  assert_eq!(raw.as_slice()[0], 42);
}

#[test]
fn test_raw_slice() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE * 4).unwrap();
  let raw = unsafe { buf.as_raw() };

  // slice at aligned offset
  let sub = unsafe { raw.slice(PAGE_SIZE, PAGE_SIZE) };
  assert_eq!(sub.len(), 0);
  assert_eq!(sub.cap(), PAGE_SIZE);
  assert_eq!(sub.as_ptr() as usize % ALIGNMENT, 0);
}

#[test]
fn test_raw_slice_data() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE * 4).unwrap();
  buf.as_mut()[0] = 42;
  let raw = unsafe { buf.as_raw() };

  // slice_data for write (len = cap)
  let sub = unsafe { raw.slice_data(0, PAGE_SIZE) };
  assert_eq!(sub.len(), PAGE_SIZE);
  assert_eq!(sub.cap(), PAGE_SIZE);
  assert_eq!(sub.as_slice()[0], 42);
}

#[test]
fn test_raw_slice_unchecked() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE * 4).unwrap();
  let raw = unsafe { buf.as_raw() };

  let sub = unsafe { raw.slice_unchecked(PAGE_SIZE * 2, PAGE_SIZE) };
  assert_eq!(sub.cap(), PAGE_SIZE);
  assert_eq!(sub.as_ptr() as usize % ALIGNMENT, 0);
}

#[test]
fn test_raw_len_cap_is_empty() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let raw = unsafe { RawIoBuf::new(buf.as_mut_ptr(), PAGE_SIZE) };
  assert_eq!(raw.len(), 0);
  assert_eq!(raw.cap(), PAGE_SIZE);
  assert!(raw.is_empty());
}

#[test]
fn test_raw_as_ptr_as_mut_ptr() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let raw = unsafe { buf.as_raw() };
  assert_eq!(raw.as_ptr(), buf.as_ptr());
  assert_eq!(raw.as_mut_ptr(), buf.as_mut_ptr());
}

#[test]
fn test_raw_as_slice() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  buf.as_mut()[0] = 99;
  let raw = unsafe { buf.as_raw() };
  assert_eq!(raw.as_slice()[0], 99);
}

#[test]
fn test_raw_as_mut_slice() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let mut raw = unsafe { buf.as_raw() };
  raw.as_mut_slice()[0] = 77;
  assert_eq!(buf.as_ref()[0], 77);
}

#[test]
fn test_raw_set_len() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let mut raw = unsafe { RawIoBuf::new(buf.as_mut_ptr(), PAGE_SIZE) };
  assert_eq!(raw.len(), 0);
  unsafe { raw.set_len(512) };
  assert_eq!(raw.len(), 512);
}

#[test]
fn test_raw_copy() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let raw1 = unsafe { buf.as_raw() };
  let raw2 = raw1; // Copy
  assert_eq!(raw1.as_ptr(), raw2.as_ptr());
  assert_eq!(raw1.cap(), raw2.cap());
}

#[test]
fn test_raw_clone() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let raw1 = unsafe { buf.as_raw() };
  let raw2 = raw1;
  assert_eq!(raw1.as_ptr(), raw2.as_ptr());
}

#[test]
fn test_raw_debug() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let raw = unsafe { buf.as_raw() };
  let s = format!("{raw:?}");
  assert!(s.contains("RawIoBuf"));
  assert!(s.contains("len"));
  assert!(s.contains("cap"));
}

// ============================================================================
// Compio Trait Tests
// ============================================================================

#[test]
fn test_aligned_buf_io_buf() {
  use compio_buf::IoBuf;
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(b"io test").unwrap();

  assert_eq!(buf.as_buf_ptr(), buf.as_ptr());
  assert_eq!(buf.buf_len(), 7);
  assert_eq!(buf.buf_capacity(), buf.cap());
}

#[test]
fn test_aligned_buf_io_buf_mut() {
  use compio_buf::IoBufMut;
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  assert_eq!(buf.as_buf_mut_ptr(), buf.as_mut_ptr());
}

#[test]
fn test_aligned_buf_set_buf_init() {
  use compio_buf::SetBufInit;
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  unsafe { buf.set_buf_init(256) };
  assert_eq!(buf.len(), 256);
}

#[test]
fn test_raw_io_buf() {
  use compio_buf::IoBuf;
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let mut raw = unsafe { RawIoBuf::new(buf.as_mut_ptr(), PAGE_SIZE) };
  unsafe { raw.set_len(100) };

  assert_eq!(raw.as_buf_ptr(), buf.as_ptr());
  assert_eq!(raw.buf_len(), 100);
  assert_eq!(raw.buf_capacity(), PAGE_SIZE);
}

#[test]
fn test_raw_io_buf_mut() {
  use compio_buf::IoBufMut;
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let mut raw = unsafe { buf.as_raw() };
  assert_eq!(raw.as_buf_mut_ptr(), buf.as_mut_ptr());
}

#[test]
fn test_raw_set_buf_init() {
  use compio_buf::SetBufInit;
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let mut raw = unsafe { buf.as_raw() };
  unsafe { raw.set_buf_init(512) };
  assert_eq!(raw.len(), 512);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_min_cap_enforcement() {
  // with_cap(0) should still allocate PAGE_SIZE
  let buf = AlignedBuf::with_cap(0).unwrap();
  assert!(buf.cap() >= PAGE_SIZE);
}

#[test]
fn test_zeroed_min_cap() {
  let buf = AlignedBuf::zeroed(1).unwrap();
  assert!(buf.cap() >= PAGE_SIZE);
  assert_eq!(buf.len(), 1);
}

#[test]
fn test_extend_empty() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(b"").unwrap();
  assert_eq!(buf.len(), 0);
}

#[test]
fn test_arena_pattern() {
  // Simulate Buffer Pool Arena usage
  let arena = AlignedBuf::zeroed(PAGE_SIZE * 4).unwrap();
  let base = unsafe { arena.as_raw_view() };

  // Slice into frames
  let frame0 = unsafe { base.slice_unchecked(0, PAGE_SIZE) };
  let frame1 = unsafe { base.slice_unchecked(PAGE_SIZE, PAGE_SIZE) };
  let frame2 = unsafe { base.slice_unchecked(PAGE_SIZE * 2, PAGE_SIZE) };

  assert_eq!(frame0.cap(), PAGE_SIZE);
  assert_eq!(frame1.cap(), PAGE_SIZE);
  assert_eq!(frame2.cap(), PAGE_SIZE);

  // Verify alignment
  assert_eq!(frame0.as_ptr() as usize % ALIGNMENT, 0);
  assert_eq!(frame1.as_ptr() as usize % ALIGNMENT, 0);
  assert_eq!(frame2.as_ptr() as usize % ALIGNMENT, 0);

  // Verify offsets
  assert_eq!(
    frame1.as_ptr() as usize - frame0.as_ptr() as usize,
    PAGE_SIZE
  );
}

#[test]
fn test_slice_into_raws() {
  let arena = AlignedBuf::zeroed(PAGE_SIZE * 8).unwrap();
  let raws: Vec<_> = unsafe { arena.slice_into_raws(PAGE_SIZE).collect() };

  assert_eq!(raws.len(), 8);
  for (i, raw) in raws.iter().enumerate() {
    assert_eq!(raw.cap(), PAGE_SIZE);
    assert_eq!(raw.len(), 0);
    assert_eq!(raw.as_ptr() as usize % ALIGNMENT, 0);
    assert_eq!(
      raw.as_ptr() as usize,
      arena.as_ptr() as usize + i * PAGE_SIZE
    );
  }
}

#[test]
fn test_into_from_raw_parts() {
  let buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let ptr = buf.as_ptr();
  let (nptr, len, cap) = buf.into_raw_parts();

  assert_eq!(nptr.as_ptr() as *const u8, ptr);
  assert_eq!(len, PAGE_SIZE);
  assert_eq!(cap, PAGE_SIZE);

  // Reconstruct and drop properly
  let buf2 = unsafe { AlignedBuf::from_raw_parts(nptr, len, cap) };
  assert_eq!(buf2.len(), PAGE_SIZE);
  assert_eq!(buf2.cap(), PAGE_SIZE);
}

#[test]
fn test_raw_with_len() {
  let mut buf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  let raw = unsafe { RawIoBuf::new(buf.as_mut_ptr(), PAGE_SIZE).with_len(512) };
  assert_eq!(raw.len(), 512);
  assert_eq!(raw.cap(), PAGE_SIZE);
}
