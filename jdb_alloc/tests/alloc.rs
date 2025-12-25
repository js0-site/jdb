use jdb_alloc::{ALIGNMENT, AlignedBuf, PAGE_SIZE, RawIoBuf};

#[test]
fn buf() {
  // Test with_cap
  let mut buf = AlignedBuf::with_cap(1024).unwrap();
  assert!(buf.cap() >= PAGE_SIZE);
  assert_eq!(buf.len(), 0);
  assert!(buf.is_empty());

  // Test extend
  buf.extend(b"hello").unwrap();
  assert_eq!(buf.len(), 5);
  assert_eq!(&buf[..], b"hello");

  // Test truncate
  buf.truncate(2);
  assert_eq!(buf.len(), 2);
  assert_eq!(&buf[..], b"he");

  // Test clear
  buf.clear();
  assert_eq!(buf.len(), 0);
  assert!(buf.is_empty());

  // Test zeroed
  let zbuf = AlignedBuf::zeroed(PAGE_SIZE).unwrap();
  assert_eq!(zbuf.len(), PAGE_SIZE);
  assert!(zbuf.iter().all(|&b| b == 0));

  // Test page
  let pbuf = AlignedBuf::page().unwrap();
  assert_eq!(pbuf.len(), PAGE_SIZE);
  assert_eq!(pbuf.cap(), PAGE_SIZE);

  // Test Alignment
  assert_eq!(buf.as_ptr() as usize % ALIGNMENT, 0);
  assert_eq!(zbuf.as_ptr() as usize % ALIGNMENT, 0);
  assert_eq!(pbuf.as_ptr() as usize % ALIGNMENT, 0);
}

#[test]
fn raw() {
  let mut buf = AlignedBuf::page().unwrap();
  buf.clear();
  buf.extend(b"world").unwrap();

  // Test as_raw
  let raw = unsafe { buf.as_raw() };
  assert_eq!(raw.len(), 5);
  assert_eq!(raw.cap(), PAGE_SIZE);
  assert_eq!(raw.as_slice(), b"world");

  // Test as_raw_view
  let raw_view = unsafe { buf.as_raw_view() };
  assert_eq!(raw_view.len(), 5);

  // Test RawIoBuf::new
  let raw2 = unsafe { RawIoBuf::new(buf.as_mut_ptr(), buf.cap()) };
  assert_eq!(raw2.len(), 0);

  // Test RawIoBuf::with_len
  let raw2 = raw2.with_len(3);
  assert_eq!(raw2.len(), 3);

  // Test from_slice
  let mut data = [1, 2, 3];
  let raw3 = RawIoBuf::from_slice(&mut data);
  assert_eq!(raw3.len(), 3);
  assert_eq!(raw3.as_slice(), &[1, 2, 3]);

  // Test set_len
  let mut raw4 = raw3;
  unsafe { raw4.set_len(1) };
  assert_eq!(raw4.len(), 1);
}

#[test]
fn raw_slice() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE * 2).unwrap();
  let raw = unsafe { buf.as_raw() };

  // Test slice (for reading)
  let sliced = unsafe { raw.slice(PAGE_SIZE, PAGE_SIZE) };
  assert_eq!(sliced.len(), 0);
  assert_eq!(sliced.cap(), PAGE_SIZE);
  assert_eq!(sliced.as_ptr(), unsafe { raw.as_ptr().add(PAGE_SIZE) });

  // Test slice_data (for writing)
  let sliced_data = unsafe { raw.slice_data(0, PAGE_SIZE) };
  assert_eq!(sliced_data.len(), PAGE_SIZE);
  assert_eq!(sliced_data.cap(), PAGE_SIZE);

  // Test slice_unchecked
  let sliced_un = unsafe { raw.slice_unchecked(PAGE_SIZE, 100) };
  assert_eq!(sliced_un.len(), 0);
  assert_eq!(sliced_un.cap(), 100);
}

#[test]
fn clone() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(b"clone me").unwrap();

  // Test try_clone
  let cloned = buf.try_clone().unwrap();
  assert_eq!(cloned.len(), buf.len());
  assert_eq!(cloned.cap(), buf.cap());
  assert_eq!(&cloned[..], &buf[..]);
  assert_ne!(cloned.as_ptr(), buf.as_ptr());

  // Test Clone trait
  #[allow(clippy::redundant_clone)]
  let cloned2 = buf.clone();
  assert_eq!(&cloned2[..], &buf[..]);
}

#[test]
fn parts() {
  let mut buf = AlignedBuf::page().unwrap();
  buf.clear();
  buf.extend(b"parts").unwrap();
  let (ptr, len, cap) = buf.into_raw_parts();

  assert_eq!(len, 5);
  assert_eq!(cap, PAGE_SIZE);

  let buf2 = unsafe { AlignedBuf::from_raw_parts(ptr, len, cap) };
  assert_eq!(buf2.len(), 5);
  assert_eq!(&buf2[..], b"parts");

  let raw = unsafe { RawIoBuf::from_raw_parts(ptr, len, cap) };
  assert_eq!(raw.len(), 5);
  assert_eq!(raw.as_slice(), b"parts");
}

#[test]
fn error() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  buf.extend(&vec![0; PAGE_SIZE]).unwrap();

  // Test Overflow
  let res = buf.extend(b"too much");
  assert!(res.is_err());
  if let Err(jdb_alloc::Error::Overflow(new, cap)) = res {
    assert_eq!(new, PAGE_SIZE + 8);
    assert_eq!(cap, PAGE_SIZE);
  } else {
    panic!("expected Overflow error");
  }
}

#[test]
fn debug() {
  let buf = AlignedBuf::page().unwrap();
  let s = format!("{buf:?}");
  assert!(s.contains("AlignedBuf"));
  assert!(s.contains("len"));
  assert!(s.contains("cap"));

  let raw = unsafe { buf.as_raw_view() };
  let s = format!("{raw:?}");
  assert!(s.contains("RawIoBuf"));
  assert!(s.contains("ptr"));
}

#[test]
fn io_traits() {
  use compio_buf::{IoBuf, IoBufMut, SetBufInit};

  let mut buf = AlignedBuf::page().unwrap();
  assert_eq!(IoBuf::as_buf_ptr(&buf), buf.as_ptr());
  assert_eq!(IoBuf::buf_len(&buf), buf.len());
  assert_eq!(IoBuf::buf_capacity(&buf), buf.cap());

  assert_eq!(IoBufMut::as_buf_mut_ptr(&mut buf), buf.as_mut_ptr());

  unsafe { SetBufInit::set_buf_init(&mut buf, 10) };
  assert_eq!(buf.len(), 10);

  let mut raw = unsafe { buf.as_raw_view() };
  assert_eq!(IoBuf::as_buf_ptr(&raw), raw.as_ptr());
  assert_eq!(IoBuf::buf_len(&raw), raw.len());
  assert_eq!(IoBuf::buf_capacity(&raw), raw.cap());

  assert_eq!(IoBufMut::as_buf_mut_ptr(&mut raw), raw.as_mut_ptr());

  unsafe { SetBufInit::set_buf_init(&mut raw, 20) };
  assert_eq!(raw.len(), 20);
}
