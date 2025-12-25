use jdb_alloc::{AlignedBuf, PAGE_SIZE, RawIoBuf};

#[test]
fn aligned_buf_boundary() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE).unwrap();
  assert_eq!(buf.cap(), PAGE_SIZE);

  // Exact capacity extend
  let data = vec![1u8; PAGE_SIZE];
  buf.extend(&data).unwrap();
  assert_eq!(buf.len(), PAGE_SIZE);

  // Overflow by 1 byte
  let res = buf.extend(&[2]);
  assert!(res.is_err());
}

#[test]
fn aligned_buf_truncate() {
  let mut buf = AlignedBuf::page().unwrap();
  buf.clear();
  buf.extend(b"hello").unwrap();

  // Truncate to current len
  buf.truncate(5);
  assert_eq!(buf.len(), 5);

  // Truncate to larger than current len
  buf.truncate(10);
  assert_eq!(buf.len(), 5);

  // Truncate to smaller
  buf.truncate(2);
  assert_eq!(buf.len(), 2);
  assert_eq!(&buf[..], b"he");
}

#[test]
fn raw_io_buf_slicing() {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE * 4).unwrap();
  let raw = unsafe { buf.as_raw() };

  // Slice at page boundaries
  let s1 = unsafe { raw.slice(PAGE_SIZE, PAGE_SIZE) };
  assert_eq!(s1.as_ptr(), unsafe { raw.as_ptr().add(PAGE_SIZE) });
  assert_eq!(s1.cap(), PAGE_SIZE);

  let s2 = unsafe { raw.slice(PAGE_SIZE * 2, PAGE_SIZE) };
  assert_eq!(s2.as_ptr(), unsafe { raw.as_ptr().add(PAGE_SIZE * 2) });

  // Slice data
  let s3 = unsafe { raw.slice_data(PAGE_SIZE, PAGE_SIZE) };
  assert_eq!(s3.len(), PAGE_SIZE);
}

#[test]
fn aligned_buf_as_raw_mut() {
  let mut buf = AlignedBuf::page().unwrap();
  buf.clear();
  buf.extend(b"init").unwrap();

  let mut raw = unsafe { buf.as_raw() };
  assert_eq!(raw.as_slice(), b"init");

  // Modify via raw
  let slice = raw.as_mut_slice();
  slice[0] = b'x';

  assert_eq!(&buf[..4], b"xnit");
}

#[test]
fn aligned_buf_as_ref_mut() {
  let mut buf = AlignedBuf::page().unwrap();
  buf.clear();
  buf.extend(b"test").unwrap();

  let r: &[u8] = buf.as_ref();
  assert_eq!(r, b"test");

  let m: &mut [u8] = buf.as_mut();
  m[0] = b'T';
  assert_eq!(&buf[..4], b"Test");
}

#[test]
fn raw_io_buf_from_parts() {
  let mut buf = AlignedBuf::page().unwrap();
  let ptr = std::ptr::NonNull::new(buf.as_mut_ptr()).unwrap();
  let raw = unsafe { RawIoBuf::from_raw_parts(ptr, 10, PAGE_SIZE) };
  assert_eq!(raw.len(), 10);
  assert_eq!(raw.cap(), PAGE_SIZE);
  assert_eq!(raw.as_ptr(), ptr.as_ptr());
}
