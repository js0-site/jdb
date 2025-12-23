use aok::{OK, Void};
use jdb_alloc::AlignedBuf;
use jdb_comm::PAGE_SIZE;
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_aligned_buf_create() -> Void {
  let buf = AlignedBuf::with_cap(PAGE_SIZE);
  assert_eq!(buf.cap(), PAGE_SIZE);
  assert_eq!(buf.len(), 0);
  assert!(buf.is_empty());

  // Check alignment 检查对齐
  assert_eq!(buf.as_ptr() as usize % PAGE_SIZE, 0);

  info!("AlignedBuf: {buf:?}");
  OK
}

#[test]
fn test_aligned_buf_page() -> Void {
  let buf = AlignedBuf::page();
  assert_eq!(buf.len(), PAGE_SIZE);
  assert_eq!(buf.cap(), PAGE_SIZE);

  // Check zeroed 检查零初始化
  assert!(buf.iter().all(|&b| b == 0));

  info!("page buf ok");
  OK
}

#[test]
fn test_aligned_buf_extend() -> Void {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE);
  let data = b"hello world";

  buf.extend(data);
  assert_eq!(buf.len(), data.len());
  assert_eq!(&buf[..], data);

  buf.extend(b"!");
  assert_eq!(buf.len(), 12);
  assert_eq!(&buf[..], b"hello world!");

  info!("extend ok");
  OK
}

#[test]
fn test_aligned_buf_clear() -> Void {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE);
  buf.extend(b"test data");
  assert!(!buf.is_empty());

  buf.clear();
  assert!(buf.is_empty());
  assert_eq!(buf.len(), 0);

  info!("clear ok");
  OK
}

#[test]
fn test_aligned_buf_clone() -> Void {
  let mut buf = AlignedBuf::with_cap(PAGE_SIZE);
  buf.extend(b"clone test");

  let cloned = buf.clone();
  assert_eq!(cloned.len(), buf.len());
  assert_eq!(&cloned[..], &buf[..]);

  // Different pointers 不同指针
  assert_ne!(buf.as_ptr(), cloned.as_ptr());

  info!("clone ok");
  OK
}

#[test]
fn test_aligned_buf_deref_mut() -> Void {
  let mut buf = AlignedBuf::zeroed(16);

  buf[0] = 0xAB;
  buf[15] = 0xCD;

  assert_eq!(buf[0], 0xAB);
  assert_eq!(buf[15], 0xCD);

  info!("deref_mut ok");
  OK
}
