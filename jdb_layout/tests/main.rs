use jdb_layout::{crc32, verify, page_type, PageHeader, BlobPtr, PAGE_MAGIC};

// Page size constant - 4KB
pub const PAGE_SIZE: usize = 4096;

// Page header size constant
pub const PAGE_HEADER_SIZE: usize = 32;

#[test]
fn test_page_header_size() {
  assert_eq!(size_of::<PageHeader>(), PAGE_HEADER_SIZE);
  assert_eq!(PageHeader::SIZE, 32);
  assert_eq!(PageHeader::PAYLOAD_SIZE, PAGE_SIZE - 32);
}

#[test]
fn test_page_header_default() {
  let h = PageHeader::default();
  assert_eq!(h.magic, PAGE_MAGIC);
  assert_eq!(h.page_id, 0);
  assert_eq!(h.typ, 0);
  assert_eq!(h.free_start, PAGE_HEADER_SIZE as u16);
  assert_eq!(h.free_end, PAGE_SIZE as u16);
  assert_eq!(h.next, u32::MAX);
  assert!(h.is_valid());
}

#[test]
fn test_page_header_new() {
  let h = PageHeader::new(42, page_type::DATA);
  assert_eq!(h.page_id, 42);
  assert_eq!(h.typ, page_type::DATA);
  assert!(h.is_valid());
}

#[test]
fn test_page_header_free_space() {
  let mut h = PageHeader::default();
  assert_eq!(h.free_space(), PAGE_SIZE - PAGE_HEADER_SIZE);

  h.free_start = 100;
  h.free_end = 200;
  assert_eq!(h.free_space(), 100);
}

#[test]
fn test_page_header_encode_decode() {
  let h1 = PageHeader {
    magic: PAGE_MAGIC,
    page_id: 12345,
    typ: page_type::INDEX_LEAF,
    flags: 0x0F,
    count: 100,
    free_start: 64,
    free_end: 4000,
    next: 999,
    checksum: 0xDEADBEEF,
    _pad: [0; 8],
  };

  let mut buf = [0u8; 32];
  h1.encode(&mut buf);

  let h2 = PageHeader::decode(&buf);
  assert_eq!(h2.magic, h1.magic);
  assert_eq!(h2.page_id, h1.page_id);
  assert_eq!(h2.typ, h1.typ);
  assert_eq!(h2.flags, h1.flags);
  assert_eq!(h2.count, h1.count);
  assert_eq!(h2.free_start, h1.free_start);
  assert_eq!(h2.free_end, h1.free_end);
  assert_eq!(h2.next, h1.next);
  assert_eq!(h2.checksum, h1.checksum);
}

#[test]
fn test_blob_ptr_size() {
  assert_eq!(size_of::<BlobPtr>(), BlobPtr::SIZE);
  assert_eq!(BlobPtr::SIZE, 16);
}

#[test]
fn test_blob_ptr_invalid() {
  let p = BlobPtr::INVALID;
  assert!(!p.is_valid());
  assert_eq!(p.file_id, u32::MAX);
}

#[test]
fn test_blob_ptr_new() {
  let p = BlobPtr::new(1, 1024, 256);
  assert!(p.is_valid());
  assert_eq!(p.file_id, 1);
  assert_eq!(p.offset, 1024);
  assert_eq!(p.len, 256);
}

#[test]
fn test_blob_ptr_encode_decode() {
  let p1 = BlobPtr::new(42, 0x123456789ABC, 65536);

  let mut buf = [0u8; 16];
  p1.encode(&mut buf);

  let p2 = BlobPtr::decode(&buf);
  assert_eq!(p2.file_id, p1.file_id);
  assert_eq!(p2.offset, p1.offset);
  assert_eq!(p2.len, p1.len);
}

#[test]
fn test_crc32_deterministic() {
  let data = b"hello world";
  let c1 = crc32(data);
  let c2 = crc32(data);
  assert_eq!(c1, c2);
}

#[test]
fn test_crc32_different_input() {
  let c1 = crc32(b"hello");
  let c2 = crc32(b"world");
  assert_ne!(c1, c2);
}

#[test]
fn test_crc32_verify() {
  let data = b"test data";
  let checksum = crc32(data);
  assert!(verify(data, checksum));
  assert!(!verify(data, checksum + 1));
}

#[test]
fn test_page_types() {
  assert_eq!(page_type::DATA, 1);
  assert_eq!(page_type::INDEX_LEAF, 2);
  assert_eq!(page_type::INDEX_INTERNAL, 3);
  assert_eq!(page_type::OVERFLOW, 4);
  assert_eq!(page_type::META, 5);
}
