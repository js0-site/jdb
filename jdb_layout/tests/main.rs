use aok::{OK, Void};
use jdb_comm::{Lsn, PageID, TableID, Timestamp, PAGE_HEADER_SIZE};
use jdb_layout::{
  crc32, decode, encode, page_type, verify, BlobHeader, BlobPtr, PageHeader, WalEntry,
  BLOB_HEADER_SIZE,
};
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

// ============ Page ============

#[test]
fn test_page_header_rw() -> Void {
  let hdr = PageHeader::new(PageID::new(42), page_type::LEAF, Lsn::new(100));

  let mut buf = [0u8; PAGE_HEADER_SIZE];
  hdr.write(&mut buf);

  let restored = PageHeader::read(&buf);
  assert_eq!(restored.id, 42);
  assert_eq!(restored.typ, page_type::LEAF);
  assert_eq!(restored.lsn, 100);
  assert_eq!(restored.free_off, PAGE_HEADER_SIZE as u16);

  info!("PageHeader: {hdr:?}");
  OK
}

#[test]
fn test_page_payload_size() -> Void {
  assert_eq!(PageHeader::PAYLOAD_SIZE, 4096 - 32);
  info!("payload size: {}", PageHeader::PAYLOAD_SIZE);
  OK
}

// ============ WAL ============

#[test]
fn test_wal_put() -> Void {
  let entry = WalEntry::Put {
    table: TableID::new(123),
    ts: Timestamp::new(999),
    key: b"key1".to_vec(),
    val: b"value1".to_vec(),
  };

  let encoded = encode(&entry);
  let decoded = decode(&encoded).expect("decode");

  if let WalEntry::Put { table, ts, key, val } = decoded {
    assert_eq!(table.0, 123);
    assert_eq!(ts.0, 999);
    assert_eq!(key, b"key1");
    assert_eq!(val, b"value1");
  } else {
    panic!("wrong type");
  }

  info!("WAL Put encoded size: {}", encoded.len());
  OK
}

#[test]
fn test_wal_delete() -> Void {
  let entry = WalEntry::Delete {
    table: TableID::new(456),
    ts: Timestamp::new(888),
    key: b"del_key".to_vec(),
  };

  let encoded = encode(&entry);
  let decoded = decode(&encoded).expect("decode");

  if let WalEntry::Delete { table, ts, key } = decoded {
    assert_eq!(table.0, 456);
    assert_eq!(ts.0, 888);
    assert_eq!(key, b"del_key");
  } else {
    panic!("wrong type");
  }

  info!("WAL Delete ok");
  OK
}

#[test]
fn test_wal_barrier() -> Void {
  let entry = WalEntry::Barrier { lsn: Lsn::new(777) };

  let encoded = encode(&entry);
  let decoded = decode(&encoded).expect("decode");

  if let WalEntry::Barrier { lsn } = decoded {
    assert_eq!(lsn.0, 777);
  } else {
    panic!("wrong type");
  }

  info!("WAL Barrier ok");
  OK
}

// ============ Blob ============

#[test]
fn test_blob_header_rw() -> Void {
  let hdr = BlobHeader::new(1024, 0xDEADBEEF, 123456789);

  let mut buf = [0u8; BLOB_HEADER_SIZE];
  hdr.write(&mut buf);

  let restored = BlobHeader::read(&buf);
  assert_eq!(restored.len, 1024);
  assert_eq!(restored.checksum, 0xDEADBEEF);
  assert_eq!(restored.ts, 123456789);

  info!("BlobHeader: {hdr:?}");
  OK
}

#[test]
fn test_blob_ptr_rw() -> Void {
  let ptr = BlobPtr::new(5, 4096 * 100, 2048);

  let mut buf = [0u8; BlobPtr::SIZE];
  ptr.write(&mut buf);

  let restored = BlobPtr::read(&buf);
  assert_eq!(restored.file_id, 5);
  assert_eq!(restored.offset, 4096 * 100);
  assert_eq!(restored.len, 2048);

  info!("BlobPtr: {ptr:?}");
  OK
}

// ============ Checksum ============

#[test]
fn test_crc32() -> Void {
  let data = b"hello world";

  let c1 = crc32(data);
  let c2 = crc32(data);
  let c3 = crc32(b"hello world!");

  assert_eq!(c1, c2);
  assert_ne!(c1, c3);

  info!("crc32: {c1:#x}");
  OK
}

#[test]
fn test_verify() -> Void {
  let data = b"test data";
  let checksum = crc32(data);

  assert!(verify(data, checksum));
  assert!(!verify(data, checksum + 1));

  info!("verify ok");
  OK
}
