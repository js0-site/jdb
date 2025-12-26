use std::mem;

use jdb_val::{Compress, Head, Key, KeyRef, Kind, ValRef};
use zerocopy::IntoBytes;

#[test]
fn test_meta_header_size() {
  assert_eq!(mem::size_of::<Head>(), 128);
}

#[test]
fn test_new_and_inline_value() {
  let key_bytes = b"test_key";
  let data = b"hello world";

  let mut key = Key::default();
  key.new_inline(key_bytes);

  let meta = Head::new(1, 12345, key_bytes.len() as u16, key, data, 3600, 100, 1).unwrap();

  if let ValRef::Inline(v) = meta.val_ref() {
    assert_eq!(v, data);
  } else {
    panic!("Value should be inline");
  }

  if let KeyRef::Inline(k) = meta.key_ref() {
    assert_eq!(k, key_bytes);
  } else {
    panic!("Key should be inline");
  }

  assert_eq!(meta.seq_id, 1);
  assert_eq!(meta.key_hash, 12345);
  assert_eq!(meta.ttl, 3600);
  assert_eq!(meta.prev_offset, 100);
  assert_eq!(meta.prev_file, 1);
  assert!(meta.header_crc != 0);
}

#[test]
fn test_new_and_normal_value() {
  let key_bytes = b"test_key";
  let data = vec![0u8; 1024];

  let mut key = Key::default();
  key.new_inline(key_bytes);

  let meta = Head::new(2, 67890, key_bytes.len() as u16, key, &data, 0, 0, 0).unwrap();

  if let ValRef::External { len, crc } = meta.val_ref() {
    assert_eq!(len, 1024);
    assert_eq!(crc, crc32fast::hash(&data));
  } else {
    panic!("Value should be external");
  }

  assert_eq!(meta.kind(), Kind::Val);
  assert_eq!(meta.compress(), Compress::None);
}

#[test]
fn test_new_rm() {
  let key_bytes = b"delete_this";
  let mut key = Key::default();
  key.new_inline(key_bytes);

  let meta = Head::new_rm(4, 999, key_bytes.len() as u16, key, 0, 500, 2).unwrap();

  assert!(meta.is_rm());
  if let ValRef::External { len, crc } = meta.val_ref() {
    assert_eq!(len, 0);
    assert_eq!(crc, 0);
  } else {
    panic!("RM record should have external-style 0 len/crc");
  }
}

#[test]
fn test_load() {
  let key_bytes = b"load_key";
  let data = b"load_data";

  let mut key = Key::default();
  key.new_inline(key_bytes);

  let meta1 = Head::new(10, 888, key_bytes.len() as u16, key, data, 0, 0, 0).unwrap();

  let bin = meta1.as_bytes();
  let meta2 = Head::load(bin).unwrap();

  assert_eq!(meta1.seq_id, meta2.seq_id);
  assert_eq!(meta1.header_crc, meta2.header_crc);

  assert_eq!(meta1.key_ref().as_ref(), meta2.key_ref().as_ref());
  assert_eq!(meta1.val_ref().as_ref(), meta2.val_ref().as_ref());
}

#[test]
fn test_external_key() {
  let key_bytes = vec![b'k'; 100]; // 100B key
  let data = b"val";

  let mut key = Key::default();
  key.new_ext(&key_bytes, 123, 45678, crc32fast::hash(&key_bytes));

  let meta = Head::new(1, 999, key_bytes.len() as u16, key, data, 0, 0, 0).unwrap();

  assert_eq!(meta.key_len, 100);

  if let KeyRef::External {
    prefix,
    len,
    file_id,
    offset,
    crc,
  } = meta.key_ref()
  {
    assert_eq!(prefix, &key_bytes[..48]);
    assert_eq!(len, 100);
    assert_eq!(file_id, 123);
    assert_eq!(offset, 45678);
    assert_eq!(crc, crc32fast::hash(&key_bytes));
  } else {
    panic!("Key should be external");
  }
}
