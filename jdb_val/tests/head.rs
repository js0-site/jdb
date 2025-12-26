use std::mem;

use jdb_val::{Compress, Head, HeadArgs, Key, KeyRef, Kind};
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

  let meta = Head::new(HeadArgs {
    ts: 1000,
    seq_id: 1,
    key_len: key_bytes.len() as u16,
    key,
    val_bytes: data,
    ttl: 3600,
    prev_offset: 100,
    prev_file: 1,
  });

  assert_eq!(meta.key_hash(), 0); // Inline key hash is 0 currently

  assert_eq!(meta.kind(), Kind::Inline);
  assert_eq!(meta.val.inline(meta.extra_meta as usize), data);

  if let KeyRef::Inline(k) = meta.key_ref() {
    assert_eq!(k, key_bytes);
  } else {
    panic!("Key should be inline");
  }

  assert_eq!(meta.seq_id, 1);
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

  let meta = Head::new(HeadArgs {
    ts: 1000,
    seq_id: 2,
    key_len: key_bytes.len() as u16,
    key,
    val_bytes: &data,
    ttl: 0,
    prev_offset: 0,
    prev_file: 0,
  });

  assert_eq!(meta.kind(), Kind::Val);
  let (len, crc) = meta.val.external();
  assert_eq!(len, 1024);
  assert_eq!(crc, crc32fast::hash(&data));

  assert_eq!(meta.kind(), Kind::Val);
  assert_eq!(meta.compress(), Compress::None);
}

#[test]
fn test_new_rm() {
  let key_bytes = b"delete_this";
  let mut key = Key::default();
  key.new_inline(key_bytes);

  let meta = Head::new_rm(1000, 4, key_bytes.len() as u16, key, 0, 500, 2);

  assert!(meta.is_rm());
  let (len, crc) = meta.val.external();
  assert_eq!(len, 0);
  assert_eq!(crc, 0);
}

#[test]
fn test_load() {
  let key_bytes = b"load_key";
  let data = b"load_data";

  let mut key = Key::default();
  key.new_inline(key_bytes);

  let meta1 = Head::new(HeadArgs {
    ts: 1000,
    seq_id: 10,
    key_len: key_bytes.len() as u16,
    key,
    val_bytes: data,
    ttl: 0,
    prev_offset: 0,
    prev_file: 0,
  });

  let bin = meta1.as_bytes();
  let meta2 = Head::load(bin).unwrap();

  assert_eq!(meta1.seq_id, meta2.seq_id);
  assert_eq!(meta1.header_crc, meta2.header_crc);

  assert_eq!(meta1.key_ref().as_ref(), meta2.key_ref().as_ref());
  assert_eq!(
    meta1.val.inline(meta1.extra_meta as usize),
    meta2.val.inline(meta2.extra_meta as usize)
  );
}

#[test]
fn test_external_key() {
  let key_bytes = vec![b'k'; 100]; // 100B key
  let data = b"val";

  let mut key = Key::default();
  key.new_ext(999, &key_bytes, 123, 45678, crc32fast::hash(&key_bytes));

  let meta = Head::new(HeadArgs {
    ts: 1000,
    seq_id: 1,
    key_len: key_bytes.len() as u16,
    key,
    val_bytes: data,
    ttl: 0,
    prev_offset: 0,
    prev_file: 0,
  });

  assert_eq!(meta.key_len, 100);
  assert_eq!(meta.key_hash(), 999);

  if let KeyRef::External {
    hash: _,
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
