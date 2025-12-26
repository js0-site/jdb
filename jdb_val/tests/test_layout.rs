use std::mem;

use jdb_val::{Head, HeadArgs, Key};

#[test]
fn test_struct_layout() {
  println!("Head size: {}", mem::size_of::<Head>());
  println!("Head align: {}", mem::align_of::<Head>());
  assert_eq!(mem::size_of::<Head>(), 128);

  // 检查各个字段的偏移量
  let header = Head::new(HeadArgs {
    ts: 123456789,
    seq_id: 1,
    key_len: 3,
    key: Key::default(),
    val_bytes: b"val",
    ttl: 0,
    prev_offset: 0,
    prev_file: 0,
  });

  let base_ptr = &header as *const _ as usize;

  println!(
    "header_crc offset: {}",
    (&header.header_crc as *const _ as usize) - base_ptr
  );
  println!(
    "ttl offset: {}",
    (&header.ttl as *const _ as usize) - base_ptr
  );
  println!(
    "ts offset: {}",
    (&header.ts as *const _ as usize) - base_ptr
  );
  println!(
    "seq_id offset: {}",
    (&header.seq_id as *const _ as usize) - base_ptr
  );
  println!(
    "prev_offset offset: {}",
    (&header.prev_offset as *const _ as usize) - base_ptr
  );
  println!(
    "prev_file offset: {}",
    (&header.prev_file as *const _ as usize) - base_ptr
  );
  println!(
    "key_len offset: {}",
    (&header.key_len as *const _ as usize) - base_ptr
  );
  println!(
    "val offset: {}",
    (&header.val as *const _ as usize) - base_ptr
  );
  println!(
    "key offset: {}",
    (&header.key as *const _ as usize) - base_ptr
  );

  assert_eq!((&header.header_crc as *const _ as usize) - base_ptr, 0);
  assert_eq!((&header.ttl as *const _ as usize) - base_ptr, 4);
  assert_eq!((&header.ts as *const _ as usize) - base_ptr, 8);
  assert_eq!((&header.seq_id as *const _ as usize) - base_ptr, 16);
  assert_eq!((&header.prev_offset as *const _ as usize) - base_ptr, 24);
  assert_eq!((&header.prev_file as *const _ as usize) - base_ptr, 32);
  assert_eq!((&header.key_len as *const _ as usize) - base_ptr, 36);
  assert_eq!((&header.val as *const _ as usize) - base_ptr, 40);
  assert_eq!((&header.key as *const _ as usize) - base_ptr, 56);
}
