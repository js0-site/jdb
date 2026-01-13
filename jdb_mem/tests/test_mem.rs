use std::{
  future::{Future, ready},
  io,
};

use aok::{OK, Void};
use jdb_base::{
  Discard, Flag, Mem as _, Pos,
  ckp::Meta,
  sst::{Kv, Level, MemToSst},
};
use jdb_mem::Mem;

#[derive(Debug)]
struct MockSst;

impl MemToSst for MockSst {
  type Error = io::Error;

  fn write<'a>(
    &self,
    _iter: impl Iterator<Item = Kv<'a>>,
  ) -> impl Future<Output = Result<Meta, Self::Error>> {
    ready(Ok(Meta {
      sst: jdb_base::ckp::Sst {
        level: Level::L0,
        rmed: 0,
        size: 0,
      },
      meta: jdb_base::sst::Meta {
        id: 0,
        min: Box::default(),
        max: Box::default(),
      },
    }))
  }

  fn push(&mut self, _meta: Meta) {}
}

#[derive(Debug)]
struct MockDiscard;

impl Discard for MockDiscard {
  type Error = io::Error;

  fn discard(&mut self, _key: &[u8], _pos: &Pos) {}

  fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> {
    ready(Ok(()))
  }
}

#[compio::test]
async fn test_mem_ops() -> Void {
  let mut mem = Mem::new(64 * 1024 * 1024, MockSst, MockDiscard);

  // 1. Put initial data
  let k1 = b"key1".to_vec();
  let p1 = Pos::new(1, Flag::INFILE, 0, 100, 10);
  mem.put(k1.clone(), p1).await.unwrap();

  assert_eq!(mem.get(&k1[..]), Some(p1));

  // 2. Rotate
  mem.rotate().unwrap();
  assert!(mem.freeze.is_some());
  // Should still find k1 in old map
  assert_eq!(mem.get(&k1[..]), Some(p1));

  // 3. Put new data updates k1 (shadowing)
  let p1_v2 = Pos::new(2, Flag::INFILE, 0, 200, 20);
  mem.put(k1.clone(), p1_v2).await.unwrap();
  assert_eq!(mem.get(&k1[..]), Some(p1_v2));

  // Check discard_li in now map (should be empty as we didn't overwrite within 'now' yet,
  // we shadowed 'old'. So now.put adds k1. no overwrite in `now`. discard_li empty.)
  assert!(mem.now.discards.is_empty());

  // 4. Update k1 again in 'now'
  let p1_v3 = Pos::new(3, Flag::INFILE, 0, 300, 30);
  mem.put(k1.clone(), p1_v3).await.unwrap();
  assert_eq!(mem.get(&k1[..]), Some(p1_v3));
  // Now discard_li should have p1_v2
  assert_eq!(mem.now.discards.len(), 1);
  assert_eq!(mem.now.discards[0], (k1.clone().into_boxed_slice(), p1_v2));

  // 5. Simulate removal by putting a tombstone
  let p1_tomb = p1_v3.to_tombstone();
  mem.put(k1.clone(), p1_tomb).await.unwrap();

  let val = mem.get(&k1[..]).unwrap();
  assert!(val.is_tombstone());
  assert_eq!(val.len(), p1_v3.len());

  // Check discard_li
  // We had 1 item (p1_v2).
  // The put(p1_tomb) over k1 (which was p1_v3 in 'now') adds p1_v3 to discard_li.
  let discards = &mem.now.discards;
  assert_eq!(discards.len(), 2);
  assert_eq!(discards[1], (k1.clone().into_boxed_slice(), p1_v3));

  // 6. Test range query using standard Rust range syntax
  // Use explicit type to help inference for RangeFull
  let mut iter = mem.range::<[u8]>(..);
  let (k, p) = iter.next().unwrap();
  assert_eq!(k, &k1[..]);
  assert!(p.is_tombstone());
  assert!(iter.next().is_none());

  // 7. Test range query with specific bounds
  let mut iter = mem.range(b"key0".as_ref()..b"key2".as_ref());
  let (k, _) = iter.next().unwrap();
  assert_eq!(k, &k1[..]);
  assert!(iter.next().is_none());

  // 8. Verify size tracking
  // We have one entry in 'now': key1 -> tombstone (68 bytes)
  // And 2 discarded entries in 'discard_li' (2 * 68 = 136 bytes)
  // size = 68 + 136 = 204
  assert_eq!(mem.size, 3 * (4 + Pos::SIZE + jdb_mem::Map::ENTRY_OVERHEAD));

  // 9. Test auto-rotation
  // Current size is 68. Set rotate_size to 70.
  mem.rotate_size = 70;
  // Putting key2 (len 4) + Pos::SIZE + overhead = 68. 68+68 = 136 >= 70. Should rotate.
  let k2 = b"key2".to_vec();
  let p2 = Pos::new(4, Flag::INFILE, 0, 400, 10);
  mem.put(k2.clone(), p2).await.unwrap();

  assert!(mem.freeze.is_some());
  // Should have 1 frozen map (key2), key1 is flushed
  assert_eq!(mem.size, 4 + Pos::SIZE + jdb_mem::Map::ENTRY_OVERHEAD); // New active map should contain the new key
  assert_eq!(mem.get(&k2[..]), Some(p2));

  OK
}
