use aok::{OK, Void};
use jdb_base::{Flag, Mem as _, Pos};
use jdb_mem::Mem;

#[test]
fn test_mem_ops() -> Void {
  let mut mem = Mem::new();

  // 1. Put initial data
  let k1 = b"key1".to_vec();
  let p1 = Pos::new(1, Flag::INFILE, 0, 100, 10);
  mem.put(k1.clone(), p1);

  assert_eq!(mem.get(&k1[..]), Some(p1));

  // 2. Rotate
  mem.rotate();
  assert_eq!(mem.old.len(), 1);
  // Should still find k1 in old map
  assert_eq!(mem.get(&k1[..]), Some(p1));

  // 3. Put new data updates k1 (shadowing)
  let p1_v2 = Pos::new(2, Flag::INFILE, 0, 200, 20);
  mem.put(k1.clone(), p1_v2);
  assert_eq!(mem.get(&k1[..]), Some(p1_v2));

  // Check discard_li in now map (should be empty as we didn't overwrite within 'now' yet,
  // we shadowed 'old'. So now.put adds k1. no overwrite in `now`. discard_li empty.)
  assert!(mem.now.discard_li.is_empty());

  // 4. Update k1 again in 'now'
  let p1_v3 = Pos::new(3, Flag::INFILE, 0, 300, 30);
  mem.put(k1.clone(), p1_v3);
  assert_eq!(mem.get(&k1[..]), Some(p1_v3));
  // Now discard_li should have p1_v2
  assert_eq!(mem.now.discard_li.len(), 1);
  assert_eq!(
    mem.now.discard_li[0],
    (k1.clone().into_boxed_slice(), p1_v2)
  );

  // 5. Simulate removal by putting a tombstone
  let p1_tomb = p1_v3.to_tombstone();
  mem.put(k1.clone(), p1_tomb);

  let val = mem.get(&k1[..]).unwrap();
  assert!(val.is_tombstone());
  assert_eq!(val.len(), p1_v3.len());

  // Check discard_li
  // We had 1 item (p1_v2).
  // The put(p1_tomb) over k1 (which was p1_v3 in 'now') adds p1_v3 to discard_li.
  let discards = &mem.now.discard_li;
  assert_eq!(discards.len(), 2);
  assert_eq!(discards[1], (k1.clone().into_boxed_slice(), p1_v3));

  OK
}
