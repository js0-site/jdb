mod consts;

use std::ops::Bound;

use aok::{OK, Void};
use jdb_base::Pos;
use jdb_mem::{Mems, Order, Table, TableMut};

use consts::*;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_mems_basic() -> Void {
  let mut mems = Mems::new();
  assert!(!mems.has_frozen());
  assert_eq!(mems.frozen_count(), 0);
  assert_eq!(mems.active_size(), 0);

  // put/get/rm
  let p = pos(100);
  mems.put(b"hello".as_slice(), p);
  assert_eq!(mems.get(b"hello").unwrap(), p);
  assert!(mems.get(b"world").is_none());

  mems.rm(b"hello".as_slice());
  assert!(mems.get(b"hello").unwrap().is_tombstone());
  OK
}

#[test]
fn test_mems_iter() -> Void {
  let mut mems = Mems::new();
  build(&mut mems, &[(K_C, Some(300)), (K_A, Some(100)), (K_B, Some(200))]);

  assert_eq!(keys(&mems.iter().collect::<Vec<_>>()), vec![K_A, K_B, K_C]);
  assert_eq!(keys(&mems.rev_iter().collect::<Vec<_>>()), vec![K_C, K_B, K_A]);
  OK
}

#[test]
fn test_mems_range() -> Void {
  let mut mems = Mems::new();
  for (i, &k) in [K_A, K_B, K_C, K_D].iter().enumerate() {
    mems.put(k, pos((i as u64 + 1) * 100));
  }

  let fwd: Vec<_> = mems.range(Bound::Included(K_B), Bound::Excluded(K_D), Order::Asc).collect();
  let rev: Vec<_> = mems.range(Bound::Included(K_B), Bound::Excluded(K_D), Order::Desc).collect();

  assert_eq!(keys(&fwd), vec![K_B, K_C]);
  assert_eq!(keys(&rev), vec![K_C, K_B]);
  OK
}

#[test]
fn test_mems_freeze() -> Void {
  let mut mems = Mems::new();

  mems.put(b"key1".as_slice(), pos(100));
  let handle = mems.freeze();

  assert!(mems.has_frozen());
  assert_eq!(mems.frozen_count(), 1);
  assert!(handle.mem.get(b"key1").is_some());
  assert_eq!(mems.active_size(), 0);
  assert!(mems.get(b"key1").is_some());
  OK
}

#[test]
fn test_mems_multi_freeze() -> Void {
  let mut mems = Mems::new();
  let mut handles = vec![];

  let ops = [(K_A, 100u64), (K_B, 200), (K_C, 300)];
  for (i, &(k, off)) in ops.iter().enumerate() {
    mems.put(k, pos(off));
    if i < 2 { handles.push(mems.freeze()); }
  }

  assert_eq!(mems.frozen_count(), 2);
  for &(k, _) in &ops {
    assert!(mems.get(k).is_some());
  }
  assert_eq!(mems.iter().count(), 3);
  drop(handles);
  OK
}

#[test]
fn test_mems_newest_wins() -> Void {
  let mut mems = Mems::new();

  mems.put(b"key".as_slice(), pos(100));
  mems.freeze();
  mems.put(b"key".as_slice(), pos(200));

  assert_eq!(mems.get(b"key").unwrap().offset(), 200);
  let items: Vec<_> = mems.iter().collect();
  assert_eq!(items.len(), 1);
  assert_eq!(items[0].1.offset(), 200);
  OK
}


#[test]
fn test_null_byte_keys() -> Void {
  let mut mems = Mems::new();

  let null_keys: [&[u8]; 5] = [
    &[0u8], &[0u8, 0u8], &[0u8, 0u8, 0u8], &[0u8, 1u8], &[0u8, 0u8, 1u8]
  ];

  for (i, k) in null_keys.iter().enumerate() {
    mems.put(k.to_vec(), pos((i as u64 + 1) * 10));
  }

  for k in &null_keys {
    assert!(mems.get(*k).is_some(), "{k:?} should exist");
  }

  let items: Vec<Vec<u8>> = mems.iter().map(|(k, _)| k.to_vec()).collect();
  let mut sorted = items.clone();
  sorted.sort();
  assert_eq!(items, sorted);
  OK
}

#[test]
fn test_mems_prefix() -> Void {
  let mut mems = Mems::new();

  let data = [
    (b"user:1".as_slice(), 1), (b"user:2".as_slice(), 2),
    (b"user:10".as_slice(), 3), (b"item:1".as_slice(), 4)
  ];
  for &(k, off) in &data {
    mems.put(k, pos(off));
  }

  assert_eq!(mems.prefix(b"user:", Order::Asc).count(), 3);
  assert_eq!(mems.prefix(b"item:", Order::Asc).count(), 1);
  OK
}

#[test]
fn test_mems_size() -> Void {
  let mut mems = Mems::new();
  assert_eq!(mems.active_size(), 0);

  let unit = 4 + Pos::SIZE as u64;
  mems.put(b"key1".as_slice(), pos(100));
  assert_eq!(mems.active_size(), unit);

  mems.put(b"key1".as_slice(), pos(200)); // replace
  assert_eq!(mems.active_size(), unit);

  mems.put(b"key2".as_slice(), pos(300));
  assert_eq!(mems.active_size(), unit * 2);
  OK
}

#[test]
fn test_frozen_cleanup() -> Void {
  let mut mems = Mems::new();

  mems.put(b"key1".as_slice(), pos(100));
  let h1 = mems.freeze();
  let id1 = h1.mem.id();

  mems.put(b"key2".as_slice(), pos(200));
  let h2 = mems.freeze();

  assert_eq!(mems.frozen_count(), 2);

  drop(h1);
  assert_eq!(mems.frozen_count(), 1);
  assert!(mems.get_frozen(id1).is_none());

  drop(h2);
  assert_eq!(mems.frozen_count(), 0);
  OK
}

#[test]
fn test_tombstone_lifecycle() -> Void {
  let mut mems = Mems::new();

  // put -> del -> put -> del (no resurrection)
  mems.put(K_X, pos(100));
  mems.freeze();
  mems.rm(K_X);
  mems.freeze();
  mems.put(K_X, pos(300));
  mems.freeze();
  mems.rm(K_X);

  let fwd: Vec<_> = mems.iter().collect();
  let rev: Vec<_> = mems.rev_iter().collect();

  assert_eq!(fwd.len(), 1);
  assert!(fwd[0].1.is_tombstone());
  assert_eq!(rev.len(), 1);
  assert!(rev[0].1.is_tombstone());

  // put -> del -> put (resurrection)
  let mut mems2 = Mems::new();
  mems2.put(K_Y, pos(100));
  mems2.freeze();
  mems2.rm(K_Y);
  mems2.freeze();
  mems2.put(K_Y, pos(300));

  let fwd: Vec<_> = mems2.iter().collect();
  assert_eq!(fwd.len(), 1);
  assert!(!fwd[0].1.is_tombstone());
  assert_eq!(fwd[0].1.offset(), 300);
  OK
}


#[test]
fn test_multi_mem_overlap() -> Void {
  let mut mems = Mems::new();

  // 4 mems with overlapping keys
  // 4 个 mem，key 有交叉
  let batches: [&[Op]; 4] = [
    &[(K_A, Some(10)), (K_B, Some(20)), (K_C, Some(30)), (K_D, Some(40))],
    &[(K_B, Some(200)), (K_C, Some(300)), (K_E, Some(50)), (K_F, Some(60))],
    &[(K_A, Some(100)), (K_D, Some(400)), (K_E, Some(500)), (K_G, Some(70))],
    &[(K_C, Some(3000)), (K_F, Some(600)), (K_G, Some(700))],
  ];

  let _handles = build_batches(&mut mems, &batches);

  let fwd: Vec<_> = mems.iter().collect();
  let rev: Vec<_> = mems.rev_iter().collect();

  assert_eq!(keys(&fwd), KEYS_AG.to_vec());
  assert_eq!(keys(&rev), KEYS_GA.to_vec());

  // Newest wins
  // 最新值胜出
  let expects: [Expect; 7] = [
    (K_A, false, 100), (K_B, false, 200), (K_C, false, 3000),
    (K_D, false, 400), (K_E, false, 500), (K_F, false, 600), (K_G, false, 700),
  ];
  check(&fwd, &expects);
  check(&rev, &expects);
  OK
}

#[test]
fn test_multi_mem_tombstone_overlap() -> Void {
  let mut mems = Mems::new();

  // 4 mems with tombstones
  // 4 个 mem，有墓碑
  let batches: [&[Op]; 4] = [
    &[(K_A, Some(10)), (K_B, Some(20)), (K_C, Some(30)), (K_D, Some(40))],
    &[(K_A, None), (K_B, Some(200)), (K_C, None), (K_E, Some(50))],
    &[(K_A, Some(100)), (K_B, None), (K_C, Some(300)), (K_F, Some(60))],
    &[(K_A, None), (K_B, Some(2000)), (K_D, None), (K_G, Some(70))],
  ];

  let _handles = build_batches(&mut mems, &batches);

  let fwd: Vec<_> = mems.iter().collect();
  let rev: Vec<_> = mems.rev_iter().collect();

  assert_eq!(keys(&fwd), KEYS_AG.to_vec());
  assert_eq!(keys(&rev), KEYS_GA.to_vec());

  let expects: [Expect; 7] = [
    (K_A, true, 0), (K_B, false, 2000), (K_C, false, 300),
    (K_D, true, 0), (K_E, false, 50), (K_F, false, 60), (K_G, false, 70),
  ];
  check(&fwd, &expects);
  check(&rev, &expects);

  assert_eq!(live_keys(&fwd), vec![K_B, K_C, K_E, K_F, K_G]);
  assert_eq!(live_keys(&rev), vec![K_G, K_F, K_E, K_C, K_B]);
  OK
}

#[test]
fn test_five_mems_complex() -> Void {
  let mut mems = Mems::new();

  // 5 mems with complex operations
  // 5 个 mem，复杂操作
  let batches: [&[Op]; 5] = [
    &[(K_A, Some(10)), (K_C, Some(30)), (K_E, Some(50)), (K_G, Some(70))],
    &[(K_A, Some(100)), (K_B, Some(20)), (K_D, Some(40)), (K_F, Some(60))],
    &[(K_A, None), (K_B, None), (K_C, None), (K_D, Some(400)), (K_E, Some(500))],
    &[(K_A, Some(1000)), (K_B, Some(2000)), (K_F, None), (K_G, None)],
    &[(K_A, None), (K_C, Some(3000)), (K_F, Some(6000)), (K_G, Some(7000))],
  ];

  let _handles = build_batches(&mut mems, &batches);

  let fwd: Vec<_> = mems.iter().collect();
  let rev: Vec<_> = mems.rev_iter().collect();

  assert_eq!(keys(&fwd), KEYS_AG.to_vec());
  assert_eq!(keys(&rev), KEYS_GA.to_vec());

  let expects: [Expect; 7] = [
    (K_A, true, 0), (K_B, false, 2000), (K_C, false, 3000),
    (K_D, false, 400), (K_E, false, 500), (K_F, false, 6000), (K_G, false, 7000),
  ];
  check(&fwd, &expects);

  // Consistency
  // 一致性
  let fwd_rev: Vec<_> = fwd.iter().rev().cloned().collect();
  assert_eq!(fwd_rev, rev);
  OK
}

#[test]
fn test_range_multi_mem() -> Void {
  let mut mems = Mems::new();

  let batches: [&[Op]; 3] = [
    &[(K_A, Some(10)), (K_B, Some(20)), (K_C, Some(30)), (K_D, Some(40))],
    &[(K_C, Some(300)), (K_D, Some(400)), (K_E, Some(50)), (K_F, Some(60))],
    &[(K_B, None), (K_D, None), (K_E, Some(500)), (K_G, Some(70))],
  ];

  let _handles = build_batches(&mut mems, &batches);

  // Range [b, f)
  let fwd: Vec<_> = mems.range(Bound::Included(K_B), Bound::Excluded(K_F), Order::Asc).collect();
  let rev: Vec<_> = mems.range(Bound::Included(K_B), Bound::Excluded(K_F), Order::Desc).collect();

  assert_eq!(keys(&fwd), vec![K_B, K_C, K_D, K_E]);
  assert_eq!(keys(&rev), vec![K_E, K_D, K_C, K_B]);

  let expects: [Expect; 4] = [
    (K_B, true, 0), (K_C, false, 300), (K_D, true, 0), (K_E, false, 500),
  ];
  check(&fwd, &expects);
  check(&rev, &expects);
  OK
}

#[test]
fn test_same_key_many_mems() -> Void {
  let mut mems = Mems::new();

  // Key a toggled across 6 mems
  // 键 a 在 6 个 mem 中反复切换
  let batches: [&[Op]; 6] = [
    &[(K_A, Some(10)), (K_B, Some(20)), (K_C, Some(30))],
    &[(K_A, None), (K_D, Some(40)), (K_E, Some(50))],
    &[(K_A, Some(100)), (K_F, Some(60)), (K_G, Some(70))],
    &[(K_A, None), (K_B, Some(200)), (K_C, Some(300))],
    &[(K_A, Some(1000)), (K_D, Some(400)), (K_E, Some(500))],
    &[(K_A, None), (K_F, Some(600)), (K_G, Some(700))],
  ];

  let _handles = build_batches(&mut mems, &batches);

  let fwd: Vec<_> = mems.iter().collect();
  let rev: Vec<_> = mems.rev_iter().collect();

  assert_eq!(keys(&fwd), KEYS_AG.to_vec());
  assert_eq!(keys(&rev), KEYS_GA.to_vec());

  // Final: a=tomb, others updated
  let expects: [Expect; 7] = [
    (K_A, true, 0), (K_B, false, 200), (K_C, false, 300),
    (K_D, false, 400), (K_E, false, 500), (K_F, false, 600), (K_G, false, 700),
  ];
  check(&fwd, &expects);
  check(&rev, &expects);

  // Same data both directions
  // 两个方向数据相同
  let fwd_set: std::collections::HashSet<_> = fwd.iter().map(|(k, p)| (k.clone(), p.offset())).collect();
  let rev_set: std::collections::HashSet<_> = rev.iter().map(|(k, p)| (k.clone(), p.offset())).collect();
  assert_eq!(fwd_set, rev_set);
  OK
}
