mod consts;

use std::ops::Bound;

use consts::{
  Expect, K_A, K_B, K_C, K_D, K_E, K_F, K_G, K_X, KEYS_AG, KEYS_GA, Op, build, build_batches,
  check, keys, live_keys, new_mems, pos,
};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[compio::test]
async fn test_mems_basic() {
  let mut mems = new_mems();

  let p = pos(100);
  mems.put(b"hello".as_slice(), p);
  assert_eq!(mems.get(b"hello").unwrap(), p);
  assert!(mems.get(b"world").is_none());

  mems.rm(b"hello".as_slice(), p);
  assert!(mems.get(b"hello").unwrap().is_tombstone());
}

#[compio::test]
async fn test_mems_iter() {
  let mut mems = new_mems();
  build(
    &mut mems,
    &[(K_C, Some(300)), (K_A, Some(100)), (K_B, Some(200))],
  );

  assert_eq!(keys(&mems.iter().collect::<Vec<_>>()), vec![K_A, K_B, K_C]);
  assert_eq!(
    keys(&mems.rev_iter().collect::<Vec<_>>()),
    vec![K_C, K_B, K_A]
  );
}

#[compio::test]
async fn test_mems_range() {
  let mut mems = new_mems();
  for (i, &k) in [K_A, K_B, K_C, K_D].iter().enumerate() {
    mems.put(k, pos((i as u64 + 1) * 100));
  }

  let fwd: Vec<_> = mems
    .range(Bound::Included(K_B), Bound::Excluded(K_D))
    .collect();
  let rev: Vec<_> = mems
    .rev_range(Bound::Excluded(K_D), Bound::Included(K_B))
    .collect();

  assert_eq!(keys(&fwd), vec![K_B, K_C]);
  assert_eq!(keys(&rev), vec![K_C, K_B]);
}

#[compio::test]
async fn test_mems_newest_wins() {
  let mut mems = new_mems();

  mems.put(b"key".as_slice(), pos(100));
  mems.put(b"key".as_slice(), pos(200));

  assert_eq!(mems.get(b"key").unwrap().offset(), 200);
  let items: Vec<_> = mems.iter().collect();
  assert_eq!(items.len(), 1);
  assert_eq!(items[0].1.offset(), 200);
}

#[compio::test]
async fn test_null_byte_keys() {
  let mut mems = new_mems();

  let null_keys: [&[u8]; 5] = [
    &[0u8],
    &[0u8, 0u8],
    &[0u8, 0u8, 0u8],
    &[0u8, 1u8],
    &[0u8, 0u8, 1u8],
  ];

  for (i, k) in null_keys.iter().enumerate() {
    mems.put(k.to_vec(), pos((i as u64 + 1) * 10));
  }

  for k in &null_keys {
    assert!(mems.get(k).is_some(), "{k:?} should exist");
  }

  let items: Vec<Vec<u8>> = mems.iter().map(|(k, _)| k.to_vec()).collect();
  let mut sorted = items.clone();
  sorted.sort();
  assert_eq!(items, sorted);
}

#[compio::test]
async fn test_mems_prefix() {
  use jdb_base::prefix_end;

  let mut mems = new_mems();

  let data = [
    (b"user:1".as_slice(), 1),
    (b"user:2".as_slice(), 2),
    (b"user:10".as_slice(), 3),
    (b"item:1".as_slice(), 4),
  ];
  for &(k, off) in &data {
    mems.put(k, pos(off));
  }

  // Test prefix via range
  // 通过 range 测试前缀
  let prefix = b"user:";
  let start = Bound::Included(prefix.as_slice());
  let end = prefix_end(prefix);
  let end_bound = match &end {
    Some(e) => Bound::Excluded(e.as_ref()),
    None => Bound::Unbounded,
  };
  assert_eq!(mems.range(start, end_bound).count(), 3);

  let prefix = b"item:";
  let start = Bound::Included(prefix.as_slice());
  let end = prefix_end(prefix);
  let end_bound = match &end {
    Some(e) => Bound::Excluded(e.as_ref()),
    None => Bound::Unbounded,
  };
  assert_eq!(mems.range(start, end_bound).count(), 1);
}

#[compio::test]
async fn test_tombstone() {
  let mut mems = new_mems();

  let p = pos(100);
  mems.put(K_X, p);
  mems.rm(K_X, p);

  let fwd: Vec<_> = mems.iter().collect();
  assert_eq!(fwd.len(), 1);
  assert!(fwd[0].1.is_tombstone());

  // Overwrite tombstone
  // 覆盖墓碑
  mems.put(K_X, pos(300));
  let fwd: Vec<_> = mems.iter().collect();
  assert_eq!(fwd.len(), 1);
  assert!(!fwd[0].1.is_tombstone());
  assert_eq!(fwd[0].1.offset(), 300);
}

#[compio::test]
async fn test_multi_ops_overlap() {
  let mut mems = new_mems();

  let batches: [&[Op]; 4] = [
    &[
      (K_A, Some(10)),
      (K_B, Some(20)),
      (K_C, Some(30)),
      (K_D, Some(40)),
    ],
    &[
      (K_B, Some(200)),
      (K_C, Some(300)),
      (K_E, Some(50)),
      (K_F, Some(60)),
    ],
    &[
      (K_A, Some(100)),
      (K_D, Some(400)),
      (K_E, Some(500)),
      (K_G, Some(70)),
    ],
    &[(K_C, Some(3000)), (K_F, Some(600)), (K_G, Some(700))],
  ];

  build_batches(&mut mems, &batches);

  let fwd: Vec<_> = mems.iter().collect();
  let rev: Vec<_> = mems.rev_iter().collect();

  assert_eq!(keys(&fwd), KEYS_AG.to_vec());
  assert_eq!(keys(&rev), KEYS_GA.to_vec());

  let expects: [Expect; 7] = [
    (K_A, false, 100),
    (K_B, false, 200),
    (K_C, false, 3000),
    (K_D, false, 400),
    (K_E, false, 500),
    (K_F, false, 600),
    (K_G, false, 700),
  ];
  check(&fwd, &expects);
  check(&rev, &expects);
}

#[compio::test]
async fn test_multi_ops_tombstone_overlap() {
  let mut mems = new_mems();

  let batches: [&[Op]; 4] = [
    &[
      (K_A, Some(10)),
      (K_B, Some(20)),
      (K_C, Some(30)),
      (K_D, Some(40)),
    ],
    &[(K_A, None), (K_B, Some(200)), (K_C, None), (K_E, Some(50))],
    &[
      (K_A, Some(100)),
      (K_B, None),
      (K_C, Some(300)),
      (K_F, Some(60)),
    ],
    &[(K_A, None), (K_B, Some(2000)), (K_D, None), (K_G, Some(70))],
  ];

  build_batches(&mut mems, &batches);

  let fwd: Vec<_> = mems.iter().collect();
  let rev: Vec<_> = mems.rev_iter().collect();

  assert_eq!(keys(&fwd), KEYS_AG.to_vec());
  assert_eq!(keys(&rev), KEYS_GA.to_vec());

  let expects: [Expect; 7] = [
    (K_A, true, 0),
    (K_B, false, 2000),
    (K_C, false, 300),
    (K_D, true, 0),
    (K_E, false, 50),
    (K_F, false, 60),
    (K_G, false, 70),
  ];
  check(&fwd, &expects);
  check(&rev, &expects);

  assert_eq!(live_keys(&fwd), vec![K_B, K_C, K_E, K_F, K_G]);
  assert_eq!(live_keys(&rev), vec![K_G, K_F, K_E, K_C, K_B]);
}

#[compio::test]
async fn test_five_batches_complex() {
  let mut mems = new_mems();

  let batches: [&[Op]; 5] = [
    &[
      (K_A, Some(10)),
      (K_C, Some(30)),
      (K_E, Some(50)),
      (K_G, Some(70)),
    ],
    &[
      (K_A, Some(100)),
      (K_B, Some(20)),
      (K_D, Some(40)),
      (K_F, Some(60)),
    ],
    &[
      (K_A, None),
      (K_B, None),
      (K_C, None),
      (K_D, Some(400)),
      (K_E, Some(500)),
    ],
    &[
      (K_A, Some(1000)),
      (K_B, Some(2000)),
      (K_F, None),
      (K_G, None),
    ],
    &[
      (K_A, None),
      (K_C, Some(3000)),
      (K_F, Some(6000)),
      (K_G, Some(7000)),
    ],
  ];

  build_batches(&mut mems, &batches);

  let fwd: Vec<_> = mems.iter().collect();
  let rev: Vec<_> = mems.rev_iter().collect();

  assert_eq!(keys(&fwd), KEYS_AG.to_vec());
  assert_eq!(keys(&rev), KEYS_GA.to_vec());

  let expects: [Expect; 7] = [
    (K_A, true, 0),
    (K_B, false, 2000),
    (K_C, false, 3000),
    (K_D, false, 400),
    (K_E, false, 500),
    (K_F, false, 6000),
    (K_G, false, 7000),
  ];
  check(&fwd, &expects);

  let fwd_rev: Vec<_> = fwd.iter().rev().cloned().collect();
  assert_eq!(fwd_rev, rev);
}

#[compio::test]
async fn test_range_ops() {
  let mut mems = new_mems();

  let batches: [&[Op]; 3] = [
    &[
      (K_A, Some(10)),
      (K_B, Some(20)),
      (K_C, Some(30)),
      (K_D, Some(40)),
    ],
    &[
      (K_C, Some(300)),
      (K_D, Some(400)),
      (K_E, Some(50)),
      (K_F, Some(60)),
    ],
    &[(K_B, None), (K_D, None), (K_E, Some(500)), (K_G, Some(70))],
  ];

  build_batches(&mut mems, &batches);

  let fwd: Vec<_> = mems
    .range(Bound::Included(K_B), Bound::Excluded(K_F))
    .collect();
  let rev: Vec<_> = mems
    .rev_range(Bound::Excluded(K_F), Bound::Included(K_B))
    .collect();

  assert_eq!(keys(&fwd), vec![K_B, K_C, K_D, K_E]);
  assert_eq!(keys(&rev), vec![K_E, K_D, K_C, K_B]);

  let expects: [Expect; 4] = [
    (K_B, true, 0),
    (K_C, false, 300),
    (K_D, true, 0),
    (K_E, false, 500),
  ];
  check(&fwd, &expects);
  check(&rev, &expects);
}

#[compio::test]
async fn test_same_key_many_ops() {
  let mut mems = new_mems();

  let batches: [&[Op]; 6] = [
    &[(K_A, Some(10)), (K_B, Some(20)), (K_C, Some(30))],
    &[(K_A, None), (K_D, Some(40)), (K_E, Some(50))],
    &[(K_A, Some(100)), (K_F, Some(60)), (K_G, Some(70))],
    &[(K_A, None), (K_B, Some(200)), (K_C, Some(300))],
    &[(K_A, Some(1000)), (K_D, Some(400)), (K_E, Some(500))],
    &[(K_A, None), (K_F, Some(600)), (K_G, Some(700))],
  ];

  build_batches(&mut mems, &batches);

  let fwd: Vec<_> = mems.iter().collect();
  let rev: Vec<_> = mems.rev_iter().collect();

  assert_eq!(keys(&fwd), KEYS_AG.to_vec());
  assert_eq!(keys(&rev), KEYS_GA.to_vec());

  let expects: [Expect; 7] = [
    (K_A, true, 0),
    (K_B, false, 200),
    (K_C, false, 300),
    (K_D, false, 400),
    (K_E, false, 500),
    (K_F, false, 600),
    (K_G, false, 700),
  ];
  check(&fwd, &expects);
  check(&rev, &expects);

  let fwd_set: std::collections::HashSet<_> =
    fwd.iter().map(|(k, p)| (k.clone(), p.offset())).collect();
  let rev_set: std::collections::HashSet<_> =
    rev.iter().map(|(k, p)| (k.clone(), p.offset())).collect();
  assert_eq!(fwd_set, rev_set);
}

#[compio::test]
async fn test_flush_clears_data() {
  let mut mems = new_mems();

  // Put data
  // 写入数据
  mems.put(K_A, pos(100));
  mems.put(K_B, pos(200));
  assert!(mems.get(K_A).is_some());
  assert!(mems.get(K_B).is_some());

  // Flush to disk
  // 刷盘
  mems.flush().await;

  // Data should be gone (flushed to SST)
  // 数据应该没了（已刷到 SST）
  assert!(mems.get(K_A).is_none());
  assert!(mems.get(K_B).is_none());
  assert_eq!(mems.iter().count(), 0);
}

#[compio::test]
async fn test_flush_then_write() {
  let mut mems = new_mems();

  // First batch
  // 第一批
  mems.put(K_A, pos(100));
  mems.flush().await;

  // Old data gone
  // 旧数据没了
  assert!(mems.get(K_A).is_none());

  // New data works
  // 新数据正常
  mems.put(K_B, pos(200));
  assert!(mems.get(K_B).is_some());
  assert_eq!(mems.get(K_B).unwrap().offset(), 200);
}

#[compio::test]
async fn test_multiple_flush() {
  let mut mems = new_mems();

  // Batch 1
  mems.put(K_A, pos(100));
  mems.flush().await;
  assert!(mems.get(K_A).is_none());

  // Batch 2
  mems.put(K_B, pos(200));
  mems.flush().await;
  assert!(mems.get(K_B).is_none());

  // Batch 3
  mems.put(K_C, pos(300));
  assert!(mems.get(K_C).is_some());
  assert_eq!(mems.iter().count(), 1);
}
