use jdb_pef::Pef;

#[test]
fn test_empty_input() {
  let data: Vec<u64> = vec![];
  let pef = Pef::new(&data, 128);

  assert_eq!(pef.get(0), None);
  assert_eq!(pef.next_ge(0), None);

  let mut iter = pef.iter();
  assert_eq!(iter.seek(0), None);
  assert_eq!(iter.next(), None);
}

#[test]
fn test_single_element() {
  let data = vec![42];
  let pef = Pef::new(&data, 128);

  assert_eq!(pef.get(0), Some(42));
  assert_eq!(pef.get(1), None);
  assert_eq!(pef.next_ge(0), Some(42));
  assert_eq!(pef.next_ge(43), None);

  let mut iter = pef.iter();
  assert_eq!(iter.seek(42), Some(42));
  assert_eq!(iter.next(), Some(42));
  assert_eq!(iter.next(), None);
}

#[test]
fn test_duplicates() {
  let data = vec![10, 10, 10, 20, 20];
  let pef = Pef::new(&data, 2);

  assert_eq!(pef.get(0), Some(10));
  assert_eq!(pef.get(2), Some(10));

  // Search
  assert_eq!(pef.next_ge(10), Some(10));
  assert_eq!(pef.next_ge(11), Some(20));

  // Iter behavior with duplicates
  let mut iter = pef.iter();
  assert_eq!(iter.seek(10), Some(10));
  // The cursor should point to the *first* 10 found by standard search logic.
  assert_eq!(iter.next(), Some(10));
  assert_eq!(iter.next(), Some(10));
  assert_eq!(iter.next(), Some(10));
  assert_eq!(iter.next(), Some(20));
  assert_eq!(iter.next(), Some(20));
  assert_eq!(iter.next(), None);
}

#[test]
fn test_large_gaps() {
  let data = vec![0, 1_000_000_000_000];
  let pef = Pef::new(&data, 10);

  assert_eq!(pef.get(0), Some(0));
  assert_eq!(pef.get(1), Some(1_000_000_000_000));

  let mut iter = pef.iter();
  assert_eq!(iter.seek(1_000_000_000_000), Some(1_000_000_000_000));
}

#[test]
fn test_block_boundaries() {
  let mut data = Vec::new();
  for i in 0..100 {
    data.push(i * 2); // 0, 2, 4..., 198
  }

  // Small block size to force many chunks
  let pef = Pef::new(&data, 4); // chunks: [0..6], [8..14], etc.

  let mut iter = pef.iter();

  // Seek across boundary
  assert_eq!(iter.seek(35), Some(36)); // 36 is in chunk [32, 34, 36, 38]?
  assert_eq!(iter.next(), Some(36));

  // Seek far
  assert_eq!(iter.seek(75), Some(76));
  assert_eq!(iter.next(), Some(76));
}

#[test]
fn test_max_value_boundary() {
  let data = vec![u64::MAX - 10, u64::MAX];
  let pef = Pef::new(&data, 128);

  assert_eq!(pef.get(1), Some(u64::MAX));
  assert_eq!(pef.next_ge(u64::MAX), Some(u64::MAX));
}

#[test]
fn test_iter_multi_seek_and_skip() {
  let data: Vec<u64> = (0..200).filter(|x| x % 2 == 0).collect(); // 0, 2, 4, ... 198 (100 elements)
  let pef = Pef::new(&data, 10); // small blocks

  let mut iter = pef.iter();

  // 1. Initial Seek
  assert_eq!(iter.seek(5), Some(6)); // First >= 5 is 6

  // 2. Exact match seek
  assert_eq!(iter.seek(8), Some(8)); // Exact match

  // 3. Skip forward
  assert_eq!(iter.seek(50), Some(50));

  // 4. Large skip
  assert_eq!(iter.seek(190), Some(190));

  // 5. Next after seek
  assert_eq!(iter.next(), Some(190));
  assert_eq!(iter.next(), Some(192));

  // 6. Seek past end
  assert_eq!(iter.seek(200), None);
}

#[test]
fn test_dense_and_sparse_mix() {
  let mut data = Vec::new();
  // Dense [0..100]
  for i in 0..100 {
    data.push(i);
  }
  // Sparse [1000, 2000, ...]
  for i in 1..=50 {
    data.push(i * 1000);
  }

  let pef = Pef::new(&data, 16);

  // Verify random access
  for (i, &v) in data.iter().enumerate() {
    assert_eq!(pef.get(i), Some(v));
  }

  // Verify seek
  let mut iter = pef.iter();
  assert_eq!(iter.seek(50), Some(50));
  assert_eq!(iter.seek(500), Some(1000));
  assert_eq!(iter.seek(1050), Some(2000));
}
