use jdb_pef::Pef;

#[test]
fn test_pef_random_access() {
  // Generate 1000 incremental numbers
  let mut data = Vec::new();
  let mut val = 0;
  for i in 0..1000 {
    val += if i % 100 == 0 { 1000 } else { 2 };
    data.push(val);
  }

  // Block size 128
  let pef = Pef::new(&data, 128);

  // Verify Get
  for (i, &v) in data.iter().enumerate() {
    assert_eq!(pef.get(i), Some(v), "Failed at index {}", i);
  }
}

#[test]
fn test_pef_search() {
  let data = vec![
    10, 12, 14, // Chunk 0 (Max 14)
    100, 102, 105, // Chunk 1 (Max 105)
    500, 501, 502, // Chunk 2 (Max 502)
  ];

  let pef = Pef::new(&data, 3);

  assert_eq!(pef.next_ge(11), Some(12));
  assert_eq!(pef.next_ge(14), Some(14));
  assert_eq!(pef.next_ge(15), Some(100)); // Cross-chunk
  assert_eq!(pef.next_ge(105), Some(105));
  assert_eq!(pef.next_ge(200), Some(500)); // Cross-chunk
  assert_eq!(pef.next_ge(600), None);
}

#[test]
fn test_into_iter() {
  let data = vec![10, 20, 30];
  let pef = Pef::new(&data, 2);

  let mut iter = (&pef).into_iter();
  assert_eq!(iter.next(), Some(10));
  assert_eq!(iter.next(), Some(20));
  assert_eq!(iter.next(), Some(30));
  assert_eq!(iter.next(), None);

  // Loop syntax
  let mut count = 0;
  for _ in &pef {
    count += 1;
  }
  assert_eq!(count, 3);
}
