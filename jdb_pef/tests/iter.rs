use jdb_pef::Pef;

#[test]
fn test_iter_seek() {
  let data = vec![1, 10, 20, 30, 40, 50, 60, 70];
  let pef = Pef::new(&data, 2);
  // Chunks: [1, 10], [20, 30], [40, 50], [60, 70]

  let mut iter = pef.iter();

  // Seek 5 -> find 10
  assert_eq!(iter.seek(5), Some(10));

  // Next should be 10
  assert_eq!(iter.next(), Some(10));
  assert_eq!(iter.next(), Some(20));

  // Seek 25 -> find 30
  assert_eq!(iter.seek(25), Some(30));
  assert_eq!(iter.next(), Some(30));

  // Seek 40 -> find 40
  assert_eq!(iter.seek(40), Some(40));
}
