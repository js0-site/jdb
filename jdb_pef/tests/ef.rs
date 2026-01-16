use jdb_pef::Ef;

#[test]
fn test_ef_correctness() {
  let data = vec![2, 3, 5, 7, 11, 13, 17, 19, 23, 29];
  let ef = Ef::new(&data);

  for (i, &v) in data.iter().enumerate() {
    assert_eq!(ef.get(i), Some(v), "Index {} mismatch", i);
  }

  assert_eq!(ef.next_ge(6), Some((3, 7))); // index 3 is val 7
  assert_eq!(ef.next_ge(7), Some((3, 7)));
  assert_eq!(ef.next_ge(12), Some((5, 13)));
}
