use jdb_trait::ValRef;

#[test]
fn valref_tombstone() {
  let v = ValRef {
    file_id: 1,
    offset: 100,
    prev_file_id: 0,
    prev_offset: 0,
  };
  assert!(!v.is_tombstone());
  assert_eq!(v.real_offset(), 100);

  let tomb = ValRef {
    file_id: 1,
    offset: 100 | (1 << 63),
    prev_file_id: 0,
    prev_offset: 0,
  };
  assert!(tomb.is_tombstone());
  assert_eq!(tomb.real_offset(), 100);
}

#[test]
fn valref_prev() {
  let v1 = ValRef {
    file_id: 1,
    offset: 100,
    prev_file_id: 0,
    prev_offset: 0,
  };
  assert!(!v1.has_prev());

  let v2 = ValRef {
    file_id: 1,
    offset: 200,
    prev_file_id: 1,
    prev_offset: 100,
  };
  assert!(v2.has_prev());
}
