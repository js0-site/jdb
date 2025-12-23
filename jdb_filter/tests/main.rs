use jdb_filter::Filter;

#[test]
fn test_filter_basic() {
  let keys: Vec<u64> = (0..1000).collect();
  let filter = Filter::new(&keys).unwrap();

  // 已存在的 key 必须返回 true
  // Existing keys must return true
  for k in &keys {
    assert!(filter.may_contain(*k));
  }

  // 不存在的 key 大概率返回 false (允许少量误判)
  // Non-existing keys should mostly return false (some false positives allowed)
  let mut false_positives = 0;
  for k in 10000..11000 {
    if filter.may_contain(k) {
      false_positives += 1;
    }
  }
  // 误判率应 < 1%
  assert!(false_positives < 10, "too many false positives: {false_positives}");
}

#[test]
fn test_filter_empty() {
  let keys: Vec<u64> = vec![];
  // 空列表可以构建，但查询任何 key 都应返回 false
  // Empty list can be built, but any key query should return false
  if let Some(filter) = Filter::new(&keys) {
    assert!(!filter.may_contain(0));
    assert!(!filter.may_contain(42));
  }
}

#[test]
fn test_filter_single() {
  let keys = vec![42u64];
  let filter = Filter::new(&keys).unwrap();
  assert!(filter.may_contain(42));
}

#[test]
fn test_filter_size() {
  let keys: Vec<u64> = (0..10000).collect();
  let filter = Filter::new(&keys).unwrap();
  // 每 key 约 1.125 bytes，10000 keys ≈ 11250 bytes
  let size = filter.size();
  assert!(size > 10000 && size < 15000, "unexpected size: {size}");
}
