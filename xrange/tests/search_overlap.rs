use xrange::overlap_for_sorted;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_overlap_for_sorted_basic() {
  // Test with i32 ranges
  let ranges: Vec<std::ops::RangeInclusive<i32>> = vec![1..=5, 6..=10, 11..=15, 16..=20];

  // Query that overlaps with middle ranges
  let result: Vec<_> = overlap_for_sorted(8..=12, &ranges).collect();
  assert_eq!(result.len(), 2); // overlaps with 6..=10 and 11..=15
}

#[test]
fn test_overlap_for_sorted_no_overlap() {
  let ranges: Vec<std::ops::RangeInclusive<i32>> = vec![1..=5, 6..=10, 11..=15];

  // Query that doesn't overlap with any
  let result: Vec<_> = overlap_for_sorted(20..=25, &ranges).collect();
  assert_eq!(result.len(), 0);
}

#[test]
fn test_overlap_for_sorted_all_overlap() {
  let ranges: Vec<std::ops::RangeInclusive<i32>> = vec![1..=5, 6..=10, 11..=15];

  // Query that overlaps with all
  let result: Vec<_> = overlap_for_sorted(0..=20, &ranges).collect();
  assert_eq!(result.len(), 3);
}

#[test]
fn test_overlap_for_sorted_boundary() {
  let ranges: Vec<std::ops::RangeInclusive<i32>> = vec![1..=5, 6..=10, 11..=15];

  // Query at boundary
  let result: Vec<_> = overlap_for_sorted(5..=6, &ranges).collect();
  assert_eq!(result.len(), 2); // overlaps with both 1..=5 and 6..=10
}

#[test]
fn test_overlap_for_sorted_empty_slice() {
  let ranges: Vec<std::ops::RangeInclusive<i32>> = vec![];

  let result: Vec<_> = overlap_for_sorted(1..=10, &ranges).collect();
  assert_eq!(result.len(), 0);
}

#[test]
fn test_overlap_for_sorted_single_item() {
  let ranges: Vec<std::ops::RangeInclusive<i32>> = vec![5..=10];

  // Overlapping
  let result: Vec<_> = overlap_for_sorted(8..=12, &ranges).collect();
  assert_eq!(result.len(), 1);

  // Not overlapping
  let result: Vec<_> = overlap_for_sorted(15..=20, &ranges).collect();
  assert_eq!(result.len(), 0);
}

#[test]
fn test_overlap_for_sorted_with_exclusive_bounds() {
  let ranges: Vec<std::ops::Range<i32>> = vec![1..5, 6..10, 11..15];

  // Query with exclusive bounds
  let result: Vec<_> = overlap_for_sorted(5..11, &ranges).collect();
  // 5 is excluded from first range, 11 is excluded from third range
  // Should overlap with 6..10 only
  assert_eq!(result.len(), 1);
}

#[test]
fn test_overlap_for_sorted_mixed_bounds() {
  let ranges: Vec<std::ops::RangeInclusive<i32>> = vec![1..=5, 6..=10, 11..=15];

  // Query with exclusive end
  let result: Vec<_> = overlap_for_sorted(5..11, &ranges).collect();
  // 5 is included in first, 11 is excluded from query
  // Should overlap with 1..=5 (5 included) and 6..=10, but not 11..=15 (11 excluded)
  assert_eq!(result.len(), 2);
}

#[test]
fn test_overlap_for_sorted_large_slice() {
  let ranges: Vec<std::ops::RangeInclusive<i32>> =
    (0..100).map(|i| (i * 10)..=(i * 10 + 5)).collect();

  // Query in the middle
  let result: Vec<_> = overlap_for_sorted(250..=260, &ranges).collect();
  // Should overlap with ranges around 250-260
  assert!(!result.is_empty());
  assert!(result.len() <= 3);
}

#[test]
fn test_overlap_for_sorted_with_byte_slices() {
  // Test with &[u8] as boundary type (unsized type)
  let ranges: Vec<std::ops::RangeInclusive<&[u8]>> = vec![
    &b"apple"[..]..=&b"banana"[..],
    &b"cherry"[..]..=&b"date"[..],
    &b"elder"[..]..=&b"fig"[..],
  ];

  // Query that overlaps with middle range
  let result: Vec<_> = overlap_for_sorted(&b"coconut"[..]..=&b"dragon"[..], &ranges).collect();
  assert_eq!(result.len(), 1); // overlaps with cherry..=date
}

#[test]
fn test_overlap_for_sorted_unbounded_query() {
  let ranges: Vec<std::ops::RangeInclusive<i32>> = vec![1..=5, 6..=10, 11..=15];

  // Query with unbounded start
  let result: Vec<_> = overlap_for_sorted(..=10, &ranges).collect();
  assert_eq!(result.len(), 2); // overlaps with 1..=5 and 6..=10

  // Query with unbounded end
  let result: Vec<_> = overlap_for_sorted(6.., &ranges).collect();
  assert_eq!(result.len(), 2); // overlaps with 6..=10 and 11..=15

  // Query with both unbounded
  let result: Vec<_> = overlap_for_sorted(.., &ranges).collect();
  assert_eq!(result.len(), 3); // overlaps with all
}
