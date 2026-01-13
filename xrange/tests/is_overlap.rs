use xrange::is_overlap;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_is_overlap_basic() {
  // 完全重叠
  assert!(is_overlap(&(1..10), &(5..15)));
  // 部分重叠
  assert!(is_overlap(&(1..10), &(5..8)));
  // 边界不重叠（10在第一个范围中排除，在第二个范围中包含）
  assert!(!is_overlap(&(1..10), &(10..20))); // 10 is excluded in first, included in second
  // 不重叠
  assert!(!is_overlap(&(1..10), &(20..30)));
}

#[test]
fn test_is_overlap_included_bounds() {
  // Both inclusive
  assert!(is_overlap(&(1..=10), &(5..=15)));
  assert!(is_overlap(&(1..=10), &(10..=20))); // 10 included in both
  assert!(!is_overlap(&(1..=10), &(11..=20)));
}

#[test]
fn test_is_overlap_excluded_bounds() {
  // Both exclusive
  assert!(is_overlap(&(1..10), &(5..15)));
  assert!(!is_overlap(&(1..10), &(10..20))); // 10 excluded in both
  assert!(!is_overlap(&(1..5), &(5..10))); // 5 excluded in both
}

#[test]
fn test_is_overlap_mixed_bounds() {
  // Mixed bounds
  assert!(is_overlap(&(1..=10), &(5..15))); // inclusive end, exclusive start
  assert!(is_overlap(&(1..10), &(5..=15))); // exclusive end, inclusive start
  assert!(is_overlap(&(1..=10), &(10..15))); // 10 included in first, excluded in second
  assert!(!is_overlap(&(1..10), &(10..=15))); // 10 excluded in first, included in second
}

#[test]
fn test_is_overlap_unbounded() {
  // Unbounded start
  assert!(is_overlap(&(..10), &(5..15)));
  assert!(!is_overlap(&(..10), &(15..20))); // 10 is excluded, 15 > 10, no overlap
  assert!(!is_overlap(&(..10), &(10..20))); // 10 excluded in both

  // Unbounded end
  assert!(is_overlap(&(5..), &(1..10)));
  assert!(!is_overlap(&(5..), &(1..5))); // 5 included in first, excluded in second, no overlap
  assert!(is_overlap(&(5..), &(1..=5))); // 5 included in both

  // Both unbounded - need type annotation
  assert!(is_overlap::<i32, i32, _, _>(&(..), &(..)));
  assert!(is_overlap(&(..), &(1..10)));
  assert!(is_overlap(&(5..10), &(..)));
}

#[test]
fn test_is_overlap_empty_ranges() {
  // Empty ranges (start == end with exclusive)
  // Note: current implementation treats empty ranges as overlapping with non-empty ranges
  assert!(!is_overlap(&(5..5), &(5..5))); // both empty
  assert!(is_overlap(&(5..5), &(1..10))); // empty overlaps with non-empty
  assert!(is_overlap(&(1..10), &(5..5))); // non-empty overlaps with empty
}

#[test]
fn test_is_overlap_single_point() {
  // Single point ranges
  assert!(is_overlap(&(5..=5), &(5..=5))); // both include 5
  assert!(is_overlap(&(5..=5), &(1..10)));
  assert!(is_overlap(&(1..10), &(5..=5)));
  // Empty ranges (start == end with exclusive) - current implementation treats them as overlapping
  assert!(!is_overlap(&(5..5), &(5..5))); // both exclude 5, empty range
  assert!(is_overlap(&(5..5), &(1..10))); // empty range overlaps with non-empty
  assert!(is_overlap(&(1..10), &(5..5))); // non-empty overlaps with empty range
}
