use std::ops::{Bound, Range, RangeBounds, RangeFull};

use xrange::{is_overlap, overlap_for_sorted};

/// 模拟 is_before 的逻辑
fn is_before<B: PartialOrd>(item_end: Bound<&B>, query_start: Bound<&B>) -> bool {
  use Bound::*;
  match (item_end, query_start) {
    (Included(e), Included(s)) => e < s,
    (Included(e), Excluded(s)) => e <= s,
    (Excluded(e), Included(s)) => e <= s,
    (Excluded(e), Excluded(s)) => e <= s,
    (Unbounded, _) => false,
    (_, Unbounded) => false,
  }
}

/// 模拟 is_after 的逻辑
fn is_after<B: PartialOrd>(item_start: Bound<&B>, query_end: Bound<&B>) -> bool {
  use Bound::*;
  match (item_start, query_end) {
    (Included(s), Included(e)) => s > e,
    (Included(s), Excluded(e)) => s >= e,
    (Excluded(s), Included(e)) => s >= e,
    (Excluded(s), Excluded(e)) => s >= e,
    (Unbounded, _) => false,
    (_, Unbounded) => false,
  }
}

#[test]
fn test_unbounded_query_start() {
  let ranges: Vec<Range<i32>> = vec![5..10, 10..15, 15..20];
  let query = ..15; // Unbounded start, Included(15)

  let result = overlap_for_sorted(query, &ranges);

  println!("Test unbounded query start:");
  println!("  ranges = {:?}", ranges);
  println!("  query = {:?}", query);
  println!("  result = {:?}", result);

  // 应该与 5..10 和 10..15 重叠
  assert_eq!(result.len(), 2);
}

#[test]
fn test_unbounded_query_end() {
  let ranges: Vec<Range<i32>> = vec![5..10, 10..15, 15..20];
  let query = 10..; // Included(10), Unbounded end

  let result = overlap_for_sorted(query.clone(), &ranges);

  println!("Test unbounded query end:");
  println!("  ranges = {:?}", ranges);
  println!("  query = {:?}", query);
  println!("  result = {:?}", result);

  // 应该与 10..15 和 15..20 重叠
  assert_eq!(result.len(), 2);
}

#[test]
fn test_fully_unbounded_query() {
  let ranges: Vec<Range<i32>> = vec![5..10, 10..15, 15..20];
  let query = ..; // Fully unbounded

  let result =
    overlap_for_sorted::<Range<i32>, i32, i32, i32, Range<i32>, RangeFull>(query, &ranges);

  println!("Test fully unbounded query:");
  println!("  ranges = {:?}", ranges);
  println!("  query = {:?}", query);
  println!("  result = {:?}", result);

  // 应该与所有范围重叠
  assert_eq!(result.len(), 3);
}

#[test]
fn test_filter_equivalence_unbounded() {
  // 测试 Unbounded 情况下，前两个 filter 是否等价于 is_overlap
  let ranges: Vec<Range<i32>> = vec![5..10, 10..15, 15..20];
  let query = ..15; // Unbounded start

  for item in &ranges {
    let item_end = item.end_bound();
    let query_start = query.start_bound();
    let item_start = item.start_bound();
    let query_end = query.end_bound();

    let passes_first_two_filters =
      !is_before(item_end, query_start) && !is_after(item_start, query_end);
    let actually_overlaps = is_overlap(item, &query);

    println!("Item = {:?}, Query = {:?}", item, query);
    println!("  Passes first two filters? {}", passes_first_two_filters);
    println!("  is_overlap? {}", actually_overlaps);

    assert_eq!(
      passes_first_two_filters, actually_overlaps,
      "The first two filters should be equivalent to is_overlap for unbounded queries"
    );
  }
}
