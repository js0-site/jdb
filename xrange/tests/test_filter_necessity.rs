use std::ops::{Bound, Range, RangeBounds};

use xrange::{is_overlap, overlap_for_sorted};

/// 模拟 is_before 的逻辑
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
fn test_filter_necessity_case1() {
  // item: 5..10 (Excluded(10))
  // query: 10..15 (Included(10))
  let item: Range<i32> = 5..10;
  let query: Range<i32> = 10..15;

  let item_end = item.end_bound();
  let query_start = query.start_bound();
  let item_start = item.start_bound();
  let query_end = query.end_bound();

  let passes_first_two_filters =
    !is_before(item_end, query_start) && !is_after(item_start, query_end);
  let actually_overlaps = is_overlap(&item, &query);

  println!("Test case 1: item = 5..10, query = 10..15");
  println!(
    "  is_before(item_end, query_start) = {}",
    is_before(item_end, query_start)
  );
  println!(
    "  is_after(item_start, query_end) = {}",
    is_after(item_start, query_end)
  );
  println!("  Passes first two filters? {}", passes_first_two_filters);
  println!("  is_overlap(&item, &query) = {}", actually_overlaps);

  // 如果 passes_first_two_filters != actually_overlaps，说明 .filter 是必要的
  assert_eq!(
    passes_first_two_filters, actually_overlaps,
    "The first two filters should be equivalent to is_overlap"
  );
}

#[test]
fn test_filter_necessity_case2() {
  // item: 5..=10 (Included(10))
  // query: 10..15 (Included(10))
  let item: std::ops::RangeInclusive<i32> = 5..=10;
  let query: Range<i32> = 10..15;

  let item_end = item.end_bound();
  let query_start = query.start_bound();
  let item_start = item.start_bound();
  let query_end = query.end_bound();

  let passes_first_two_filters =
    !is_before(item_end, query_start) && !is_after(item_start, query_end);
  let actually_overlaps = is_overlap(&item, &query);

  println!("Test case 2: item = 5..=10, query = 10..15");
  println!(
    "  is_before(item_end, query_start) = {}",
    is_before(item_end, query_start)
  );
  println!(
    "  is_after(item_start, query_end) = {}",
    is_after(item_start, query_end)
  );
  println!("  Passes first two filters? {}", passes_first_two_filters);
  println!("  is_overlap(&item, &query) = {}", actually_overlaps);

  assert_eq!(
    passes_first_two_filters, actually_overlaps,
    "The first two filters should be equivalent to is_overlap"
  );
}

#[test]
fn test_filter_necessity_case3() {
  // item: 5..10 (Excluded(10))
  // query: 10..=15 (Included(10))
  let item: Range<i32> = 5..10;
  let query: std::ops::RangeInclusive<i32> = 10..=15;

  let item_end = item.end_bound();
  let query_start = query.start_bound();
  let item_start = item.start_bound();
  let query_end = query.end_bound();

  let passes_first_two_filters =
    !is_before(item_end, query_start) && !is_after(item_start, query_end);
  let actually_overlaps = is_overlap(&item, &query);

  println!("Test case 3: item = 5..10, query = 10..=15");
  println!(
    "  is_before(item_end, query_start) = {}",
    is_before(item_end, query_start)
  );
  println!(
    "  is_after(item_start, query_end) = {}",
    is_after(item_start, query_end)
  );
  println!("  Passes first two filters? {}", passes_first_two_filters);
  println!("  is_overlap(&item, &query) = {}", actually_overlaps);

  assert_eq!(
    passes_first_two_filters, actually_overlaps,
    "The first two filters should be equivalent to is_overlap"
  );
}

#[test]
fn test_filter_necessity_with_overlap_for_sorted() {
  // 测试实际的 overlap_for_sorted 函数
  // 如果前两个 filter 足够，那么不需要最后的 .filter
  let ranges: Vec<Range<i32>> = vec![5..10, 10..15, 15..20];
  let query: Range<i32> = 10..15;

  let result = overlap_for_sorted(query.clone(), &ranges);

  println!("Test with overlap_for_sorted:");
  println!("  ranges = {:?}", ranges);
  println!("  query = {:?}", query);
  println!("  result = {:?}", result);

  // 10..15 应该与 10..15 重叠（边界点）
  // 但 5..10 和 15..20 不应该重叠
  assert_eq!(result.len(), 1, "Should only overlap with 10..15");
}
