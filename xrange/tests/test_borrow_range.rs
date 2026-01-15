#[test]
fn test_borrow_range_vec() {
  use std::ops::RangeBounds;

  use xrange::BorrowRange;

  let start = vec![1, 2, 3];
  let end = vec![4, 5, 6];
  let range = start.clone()..end.clone();
  let borrow_range = BorrowRange(&range, std::marker::PhantomData);

  assert_eq!(
    borrow_range.start_bound(),
    std::ops::Bound::Included(start.as_slice())
  );
  assert_eq!(
    borrow_range.end_bound(),
    std::ops::Bound::Excluded(end.as_slice())
  );
}

#[test]
fn test_borrow_range_slice() {
  use std::ops::RangeBounds;

  use xrange::BorrowRange;

  let start = b"abc";
  let end = b"def";
  let range = start.as_slice()..end.as_slice();
  let borrow_range = BorrowRange(&range, std::marker::PhantomData);

  assert_eq!(
    borrow_range.start_bound(),
    std::ops::Bound::Included(start.as_slice())
  );
  assert_eq!(
    borrow_range.end_bound(),
    std::ops::Bound::Excluded(end.as_slice())
  );
}

#[test]
fn test_borrow_range_box() {
  use std::ops::RangeBounds;

  use xrange::BorrowRange;

  let start: Box<[u8]> = Box::new([1, 2, 3]);
  let end: Box<[u8]> = Box::new([4, 5, 6]);
  let range = start.clone()..end.clone();
  let borrow_range = BorrowRange(&range, std::marker::PhantomData);

  assert_eq!(
    borrow_range.start_bound(),
    std::ops::Bound::Included(start.as_ref())
  );
  assert_eq!(
    borrow_range.end_bound(),
    std::ops::Bound::Excluded(end.as_ref())
  );
}
