//! MergeIter tests
//! 合并迭代器测试

use hipstr::HipByt;
use jdb_base::{
  Pos,
  table::{Kv, MergeIter},
};

fn pos(id: u64) -> Pos {
  Pos::infile(id, 0, 0)
}

fn tombstone() -> Pos {
  Pos::tombstone(0, 0)
}

fn to_kv_vec(src: Vec<(&[u8], Pos)>) -> Vec<Kv> {
  src.into_iter().map(|(k, p)| (HipByt::from(k), p)).collect()
}

#[test]
fn empty() {
  let sources: Vec<std::vec::IntoIter<Kv>> = vec![];
  let mut iter = MergeIter::new(sources, false);
  assert!(iter.next().is_none());
}

#[test]
fn single_source() {
  let src = to_kv_vec(vec![(b"a".as_slice(), pos(1)), (b"b".as_slice(), pos(2))]);
  let mut iter = MergeIter::new(vec![src.into_iter()], false);

  let (k, p) = iter.next().unwrap();
  assert_eq!(k.as_ref(), b"a");
  assert_eq!(p.id(), 1);

  let (k, p) = iter.next().unwrap();
  assert_eq!(k.as_ref(), b"b");
  assert_eq!(p.id(), 2);

  assert!(iter.next().is_none());
}

#[test]
fn merge_priority() {
  // src0 is newest (priority 0)
  // src0 是最新的（优先级 0）
  let src0 = to_kv_vec(vec![(b"a".as_slice(), pos(10))]);
  let src1 = to_kv_vec(vec![(b"a".as_slice(), pos(1))]);

  let mut iter = MergeIter::new(vec![src0.into_iter(), src1.into_iter()], false);

  let (k, p) = iter.next().unwrap();
  assert_eq!(k.as_ref(), b"a");
  assert_eq!(p.id(), 10); // newest wins / 最新的胜出

  assert!(iter.next().is_none());
}

#[test]
fn merge_sorted() {
  let src0 = to_kv_vec(vec![(b"b".as_slice(), pos(2)), (b"d".as_slice(), pos(4))]);
  let src1 = to_kv_vec(vec![(b"a".as_slice(), pos(1)), (b"c".as_slice(), pos(3))]);

  let iter = MergeIter::new(vec![src0.into_iter(), src1.into_iter()], false);
  let keys: Vec<_> = iter.map(|(k, _)| k).collect();

  assert_eq!(keys.len(), 4);
  assert_eq!(keys[0].as_ref(), b"a");
  assert_eq!(keys[1].as_ref(), b"b");
  assert_eq!(keys[2].as_ref(), b"c");
  assert_eq!(keys[3].as_ref(), b"d");
}

#[test]
fn skip_tombstone() {
  let src = to_kv_vec(vec![
    (b"a".as_slice(), pos(1)),
    (b"b".as_slice(), tombstone()),
    (b"c".as_slice(), pos(3)),
  ]);

  // skip_rm = true
  let iter = MergeIter::new(vec![src.clone().into_iter()], true);
  let keys: Vec<_> = iter.map(|(k, _)| k).collect();
  assert_eq!(keys.len(), 2);
  assert_eq!(keys[0].as_ref(), b"a");
  assert_eq!(keys[1].as_ref(), b"c");

  // skip_rm = false
  let iter = MergeIter::new(vec![src.into_iter()], false);
  let keys: Vec<_> = iter.map(|(k, _)| k).collect();
  assert_eq!(keys.len(), 3);
}

#[test]
fn tombstone_overrides() {
  // Newer source has tombstone, older has value
  // 新源有删除标记，旧源有值
  let src0 = to_kv_vec(vec![(b"a".as_slice(), tombstone())]);
  let src1 = to_kv_vec(vec![(b"a".as_slice(), pos(1))]);

  // skip_rm = true: tombstone wins, key is skipped
  // skip_rm = true：删除标记胜出，键被跳过
  let iter = MergeIter::new(
    vec![src0.clone().into_iter(), src1.clone().into_iter()],
    true,
  );
  let keys: Vec<_> = iter.collect();
  assert!(keys.is_empty());

  // skip_rm = false: tombstone is returned
  // skip_rm = false：返回删除标记
  let mut iter = MergeIter::new(vec![src0.into_iter(), src1.into_iter()], false);
  let (k, p) = iter.next().unwrap();
  assert_eq!(k.as_ref(), b"a");
  assert!(p.is_tombstone());
  assert!(iter.next().is_none());
}

#[test]
fn lazy_merge() {
  // Test that merge is lazy (doesn't consume all sources upfront)
  // 测试归并是惰性的（不会预先消费所有源）
  let src0 = to_kv_vec(vec![(b"a".as_slice(), pos(1)), (b"c".as_slice(), pos(3))]);
  let src1 = to_kv_vec(vec![(b"b".as_slice(), pos(2)), (b"d".as_slice(), pos(4))]);

  let mut iter = MergeIter::new(vec![src0.into_iter(), src1.into_iter()], false);

  // Only consume first two
  // 只消费前两个
  let (k, _) = iter.next().unwrap();
  assert_eq!(k.as_ref(), b"a");

  let (k, _) = iter.next().unwrap();
  assert_eq!(k.as_ref(), b"b");

  // Rest should still be available
  // 剩余的应该仍然可用
  let (k, _) = iter.next().unwrap();
  assert_eq!(k.as_ref(), b"c");

  let (k, _) = iter.next().unwrap();
  assert_eq!(k.as_ref(), b"d");

  assert!(iter.next().is_none());
}
