//! Merge tests
//! 合并流测试

use std::{
  ops::Bound,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::Stream;
use futures_executor::block_on;
use futures_util::StreamExt;
use jdb::{Asc, Desc, Merge, MergeBuilder};
use jdb_base::{
  Pos,
  table::{Kv, SsTable, Table},
};

fn pos(id: u64) -> Pos {
  Pos::infile(id, id, 0, 0)
}

fn tombstone() -> Pos {
  Pos::tombstone(0, 0, 0)
}

fn to_kv_vec(src: Vec<(&[u8], Pos)>) -> Vec<Kv> {
  src
    .into_iter()
    .map(|(k, p)| (k.to_vec().into_boxed_slice(), p))
    .collect()
}

/// Wrapper stream for Vec<Kv>
/// Vec<Kv> 的包装流
struct VecStream(std::vec::IntoIter<Kv>);

impl Stream for VecStream {
  type Item = Kv;

  fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Poll::Ready(self.0.next())
  }
}

impl Unpin for VecStream {}

fn to_stream(src: Vec<Kv>) -> VecStream {
  VecStream(src.into_iter())
}

/// Empty stream for sst
/// 空流用于 sst
struct EmptyStream;

impl Stream for EmptyStream {
  type Item = Kv;

  fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Poll::Ready(None)
  }
}

impl Unpin for EmptyStream {}

#[test]
fn empty() {
  block_on(async {
    let mem: Vec<std::vec::IntoIter<Kv>> = vec![];
    let sst = EmptyStream;
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);
    assert!(stream.next().await.is_none());
  });
}

#[test]
fn single_mem_source() {
  block_on(async {
    let src = to_kv_vec(vec![(b"a".as_slice(), pos(1)), (b"b".as_slice(), pos(2))]);
    let mem = vec![src.into_iter()];
    let sst = EmptyStream;
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");
    assert_eq!(p.wal_id(), 1);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"b");
    assert_eq!(p.wal_id(), 2);

    assert!(stream.next().await.is_none());
  });
}

#[test]
fn single_sst_source() {
  block_on(async {
    let src = to_kv_vec(vec![(b"a".as_slice(), pos(1)), (b"b".as_slice(), pos(2))]);
    let mem: Vec<std::vec::IntoIter<Kv>> = vec![];
    let sst = to_stream(src);
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");
    assert_eq!(p.wal_id(), 1);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"b");
    assert_eq!(p.wal_id(), 2);

    assert!(stream.next().await.is_none());
  });
}

#[test]
fn mem_priority_over_sst() {
  block_on(async {
    // mem (idx 0) has higher priority than sst
    // mem（索引 0）优先级高于 sst
    let mem_src = to_kv_vec(vec![(b"a".as_slice(), pos(10))]);
    let sst_src = to_kv_vec(vec![(b"a".as_slice(), pos(1))]);

    let mem = vec![mem_src.into_iter()];
    let sst = to_stream(sst_src);
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");
    assert_eq!(p.wal_id(), 10); // mem wins

    assert!(stream.next().await.is_none());
  });
}

#[test]
fn merge_sorted_asc() {
  block_on(async {
    let mem_src = to_kv_vec(vec![(b"b".as_slice(), pos(2)), (b"d".as_slice(), pos(4))]);
    let sst_src = to_kv_vec(vec![(b"a".as_slice(), pos(1)), (b"c".as_slice(), pos(3))]);

    let mem = vec![mem_src.into_iter()];
    let sst = to_stream(sst_src);
    let stream = Merge::<_, _, Asc>::new(mem, sst, false);
    let keys: Vec<_> = stream.map(|(k, _)| k).collect().await;

    assert_eq!(keys.len(), 4);
    assert_eq!(keys[0].as_ref(), b"a");
    assert_eq!(keys[1].as_ref(), b"b");
    assert_eq!(keys[2].as_ref(), b"c");
    assert_eq!(keys[3].as_ref(), b"d");
  });
}

#[test]
fn skip_tombstone() {
  block_on(async {
    let src = to_kv_vec(vec![
      (b"a".as_slice(), pos(1)),
      (b"b".as_slice(), tombstone()),
      (b"c".as_slice(), pos(3)),
    ]);

    // skip_rm = true
    let mem = vec![src.clone().into_iter()];
    let sst = EmptyStream;
    let stream = Merge::<_, _, Asc>::new(mem, sst, true);
    let keys: Vec<_> = stream.map(|(k, _)| k).collect().await;
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].as_ref(), b"a");
    assert_eq!(keys[1].as_ref(), b"c");

    // skip_rm = false
    let mem = vec![src.into_iter()];
    let sst = EmptyStream;
    let stream = Merge::<_, _, Asc>::new(mem, sst, false);
    let keys: Vec<_> = stream.map(|(k, _)| k).collect().await;
    assert_eq!(keys.len(), 3);
  });
}

#[test]
fn tombstone_overrides() {
  block_on(async {
    // Newer source (mem) has tombstone, older (sst) has value
    // 新源（mem）有删除标记，旧源（sst）有值
    let mem_src = to_kv_vec(vec![(b"a".as_slice(), tombstone())]);
    let sst_src = to_kv_vec(vec![(b"a".as_slice(), pos(1))]);

    // skip_rm = true: tombstone wins, key is skipped
    // skip_rm = true：删除标记胜出，键被跳过
    let mem = vec![mem_src.clone().into_iter()];
    let sst = to_stream(sst_src.clone());
    let stream = Merge::<_, _, Asc>::new(mem, sst, true);
    let keys: Vec<_> = stream.collect().await;
    assert!(keys.is_empty());

    // skip_rm = false: tombstone is returned
    // skip_rm = false：返回删除标记
    let mem = vec![mem_src.into_iter()];
    let sst = to_stream(sst_src);
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);
    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");
    assert!(p.is_tombstone());
    assert!(stream.next().await.is_none());
  });
}

#[test]
fn lazy_merge() {
  block_on(async {
    // Test that merge is lazy (doesn't consume all sources upfront)
    // 测试归并是惰性的（不会预先消费所有源）
    let mem_src = to_kv_vec(vec![(b"a".as_slice(), pos(1)), (b"c".as_slice(), pos(3))]);
    let sst_src = to_kv_vec(vec![(b"b".as_slice(), pos(2)), (b"d".as_slice(), pos(4))]);

    let mem = vec![mem_src.into_iter()];
    let sst = to_stream(sst_src);
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);

    // Only consume first two
    // 只消费前两个
    let (k, _) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");

    let (k, _) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"b");

    // Rest should still be available
    // 剩余的应该仍然可用
    let (k, _) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"c");

    let (k, _) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"d");

    assert!(stream.next().await.is_none());
  });
}

#[test]
fn descending_order() {
  block_on(async {
    // Sources must be in descending order for Desc merge
    // Desc 合并时源必须是降序的
    let mem_src = to_kv_vec(vec![(b"d".as_slice(), pos(4)), (b"b".as_slice(), pos(2))]);
    let sst_src = to_kv_vec(vec![(b"c".as_slice(), pos(3)), (b"a".as_slice(), pos(1))]);

    let mem = vec![mem_src.into_iter()];
    let sst = to_stream(sst_src);
    let stream = Merge::<_, _, Desc>::new(mem, sst, false);
    let keys: Vec<_> = stream.map(|(k, _)| k).collect().await;

    assert_eq!(keys.len(), 4);
    assert_eq!(keys[0].as_ref(), b"d");
    assert_eq!(keys[1].as_ref(), b"c");
    assert_eq!(keys[2].as_ref(), b"b");
    assert_eq!(keys[3].as_ref(), b"a");
  });
}

#[test]
fn desc_priority() {
  block_on(async {
    // mem (idx 0) has higher priority than sst
    // mem（索引 0）优先级高于 sst
    let mem_src = to_kv_vec(vec![(b"a".as_slice(), pos(10))]);
    let sst_src = to_kv_vec(vec![(b"a".as_slice(), pos(1))]);

    let mem = vec![mem_src.into_iter()];
    let sst = to_stream(sst_src);
    let mut stream = Merge::<_, _, Desc>::new(mem, sst, false);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");
    assert_eq!(p.wal_id(), 10); // mem wins

    assert!(stream.next().await.is_none());
  });
}

#[test]
fn multiple_mem_sources() {
  block_on(async {
    // Multiple mem sources, idx 0 has highest priority
    // 多个 mem 源，索引 0 优先级最高
    let mem0 = to_kv_vec(vec![
      (b"a".as_slice(), pos(100)),
      (b"c".as_slice(), pos(300)),
    ]);
    let mem1 = to_kv_vec(vec![(b"a".as_slice(), pos(10)), (b"b".as_slice(), pos(20))]);

    let mem = vec![mem0.into_iter(), mem1.into_iter()];
    let sst = EmptyStream;
    let stream = Merge::<_, _, Asc>::new(mem, sst, false);
    let result: Vec<_> = stream.collect().await;

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].0.as_ref(), b"a");
    assert_eq!(result[0].1.wal_id(), 100); // mem0 wins
    assert_eq!(result[1].0.as_ref(), b"b");
    assert_eq!(result[1].1.wal_id(), 20);
    assert_eq!(result[2].0.as_ref(), b"c");
    assert_eq!(result[2].1.wal_id(), 300);
  });
}

#[test]
fn dedup_same_key() {
  block_on(async {
    // Same key in multiple sources, only first (highest priority) is returned
    // 多个源中相同的键，只返回第一个（最高优先级）
    let mem = to_kv_vec(vec![(b"a".as_slice(), pos(1))]);
    let sst = to_kv_vec(vec![(b"a".as_slice(), pos(2))]);

    let mem = vec![mem.into_iter()];
    let sst = to_stream(sst);
    let stream = Merge::<_, _, Asc>::new(mem, sst, false);
    let result: Vec<_> = stream.collect().await;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1.wal_id(), 1); // mem wins
  });
}

// Mock Table impl for MergeBuilder tests
// MergeBuilder 测试用的 Mock Table 实现
struct MockTable(Vec<Kv>);

impl Table for MockTable {
  type Iter<'a>
    = std::vec::IntoIter<Kv>
  where
    Self: 'a;

  fn get(&self, key: &[u8]) -> Option<Pos> {
    self
      .0
      .iter()
      .find(|(k, _)| k.as_ref() == key)
      .map(|(_, p)| *p)
  }

  fn range(&self, _start: Bound<&[u8]>, _end: Bound<&[u8]>) -> Self::Iter<'_> {
    self.0.clone().into_iter()
  }
}

// Mock SsTable impl for MergeBuilder tests
// MergeBuilder 测试用的 Mock SsTable 实现
struct MockSsTable(Vec<Kv>);

impl SsTable for MockSsTable {
  type RangeStream<'a>
    = VecStream
  where
    Self: 'a;
  type RevStream<'a>
    = VecStream
  where
    Self: 'a;

  async fn get(&mut self, key: &[u8]) -> Option<Pos> {
    self
      .0
      .iter()
      .find(|(k, _)| k.as_ref() == key)
      .map(|(_, p)| *p)
  }

  fn range(&mut self, _start: Bound<&[u8]>, _end: Bound<&[u8]>) -> Self::RangeStream<'_> {
    VecStream(self.0.clone().into_iter())
  }

  fn rev_range(&mut self, _start: Bound<&[u8]>, _end: Bound<&[u8]>) -> Self::RevStream<'_> {
    let mut v = self.0.clone();
    v.reverse();
    VecStream(v.into_iter())
  }
}

#[test]
fn merge_builder_iter() {
  block_on(async {
    let mem = vec![MockTable(to_kv_vec(vec![
      (b"a".as_slice(), pos(1)),
      (b"c".as_slice(), pos(3)),
    ]))];
    let mut sst = MockSsTable(to_kv_vec(vec![
      (b"b".as_slice(), pos(2)),
      (b"d".as_slice(), pos(4)),
    ]));

    let mut builder = MergeBuilder::new(&mem, &mut sst, false);
    let stream = builder.iter();
    let keys: Vec<_> = stream.map(|(k, _)| k).collect().await;

    assert_eq!(keys.len(), 4);
    assert_eq!(keys[0].as_ref(), b"a");
    assert_eq!(keys[1].as_ref(), b"b");
    assert_eq!(keys[2].as_ref(), b"c");
    assert_eq!(keys[3].as_ref(), b"d");
  });
}

#[test]
fn merge_builder_rev_iter() {
  block_on(async {
    let mem = vec![MockTable(to_kv_vec(vec![
      (b"a".as_slice(), pos(1)),
      (b"c".as_slice(), pos(3)),
    ]))];
    let mut sst = MockSsTable(to_kv_vec(vec![
      (b"b".as_slice(), pos(2)),
      (b"d".as_slice(), pos(4)),
    ]));

    let mut builder = MergeBuilder::new(&mem, &mut sst, false);
    let stream = builder.rev_iter();
    let keys: Vec<_> = stream.map(|(k, _)| k).collect().await;

    assert_eq!(keys.len(), 4);
    assert_eq!(keys[0].as_ref(), b"d");
    assert_eq!(keys[1].as_ref(), b"c");
    assert_eq!(keys[2].as_ref(), b"b");
    assert_eq!(keys[3].as_ref(), b"a");
  });
}

#[test]
fn merge_builder_skip_rm() {
  block_on(async {
    let mem = vec![MockTable(to_kv_vec(vec![
      (b"a".as_slice(), pos(1)),
      (b"b".as_slice(), tombstone()),
    ]))];
    let mut sst = MockSsTable(to_kv_vec(vec![(b"c".as_slice(), pos(3))]));

    let mut builder = MergeBuilder::new(&mem, &mut sst, true);
    let stream = builder.iter();
    let keys: Vec<_> = stream.map(|(k, _)| k).collect().await;

    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].as_ref(), b"a");
    assert_eq!(keys[1].as_ref(), b"c");
  });
}
