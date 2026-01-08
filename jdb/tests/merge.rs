//! Merge tests
//! 合并流测试

use std::{
  cmp::Ordering,
  pin::Pin,
  task::{Context, Poll},
};

use futures_core::Stream;
use futures_executor::block_on;
use futures_util::StreamExt;
use jdb::{Asc, Desc, Merge, PeekIter};
use jdb_base::{Pos, table::Kv};

/// Create position with ver (use random wal_id to avoid confusion)
/// 创建带 ver 的位置（使用随机 wal_id 避免混淆）
fn pos(ver: u64) -> Pos {
  Pos::infile(ver, fastrand::u64(..), 0, 0)
}

/// Create tombstone position
/// 创建删除标记位置
fn tombstone() -> Pos {
  Pos::tombstone(0, fastrand::u64(..), 0)
}

/// Convert slice pairs to Kv vec
/// 将切片对转换为 Kv 向量
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

impl futures_core::stream::FusedStream for VecStream {
  fn is_terminated(&self) -> bool {
    self.0.len() == 0
  }
}

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

impl futures_core::stream::FusedStream for EmptyStream {
  fn is_terminated(&self) -> bool {
    true
  }
}

/// Test iterator wrapper implementing PeekIter
/// 测试用的迭代器包装器，实现 PeekIter
struct TestIter<I> {
  inner: I,
  cur: Option<Kv>,
  id: u64,
}

impl<I: Iterator<Item = Kv>> TestIter<I> {
  fn new(mut inner: I, id: u64) -> Self {
    let cur = inner.next();
    Self { inner, cur, id }
  }
}

impl<I: Iterator<Item = Kv>> PeekIter for TestIter<I> {
  fn peek(&self) -> Option<&Kv> {
    self.cur.as_ref()
  }

  fn take(&mut self) -> Option<Kv> {
    let item = self.cur.take();
    self.cur = self.inner.next();
    item
  }
}

impl<I: Iterator<Item = Kv>> Eq for TestIter<I> {}

impl<I: Iterator<Item = Kv>> PartialEq for TestIter<I> {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl<I: Iterator<Item = Kv>> PartialOrd for TestIter<I> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl<I: Iterator<Item = Kv>> Ord for TestIter<I> {
  fn cmp(&self, other: &Self) -> Ordering {
    match (&self.cur, &other.cur) {
      (Some((k1, _)), Some((k2, _))) => {
        // Reverse for min-heap (BinaryHeap is max-heap)
        // 反转以实现最小堆（BinaryHeap 是最大堆）
        match k2.as_ref().cmp(k1.as_ref()) {
          // Same key: higher id (newer) wins
          // 相同键：id 大的（更新）胜出
          Ordering::Equal => self.id.cmp(&other.id),
          ord => ord,
        }
      }
      (Some(_), None) => Ordering::Greater,
      (None, Some(_)) => Ordering::Less,
      (None, None) => Ordering::Equal,
    }
  }
}

#[test]
fn empty_sources() {
  block_on(async {
    let mem: Vec<TestIter<std::vec::IntoIter<Kv>>> = vec![];
    let sst = EmptyStream;
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);
    assert!(stream.next().await.is_none());
  });
}

#[test]
fn single_mem_source() {
  block_on(async {
    let src = to_kv_vec(vec![(b"a".as_slice(), pos(1)), (b"b".as_slice(), pos(2))]);
    let mem = vec![TestIter::new(src.into_iter(), 10)];
    let sst = EmptyStream;
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");
    assert_eq!(p.ver(), 1);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"b");
    assert_eq!(p.ver(), 2);

    assert!(stream.next().await.is_none());
  });
}

#[test]
fn single_sst_source() {
  block_on(async {
    let src = to_kv_vec(vec![(b"a".as_slice(), pos(1)), (b"b".as_slice(), pos(2))]);
    let mem: Vec<TestIter<std::vec::IntoIter<Kv>>> = vec![];
    let sst = to_stream(src);
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");
    assert_eq!(p.ver(), 1);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"b");
    assert_eq!(p.ver(), 2);

    assert!(stream.next().await.is_none());
  });
}

#[test]
fn mem_priority_over_sst() {
  block_on(async {
    // Same key in mem and sst, mem should win
    // 相同键在 mem 和 sst 中，mem 应该胜出
    let mem_src = to_kv_vec(vec![(b"a".as_slice(), pos(10))]);
    let sst_src = to_kv_vec(vec![(b"a".as_slice(), pos(1))]);

    let mem = vec![TestIter::new(mem_src.into_iter(), 10)];
    let sst = to_stream(sst_src);
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");
    assert_eq!(p.ver(), 10); // mem wins

    assert!(stream.next().await.is_none());
  });
}

#[test]
fn interleaved_keys() {
  block_on(async {
    let mem_src = to_kv_vec(vec![(b"a".as_slice(), pos(10)), (b"c".as_slice(), pos(30))]);
    let sst_src = to_kv_vec(vec![(b"b".as_slice(), pos(2)), (b"d".as_slice(), pos(4))]);

    let mem = vec![TestIter::new(mem_src.into_iter(), 10)];
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
    let mem = vec![TestIter::new(src.clone().into_iter(), 10)];
    let sst = EmptyStream;
    let stream = Merge::<_, _, Asc>::new(mem, sst, true);
    let keys: Vec<_> = stream.map(|(k, _)| k).collect().await;

    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].as_ref(), b"a");
    assert_eq!(keys[1].as_ref(), b"c");

    // skip_rm = false
    let mem = vec![TestIter::new(src.into_iter(), 10)];
    let sst = EmptyStream;
    let stream = Merge::<_, _, Asc>::new(mem, sst, false);
    let keys: Vec<_> = stream.map(|(k, _)| k).collect().await;

    assert_eq!(keys.len(), 3);
  });
}

#[test]
fn tombstone_overrides() {
  block_on(async {
    // mem has tombstone, sst has value - mem wins
    // mem 有删除标记，sst 有值 - mem 胜出
    let mem_src = to_kv_vec(vec![(b"a".as_slice(), tombstone())]);
    let sst_src = to_kv_vec(vec![(b"a".as_slice(), pos(1))]);

    // skip_rm = true: tombstone wins, key is skipped
    // skip_rm = true：删除标记胜出，键被跳过
    let mem = vec![TestIter::new(mem_src.clone().into_iter(), 10)];
    let sst = to_stream(sst_src.clone());
    let stream = Merge::<_, _, Asc>::new(mem, sst, true);
    let keys: Vec<_> = stream.map(|(k, _)| k).collect().await;
    assert!(keys.is_empty());

    // skip_rm = false: tombstone is returned
    // skip_rm = false：返回删除标记
    let mem = vec![TestIter::new(mem_src.into_iter(), 10)];
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
    // Test that merge is lazy
    // 测试合并是惰性的
    let mem_src = to_kv_vec(vec![(b"a".as_slice(), pos(1)), (b"c".as_slice(), pos(3))]);
    let sst_src = to_kv_vec(vec![(b"b".as_slice(), pos(2)), (b"d".as_slice(), pos(4))]);

    let mem = vec![TestIter::new(mem_src.into_iter(), 10)];
    let sst = to_stream(sst_src);
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);

    let (k, _) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");

    let (k, _) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"b");

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
    // Sources in descending order for Desc merge
    // Desc 合并时源是降序的
    let mem_src = to_kv_vec(vec![(b"c".as_slice(), pos(3)), (b"a".as_slice(), pos(1))]);
    let sst_src = to_kv_vec(vec![(b"d".as_slice(), pos(4)), (b"b".as_slice(), pos(2))]);

    let mem = vec![TestIter::new(mem_src.into_iter(), 10)];
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
fn multiple_mem_sources() {
  block_on(async {
    // Multiple mem sources, higher id wins on same key
    // 多个 mem 源，相同键时 id 大的胜出
    let mem0 = to_kv_vec(vec![(b"a".as_slice(), pos(10)), (b"c".as_slice(), pos(30))]);
    let mem1 = to_kv_vec(vec![
      (b"a".as_slice(), pos(20)),
      (b"b".as_slice(), pos(100)),
    ]);

    // mem0 id=10, mem1 id=20
    let mem = vec![
      TestIter::new(mem0.into_iter(), 10),
      TestIter::new(mem1.into_iter(), 20),
    ];
    let sst = EmptyStream;
    let stream = Merge::<_, _, Asc>::new(mem, sst, false);
    let result: Vec<_> = stream.collect().await;

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].0.as_ref(), b"a");
    assert_eq!(result[0].1.ver(), 20); // mem1 wins (higher id)
    assert_eq!(result[1].0.as_ref(), b"b");
    assert_eq!(result[1].1.ver(), 100);
    assert_eq!(result[2].0.as_ref(), b"c");
    assert_eq!(result[2].1.ver(), 30);
  });
}

#[test]
fn dedup_same_key() {
  block_on(async {
    // Same key: mem always wins over sst
    // 相同键：mem 总是胜出
    let mem = to_kv_vec(vec![(b"a".as_slice(), pos(10))]);
    let sst = to_kv_vec(vec![(b"a".as_slice(), pos(20))]);

    let mem = vec![TestIter::new(mem.into_iter(), 10)];
    let sst = to_stream(sst);
    let mut stream = Merge::<_, _, Asc>::new(mem, sst, false);

    let (k, p) = stream.next().await.unwrap();
    assert_eq!(k.as_ref(), b"a");
    assert_eq!(p.ver(), 10); // mem wins

    assert!(stream.next().await.is_none());
  });
}
