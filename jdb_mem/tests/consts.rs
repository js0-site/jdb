//! Test constants and helpers
//! 测试常量和辅助函数

use jdb_base::{
  Pos,
  sst::{Flush, Meta, OnFlush},
};
use jdb_mem::{DEFAULT_MEM_SIZE, Mems};

// Common keys
// 常用键
pub const K_A: &[u8] = b"a";
pub const K_B: &[u8] = b"b";
pub const K_C: &[u8] = b"c";
pub const K_D: &[u8] = b"d";
pub const K_E: &[u8] = b"e";
pub const K_F: &[u8] = b"f";
pub const K_G: &[u8] = b"g";
pub const K_X: &[u8] = b"x";

pub const KEYS_AG: [&[u8]; 7] = [K_A, K_B, K_C, K_D, K_E, K_F, K_G];
pub const KEYS_GA: [&[u8]; 7] = [K_G, K_F, K_E, K_D, K_C, K_B, K_A];

/// Operation: Some(offset) = put, None = rm
/// 操作：Some(offset) = 插入, None = 删除
pub type Op<'a> = (&'a [u8], Option<u64>);

/// Expected result: (key, is_tombstone, offset if live)
/// 预期结果：(键, 是否墓碑, 活值的偏移)
pub type Expect<'a> = (&'a [u8], bool, u64);

/// Mock flusher for testing (delays response)
/// 测试用模拟刷盘器（延迟响应）
pub struct MockFlush;

impl Flush for MockFlush {
  type Error = ();

  fn flush<'a, I>(&mut self, _iter: I) -> oneshot::Receiver<Result<Meta, ()>>
  where
    I: Iterator<Item = (&'a Box<[u8]>, &'a Pos)>,
  {
    let (tx, rx) = oneshot::channel();
    // Delay response so frozen data is available during await
    // 延迟响应，使 frozen 数据在 await 期间可用
    std::thread::spawn(move || {
      std::thread::sleep(std::time::Duration::from_millis(10));
      let _ = tx.send(Ok(Meta::default()));
    });
    rx
  }
}

/// Mock notify for testing
/// 测试用模拟通知器
pub struct MockNotify;

impl OnFlush for MockNotify {
  fn on_flush(&mut self, _meta: Meta) {}
}

pub type TestMems = Mems<MockFlush, MockNotify>;

#[inline]
pub fn new_mems() -> TestMems {
  Mems::new(MockFlush, MockNotify, DEFAULT_MEM_SIZE)
}

#[inline]
pub fn pos(offset: u64) -> Pos {
  Pos::infile(1, 1, offset, 10)
}

/// Collect keys from iterator
/// 从迭代器收集键
pub fn keys(items: &[(Box<[u8]>, Pos)]) -> Vec<&[u8]> {
  items.iter().map(|(k, _)| k.as_ref()).collect()
}

/// Build mems with operations
/// 构建 mems
pub fn build(mems: &mut TestMems, ops: &[Op]) {
  for &(key, val) in ops {
    match val {
      Some(off) => mems.put(key, pos(off)),
      None => mems.rm(key),
    }
  }
}

/// Build mems with all operations in active (no freeze)
/// 构建 mems，所有操作在 active 中（不冻结）
pub fn build_batches(mems: &mut TestMems, batches: &[&[Op<'_>]]) {
  for ops in batches.iter() {
    build(mems, ops);
  }
}

/// Assert all expected values
/// 断言所有预期值
pub fn check(items: &[(Box<[u8]>, Pos)], expects: &[Expect]) {
  for &(key, is_tomb, offset) in expects {
    let item = items
      .iter()
      .find(|(k, _)| k.as_ref() == key)
      .unwrap_or_else(|| panic!("key {key:?} not found"));
    assert_eq!(
      item.1.is_tombstone(),
      is_tomb,
      "key {key:?} tombstone mismatch"
    );
    if !is_tomb {
      assert_eq!(item.1.offset(), offset, "key {key:?} offset mismatch");
    }
  }
}

/// Filter live keys
/// 过滤活键
pub fn live_keys(items: &[(Box<[u8]>, Pos)]) -> Vec<&[u8]> {
  items
    .iter()
    .filter(|(_, p)| !p.is_tombstone())
    .map(|(k, _)| k.as_ref())
    .collect()
}
