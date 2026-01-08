//! Test constants and helpers
//! 测试常量和辅助函数

use std::rc::Rc;

use jdb_base::Pos;
use jdb_mem::{Handle, Mems, TableMut};

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
pub const K_Y: &[u8] = b"y";

pub const KEYS_AG: [&[u8]; 7] = [K_A, K_B, K_C, K_D, K_E, K_F, K_G];
pub const KEYS_GA: [&[u8]; 7] = [K_G, K_F, K_E, K_D, K_C, K_B, K_A];

/// Operation: Some(offset) = put, None = rm
/// 操作：Some(offset) = 插入, None = 删除
pub type Op<'a> = (&'a [u8], Option<u64>);

/// Expected result: (key, is_tombstone, offset if live)
/// 预期结果：(键, 是否墓碑, 活值的偏移)
pub type Expect<'a> = (&'a [u8], bool, u64);

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
pub fn build(mems: &mut Mems, ops: &[Op]) {
  for &(key, val) in ops {
    match val {
      Some(off) => mems.put(key, pos(off)),
      None => mems.rm(key),
    }
  }
}

/// Build mems with multiple batches, freeze between batches
/// 构建多批次 mems，批次间冻结
pub fn build_batches(mems: &mut Mems, batches: &[&[Op]]) -> Vec<Rc<Handle>> {
  let mut handles = vec![];
  for (i, ops) in batches.iter().enumerate() {
    build(mems, ops);
    if i < batches.len() - 1 {
      handles.push(mems.freeze());
    }
  }
  handles
}

/// Assert all expected values
/// 断言所有预期值
pub fn check(items: &[(Box<[u8]>, Pos)], expects: &[Expect]) {
  for &(key, is_tomb, offset) in expects {
    let item = items.iter().find(|(k, _)| k.as_ref() == key)
      .unwrap_or_else(|| panic!("key {key:?} not found"));
    assert_eq!(item.1.is_tombstone(), is_tomb, "key {key:?} tombstone mismatch");
    if !is_tomb {
      assert_eq!(item.1.offset(), offset, "key {key:?} offset mismatch");
    }
  }
}

/// Filter live keys
/// 过滤活键
pub fn live_keys(items: &[(Box<[u8]>, Pos)]) -> Vec<&[u8]> {
  items.iter()
    .filter(|(_, p)| !p.is_tombstone())
    .map(|(k, _)| k.as_ref())
    .collect()
}
