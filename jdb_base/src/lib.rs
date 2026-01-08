//! jdb_base - Shared types for jdb
//! jdb 共享类型
//!
//! Contains Flag, Pos, table and other common types.
//! 包含 Flag、Pos、table 等公共类型。

pub mod ckp;
pub mod compact;
mod flag;
mod id;
mod mem;
mod pos;
mod sorted_vec;
pub mod sst;
pub use ckp::{Ckp, WalId, WalIdOffset, WalOffset};
pub use compact::Compact;
pub use flag::Flag;
pub use id::{id, id_init};
pub use mem::Mem;
pub use pos::Pos;
pub use sorted_vec::SortedVec;
pub use sst::SsTable;

/// Kv pair with boxed byte string key
/// 带盒装字节串 Key 的键值对
pub type Kv = (Box<[u8]>, crate::Pos);

/// Calculate exclusive end bound for prefix
/// 计算前缀的排他结束边界
#[inline]
pub fn prefix_end(prefix: &[u8]) -> Option<Box<[u8]>> {
  // Find last non-0xff byte from end
  // 从末尾找到最后一个非 0xff 字节
  let pos = prefix.iter().rposition(|&b| b < 0xff)?;

  // Construct new key: prefix[..pos] + (prefix[pos] + 1)
  // 构造新 Key
  let mut end = Vec::with_capacity(pos + 1);
  end.extend_from_slice(&prefix[..pos]);
  end.push(prefix[pos] + 1);
  Some(end.into_boxed_slice())
}
