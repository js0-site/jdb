#![cfg_attr(docsrs, feature(doc_cfg))]

//! jdb_index - Merged index for mem + sst
//! 内存表与 SST 的合并索引

mod merge;
mod prefix;

use std::ops::Bound;

use futures_core::Stream;
use jdb_base::{
  Kv, Pos, prefix_end,
  sst::{Flush, OnFlush},
};
use jdb_level::Read;
use jdb_mem::Mems;
pub use merge::{MergeAsc, MergeDesc};
pub use prefix::{PrefixAsc, PrefixDesc};

/// Merged index combining Mems and Read
/// 合并 Mems 和 Read 的索引
pub struct Index<F, N> {
  pub mem: Mems<F, N>,
  pub sst: Read,
}

impl<F: Flush, N: OnFlush> Index<F, N> {
  #[inline]
  pub fn new(mem: Mems<F, N>, sst: Read) -> Self {
    Self { mem, sst }
  }

  /// Get by key (mem first, then sst)
  /// 按键获取（先内存，后 SST）
  pub async fn get(&mut self, key: &[u8]) -> Option<Pos> {
    if let Some(pos) = self.mem.get(key) {
      return Some(pos);
    }
    self.sst.get(key).await
  }

  /// Put key-value pair
  /// 插入键值对
  #[inline]
  pub fn put(&mut self, key: impl Into<Box<[u8]>>, pos: Pos) {
    self.mem.put(key, pos);
  }

  /// Remove key
  /// 删除键
  #[inline]
  pub fn rm(&mut self, key: impl Into<Box<[u8]>>, old_pos: Pos) {
    self.mem.rm(key, old_pos);
  }

  /// Forward range query [start, end)
  /// 正向范围查询 [start, end)
  #[inline]
  pub fn range(
    &mut self,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
  ) -> impl Stream<Item = Kv> + Unpin + '_ {
    MergeAsc::new(self.mem.range(start, end), self.sst.range(start, end))
  }

  /// Reverse range query (end, start]
  /// 反向范围查询 (end, start]
  #[inline]
  pub fn rev_range(
    &mut self,
    end: Bound<&[u8]>,
    start: Bound<&[u8]>,
  ) -> impl Stream<Item = Kv> + Unpin + '_ {
    MergeDesc::new(
      self.mem.rev_range(end, start),
      self.sst.rev_range(end, start),
    )
  }

  /// Iterate all entries ascending
  /// 升序迭代所有条目
  #[inline]
  pub fn iter(&mut self) -> impl Stream<Item = Kv> + Unpin + '_ {
    self.range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Iterate all entries descending
  /// 降序迭代所有条目
  #[inline]
  pub fn rev_iter(&mut self) -> impl Stream<Item = Kv> + Unpin + '_ {
    self.rev_range(Bound::Unbounded, Bound::Unbounded)
  }

  /// Forward prefix scan
  /// 正向前缀扫描
  pub fn prefix(&mut self, prefix: &[u8]) -> impl Stream<Item = Kv> + Unpin + '_ {
    let end = prefix_end(prefix);
    let prefix_owned: Box<[u8]> = prefix.into();

    // SAFETY: end is owned by PrefixAsc, iterators borrow from it via raw pointer
    // The struct ensures end outlives iterators through ManuallyDrop
    // 安全性：end 由 PrefixAsc 拥有，迭代器通过裸指针借用
    // 结构体通过 ManuallyDrop 确保 end 比迭代器活得更久
    unsafe {
      let start = Bound::Included(prefix);
      let end_b = end
        .as_ref()
        .map(|e| Bound::Excluded(std::slice::from_raw_parts(e.as_ptr(), e.len())))
        .unwrap_or(Bound::Unbounded);
      let mem_iter = self.mem.range(start, end_b);
      let sst_stream = self.sst.range(start, end_b);
      PrefixAsc::from_raw(mem_iter, sst_stream, prefix_owned, end)
    }
  }

  /// Reverse prefix scan
  /// 反向前缀扫描
  pub fn rev_prefix(&mut self, prefix: &[u8]) -> impl Stream<Item = Kv> + Unpin + '_ {
    let end = prefix_end(prefix);
    let prefix_owned: Box<[u8]> = prefix.into();

    unsafe {
      let start = Bound::Included(prefix);
      let end_b = end
        .as_ref()
        .map(|e| Bound::Excluded(std::slice::from_raw_parts(e.as_ptr(), e.len())))
        .unwrap_or(Bound::Unbounded);
      let mem_iter = self.mem.rev_range(end_b, start);
      let sst_stream = self.sst.rev_range(end_b, start);
      PrefixDesc::from_raw(mem_iter, sst_stream, prefix_owned)
    }
  }

  /// Flush mem to sst
  /// 刷盘
  pub async fn flush(&mut self) {
    self.mem.flush().await;
  }

  /// Sink sst (compaction)
  /// 下沉 SST
  pub async fn sink(&mut self) -> jdb_level::Result<bool> {
    self.sst.sink().await
  }
}
