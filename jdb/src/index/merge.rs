//! Merge iterator for combining multiple sorted sources
//! 合并迭代器，用于组合多个有序源
//!
//! Merges entries from memtable, sealed memtables, and SSTables.
//! 合并来自内存表、密封内存表和 SSTable 的条目。

use std::cmp::Ordering;

use super::Entry;

/// Merged entry with source priority
/// 带源优先级的合并条目
#[derive(Debug, Clone)]
pub struct MergedEntry {
  pub key: Box<[u8]>,
  pub entry: Entry,
}

/// Merge iterator that combines multiple sorted sources
/// 合并多个有序源的迭代器
///
/// Sources are ordered by priority (newest first).
/// Entries with same key use the highest priority source.
/// 源按优先级排序（最新的优先）。
/// 相同键的条目使用最高优先级的源。
pub struct MergeIter {
  /// All entries sorted by key, with duplicates removed (newest wins)
  /// 所有条目按键排序，去除重复（最新的获胜）
  entries: Vec<MergedEntry>,
  /// Current forward position
  /// 当前正向位置
  lo: usize,
  /// Current backward offset from end
  /// 当前反向偏移（从末尾）
  hi_offset: usize,
  /// Whether to skip tombstones
  /// 是否跳过删除标记
  skip_tombstones: bool,
}

impl MergeIter {
  /// Create new merge iterator from multiple sources
  /// 从多个源创建新的合并迭代器
  ///
  /// Sources should be provided in priority order (newest first).
  /// Each source is an iterator of (key, entry) pairs.
  /// 源应按优先级顺序提供（最新的优先）。
  /// 每个源是 (key, entry) 对的迭代器。
  pub fn new<I, K>(sources: Vec<I>, skip_tombstones: bool) -> Self
  where
    I: IntoIterator<Item = (K, Entry)>,
    K: AsRef<[u8]>,
  {
    // Collect all entries with source priority
    // 收集所有条目及其源优先级
    let mut all_entries: Vec<(Box<[u8]>, Entry, usize)> = Vec::new();

    for (priority, source) in sources.into_iter().enumerate() {
      for (key, entry) in source {
        all_entries.push((key.as_ref().into(), entry, priority));
      }
    }

    // Sort by key, then by priority (lower priority = newer = wins)
    // 按键排序，然后按优先级（较低优先级 = 较新 = 获胜）
    all_entries.sort_by(|a, b| match a.0.cmp(&b.0) {
      Ordering::Equal => a.2.cmp(&b.2),
      other => other,
    });

    // Deduplicate: keep first occurrence of each key (lowest priority = newest)
    // 去重：保留每个键的第一次出现（最低优先级 = 最新）
    let mut entries: Vec<MergedEntry> = Vec::new();
    let mut last_key: Option<Box<[u8]>> = None;

    for (key, entry, _priority) in all_entries {
      if last_key
        .as_ref()
        .is_some_and(|k| k.as_ref() == key.as_ref())
      {
        continue; // Skip duplicate
      }
      last_key = Some(key.clone());
      entries.push(MergedEntry { key, entry });
    }

    Self {
      entries,
      lo: 0,
      hi_offset: 0,
      skip_tombstones,
    }
  }

  /// Get remaining count
  /// 获取剩余数量
  #[inline]
  fn remaining(&self) -> usize {
    self.entries.len().saturating_sub(self.lo + self.hi_offset)
  }
}

impl Iterator for MergeIter {
  type Item = MergedEntry;

  fn next(&mut self) -> Option<Self::Item> {
    while self.lo < self.entries.len().saturating_sub(self.hi_offset) {
      let entry = self.entries[self.lo].clone();
      self.lo += 1;

      if self.skip_tombstones && entry.entry.is_tombstone() {
        continue;
      }

      return Some(entry);
    }
    None
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    let remaining = self.remaining();
    (0, Some(remaining))
  }
}

impl DoubleEndedIterator for MergeIter {
  fn next_back(&mut self) -> Option<Self::Item> {
    while self.lo < self.entries.len().saturating_sub(self.hi_offset) {
      self.hi_offset += 1;
      let idx = self.entries.len() - self.hi_offset;
      let entry = self.entries[idx].clone();

      if self.skip_tombstones && entry.entry.is_tombstone() {
        continue;
      }

      return Some(entry);
    }
    None
  }
}

#[cfg(test)]
mod tests {
  use jdb_base::Pos;

  use super::*;

  #[test]
  fn test_merge_single_source() {
    let source = vec![
      (b"a".to_vec(), Entry::Value(Pos::infile(1, 100, 10))),
      (b"b".to_vec(), Entry::Value(Pos::infile(1, 200, 20))),
      (b"c".to_vec(), Entry::Value(Pos::infile(1, 300, 30))),
    ];

    let iter = MergeIter::new(vec![source], false);
    let keys: Vec<_> = iter.map(|e| e.key.to_vec()).collect();

    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
  }

  #[test]
  fn test_merge_multiple_sources() {
    let source1 = vec![
      (b"a".to_vec(), Entry::Value(Pos::infile(1, 100, 10))),
      (b"c".to_vec(), Entry::Value(Pos::infile(1, 300, 30))),
    ];
    let source2 = vec![
      (b"b".to_vec(), Entry::Value(Pos::infile(2, 200, 20))),
      (b"d".to_vec(), Entry::Value(Pos::infile(2, 400, 40))),
    ];

    let iter = MergeIter::new(vec![source1, source2], false);
    let keys: Vec<_> = iter.map(|e| e.key.to_vec()).collect();

    assert_eq!(
      keys,
      vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
    );
  }

  #[test]
  fn test_merge_priority() {
    // source1 is newer (priority 0), source2 is older (priority 1)
    // source1 更新（优先级 0），source2 更旧（优先级 1）
    let source1 = vec![(b"a".to_vec(), Entry::Value(Pos::infile(1, 100, 10)))];
    let source2 = vec![(b"a".to_vec(), Entry::Value(Pos::infile(2, 200, 20)))];

    let iter = MergeIter::new(vec![source1, source2], false);
    let entries: Vec<_> = iter.collect();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].entry, Entry::Value(Pos::infile(1, 100, 10)));
  }

  #[test]
  fn test_merge_skip_tombstones() {
    let source = vec![
      (b"a".to_vec(), Entry::Value(Pos::infile(1, 100, 10))),
      (b"b".to_vec(), Entry::Tombstone),
      (b"c".to_vec(), Entry::Value(Pos::infile(1, 300, 30))),
    ];

    let iter = MergeIter::new(vec![source], true);
    let keys: Vec<_> = iter.map(|e| e.key.to_vec()).collect();

    assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
  }

  #[test]
  fn test_merge_backward() {
    let source = vec![
      (b"a".to_vec(), Entry::Value(Pos::infile(1, 100, 10))),
      (b"b".to_vec(), Entry::Value(Pos::infile(1, 200, 20))),
      (b"c".to_vec(), Entry::Value(Pos::infile(1, 300, 30))),
    ];

    let iter = MergeIter::new(vec![source], false);
    let keys: Vec<_> = iter.rev().map(|e| e.key.to_vec()).collect();

    assert_eq!(keys, vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);
  }

  #[test]
  fn test_merge_ping_pong() {
    let source = vec![
      (b"a".to_vec(), Entry::Value(Pos::infile(1, 100, 10))),
      (b"b".to_vec(), Entry::Value(Pos::infile(1, 200, 20))),
      (b"c".to_vec(), Entry::Value(Pos::infile(1, 300, 30))),
      (b"d".to_vec(), Entry::Value(Pos::infile(1, 400, 40))),
    ];

    let mut iter = MergeIter::new(vec![source], false);

    assert_eq!(iter.next().map(|e| e.key.to_vec()), Some(b"a".to_vec()));
    assert_eq!(
      iter.next_back().map(|e| e.key.to_vec()),
      Some(b"d".to_vec())
    );
    assert_eq!(iter.next().map(|e| e.key.to_vec()), Some(b"b".to_vec()));
    assert_eq!(
      iter.next_back().map(|e| e.key.to_vec()),
      Some(b"c".to_vec())
    );
    assert!(iter.next().is_none());
    assert!(iter.next_back().is_none());
  }
}
