//! GC tests / GC 测试

use jdb_gc::{FileStat, GcStats, GcWorker, LiveTracker};
use jdb_trait::ValRef;

fn vref(file_id: u64, offset: u64) -> ValRef {
  ValRef {
    file_id,
    offset,
    prev_file_id: 0,
    prev_offset: 0,
  }
}

#[test]
fn live_tracker_mark() {
  let mut tracker = LiveTracker::new();

  tracker.mark(&vref(1, 0));
  tracker.mark(&vref(1, 4096));
  tracker.mark(&vref(2, 0));

  assert!(tracker.is_live(1, 0));
  assert!(tracker.is_live(1, 4096));
  assert!(tracker.is_live(2, 0));
  assert!(!tracker.is_live(3, 0));

  assert_eq!(tracker.live_count(1), 2);
  assert_eq!(tracker.live_count(2), 1);
  assert_eq!(tracker.live_count(3), 0);
}

#[test]
fn live_tracker_merge() {
  let mut t1 = LiveTracker::new();
  t1.mark(&vref(1, 0));
  t1.mark(&vref(2, 0));

  let mut t2 = LiveTracker::new();
  t2.mark(&vref(2, 4096));
  t2.mark(&vref(3, 0));

  t1.merge(&t2);

  assert!(t1.is_live(1, 0));
  assert!(t1.is_live(2, 0));
  assert!(t1.is_live(2, 4096));
  assert!(t1.is_live(3, 0));
}

#[test]
fn file_stat_garbage_ratio() {
  let stat = FileStat {
    total: 100,
    live: 60,
    size: 1024,
  };
  assert!((stat.garbage_ratio() - 0.4).abs() < 0.01);

  let empty = FileStat::default();
  assert_eq!(empty.garbage_ratio(), 0.0);
  assert!(empty.is_empty());
}

#[test]
fn gc_stats_merge() {
  let mut s1 = GcStats {
    pages_freed: 10,
    files_deleted: 2,
    files_compacted: 1,
    bytes_reclaimed: 1000,
    tables_scanned: 3,
    keys_scanned: 100,
  };

  let s2 = GcStats {
    pages_freed: 5,
    files_deleted: 1,
    files_compacted: 0,
    bytes_reclaimed: 500,
    tables_scanned: 2,
    keys_scanned: 50,
  };

  s1.merge(&s2);

  assert_eq!(s1.pages_freed, 15);
  assert_eq!(s1.files_deleted, 3);
  assert_eq!(s1.files_compacted, 1);
  assert_eq!(s1.bytes_reclaimed, 1500);
  assert_eq!(s1.tables_scanned, 5);
  assert_eq!(s1.keys_scanned, 150);
}

#[test]
fn gc_worker_state() {
  let mut worker = GcWorker::new();

  assert!(worker.is_idle());
  assert!(!worker.is_done());

  worker.start();
  assert!(!worker.is_idle());

  worker.finish();
  assert!(worker.is_done());

  worker.reset();
  assert!(worker.is_idle());
}

#[test]
fn live_tracker_blob() {
  let mut tracker = LiveTracker::new();

  // Initially no blobs are live / 初始没有存活 blob
  assert!(!tracker.is_blob_live(1));
  assert!(!tracker.is_blob_live(2));

  // Mark blobs / 标记 blob
  tracker.mark_blob(1);
  tracker.mark_blob(3);

  // Check live status / 检查存活状态
  assert!(tracker.is_blob_live(1));
  assert!(!tracker.is_blob_live(2));
  assert!(tracker.is_blob_live(3));

  // Clear should reset blobs / 清空应重置 blob
  tracker.clear();
  assert!(!tracker.is_blob_live(1));
  assert!(!tracker.is_blob_live(3));
}

#[test]
fn live_tracker_merge_blobs() {
  let mut t1 = LiveTracker::new();
  t1.mark_blob(1);
  t1.mark_blob(2);

  let mut t2 = LiveTracker::new();
  t2.mark_blob(2);
  t2.mark_blob(3);

  t1.merge(&t2);

  assert!(t1.is_blob_live(1));
  assert!(t1.is_blob_live(2));
  assert!(t1.is_blob_live(3));
}
