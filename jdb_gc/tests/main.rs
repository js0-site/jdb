//! GC tests / GC 测试

use std::path::PathBuf;

use jdb_gc::{GcStats, PageGc, SegmentBitmap, VlogGc};
use jdb_page::PageStore;
use jdb_trait::ValRef;

fn temp_path() -> PathBuf {
  let id = fastrand::u64(..);
  std::env::temp_dir().join(format!("jdb_gc_test_{id}.jdb"))
}

fn vref(file_id: u64, offset: u64) -> ValRef {
  ValRef {
    file_id,
    offset,
    prev_file_id: 0,
    prev_offset: 0,
  }
}

#[compio::test]
async fn page_gc_mark_sweep() {
  let path = temp_path();
  let mut store = PageStore::open(&path).await.unwrap();

  // Allocate pages / 分配页
  let p1 = store.alloc();
  let p2 = store.alloc();
  let p3 = store.alloc();

  // Mark only p1 and p3 as reachable / 只标记 p1 和 p3 为可达
  let mut gc = PageGc::new();
  gc.mark(p1);
  gc.mark(p3);

  // p2 should be unreachable / p2 应该不可达
  assert!(!gc.is_marked(p2));
  assert!(gc.is_marked(p1));
  assert!(gc.is_marked(p3));

  // Sweep / 清扫
  let freed = gc.sweep(&mut store);
  assert_eq!(freed, 1);

  let _ = std::fs::remove_file(&path);
}

#[compio::test]
async fn page_gc_stats() {
  let path = temp_path();
  let mut store = PageStore::open(&path).await.unwrap();

  store.alloc();
  store.alloc();
  store.alloc();

  let mut gc = PageGc::new();
  gc.mark(1);

  let stats = gc.stats(store.page_count());
  assert_eq!(stats.total, 4); // 0 + 3 allocated
  assert_eq!(stats.reachable, 1);
  assert_eq!(stats.garbage, 2);
  assert!(stats.ratio() > 0.0);

  let _ = std::fs::remove_file(&path);
}

#[test]
fn vlog_gc_mark() {
  let mut gc = VlogGc::new();

  gc.mark(&vref(1, 0));
  gc.mark(&vref(1, 4096));
  gc.mark(&vref(2, 0));

  // file 1 marked twice, but only counts once / 文件 1 标记两次，但只计一次
  assert_eq!(gc.live_count(), 2);

  let live_files = gc.live_files();
  assert!(live_files.contains(&1));
  assert!(live_files.contains(&2));
}

#[test]
fn vlog_gc_deletable() {
  let mut gc = VlogGc::new();

  gc.mark(&vref(2, 0));
  gc.mark(&vref(3, 0));

  let all_files = vec![1, 2, 3, 4];
  let deletable = gc.deletable_files(&all_files);

  // File 1 and 4 have no live refs / 文件 1 和 4 没有存活引用
  assert!(deletable.contains(&1));
  assert!(deletable.contains(&4));
  assert!(!deletable.contains(&2));
  assert!(!deletable.contains(&3));
}

#[test]
fn gc_stats_ratio() {
  let stats = GcStats {
    total: 100,
    reachable: 60,
    garbage: 39,
  };
  assert!((stats.ratio() - 0.39).abs() < 0.01);

  let empty = GcStats {
    total: 0,
    reachable: 0,
    garbage: 0,
  };
  assert_eq!(empty.ratio(), 0.0);
}

#[compio::test]
async fn segment_bitmap() {
  let path = temp_path();
  let mut store = PageStore::open(&path).await.unwrap();

  let p1 = store.alloc();
  let p2 = store.alloc();
  let p3 = store.alloc();

  // Create segment bitmap / 创建段位图
  let mut seg = SegmentBitmap::new(0);
  assert_eq!(SegmentBitmap::mem_size(), 128 * 1024); // 128KB

  // Mark p1 and p3 / 标记 p1 和 p3
  seg.mark(p1);
  seg.mark(p3);

  assert!(seg.is_marked(p1));
  assert!(!seg.is_marked(p2));
  assert!(seg.is_marked(p3));

  // Sweep / 清扫
  let freed = seg.sweep(&mut store);
  assert_eq!(freed, 1); // p2 freed

  let _ = std::fs::remove_file(&path);
}
