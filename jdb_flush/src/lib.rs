//! Flush policy for buffered writes / 缓冲写入刷新策略
//!
//! Redis-style flush triggers: flush if N changes in M seconds.
//! Redis 风格刷新触发器：M 秒内 N 次变更则刷新。

#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{
  cell::UnsafeCell,
  future::Future,
  pin::Pin,
  rc::{Rc, Weak},
  sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
  time::Duration,
};

/// Sec-Item threshold / 秒-条数阈值
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SecItem(pub usize, pub usize);

/// Async flush function / 异步刷新函数
pub type AsyncFn = Rc<dyn Fn() -> Pin<Box<dyn Future<Output = ()>>>>;

/// Flush policy / 刷新策略
pub struct Flush {
  li: Vec<SecItem>,
  hook: UnsafeCell<Vec<AsyncFn>>,
  n: AtomicUsize,
  ts: AtomicU64,
}

/// Background timer task / 后台定时任务
async fn timer(weak: Weak<Flush>) {
  loop {
    compio::time::sleep(Duration::from_secs(1)).await;
    match weak.upgrade() {
      Some(f) => f.try_flush().await,
      None => break,
    };
  }
}

impl Flush {
  /// Create with thresholds, start background task if not empty
  /// 创建刷新策略，非空时启动后台任务
  pub fn new(mut li: Vec<SecItem>) -> Rc<Self> {
    li.sort();
    let has_task = !li.is_empty();
    let this = Rc::new(Self {
      li,
      hook: UnsafeCell::new(Vec::new()),
      n: AtomicUsize::new(0),
      ts: AtomicU64::new(ts_::sec()),
    });

    if has_task {
      compio::runtime::spawn(timer(Rc::downgrade(&this))).detach();
    }

    this
  }

  /// Add hook / 添加回调
  pub fn hook(&self, f: AsyncFn) {
    // Safe: single-threaded / 安全：单线程
    unsafe { (*self.hook.get()).push(f) };
  }

  /// Increment / 增加计数
  pub fn incr(&self) {
    self.n.fetch_add(1, Relaxed);
  }

  /// Check & flush if needed / 检查并按需刷新
  pub async fn try_flush(&self) -> bool {
    let n = self.n.load(Relaxed);
    if n == 0 {
      return false;
    }

    let elapsed = ts_::sec() - self.ts.load(Relaxed);
    let need = self.li.iter().any(|s| elapsed >= s.0 as u64 && n >= s.1);

    if need {
      // Safe: single-threaded / 安全：单线程
      let hooks = unsafe { &*self.hook.get() };
      for f in hooks {
        f().await;
      }
      self.n.store(0, Relaxed);
      self.ts.store(ts_::sec(), Relaxed);
    }

    need
  }

  /// Get current count / 获取当前计数
  pub fn count(&self) -> usize {
    self.n.load(Relaxed)
  }
}
