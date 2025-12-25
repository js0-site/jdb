//! Periodic flush for buffered writes / 定时刷新缓冲写入
//!
//! Flush every N seconds (default 1s) / 每 N 秒刷新一次（默认 1 秒）

#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{
  cell::UnsafeCell,
  future::Future,
  pin::Pin,
  rc::{Rc, Weak},
  sync::atomic::{AtomicBool, Ordering::Relaxed},
  time::Duration,
};

/// Default flush interval / 默认刷新间隔
pub const DEFAULT_SEC: u64 = 1;

/// Async flush function / 异步刷新函数
pub type AsyncFn = Rc<dyn Fn() -> Pin<Box<dyn Future<Output = ()>>>>;

/// Periodic flush / 定时刷新
pub struct Flush {
  sec: u64,
  hook: UnsafeCell<Vec<AsyncFn>>,
  dirty: AtomicBool,
}

/// Background timer task / 后台定时任务
async fn timer(weak: Weak<Flush>, sec: u64) {
  let dur = Duration::from_secs(sec);
  loop {
    compio::time::sleep(dur).await;
    match weak.upgrade() {
      Some(f) => f.try_flush().await,
      None => break,
    };
  }
}

impl Flush {
  /// Create with interval seconds, default 1s / 创建定时刷新，默认 1 秒
  pub fn new(sec: u64) -> Rc<Self> {
    let sec = if sec == 0 { DEFAULT_SEC } else { sec };
    let this = Rc::new(Self {
      sec,
      hook: UnsafeCell::new(Vec::new()),
      dirty: AtomicBool::new(false),
    });

    compio::runtime::spawn(timer(Rc::downgrade(&this), sec)).detach();
    this
  }

  /// Add hook / 添加回调
  pub fn hook(&self, f: AsyncFn) {
    // Safe: single-threaded / 安全：单线程
    unsafe { (*self.hook.get()).push(f) };
  }

  /// Mark dirty / 标记脏
  pub fn mark(&self) {
    self.dirty.store(true, Relaxed);
  }

  /// Flush if dirty / 脏则刷新
  pub async fn try_flush(&self) -> bool {
    if !self.dirty.swap(false, Relaxed) {
      return false;
    }

    // Safe: single-threaded / 安全：单线程
    let hooks = unsafe { &*self.hook.get() };
    for f in hooks {
      f().await;
    }
    true
  }

  /// Get interval / 获取间隔
  pub fn interval(&self) -> u64 {
    self.sec
  }
}
