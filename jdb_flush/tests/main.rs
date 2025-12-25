use std::{cell::RefCell, pin::Pin, rc::Rc, time::Duration};

use jdb_flush::{AsyncFn, Flush};

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

fn counter() -> (Rc<RefCell<usize>>, AsyncFn) {
  let c = Rc::new(RefCell::new(0));
  let c2 = c.clone();
  let f: AsyncFn = Rc::new(move || {
    let c = c2.clone();
    Box::pin(async move { *c.borrow_mut() += 1 }) as Pin<Box<dyn std::future::Future<Output = ()>>>
  });
  (c, f)
}

#[compio::test]
async fn test_flush() {
  let (c, f) = counter();
  let flush = Flush::new(1);
  flush.hook(f);

  // Not dirty, no flush / 未标记脏，不刷新
  assert!(!flush.try_flush().await);

  flush.mark();
  assert!(flush.try_flush().await);
  assert_eq!(*c.borrow(), 1);

  // Already flushed / 已刷新
  assert!(!flush.try_flush().await);
}

#[compio::test]
async fn test_background_flush() {
  let (c, f) = counter();
  let flush = Flush::new(1);
  flush.hook(f);

  flush.mark();

  // Wait for background task / 等待后台任务
  compio::time::sleep(Duration::from_millis(1500)).await;

  assert_eq!(*c.borrow(), 1);
}

#[compio::test]
async fn test_drop_stops_task() {
  let (c, f) = counter();
  {
    let flush = Flush::new(1);
    flush.hook(f);
    flush.mark();
    // Drop here / 在此 drop
  }

  // Wait to ensure task stopped / 等待确保任务停止
  compio::time::sleep(Duration::from_millis(1500)).await;

  // Should not flush after drop / drop 后不应刷新
  assert_eq!(*c.borrow(), 0);
}

#[compio::test]
async fn test_multiple_hooks() {
  let (c1, f1) = counter();
  let (c2, f2) = counter();
  let flush = Flush::new(1);
  flush.hook(f1);
  flush.hook(f2);

  flush.mark();
  flush.try_flush().await;

  assert_eq!(*c1.borrow(), 1);
  assert_eq!(*c2.borrow(), 1);
}
