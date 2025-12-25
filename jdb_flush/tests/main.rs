use std::{cell::RefCell, pin::Pin, rc::Rc, time::Duration};

use jdb_flush::{AsyncFn, Flush, SecItem};

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
  let flush = Flush::new(vec![SecItem(0, 2)]);
  flush.hook(f);

  flush.incr();
  assert!(!flush.try_flush().await); // 1 < 2

  flush.incr();
  assert!(flush.try_flush().await); // 2 >= 2
  assert_eq!(*c.borrow(), 1);
}

#[compio::test]
async fn test_background_flush() {
  let (c, f) = counter();
  // 1 sec, 1 item -> flush
  let flush = Flush::new(vec![SecItem(1, 1)]);
  flush.hook(f);

  flush.incr();
  assert_eq!(flush.count(), 1);

  // Wait for background task / 等待后台任务
  compio::time::sleep(Duration::from_millis(1500)).await;

  assert_eq!(*c.borrow(), 1);
  assert_eq!(flush.count(), 0);
}

#[compio::test]
async fn test_drop_stops_task() {
  let (c, f) = counter();
  {
    let flush = Flush::new(vec![SecItem(1, 1)]);
    flush.hook(f);
    flush.incr();
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
  let flush = Flush::new(vec![SecItem(0, 1)]);
  flush.hook(f1);
  flush.hook(f2);

  flush.incr();
  flush.try_flush().await;

  assert_eq!(*c1.borrow(), 1);
  assert_eq!(*c2.borrow(), 1);
}
