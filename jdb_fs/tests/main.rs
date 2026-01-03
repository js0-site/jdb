use aok::{OK, Void};
use log::trace;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

// #[compio::test]
// async fn test_async() -> Void {
//   trace!("async {}", 123456);
//   OK
// }

#[test]
fn test() -> Void {
  trace!("> test {}", 123456);
  OK
}
