#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

mod sst_tests {
  use jdb_sst::{FOOT_SIZE, Foot};

  #[test]
  fn test_foot_size() {
    assert_eq!(std::mem::size_of::<Foot>(), FOOT_SIZE);
  }
}
