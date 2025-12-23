use aok::{OK, Void};
use jdb_comm::{
  fast_hash128, fast_hash64, route_to_vnode, JdbError, JdbResult, KernelConfig, Lsn, PageID,
  TableID, Timestamp, VNodeID, FILE_MAGIC, INVALID_PAGE_ID, PAGE_HEADER_SIZE, PAGE_SIZE,
};
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

// ============ Types ============

#[test]
fn test_table_id() -> Void {
  let id1 = TableID::new(12345);
  let id2 = TableID::new(12345);
  let id3 = TableID::new(99999);

  assert_eq!(id1, id2);
  assert_ne!(id1, id3);

  // from_name determinism 从名称生成确定性
  let n1 = TableID::from_name(b"test_table");
  let n2 = TableID::from_name(b"test_table");
  let n3 = TableID::from_name(b"other_table");

  assert_eq!(n1, n2);
  assert_ne!(n1, n3);

  // Binary name support 支持二进制名称
  let bin = TableID::from_name(&[0x00, 0xFF, 0xAB]);
  assert!(bin.0 > 0);

  info!("TableID: {id1:?}");
  OK
}

#[test]
fn test_page_id() -> Void {
  let page = PageID::new(100);
  let invalid = PageID::new(INVALID_PAGE_ID);

  assert!(!page.is_invalid());
  assert!(invalid.is_invalid());

  info!("PageID: {page:?}");
  OK
}

#[test]
fn test_vnode_id() -> Void {
  let vnode = VNodeID::new(42);
  assert_eq!(vnode.0, 42);
  info!("VNodeID: {vnode:?}");
  OK
}

#[test]
fn test_timestamp() -> Void {
  let ts1 = Timestamp::now();
  let ts2 = Timestamp::now();

  assert!(ts2 >= ts1);
  assert!(ts1.0 > 0);

  let fixed = Timestamp::new(1234567890);
  assert_eq!(fixed.0, 1234567890);

  info!("Timestamp: {ts1:?}, {ts2:?}");
  OK
}

#[test]
fn test_lsn() -> Void {
  let lsn = Lsn::new(100);
  let next = lsn.next();

  assert_eq!(lsn.0, 100);
  assert_eq!(next.0, 101);
  assert!(next > lsn);

  info!("Lsn: {lsn:?} -> {next:?}");
  OK
}

// ============ Hash ============

#[test]
fn test_hash64_deterministic() -> Void {
  let data = b"hello world";

  let h1 = fast_hash64(data);
  let h2 = fast_hash64(data);
  let h3 = fast_hash64(b"hello world!");

  assert_eq!(h1, h2);
  assert_ne!(h1, h3);

  info!("hash64: {h1:#x}");
  OK
}

#[test]
fn test_hash128_deterministic() -> Void {
  let data = b"test data";

  let h1 = fast_hash128(data);
  let h2 = fast_hash128(data);

  assert_eq!(h1, h2);

  info!("hash128: {h1:#x}");
  OK
}

#[test]
fn test_route_vnode() -> Void {
  let hash = fast_hash64(b"device_001");
  let total = 256u16;

  let v1 = route_to_vnode(hash, total);
  let v2 = route_to_vnode(hash, total);

  assert_eq!(v1, v2);
  assert!(v1.0 < total);

  info!("route: hash={hash:#x} -> vnode={}", v1.0);
  OK
}

// ============ Config ============

#[test]
fn test_config_default() -> Void {
  let cfg = KernelConfig::default();

  assert_eq!(cfg.vnode_count, 256);
  assert_eq!(cfg.io_depth, 128);
  assert_eq!(cfg.block_cache_size, 1024 * 1024 * 1024);

  info!("KernelConfig: {cfg:?}");
  OK
}

#[test]
fn test_config_serde() -> Void {
  let cfg = KernelConfig::default();

  let json = serde_json::to_string(&cfg).expect("json serialize");
  let restored: KernelConfig = serde_json::from_str(&json).expect("json deserialize");

  assert_eq!(cfg.vnode_count, restored.vnode_count);
  assert_eq!(cfg.io_depth, restored.io_depth);

  info!("KernelConfig serde ok");
  OK
}

// ============ Consts ============

#[test]
fn test_consts() -> Void {
  assert_eq!(PAGE_SIZE, 4096);
  assert_eq!(PAGE_HEADER_SIZE, 32);
  assert_eq!(FILE_MAGIC, 0x4A_44_42_5F_46_49_4C_45);
  assert_eq!(INVALID_PAGE_ID, u32::MAX);

  info!("consts: PAGE_SIZE={PAGE_SIZE}, MAGIC={FILE_MAGIC:#x}");
  OK
}

// ============ Error ============

#[test]
fn test_error() -> Void {
  let io_err: JdbError =
    std::io::Error::new(std::io::ErrorKind::NotFound, "not found").into();
  assert!(matches!(io_err, JdbError::Io(_)));

  let cksum_err = JdbError::Checksum {
    expected: 0x1234,
    actual: 0x5678,
  };
  let msg = format!("{cksum_err}");
  assert!(msg.contains("0x1234"));

  let page_err = JdbError::PageNotFound(PageID::new(42));
  let msg = format!("{page_err}");
  assert!(msg.contains("42"));

  info!("JdbError ok");
  OK
}

#[test]
fn test_result() -> Void {
  fn ok_fn() -> JdbResult<i32> {
    Ok(42)
  }
  fn err_fn() -> JdbResult<i32> {
    Err(JdbError::WalFull)
  }

  assert!(ok_fn().is_ok());
  assert!(err_fn().is_err());

  info!("JdbResult ok");
  OK
}

// ============ NewType Safety ============

#[test]
fn test_newtype_safety() -> Void {
  // Compile-time safety: different NewTypes cannot be mixed
  // 编译时安全：不同 NewType 不能混用
  let tid = TableID::new(100);
  let pid = PageID::new(100);

  // Same inner value, different types 内部值相同，类型不同
  assert_eq!(tid.0 as u32, pid.0);

  info!("NewType safety ok");
  OK
}
