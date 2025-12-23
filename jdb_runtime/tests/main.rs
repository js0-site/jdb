use aok::{OK, Void};
use jdb_comm::TableID;
use jdb_runtime::{Runtime, RuntimeConfig};
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_runtime_basic() -> Void {
  let dir = "/tmp/jdb_runtime_test";
  std::fs::remove_dir_all(dir).ok();

  let mut rt = Runtime::new();
  let cfg = RuntimeConfig {
    workers: 1,
    bind_cores: false,
    data_dir: dir.into(),
  };

  rt.start(cfg).expect("start");

  // Use tokio for async test 使用 tokio 进行异步测试
  tokio::runtime::Runtime::new().unwrap().block_on(async {
    let table = TableID::new(1);

    // Put 写入
    rt.put(table, b"key1".to_vec(), b"val1".to_vec())
      .await
      .expect("put");

    // Get 读取
    let val = rt.get(b"key1").await.expect("get");
    assert_eq!(val, Some(b"val1".to_vec()));

    // Delete 删除
    let deleted = rt.delete(table, b"key1").await.expect("delete");
    assert!(deleted);

    // Get after delete 删除后读取
    let val = rt.get(b"key1").await.expect("get");
    assert_eq!(val, None);

    info!("runtime basic ok");
  });

  rt.shutdown();
  std::fs::remove_dir_all(dir).ok();
  OK
}

#[test]
fn test_runtime_range() -> Void {
  let dir = "/tmp/jdb_runtime_range";
  std::fs::remove_dir_all(dir).ok();

  let mut rt = Runtime::new();
  let cfg = RuntimeConfig {
    workers: 1,
    bind_cores: false,
    data_dir: dir.into(),
  };

  rt.start(cfg).expect("start");

  tokio::runtime::Runtime::new().unwrap().block_on(async {
    let table = TableID::new(2);

    for i in 0..10u32 {
      let key = format!("k{i:02}").into_bytes();
      let val = format!("v{i:02}").into_bytes();
      rt.put(table, key, val).await.expect("put");
    }

    let result = rt.range(b"k03", b"k07").await.expect("range");
    assert_eq!(result.len(), 5);

    info!("runtime range ok");
  });

  rt.shutdown();
  std::fs::remove_dir_all(dir).ok();
  OK
}

#[test]
fn test_runtime_flush() -> Void {
  let dir = "/tmp/jdb_runtime_flush";
  std::fs::remove_dir_all(dir).ok();

  let mut rt = Runtime::new();
  let cfg = RuntimeConfig {
    workers: 1,
    bind_cores: false,
    data_dir: dir.into(),
  };

  rt.start(cfg).expect("start");

  tokio::runtime::Runtime::new().unwrap().block_on(async {
    let table = TableID::new(3);
    rt.put(table, b"flush_key".to_vec(), b"flush_val".to_vec())
      .await
      .expect("put");

    rt.flush().await.expect("flush");
    info!("runtime flush ok");
  });

  rt.shutdown();
  std::fs::remove_dir_all(dir).ok();
  OK
}
