use aok::{OK, Void};
use jdb_api::JdbClient;
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_client_basic() -> Void {
  let dir = "/tmp/jdb_api_test";
  std::fs::remove_dir_all(dir).ok();

  tokio::runtime::Runtime::new().unwrap().block_on(async {
    let client = JdbClient::open(dir).expect("open");

    // Put 写入
    client.put(b"test_table", b"key1", b"val1").await.expect("put");
    client.put(b"test_table", b"key2", b"val2").await.expect("put");

    // Get 读取
    let val = client.get(b"key1").await.expect("get");
    assert_eq!(val, Some(b"val1".to_vec()));

    // Delete 删除
    let deleted = client.delete(b"test_table", b"key1").await.expect("delete");
    assert!(deleted);

    // Get after delete 删除后读取
    let val = client.get(b"key1").await.expect("get");
    assert_eq!(val, None);

    client.close();
    info!("client basic ok");
  });

  std::fs::remove_dir_all(dir).ok();
  OK
}

#[test]
fn test_client_range() -> Void {
  let dir = "/tmp/jdb_api_range";
  std::fs::remove_dir_all(dir).ok();

  tokio::runtime::Runtime::new().unwrap().block_on(async {
    let client = JdbClient::open(dir).expect("open");

    for i in 0..10u32 {
      let key = format!("k{i:02}").into_bytes();
      let val = format!("v{i:02}").into_bytes();
      client.put(b"range_table", &key, &val).await.expect("put");
    }

    let result = client.range(b"k03", b"k07").await.expect("range");
    assert_eq!(result.len(), 5);

    client.close();
    info!("client range ok");
  });

  std::fs::remove_dir_all(dir).ok();
  OK
}

#[test]
fn test_client_flush() -> Void {
  let dir = "/tmp/jdb_api_flush";
  std::fs::remove_dir_all(dir).ok();

  tokio::runtime::Runtime::new().unwrap().block_on(async {
    let client = JdbClient::open(dir).expect("open");

    client.put(b"flush_table", b"fkey", b"fval").await.expect("put");
    client.flush().await.expect("flush");

    client.close();
    info!("client flush ok");
  });

  std::fs::remove_dir_all(dir).ok();
  OK
}
