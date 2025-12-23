use aok::{OK, Void};
use jdb_comm::{TableID, VNodeID};
use jdb_tablet::Tablet;
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_tablet_put_get() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let dir = "/tmp/jdb_tablet_test";
    std::fs::remove_dir_all(dir).ok();

    let mut tablet = Tablet::create(dir, VNodeID::new(1)).await.expect("create");

    let table = TableID::new(100);
    tablet
      .put(table, b"key1".to_vec(), b"val1".to_vec())
      .await
      .expect("put");
    tablet
      .put(table, b"key2".to_vec(), b"val2".to_vec())
      .await
      .expect("put");

    assert_eq!(tablet.get(b"key1"), Some(b"val1".to_vec()));
    assert_eq!(tablet.get(b"key2"), Some(b"val2".to_vec()));
    assert_eq!(tablet.get(b"key3"), None);

    std::fs::remove_dir_all(dir).ok();
    info!("tablet put/get ok");
  });
  OK
}

#[test]
fn test_tablet_delete() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let dir = "/tmp/jdb_tablet_del";
    std::fs::remove_dir_all(dir).ok();

    let mut tablet = Tablet::create(dir, VNodeID::new(2)).await.expect("create");

    let table = TableID::new(200);
    tablet
      .put(table, b"key".to_vec(), b"val".to_vec())
      .await
      .expect("put");

    assert!(tablet.delete(table, b"key").await.expect("delete"));
    assert_eq!(tablet.get(b"key"), None);

    std::fs::remove_dir_all(dir).ok();
    info!("tablet delete ok");
  });
  OK
}

#[test]
fn test_tablet_range() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let dir = "/tmp/jdb_tablet_range";
    std::fs::remove_dir_all(dir).ok();

    let mut tablet = Tablet::create(dir, VNodeID::new(3)).await.expect("create");

    let table = TableID::new(300);
    for i in 0..10u32 {
      let key = format!("k{i:02}").into_bytes();
      let val = format!("v{i:02}").into_bytes();
      tablet.put(table, key, val).await.expect("put");
    }

    let result = tablet.range(b"k03", b"k07");
    assert_eq!(result.len(), 5);

    std::fs::remove_dir_all(dir).ok();
    info!("tablet range ok");
  });
  OK
}

#[test]
fn test_tablet_recovery() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let dir = "/tmp/jdb_tablet_recover";
    std::fs::remove_dir_all(dir).ok();

    // Create and write 创建并写入
    {
      let mut tablet = Tablet::create(dir, VNodeID::new(4)).await.expect("create");
      let table = TableID::new(400);
      tablet
        .put(table, b"persist".to_vec(), b"data".to_vec())
        .await
        .expect("put");
      tablet.flush().await.expect("flush");
    }

    // Reopen and verify 重新打开并验证
    {
      let tablet = Tablet::open(dir, VNodeID::new(4)).await.expect("open");
      assert_eq!(tablet.get(b"persist"), Some(b"data".to_vec()));
    }

    std::fs::remove_dir_all(dir).ok();
    info!("tablet recovery ok");
  });
  OK
}

#[test]
fn test_tablet_tags() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let dir = "/tmp/jdb_tablet_tags";
    std::fs::remove_dir_all(dir).ok();

    let mut tablet = Tablet::create(dir, VNodeID::new(5)).await.expect("create");

    // Add tags 添加标签
    tablet.add_tag(1, b"region", b"us");
    tablet.add_tag(1, b"type", b"sensor");
    tablet.add_tag(2, b"region", b"us");
    tablet.add_tag(2, b"type", b"gateway");
    tablet.add_tag(3, b"region", b"eu");

    // Query 查询
    let us_sensors = tablet.query_tags(&[(b"region".as_slice(), b"us".as_slice()), (b"type", b"sensor")]);
    assert_eq!(us_sensors, vec![1]);

    std::fs::remove_dir_all(dir).ok();
    info!("tablet tags ok");
  });
  OK
}
