use aok::{OK, Void};
use jdb_comm::{Lsn, TableID, Timestamp};
use jdb_layout::WalEntry;
use jdb_wal::{WalReader, WalWriter};
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_wal_write_read() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let path = "/tmp/jdb_wal_test.wal";

    // Write entries 写入条目
    let mut writer = WalWriter::create(path).await.expect("create");

    let e1 = WalEntry::Put {
      table: TableID::new(1),
      ts: Timestamp::new(100),
      key: b"key1".to_vec(),
      val: b"val1".to_vec(),
    };
    let lsn1 = writer.append(&e1).expect("append");

    let e2 = WalEntry::Delete {
      table: TableID::new(2),
      ts: Timestamp::new(200),
      key: b"key2".to_vec(),
    };
    let lsn2 = writer.append(&e2).expect("append");

    let e3 = WalEntry::Barrier { lsn: Lsn::new(999) };
    let lsn3 = writer.append(&e3).expect("append");

    writer.flush().await.expect("flush");

    assert_eq!(lsn1.0, 1);
    assert_eq!(lsn2.0, 2);
    assert_eq!(lsn3.0, 3);

    // Read back 读回
    let mut reader = WalReader::open(path).await.expect("open");

    let r1 = reader.next().expect("read").expect("entry");
    if let WalEntry::Put { table, key, val, .. } = r1 {
      assert_eq!(table.0, 1);
      assert_eq!(key, b"key1");
      assert_eq!(val, b"val1");
    } else {
      panic!("wrong type");
    }

    let r2 = reader.next().expect("read").expect("entry");
    if let WalEntry::Delete { table, key, .. } = r2 {
      assert_eq!(table.0, 2);
      assert_eq!(key, b"key2");
    } else {
      panic!("wrong type");
    }

    let r3 = reader.next().expect("read").expect("entry");
    if let WalEntry::Barrier { lsn } = r3 {
      assert_eq!(lsn.0, 999);
    } else {
      panic!("wrong type");
    }

    // No more entries 没有更多条目
    let r4 = reader.next().expect("read");
    assert!(r4.is_none());

    std::fs::remove_file(path).ok();
    info!("wal write/read ok");
  });
  OK
}

#[test]
fn test_wal_append_sync() -> Void {
  compio::runtime::Runtime::new().unwrap().block_on(async {
    let path = "/tmp/jdb_wal_sync.wal";

    let mut writer = WalWriter::create(path).await.expect("create");

    let entry = WalEntry::Put {
      table: TableID::new(42),
      ts: Timestamp::new(12345),
      key: b"sync_key".to_vec(),
      val: b"sync_val".to_vec(),
    };

    let lsn = writer.append_sync(&entry).await.expect("append_sync");
    assert_eq!(lsn.0, 1);

    // Verify immediately 立即验证
    let mut reader = WalReader::open(path).await.expect("open");
    let r = reader.next().expect("read").expect("entry");

    if let WalEntry::Put { key, val, .. } = r {
      assert_eq!(key, b"sync_key");
      assert_eq!(val, b"sync_val");
    } else {
      panic!("wrong type");
    }

    std::fs::remove_file(path).ok();
    info!("wal append_sync ok");
  });
  OK
}
