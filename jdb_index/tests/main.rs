//! jdb_index 测试 jdb_index tests

use std::time::Duration;

use jdb_comm::{E, R};
use jdb_fs::File;
use jdb_index::{BTree, Key};
use jdb_page::Pool;
use jdb_trait::Val;

/// 运行异步测试 (带超时) Run async test with timeout
fn run<F: std::future::Future<Output = R<()>>>(f: F) -> R<()> {
  let rt = compio::runtime::Runtime::new().unwrap();
  rt.block_on(async {
    compio::time::timeout(Duration::from_secs(5), f)
      .await
      .map_err(|_| E::other("timeout"))?
  })
}

/// 创建临时测试文件 Create temp test file
async fn temp_pool(name: &str) -> R<Pool> {
  let path = format!("/tmp/jdb_index_test_{name}.db");
  let _ = std::fs::remove_file(&path);
  let file = File::create(&path).await.map_err(E::Io)?;
  Pool::open(file, 64).await
}

// ============================================================================
// Key 编码测试 Key encoding tests
// ============================================================================

#[test]
fn test_key_encode_decode_i64() {
  let vals = vec![Val::I64(-100), Val::I64(0), Val::I64(100)];
  let key = Key::encode(&vals);
  let decoded = key.decode();
  assert_eq!(vals, decoded);
}

#[test]
fn test_key_encode_decode_str() {
  let vals = vec![Val::Str("hello".into()), Val::U32(42)];
  let key = Key::encode(&vals);
  let decoded = key.decode();
  assert_eq!(vals, decoded);
}

#[test]
fn test_key_encode_decode_mixed() {
  let vals = vec![
    Val::Bool(true),
    Val::I8(-10),
    Val::U16(1000),
    Val::F64(3.14.into()),
    Val::Str("test".into()),
    Val::Bin(vec![1, 2, 3].into()),
  ];
  let key = Key::encode(&vals);
  let decoded = key.decode();
  assert_eq!(vals, decoded);
}

#[test]
fn test_key_ordering_i64() {
  let k1 = Key::encode(&[Val::I64(-100)]);
  let k2 = Key::encode(&[Val::I64(-1)]);
  let k3 = Key::encode(&[Val::I64(0)]);
  let k4 = Key::encode(&[Val::I64(1)]);
  let k5 = Key::encode(&[Val::I64(100)]);

  assert!(k1 < k2);
  assert!(k2 < k3);
  assert!(k3 < k4);
  assert!(k4 < k5);
}

#[test]
fn test_key_ordering_str() {
  let k1 = Key::encode(&[Val::Str("aaa".into())]);
  let k2 = Key::encode(&[Val::Str("aab".into())]);
  let k3 = Key::encode(&[Val::Str("bbb".into())]);

  assert!(k1 < k2);
  assert!(k2 < k3);
}

// ============================================================================
// B+ 树基础测试 B+ tree basic tests
// ============================================================================

#[test]
fn test_btree_create() -> R<()> {
  run(async {
    let pool = temp_pool("create").await?;
    let tree = BTree::create(pool, true).await?;

    assert_eq!(tree.height(), 1);
    assert_eq!(tree.len(), 0);
    assert!(tree.is_empty());
    assert!(tree.is_unique());

    Ok(())
  })
}

#[test]
fn test_btree_insert_get() -> R<()> {
  run(async {
    let pool = temp_pool("insert_get").await?;
    let mut tree = BTree::create(pool, false).await?; // 非唯一，避免锁问题

    tree.insert(&[Val::I64(1)], 100).await?;
    tree.insert(&[Val::I64(2)], 200).await?;
    tree.insert(&[Val::I64(3)], 300).await?;

    assert_eq!(tree.len(), 3);

    assert_eq!(tree.get(&[Val::I64(1)]).await?, Some(100));
    assert_eq!(tree.get(&[Val::I64(2)]).await?, Some(200));
    assert_eq!(tree.get(&[Val::I64(3)]).await?, Some(300));
    assert_eq!(tree.get(&[Val::I64(4)]).await?, None);

    Ok(())
  })
}

#[test]
fn test_btree_delete() -> R<()> {
  run(async {
    let pool = temp_pool("delete").await?;
    let mut tree = BTree::create(pool, false).await?;

    tree.insert(&[Val::I64(1)], 100).await?;
    tree.insert(&[Val::I64(2)], 200).await?;
    tree.insert(&[Val::I64(3)], 300).await?;

    assert_eq!(tree.len(), 3);

    assert!(tree.delete(&[Val::I64(2)]).await?);
    assert_eq!(tree.len(), 2);
    assert_eq!(tree.get(&[Val::I64(2)]).await?, None);

    assert!(!tree.delete(&[Val::I64(99)]).await?);
    assert_eq!(tree.len(), 2);

    Ok(())
  })
}

#[test]
fn test_btree_bulk_insert() -> R<()> {
  run(async {
    let pool = temp_pool("bulk").await?;
    let mut tree = BTree::create(pool, false).await?;

    for i in 0..50u64 {
      tree.insert(&[Val::U64(i)], i * 10).await?;
    }

    assert_eq!(tree.len(), 50);

    for i in 0..50u64 {
      assert_eq!(tree.get(&[Val::U64(i)]).await?, Some(i * 10));
    }

    Ok(())
  })
}

#[test]
fn test_btree_string_keys() -> R<()> {
  run(async {
    let pool = temp_pool("string_keys").await?;
    let mut tree = BTree::create(pool, false).await?;

    let keys = ["apple", "banana", "cherry"];

    for (i, &k) in keys.iter().enumerate() {
      tree.insert(&[Val::Str(k.into())], i as u64).await?;
    }

    for (i, &k) in keys.iter().enumerate() {
      assert_eq!(tree.get(&[Val::Str(k.into())]).await?, Some(i as u64));
    }

    assert_eq!(tree.get(&[Val::Str("fig".into())]).await?, None);

    Ok(())
  })
}

#[test]
fn test_btree_sync() -> R<()> {
  run(async {
    let pool = temp_pool("sync").await?;
    let mut tree = BTree::create(pool, false).await?;

    for i in 0..10u64 {
      tree.insert(&[Val::U64(i)], i * 10).await?;
    }

    tree.sync().await?;

    for i in 0..10u64 {
      assert_eq!(tree.get(&[Val::U64(i)]).await?, Some(i * 10));
    }

    Ok(())
  })
}

#[test]
fn test_btree_unique_constraint() -> R<()> {
  run(async {
    let pool = temp_pool("unique").await?;
    let mut tree = BTree::create(pool, true).await?;

    tree.insert(&[Val::I64(1)], 100).await?;

    // 重复插入应失败 Duplicate insert should fail
    let result = tree.insert(&[Val::I64(1)], 200).await;
    assert!(result.is_err());

    // 原值不变 Original value unchanged
    assert_eq!(tree.get(&[Val::I64(1)]).await?, Some(100));

    Ok(())
  })
}

#[test]
fn test_btree_update() -> R<()> {
  run(async {
    let pool = temp_pool("update").await?;
    let mut tree = BTree::create(pool, false).await?;

    tree.insert(&[Val::I64(1)], 100).await?;
    assert_eq!(tree.get(&[Val::I64(1)]).await?, Some(100));

    // 更新 Update
    tree.insert(&[Val::I64(1)], 999).await?;
    assert_eq!(tree.get(&[Val::I64(1)]).await?, Some(999));

    Ok(())
  })
}

#[test]
fn test_btree_composite_keys() -> R<()> {
  run(async {
    let pool = temp_pool("composite").await?;
    let mut tree = BTree::create(pool, false).await?;

    // (user_id, timestamp) -> value
    tree.insert(&[Val::U32(1), Val::U64(1000)], 100).await?;
    tree.insert(&[Val::U32(1), Val::U64(2000)], 200).await?;
    tree.insert(&[Val::U32(2), Val::U64(1000)], 300).await?;

    assert_eq!(tree.get(&[Val::U32(1), Val::U64(1000)]).await?, Some(100));
    assert_eq!(tree.get(&[Val::U32(1), Val::U64(2000)]).await?, Some(200));
    assert_eq!(tree.get(&[Val::U32(2), Val::U64(1000)]).await?, Some(300));

    Ok(())
  })
}

// ============================================================================
// 大数据量测试 (触发分裂) Large data tests (trigger splits)
// ============================================================================

#[test]
fn test_btree_large_insert() -> R<()> {
  run(async {
    let pool = temp_pool("large").await?;
    let mut tree = BTree::create(pool, false).await?;

    // 插入 500 条，触发多次分裂
    for i in 0..500u64 {
      tree.insert(&[Val::U64(i)], i * 10).await?;
    }

    assert_eq!(tree.len(), 500);
    assert!(tree.height() > 1); // 应该有多层

    // 验证所有数据
    for i in 0..500u64 {
      assert_eq!(tree.get(&[Val::U64(i)]).await?, Some(i * 10));
    }

    Ok(())
  })
}

#[test]
fn test_btree_random_large() -> R<()> {
  run(async {
    let pool = temp_pool("random_large").await?;
    let mut tree = BTree::create(pool, false).await?;

    // 生成随机顺序
    let mut keys: Vec<u64> = (0..300).collect();
    // 简单洗牌
    for i in (1..keys.len()).rev() {
      let j = (fastrand::u64(..) as usize) % (i + 1);
      keys.swap(i, j);
    }

    for &k in &keys {
      tree.insert(&[Val::U64(k)], k * 2).await?;
    }

    assert_eq!(tree.len(), 300);

    // 验证
    for k in 0..300u64 {
      assert_eq!(tree.get(&[Val::U64(k)]).await?, Some(k * 2));
    }

    Ok(())
  })
}

#[test]
fn test_btree_long_keys() -> R<()> {
  run(async {
    let pool = temp_pool("long_keys").await?;
    let mut tree = BTree::create(pool, false).await?;

    // 长字符串键
    for i in 0..50u64 {
      let key = format!("very_long_key_prefix_{i:05}_suffix");
      tree.insert(&[Val::Str(key.into())], i).await?;
    }

    assert_eq!(tree.len(), 50);

    for i in 0..50u64 {
      let key = format!("very_long_key_prefix_{i:05}_suffix");
      assert_eq!(tree.get(&[Val::Str(key.into())]).await?, Some(i));
    }

    Ok(())
  })
}
