use aok::{OK, Void};
use jdb_lru::{Cache, Lru, NoCache};
use log::info;

#[static_init::constructor(0)]
extern "C" fn _log_init() {
  log_init::init();
}

#[test]
fn test_lru_basic_operations() -> Void {
  info!("> 测试 LRU 缓存基本操作");

  let mut cache = Lru::new(3);

  // 测试 set 和 get
  cache.set("key1".to_string(), "value1");
  cache.set("key2".to_string(), "value2");
  cache.set("key3".to_string(), "value3");

  assert_eq!(cache.get(&"key1".to_string()), Some(&"value1"));
  assert_eq!(cache.get(&"key2".to_string()), Some(&"value2"));
  assert_eq!(cache.get(&"key3".to_string()), Some(&"value3"));

  // 测试不存在的键
  assert_eq!(cache.get(&"nonexistent".to_string()), None);

  // 测试 rm
  cache.rm(&"key2".to_string());
  assert_eq!(cache.get(&"key2".to_string()), None);
  assert_eq!(cache.get(&"key1".to_string()), Some(&"value1"));
  assert_eq!(cache.get(&"key3".to_string()), Some(&"value3"));

  info!("基本操作测试通过");
  OK
}

#[test]
fn test_lru_capacity_and_eviction() -> Void {
  info!("> 测试 LRU 缓存容量限制和淘汰策略");

  let mut cache = Lru::new(2);

  // 填满缓存
  cache.set(1, "a");
  cache.set(2, "b");

  // 访问第一个元素使其成为最近使用
  assert_eq!(cache.get(&1), Some(&"a"));

  // 添加第三个元素，应该淘汰最久未使用的（key=2）
  cache.set(3, "c");

  assert_eq!(cache.get(&1), Some(&"a")); // 仍然存在，因为最近被访问
  assert_eq!(cache.get(&2), None); // 被淘汰
  assert_eq!(cache.get(&3), Some(&"c"));

  // 再次添加元素，淘汰 key=1
  cache.set(4, "d");
  assert_eq!(cache.get(&1), None); // 被淘汰
  assert_eq!(cache.get(&3), Some(&"c"));
  assert_eq!(cache.get(&4), Some(&"d"));

  info!("容量限制和淘汰策略测试通过");
  OK
}

#[test]
fn test_lru_update_existing() -> Void {
  info!("> 测试更新已存在的键");

  let mut cache = Lru::new(2);

  cache.set("key".to_string(), "old_value");
  assert_eq!(cache.get(&"key".to_string()), Some(&"old_value"));

  // 更新已存在的键
  cache.set("key".to_string(), "new_value");
  assert_eq!(cache.get(&"key".to_string()), Some(&"new_value"));

  // 简单测试：添加第二个键，然后添加第三个键
  cache.set("key2".to_string(), "value2");
  info!("添加key2后，缓存包含key和key2");

  // 检查当前状态
  assert_eq!(cache.get(&"key".to_string()), Some(&"new_value"));
  assert_eq!(cache.get(&"key2".to_string()), Some(&"value2"));

  // 添加第三个键，会触发淘汰
  cache.set("key3".to_string(), "value3");
  info!("添加key3后，某个键被淘汰");

  // 检查哪些键还存在
  let key_exists = cache.get(&"key".to_string()).is_some();
  let key2_exists = cache.get(&"key2".to_string()).is_some();
  let key3_exists = cache.get(&"key3".to_string()).is_some();

  info!(
    "key存在: {}, key2存在: {}, key3存在: {}",
    key_exists, key2_exists, key3_exists
  );

  // 应该只有两个键存在
  let existing_count = if key_exists { 1 } else { 0 }
    + if key2_exists { 1 } else { 0 }
    + if key3_exists { 1 } else { 0 };
  assert_eq!(existing_count, 2);
  // key3应该存在（刚添加的）
  assert!(key3_exists);

  info!("更新已存在键测试通过");
  OK
}

#[test]
fn test_nocache_operations() -> Void {
  info!("> 测试 NoCache 操作");

  // 创建特定类型的NoCache实例进行测试
  fn test_string_cache() {
    let mut cache: NoCache = NoCache;
    // 所有操作都应该安全且返回None
    assert!(<NoCache as Cache<String, String>>::get(&mut cache, &"test".to_string()).is_none());
    <NoCache as Cache<String, String>>::set(&mut cache, "key".to_string(), "value".to_string());
    assert!(<NoCache as Cache<String, String>>::get(&mut cache, &"key".to_string()).is_none());
    <NoCache as Cache<String, String>>::rm(&mut cache, &"key".to_string());
    assert!(<NoCache as Cache<String, String>>::get(&mut cache, &"key".to_string()).is_none());
  }

  fn test_int_cache() {
    let mut cache: NoCache = NoCache;
    // 所有操作都应该安全且返回None
    assert!(<NoCache as Cache<i32, &str>>::get(&mut cache, &123).is_none());
    <NoCache as Cache<i32, &str>>::set(&mut cache, 42, "answer");
    assert!(<NoCache as Cache<i32, &str>>::get(&mut cache, &42).is_none());
    <NoCache as Cache<i32, &str>>::rm(&mut cache, &42);
    assert!(<NoCache as Cache<i32, &str>>::get(&mut cache, &42).is_none());
  }

  test_string_cache();
  test_int_cache();

  info!("NoCache 测试通过");
  OK
}

#[test]
fn test_lru_edge_cases() -> Void {
  info!("> 测试边界情况");

  // 测试容量为 1 的情况
  let mut cache = Lru::new(1);
  cache.set(1, "first");
  assert_eq!(cache.get(&1), Some(&"first"));

  cache.set(2, "second");
  assert_eq!(cache.get(&1), None); // 被淘汰
  assert_eq!(cache.get(&2), Some(&"second"));

  // 测试容量为 0 的情况（应该自动调整为 1）
  let mut cache = Lru::new(0);
  cache.set("test".to_string(), "value");
  assert_eq!(cache.get(&"test".to_string()), Some(&"value"));

  // 测试删除不存在的键
  let mut cache: Lru<String, String> = Lru::new(3);
  cache.rm(&"nonexistent".to_string()); // 不应该 panic
  assert_eq!(cache.get(&"nonexistent".to_string()), None);

  // 测试空缓存的删除操作
  let mut empty_cache: Lru<String, String> = Lru::new(5);
  empty_cache.rm(&"key".to_string()); // 不应该 panic

  info!("边界情况测试通过");
  OK
}

#[test]
fn test_lru_complex_types() -> Void {
  info!("> 测试复杂类型");

  #[derive(Debug, PartialEq, Eq, Hash, Clone)]
  struct ComplexKey {
    id: u32,
    name: String,
  }

  #[derive(Debug, PartialEq, Clone)]
  struct ComplexValue {
    data: Vec<i32>,
    metadata: String,
  }

  let mut cache = Lru::new(2);

  let key1 = ComplexKey {
    id: 1,
    name: "first".to_string(),
  };
  let value1 = ComplexValue {
    data: vec![1, 2, 3],
    metadata: "test1".to_string(),
  };

  let key2 = ComplexKey {
    id: 2,
    name: "second".to_string(),
  };
  let value2 = ComplexValue {
    data: vec![4, 5, 6],
    metadata: "test2".to_string(),
  };

  cache.set(key1.clone(), value1.clone());
  cache.set(key2.clone(), value2.clone());

  assert_eq!(cache.get(&key1), Some(&value1));
  assert_eq!(cache.get(&key2), Some(&value2));

  // 测试更新
  let updated_value = ComplexValue {
    data: vec![7, 8, 9],
    metadata: "updated".to_string(),
  };
  cache.set(key1.clone(), updated_value.clone());
  assert_eq!(cache.get(&key1), Some(&updated_value));

  info!("复杂类型测试通过");
  OK
}

#[test]
fn test() -> Void {
  info!("> 运行所有测试 {}", 123456);
  OK
}
