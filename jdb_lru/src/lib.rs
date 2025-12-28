//! LRU cache trait and implementations / LRU 缓存 trait 及实现

use std::hash::Hash;

use hashlink::LruCache;

/// LRU cache trait providing basic cache operations
/// 
/// This trait defines the essential operations for a cache implementation:
/// - retrieving values by key
/// - inserting key-value pairs  
/// - removing entries by key
/// 
/// # Type Parameters
/// - `K`: The key type, must implement `Hash` and `Eq` for LRU implementation
/// - `V`: The value type
pub trait Cache<K, V> {
  /// Retrieves a reference to the value corresponding to the key.
  /// 
  /// # Arguments
  /// * `key` - The key to look up
  /// 
  /// # Returns
  /// * `Some(&V)` if the key exists in the cache
  /// * `None` if the key is not found
  fn get(&mut self, key: &K) -> Option<&V>;

  /// Inserts a key-value pair into the cache.
  /// 
  /// If the key already exists, its value will be updated.
  /// For LRU caches, this may affect the eviction order.
  /// 
  /// # Arguments
  /// * `key` - The key to insert
  /// * `val` - The value to associate with the key
  fn set(&mut self, key: K, val: V);

  /// Removes a key-value pair from the cache.
  /// 
  /// # Arguments
  /// * `key` - The key to remove
  /// 
  /// # Returns
  /// This method does not return the removed value to avoid
  /// unnecessary cloning or moving in no-op implementations.
  fn rm(&mut self, key: &K);
}

/// A real LRU (Least Recently Used) cache implementation.
/// 
/// This cache automatically evicts the least recently used items
/// when the capacity limit is reached. The most recently accessed
/// items are kept in the cache.
/// 
/// # Type Parameters  
/// - `K`: Key type, must implement `Hash` + `Eq`
/// - `V`: Value type
/// 
/// # Examples
/// ```
/// use jdb_lru::{Cache, Lru};
/// 
/// let mut cache: Lru<&str, &str> = Lru::new(2);
/// cache.set("key1", "value1");
/// cache.set("key2", "value2");
/// 
/// assert_eq!(cache.get(&"key1"), Some(&"value1"));
/// 
/// // Adding a third item evicts the least recently used
/// cache.set("key3", "value3");
/// assert_eq!(cache.get(&"key2"), None); // evicted
/// ```
pub struct Lru<K: Hash + Eq, V>(pub LruCache<K, V>);

impl<K: Hash + Eq, V> Lru<K, V> {
  /// Creates a new LRU cache with the specified capacity.
  /// 
  /// # Arguments
  /// * `cap` - Maximum number of items the cache can hold
  /// 
  /// # Notes
  /// - If `cap` is 0, it will be automatically set to 1
  /// - The cache will start evicting items once this limit is reached
  /// 
  /// # Panics
  /// This method never panics.
  /// 
  /// # Examples
  /// ```
  /// use jdb_lru::Lru;
  /// 
  /// let cache: Lru<i32, &str> = Lru::new(10);
  /// ```
  #[inline(always)]
  pub fn new(cap: usize) -> Self {
    let capacity = if cap == 0 { 1 } else { cap };
    Self(LruCache::new(capacity))
  }
}

impl<K: Hash + Eq, V> Cache<K, V> for Lru<K, V> {
  #[inline(always)]
  fn get(&mut self, key: &K) -> Option<&V> {
    self.0.get(key)
  }

  #[inline(always)]
  fn set(&mut self, key: K, val: V) {
    self.0.insert(key, val);
  }

  #[inline(always)]
  fn rm(&mut self, key: &K) {
    self.0.remove(key);
  }
}

/// A no-op cache implementation with zero overhead.
/// 
/// This implementation performs no actual caching operations,
/// making it ideal for scenarios where caching is disabled
/// or for garbage collection friendly code paths.
/// 
/// All operations are no-ops and `get()` always returns `None`.
/// This provides a consistent interface while eliminating any
/// allocation or storage overhead.
/// 
/// # Examples
/// 
/// ```ignore
/// use jdb_lru::{Cache, NoCache};
/// 
/// let mut cache: NoCache = NoCache;
/// 
/// // All operations are no-ops
/// cache.set("key", "value");
/// cache.rm(&"key"); // also a no-op
/// // No matter what you do, get() always returns None
/// ```
pub struct NoCache;

impl<K, V> Cache<K, V> for NoCache {
  #[inline(always)]
  fn get(&mut self, _: &K) -> Option<&V> {
    None
  }

  #[inline(always)]
  fn set(&mut self, _: K, _: V) {}

  #[inline(always)]
  fn rm(&mut self, _: &K) {}
}
