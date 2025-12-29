//! Regression tests for ScalableCuckooFilter.
//! ScalableCuckooFilter 回归测试

use autoscale_cuckoo_filter::{ScalableCuckooFilter, ScalableCuckooFilterBuilder};

/// Test basic add and contains operations.
#[test]
fn test_basic_add_contains() {
    let mut filter = ScalableCuckooFilter::<u64>::new(1000, 0.001);

    for i in 0..100u64 {
        filter.add(&i);
    }

    for i in 0..100u64 {
        assert!(filter.contains(&i), "Item {i} should be in filter");
    }

    assert_eq!(filter.len(), 100);
}

/// Test FPP within acceptable range.
#[test]
fn test_false_positive_rate() {
    let fpp = 0.01;
    let capacity = 1_000;
    let mut filter = ScalableCuckooFilter::<u64>::new(capacity, fpp);

    for i in 0..capacity as u64 {
        filter.add(&i);
    }

    let mut false_positives = 0;
    let test_count = 1_000;
    for i in capacity as u64..(capacity + test_count) as u64 {
        if filter.contains(&i) {
            false_positives += 1;
        }
    }

    let actual_fpp = false_positives as f64 / test_count as f64;
    assert!(actual_fpp < fpp * 5.0, "FPP {actual_fpp} too high");
}

/// Test remove operation.
#[test]
fn test_remove() {
    let mut filter = ScalableCuckooFilter::<u64>::new(1000, 0.001);

    for i in 0..100u64 {
        filter.add(&i);
    }

    for i in 0..50u64 {
        assert!(filter.remove(&i), "Should remove {i}");
    }

    for i in 0..50u64 {
        assert!(!filter.contains(&i), "Item {i} should be removed");
    }

    for i in 50..100u64 {
        assert!(filter.contains(&i), "Item {i} should exist");
    }

    assert_eq!(filter.len(), 50);
}

/// Test duplicate add_unchecked handling.
#[test]
fn test_duplicate_add_unchecked() {
    let mut filter = ScalableCuckooFilter::<str>::new(1000, 0.001);

    filter.add_unchecked("foo");
    filter.add_unchecked("foo");
    filter.add_unchecked("foo");

    assert_eq!(filter.len(), 3);
    assert!(filter.contains("foo"));

    filter.remove("foo");
    assert!(filter.contains("foo"));
    assert_eq!(filter.len(), 2);

    filter.remove("foo");
    filter.remove("foo");
    assert!(!filter.contains("foo"));
    assert_eq!(filter.len(), 0);
}

/// Test add returns correct value.
#[test]
fn test_add() {
    let mut filter = ScalableCuckooFilter::<u64>::new(1000, 0.001);

    // First add returns false (not previously contained)
    assert!(!filter.add(&42));
    assert_eq!(filter.len(), 1);

    // Second add returns true (already contained)
    assert!(filter.add(&42));
    assert_eq!(filter.len(), 1);

    // Different item
    assert!(!filter.add(&43));
    assert_eq!(filter.len(), 2);
}

/// Test automatic scaling.
#[test]
fn test_auto_scaling() {
    let initial_capacity = 100;
    let mut filter = ScalableCuckooFilter::<u64>::new(initial_capacity, 0.001);

    let initial_cap = filter.capacity();

    for i in 0..1000u64 {
        filter.add(&i);
    }

    assert!(
        filter.capacity() > initial_cap,
        "Capacity should grow: {} > {initial_cap}",
        filter.capacity()
    );

    for i in 0..1000u64 {
        assert!(filter.contains(&i), "Item {i} should exist after scaling");
    }
}

/// Test shrink_to_fit.
#[test]
fn test_shrink_to_fit() {
    let mut filter = ScalableCuckooFilter::<i32>::new(1000, 0.001);

    for i in 0..100i32 {
        filter.add(&i);
    }

    for i in 0..100i32 {
        assert!(filter.contains(&i), "Item {i} missing before shrink");
    }

    let bits_before = filter.bits();
    filter.shrink_to_fit();
    let bits_after = filter.bits();

    assert!(bits_after <= bits_before, "Bits should decrease: {bits_after} <= {bits_before}");

    for i in 0..100i32 {
        assert!(filter.contains(&i), "Item {i} should exist after shrink");
    }
}

/// Test with str type.
#[test]
fn test_str_type() {
    let mut filter = ScalableCuckooFilter::<str>::new(1000, 0.001);

    let items = ["hello", "world", "foo", "bar", "baz"];
    for item in &items {
        filter.add(*item);
    }

    for item in &items {
        assert!(filter.contains(*item));
    }

    assert!(!filter.contains("not_in_filter"));
}

/// Test with owned String type.
#[test]
fn test_owned_string() {
    let mut filter = ScalableCuckooFilter::<String>::new(1000, 0.001);

    let items: Vec<String> = vec!["hello".into(), "world".into(), "test".into()];

    for item in &items {
        filter.add(item);
    }

    for item in &items {
        assert!(filter.contains(item));
    }
}

/// Test empty filter.
#[test]
fn test_empty_filter() {
    let filter = ScalableCuckooFilter::<u64>::new(1000, 0.001);

    assert!(filter.is_empty());
    assert_eq!(filter.len(), 0);
    assert!(!filter.contains(&42));
}

/// Test clone.
#[test]
fn test_clone() {
    let mut filter = ScalableCuckooFilter::<u64>::new(1000, 0.001);

    for i in 0..100u64 {
        filter.add(&i);
    }

    let cloned = filter.clone();

    for i in 0..100u64 {
        assert!(filter.contains(&i));
        assert!(cloned.contains(&i));
    }

    assert_eq!(filter.len(), cloned.len());
    assert_eq!(filter.capacity(), cloned.capacity());
}

/// Test builder configuration.
#[test]
fn test_builder_configuration() {
    let filter: ScalableCuckooFilter<u64> = ScalableCuckooFilterBuilder::new()
        .initial_capacity(500)
        .false_positive_probability(0.01)
        .entries_per_bucket(4)
        .max_kicks(256)
        .finish();

    assert_eq!(filter.false_positive_probability(), 0.01);
    assert_eq!(filter.entries_per_bucket(), 4);
    assert_eq!(filter.max_kicks(), 256);
}

/// Test deterministic behavior with seeded RNG.
#[test]
fn test_deterministic_with_seeded_rng() {
    fastrand::seed(42);

    let mut filter1: ScalableCuckooFilter<u64> = ScalableCuckooFilterBuilder::new()
        .initial_capacity(100)
        .false_positive_probability(0.001)
        .finish();

    fastrand::seed(42);

    let mut filter2: ScalableCuckooFilter<u64> = ScalableCuckooFilterBuilder::new()
        .initial_capacity(100)
        .false_positive_probability(0.001)
        .finish();

    for i in 0..1000u64 {
        filter1.add(&i);
        filter2.add(&i);
    }

    assert_eq!(filter1.len(), filter2.len());
}

/// Stress test.
#[test]
fn test_large_scale() {
    let mut filter = ScalableCuckooFilter::<u64>::new(1_000, 0.01);

    let count = 5_000u64;
    for i in 0..count {
        filter.add(&i);
    }

    // add() skips duplicates, so len may be less than count due to hash collisions
    // add() 会跳过重复项，因此由于哈希冲突 len 可能小于 count
    assert!(filter.len() <= count as usize);
    assert!(filter.len() > count as usize - 100); // Allow some FP collisions

    fastrand::seed(12345);
    for _ in 0..100 {
        let i: u64 = fastrand::u64(0..count);
        assert!(filter.contains(&i));
    }
}

/// Test remove non-existent item.
#[test]
fn test_remove_nonexistent() {
    let mut filter = ScalableCuckooFilter::<u64>::new(1000, 0.001);

    filter.add(&1);
    filter.add(&2);

    assert!(!filter.remove(&999));
    assert_eq!(filter.len(), 2);
}

/// Test filter info methods.
#[test]
fn test_filter_info() {
    let mut filter = ScalableCuckooFilter::<u64>::new(1000, 0.001);

    assert!(filter.bits() > 0);
    assert!(filter.capacity() > 0);
    assert_eq!(filter.false_positive_probability(), 0.001);

    for i in 0..100u64 {
        filter.add(&i);
    }

    assert_eq!(filter.len(), 100);
    assert!(!filter.is_empty());
}

/// Test with various numeric types.
#[test]
fn test_numeric_types() {
    let mut filter_i32 = ScalableCuckooFilter::<i32>::new(100, 0.01);
    let mut filter_i64 = ScalableCuckooFilter::<i64>::new(100, 0.01);
    let mut filter_usize = ScalableCuckooFilter::<usize>::new(100, 0.01);

    for i in 0i32..50 {
        filter_i32.add(&i);
        filter_i64.add(&(i as i64));
        filter_usize.add(&(i as usize));
    }

    for i in 0i32..50 {
        assert!(filter_i32.contains(&i));
        assert!(filter_i64.contains(&(i as i64)));
        assert!(filter_usize.contains(&(i as usize)));
    }
}
