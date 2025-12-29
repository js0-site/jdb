//! Compare FPP between autoscale and original scalable_cuckoo_filter.
//! 对比 autoscale 和原版 scalable_cuckoo_filter 的 FPP

use autoscale_cuckoo_filter::ScalableCuckooFilterBuilder;
use gxhash::GxHasher;
use scalable_cuckoo_filter::ScalableCuckooFilter as OriginalScalableCuckooFilter;

const ITEMS: usize = 100_000;
const CAPACITY: usize = ITEMS * 2;
const FPP_TEST: usize = 100_000;

fn main() {
    println!("=== FPP Comparison ===\n");

    for fpp in [0.1, 0.01, 0.001, 0.0001] {
        println!("Target FPP: {fpp}");

        // Generate keys
        // 生成键
        fastrand::seed(12345);
        let keys: Vec<u64> = (0..ITEMS as u64).collect();

        fastrand::seed(99999);
        let test_keys: Vec<u64> = (0..FPP_TEST).map(|_| fastrand::u64(ITEMS as u64..)).collect();

        // autoscale_cuckoo_filter
        let mut autoscale = ScalableCuckooFilterBuilder::new()
            .initial_capacity(CAPACITY)
            .false_positive_probability(fpp)
            .hasher(GxHasher::default())
            .finish::<u64>();

        for k in &keys {
            autoscale.add(k);
        }

        let autoscale_fp: usize = test_keys.iter().filter(|k| autoscale.contains(k)).count();
        let autoscale_fpp = autoscale_fp as f64 / FPP_TEST as f64;

        // original scalable_cuckoo_filter
        let mut original = OriginalScalableCuckooFilter::<u64>::new(CAPACITY, fpp);

        for k in &keys {
            original.insert_if_not_contained(k);
        }

        let original_fp: usize = test_keys.iter().filter(|k| original.contains(k)).count();
        let original_fpp = original_fp as f64 / FPP_TEST as f64;

        println!("  autoscale:  {:.4}% ({autoscale_fp} / {FPP_TEST})", autoscale_fpp * 100.0);
        println!("  original:   {:.4}% ({original_fp} / {FPP_TEST})", original_fpp * 100.0);

        // Compare filter internals
        // 对比过滤器内部
        println!("  autoscale bits: {}", autoscale.bits());
        println!("  original bits:  {}", original.bits());
        println!();
    }

    // Detailed analysis for FPP=0.01
    // FPP=0.01 的详细分析
    println!("=== Detailed Analysis (FPP=0.01) ===\n");

    let fpp = 0.01;
    let keys: Vec<u64> = (0..ITEMS as u64).collect();

    let mut autoscale = ScalableCuckooFilterBuilder::new()
        .initial_capacity(CAPACITY)
        .false_positive_probability(fpp)
        .hasher(GxHasher::default())
        .finish::<u64>();

    let mut original = OriginalScalableCuckooFilter::<u64>::new(CAPACITY, fpp);

    for k in &keys {
        autoscale.add(k);
        original.insert_if_not_contained(k);
    }

    println!("autoscale:");
    println!("  len: {}", autoscale.len());
    println!("  capacity: {}", autoscale.capacity());
    println!("  bits: {}", autoscale.bits());
    println!("  entries_per_bucket: {}", autoscale.entries_per_bucket());

    println!("\noriginal:");
    println!("  len: {}", original.len());
    println!("  capacity: {}", original.capacity());
    println!("  bits: {}", original.bits());
    println!("  entries_per_bucket: {}", original.entries_per_bucket());
}
