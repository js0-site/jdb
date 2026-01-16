
use jdb_pef::{Pef, conf::Conf};
use std::time::Instant;
use rand::Rng;
use std::collections::HashMap;

// Unified Hyperparameter Optimization Script for jdb_pef
// Optimizes:
// 1. L1 Sampling Rate (Select Index L1)
// 2. L2 Sampling Rate (Select Index L2)
// 3. Block Size (Partitioned Chunk Size)

fn main() {
    println!("=== jdb_pef Hyperparameter Search (Random Search) ===");

    // Configuration
    let n_elements = 200_000;
    let iterations = 50; 
    
    // Search Space
    let l1_options = [512, 1024, 2048, 4096];
    let l2_options = [16, 32, 64];
    let block_size_options = [64, 128, 256, 512, 1024];

    println!("Generating dataset ({} elements, SST-like offset distribution)...", n_elements);
    let mut data = Vec::with_capacity(n_elements);
    let mut rng = rand::rng();
    let mut current = 0;
    for _ in 0..n_elements {
        // Average gap 64 bytes (32..96) simluating SST keys
        current += rng.random_range(32..96);
        data.push(current);
    }
    let _max_val = *data.last().unwrap();
    let _min_val = *data.first().unwrap();

    // Benchmarking Data
    let n_queries = 50_000;
    let queries: Vec<usize> = (0..n_queries).map(|_| rng.random_range(0..n_elements)).collect();
    
    // Tracking
    let mut best_score = 0.0;
    let mut best_params = (0, 0, 0);
    let mut tried = HashMap::new();

    println!("\n| {:^6} | {:^6} | {:^6} | {:^8} | {:^10} | {:^10} | {:^10} | {:^8} |", 
             "L1", "L2", "Block", "Size(MB)", "Ratio(%)", "Get(M/s)", "Iter(M/s)", "Score");
    println!("|{:-<8}|{:-<8}|{:-<8}|{:-<10}|{:-<12}|{:-<12}|{:-<12}|{:-<10}|", "-", "-", "-", "-", "-", "-", "-", "-");

    for _i in 1..=iterations {
        // Sample parameters
        let l1 = l1_options[rng.random_range(0..l1_options.len())];
        let l2 = l2_options[rng.random_range(0..l2_options.len())];
        let blk = block_size_options[rng.random_range(0..block_size_options.len())];
        
        // Constraint: L2 must be smaller than L1
        if l2 >= l1 { continue; }
        
        // Avoid duplicates
        if tried.contains_key(&(l1, l2, blk)) { continue; }
        tried.insert((l1, l2, blk), true);

        // --- Build Phase ---
        // Uses the newly added Pef::new_with_params for instance-level config
        let pef = Pef::new_with_conf(&data, Conf { 
            l1_rate: l1, 
            l2_rate: l2, 
            block_size: blk 
        });
        
        // --- Metric: Memory Usage & Compression ---
        let size_bytes = pef.memory_usage();
        let size_mb = size_bytes as f64 / 1024.0 / 1024.0;
        let bpe = (size_bytes as f64 * 8.0) / n_elements as f64;
        let compression_ratio = (bpe / 64.0) * 100.0;
        
        // --- Metric: Random Access (Get) ---
        // Warmup
        let _ = pef.get(queries[0]); 
        
        let start = Instant::now();
        let mut trash = 0;
        for &idx in &queries {
             if let Some(v) = pef.get(idx) {
                 trash ^= v;
             }
        }
        let duration = start.elapsed();
        std::hint::black_box(trash);
        let get_mops = n_queries as f64 / duration.as_secs_f64() / 1_000_000.0;

        // --- Metric: Sequental Iteration (Iter) ---
        // Warmup
        let _ = pef.iter().next();

        let start = Instant::now();
        let mut count = 0;
        for _ in pef.iter() {
            count += 1;
        }
        let duration = start.elapsed();
        std::hint::black_box(count);
        let iter_mops = n_elements as f64 / duration.as_secs_f64() / 1_000_000.0;

        // --- Scoring ---
        // Formula: Score = Get_Speed + 0.4 * Iter_Speed - 2.0 * Compression_Ratio
        // We penalize High Compression Ratio (which means larger size).
        // A lower ratio is better.
        // E.g. 10% ratio is better than 15%.
        // Let's adjust weight: 1% ratio saving ~ how many Mops?
        // Let's say saving space is important but performance is key.
        // Score = Get + 0.4*Iter - Ratio.
        let score = get_mops + iter_mops * 0.4 - compression_ratio;

        println!("| {:<6} | {:<6} | {:<6} | {:<8.2} | {:<10.2} | {:<10.2} | {:<10.2} | {:<8.2} |", 
                 l1, l2, blk, size_mb, compression_ratio, get_mops, iter_mops, score);

        if score > best_score {
            best_score = score;
            best_params = (l1, l2, blk);
        }
    }

    println!("\n=== Optimization Complete ===");
    println!("Best Configuration Found:");
    println!("  L1 Rate    : {}", best_params.0);
    println!("  L2 Rate    : {}", best_params.1);
    println!("  Block Size : {}", best_params.2);
    println!("  Max Score  : {:.2}", best_score);
    println!("=============================");
}
