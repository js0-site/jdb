use std::{collections::HashSet, time::Instant};

use jdb_pef::{Pef, conf::Conf};
use perpetual::{Matrix, PerpetualBooster, objective_functions::Objective};

// jdb_pef Hyperparameter Search using PerpetualBooster as a surrogate model
// Optimizes: L1 Rate, L2 Rate, Block Size

fn main() {
  println!("=== jdb_pef Hyperparameter Search (Perpetual GBM) ===");

  // Configuration
  let n_elements = 200_000;
  let iterations = 30;

  // Search Space
  let l1_options = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096];
  let l2_options = [8, 16, 32, 64, 128];
  let block_size_options = [64, 128, 256, 512, 1024, 2048];

  // Build Grid
  let mut grid = Vec::new();
  for &l1 in &l1_options {
    for &l2 in &l2_options {
      for &blk in &block_size_options {
        if l2 < l1 {
          grid.push((l1, l2, blk));
        }
      }
    }
  }
  println!("Search space size: {}", grid.len());

  // RNG
  let mut rng = fastrand::Rng::with_seed(42);

  println!(
    "Generating dataset ({} elements, SST-like offset distribution)...",
    n_elements
  );
  let mut data = Vec::with_capacity(n_elements);
  let mut current: u64 = 0;
  for _ in 0..n_elements {
    current += rng.u64(32..128);
    data.push(current);
  }

  // Benchmarking Data
  let n_queries = 50_000;
  let queries: Vec<usize> = (0..n_queries).map(|_| rng.usize(0..n_elements)).collect();

  // History
  let mut history_params: Vec<(usize, usize, usize)> = Vec::new();
  let mut history_scores: Vec<f64> = Vec::new();
  let mut tested_set: HashSet<(usize, usize, usize)> = HashSet::new();

  let mut best_score = f64::NEG_INFINITY;
  let mut best_params = (0, 0, 0);

  println!(
    "\n| {:^6} | {:^6} | {:^6} | {:^8} | {:^10} | {:^10} | {:^10} | {:^8} |",
    "L1", "L2", "Block", "Size(MB)", "Ratio(%)", "Get(M/s)", "Iter(M/s)", "Score"
  );
  println!(
    "|{:-<8}|{:-<8}|{:-<8}|{:-<10}|{:-<12}|{:-<12}|{:-<12}|{:-<10}|",
    "-", "-", "-", "-", "-", "-", "-", "-"
  );

  // Initial Random Samples (Warmup)
  let init_samples = 5;
  for _ in 0..init_samples {
    let candidate = grid[rng.usize(0..grid.len())];
    if !tested_set.contains(&candidate) {
      evaluate(
        candidate,
        &data,
        &queries,
        &mut history_params,
        &mut history_scores,
        &mut tested_set,
        &mut best_score,
        &mut best_params,
      );
    }
  }

  // Optimization Loop
  for _ in 0..(iterations - init_samples) {
    if grid.len() == tested_set.len() {
      break;
    }

    // 1. Prepare Trainting Data
    let mut features = Vec::with_capacity(history_params.len() * 3);
    let targets = history_scores.clone();
    for (l1, l2, blk) in &history_params {
      features.push(*l1 as f64);
      features.push(*l2 as f64);
      features.push(*blk as f64);
    }

    // Matrix::new takes flat slice, rows, cols
    let train_mat = Matrix::new(&features, history_scores.len(), 3);

    // 2. Train Model
    // Default might use LogLoss, so we set SquaredLoss for regression
    let mut model = PerpetualBooster::default();
    model.cfg.objective = Objective::SquaredLoss; // Check if this field allows write

    // fit(data, y, sample_weight, group)
    model.fit(&train_mat, &targets, None, None).unwrap();

    // 3. Predict all untried candidates
    let mut best_candidate = None;
    let mut best_pred = f64::NEG_INFINITY;

    let mut candidate_features = Vec::new();
    let mut candidates = Vec::new();

    for &candidate in &grid {
      if !tested_set.contains(&candidate) {
        candidates.push(candidate);
        candidate_features.push(candidate.0 as f64);
        candidate_features.push(candidate.1 as f64);
        candidate_features.push(candidate.2 as f64);
      }
    }

    if candidates.is_empty() {
      break;
    }

    let pred_mat = Matrix::new(&candidate_features, candidates.len(), 3);
    let preds = model.predict(&pred_mat, false); // false for single thread

    for (i, &pred) in preds.iter().enumerate() {
      if pred > best_pred {
        best_pred = pred;
        best_candidate = Some(candidates[i]);
      }
    }

    if let Some(cand) = best_candidate {
      evaluate(
        cand,
        &data,
        &queries,
        &mut history_params,
        &mut history_scores,
        &mut tested_set,
        &mut best_score,
        &mut best_params,
      );
    } else {
      break;
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

fn evaluate(
  params: (usize, usize, usize),
  data: &[u64],
  queries: &[usize],
  history_params: &mut Vec<(usize, usize, usize)>,
  history_scores: &mut Vec<f64>,
  tested_set: &mut HashSet<(usize, usize, usize)>,
  best_score: &mut f64,
  best_params: &mut (usize, usize, usize),
) {
  tested_set.insert(params);
  let (l1, l2, blk) = params;
  let n_elements = data.len();
  let n_queries = queries.len();

  // Build
  // Since we're in 'examples', we assume Pef is available
  let pef = Pef::new_with_conf(
    data,
    Conf {
      l1_rate: l1,
      l2_rate: l2,
      block_size: blk,
    },
  );

  // Memory
  let size_bytes = pef.memory_usage();
  let size_mb = size_bytes as f64 / 1024.0 / 1024.0;
  let bpe = (size_bytes as f64 * 8.0) / n_elements as f64;
  let compression_ratio = (bpe / 64.0) * 100.0;

  // Get Speed
  let _ = pef.get(queries[0]);
  let start = Instant::now();
  let mut trash = 0;
  for &idx in queries {
    if let Some(v) = pef.get(idx) {
      trash ^= v;
    }
  }
  let duration = start.elapsed();
  std::hint::black_box(trash);
  let get_mops = n_queries as f64 / duration.as_secs_f64() / 1_000_000.0;

  // Iter Speed
  let _ = pef.iter().next();
  let start = Instant::now();
  let mut count = 0;
  for _ in pef.iter() {
    count += 1;
  }
  let duration = start.elapsed();
  std::hint::black_box(count);
  let iter_mops = n_elements as f64 / duration.as_secs_f64() / 1_000_000.0;

  // Score
  let score = get_mops + iter_mops * 0.4 - compression_ratio;

  println!(
    "| {:<6} | {:<6} | {:<6} | {:<8.2} | {:<10.2} | {:<10.2} | {:<10.2} | {:<8.2} |",
    l1, l2, blk, size_mb, compression_ratio, get_mops, iter_mops, score
  );

  history_params.push(params);
  history_scores.push(score);

  if score > *best_score {
    *best_score = score;
    *best_params = params;
  }
}
