use std::{collections::HashSet, time::Instant};

use jdb_pef::{Pef, conf::Conf};
use perpetual::{Matrix, PerpetualBooster, objective_functions::Objective};
use tabled::{Table, Tabled, settings::Style};

// jdb_pef Hyperparameter Search using PerpetualBooster as a surrogate model
// Optimizes: L1 Rate, L2 Rate, Block Size

#[derive(Tabled)]
struct Record {
  #[tabled(rename = "L1")]
  l1: usize,
  #[tabled(rename = "L2")]
  l2: usize,
  #[tabled(rename = "Block")]
  block: usize,
  #[tabled(rename = "Size(MB)")]
  size_mb: f64,
  #[tabled(rename = "Ratio(%)")]
  ratio: f64,
  #[tabled(rename = "Get(M/s)")]
  get_mops: f64,
  #[tabled(rename = "Iter(M/s)")]
  iter_mops: f64,
  #[tabled(rename = "Score")]
  score: f64,
}

fn main() {
  println!("=== jdb_pef Hyperparameter Search (Perpetual GBM) ===");

  // Configuration
  let n_elements = 200_000;
  let iterations = 300;

  // Search Ranges
  let l1_range = 1..8192;
  let l2_range = 4..256;
  let block_size_range = 32..8192;

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
  let mut records = Vec::new();

  let mut best_score = f64::NEG_INFINITY;
  let mut best_params = (0, 0, 0);

  // Initial Random Samples (Warmup)
  let init_samples = 5;
  for _ in 0..init_samples {
    // Find valid random candidate
    // 查找有效的随机候选
    let candidate = loop {
      let l1 = rng.usize(l1_range.clone());
      let l2 = rng.usize(l2_range.clone());
      let blk = rng.usize(block_size_range.clone());
      if l2 < l1 {
        break (l1, l2, blk);
      }
    };

    if !tested_set.contains(&candidate)
      && let Some(record) = evaluate(
        candidate,
        &data,
        &queries,
        &mut history_params,
        &mut history_scores,
        &mut tested_set,
        &mut best_score,
        &mut best_params,
      )
    {
      records.push(record);
      print_table(&records);
    }
  }

  // Optimization Loop
  for _ in 0..(iterations - init_samples) {
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
    model.cfg.objective = Objective::SquaredLoss;

    // fit(data, y, sample_weight, group)
    model.fit(&train_mat, &targets, None, None).unwrap();

    // 3. Generate Random Candidates and Predict
    let pool_size = 5000;

    let mut candidate_features = Vec::with_capacity(pool_size * 3);
    let mut candidates = Vec::with_capacity(pool_size);

    for _ in 0..pool_size {
      let l1 = rng.usize(l1_range.clone());
      let l2 = rng.usize(l2_range.clone());
      let blk = rng.usize(block_size_range.clone());

      let cand = (l1, l2, blk);
      if l2 < l1 && !tested_set.contains(&cand) {
        candidates.push(cand);
        candidate_features.push(l1 as f64);
        candidate_features.push(l2 as f64);
        candidate_features.push(blk as f64);
      }
    }

    if candidates.is_empty() {
      break;
    }

    let pred_mat = Matrix::new(&candidate_features, candidates.len(), 3);
    let preds = model.predict(&pred_mat, false); // false for single thread

    let mut best_candidate = None;
    let mut best_pred = f64::NEG_INFINITY;

    for (i, &pred) in preds.iter().enumerate() {
      if pred > best_pred {
        best_pred = pred;
        best_candidate = Some(candidates[i]);
      }
    }

    if let Some(cand) = best_candidate {
      if let Some(record) = evaluate(
        cand,
        &data,
        &queries,
        &mut history_params,
        &mut history_scores,
        &mut tested_set,
        &mut best_score,
        &mut best_params,
      ) {
        records.push(record);
        // Only print if this record is a new best (or close to it)
        // For now, let's print if it's the current best
        if history_scores.last().copied().unwrap() >= best_score {
          print_table(&records);
        }
      }
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

fn print_table(records: &[Record]) {
  if let Some(last) = records.last() {
    let table = Table::new(vec![last]).with(Style::empty()).to_string();
    // Remove header (first line) if we only want the row
    // But Table::new usually includes headers.
    // For a one-line progress log, we might just want to print the table as is,
    // but maybe suppress header after the first time?
    // For simplicity, let's just print the whole table for the single record,
    // which includes headers. It's verbose but clear.
    println!("{}", table);
  }
}

#[allow(clippy::too_many_arguments)]
fn evaluate(
  params: (usize, usize, usize),
  data: &[u64],
  queries: &[usize],
  history_params: &mut Vec<(usize, usize, usize)>,
  history_scores: &mut Vec<f64>,
  tested_set: &mut HashSet<(usize, usize, usize)>,
  best_score: &mut f64,
  best_params: &mut (usize, usize, usize),
) -> Option<Record> {
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

  // Score: higher weight on compression ratio for better space efficiency
  let score = get_mops + iter_mops * 0.4 - compression_ratio * 2.0;

  history_params.push(params);
  history_scores.push(score);

  if score > *best_score {
    *best_score = score;
    *best_params = params;
  }

  Some(Record {
    l1,
    l2,
    block: blk,
    size_mb: (size_mb * 100.0).round() / 100.0,
    ratio: (compression_ratio * 100.0).round() / 100.0,
    get_mops: (get_mops * 100.0).round() / 100.0,
    iter_mops: (iter_mops * 100.0).round() / 100.0,
    score: (score * 100.0).round() / 100.0,
  })
}
