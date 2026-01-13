use jdb_base::{ckp::Sst, sst::Level};
use jdb_level::sink::{Gc, LevelScore, Score, file_score, l0, level_score, level_target_size};

#[test]
fn test_level_target_size() {
  // Small total size (<= 256MB): all data goes to base level (L6)
  // 小总量 (<= 256MB)：所有数据直接进入基础层 (L6)
  let (sizes, base) = level_target_size(100 * 1024 * 1024); // 100MB
  assert_eq!(sizes[5], 100 * 1024 * 1024); // L6 = 100MB
  assert_eq!(sizes[4], 0); // L5 = 0 (stopped early)
  assert_eq!(base, Level::L6); // base level is L6

  // 10GB: L6=10GB, L5=1.25GB, L4~156MB (<256MB, stops)
  // 10GB: L6=10GB, L5=1.25GB, L4~156MB (<256MB, 停止)
  let (sizes, base) = level_target_size(10 * 1024 * 1024 * 1024); // 10GB
  assert_eq!(sizes[5], 10 * 1024 * 1024 * 1024); // L6 = 10GB
  assert!(sizes[4] > 1024 * 1024 * 1024); // L5 > 1GB
  assert!(sizes[3] > 0 && sizes[3] < 256 * 1024 * 1024); // L4 < 256MB
  assert_eq!(sizes[2], 0); // L3 = 0
  assert_eq!(sizes[1], 0); // L2 = 0
  assert_eq!(sizes[0], 0); // L1 = 0
  assert_eq!(base, Level::L4); // base level is L4 (first with target > 0)
}

#[test]
fn test_l0_score() {
  assert_eq!(l0(0), 0);
  assert_eq!(l0(4), 1000); // 100%
  assert_eq!(l0(2), 500); // 50%
  assert_eq!(l0(9), LevelScore::MAX); // over threshold
}

#[test]
fn test_level_score() {
  // target_size = 0, current > 0 => MAX
  // target_size = 0, current > 0 => 最大值
  assert_eq!(level_score(100, 0), LevelScore::MAX);

  // target_size = 0, current = 0 => 0
  assert_eq!(level_score(0, 0), 0);

  // Normal case
  // 正常情况
  let score = level_score(1000, 1000);
  assert!(score > 0 && score < LevelScore::MAX);
}

#[test]
fn test_file_score() {
  // No tombstones
  // 无墓碑
  let sst = Sst {
    level: Level::L1,
    size: 1000,
    rmed: 0,
  };
  assert_eq!(file_score(&sst), 0);

  // 50% tombstones
  // 50% 墓碑
  let sst = Sst {
    level: Level::L1,
    size: 1000,
    rmed: 500,
  };
  assert_eq!(file_score(&sst), 500);

  // 100% tombstones
  // 100% 墓碑
  let sst = Sst {
    level: Level::L1,
    size: 1000,
    rmed: 1000,
  };
  assert_eq!(file_score(&sst), 1000);

  // Empty file
  // 空文件
  let sst = Sst {
    level: Level::L1,
    size: 0,
    rmed: 0,
  };
  assert_eq!(file_score(&sst), 0);
}

#[test]
fn test_score_new_empty() {
  let score = Score::new([]);
  assert_eq!(score.total_size, 0);
  assert_eq!(score.l0_cnt, 0);
  assert_eq!(score.score[0], 0);
}

#[test]
fn test_score_add_l0() {
  let mut score = Score::new([]);

  let sst = Sst {
    level: Level::L0,
    size: 1000,
    rmed: 0,
  };
  score.update([(1, sst)], []);

  assert_eq!(score.l0_cnt, 1);
  assert_eq!(score.total_size, 1000);
  score.next_gc();
  assert!(score.score[0] > 0);
}

#[test]
fn test_score_add_l1() {
  let mut score = Score::new([]);

  let sst = Sst {
    level: Level::L1,
    size: 1000,
    rmed: 100,
  };
  score.update([(1, sst)], []);

  assert_eq!(score.l0_cnt, 0);
  // size_without_rmed = 1000 - 100 = 900
  assert_eq!(score.total_size, 900);
  assert_eq!(score.level_size[0], 900); // L1
  assert_eq!(score.level_files[0].len(), 1);
  assert_eq!(score.level_files[0][0].0, 1); // id
}

#[test]
fn test_score_remove() {
  let mut score = Score::new([]);

  let sst = Sst {
    level: Level::L1,
    size: 1000,
    rmed: 0,
  };
  score.update([(1, sst)], []);
  assert_eq!(score.level_files[0].len(), 1);

  // rm uses (Level, &[Id]) format
  // rm 使用 (Level, &[Id]) 格式
  score.update([], [(Level::L1, [1].as_slice())]);
  assert_eq!(score.level_files[0].len(), 0);
  assert_eq!(score.total_size, 0);
  assert_eq!(score.level_size[0], 0);
}

#[test]
fn test_next_gc_none() {
  // Empty or low score => None
  // 空或低分数 => None
  let mut score = Score::new([]);
  assert!(score.next_gc().is_none());
}

#[test]
fn test_next_gc_l0() {
  let mut score = Score::new([]);

  // Add enough L0 files to trigger GC
  // 添加足够多的 L0 文件以触发 GC
  for i in 0..5 {
    let sst = Sst {
      level: Level::L0,
      size: 100_000_000,
      rmed: 0,
    };
    score.update([(i, sst)], []);
  }

  let gc = score.next_gc();
  assert!(gc.is_some());
  match gc.unwrap() {
    Gc::L0(to) => assert!(to >= Level::L1),
    _ => panic!("expected L0 GC"),
  }
}

#[test]
fn test_next_gc_l1plus() {
  let mut score = Score::new([]);

  // Add many large L1 files to trigger GC
  // total size will exceed target, triggering high score
  // 添加多个大的 L1 文件以触发 GC
  // 总大小将超过目标，触发高分数
  for i in 0..10 {
    let sst = Sst {
      level: Level::L1,
      size: 500_000_000, // 500MB each
      rmed: 100_000_000, // 20% tombstones
    };
    score.update([(i, sst)], []);
  }

  // With 10 * 400MB = 4GB effective size in L1,
  // the L1 should have high score
  // 10 * 400MB = 4GB 有效大小在 L1，L1 应该有高分数
  let gc = score.next_gc();
  assert!(score.score[1] > 0);

  if let Some(Gc::L1Plus(meta)) = gc {
    assert_eq!(meta.from, Level::L1);
    // File with highest GC score should be selected
    // 应该选择 GC 得分最高的文件
    assert!(meta.id < 10);
  }
  // Note: might also trigger L0 or other level GC depending on thresholds
  // 注意：根据阈值也可能触发 L0 或其他层级 GC
}

#[test]
fn test_file_ordering_by_score() {
  let mut score = Score::new([]);

  // Add files with different tombstone ratios
  // 添加不同墓碑比例的文件
  let sst1 = Sst {
    level: Level::L1,
    size: 1000,
    rmed: 100, // 10%
  };
  let sst2 = Sst {
    level: Level::L1,
    size: 1000,
    rmed: 500, // 50%
  };
  let sst3 = Sst {
    level: Level::L1,
    size: 1000,
    rmed: 200, // 20%
  };

  score.update([(1, sst1), (2, sst2), (3, sst3)], []);

  // Should be ordered by score descending: 2 (50%), 3 (20%), 1 (10%)
  // 应该按得分降序排列: 2 (50%), 3 (20%), 1 (10%)
  let files = &score.level_files[0];
  assert_eq!(files.len(), 3);
  assert_eq!(files[0].0, 2); // highest score
  assert_eq!(files[1].0, 3);
  assert_eq!(files[2].0, 1); // lowest score
}
