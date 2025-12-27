//! GC (Garbage Collection) / 垃圾回收

use std::collections::HashMap;
use std::fs::{self, File};
use std::future::Future;
use std::path::PathBuf;

use coarsetime::Clock;
use fd_lock::RwLock;

use crate::{Loc, Result, Wal};

/// GC log file name / GC 日志文件名
const GC_LOG: &str = "gc.log";
/// Lock file extension / 锁文件扩展名
const LOCK_EXT: &str = ".lock";
/// Default reclaim threshold / 默认回收阈值
const DEFAULT_THRESHOLD: f64 = 0.25;
/// Max iterations per gc_auto call / 每次 gc_auto 最大迭代次数
const MAX_ITER: usize = 16;

/// Check if key is deleted / 检查键是否已删除
pub trait Gc {
  fn is_rm(&self, key: &[u8]) -> impl Future<Output = bool> + Send;
}

/// GC state / GC 状态
pub struct GcState {
  dir: PathBuf,
  /// WAL ID -> last GC timestamp / WAL ID -> 上次 GC 时间戳
  log: HashMap<u64, u64>,
  /// Reclaim threshold (0.0-1.0) / 回收阈值
  threshold: f64,
}

impl GcState {
  /// Create GC state / 创建 GC 状态
  pub fn new(dir: impl Into<PathBuf>) -> Self {
    let dir = dir.into();
    let log = Self::load_log(&dir);
    Self {
      dir,
      log,
      threshold: DEFAULT_THRESHOLD,
    }
  }

  /// Set reclaim threshold / 设置回收阈值
  pub fn set_threshold(&mut self, t: f64) {
    self.threshold = t.clamp(0.0, 1.0);
  }

  /// Load GC log from file / 从文件加载 GC 日志
  fn load_log(dir: &PathBuf) -> HashMap<u64, u64> {
    let path = dir.join(GC_LOG);
    let mut map = HashMap::new();
    if let Ok(content) = fs::read_to_string(&path) {
      for line in content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() == 2 {
          if let (Ok(id), Ok(ts)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>()) {
            map.insert(id, ts);
          }
        }
      }
    }
    map
  }

  /// Save GC log to file / 保存 GC 日志到文件
  fn save_log(&self) {
    let path = self.dir.join(GC_LOG);
    let content: String = self.log.iter().map(|(id, ts)| format!("{id} {ts}\n")).collect();
    let _ = fs::write(&path, content);
  }

  /// Record GC time / 记录 GC 时间
  fn record(&mut self, id: u64) {
    let ts = Clock::now_since_epoch().as_secs();
    self.log.insert(id, ts);
    self.save_log();
  }

  /// Remove from log / 从日志移除
  fn remove(&mut self, id: u64) {
    self.log.remove(&id);
    self.save_log();
  }

  /// Get oldest unGC'd WAL ID / 获取最久未 GC 的 WAL ID
  fn oldest(&self, ids: &[u64], cur_id: u64) -> Option<u64> {
    let mut candidates: Vec<_> = ids
      .iter()
      .filter(|&&id| id != cur_id)
      .map(|&id| (id, self.log.get(&id).copied().unwrap_or(0)))
      .collect();

    if candidates.is_empty() {
      return None;
    }

    // Sort by last GC time (oldest first) / 按上次 GC 时间排序（最旧优先）
    candidates.sort_by_key(|&(_, ts)| ts);

    // Random pick from oldest 25% / 从最旧的 25% 中随机选
    let n = (candidates.len() / 4).max(1);
    let idx = fastrand::usize(..n);
    Some(candidates[idx].0)
  }

  /// Get lock file path / 获取锁文件路径
  fn lock_path(&self, id: u64) -> PathBuf {
    self.dir.join(format!("{id}{LOCK_EXT}"))
  }

  /// Try acquire exclusive lock / 尝试获取排他锁
  fn try_lock(&self, id: u64) -> Option<RwLock<File>> {
    let path = self.lock_path(id);
    let file = File::create(&path).ok()?;
    let mut lock = RwLock::new(file);
    lock.try_write().ok()?;
    Some(lock)
  }
}

impl Wal {
  /// GC a single WAL file / 对单个 WAL 文件进行 GC
  ///
  /// Returns (old_loc, new_loc) mapping and reclaim ratio
  /// 返回新旧位置映射和回收率
  pub async fn gc<T: Gc>(
    &mut self,
    id: u64,
    checker: &T,
    state: &mut GcState,
  ) -> Result<(Vec<(Loc, Loc)>, f64)> {
    if id == self.cur_id() {
      return Err(crate::Error::CannotRemoveCurrent);
    }

    // Try acquire lock / 尝试获取锁
    let _lock = state.try_lock(id).ok_or(crate::Error::Locked)?;

    let mut mapping = Vec::new();
    let mut entries = Vec::new();
    let mut reclaimed = 0usize;

    // Scan and collect entries / 扫描并收集条目
    self
      .scan(id, |pos, head| {
        entries.push((pos, *head));
        true
      })
      .await?;

    let total = entries.len();

    for (pos, head) in entries {
      let old_loc = Loc::new(id, pos);
      let key = self.get_key(&head).await?;

      if checker.is_rm(&key).await {
        reclaimed += 1;
        continue;
      }

      let val = self.get_val(&head).await?;
      let new_loc = self.put(&key, &val).await?;
      mapping.push((old_loc, new_loc));
    }

    // Record GC time / 记录 GC 时间
    state.record(id);

    let ratio = if total > 0 {
      reclaimed as f64 / total as f64
    } else {
      0.0
    };

    Ok((mapping, ratio))
  }

  /// Auto GC with Redis-like strategy / 类 Redis 策略自动 GC
  ///
  /// Randomly pick oldest unGC'd file, continue if reclaim ratio > threshold
  /// 随机选最久未 GC 的文件，回收率超阈值继续
  pub async fn gc_auto<T: Gc>(
    &mut self,
    checker: &T,
    state: &mut GcState,
  ) -> Result<Vec<(Loc, Loc)>> {
    let mut all_mapping = Vec::new();
    let cur_id = self.cur_id();

    for _ in 0..MAX_ITER {
      let ids: Vec<u64> = self.iter().collect();
      let Some(id) = state.oldest(&ids, cur_id) else {
        break;
      };

      let (mapping, ratio) = match self.gc(id, checker, state).await {
        Ok(r) => r,
        Err(crate::Error::Locked) => continue,
        Err(e) => return Err(e),
      };

      if !mapping.is_empty() {
        all_mapping.extend(mapping);
      }

      // Remove old WAL file / 删除旧 WAL 文件
      self.remove(id)?;
      state.remove(id);

      // Clean up lock file / 清理锁文件
      let _ = fs::remove_file(state.lock_path(id));

      // Stop if reclaim ratio below threshold / 回收率低于阈值则停止
      if ratio < state.threshold {
        break;
      }
    }

    Ok(all_mapping)
  }
}
