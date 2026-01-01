use std::{
  collections::VecDeque,
  path::{Path, PathBuf},
};

use compio::{
  fs::{File, OpenOptions},
  io::AsyncWriteAtExt,
};

use crate::{conf::Conf, error::Result, row::Row};

// Type definitions
// 类型定义

/// WAL file identifier
/// WAL 文件标识符
pub type WalId = u64;

/// Offset within WAL file
/// WAL 文件内的偏移量
pub type WalOffset = u64;

#[derive(Debug, Clone)]
pub struct WalIdOffset {
  pub wal_id: WalId,
  pub offset: WalOffset,
}

/// Replay information after recovery
/// 恢复后的回放信息
#[derive(Debug, Clone)]
pub struct After {
  pub wal_id: WalId,
  pub offset: WalOffset,
  /// All Rotate events that occurred after the Checkpoint
  /// Checkpoint 之后发生的所有 Rotate 事件
  pub rotate_wal_id_li: Vec<WalId>,
}

// Constants
// 常量定义
const DEFAULT_TRUNCATE_THRESHOLD: usize = 65536;
const DEFAULT_KEEP_COUNT: usize = 3;
const CKP_WAL: &str = "ckp.wal";
const CKP_TMP: &str = "ckp.tmp";

pub struct Ckp {
  /// 检查点文件目录
  dir: PathBuf,
  /// 当前打开的文件句柄
  file: File,
  /// 当前文件尾部的物理偏移量 (用于追加写入)
  file_pos: u64,

  /// 累计写入条目数 (用于触发 GC)
  count: usize,
  /// 配置：触发压缩的条目阈值
  truncate_threshold: usize,
  /// 配置：保留的 Save 点数量
  keep: usize,

  /// 核心状态：持有的最近 N 个 Save 点 (包含物理位置信息)
  /// 队尾是最新的，队头是最旧的
  last_li: VecDeque<WalIdOffset>,

  /// 大于N的所有轮转点
  rotate_after_last_n: Vec<WalIdOffset>,
}

impl Ckp {
  /// Open or create checkpoint manager
  /// 打开或创建检查点管理器
  ///
  /// Returns manager instance and replay information
  /// 返回管理器实例和回放信息
  pub async fn open(dir: &Path, conf: &[Conf]) -> Result<(Self, Option<After>)> {
    let dir = dir.join(CKP_WAL);

    // Apply configuration
    // 应用配置
    let mut truncate_threshold = DEFAULT_TRUNCATE_THRESHOLD;
    let mut keep = DEFAULT_KEEP_COUNT;
    for c in conf {
      match c {
        Conf::TruncateAfter(n) => truncate_threshold = *n,
        Conf::Keep(n) => keep = *n,
      }
    }

    // Open file
    // 打开文件
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(&dir)
      .await?;

    let file_len = file.metadata().await?.len();

    // Scan file to build index
    // 扫描文件建立索引
    let mut last_li = VecDeque::new();
    let mut rotate_after_last_n = Vec::new();
    let mut scan_pos = 0u64;
    let mut count = 0;

    while scan_pos < file_len {
      // Try to read DiskRow from file
      let disk_row = match crate::row::DiskRow::try_from_file(&file, scan_pos, file_len).await {
        Ok(Some(row)) => row,
        Ok(None) => break, // Incomplete data / 数据不完整
        Err(_) => break,   // Error reading / 读取错误
      };

      let total_len = disk_row.total_size();
      let item: Row = disk_row.try_into()?;

      let idx = WalIdOffset {
        wal_id: scan_pos,
        offset: total_len,
      };

      match item {
        Row::Save { .. } => {
          last_li.push_back(idx);
          // Keep all during open phase for correct After calculation
          // 如果超过保留数量，弹出最旧的 (但此时还在 open 阶段，
          // 为了能正确计算 After 信息，我们先保留所有，最后再整理内存)
        }
        Row::Rotate { .. } => {
          rotate_after_last_n.push(idx);
        }
      }

      scan_pos += total_len;
      count += 1;
    }

    // Truncate file if there's corrupted data at the end
    // 如果文件末尾有坏数据（scan_pos < file_len），需要截断文件
    if scan_pos < file_len {
      let std_file = std::fs::OpenOptions::new().write(true).open(&dir)?;
      std_file.set_len(scan_pos)?;
    }

    // Trim saved checkpoints to keep
    // 整理内存中的 Save Points，只保留最后 keep 个
    // Important: compact relies on last_li[0] as start point
    // 这一步很重要，因为 compact 依赖于 last_li[0] 作为起点
    while last_li.len() > keep {
      last_li.pop_front();
    }

    // Build replay information (After)
    // 构建回放信息 (After)
    // Rule: use the most recent checkpoint as baseline
    // Find all rotate_after_last_n with wal_id greater than baseline
    // 规则：取 last_li 中最新的一个作为基准
    // 找出所有 rotate_after_last_n 中，wal_id 大于该基准的 wal_id 的条目
    // Rotate is globally ordered. If recent Save happened after Rotate X,
    // replay doesn't need Rotate X. We need Rotates after Last Save.
    // Since we append sequentially, just check physical position:
    // All Rotates after Last Save's physical position are valid.
    // *修正逻辑*：Rotate 是全局顺序的。如果最近的 Save 发生在 Rotate X 之后，
    // 那么回放时不需要关心 Rotate X。
    // 我们需要的是：在 Last Save 之后出现的 Rotate。
    // 由于我们是顺序 Append 的，所以只需看物理位置：
    // 在 Last Save 物理位置之后的所有 Rotate 都是有效的。

    let replay_info = if let Some(last_save) = last_li.back() {
      // Read item from file
      let last_save_item = Self::read_item_from_file(&file, last_save.wal_id).await?;

      let last_save_data = match last_save_item {
        Row::Save { wal_id, offset } => (wal_id, offset),
        _ => return Err(crate::error::Error::Corrupted(last_save.wal_id)),
      };

      // Filter rotate_after_last_n: only keep those after last_save's physical position
      // Or with larger wal_id (more robust)
      // 过滤 Rotate：只保留物理位置在 last_save 之后的
      // 或者 wal_id 更大的 (更稳健)
      let mut after_ids = Vec::new();

      // During scan, rotate_after_last_n contains all rotates in file
      // Return "what needs to be done after last Checkpoint"
      // 在扫描阶段，rotate_after_last_n 包含了文件里所有的 rotate
      // 我们只需要把那些在 last_li 能够覆盖范围之前的清理掉吗？
      // 不，open 返回的是"从最后一次 Checkpoint 之后需要做的事"。

      for r in &rotate_after_last_n {
        if r.wal_id > last_save.wal_id {
          let item = Self::read_item_from_file(&file, r.wal_id).await?;
          if let Row::Rotate { wal_id } = item {
            after_ids.push(wal_id);
          }
        }
      }

      Some(After {
        wal_id: last_save_data.0,
        offset: last_save_data.1,
        rotate_wal_id_li: after_ids,
      })
    } else {
      None
    };

    // Clean up expired rotates from memory
    // Rule: all rotates with wal_id <= min kept Save ID can be removed
    // (they're still on disk until next compact)
    // 清理内存中过期的 Rotate
    // 规则：所有 wal_id 小于等于 最小持有的 Save ID 的 rotate 都可以从内存移除
    // (虽然它们还在磁盘上，直到下次 Compact)
    // 更新 rotate_after_last_n，确保只保留大于 last_li[0] 的 wal_id
    let min_id = if let Some(first) = last_li.front() {
      let item = Self::read_item_from_file(&file, first.wal_id).await?;
      match item {
        Row::Save { wal_id, .. } => wal_id,
        _ => 0,
      }
    } else {
      0
    };

    rotate_after_last_n = Self::filter_rotates(&rotate_after_last_n, min_id);

    let ckp = Self {
      dir,
      file,
      file_pos: scan_pos,
      count,
      truncate_threshold,
      keep,
      last_li,
      rotate_after_last_n,
    };

    Ok((ckp, replay_info))
  }

  /// Write checkpoint
  /// 写入检查点
  pub async fn set(&mut self, wal_id: WalId, offset: WalOffset) -> Result<()> {
    let item = Row::Save { wal_id, offset };
    let idx = self.append_item(item).await?;

    // Update in-memory state
    // 更新内存状态
    self.last_li.push_back(idx);
    if self.last_li.len() > self.keep {
      self.last_li.pop_front();
    }

    // Check if compaction is needed
    // 检查是否需要压缩
    if self.count >= self.truncate_threshold {
      self.compact().await?;
    }

    Ok(())
  }

  /// Write rotate record
  /// 写入 Rotate 记录
  pub async fn rotate(&mut self, wal_id: WalId) -> Result<()> {
    let item = Row::Rotate { wal_id };
    let idx = self.append_item(item).await?;

    self.rotate_after_last_n.push(idx);

    Ok(())
  }

  /// Get last saved position
  /// 获取最后保存的位置
  pub async fn wal_id_offset(&self) -> Option<(WalId, WalOffset)> {
    if let Some(idx) = self.last_li.back() {
      match Self::read_item_from_file(&self.file, idx.wal_id).await {
        Ok(Row::Save { wal_id, offset }) => Some((wal_id, offset)),
        _ => None,
      }
    } else {
      None
    }
  }

  // ==========================================
  // Private helper methods
  // 私有辅助方法
  // ==========================================

  /// Filter rotates to keep only those with wal_id > min_id
  /// 过滤旋转条目，只保留 wal_id > min_id 的条目
  fn filter_rotates(rotates: &[WalIdOffset], min_id: WalId) -> Vec<WalIdOffset> {
    rotates
      .iter()
      .filter(|r| r.wal_id > min_id)
      .cloned()
      .collect()
  }

  /// Read entry from file at given position
  /// 从文件的指定位置读取条目
  async fn read_item_from_file(file: &File, pos: u64) -> Result<Row> {
    let disk_row = crate::row::DiskRow::from_file(file, pos).await?;
    disk_row.try_into()
  }

  /// Low-level append write
  /// 底层追加写入
  async fn append_item(&mut self, item: Row) -> Result<WalIdOffset> {
    let disk_row = crate::row::DiskRow::from(item);
    let data = disk_row.to_bytes();
    let total_len = disk_row.total_size();

    self.file.write_all_at(data, self.file_pos).await.0?;

    // Flush to disk
    // 刷盘
    self.file.sync_all().await?;

    let idx = WalIdOffset {
      wal_id: self.file_pos,
      offset: total_len,
    };

    self.file_pos += total_len;
    self.count += 1;

    Ok(idx)
  }

  /// Core compaction logic: copy based on physical offset
  /// 核心压缩逻辑：基于物理 offset 的对拷
  async fn compact(&mut self) -> Result<()> {
    // 1. Filter and Sort Entries
    // 过滤并排序条目

    // Calculate min_id from last_li[0] (if exists)
    let min_id = if let Some(first) = self.last_li.front() {
      let item = Self::read_item_from_file(&self.file, first.wal_id).await?;
      match item {
        Row::Save { wal_id, .. } => wal_id,
        _ => 0,
      }
    } else {
      0
    };

    // Filter rotates: keep only those > min_id
    self.rotate_after_last_n = Self::filter_rotates(&self.rotate_after_last_n, min_id);

    // Collect all entries to write
    let mut all_entries: Vec<&mut WalIdOffset> = self
      .last_li
      .iter_mut()
      .chain(self.rotate_after_last_n.iter_mut())
      .collect();

    // Sort by old wal_id to preserve history order
    all_entries.sort_by_key(|e| e.wal_id);

    // 2. Prepare temporary file
    // 准备临时文件
    // 临时文件用 dir.join(ckp.tmp) -> checkpoint.wal.tmp
    let tmp_dir = self.dir.with_file_name(CKP_TMP);

    let mut tmp_file = OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(&tmp_dir)
      .await?;

    // 3. Write entries to new file and update offsets
    // 写入新文件并更新内存中的偏移量
    let mut cursor = 0u64;
    for item in all_entries {
      let row = Self::read_item_from_file(&self.file, item.wal_id).await?;
      let disk_row = crate::row::DiskRow::from(row);
      let data = disk_row.to_bytes();
      let total = disk_row.total_size();

      tmp_file.write_all_at(data, cursor).await.0?;

      // Update in-memory state directly (held via mutable reference)
      item.wal_id = cursor;
      item.offset = total;

      cursor += total;
    }

    // 4. Flush and Close Temp File
    // 确保临时文件落盘
    tmp_file.sync_all().await?;
    drop(tmp_file); // Explicit close

    // 5. Unsafe Swap (Close old handle before rename)
    // 强制关闭当前文件句柄，以便 Windows 上可以重命名
    // 使用 unsafe 将 file 读出并 drop，暂时留下未初始化内存，必须在函数返回前写回
    let old_file = unsafe { std::ptr::read(&self.file) };
    drop(old_file);

    // Rename
    compio::fs::rename(&tmp_dir, &self.dir).await?;

    // 6. Reopen file
    // 重新打开文件
    let new_file = OpenOptions::new()
      .read(true)
      .write(true) // Open with write lock/access
      .create(true)
      .open(&self.dir)
      .await?;

    // Write back to struct (restore validity)
    unsafe { std::ptr::write(&mut self.file, new_file) };

    // Update Ckp state
    // 更新 Ckp 状态
    self.file_pos = cursor;
    self.count = 0; // Reset counter / 重置计数器

    Ok(())
  }
}
