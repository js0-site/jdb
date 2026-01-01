//! Checkpoint module
//! 检查点模块
//!
//! Records two types of events:
//! 记录两种事件：
//! - Save: checkpoint position (id, offset)
//!   Save：检查点位置 (id, offset)
//! - Rotate: WAL file rotation (id)
//!   Rotate：WAL 文件轮转 (id)

pub(crate) mod entry;
pub mod log;

use std::path::{Path, PathBuf};

use self::log::{Iter, Log};
use crate::Result;

/// Checkpoint file name
/// 检查点文件名
pub const CKP_FILE: &str = "ckp.wlog";

/// Truncate threshold
/// 压缩阈值
const TRUNCATE_THRESHOLD: usize = 65536;

/// Keep last N saves
/// 保留最后 N 个 save
const KEEP_SAVES: usize = 3;

/// WAL pointer
/// WAL 指针
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalPtr {
  pub id: u64,
  pub offset: u64,
}

impl WalPtr {
  #[inline]
  pub fn new(id: u64, offset: u64) -> Self {
    Self { id, offset }
  }
}

/// Checkpoint entry kind
/// 检查点条目类型
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CkpKind {
  /// Checkpoint save (id, offset)
  /// 检查点保存
  Save = 0,
  /// WAL rotation (id)
  /// WAL 轮转
  Rotate = 1,
}

impl CkpKind {
  #[inline]
  fn from_u8(v: u8) -> Option<Self> {
    match v {
      0 => Some(Self::Save),
      1 => Some(Self::Rotate),
      _ => None,
    }
  }
}

/// Save entry size: kind(1) + id(8) + offset(8) = 17
const SAVE_SIZE: usize = 17;

/// Rotate entry size: kind(1) + id(8) = 9
const ROTATE_SIZE: usize = 9;

/// Checkpoint manager
/// 检查点管理器
pub struct Ckp {
  path: PathBuf,
  log: Log,
  /// Last save position
  /// 最后保存位置
  last_save: Option<WalPtr>,
  /// Write count since last truncate
  /// 上次压缩后的写入次数
  write_count: usize,
}

impl Ckp {
  /// Open checkpoint file
  /// 打开检查点文件
  pub async fn open(dir: &Path) -> Result<Self> {
    let path = dir.join(CKP_FILE);
    let mut log = Log::new(&path);
    log.open().await?;

    let mut ckp = Self {
      path,
      log,
      last_save: None,
      write_count: 0,
    };

    // Load last save and auto truncate
    // 加载最后的保存点并自动清理
    ckp.truncate().await?;

    Ok(ckp)
  }

  /// Parse entry bytes
  /// 解析条目字节
  #[inline]
  fn parse_entry(data: &[u8]) -> Option<(CkpKind, WalPtr)> {
    let kind = CkpKind::from_u8(*data.first()?)?;
    match kind {
      CkpKind::Save if data.len() >= SAVE_SIZE => {
        // Safety: length checked above
        // 安全：上面已检查长度
        let id = u64::from_le_bytes(unsafe { *(data.as_ptr().add(1) as *const [u8; 8]) });
        let offset = u64::from_le_bytes(unsafe { *(data.as_ptr().add(9) as *const [u8; 8]) });
        Some((kind, WalPtr::new(id, offset)))
      }
      CkpKind::Rotate if data.len() >= ROTATE_SIZE => {
        let id = u64::from_le_bytes(unsafe { *(data.as_ptr().add(1) as *const [u8; 8]) });
        Some((kind, WalPtr::new(id, 0)))
      }
      _ => None,
    }
  }

  /// Save checkpoint
  /// 保存检查点
  pub async fn save(&mut self, id: u64, offset: u64) -> Result<()> {
    let mut buf = [0u8; SAVE_SIZE];
    buf[0] = CkpKind::Save as u8;
    buf[1..9].copy_from_slice(&id.to_le_bytes());
    buf[9..17].copy_from_slice(&offset.to_le_bytes());

    self.log.append(&buf).await?;
    self.log.sync().await?;
    self.last_save = Some(WalPtr::new(id, offset));

    self.write_count += 1;
    if self.write_count >= TRUNCATE_THRESHOLD {
      self.truncate().await?;
    }
    Ok(())
  }

  /// Record WAL rotation
  /// 记录 WAL 轮转
  pub async fn rotate(&mut self, id: u64) -> Result<()> {
    let mut buf = [0u8; ROTATE_SIZE];
    buf[0] = CkpKind::Rotate as u8;
    buf[1..9].copy_from_slice(&id.to_le_bytes());

    self.log.append(&buf).await?;
    self.log.sync().await?;

    self.write_count += 1;
    if self.write_count >= TRUNCATE_THRESHOLD {
      self.truncate().await?;
    }
    Ok(())
  }

  /// Truncate log, keep last N saves and entries after
  /// 压缩日志，保留最后 N 个 save 及之后的条目
  async fn truncate(&mut self) -> Result<()> {
    // Single pass: find last save and start offset
    // 单次遍历：找到最后的 save 和起始偏移
    let buf = self.log.read_all().await?;
    let mut save_offsets: [usize; KEEP_SAVES] = [0; KEEP_SAVES];
    let mut save_count = 0usize;
    let mut last_save = None;

    for (offset, data) in Iter::new(&buf) {
      if let Some((kind, ptr)) = Self::parse_entry(data)
        && kind == CkpKind::Save
      {
        save_offsets[save_count % KEEP_SAVES] = offset;
        save_count += 1;
        last_save = Some(ptr);
      }
    }

    self.last_save = last_save;

    if save_count <= KEEP_SAVES {
      self.write_count = 0;
      return Ok(());
    }

    // Start offset = oldest save in ring buffer
    // 起始偏移 = 环形缓冲中最旧的 save
    let start_offset = save_offsets[save_count % KEEP_SAVES];

    // Copy [start_offset..] to tmp file
    // 复制 [start_offset..] 到临时文件
    let tmp_path = self.path.with_extension("tmp");

    compio_fs::write(&tmp_path, Vec::from(&buf[start_offset..]))
      .await
      .0?;

    // Atomic rename
    // 原子重命名
    compio_fs::rename(&tmp_path, &self.path).await?;

    // Reopen
    // 重新打开
    let mut log = Log::new(&self.path);
    log.open().await?;
    self.log = log;
    self.write_count = 0;

    Ok(())
  }

  /// Get last save position
  /// 获取最后保存位置
  #[inline]
  pub fn last_save(&self) -> Option<WalPtr> {
    self.last_save
  }

  /// Get last save id for GC boundary
  /// 获取最后保存的 id 用于 GC 边界
  #[inline]
  pub fn last_save_id(&self) -> Option<u64> {
    self.last_save.map(|p| p.id)
  }

  /// Load replay info: (save_ptr, all file ids to replay)
  /// 加载回放信息：(保存点, 需要回放的所有文件 ID)
  ///
  /// Returns save_ptr and file ids: [save.id, rotate1, rotate2, ...]
  /// 返回保存点和文件 ID 列表：[save.id, rotate1, rotate2, ...]
  pub async fn load_replay(&self) -> Result<Option<(WalPtr, Vec<u64>)>> {
    let Some(save_ptr) = self.last_save else {
      return Ok(None);
    };

    let buf = self.log.read_all().await?;
    let mut found_save = false;
    let mut file_ids = Vec::new();

    for (_, data) in Iter::new(&buf) {
      if let Some((kind, ptr)) = Self::parse_entry(data) {
        match kind {
          CkpKind::Save if ptr == save_ptr => {
            found_save = true;
            file_ids.clear();
            file_ids.push(save_ptr.id);
          }
          CkpKind::Rotate if found_save => {
            file_ids.push(ptr.id);
          }
          _ => {}
        }
      }
    }

    Ok(Some((save_ptr, file_ids)))
  }
}
