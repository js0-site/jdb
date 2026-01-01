//! Checkpoint for WAL recovery
//! WAL 恢复检查点
//!
//! Checkpoint stores the current WAL position for crash recovery.
//! 检查点存储当前 WAL 位置用于崩溃恢复。

use std::path::Path;

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{Error, Result};

/// Checkpoint magic number / 检查点魔数
const CHECKPOINT_MAGIC: u64 = 0x4A44425F43484B50; // "JDB_CHKP"

/// Checkpoint version / 检查点版本
const CHECKPOINT_VERSION: u32 = 1;

/// Checkpoint state for WAL recovery
/// WAL 恢复检查点状态
///
/// Layout (32 bytes):
/// - magic: 8 bytes (0x4A44425F43484B50 = "JDB_CHKP")
/// - version: 4 bytes
/// - _pad: 4 bytes
/// - wal_id: 8 bytes (current WAL file ID)
/// - wal_pos: 8 bytes (position in WAL)
#[repr(C)]
#[derive(
  Debug, Clone, Copy, Default, FromBytes, IntoBytes, Immutable, KnownLayout, PartialEq, Eq,
)]
pub struct Checkpoint {
  magic: u64,
  version: u32,
  _pad: u32,
  /// Current WAL file ID / 当前 WAL 文件 ID
  pub wal_id: u64,
  /// Position in WAL / WAL 中的位置
  pub wal_pos: u64,
}

impl Checkpoint {
  pub const SIZE: usize = 32;

  /// Create new checkpoint / 创建新检查点
  #[inline]
  pub fn new(wal_id: u64, wal_pos: u64) -> Self {
    Self {
      magic: CHECKPOINT_MAGIC,
      version: CHECKPOINT_VERSION,
      _pad: 0,
      wal_id,
      wal_pos,
    }
  }

  /// Validate checkpoint / 验证检查点
  #[inline]
  pub fn is_valid(&self) -> bool {
    self.magic == CHECKPOINT_MAGIC && self.version == CHECKPOINT_VERSION
  }

  /// Load checkpoint from file / 从文件加载检查点
  pub async fn load(path: &Path) -> Result<Option<Self>> {
    if !path.exists() {
      return Ok(None);
    }

    let file = compio_fs::OpenOptions::new().read(true).open(path).await?;
    let meta = file.metadata().await?;

    if meta.len() < Self::SIZE as u64 {
      return Err(Error::CheckpointCorrupt {
        path: path.to_path_buf(),
      });
    }

    let buf = vec![0u8; Self::SIZE];
    let slice = buf.slice(0..Self::SIZE);
    let res = file.read_exact_at(slice, 0).await;
    res.0?;
    let buf = res.1.into_inner();

    let checkpoint = Self::read_from_bytes(&buf).map_err(|_| Error::CheckpointCorrupt {
      path: path.to_path_buf(),
    })?;

    if !checkpoint.is_valid() {
      return Err(Error::CheckpointCorrupt {
        path: path.to_path_buf(),
      });
    }

    Ok(Some(checkpoint))
  }

  /// Save checkpoint to file (atomic via temp file + rename)
  /// 保存检查点到文件（通过临时文件+重命名实现原子性）
  pub async fn save(&self, path: &Path) -> Result<()> {
    let tmp_path = path.with_extension("tmp");

    // Write to temp file / 写入临时文件
    let mut file = compio_fs::OpenOptions::new()
      .write(true)
      .create(true)
      .truncate(true)
      .open(&tmp_path)
      .await?;

    let buf = self.as_bytes().to_vec();
    let slice = buf.slice(0..Self::SIZE);
    let res = file.write_all_at(slice, 0).await;
    res.0?;
    file.sync_all().await?;
    drop(file);

    // Atomic rename / 原子重命名
    compio_fs::rename(&tmp_path, path).await?;

    Ok(())
  }
}

const _: () = assert!(size_of::<Checkpoint>() == 32);
