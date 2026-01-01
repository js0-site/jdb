use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("invalid WAL header / 无效的 WAL 文件头")]
  InvalidHeader,

  #[error("invalid magic / 无效的魔数")]
  InvalidMagic,

  #[error("data length {0} exceeds limit {1} / 数据长度 {0} 超过限制 {1}")]
  DataTooLong(usize, usize),

  #[error("WAL not open / WAL 未打开")]
  NotOpen,

  #[error("invalid head / 无效的头")]
  InvalidHead,

  #[error(
    "CRC mismatch: file_id={file_id}, pos={pos} / CRC 校验失败: file_id={file_id}, pos={pos}"
  )]
  CrcMismatch { file_id: u64, pos: u64 },

  #[error("cannot rm current WAL / 不能删除当前 WAL")]
  CannotRemoveCurrent,

  #[error("index update failed / 索引更新失败")]
  UpdateFailed,

  #[error("write channel closed / 写入通道已关闭")]
  ChannelClosed,

  #[error("io error / IO 错误: {0}")]
  Io(#[from] std::io::Error),

  #[error("lock error / 锁错误: {0}")]
  Lock(#[from] jdb_lock::Error),

  #[error("checkpoint corrupted at {path} / 检查点损坏: {path}")]
  CheckpointCorrupt { path: PathBuf },

  #[error("checkpoint error / 检查点错误: {0}")]
  Ckp(#[from] jdb_ckp::Error),
}

impl From<jdb_base::HeadError> for Error {
  fn from(err: jdb_base::HeadError) -> Self {
    Self::CrcMismatch {
      file_id: err.file_id,
      pos: err.pos,
    }
  }
}

pub type Result<T> = std::result::Result<T, Error>;
