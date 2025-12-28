use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("invalid WAL header / 无效的 WAL 文件头")]
  InvalidHeader,

  #[error("invalid magic / 无效的魔数")]
  InvalidMagic,

  #[error("key length {0} exceeds limit {1}")]
  KeyTooLong(usize, usize),

  #[error("value length {0} exceeds limit {1}")]
  ValTooLong(usize, usize),

  #[error("invalid flag combination: key={0:#04X} val={1:#04X}")]
  InvalidFlag(u8, u8),

  #[error("WAL not open / WAL 未打开")]
  NotOpen,

  #[error("invalid head / 无效的头")]
  InvalidHead,

  #[error("CRC mismatch: expected {0}, got {1}")]
  CrcMismatch(u32, u32),

  #[error("cannot rm current WAL / 不能删除当前 WAL")]
  CannotRemoveCurrent,

  #[error("file locked / 文件已锁定")]
  Locked,

  #[error("index update failed / 索引更新失败")]
  UpdateFailed,

  #[error("write channel closed / 写入通道已关闭")]
  ChannelClosed,

  #[error("io error: {0}")]
  Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
