use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
  #[error("invalid WAL header / 无效的 WAL 文件头")]
  InvalidHeader,

  #[error("invalid magic / 无效的魔数")]
  InvalidMagic,

  #[error("data length {0} exceeds limit {1}")]
  DataTooLong(usize, usize),

  #[error("WAL not open / WAL 未打开")]
  NotOpen,

  #[error("invalid head / 无效的头")]
  InvalidHead,

  #[error("CRC mismatch: expected {0}, got {1}")]
  CrcMismatch(u32, u32),

  #[error("hash mismatch / 哈希不匹配")]
  HashMismatch,

  #[error("cannot rm current WAL / 不能删除当前 WAL")]
  CannotRemoveCurrent,

  #[error("index update failed / 索引更新失败")]
  UpdateFailed,

  #[error("write channel closed / 写入通道已关闭")]
  ChannelClosed,

  #[error("io error: {0}")]
  Io(#[from] std::io::Error),

  #[error("LZ4 decompress failed / LZ4 解压缩失败")]
  DecompressFailed,

  #[error("lock error: {0}")]
  Lock(#[from] jdb_lock::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
