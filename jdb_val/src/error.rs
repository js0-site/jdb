use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
  KeyTooLong,
  ValueTooLong,
  InvalidValType,
  InvalidCompression,
  ChecksumMismatch,
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Error::KeyTooLong => write!(f, "Key too long for inline storage (max 64B)"),
      Error::ValueTooLong => write!(f, "Value too long for inline storage (max 16B)"),
      Error::InvalidValType => write!(f, "Invalid record type"),
      Error::InvalidCompression => write!(f, "Invalid compression algorithm"),
      Error::ChecksumMismatch => write!(f, "Header checksum mismatch"),
    }
  }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
