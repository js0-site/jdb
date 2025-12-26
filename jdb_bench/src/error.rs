use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
  #[error("Histogram error: {0}")]
  Histogram(#[from] hdrhistogram::CreationError),

  #[error("Histogram record error: {0}")]
  HistogramRecord(#[from] hdrhistogram::RecordError),

  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("JSON error: {0}")]
  Json(#[from] sonic_rs::Error),

  #[cfg(feature = "jdb_slab")]
  #[error("JdbSlab error: {0}")]
  JdbSlab(#[from] jdb_slab::Error),

  #[cfg(feature = "fjall")]
  #[error("Fjall error: {0}")]
  Fjall(#[from] fjall::Error),

  #[cfg(feature = "rocksdb")]
  #[error("RocksDB error: {0}")]
  RocksDb(#[from] rocksdb::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
