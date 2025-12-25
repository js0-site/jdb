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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
