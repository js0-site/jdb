//! Memory table implementation for JDB
//! JDB 的内存表实现

mod impl_trait;
mod iter;
mod map;
mod mem;
mod merge;

pub use map::Map;
pub use mem::Mem;

mod disk;
use std::fmt::Debug;

pub use disk::{Disk, Error};

#[cold]
pub(crate) fn log_err(msg: &str, err: impl Debug) {
  log::error!("{}: {:?}", msg, err);
}
