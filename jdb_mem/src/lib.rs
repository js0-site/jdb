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
mod error;

pub use disk::Disk;
pub use error::{Error, Result};
