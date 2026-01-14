pub mod error;
mod levels;
mod meta;
pub mod sink;

use std::{cell::RefCell, rc::Rc};

use file_lru::FileLru;
pub use levels::Levels;
pub use meta::Meta;

/// Shared FileLru type alias
/// 共享 FileLru 类型别名
pub type Lru = Rc<RefCell<FileLru>>;
pub type Id = u64;

pub const LEVEL_LEN_MINUS_1: usize = jdb_base::sst::Level::LEN - 1;
