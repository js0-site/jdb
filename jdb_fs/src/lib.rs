#![cfg_attr(docsrs, feature(doc_cfg))]

mod atom_write;
mod auto_compact;
mod buf;
mod compact;
pub mod consts;
mod fs;
pub mod item;
pub mod load;
mod push;
mod try_rm;

pub type Len = usize;
pub type Size = u64;
pub type Pos = u64;
pub use atom_write::AtomWrite;
pub use auto_compact::AutoCompact;
pub use buf::{buf_writer, buf_writer_with_pos, BufFile};
pub use compact::{Compact, IncrCount};
pub use fs::read_write;
pub use item::{DataLen, Error, Item, Offset, Result, Row};
pub use push::push_iter;
pub use try_rm::try_rm;
