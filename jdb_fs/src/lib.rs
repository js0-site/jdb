#![cfg_attr(docsrs, feature(doc_cfg))]

mod add_ext;
mod atom_write;
mod auto_compact;
mod buf;
mod compact;
pub mod consts;
mod file;
pub mod item;
mod try_rm;

pub use add_ext::add_ext;
pub use atom_write::AtomWrite;
pub use auto_compact::AutoCompact;
pub use buf::{buf_writer, buf_writer_with_pos};
pub use compact::{Compact, Decoded, IncrCount};
pub use file::{read, read_write};
pub use try_rm::try_rm;
