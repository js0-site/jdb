#![cfg_attr(docsrs, feature(doc_cfg))]

mod add_ext;
mod atom_write;
mod auto_compact;
mod buf;
mod compact;
pub mod consts;
mod encode;
mod file;
mod parse;
mod try_rm;

pub use add_ext::add_ext;
pub use atom_write::AtomWrite;
pub use auto_compact::AutoCompact;
pub use buf::{buf_writer, buf_writer_with_pos};
pub use compact::{Compact, DecodeResult, Decoded, IncrCount};
pub use encode::encode;
pub use file::{read, read_write};
pub use parse::{Parse, ParseResult};
pub use try_rm::try_rm;
