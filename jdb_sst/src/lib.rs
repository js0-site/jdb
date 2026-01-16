#![cfg_attr(docsrs, feature(doc_cfg))]

mod compress;
mod conf;
pub mod disk;
pub use compress::Compress;
pub use conf::{Conf, Config};

pub const MAGIC_VER: u64 = u64::from_be_bytes(*b"sst00001");
