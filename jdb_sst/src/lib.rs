#![cfg_attr(docsrs, feature(doc_cfg))]

use zerocopy::little_endian::U64;

mod compress;
mod conf;
mod foot;

pub use compress::Compress;
pub use conf::{Conf, Config};
pub use foot::{Foot, FootCrc};

pub const MAGIC_VER: U64 = U64::new(u64::from_be_bytes(*b"jdbSst01"));
