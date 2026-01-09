#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable - Sorted String Table base structures
//! 有序字符串表基础数据结构

pub mod block;
mod conf;
mod consts;
mod discard;
mod error;
mod foot;
mod multi;
mod order;

pub use conf::{Conf, default};
pub use consts::{HOUR_MS, TMP_DIR};
pub use discard::{NoDiscard, OnDiscard};
pub use error::{Error, Result};
pub use foot::{FOOT_SIZE, Foot, VERSION};
pub use jdb_base::sst::Meta;
pub use multi::{Item, Multi};
pub use order::{Asc, Desc, Order};
