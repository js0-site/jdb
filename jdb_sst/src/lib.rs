#![cfg_attr(docsrs, feature(doc_cfg))]

//! SSTable - Sorted String Table base structures
//! 有序字符串表基础数据结构

pub mod block;
pub mod conf;
pub mod error;
mod foot;
mod multi;
mod order;

pub use conf::{Conf, default};
pub use error::{Error, Result};
pub use foot::{FOOT_SIZE, Foot, VERSION};
pub use multi::{Item, Multi};
pub use order::{Asc, Desc, Order};
