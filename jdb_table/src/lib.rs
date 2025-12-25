//! Table implementation for JDB
//! JDB 表实现

#![cfg_attr(docsrs, feature(doc_cfg))]

mod conf;
mod error;
mod table;

pub use conf::{Conf, Keep};
pub use error::{Error, Result};
pub use table::{Commit, Table};
