#![cfg_attr(docsrs, feature(doc_cfg))]

mod consts;
mod crc;
mod error;
mod page;
mod ptr;

pub use consts::*;
pub use error::{Error, Result};
pub use crc::{crc32, verify};
pub use page::{page_type, PageHeader, PAGE_MAGIC};
pub use ptr::BlobPtr;
