#![cfg_attr(docsrs, feature(doc_cfg))]

mod crc;
mod page;
mod ptr;

pub use crc::{crc32, verify};
pub use page::{page_type, PageHeader, PAGE_MAGIC};
pub use ptr::BlobPtr;
